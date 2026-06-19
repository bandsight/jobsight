import bz2
import json
from io import BytesIO
from zipfile import ZipFile

from bs4 import BeautifulSoup

from jobsight.extractors.documents import document_urls_from_soup, text_from_document_bytes
from jobsight.extractors.band import extract_band
from jobsight.extractors.closing_date import extract_closing_date
from jobsight.extractors.salary import extract_salary, normalise_salary_fields
from jobsight.extractors.title import best_job_title, clean_role_title
from jobsight.outputs import append_jsonl, build_job_board_data, rebuild_outputs, validate_board_payload, write_json
from jobsight.platforms.generic import is_probable_job_raw, job_from_detail
from jobsight.platforms.pulse import _merge_with_detail_raw, _raw_from_pulse_job_info
from jobsight.text import compact_text


def test_extracts_explicit_band():
    result = extract_band("Classification: Band 6 role")
    assert result.band == 6
    assert result.status == "explicit_band"


def test_extracts_band_from_wider_classification_text():
    assert extract_band("Municipal Employee Level 4A").band == 4
    assert extract_band("Remuneration band six plus super").band == 6
    assert extract_band("Level 7 under the Local Government Award").band == 7


def test_extracts_salary_range_and_period():
    result = extract_salary("$91,000 - $102,000 per annum plus super")
    assert result.minimum == 91000
    assert result.maximum == 102000
    assert result.period == "year"


def test_normalises_compact_and_uncommaed_annual_salary():
    compact = extract_salary("$133k-$149k + super")
    assert compact.minimum == 133000
    assert compact.maximum == 149000
    assert compact.period == "year"

    uncommaed = extract_salary("$77725 to $89201 pa")
    assert uncommaed.minimum == 77725
    assert uncommaed.maximum == 89201
    assert uncommaed.period == "year"


def test_salary_period_uses_amount_not_roster_words():
    result = extract_salary("Band 7A 36 Hour Week AUD $108,150.75 - $120,323.65")
    assert result.period == "year"


def test_salary_ignores_reference_numbers_and_keeps_actual_package():
    result = extract_salary(
        "Reference number 35-2026 Job type Temporary full-time Package $50.94 per hour "
        "Information pack Information on how to apply"
    )

    assert result.minimum == 50.94
    assert result.maximum == 50.94
    assert result.period == "hour"


def test_salary_keeps_pa_suffix():
    result = extract_salary("Competitive Salary: Band 7 - $114,501.15 p.a + 12% superannuation")

    assert result.text == "$114,501.15 p.a + 12% superannuation"
    assert result.period == "year"


def test_salary_text_stops_before_information_pack_and_keeps_decimal_allowance():
    result = extract_salary(
        "Reference number 31-2026 Job type Permanent full-time Package commencing at "
        "$33.82 per hour (plus $1.05 per hour industry allowance) Information pack "
        "Information on how to apply"
    )

    assert result.text == "$33.82 per hour (plus $1.05 per hour industry allowance)"
    assert result.minimum == 33.82
    assert result.period == "hour"


def test_salary_normalisation_recovers_compact_range_from_stale_values():
    result = extract_salary("$127K?$143K per annum (dependent on experience) + superannuation About the team")

    assert result.minimum == 127000
    assert result.maximum == 143000
    assert result.text == "$127K?$143K per annum (dependent on experience) + superannuation"


def test_salary_text_trims_parenthesised_hourly_equivalent_for_annual_salary():
    result = extract_salary("Band 4 - Starting at $72,735 pa ($36.80 per hour) + Super")

    assert result.text == "$72,735 pa"
    assert result.minimum == 72735
    assert result.period == "year"


def test_salary_text_stops_before_non_salary_role_benefits():
    text = (
        "Local Laws Officer Join a dynamic, investigations team that supports each other to succeed "
        "Deliver meaningful outcomes to the community in safety and amenity $91,334 - $98,476 per annum "
        "(plus Super) Tool of trade vehicle provided (non-hybrid position) Join us in building a professional"
    )
    result = extract_salary(text)

    assert result.text == "$91,334 - $98,476 per annum (plus Super)"
    assert result.minimum == 91334
    assert result.maximum == 98476
    assert result.period == "year"
    assert normalise_salary_fields(text, 91334, 98476, "year").text == "$91,334 - $98,476 per annum (plus Super)"


def test_compact_text_removes_placeholder_runs_without_touching_single_separator():
    assert compact_text("Band 4 ?????????????") == "Band 4"
    assert compact_text("$127K?$143K") == "$127K?$143K"


def test_salary_normalisation_repairs_malformed_comma_suffix():
    result = normalise_salary_fields("$70,9374 per annum inclusive of superannuation", 70937, 70937, "year")

    assert result.text == "$70,937 per annum inclusive of superannuation"
    assert result.minimum == 70937


def test_recovers_generic_role_title_from_url():
    assert best_job_title(
        "Recommended for you",
        url="https://careers.bayside.vic.gov.au/jobs/team-leader-recreation-bayside-vic-australia",
        council_key="Bayside",
    ) == "Team Leader Recreation"


def test_cleans_trailing_location_and_hash_from_role_title():
    assert clean_role_title("Road Sweeping Cleansing Supervisor Seaford Vic Australia 8831 4e16 B446") == (
        "Road Sweeping Cleansing Supervisor Seaford"
    )


def test_cleans_structured_metadata_from_role_title():
    assert clean_role_title("Change Analyst Type Full-time Duration Ongoing Salary") == "Change Analyst"


def test_cleans_elmo_search_prefix_from_role_title():
    assert clean_role_title("Search Jobs Aquatic Services Officer") == "Aquatic Services Officer"


def test_cleans_dangling_parenthesis_from_role_title():
    assert clean_role_title("Theatre Technician (") == "Theatre Technician"


def test_cleans_trailing_month_from_role_title():
    assert clean_role_title("Rapid Response Officer May") == "Rapid Response Officer"


def test_finds_document_urls_in_pdfjs_iframe():
    soup = BeautifulSoup(
        '<iframe src="/pdfjs/web/viewer.html?file=%2Fuploads%2Fjobs%2Fdocuments%2Fpd.pdf"></iframe>',
        "lxml",
    )

    assert document_urls_from_soup(soup, "https://example.test/job/1") == [
        "https://example.test/uploads/jobs/documents/pd.pdf"
    ]


def test_extracts_band_from_attached_position_document():
    soup = BeautifulSoup(
        '<main><h1>Theatre Technician</h1><p>Casual role</p>'
        '<a href="/pd.docx">Download Job Specification</a>'
        '<aside>Related job Salary: Band 5</aside></main>',
        "lxml",
    )
    raw, _ = job_from_detail(
        {},
        "Theatre Technician",
        "https://example.test/job/theatre-technician",
        soup,
        client=_DocumentClient(_docx_bytes("CLASSIFICATION Band 4")),
    )

    assert raw["band"] == 4
    assert raw["classification_status"] == "explicit_band"


def test_councildirect_detail_uses_document_band_and_page_salary():
    soup = BeautifulSoup(
        '<main><h1>Theatre Technician (Casual)</h1>'
        '<div class="salery"><h4>Salary</h4><h6>Hourly Rate AUD $44.62 - $47.48</h6></div>'
        '<a href="/pd.docx">Download Job Specification</a>'
        '<aside>Related job Salary AUD $95,321.00 - $111,656.00</aside></main>',
        "lxml",
    )
    raw, _ = job_from_detail(
        {"short_name": "Ballarat", "council_name": "Ballarat City Council"},
        "Theatre Technician (",
        "https://www.councildirect.com.au/job/theatre-technician-casual",
        soup,
        client=_DocumentClient(_docx_bytes("CLASSIFICATION Band 4")),
    )

    assert raw["title"] == "Theatre Technician (Casual)"
    assert raw["band"] == 4
    assert raw["advertised_salary_min"] == 44.62
    assert raw["advertised_salary_max"] == 47.48
    assert raw["advertised_salary_period"] == "hour"


def test_recovers_title_from_position_description_heading():
    assert best_job_title(
        "Main navigation",
        body="CENTRE OPERATOR CLASSIFICATION: Band 4 OCCUPANT: Vacant",
    ) == "Centre Operator"


def test_recovers_title_from_created_position_description_heading():
    assert best_job_title(
        "Breadcrumb",
        body=(
            "CREATED 18/05/2026 PAGE 1 Position Description MULTI-PURPOSE CREW (BRIGHT) "
            "Alpine Shire Council's Values - ICARE Innovation"
        ),
    ) == "Multi-Purpose Crew (Bright)"


def test_recovers_title_from_dismissed_announcement_slug():
    assert best_job_title(
        "1 Dismissed Announcement",
        url="https://www.murrindindi.vic.gov.au/Council/Jobs-and-Tenders/Vacant-Positions/Geospatial-Data-and-Systems-Officer-May-2026",
        council_key="Murrindindi",
    ) == "Geospatial Data and Systems Officer"


def test_recovers_title_from_successfactors_javascript_shell():
    assert best_job_title(
        "JavaScript is turned off in your web browser. Turn it on to take full advantage of this site, then refresh the page.",
        body="Team Leader Permits and Events New role to lead our busy permits and events programs that bring our community to life",
    ) == "Team Leader Permits and Events"


def test_extracts_closing_date():
    result = extract_closing_date("Applications close 5pm, Sunday 14 June 2026")
    assert result.date == "2026-06-14"


def test_rebuild_uses_previous_jobs_when_seed_archive_is_unreadable(tmp_path):
    data_root = tmp_path / "data"
    observation_root = data_root / "observations"
    observation_root.mkdir(parents=True)
    (observation_root / "2026-05-27-seed.jsonl.bz2").write_bytes(b"not bzip2")

    previous_job = _job_with(
        "old-job",
        "seed-20260527",
        "2026-05-27T00:00:00Z",
        title="Planning Officer",
        salary="$80,000 - $90,000 per annum",
        description="Applications close soon.",
    )
    write_json(data_root / "all-jobs.json", {"jobs": [previous_job]})
    append_jsonl(observation_root / "2026-05-28.jsonl", [_job("new-job", "run-20260528", "2026-05-28T00:00:00Z")])

    summary = rebuild_outputs(data_root, [], run_id="run-20260528")

    assert summary["current_jobs"] == 1
    assert summary["report_jobs"] == 2
    assert summary["observations"] == 2
    payload = json.loads((data_root / "current-jobs.json").read_text(encoding="utf-8"))
    statuses = {job["job_id"]: job["observed_status"] for job in payload["jobs"]}
    assert statuses == {"new-job": "seen_latest_run"}
    report_payload = json.loads((data_root / "report-jobs.json").read_text(encoding="utf-8"))
    assert {job["job_id"] for job in report_payload["jobs"]} == {"old-job", "new-job"}
    board_payload = json.loads((data_root / "job-board-data.json").read_text(encoding="utf-8"))
    assert {job["job_uid"] for job in board_payload["jobs"]} == {"old-job", "new-job"}
    all_payload = json.loads((data_root / "all-jobs.json").read_text(encoding="utf-8"))
    all_statuses = {job["job_id"]: job["observed_status"] for job in all_payload["jobs"]}
    assert all_statuses == {"old-job": "not_seen_latest_run", "new-job": "seen_latest_run"}


def test_rebuild_keeps_full_valid_seed_observations(tmp_path):
    data_root = tmp_path / "data"
    observation_root = data_root / "observations"
    observation_root.mkdir(parents=True)
    rows = [
        _job("same-job", "seed-20260527", "2026-05-27T00:00:00Z"),
        _job("same-job", "seed-20260527", "2026-05-27T00:00:01Z"),
    ]
    seed = "\n".join(json.dumps(row) for row in rows).encode("utf-8")
    (observation_root / "2026-05-27-seed.jsonl.bz2").write_bytes(bz2.compress(seed))
    write_json(data_root / "all-jobs.json", {"jobs": [_job("same-job", "seed-20260527", "2026-05-27T00:00:01Z")]})

    summary = rebuild_outputs(data_root, [], run_id="seed-20260527")

    assert summary["current_jobs"] == 1
    assert summary["report_jobs"] == 1
    assert summary["observations"] == 2
    payload = json.loads((data_root / "current-jobs.json").read_text(encoding="utf-8"))
    assert payload["jobs"][0]["sighting_count"] == 2


def test_rebuild_keeps_failed_source_rows_in_history_not_current(tmp_path):
    data_root = tmp_path / "data"
    observation_root = data_root / "observations"
    observation_root.mkdir(parents=True)
    append_jsonl(observation_root / "2026-05-28.jsonl", [_job("old-job", "run-20260528", "2026-05-28T00:00:00Z")])

    summary = rebuild_outputs(
        data_root,
        [],
        run_id="run-20260529",
        failed_source_ids={"sample-source"},
    )

    assert summary["current_jobs"] == 0
    assert summary["report_jobs"] == 1
    payload = json.loads((data_root / "current-jobs.json").read_text(encoding="utf-8"))
    assert payload["jobs"] == []
    report_payload = json.loads((data_root / "report-jobs.json").read_text(encoding="utf-8"))
    assert report_payload["jobs"][0]["observed_status"] == "source_unavailable_latest_run"
    board_payload = json.loads((data_root / "job-board-data.json").read_text(encoding="utf-8"))
    assert board_payload["jobs"][0]["observed_status"] == "source_unavailable_latest_run"
    all_payload = json.loads((data_root / "all-jobs.json").read_text(encoding="utf-8"))
    assert all_payload["jobs"][0]["observed_status"] == "source_unavailable_latest_run"


def test_board_data_filters_navigation_and_marketing_rows():
    breadcrumb = _job_with(
        "breadcrumb",
        "run-20260528",
        "2026-05-28T00:00:00Z",
        title="Breadcrumb",
        url="https://www.ararat.vic.gov.au/council/careers/cleaner",
        description="Page Page URL Is this page useful? Yes Somewhat No Comments (Optional)",
    )
    breadcrumb.update({"classification_status": "explicit_band", "classification_band": "Band 1"})
    jora_search = _job_with(
        "jora-search",
        "run-20260528",
        "2026-05-28T00:00:00Z",
        title="Search 401,741 jobs now",
        url="https://au.jora.com/",
        description="Don't let rejection get you down! Take the opportunity to ask for feedback.",
    )
    jora_search.update({"classification_status": "explicit_band", "classification_band": "Band 1"})
    jobs = [
        _job_with("about-region", "run-20260528", "2026-05-28T00:00:00Z", title="About the Region"),
        _job_with("facebook", "run-20260528", "2026-05-28T00:00:00Z", title="Facebook", url="https://facebook.com/example"),
        _job_with("employment-hero", "run-20260528", "2026-05-28T00:00:00Z", title="Employment. Intelligently Run."),
        breadcrumb,
        jora_search,
        _job_with(
            "real-role",
            "run-20260528",
            "2026-05-28T00:00:00Z",
            title="Senior Project Engineer",
            salary="$110,534 - $119,249 a year",
            description="Full-time role with applications closing soon.",
        ),
    ]
    payload = build_job_board_data(jobs, [], {"generated_at": "2026-05-28T00:00:00Z", "sources_configured": 1})

    assert [job["job_title"] for job in payload["jobs"]] == ["Senior Project Engineer"]


def test_board_data_requires_salary_or_band_signal():
    no_compensation = _job_with(
        "no-compensation",
        "run-20260528",
        "2026-05-28T00:00:00Z",
        title="Community Services Officer",
        description="Applications close 12 June 2026. Full-time role supporting community programs.",
    )
    salary_row = _job_with(
        "salary-row",
        "run-20260528",
        "2026-05-28T00:00:00Z",
        title="Senior Project Engineer",
        salary="$110,534 - $119,249 a year",
        description="Full-time role with applications closing soon.",
    )

    payload = build_job_board_data(
        [no_compensation, salary_row],
        [],
        {"generated_at": "2026-05-28T00:00:00Z", "sources_configured": 1},
    )

    assert [job["job_title"] for job in payload["jobs"]] == ["Senior Project Engineer"]



def test_board_data_dedupes_same_listing_url_by_quality():
    old_row = _job_with(
        "old-row",
        "run-20260527",
        "2026-05-27T00:00:00Z",
        title="Multi Purpose Crew",
        url="https://www.alpineshire.vic.gov.au/about-us/careers/current-vacancies/multi-purpose-crew-0",
        salary="$34.42 per hour",
    )
    rich_row = _job_with(
        "rich-row",
        "run-20260528",
        "2026-05-28T00:00:00Z",
        title="Breadcrumb",
        url="https://www.alpineshire.vic.gov.au/about-us/careers/current-vacancies/multi-purpose-crew-0",
        salary="Band 3A hourly rate: $34.42 per hour",
        description=(
            "CREATED 18/05/2026 PAGE 1 Position Description MULTI-PURPOSE CREW (BRIGHT) "
            "Alpine Shire Council's Values - ICARE Innovation"
        ),
    )
    rich_row.update({"classification_status": "explicit_band", "classification_band": "Band 3"})

    payload = build_job_board_data([old_row, rich_row], [], {"generated_at": "2026-05-28T00:00:00Z", "sources_configured": 1})

    assert [(job["job_uid"], job["job_title"], job["standard_band_number"]) for job in payload["jobs"]] == [
        ("rich-row", "Multi-Purpose Crew (Bright)", 3)
    ]


def test_board_data_keeps_distinct_roles_on_shared_board_url():
    jobs = [
        _job_with(
            "planner",
            "run-20260528",
            "2026-05-28T00:00:00Z",
            title="Strategic Planner",
            url="https://example.test/careers",
            salary="$90,000 - $100,000 per annum",
        ),
        _job_with(
            "engineer",
            "run-20260528",
            "2026-05-28T00:00:00Z",
            title="Project Engineer",
            url="https://example.test/careers",
            salary="$100,000 - $110,000 per annum",
        ),
    ]

    payload = build_job_board_data(jobs, [], {"generated_at": "2026-05-28T00:00:00Z", "sources_configured": 1})

    assert {job["job_uid"] for job in payload["jobs"]} == {"planner", "engineer"}


def test_board_data_dedupes_mirrored_listing_by_similar_title_and_salary():
    mirror = _job_with(
        "mirror-row",
        "seed-20260527",
        "2026-05-27T00:00:00Z",
        title="Theatre Technician (",
        url="https://www.councildirect.com.au/job/theatre-technician-casual-1779161217-6a0bd881061e4",
        salary="Hourly Rate AUD $44.62 - $47.48",
    )
    mirror.update({"work_type": "Casual"})
    rich = _job_with(
        "pulse-row",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Theatre Technician (Casual)",
        url="https://ballarat.pulsesoftware.com/Pulse/job/Dtbutp/Theatre-Technician-Casual?source=public",
        salary="Hourly Rate AUD $44.62 - $47.48",
        description="Casual venue role. Band 4 classification.",
    )
    rich.update({"classification_status": "explicit_band", "classification_band": "Band 4", "band": 4, "work_type": "Casual"})

    payload = build_job_board_data([mirror, rich], [], {"generated_at": "2026-05-29T00:00:00Z", "sources_configured": 1})

    assert [(job["job_uid"], job["job_title"], job["standard_band_number"]) for job in payload["jobs"]] == [
        ("pulse-row", "Theatre Technician (Casual)", 4)
    ]


def test_board_data_dedupes_same_platform_slug_variants():
    old_row = _job_with(
        "old-pulse-row",
        "seed-20260527",
        "2026-05-27T00:00:00Z",
        title="Theatre Technician (Casual)",
        url="https://ballarat.pulsesoftware.com/Pulse/job/Dtbutp/Theatre-Technician--Casual-",
        salary="Hourly Rate AUD $44.62 - $47.48",
    )
    old_row.update({"classification_status": "explicit_band", "classification_band": "Band 4", "band": 4, "work_type": "Casual"})
    current = _job_with(
        "current-pulse-row",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Theatre Technician (Casual)",
        url="https://ballarat.pulsesoftware.com/Pulse/job/Dtbutp/Theatre-Technician-Casual?source=public",
        salary="Hourly Rate AUD $44.62 - $47.48",
    )
    current.update({
        "classification_status": "explicit_band",
        "classification_band": "Band 4",
        "band": 4,
        "work_type": "Casual",
        "observed_status": "seen_latest_run",
    })

    payload = build_job_board_data([old_row, current], [], {"generated_at": "2026-05-29T00:00:00Z", "sources_configured": 1})

    assert [job["job_uid"] for job in payload["jobs"]] == ["current-pulse-row"]


def test_board_data_dedupes_weak_mirror_against_official_role():
    mirror = _job_with(
        "jora-row",
        "seed-20260527",
        "2026-05-27T00:00:00Z",
        title="Geospatial Data and Systems Officer",
        url="https://au.jora.com/job/Systems-Officer-072734cf46ca529259def4afc62520f1",
        description="Geospatial Data and Systems Officer Murrindindi Shire Council Alexandra VIC Full time",
    )
    official = _job_with(
        "official-row",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Geospatial Data and Systems Officer May",
        url="https://www.murrindindi.vic.gov.au/Council/Jobs-and-Tenders/Vacant-Positions/Geospatial-Data-and-Systems-Officer-May-2026",
        description="Reference Number SF/5436 Job Type Full Time Package Band 7 plus 12% Superannuation",
    )
    official.update({"classification_status": "explicit_band", "classification_band": "Band 7", "band": 7})

    payload = build_job_board_data([mirror, official], [], {"generated_at": "2026-05-29T00:00:00Z", "sources_configured": 1})

    assert [(job["job_uid"], job["job_title"], job["standard_band_number"]) for job in payload["jobs"]] == [
        ("official-row", "Geospatial Data and Systems Officer", 7)
    ]


def test_board_data_suppresses_band_when_salary_matches_peer_band_profile():
    band4_peer = _job_with(
        "band4-peer",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Permits Officer",
        salary="$76,517 - $82,466 per annum",
        description="Applications close soon.",
    )
    band4_peer.update({
        "council_key": "BOROONDARA",
        "classification_status": "explicit_band",
        "band": 4,
        "advertised_salary_min": 76517,
        "advertised_salary_max": 82466,
        "advertised_salary_period": "year",
    })
    band5_peer = _job_with(
        "band5-peer",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Insurance Officer",
        salary="$84,770 - $98,205 per annum",
        description="Applications close soon.",
    )
    band5_peer.update({
        "council_key": "BOROONDARA",
        "classification_status": "explicit_band",
        "band": 5,
        "advertised_salary_min": 84770,
        "advertised_salary_max": 98205,
        "advertised_salary_period": "year",
    })
    disputed = _job_with(
        "disputed-row",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Local Laws Officer",
        salary="$91,334 - $98,476 per annum",
        description="Applications close soon.",
    )
    disputed.update({
        "council_key": "BOROONDARA",
        "classification_status": "explicit_band",
        "band": 4,
        "advertised_salary_min": 91334,
        "advertised_salary_max": 98476,
        "advertised_salary_period": "year",
        "evidence": {"band_text": "CLASSIFICATION: Band 4"},
    })

    payload = build_job_board_data(
        [band4_peer, band5_peer, disputed],
        [],
        {"generated_at": "2026-05-29T00:00:00Z", "sources_configured": 1},
    )

    row = next(job for job in payload["jobs"] if job["job_uid"] == "disputed-row")
    assert "standard_band_number" not in row
    assert row["classification_status"] == "salary_band_conflict"


def test_board_data_keeps_band_when_band_and_salary_share_source_text():
    lower_band_peer = _job_with(
        "lower-band6-peer",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Asset Officer",
        salary="$91,938 - $102,015 per annum",
        description="Band 6 $91,938 - $102,015 per annum Applications close soon.",
    )
    lower_band_peer.update({
        "council_key": "BALLARAT",
        "classification_status": "explicit_band",
        "band": 6,
        "advertised_salary_min": 91938,
        "advertised_salary_max": 102015,
        "advertised_salary_period": "year",
    })
    band7_peer = _job_with(
        "band7-peer",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Library Coordinator",
        salary="$103,987 - $115,565 per annum",
        description="Band 7 $103,987 - $115,565 per annum Applications close soon.",
    )
    band7_peer.update({
        "council_key": "BALLARAT",
        "classification_status": "explicit_band",
        "band": 7,
        "advertised_salary_min": 103987,
        "advertised_salary_max": 115565,
        "advertised_salary_period": "year",
    })
    annualised = _job_with(
        "annualised-band6",
        "run-20260529",
        "2026-05-29T00:00:00Z",
        title="Events Officer",
        salary="$106,575 - $118,257 per annum",
        description="Permanent Position Band 6 Annualised $106,575 - $118,257 + super Applications close soon.",
    )
    annualised.update({
        "council_key": "BALLARAT",
        "classification_status": "explicit_band",
        "band": 6,
        "advertised_salary_min": 106575,
        "advertised_salary_max": 118257,
        "advertised_salary_period": "year",
    })

    payload = build_job_board_data(
        [lower_band_peer, band7_peer, annualised],
        [],
        {"generated_at": "2026-05-29T00:00:00Z", "sources_configured": 1},
    )

    row = next(job for job in payload["jobs"] if job["job_uid"] == "annualised-band6")
    assert row["standard_band_number"] == 6
    assert row["classification_status"] == "explicit_band"


def test_pulse_job_info_extracts_band_from_description_text():
    raw = _raw_from_pulse_job_info(
        {"url": "https://ballarat.pulsesoftware.com/Pulse/jobs"},
        {
            "LinkId": "abc123",
            "JobInfo": {
                "Title": "Theatre Technician (Casual)",
                "Description": "<p>Band 4 casual role supporting venues.</p>",
                "Compensation": "Hourly Rate AUD $44.62 - $47.48",
                "EmploymentType": "Casual",
            },
        },
    )

    assert raw["band"] == 4
    assert raw["classification_status"] == "explicit_band"
    assert raw["evidence"]["band_text"] == "Band 4"


def test_pulse_detail_merge_adds_band_from_detail_document():
    raw = {
        "title": "Theatre Technician (",
        "url": "https://ballarat.pulsesoftware.com/Pulse/job/Dtbutp/Theatre-Technician-Casual?source=public",
        "classification_status": "salary_only",
        "advertised_salary_text": "Hourly Rate AUD $44.62 - $47.48",
        "work_type": "Casual",
        "evidence": {"salary_text": "$44.62 - $47.48"},
        "description_excerpt": "Theatre Technician (Casual) Casual Salary: Hourly Rate AUD $44.62 - $47.48",
    }
    detail = {
        "title": "Theatre Technician (Casual)",
        "band": 4,
        "classification_status": "explicit_band",
        "evidence": {"band_text": "Band 4", "document_urls": ["https://example.test/pd.pdf"]},
        "description_excerpt": "POSITION DESCRIPTION Theatre Technician CLASSIFICATION Band 4",
        "description_hash": "abc",
        "description_status": "document",
    }

    merged = _merge_with_detail_raw(raw, detail)

    assert merged["title"] == "Theatre Technician (Casual)"
    assert merged["band"] == 4
    assert merged["classification_status"] == "explicit_band"
    assert merged["advertised_salary_text"] == "Hourly Rate AUD $44.62 - $47.48"
    assert merged["evidence"]["band_text"] == "Band 4"


def test_board_quality_gate_rejects_count_outliers():
    jobs = [
        _job_with(
            f"job-{index}",
            "run-20260528",
            "2026-05-28T00:00:00Z",
            title=f"Project Officer {index}",
            salary="$80,000 - $90,000 per annum",
            description="Applications close soon.",
        )
        for index in range(51)
    ]
    payload = build_job_board_data(jobs, [], {"generated_at": "2026-05-28T00:00:00Z", "sources_configured": 1})

    try:
        validate_board_payload(payload)
    except ValueError as exc:
        assert "count outlier" in str(exc)
    else:
        raise AssertionError("Expected board outlier validation to fail")


def test_board_quality_gate_allows_cumulative_growth_across_runs():
    jobs = [
        _job_with(
            f"job-{index}",
            f"run-202605{index:02d}",
            f"2026-05-{index:02d}T00:00:00Z",
            title=f"Project Officer {index}",
            salary="$80,000 - $90,000 per annum",
            description="Applications close soon.",
        )
        for index in range(1, 52)
    ]
    payload = build_job_board_data(jobs, [], {"generated_at": "2026-05-28T00:00:00Z", "sources_configured": 1})

    validate_board_payload(payload)
    assert len(payload["jobs"]) == 51


def test_generic_parser_rejects_marketing_and_navigation_rows():
    assert not is_probable_job_raw({
        "title": "Find your next role with Employment Hero Jobs",
        "url": "https://employmenthero.com/jobs",
        "advertised_salary_text": "$30,000 - $50,000",
    })
    assert not is_probable_job_raw({
        "title": "Main navigation",
        "url": "https://example.test/jobs/project-officer",
        "advertised_salary_text": "$80,000 - $90,000 per annum",
    })
    assert is_probable_job_raw({
        "title": "Theatre Technician Casual",
        "url": "https://www.councildirect.com.au/job/theatre-technician-casual",
        "advertised_salary_text": "$40 - $45 per hour",
    })


def _job(job_id: str, run_id: str, observed_at: str) -> dict[str, str]:
    return _job_with(
        job_id,
        run_id,
        observed_at,
        title="Project Officer",
        salary="$80,000 - $90,000 per annum",
        description="Applications close soon.",
    )


class _DocumentClient:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def get_bytes(self, url: str):
        return type(
            "Result",
            (),
            {
                "url": url,
                "status_code": 200,
                "content": self.content,
                "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "error": None,
            },
        )()


def _docx_bytes(text: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(
            "word/document.xml",
            f'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            f"<w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body></w:document>",
        )
    return buffer.getvalue()


def _job_with(
    job_id: str,
    run_id: str,
    observed_at: str,
    *,
    title: str | None = None,
    url: str | None = None,
    salary: str = "",
    description: str = "",
) -> dict[str, str]:
    return {
        "job_id": job_id,
        "run_id": run_id,
        "observed_at": observed_at,
        "council_key": "sample",
        "council_name": "Sample Council",
        "short_name": "Sample",
        "title": title or job_id.replace("-", " ").title(),
        "url": url or f"https://example.test/{job_id}",
        "classification_status": "salary_only" if salary else "unclassified",
        "advertised_salary_text": salary,
        "description_excerpt": description,
        "source_id": "sample-source",
        "source_platform": "test",
    }
