import json
from pathlib import Path

import pytest

from jobsight.http import FetchBytesResult, FetchResult
from jobsight.intelligence import (
    empty_profiles,
    parse_source_smart,
    update_global_rules_from_quarantine,
    write_source_health,
)
from jobsight.outputs import build_job_board_data, validate_board_payload


class FakeClient:
    def __init__(self, responses):
        self.responses = responses
        self.requested = []

    def get(self, url):
        self.requested.append(url)
        text = self.responses.get(url)
        if text is None:
            return FetchResult(url=url, status_code=404, text="", content_type="", error="HTTP 404")
        content_type = "application/json" if str(text).lstrip().startswith(("{", "[")) else "text/html"
        return FetchResult(url=url, status_code=200, text=text, content_type=content_type)

    def get_bytes(self, url):
        return FetchBytesResult(url=url, status_code=404, content=b"", content_type="", error="HTTP 404")


def test_smart_parser_discovers_endpoint_and_promotes_profile():
    source = {
        "source_id": "demo-pageup",
        "council_key": "DEMO",
        "short_name": "Demo",
        "council_name": "Demo Council",
        "platform": "pageup",
        "url": "https://jobs.example.test/careers",
    }
    client = FakeClient({
        "https://jobs.example.test/careers": '<html><script>window.jobsApi="/api/jobs";</script></html>',
        "https://jobs.example.test/api/jobs": json.dumps({
            "jobs": [{
                "Title": "Library Services Officer",
                "Url": "/jobs/library-services-officer",
                "Description": "Band 5 salary $82,000 per annum. Applications close 12 June 2026.",
            }]
        }),
    })

    result = parse_source_smart(
        source,
        client,
        empty_profiles(),
        observed_at="2026-05-30T00:00:00Z",
        run_id="run-20260530",
    )

    assert len(result.jobs) == 1
    assert result.health["chosen_strategy"] == "discovered_endpoint"
    assert result.health["status"] == "ok"
    assert result.profile["preferred_strategy"] == "discovered_endpoint"
    assert result.profile["known_endpoints"][0]["url"] == "https://jobs.example.test/api/jobs"
    assert result.profile["known_endpoints"][0]["status"] == "accepted"
    assert result.jobs[0]["field_evidence"]["salary"] == "endpoint_json"


def test_smart_parser_records_endpoint_hints_when_html_strategy_wins():
    source = {
        "source_id": "demo-native",
        "council_key": "DEMO",
        "short_name": "Demo",
        "council_name": "Demo Council",
        "platform": "native_council",
        "url": "https://demo.example.test/careers",
    }
    client = FakeClient({
        "https://demo.example.test/careers": (
            '<html><script>window.jobsApi="/api/jobs";</script>'
            '<a href="/jobs/community-planner">Community Planner</a></html>'
        ),
        "https://demo.example.test/jobs/community-planner": (
            "<html><h1>Community Planner</h1>"
            "<p>Band 6. Salary $91,000 per annum. Applications close 12 June 2026.</p></html>"
        ),
    })

    result = parse_source_smart(
        source,
        client,
        empty_profiles(),
        observed_at="2026-05-30T00:00:00Z",
        run_id="run-20260530",
    )

    assert result.health["chosen_strategy"] == "platform:native_council"
    assert result.health["discovered_endpoints"][0]["url"] == "https://demo.example.test/api/jobs"
    assert result.profile["known_endpoints"][0]["url"] == "https://demo.example.test/api/jobs"
    assert result.profile["known_endpoints"][0]["status"] == "candidate"


def test_smart_parser_rejects_rows_without_salary_or_band():
    source = {
        "source_id": "demo-pageup",
        "council_key": "DEMO",
        "short_name": "Demo",
        "council_name": "Demo Council",
        "platform": "pageup",
        "url": "https://jobs.example.test/careers",
    }
    client = FakeClient({
        "https://jobs.example.test/careers": '<html><script>window.jobsApi="/api/jobs";</script></html>',
        "https://jobs.example.test/api/jobs": json.dumps({
            "jobs": [
                {
                    "Title": "Library Services Officer",
                    "Url": "/jobs/library-services-officer",
                    "Description": "Applications close 12 June 2026. Full-time role in community services.",
                },
                {
                    "Title": "Project Officer",
                    "Url": "/jobs/project-officer",
                    "Description": "Band 5 salary $82,000 per annum. Applications close 12 June 2026.",
                },
            ]
        }),
    })

    result = parse_source_smart(
        source,
        client,
        empty_profiles(),
        observed_at="2026-05-30T00:00:00Z",
        run_id="run-20260530",
    )

    assert [job["title"] for job in result.jobs] == ["Project Officer"]
    assert result.quarantine[0]["title"] == "Library Services Officer"
    assert result.quarantine[0]["reason"] == "missing_salary_or_band"



def test_smart_parser_quarantines_severe_count_spike_and_uses_fallback():
    source = {
        "source_id": "demo-pageup",
        "council_key": "DEMO",
        "short_name": "Demo",
        "council_name": "Demo Council",
        "platform": "pageup",
        "url": "https://jobs.example.test/careers",
    }
    endpoint_jobs = [
        {
            "Title": f"Project Officer {index}",
            "Url": f"/jobs/project-officer-{index}",
            "Description": "Band 5 salary $82,000 per annum. Applications close 12 June 2026.",
        }
        for index in range(60)
    ]
    client = FakeClient({
        "https://jobs.example.test/careers": '<html><script>window.jobsApi="/api/jobs";</script></html>',
        "https://jobs.example.test/api/jobs": json.dumps({"jobs": endpoint_jobs}),
    })
    profiles = empty_profiles()
    profiles["sources"]["demo-pageup"] = {
        "source_id": "demo-pageup",
        "last_good_count": 4,
        "last_good_score": 100,
        "preferred_strategy": "platform:pageup",
    }

    result = parse_source_smart(
        source,
        client,
        profiles,
        observed_at="2026-05-30T00:00:00Z",
        run_id="run-20260530",
    )

    assert result.jobs == []
    assert result.health["status"] == "degraded"
    assert result.health["fallback_used"] is True
    assert {flag["code"] for flag in result.health["drift_flags"]} & {"source_count_spike", "historical_count_spike"}
    assert result.quarantine
    assert result.quarantine[0]["reason"] == "severe_drift"


def test_global_rule_promotion_uses_cross_source_quarantine():
    profiles = empty_profiles()
    rows = [
        {"title": "Search 400,000 jobs now", "source_id": "a", "platform": "native_council", "reason": "quality_rejected"},
        {"title": "Search 400,000 jobs now", "source_id": "b", "platform": "pulse", "reason": "quality_rejected"},
        {"title": "Search 400,000 jobs now", "source_id": "c", "platform": "pageup", "reason": "quality_rejected"},
    ]

    added = update_global_rules_from_quarantine(profiles, rows)

    assert added == ["search 400,000 jobs now"]
    assert profiles["global_rules"]["reject_title_exact"] == ["search 400,000 jobs now"]


def test_board_payload_carries_evidence_fields():
    job = {
        "job_id": "abc123",
        "title": "Library Services Officer",
        "url": "https://jobs.example.test/jobs/library-services-officer",
        "council_key": "DEMO",
        "council_name": "Demo Council",
        "short_name": "Demo",
        "source_platform": "pageup",
        "source_id": "demo-pageup",
        "source_url": "https://jobs.example.test/careers",
        "source_strategy": "discovered_endpoint",
        "observed_at": "2026-05-30T00:00:00Z",
        "run_id": "run-20260530",
        "observed_status": "seen_latest_run",
        "classification_status": "explicit_band",
        "band": 5,
        "advertised_salary_text": "$82,000 per annum",
        "advertised_salary_min": 82000,
        "advertised_salary_max": 82000,
        "advertised_salary_period": "year",
        "description_excerpt": "Band 5 salary $82,000 per annum. Applications close 12 June 2026.",
        "field_evidence": {
            "title": "endpoint_json",
            "salary": "endpoint_json",
            "band": "endpoint_json",
            "closing_date": "endpoint_json",
            "description": "endpoint_json",
            "strategy": "discovered_endpoint",
        },
    }

    payload = build_job_board_data([job], [], {"schema_version": "jobsight.run_summary.v1"}, data_root=Path("missing"))

    assert payload["jobs"][0]["source_strategy"] == "discovered_endpoint"
    assert payload["jobs"][0]["field_evidence"]["salary"] == "endpoint_json"


def test_board_payload_backfills_evidence_for_historical_rows():
    job = {
        "job_id": "historic123",
        "title": "Multi-Purpose Crew",
        "url": "https://demo.example.test/jobs/multi-purpose-crew",
        "council_key": "DEMO",
        "council_name": "Demo Council",
        "short_name": "Demo",
        "source_platform": "native_council",
        "source_id": "demo-native",
        "source_url": "https://demo.example.test/careers",
        "observed_at": "2026-05-29T00:00:00Z",
        "run_id": "historic-run",
        "observed_status": "not_seen_latest_run",
        "classification_status": "explicit_band",
        "band": 3,
        "advertised_salary_text": "Band 3A hourly rate: $34.42 per hour",
        "advertised_salary_min": 34.42,
        "advertised_salary_max": 34.42,
        "advertised_salary_period": "hour",
        "closing_date": "2026-06-08",
        "closing_text": "Applications close 8 June 2026.",
        "description_excerpt": "Position description for a Band 3 operations role.",
        "description_status": "document",
    }

    payload = build_job_board_data([job], [], {"schema_version": "jobsight.run_summary.v1"}, data_root=Path("missing"))
    evidence = payload["jobs"][0]["field_evidence"]

    assert payload["jobs"][0]["source_strategy"] == "historical:platform:native_council"
    assert evidence["title"] == "detail_html"
    assert evidence["salary"] == "pdf_or_docx"
    assert evidence["band"] == "pdf_or_docx"
    assert evidence["closing_date"] == "pdf_or_docx"
    assert evidence["description"] == "pdf_or_docx"


def test_board_validation_requires_evidence_for_present_fields():
    payload = {
        "schema_version": "jobsight.board.v1",
        "jobs": [{
            "job_uid": "abc123",
            "job_title": "Library Services Officer",
            "job_url": "https://jobs.example.test/jobs/library-services-officer",
            "council_key": "DEMO",
            "classification_status": "explicit_band",
            "classification_band": "Band 5",
            "standard_band_number": 5,
            "advertised_salary_text": "$82,000 per annum",
            "description_text": "Band 5 salary $82,000 per annum. Applications close 12 June 2026.",
            "closing_at": "2026-06-12",
            "last_seen_run_id": "run-20260530",
        }],
    }

    with pytest.raises(ValueError, match="evidence gate"):
        validate_board_payload(payload)


def test_board_validation_rejects_rows_without_salary_or_band():
    payload = {
        "schema_version": "jobsight.board.v1",
        "jobs": [{
            "job_uid": "abc123",
            "job_title": "Library Services Officer",
            "job_url": "https://jobs.example.test/jobs/library-services-officer",
            "council_key": "DEMO",
            "classification_status": "unclassified",
            "classification_band": "unclassified",
            "description_text": "Applications close 12 June 2026. Full-time role in community services.",
            "closing_at": "2026-06-12",
            "last_seen_run_id": "run-20260530",
            "field_evidence": {
                "title": "detail_html",
                "description": "detail_html",
                "closing_date": "detail_html",
                "strategy": "platform:native_council",
            },
        }],
    }

    with pytest.raises(ValueError, match="compensation gate"):
        validate_board_payload(payload)



def test_source_health_includes_platform_rollups(tmp_path):
    health_path = tmp_path / "source-health.json"
    rows = [
        {
            "source_id": "demo-pageup-1",
            "platform": "pageup",
            "status": "ok",
            "chosen_strategy": "discovered_endpoint",
            "fallback_used": False,
            "quarantined_rows": 0,
            "quality": {"accepted_count": 3, "candidate_count": 4, "rejected_count": 1, "band_count": 2, "salary_count": 3, "description_count": 3},
            "drift_flags": [{"code": "moderate_drop", "severity": "moderate"}],
        },
        {
            "source_id": "demo-pageup-2",
            "platform": "pageup",
            "status": "degraded",
            "chosen_strategy": "platform:pageup",
            "fallback_used": True,
            "quarantined_rows": 2,
            "quality": {"accepted_count": 0, "candidate_count": 10, "rejected_count": 10, "band_count": 0, "salary_count": 0, "description_count": 0},
            "drift_flags": [{"code": "source_count_spike", "severity": "severe"}],
        },
    ]

    write_source_health(health_path, rows, run_id="run-20260530", generated_at="2026-05-30T00:00:00Z")
    payload = json.loads(health_path.read_text(encoding="utf-8"))

    assert payload["platforms"]["pageup"]["sources"] == 2
    assert payload["platforms"]["pageup"]["status"] == "degraded"
    assert payload["platforms"]["pageup"]["accepted_count"] == 3
    assert payload["platforms"]["pageup"]["quarantined_rows"] == 2
    assert payload["platforms"]["pageup"]["drift_flags"]["source_count_spike"] == 1
