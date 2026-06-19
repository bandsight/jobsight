# JobSight

JobSight is a deterministic Victorian council jobs dataset and static browser.
It fetches public council job sources, applies quality gates, appends accepted
observations, and publishes a GitHub Pages site plus machine-readable JSON/RSS.

The project is designed to be forked or copied. It does not require private
services, API keys, pay-table joins, or generated scraper code.

## What It Publishes

- `data/job-board-data.json` - compact dataset used by the public map and job board.
- `data/current-jobs.json` - latest known row for jobs seen in the latest accepted run, with source fallback handling.
- `data/all-jobs.json` - latest accepted row for every observed job in the retained history.
- `data/run-summary.json` - run-level counts and coverage metrics.
- `data/source-health.json` - source quality, drift, fallback, and quarantine summary.
- `data/quarantine/YYYY-MM-DD.jsonl` - rejected candidate rows retained for audit.
- `data/observations/YYYY-MM-DD.jsonl` - append-only accepted observations.
- `jobs.xml` - RSS feed generated from accepted jobs.

Example raw ingestion URL after forking:

```text
https://raw.githubusercontent.com/OWNER/REPO/main/data/job-board-data.json
```

## How It Works

1. Load the configured public source registry from `data/sources.json` or the packed seed in `data/seed/`.
2. Fetch public job boards using deterministic platform adapters and fallback HTML parsing.
3. Extract title, URL, salary, band, closing date, description excerpts, and field evidence.
4. Reject low-quality rows and write rejected candidates to quarantine.
5. Append accepted observations and rebuild public JSON/RSS.
6. Deploy the static site through GitHub Pages.

## Local Use

```bash
python -m pip install -r requirements.txt
PYTHONPATH=src python -m jobsight.cli --root . run --timeout 8 --workers 8
PYTHONPATH=src python -m jobsight.cli --root . public
python -m pytest
```

Open the generated site through a local web server rather than opening
`index.html` directly, because the app loads JSON assets.

## GitHub Automation

The workflow in `.github/workflows/fetch.yml` runs:

- on push to `main`
- daily at `18:00 UTC`
- manually through `workflow_dispatch`

It installs dependencies, fetches sources, rebuilds data, commits generated
outputs back to the repository, and deploys `public/` with GitHub Pages.

For a copied repository, enable:

- GitHub Actions
- GitHub Pages set to **GitHub Actions**
- workflow permissions that allow `contents: write`

See [docs/FORKING.md](docs/FORKING.md) and [docs/OPERATIONS.md](docs/OPERATIONS.md)
for handover and operations notes.

## Quality Policy

Public output favours quality over volume. A row must have a credible job title,
a valid external URL, and at least salary or band evidence to appear on the board.
Sources that spike, drop, or produce too much junk are marked for review and can
fall back to their last accepted rows.
