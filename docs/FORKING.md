# Forking And Handover

This document is for a new maintainer who wants to copy, fork, or operate
JobSight without access to any private workspace.

## Copy Checklist

1. Fork or copy the repository.
2. In GitHub, enable Actions for the repository.
3. In repository settings, set **Actions > General > Workflow permissions** to allow read and write permissions.
4. In repository settings, set **Pages > Build and deployment > Source** to **GitHub Actions**.
5. Run the **Fetch Jobs** workflow manually once from the Actions tab.
6. Confirm the Pages deployment completes and opens at the repository's Pages URL.
7. Confirm `data/job-board-data.json`, `data/source-health.json`, and `jobs.xml` are updated by the workflow.

No secrets are required for the default public-source configuration.

## Repository Map

- `.github/workflows/fetch.yml` - daily fetch/build/deploy workflow.
- `src/jobsight/` - deterministic fetch, extraction, quality, and output code.
- `src/jobsight/platforms/` - platform-specific adapters.
- `src/jobsight/extractors/` - salary, band, title, and description extraction helpers.
- `data/seed/` - packed source registry and seeded observations for a cold start.
- `data/sources.json` - unpacked source registry used by local runs.
- `data/observations/` - append-only accepted observations.
- `data/quarantine/` - rejected candidate rows for audit.
- `index.html`, `app.js`, `styles.css` - public static board.
- `tests/` - regression and workflow tests.

## Cold Start Behaviour

A copied repository can run from the packed seed files in `data/seed/`.
The workflow unpacks the source registry and seeded observation archive before
the first fetch. After the first successful run, generated JSON files are
committed back to the repository.

## Public URLs After Forking

Replace `OWNER` and `REPO` with the copied repository location:

```text
https://OWNER.github.io/REPO/
https://raw.githubusercontent.com/OWNER/REPO/main/data/job-board-data.json
https://raw.githubusercontent.com/OWNER/REPO/main/data/source-health.json
https://raw.githubusercontent.com/OWNER/REPO/main/jobs.xml
```

## Maintenance Boundaries

The system can promote declarative source hints and use deterministic fallback
logic. It should not generate or mutate executable scraper code automatically.

Quality gates are intentionally conservative. Rejected rows are retained in
quarantine so a maintainer can inspect what was excluded without publishing
low-confidence records.
