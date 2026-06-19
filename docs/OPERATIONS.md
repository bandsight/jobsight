# Operations Guide

## Daily Operation

The `Fetch Jobs` workflow runs daily at `18:00 UTC`. It:

1. checks out the repository,
2. installs Python dependencies,
3. unpacks seeded source data when present,
4. fetches configured public sources,
5. rebuilds public data and RSS,
6. commits generated outputs back to `main`,
7. deploys the static site to GitHub Pages.

The workflow can also be started manually from the GitHub Actions tab.

## Health Terms

- `current` or `ok`: the source produced usable rows and passed quality checks.
- `under review`: the source produced data, but the run tripped a quality or drift flag.
- `unavailable`: the source did not produce usable accepted rows for the run.
- `using fallback`: the public board carried forward the last accepted rows for that source.
- `rejected rows`: candidate rows excluded by quality gates and written to quarantine.

The public board can still be healthy when one source is under review. Fallback
prevents a suspect fetch from replacing better previously accepted rows.

## Local Commands

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run a fetch:

```bash
PYTHONPATH=src python -m jobsight.cli --root . run --timeout 8 --workers 8
```

Build the static site:

```bash
PYTHONPATH=src python -m jobsight.cli --root . public
```

Run tests:

```bash
python -m pytest
```

## Adding Or Editing Sources

Sources live in `data/sources.json` after unpacking. Each source should include:

- `source_id`
- council name/key fields
- public job-board URL
- platform family when known

After editing sources, run tests and a limited fetch before allowing the daily
workflow to publish the result.

## Interpreting Quarantine

Quarantine files are stored under `data/quarantine/YYYY-MM-DD.jsonl`. They are
audit artifacts, not public board rows. Common rejection reasons include:

- navigation, marketing, or language-selector links,
- missing salary and band evidence,
- generic careers pages rather than job listings,
- duplicate listings where a richer row already exists,
- source count spikes that look like parser drift.

## Common Fixes

- If Pages deployment fails, confirm Pages is set to GitHub Actions.
- If generated data is not committed, confirm workflow permissions allow write access.
- If a source is repeatedly under review, inspect `data/source-health.json` and the matching quarantine file.
- If a platform changes markup, add or update deterministic parsing rules and regression tests.
- If local TLS validation fails on Windows, use the project's Python dependencies rather than replacing request logic.

## Release Readiness Check

Before handing the repository to a new maintainer:

```bash
python -m compileall -q src tests
python -m pytest -q
PYTHONPATH=src python -m jobsight.cli --root . public
```

Then check the hosted site and confirm:

- the map renders,
- the job list scrolls,
- date filters update counts,
- salary and band filters work,
- `data/source-health.json` exists,
- no public board row is missing both salary and band.
