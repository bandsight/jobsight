from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "fetch.yml"


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_fetch_workflow_uses_checked_out_source_module():
    text = workflow_text()

    assert "Verify deterministic source update" in text
    assert "parse_source_smart" in text
    assert "PYTHONPATH=src python -m jobsight.cli --root . run" in text
    assert "PYTHONPATH=src python -m jobsight.cli --root . public" in text
    assert "run: jobsight run" not in text
    assert "run: jobsight public" not in text


def test_fetch_workflow_runs_daily_without_temporary_patch_hooks():
    text = workflow_text()

    assert 'cron: "0 18 * * *"' in text
    assert "source-patch.py" not in text
    assert "source-update.tar.bz2.b64" not in text


def test_generated_data_push_does_not_stage_workflow_files():
    text = workflow_text()
    git_add_lines = [line.strip() for line in text.splitlines() if line.strip().startswith("git add -A ")]

    assert len(git_add_lines) == 1
    git_add_line = git_add_lines[0]
    assert ".github" not in git_add_line
    assert "src/jobsight/intelligence.py" in git_add_line
    assert "data" in git_add_line
