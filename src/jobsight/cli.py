from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from jobsight.import_seed import import_seed
from jobsight.outputs import append_jsonl, build_public_dir, rebuild_outputs, write_descriptions
from jobsight.registry import load_sources
from jobsight.text import now_utc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="jobsight")
    parser.add_argument("--root", type=Path, default=Path("."))
    subcommands = parser.add_subparsers(dest="command", required=True)

    run_parser = subcommands.add_parser("run", help="Fetch sources, append observations, and rebuild outputs.")
    run_parser.add_argument("--sources", type=Path, default=Path("data/sources.json"))
    run_parser.add_argument("--data", type=Path, default=Path("data"))
    run_parser.add_argument("--timeout", type=int, default=20)
    run_parser.add_argument("--limit", type=int, default=0, help="Limit sources for smoke tests.")
    run_parser.add_argument("--workers", type=int, default=1, help="Fetch sources concurrently.")

    build_parser = subcommands.add_parser("build", help="Rebuild outputs from existing observations.")
    build_parser.add_argument("--sources", type=Path, default=Path("data/sources.json"))
    build_parser.add_argument("--data", type=Path, default=Path("data"))

    public_parser = subcommands.add_parser("public", help="Build the GitHub Pages publish directory.")
    public_parser.add_argument("--data", type=Path, default=Path("data"))
    public_parser.add_argument("--site", type=Path, default=Path("site"))
    public_parser.add_argument("--out", type=Path, default=Path("public"))

    seed_parser = subcommands.add_parser("import-seed", help="Import a historical public snapshot into observations.")
    seed_parser.add_argument("input", type=Path)
    seed_parser.add_argument("--sources", type=Path, default=Path("data/sources.json"))
    seed_parser.add_argument("--data", type=Path, default=Path("data"))
    seed_parser.add_argument("--observed-at", default=None)
    seed_parser.add_argument("--include-descriptions", action="store_true")

    args = parser.parse_args(argv)
    root = args.root.resolve()
    if args.command == "run":
        return run(root, args.sources, args.data, args.timeout, args.limit, args.workers)
    if args.command == "build":
        sources = load_sources(root / args.sources)
        summary = rebuild_outputs(root / args.data, sources)
        print(summary)
        return 0
    if args.command == "public":
        build_public_dir(root, root / args.data, root / args.site, root / args.out)
        return 0
    if args.command == "import-seed":
        sources = load_sources(root / args.sources)
        summary = import_seed(
            args.input,
            root / args.data,
            sources,
            observed_at=args.observed_at,
            include_descriptions=args.include_descriptions,
        )
        print(summary)
        return 0
    raise AssertionError(args.command)


def run(root: Path, sources_path: Path, data_path: Path, timeout: int, limit: int, workers: int) -> int:
    from jobsight.http import HttpClient
    from jobsight.intelligence import (
        append_quarantine,
        load_source_profiles,
        parse_source_smart,
        promote_source_reject_rules_from_quarantine,
        save_source_profiles,
        update_global_rules_from_quarantine,
        write_source_health,
    )
    from jobsight.models import make_observation

    sources = load_sources(root / sources_path)
    if limit:
        sources = sources[:limit]
    data_root = root / data_path
    observed_at = now_utc()
    run_id = observed_at.replace(":", "").replace("-", "").replace("Z", "Z")
    observations = []
    descriptions = []
    source_results = []
    health_rows = []
    quarantine_rows = []
    failed_source_ids = set()
    results = []
    profiles_path = data_root / "source-profiles.json"
    profiles = load_source_profiles(profiles_path)

    def fetch_one(index: int, source: dict):
        client = HttpClient(timeout=timeout)
        smart = parse_source_smart(source, client, profiles, observed_at=observed_at, run_id=run_id)
        return index, source, smart

    worker_count = max(1, min(workers, len(sources) or 1))
    if worker_count == 1:
        for index, source in enumerate(sources):
            results.append(fetch_one(index, source))
            print(f"{source.get('short_name') or source.get('council_name')}: {len(results[-1][2].jobs)} jobs", flush=True)
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(fetch_one, index, source) for index, source in enumerate(sources)]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                _, source, smart = result
                print(f"{source.get('short_name') or source.get('council_name')}: {len(smart.jobs)} jobs", flush=True)

    for _, source, smart in sorted(results, key=lambda item: item[0]):
        raw_jobs = smart.jobs
        raw_descriptions = smart.descriptions
        profiles.setdefault("sources", {})[source.get("source_id")] = smart.profile
        health_rows.append(smart.health)
        quarantine_rows.extend(smart.quarantine)
        source_failed = any(item.get("status") == "failed" for item in raw_descriptions if isinstance(item, dict))
        if source_failed or smart.health.get("fallback_used") or smart.health.get("status") == "failed":
            failed_source_ids.add(source.get("source_id"))
        for raw in raw_jobs:
            if raw.get("title") and raw.get("url"):
                observations.append(make_observation(run_id=run_id, observed_at=observed_at, source=source, raw=raw))
        descriptions.extend(raw_descriptions)
        source_results.append({
            "source_id": source.get("source_id"),
            "council_name": source.get("council_name"),
            "platform": source.get("platform"),
            "jobs": len(raw_jobs),
            "status": smart.health.get("status") or ("failed" if source_failed else "ok"),
            "chosen_strategy": smart.health.get("chosen_strategy"),
            "fallback_used": smart.health.get("fallback_used"),
        })
    for source_id, profile in list(profiles.get("sources", {}).items()):
        profiles["sources"][source_id] = promote_source_reject_rules_from_quarantine(profile, quarantine_rows)
    update_global_rules_from_quarantine(profiles, quarantine_rows)

    observation_path = data_root / "observations" / f"{observed_at[:10]}.jsonl"
    append_jsonl(observation_path, observations)
    append_quarantine(data_root / "quarantine" / f"{observed_at[:10]}.jsonl", quarantine_rows)
    save_source_profiles(profiles_path, profiles, updated_at=observed_at)
    write_source_health(data_root / "source-health.json", health_rows, run_id=run_id, generated_at=observed_at)
    description_count = write_descriptions(data_root, descriptions, observed_at)
    summary = rebuild_outputs(data_root, sources, run_id=run_id, failed_source_ids=failed_source_ids)
    summary["latest_run_source_results"] = source_results
    summary["latest_run_descriptions_written"] = description_count
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
