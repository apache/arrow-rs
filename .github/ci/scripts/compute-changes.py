"""Select CI suites from the event and changed PR paths."""

import fnmatch
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
ALWAYS = {"dev", "rust", "docs"}
UPSTREAM = "apache/arrow-rs"


def is_fork_pr(event, repository, head_repository, base_ref):
    return (event == "pull_request" and repository == UPSTREAM
            and bool(head_repository) and head_repository != repository
            and base_ref == "main")


def load_yaml(path):
    import yaml

    return yaml.safe_load(path.read_text())


def select_suites(event, paths, filters, *, repository=UPSTREAM,
                 head_repository=UPSTREAM, base_ref="main"):
    suites = set(filters["suites"]) | ALWAYS
    if event == "merge_group":
        return sorted(suites)
    if event == "workflow_dispatch" or (event == "push" and repository != UPSTREAM):
        # Fork pushes test the branch head, including pushes to the fork's main.
        # Unlike upstream main, these commits have not passed the merge queue.
        return sorted(suites)
    if event == "push":
        # The queue already tested the merge result. Publish docs and populate
        # main's caches through the reusable workflows' cache-refresh-only mode.
        return ["docs", "integration", "parquet"]
    if event != "pull_request":
        raise ValueError(f"Unsupported CI event: {event}")
    if is_fork_pr(event, repository, head_repository, base_ref):
        # Keep quick checks upstream; the queue still selects all regular suites.
        return ["dev", "rust"]

    def matches(patterns):
        return any(fnmatch.fnmatchcase(path, pattern) for path in paths for pattern in patterns)

    if matches(filters["common"]):
        return sorted(suites)
    return sorted(ALWAYS | {name for name, patterns in filters["suites"].items() if matches(patterns)})


def main():
    event = os.environ["GITHUB_EVENT_NAME"]
    repository = os.environ["GITHUB_REPOSITORY"]
    head_repository = os.environ.get("PR_HEAD_REPOSITORY", "")
    base_ref = os.environ.get("PR_BASE_REF", "")
    fork_pr = is_fork_pr(event, repository, head_repository, base_ref)
    paths = []
    if event == "pull_request" and not fork_pr:
        base = os.environ["PR_BASE_SHA"]
        head = os.environ["PR_HEAD_SHA"]
        # Full checkout history includes both PR parents. Disable rename
        # detection so moving a file selects both its old and new suites.
        changed = subprocess.check_output(
            ["git", "diff", "--no-renames", "--name-only", "-z", f"{base}...{head}", "--"],
            cwd=ROOT,
        )
        paths = changed.decode("utf-8", errors="surrogateescape").split("\0")[:-1]
    suites = select_suites(event, paths, load_yaml(ROOT / ".github/ci/paths.yaml"),
                          repository=repository, head_repository=head_repository,
                          base_ref=base_ref)
    cache_refresh = event == "push" and repository == UPSTREAM
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"suites={json.dumps(suites)}\n")
        output.write(f"cache-refresh-only={json.dumps(cache_refresh)}\n")
        output.write(f"fork-pr={json.dumps(fork_pr)}\n")
    print(f"Selected suites: {', '.join(suites)}")


if __name__ == "__main__":
    main()
