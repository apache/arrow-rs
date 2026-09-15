# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Select CI suites and validate the required merge-queue check.

Use a local path filter, as in datafusion-comet: dorny/paths-filter is not
on the ASF Actions allowlist. Paths use fnmatch globs (including directory/**).
"""

import fnmatch
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
ALWAYS = {"dev", "rust", "docs"}


def load_yaml(path):
    import yaml

    return yaml.safe_load(path.read_text())


def select_suites(event, paths, filters):
    suites = set(filters["suites"]) | ALWAYS
    if event == "merge_group":
        return sorted(suites | {"miri"})
    if event == "push":
        return sorted(suites)
    if event != "pull_request":
        raise ValueError(f"Unsupported CI event: {event}")

    def matches(patterns):
        return any(fnmatch.fnmatchcase(path, pattern) for path in paths for pattern in patterns)

    if matches(filters["common"]):
        return sorted(suites)
    return sorted(ALWAYS | {name for name, patterns in filters["suites"].items() if matches(patterns)})


def validate_config(root=ROOT):
    """Catch stale check names, omitted suites, and workflow-level path filters."""
    workflows = root / ".github/workflows"
    ci = load_yaml(workflows / "ci.yml")
    filters = load_yaml(root / ".github/ci/paths.yaml")
    jobs = ci["jobs"]
    suites = set(filters["suites"]) | ALWAYS | {"miri"}
    assert not (set(filters["suites"]) & (ALWAYS | {"miri"})), "Unexpected filtered suite"
    assert set(jobs) == suites | {"changes", "required-checks"}, "CI suites and jobs differ"
    # PyYAML's YAML 1.1 loader treats the key 'on' as the boolean True.
    events = ci[True]
    assert set(events) == {"pull_request", "push", "merge_group"}, "Unexpected CI triggers"
    assert events["pull_request"] is None, "Required CI must run on every PR"
    assert events["merge_group"] == {"types": ["checks_requested"]}, "Missing queue trigger"
    assert events["push"] == {"branches": ["main"]}, "Only main pushes should run CI"

    gate = jobs["required-checks"]
    assert set(gate["needs"]) == set(jobs) - {"required-checks"}, "Required Checks omits a job"
    assert gate["if"] == "always()", "Required Checks must run after failures and cancellations"
    asf = load_yaml(root / ".asf.yaml")["github"]
    checks = asf["protected_branches"]["main"]["required_status_checks"]
    assert checks["contexts"] == [gate["name"]], "Required check name does not match .asf.yaml"
    assert checks["strict"] is False, "The queue handles up-to-date checks"

    called = set()
    for suite in suites:
        job = jobs[suite]
        assert job["needs"] == "changes", f"{suite}: missing suite selection dependency"
        condition = f"contains(fromJSON(needs.changes.outputs.suites), '{suite}')"
        assert job["if"] == condition, f"{suite}: unexpected routing condition"
        path = root / job["uses"]
        workflow = load_yaml(path)
        assert workflow[True] == {"workflow_call": None}, f"{suite}: duplicate or missing triggers"
        assert "concurrency" not in workflow, f"{suite}: concurrency is owned by ci.yml"
        called.add(path.resolve())
    reusable = {
        path.resolve()
        for path in workflows.iterdir()
        if path.suffix in {".yml", ".yaml"}
        and "workflow_call" in load_yaml(path).get(True, {})
    }
    assert called == reusable, "A reusable workflow is missing from Required Checks"
    miri = load_yaml(workflows / "miri.yaml")["jobs"]["miri-checks"]
    assert miri["if"] == "github.event_name == 'merge_group'", "Miri must only run in the queue"
    print("CI configuration is valid")


def main():
    command = sys.argv[1]
    if command == "validate":
        validate_config()
    elif command == "select":
        event = os.environ["GITHUB_EVENT_NAME"]
        paths = []
        if event == "pull_request":
            base = os.environ["PR_BASE_SHA"]
            head = os.environ["PR_HEAD_SHA"]
            # Full checkout history includes both PR parents. Disable rename
            # detection so moving a file selects both its old and new suites.
            changed = subprocess.check_output(
                ["git", "diff", "--no-renames", "--name-only", "-z", f"{base}...{head}", "--"],
                cwd=ROOT,
            )
            paths = changed.decode("utf-8", errors="surrogateescape").split("\0")[:-1]
        suites = select_suites(event, paths, load_yaml(ROOT / ".github/ci/paths.yaml"))
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"suites={json.dumps(suites)}\n")
        print(f"Selected suites: {', '.join(suites)}")
    else:
        raise ValueError(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
