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

"""Select CI suites from the event and changed PR paths."""

import fnmatch
import json
import os
from pathlib import Path
import subprocess

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
        # The queue already tested the merge result. Publish docs and populate
        # main's caches through the reusable workflows' cache-refresh-only mode.
        return ["docs", "integration", "parquet"]
    if event != "pull_request":
        raise ValueError(f"Unsupported CI event: {event}")

    def matches(patterns):
        return any(fnmatch.fnmatchcase(path, pattern) for path in paths for pattern in patterns)

    if matches(filters["common"]):
        return sorted(suites)
    return sorted(ALWAYS | {name for name, patterns in filters["suites"].items() if matches(patterns)})


def main():
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
        output.write(f"cache-refresh-only={json.dumps(event == 'push')}\n")
    print(f"Selected suites: {', '.join(suites)}")


if __name__ == "__main__":
    main()
