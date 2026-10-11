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

"""Check CI configuration and routing policy, following Comet's structure.

Run from the repository root:
    python3 .github/ci/scripts/check-ci-config.py
"""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest

spec = importlib.util.spec_from_file_location(
    "compute_changes", Path(__file__).with_name("compute-changes.py")
)
changes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(changes)

ROOT = changes.ROOT
ALWAYS = changes.ALWAYS
load_yaml = changes.load_yaml

# Only these jobs and shell steps should run when warming main's caches.
CACHE_REFRESH_STEPS = {
    "integration": {
        "pyarrow-integration-test": {
            "Setup Rust toolchain", "Upgrade pip and setuptools",
            "Create virtualenv and install dependencies", "Build Python extension",
        },
    },
    "parquet": {"pyspark-integration-test": {"Install Python dependencies"}},
}
FULL_RUN = "${{ !inputs.cache-refresh-only }}"


def validate_cache_refresh(ci, suite, workflow):
    expected = "${{ needs.changes.outputs.cache-refresh-only == 'true' }}"
    assert ci["jobs"][suite].get("with", {}).get("cache-refresh-only") == expected, f"{suite}: missing cache mode input"
    cache_jobs = CACHE_REFRESH_STEPS[suite]
    assert cache_jobs.keys() <= workflow["jobs"].keys(), f"{suite}: missing cache writer"
    for name, job in workflow["jobs"].items():
        if name not in cache_jobs:
            assert job.get("if") == FULL_RUN, f"{suite}/{name}: runs during cache refresh"
            continue
        assert "if" not in job, f"{suite}/{name}: cache writer must run in both modes"
        run_steps = {step["name"]: step for step in job["steps"] if "run" in step}
        assert cache_jobs[name] <= run_steps.keys(), f"{suite}/{name}: missing cache setup step"
        for step in job["steps"]:
            if "run" not in step or step["name"] in cache_jobs[name]:
                assert "if" not in step, f"{suite}/{name}: cache setup must run in both modes"
            else:
                assert step.get("if") == FULL_RUN, f"{suite}/{name}/{step['name']}: runs during cache refresh"


def validate_package_paths(root, suite, workflow, filters):
    """Check literal Cargo package arguments, without interpreting shell scripts."""
    packages = {}
    for manifest in root.glob("*/Cargo.toml"):
        package = tomllib.loads(manifest.read_text()).get("package")
        if package:
            packages[package["name"]] = manifest.parent.relative_to(root).as_posix()
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            for command in re.findall(r"\bcargo\b[^\n]*", step.get("run", "")):
                for name in re.findall(r"(?:^|\s)(?:-p|--package)(?:\s+|=)[\"']?([\w-]+)", command):
                    assert name in packages, f"{suite}: unknown local package {name}"
                    path = f"{packages[name]}/src/lib.rs"
                    selected = changes.select_suites("pull_request", [path], filters)
                    assert suite in selected, f"{suite}: changes to {name} do not select its tests"


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
    assert jobs["changes"]["outputs"].get("cache-refresh-only") == "${{ steps.select.outputs.cache-refresh-only }}", "Missing cache mode output"
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
        call = None
        if suite in CACHE_REFRESH_STEPS:
            call = {"inputs": {"cache-refresh-only": {
                "description": "Populate caches without running tests or linters",
                "type": "boolean", "default": False,
            }}}
            validate_cache_refresh(ci, suite, workflow)
        assert workflow[True] == {"workflow_call": call}, f"{suite}: duplicate or missing triggers/inputs"
        assert "concurrency" not in workflow, f"{suite}: concurrency is owned by ci.yml"
        if suite in filters["suites"]:
            validate_package_paths(root, suite, workflow, filters)
        called.add(path.resolve())
    reusable = {
        path.resolve()
        for path in workflows.iterdir()
        if path.suffix in {".yml", ".yaml"}
        and "workflow_call" in load_yaml(path).get(True, {})
    }
    assert called == reusable, "A reusable workflow is missing from Required Checks"
    miri = load_yaml(workflows / "miri.yaml")["jobs"]["miri-checks"]
    assert "if" not in miri, "Miri event routing belongs in compute-changes.py"
    print("CI configuration is valid")


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.filters = changes.load_yaml(changes.ROOT / ".github/ci/paths.yaml")
        self.always = {"dev", "rust", "docs"}
        self.all_suites = self.always | {
            "arrow", "arrow-flight", "parquet", "integration", "audit",
        }

    def select(self, event, paths):
        return set(changes.select_suites(event, paths, self.filters))

    def test_event_policy(self):
        for paths in [[], ["README.md"], ["arrow-buffer/src/lib.rs"]]:
            with self.subTest(paths=paths):
                self.assertEqual(self.select("merge_group", paths), self.all_suites | {"miri"})
                self.assertEqual(self.select("push", paths), {"docs", "integration", "parquet"})
                self.assertNotIn("miri", self.select("pull_request", paths))
        with self.assertRaises(ValueError):
            self.select("pull_request_target", [])

    def test_selector_outputs(self):
        for event, expected in [("push", {"docs", "integration", "parquet"}),
                                ("merge_group", self.all_suites | {"miri"}),
                                ("pull_request", self.always)]:
            with self.subTest(event=event), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                env = dict(os.environ, GITHUB_EVENT_NAME=event, GITHUB_OUTPUT=str(output),
                           PR_BASE_SHA="HEAD", PR_HEAD_SHA="HEAD")
                subprocess.run([sys.executable, str(ROOT / ".github/ci/scripts/compute-changes.py")],
                               env=env, check=True, capture_output=True)
                values = dict(line.split("=", 1) for line in output.read_text().splitlines())
                self.assertEqual(set(json.loads(values["suites"])), expected)
                self.assertEqual(json.loads(values["cache-refresh-only"]), event == "push")

    def test_pr_path_routing(self):
        cases = [
            ([], set()),
            (["README.md"], set()),
            (["arrow-buffer/src/lib.rs"], {"arrow", "arrow-flight", "parquet", "integration"}),
            (["arrow-avro/src/lib.rs"], {"arrow"}),
            (["arrow-flight/src/lib.rs"], {"arrow-flight", "integration"}),
            (["arrow-pyarrow/src/lib.rs"], {"integration"}),
            (["arrow-integration-testing/src/lib.rs"], {"arrow", "integration"}),
            (["arrow-pyarrow-testing/src/lib.rs"], {"integration"}),
            (["parquet/src/lib.rs"], {"parquet"}),
            (["parquet_derive/src/lib.rs"], {"parquet"}),
            (["parquet_derive_test/src/lib.rs"], {"parquet"}),
            (["parquet-variant-json/src/lib.rs"], {"parquet"}),
            (["parquet-geospatial/src/lib.rs"], {"parquet"}),
            (["arrow-row/Cargo.toml"], {"arrow", "parquet", "integration", "audit"}),
            (["some/nested/Cargo.lock"], {"audit"}),
            (["parquet-variant/src/lib.rs"], {"parquet"}),
            (["parquet-variant-compute/src/lib.rs"], {"parquet"}),
            (["arrow-arith/src/lib.rs"], {"arrow", "parquet", "integration"}),
            (["arrow-ord/src/lib.rs"], {"arrow", "parquet", "integration"}),
            (["arrow-string/src/lib.rs"], {"arrow", "parquet", "integration"}),
            # A move or deletion must still select the affected suites.
            (["arrow-cmp/old.rs", "parquet-geospatial/new.rs"], {"arrow", "parquet", "integration"}),
            (["arrow-cmp/file\nwith-newline.rs"], {"arrow", "parquet", "integration"}),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                self.assertEqual(self.select("pull_request", paths), self.always | expected)

    def test_shared_build_inputs(self):
        for path in [
            ".asf.yaml", ".github/workflows/ci.yml", ".github/ci/paths.yaml",
            ".github/actions/setup-builder/action.yaml", ".gitmodules",
            "Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
            "testing", "parquet-testing", "format/Flight.proto", "format/FlightSql.proto",
            ".config/nextest.toml",
        ]:
            with self.subTest(path=path):
                self.assertEqual(self.select("pull_request", [path]), self.all_suites)


class ConfigurationTests(unittest.TestCase):
    def test_current_configuration(self):
        with contextlib.redirect_stdout(io.StringIO()):
            validate_config()

    def test_configuration_regressions(self):
        cases = [
            (".asf.yaml", '"Required Checks"', '"Missing Check"'),
            (".github/workflows/ci.yml", "      - miri\n", ""),
            (".github/workflows/ci.yml", "  pull_request:\n", "  pull_request:\n    paths: ['arrow/**']\n"),
            (".github/workflows/ci.yml", "    if: always()", "    if: success()"),
            (".github/workflows/miri.yaml", "    name: MIRI\n", "    name: MIRI\n    if: github.event_name == 'merge_group'\n"),
            (".github/workflows/arrow.yml", "  workflow_call:", "  workflow_call:\n  pull_request:"),
            (".github/ci/paths.yaml", "    - arrow-integration-testing/**\n", ""),
            (".github/workflows/ci.yml", "      cache-refresh-only: ${{ steps.select.outputs.cache-refresh-only }}\n", ""),
            (".github/workflows/ci.yml", "    with:\n      cache-refresh-only: ${{ needs.changes.outputs.cache-refresh-only == 'true' }}\n", ""),
            (".github/workflows/parquet.yml", "\n    if: ${{ !inputs.cache-refresh-only }}\n", "\n"),
            (".github/workflows/integration.yml", "\n    if: ${{ !inputs.cache-refresh-only }}\n", "\n"),
            (".github/workflows/parquet.yml", "        if: ${{ !inputs.cache-refresh-only }}\n", ""),
            (".github/workflows/integration.yml", "        if: ${{ !inputs.cache-refresh-only }}\n", ""),
            (".github/workflows/integration.yml", "      - name: Build Python extension\n", "      - name: Build Python extension\n        if: ${{ !inputs.cache-refresh-only }}\n"),
        ]
        for filename, before, after in cases:
            with self.subTest(filename=filename, before=before), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copytree(changes.ROOT / ".github", root / ".github")
                shutil.copy(changes.ROOT / ".asf.yaml", root / ".asf.yaml")
                for manifest in changes.ROOT.glob("*/Cargo.toml"):
                    target = root / manifest.relative_to(changes.ROOT)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(manifest, target)
                path = root / filename
                original = path.read_text()
                self.assertIn(before, original)
                path.write_text(original.replace(before, after))
                with self.assertRaises(AssertionError):
                    validate_config(root)


if __name__ == "__main__":
    unittest.main()
