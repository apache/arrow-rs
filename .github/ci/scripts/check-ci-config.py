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
from pathlib import Path
import shutil
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "compute_changes", Path(__file__).with_name("compute-changes.py")
)
changes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(changes)

ROOT = changes.ROOT
ALWAYS = changes.ALWAYS
load_yaml = changes.load_yaml


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


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.filters = changes.load_yaml(changes.ROOT / ".github/ci/paths.yaml")
        self.always = {"dev", "rust", "docs"}
        self.all_suites = self.always | {
            "arrow", "arrow-flight", "parquet", "parquet-derive",
            "parquet-variant", "parquet-geospatial", "integration", "audit",
        }

    def select(self, event, paths):
        return set(changes.select_suites(event, paths, self.filters))

    def test_event_policy(self):
        for paths in [[], ["README.md"], ["arrow-buffer/src/lib.rs"]]:
            with self.subTest(paths=paths):
                self.assertEqual(self.select("merge_group", paths), self.all_suites | {"miri"})
                self.assertEqual(self.select("push", paths), self.all_suites)
                self.assertNotIn("miri", self.select("pull_request", paths))
        with self.assertRaises(ValueError):
            self.select("pull_request_target", [])

    def test_pr_path_routing(self):
        cases = [
            ([], set()),
            (["README.md"], set()),
            (["arrow-buffer/src/lib.rs"], {"arrow", "arrow-flight", "parquet", "integration"}),
            (["arrow-avro/src/lib.rs"], {"arrow", "parquet"}),
            (["arrow-flight/src/lib.rs"], {"arrow-flight", "integration"}),
            (["arrow-pyarrow/src/lib.rs"], {"integration"}),
            (["parquet/src/lib.rs"], {"parquet", "parquet-derive"}),
            (["parquet_derive/src/lib.rs"], {"parquet-derive"}),
            (["parquet-variant-json/src/lib.rs"], {"parquet", "parquet-variant"}),
            (["parquet-geospatial/src/lib.rs"], {"parquet-geospatial"}),
            (["arrow-row/Cargo.toml"], {"arrow", "integration", "audit"}),
            (["some/nested/Cargo.lock"], {"audit"}),
            # A move or deletion must still select the affected suites.
            (["arrow-cmp/old.rs", "parquet-geospatial/new.rs"], {"arrow", "integration", "parquet-geospatial"}),
            (["arrow-cmp/file\nwith-newline.rs"], {"arrow", "integration"}),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                self.assertEqual(self.select("pull_request", paths), self.always | expected)

    def test_shared_build_inputs(self):
        for path in [
            ".asf.yaml", ".github/workflows/ci.yml", ".github/ci/paths.yaml",
            ".github/actions/setup-builder/action.yaml", ".gitmodules",
            "Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
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
            (".github/workflows/miri.yaml", "'merge_group'", "'pull_request'"),
            (".github/workflows/arrow.yml", "  workflow_call:", "  workflow_call:\n  pull_request:"),
        ]
        for filename, before, after in cases:
            with self.subTest(filename=filename, before=before), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copytree(changes.ROOT / ".github", root / ".github")
                shutil.copy(changes.ROOT / ".asf.yaml", root / ".asf.yaml")
                path = root / filename
                original = path.read_text()
                self.assertIn(before, original)
                path.write_text(original.replace(before, after))
                with self.assertRaises(AssertionError):
                    validate_config(root)


if __name__ == "__main__":
    unittest.main()
