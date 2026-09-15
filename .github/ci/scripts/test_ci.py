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

import contextlib
import io
from pathlib import Path
import shutil
import tempfile
import unittest

import ci


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.filters = ci.load_yaml(ci.ROOT / ".github/ci/paths.yaml")
        self.always = {"dev", "rust", "docs"}
        self.all_suites = self.always | {
            "arrow", "arrow-flight", "parquet", "parquet-derive",
            "parquet-variant", "parquet-geospatial", "integration", "audit",
        }

    def select(self, event, paths):
        return set(ci.select_suites(event, paths, self.filters))

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
            ci.validate_config()

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
                shutil.copytree(ci.ROOT / ".github", root / ".github")
                shutil.copy(ci.ROOT / ".asf.yaml", root / ".asf.yaml")
                path = root / filename
                original = path.read_text()
                self.assertIn(before, original)
                path.write_text(original.replace(before, after))
                with self.assertRaises(AssertionError):
                    ci.validate_config(root)


if __name__ == "__main__":
    unittest.main()
