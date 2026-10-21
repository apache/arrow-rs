<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

CI runs lightweight checks upstream for fork pull requests to `main`. Fork
branch pushes and manual CI runs execute all regular suites, excluding Miri.
Other pull requests run suites selected by their changed files upstream.
The merge queue runs the full test suite against the proposed merge result.
Pushes to upstream `main` populate caches and publish documentation without repeating tests.
A single required status check combines the results and blocks merging on failure.

`fork_ci_status.yml` mirrors fork results in an informational `CI (fork)` check,
using `workflow_run` and scheduled polling. It reads fork runs and writes upstream
checks through GitHub APIs without checking out PR code or consuming PR artifacts.
See [fork CI setup](../../CONTRIBUTING.md#pull-requests-from-forks).

Run the routing and bridge tests with Python 3.11+, PyYAML, and Node.js 24:

```shell
python3 .github/ci/scripts/check-ci-config.py
node --test .github/ci/scripts/test-fork-ci-status.cjs
```

Bridge tests mock GitHub APIs. End-to-end testing requires a test repository
with the upstream guards adapted and the bridge installed on its default branch.
