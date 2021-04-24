<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Arrow Developer Scripts

This directory contains scripts useful to developers when packaging,
testing, or committing to Arrow.

## Verifying Release Candidates

We have provided a script to assist with verifying release candidates:

```shell
bash dev/release/verify-release-candidate.sh 0.7.0 0
```

Currently this only works on Linux (patches to expand to macOS welcome!). Read
the script for information about system dependencies.

On Windows, we have a script that verifies C++ and Python (requires Visual
Studio 2015):

```
dev/release/verify-release-candidate.bat apache-arrow-0.7.0.tar.gz
```

### Verifying the JavaScript release

For JavaScript-specific releases, use a different verification script:

```shell
bash dev/release/js-verify-release-candidate.sh 0.7.0 0
```

# Integration testing

Build the following base image used by multiple tests:

```shell
docker build -t arrow_integration_xenial_base -f docker_common/Dockerfile.xenial.base .
```
