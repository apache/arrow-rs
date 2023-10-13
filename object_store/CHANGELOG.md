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

# Changelog

## [object_store_0.7.1](https://github.com/apache/arrow-rs/tree/object_store_0.7.1) (2023-09-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.7.0...object_store_0.7.1)

**Implemented enhancements:**

- Automatically Cleanup LocalFileSystem Temporary Files [\#4778](https://github.com/apache/arrow-rs/issues/4778) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object-store: Expose an async reader API for object store [\#4762](https://github.com/apache/arrow-rs/issues/4762)
- Improve proxy support by using reqwest::Proxy as configuration [\#4713](https://github.com/apache/arrow-rs/issues/4713) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- object-store: http shouldn't perform range requests unless `accept-ranges: bytes` header is present [\#4839](https://github.com/apache/arrow-rs/issues/4839)
- object-store: http-store fails when url doesn't have last-modified header on 0.7.0 [\#4831](https://github.com/apache/arrow-rs/issues/4831)
- object-store fails to compile for `wasm32-unknown-unknown` with `http` feature [\#4776](https://github.com/apache/arrow-rs/issues/4776) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object-store: could not find `header` in `client` for `http` feature [\#4775](https://github.com/apache/arrow-rs/issues/4775) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- LocalFileSystem Copy and Rename Don't Create Intermediate Directories [\#4760](https://github.com/apache/arrow-rs/issues/4760) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- LocalFileSystem Copy is not Atomic [\#4758](https://github.com/apache/arrow-rs/issues/4758) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- object\_store Azure Government Cloud functionality? [\#4853](https://github.com/apache/arrow-rs/issues/4853)

**Merged pull requests:**

- Add ObjectStore BufReader \(\#4762\) [\#4857](https://github.com/apache/arrow-rs/pull/4857) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Allow overriding azure endpoint [\#4854](https://github.com/apache/arrow-rs/pull/4854) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Minor: Improve object\_store docs.rs landing page [\#4849](https://github.com/apache/arrow-rs/pull/4849) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))
- Error if Remote Ignores HTTP Range Header [\#4841](https://github.com/apache/arrow-rs/pull/4841) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([universalmind303](https://github.com/universalmind303))
- Perform HEAD request for HttpStore::head [\#4837](https://github.com/apache/arrow-rs/pull/4837) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- fix: object store http header last modified [\#4834](https://github.com/apache/arrow-rs/pull/4834) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([universalmind303](https://github.com/universalmind303))
- Prepare arrow 47.0.0 [\#4827](https://github.com/apache/arrow-rs/pull/4827) ([tustvold](https://github.com/tustvold))
- ObjectStore Wasm32 Fixes \(\#4775\) \(\#4776\) [\#4796](https://github.com/apache/arrow-rs/pull/4796) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Best effort cleanup of staged upload files \(\#4778\) [\#4792](https://github.com/apache/arrow-rs/pull/4792) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Relaxing type bounds on coalesce\_ranges and collect\_bytes [\#4787](https://github.com/apache/arrow-rs/pull/4787) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sumerman](https://github.com/sumerman))
- Update object\_store chrono deprecations [\#4786](https://github.com/apache/arrow-rs/pull/4786) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Make coalesce\_ranges and collect\_bytes available for crate users [\#4784](https://github.com/apache/arrow-rs/pull/4784) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sumerman](https://github.com/sumerman))
- Bump actions/checkout from 3 to 4 [\#4767](https://github.com/apache/arrow-rs/pull/4767) ([dependabot[bot]](https://github.com/apps/dependabot))
- Make ObjectStore::copy Atomic and Automatically Create Parent Directories \(\#4758\) \(\#4760\) [\#4759](https://github.com/apache/arrow-rs/pull/4759) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update nix requirement from 0.26.1 to 0.27.1 in /object\_store [\#4744](https://github.com/apache/arrow-rs/pull/4744) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))
- Add `with_proxy_ca_certificate` and `with_proxy_excludes` [\#4714](https://github.com/apache/arrow-rs/pull/4714) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([gordonwang0](https://github.com/gordonwang0))
- Update object\_store Dependencies and Configure Dependabot [\#4700](https://github.com/apache/arrow-rs/pull/4700) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
