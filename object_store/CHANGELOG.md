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

## [object_store_0.10.0](https://github.com/apache/arrow-rs/tree/object_store_0.10.0) (2024-04-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.9.1...object_store_0.10.0)

**Breaking changes:**

- Add put\_multipart\_opts \(\#5435\) [\#5652](https://github.com/apache/arrow-rs/pull/5652) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add Attributes API \(\#5329\) [\#5650](https://github.com/apache/arrow-rs/pull/5650) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Support non-contiguous put payloads / vectored writes \(\#5514\) [\#5538](https://github.com/apache/arrow-rs/pull/5538) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Replace AsyncWrite with Upload trait and rename MultiPartStore to MultipartStore \(\#5458\) [\#5500](https://github.com/apache/arrow-rs/pull/5500) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Improve Retry Coverage [\#5608](https://github.com/apache/arrow-rs/issues/5608) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Zero Copy Support [\#5593](https://github.com/apache/arrow-rs/issues/5593)
- ObjectStore bulk delete [\#5591](https://github.com/apache/arrow-rs/issues/5591)
- Retry on Broken Connection [\#5589](https://github.com/apache/arrow-rs/issues/5589)
- Inconsistent Multipart Nomenclature [\#5526](https://github.com/apache/arrow-rs/issues/5526) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[ObjectStore\] Non-Contiguous Write Payloads [\#5514](https://github.com/apache/arrow-rs/issues/5514) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- In Object Store, return version & etag on multipart put. [\#5443](https://github.com/apache/arrow-rs/issues/5443) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Release Object Store 0.9.1 [\#5436](https://github.com/apache/arrow-rs/issues/5436) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object\_store: allow setting content-type per request [\#5329](https://github.com/apache/arrow-rs/issues/5329) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- GCS Signed URL Support [\#5233](https://github.com/apache/arrow-rs/issues/5233)

**Fixed bugs:**

- \[object\_store\] minor bug: typos present in local variable  [\#5628](https://github.com/apache/arrow-rs/issues/5628) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- \[arrow-csv\] Schema inference requires csv on disk [\#5551](https://github.com/apache/arrow-rs/issues/5551)
- Local object store copy/rename with nonexistent `from` file loops forever instead of erroring [\#5503](https://github.com/apache/arrow-rs/issues/5503) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- object store ApplicationDefaultCredentials auth is not working on windows  [\#5466](https://github.com/apache/arrow-rs/issues/5466) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- MicrosoftAzure store list result omits empty objects [\#5451](https://github.com/apache/arrow-rs/issues/5451) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Documentation updates:**

- Minor: add additional documentation about `BufWriter` [\#5519](https://github.com/apache/arrow-rs/pull/5519) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- minor-fix: removed typos in object\_store sub crate [\#5629](https://github.com/apache/arrow-rs/pull/5629) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Silemo](https://github.com/Silemo))
- Retry on More Error Classes [\#5609](https://github.com/apache/arrow-rs/pull/5609) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([andrebsguedes](https://github.com/andrebsguedes))
- Fix handling of empty multipart uploads for GCS [\#5590](https://github.com/apache/arrow-rs/pull/5590) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Upgrade object\_store dependency to use chrono `0.4.34` [\#5578](https://github.com/apache/arrow-rs/pull/5578) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([l1nxy](https://github.com/l1nxy))
- Fix Latest Clippy Lints for object\_store [\#5546](https://github.com/apache/arrow-rs/pull/5546) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Update reqwest 0.12 and http 1.0 [\#5536](https://github.com/apache/arrow-rs/pull/5536) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Implement MultipartStore for ThrottledStore [\#5533](https://github.com/apache/arrow-rs/pull/5533) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- fix: copy/rename return error if source is nonexistent [\#5528](https://github.com/apache/arrow-rs/pull/5528) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dimbtp](https://github.com/dimbtp))
- Prepare arrow 51.0.0 [\#5516](https://github.com/apache/arrow-rs/pull/5516) ([tustvold](https://github.com/tustvold))
- Implement MultiPartStore for InMemory [\#5495](https://github.com/apache/arrow-rs/pull/5495) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add more comprehensive documentation on testing and benchmarking to CONTRIBUTING.md [\#5478](https://github.com/apache/arrow-rs/pull/5478) ([monkwire](https://github.com/monkwire))
- add support for gcp application default auth on windows in object store [\#5473](https://github.com/apache/arrow-rs/pull/5473) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Itayazolay](https://github.com/Itayazolay))
- Update base64 requirement from 0.21 to 0.22 in /object\_store [\#5465](https://github.com/apache/arrow-rs/pull/5465) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Uses ResourceType for filtering list directories instead of workaround [\#5452](https://github.com/apache/arrow-rs/pull/5452) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([andrebsguedes](https://github.com/andrebsguedes))
- Add GCS signed URL support [\#5300](https://github.com/apache/arrow-rs/pull/5300) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([l1nxy](https://github.com/l1nxy))

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
