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

## [object_store_0.11.1](https://github.com/apache/arrow-rs/tree/object_store_0.11.1) (2024-10-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.11.0...object_store_0.11.1)

**Implemented enhancements:**

- There is no way to pass object store client options as environment variables [\#6333](https://github.com/apache/arrow-rs/issues/6333) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Better Document Backoff Algorithm [\#6324](https://github.com/apache/arrow-rs/issues/6324) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add direction to `list_with_offset` [\#6274](https://github.com/apache/arrow-rs/issues/6274) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Support server-side encryption with customer-provided keys \(SSE-C\) [\#6229](https://github.com/apache/arrow-rs/issues/6229) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Release object store `0.11.0` \(breaking API\) around Aug 15 2024 [\#6121](https://github.com/apache/arrow-rs/issues/6121) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Fixed bugs:**

- \[object-store\] Requested tokio version is too old - does not compile [\#6458](https://github.com/apache/arrow-rs/issues/6458) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Azure SAS tokens are visible when retry errors are logged via object\_store [\#6322](https://github.com/apache/arrow-rs/issues/6322) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- Columnar json writer for arrow-json [\#6411](https://github.com/apache/arrow-rs/issues/6411)
- Different numeric type may be able to compare [\#6357](https://github.com/apache/arrow-rs/issues/6357)
- Simplify take octokit workflow [\#6279](https://github.com/apache/arrow-rs/issues/6279)
- Casting to and from unions [\#6247](https://github.com/apache/arrow-rs/issues/6247)
- Port `take` workflow to use `oktokit` [\#6242](https://github.com/apache/arrow-rs/issues/6242)

**Merged pull requests:**

- object\_store: Clarify what is a prefix in list\(\) documentation [\#6520](https://github.com/apache/arrow-rs/pull/6520) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([progval](https://github.com/progval))
- object\_store: enable lint `unreachable_pub` [\#6512](https://github.com/apache/arrow-rs/pull/6512) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))
- \[object\_store\] Retry S3 requests with 200 response with "Error" in body [\#6508](https://github.com/apache/arrow-rs/pull/6508) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([PeterKeDer](https://github.com/PeterKeDer))
- Prepare for 53.1.0 release \(CHANGELOG and version\) [\#6501](https://github.com/apache/arrow-rs/pull/6501) ([alamb](https://github.com/alamb))
- Minor: Fix path in format command in CONTRIBUTING.md [\#6494](https://github.com/apache/arrow-rs/pull/6494) ([etseidl](https://github.com/etseidl))
- \[object-store\] Require tokio 1.29.0. [\#6459](https://github.com/apache/arrow-rs/pull/6459) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ashtuchkin](https://github.com/ashtuchkin))
- Fix CI by disabling newly failing rust \<\> nanoarrow integration test in CI [\#6449](https://github.com/apache/arrow-rs/pull/6449) ([alamb](https://github.com/alamb))
- feat: expose HTTP/2 max frame size in `object_store` [\#6442](https://github.com/apache/arrow-rs/pull/6442) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- Derive `Clone` for `object_store::aws::AmazonS3` [\#6414](https://github.com/apache/arrow-rs/pull/6414) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ethe](https://github.com/ethe))
- Reduce integration test matrix [\#6407](https://github.com/apache/arrow-rs/pull/6407) ([kou](https://github.com/kou))
- object\_score: Support Azure Fabric OAuth Provider [\#6382](https://github.com/apache/arrow-rs/pull/6382) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([RobinLin666](https://github.com/RobinLin666))
- `object_store::GetOptions` derive `Clone` [\#6361](https://github.com/apache/arrow-rs/pull/6361) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([samuelcolvin](https://github.com/samuelcolvin))
- Add breaking change from \#6043 to `CHANGELOG` [\#6354](https://github.com/apache/arrow-rs/pull/6354) ([mbrobbel](https://github.com/mbrobbel))
- Prepare arrow/parquet `53.0.0` release [\#6338](https://github.com/apache/arrow-rs/pull/6338) ([alamb](https://github.com/alamb))
- \[object\_store\] Propagate env vars as object store client options [\#6334](https://github.com/apache/arrow-rs/pull/6334) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ccciudatu](https://github.com/ccciudatu))
- docs\[object\_store\]: clarify the backoff strategy that is actually implemented [\#6325](https://github.com/apache/arrow-rs/pull/6325) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([westonpace](https://github.com/westonpace))
- fix: azure sas token visible in logs [\#6323](https://github.com/apache/arrow-rs/pull/6323) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- ci: use octokit to add assignee [\#6267](https://github.com/apache/arrow-rs/pull/6267) ([dsgibbons](https://github.com/dsgibbons))
- object\_store/delimited: Fix `TrailingEscape` condition [\#6265](https://github.com/apache/arrow-rs/pull/6265) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Turbo87](https://github.com/Turbo87))
- fix\(object\_store\): only add encryption headers for SSE-C in get request [\#6260](https://github.com/apache/arrow-rs/pull/6260) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jiachengdb](https://github.com/jiachengdb))
- Minor: Remove non standard footer from LICENSE.txt / reference to Apache Aurora [\#6237](https://github.com/apache/arrow-rs/pull/6237) ([alamb](https://github.com/alamb))
- docs: Add parquet\_opendal in related projects [\#6236](https://github.com/apache/arrow-rs/pull/6236) ([Xuanwo](https://github.com/Xuanwo))
- feat\(object\_store\): add support for server-side encryption with customer-provided keys \(SSE-C\) [\#6230](https://github.com/apache/arrow-rs/pull/6230) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jiachengdb](https://github.com/jiachengdb))
- feat: further TLS options on ClientOptions: \#5034 [\#6148](https://github.com/apache/arrow-rs/pull/6148) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
