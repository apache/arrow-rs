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

**Fixed bugs:**

- \[object-store\] Requested tokio version is too old - does not compile [\#6458](https://github.com/apache/arrow-rs/issues/6458) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Azure SAS tokens are visible when retry errors are logged via object\_store [\#6322](https://github.com/apache/arrow-rs/issues/6322) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- object\_store: fix typo in with\_connect\_timeout\_disabled that actually disabled non-connect timeouts [\#6563](https://github.com/apache/arrow-rs/pull/6563) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([adriangb](https://github.com/adriangb))
- object\_store: Clarify what is a prefix in list\(\) documentation [\#6520](https://github.com/apache/arrow-rs/pull/6520) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([progval](https://github.com/progval))
- object\_store: enable lint `unreachable_pub` [\#6512](https://github.com/apache/arrow-rs/pull/6512) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))
- \[object\_store\] Retry S3 requests with 200 response with "Error" in body [\#6508](https://github.com/apache/arrow-rs/pull/6508) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([PeterKeDer](https://github.com/PeterKeDer))
- \[object-store\] Require tokio 1.29.0. [\#6459](https://github.com/apache/arrow-rs/pull/6459) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ashtuchkin](https://github.com/ashtuchkin))
- feat: expose HTTP/2 max frame size in `object_store` [\#6442](https://github.com/apache/arrow-rs/pull/6442) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([crepererum](https://github.com/crepererum))
- Derive `Clone` for `object_store::aws::AmazonS3` [\#6414](https://github.com/apache/arrow-rs/pull/6414) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ethe](https://github.com/ethe))
- object\_score: Support Azure Fabric OAuth Provider [\#6382](https://github.com/apache/arrow-rs/pull/6382) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([RobinLin666](https://github.com/RobinLin666))
- `object_store::GetOptions` derive `Clone` [\#6361](https://github.com/apache/arrow-rs/pull/6361) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([samuelcolvin](https://github.com/samuelcolvin))
- \[object\_store\] Propagate env vars as object store client options [\#6334](https://github.com/apache/arrow-rs/pull/6334) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ccciudatu](https://github.com/ccciudatu))
- docs\[object\_store\]: clarify the backoff strategy that is actually implemented [\#6325](https://github.com/apache/arrow-rs/pull/6325) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([westonpace](https://github.com/westonpace))
- fix: azure sas token visible in logs [\#6323](https://github.com/apache/arrow-rs/pull/6323) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- object\_store/delimited: Fix `TrailingEscape` condition [\#6265](https://github.com/apache/arrow-rs/pull/6265) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([Turbo87](https://github.com/Turbo87))
- fix\(object\_store\): only add encryption headers for SSE-C in get request [\#6260](https://github.com/apache/arrow-rs/pull/6260) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jiachengdb](https://github.com/jiachengdb))
- docs: Add parquet\_opendal in related projects [\#6236](https://github.com/apache/arrow-rs/pull/6236) ([Xuanwo](https://github.com/Xuanwo))
- feat\(object\_store\): add support for server-side encryption with customer-provided keys \(SSE-C\) [\#6230](https://github.com/apache/arrow-rs/pull/6230) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([jiachengdb](https://github.com/jiachengdb))
- feat: further TLS options on ClientOptions: \#5034 [\#6148](https://github.com/apache/arrow-rs/pull/6148) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
