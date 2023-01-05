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

## [object_store_0.5.3](https://github.com/apache/arrow-rs/tree/object_store_0.5.3) (2023-01-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.2...object_store_0.5.3)

**Implemented enhancements:**

- Derive Clone for the builders in object-store. [\#3419](https://github.com/apache/arrow-rs/issues/3419)
- Add a constant prefix object store wrapper [\#3328](https://github.com/apache/arrow-rs/issues/3328) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add support for content-type while uploading files through ObjectStore API [\#3300](https://github.com/apache/arrow-rs/issues/3300) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add HttpStore [\#3294](https://github.com/apache/arrow-rs/issues/3294) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Add support for Azure Data Lake Storage Gen2 \(aka: ADLS Gen2\) in Object Store library [\#3283](https://github.com/apache/arrow-rs/issues/3283)
- object\_store: Add Put and Multipart Upload Doc Examples [\#2863](https://github.com/apache/arrow-rs/issues/2863) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Closed issues:**

- Only flush buffered multi-part data on poll\_shutdown not on poll\_flush [\#3390](https://github.com/apache/arrow-rs/issues/3390) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]

**Merged pull requests:**

- object\_store: builder configuration api [\#3436](https://github.com/apache/arrow-rs/pull/3436) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Derive Clone for ObjectStore builders and Make URL Parsing Stricter \(\#3419\) [\#3424](https://github.com/apache/arrow-rs/pull/3424) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add Put and Multipart Put doc examples [\#3420](https://github.com/apache/arrow-rs/pull/3420) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([GeauxEric](https://github.com/GeauxEric))
- object\_store: update localstack instructions [\#3403](https://github.com/apache/arrow-rs/pull/3403) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- object\_store: Flush buffered multipart only during poll\_shutdown [\#3397](https://github.com/apache/arrow-rs/pull/3397) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([askoa](https://github.com/askoa))
- Update quick-xml to 0.27 [\#3395](https://github.com/apache/arrow-rs/pull/3395) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Add HttpStore \(\#3294\) [\#3380](https://github.com/apache/arrow-rs/pull/3380) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- add support for content-type in `ClientOptions` [\#3358](https://github.com/apache/arrow-rs/pull/3358) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([ByteBaker](https://github.com/ByteBaker))
- Update AWS SDK [\#3349](https://github.com/apache/arrow-rs/pull/3349) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Upstream newline\_delimited\_stream and ChunkedStore from DataFusion [\#3341](https://github.com/apache/arrow-rs/pull/3341) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- feat\(object\_store\): add PrefixObjectStore [\#3329](https://github.com/apache/arrow-rs/pull/3329) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- feat\(object\_store\): parse well-known storage urls [\#3327](https://github.com/apache/arrow-rs/pull/3327) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([roeap](https://github.com/roeap))
- Disable getrandom object\_store [\#3278](https://github.com/apache/arrow-rs/pull/3278) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Reload token from AWS\_WEB\_IDENTITY\_TOKEN\_FILE [\#3274](https://github.com/apache/arrow-rs/pull/3274) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))
- Minor: skip aws integration test if TEST\_INTEGRATION is not set [\#3262](https://github.com/apache/arrow-rs/pull/3262) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
