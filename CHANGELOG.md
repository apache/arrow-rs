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

## [58.3.0](https://github.com/apache/arrow-rs/tree/58.3.0) (2026-05-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/58.2.0...58.3.0)

**Implemented enhancements:**

- Configurable data page v2 compression threshold [\#9827](https://github.com/apache/arrow-rs/issues/9827)

**Fixed bugs:**

- \[arrow-cast\] incorrect Time32 -\> Time64 conversion [\#9851](https://github.com/apache/arrow-rs/issues/9851)
- Panic when reading malformed compact-Thrift bool fields in Parquet page metadata [\#9839](https://github.com/apache/arrow-rs/issues/9839)
- Parquet `DeltaBitPackDecoder::skip` could panic on "non-standard" miniblocks [\#9793](https://github.com/apache/arrow-rs/issues/9793)

**Documentation updates:**

- Add more documentation for FixedSizeBinary arrays [\#9866](https://github.com/apache/arrow-rs/pull/9866) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: document why FixedSizeBinary offset is always 0 [\#9861](https://github.com/apache/arrow-rs/pull/9861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: Update contributing guidelines with benchmark results [\#9782](https://github.com/apache/arrow-rs/pull/9782) ([alamb](https://github.com/alamb))

**Closed issues:**

- Parquet reader rejects canonical UNKNOWN logical type on BOOLEAN physical columns [\#9844](https://github.com/apache/arrow-rs/issues/9844)
- ColumnIndex length mismatch can cause panic during decoding in Parquet [\#9832](https://github.com/apache/arrow-rs/issues/9832)
- Bug converting json to fixed list of zero size [\#9780](https://github.com/apache/arrow-rs/issues/9780)

**Merged pull requests:**

- Prevent `FixedSizeBinaryArray` `i32` offset overflows \(try 2\) [\#9872](https://github.com/apache/arrow-rs/pull/9872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[PARQUET\] Allow `UNKNOWN` logical type annotation on any physical type [\#9855](https://github.com/apache/arrow-rs/pull/9855) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[arrow-ipc\]: dictionary builders for delta - doc fix and integration tests for nested types [\#9853](https://github.com/apache/arrow-rs/pull/9853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- fix\(arrow-cast\): fix incorrect conversion [\#9852](https://github.com/apache/arrow-rs/pull/9852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bboissin](https://github.com/bboissin))
- REE row conversion speed up [\#9845](https://github.com/apache/arrow-rs/pull/9845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- fix\(parquet\): Avoid panic on malformed thrift bool fields in parquet metadata [\#9840](https://github.com/apache/arrow-rs/pull/9840) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([BoazC-MSFT](https://github.com/BoazC-MSFT))
- fix\(parquet\): avoid panic on ColumnIndex length mismatch [\#9833](https://github.com/apache/arrow-rs/pull/9833) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([pchintar](https://github.com/pchintar))
- configurable data page v2 compression threshold [\#9826](https://github.com/apache/arrow-rs/pull/9826) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([leoyvens](https://github.com/leoyvens))
- Prevent `ArrayData::slice` length overflow [\#9813](https://github.com/apache/arrow-rs/pull/9813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix JSON reader panic for non-nullable zero-size FixedSizeList [\#9810](https://github.com/apache/arrow-rs/pull/9810) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- \[Parquet\] Do not panic when trying to skip records in delta encoded files using non-standard block sizes [\#9794](https://github.com/apache/arrow-rs/pull/9794) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
