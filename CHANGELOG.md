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

## [52.2.0](https://github.com/apache/arrow-rs/tree/52.2.0) (2024-07-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/52.1.0...52.2.0)

**Implemented enhancements:**

- Faster min/max for string/binary view arrays [\#6088](https://github.com/apache/arrow-rs/issues/6088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting to/from Utf8View [\#6076](https://github.com/apache/arrow-rs/issues/6076) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Min/max support for String/BinaryViewArray [\#6052](https://github.com/apache/arrow-rs/issues/6052) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of constructing `ByteView`s for small strings [\#6034](https://github.com/apache/arrow-rs/issues/6034) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fast UTF-8 validation when reading StringViewArray from Parquet [\#5995](https://github.com/apache/arrow-rs/issues/5995) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize StringView row decoding [\#5945](https://github.com/apache/arrow-rs/issues/5945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implementing `deduplicate` / `intern` functionality for StringView [\#5910](https://github.com/apache/arrow-rs/issues/5910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `FlightSqlServiceClient::new_from_inner` [\#6003](https://github.com/apache/arrow-rs/pull/6003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Complete `StringViewArray` and `BinaryViewArray` parquet decoder:  [\#6004](https://github.com/apache/arrow-rs/pull/6004) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add begin/end\_transaction methods in FlightSqlServiceClient [\#6026](https://github.com/apache/arrow-rs/pull/6026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Read Parquet statistics as arrow `Arrays`  [\#6046](https://github.com/apache/arrow-rs/pull/6046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([efredine](https://github.com/efredine))

**Fixed bugs:**

- Panic in `ParquetMetadata::memory_size` if no min/max set [\#6091](https://github.com/apache/arrow-rs/issues/6091) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- BinaryViewArray doesn't roundtrip a single `Some(&[])` through parquet [\#6086](https://github.com/apache/arrow-rs/issues/6086) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet `ColumnIndex` for null columns is written even when statistics are disabled [\#6010](https://github.com/apache/arrow-rs/issues/6010) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Fix typo in GenericByteViewArray documentation [\#6054](https://github.com/apache/arrow-rs/pull/6054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([progval](https://github.com/progval))
- Minor: Improve parquet PageIndex documentation [\#6042](https://github.com/apache/arrow-rs/pull/6042) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Potential performance improvements for reading Parquet to StringViewArray/BinaryViewArray [\#5904](https://github.com/apache/arrow-rs/issues/5904) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Faster `GenericByteView` construction [\#6102](https://github.com/apache/arrow-rs/pull/6102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add benchmark to track byte-view construction performance [\#6101](https://github.com/apache/arrow-rs/pull/6101) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Optimize `bool_or` using `max_boolean` [\#6100](https://github.com/apache/arrow-rs/pull/6100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Optimize `max_boolean` by operating on u64 chunks [\#6098](https://github.com/apache/arrow-rs/pull/6098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- fix panic in `ParquetMetadata::memory_size`: check has\_min\_max\_set before invoking min\(\)/max\(\) [\#6092](https://github.com/apache/arrow-rs/pull/6092) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Fischer0522](https://github.com/Fischer0522))
- Implement specialized min/max for `GenericBinaryView` \(`StringView` and `BinaryView`\) [\#6089](https://github.com/apache/arrow-rs/pull/6089) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add PartialEq to ParquetMetaData and FileMetadata [\#6082](https://github.com/apache/arrow-rs/pull/6082) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Enable casting from Utf8View [\#6077](https://github.com/apache/arrow-rs/pull/6077) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- StringView support in arrow-csv [\#6062](https://github.com/apache/arrow-rs/pull/6062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([2010YOUY01](https://github.com/2010YOUY01))
- Implement min max support for string/binary view types [\#6053](https://github.com/apache/arrow-rs/pull/6053) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Minor: clarify the relationship between `file::metadata` and `format` in docs [\#6049](https://github.com/apache/arrow-rs/pull/6049) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor API adjustments for StringViewBuilder [\#6047](https://github.com/apache/arrow-rs/pull/6047) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add parquet `StatisticsConverter` for arrow reader [\#6046](https://github.com/apache/arrow-rs/pull/6046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([efredine](https://github.com/efredine))
- Directly decode String/BinaryView types from arrow-row format [\#6044](https://github.com/apache/arrow-rs/pull/6044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Clean up unused code for view types in offset buffer [\#6040](https://github.com/apache/arrow-rs/pull/6040) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Avoid using Buffer api that accidentally copies data [\#6039](https://github.com/apache/arrow-rs/pull/6039) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([XiangpengHao](https://github.com/XiangpengHao))
- MINOR: Fix `hashbrown` version in `arrow-array`, remove from `arrow-row` [\#6035](https://github.com/apache/arrow-rs/pull/6035) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Improve performance reading `ByteViewArray` from parquet by removing an implicit copy [\#6031](https://github.com/apache/arrow-rs/pull/6031) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add begin/end\_transaction methods in FlightSqlServiceClient [\#6026](https://github.com/apache/arrow-rs/pull/6026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Unsafe improvements: core `parquet` crate. [\#6024](https://github.com/apache/arrow-rs/pull/6024) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([veluca93](https://github.com/veluca93))
- Additional tests for parquet reader utf8 validation [\#6023](https://github.com/apache/arrow-rs/pull/6023) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update zstd-sys requirement from \>=2.0.0, \<2.0.12 to \>=2.0.0, \<2.0.13 [\#6019](https://github.com/apache/arrow-rs/pull/6019) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix doc ci in latest rust nightly version [\#6012](https://github.com/apache/arrow-rs/pull/6012) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rachelint](https://github.com/Rachelint))
- Do not write `ColumnIndex` for null columns when not writing page statistics [\#6011](https://github.com/apache/arrow-rs/pull/6011) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fast utf8 validation when loading string view from parquet [\#6009](https://github.com/apache/arrow-rs/pull/6009) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Deduplicate strings/binarys when building view types [\#6005](https://github.com/apache/arrow-rs/pull/6005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Complete `StringViewArray` and `BinaryViewArray` parquet decoder:  implement delta byte array and delta length byte array encoding [\#6004](https://github.com/apache/arrow-rs/pull/6004) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add `FlightSqlServiceClient::new_from_inner` [\#6003](https://github.com/apache/arrow-rs/pull/6003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Rename `Schema::all_fields` to `flattened_fields` [\#6001](https://github.com/apache/arrow-rs/pull/6001) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Refine documentation and examples for `DataType` [\#5997](https://github.com/apache/arrow-rs/pull/5997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- implement `DataType::try_form(&str)` [\#5994](https://github.com/apache/arrow-rs/pull/5994) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Implement dictionary support for reading ByteView from parquet [\#5973](https://github.com/apache/arrow-rs/pull/5973) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
