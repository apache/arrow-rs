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

## [58.3.0](https://github.com/apache/arrow-rs/tree/58.3.0) (2026-05-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/58.2.0...58.3.0)

**Implemented enhancements:**

- Add `DatePart::from_str` API [\#9930](https://github.com/apache/arrow-rs/issues/9930) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- should use DictionaryArray::with\_values instead of try\_new on the dictionary fast path [\#9889](https://github.com/apache/arrow-rs/issues/9889) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-string\] add concat\_elements for BinaryViewArray and FixedSizeBinary [\#9875](https://github.com/apache/arrow-rs/issues/9875) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose eq ignore ascii case from arrow-string [\#9870](https://github.com/apache/arrow-rs/issues/9870) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Configurable data page v2 compression threshold [\#9827](https://github.com/apache/arrow-rs/issues/9827) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- \[arrow-cast\] incorrect Time32 -\> Time64 conversion [\#9851](https://github.com/apache/arrow-rs/issues/9851) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when reading malformed compact-Thrift bool fields in Parquet page metadata [\#9839](https://github.com/apache/arrow-rs/issues/9839) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet `DeltaBitPackDecoder::skip` could panic on "non-standard" miniblocks [\#9793](https://github.com/apache/arrow-rs/issues/9793) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- docs: Add guidance for AI assisted submissions to CONTRIBUTING.md [\#9892](https://github.com/apache/arrow-rs/pull/9892) ([etseidl](https://github.com/etseidl))
- Update release schedule on README [\#9881](https://github.com/apache/arrow-rs/pull/9881) ([alamb](https://github.com/alamb))
- Add more documentation for FixedSizeBinary arrays [\#9866](https://github.com/apache/arrow-rs/pull/9866) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: document why FixedSizeBinary offset is always 0 [\#9861](https://github.com/apache/arrow-rs/pull/9861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: Update contributing guidelines with benchmark results [\#9782](https://github.com/apache/arrow-rs/pull/9782) ([alamb](https://github.com/alamb))

**Closed issues:**

- GenericByteDictionaryBuilder::with\_capacity does not pre-size dedup HashTable [\#9907](https://github.com/apache/arrow-rs/issues/9907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-buffer\] Integer overflow in repeat\_slice\_n\_times leads to undefined behavior [\#9904](https://github.com/apache/arrow-rs/issues/9904) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-buffer\] Integer overflow in BitChunks::new leads to undefined behavior [\#9903](https://github.com/apache/arrow-rs/issues/9903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-row\] Integer overflow in Rows::row index handling leads to undefined behavior [\#9901](https://github.com/apache/arrow-rs/issues/9901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-data\] Integer overflow in ArrayData validation leads to undefined behavior [\#9900](https://github.com/apache/arrow-rs/issues/9900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-data\] Integer overflow in ArrayData::slice leads to undefined behavior [\#9899](https://github.com/apache/arrow-rs/issues/9899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-array\] Integer overflow in FixedSizeBinaryArray::value leads to undefined behavior [\#9898](https://github.com/apache/arrow-rs/issues/9898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-buffer\] Integer overflow in BufferBuilder::reserve leads to undefined behavior [\#9897](https://github.com/apache/arrow-rs/issues/9897) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-csv: integer overflow panic in Reader::records::flush [\#9885](https://github.com/apache/arrow-rs/issues/9885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make an API to help with the pattern of 'replaces the values of the REE array'  [\#9854](https://github.com/apache/arrow-rs/issues/9854) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet reader rejects canonical UNKNOWN logical type on BOOLEAN physical columns [\#9844](https://github.com/apache/arrow-rs/issues/9844) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- ColumnIndex length mismatch can cause panic during decoding in Parquet [\#9832](https://github.com/apache/arrow-rs/issues/9832) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Bug converting json to fixed list of zero size [\#9780](https://github.com/apache/arrow-rs/issues/9780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- impl `FromStr` for `DatePart` [\#9931](https://github.com/apache/arrow-rs/pull/9931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sdf-jkl](https://github.com/sdf-jkl))
- Pre-size dedup HashTable in GenericByteDictionaryBuilder::with\_capacity [\#9908](https://github.com/apache/arrow-rs/pull/9908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rabenhorst](https://github.com/rabenhorst))
- \[arrow-array\] Use consistent `value_length` name in FixedSizeBinaryArray [\#9905](https://github.com/apache/arrow-rs/pull/9905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- replace Dictionary::try\_new\(\) calls with with\_values. [\#9894](https://github.com/apache/arrow-rs/pull/9894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- API to help with the pattern of 'replaces the values of the REE array [\#9891](https://github.com/apache/arrow-rs/pull/9891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- fix\(arrow-csv\): bound RecordDecoder::flush offset accumulation [\#9886](https://github.com/apache/arrow-rs/pull/9886) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([masumi-ryugo](https://github.com/masumi-ryugo))
- fix\(parquet\): bound schema num\_children before Vec::with\_capacity [\#9884](https://github.com/apache/arrow-rs/pull/9884) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([masumi-ryugo](https://github.com/masumi-ryugo))
- feat\(arrow-string\): concat\_elements for view, fixed binary [\#9876](https://github.com/apache/arrow-rs/pull/9876) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([theirix](https://github.com/theirix))
- Prevent `FixedSizeBinaryArray` `i32` offset overflows \(try 2\) [\#9872](https://github.com/apache/arrow-rs/pull/9872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[arrow-string\]: add `like::eq_ascii_ignore_case` kernel [\#9871](https://github.com/apache/arrow-rs/pull/9871) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- fix\(parquet\): Prevent negative list sizes in Thrift compact protocol parser [\#9868](https://github.com/apache/arrow-rs/pull/9868) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([masumi-ryugo](https://github.com/masumi-ryugo))
- \[PARQUET\] Allow `UNKNOWN` logical type annotation on any physical type [\#9855](https://github.com/apache/arrow-rs/pull/9855) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[arrow-ipc\]: dictionary builders for delta - doc fix and integration tests for nested types [\#9853](https://github.com/apache/arrow-rs/pull/9853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- fix\(arrow-cast\): fix incorrect conversion [\#9852](https://github.com/apache/arrow-rs/pull/9852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bboissin](https://github.com/bboissin))
- chore\[benches\]: add REE interleave benchmarks [\#9849](https://github.com/apache/arrow-rs/pull/9849) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- test\(parquet\): replace `InMemoryArrayReader` with `PrimitiveArrayReader` in tests [\#9847](https://github.com/apache/arrow-rs/pull/9847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- REE row conversion speed up [\#9845](https://github.com/apache/arrow-rs/pull/9845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- fix\(parquet\): Avoid panic on malformed thrift bool fields in parquet metadata [\#9840](https://github.com/apache/arrow-rs/pull/9840) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([BoazC-MSFT](https://github.com/BoazC-MSFT))
- fix\(parquet\): avoid panic on ColumnIndex length mismatch [\#9833](https://github.com/apache/arrow-rs/pull/9833) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([pchintar](https://github.com/pchintar))
- configurable data page v2 compression threshold [\#9826](https://github.com/apache/arrow-rs/pull/9826) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([leoyvens](https://github.com/leoyvens))
- Prevent `ArrayData::slice` length overflow [\#9813](https://github.com/apache/arrow-rs/pull/9813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix JSON reader panic for non-nullable zero-size FixedSizeList [\#9810](https://github.com/apache/arrow-rs/pull/9810) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- \[Parquet\] Do not panic when trying to skip records in delta encoded files using non-standard block sizes [\#9794](https://github.com/apache/arrow-rs/pull/9794) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
