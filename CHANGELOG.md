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

## [15.0.0](https://github.com/apache/arrow-rs/tree/15.0.0) (2022-05-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/14.0.0...15.0.0)

**Breaking changes:**

- Remove `null_count` from `ArrayData::try_new()` [\#1721](https://github.com/apache/arrow-rs/pull/1721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Change parquet writers to use standard `std:io::Write` rather custom `ParquetWriter` trait \(\#1717\) \(\#1163\) [\#1719](https://github.com/apache/arrow-rs/pull/1719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add explicit column mask construction in parquet: `ProjectionMask` \(\#1701\) [\#1716](https://github.com/apache/arrow-rs/pull/1716) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add type ids in Union datatype [\#1703](https://github.com/apache/arrow-rs/pull/1703) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Parquet Arrow Schema Inference [\#1682](https://github.com/apache/arrow-rs/pull/1682) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Improve integration test document to follow Arrow C++ repo CI [\#1741](https://github.com/apache/arrow-rs/issues/1741)
- Fix schema comparison for non\_canonical\_map when running flight test [\#1730](https://github.com/apache/arrow-rs/issues/1730)
- Fix incorrect null\_count in `generate_unions_case` integration test [\#1712](https://github.com/apache/arrow-rs/issues/1712)
- Keep type ids in Union datatype to follow Arrow spec and integrate with other implementations [\#1690](https://github.com/apache/arrow-rs/issues/1690)
- Support Reading Alternative List Representations to Arrow From Parquet [\#1680](https://github.com/apache/arrow-rs/issues/1680)
- Speed up the offsets checking [\#1675](https://github.com/apache/arrow-rs/issues/1675)
- Separate Parquet -\> Arrow Schema Conversion From ArrayBuilder [\#1655](https://github.com/apache/arrow-rs/issues/1655)
- Add `leaf_columns` argument to `ArrowReader::get_record_reader_by_columns` [\#1653](https://github.com/apache/arrow-rs/issues/1653)
- Implement `string_concat` kernel  [\#1540](https://github.com/apache/arrow-rs/issues/1540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve Unit Test Coverage of ArrayReaderBuilder [\#1484](https://github.com/apache/arrow-rs/issues/1484)

**Fixed bugs:**

- Latest nightly fails to build with feature simd [\#1734](https://github.com/apache/arrow-rs/issues/1734)
- Trying to write parquet file in parallel results in corrupt file [\#1717](https://github.com/apache/arrow-rs/issues/1717) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet command line tool installs "globally"? [\#1710](https://github.com/apache/arrow-rs/issues/1710)
- Roundtrip failure when using DELTA\_BINARY\_PACKED [\#1708](https://github.com/apache/arrow-rs/issues/1708)
- `ArrayData::try_new` cannot always return expected error. [\#1707](https://github.com/apache/arrow-rs/issues/1707)
-  "out of order projection is not supported" after Fix Parquet Arrow Schema Inference [\#1701](https://github.com/apache/arrow-rs/issues/1701) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Interoperability with C++ for IPC schemas with dictionaries [\#1694](https://github.com/apache/arrow-rs/issues/1694)
- Incorrect Repeated Field Schema Inference [\#1681](https://github.com/apache/arrow-rs/issues/1681)
- Parquet Treats Embedded Arrow Schema as Authoritative [\#1663](https://github.com/apache/arrow-rs/issues/1663)
- parquet\_to\_arrow\_schema\_by\_columns Incorrectly Handles Nested Types [\#1654](https://github.com/apache/arrow-rs/issues/1654)
- Inconsistent Arrow Schema When Projecting Nested Parquet File [\#1652](https://github.com/apache/arrow-rs/issues/1652)
- StructArrayReader Cannot Handle Nested Lists [\#1651](https://github.com/apache/arrow-rs/issues/1651)
- Bug \(`substring` kernel\): The null buffer is not aligned when `offset != 0` [\#1639](https://github.com/apache/arrow-rs/issues/1639)

**Documentation updates:**

- Improve integration test document to follow Arrow C++ repo CI [\#1742](https://github.com/apache/arrow-rs/pull/1742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Merged pull requests:**

- Pin nightly version to bypass packed\_simd build error [\#1743](https://github.com/apache/arrow-rs/pull/1743) ([viirya](https://github.com/viirya))
- `cargo install` installs not globally [\#1732](https://github.com/apache/arrow-rs/pull/1732) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kazuk](https://github.com/kazuk))
- Fix schema comparison for non\_canonical\_map when running flight test [\#1731](https://github.com/apache/arrow-rs/pull/1731) ([viirya](https://github.com/viirya))
- Fix parquet benchmarks [\#1723](https://github.com/apache/arrow-rs/pull/1723) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix BitReader::get\_batch zero extension \(\#1708\) [\#1722](https://github.com/apache/arrow-rs/pull/1722) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Implementation string concat [\#1720](https://github.com/apache/arrow-rs/pull/1720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))
- Check the length of `null_bit_buffer` in `ArrayData::try_new()` [\#1714](https://github.com/apache/arrow-rs/pull/1714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix incorrect null\_count in `generate_unions_case` integration test [\#1713](https://github.com/apache/arrow-rs/pull/1713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix: Null buffer accounts for `offset` in `substring` kernel. [\#1704](https://github.com/apache/arrow-rs/pull/1704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Minor: Refine `OffsetSizeTrait` to extend `num::Integer`  [\#1702](https://github.com/apache/arrow-rs/pull/1702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix StructArrayReader handling nested lists \(\#1651\)  [\#1700](https://github.com/apache/arrow-rs/pull/1700) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Speed up the offsets checking [\#1684](https://github.com/apache/arrow-rs/pull/1684) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
