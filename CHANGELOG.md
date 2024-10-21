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

## [53.2.0](https://github.com/apache/arrow-rs/tree/53.2.0) (2024-10-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.1.0...53.2.0)

**Implemented enhancements:**

- Implement arrow\_json encoder for Decimal128 & Decimal256 DataTypes [\#6605](https://github.com/apache/arrow-rs/issues/6605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DataType::FixedSizeList in make\_builder within struct\_builder.rs [\#6594](https://github.com/apache/arrow-rs/issues/6594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DataType::Dictionary in `make_builder` within struct\_builder.rs [\#6589](https://github.com/apache/arrow-rs/issues/6589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Interval parsing from string - accept "mon" and "mons" token [\#6548](https://github.com/apache/arrow-rs/issues/6548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `AsyncArrowWriter` API to get the total size of a written parquet file [\#6530](https://github.com/apache/arrow-rs/issues/6530) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `append_many` for Dictionary builders [\#6529](https://github.com/apache/arrow-rs/issues/6529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Missing tonic `GRPC_STATUS` with tonic 0.12.1 [\#6515](https://github.com/apache/arrow-rs/issues/6515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add example of how to use parquet metadata reader APIs for a local cache [\#6504](https://github.com/apache/arrow-rs/issues/6504) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove reliance on `raw-entry` feature of Hashbrown [\#6498](https://github.com/apache/arrow-rs/issues/6498) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Improve page index metadata loading in `SerializedFileReader::new_with_options` [\#6491](https://github.com/apache/arrow-rs/issues/6491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Release arrow-rs / parquet minor version `53.1.0` \(October 2024\) [\#6340](https://github.com/apache/arrow-rs/issues/6340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Compilation fail where `c_char = u8` [\#6571](https://github.com/apache/arrow-rs/issues/6571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow flight CI test failing on `master` [\#6568](https://github.com/apache/arrow-rs/issues/6568) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Documentation updates:**

- Minor: Document SIMD rationale and tips [\#6554](https://github.com/apache/arrow-rs/pull/6554) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Casting to and from unions [\#6247](https://github.com/apache/arrow-rs/issues/6247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Minor: more comments for `RecordBatch.get_array_memory_size()` [\#6607](https://github.com/apache/arrow-rs/pull/6607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([2010YOUY01](https://github.com/2010YOUY01))
- Implement arrow\_json encoder for Decimal128 & Decimal256 [\#6606](https://github.com/apache/arrow-rs/pull/6606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phillipleblanc](https://github.com/phillipleblanc))
- Add support for building FixedSizeListBuilder in struct\_builder's mak… [\#6595](https://github.com/apache/arrow-rs/pull/6595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszlim](https://github.com/kszlim))
- Add limited support for dictionary builders in `make_builders` for stru… [\#6593](https://github.com/apache/arrow-rs/pull/6593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszlim](https://github.com/kszlim))
- Fix CI with new valid certificates and add script for future usage [\#6585](https://github.com/apache/arrow-rs/pull/6585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))
- Update proc-macro2 requirement from =1.0.87 to =1.0.88 [\#6579](https://github.com/apache/arrow-rs/pull/6579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix clippy complaints [\#6573](https://github.com/apache/arrow-rs/pull/6573) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))
- Use c\_char instead of i8 to compile on platforms where c\_char = u8 [\#6572](https://github.com/apache/arrow-rs/pull/6572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([itsjunetime](https://github.com/itsjunetime))
- Bump pyspark from 3.3.1 to 3.3.2 in /parquet/pytest [\#6564](https://github.com/apache/arrow-rs/pull/6564) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- `unsafe` improvements [\#6551](https://github.com/apache/arrow-rs/pull/6551) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ssbr](https://github.com/ssbr))
- Update README.md [\#6550](https://github.com/apache/arrow-rs/pull/6550) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Abdullahsab3](https://github.com/Abdullahsab3))
- Fix string '0' cast to decimal with scale 0 [\#6547](https://github.com/apache/arrow-rs/pull/6547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Add finish to `AsyncArrowWriter::finish` [\#6543](https://github.com/apache/arrow-rs/pull/6543) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add append\_nulls to dictionary builders [\#6542](https://github.com/apache/arrow-rs/pull/6542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- Improve UnionArray::is\_nullable [\#6540](https://github.com/apache/arrow-rs/pull/6540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow to read parquet binary column as UTF8 type [\#6539](https://github.com/apache/arrow-rs/pull/6539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([goldmedal](https://github.com/goldmedal))
- Use HashTable instead of raw\_entry\_mut [\#6537](https://github.com/apache/arrow-rs/pull/6537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add append\_many to dictionary arrays to allow adding repeated values [\#6534](https://github.com/apache/arrow-rs/pull/6534) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- Adds documentation and example recommending Vec\<ArrayRef\> over ChunkedArray [\#6527](https://github.com/apache/arrow-rs/pull/6527) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([efredine](https://github.com/efredine))
- Update proc-macro2 requirement from =1.0.86 to =1.0.87 [\#6526](https://github.com/apache/arrow-rs/pull/6526) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `ColumnChunkMetadataBuilder` clear APIs [\#6523](https://github.com/apache/arrow-rs/pull/6523) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update sysinfo requirement from 0.31.2 to 0.32.0 [\#6521](https://github.com/apache/arrow-rs/pull/6521) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update Tonic to 0.12.3 [\#6517](https://github.com/apache/arrow-rs/pull/6517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([cisaacson](https://github.com/cisaacson))
- Detect missing page indexes while reading Parquet metadata [\#6507](https://github.com/apache/arrow-rs/pull/6507) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Use ParquetMetaDataReader to load page indexes in `SerializedFileReader::new_with_options` [\#6506](https://github.com/apache/arrow-rs/pull/6506) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve parquet `MetadataFetch` and `AsyncFileReader` docs [\#6505](https://github.com/apache/arrow-rs/pull/6505) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix arrow-json encoding with dictionary including nulls [\#6503](https://github.com/apache/arrow-rs/pull/6503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Update brotli requirement from 6.0 to 7.0 [\#6499](https://github.com/apache/arrow-rs/pull/6499) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Benchmark both scenarios, with records skipped and without skipping, for delta-bin-packed primitive arrays with half nulls. [\#6489](https://github.com/apache/arrow-rs/pull/6489) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- Add round trip tests for reading/writing parquet metadata [\#6463](https://github.com/apache/arrow-rs/pull/6463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
