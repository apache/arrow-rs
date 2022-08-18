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

## [21.0.0](https://github.com/apache/arrow-rs/tree/21.0.0) (2022-08-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/20.0.0...21.0.0)

**Breaking changes:**

- Push `ChunkReader` into `SerializedPageReader` \(\#2463\) [\#2464](https://github.com/apache/arrow-rs/pull/2464) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Revise FromIterator for Decimal128Array to use Into instead of Borrow [\#2442](https://github.com/apache/arrow-rs/pull/2442) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use Fixed-Length Array in BasicDecimal new and raw\_value [\#2405](https://github.com/apache/arrow-rs/pull/2405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove deprecated ParquetWriter [\#2380](https://github.com/apache/arrow-rs/pull/2380) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove deprecated SliceableCursor and InMemoryWriteableCursor [\#2378](https://github.com/apache/arrow-rs/pull/2378) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Remove byteorder dependency [\#2472](https://github.com/apache/arrow-rs/issues/2472)
- Push `ChunkReader` into `SerializedPageReader` [\#2463](https://github.com/apache/arrow-rs/issues/2463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support SerializedPageReader::skip\_page without OffsetIndex [\#2459](https://github.com/apache/arrow-rs/issues/2459)
- Support Time64/Time32 comparison [\#2457](https://github.com/apache/arrow-rs/issues/2457) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Revise FromIterator for Decimal128Array to use Into instead of Borrow [\#2441](https://github.com/apache/arrow-rs/issues/2441)
- Support `RowFilter` within`ParquetRecordBatchReader` [\#2431](https://github.com/apache/arrow-rs/issues/2431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove the field `StructBuilder::len` [\#2429](https://github.com/apache/arrow-rs/issues/2429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Standardize creation and configuration of parquet --\> Arrow readers \( `ParquetRecordBatchReaderBuilder`\) [\#2427](https://github.com/apache/arrow-rs/issues/2427) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use `OffsetIndex` to Prune IO in `ParquetRecordBatchStream` [\#2426](https://github.com/apache/arrow-rs/issues/2426) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `peek_next_page` and `skip_next_page` in `InMemoryPageReader` [\#2406](https://github.com/apache/arrow-rs/issues/2406) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from `Utf8`/`LargeUtf8` to `Binary`/`LargeBinary` [\#2402](https://github.com/apache/arrow-rs/issues/2402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting between `Decimal128` and `Decimal256` arrays [\#2375](https://github.com/apache/arrow-rs/issues/2375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Combine multiple selections into the same batch size in `skip_records` [\#2358](https://github.com/apache/arrow-rs/issues/2358) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add API to change timezone for timestamp array [\#2346](https://github.com/apache/arrow-rs/issues/2346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change the output of `read_buffer` Arrow IPC API to return `Result<_>` [\#2342](https://github.com/apache/arrow-rs/issues/2342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow `skip_records` in `GenericColumnReader` to skip across row groups [\#2331](https://github.com/apache/arrow-rs/issues/2331) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize the validation of `Decimal256` [\#2320](https://github.com/apache/arrow-rs/issues/2320) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement Skip for `DeltaBitPackDecoder` [\#2281](https://github.com/apache/arrow-rs/issues/2281) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Changes to `ParquetRecordBatchStream` to support row filtering in DataFusion [\#2270](https://github.com/apache/arrow-rs/issues/2270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `ArrayReader::skip_records` API [\#2197](https://github.com/apache/arrow-rs/issues/2197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Flight SQL Server sends incorrect response for `DoPutUpdateResult` [\#2403](https://github.com/apache/arrow-rs/issues/2403) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- `AsyncFileReader`No Longer Object-Safe [\#2372](https://github.com/apache/arrow-rs/issues/2372) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StructBuilder Does not Verify Child Lengths [\#2252](https://github.com/apache/arrow-rs/issues/2252)

**Closed issues:**

- Combine `DecimalArray` validation [\#2447](https://github.com/apache/arrow-rs/issues/2447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- remove byteorder dependency from parquet [\#2486](https://github.com/apache/arrow-rs/pull/2486) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Remove Position trait \(\#1163\) [\#2479](https://github.com/apache/arrow-rs/pull/2479) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add ChunkReader::get\_bytes [\#2478](https://github.com/apache/arrow-rs/pull/2478) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use Parquet OffsetIndex to prune IO with RowSelection [\#2473](https://github.com/apache/arrow-rs/pull/2473) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Remove unnecessary Option from Int96 [\#2471](https://github.com/apache/arrow-rs/pull/2471) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- remove len field from StructBuilder [\#2468](https://github.com/apache/arrow-rs/pull/2468) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Make Parquet reader filter APIs public \(\#1792\) [\#2467](https://github.com/apache/arrow-rs/pull/2467) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- enable ipc compression feature for integration test [\#2462](https://github.com/apache/arrow-rs/pull/2462) ([liukun4515](https://github.com/liukun4515))
- Simplify implementation of Schema [\#2461](https://github.com/apache/arrow-rs/pull/2461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support skip\_page missing OffsetIndex Fallback in SerializedPageReader [\#2460](https://github.com/apache/arrow-rs/pull/2460) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- support time32/time64 comparison [\#2458](https://github.com/apache/arrow-rs/pull/2458) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Utf8array casting [\#2456](https://github.com/apache/arrow-rs/pull/2456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Remove outdated license text [\#2455](https://github.com/apache/arrow-rs/pull/2455) ([alamb](https://github.com/alamb))
- Support RowFilter within ParquetRecordBatchReader \(\#2431\) [\#2452](https://github.com/apache/arrow-rs/pull/2452) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- benchmark: decimal builder and vec to decimal array [\#2450](https://github.com/apache/arrow-rs/pull/2450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Collocate Decimal Array Validation Logic [\#2446](https://github.com/apache/arrow-rs/pull/2446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Minor: Move From trait for Decimal256 impl to decimal.rs [\#2443](https://github.com/apache/arrow-rs/pull/2443) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- decimal benchmark: arrow reader decimal from parquet int32 and int64 [\#2438](https://github.com/apache/arrow-rs/pull/2438) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- MINOR: Simplify `split_second` function [\#2436](https://github.com/apache/arrow-rs/pull/2436) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add ParquetRecordBatchReaderBuilder \(\#2427\) [\#2435](https://github.com/apache/arrow-rs/pull/2435) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: refine validation for decimal128 array [\#2428](https://github.com/apache/arrow-rs/pull/2428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Benchmark of casting decimal arrays [\#2424](https://github.com/apache/arrow-rs/pull/2424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Test non-annotated repeated fields \(\#2394\) [\#2422](https://github.com/apache/arrow-rs/pull/2422) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix \#2416 Automatic version updates for github actions with dependabot [\#2417](https://github.com/apache/arrow-rs/pull/2417) ([iemejia](https://github.com/iemejia))
- Add validation logic for StructBuilder::finish [\#2413](https://github.com/apache/arrow-rs/pull/2413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- test: add test for reading decimal value from primitive array reader [\#2411](https://github.com/apache/arrow-rs/pull/2411) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Upgrade ahash to 0.8 [\#2410](https://github.com/apache/arrow-rs/pull/2410) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Support peek\_next\_page and skip\_next\_page in InMemoryPageReader [\#2407](https://github.com/apache/arrow-rs/pull/2407) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Fix DoPutUpdateResult [\#2404](https://github.com/apache/arrow-rs/pull/2404) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Implement Skip for DeltaBitPackDecoder [\#2393](https://github.com/apache/arrow-rs/pull/2393) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- fix: Don't instantiate the scalar composition code quadratically for dictionaries [\#2391](https://github.com/apache/arrow-rs/pull/2391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Marwes](https://github.com/Marwes))
- MINOR: Remove unused trait and some cleanup [\#2389](https://github.com/apache/arrow-rs/pull/2389) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Decouple parquet fuzz tests from converter \(\#1661\) [\#2386](https://github.com/apache/arrow-rs/pull/2386) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rewrite `Decimal` and `DecimalArray` using `const_generic` [\#2383](https://github.com/apache/arrow-rs/pull/2383) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Simplify BitReader \(~5-10% faster\) [\#2381](https://github.com/apache/arrow-rs/pull/2381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix parquet clippy lints \(\#1254\) [\#2377](https://github.com/apache/arrow-rs/pull/2377) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cast between `Decimal128` and `Decimal256` arrays [\#2376](https://github.com/apache/arrow-rs/pull/2376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- support compression for IPC with revamped feature flags [\#2369](https://github.com/apache/arrow-rs/pull/2369) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement AsyncFileReader for `Box<dyn AsyncFileReader>` [\#2368](https://github.com/apache/arrow-rs/pull/2368) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove get\_byte\_ranges where bound [\#2366](https://github.com/apache/arrow-rs/pull/2366) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: Make read\_num\_bytes a function instead of a macro [\#2364](https://github.com/apache/arrow-rs/pull/2364) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- refactor: Group metrics into page and column metrics structs [\#2363](https://github.com/apache/arrow-rs/pull/2363) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- Speed up `Decimal256` validation based on bytes comparison and add benchmark test [\#2360](https://github.com/apache/arrow-rs/pull/2360) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Combine multiple selections into the same batch size in skip\_records [\#2359](https://github.com/apache/arrow-rs/pull/2359) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add API to change timezone for timestamp array [\#2347](https://github.com/apache/arrow-rs/pull/2347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Clean the code in `field.rs` and add more tests [\#2345](https://github.com/apache/arrow-rs/pull/2345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add Parquet RowFilter API [\#2335](https://github.com/apache/arrow-rs/pull/2335) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make skip\_records in complex\_object\_array can skip cross row groups [\#2332](https://github.com/apache/arrow-rs/pull/2332) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Integrate Record Skipping into Column Reader Fuzz Test [\#2315](https://github.com/apache/arrow-rs/pull/2315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
