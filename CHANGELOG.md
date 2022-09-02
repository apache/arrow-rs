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

## [22.0.0](https://github.com/apache/arrow-rs/tree/22.0.0) (2022-09-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/21.0.0...22.0.0)

**Breaking changes:**

- Use `total_cmp` for floating value ordering and remove `nan_ordering` feature flag [\#2614](https://github.com/apache/arrow-rs/pull/2614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Gate dyn comparison of dictionary arrays behind `dyn_cmp_dict` [\#2597](https://github.com/apache/arrow-rs/pull/2597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move JsonSerializable to json module \(\#2300\) [\#2595](https://github.com/apache/arrow-rs/pull/2595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Decimal precision scale datatype change [\#2532](https://github.com/apache/arrow-rs/pull/2532) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor PrimitiveBuilder Constructors [\#2518](https://github.com/apache/arrow-rs/pull/2518) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactoring DecimalBuilder constructors [\#2517](https://github.com/apache/arrow-rs/pull/2517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor FixedSizeBinaryBuilder Constructors [\#2516](https://github.com/apache/arrow-rs/pull/2516) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor BooleanBuilder Constructors [\#2515](https://github.com/apache/arrow-rs/pull/2515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor UnionBuilder Constructors [\#2488](https://github.com/apache/arrow-rs/pull/2488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))

**Implemented enhancements:**

- Support comparison between DictionaryArray and BooleanArray [\#2617](https://github.com/apache/arrow-rs/issues/2617) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `total_cmp` for floating value ordering and remove `nan_ordering` feature flag [\#2613](https://github.com/apache/arrow-rs/issues/2613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support empty projection in CSV, JSON readers [\#2603](https://github.com/apache/arrow-rs/issues/2603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support SQL-compliant NaN ordering between for DictionaryArray and non-DictionaryArray [\#2599](https://github.com/apache/arrow-rs/issues/2599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `dyn_cmp_dict` feature flag to gate dyn comparison of dictionary arrays [\#2596](https://github.com/apache/arrow-rs/issues/2596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add max\_dyn and min\_dyn for max/min for dictionary array [\#2584](https://github.com/apache/arrow-rs/issues/2584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow FlightSQL implementers to extend `do_get()` [\#2581](https://github.com/apache/arrow-rs/issues/2581) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support SQL-compliant behavior on `eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#2569](https://github.com/apache/arrow-rs/issues/2569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add sql-compliant feature for enabling sql-compliant kernel behavior [\#2568](https://github.com/apache/arrow-rs/issues/2568)
- Calculate `sum` for dictionary array [\#2565](https://github.com/apache/arrow-rs/issues/2565) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add test for float nan comparison [\#2556](https://github.com/apache/arrow-rs/issues/2556) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with string array [\#2548](https://github.com/apache/arrow-rs/issues/2548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with primitive array in `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#2538](https://github.com/apache/arrow-rs/issues/2538) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with primitive array in `eq_dyn` and `neq_dyn` [\#2535](https://github.com/apache/arrow-rs/issues/2535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- UnionBuilder Create Children With Capacity [\#2523](https://github.com/apache/arrow-rs/issues/2523) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up `like_utf8_scalar` for `%pat%` [\#2519](https://github.com/apache/arrow-rs/issues/2519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace macro with TypedDictionaryArray in comparison kernels [\#2513](https://github.com/apache/arrow-rs/issues/2513) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use same codebase for boolean kernels [\#2507](https://github.com/apache/arrow-rs/issues/2507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use u8 for Decimal Precision and Scale [\#2496](https://github.com/apache/arrow-rs/issues/2496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Integrate skip row without pageIndex in SerializedPageReader in Fuzz Test [\#2475](https://github.com/apache/arrow-rs/issues/2475) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Avoid unecessary copies in Arrow IPC reader [\#2437](https://github.com/apache/arrow-rs/issues/2437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add GenericColumnReader::skip\_records Missing OffsetIndex Fallback [\#2433](https://github.com/apache/arrow-rs/issues/2433) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Reading PageIndex with ParquetRecordBatchStream [\#2430](https://github.com/apache/arrow-rs/issues/2430) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Specialize FixedLenByteArrayReader for Parquet [\#2318](https://github.com/apache/arrow-rs/issues/2318) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make JSON support Optional via Feature Flag [\#2300](https://github.com/apache/arrow-rs/issues/2300) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Casting timestamp array to string should not ignore timezone [\#2607](https://github.com/apache/arrow-rs/issues/2607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Ilike\_ut8\_scalar kernals have incorrect logic [\#2544](https://github.com/apache/arrow-rs/issues/2544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Always validate the array data when creating array in IPC reader [\#2541](https://github.com/apache/arrow-rs/issues/2541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Int96Converter Truncates Timestamps [\#2480](https://github.com/apache/arrow-rs/issues/2480) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error Reading Page Index When Not Available  [\#2434](https://github.com/apache/arrow-rs/issues/2434) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ParquetFileArrowReader::get_record_reader[_by_colum]` `batch_size` overallocates [\#2321](https://github.com/apache/arrow-rs/issues/2321) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- JsonEqual for DictionaryArray Only Compares Keys [\#2294](https://github.com/apache/arrow-rs/issues/2294) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add support for CAST from `Interval(DayTime)` to `Timestamp(Nanosecond, None)` [\#2606](https://github.com/apache/arrow-rs/issues/2606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Why do we check for null in TypedDictionaryArray value function [\#2564](https://github.com/apache/arrow-rs/issues/2564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add the `length` field for `Buffer` [\#2524](https://github.com/apache/arrow-rs/issues/2524) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Avoid large over allocate buffer in async reader [\#2512](https://github.com/apache/arrow-rs/issues/2512) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rewriting Decimal Builders using `const_generic`. [\#2390](https://github.com/apache/arrow-rs/issues/2390) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rewrite Decimal Array using `const_generic` [\#2384](https://github.com/apache/arrow-rs/issues/2384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Document dyn\_cmp\_dict [\#2624](https://github.com/apache/arrow-rs/pull/2624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support comparison between DictionaryArray and BooleanArray [\#2618](https://github.com/apache/arrow-rs/pull/2618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cast timestamp array to string array with timezone [\#2608](https://github.com/apache/arrow-rs/pull/2608) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support empty projection in CSV and JSON readers [\#2604](https://github.com/apache/arrow-rs/pull/2604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make JSON support optional via a feature flag \(\#2300\) [\#2601](https://github.com/apache/arrow-rs/pull/2601) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support SQL-compliant NaN ordering for DictionaryArray and non-DictionaryArray [\#2600](https://github.com/apache/arrow-rs/pull/2600) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split out integration test plumbing \(\#2594\) \(\#2300\) [\#2598](https://github.com/apache/arrow-rs/pull/2598) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Refactor Binary Builder and String Builder Constructors [\#2592](https://github.com/apache/arrow-rs/pull/2592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Dictionary like scalar kernels [\#2591](https://github.com/apache/arrow-rs/pull/2591) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Validate dictionary key in TypedDictionaryArray \(\#2578\) [\#2589](https://github.com/apache/arrow-rs/pull/2589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add max\_dyn and min\_dyn for max/min for dictionary array [\#2585](https://github.com/apache/arrow-rs/pull/2585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Code cleanup of array value functions [\#2583](https://github.com/apache/arrow-rs/pull/2583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Allow overriding of do\_get & export useful macro [\#2582](https://github.com/apache/arrow-rs/pull/2582) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- MINOR: Upgrade to pyo3 0.17 [\#2576](https://github.com/apache/arrow-rs/pull/2576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Support SQL-compliant NaN behavior on eq\_dyn, neq\_dyn, lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn [\#2570](https://github.com/apache/arrow-rs/pull/2570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add sum\_dyn to calculate sum for dictionary array [\#2566](https://github.com/apache/arrow-rs/pull/2566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- struct UnionBuilder will create child buffers with capacity [\#2560](https://github.com/apache/arrow-rs/pull/2560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kastolars](https://github.com/kastolars))
- Don't panic on RleValueEncoder::flush\_buffer if empty \(\#2558\) [\#2559](https://github.com/apache/arrow-rs/pull/2559) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add the `length` field for Buffer and use more `Buffer` in IPC reader to avoid memory copy. [\#2557](https://github.com/apache/arrow-rs/pull/2557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([HaoYang670](https://github.com/HaoYang670))
- Add test for float nan comparison [\#2555](https://github.com/apache/arrow-rs/pull/2555) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Compare dictionary array with string array [\#2549](https://github.com/apache/arrow-rs/pull/2549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Always validate the array data \(except the `Decimal`\) when creating array in IPC reader [\#2547](https://github.com/apache/arrow-rs/pull/2547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- MINOR: Fix test\_row\_type\_validation test [\#2546](https://github.com/apache/arrow-rs/pull/2546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix ilike\_utf8\_scalar kernals [\#2545](https://github.com/apache/arrow-rs/pull/2545) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- fix typo [\#2540](https://github.com/apache/arrow-rs/pull/2540) ([00Masato](https://github.com/00Masato))
- Compare dictionary array and primitive array in lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn kernels [\#2539](https://github.com/apache/arrow-rs/pull/2539) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- \[MINOR\]Avoid large over allocate buffer in async reader [\#2537](https://github.com/apache/arrow-rs/pull/2537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Compare dictionary with primitive array in `eq_dyn` and `neq_dyn` [\#2533](https://github.com/apache/arrow-rs/pull/2533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add iterator for FixedSizeBinaryArray [\#2531](https://github.com/apache/arrow-rs/pull/2531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- add bench: decimal with byte array and fixed length byte array [\#2529](https://github.com/apache/arrow-rs/pull/2529) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Add FixedLengthByteArrayReader Remove ComplexObjectArrayReader [\#2528](https://github.com/apache/arrow-rs/pull/2528) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out byte array decoders \(\#2318\) [\#2527](https://github.com/apache/arrow-rs/pull/2527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use offset index in ParquetRecordBatchStream [\#2526](https://github.com/apache/arrow-rs/pull/2526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Clean the `create_array` in IPC reader. [\#2525](https://github.com/apache/arrow-rs/pull/2525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove DecimalByteArrayConvert \(\#2480\) [\#2522](https://github.com/apache/arrow-rs/pull/2522) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Improve performance of `%pat%` \(\>3x speedup\) [\#2521](https://github.com/apache/arrow-rs/pull/2521) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- remove len field from MapBuilder [\#2520](https://github.com/apache/arrow-rs/pull/2520) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
-  Replace macro with TypedDictionaryArray in comparison kernels [\#2514](https://github.com/apache/arrow-rs/pull/2514) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Avoid large over allocate buffer in sync reader [\#2511](https://github.com/apache/arrow-rs/pull/2511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Avoid useless memory copies in IPC reader. [\#2510](https://github.com/apache/arrow-rs/pull/2510) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor boolean kernels to use same codebase [\#2508](https://github.com/apache/arrow-rs/pull/2508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove Int96Converter \(\#2480\) [\#2481](https://github.com/apache/arrow-rs/pull/2481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
