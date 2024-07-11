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

## [52.1.0](https://github.com/apache/arrow-rs/tree/52.1.0) (2024-07-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/52.0.0...52.1.0)


**Implemented enhancements:**

- Implement `eq` comparison for StructArray [\#5960](https://github.com/apache/arrow-rs/issues/5960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- A new feature as a workaround hack to unavailable offset support in Arrow Java [\#5959](https://github.com/apache/arrow-rs/issues/5959) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `min_bytes` and `max_bytes` to `PageIndex` [\#5949](https://github.com/apache/arrow-rs/issues/5949) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error message in ArrowNativeTypeOp::neg\_checked doesn't include the operation [\#5944](https://github.com/apache/arrow-rs/issues/5944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add object\_store\_opendal as related projects [\#5925](https://github.com/apache/arrow-rs/issues/5925)
- Opaque retry errors make debugging difficult [\#5923](https://github.com/apache/arrow-rs/issues/5923)
- Implement arrow-row en/decoding for GenericByteView types [\#5921](https://github.com/apache/arrow-rs/issues/5921) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The arrow-rs repo is very large [\#5908](https://github.com/apache/arrow-rs/issues/5908)
- \[DISCUSS\] Release arrow-rs / parquet patch release `52.0.1` [\#5906](https://github.com/apache/arrow-rs/issues/5906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `compare_op` for `GenericBinaryView`  [\#5897](https://github.com/apache/arrow-rs/issues/5897) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- New null with view types are not supported [\#5893](https://github.com/apache/arrow-rs/issues/5893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cleanup ByteView construction [\#5878](https://github.com/apache/arrow-rs/issues/5878) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `cast` kernel support for `StringViewArray` and `BinaryViewArray` `\<--\> `DictionaryArray` [\#5861](https://github.com/apache/arrow-rs/issues/5861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet::ArrowWriter show allow writing Bloom filters before the end of the file [\#5859](https://github.com/apache/arrow-rs/issues/5859) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- API to get memory usage for parquet ArrowWriter [\#5851](https://github.com/apache/arrow-rs/issues/5851) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing `IntervalMonthDayNanoArray` to parquet via Arrow Writer  [\#5849](https://github.com/apache/arrow-rs/issues/5849) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Write parquet statistics for `IntervalDayTimeArray` , `IntervalMonthDayNanoArray` and `IntervalYearMonthArray` [\#5847](https://github.com/apache/arrow-rs/issues/5847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `RowSelection::from_consecutive_ranges` public [\#5846](https://github.com/apache/arrow-rs/issues/5846) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Schema::try_merge` should be able to merge List of any data type with List of Null data type [\#5843](https://github.com/apache/arrow-rs/issues/5843) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a way to move `fields` out of parquet `Row` [\#5841](https://github.com/apache/arrow-rs/issues/5841) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `TimeUnit` and `IntervalUnit` `Copy` [\#5839](https://github.com/apache/arrow-rs/issues/5839) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Limit Parquet Page Row Count By Default to reduce writer memory requirements with highly compressable columns [\#5797](https://github.com/apache/arrow-rs/issues/5797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Report / blog on parquet metadata sizes for "large" \(1000+\) numbers of columns [\#5770](https://github.com/apache/arrow-rs/issues/5770) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Structured ByteView Access \(underlying StringView/BinaryView representation\) [\#5736](https://github.com/apache/arrow-rs/issues/5736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\_derive\] support OPTIONAL \(def\_level = 1\) columns by default [\#5716](https://github.com/apache/arrow-rs/issues/5716)
- Maps cast to other Maps with different Elements, Key and Value Names [\#5702](https://github.com/apache/arrow-rs/issues/5702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Provide Arrow Schema Hint to Parquet Reader [\#5657](https://github.com/apache/arrow-rs/issues/5657) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Wrong error type in case of invalid amount in Interval components [\#5986](https://github.com/apache/arrow-rs/issues/5986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Empty and Null structarray fails to IPC roundtrip  [\#5920](https://github.com/apache/arrow-rs/issues/5920)
- FixedSizeList got out of range when the total length of the underlying values over i32::MAX [\#5901](https://github.com/apache/arrow-rs/issues/5901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Out of range when extending on a slice of string array imported through FFI [\#5896](https://github.com/apache/arrow-rs/issues/5896) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- cargo msrv test is failing on main for `object_store` [\#5864](https://github.com/apache/arrow-rs/issues/5864) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- chore: update RunArray reference in run\_iterator.rs [\#5892](https://github.com/apache/arrow-rs/pull/5892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Minor: Clarify when page index structures are read [\#5886](https://github.com/apache/arrow-rs/pull/5886) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve Parquet reader/writer properties docs [\#5863](https://github.com/apache/arrow-rs/pull/5863) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Refine documentation for `unary_mut` and `binary_mut` [\#5798](https://github.com/apache/arrow-rs/pull/5798) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Implement benchmarks for `compare_op` for `GenericBinaryView` [\#5903](https://github.com/apache/arrow-rs/issues/5903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- fix: error in case of invalid amount interval component [\#5987](https://github.com/apache/arrow-rs/pull/5987) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([DDtKey](https://github.com/DDtKey))
- Minor: fix clippy complaint in parquet\_derive [\#5984](https://github.com/apache/arrow-rs/pull/5984) ([alamb](https://github.com/alamb))
- Reduce repo size by removing accumulative commits in CI job [\#5982](https://github.com/apache/arrow-rs/pull/5982) ([Owen-CH-Leung](https://github.com/Owen-CH-Leung))
- Add operation in ArrowNativeTypeOp::neg\_check error message \(\#5944\) [\#5980](https://github.com/apache/arrow-rs/pull/5980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhao-gang](https://github.com/zhao-gang))
- Implement directly build byte view array on top of parquet buffer [\#5972](https://github.com/apache/arrow-rs/pull/5972) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Handle flight dictionary ID assignment automatically [\#5971](https://github.com/apache/arrow-rs/pull/5971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([thinkharderdev](https://github.com/thinkharderdev))
- Add view buffer for parquet reader [\#5970](https://github.com/apache/arrow-rs/pull/5970) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add benchmark for reading binary/binary view from parquet [\#5968](https://github.com/apache/arrow-rs/pull/5968) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- feat\(5851\): ArrowWriter memory usage [\#5967](https://github.com/apache/arrow-rs/pull/5967) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- Add ParquetMetadata::memory\_size size estimation [\#5965](https://github.com/apache/arrow-rs/pull/5965) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix FFI array offset handling [\#5964](https://github.com/apache/arrow-rs/pull/5964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement sort for String/BinaryViewArray [\#5963](https://github.com/apache/arrow-rs/pull/5963) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Improve error message for unsupported nested comparison [\#5961](https://github.com/apache/arrow-rs/pull/5961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- chore\(5797\): change default parquet data\_page\_row\_limit to 20k [\#5957](https://github.com/apache/arrow-rs/pull/5957) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- Document process for PRs with breaking changes [\#5953](https://github.com/apache/arrow-rs/pull/5953) ([alamb](https://github.com/alamb))
- Minor: fixup contribution guide about clippy [\#5952](https://github.com/apache/arrow-rs/pull/5952) ([alamb](https://github.com/alamb))
- feat: add max\_bytes and min\_bytes on PageIndex [\#5950](https://github.com/apache/arrow-rs/pull/5950) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tshauck](https://github.com/tshauck))
- test: Add unit test for extending slice of list array [\#5948](https://github.com/apache/arrow-rs/pull/5948) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- minor: row format benches for bool & nullable int [\#5943](https://github.com/apache/arrow-rs/pull/5943) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([korowa](https://github.com/korowa))
- Better document support for nested comparison [\#5942](https://github.com/apache/arrow-rs/pull/5942) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Provide Arrow Schema Hint to Parquet Reader - Alternative 2 [\#5939](https://github.com/apache/arrow-rs/pull/5939) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([efredine](https://github.com/efredine))
- `like` benchmark for StringView [\#5936](https://github.com/apache/arrow-rs/pull/5936) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix typo in benchmark name `egexp` --\> `regexp` [\#5935](https://github.com/apache/arrow-rs/pull/5935) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Revert "Write Bloom filters between row groups instead of the end " [\#5932](https://github.com/apache/arrow-rs/pull/5932) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Implement like/ilike etc for StringViewArray [\#5931](https://github.com/apache/arrow-rs/pull/5931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- docs: Fix broken links of object\_store\_opendal README [\#5929](https://github.com/apache/arrow-rs/pull/5929) ([Xuanwo](https://github.com/Xuanwo))
- Expose `IntervalMonthDayNano` and `IntervalDayTime` and update docs [\#5928](https://github.com/apache/arrow-rs/pull/5928) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update proc-macro2 requirement from =1.0.85 to =1.0.86 [\#5927](https://github.com/apache/arrow-rs/pull/5927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- docs: Add object\_store\_opendal as related projects [\#5926](https://github.com/apache/arrow-rs/pull/5926) ([Xuanwo](https://github.com/Xuanwo))
- Add eq benchmark for StringArray/StringViewArray [\#5924](https://github.com/apache/arrow-rs/pull/5924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Implement arrow-row encoding/decoding for view types [\#5922](https://github.com/apache/arrow-rs/pull/5922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- fix\(ipc\): set correct row count when reading struct arrays with zero fields [\#5918](https://github.com/apache/arrow-rs/pull/5918) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Update zstd-sys requirement from \>=2.0.0, \<2.0.10 to \>=2.0.0, \<2.0.12 [\#5913](https://github.com/apache/arrow-rs/pull/5913) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: prevent potential out-of-range access in FixedSizeListArray [\#5902](https://github.com/apache/arrow-rs/pull/5902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([BubbleCal](https://github.com/BubbleCal))
- Implement compare operations for view types [\#5900](https://github.com/apache/arrow-rs/pull/5900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- minor: use as\_primitive replace downcast\_ref [\#5898](https://github.com/apache/arrow-rs/pull/5898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- fix: Adjust FFI\_ArrowArray offset based on the offset of offset buffer [\#5895](https://github.com/apache/arrow-rs/pull/5895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- implement `new_null_array` for view types [\#5894](https://github.com/apache/arrow-rs/pull/5894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- chore: add view type single column tests [\#5891](https://github.com/apache/arrow-rs/pull/5891) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ariesdevil](https://github.com/ariesdevil))
- Minor: expose timestamp\_tz\_format for csv writing [\#5890](https://github.com/apache/arrow-rs/pull/5890) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tmi](https://github.com/tmi))
- chore: implement parquet error handling for object\_store [\#5889](https://github.com/apache/arrow-rs/pull/5889) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([abhiaagarwal](https://github.com/abhiaagarwal))
- Document when the ParquetRecordBatchReader will re-read metadata [\#5887](https://github.com/apache/arrow-rs/pull/5887) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add simple GC for view array types [\#5885](https://github.com/apache/arrow-rs/pull/5885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Update for new clippy rules [\#5881](https://github.com/apache/arrow-rs/pull/5881) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- clean up ByteView construction [\#5879](https://github.com/apache/arrow-rs/pull/5879) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Avoid copy/allocation when read view types from parquet [\#5877](https://github.com/apache/arrow-rs/pull/5877) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Document parquet ArrowWriter type limitations [\#5875](https://github.com/apache/arrow-rs/pull/5875) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Benchmark for casting view to dict arrays \(and the reverse\) [\#5874](https://github.com/apache/arrow-rs/pull/5874) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Implement Take for Dense UnionArray [\#5873](https://github.com/apache/arrow-rs/pull/5873) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Improve performance of casting `StringView`/`BinaryView` to `DictionaryArray` [\#5872](https://github.com/apache/arrow-rs/pull/5872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Improve performance of casting `DictionaryArray` to `StringViewArray` [\#5871](https://github.com/apache/arrow-rs/pull/5871) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- fix: msrv CI for object\_store [\#5866](https://github.com/apache/arrow-rs/pull/5866) ([korowa](https://github.com/korowa))
- parquet: Fix warning about unused import [\#5865](https://github.com/apache/arrow-rs/pull/5865) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- Preallocate for `FixedSizeList` in `concat` [\#5862](https://github.com/apache/arrow-rs/pull/5862) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([judahrand](https://github.com/judahrand))
- Faster primitive arrays encoding into row format [\#5858](https://github.com/apache/arrow-rs/pull/5858) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([korowa](https://github.com/korowa))
- Added panic message to docs. [\#5857](https://github.com/apache/arrow-rs/pull/5857) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SeeRightThroughMe](https://github.com/SeeRightThroughMe))
- feat: call try\_merge recursively for list field  [\#5852](https://github.com/apache/arrow-rs/pull/5852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mnpw](https://github.com/mnpw))
- Minor: refine row selection example more [\#5850](https://github.com/apache/arrow-rs/pull/5850) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Make RowSelection's from\_consecutive\_ranges public [\#5848](https://github.com/apache/arrow-rs/pull/5848) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([advancedxy](https://github.com/advancedxy))
- Add exposing fields from parquet row [\#5842](https://github.com/apache/arrow-rs/pull/5842) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SHaaD94](https://github.com/SHaaD94))
- Derive `Copy` for `TimeUnit` and `IntervalUnit` [\#5840](https://github.com/apache/arrow-rs/pull/5840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- feat: support reading OPTIONAL column in parquet\_derive [\#5717](https://github.com/apache/arrow-rs/pull/5717) ([double-free](https://github.com/double-free))
- Add the ability for Maps to cast to another case where the field names are different [\#5703](https://github.com/apache/arrow-rs/pull/5703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HawaiianSpork](https://github.com/HawaiianSpork))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
