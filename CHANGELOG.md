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

## [50.0.0](https://github.com/apache/arrow-rs/tree/50.0.0) (2024-01-08)

[Full Changelog](https://github.com/apache/arrow-rs/compare/49.0.0...50.0.0)

**Breaking changes:**

- Make regexp\_match take scalar pattern and flag [\#5245](https://github.com/apache/arrow-rs/pull/5245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use Vec in ColumnReader \(\#5177\) [\#5193](https://github.com/apache/arrow-rs/pull/5193) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove SIMD Feature [\#5184](https://github.com/apache/arrow-rs/pull/5184) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Total Ordering for Aggregates and Refactor for Better Auto-Vectorization [\#5100](https://github.com/apache/arrow-rs/pull/5100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Allow the `zip` compute function to operator on `Scalar` values via `Datum` [\#5086](https://github.com/apache/arrow-rs/pull/5086) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Nathan-Fenner](https://github.com/Nathan-Fenner))
- Improve C Data Interface and Add Integration Testing Entrypoints [\#5080](https://github.com/apache/arrow-rs/pull/5080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pitrou](https://github.com/pitrou))
- Parquet: read/write f16 for Arrow [\#5003](https://github.com/apache/arrow-rs/pull/5003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))

**Implemented enhancements:**

- Support get offsets or blocks info from arrow file.  [\#5252](https://github.com/apache/arrow-rs/issues/5252) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make regexp\_match take scalar pattern and flag [\#5246](https://github.com/apache/arrow-rs/issues/5246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cannot access pen state website on arrow-row [\#5238](https://github.com/apache/arrow-rs/issues/5238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordBatch with\_schema's error message is hard to read [\#5227](https://github.com/apache/arrow-rs/issues/5227) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support cast between StructArray. [\#5219](https://github.com/apache/arrow-rs/issues/5219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove nightly-only simd feature and related code in ArrowNumericType [\#5185](https://github.com/apache/arrow-rs/issues/5185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use Vec instead of Slice in ColumnReader [\#5177](https://github.com/apache/arrow-rs/issues/5177) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Request to Memmap Arrow IPC files on disk [\#5153](https://github.com/apache/arrow-rs/issues/5153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- GenericColumnReader::read\_records Yields Truncated Records [\#5150](https://github.com/apache/arrow-rs/issues/5150) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nested Schema Projection [\#5148](https://github.com/apache/arrow-rs/issues/5148) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support specifying `quote` and `escape` in Csv `WriterBuilder` [\#5146](https://github.com/apache/arrow-rs/issues/5146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting of Float16 with other numeric types [\#5138](https://github.com/apache/arrow-rs/issues/5138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: read parquet metadata with page index in async and with size hints [\#5129](https://github.com/apache/arrow-rs/issues/5129) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Cast from floating/timestamp to timestamp/floating [\#5122](https://github.com/apache/arrow-rs/issues/5122) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Casting List To/From LargeList in Cast Kernel [\#5113](https://github.com/apache/arrow-rs/issues/5113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose a path for converting `bytes::Bytes` into `arrow_buffer::Buffer` without copy [\#5104](https://github.com/apache/arrow-rs/issues/5104) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- API inconsistency of ListBuilder make it hard to use as nested builder [\#5098](https://github.com/apache/arrow-rs/issues/5098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: don't truncate min/max statistics for float16 and decimal when writing file [\#5075](https://github.com/apache/arrow-rs/issues/5075) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: derive boundary order when writing columns [\#5074](https://github.com/apache/arrow-rs/issues/5074) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support new Arrow PyCapsule Interface for Python FFI [\#5067](https://github.com/apache/arrow-rs/issues/5067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `48.0.1 ` arrow patch release [\#5050](https://github.com/apache/arrow-rs/issues/5050) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Binary columns do not receive truncated statistics [\#5037](https://github.com/apache/arrow-rs/issues/5037) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Re-evaluate Explicit SIMD Aggregations [\#5032](https://github.com/apache/arrow-rs/issues/5032) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Min/Max Kernels Should Use Total Ordering [\#5031](https://github.com/apache/arrow-rs/issues/5031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow `zip` compute kernel to take `Scalar` / `Datum`  [\#5011](https://github.com/apache/arrow-rs/issues/5011) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Float16/Half-float logical type to Parquet [\#4986](https://github.com/apache/arrow-rs/issues/4986) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- feat: cast \(Large\)List to FixedSizeList [\#5081](https://github.com/apache/arrow-rs/pull/5081) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Update Parquet Encoding Documentation [\#5051](https://github.com/apache/arrow-rs/issues/5051) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- json schema inference can't handle null field turned into object field in subsequent rows [\#5215](https://github.com/apache/arrow-rs/issues/5215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Invalid trailing content after `Z` in timezone is ignored [\#5182](https://github.com/apache/arrow-rs/issues/5182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Take panics on a fixed size list array when given null indices [\#5169](https://github.com/apache/arrow-rs/issues/5169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- EnabledStatistics::Page  does not take effect on ByteArrayEncoder [\#5162](https://github.com/apache/arrow-rs/issues/5162) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: ColumnOrder not being written when writing parquet files [\#5152](https://github.com/apache/arrow-rs/issues/5152) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: Interval columns shouldn't write min/max stats [\#5145](https://github.com/apache/arrow-rs/issues/5145) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
-  cast `Utf8` to decimal failure [\#5127](https://github.com/apache/arrow-rs/issues/5127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- coerce\_primitive not honored when decoding from serde object [\#5095](https://github.com/apache/arrow-rs/issues/5095) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unsound MutableArrayData Constructor [\#5091](https://github.com/apache/arrow-rs/issues/5091) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RowGroupReader.get\_row\_iter\(\) fails with Path ColumnPath not found [\#5064](https://github.com/apache/arrow-rs/issues/5064) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- cast format 'yyyymmdd'  to Date32 give a error [\#5044](https://github.com/apache/arrow-rs/issues/5044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Performance improvements:**

- ArrowArrayStreamReader imports FFI\_ArrowSchema on each iteration [\#5103](https://github.com/apache/arrow-rs/issues/5103) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Working example of list\_flights with ObjectStore [\#5116](https://github.com/apache/arrow-rs/issues/5116)
- \(object\_store\) Error broken pipe on S3 multipart upload [\#5106](https://github.com/apache/arrow-rs/issues/5106)

**Merged pull requests:**

- Update parquet object\_store dependency to 0.9.0 [\#5290](https://github.com/apache/arrow-rs/pull/5290) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.75 to =1.0.76 [\#5289](https://github.com/apache/arrow-rs/pull/5289) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Enable JS tests again [\#5287](https://github.com/apache/arrow-rs/pull/5287) ([domoritz](https://github.com/domoritz))
- Update proc-macro2 requirement from =1.0.74 to =1.0.75 [\#5279](https://github.com/apache/arrow-rs/pull/5279) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.73 to =1.0.74 [\#5271](https://github.com/apache/arrow-rs/pull/5271) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.71 to =1.0.73 [\#5265](https://github.com/apache/arrow-rs/pull/5265) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update docs for datatypes [\#5260](https://github.com/apache/arrow-rs/pull/5260) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Don't suppress errors in ArrowArrayStreamReader [\#5256](https://github.com/apache/arrow-rs/pull/5256) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add IPC FileDecoder [\#5249](https://github.com/apache/arrow-rs/pull/5249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- optimize the next function of ArrowArrayStreamReader [\#5248](https://github.com/apache/arrow-rs/pull/5248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- ci: Fail Miri CI on first failure [\#5243](https://github.com/apache/arrow-rs/pull/5243) ([Jefffrey](https://github.com/Jefffrey))
- Remove 'unwrap' from Result [\#5241](https://github.com/apache/arrow-rs/pull/5241) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Update arrow-row docs URL [\#5239](https://github.com/apache/arrow-rs/pull/5239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([thomas-k-cameron](https://github.com/thomas-k-cameron))
- Improve regexp kernels performance by avoiding cloning Regex [\#5235](https://github.com/apache/arrow-rs/pull/5235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.70 to =1.0.71 [\#5231](https://github.com/apache/arrow-rs/pull/5231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Minor: Improve comments and errors for ArrowPredicate [\#5230](https://github.com/apache/arrow-rs/pull/5230) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Bump actions/upload-pages-artifact from 2 to 3 [\#5229](https://github.com/apache/arrow-rs/pull/5229) ([dependabot[bot]](https://github.com/apps/dependabot))
- make with\_schema's error more readable [\#5228](https://github.com/apache/arrow-rs/pull/5228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([shuoli84](https://github.com/shuoli84))
- Use `try_new` when casting between structs to propagate error [\#5226](https://github.com/apache/arrow-rs/pull/5226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat\(cast\): support cast between struct [\#5221](https://github.com/apache/arrow-rs/pull/5221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([my-vegetable-has-exploded](https://github.com/my-vegetable-has-exploded))
- Add `entries` to `MapBuilder` to return both key and value array builders [\#5218](https://github.com/apache/arrow-rs/pull/5218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix\(json\): fix inferring object after field was null [\#5216](https://github.com/apache/arrow-rs/pull/5216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kskalski](https://github.com/kskalski))
- Support MapBuilder in make\_builder [\#5210](https://github.com/apache/arrow-rs/pull/5210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- impl `From<OffsetBuffer<T>>` for `ScalarBuffer<T>` [\#5203](https://github.com/apache/arrow-rs/pull/5203) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- impl `From<BufferBuilder<T>>` for `Buffer` [\#5202](https://github.com/apache/arrow-rs/pull/5202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- impl `From<BufferBuilder<T>>` for `ScalarBuffer<T>` [\#5201](https://github.com/apache/arrow-rs/pull/5201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- feat: Support  quote and escape in Csv WriterBuilder [\#5196](https://github.com/apache/arrow-rs/pull/5196) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([my-vegetable-has-exploded](https://github.com/my-vegetable-has-exploded))
- chore: simplify cast\_string\_to\_interval [\#5195](https://github.com/apache/arrow-rs/pull/5195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Clarify interval comparison behavior with documentation and tests [\#5192](https://github.com/apache/arrow-rs/pull/5192) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `BooleanArray::into_parts` method [\#5191](https://github.com/apache/arrow-rs/pull/5191) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Fix deprecated note for `Buffer::from_raw_parts` [\#5190](https://github.com/apache/arrow-rs/pull/5190) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Fix: Ensure Timestamp Parsing Rejects Characters After 'Z [\#5189](https://github.com/apache/arrow-rs/pull/5189) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([razeghi71](https://github.com/razeghi71))
- Simplify parquet statistics generation [\#5183](https://github.com/apache/arrow-rs/pull/5183) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Parquet: Ensure page statistics are written only when conifgured from the Arrow Writer [\#5181](https://github.com/apache/arrow-rs/pull/5181) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- Blockwise IO in IPC FileReader \(\#5153\) [\#5179](https://github.com/apache/arrow-rs/pull/5179) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Replace ScalarBuffer in Parquet with Vec \(\#1849\) \(\#5177\) [\#5178](https://github.com/apache/arrow-rs/pull/5178) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Bump actions/setup-python from 4 to 5 [\#5175](https://github.com/apache/arrow-rs/pull/5175) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `LargeListBuilder` to `make_builder` [\#5171](https://github.com/apache/arrow-rs/pull/5171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix: ensure take\_fixed\_size\_list can handle null indices [\#5170](https://github.com/apache/arrow-rs/pull/5170) ([westonpace](https://github.com/westonpace))
- Removing redundant `as casts` in parquet [\#5168](https://github.com/apache/arrow-rs/pull/5168) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Bump actions/labeler from 4.3.0 to 5.0.0 [\#5167](https://github.com/apache/arrow-rs/pull/5167) ([dependabot[bot]](https://github.com/apps/dependabot))
- improve: make RunArray displayable [\#5166](https://github.com/apache/arrow-rs/pull/5166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yukkit](https://github.com/yukkit))
- ci: Add cargo audit CI action [\#5160](https://github.com/apache/arrow-rs/pull/5160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Parquet: write column\_orders in FileMetaData [\#5158](https://github.com/apache/arrow-rs/pull/5158) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Adding `is_null` datatype shortcut method [\#5157](https://github.com/apache/arrow-rs/pull/5157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Parquet: don't truncate f16/decimal min/max stats [\#5154](https://github.com/apache/arrow-rs/pull/5154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Support nested schema projection \(\#5148\) [\#5149](https://github.com/apache/arrow-rs/pull/5149) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Parquet: omit min/max for interval columns when writing stats [\#5147](https://github.com/apache/arrow-rs/pull/5147) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Deprecate Fields::remove and Schema::remove [\#5144](https://github.com/apache/arrow-rs/pull/5144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support casting of Float16 with other numeric types [\#5139](https://github.com/apache/arrow-rs/pull/5139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Parquet: Make `MetadataLoader` public [\#5137](https://github.com/apache/arrow-rs/pull/5137) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- Add FileReaderBuilder for arrow-ipc to allow reading large no. of column files [\#5136](https://github.com/apache/arrow-rs/pull/5136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Parquet: clear metadata and project fields of ParquetRecordBatchStream::schema [\#5135](https://github.com/apache/arrow-rs/pull/5135) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- JSON: write struct array nulls as null [\#5133](https://github.com/apache/arrow-rs/pull/5133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Update proc-macro2 requirement from =1.0.69 to =1.0.70 [\#5131](https://github.com/apache/arrow-rs/pull/5131) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix negative decimal string [\#5128](https://github.com/apache/arrow-rs/pull/5128) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cleanup list casting and support nested lists \(\#5113\) [\#5124](https://github.com/apache/arrow-rs/pull/5124) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cast from numeric/timestamp to timestamp/numeric [\#5123](https://github.com/apache/arrow-rs/pull/5123) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve cast docs [\#5114](https://github.com/apache/arrow-rs/pull/5114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.12.2 to =0.12.3 [\#5112](https://github.com/apache/arrow-rs/pull/5112) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Parquet: derive boundary order when writing [\#5110](https://github.com/apache/arrow-rs/pull/5110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Implementing `ArrayBuilder` for `Box<dyn ArrayBuilder>` [\#5109](https://github.com/apache/arrow-rs/pull/5109) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix 'ColumnPath not found' error reading Parquet files with nested REPEATED fields [\#5102](https://github.com/apache/arrow-rs/pull/5102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- fix: coerce\_primitive for serde decoded data [\#5101](https://github.com/apache/arrow-rs/pull/5101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fansehep](https://github.com/fansehep))
- Extend aggregation benchmarks [\#5096](https://github.com/apache/arrow-rs/pull/5096) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Expand parquet crate overview doc [\#5093](https://github.com/apache/arrow-rs/pull/5093) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- Ensure arrays passed to MutableArrayData have same type \(\#5091\) [\#5092](https://github.com/apache/arrow-rs/pull/5092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.12.1 to =0.12.2 [\#5088](https://github.com/apache/arrow-rs/pull/5088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add FFI from\_raw [\#5082](https://github.com/apache/arrow-rs/pull/5082) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- \[fix \#5044\] Support converting 'yyyymmdd' format to date [\#5078](https://github.com/apache/arrow-rs/pull/5078) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Tangruilin](https://github.com/Tangruilin))
- Enable truncation of binary statistics columns [\#5076](https://github.com/apache/arrow-rs/pull/5076) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([emcake](https://github.com/emcake))
- IPC writer truncated sliced list/map values [\#5071](https://github.com/apache/arrow-rs/pull/5071) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Implement Arrow PyCapsule Interface [\#5070](https://github.com/apache/arrow-rs/pull/5070) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Remove ByteBufferPtr and replace with Bytes [\#5055](https://github.com/apache/arrow-rs/pull/5055) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Support multiple GZip members in parquet page [\#4951](https://github.com/apache/arrow-rs/pull/4951) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
