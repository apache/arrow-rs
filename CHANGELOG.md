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

## [58.0.0](https://github.com/apache/arrow-rs/tree/58.0.0) (2026-02-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.3.0...58.0.0)

**Breaking changes:**

- Remove support for List types in bit\_length kernel [\#9350](https://github.com/apache/arrow-rs/pull/9350) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- Optimize `from_bitwise_unary_op` [\#9297](https://github.com/apache/arrow-rs/pull/9297) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Mark `BufferBuilder::new_from_buffer` as unsafe [\#9292](https://github.com/apache/arrow-rs/pull/9292) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- \[Variant\] Support `['fieldName']` in VariantPath parser [\#9276](https://github.com/apache/arrow-rs/pull/9276) ([klion26](https://github.com/klion26))
- Remove parquet arrow\_cast dependency [\#9077](https://github.com/apache/arrow-rs/pull/9077) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: change default behavior for Parquet `PageEncodingStats` to bitmask [\#9051](https://github.com/apache/arrow-rs/pull/9051) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([WaterWhisperer](https://github.com/WaterWhisperer))
- \[arrow\] Minimize allocation in GenericViewArray::slice\(\) [\#9016](https://github.com/apache/arrow-rs/pull/9016) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))

**Implemented enhancements:**

- Avoid allocating a `Vec` in `StructBuilder` [\#9427](https://github.com/apache/arrow-rs/issues/9427)
- Zstd context reuse [\#9401](https://github.com/apache/arrow-rs/issues/9401)
- Optimize `from_bitwise_unary_op` [\#9364](https://github.com/apache/arrow-rs/issues/9364)
- Support `RunEndEncoded` in ord comparator [\#9360](https://github.com/apache/arrow-rs/issues/9360)
- Support `RunEndEncoded` arrays in `arrow-json` [\#9359](https://github.com/apache/arrow-rs/issues/9359)
- Support `BinaryView` in `bit_length` kernel [\#9351](https://github.com/apache/arrow-rs/issues/9351)
- Remove support for `List` types in `bit_length` kernel [\#9349](https://github.com/apache/arrow-rs/issues/9349)
- Support roundtrip `ListView` in parquet arrow writer [\#9344](https://github.com/apache/arrow-rs/issues/9344)
- Support `ListView` in `length` kernel [\#9343](https://github.com/apache/arrow-rs/issues/9343)
- Support `ListView` in sort kernel [\#9341](https://github.com/apache/arrow-rs/issues/9341)
- Add some way to create a Timestamp from a `DateTime` [\#9337](https://github.com/apache/arrow-rs/issues/9337)
- Introduce `DataType::is_list` and `DataType::IsBinary` [\#9326](https://github.com/apache/arrow-rs/issues/9326)
- Performance of creating all null dictionary array can be improved [\#9321](https://github.com/apache/arrow-rs/issues/9321)
- \[arrow-avro\] Add missing Arrow DataType support with `avro_custom_types` round-trip + non-custom fallbacks [\#9290](https://github.com/apache/arrow-rs/issues/9290)

**Fixed bugs:**

- ArrowArrayStreamReader errors on zero-column record batches [\#9394](https://github.com/apache/arrow-rs/issues/9394)
- Regression on main \(58\): Parquet argument error: Parquet error: Required field type\_ is missing [\#9315](https://github.com/apache/arrow-rs/issues/9315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Improve safety documentation of the `Array` trait [\#9314](https://github.com/apache/arrow-rs/pull/9314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve docs and add build\(\) method to `{Null,Boolean,}BufferBuilder` [\#9155](https://github.com/apache/arrow-rs/pull/9155) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `ArrowReaderBuilder::with_row_filter` documentation [\#9153](https://github.com/apache/arrow-rs/pull/9153) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- docs: Improve main README.md and highlight community [\#9119](https://github.com/apache/arrow-rs/pull/9119) ([alamb](https://github.com/alamb))
- Docs: Add additional documentation and example for  `make_array` [\#9112](https://github.com/apache/arrow-rs/pull/9112) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- doc: fix link on FixedSizeListArray doc [\#9033](https://github.com/apache/arrow-rs/pull/9033) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))

**Performance improvements:**

- Replace `ArrayData` with direct Array construction [\#9338](https://github.com/apache/arrow-rs/pull/9338) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Remove some `unsafe` and allocations when creating PrimitiveArrays from Vec and `from_trusted_len_iter` [\#9299](https://github.com/apache/arrow-rs/pull/9299) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- parquet: rle skip decode loop when batch contains all max levels \(aka no nulls\) [\#9258](https://github.com/apache/arrow-rs/pull/9258) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lyang24](https://github.com/lyang24))
- Improve parquet BinaryView / StringView decoder performance \(up to -35%\) [\#9236](https://github.com/apache/arrow-rs/pull/9236) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Avoid a clone when creating `BooleanArray` from ArrayData [\#9159](https://github.com/apache/arrow-rs/pull/9159) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid overallocating arrays in coalesce primitives / views [\#9132](https://github.com/apache/arrow-rs/pull/9132) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- perf: Avoid ArrayData allocation in PrimitiveArray::reinterpret\_cast [\#9129](https://github.com/apache/arrow-rs/pull/9129) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Parquet\] perf: Create StructArrays directly rather than via `ArrayData` \(1% improvement\) [\#9120](https://github.com/apache/arrow-rs/pull/9120) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid clones in `make_array` for `StructArray` and `GenericByteViewArray` [\#9114](https://github.com/apache/arrow-rs/pull/9114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- perf: optimize hex decoding in json \(1.8x faster in binary-heavy\) [\#9091](https://github.com/apache/arrow-rs/pull/9091) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Speed up binary kernels \(30% faster `and` and `or`\), add `BooleanBuffer::from_bitwise_binary_op` [\#9090](https://github.com/apache/arrow-rs/pull/9090) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- perf: improve field indexing in JSON StructArrayDecoder \(1.7x speed up\) [\#9086](https://github.com/apache/arrow-rs/pull/9086) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- bench: added to row\_format benchmark conversion of 53 non-nested columns [\#9081](https://github.com/apache/arrow-rs/pull/9081) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: improve calculating length performance for view byte array in row conversion [\#9080](https://github.com/apache/arrow-rs/pull/9080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: improve calculating length performance for nested arrays in row conversion [\#9079](https://github.com/apache/arrow-rs/pull/9079) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: improve calculating length performance for `GenericByteArray` in row conversion [\#9078](https://github.com/apache/arrow-rs/pull/9078) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))

**Closed issues:**

- BatchCoalescer::push\_batch panics on schema mismatch instead of returning error [\#9389](https://github.com/apache/arrow-rs/issues/9389)
- Release arrow-rs / parquet Minor version `57.3.0` \(January 2026\) [\#9240](https://github.com/apache/arrow-rs/issues/9240)
- \[Variant\] support `..` and `['fieldName']` syntax in the VariantPath parser [\#9050](https://github.com/apache/arrow-rs/issues/9050)
- Support Float16 for create\_random\_array [\#9028](https://github.com/apache/arrow-rs/issues/9028)

**Merged pull requests:**

- Avoid allocating a `Vec` in `StructBuilder` [\#9428](https://github.com/apache/arrow-rs/pull/9428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Fokko](https://github.com/Fokko))
- fix: fixed trait functions clash get\_date\_time\_part\_extract\_fn \(\#8221\) [\#9424](https://github.com/apache/arrow-rs/pull/9424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([esavier](https://github.com/esavier))
- \[Minor\] Use per-predicate projection masks in arrow\_reader\_clickbench benchmark [\#9413](https://github.com/apache/arrow-rs/pull/9413) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Fix `ArrowArrayStreamReader` for 0-columns record batch streams [\#9405](https://github.com/apache/arrow-rs/pull/9405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonded94](https://github.com/jonded94))
- Use zstd::bulk API in IPC and Parquet with context reuse for compression and decompression [\#9400](https://github.com/apache/arrow-rs/pull/9400) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Reproduce the issue of \#9370 in a minimal, end-to-end way [\#9399](https://github.com/apache/arrow-rs/pull/9399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jonded94](https://github.com/jonded94))
- perf: optimize skipper for varint values used when projecting Avro record types [\#9397](https://github.com/apache/arrow-rs/pull/9397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- fix: return error instead of panic on schema mismatch in BatchCoalescer::push\_batch [\#9390](https://github.com/apache/arrow-rs/pull/9390) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bvolpato-dd](https://github.com/bvolpato-dd))
- Minor: Add additional test coverage for WriterProperties::{max\_row\_group\_row\_count,max\_row\_group\_size} [\#9387](https://github.com/apache/arrow-rs/pull/9387) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Moving invalid\_utf8 tests into a separate mod [\#9384](https://github.com/apache/arrow-rs/pull/9384) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdf-jkl](https://github.com/sdf-jkl))
- Update sysinfo requirement from 0.37.1 to 0.38.1 [\#9383](https://github.com/apache/arrow-rs/pull/9383) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: support RunEndEncoded arrays in arrow-json reader and writer [\#9379](https://github.com/apache/arrow-rs/pull/9379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Abhisheklearn12](https://github.com/Abhisheklearn12))
- Remove lint issues in parquet-related code. [\#9375](https://github.com/apache/arrow-rs/pull/9375) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([brunal](https://github.com/brunal))
- Add RunEndEncoded array comparator [\#9368](https://github.com/apache/arrow-rs/pull/9368) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- feat: support BinaryView in bit\_length kernel [\#9363](https://github.com/apache/arrow-rs/pull/9363) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Abhisheklearn12](https://github.com/Abhisheklearn12))
- Add regression tests for Parquet large binary offset overflow [\#9361](https://github.com/apache/arrow-rs/pull/9361) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([vigneshsiva11](https://github.com/vigneshsiva11))
- feat: add max\_row\_group\_bytes option to WriterProperties [\#9357](https://github.com/apache/arrow-rs/pull/9357) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yonipeleg33](https://github.com/yonipeleg33))
- doc: remove disclaimer about `ListView` not being fully supported [\#9356](https://github.com/apache/arrow-rs/pull/9356) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Move row\_filter async tests from parquet async reader [\#9355](https://github.com/apache/arrow-rs/pull/9355) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdf-jkl](https://github.com/sdf-jkl))
- \[Parquet\] Allow setting page size per column [\#9353](https://github.com/apache/arrow-rs/pull/9353) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- feat: Support roundtrip ListView in parquet arrow writer [\#9352](https://github.com/apache/arrow-rs/pull/9352) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([codephage2020](https://github.com/codephage2020))
- feat: add ListView and LargeListView support to arrow-ord [\#9347](https://github.com/apache/arrow-rs/pull/9347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- Support ListView in length kernel [\#9346](https://github.com/apache/arrow-rs/pull/9346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vegarsti](https://github.com/vegarsti))
- feat: Add from\_datetime method to Timestamp types [\#9345](https://github.com/apache/arrow-rs/pull/9345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- \[main\] Update version to 57.3.0, add changelog [\#9334](https://github.com/apache/arrow-rs/pull/9334) ([alamb](https://github.com/alamb))
- build\(deps\): update pyo3 requirement from 0.27.1 to 0.28.0 [\#9331](https://github.com/apache/arrow-rs/pull/9331) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `DataType::is_list` and `DataType::is_binary` [\#9327](https://github.com/apache/arrow-rs/pull/9327) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- Fix string array equality when the values buffer is the same and only the offsets to access it differ [\#9325](https://github.com/apache/arrow-rs/pull/9325) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
-  perf: skip validation of dictionary keys if all null [\#9322](https://github.com/apache/arrow-rs/pull/9322) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- parquet: use rwlock instead of mutex in predicate cache [\#9319](https://github.com/apache/arrow-rs/pull/9319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lyang24](https://github.com/lyang24))
- nit: remove usused code [\#9318](https://github.com/apache/arrow-rs/pull/9318) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lyang24](https://github.com/lyang24))
- Remove unnecessary Arc\<ArrayRef\> [\#9316](https://github.com/apache/arrow-rs/pull/9316) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- Optimize data page statistics conversion \(up to 4x\) [\#9303](https://github.com/apache/arrow-rs/pull/9303) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- \[regression\] Error with adaptive predicate pushdown: "Invalid offset in sparse column chunk data: 754, no matching page found." [\#9301](https://github.com/apache/arrow-rs/pull/9301) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdf-jkl](https://github.com/sdf-jkl))
- Improve `PrimitiveArray::from_iter` perf [\#9294](https://github.com/apache/arrow-rs/pull/9294) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Add additional Arrow type support  [\#9291](https://github.com/apache/arrow-rs/pull/9291) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- fix: ensure `BufferBuilder::truncate` doesn't overset length [\#9288](https://github.com/apache/arrow-rs/pull/9288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add benchmark for row group index reader perf [\#9285](https://github.com/apache/arrow-rs/pull/9285) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- fix union array row converter to handle non-sequential type ids [\#9283](https://github.com/apache/arrow-rs/pull/9283) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- parquet: reduce clone in delta byte array decoder [\#9282](https://github.com/apache/arrow-rs/pull/9282) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lyang24](https://github.com/lyang24))
- fix: fix \[\[NULL\]\] array doesn't roundtrip in arrow-row bug [\#9275](https://github.com/apache/arrow-rs/pull/9275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lichuang](https://github.com/lichuang))
- Enhance list casting, adding more cases for list views [\#9274](https://github.com/apache/arrow-rs/pull/9274) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- \[Variant\] Add path index access tests for list [\#9273](https://github.com/apache/arrow-rs/pull/9273) ([liamzwbao](https://github.com/liamzwbao))
- Factor out json reader's static make\_decoder args to a struct [\#9271](https://github.com/apache/arrow-rs/pull/9271) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- make\_decoder accepts borrowed DataType instead of owned [\#9270](https://github.com/apache/arrow-rs/pull/9270) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- Implement a more generic from\_nested\_iter method for list arrays [\#9268](https://github.com/apache/arrow-rs/pull/9268) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Move extension type construction logic out of Field [\#9266](https://github.com/apache/arrow-rs/pull/9266) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- fix: support casting string to f16 [\#9262](https://github.com/apache/arrow-rs/pull/9262) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add additional coverage for StringViewArray comparisons [\#9257](https://github.com/apache/arrow-rs/pull/9257) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Upgrade to object store 0.13.1 [\#9256](https://github.com/apache/arrow-rs/pull/9256) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- \[Parquet\] test adaptive predicate pushdown with skipped page [\#9251](https://github.com/apache/arrow-rs/pull/9251) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdf-jkl](https://github.com/sdf-jkl))
- Speed up string view comparison \(up to 3x\) [\#9250](https://github.com/apache/arrow-rs/pull/9250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Add bench for LocalFileSystem [\#9248](https://github.com/apache/arrow-rs/pull/9248) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- \[Parquet\] Add test for reading/writing long UTF8 StringViews [\#9246](https://github.com/apache/arrow-rs/pull/9246) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Parquet\] test adaptive predicate pushdown with skipped page [\#9243](https://github.com/apache/arrow-rs/pull/9243) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([erratic-pattern](https://github.com/erratic-pattern))
- Add tests and fixes for schema resolution bug [\#9237](https://github.com/apache/arrow-rs/pull/9237) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Revert "Seal Array trait \(\#9092\)", mark `Array` as `unsafe` [\#9234](https://github.com/apache/arrow-rs/pull/9234) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gabotechs](https://github.com/gabotechs))
- Speedup filter \(up to ~1.5x\) `FilterBuilder::Optimize`/`BitIndexIterator`/`iter_set_bits_rev` [\#9229](https://github.com/apache/arrow-rs/pull/9229) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- optimize `RowGroupIndexReader` for single row group reads [\#9226](https://github.com/apache/arrow-rs/pull/9226) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- test: improve arrow-row fuzz tests [\#9222](https://github.com/apache/arrow-rs/pull/9222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- arrow-cast: support packing to Dictionary\(\_, Utf8View/BinaryView\) [\#9220](https://github.com/apache/arrow-rs/pull/9220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ethan-tyler](https://github.com/ethan-tyler))
- Add additional test coverage for  `BatchCoalescer` push\_batch\_with\_filter [\#9218](https://github.com/apache/arrow-rs/pull/9218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Parquet\] Optimize appending max level comparison in DefinitionLevelDecoder [\#9217](https://github.com/apache/arrow-rs/pull/9217) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- Remove dead code to fix clippy failure on main [\#9215](https://github.com/apache/arrow-rs/pull/9215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Parquet\] perf: reuse seeked File clone in ChunkReader::get\_read\(\) [\#9214](https://github.com/apache/arrow-rs/pull/9214) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([fvaleye](https://github.com/fvaleye))
- fix: \[9018\]Fixed RunArray slice offsets\(row, cast, eq\) [\#9213](https://github.com/apache/arrow-rs/pull/9213) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([manishkr](https://github.com/manishkr))
- Add benchmarks for reading struct arrays from parquet [\#9210](https://github.com/apache/arrow-rs/pull/9210) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- Support casting negative scale decimals to numeric [\#9207](https://github.com/apache/arrow-rs/pull/9207) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Chiicake](https://github.com/Chiicake))
- Deprecate `ArrowReaderOptions::with_page_index` and update API [\#9199](https://github.com/apache/arrow-rs/pull/9199) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- arrow-ipc: add reset method to DictionaryTracker [\#9196](https://github.com/apache/arrow-rs/pull/9196) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- Avoid a clone when creating `ListArray` from ArrayData [\#9194](https://github.com/apache/arrow-rs/pull/9194) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `ListViewArray` from ArrayData [\#9193](https://github.com/apache/arrow-rs/pull/9193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `MapArray` from ArrayData [\#9192](https://github.com/apache/arrow-rs/pull/9192) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `NullArray` from ArrayData [\#9191](https://github.com/apache/arrow-rs/pull/9191) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `PrimitiveArray` from ArrayData [\#9190](https://github.com/apache/arrow-rs/pull/9190) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `RunEndArray` from ArrayData [\#9189](https://github.com/apache/arrow-rs/pull/9189) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `UnionArray` from ArrayData [\#9188](https://github.com/apache/arrow-rs/pull/9188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `FixedSizeListArray` from ArrayData [\#9187](https://github.com/apache/arrow-rs/pull/9187) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `FixedSizeBinaryArray` from ArrayData [\#9186](https://github.com/apache/arrow-rs/pull/9186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Avoid a clone when creating `DictionaryArray` from ArrayData [\#9185](https://github.com/apache/arrow-rs/pull/9185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: take\_run return empty array instead of panic. [\#9182](https://github.com/apache/arrow-rs/pull/9182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([thorfour](https://github.com/thorfour))
- lint: remove unused function \(fix clippy [\#9178](https://github.com/apache/arrow-rs/pull/9178) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- add `#[inline]` to `BitIterator` `next` function  [\#9177](https://github.com/apache/arrow-rs/pull/9177) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Add ListView support to `arrow-row` and `arrow-ord` [\#9176](https://github.com/apache/arrow-rs/pull/9176) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- arrow-cast: Add display formatter for ListView [\#9175](https://github.com/apache/arrow-rs/pull/9175) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Add BinaryFormatSupport and Row Encoder to `arrow-avro` Writer [\#9171](https://github.com/apache/arrow-rs/pull/9171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- docs\(parquet\): move async parquet example into ArrowReaderBuilder docs [\#9167](https://github.com/apache/arrow-rs/pull/9167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([vigneshsiva11](https://github.com/vigneshsiva11))
- feat\(array\): add `RecordBatchStream` trait [\#9166](https://github.com/apache/arrow-rs/pull/9166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lidavidm](https://github.com/lidavidm))
- refactor: streamline date64 tests [\#9165](https://github.com/apache/arrow-rs/pull/9165) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([cht42](https://github.com/cht42))
- docs: update examples in ArrowReaderOptions to use in-memory buffers [\#9163](https://github.com/apache/arrow-rs/pull/9163) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AndreaBozzo](https://github.com/AndreaBozzo))
- Add Avro Reader projection API [\#9162](https://github.com/apache/arrow-rs/pull/9162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Avoid a clone when creating StringArray/BinaryArray from ArrayData [\#9160](https://github.com/apache/arrow-rs/pull/9160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix missing utf8 check for conversion from BinaryViewArray to StringViewArray [\#9158](https://github.com/apache/arrow-rs/pull/9158) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: try and avoid an allocation creating `GenericByteViewArray` from `ArrayData` [\#9156](https://github.com/apache/arrow-rs/pull/9156) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add find\_nth\_set\_bit\_position [\#9151](https://github.com/apache/arrow-rs/pull/9151) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- feat: add null comparison handling in make\_comparator [\#9150](https://github.com/apache/arrow-rs/pull/9150) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Uncomment part of test\_utf8\_single\_column\_reader\_test [\#9148](https://github.com/apache/arrow-rs/pull/9148) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdf-jkl](https://github.com/sdf-jkl))
- arrow-ipc: Add tests for nested dicts for Map and Union arrays [\#9146](https://github.com/apache/arrow-rs/pull/9146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Update ASF copyright year in NOTICE [\#9145](https://github.com/apache/arrow-rs/pull/9145) ([mohit7705](https://github.com/mohit7705))
- Avoid panic on Date32 overflow [\#9144](https://github.com/apache/arrow-rs/pull/9144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([cht42](https://github.com/cht42))
- feat: add `reserve` to `Rows` [\#9142](https://github.com/apache/arrow-rs/pull/9142) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- docs\(variant\): fix VariantObject::get documentation to reflect Option return type [\#9139](https://github.com/apache/arrow-rs/pull/9139) ([mohit7705](https://github.com/mohit7705))
- Add `BooleanBufferBuilder::extend_trusted_len` [\#9137](https://github.com/apache/arrow-rs/pull/9137) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- fix: support cast from `Null` to list view/run encoded/union types [\#9134](https://github.com/apache/arrow-rs/pull/9134) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Fix clippy [\#9130](https://github.com/apache/arrow-rs/pull/9130) ([alamb](https://github.com/alamb))
- Fix IPC roundtripping dicts nested in ListViews [\#9126](https://github.com/apache/arrow-rs/pull/9126) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Update readme for geospatial crate [\#9124](https://github.com/apache/arrow-rs/pull/9124) ([paleolimbot](https://github.com/paleolimbot))
- \[Parquet\] perf: Create `PrimitiveArray`s directly rather than via `ArrayData` [\#9122](https://github.com/apache/arrow-rs/pull/9122) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Parquet\] perf: Create Utf8/BinaryViewArray directly rather than via `ArrayData` [\#9121](https://github.com/apache/arrow-rs/pull/9121) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[parquet\] Add row group index virtual column [\#9117](https://github.com/apache/arrow-rs/pull/9117) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- docs\(parquet\): add example for preserving dictionary encoding [\#9116](https://github.com/apache/arrow-rs/pull/9116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AndreaBozzo](https://github.com/AndreaBozzo))
- doc: add example of RowFilter usage [\#9115](https://github.com/apache/arrow-rs/pull/9115) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sonhmai](https://github.com/sonhmai))
- docs: Update release schedule in README.md [\#9111](https://github.com/apache/arrow-rs/pull/9111) ([alamb](https://github.com/alamb))
- feat: add benchmarks for json parser [\#9107](https://github.com/apache/arrow-rs/pull/9107) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- chore: switch test from `bincode` to maintained `postcard` crate \(RUSTSEC-2025-0141 \) [\#9104](https://github.com/apache/arrow-rs/pull/9104) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add nullif\_kernel benchmark [\#9089](https://github.com/apache/arrow-rs/pull/9089) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Support Shredded Lists/Array in `variant_get` [\#9049](https://github.com/apache/arrow-rs/pull/9049) ([liamzwbao](https://github.com/liamzwbao))
- fix:\[9018\]Fixed RunArray slice offsets [\#9036](https://github.com/apache/arrow-rs/pull/9036) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([manishkr](https://github.com/manishkr))
- Support Float16 for create\_random\_array [\#9029](https://github.com/apache/arrow-rs/pull/9029) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([niebayes](https://github.com/niebayes))
- fix: display `0 secs` for empty DayTime/MonthDayNano intervals [\#9023](https://github.com/apache/arrow-rs/pull/9023) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add options to skip decoding `Statistics` and `SizeStatistics` in Parquet metadata [\#9008](https://github.com/apache/arrow-rs/pull/9008) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
