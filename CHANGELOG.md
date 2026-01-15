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

## [Unreleased]

**Breaking changes:**

- Deprecate `ArrowReaderOptions::with_page_index` in favor of `with_page_index_policy` to support `PageIndexPolicy` enum with Skip, Optional, and Required policies. The deprecated method converts boolean to Required (true) or Skip (false). Use `with_page_index_policy`, `with_column_index_policy`, or `with_offset_index_policy` for fine-grained control.

## [57.2.0](https://github.com/apache/arrow-rs/tree/57.2.0) (2026-01-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.1.0...57.2.0)

**Breaking changes:**

- Seal Array trait [\#9092](https://github.com/apache/arrow-rs/pull/9092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- \[Variant\] Unify the CastOptions usage in parquet-variant-compute [\#8984](https://github.com/apache/arrow-rs/pull/8984) ([klion26](https://github.com/klion26))

**Implemented enhancements:**

- \[parquet\] further relax `LevelInfoBuilder::types_compatible` for `ArrowWriter` [\#9098](https://github.com/apache/arrow-rs/issues/9098)
- Update arrow-row documentation with Union encoding [\#9084](https://github.com/apache/arrow-rs/issues/9084)
- Add code examples for min and max compute functions [\#9055](https://github.com/apache/arrow-rs/issues/9055)
- Add `append_n` to bytes view builder API [\#9034](https://github.com/apache/arrow-rs/issues/9034) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Move `RunArray::get_physical_indices` to `RunEndBuffer` [\#9025](https://github.com/apache/arrow-rs/issues/9025) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow quote style in csv writer [\#9003](https://github.com/apache/arrow-rs/issues/9003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- IPC support for ListView [\#9002](https://github.com/apache/arrow-rs/issues/9002) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `BinaryArrayType` for `&FixedSizeBinaryArray`s [\#8992](https://github.com/apache/arrow-rs/issues/8992) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-buffer: implement num-traits for i256 [\#8976](https://github.com/apache/arrow-rs/issues/8976) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support for `Arc<str>` in `ParquetRecordWriter` derive macro [\#8972](https://github.com/apache/arrow-rs/issues/8972)
- \[arrow-avro\] suggest switching from xz to liblzma [\#8970](https://github.com/apache/arrow-rs/issues/8970) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-buffer: add i256::trailing\_zeros [\#8968](https://github.com/apache/arrow-rs/issues/8968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-buffer: make i256::leading\_zeros public [\#8965](https://github.com/apache/arrow-rs/issues/8965) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add spark like `ignoreLeadingWhiteSpace` and `ignoreTrailingWhiteSpace` options to the csv writer [\#8961](https://github.com/apache/arrow-rs/issues/8961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add round trip benchmark for Parquet writer/reader [\#8955](https://github.com/apache/arrow-rs/issues/8955) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support performant `interleave` for List/LargeList [\#8952](https://github.com/apache/arrow-rs/issues/8952) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Support array access when parsing `VariantPath` [\#8946](https://github.com/apache/arrow-rs/issues/8946)
- Some panic!s could be represented as unimplemented!s [\#8932](https://github.com/apache/arrow-rs/issues/8932) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] easier way to construct a shredded schema [\#8922](https://github.com/apache/arrow-rs/issues/8922)
- Support `DataType::ListView` and `DataType::LargeListView` in `ArrayData::new_null` [\#8908](https://github.com/apache/arrow-rs/issues/8908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `GenericListViewArray::from_iter_primitive` [\#8906](https://github.com/apache/arrow-rs/issues/8906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Unify the cast option usage in ParquentVariant [\#8873](https://github.com/apache/arrow-rs/issues/8873)
- Blog post about efficient filter representation in Parquet filter pushdown [\#8843](https://github.com/apache/arrow-rs/issues/8843) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add comparison support for Union arrays in the `cmp` kernel [\#8837](https://github.com/apache/arrow-rs/issues/8837) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Support array shredding into `List/LargeList/ListView/LargeListView` [\#8830](https://github.com/apache/arrow-rs/issues/8830)
- Support `Union` data types for row format [\#8828](https://github.com/apache/arrow-rs/issues/8828) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FFI support for ListView [\#8819](https://github.com/apache/arrow-rs/issues/8819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Support more Arrow Datatypes from Variant primitive types [\#8805](https://github.com/apache/arrow-rs/issues/8805)
- `FixedSizeBinaryBuilder` supports `append_array` [\#8750](https://github.com/apache/arrow-rs/issues/8750) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement special case `zip` with scalar for Utf8View [\#8724](https://github.com/apache/arrow-rs/issues/8724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[geometry\] Wire up arrow reader/writer for `GEOMETRY` and `GEOGRAPHY` [\#8717](https://github.com/apache/arrow-rs/issues/8717) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Soundness Bug in `try_binary` when `Array` is implemented incorrectly in external crate [\#9106](https://github.com/apache/arrow-rs/issues/9106)
- casting `Dict(_, LargeUtf8)` to `Utf8View` \(`StringViewArray`\) panics [\#9101](https://github.com/apache/arrow-rs/issues/9101)
- wrong results for null count of `nullif` kernel [\#9085](https://github.com/apache/arrow-rs/issues/9085) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Empty first line in some code examples [\#9063](https://github.com/apache/arrow-rs/issues/9063)
- GenericByteViewArray::slice is not zero-copy but ought to be [\#9014](https://github.com/apache/arrow-rs/issues/9014)
- Regression in struct casting in 57.2.0 \(not yet released\) [\#9005](https://github.com/apache/arrow-rs/issues/9005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix panic when decoding multiple Union columns in RowConverter [\#8999](https://github.com/apache/arrow-rs/issues/8999) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `take_fixed_size_binary` Does Not Consider NULL Indices [\#8947](https://github.com/apache/arrow-rs/issues/8947) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[arrow-avro\] RecordEncoder Bugs [\#8934](https://github.com/apache/arrow-rs/issues/8934) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `FixedSizeBinaryArray::try_new(...)` Panics with Item Length of Zero [\#8926](https://github.com/apache/arrow-rs/issues/8926) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `cargo test -p arrow-cast` fails on main [\#8910](https://github.com/apache/arrow-rs/issues/8910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `GenericListViewArray::new_null` ignores `len` and returns an empty array [\#8904](https://github.com/apache/arrow-rs/issues/8904) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `FixedSizeBinaryArray::new_null` Does Not Properly Set the Length of the Values Buffer [\#8900](https://github.com/apache/arrow-rs/issues/8900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Struct casting requires same order of fields [\#8870](https://github.com/apache/arrow-rs/issues/8870) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cannot cast string dictionary to binary view [\#8841](https://github.com/apache/arrow-rs/issues/8841) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Add Union encoding documentation  [\#9102](https://github.com/apache/arrow-rs/pull/9102) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([EduardAkhmetshin](https://github.com/EduardAkhmetshin))
- docs: fix misleading reserve documentation [\#9076](https://github.com/apache/arrow-rs/pull/9076) ([WaterWhisperer](https://github.com/WaterWhisperer))
- Fix headers and empty lines in code examples [\#9064](https://github.com/apache/arrow-rs/pull/9064) ([EduardAkhmetshin](https://github.com/EduardAkhmetshin))
- Add examples for min and max functions [\#9062](https://github.com/apache/arrow-rs/pull/9062) ([EduardAkhmetshin](https://github.com/EduardAkhmetshin))
- Improve arrow-buffer documentation [\#9020](https://github.com/apache/arrow-rs/pull/9020) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Move examples in arrow-csv to docstrings, polish up docs [\#9001](https://github.com/apache/arrow-rs/pull/9001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add example of parsing field names as VariantPath [\#8945](https://github.com/apache/arrow-rs/pull/8945) ([alamb](https://github.com/alamb))
- Improve documentation for `prep\_null\_mask\_flter [\#8722](https://github.com/apache/arrow-rs/pull/8722) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- \[parquet\] Avoid a clone while resolving the read strategy [\#9056](https://github.com/apache/arrow-rs/pull/9056) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- perf: improve performance of encoding `GenericByteArray` by 8% [\#9054](https://github.com/apache/arrow-rs/pull/9054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Speed up unary `not` kernel by 50%, add `BooleanBuffer::from_bitwise_unary` [\#8996](https://github.com/apache/arrow-rs/pull/8996) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- arrow-select: improve dictionary interleave fallback performance [\#8978](https://github.com/apache/arrow-rs/pull/8978) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- Add special implementation for zip for Utf8View/BinaryView scalars [\#8963](https://github.com/apache/arrow-rs/pull/8963) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mkleen](https://github.com/mkleen))
- arrow-select: implement specialized interleave\_list [\#8953](https://github.com/apache/arrow-rs/pull/8953) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))

**Closed issues:**

- impl `Index` for `UnionFields` [\#8958](https://github.com/apache/arrow-rs/issues/8958) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Add `DataType::is_decimal` [\#9100](https://github.com/apache/arrow-rs/pull/9100) ([AdamGS](https://github.com/AdamGS))
- feat\(parquet\): relax type compatility check in parquet ArrowWriter [\#9099](https://github.com/apache/arrow-rs/pull/9099) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([gruuya](https://github.com/gruuya))
- \[Variant\] Move `ArrayVariantToArrowRowBuilder` to `variant_to_arrow` [\#9094](https://github.com/apache/arrow-rs/pull/9094) ([liamzwbao](https://github.com/liamzwbao))
- chore: increase row count and batch size for more deterministic tests [\#9088](https://github.com/apache/arrow-rs/pull/9088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Fix `nullif` kernel [\#9087](https://github.com/apache/arrow-rs/pull/9087) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `FlightInfo::with_endpoints` method [\#9075](https://github.com/apache/arrow-rs/pull/9075) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- chore: run validation when debug assertion enabled and not only for test [\#9073](https://github.com/apache/arrow-rs/pull/9073) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Minor: make it clear cache array reader is not cloning arrays [\#9057](https://github.com/apache/arrow-rs/pull/9057) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: avoid clone in RunArray row decoding via buffer stealing [\#9052](https://github.com/apache/arrow-rs/pull/9052) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lyang24](https://github.com/lyang24))
- Minor: avoid some clones when reading parquet [\#9048](https://github.com/apache/arrow-rs/pull/9048) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix: don't generate nulls for `Decimal128` and `Decimal256` when field is non-nullable and have non-zero `null_density` [\#9046](https://github.com/apache/arrow-rs/pull/9046) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- fix: `Rows` `size` should use `capacity` and not `len` [\#9044](https://github.com/apache/arrow-rs/pull/9044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- fix: integration / Archery test With other arrows container ran out of space [\#9043](https://github.com/apache/arrow-rs/pull/9043) ([lyang24](https://github.com/lyang24))
- feat: add new `try_append_value_n()` function to `GenericByteViewBuilder` [\#9040](https://github.com/apache/arrow-rs/pull/9040) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lyang24](https://github.com/lyang24))
- Rename fields in BooleanBuffer for clarity [\#9039](https://github.com/apache/arrow-rs/pull/9039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Allocate buffers before work in `boolean_kernels` benchmark [\#9035](https://github.com/apache/arrow-rs/pull/9035) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Move RunArray::get\_physical\_indices to RunEndBuffer [\#9027](https://github.com/apache/arrow-rs/pull/9027) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lyang24](https://github.com/lyang24))
- Improve `RunArray` documentation [\#9019](https://github.com/apache/arrow-rs/pull/9019) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add BooleanArray tests for null and slice behavior [\#9013](https://github.com/apache/arrow-rs/pull/9013) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([UtkarshSahay123](https://github.com/UtkarshSahay123))
- feat: support array indices in VariantPath dot notation [\#9012](https://github.com/apache/arrow-rs/pull/9012) ([foskey51](https://github.com/foskey51))
- arrow-cast: Bring back in-order field casting for `StructArray` [\#9007](https://github.com/apache/arrow-rs/pull/9007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- arrow-ipc: Add ListView support [\#9006](https://github.com/apache/arrow-rs/pull/9006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Add quote style to csv writer [\#9004](https://github.com/apache/arrow-rs/pull/9004) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xanderbailey](https://github.com/xanderbailey))
- Fix row slice bug in Union column decoding with many columns [\#9000](https://github.com/apache/arrow-rs/pull/9000) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- build\(deps\): bump actions/download-artifact from 6 to 7 [\#8995](https://github.com/apache/arrow-rs/pull/8995) ([dependabot[bot]](https://github.com/apps/dependabot))
- minor: Add comment blocks to PR template [\#8994](https://github.com/apache/arrow-rs/pull/8994) ([Jefffrey](https://github.com/Jefffrey))
- Implement `BinaryArrayType` for `&FixedSizeBinaryArray`s [\#8993](https://github.com/apache/arrow-rs/pull/8993) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- feat: impl BatchCoalescer::push\_batch\_with\_indices [\#8991](https://github.com/apache/arrow-rs/pull/8991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ClSlaid](https://github.com/ClSlaid))
- \[Arrow\]Configure max deduplication length for `StringView` [\#8990](https://github.com/apache/arrow-rs/pull/8990) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lichuang](https://github.com/lichuang))
- feat: implement append\_array for FixedSizeBinaryBuilder [\#8989](https://github.com/apache/arrow-rs/pull/8989) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ClSlaid](https://github.com/ClSlaid))
- Add benchmarks for Utf8View scalars for zip [\#8988](https://github.com/apache/arrow-rs/pull/8988) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mkleen](https://github.com/mkleen))
- build\(deps\): bump actions/cache from 4 to 5 [\#8986](https://github.com/apache/arrow-rs/pull/8986) ([dependabot[bot]](https://github.com/apps/dependabot))
- Take fsb null indices [\#8981](https://github.com/apache/arrow-rs/pull/8981) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add List to `interleave_kernels` benchmark [\#8980](https://github.com/apache/arrow-rs/pull/8980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix ipc errors for `LargeList` containing sliced `StringViews` [\#8979](https://github.com/apache/arrow-rs/pull/8979) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fabianmurariu](https://github.com/fabianmurariu))
- arrow-buffer: implement num-traits numeric operations [\#8977](https://github.com/apache/arrow-rs/pull/8977) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([theirix](https://github.com/theirix))
- Update `xz` crate dependency to use `liblzma` in arrow-avro [\#8975](https://github.com/apache/arrow-rs/pull/8975) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- arrow-data: avoid allocating in get\_last\_run\_end [\#8974](https://github.com/apache/arrow-rs/pull/8974) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- Support for `Arc<str>` in `ParquetRecordWriter` derive macro [\#8973](https://github.com/apache/arrow-rs/pull/8973) ([heilhead](https://github.com/heilhead))
- feat: support casting  `Time32` to `Int64` [\#8971](https://github.com/apache/arrow-rs/pull/8971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tshauck](https://github.com/tshauck))
- arrow-buffer: add i256::trailing\_zeros [\#8969](https://github.com/apache/arrow-rs/pull/8969) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([theirix](https://github.com/theirix))
- Perf: Vectorize check\_bounds\(2x speedup\) [\#8966](https://github.com/apache/arrow-rs/pull/8966) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- arrow-buffer: make i256::leading\_zeros public and tested [\#8964](https://github.com/apache/arrow-rs/pull/8964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([theirix](https://github.com/theirix))
- Add ignore leading and trailing white space to csv parser [\#8960](https://github.com/apache/arrow-rs/pull/8960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xanderbailey](https://github.com/xanderbailey))
- Access `UnionFields` elements by index [\#8959](https://github.com/apache/arrow-rs/pull/8959) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- Add Parquet roundtrip benchmarks [\#8956](https://github.com/apache/arrow-rs/pull/8956) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[Variant\] Add variant to arrow for Date64/Timestamp\(Second/Millisecond\)/Time32/Time64 [\#8950](https://github.com/apache/arrow-rs/pull/8950) ([klion26](https://github.com/klion26))
- Let `ArrowArrayStreamReader` handle schema with attached metadata + do schema checking [\#8944](https://github.com/apache/arrow-rs/pull/8944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonded94](https://github.com/jonded94))
- Adds ExtensionType for Parquet geospatial WKB arrays [\#8943](https://github.com/apache/arrow-rs/pull/8943) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([BlakeOrth](https://github.com/BlakeOrth))
- Add builder to help create Schemas for shredding \(`ShreddedSchemaBuilder`\) [\#8940](https://github.com/apache/arrow-rs/pull/8940) ([XiangpengHao](https://github.com/XiangpengHao))
- build\(deps\): update criterion requirement from 0.7.0 to 0.8.0 [\#8939](https://github.com/apache/arrow-rs/pull/8939) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: Resolve Avro RecordEncoder bugs related to nullable Struct fields and Union type ids [\#8935](https://github.com/apache/arrow-rs/pull/8935) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Some panic!s could more semantically be unimplemented! [\#8933](https://github.com/apache/arrow-rs/pull/8933) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([abacef](https://github.com/abacef))
- fix: ipc decode panic with invalid data [\#8931](https://github.com/apache/arrow-rs/pull/8931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([leiysky](https://github.com/leiysky))
- Allow creating zero-sized FixedSizeBinary arrays [\#8927](https://github.com/apache/arrow-rs/pull/8927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- Update `test_variant_get_error_when_cast_failure...`  tests to uses a valid `VariantArray` [\#8921](https://github.com/apache/arrow-rs/pull/8921) ([alamb](https://github.com/alamb))
- Make flight sql client generic [\#8915](https://github.com/apache/arrow-rs/pull/8915) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- \[minor\] Name Magic Number "8" in `FixedSizeBinaryArray::new_null` [\#8914](https://github.com/apache/arrow-rs/pull/8914) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- fix: cast Binary/String dictionary to view [\#8912](https://github.com/apache/arrow-rs/pull/8912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- \[8910\]Fixed doc test with feature prettyprint [\#8911](https://github.com/apache/arrow-rs/pull/8911) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([manishkr](https://github.com/manishkr))
- feat: `ArrayData::new_null` for `ListView` / `LargeListView` [\#8909](https://github.com/apache/arrow-rs/pull/8909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dqkqd](https://github.com/dqkqd))
- fead: add `GenericListViewArray::from_iter_primitive` [\#8907](https://github.com/apache/arrow-rs/pull/8907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dqkqd](https://github.com/dqkqd))
- fix: `GenericListViewArray::new_null` returns empty array [\#8905](https://github.com/apache/arrow-rs/pull/8905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dqkqd](https://github.com/dqkqd))
- Allocate a zeroed buffer for FixedSizeBinaryArray::null [\#8901](https://github.com/apache/arrow-rs/pull/8901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- build\(deps\): bump actions/checkout from 5 to 6 [\#8899](https://github.com/apache/arrow-rs/pull/8899) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add getters to `UnionFields` [\#8895](https://github.com/apache/arrow-rs/pull/8895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- Add validated constructors for UnionFields [\#8891](https://github.com/apache/arrow-rs/pull/8891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([friendlymatthew](https://github.com/friendlymatthew))
- Add bit width check [\#8888](https://github.com/apache/arrow-rs/pull/8888) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rambleraptor](https://github.com/rambleraptor))
- \[Variant\] Improve `variant_get` performance on a perfect shredding [\#8887](https://github.com/apache/arrow-rs/pull/8887) ([XiangpengHao](https://github.com/XiangpengHao))
- Add UnionArray::fields [\#8884](https://github.com/apache/arrow-rs/pull/8884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- Struct casting field order [\#8871](https://github.com/apache/arrow-rs/pull/8871) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Add support for `Union` types in `RowConverter` [\#8839](https://github.com/apache/arrow-rs/pull/8839) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- Add comparison support for Union arrays [\#8838](https://github.com/apache/arrow-rs/pull/8838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Support array shredding into `List/LargeList/ListView/LargeListView` [\#8831](https://github.com/apache/arrow-rs/pull/8831) ([liamzwbao](https://github.com/liamzwbao))
- Add support for using ListView arrays and types through FFI [\#8822](https://github.com/apache/arrow-rs/pull/8822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- Add ability to skip or transform page encoding statistics in Parquet metadata [\#8797](https://github.com/apache/arrow-rs/pull/8797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Implement a `Vec<RecordBatch>` wrapper for `pyarrow.Table` convenience [\#8790](https://github.com/apache/arrow-rs/pull/8790) ([jonded94](https://github.com/jonded94))
- Make Parquet SBBF serialize/deserialize helpers public for external reuse [\#8762](https://github.com/apache/arrow-rs/pull/8762) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([RoseZhang123](https://github.com/RoseZhang123))
- Add cast support for \(Large\)ListView \<-\> \(Large\)List [\#8735](https://github.com/apache/arrow-rs/pull/8735) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vegarsti](https://github.com/vegarsti))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
