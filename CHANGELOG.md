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

## [53.0.0](https://github.com/apache/arrow-rs/tree/53.0.0) (2024-08-31)

[Full Changelog](https://github.com/apache/arrow-rs/compare/52.2.0...53.0.0)

**Breaking changes:**

- parquet\_derive: Match fields by name, support reading selected fields rather than all [\#6269](https://github.com/apache/arrow-rs/pull/6269) ([double-free](https://github.com/double-free))
- Update parquet object\_store dependency to 0.11.0 [\#6264](https://github.com/apache/arrow-rs/pull/6264) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- parquet Statistics - deprecate `has_*` APIs and add `_opt` functions that return `Option<T>` [\#6216](https://github.com/apache/arrow-rs/pull/6216) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Michael-J-Ward](https://github.com/Michael-J-Ward))
- Expose bulk ingest in flight sql client and server [\#6201](https://github.com/apache/arrow-rs/pull/6201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([djanderson](https://github.com/djanderson))
- Upgrade protobuf definitions to flightsql 17.0 \(\#6133\) [\#6169](https://github.com/apache/arrow-rs/pull/6169) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Remove automatic buffering in `ipc::reader::FileReader` for for consistent buffering [\#6132](https://github.com/apache/arrow-rs/pull/6132) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([V0ldek](https://github.com/V0ldek))
- No longer write Parquet column metadata after column chunks \*and\* in the footer [\#6117](https://github.com/apache/arrow-rs/pull/6117) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Remove `impl<T: AsRef<[u8]>> From<T> for Buffer` that easily accidentally copies data [\#6043](https://github.com/apache/arrow-rs/pull/6043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))

**Implemented enhancements:**

- Derive `PartialEq` and `Eq` for `parquet::arrow::ProjectionMask` [\#6329](https://github.com/apache/arrow-rs/issues/6329) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Allow converting empty `pyarrow.RecordBatch` to `arrow::RecordBatch` [\#6318](https://github.com/apache/arrow-rs/issues/6318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet writer should not write any min/max data to ColumnIndex when all values are null [\#6315](https://github.com/apache/arrow-rs/issues/6315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: Add `union` method to `RowSelection` [\#6307](https://github.com/apache/arrow-rs/issues/6307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing `UTC adjusted time` arrow array to parquet [\#6277](https://github.com/apache/arrow-rs/issues/6277) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- A better way to resize the buffer for the snappy encode/decode [\#6276](https://github.com/apache/arrow-rs/issues/6276) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet\_derive: support reading selected columns from parquet file [\#6268](https://github.com/apache/arrow-rs/issues/6268)
- Tests for invalid parquet files [\#6261](https://github.com/apache/arrow-rs/issues/6261) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement `date_part` for `Duration` [\#6245](https://github.com/apache/arrow-rs/issues/6245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Avoid unnecessary null buffer construction when converting arrays to a different type [\#6243](https://github.com/apache/arrow-rs/issues/6243) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `parquet_opendal` in related projects [\#6235](https://github.com/apache/arrow-rs/issues/6235)
- Look into optimizing reading FixedSizeBinary arrays from parquet [\#6219](https://github.com/apache/arrow-rs/issues/6219) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add benchmarks for `BYTE_STREAM_SPLIT` encoded Parquet `FIXED_LEN_BYTE_ARRAY` data [\#6203](https://github.com/apache/arrow-rs/issues/6203) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make it easy to write parquet to object\_store -- Implement `AsyncFileWriter` for a type that implements `obj_store::MultipartUpload` for `AsyncArrowWriter` [\#6200](https://github.com/apache/arrow-rs/issues/6200) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove test duplication in parquet statistics tets [\#6185](https://github.com/apache/arrow-rs/issues/6185) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support BinaryView Types in C Schema FFI [\#6170](https://github.com/apache/arrow-rs/issues/6170) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- speedup take\_byte\_view kernel [\#6167](https://github.com/apache/arrow-rs/issues/6167) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `StringView` and `BinaryView` statistics in `StatisticsConverter` [\#6164](https://github.com/apache/arrow-rs/issues/6164) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting `BinaryView` --\> `Utf8` and `LargeUtf8` [\#6162](https://github.com/apache/arrow-rs/issues/6162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `filter` kernel specially for `FixedSizeByteArray` [\#6153](https://github.com/apache/arrow-rs/issues/6153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `LevelHistogram` throughout Parquet metadata [\#6134](https://github.com/apache/arrow-rs/issues/6134) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support DoPutStatementIngest from Arrow Flight SQL 17.0 [\#6124](https://github.com/apache/arrow-rs/issues/6124) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- ColumnMetaData should no longer be written inline with data [\#6115](https://github.com/apache/arrow-rs/issues/6115) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement date\_part for `Interval`  [\#6113](https://github.com/apache/arrow-rs/issues/6113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Into<Arc<dyn Array>>` for `ArrayData` [\#6104](https://github.com/apache/arrow-rs/issues/6104)
- Allow flushing or non-buffered writes from `arrow::ipc::writer::StreamWriter` [\#6099](https://github.com/apache/arrow-rs/issues/6099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Default block\_size for `StringViewArray` [\#6094](https://github.com/apache/arrow-rs/issues/6094) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove `Statistics::has_min_max_set` and `ValueStatistics::has_min_max_set` and use `Option` instead [\#6093](https://github.com/apache/arrow-rs/issues/6093) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Upgrade arrow-flight to tonic 0.12 [\#6072](https://github.com/apache/arrow-rs/issues/6072)
- Improve speed of row converter by skipping utf8 checks [\#6058](https://github.com/apache/arrow-rs/issues/6058) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Extend support for BYTE\_STREAM\_SPLIT to FIXED\_LEN\_BYTE\_ARRAY, INT32, and INT64 primitive types [\#6048](https://github.com/apache/arrow-rs/issues/6048) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Release arrow-rs / parquet minor version `52.2.0` \(August 2024\) [\#5998](https://github.com/apache/arrow-rs/issues/5998) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Invalid `ColumnIndex` written in parquet [\#6310](https://github.com/apache/arrow-rs/issues/6310) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- comparison\_kernels benchmarks panic [\#6283](https://github.com/apache/arrow-rs/issues/6283) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Printing schema metadata includes possibly incorrect compression level [\#6270](https://github.com/apache/arrow-rs/issues/6270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Don't panic when creating `Field` from `FFI_ArrowSchema` with no name [\#6251](https://github.com/apache/arrow-rs/issues/6251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- lexsort\_to\_indices should not fallback to non-lexical sort if the datatype is not supported [\#6226](https://github.com/apache/arrow-rs/issues/6226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Statistics null\_count does not distinguish between `0` and not specified [\#6215](https://github.com/apache/arrow-rs/issues/6215) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Using a take kernel on a dense union can result in reaching "unreachable" code [\#6206](https://github.com/apache/arrow-rs/issues/6206) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Adding sub day seconds to Date64 is ignored. [\#6198](https://github.com/apache/arrow-rs/issues/6198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- mismatch between parquet type `is_optional` codes and comment [\#6191](https://github.com/apache/arrow-rs/issues/6191) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Minor: improve filter documentation [\#6317](https://github.com/apache/arrow-rs/pull/6317) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Improve comments on GenericByteViewArray::bytes\_iter\(\), prefix\_iter\(\) and suffix\_iter\(\) [\#6306](https://github.com/apache/arrow-rs/pull/6306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: improve `RowFilter` and `ArrowPredicate` docs [\#6301](https://github.com/apache/arrow-rs/pull/6301) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve documentation for `MutableArrayData` [\#6272](https://github.com/apache/arrow-rs/pull/6272) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add examples to `StringViewBuilder` and `BinaryViewBuilder` [\#6240](https://github.com/apache/arrow-rs/pull/6240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- minor: enhance document for ParquetField [\#6239](https://github.com/apache/arrow-rs/pull/6239) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- Minor: Improve Type documentation [\#6224](https://github.com/apache/arrow-rs/pull/6224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Update `DateType::Date64` docs [\#6223](https://github.com/apache/arrow-rs/pull/6223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add \(more\) Parquet Metadata Documentation [\#6184](https://github.com/apache/arrow-rs/pull/6184) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add additional documentation and examples to `ArrayAccessor` [\#6141](https://github.com/apache/arrow-rs/pull/6141) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: improve comments in temporal.rs tests [\#6140](https://github.com/apache/arrow-rs/pull/6140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Update release schedule in README [\#6125](https://github.com/apache/arrow-rs/pull/6125) ([alamb](https://github.com/alamb))

**Closed issues:**

- Simplify take octokit workflow [\#6279](https://github.com/apache/arrow-rs/issues/6279)
- Make the bearer token visible in FlightSqlServiceClient [\#6253](https://github.com/apache/arrow-rs/issues/6253) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Port `take` workflow to use `oktokit` [\#6242](https://github.com/apache/arrow-rs/issues/6242)
- Remove `SchemaBuilder` dependency from `StructArray` constructors [\#6138](https://github.com/apache/arrow-rs/issues/6138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Derive PartialEq and Eq for parquet::arrow::ProjectionMask [\#6330](https://github.com/apache/arrow-rs/pull/6330) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Support zero column `RecordBatch`es in pyarrow integration \(use RecordBatchOptions when converting a pyarrow RecordBatch\) [\#6320](https://github.com/apache/arrow-rs/pull/6320) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Michael-J-Ward](https://github.com/Michael-J-Ward))
- Fix writing of invalid  Parquet ColumnIndex when row group contains null pages [\#6319](https://github.com/apache/arrow-rs/pull/6319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Pass empty vectors as min/max for all null pages when building ColumnIndex [\#6316](https://github.com/apache/arrow-rs/pull/6316) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update tonic-build requirement from =0.12.0 to =0.12.2 [\#6314](https://github.com/apache/arrow-rs/pull/6314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Parquet: add `union` method to `RowSelection` [\#6308](https://github.com/apache/arrow-rs/pull/6308) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdd](https://github.com/sdd))
- Specialize filter for structs and sparse unions [\#6304](https://github.com/apache/arrow-rs/pull/6304) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Err on `try_from_le_slice` [\#6295](https://github.com/apache/arrow-rs/pull/6295) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([samuelcolvin](https://github.com/samuelcolvin))
- fix reference in doctest to size\_of which is not imported by default [\#6286](https://github.com/apache/arrow-rs/pull/6286) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rtyler](https://github.com/rtyler))
- Support writing UTC adjusted time arrays to parquet [\#6278](https://github.com/apache/arrow-rs/pull/6278) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([aykut-bozkurt](https://github.com/aykut-bozkurt))
- Minor: `pub use ByteView` in arrow and improve documentation [\#6275](https://github.com/apache/arrow-rs/pull/6275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix accessing name from ffi schema [\#6273](https://github.com/apache/arrow-rs/pull/6273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Do not print compression level in schema printer [\#6271](https://github.com/apache/arrow-rs/pull/6271) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ttencate](https://github.com/ttencate))
- ci: use octokit to add assignee [\#6267](https://github.com/apache/arrow-rs/pull/6267) ([dsgibbons](https://github.com/dsgibbons))
- Add tests for bad parquet files [\#6262](https://github.com/apache/arrow-rs/pull/6262) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add `Statistics::distinct_count_opt` and deprecate `Statistics::distinct_count` [\#6259](https://github.com/apache/arrow-rs/pull/6259) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: move `FallibleRequestStream` and `FallibleTonicResponseStream` to a module [\#6258](https://github.com/apache/arrow-rs/pull/6258) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Make the bearer token visible in FlightSqlServiceClient [\#6254](https://github.com/apache/arrow-rs/pull/6254) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([ccciudatu](https://github.com/ccciudatu))
- Use `unary()` for array conversion in Parquet array readers, speed up `Decimal128`, `Decimal256` and `Float16`  [\#6252](https://github.com/apache/arrow-rs/pull/6252) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Update tower requirement from 0.4.13 to 0.5.0 [\#6250](https://github.com/apache/arrow-rs/pull/6250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Implement date\_part for durations [\#6246](https://github.com/apache/arrow-rs/pull/6246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nrc](https://github.com/nrc))
- Remove unnecessary null buffer construction when converting arrays to a different type [\#6244](https://github.com/apache/arrow-rs/pull/6244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Implement PartialEq for GenericByteViewArray [\#6241](https://github.com/apache/arrow-rs/pull/6241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Remove non standard footer from LICENSE.txt / reference to Apache Aurora [\#6237](https://github.com/apache/arrow-rs/pull/6237) ([alamb](https://github.com/alamb))
- docs: Add parquet\_opendal in related projects [\#6236](https://github.com/apache/arrow-rs/pull/6236) ([Xuanwo](https://github.com/Xuanwo))
- Avoid infinite loop in bad parquet by checking the number of rep levels  [\#6232](https://github.com/apache/arrow-rs/pull/6232) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Specialize Prefix/Suffix Match for `Like/ILike` between Array and Scalar for StringViewArray [\#6231](https://github.com/apache/arrow-rs/pull/6231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xinlifoobar](https://github.com/xinlifoobar))
- fix: lexsort\_to\_indices should not fallback to non-lexical sort if the datatype is not supported [\#6225](https://github.com/apache/arrow-rs/pull/6225) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Modest improvement to FixedLenByteArray BYTE\_STREAM\_SPLIT arrow decoder [\#6222](https://github.com/apache/arrow-rs/pull/6222) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve performance of `FixedLengthBinary` decoding  [\#6220](https://github.com/apache/arrow-rs/pull/6220) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update documentation for Parquet BYTE\_STREAM\_SPLIT encoding [\#6212](https://github.com/apache/arrow-rs/pull/6212) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve interval parsing [\#6211](https://github.com/apache/arrow-rs/pull/6211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- minor: Suggest take on interleave docs [\#6210](https://github.com/apache/arrow-rs/pull/6210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- fix: Correctly handle take on dense union of a single selected type [\#6209](https://github.com/apache/arrow-rs/pull/6209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Add time dictionary coercions [\#6208](https://github.com/apache/arrow-rs/pull/6208) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- fix\(arrow\): restrict the range of temporal values produced via `data_gen` [\#6205](https://github.com/apache/arrow-rs/pull/6205) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kyle-mccarthy](https://github.com/kyle-mccarthy))
- Add benchmarks for `BYTE_STREAM_SPLIT` encoded Parquet `FIXED_LEN_BYTE_ARRAY` data  [\#6204](https://github.com/apache/arrow-rs/pull/6204) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Move `ParquetMetadataWriter` to its own module, update documentation [\#6202](https://github.com/apache/arrow-rs/pull/6202) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add `ThriftMetadataWriter` for writing Parquet metadata [\#6197](https://github.com/apache/arrow-rs/pull/6197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Update zstd-sys requirement from \>=2.0.0, \<2.0.13 to \>=2.0.0, \<2.0.14 [\#6196](https://github.com/apache/arrow-rs/pull/6196) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix parquet type `is_optional` comments [\#6192](https://github.com/apache/arrow-rs/pull/6192) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Remove duplicated statistics tests in parquet [\#6190](https://github.com/apache/arrow-rs/pull/6190) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Kev1n8](https://github.com/Kev1n8))
- Benchmarks for `bool_and` [\#6189](https://github.com/apache/arrow-rs/pull/6189) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Fix typo in documentation of Float64Array [\#6188](https://github.com/apache/arrow-rs/pull/6188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mesejo](https://github.com/mesejo))
- Make it clear that `StatisticsConverter` can not panic [\#6187](https://github.com/apache/arrow-rs/pull/6187) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- add filter benchmark for `FixedSizeBinaryArray` [\#6186](https://github.com/apache/arrow-rs/pull/6186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chloro-pn](https://github.com/chloro-pn))
- Update sysinfo requirement from 0.30.12 to 0.31.2 [\#6182](https://github.com/apache/arrow-rs/pull/6182) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add support for `StringView` and `BinaryView` statistics in `StatisticsConverter` [\#6181](https://github.com/apache/arrow-rs/pull/6181) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Kev1n8](https://github.com/Kev1n8))
- Support casting between BinaryView \<--\> Utf8 and LargeUtf8 [\#6180](https://github.com/apache/arrow-rs/pull/6180) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xinlifoobar](https://github.com/xinlifoobar))
- Implement specialized filter kernel for `FixedSizeByteArray` [\#6178](https://github.com/apache/arrow-rs/pull/6178) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chloro-pn](https://github.com/chloro-pn))
- Support `StringView` and `BinaryView` in CDataInterface  [\#6171](https://github.com/apache/arrow-rs/pull/6171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- Optimize `take` kernel for `BinaryViewArray` and `StringViewArray` [\#6168](https://github.com/apache/arrow-rs/pull/6168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- Support Parquet `BYTE_STREAM_SPLIT` for INT32, INT64, and FIXED\_LEN\_BYTE\_ARRAY primitive types [\#6159](https://github.com/apache/arrow-rs/pull/6159) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix comparison kernel benchmarks [\#6147](https://github.com/apache/arrow-rs/pull/6147) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- improve `LIKE` regex performance up to 12x [\#6145](https://github.com/apache/arrow-rs/pull/6145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Optimize `min_boolean` and `bool_and` [\#6144](https://github.com/apache/arrow-rs/pull/6144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Reduce bounds check in `RowIter`, add `unsafe Rows::row_unchecked` [\#6142](https://github.com/apache/arrow-rs/pull/6142) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Minor: Simplify `StructArray` constructors [\#6139](https://github.com/apache/arrow-rs/pull/6139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rafferty97](https://github.com/Rafferty97))
- Implement exponential block size growing strategy for `StringViewBuilder` [\#6136](https://github.com/apache/arrow-rs/pull/6136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Use `LevelHistogram` in `PageIndex` [\#6135](https://github.com/apache/arrow-rs/pull/6135) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add ArrowError::ArithmeticError [\#6130](https://github.com/apache/arrow-rs/pull/6130) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Improve `LIKE` performance for "contains" style queries [\#6128](https://github.com/apache/arrow-rs/pull/6128) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Add `BooleanArray::new_from_packed` and `BooleanArray::new_from_u8` [\#6127](https://github.com/apache/arrow-rs/pull/6127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chloro-pn](https://github.com/chloro-pn))
- improvements to `(i)starts_with` and `(i)ends_with` performance [\#6118](https://github.com/apache/arrow-rs/pull/6118) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Fix Clippy for the Rust 1.80 release [\#6116](https://github.com/apache/arrow-rs/pull/6116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- added a flush method to IPC writers [\#6108](https://github.com/apache/arrow-rs/pull/6108) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([V0ldek](https://github.com/V0ldek))
- Add support for level histograms added in PARQUET-2261 to `ParquetMetaData` [\#6105](https://github.com/apache/arrow-rs/pull/6105) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Implement date\_part for intervals [\#6071](https://github.com/apache/arrow-rs/pull/6071) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nrc](https://github.com/nrc))
- feat\(parquet\): Implement AsyncFileWriter for `object_store::buffered::BufWriter` [\#6013](https://github.com/apache/arrow-rs/pull/6013) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Xuanwo](https://github.com/Xuanwo))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
