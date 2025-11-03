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

## [57.0.0](https://github.com/apache/arrow-rs/tree/57.0.0) (2025-10-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/56.2.0...57.0.0)

**Breaking changes:**

- Use `Arc<FileEncryptionProperties>` everywhere to be be consistent with `FileDecryptionProperties` [\#8626](https://github.com/apache/arrow-rs/pull/8626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- feat: Improve DataType display for `RunEndEncoded` [\#8596](https://github.com/apache/arrow-rs/pull/8596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Add `ArrowError::AvroError`, remaining types and roundtrip tests to `arrow-avro`,  [\#8595](https://github.com/apache/arrow-rs/pull/8595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[thrift-remodel\] Refactor Thrift encryption and store encodings as bitmask [\#8587](https://github.com/apache/arrow-rs/pull/8587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- feat: Enhance `Map` display formatting in DataType [\#8570](https://github.com/apache/arrow-rs/pull/8570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- feat: Enhance DataType display formatting for `ListView` and `LargeListView` variants [\#8569](https://github.com/apache/arrow-rs/pull/8569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Use custom thrift parser for parquet metadata \(phase 1 of Thrift remodel\) [\#8530](https://github.com/apache/arrow-rs/pull/8530) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- refactor: improve display formatting for Union [\#8529](https://github.com/apache/arrow-rs/pull/8529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Use `Arc<FileDecryptionProperties>` to reduce size of ParquetMetadata and avoid copying when `encryption` is enabled [\#8470](https://github.com/apache/arrow-rs/pull/8470) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix for column name based projection mask creation [\#8447](https://github.com/apache/arrow-rs/pull/8447) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve Display formatting of DataType::Timestamp [\#8425](https://github.com/apache/arrow-rs/pull/8425) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Use more compact Debug formatting of Field [\#8424](https://github.com/apache/arrow-rs/pull/8424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Reuse zstd compression context when writing IPC [\#8405](https://github.com/apache/arrow-rs/pull/8405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([albertlockett](https://github.com/albertlockett))
- \[Decimal\] Add scale argument to validation functions to ensure accurate error logging [\#8396](https://github.com/apache/arrow-rs/pull/8396) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Quote `DataType::Struct` field names in `Display` formatting [\#8291](https://github.com/apache/arrow-rs/pull/8291) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Improve `Display` for `DataType` and `Field` [\#8290](https://github.com/apache/arrow-rs/pull/8290) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Bump pyo3 to 0.26.0 [\#8286](https://github.com/apache/arrow-rs/pull/8286) ([mbrobbel](https://github.com/mbrobbel))

**Implemented enhancements:**

- Added Avro support (new `arrow-avro` crate) [\#4886](https://github.com/apache/arrow-rs/issues/4886)
- parquet-rewrite: supports compression level and write batch size [\#8639](https://github.com/apache/arrow-rs/issues/8639)
- Error not panic when int96 stastistics aren't size 12 [\#8614](https://github.com/apache/arrow-rs/issues/8614) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Make `VariantArray` iterable [\#8612](https://github.com/apache/arrow-rs/issues/8612)
- \[Variant\] impl `PartialEq` for `VariantArray` [\#8610](https://github.com/apache/arrow-rs/issues/8610)
- \[Variant\] Remove potential panics when probing `VariantArray` [\#8609](https://github.com/apache/arrow-rs/issues/8609)
- \[Variant\] Remove ceremony of going from list of `Variant` to `VariantArray` [\#8606](https://github.com/apache/arrow-rs/issues/8606)
- Eliminate redundant validation in `RecordBatch::project` [\#8591](https://github.com/apache/arrow-rs/issues/8591) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[PARQUET\]\[BENCH\] Arrow writer bench with compression and/or page v2 [\#8559](https://github.com/apache/arrow-rs/issues/8559) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] casting functions are confusingly named [\#8531](https://github.com/apache/arrow-rs/issues/8531) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing GeospatialStatistics in Parquet writer [\#8523](https://github.com/apache/arrow-rs/issues/8523) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[thrift-remodel\] Optimize `convert_row_groups` [\#8517](https://github.com/apache/arrow-rs/issues/8517) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Add variant to arrow primitive support for boolean/timestamp/time [\#8515](https://github.com/apache/arrow-rs/issues/8515)
- Test `thrift-remodel` branch with DataFusion [\#8513](https://github.com/apache/arrow-rs/issues/8513) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `UnionArray::is_dense` Method Public [\#8503](https://github.com/apache/arrow-rs/issues/8503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `append_n` method to `FixedSizeBinaryDictionaryBuilder` [\#8497](https://github.com/apache/arrow-rs/issues/8497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] Reduce size of ParquetMetadata when encryption feature is enabled [\#8469](https://github.com/apache/arrow-rs/issues/8469) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Parquet\] Remove useless mut requirements in geting bloom filter function [\#8461](https://github.com/apache/arrow-rs/issues/8461) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Change `serde` dependency to `serde_core` where applicable [\#8451](https://github.com/apache/arrow-rs/issues/8451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] Split `ParquetMetadataReader` into IO/decoder state machine and thrift parsing [\#8439](https://github.com/apache/arrow-rs/issues/8439) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove compiler warning for redundant config enablement [\#8412](https://github.com/apache/arrow-rs/issues/8412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add geospatial statistics creation support for GEOMETRY/GEOGRAPHY Parquet logical types [\#8411](https://github.com/apache/arrow-rs/issues/8411) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow_json` lacks  `with_timestamp_format` functions like `arrow_csv` had offered [\#8398](https://github.com/apache/arrow-rs/issues/8398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unify API for writing column chunks / row groups in parallel [\#8389](https://github.com/apache/arrow-rs/issues/8389) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Reuse zstd context in arrow IPC writer [\#8386](https://github.com/apache/arrow-rs/issues/8386) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[Variant\] Support reading/writing Parquet Variant LogicalType [\#8370](https://github.com/apache/arrow-rs/issues/8370) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Implement a `shred_variant` function [\#8361](https://github.com/apache/arrow-rs/issues/8361)
- \[Parquet\] Expose ReadPlan and ReadPlanBuilder [\#8347](https://github.com/apache/arrow-rs/issues/8347) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] \[Shredding\] Support typed\_access for `List` [\#8337](https://github.com/apache/arrow-rs/issues/8337) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] \[Shredding\] Support typed\_access for `Struct` [\#8336](https://github.com/apache/arrow-rs/issues/8336) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] \[Shredding\] Support typed\_access for `Time64(Microsecond)` [\#8334](https://github.com/apache/arrow-rs/issues/8334) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] \[Shredding\] Support typed\_access for `Decimal128` [\#8332](https://github.com/apache/arrow-rs/issues/8332) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] \[Shredding\] Support typed\_access for `Timestamp(Microsecond, _)` and `Timestamp(Nanosecond, _)` [\#8331](https://github.com/apache/arrow-rs/issues/8331) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] \[Shredding\] Support typed\_access for `Date32` [\#8330](https://github.com/apache/arrow-rs/issues/8330) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Support strict casting for all data types [\#8303](https://github.com/apache/arrow-rs/issues/8303)
- \[Variant\] Support typed access for string types in variant\_get [\#8285](https://github.com/apache/arrow-rs/issues/8285)
- \[Variant\]: Implement `DataType::FixedSizeList` support for `cast_to_variant` kernel [\#8281](https://github.com/apache/arrow-rs/issues/8281)

**Fixed bugs:**

- Fix arrow-avro Writer Documentation related to AvroBinaryFormat [\#8631](https://github.com/apache/arrow-rs/issues/8631) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Decimal -\> Decimal cast wrongly fails for large scale reduction [\#8579](https://github.com/apache/arrow-rs/issues/8579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] Avoid fetching multiple pages when `max_predicate_cache_size`is 0 [\#8542](https://github.com/apache/arrow-rs/issues/8542) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- DataType parsing no longer works correctly for old formatted timestamps [\#8539](https://github.com/apache/arrow-rs/issues/8539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] ArrowWriter flush does not work [\#8534](https://github.com/apache/arrow-rs/issues/8534) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `arrow::compute::interleave` fails with struct arrays with no fields [\#8533](https://github.com/apache/arrow-rs/issues/8533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] Over memory consumation for writer page v1 compressed [\#8526](https://github.com/apache/arrow-rs/issues/8526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Incorrect Behavior of Collecting a filtered iterator to a BooleanArray [\#8505](https://github.com/apache/arrow-rs/issues/8505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] ProjectionMask::columns name handling is bug prone [\#8443](https://github.com/apache/arrow-rs/issues/8443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Shredded typed\_value columns must have valid variant types [\#8435](https://github.com/apache/arrow-rs/issues/8435) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- cargo test -p parquet fails with default `ulimit` [\#8406](https://github.com/apache/arrow-rs/issues/8406) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Column with List\(Struct\) causes failed to decode level data for struct array [\#8404](https://github.com/apache/arrow-rs/issues/8404) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Binaryview Utf8 Cast Issue [\#8403](https://github.com/apache/arrow-rs/issues/8403) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Decimal precision validation displays value without accounting for scale [\#8382](https://github.com/apache/arrow-rs/issues/8382) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] `VariantArray::data_type` returns `StructType`, causing `Array::as_struct` to panic [\#8319](https://github.com/apache/arrow-rs/issues/8319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] writing a VariantArray to parquet panics [\#8296](https://github.com/apache/arrow-rs/issues/8296) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Docs: Add more comments to the Parquet writer code [\#8383](https://github.com/apache/arrow-rs/pull/8383) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- \[parquet\] Improve encoding mask API \(wrap bare  i32 in a struct w/ docs\) [\#8588](https://github.com/apache/arrow-rs/issues/8588) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- bench: create `zip` kernel benchmarks [\#8654](https://github.com/apache/arrow-rs/pull/8654) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Skip redundant validation checks in RecordBatch\#project [\#8583](https://github.com/apache/arrow-rs/pull/8583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- \[thrift-remodel\] Remove conversion functions for row group and column metadata [\#8574](https://github.com/apache/arrow-rs/pull/8574) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[PARQUET\] Improve memory efficency for compressed writer parquet 1.0 [\#8527](https://github.com/apache/arrow-rs/pull/8527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lilianm](https://github.com/lilianm))
- perf: improve `GenericByteBuilder::append_array` to use SIMD for extending the offsets [\#8388](https://github.com/apache/arrow-rs/pull/8388) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))

**Closed issues:**

- Utf-8, LargeUtf8, Utf8View [\#8601](https://github.com/apache/arrow-rs/issues/8601)
- \[Variant\] Improve the get type logic for DataType in variant to arrow row builder [\#8538](https://github.com/apache/arrow-rs/issues/8538)
- Add a README.md for arrow-avro [\#8504](https://github.com/apache/arrow-rs/issues/8504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix UnionArray references to "positive" values [\#8418](https://github.com/apache/arrow-rs/issues/8418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] `metadata` field should be marked is non-nullable [\#8410](https://github.com/apache/arrow-rs/issues/8410) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Avro\] Example read\_with\_utf8view.rs fails to run with error "Error: ParseError\("Unexpected EOF while reading Avro header"\)" [\#8380](https://github.com/apache/arrow-rs/issues/8380) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Geospatial\]: Add CI checks for `parquet-geospatial` crate [\#8377](https://github.com/apache/arrow-rs/issues/8377)
- \[Geospatial\] Create new `parquet-geometry` crate [\#8374](https://github.com/apache/arrow-rs/issues/8374)

**Merged pull requests:**

- parquet-rewrite: add write\_batch\_size and compression\_level config  [\#8642](https://github.com/apache/arrow-rs/pull/8642) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- Introduce a ThriftProtocolError to avoid allocating and formattings strings for error messages [\#8636](https://github.com/apache/arrow-rs/pull/8636) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- \[thrift-remodel\] Add macro to reduce boilerplate necessary to implement Thrift serialization [\#8634](https://github.com/apache/arrow-rs/pull/8634) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix Writer docs and rename `AvroBinaryFormat` to `AvroSoeFormat` [\#8633](https://github.com/apache/arrow-rs/pull/8633) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\] Bulk insert elements into List and Object Builders [\#8629](https://github.com/apache/arrow-rs/pull/8629) ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] impl `PartialEq` and `FromIterator<Option<..>>` for `VariantArray` [\#8627](https://github.com/apache/arrow-rs/pull/8627) ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Remove ceremony from iterator of variants into VariantArray [\#8625](https://github.com/apache/arrow-rs/pull/8625) ([friendlymatthew](https://github.com/friendlymatthew))
- Undeprecate `ArrowWriter::into_serialized_writer` and add docs [\#8621](https://github.com/apache/arrow-rs/pull/8621) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix: incorrect assertion in `BitChunks::new` [\#8620](https://github.com/apache/arrow-rs/pull/8620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Variant\] Clean up redundant `get_type_name` [\#8617](https://github.com/apache/arrow-rs/pull/8617) ([liamzwbao](https://github.com/liamzwbao))
- \[Minor\] Hide thrift macros [\#8616](https://github.com/apache/arrow-rs/pull/8616) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Deprecate `parquet::format` module [\#8615](https://github.com/apache/arrow-rs/pull/8615) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[Variant\] Make `VariantArray` iterable [\#8613](https://github.com/apache/arrow-rs/pull/8613) ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Impl `Extend` for `VariantArrayBuilder` [\#8611](https://github.com/apache/arrow-rs/pull/8611) ([friendlymatthew](https://github.com/friendlymatthew))
- build\(deps\): bump actions/setup-node from 5 to 6 [\#8604](https://github.com/apache/arrow-rs/pull/8604) ([dependabot[bot]](https://github.com/apps/dependabot))
- Check int96 min/max instead of panicking [\#8603](https://github.com/apache/arrow-rs/pull/8603) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rambleraptor](https://github.com/rambleraptor))
- \[thrift-remodel\] Refactor Parquet Thrift code into new `thrift` module [\#8599](https://github.com/apache/arrow-rs/pull/8599) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[Parquet\] Remove use of `parquet::format` in metadata bench code [\#8598](https://github.com/apache/arrow-rs/pull/8598) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lichuang](https://github.com/lichuang))
- Remove experimental warning from `extension` module [\#8597](https://github.com/apache/arrow-rs/pull/8597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Adding `try_append_value` implementation to `ByteViewBuilder` [\#8594](https://github.com/apache/arrow-rs/pull/8594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samueleresca](https://github.com/samueleresca))
- Add RecordBatch::project microbenchmark [\#8592](https://github.com/apache/arrow-rs/pull/8592) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- \[parquet\] Add a sync fn to ArrowWriter that flushes Writer [\#8586](https://github.com/apache/arrow-rs/pull/8586) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([PiotrSrebrny](https://github.com/PiotrSrebrny))
- chore: use magic number`FOOTER_SIZE` instead of hard code number [\#8585](https://github.com/apache/arrow-rs/pull/8585) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lichuang](https://github.com/lichuang))
- Add support for run-end encoded \(REE\) arrays in arrow-avro [\#8584](https://github.com/apache/arrow-rs/pull/8584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Unify API for writing column chunks / row groups in parallel [\#8582](https://github.com/apache/arrow-rs/pull/8582) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Fix linting issues missed by \#8506 [\#8581](https://github.com/apache/arrow-rs/pull/8581) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix broken decimal-\>decimal casting with large scale reduction [\#8580](https://github.com/apache/arrow-rs/pull/8580) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- Migrate `arrow` and workspace to Rust 2024 [\#8578](https://github.com/apache/arrow-rs/pull/8578) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Fix doctests of parquet push decoded without default features [\#8577](https://github.com/apache/arrow-rs/pull/8577) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- Avoid panics and warnings when building avro without default features [\#8576](https://github.com/apache/arrow-rs/pull/8576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Add support for 64-bit Schema Registry IDs \(Id64\) in arrow-avro [\#8575](https://github.com/apache/arrow-rs/pull/8575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- fix: bug when struct nullability determined from `Dict<_, ByteArray>>` column [\#8573](https://github.com/apache/arrow-rs/pull/8573) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([albertlockett](https://github.com/albertlockett))
- fix: Support `interleave_struct` to handle empty fields [\#8563](https://github.com/apache/arrow-rs/pull/8563) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- \[Variant\] Define and use VariantDecimalType trait [\#8562](https://github.com/apache/arrow-rs/pull/8562) ([scovich](https://github.com/scovich))
- \[PARQUET\] Update parquet writer bench with compression and pagev2 [\#8560](https://github.com/apache/arrow-rs/pull/8560) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lilianm](https://github.com/lilianm))
- Replace serde with `serde_core` when possible [\#8558](https://github.com/apache/arrow-rs/pull/8558) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- fix: use default field name when name is None in Field conversion [\#8557](https://github.com/apache/arrow-rs/pull/8557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Add arrow-avro README.md file [\#8556](https://github.com/apache/arrow-rs/pull/8556) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- minor\(parquet\): Fix test\_not\_found on Windows [\#8555](https://github.com/apache/arrow-rs/pull/8555) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nuno-faria](https://github.com/nuno-faria))
- \[Parquet\] Avoid fetching multiple pages when the predicate cache is disabled [\#8554](https://github.com/apache/arrow-rs/pull/8554) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nuno-faria](https://github.com/nuno-faria))
- \[Variant\] Support variant to `Decimal32/64/128/256` [\#8552](https://github.com/apache/arrow-rs/pull/8552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Arrow-avro Writer Dense Union support  [\#8550](https://github.com/apache/arrow-rs/pull/8550) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- Arrow-Avro: Resolve named field discrepancies [\#8546](https://github.com/apache/arrow-rs/pull/8546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- Migrate `arrow-avro` to Rust 2024 [\#8545](https://github.com/apache/arrow-rs/pull/8545) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- feat: Export `is_dense` public [\#8544](https://github.com/apache/arrow-rs/pull/8544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Fix "Incorrect Behavior of Collecting a filtered iterator to a BooleanArray" [\#8543](https://github.com/apache/arrow-rs/pull/8543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- Support old syntax for DataType parsing [\#8541](https://github.com/apache/arrow-rs/pull/8541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Decimal unshredding support [\#8540](https://github.com/apache/arrow-rs/pull/8540) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- \[Variant\] Improve documentation and make kernels consistent [\#8536](https://github.com/apache/arrow-rs/pull/8536) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- feat: support casting from null to float16 [\#8535](https://github.com/apache/arrow-rs/pull/8535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chenkovsky](https://github.com/chenkovsky))
- Add benchmarks for FromIter \(PrimitiveArray and BooleanArray\) [\#8525](https://github.com/apache/arrow-rs/pull/8525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- Support writing GeospatialStatistics in Parquet writer [\#8524](https://github.com/apache/arrow-rs/pull/8524) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([paleolimbot](https://github.com/paleolimbot))
- Fix some new rustdoc warnings [\#8522](https://github.com/apache/arrow-rs/pull/8522) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[Variant\] Reverse VariantAsPrimitive trait to PrimitiveFromVariant [\#8519](https://github.com/apache/arrow-rs/pull/8519) ([scovich](https://github.com/scovich))
- \[Variant\] Add variant to arrow primitive support for boolean/timestamp/time [\#8516](https://github.com/apache/arrow-rs/pull/8516) ([klion26](https://github.com/klion26))
- \[Variant\] Add list support to unshred\_variant [\#8514](https://github.com/apache/arrow-rs/pull/8514) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Migrate `parquet-variant-json` to Rust 2024 [\#8512](https://github.com/apache/arrow-rs/pull/8512) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `parquet-variant-compute` to Rust 2024 [\#8511](https://github.com/apache/arrow-rs/pull/8511) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `parquet-variant` to Rust 2024 [\#8510](https://github.com/apache/arrow-rs/pull/8510) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `parquet-geospatial` to Rust 2024 [\#8509](https://github.com/apache/arrow-rs/pull/8509) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `parquet_derive_test` to Rust 2024 [\#8508](https://github.com/apache/arrow-rs/pull/8508) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `parquet_derive` to Rust 2024 [\#8507](https://github.com/apache/arrow-rs/pull/8507) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `parquet` to Rust 2024 [\#8506](https://github.com/apache/arrow-rs/pull/8506) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\] ReadOnlyMetadataBuilder borrows its underlying VariantMetadata [\#8502](https://github.com/apache/arrow-rs/pull/8502) ([scovich](https://github.com/scovich))
- \[Variant\] Add a VariantBuilderExt impl for VariantValueArrayBuilder [\#8501](https://github.com/apache/arrow-rs/pull/8501) ([scovich](https://github.com/scovich))
- build\(deps\): update sysinfo requirement from 0.36.0 to 0.37.1 [\#8500](https://github.com/apache/arrow-rs/pull/8500) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- \[Variant\] Introduce new BorrowedShreddingState concept [\#8499](https://github.com/apache/arrow-rs/pull/8499) ([scovich](https://github.com/scovich))
- Add `append_n` method to `FixedSizeBinaryDictionaryBuilder` [\#8498](https://github.com/apache/arrow-rs/pull/8498) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- Fix docs.rs build: Use `doc_cfg` instead of removed `doc_auto_cfg` [\#8494](https://github.com/apache/arrow-rs/pull/8494) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Remove allow unused from arrow-avro lib.rs file [\#8493](https://github.com/apache/arrow-rs/pull/8493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Regression Testing, Bug Fixes, and Public API Tightening for arrow-avro [\#8492](https://github.com/apache/arrow-rs/pull/8492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Migrate `arrow-string` to Rust 2024 [\#8491](https://github.com/apache/arrow-rs/pull/8491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-select` to Rust 2024 [\#8490](https://github.com/apache/arrow-rs/pull/8490) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-schema` to Rust 2024 [\#8489](https://github.com/apache/arrow-rs/pull/8489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-row` to Rust 2024 [\#8488](https://github.com/apache/arrow-rs/pull/8488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-pyarrow-testing` to Rust 2024 [\#8487](https://github.com/apache/arrow-rs/pull/8487) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-pyarrow-integration-testing` to Rust 2024 [\#8486](https://github.com/apache/arrow-rs/pull/8486) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-pyarrow` to Rust 2024 [\#8485](https://github.com/apache/arrow-rs/pull/8485) ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-ord` to Rust 2024 [\#8484](https://github.com/apache/arrow-rs/pull/8484) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\] Support strict casting for Decimals [\#8483](https://github.com/apache/arrow-rs/pull/8483) ([liamzwbao](https://github.com/liamzwbao))
- feat\(json\): Add temporal formatting options when write to JSON [\#8482](https://github.com/apache/arrow-rs/pull/8482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([linyihai](https://github.com/linyihai))
- \[Variant\] Define and use unshred\_variant function [\#8481](https://github.com/apache/arrow-rs/pull/8481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- \[Minor\] Remove private APIs from Parquet metadata benchmark [\#8478](https://github.com/apache/arrow-rs/pull/8478) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add examples of using `Field::try_extension_type` [\#8475](https://github.com/apache/arrow-rs/pull/8475) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix Rustfmt in arrow-cast [\#8473](https://github.com/apache/arrow-rs/pull/8473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Disable incremental builds in CI [\#8471](https://github.com/apache/arrow-rs/pull/8471) ([mbrobbel](https://github.com/mbrobbel))
- Update Rust toolchain to 1.90 [\#8468](https://github.com/apache/arrow-rs/pull/8468) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Parquet\] Minor: Remove mut ref for getting row-group bloom filter [\#8462](https://github.com/apache/arrow-rs/pull/8462) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- refactor: split `num` dependency [\#8459](https://github.com/apache/arrow-rs/pull/8459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Migrate `arrow-json` to Rust 2024 [\#8458](https://github.com/apache/arrow-rs/pull/8458) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
-  Migrate `arrow-ipc` to Rust 2024 [\#8457](https://github.com/apache/arrow-rs/pull/8457) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-flight` to Rust 2024 [\#8456](https://github.com/apache/arrow-rs/pull/8456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-data` to Rust 2024 [\#8455](https://github.com/apache/arrow-rs/pull/8455) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-csv` to Rust 2024 [\#8454](https://github.com/apache/arrow-rs/pull/8454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-cast` to Rust 2024 [\#8453](https://github.com/apache/arrow-rs/pull/8453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-buffer` to Rust 2024 [\#8452](https://github.com/apache/arrow-rs/pull/8452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-array` to Rust 2024 [\#8450](https://github.com/apache/arrow-rs/pull/8450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Migrate `arrow-arith` to Rust 2024 [\#8449](https://github.com/apache/arrow-rs/pull/8449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Expose `fields` in `StructBuilder` [\#8448](https://github.com/apache/arrow-rs/pull/8448) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lewiszlw](https://github.com/lewiszlw))
- \[Variant\] Simpler shredding state [\#8444](https://github.com/apache/arrow-rs/pull/8444) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Unpin comfytable [\#8440](https://github.com/apache/arrow-rs/pull/8440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Variant integration fixes [\#8438](https://github.com/apache/arrow-rs/pull/8438) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Refactor: extract FooterTail from ParquetMetadataReader [\#8437](https://github.com/apache/arrow-rs/pull/8437) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Refactor: Move parquet metadata parsing code into its own module [\#8436](https://github.com/apache/arrow-rs/pull/8436) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update `UnionArray` wording to 'non-negative' [\#8434](https://github.com/apache/arrow-rs/pull/8434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jdockerty](https://github.com/jdockerty))
- Adds Duration\(TimeUnit\) support to arrow-avro reader and writer [\#8433](https://github.com/apache/arrow-rs/pull/8433) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- Update release schedule [\#8432](https://github.com/apache/arrow-rs/pull/8432) ([mbrobbel](https://github.com/mbrobbel))
- expose read plan and plan builder via mod [\#8431](https://github.com/apache/arrow-rs/pull/8431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yeya24](https://github.com/yeya24))
- Bump MSRV to 1.85 [\#8429](https://github.com/apache/arrow-rs/pull/8429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Fix clippy [\#8426](https://github.com/apache/arrow-rs/pull/8426) ([alamb](https://github.com/alamb))
- Fix red main by updating test [\#8421](https://github.com/apache/arrow-rs/pull/8421) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([emilk](https://github.com/emilk))
- Implement AsRef for Schema and Field [\#8417](https://github.com/apache/arrow-rs/pull/8417) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- \[Variant\] mark metadata field as non-nullable [\#8416](https://github.com/apache/arrow-rs/pull/8416) ([ding-young](https://github.com/ding-young))
- Respect `CastOptions.safe` when casting `BinaryView` → `Utf8View` \(return `null` for invalid UTF‑8\) [\#8415](https://github.com/apache/arrow-rs/pull/8415) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kosiew](https://github.com/kosiew))
- Add Parquet geospatial statistics utility [\#8414](https://github.com/apache/arrow-rs/pull/8414) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([paleolimbot](https://github.com/paleolimbot))
- Remove explicit default cfg option [\#8413](https://github.com/apache/arrow-rs/pull/8413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([abacef](https://github.com/abacef))
- Support parquet canonical extension type roundtrip [\#8409](https://github.com/apache/arrow-rs/pull/8409) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Support reading/writing `VariantArray` to parquet with Variant LogicalType [\#8408](https://github.com/apache/arrow-rs/pull/8408) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Follow-up on arrow-avro Documentation [\#8402](https://github.com/apache/arrow-rs/pull/8402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\]\[Shredding\] Support typed\_access for timestamp\_micro/timestamp\_nano [\#8401](https://github.com/apache/arrow-rs/pull/8401) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([klion26](https://github.com/klion26))
- Expose ReadPlan and ReadPlanBuilder [\#8399](https://github.com/apache/arrow-rs/pull/8399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yeya24](https://github.com/yeya24))
- Propagate errors instead of panics: Replace usages of `new` with `try_new` for Array types [\#8397](https://github.com/apache/arrow-rs/pull/8397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- \[Variant\] Fix NULL handling for shredded object fields [\#8395](https://github.com/apache/arrow-rs/pull/8395) ([scovich](https://github.com/scovich))
- Add Arrow Variant Extension Type, remove  `Array` impl for `VariantArray` and `ShreddedVariantFieldArray` [\#8392](https://github.com/apache/arrow-rs/pull/8392) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor cleanup creating Schema [\#8391](https://github.com/apache/arrow-rs/pull/8391) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Geospatial\]: Add CI checks for `parquet-geospatial` crate [\#8390](https://github.com/apache/arrow-rs/pull/8390) ([kylebarron](https://github.com/kylebarron))
- Follow-up Improvements to Avro union handling  [\#8385](https://github.com/apache/arrow-rs/pull/8385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- fix: reset the offset of 'file\_for\_view' [\#8381](https://github.com/apache/arrow-rs/pull/8381) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([TrevorADHD](https://github.com/TrevorADHD))
- \[Variant\] \[Shredding\] feat: Support typed\_access for Date32 [\#8379](https://github.com/apache/arrow-rs/pull/8379) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([PinkCrow007](https://github.com/PinkCrow007))
- \[Geospatial\]: Scaffolding for new `parquet-geospatial` crate [\#8375](https://github.com/apache/arrow-rs/pull/8375) ([kylebarron](https://github.com/kylebarron))
- Avro writer prefix support [\#8371](https://github.com/apache/arrow-rs/pull/8371) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- \[Variant\] Define new shred\_variant function [\#8366](https://github.com/apache/arrow-rs/pull/8366) ([scovich](https://github.com/scovich))
- Add arrow-avro Reader support for Dense Union and Union resolution \(Part 2\) [\#8349](https://github.com/apache/arrow-rs/pull/8349) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Move ParquetMetadata decoder state machine into ParquetMetadataPushDecoder [\#8340](https://github.com/apache/arrow-rs/pull/8340) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\]: Implement `DataType::FixedSizeList` support for `cast_to_variant` kernel [\#8282](https://github.com/apache/arrow-rs/pull/8282) ([liamzwbao](https://github.com/liamzwbao))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
