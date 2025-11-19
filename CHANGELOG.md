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

## [57.1.0](https://github.com/apache/arrow-rs/tree/57.1.0) (2025-11-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.0.0...57.1.0)

**Implemented enhancements:**

- Eliminate bound checks in filter kernels [\#8865](https://github.com/apache/arrow-rs/issues/8865)
- Respect page index policy option for ParquetObjectReader when it's not skip [\#8856](https://github.com/apache/arrow-rs/issues/8856)
- Speed up collect\_bool and remove `unsafe` [\#8848](https://github.com/apache/arrow-rs/issues/8848)
- Error reading parquet FileMetaData with empty lists encoded as element-type=0 [\#8826](https://github.com/apache/arrow-rs/issues/8826)
- ValueStatistics methods can't be used from generic context in external crate [\#8823](https://github.com/apache/arrow-rs/issues/8823)
- Custom Pretty-Printing Implementation for Column when Formatting Record Batches [\#8821](https://github.com/apache/arrow-rs/issues/8821)
- Parquet-concat: supports bloom filter and page index [\#8804](https://github.com/apache/arrow-rs/issues/8804)
- \[Parquet\] virtual row group number support [\#8800](https://github.com/apache/arrow-rs/issues/8800)
- \[Variant\] Enforce shredded-type validation in `shred_variant` [\#8795](https://github.com/apache/arrow-rs/issues/8795)
- Simplify decision logic to call `FilterBuilder::optimize` or not [\#8781](https://github.com/apache/arrow-rs/issues/8781)
- \[Variant\] Add variant to arrow for DataType::{Binary, LargeBinary, BinaryView} [\#8767](https://github.com/apache/arrow-rs/issues/8767)
- Provide algorithm that allows zipping arrays whose values are not prealigned [\#8752](https://github.com/apache/arrow-rs/issues/8752)
- \[Parquet\] ParquetMetadataReader decodes too much metadata under point-get scenerio [\#8751](https://github.com/apache/arrow-rs/issues/8751) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `arrow-json` supports encoding binary arrays, but not decoding [\#8736](https://github.com/apache/arrow-rs/issues/8736)
- Allow `FilterPredicate` instances to be reused for RecordBatches [\#8692](https://github.com/apache/arrow-rs/issues/8692)
- ArrowJsonBatch::from\_batch is incomplete [\#8684](https://github.com/apache/arrow-rs/issues/8684)
- parquet-layout: More info about layout including footer size, page index, bloom filter? [\#8682](https://github.com/apache/arrow-rs/issues/8682)
- Rewrite `ParquetRecordBatchStream` \(async API\) in terms of the PushDecoder [\#8677](https://github.com/apache/arrow-rs/issues/8677)
- \[JSON\] Add encoding for binary view [\#8674](https://github.com/apache/arrow-rs/issues/8674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor arrow-cast decimal casting to unify the rescale logic used in Parquet variant casts [\#8670](https://github.com/apache/arrow-rs/issues/8670)
- \[Variant\] Support Uuid/`FixedSizeBinary(16)` shredding [\#8665](https://github.com/apache/arrow-rs/issues/8665)
- \[Parquet\]There should be an encoding counter to know how many encodings the repo supports in total [\#8662](https://github.com/apache/arrow-rs/issues/8662)
- Improve `parse_data_type` for `List`, `ListView`, `LargeList`, `LargeListView`, `FixedSizeList`, `Union`, `Map`, `RunEndCoded`. [\#8648](https://github.com/apache/arrow-rs/issues/8648)
- \[Variant\] Support variant to arrow primitive support null/time/decimal\_\* [\#8637](https://github.com/apache/arrow-rs/issues/8637)
- Return error from `RleDecoder::reset` rather than panic [\#8632](https://github.com/apache/arrow-rs/issues/8632)
- Add bitwise ops on `BooleanBufferBuilder` and `MutableBuffer` that mutate directly the buffer [\#8618](https://github.com/apache/arrow-rs/issues/8618)
- \[Variant\] Add variant\_to\_arrow Utf-8, LargeUtf8, Utf8View types support [\#8567](https://github.com/apache/arrow-rs/issues/8567)
- Blog post about new rust Metadata Parser [\#8537](https://github.com/apache/arrow-rs/issues/8537)
- Blog post for arrow 57 [\#8463](https://github.com/apache/arrow-rs/issues/8463)

**Fixed bugs:**

- RowNumber reader has wrong row group ordering [\#8864](https://github.com/apache/arrow-rs/issues/8864)
- `ThriftMetadataWriter::write_column_indexes` cannot handle a `ColumnIndexMetaData::NONE` [\#8815](https://github.com/apache/arrow-rs/issues/8815)
- "Archery test With other arrows" Integration test failing on main: [\#8813](https://github.com/apache/arrow-rs/issues/8813)
- \[Parquet\] Writing in 57.0.0 seems 10% slower than 56.0.0 [\#8783](https://github.com/apache/arrow-rs/issues/8783)
- Parquet reader cannot handle files with unknown logical types [\#8776](https://github.com/apache/arrow-rs/issues/8776)
- zip now treats nulls as false in provided mask regardless of the underlying bit value [\#8721](https://github.com/apache/arrow-rs/issues/8721)
- \[avro\] Incorrect version in crate.io landing page [\#8691](https://github.com/apache/arrow-rs/issues/8691)
- Array: ViewType gc\(\) has bug when array sum length exceed i32::MAX [\#8681](https://github.com/apache/arrow-rs/issues/8681) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet 56: encounter `error: item_reader def levels are None` when reading nested field with row filter [\#8657](https://github.com/apache/arrow-rs/issues/8657) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Degnerate and non-nullable `FixedSizeListArray`s are not handled [\#8623](https://github.com/apache/arrow-rs/issues/8623)
- \[Parquet\]Performance Degradation with RowFilter on Unsorted Columns due to Fragmented ReadPlan [\#8565](https://github.com/apache/arrow-rs/issues/8565)
- ParquetMetaData memory size is not reported accurately when `encryption` is enabled [\#8472](https://github.com/apache/arrow-rs/issues/8472)

**Documentation updates:**

- docs: Add example for creating a `MutableBuffer` from `Buffer` [\#8853](https://github.com/apache/arrow-rs/pull/8853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: Add examples for creating MutableBuffer from Vec [\#8852](https://github.com/apache/arrow-rs/pull/8852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve ParquetDecoder docs [\#8802](https://github.com/apache/arrow-rs/pull/8802) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update docs for zero copy conversion of ScalarBuffer [\#8772](https://github.com/apache/arrow-rs/pull/8772) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add example to convert `PrimitiveArray` to a `Vec` [\#8771](https://github.com/apache/arrow-rs/pull/8771) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: Add links for arrow-avro [\#8770](https://github.com/apache/arrow-rs/pull/8770) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Parquet\] Minor: Update comments in page decompressor [\#8764](https://github.com/apache/arrow-rs/pull/8764) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Document limitations of the `arrow_integration_test` crate [\#8738](https://github.com/apache/arrow-rs/pull/8738) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phil-opp](https://github.com/phil-opp))
- docs: Add link to the Arrow implementation status page [\#8732](https://github.com/apache/arrow-rs/pull/8732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: Update Parquet readme implementation status [\#8731](https://github.com/apache/arrow-rs/pull/8731) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Speed up `collect_bool` and remove `unsafe`, optimize `take_bits`, `take_native` for null values [\#8849](https://github.com/apache/arrow-rs/pull/8849) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Change `BooleanBuffer::append_packed_range` to use `apply_bitwise_binary_op` [\#8812](https://github.com/apache/arrow-rs/pull/8812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Parquet\] Avoid copying `LogicalType` in `ColumnOrder::get_sort_order`, deprecate `get_logical_type` [\#8789](https://github.com/apache/arrow-rs/pull/8789) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- perf: Speed up Parquet file writing \(10%, back to speed of 56\) [\#8786](https://github.com/apache/arrow-rs/pull/8786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- perf: override `ArrayIter` default impl for `nth`, `nth_back`, `last` and `count` [\#8785](https://github.com/apache/arrow-rs/pull/8785) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Parquet\] Reduce one copy in `SerializedPageReader` [\#8745](https://github.com/apache/arrow-rs/pull/8745) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Small optimization in Parquet varint decoder [\#8742](https://github.com/apache/arrow-rs/pull/8742) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- perf: override `count`, `nth`, `nth_back`, `last` and `max` for BitIterator [\#8696](https://github.com/apache/arrow-rs/pull/8696) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Add `FilterPredicate::filter_record_batch` [\#8693](https://github.com/apache/arrow-rs/pull/8693) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- perf: zero-copy path in `RowConverter::from_binary` [\#8686](https://github.com/apache/arrow-rs/pull/8686) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- perf: add optimized zip implementation for scalars [\#8653](https://github.com/apache/arrow-rs/pull/8653) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- feat: add `apply_unary_op` and `apply_binary_op` bitwise operations [\#8619](https://github.com/apache/arrow-rs/pull/8619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Parquet\]Optimize the performance in record reader [\#8607](https://github.com/apache/arrow-rs/pull/8607) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([hhhizzz](https://github.com/hhhizzz))

**Closed issues:**

- Unify display representation for `Field` [\#8784](https://github.com/apache/arrow-rs/issues/8784)
- Misleading configuration name: skip\_arrow\_metadata [\#8780](https://github.com/apache/arrow-rs/issues/8780)
- Inconsistent display for types with Metadata [\#8761](https://github.com/apache/arrow-rs/issues/8761)
- Internal `arrow-integration-test` crate is linked from `arrow` docs [\#8739](https://github.com/apache/arrow-rs/issues/8739)
- Add benchmark for RunEndEncoded casting [\#8709](https://github.com/apache/arrow-rs/issues/8709)
- `RowConverter::from_binary` should opportunistically take ownership of the buffer [\#8685](https://github.com/apache/arrow-rs/issues/8685)
- \[Varaint\] Support `VariantArray::value` to return a `Result<Variant>` [\#8672](https://github.com/apache/arrow-rs/issues/8672)

**Merged pull requests:**

- Speed up filter some more \(up to 2x\) [\#8868](https://github.com/apache/arrow-rs/pull/8868) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make `ArrowReaderOptions::with_virtual_columns` error rather than panic on invalid input [\#8867](https://github.com/apache/arrow-rs/pull/8867) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix errors when reading nested Lists with pushdown predicates. [\#8866](https://github.com/apache/arrow-rs/pull/8866) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix `RowNumberReader` when not all row groups are selected [\#8863](https://github.com/apache/arrow-rs/pull/8863) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([vustef](https://github.com/vustef))
- Respect page index policy option for ParquetObjectReader when it's not skip [\#8857](https://github.com/apache/arrow-rs/pull/8857) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- build\(deps\): update apache-avro requirement from 0.20.0 to 0.21.0 [\#8832](https://github.com/apache/arrow-rs/pull/8832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Allow Users to Provide Custom `ArrayFormatter`s when Pretty-Printing Record Batches [\#8829](https://github.com/apache/arrow-rs/pull/8829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- Allow reading of improperly constructed empty lists in Parquet metadata [\#8827](https://github.com/apache/arrow-rs/pull/8827) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- remove T: ParquetValueType bound on ValueStatistics [\#8824](https://github.com/apache/arrow-rs/pull/8824) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([pmarks](https://github.com/pmarks))
- build\(deps\): update lz4\_flex requirement from 0.11 to 0.12 [\#8820](https://github.com/apache/arrow-rs/pull/8820) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix bug in handling of empty Parquet page index structures [\#8817](https://github.com/apache/arrow-rs/pull/8817) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Parquet-concat: supports page index and bloom filter [\#8811](https://github.com/apache/arrow-rs/pull/8811) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- \[Doc\] Correct `ListArray` documentation [\#8803](https://github.com/apache/arrow-rs/pull/8803) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- \[Parquet\] Add additional docs for `ArrowReaderOptions` and `ArrowReaderMetadata` [\#8798](https://github.com/apache/arrow-rs/pull/8798) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Enforce shredded-type validation in `shred_variant` [\#8796](https://github.com/apache/arrow-rs/pull/8796) ([liamzwbao](https://github.com/liamzwbao))
- Add `VariantPath::is_empty` [\#8791](https://github.com/apache/arrow-rs/pull/8791) ([friendlymatthew](https://github.com/friendlymatthew))
- Add FilterBuilder::is\_optimize\_beneficial [\#8782](https://github.com/apache/arrow-rs/pull/8782) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- \[Parquet\] Allow reading of files with unknown logical types [\#8777](https://github.com/apache/arrow-rs/pull/8777) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- bench: add `ArrayIter` benchmarks [\#8774](https://github.com/apache/arrow-rs/pull/8774) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Update Rust toolchain to 1.91 [\#8769](https://github.com/apache/arrow-rs/pull/8769) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\] Add variant to arrow for `DataType::{Binary/LargeBinary/BinaryView}` [\#8768](https://github.com/apache/arrow-rs/pull/8768) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([klion26](https://github.com/klion26))
-  feat: parse `DataType::Union`, `DataType::Map`, `DataType::RunEndEncoded` [\#8765](https://github.com/apache/arrow-rs/pull/8765) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dqkqd](https://github.com/dqkqd))
- Add options to control various aspects of Parquet metadata decoding [\#8763](https://github.com/apache/arrow-rs/pull/8763) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- feat: Ensure consistent metadata display for data types [\#8760](https://github.com/apache/arrow-rs/pull/8760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mhilton](https://github.com/mhilton))
- Clean up predicate\_cache tests [\#8755](https://github.com/apache/arrow-rs/pull/8755) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- refactor `test_cache_projection_excludes_nested_columns` to use high level APIs [\#8754](https://github.com/apache/arrow-rs/pull/8754) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add `merge` and `merge_n` kernels [\#8753](https://github.com/apache/arrow-rs/pull/8753) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- Fix lint in arrow-flight by updating assert\_cmd after it upgraded [\#8741](https://github.com/apache/arrow-rs/pull/8741) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([vegarsti](https://github.com/vegarsti))
- Remove link to internal `arrow-integration-test` crate from main `arrow` crate [\#8740](https://github.com/apache/arrow-rs/pull/8740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phil-opp](https://github.com/phil-opp))
- Implement hex decoding of JSON strings to binary arrays [\#8737](https://github.com/apache/arrow-rs/pull/8737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phil-opp](https://github.com/phil-opp))
- \[Parquet\] Adaptive Parquet Predicate Pushdown [\#8733](https://github.com/apache/arrow-rs/pull/8733) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([hhhizzz](https://github.com/hhhizzz))
- \[Parquet\] Return error from `RleDecoder::reload` rather than panic [\#8729](https://github.com/apache/arrow-rs/pull/8729) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liamzwbao](https://github.com/liamzwbao))
- fix: `ArrayIter` does not report size hint correctly after advancing from the iterator back [\#8728](https://github.com/apache/arrow-rs/pull/8728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: Use Vec::with\_capacity in cast\_to\_run\_end\_encoded [\#8726](https://github.com/apache/arrow-rs/pull/8726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vegarsti](https://github.com/vegarsti))
- \[Variant\] Fix the index of an item in VariantArray in a unit test [\#8725](https://github.com/apache/arrow-rs/pull/8725) ([martin-g](https://github.com/martin-g))
- build\(deps\): bump actions/download-artifact from 5 to 6 [\#8720](https://github.com/apache/arrow-rs/pull/8720) ([dependabot[bot]](https://github.com/apps/dependabot))
- \[Variant\] Add try\_value/value for VariantArray [\#8719](https://github.com/apache/arrow-rs/pull/8719) ([klion26](https://github.com/klion26))
- General virtual columns support + row numbers as a first use-case [\#8715](https://github.com/apache/arrow-rs/pull/8715) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([vustef](https://github.com/vustef))
- feat: Parquet-layout add Index and Footer info [\#8712](https://github.com/apache/arrow-rs/pull/8712) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- fix: `zip` now treats nulls as false in provided mask regardless of the underlying bit value [\#8711](https://github.com/apache/arrow-rs/pull/8711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Add benchmark for casting to RunEndEncoded \(REE\) [\#8710](https://github.com/apache/arrow-rs/pull/8710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vegarsti](https://github.com/vegarsti))
- \[Minor\]: Document visibility for enums produced by Thrift macros [\#8706](https://github.com/apache/arrow-rs/pull/8706) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update `arrow-avro` `README.md` version to 57 [\#8695](https://github.com/apache/arrow-rs/pull/8695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Fix: ViewType gc on huge batch would produce bad output [\#8694](https://github.com/apache/arrow-rs/pull/8694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))
- Refactor arrow-cast decimal casting to unify the rescale logic used in Parquet variant casts [\#8689](https://github.com/apache/arrow-rs/pull/8689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- check bit width to avoid panic in DeltaBitPackDecoder [\#8688](https://github.com/apache/arrow-rs/pull/8688) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rambleraptor](https://github.com/rambleraptor))
- \[thrift-remodel\] Use `thrift_enum` macro for `ConvertedType` [\#8680](https://github.com/apache/arrow-rs/pull/8680) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[JSON\] Map key supports utf8 view [\#8679](https://github.com/apache/arrow-rs/pull/8679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))
- \[JSON\] Add encoding for binary view [\#8675](https://github.com/apache/arrow-rs/pull/8675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))
- \[Parquet\] Account for FileDecryptor in ParquetMetaData heap size calculation [\#8671](https://github.com/apache/arrow-rs/pull/8671) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- chore: update `OffsetBuffer::from_lengths(std::iter::repeat_n(<val>, <repeat>));` with `OffsetBuffer::from_repeated_length(<val>, <repeat>);` [\#8669](https://github.com/apache/arrow-rs/pull/8669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Variant\] Support `shred_variant` for Uuids [\#8666](https://github.com/apache/arrow-rs/pull/8666) ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Remove `create_test_variant_array` helper method [\#8664](https://github.com/apache/arrow-rs/pull/8664) ([friendlymatthew](https://github.com/friendlymatthew))
- \[parquet\] Adding counting method in thrift\_enum macro to support  ENCODING\_SLOTS [\#8663](https://github.com/apache/arrow-rs/pull/8663) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([hhhizzz](https://github.com/hhhizzz))
- chore: add test case of RowSelection::trim [\#8660](https://github.com/apache/arrow-rs/pull/8660) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lichuang](https://github.com/lichuang))
- feat: add `new_repeated` to `ByteArray` [\#8659](https://github.com/apache/arrow-rs/pull/8659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: add `repeat_slice_n_times` to `MutableBuffer` [\#8658](https://github.com/apache/arrow-rs/pull/8658) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: add optimized function to create offset with same length [\#8656](https://github.com/apache/arrow-rs/pull/8656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Variant\] `rescale_decimal` followup [\#8655](https://github.com/apache/arrow-rs/pull/8655) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- feat: parse DataType `List`, `ListView`, `LargeList`, `LargeListView`, `FixedSizeList` [\#8649](https://github.com/apache/arrow-rs/pull/8649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dqkqd](https://github.com/dqkqd))
- Support more operations on ListView [\#8645](https://github.com/apache/arrow-rs/pull/8645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- \[Variant\] Implement primitive type access for null/time/decimal\* [\#8638](https://github.com/apache/arrow-rs/pull/8638) ([klion26](https://github.com/klion26))
- \[Variant\] refactor: Split builder.rs into several smaller files [\#8635](https://github.com/apache/arrow-rs/pull/8635) ([Weijun-H](https://github.com/Weijun-H))
- add `try_new_with_length` constructor to `FixedSizeList` [\#8624](https://github.com/apache/arrow-rs/pull/8624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([connortsui20](https://github.com/connortsui20))
- Change some panics to errors in parquet decoder [\#8602](https://github.com/apache/arrow-rs/pull/8602) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rambleraptor](https://github.com/rambleraptor))
- Support `variant_to_arrow` for utf8 [\#8600](https://github.com/apache/arrow-rs/pull/8600) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sdf-jkl](https://github.com/sdf-jkl))
- Cast support for RunEndEncoded arrays [\#8589](https://github.com/apache/arrow-rs/pull/8589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vegarsti](https://github.com/vegarsti))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
