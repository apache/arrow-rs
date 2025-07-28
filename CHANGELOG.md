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

## [56.0.0](https://github.com/apache/arrow-rs/tree/56.0.0) (2025-07-28)

[Full Changelog](https://github.com/apache/arrow-rs/compare/55.2.0...56.0.0)

**Breaking changes:**

- arrow-schema: Remove dict\_id from being required equal for merging [\#7968](https://github.com/apache/arrow-rs/pull/7968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- \[Parquet\] Use `u64` for `SerializedPageReaderState.offset` & `remaining_bytes`, instead of `usize` [\#7918](https://github.com/apache/arrow-rs/pull/7918) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([JigaoLuo](https://github.com/JigaoLuo))
- Upgrade tonic dependencies to 0.13.0 version \(try 2\) [\#7839](https://github.com/apache/arrow-rs/pull/7839) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Remove deprecated Arrow functions [\#7830](https://github.com/apache/arrow-rs/pull/7830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([etseidl](https://github.com/etseidl))
- Remove deprecated temporal functions [\#7813](https://github.com/apache/arrow-rs/pull/7813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Remove functions from parquet crate deprecated in or before 54.0.0 [\#7811](https://github.com/apache/arrow-rs/pull/7811) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- GH-7686: \[Parquet\] Fix int96 min/max stats [\#7687](https://github.com/apache/arrow-rs/pull/7687) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rahulketch](https://github.com/rahulketch))

**Implemented enhancements:**

- Support casting int64 to interval [\#7988](https://github.com/apache/arrow-rs/issues/7988) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Add `ListBuilder::with_value` for convenience [\#7951](https://github.com/apache/arrow-rs/issues/7951) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Add  `ObjectBuilder::with_field` for convenience [\#7949](https://github.com/apache/arrow-rs/issues/7949) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Impl PartialEq for VariantObject \#7943 [\#7948](https://github.com/apache/arrow-rs/issues/7948)
- \[Variant\] Offer `simdutf8` as an optional dependency when validating metadata [\#7902](https://github.com/apache/arrow-rs/issues/7902) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Avoid collecting offset iterator [\#7901](https://github.com/apache/arrow-rs/issues/7901) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Remove superfluous check when validating monotonic offsets [\#7900](https://github.com/apache/arrow-rs/issues/7900) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Avoid extra allocation in `ObjectBuilder` [\#7899](https://github.com/apache/arrow-rs/issues/7899) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]\[Compute\] `variant_get` kernel [\#7893](https://github.com/apache/arrow-rs/issues/7893) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]\[Compute\] Add batch processing for Variant-JSON String conversion [\#7883](https://github.com/apache/arrow-rs/issues/7883) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `MapArray` in lexsort [\#7881](https://github.com/apache/arrow-rs/issues/7881) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Add testing for invalid variants \(fuzz testing??\) [\#7842](https://github.com/apache/arrow-rs/issues/7842) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] VariantMetadata, VariantList and VariantObject are too big for Copy [\#7831](https://github.com/apache/arrow-rs/issues/7831) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Tests for creating "large" `VariantObjects`s [\#7821](https://github.com/apache/arrow-rs/issues/7821) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Tests for creating "large" `VariantList`s [\#7820](https://github.com/apache/arrow-rs/issues/7820) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Support VariantBuilder to write to buffers owned by the caller [\#7805](https://github.com/apache/arrow-rs/issues/7805) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Move JSON related functionality to different crate. [\#7800](https://github.com/apache/arrow-rs/issues/7800) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Add flag in `ObjectBuilder` to control validation behavior on duplicate field write [\#7777](https://github.com/apache/arrow-rs/issues/7777) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] make `serde_json` an optional dependency of `parquet-variant` [\#7775](https://github.com/apache/arrow-rs/issues/7775) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Clickbench microbenchmark spends significant time in memcmp for not\_empty predicate [\#7766](https://github.com/apache/arrow-rs/issues/7766) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[coalesce\] Implement specialized `BatchCoalescer::push_batch` for `PrimitiveArray` [\#7763](https://github.com/apache/arrow-rs/issues/7763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add sort\_kernel benchmark for StringViewArray case [\#7758](https://github.com/apache/arrow-rs/issues/7758) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Improved API for accessing Variant Objects and lists [\#7756](https://github.com/apache/arrow-rs/issues/7756) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Buildable reproducible release builds [\#7751](https://github.com/apache/arrow-rs/issues/7751)
- Allow per-column parquet dictionary page size limit [\#7723](https://github.com/apache/arrow-rs/issues/7723) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Test and implement efficient building for "large" Arrays [\#7699](https://github.com/apache/arrow-rs/issues/7699) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Improve VariantBuilder when creating field name dictionaries / sorted dictionaries [\#7698](https://github.com/apache/arrow-rs/issues/7698) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Add input validation in `VariantBuilder` [\#7697](https://github.com/apache/arrow-rs/issues/7697) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Support Nested Data in `VariantBuilder` [\#7696](https://github.com/apache/arrow-rs/issues/7696) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: Incorrect min/max stats for int96 columns [\#7686](https://github.com/apache/arrow-rs/issues/7686) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `DictionaryArray::gc` method [\#7683](https://github.com/apache/arrow-rs/issues/7683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Add negative tests for reading invalid primitive variant values [\#7645](https://github.com/apache/arrow-rs/issues/7645) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- \[Variant\] Panic when appending nested objects to VariantBuilder [\#7907](https://github.com/apache/arrow-rs/issues/7907) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Panic when casting large Decimal256 to f64 due to unchecked `unwrap()`  [\#7886](https://github.com/apache/arrow-rs/issues/7886) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect inlined string view comparison after " Add prefix compare for inlined" [\#7874](https://github.com/apache/arrow-rs/issues/7874) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] `test_json_to_variant_object_very_large` takes over 20s [\#7872](https://github.com/apache/arrow-rs/issues/7872) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] If `ObjectBuilder::finalize` is not called, the resulting Variant object is malformed. [\#7863](https://github.com/apache/arrow-rs/issues/7863) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- CSV error message has values transposed [\#7848](https://github.com/apache/arrow-rs/issues/7848) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Concating struct arrays with no fields unnecessarily errors [\#7828](https://github.com/apache/arrow-rs/issues/7828) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clippy CI is failing on main after Rust `1.88` upgrade [\#7796](https://github.com/apache/arrow-rs/issues/7796) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[Variant\] Field lookup with out of bounds index causes unwanted behavior [\#7784](https://github.com/apache/arrow-rs/issues/7784) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error verifying `parquet-variant` crate on 55.2.0 with `verify-release-candidate.sh` [\#7746](https://github.com/apache/arrow-rs/issues/7746)
- `test_to_pyarrow` tests fail during release verification [\#7736](https://github.com/apache/arrow-rs/issues/7736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\_derive\] Example for ParquetRecordWriter is broken. [\#7732](https://github.com/apache/arrow-rs/issues/7732)
- \[Variant\] `Variant::Object` can contain two fields with the same field name [\#7730](https://github.com/apache/arrow-rs/issues/7730) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Panic when appending Object or List to VariantBuilder [\#7701](https://github.com/apache/arrow-rs/issues/7701) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Slicing a single-field dense union array creates an array with incorrect `logical_nulls` length  [\#7647](https://github.com/apache/arrow-rs/issues/7647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Ensure page encoding statistics are written to Parquet file [\#7643](https://github.com/apache/arrow-rs/pull/7643) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))

**Documentation updates:**

- Minor: Upate `cast_with_options` docs about casting integers --\> intervals [\#8002](https://github.com/apache/arrow-rs/pull/8002) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: More docs to `BatchCoalescer` [\#7891](https://github.com/apache/arrow-rs/pull/7891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([2010YOUY01](https://github.com/2010YOUY01))
- chore: fix a typo in `ExtensionType::supports_data_type` docs [\#7682](https://github.com/apache/arrow-rs/pull/7682) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\] Add variant docs and examples [\#7661](https://github.com/apache/arrow-rs/pull/7661) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Add version to deprecation notice for `ParquetMetaDataReader::decode_footer` [\#7639](https://github.com/apache/arrow-rs/pull/7639) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))

**Performance improvements:**

- `RowConverter` on list should only encode the sliced list values and not the entire data [\#7993](https://github.com/apache/arrow-rs/issues/7993) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Avoid extra allocation in list builder [\#7977](https://github.com/apache/arrow-rs/issues/7977) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Convert JSON to Variant with fewer copies [\#7964](https://github.com/apache/arrow-rs/issues/7964) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize sort kernels partition\_validity method [\#7936](https://github.com/apache/arrow-rs/issues/7936) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speedup sorting for inline views [\#7857](https://github.com/apache/arrow-rs/issues/7857) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Perf: Investigate and improve parquet writing performance [\#7822](https://github.com/apache/arrow-rs/issues/7822) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Perf: optimize sort string\_view performance [\#7790](https://github.com/apache/arrow-rs/issues/7790) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add efficient way to upgrade keys for additional dictionary builders [\#7654](https://github.com/apache/arrow-rs/issues/7654) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Perf: Make sort string view fast\(1.5X ~ 3X faster\) [\#7792](https://github.com/apache/arrow-rs/pull/7792) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Add specialized coalesce path for PrimitiveArrays [\#7772](https://github.com/apache/arrow-rs/pull/7772) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

-  `validated` and `is_fully_validated` flags  doesn't need to be part of PartialEq [\#7952](https://github.com/apache/arrow-rs/issues/7952) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] remove VariantMetadata::dictionary\_size [\#7947](https://github.com/apache/arrow-rs/issues/7947) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Improve `VariantArray` performance by storing the index of the metadata and value arrays [\#7920](https://github.com/apache/arrow-rs/issues/7920)
- \[Variant\] Converting variant to JSON string seems slow [\#7869](https://github.com/apache/arrow-rs/issues/7869) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Present Variant at Iceberg Summit NYC July 10, 2025 [\#7858](https://github.com/apache/arrow-rs/issues/7858)
- Allow choosing flate2 backend [\#7826](https://github.com/apache/arrow-rs/issues/7826) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Avoid second copy of field name in MetadataBuilder [\#7814](https://github.com/apache/arrow-rs/issues/7814) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove APIs deprecated in or before 54.0.0 [\#7810](https://github.com/apache/arrow-rs/issues/7810) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[Variant\] Make it harder to forget to finish a pending parent i n ObjectBuilder [\#7798](https://github.com/apache/arrow-rs/issues/7798) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Remove explicit ObjectBuilder::finish\(\) and ListBuilder::finish and move to `Drop` impl [\#7780](https://github.com/apache/arrow-rs/issues/7780) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use prefix first for comparisons, resort to data buffer for remaining data on equal values [\#7744](https://github.com/apache/arrow-rs/issues/7744)
- Change use of `inline_value`  to inline it to a u128 [\#7743](https://github.com/apache/arrow-rs/issues/7743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reduce repetition in tests for arrow-row/src/run.rs [\#7692](https://github.com/apache/arrow-rs/issues/7692) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Add tests for invalid variant values \(aka verify invalid inputs\) [\#7681](https://github.com/apache/arrow-rs/issues/7681) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Introduce structs for Variant::Decimal types  [\#7660](https://github.com/apache/arrow-rs/issues/7660) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- bench: add benchmark for converting list and sliced list to row format [\#8008](https://github.com/apache/arrow-rs/pull/8008) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- bench: benchmark interleave structs [\#8007](https://github.com/apache/arrow-rs/pull/8007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- doc: remove outdated info from CONTRIBUTING doc in project root dir. [\#7998](https://github.com/apache/arrow-rs/pull/7998) ([sonhmai](https://github.com/sonhmai))
- perf: only encode actual list values in `RowConverter` \(16-26 times faster for small sliced list\) [\#7996](https://github.com/apache/arrow-rs/pull/7996) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- test: add tests for converting sliced list to row based [\#7994](https://github.com/apache/arrow-rs/pull/7994) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- perf: Improve `interleave` performance for struct \(3-6 times faster\) [\#7991](https://github.com/apache/arrow-rs/pull/7991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Variant\] Avoid extra buffer allocation in ListBuilder [\#7987](https://github.com/apache/arrow-rs/pull/7987) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([klion26](https://github.com/klion26))
- Minor: Restore warning comment on Int96 statistics read [\#7975](https://github.com/apache/arrow-rs/pull/7975) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add additional integration tests to arrow-avro [\#7974](https://github.com/apache/arrow-rs/pull/7974) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- Perf: optimize actual\_buffer\_size to use only data buffer capacity for coalesce [\#7967](https://github.com/apache/arrow-rs/pull/7967) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Implement Improved arrow-avro Reader Zero-Byte Record Handling [\#7966](https://github.com/apache/arrow-rs/pull/7966) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\] Revisit VariantMetadata and Object equality [\#7961](https://github.com/apache/arrow-rs/pull/7961) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Add ListBuilder::with\_value for convenience [\#7959](https://github.com/apache/arrow-rs/pull/7959) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([codephage2020](https://github.com/codephage2020))
- \[Variant\] remove VariantMetadata::dictionary\_size [\#7958](https://github.com/apache/arrow-rs/pull/7958) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([codephage2020](https://github.com/codephage2020))
- \[Variant\] VariantMetadata is allowed to contain the empty string [\#7956](https://github.com/apache/arrow-rs/pull/7956) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Add arrow-avro support for Impala Nullability [\#7954](https://github.com/apache/arrow-rs/pull/7954) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([veronica-m-ef](https://github.com/veronica-m-ef))
- \[Test\] Add tests for VariantList equality [\#7953](https://github.com/apache/arrow-rs/pull/7953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Add ObjectBuilder::with\_field for convenience [\#7950](https://github.com/apache/arrow-rs/pull/7950) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Adding code to store metadata and value references in VariantArray [\#7945](https://github.com/apache/arrow-rs/pull/7945) ([abacef](https://github.com/abacef))
- \[Variant\] Add `variant_kernels` benchmark [\#7944](https://github.com/apache/arrow-rs/pull/7944) ([alamb](https://github.com/alamb))
- \[Variant\] Impl `PartialEq` for VariantObject [\#7943](https://github.com/apache/arrow-rs/pull/7943) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Add documentation, tests and cleaner api for Variant::get\_path [\#7942](https://github.com/apache/arrow-rs/pull/7942) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- arrow-ipc: Remove all abilities to preserve dict IDs [\#7940](https://github.com/apache/arrow-rs/pull/7940) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([brancz](https://github.com/brancz))
- Optimize partition\_validity function used in sort kernels [\#7937](https://github.com/apache/arrow-rs/pull/7937) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- \[Variant\] Avoid extra allocation in object builder [\#7935](https://github.com/apache/arrow-rs/pull/7935) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([klion26](https://github.com/klion26))
- \[Variant\] Avoid collecting offset iterator [\#7934](https://github.com/apache/arrow-rs/pull/7934) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([codephage2020](https://github.com/codephage2020))
- Minor: Support BinaryView and StringView builders in `make_builder` [\#7931](https://github.com/apache/arrow-rs/pull/7931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- chore: bump MSRV to 1.84 [\#7926](https://github.com/apache/arrow-rs/pull/7926) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Update bzip2 requirement from 0.4.4 to 0.6.0 [\#7924](https://github.com/apache/arrow-rs/pull/7924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\] Reserve capacity beforehand during large object building [\#7922](https://github.com/apache/arrow-rs/pull/7922) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Add `variant_get` compute kernel [\#7919](https://github.com/apache/arrow-rs/pull/7919) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Samyak2](https://github.com/Samyak2))
- Restructure compare\_greater function used in parquet statistics for better performance [\#7916](https://github.com/apache/arrow-rs/pull/7916) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- \[Variant\] Support appending complex variants in `VariantBuilder` [\#7914](https://github.com/apache/arrow-rs/pull/7914) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Add `VariantBuilder::new_with_buffers` to write to existing buffers [\#7912](https://github.com/apache/arrow-rs/pull/7912) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Convert JSON to VariantArray without copying \(8 - 32% faster\) [\#7911](https://github.com/apache/arrow-rs/pull/7911) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Use simdutf8 for UTF-8 validation [\#7908](https://github.com/apache/arrow-rs/pull/7908) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- \[Variant\] Avoid superflous validation checks [\#7906](https://github.com/apache/arrow-rs/pull/7906) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- Add `VariantArray` and `VariantArrayBuilder` for constructing Arrow Arrays of Variants [\#7905](https://github.com/apache/arrow-rs/pull/7905) ([alamb](https://github.com/alamb))
- Update sysinfo requirement from 0.35.0 to 0.36.0 [\#7904](https://github.com/apache/arrow-rs/pull/7904) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix current CI failure [\#7898](https://github.com/apache/arrow-rs/pull/7898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove redundant is\_err checks in Variant tests [\#7897](https://github.com/apache/arrow-rs/pull/7897) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- \[Variant\] test: add variant object tests with different sizes [\#7896](https://github.com/apache/arrow-rs/pull/7896) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([odysa](https://github.com/odysa))
- \[Variant\] Define basic convenience methods for variant pathing [\#7894](https://github.com/apache/arrow-rs/pull/7894) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- fix: `view_types` benchmark slice should follow by correct len array [\#7892](https://github.com/apache/arrow-rs/pull/7892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Add arrow-avro support for bzip2 and xz compression [\#7890](https://github.com/apache/arrow-rs/pull/7890) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Add arrow-avro support for Duration type and minor fixes for UUID decoding [\#7889](https://github.com/apache/arrow-rs/pull/7889) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\] Reduce variant-related struct sizes [\#7888](https://github.com/apache/arrow-rs/pull/7888) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Fix panic on lossy decimal to float casting: round to saturation for overflows  [\#7887](https://github.com/apache/arrow-rs/pull/7887) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kosiew](https://github.com/kosiew))
- Add tests for invalid variant metadata and value [\#7885](https://github.com/apache/arrow-rs/pull/7885) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- \[Variant\] Introduce parquet-variant-compute crate to transform batches of JSON strings to and from Variants [\#7884](https://github.com/apache/arrow-rs/pull/7884) ([harshmotw-db](https://github.com/harshmotw-db))
- feat: support `MapArray` in lexsort [\#7882](https://github.com/apache/arrow-rs/pull/7882) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- fix: mark `DataType::Map` as unsupported in `RowConverter` [\#7880](https://github.com/apache/arrow-rs/pull/7880) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[Variant\] Speedup validation [\#7878](https://github.com/apache/arrow-rs/pull/7878) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- benchmark: Add StringViewArray gc benchmark with not null cases [\#7877](https://github.com/apache/arrow-rs/pull/7877) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- \[ARROW-RS-7820\]\[Variant\] Add tests for large variant lists [\#7876](https://github.com/apache/arrow-rs/pull/7876) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([klion26](https://github.com/klion26))
- fix: Incorrect inlined string view comparison after Add prefix compar… [\#7875](https://github.com/apache/arrow-rs/pull/7875) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- perf: speed up StringViewArray gc 1.4 ~5.x faster [\#7873](https://github.com/apache/arrow-rs/pull/7873) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- \[Variant\] Remove superflous validate call and rename methods [\#7871](https://github.com/apache/arrow-rs/pull/7871) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- Benchmark: Add rich testing cases for sort string\(utf8\) [\#7867](https://github.com/apache/arrow-rs/pull/7867) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- chore: update link for `row_filter.rs` [\#7866](https://github.com/apache/arrow-rs/pull/7866) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([haohuaijin](https://github.com/haohuaijin))
- \[Variant\] List and object builders have no effect until finalized [\#7865](https://github.com/apache/arrow-rs/pull/7865) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Added number to string benches for json\_writer [\#7864](https://github.com/apache/arrow-rs/pull/7864) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([abacef](https://github.com/abacef))
- \[Variant\] Introduce `parquet-variant-json` crate [\#7862](https://github.com/apache/arrow-rs/pull/7862) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Remove dead code, add comments [\#7861](https://github.com/apache/arrow-rs/pull/7861) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Speedup sorting for inline views: 1.4x - 1.7x improvement [\#7856](https://github.com/apache/arrow-rs/pull/7856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Fix union slice logical\_nulls length [\#7855](https://github.com/apache/arrow-rs/pull/7855) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- Add `get_ref/get_mut` to JSON Writer [\#7854](https://github.com/apache/arrow-rs/pull/7854) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([cetra3](https://github.com/cetra3))
- \[Minor\] Add Benchmark for RowConverter::append [\#7853](https://github.com/apache/arrow-rs/pull/7853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Add Enum type support to arrow-avro and Minor Decimal type fix [\#7852](https://github.com/apache/arrow-rs/pull/7852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- CSV error message has values transposed [\#7851](https://github.com/apache/arrow-rs/pull/7851) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Omega359](https://github.com/Omega359))
- \[Variant\]   Fuzz testing and benchmarks for vaildation [\#7849](https://github.com/apache/arrow-rs/pull/7849) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([carpecodeum](https://github.com/carpecodeum))
- \[Variant\] Follow up nits and uncomment test cases [\#7846](https://github.com/apache/arrow-rs/pull/7846) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Make sure ObjectBuilder and ListBuilder to be finalized before its parent builder [\#7843](https://github.com/apache/arrow-rs/pull/7843) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add decimal32 and decimal64 support to Parquet, JSON and CSV readers and writers [\#7841](https://github.com/apache/arrow-rs/pull/7841) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([CurtHagenlocher](https://github.com/CurtHagenlocher))
- Implement arrow-avro Reader and ReaderBuilder [\#7834](https://github.com/apache/arrow-rs/pull/7834) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\] Support creating sorted dictionaries [\#7833](https://github.com/apache/arrow-rs/pull/7833) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- Add Decimal type support to arrow-avro  [\#7832](https://github.com/apache/arrow-rs/pull/7832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Allow concating struct arrays with no fields [\#7829](https://github.com/apache/arrow-rs/pull/7829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- Add features to configure flate2 [\#7827](https://github.com/apache/arrow-rs/pull/7827) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- make builder public under experimental [\#7825](https://github.com/apache/arrow-rs/pull/7825) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Improvements for parquet writing performance \(25%-44%\) [\#7824](https://github.com/apache/arrow-rs/pull/7824) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Use in-memory buffer for arrow\_writer benchmark [\#7823](https://github.com/apache/arrow-rs/pull/7823) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- \[Variant\] impl \[Try\]From for VariantDecimalXX types [\#7809](https://github.com/apache/arrow-rs/pull/7809) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- \[Variant\] Speedup `ObjectBuilder` \(62x faster\) [\#7808](https://github.com/apache/arrow-rs/pull/7808) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[VARIANT\] Support both fallible and infallible access to variants [\#7807](https://github.com/apache/arrow-rs/pull/7807) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Minor: fix clippy in parquet-variant after logical conflict [\#7803](https://github.com/apache/arrow-rs/pull/7803) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Add flag in `ObjectBuilder` to control validation behavior on duplicate field write [\#7801](https://github.com/apache/arrow-rs/pull/7801) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([micoo227](https://github.com/micoo227))
- Fix clippy for Rust 1.88 release [\#7797](https://github.com/apache/arrow-rs/pull/7797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- \[Variant\] Simplify `Builder` buffer operations [\#7795](https://github.com/apache/arrow-rs/pull/7795) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- fix: Change panic to error in`take` kernel for StringArrary/BinaryArray on overflow [\#7793](https://github.com/apache/arrow-rs/pull/7793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chenkovsky](https://github.com/chenkovsky))
- Update base64 requirement from 0.21 to 0.22 [\#7791](https://github.com/apache/arrow-rs/pull/7791) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix RowConverter when FixedSizeList is not the last [\#7789](https://github.com/apache/arrow-rs/pull/7789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Add schema with only primitive arrays to `coalesce_kernel` benchmark [\#7788](https://github.com/apache/arrow-rs/pull/7788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add sort\_kernel benchmark for StringViewArray case [\#7787](https://github.com/apache/arrow-rs/pull/7787) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- \[Variant\] Check pending before `VariantObject::insert` [\#7786](https://github.com/apache/arrow-rs/pull/7786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[VARIANT\] impl Display for VariantDecimalXX [\#7785](https://github.com/apache/arrow-rs/pull/7785) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- \[VARIANT\] Add support for the json\_to\_variant API [\#7783](https://github.com/apache/arrow-rs/pull/7783) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([harshmotw-db](https://github.com/harshmotw-db))
- \[Variant\] Consolidate examples for json writing [\#7782](https://github.com/apache/arrow-rs/pull/7782) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add benchmark for about view array slice [\#7781](https://github.com/apache/arrow-rs/pull/7781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ctsk](https://github.com/ctsk))
- \[Variant\] Add negative tests for reading invalid primitive variant values [\#7779](https://github.com/apache/arrow-rs/pull/7779) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([superserious-dev](https://github.com/superserious-dev))
- \[Variant\] Support creating nested objects and object with lists [\#7778](https://github.com/apache/arrow-rs/pull/7778) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[VARIANT\] Validate precision in VariantDecimalXX structs and add missing tests [\#7776](https://github.com/apache/arrow-rs/pull/7776) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Add tests for `BatchCoalescer::push_batch_with_filter`, fix bug [\#7774](https://github.com/apache/arrow-rs/pull/7774) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Minor: make fields in `VariantDecimal*` private, add examples [\#7770](https://github.com/apache/arrow-rs/pull/7770) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Extend the fast path in GenericByteViewArray::is\_eq for comparing against empty strings [\#7767](https://github.com/apache/arrow-rs/pull/7767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- \[Variant\] Improve getter API for `VariantList` and `VariantObject` [\#7757](https://github.com/apache/arrow-rs/pull/7757) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Add Variant::as\_object and Variant::as\_list [\#7755](https://github.com/apache/arrow-rs/pull/7755) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Fix several overflow panic risks for 32-bit arch [\#7752](https://github.com/apache/arrow-rs/pull/7752) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Add testing section to pull request template [\#7749](https://github.com/apache/arrow-rs/pull/7749) ([alamb](https://github.com/alamb))
- Perf: Add prefix compare for inlined compare and change use of inline\_value to inline it to a u128  [\#7748](https://github.com/apache/arrow-rs/pull/7748) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Move arrow-pyarrow tests that require `pyarrow` to be installed into `arrow-pyarrow-testing` crate [\#7742](https://github.com/apache/arrow-rs/pull/7742) ([alamb](https://github.com/alamb))
- \[Variant\] Improve write API in `Variant::Object` [\#7741](https://github.com/apache/arrow-rs/pull/7741) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- \[Variant\] Support nested lists and object lists [\#7740](https://github.com/apache/arrow-rs/pull/7740) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- feat: \[Variant\] Add Validation for Variant Deciaml [\#7738](https://github.com/apache/arrow-rs/pull/7738) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Weijun-H](https://github.com/Weijun-H))
- Add fallible versions of temporal functions that may panic [\#7737](https://github.com/apache/arrow-rs/pull/7737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- fix: Implement support for appending Object and List variants in VariantBuilder [\#7735](https://github.com/apache/arrow-rs/pull/7735) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Weijun-H](https://github.com/Weijun-H))
- parquet\_derive: update in working example for ParquetRecordWriter [\#7733](https://github.com/apache/arrow-rs/pull/7733) ([LanHikari22](https://github.com/LanHikari22))
- Perf: Optimize comparison kernels for inlined views [\#7731](https://github.com/apache/arrow-rs/pull/7731) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- arrow-row: Refactor arrow-row REE roundtrip tests [\#7729](https://github.com/apache/arrow-rs/pull/7729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- arrow-array: Implement PartialEq for RunArray [\#7727](https://github.com/apache/arrow-rs/pull/7727) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- fix: Do not add null buffer for `NullArray` in MutableArrayData [\#7726](https://github.com/apache/arrow-rs/pull/7726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Allow per-column parquet dictionary page size limit [\#7724](https://github.com/apache/arrow-rs/pull/7724) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- fix JSON decoder error checking for UTF16 / surrogate parsing panic [\#7721](https://github.com/apache/arrow-rs/pull/7721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nicklan](https://github.com/nicklan))
- \[Variant\] Use `BTreeMap` for `VariantBuilder.dict` and `ObjectBuilder.fields` to maintain invariants upon entry writes [\#7720](https://github.com/apache/arrow-rs/pull/7720) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- Introduce `MAX_INLINE_VIEW_LEN` constant for string/byte views [\#7719](https://github.com/apache/arrow-rs/pull/7719) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Introduce new type over &str for ShortString [\#7718](https://github.com/apache/arrow-rs/pull/7718) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([friendlymatthew](https://github.com/friendlymatthew))
- Split out variant code into several new sub-modules [\#7717](https://github.com/apache/arrow-rs/pull/7717) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- add `garbage_collect_dictionary` to `arrow-select` [\#7716](https://github.com/apache/arrow-rs/pull/7716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([davidhewitt](https://github.com/davidhewitt))
- Support write to buffer api for SerializedFileWriter [\#7714](https://github.com/apache/arrow-rs/pull/7714) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Support `FixedSizeList` RowConverter [\#7705](https://github.com/apache/arrow-rs/pull/7705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Make variant iterators safely infallible [\#7704](https://github.com/apache/arrow-rs/pull/7704) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Speedup `interleave_views` \(4-7x faster\) [\#7695](https://github.com/apache/arrow-rs/pull/7695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Define a "arrow-pyrarrow" crate to implement the "pyarrow" feature. [\#7694](https://github.com/apache/arrow-rs/pull/7694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brunal](https://github.com/brunal))
- feat: add constructor to efficiently upgrade dict key type to remaining builders [\#7689](https://github.com/apache/arrow-rs/pull/7689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([albertlockett](https://github.com/albertlockett))
- Document REE row format and add some more tests [\#7680](https://github.com/apache/arrow-rs/pull/7680) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat: add min max aggregate support for FixedSizeBinary [\#7675](https://github.com/apache/arrow-rs/pull/7675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- arrow-data: Add REE support for `build_extend` and `build_extend_nulls` [\#7671](https://github.com/apache/arrow-rs/pull/7671) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Variant: Write Variant Values as JSON [\#7670](https://github.com/apache/arrow-rs/pull/7670) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([carpecodeum](https://github.com/carpecodeum))
- Remove `lazy_static` dependency [\#7669](https://github.com/apache/arrow-rs/pull/7669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Expyron](https://github.com/Expyron))
- Finish implementing Variant::Object and Variant::List [\#7666](https://github.com/apache/arrow-rs/pull/7666) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- Add `RecordBatch::schema_metadata_mut` and `Field::metadata_mut` [\#7664](https://github.com/apache/arrow-rs/pull/7664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- \[Variant\] Simplify creation of Variants from metadata and value [\#7663](https://github.com/apache/arrow-rs/pull/7663) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- chore: group prost dependabot updates [\#7659](https://github.com/apache/arrow-rs/pull/7659) ([mbrobbel](https://github.com/mbrobbel))
- Initial Builder API for Creating Variant Values [\#7653](https://github.com/apache/arrow-rs/pull/7653) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([PinkCrow007](https://github.com/PinkCrow007))
- Add `BatchCoalescer::push_filtered_batch` and docs [\#7652](https://github.com/apache/arrow-rs/pull/7652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Optimize coalesce kernel for StringView \(10-50% faster\) [\#7650](https://github.com/apache/arrow-rs/pull/7650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- arrow-row: Add support for REE [\#7649](https://github.com/apache/arrow-rs/pull/7649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Use approximate comparisons for pow tests [\#7646](https://github.com/apache/arrow-rs/pull/7646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adamreeve](https://github.com/adamreeve))
- \[Variant\] Implement read support for remaining primitive types [\#7644](https://github.com/apache/arrow-rs/pull/7644) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([superserious-dev](https://github.com/superserious-dev))
- Add `pretty_format_batches_with_schema` function [\#7642](https://github.com/apache/arrow-rs/pull/7642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lewiszlw](https://github.com/lewiszlw))
- Deprecate old Parquet page index parsing functions [\#7640](https://github.com/apache/arrow-rs/pull/7640) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update FlightSQL `GetDbSchemas` and `GetTables` schemas to fully match the protocol [\#7638](https://github.com/apache/arrow-rs/pull/7638) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([sgrebnov](https://github.com/sgrebnov))
- Minor: Remove outdated FIXME from `ParquetMetaDataReader` [\#7635](https://github.com/apache/arrow-rs/pull/7635) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix the error info of `StructArray::try_new` [\#7634](https://github.com/apache/arrow-rs/pull/7634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xudong963](https://github.com/xudong963))
- Fix reading encrypted Parquet pages when using the page index [\#7633](https://github.com/apache/arrow-rs/pull/7633) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- \[Variant\] Add commented out primitive test casees [\#7631](https://github.com/apache/arrow-rs/pull/7631) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve `coalesce` kernel tests [\#7626](https://github.com/apache/arrow-rs/pull/7626) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Revert "Revert "Improve `coalesce` and `concat` performance for views… [\#7625](https://github.com/apache/arrow-rs/pull/7625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Revert "Improve `coalesce` and `concat` performance for views \(\#7614\)" [\#7623](https://github.com/apache/arrow-rs/pull/7623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Improve coalesce\_kernel benchmark to capture inline vs non inline views [\#7619](https://github.com/apache/arrow-rs/pull/7619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `coalesce` and `concat` performance for views [\#7614](https://github.com/apache/arrow-rs/pull/7614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
