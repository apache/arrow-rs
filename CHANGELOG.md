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

## [56.2.0](https://github.com/apache/arrow-rs/tree/56.2.0) (2025-09-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/56.1.0...56.2.0)

**Breaking changes:**

- \[avro\] Support all default types for avro schema's record field [\#8210](https://github.com/apache/arrow-rs/pull/8210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yongkyunlee](https://github.com/yongkyunlee))

**Implemented enhancements:**

- \[Variant\] \[Shredding\] Support typed\_access for `FixedSizeBinary` [\#8335](https://github.com/apache/arrow-rs/issues/8335)
- \[Variant\] \[Shredding\] Support typed\_access for `Utf8` and `BinaryView` [\#8333](https://github.com/apache/arrow-rs/issues/8333)
- \[Variant\] \[Shredding\] Support typed\_access for `Boolean` [\#8329](https://github.com/apache/arrow-rs/issues/8329)
- Allow specifying projection in ParquetRecordBatchReader::try\_new\_with\_row\_groups [\#8326](https://github.com/apache/arrow-rs/issues/8326)
- \[Parquet\] Expose predicates from RowFilter [\#8314](https://github.com/apache/arrow-rs/issues/8314)
- \[Variant\] Use row-oriented builders in `cast_to_variant` [\#8310](https://github.com/apache/arrow-rs/issues/8310)
- Use apache/arrow-dotnet for integration test [\#8294](https://github.com/apache/arrow-rs/issues/8294)
- \[Variant\] Add `Vairant::as_u*` [\#8283](https://github.com/apache/arrow-rs/issues/8283)
- Add a way to modify WriterProperties [\#8273](https://github.com/apache/arrow-rs/issues/8273)
- Dont truncate timestamps on display for Row [\#8265](https://github.com/apache/arrow-rs/issues/8265)
- \[Parquet\] Add row group write with AsyncArrowWriter [\#8261](https://github.com/apache/arrow-rs/issues/8261)
- Parquet: Do not compress v2 data page when compress is bad quality [\#8256](https://github.com/apache/arrow-rs/issues/8256) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Refactor `cast_to_variant` [\#8234](https://github.com/apache/arrow-rs/issues/8234)
- \[Variant\]: Implement `DataType::Union` support for `cast_to_variant` kernel [\#8195](https://github.com/apache/arrow-rs/issues/8195)
- \[Variant\]: Implement `DataType::Duration` support for `cast_to_variant` kernel [\#8194](https://github.com/apache/arrow-rs/issues/8194)
- \[Variant\] Support typed access for numeric types in variant\_get [\#8178](https://github.com/apache/arrow-rs/issues/8178)
- Implement a "push style" API for decoding Parquet Metadata [\#8164](https://github.com/apache/arrow-rs/issues/8164)
- \[Variant\] Support creating Variants with pre-existing Metadata [\#8152](https://github.com/apache/arrow-rs/issues/8152)
- \[Variant\] Support Shredded Objects in `variant_get`: typed path access \(STEP 1\) [\#8150](https://github.com/apache/arrow-rs/issues/8150)
- \[Variant\] Add `variant` feature to `parquet` crate [\#8132](https://github.com/apache/arrow-rs/issues/8132)
- \[Variant\] Implement `VariantArray::value` for shredded variants [\#8091](https://github.com/apache/arrow-rs/issues/8091)
- \[Variant\] Integration tests for reading parquet w/ Variants [\#8084](https://github.com/apache/arrow-rs/issues/8084)
- \[Variant\]: Implement `DataType::Map` support for `cast_to_variant` kernel [\#8063](https://github.com/apache/arrow-rs/issues/8063)
- \[Variant\]: Implement `DataType::List/LargeList` support for `cast_to_variant` kernel [\#8060](https://github.com/apache/arrow-rs/issues/8060)

**Fixed bugs:**

- Casting floating point numbers fails for Decimal64 but works for other variants [\#8362](https://github.com/apache/arrow-rs/issues/8362)
- \[Variant\] cast\_to\_variant conflates empty map with NULL [\#8289](https://github.com/apache/arrow-rs/issues/8289)
- \[Avro\] Decoder flush panics for map whose value field contains metadata [\#8270](https://github.com/apache/arrow-rs/issues/8270)
- Parquet: Avoid page size exceeds i32::MAX [\#8263](https://github.com/apache/arrow-rs/issues/8263) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Avro\] Decoder panics on flush when schema contains map whose value is non-nullable [\#8253](https://github.com/apache/arrow-rs/issues/8253)
- Avro nullable field decode failure leads to panic upon decoder flush [\#8212](https://github.com/apache/arrow-rs/issues/8212)
- Avro to arrow schema conversion fails when a field has a default type that is not string [\#8209](https://github.com/apache/arrow-rs/issues/8209)
- parquet: No method named `to_ne_bytes` found for struct `bloom_filter::Block` for target `s390x-unknown-linux-gnu` [\#8207](https://github.com/apache/arrow-rs/issues/8207)
- \[Variant\] cast\_to\_variant will panic on certain `Date64` or Timestamp Values values [\#8155](https://github.com/apache/arrow-rs/issues/8155)
- Parquet: Avoid page-size overflows i32 [\#8264](https://github.com/apache/arrow-rs/pull/8264) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))

**Documentation updates:**

- Update docstring comment for Writer::write\(\) in writer.rs [\#8267](https://github.com/apache/arrow-rs/pull/8267) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([YKoustubhRao](https://github.com/YKoustubhRao))

**Closed issues:**

- comfy-table release 7.2.0 breaks MSRV [\#8243](https://github.com/apache/arrow-rs/issues/8243)
- \[Variant\] Add `Variant::as_f16` [\#8228](https://github.com/apache/arrow-rs/issues/8228)
- Support appending raw bytes to variant objects and lists [\#8217](https://github.com/apache/arrow-rs/issues/8217)
- `VariantArrayBuilder` uses `ParentState` for simpler rollbacks [\#8205](https://github.com/apache/arrow-rs/issues/8205)
- Make `ObjectBuilder::finish` signature infallible [\#8184](https://github.com/apache/arrow-rs/issues/8184)
- Improve performance of `i256` to `f64` [\#8013](https://github.com/apache/arrow-rs/issues/8013)

**Merged pull requests:**

- \[Variant\] \[Shredding\] Support typed\_access for Utf8 and BinaryView [\#8364](https://github.com/apache/arrow-rs/pull/8364) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([petern48](https://github.com/petern48))
- Fix casting floats to Decimal64 [\#8363](https://github.com/apache/arrow-rs/pull/8363) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- \[Variant\] Add constants for empty variant metadata [\#8359](https://github.com/apache/arrow-rs/pull/8359) ([scovich](https://github.com/scovich))
- \[Variant\] Allow lossless casting from integer to floating point [\#8357](https://github.com/apache/arrow-rs/pull/8357) ([scovich](https://github.com/scovich))
- \[Variant\] Minor code cleanups [\#8356](https://github.com/apache/arrow-rs/pull/8356) ([scovich](https://github.com/scovich))
- \[Variant\] Remove unused metadata from variant ShreddingState [\#8355](https://github.com/apache/arrow-rs/pull/8355) ([scovich](https://github.com/scovich))
- Adds Map & Enum support, round-trip & benchmark tests [\#8353](https://github.com/apache/arrow-rs/pull/8353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- \[Variant\] \[Shredding\] feat: Support typed\_access for FixedSizeBinary [\#8352](https://github.com/apache/arrow-rs/pull/8352) ([petern48](https://github.com/petern48))
- \[Variant\] feat: Support typed\_access for Boolean [\#8346](https://github.com/apache/arrow-rs/pull/8346) ([Weijun-H](https://github.com/Weijun-H))
- \[Variant\] Make VariantToArrowRowBuilder an enum [\#8345](https://github.com/apache/arrow-rs/pull/8345) ([scovich](https://github.com/scovich))
- \[Variant\] Rename VariantShreddingRowBuilder to VariantToArrowRowBuilder [\#8344](https://github.com/apache/arrow-rs/pull/8344) ([scovich](https://github.com/scovich))
- \[Variant\] Add tests for variant\_get requesting Some struct [\#8343](https://github.com/apache/arrow-rs/pull/8343) ([scovich](https://github.com/scovich))
- \[Variant\] Add nullable arg to StructArrayBuilder::with\_field [\#8342](https://github.com/apache/arrow-rs/pull/8342) ([scovich](https://github.com/scovich))
- Minor: avoid an `Arc::clone` in CacheOptions for Parquet PredicateCache [\#8338](https://github.com/apache/arrow-rs/pull/8338) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix `can_cast_types` for temporal to `Utf8View` [\#8328](https://github.com/apache/arrow-rs/pull/8328) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Update `variant_integration` test to use final approved `parquet-testing` data [\#8325](https://github.com/apache/arrow-rs/pull/8325) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] ParentState tracks builder-specific state in a uniform way [\#8324](https://github.com/apache/arrow-rs/pull/8324) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- \[Variant\] Remove boilerplate from make\_shredding\_row\_builder [\#8322](https://github.com/apache/arrow-rs/pull/8322) ([scovich](https://github.com/scovich))
- \[Variant\] Move VariantAsPrimitive to type\_conversions.rs [\#8321](https://github.com/apache/arrow-rs/pull/8321) ([scovich](https://github.com/scovich))
- \[Variant\] Remove unused output builder files [\#8320](https://github.com/apache/arrow-rs/pull/8320) ([scovich](https://github.com/scovich))
- Add arrow-avro examples and Reader documentation [\#8316](https://github.com/apache/arrow-rs/pull/8316) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Expose predicates from RowFilter [\#8315](https://github.com/apache/arrow-rs/pull/8315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yeya24](https://github.com/yeya24))
- \[Variant\] Implement row builders for cast\_to\_variant [\#8299](https://github.com/apache/arrow-rs/pull/8299) ([scovich](https://github.com/scovich))
- Adds additional type support to arrow-avro writer [\#8298](https://github.com/apache/arrow-rs/pull/8298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- Use apache/arrow-dotnet for integration test [\#8295](https://github.com/apache/arrow-rs/pull/8295) ([kou](https://github.com/kou))
- Add projection with default values support to `RecordDecoder` [\#8293](https://github.com/apache/arrow-rs/pull/8293) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Add array/map/fixed schema resolution and default value support to arrow-avro codec [\#8292](https://github.com/apache/arrow-rs/pull/8292) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Bump actions/labeler from 6.0.0 to 6.0.1 [\#8288](https://github.com/apache/arrow-rs/pull/8288) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/github-script from 7 to 8 [\#8287](https://github.com/apache/arrow-rs/pull/8287) ([dependabot[bot]](https://github.com/apps/dependabot))
- \[Variant\] Add as\_u\* for Variant [\#8284](https://github.com/apache/arrow-rs/pull/8284) ([klion26](https://github.com/klion26))
- \[Variant\] Support Shredded Objects in variant\_get \(take 2\) [\#8280](https://github.com/apache/arrow-rs/pull/8280) ([scovich](https://github.com/scovich))
- Bump actions/setup-node from 4 to 5 [\#8279](https://github.com/apache/arrow-rs/pull/8279) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/setup-python from 5 to 6 [\#8278](https://github.com/apache/arrow-rs/pull/8278) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/labeler from 5.0.0 to 6.0.0 [\#8276](https://github.com/apache/arrow-rs/pull/8276) ([dependabot[bot]](https://github.com/apps/dependabot))
- Impl `Display` for `Tz` [\#8275](https://github.com/apache/arrow-rs/pull/8275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Added List and Struct Encoding to arrow-avro Writer [\#8274](https://github.com/apache/arrow-rs/pull/8274) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Add into\_builder method for WriterProperties [\#8272](https://github.com/apache/arrow-rs/pull/8272) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([corwinjoy](https://github.com/corwinjoy))
- chore\(parquet/record/field\): dont truncate timestamps on display [\#8266](https://github.com/apache/arrow-rs/pull/8266) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Erigara](https://github.com/Erigara))
- \[Parquet\] Write row group with async writer [\#8262](https://github.com/apache/arrow-rs/pull/8262) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lilianm](https://github.com/lilianm))
- Parquet: Do not compress v2 data page when compress is bad quality [\#8257](https://github.com/apache/arrow-rs/pull/8257) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- Add Decimal32 and Decimal64 support to arrow-avro Reader [\#8255](https://github.com/apache/arrow-rs/pull/8255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Minor\] Backport changes to metadata benchmark [\#8251](https://github.com/apache/arrow-rs/pull/8251) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update hashbrown requirement from 0.15.1 to 0.16.0 [\#8248](https://github.com/apache/arrow-rs/pull/8248) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Sort: Change lexsort comment from stable to unstable [\#8245](https://github.com/apache/arrow-rs/pull/8245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))
- pin comfy-table to 7.1.2 [\#8244](https://github.com/apache/arrow-rs/pull/8244) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zachschuermann](https://github.com/zachschuermann))
- Adds Confluent wire format handling to arrow-avro crate [\#8242](https://github.com/apache/arrow-rs/pull/8242) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nathaniel-d-ef](https://github.com/nathaniel-d-ef))
- feat: gRPC compression support for flight CLI [\#8240](https://github.com/apache/arrow-rs/pull/8240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- feat: `SSLKEYLOGFILE` support for flight CLI [\#8239](https://github.com/apache/arrow-rs/pull/8239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- \[Variant\] Refactor `cast_to_variant` [\#8235](https://github.com/apache/arrow-rs/pull/8235) ([liamzwbao](https://github.com/liamzwbao))
- \[Variant\] add strict mode to cast\_to\_variant [\#8233](https://github.com/apache/arrow-rs/pull/8233) ([codephage2020](https://github.com/codephage2020))
- \[Variant\] Add Variant::as\_f16 [\#8232](https://github.com/apache/arrow-rs/pull/8232) ([klion26](https://github.com/klion26))
- Unpin nightly rust version \(MIRI job\) [\#8229](https://github.com/apache/arrow-rs/pull/8229) ([mbrobbel](https://github.com/mbrobbel))
- Update apache-avro requirement from 0.14.0 to 0.20.0 [\#8226](https://github.com/apache/arrow-rs/pull/8226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-pages-artifact from 3 to 4 [\#8224](https://github.com/apache/arrow-rs/pull/8224) ([dependabot[bot]](https://github.com/apps/dependabot))
- Added arrow-avro enum mapping support for schema resolution [\#8223](https://github.com/apache/arrow-rs/pull/8223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Added arrow-avro schema resolution value skipping [\#8220](https://github.com/apache/arrow-rs/pull/8220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Fix error condition in doc comment of `Field::try_canonical_extension_type` [\#8216](https://github.com/apache/arrow-rs/pull/8216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- \[Variant\]: Implement `DataType::Duration` support for `cast_to_variant` kernel [\#8215](https://github.com/apache/arrow-rs/pull/8215) ([liamzwbao](https://github.com/liamzwbao))
- \[Variant\] feat: remove unnecessary unwraps in `Object::finish` [\#8214](https://github.com/apache/arrow-rs/pull/8214) ([Weijun-H](https://github.com/Weijun-H))
- \[avro\] Fix Avro decoder bitmap corruption when nullable field decoding fails [\#8213](https://github.com/apache/arrow-rs/pull/8213) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yongkyunlee](https://github.com/yongkyunlee))
- Restore accidentally removed method Block::to\_ne\_bytes [\#8211](https://github.com/apache/arrow-rs/pull/8211) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- \[Variant\] Support read-only metadata builders [\#8208](https://github.com/apache/arrow-rs/pull/8208) ([scovich](https://github.com/scovich))
- \[Variant\] VariantArrayBuilder uses MetadataBuilder and ValueBuilder [\#8206](https://github.com/apache/arrow-rs/pull/8206) ([scovich](https://github.com/scovich))
- \[Variant\]: Implement DataType::List/LargeList support for cast\_to\_variant kernel [\#8201](https://github.com/apache/arrow-rs/pull/8201) ([sdf-jkl](https://github.com/sdf-jkl))
- \[Variant\]: Implement `DataType::Union` support for `cast_to_variant` kernel [\#8196](https://github.com/apache/arrow-rs/pull/8196) ([liamzwbao](https://github.com/liamzwbao))
- \[Variant\] Support typed access for numeric types in variant\_get [\#8179](https://github.com/apache/arrow-rs/pull/8179) ([superserious-dev](https://github.com/superserious-dev))
- \[Variant\] feat: add support for casting MapArray to VariantArray [\#8177](https://github.com/apache/arrow-rs/pull/8177) ([Weijun-H](https://github.com/Weijun-H))
- Add benchmarks for arrow-avro writer [\#8165](https://github.com/apache/arrow-rs/pull/8165) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\] Allow appending raw object/list bytes to variant builders [\#8141](https://github.com/apache/arrow-rs/pull/8141) ([scovich](https://github.com/scovich))
- Add `variant_experimental` feature to `parquet` crate [\#8133](https://github.com/apache/arrow-rs/pull/8133) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[Variant\] Implement `VariantArray::value` for shredded variants [\#8105](https://github.com/apache/arrow-rs/pull/8105) ([klion26](https://github.com/klion26))
- \[Parquet\] Add ParquetMetadataPushDecoder [\#8080](https://github.com/apache/arrow-rs/pull/8080) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- improve performance of i256 to f64 [\#8041](https://github.com/apache/arrow-rs/pull/8041) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([klion26](https://github.com/klion26))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
