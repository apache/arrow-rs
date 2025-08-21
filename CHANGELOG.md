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

## [56.1.0](https://github.com/apache/arrow-rs/tree/56.1.0) (2025-08-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/56.0.0...56.1.0)

**Implemented enhancements:**

- Implement cast and other operations on decimal32 and decimal64 \#7815 [\#8204](https://github.com/apache/arrow-rs/issues/8204) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up Parquet filter pushdown with predicate cache [\#8203](https://github.com/apache/arrow-rs/issues/8203) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optionally read parquet page indexes [\#8070](https://github.com/apache/arrow-rs/issues/8070) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet reader: add method for sync reader read bloom filter [\#8023](https://github.com/apache/arrow-rs/issues/8023) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[parquet\] Support writing logically equivalent types  to `ArrowWriter` [\#8012](https://github.com/apache/arrow-rs/issues/8012) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Improve StringArray\(Utf8\) sort performance [\#7847](https://github.com/apache/arrow-rs/issues/7847) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- The Rustdocs are clean CI job is failing [\#8175](https://github.com/apache/arrow-rs/issues/8175)
- \[avro\] Bug in resolving avro schema with named type [\#8045](https://github.com/apache/arrow-rs/issues/8045) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Doc test failure \(test arrow-avro/src/lib.rs - reader\) when verifying avro 56.0.0 RC1 release [\#8018](https://github.com/apache/arrow-rs/issues/8018) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- arrow-row: Document dictionary handling [\#8168](https://github.com/apache/arrow-rs/pull/8168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Docs: Clarify that Array::value does not check for nulls [\#8065](https://github.com/apache/arrow-rs/pull/8065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- docs: Fix a  typo in README [\#8036](https://github.com/apache/arrow-rs/pull/8036) ([EricccTaiwan](https://github.com/EricccTaiwan))
- Add more comments to the internal parquet reader [\#7932](https://github.com/apache/arrow-rs/pull/7932) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- perf\(arrow-ipc\): avoid counting nulls in `RecordBatchDecoder` [\#8127](https://github.com/apache/arrow-rs/pull/8127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Use `Vec` directly in builders [\#7984](https://github.com/apache/arrow-rs/pull/7984) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Improve StringArray\(Utf8\) sort performance \(~2-4x faster\) [\#7860](https://github.com/apache/arrow-rs/pull/7860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))

**Closed issues:**

- \[Variant\] Improve fuzz test for Variant [\#8199](https://github.com/apache/arrow-rs/issues/8199)
- \[Variant\] Improve fuzz test for Variant [\#8198](https://github.com/apache/arrow-rs/issues/8198)
- `VariantArrayBuilder` tracks starting offsets instead of \(offset, len\) pairs [\#8192](https://github.com/apache/arrow-rs/issues/8192)
- Rework `ValueBuilder` API to work with `ParentState` for reliable nested rollbacks [\#8188](https://github.com/apache/arrow-rs/issues/8188)
- \[Variant\] Rename `ValueBuffer` as `ValueBuilder` [\#8186](https://github.com/apache/arrow-rs/issues/8186)
- \[Variant\] Refactor `ParentState` to track and rollback state on behalf of its owning builder [\#8182](https://github.com/apache/arrow-rs/issues/8182)
- \[Variant\] `ObjectBuilder` should detect duplicates at insertion time, not at finish [\#8180](https://github.com/apache/arrow-rs/issues/8180)
- \[Variant\] ObjectBuilder does not reliably check for duplicates [\#8170](https://github.com/apache/arrow-rs/issues/8170)
- [Variant] Support `StringView` and `LargeString` in ´batch_json_string_to_variant` [\#8145](https://github.com/apache/arrow-rs/issues/8145) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Rename `batch_json_string_to_variant` and `batch_variant_to_json_string` json\_to\_variant [\#8144](https://github.com/apache/arrow-rs/issues/8144) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[avro\] Use `tempfile` crate rather than custom temporary file generator in tests [\#8143](https://github.com/apache/arrow-rs/issues/8143) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Avro\] Use `Write` rather   `dyn Write` in Decoder [\#8142](https://github.com/apache/arrow-rs/issues/8142) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Nested builder rollback is broken [\#8136](https://github.com/apache/arrow-rs/issues/8136)
- \[Variant\] Add support the remaing primitive type\(timestamp\_nanos/timestampntz\_nanos/uuid\) for parquet variant [\#8126](https://github.com/apache/arrow-rs/issues/8126)
- Meta: Implement missing Arrow 56.0 lint rules - Sequential workflow [\#8121](https://github.com/apache/arrow-rs/issues/8121)
- ARROW-012-015: Add linter rules for remaining Arrow 56.0 breaking changes [\#8120](https://github.com/apache/arrow-rs/issues/8120)
- ARROW-010 & ARROW-011: Add linter rules for Parquet Statistics and Metadata API removals [\#8119](https://github.com/apache/arrow-rs/issues/8119)
- ARROW-009: Add linter rules for IPC Dictionary API removals in Arrow 56.0 [\#8118](https://github.com/apache/arrow-rs/issues/8118)
- ARROW-008: Add linter rule for SerializedPageReaderState usize→u64 breaking change [\#8117](https://github.com/apache/arrow-rs/issues/8117)
- ARROW-007: Add linter rule for Schema.all\_fields\(\) removal in Arrow 56.0 [\#8116](https://github.com/apache/arrow-rs/issues/8116)
- \[Variant\] Implement `ShreddingState::AllNull` variant  [\#8088](https://github.com/apache/arrow-rs/issues/8088) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Support Shredded Objects in `variant_get` [\#8083](https://github.com/apache/arrow-rs/issues/8083) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::RunEndEncoded` support for `cast_to_variant` kernel [\#8064](https://github.com/apache/arrow-rs/issues/8064) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Dictionary` support for `cast_to_variant` kernel [\#8062](https://github.com/apache/arrow-rs/issues/8062) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Struct` support for `cast_to_variant` kernel [\#8061](https://github.com/apache/arrow-rs/issues/8061) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Decimal32/Decimal64/Decimal128/Decimal256` support for `cast_to_variant` kernel [\#8059](https://github.com/apache/arrow-rs/issues/8059) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Timestamp(..)` support for `cast_to_variant` kernel [\#8058](https://github.com/apache/arrow-rs/issues/8058) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Float16` support for `cast_to_variant` kernel [\#8057](https://github.com/apache/arrow-rs/issues/8057) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Interval` support for `cast_to_variant` kernel [\#8056](https://github.com/apache/arrow-rs/issues/8056) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Time32/Time64` support for `cast_to_variant` kernel [\#8055](https://github.com/apache/arrow-rs/issues/8055) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Date32 / DataType::Date64` support for `cast_to_variant` kernel [\#8054](https://github.com/apache/arrow-rs/issues/8054) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Null` support for `cast_to_variant` kernel [\#8053](https://github.com/apache/arrow-rs/issues/8053) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Boolean` support for `cast_to_variant` kernel [\#8052](https://github.com/apache/arrow-rs/issues/8052) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::FixedSizeBinary` support for `cast_to_variant` kernel [\#8051](https://github.com/apache/arrow-rs/issues/8051) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Binary/LargeBinary/BinaryView` support for `cast_to_variant` kernel [\#8050](https://github.com/apache/arrow-rs/issues/8050) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\]: Implement `DataType::Utf8/LargeUtf8/Utf8View` support for `cast_to_variant` kernel [\#8049](https://github.com/apache/arrow-rs/issues/8049) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Implement `cast_to_variant` kernel [\#8043](https://github.com/apache/arrow-rs/issues/8043) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Variant\] Support `variant_get` kernel for shredded variants [\#7941](https://github.com/apache/arrow-rs/issues/7941) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add test for casting `Decimal128` \(`i128::MIN` and `i128::MAX`\) to `f64` with overflow handling [\#7939](https://github.com/apache/arrow-rs/issues/7939) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- \[Variant\] Enhance the variant fuz test to cover time/timestamp/uuid primitive type [\#8200](https://github.com/apache/arrow-rs/pull/8200) ([klion26](https://github.com/klion26))
- \[Variant\] VariantArrayBuilder tracks only offsets [\#8193](https://github.com/apache/arrow-rs/pull/8193) ([scovich](https://github.com/scovich))
- \[Variant\] Caller provides ParentState to ValueBuilder methods [\#8189](https://github.com/apache/arrow-rs/pull/8189) ([scovich](https://github.com/scovich))
- \[Variant\] Rename ValueBuffer as ValueBuilder [\#8187](https://github.com/apache/arrow-rs/pull/8187) ([scovich](https://github.com/scovich))
- \[Variant\] ParentState handles finish/rollback for builders [\#8185](https://github.com/apache/arrow-rs/pull/8185) ([scovich](https://github.com/scovich))
- \[Variant\]: Implement `DataType::RunEndEncoded` support for `cast_to_variant` kernel [\#8174](https://github.com/apache/arrow-rs/pull/8174) ([liamzwbao](https://github.com/liamzwbao))
- \[Variant\]: Implement `DataType::Dictionary` support for `cast_to_variant` kernel [\#8173](https://github.com/apache/arrow-rs/pull/8173) ([liamzwbao](https://github.com/liamzwbao))
- Implement `ArrayBuilder` for `UnionBuilder` [\#8169](https://github.com/apache/arrow-rs/pull/8169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([grtlr](https://github.com/grtlr))
- \[Variant\] Support `LargeString` and `StringView` in `batch_json_string_to_variant` [\#8163](https://github.com/apache/arrow-rs/pull/8163) ([liamzwbao](https://github.com/liamzwbao))
- \[Variant\] Rename `batch_json_string_to_variant` and `batch_variant_to_json_string` [\#8161](https://github.com/apache/arrow-rs/pull/8161) ([liamzwbao](https://github.com/liamzwbao))
- \[Variant\] Add primitive type timestamp\_nanos\(with&without timezone\) and uuid [\#8149](https://github.com/apache/arrow-rs/pull/8149) ([klion26](https://github.com/klion26))
- refactor\(avro\): Use impl Write instead of dyn Write in encoder [\#8148](https://github.com/apache/arrow-rs/pull/8148) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Xuanwo](https://github.com/Xuanwo))
- chore: Use tempfile to replace hand-written utils functions [\#8147](https://github.com/apache/arrow-rs/pull/8147) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Xuanwo](https://github.com/Xuanwo))
- feat: support push batch direct to completed and add biggest coalesce batch support [\#8146](https://github.com/apache/arrow-rs/pull/8146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- \[Variant\] Add human-readable impl Debug for Variant [\#8140](https://github.com/apache/arrow-rs/pull/8140) ([scovich](https://github.com/scovich))
- \[Variant\] Fix broken metadata builder rollback [\#8135](https://github.com/apache/arrow-rs/pull/8135) ([scovich](https://github.com/scovich))
- \[Variant\]: Implement DataType::Interval support for cast\_to\_variant kernel [\#8125](https://github.com/apache/arrow-rs/pull/8125) ([codephage2020](https://github.com/codephage2020))
- Add schema resolution and type promotion support to arrow-avro Decoder [\#8124](https://github.com/apache/arrow-rs/pull/8124) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Add Initial `arrow-avro` writer implementation with basic type support [\#8123](https://github.com/apache/arrow-rs/pull/8123) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- \[Variant\] Add Variant::Time primitive and cast logic [\#8114](https://github.com/apache/arrow-rs/pull/8114) ([klion26](https://github.com/klion26))
- \[Variant\] Support Timestamp to variant for `cast_to_variant` kernel [\#8113](https://github.com/apache/arrow-rs/pull/8113) ([abacef](https://github.com/abacef))
- Bump actions/checkout from 4 to 5 [\#8110](https://github.com/apache/arrow-rs/pull/8110) ([dependabot[bot]](https://github.com/apps/dependabot))
- \[Varaint\]: add `DataType::Null` support to cast\_to\_variant [\#8107](https://github.com/apache/arrow-rs/pull/8107) ([feniljain](https://github.com/feniljain))
- \[Variant\] Adding fixed size byte array to variant and test [\#8106](https://github.com/apache/arrow-rs/pull/8106) ([abacef](https://github.com/abacef))
- \[VARIANT\] Initial integration tests for variant reads [\#8104](https://github.com/apache/arrow-rs/pull/8104) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([carpecodeum](https://github.com/carpecodeum))
- \[Variant\]: Implement `DataType::Decimal32/Decimal64/Decimal128/Decimal256` support for `cast_to_variant` kernel [\#8101](https://github.com/apache/arrow-rs/pull/8101) ([liamzwbao](https://github.com/liamzwbao))
- Refactor arrow-avro `Decoder` to support partial decoding [\#8100](https://github.com/apache/arrow-rs/pull/8100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- fix: Validate metadata len in IPC reader  [\#8097](https://github.com/apache/arrow-rs/pull/8097) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JakeDern](https://github.com/JakeDern))
- \[parquet\] further improve logical type compatibility in ArrowWriter [\#8095](https://github.com/apache/arrow-rs/pull/8095) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([albertlockett](https://github.com/albertlockett))
- \[Varint\] Implement ShreddingState::AllNull variant [\#8093](https://github.com/apache/arrow-rs/pull/8093) ([codephage2020](https://github.com/codephage2020))
- \[Variant\] Minor: Add comments to tickets for follow on items [\#8092](https://github.com/apache/arrow-rs/pull/8092) ([alamb](https://github.com/alamb))
- \[VARIANT\] Add support for DataType::Struct for cast\_to\_variant [\#8090](https://github.com/apache/arrow-rs/pull/8090) ([carpecodeum](https://github.com/carpecodeum))
- \[VARIANT\] Add support for DataType::Utf8/LargeUtf8/Utf8View for cast\_to\_variant [\#8089](https://github.com/apache/arrow-rs/pull/8089) ([carpecodeum](https://github.com/carpecodeum))
- \[Variant\] Implement `DataType::Boolean` support for `cast_to_variant` kernel [\#8085](https://github.com/apache/arrow-rs/pull/8085) ([sdf-jkl](https://github.com/sdf-jkl))
- \[Variant\] Implement `DataType::{Date32,Date64}` =\> `Variant::Date` [\#8081](https://github.com/apache/arrow-rs/pull/8081) ([superserious-dev](https://github.com/superserious-dev))
- Fix new clippy lints from Rust 1.89 [\#8078](https://github.com/apache/arrow-rs/pull/8078) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Implement ArrowSchema to AvroSchema conversion logic in arrow-avro [\#8075](https://github.com/apache/arrow-rs/pull/8075) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Implement `DataType::{Binary, LargeBinary, BinaryView}` =\> `Variant::Binary` [\#8074](https://github.com/apache/arrow-rs/pull/8074) ([superserious-dev](https://github.com/superserious-dev))
- \[Variant\] Implement `DataType::Float16` =\> `Variant::Float` [\#8073](https://github.com/apache/arrow-rs/pull/8073) ([superserious-dev](https://github.com/superserious-dev))
- create PageIndexPolicy to allow optional indexes [\#8071](https://github.com/apache/arrow-rs/pull/8071) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kczimm](https://github.com/kczimm))
- \[Variant\] Minor: use From impl to make conversion infallable [\#8068](https://github.com/apache/arrow-rs/pull/8068) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Bump actions/download-artifact from 4 to 5 [\#8066](https://github.com/apache/arrow-rs/pull/8066) ([dependabot[bot]](https://github.com/apps/dependabot))
- Added arrow-avro schema resolution foundations and type promotion [\#8047](https://github.com/apache/arrow-rs/pull/8047) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Fix arrow-avro type resolver register bug [\#8046](https://github.com/apache/arrow-rs/pull/8046) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yongkyunlee](https://github.com/yongkyunlee))
- implement `cast_to_variant` kernel to cast native types to `VariantArray` [\#8044](https://github.com/apache/arrow-rs/pull/8044) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add arrow-avro `SchemaStore` and fingerprinting [\#8039](https://github.com/apache/arrow-rs/pull/8039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- Add more benchmarks for Parquet thrift decoding [\#8037](https://github.com/apache/arrow-rs/pull/8037) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Support multi-threaded writing of Parquet files with modular encryption [\#8029](https://github.com/apache/arrow-rs/pull/8029) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rok](https://github.com/rok))
- Add arrow-avro Decoder Benchmarks  [\#8025](https://github.com/apache/arrow-rs/pull/8025) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- feat: add method for sync Parquet reader read bloom filter [\#8024](https://github.com/apache/arrow-rs/pull/8024) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- \[Variant\] Add `variant_get` and Shredded `VariantArray` [\#8021](https://github.com/apache/arrow-rs/pull/8021) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Implement arrow-avro SchemaStore and Fingerprinting To Enable Schema Resolution [\#8006](https://github.com/apache/arrow-rs/pull/8006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jecsand838](https://github.com/jecsand838))
- feat: arrow-ipc delta dictionary support [\#8001](https://github.com/apache/arrow-rs/pull/8001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JakeDern](https://github.com/JakeDern))
- \[Parquet\] Add tests for IO/CPU access in parquet reader [\#7971](https://github.com/apache/arrow-rs/pull/7971) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Speed up Parquet filter pushdown v4 \(Predicate evaluation cache for async\_reader\) [\#7850](https://github.com/apache/arrow-rs/pull/7850) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Implement cast and other operations on decimal32 and decimal64 [\#7815](https://github.com/apache/arrow-rs/pull/7815) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([CurtHagenlocher](https://github.com/CurtHagenlocher))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
