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

## [58.1.0](https://github.com/apache/arrow-rs/tree/58.1.0) (2026-03-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/58.0.0...58.1.0)

**Implemented enhancements:**

- Reuse compression dict lz4\_block [\#9566](https://github.com/apache/arrow-rs/issues/9566)
- \[Variant\] Add `shred_variant` support for `LargeUtf8` and `LargeBinary` types [\#9525](https://github.com/apache/arrow-rs/issues/9525)
- \[Variant\] `variant_get` tests clean up [\#9517](https://github.com/apache/arrow-rs/issues/9517)
- parquet\_variant: Support LargeUtf8 typed value in `unshred_variant` [\#9513](https://github.com/apache/arrow-rs/issues/9513)
- parquet-variant: Support string view typed value in `unshred_variant` [\#9512](https://github.com/apache/arrow-rs/issues/9512)
- Deprecate ArrowTimestampType::make\_value in favor of from\_naive\_datetime [\#9490](https://github.com/apache/arrow-rs/issues/9490) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Followup for support \['fieldName'\] in VariantPath [\#9478](https://github.com/apache/arrow-rs/issues/9478)
- Speedup DELTA\_BINARY\_PACKED decoding when bitwidth is 0 [\#9476](https://github.com/apache/arrow-rs/issues/9476) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support CSV files encoded with charsets other than UTF-8 [\#9465](https://github.com/apache/arrow-rs/issues/9465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose Avro writer schema when building the reader [\#9460](https://github.com/apache/arrow-rs/issues/9460) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `append_nulls` to `MapBuilder` [\#9431](https://github.com/apache/arrow-rs/issues/9431) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `append_non_nulls` to `StructBuilder` [\#9429](https://github.com/apache/arrow-rs/issues/9429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `append_value_n` to GenericByteBuilder [\#9425](https://github.com/apache/arrow-rs/issues/9425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Configurable Arrow representation of UTC timestamps for Avro reader [\#9279](https://github.com/apache/arrow-rs/issues/9279) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- MutableArrayData::extend does not copy child values for ListView arrays [\#9561](https://github.com/apache/arrow-rs/issues/9561) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ListView interleave bug [\#9559](https://github.com/apache/arrow-rs/issues/9559) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Flight encoding panics with "no dict id for field" with nested dict arrays [\#9555](https://github.com/apache/arrow-rs/issues/9555) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- "DeltaBitPackDecoder only supports Int32Type and Int64Type" but unsigned types are supported too [\#9551](https://github.com/apache/arrow-rs/issues/9551) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- handle Null type in try\_merge for Struct, List, LargeList, and Union [\#9523](https://github.com/apache/arrow-rs/issues/9523) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- debug\_assert\_eq! in BatchCoalescer panics in debug mode when batch\_size \< 4 [\#9506](https://github.com/apache/arrow-rs/issues/9506) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Statistics::null\_count\_opt wrongly returns Some\(0\) when stats are missing [\#9451](https://github.com/apache/arrow-rs/issues/9451) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error "Not all children array length are the same!" when decoding rows spanning across page boundaries in parquet file when using `RowSelection` [\#9370](https://github.com/apache/arrow-rs/issues/9370) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Avro schema resolution not properly supported for complex types [\#9336](https://github.com/apache/arrow-rs/issues/9336) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Update planned release schedule in README.md [\#9466](https://github.com/apache/arrow-rs/pull/9466) ([alamb](https://github.com/alamb))

**Performance improvements:**

- Introduce `NullBuffer::try_from_unsliced` to simplify array construction [\#9385](https://github.com/apache/arrow-rs/issues/9385) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use chunks\_exact for has\_true/has\_false to enable compiler unrolling [\#9570](https://github.com/apache/arrow-rs/pull/9570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))

**Closed issues:**

- Duplicate macro definition: `partially_shredded_variant_array_gen` [\#9492](https://github.com/apache/arrow-rs/issues/9492)
- Enable `LargeList` / `ListView` / `LargeListView` for `VariantArray::try_new` [\#9455](https://github.com/apache/arrow-rs/issues/9455)
- Support variables/expressions in record\_batch! macro [\#9245](https://github.com/apache/arrow-rs/issues/9245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Add mutable operations to BooleanBuffer \(Bit\*Assign\) [\#9567](https://github.com/apache/arrow-rs/pull/9567) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- chore\(deps\): update lz4\_flex requirement from 0.12 to 0.13 [\#9565](https://github.com/apache/arrow-rs/pull/9565) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- arrow-select: fix MutableArrayData interleave for ListView [\#9560](https://github.com/apache/arrow-rs/pull/9560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- Move `ValueIter` into own module, and add public `record_count` function [\#9557](https://github.com/apache/arrow-rs/pull/9557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rafferty97](https://github.com/Rafferty97))
- arrow-flight: generate dict\_ids for dicts nested inside complex types [\#9556](https://github.com/apache/arrow-rs/pull/9556) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([asubiotto](https://github.com/asubiotto))
- add `shred_variant` support for `LargeUtf8` and `LargeBinary` [\#9554](https://github.com/apache/arrow-rs/pull/9554) ([sdf-jkl](https://github.com/sdf-jkl))
- \[minor\] Download clickbench file when missing [\#9553](https://github.com/apache/arrow-rs/pull/9553) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- DeltaBitPackEncoderConversion: Fix panic message on invalid type [\#9552](https://github.com/apache/arrow-rs/pull/9552) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- Replace interleave overflow panic with error [\#9549](https://github.com/apache/arrow-rs/pull/9549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xudong963](https://github.com/xudong963))
- feat\(arrow-avro\): `HeaderInfo` to expose OCF header [\#9548](https://github.com/apache/arrow-rs/pull/9548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- chore: Protect `main` branch with required reviews [\#9547](https://github.com/apache/arrow-rs/pull/9547) ([comphead](https://github.com/comphead))
- Add benchmark for `infer_json_schema` [\#9546](https://github.com/apache/arrow-rs/pull/9546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rafferty97](https://github.com/Rafferty97))
- chore\(deps\): bump black from 24.3.0 to 26.3.1 in /parquet/pytest [\#9545](https://github.com/apache/arrow-rs/pull/9545) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Unroll interleave -25-30% [\#9542](https://github.com/apache/arrow-rs/pull/9542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Optimize `take_fixed_size_binary` For Predefined Value Lengths [\#9535](https://github.com/apache/arrow-rs/pull/9535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tobixdev](https://github.com/tobixdev))
- feat: expose arrow schema on async avro reader [\#9534](https://github.com/apache/arrow-rs/pull/9534) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- Make with\_file\_decryption\_properties pub instead of pub\(crate\) [\#9532](https://github.com/apache/arrow-rs/pull/9532) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- fix: handle Null type in try\_merge for Struct, List, LargeList, and Union [\#9524](https://github.com/apache/arrow-rs/pull/9524) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- chore: extend record\_batch macro to support variables and expressions [\#9522](https://github.com/apache/arrow-rs/pull/9522) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([buraksenn](https://github.com/buraksenn))
- \[Variant\] clean up `variant_get` tests [\#9518](https://github.com/apache/arrow-rs/pull/9518) ([sdf-jkl](https://github.com/sdf-jkl))
- support large string for unshred variant [\#9515](https://github.com/apache/arrow-rs/pull/9515) ([friendlymatthew](https://github.com/friendlymatthew))
- support string view unshred variant [\#9514](https://github.com/apache/arrow-rs/pull/9514) ([friendlymatthew](https://github.com/friendlymatthew))
- Add has\_true\(\) and has\_false\(\) to BooleanArray [\#9511](https://github.com/apache/arrow-rs/pull/9511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- Fix Invalid offset in sparse column chunk data error for multiple predicates [\#9509](https://github.com/apache/arrow-rs/pull/9509) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([cetra3](https://github.com/cetra3))
- fix: remove incorrect debug assertion in BatchCoalescer  [\#9508](https://github.com/apache/arrow-rs/pull/9508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Tim-53](https://github.com/Tim-53))
- \[Json\] Add benchmarks for list json reader [\#9507](https://github.com/apache/arrow-rs/pull/9507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- fix: first next\_back\(\) on new RowsIter panics [\#9505](https://github.com/apache/arrow-rs/pull/9505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Add some benchmarks for decoding delta encoded Parquet [\#9500](https://github.com/apache/arrow-rs/pull/9500) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- chore: remove duplicate macro `partially_shredded_variant_array_gen` [\#9498](https://github.com/apache/arrow-rs/pull/9498) ([codephage2020](https://github.com/codephage2020))
- Deprecate ArrowTimestampType::make\_value in favor of from\_naive\_datetime [\#9491](https://github.com/apache/arrow-rs/pull/9491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([codephage2020](https://github.com/codephage2020))
- fix: Do not assume missing nullcount stat means zero nullcount [\#9481](https://github.com/apache/arrow-rs/pull/9481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([scovich](https://github.com/scovich))
- \[Variant\] Enahcne bracket access for VariantPath [\#9479](https://github.com/apache/arrow-rs/pull/9479) ([klion26](https://github.com/klion26))
- Optimize delta binary decoder in the case where bitwidth=0 [\#9477](https://github.com/apache/arrow-rs/pull/9477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add PrimitiveRunBuilder::with\_data\_type\(\) to customize the values' DataType [\#9473](https://github.com/apache/arrow-rs/pull/9473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brunal](https://github.com/brunal))
- Convert `prettyprint` tests in `arrow-cast` to `insta` inline snapshots [\#9472](https://github.com/apache/arrow-rs/pull/9472) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([grtlr](https://github.com/grtlr))
- Update strum\_macros requirement from 0.27 to 0.28 [\#9471](https://github.com/apache/arrow-rs/pull/9471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- docs\(parquet\): Fix broken links in README [\#9467](https://github.com/apache/arrow-rs/pull/9467) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SYaoJun](https://github.com/SYaoJun))
- Add list-like types support to VariantArray::try\_new [\#9457](https://github.com/apache/arrow-rs/pull/9457) ([sdf-jkl](https://github.com/sdf-jkl))
- Simplify downcast\_...!\(\) macro definitions [\#9454](https://github.com/apache/arrow-rs/pull/9454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brunal](https://github.com/brunal))
- refactor: simplify iterator using cloned\(\).map\(Some\) [\#9449](https://github.com/apache/arrow-rs/pull/9449) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SYaoJun](https://github.com/SYaoJun))
- docs: fix markdown link syntax in README [\#9440](https://github.com/apache/arrow-rs/pull/9440) ([SYaoJun](https://github.com/SYaoJun))
- Move `ListLikeArray` to arrow-array to be shared with json writer and parquet unshredding [\#9437](https://github.com/apache/arrow-rs/pull/9437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Add `claim` method to recordbatch for memory accounting [\#9433](https://github.com/apache/arrow-rs/pull/9433) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([cetra3](https://github.com/cetra3))
- Add `append_nulls` to `MapBuilder` [\#9432](https://github.com/apache/arrow-rs/pull/9432) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Fokko](https://github.com/Fokko))
- Add `append_non_nulls` to `StructBuilder` [\#9430](https://github.com/apache/arrow-rs/pull/9430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Fokko](https://github.com/Fokko))
- Add `append_value_n` to GenericByteBuilder [\#9426](https://github.com/apache/arrow-rs/pull/9426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Fokko](https://github.com/Fokko))
- refactor: simplify dynamic state for Avro record projection [\#9419](https://github.com/apache/arrow-rs/pull/9419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- Add `NullBuffer::from_unsliced_buffer` helper and refactor call sites [\#9411](https://github.com/apache/arrow-rs/pull/9411) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Eyad3skr](https://github.com/Eyad3skr))
- Implement min, max, sum for run-end-encoded arrays. [\#9409](https://github.com/apache/arrow-rs/pull/9409) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brunal](https://github.com/brunal))
- feat: add `RunArray::new_unchecked` and `RunArray::into_parts` [\#9376](https://github.com/apache/arrow-rs/pull/9376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Fix skip\_records over-counting when partial record precedes num\_rows page skip [\#9374](https://github.com/apache/arrow-rs/pull/9374) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jonded94](https://github.com/jonded94))
- fix: resolution of complex type variants in Avro unions [\#9328](https://github.com/apache/arrow-rs/pull/9328) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- feat\(arrow-avro\): Configurable Arrow timezone ID for Avro timestamps [\#9280](https://github.com/apache/arrow-rs/pull/9280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
