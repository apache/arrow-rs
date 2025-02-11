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

## [54.1.0](https://github.com/apache/arrow-rs/tree/54.1.0) (2025-01-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.4.0...54.1.0)

**Implemented enhancements:**

- Create GitHub releases automatically on tagging [\#7041](https://github.com/apache/arrow-rs/issues/7041)
- Add required methods to access inner builder for `NullBufferBuilder` [\#7002](https://github.com/apache/arrow-rs/issues/7002) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Re-export `NullBufferBuilder` in the arrow crate [\#6975](https://github.com/apache/arrow-rs/issues/6975) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow-string` function should support binary input as well [\#6923](https://github.com/apache/arrow-rs/issues/6923) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- MMap support for IPC files [\#6709](https://github.com/apache/arrow-rs/issues/6709) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- fix: mark \(Large\)ListView as nested and support in equal data type [\#6995](https://github.com/apache/arrow-rs/pull/6995) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Expose min/max values for Decimal128/256 and improve docs [\#6992](https://github.com/apache/arrow-rs/pull/6992) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Parquet\] Improve speed of dictionary encoding NaN float values [\#6953](https://github.com/apache/arrow-rs/pull/6953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Optimize `BooleanBufferBuilder` for non nullable columns [\#6973](https://github.com/apache/arrow-rs/issues/6973) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow::compute::concat` should merge dictionary type when concatenating list of dictionaries  [\#6888](https://github.com/apache/arrow-rs/issues/6888) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve error message for unsupported cast between struct and other types [\#6724](https://github.com/apache/arrow-rs/issues/6724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- implement regexp\_match, regexp\_scalar\_match and regexp\_array\_match for StringViewArray [\#6717](https://github.com/apache/arrow-rs/issues/6717) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up Parquet utf8 validation [\#6667](https://github.com/apache/arrow-rs/issues/6667) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Regression: Concatenating sliced `ListArray`s is broken [\#7034](https://github.com/apache/arrow-rs/issues/7034)
- `PrimitiveDictionaryBuilder` with specific value data type and capacity [\#7011](https://github.com/apache/arrow-rs/issues/7011) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow IPC Writer Panics for sliced nested arrays [\#6997](https://github.com/apache/arrow-rs/issues/6997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordBatch with no columns cannot be roundtripped through Parquet [\#6988](https://github.com/apache/arrow-rs/issues/6988) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StringView: Using the Interleave kernel \(and potentially others\) results in many repeated buffers in variadic\_buffers [\#6780](https://github.com/apache/arrow-rs/issues/6780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- fix prefetch of page index [\#6999](https://github.com/apache/arrow-rs/pull/6999) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- fix: Parquet column writer `Dictionary(_, Decimal128)` and `Dictionary(_, Decimal256)` [\#6987](https://github.com/apache/arrow-rs/pull/6987) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([korowa](https://github.com/korowa))
- Writing floating point values containing NaN to Parquet is slow when using dictionary encoding [\#6952](https://github.com/apache/arrow-rs/issues/6952) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Public API using private types: `Buffer::from_bytes` takes unexported `Bytes` [\#6754](https://github.com/apache/arrow-rs/issues/6754) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Some MSRVs are inaccurate [\#6741](https://github.com/apache/arrow-rs/issues/6741) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Documentation updates:**

- docs: add to bit slice iterator docs that the start value is inclusive and end value is exclusive [\#7022](https://github.com/apache/arrow-rs/pull/7022) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Fix duplicate link references in README [\#7020](https://github.com/apache/arrow-rs/pull/7020) ([Jefffrey](https://github.com/Jefffrey))
- Enhance ListViewArray related docs [\#7007](https://github.com/apache/arrow-rs/pull/7007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Document data type support and examples to predicates `*like`, `starts_with`, `ends_with`, `contains` [\#7003](https://github.com/apache/arrow-rs/pull/7003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: improve documentation on timezone representations [\#7000](https://github.com/apache/arrow-rs/pull/7000) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add additional documentation for UTC representation of timestamps [\#6994](https://github.com/apache/arrow-rs/pull/6994) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Abdullahsab3](https://github.com/Abdullahsab3))
- Improve `ParquetRecordBatchStreamBuilder` docs / examples [\#6948](https://github.com/apache/arrow-rs/pull/6948) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Document the `ParquetRecordBatchStream` buffering [\#6947](https://github.com/apache/arrow-rs/pull/6947) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: improve `zip` kernel docs, add examples [\#6928](https://github.com/apache/arrow-rs/pull/6928) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add doctest example for `Buffer::from_bytes` [\#6920](https://github.com/apache/arrow-rs/pull/6920) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- \[object store\] Add planned object\_store release schedule to crate readme [\#6904](https://github.com/apache/arrow-rs/pull/6904) ([alamb](https://github.com/alamb))
- Avoid panics? [\#6737](https://github.com/apache/arrow-rs/issues/6737) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Create GitHub releases automatically on tagging [\#7042](https://github.com/apache/arrow-rs/pull/7042) ([kou](https://github.com/kou))
- Fix `concat` for sliced `ListArrays` [\#7037](https://github.com/apache/arrow-rs/pull/7037) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Clarify NullBufferBuilder::new capacity parameter [\#7016](https://github.com/apache/arrow-rs/pull/7016) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `is_valid` and `truncate` methods to `NullBufferBuilder` [\#7013](https://github.com/apache/arrow-rs/pull/7013) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Chen-Yuan-Lai](https://github.com/Chen-Yuan-Lai))
- fix: use the values builder capacity for the hash map in `PrimitiveDictionaryBuilder::new_from_builders` [\#7012](https://github.com/apache/arrow-rs/pull/7012) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Refactor ipc reading code into methods on `ArrayReader` [\#7006](https://github.com/apache/arrow-rs/pull/7006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: make it clear Predicate is crate private [\#7001](https://github.com/apache/arrow-rs/pull/7001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: Panic on reencoding offsets in arrow-ipc with sliced nested arrays [\#6998](https://github.com/apache/arrow-rs/pull/6998) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HawaiianSpork](https://github.com/HawaiianSpork))
- Add check for empty schema in `parquet::schema::types::from_thrift_helper` [\#6990](https://github.com/apache/arrow-rs/pull/6990) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add example reading data from an `mmap`ed IPC file [\#6986](https://github.com/apache/arrow-rs/pull/6986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `arrow-ipc` documentation [\#6983](https://github.com/apache/arrow-rs/pull/6983) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `simdutf8` feature to make `simdutf8` optional, consolidate `check_valid_utf8` [\#6979](https://github.com/apache/arrow-rs/pull/6979) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Export NullBufferBuilder along with BooleanBufferBuilder in `arrow` crate [\#6976](https://github.com/apache/arrow-rs/pull/6976) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: improve the documentation of NullBuffer and BooleanBuffer [\#6974](https://github.com/apache/arrow-rs/pull/6974) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Simplify Validation/Alignment APIs of `ArrayDataBuilder`: validate and align [\#6966](https://github.com/apache/arrow-rs/pull/6966) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix WASM CI for Rust 1.84 release [\#6963](https://github.com/apache/arrow-rs/pull/6963) ([alamb](https://github.com/alamb))
- \[Parquet\] Add benchmark and test for writing NaNs to Parquet [\#6955](https://github.com/apache/arrow-rs/pull/6955) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adamreeve](https://github.com/adamreeve))
- Add `peek_next_page_offset` to `SerializedPageReader` [\#6945](https://github.com/apache/arrow-rs/pull/6945) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Improve `Buffer` documentation, deprecate `Buffer::from_bytes` add `From<Bytes>` and `From<bytes::Bytes>` impls [\#6939](https://github.com/apache/arrow-rs/pull/6939) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- minor: fix test and remove println in tests  [\#6935](https://github.com/apache/arrow-rs/pull/6935) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([himadripal](https://github.com/himadripal))
- Document how to use Extend for generic methods on ArrayBuilders [\#6932](https://github.com/apache/arrow-rs/pull/6932) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wiedld](https://github.com/wiedld))
- \[Parquet\] Add projection utility functions [\#6931](https://github.com/apache/arrow-rs/pull/6931) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- \[Parquet\] Reuse buffer in `ByteViewArrayDecoderPlain`  [\#6930](https://github.com/apache/arrow-rs/pull/6930) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Support `Binary` arrays in `starts_with`, `ends_with` and `contains`  [\#6926](https://github.com/apache/arrow-rs/pull/6926) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Improve the error message for casting between struct and non-struct types [\#6919](https://github.com/apache/arrow-rs/pull/6919) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([takaebato](https://github.com/takaebato))
- Fix error message typos with Parquet compression [\#6918](https://github.com/apache/arrow-rs/pull/6918) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([orf](https://github.com/orf))
- Expose arrow-schema methods, for use when writing parquet outside of ArrowWriter [\#6916](https://github.com/apache/arrow-rs/pull/6916) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- feat\(arrow-ord\): support boolean in `rank` and add tests for sorting lists of booleans [\#6912](https://github.com/apache/arrow-rs/pull/6912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- chore\(arrow-ord\): move `can_rank` to the `rank` file [\#6910](https://github.com/apache/arrow-rs/pull/6910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- feat\(parquet\): Add next\_row\_group API for ParquetRecordBatchStream [\#6907](https://github.com/apache/arrow-rs/pull/6907) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Xuanwo](https://github.com/Xuanwo))
- feat\(arrow-select\): `concat` kernel will merge dictionary values for list of dictionaries [\#6893](https://github.com/apache/arrow-rs/pull/6893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- add `extend_dictionary` in dictionary builder for improved performance [\#6875](https://github.com/apache/arrow-rs/pull/6875) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- \[arrow-string\] Implement string view support for `regexp_match` [\#6849](https://github.com/apache/arrow-rs/pull/6849) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- Add support `StringView` / `BinaryView` in `interleave` kernel [\#6779](https://github.com/apache/arrow-rs/pull/6779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([onursatici](https://github.com/onursatici))
- `RecordBatch` normalization \(flattening\) [\#6758](https://github.com/apache/arrow-rs/pull/6758) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ngli-me](https://github.com/ngli-me))
- Convert some panics that happen on invalid parquet files to error results [\#6738](https://github.com/apache/arrow-rs/pull/6738) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Faster parquet utf8 validation using `simdutf8` [\#6668](https://github.com/apache/arrow-rs/pull/6668) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))

\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
