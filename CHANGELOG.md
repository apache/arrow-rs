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

## [55.1.0](https://github.com/apache/arrow-rs/tree/55.1.0) (2025-05-09)

[Full Changelog](https://github.com/apache/arrow-rs/compare/55.0.0...55.1.0)

**Breaking changes:**

- refactor!: do not default the struct array length to 0 in Struct::try\_new [\#7247](https://github.com/apache/arrow-rs/pull/7247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([westonpace](https://github.com/westonpace))

**Implemented enhancements:**

- Add a way to get max `usize` from `OffsetSizeTrait` [\#7474](https://github.com/apache/arrow-rs/issues/7474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Deterministic metadata encoding [\#7448](https://github.com/apache/arrow-rs/issues/7448) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Arrow type Dictionary with value FixedSizeBinary in Parquet [\#7445](https://github.com/apache/arrow-rs/issues/7445)
- Parquet: Add ability to project rowid in parquet reader [\#7444](https://github.com/apache/arrow-rs/issues/7444)
- Move parquet::file::metadata::reader::FooterTail to parquet::file::metadata so that it is public [\#7438](https://github.com/apache/arrow-rs/issues/7438) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speedup take\_bytes by precalculating capacity [\#7432](https://github.com/apache/arrow-rs/issues/7432) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of interleave\_primitive and interleave\_bytes [\#7421](https://github.com/apache/arrow-rs/issues/7421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Eq` and `Default` for `ScalarBuffer` [\#7411](https://github.com/apache/arrow-rs/issues/7411) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add decryption support for column index and offset index [\#7390](https://github.com/apache/arrow-rs/issues/7390) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing encrypted Parquet files with plaintext footers [\#7320](https://github.com/apache/arrow-rs/issues/7320) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Parquet key management tools [\#7256](https://github.com/apache/arrow-rs/issues/7256) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Verify footer tags when reading encrypted Parquet files with plaintext footers [\#7255](https://github.com/apache/arrow-rs/issues/7255) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StructArray::try\_new behavior can be unexpected when there are no child arrays [\#7246](https://github.com/apache/arrow-rs/issues/7246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet performance: improve performance of reading int8/int16 [\#7097](https://github.com/apache/arrow-rs/issues/7097) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- StructArray::try\_new validation incorrectly returns an error when `logical_nulls()` returns Some\(\) && null\_count == 0 [\#7435](https://github.com/apache/arrow-rs/issues/7435)
- Reading empty DataPageV2 fails with `snappy: corrupt input (empty)` [\#7388](https://github.com/apache/arrow-rs/issues/7388) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Improve documentation and add examples for ArrowPredicateFn [\#7480](https://github.com/apache/arrow-rs/pull/7480) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Document Arrow \<--\> Parquet schema conversion better [\#7479](https://github.com/apache/arrow-rs/pull/7479) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix a typo in arrow/examples/README.md [\#7473](https://github.com/apache/arrow-rs/pull/7473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Mottl](https://github.com/Mottl))

**Closed issues:**

- Refactor Parquet DecryptionPropertiesBuilder to fix use of unreachable [\#7476](https://github.com/apache/arrow-rs/issues/7476) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement `Eq` and `Default` for `OffsetBuffer` [\#7417](https://github.com/apache/arrow-rs/issues/7417) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Add Parquet `arrow_reader` benchmarks for {u}int{8,16}  columns [\#7484](https://github.com/apache/arrow-rs/pull/7484) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix: `rustdoc::unportable_markdown` was removed [\#7483](https://github.com/apache/arrow-rs/pull/7483) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Support round trip reading / writing Arrow `Duration` type to parquet [\#7482](https://github.com/apache/arrow-rs/pull/7482) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Liyixin95](https://github.com/Liyixin95))
- Add const MAX\_OFFSET to OffsetSizeTrait [\#7478](https://github.com/apache/arrow-rs/pull/7478) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([thinkharderdev](https://github.com/thinkharderdev))
- Refactor Parquet DecryptionPropertiesBuilder [\#7477](https://github.com/apache/arrow-rs/pull/7477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Support parsing and display pretty for StructType [\#7469](https://github.com/apache/arrow-rs/pull/7469) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([goldmedal](https://github.com/goldmedal))
- chore\(deps\): update sysinfo requirement from 0.34.0 to 0.35.0 [\#7462](https://github.com/apache/arrow-rs/pull/7462) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Verify footer tags when reading encrypted Parquet files with plaintext footers [\#7459](https://github.com/apache/arrow-rs/pull/7459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rok](https://github.com/rok))
- Improve comments for avro [\#7449](https://github.com/apache/arrow-rs/pull/7449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kumarlokesh](https://github.com/kumarlokesh))
- feat: Support round trip reading/writing Arrow type `Dictionary(_, FixedSizeBinary(_))` to Parquet [\#7446](https://github.com/apache/arrow-rs/pull/7446) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([albertlockett](https://github.com/albertlockett))
- Fix out of bounds crash in RleValueDecoder [\#7441](https://github.com/apache/arrow-rs/pull/7441) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([apilloud](https://github.com/apilloud))
- Make `FooterTail` public  [\#7440](https://github.com/apache/arrow-rs/pull/7440) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([masonh22](https://github.com/masonh22))
- Support writing encrypted Parquet files with plaintext footers [\#7439](https://github.com/apache/arrow-rs/pull/7439) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rok](https://github.com/rok))
- feat: deterministic metadata encoding [\#7437](https://github.com/apache/arrow-rs/pull/7437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([timsaucer](https://github.com/timsaucer))
- Fix validation logic in `StructArray::try_new` to account for array.logical\_nulls\(\) returning Some\(\) and null\_count == 0 [\#7436](https://github.com/apache/arrow-rs/pull/7436) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phillipleblanc](https://github.com/phillipleblanc))
- Minor: Fix typo in async\_reader comment [\#7433](https://github.com/apache/arrow-rs/pull/7433) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([amoeba](https://github.com/amoeba))
- feat: coerce fixed size binary to binary view [\#7431](https://github.com/apache/arrow-rs/pull/7431) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chenkovsky](https://github.com/chenkovsky))
- chore\(deps\): update brotli requirement from 7.0 to 8.0 [\#7430](https://github.com/apache/arrow-rs/pull/7430) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Speedup take\_bytes \(-35% -69%\) by precalculating capacity [\#7422](https://github.com/apache/arrow-rs/pull/7422) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Improve performance of interleave\_primitive \(-15% - 45%\) / interleave\_bytes \(-10-25%\) [\#7420](https://github.com/apache/arrow-rs/pull/7420) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Implement `Eq` and `Default` for `OffsetBuffer` [\#7418](https://github.com/apache/arrow-rs/pull/7418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Implement `Default` for `Buffer` & `ScalarBuffer` [\#7413](https://github.com/apache/arrow-rs/pull/7413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Implement `Eq` for `ScalarBuffer` when `T: Eq` [\#7412](https://github.com/apache/arrow-rs/pull/7412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Skip page should also support skip dict page [\#7409](https://github.com/apache/arrow-rs/pull/7409) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Replace `RecordBatch::with_schema_unchecked` with `RecordBatch::new_unchecked` [\#7405](https://github.com/apache/arrow-rs/pull/7405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Adding `with_schema_unchecked` method for `RecordBatch` [\#7402](https://github.com/apache/arrow-rs/pull/7402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Add benchmark for parquet reader with row\_filter and project settings [\#7401](https://github.com/apache/arrow-rs/pull/7401) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Parquet: Expose accessors from `ArrowReaderOptions` [\#7400](https://github.com/apache/arrow-rs/pull/7400) ([kylebarron](https://github.com/kylebarron))
- Support decryption of Parquet column and offset indexes [\#7399](https://github.com/apache/arrow-rs/pull/7399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Handle compressed empty DataPage v2 [\#7389](https://github.com/apache/arrow-rs/pull/7389) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([EnricoMi](https://github.com/EnricoMi))
- Improve performance of reading int8/int16 Parquet data [\#7055](https://github.com/apache/arrow-rs/pull/7055) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
