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

## [32.0.0](https://github.com/apache/arrow-rs/tree/32.0.0) (2023-01-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/31.0.0...32.0.0)

**Breaking changes:**

- Allow `StringArray` construction with `Vec<Option<String>>` [\#3602](https://github.com/apache/arrow-rs/pull/3602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sinistersnare](https://github.com/sinistersnare))
- Use native types in PageIndex \(\#3575\) [\#3578](https://github.com/apache/arrow-rs/pull/3578) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add external variant to ParquetError \(\#3285\) [\#3574](https://github.com/apache/arrow-rs/pull/3574) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Return reference from ListArray::values [\#3561](https://github.com/apache/arrow-rs/pull/3561) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Add `RunEndEncodedArray` [\#3553](https://github.com/apache/arrow-rs/pull/3553) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))

**Implemented enhancements:**

- There should be a `From<Vec<Option<String>>>` impl for `GenericStringArray<OffsetSize>` [\#3599](https://github.com/apache/arrow-rs/issues/3599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FlightDataEncoder Optionally send Schema even when no record batches [\#3591](https://github.com/apache/arrow-rs/issues/3591) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Use Native Types in PageIndex [\#3575](https://github.com/apache/arrow-rs/issues/3575) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Packing array into dictionary of generic byte array [\#3571](https://github.com/apache/arrow-rs/issues/3571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Error::Source` for ArrowError and FlightError [\#3566](https://github.com/apache/arrow-rs/issues/3566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[FlightSQL\] Allow access to underlying FlightClient [\#3551](https://github.com/apache/arrow-rs/issues/3551) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Arrow CSV writer should not fail when cannot cast the value [\#3547](https://github.com/apache/arrow-rs/issues/3547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Deprecated Min Max Statistics When ColumnOrder Signed [\#3526](https://github.com/apache/arrow-rs/issues/3526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Improve Performance of JSON Reader [\#3441](https://github.com/apache/arrow-rs/issues/3441)
- Support footer kv metadata for IPC file [\#3432](https://github.com/apache/arrow-rs/issues/3432)
- Add `External` variant to ParquetError [\#3285](https://github.com/apache/arrow-rs/issues/3285) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Nullif of NULL Predicate is not NULL [\#3589](https://github.com/apache/arrow-rs/issues/3589)
- BooleanBufferBuilder Fails to Clear Set Bits On Truncate [\#3587](https://github.com/apache/arrow-rs/issues/3587) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `nullif` incorrectly calculates `null_count`, sometimes panics with substraction overflow error [\#3579](https://github.com/apache/arrow-rs/issues/3579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Meet warning when use pyarrow [\#3543](https://github.com/apache/arrow-rs/issues/3543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect row group total\_byte\_size written to parquet file [\#3530](https://github.com/apache/arrow-rs/issues/3530) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Overflow when casting timestamps prior to the epoch [\#3512](https://github.com/apache/arrow-rs/issues/3512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Panic on Key Overflow in Dictionary Builders [\#3562](https://github.com/apache/arrow-rs/issues/3562) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Bumping version gives compilation error \(arrow-array\) [\#3525](https://github.com/apache/arrow-rs/issues/3525)

**Merged pull requests:**

- Add Push-Based CSV Decoder [\#3604](https://github.com/apache/arrow-rs/pull/3604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update to flatbuffers 23.1.21 [\#3597](https://github.com/apache/arrow-rs/pull/3597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster BooleanBufferBuilder::append\_n for true values [\#3596](https://github.com/apache/arrow-rs/pull/3596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support sending schemas for empty streams [\#3594](https://github.com/apache/arrow-rs/pull/3594) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Faster ListArray to StringArray conversion [\#3593](https://github.com/apache/arrow-rs/pull/3593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add conversion from StringArray to BinaryArray [\#3592](https://github.com/apache/arrow-rs/pull/3592) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix nullif null count \(\#3579\) [\#3590](https://github.com/apache/arrow-rs/pull/3590) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Clear bits in BooleanBufferBuilder \(\#3587\) [\#3588](https://github.com/apache/arrow-rs/pull/3588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Iterate all dictionary key types in cast test [\#3585](https://github.com/apache/arrow-rs/pull/3585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Propagate EOF Error from AsyncRead [\#3576](https://github.com/apache/arrow-rs/pull/3576) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Sach1nAgarwal](https://github.com/Sach1nAgarwal))
- Show row\_counts also for \(FixedLen\)ByteArray [\#3573](https://github.com/apache/arrow-rs/pull/3573) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([bmmeijers](https://github.com/bmmeijers))
- Packing array into dictionary of generic byte array [\#3572](https://github.com/apache/arrow-rs/pull/3572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove unwrap on datetime cast for CSV writer [\#3570](https://github.com/apache/arrow-rs/pull/3570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Implement `std::error::Error::source` for `ArrowError` and `FlightError` [\#3567](https://github.com/apache/arrow-rs/pull/3567) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Improve GenericBytesBuilder offset overflow panic message \(\#139\) [\#3564](https://github.com/apache/arrow-rs/pull/3564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement Extend for ArrayBuilder \(\#1841\) [\#3563](https://github.com/apache/arrow-rs/pull/3563) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update pyarrow method call with kwargs [\#3560](https://github.com/apache/arrow-rs/pull/3560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Frankonly](https://github.com/Frankonly))
- Update pyo3 requirement from 0.17 to 0.18 [\#3557](https://github.com/apache/arrow-rs/pull/3557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Expose Inner FlightServiceClient on FlightSqlServiceClient \(\#3551\) [\#3556](https://github.com/apache/arrow-rs/pull/3556) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Fix final page row count in parquet-index binary [\#3554](https://github.com/apache/arrow-rs/pull/3554) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Parquet Avoid Reading 8 Byte Footer Twice from AsyncRead [\#3550](https://github.com/apache/arrow-rs/pull/3550) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Sach1nAgarwal](https://github.com/Sach1nAgarwal))
- Improve concat kernel capacity estimation [\#3546](https://github.com/apache/arrow-rs/pull/3546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.49 to =1.0.50 [\#3545](https://github.com/apache/arrow-rs/pull/3545) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update pyarrow method call to avoid warning [\#3544](https://github.com/apache/arrow-rs/pull/3544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Frankonly](https://github.com/Frankonly))
- Enable casting between Utf8/LargeUtf8 and Binary/LargeBinary [\#3542](https://github.com/apache/arrow-rs/pull/3542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use GHA concurrency groups \(\#3495\) [\#3538](https://github.com/apache/arrow-rs/pull/3538) ([tustvold](https://github.com/tustvold))
- set sum of uncompressed column size as row group size for parquet files [\#3531](https://github.com/apache/arrow-rs/pull/3531) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sidred](https://github.com/sidred))
- Minor: Add documentation about memory use for ArrayData [\#3529](https://github.com/apache/arrow-rs/pull/3529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Upgrade to clap 4.1 + fix test [\#3528](https://github.com/apache/arrow-rs/pull/3528) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Write backwards compatible row group statistics \(\#3526\) [\#3527](https://github.com/apache/arrow-rs/pull/3527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- No panic on timestamp buffer overflow [\#3519](https://github.com/apache/arrow-rs/pull/3519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Support casting from binary to dictionary of binary [\#3482](https://github.com/apache/arrow-rs/pull/3482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add Raw JSON Reader \(~2.5x faster\) [\#3479](https://github.com/apache/arrow-rs/pull/3479) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
