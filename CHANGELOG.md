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

## [59.0.0](https://github.com/apache/arrow-rs/tree/59.0.0) (2026-06-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.3.1...59.0.0)

**Breaking changes:**

- chore: Remove some deprecated Arrow functions from the public API [\#10040](https://github.com/apache/arrow-rs/pull/10040) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- chore: Remove some deprecated functions from parquet crate [\#10035](https://github.com/apache/arrow-rs/pull/10035) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Replace `From<Vec<_>>` impls with `TryFrom`s for `FixedSizeBinaryArray` [\#10019](https://github.com/apache/arrow-rs/pull/10019) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([quantumish](https://github.com/quantumish))
- Use Thrift macro to generate Parquet `LogicalType` serialization code [\#9997](https://github.com/apache/arrow-rs/pull/9997) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- refactor: make `BloomFilterProperties` fpp/ndv private with accessors [\#9969](https://github.com/apache/arrow-rs/pull/9969) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([CuteChuanChuan](https://github.com/CuteChuanChuan))
- Remove deprecated parquet::format module and thrift dependency [\#9962](https://github.com/apache/arrow-rs/pull/9962) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- generic channel support for FlightClient [\#9933](https://github.com/apache/arrow-rs/pull/9933) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([rumenov](https://github.com/rumenov))
- Add `CompressionCodec` Thrift enum for Parquet metadata [\#9864](https://github.com/apache/arrow-rs/pull/9864) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[Variant\] remove `BorrowedShreddingState` [\#9791](https://github.com/apache/arrow-rs/pull/9791) ([sdf-jkl](https://github.com/sdf-jkl))
- Remove deprecated legacy `like` kernels in `arrow-string` [\#9674](https://github.com/apache/arrow-rs/pull/9674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))

**Implemented enhancements:**

- Allow casting plain struct to dictionary encoded struct [\#10038](https://github.com/apache/arrow-rs/issues/10038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Optimize arrow-flight [\#10029](https://github.com/apache/arrow-rs/issues/10029)
- Align buffers when importing via `from_ffi` / `ArrowArrayStreamReader` [\#10028](https://github.com/apache/arrow-rs/issues/10028) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Switch Parquet `LogicalType` enum to macro generated version [\#9995](https://github.com/apache/arrow-rs/issues/9995) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Future proof Parquet Thrift parser [\#9973](https://github.com/apache/arrow-rs/issues/9973) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `DatePart` 1-indexed variants [\#9964](https://github.com/apache/arrow-rs/issues/9964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- perf: Rework Parquet Thrift handling of boolean fields [\#9946](https://github.com/apache/arrow-rs/issues/9946) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add benchmarks for REE to parquet [\#9935](https://github.com/apache/arrow-rs/issues/9935) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \(re\) Allow Large `FixedSizeBinaryArray`s [\#9906](https://github.com/apache/arrow-rs/issues/9906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a is\_normalized flag to DictionaryArray [\#9841](https://github.com/apache/arrow-rs/issues/9841)
- \[Variant\] Remove `BorrowedShreddingState` [\#9790](https://github.com/apache/arrow-rs/issues/9790)
- \[parquet\] Expose whether FileDecryptionProperties uses a KeyRetriever [\#9721](https://github.com/apache/arrow-rs/issues/9721) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Align cast logic for from/to\_decimal for variant to cast kernel [\#9688](https://github.com/apache/arrow-rs/issues/9688) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- parquet-variant build might fail on s390x [\#10026](https://github.com/apache/arrow-rs/issues/10026)
- `FixedSizeBinaryArray` implements `From<Vec<&[u8]>>` etc despite conversion being fallible [\#10018](https://github.com/apache/arrow-rs/issues/10018) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- string -\> decimal cast should not treat empty string as 0 [\#10009](https://github.com/apache/arrow-rs/issues/10009) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast FixedSizeList to List will lost datatype metadata in list [\#10004](https://github.com/apache/arrow-rs/issues/10004) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Record reader panics with "index out of bounds" when row group num\_rows exceeds actual column data [\#9992](https://github.com/apache/arrow-rs/issues/9992) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet predicate-cache: panic / silent row drop on single-leaf nullable struct [\#9982](https://github.com/apache/arrow-rs/issues/9982) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet-variant doesn't build on 32-bit targets [\#9977](https://github.com/apache/arrow-rs/issues/9977)
- Date32 doesn't parse date with large year [\#9960](https://github.com/apache/arrow-rs/issues/9960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- msrv check failing on main due to `tonic@0.14.6` [\#9938](https://github.com/apache/arrow-rs/issues/9938) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Documentation updates:**

- Release arrow-rs / parquet  Minor/Patch version `58.3.0` or `58.2.1` \(May 2026\) [\#9859](https://github.com/apache/arrow-rs/issues/9859)
- Add docs for `BitWriter` [\#9949](https://github.com/apache/arrow-rs/pull/9949) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add docs for `BitReader` [\#9948](https://github.com/apache/arrow-rs/pull/9948) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- perf: parquet LevelInfoBuilder::write\_list can be optimized? [\#10023](https://github.com/apache/arrow-rs/issues/10023) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- perf\(parquet\): LevelInfoBuilder batch write when no repetition childs [\#10037](https://github.com/apache/arrow-rs/pull/10037) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- \[arrow-select\] Replace `ArrayData` with direct `Array` construction in filter kernels [\#9986](https://github.com/apache/arrow-rs/pull/9986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Bulk-fill definition levels for majority-null leaf columns [\#9967](https://github.com/apache/arrow-rs/pull/9967) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([RyanJamesStewart](https://github.com/RyanJamesStewart))
- perf: Remove `bool_val` from Parquet Thrift `FieldIdentifier` [\#9945](https://github.com/apache/arrow-rs/pull/9945) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- feat\(parquet\): compact level representation with generic writer dispatch [\#9831](https://github.com/apache/arrow-rs/pull/9831) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))

**Closed issues:**

- Bound ArrowWriter peak memory  [\#10071](https://github.com/apache/arrow-rs/issues/10071) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet writer can produce massively oversized data pages for large variable-width values [\#10061](https://github.com/apache/arrow-rs/issues/10061) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove the `fused_inline_view_columns` field from `BatchCoalescer` if possible [\#10055](https://github.com/apache/arrow-rs/issues/10055)
- DataType parser permits negative FixedSizeBinary size [\#10033](https://github.com/apache/arrow-rs/issues/10033) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: return error for overlong INT96 column metadata statistics [\#10002](https://github.com/apache/arrow-rs/issues/10002) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Uuid` extension type fails to deserialize when `ARROW:extension:metadata` is an empty string [\#10000](https://github.com/apache/arrow-rs/issues/10000) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: timeline for removing thrift crate dependency \(CVE-2026-43868\) [\#9999](https://github.com/apache/arrow-rs/issues/9999)
- Failure in CI: `Archery test With other arrows` - `binary_view Rust producing,  .NET consuming` [\#9989](https://github.com/apache/arrow-rs/issues/9989) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Validate FIXED\_LEN\_BYTE\_ARRAY type\_length for DECIMAL and INTERVAL in Parquet → Arrow schema conversion [\#9984](https://github.com/apache/arrow-rs/issues/9984) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC reader projection does not handle duplicate projection indices correctly [\#9950](https://github.com/apache/arrow-rs/issues/9950) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `AnyRunArray` trait [\#9909](https://github.com/apache/arrow-rs/issues/9909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release arrow-rs / parquet Patch version `57.3.1` \(May 2026\) [\#9858](https://github.com/apache/arrow-rs/issues/9858) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release arrow-rs / parquet Patch version `56.2.1` \(May 2026\) [\#9857](https://github.com/apache/arrow-rs/issues/9857) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet/arrow: should sync/async readers converge on a shared physical read planner [\#9764](https://github.com/apache/arrow-rs/issues/9764)
- `arrow-string` has a lot of macro-generated deprecated kernels in `like.rs` [\#9675](https://github.com/apache/arrow-rs/issues/9675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\] Add BloomFilterProperties builder API to make bloom filter configuration explicit [\#9667](https://github.com/apache/arrow-rs/issues/9667) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Bump max throughput in `flight` benchmark before blocking [\#10070](https://github.com/apache/arrow-rs/pull/10070) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- Add coalesce inline-view filter benchmarks [\#10050](https://github.com/apache/arrow-rs/pull/10050) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ClSlaid](https://github.com/ClSlaid))
- fix: better error handling for negative size of FixedSizeBinary [\#10042](https://github.com/apache/arrow-rs/pull/10042) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([theirix](https://github.com/theirix))
- bench\(parquet\): add Sbbf check/insert benchmarks [\#10041](https://github.com/apache/arrow-rs/pull/10041) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dmatth1](https://github.com/dmatth1))
- arrow-cast: Add ability to cast plain struct to dictionary [\#10039](https://github.com/apache/arrow-rs/pull/10039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- \[\#10029\]\[benchmarks\] arrow-flight roundtrip as well as encode/decode  [\#10031](https://github.com/apache/arrow-rs/pull/10031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- Call `align_buffers()` in `from_ffi`, remove redundant call from `arrow-pyarrow` [\#10030](https://github.com/apache/arrow-rs/pull/10030) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbutrovich](https://github.com/mbutrovich))
- Adjust Variant size expectation for s390x architecture [\#10027](https://github.com/apache/arrow-rs/pull/10027) ([frantisekz](https://github.com/frantisekz))
- bench\(parquet\): add short and large string `arrow_writer` benchmarks [\#10021](https://github.com/apache/arrow-rs/pull/10021) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Pluggable page spilling API for the Parquet ArrowWriter \(PageStore\) [\#10020](https://github.com/apache/arrow-rs/pull/10020) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- fix: Reject empty strings when casting strings to decimal [\#10010](https://github.com/apache/arrow-rs/pull/10010) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([neilconway](https://github.com/neilconway))
- feat: Implement decimal \<-\> float16 casts [\#10008](https://github.com/apache/arrow-rs/pull/10008) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([neilconway](https://github.com/neilconway))
- fix\(cast\): Trying to fix cast losting schema problem [\#10005](https://github.com/apache/arrow-rs/pull/10005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))
- fix\(parquet\): validate INT96 column metadata statistics [\#10003](https://github.com/apache/arrow-rs/pull/10003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([fallintoplace](https://github.com/fallintoplace))
- fix\(arrow-schema\): allow empty metadata value for UUID extension type [\#10001](https://github.com/apache/arrow-rs/pull/10001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- Add helper functions to create `LogicalType` struct variants [\#9996](https://github.com/apache/arrow-rs/pull/9996) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- fix: prevent panic in record reader when row group metadata overcounts num\_rows [\#9993](https://github.com/apache/arrow-rs/pull/9993) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([BoazC-MSFT](https://github.com/BoazC-MSFT))
- feat: extract `has_false` and `has_true` from BooleanArray to `BooleanBuffer` and reuse for no nulls [\#9987](https://github.com/apache/arrow-rs/pull/9987) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Validate FIXED\_LEN\_BYTE\_ARRAY length for DECIMAL and INTERVAL types [\#9985](https://github.com/apache/arrow-rs/pull/9985) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([CynicDog](https://github.com/CynicDog))
- fix\(parquet\): exclude single-leaf struct roots from predicate cache [\#9983](https://github.com/apache/arrow-rs/pull/9983) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([imhy](https://github.com/imhy))
- Adds is\_null function to RowAccessor [\#9979](https://github.com/apache/arrow-rs/pull/9979) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([choubacha](https://github.com/choubacha))
- Fix parquet-variant build on wasm targets [\#9978](https://github.com/apache/arrow-rs/pull/9978) ([AdamGS](https://github.com/AdamGS))
- Safely ignore Parquet fields with unimplemented Thrift types [\#9974](https://github.com/apache/arrow-rs/pull/9974) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- fix\(parquet\): bound data page byte size for large variable-width values [\#9972](https://github.com/apache/arrow-rs/pull/9972) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- feat\(parquet\): Add `ParquetPushDecoder::into_builder` to allow swapping projections / row filters at row group boundaries [\#9968](https://github.com/apache/arrow-rs/pull/9968) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- chore\(deps\): bump peaceiris/actions-gh-pages from 4.0.0 to 4.1.0 [\#9966](https://github.com/apache/arrow-rs/pull/9966) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `DatePart` enum 1-indexed variants [\#9965](https://github.com/apache/arrow-rs/pull/9965) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sdf-jkl](https://github.com/sdf-jkl))
- fix\(arrow-cast\): support full Date32 range when parsing extended-year dates [\#9961](https://github.com/apache/arrow-rs/pull/9961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([swanandx](https://github.com/swanandx))
- Implement AnyRee [\#9959](https://github.com/apache/arrow-rs/pull/9959) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- test: add overflow tests for MutableBuffer [\#9958](https://github.com/apache/arrow-rs/pull/9958) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SoimanVasile](https://github.com/SoimanVasile))
- feat\(parquet\): generalize value encoder inputs [\#9955](https://github.com/apache/arrow-rs/pull/9955) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- feat\(parquet\): add all-null fast paths for level building [\#9954](https://github.com/apache/arrow-rs/pull/9954) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- fix\(ipc\): handle duplicate projection indices in IPC reader [\#9952](https://github.com/apache/arrow-rs/pull/9952) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))
- Fix MSRV check by checking in Cargo.lock [\#9941](https://github.com/apache/arrow-rs/pull/9941) ([alamb](https://github.com/alamb))
- benchmarks for writing REE arrays to parquet [\#9936](https://github.com/apache/arrow-rs/pull/9936) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- Validate encoded Thrift lists match the schema [\#9924](https://github.com/apache/arrow-rs/pull/9924) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[arrow-array\] use usize arithmetic in FixedSizeBinaryArray, aggressive overflow checks [\#9910](https://github.com/apache/arrow-rs/pull/9910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat\(parquet\): add uses\_key\_retriever method to FileDecryptionProperties [\#9895](https://github.com/apache/arrow-rs/pull/9895) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Support ListView/BinaryView/RunEndEncoded types in integration test JSON parser [\#9888](https://github.com/apache/arrow-rs/pull/9888) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([paleolimbot](https://github.com/paleolimbot))
- feat\(parquet\): add BloomFilterPropertiesBuilder [\#9877](https://github.com/apache/arrow-rs/pull/9877) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([CuteChuanChuan](https://github.com/CuteChuanChuan))
- perf\[arrow-select\]: add specialized REE interleave [\#9856](https://github.com/apache/arrow-rs/pull/9856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- bench\(parquet\): add `ListArray` benchmarks for runtime and peak memory [\#9846](https://github.com/apache/arrow-rs/pull/9846) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- feat\(parquet\): separate push decoder frontier state from row-group decoding [\#9804](https://github.com/apache/arrow-rs/pull/9804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- arrow: add oversized coalesce take benchmarks [\#9799](https://github.com/apache/arrow-rs/pull/9799) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ClSlaid](https://github.com/ClSlaid))
- Remove redundant benchmarks in `cast_kernels` [\#9789](https://github.com/apache/arrow-rs/pull/9789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Align cast logic for from/to\_decimal for variant [\#9689](https://github.com/apache/arrow-rs/pull/9689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([klion26](https://github.com/klion26))
- \[Parquet\]: GH-563: Make `path_in_schema` optional [\#9678](https://github.com/apache/arrow-rs/pull/9678) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add support for FixedSizeList to variant\_to\_arrow [\#9663](https://github.com/apache/arrow-rs/pull/9663) ([rishvin](https://github.com/rishvin))
- Reduce Miri runtime even more [\#9650](https://github.com/apache/arrow-rs/pull/9650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))


\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
