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

## [52.0.0](https://github.com/apache/arrow-rs/tree/52.0.0) (2024-06-03)

[Full Changelog](https://github.com/apache/arrow-rs/compare/51.0.0...52.0.0)

**Breaking changes:**

- chore: Make binary\_mut kernel accept different type for second arg [\#5833](https://github.com/apache/arrow-rs/pull/5833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix\(flightsql\): remove Any encoding of `DoPutPreparedStatementResult` [\#5817](https://github.com/apache/arrow-rs/pull/5817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([erratic-pattern](https://github.com/erratic-pattern))
- Encode UUID as FixedLenByteArray in parquet\_derive [\#5773](https://github.com/apache/arrow-rs/pull/5773) ([conradludgate](https://github.com/conradludgate))
- Structured interval types for `IntervalMonthDayNano` or `IntervalDayTime`  \(\#3125\) \(\#5654\) [\#5769](https://github.com/apache/arrow-rs/pull/5769) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fallible stream for arrow-flight do\_exchange call \(\#3462\) [\#5698](https://github.com/apache/arrow-rs/pull/5698) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([opensourcegeek](https://github.com/opensourcegeek))
- Update object\_store dependency in arrow to `0.10.0` [\#5675](https://github.com/apache/arrow-rs/pull/5675) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove deprecated JSON writer [\#5651](https://github.com/apache/arrow-rs/pull/5651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Change `UnionArray` constructors [\#5623](https://github.com/apache/arrow-rs/pull/5623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Update py03 from 0.20 to 0.21 [\#5566](https://github.com/apache/arrow-rs/pull/5566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Optionally require alignment when reading IPC, respect alignment when writing [\#5554](https://github.com/apache/arrow-rs/pull/5554) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([hzuo](https://github.com/hzuo))

**Implemented enhancements:**

- Serialize `Binary` and `LargeBinary` as HEX with JSON Writer [\#5783](https://github.com/apache/arrow-rs/issues/5783) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Some optimizations in arrow\_buffer::util::bit\_util do more harm than good [\#5771](https://github.com/apache/arrow-rs/issues/5771) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support skipping comments in CSV files [\#5758](https://github.com/apache/arrow-rs/issues/5758) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  `parquet-derive` should be included in repository README. [\#5751](https://github.com/apache/arrow-rs/issues/5751)
- proposal: Make AsyncArrowWriter accepts AsyncFileWriter trait instead [\#5738](https://github.com/apache/arrow-rs/issues/5738) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nested nullable fields do not get treated as nullable in data\_gen [\#5712](https://github.com/apache/arrow-rs/issues/5712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Optionally support flexible column lengths [\#5678](https://github.com/apache/arrow-rs/issues/5678) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow Flight SQL example server: do\_handshake should include auth header [\#5665](https://github.com/apache/arrow-rs/issues/5665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add support for the "r+" datatype in the C Data interface / `RunArray` [\#5631](https://github.com/apache/arrow-rs/issues/5631) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Serialize `FixedSizeBinary` as HEX with JSON Writer [\#5620](https://github.com/apache/arrow-rs/issues/5620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cleanup UnionArray Constructors [\#5613](https://github.com/apache/arrow-rs/issues/5613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Zero Copy Support [\#5593](https://github.com/apache/arrow-rs/issues/5593)
- ObjectStore bulk delete [\#5591](https://github.com/apache/arrow-rs/issues/5591)
- Retry on Broken Connection [\#5589](https://github.com/apache/arrow-rs/issues/5589)
- `StreamReader` is not zero-copy [\#5584](https://github.com/apache/arrow-rs/issues/5584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Create `ArrowReaderMetadata` from externalized metadata [\#5582](https://github.com/apache/arrow-rs/issues/5582) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `filter` in `filter_leaves` API propagate error [\#5574](https://github.com/apache/arrow-rs/issues/5574) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `List` in `compare_op` [\#5572](https://github.com/apache/arrow-rs/issues/5572)
- Make FixedSizedList Json serializable [\#5568](https://github.com/apache/arrow-rs/issues/5568) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-ord: Support sortting StructArray [\#5559](https://github.com/apache/arrow-rs/issues/5559) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add scientific notation decimal parsing in `parse_decimal` [\#5549](https://github.com/apache/arrow-rs/issues/5549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `take` kernel support for `StringViewArray` and `BinaryViewArray` [\#5511](https://github.com/apache/arrow-rs/issues/5511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `filter` kernel support for `StringViewArray` and `BinaryViewArray` [\#5510](https://github.com/apache/arrow-rs/issues/5510) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Display support for `StringViewArray` and `BinaryViewArray` [\#5509](https://github.com/apache/arrow-rs/issues/5509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Arrow Flight format support for `StringViewArray` and `BinaryViewArray` [\#5507](https://github.com/apache/arrow-rs/issues/5507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
-  IPC format support for `StringViewArray` and `BinaryViewArray` [\#5506](https://github.com/apache/arrow-rs/issues/5506) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- List Row Encoding Sorts Incorrectly [\#5807](https://github.com/apache/arrow-rs/issues/5807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Schema Root Message Name Ignored by parquet-fromcsv [\#5804](https://github.com/apache/arrow-rs/issues/5804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Compute data buffer length by using start and end values in offset buffer [\#5756](https://github.com/apache/arrow-rs/issues/5756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: ByteArrayEncoder allocates large unused FallbackEncoder for Parquet 2 [\#5755](https://github.com/apache/arrow-rs/issues/5755) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The CI pipeline `Archery test With other arrow` is broken [\#5742](https://github.com/apache/arrow-rs/issues/5742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unable to parse scientific notation string to decimal when scale is 0 [\#5739](https://github.com/apache/arrow-rs/issues/5739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Stateless prepared statements wrap `DoPutPreparedStatementResult` with `Any` which is differs from Go implementation [\#5731](https://github.com/apache/arrow-rs/issues/5731) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- "Rustdocs are clean \(amd64, nightly\)" CI check is failing [\#5725](https://github.com/apache/arrow-rs/issues/5725) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- "Archery test With other arrows" integration tests are failing  [\#5719](https://github.com/apache/arrow-rs/issues/5719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet\_derive: invalid examples/documentation [\#5687](https://github.com/apache/arrow-rs/issues/5687)
- Arrow FLight SQL: invalid location in get\_flight\_info\_prepared\_statement [\#5669](https://github.com/apache/arrow-rs/issues/5669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Rust Interval definition incorrect [\#5654](https://github.com/apache/arrow-rs/issues/5654) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DECIMAL regex in csv reader does not accept positive exponent specifier [\#5648](https://github.com/apache/arrow-rs/issues/5648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- panic when casting `ListArray` to `FixedSizeList` [\#5642](https://github.com/apache/arrow-rs/issues/5642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FixedSizeListArray::try\_new Errors on Entirely Null Array With Size 0 [\#5614](https://github.com/apache/arrow-rs/issues/5614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `parquet / Build wasm32 (pull_request)` CI check failing on main [\#5565](https://github.com/apache/arrow-rs/issues/5565) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Documentation fix: example in parquet/src/column/mod.rs is incorrect [\#5560](https://github.com/apache/arrow-rs/issues/5560) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC code writes data with insufficient alignment [\#5553](https://github.com/apache/arrow-rs/issues/5553) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Cannot access example Flight SQL Server from dbeaver [\#5540](https://github.com/apache/arrow-rs/issues/5540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- parquet: "not yet implemented" error when codec is actually implemented but disabled [\#5520](https://github.com/apache/arrow-rs/issues/5520) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Minor: Improve arrow\_cast documentation [\#5825](https://github.com/apache/arrow-rs/pull/5825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Improve `ArrowReaderBuilder::with_row_selection` docs [\#5824](https://github.com/apache/arrow-rs/pull/5824) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Add examples for ColumnPath::from [\#5813](https://github.com/apache/arrow-rs/pull/5813) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Clarify docs on `EnabledStatistics` [\#5812](https://github.com/apache/arrow-rs/pull/5812) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add parquet-derive to repository README [\#5795](https://github.com/apache/arrow-rs/pull/5795) ([konjac](https://github.com/konjac))
- Refine ParquetRecordBatchReaderBuilder docs [\#5774](https://github.com/apache/arrow-rs/pull/5774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- docs: add sizing explanation to bloom filter docs in parquet [\#5705](https://github.com/apache/arrow-rs/pull/5705) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([hiltontj](https://github.com/hiltontj))

**Closed issues:**

- `binary_mut` kernel requires both args to be the same type \(which is inconsistent with `binary`\) [\#5818](https://github.com/apache/arrow-rs/issues/5818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when displaying debug the results via log::info in the browser. [\#5599](https://github.com/apache/arrow-rs/issues/5599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- feat: impl \*Assign ops for types in arrow-buffer [\#5832](https://github.com/apache/arrow-rs/pull/5832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Relax zstd-sys Version Pin [\#5829](https://github.com/apache/arrow-rs/pull/5829) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([waynexia](https://github.com/waynexia))
- Minor: Document timestamp with/without cast behavior [\#5826](https://github.com/apache/arrow-rs/pull/5826) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: invalid examples/documentation in parquet\_derive doc [\#5823](https://github.com/apache/arrow-rs/pull/5823) ([Weijun-H](https://github.com/Weijun-H))
- Check length of `FIXED_LEN_BYTE_ARRAY` for `uuid` logical parquet type [\#5821](https://github.com/apache/arrow-rs/pull/5821) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- Allow overriding the inferred parquet schema root [\#5814](https://github.com/apache/arrow-rs/pull/5814) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Revisit List Row Encoding \(\#5807\) [\#5811](https://github.com/apache/arrow-rs/pull/5811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.83 to =1.0.84 [\#5805](https://github.com/apache/arrow-rs/pull/5805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix typo continuation maker -\> marker [\#5802](https://github.com/apache/arrow-rs/pull/5802) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([djanderson](https://github.com/djanderson))
- fix: serialization of decimal [\#5801](https://github.com/apache/arrow-rs/pull/5801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Allow constructing ByteViewArray from existing blocks [\#5796](https://github.com/apache/arrow-rs/pull/5796) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Push SortOptions into DynComparator Allowing Nested Comparisons \(\#5426\) [\#5792](https://github.com/apache/arrow-rs/pull/5792) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix incorrect URL to Parquet CPP types.h [\#5790](https://github.com/apache/arrow-rs/pull/5790) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.82 to =1.0.83 [\#5789](https://github.com/apache/arrow-rs/pull/5789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update prost-build requirement from =0.12.4 to =0.12.6 [\#5788](https://github.com/apache/arrow-rs/pull/5788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Refine parquet documentation on types and metadata [\#5786](https://github.com/apache/arrow-rs/pull/5786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- feat\(arrow-json\): encode `Binary` and `LargeBinary` types as hex when writing JSON [\#5785](https://github.com/apache/arrow-rs/pull/5785) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- fix broken link to ballista crate in README.md [\#5784](https://github.com/apache/arrow-rs/pull/5784) ([navicore](https://github.com/navicore))
- feat\(arrow-csv\): support encoding of binary in CSV writer [\#5782](https://github.com/apache/arrow-rs/pull/5782) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- Fix documentation for parquet `parse_metadata`, `decode_metadata` and `decode_footer` [\#5781](https://github.com/apache/arrow-rs/pull/5781) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Support casting a `FixedSizedList<T>[1]` to `T` [\#5779](https://github.com/apache/arrow-rs/pull/5779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sadboy](https://github.com/sadboy))
- \[parquet\] Set the default size of BitWriter in DeltaBitPackEncoder to 1MB [\#5776](https://github.com/apache/arrow-rs/pull/5776) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- Remove harmful table lookup optimization for bitmap operations [\#5772](https://github.com/apache/arrow-rs/pull/5772) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- Remove deprecated comparison kernels \(\#4733\) [\#5768](https://github.com/apache/arrow-rs/pull/5768) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add environment variable definitions to run the nanoarrow integration tests [\#5764](https://github.com/apache/arrow-rs/pull/5764) ([paleolimbot](https://github.com/paleolimbot))
- Downgrade to Rust 1.77 in integration pipeline to fix CI \(\#5719\) [\#5761](https://github.com/apache/arrow-rs/pull/5761) ([tustvold](https://github.com/tustvold))
- Expose boolean builder contents [\#5760](https://github.com/apache/arrow-rs/pull/5760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- Allow specifying comment character for CSV reader [\#5759](https://github.com/apache/arrow-rs/pull/5759) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bbannier](https://github.com/bbannier))
- Expose the null buffer of every builder that has one [\#5754](https://github.com/apache/arrow-rs/pull/5754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- feat: Make AsyncArrowWriter accepts AsyncFileWriter [\#5753](https://github.com/apache/arrow-rs/pull/5753) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Xuanwo](https://github.com/Xuanwo))
- Improve repository readme [\#5752](https://github.com/apache/arrow-rs/pull/5752) ([alamb](https://github.com/alamb))
- Document object store release cadence [\#5750](https://github.com/apache/arrow-rs/pull/5750) ([alamb](https://github.com/alamb))
- Compute data buffer length by using start and end values in offset buffer [\#5741](https://github.com/apache/arrow-rs/pull/5741) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix: parse string of scientific notation to decimal when the scale is 0 [\#5740](https://github.com/apache/arrow-rs/pull/5740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Minor: avoid \(likely unreachable\) panic in FlightClient [\#5734](https://github.com/apache/arrow-rs/pull/5734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Update proc-macro2 requirement from =1.0.81 to =1.0.82 [\#5732](https://github.com/apache/arrow-rs/pull/5732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Improve error message for timestamp queries outside supported range [\#5730](https://github.com/apache/arrow-rs/pull/5730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Abdi-29](https://github.com/Abdi-29))
- Refactor to share code between do\_put and do\_exchange calls [\#5728](https://github.com/apache/arrow-rs/pull/5728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([opensourcegeek](https://github.com/opensourcegeek))
- Update brotli requirement from 5.0 to 6.0 [\#5726](https://github.com/apache/arrow-rs/pull/5726) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix `GenericListBuilder` test typo [\#5724](https://github.com/apache/arrow-rs/pull/5724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Deprecate NullBuilder capacity, as it behaves in a surprising way [\#5721](https://github.com/apache/arrow-rs/pull/5721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- Fix nested nullability when randomly generating arrays [\#5713](https://github.com/apache/arrow-rs/pull/5713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- Fix up clippy for Rust 1.78 [\#5710](https://github.com/apache/arrow-rs/pull/5710) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Support casting `StringView`/`BinaryView` --\> `StringArray`/`BinaryArray`. [\#5704](https://github.com/apache/arrow-rs/pull/5704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Fix documentation around handling of nulls in cmp kernels [\#5697](https://github.com/apache/arrow-rs/pull/5697) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Support casting `StringArray`/`BinaryArray` --\> `StringView` / `BinaryView` [\#5686](https://github.com/apache/arrow-rs/pull/5686) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Add support for flexible column lengths [\#5679](https://github.com/apache/arrow-rs/pull/5679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Posnet](https://github.com/Posnet))
- Move ffi stream and utils from arrow to arrow-array [\#5670](https://github.com/apache/arrow-rs/pull/5670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Arrow Flight SQL example JDBC driver incompatibility [\#5666](https://github.com/apache/arrow-rs/pull/5666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([istvan-fodor](https://github.com/istvan-fodor))
- Add `ListView` & `LargeListView` basic construction and validation [\#5664](https://github.com/apache/arrow-rs/pull/5664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Update proc-macro2 requirement from =1.0.80 to =1.0.81 [\#5659](https://github.com/apache/arrow-rs/pull/5659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Modify decimal regex to accept positive exponent specifier [\#5649](https://github.com/apache/arrow-rs/pull/5649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jdcasale](https://github.com/jdcasale))
- feat: JSON encoding of `FixedSizeList` [\#5646](https://github.com/apache/arrow-rs/pull/5646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- Update proc-macro2 requirement from =1.0.79 to =1.0.80 [\#5644](https://github.com/apache/arrow-rs/pull/5644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: panic when casting `ListArray` to `FixedSizeList` [\#5643](https://github.com/apache/arrow-rs/pull/5643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonahgao](https://github.com/jonahgao))
- Add more invalid utf8 parquet reader tests [\#5639](https://github.com/apache/arrow-rs/pull/5639) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update brotli requirement from 4.0 to 5.0 [\#5637](https://github.com/apache/arrow-rs/pull/5637) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update flatbuffers requirement from 23.1.21 to 24.3.25 [\#5636](https://github.com/apache/arrow-rs/pull/5636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Increase `BinaryViewArray` test coverage [\#5635](https://github.com/apache/arrow-rs/pull/5635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- PrettyPrint support for `StringViewArray` and `BinaryViewArray` [\#5634](https://github.com/apache/arrow-rs/pull/5634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat\(ffi\): add run end encoded arrays [\#5632](https://github.com/apache/arrow-rs/pull/5632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([notfilippo](https://github.com/notfilippo))
- Accept parquet schemas without explicitly required Map keys [\#5630](https://github.com/apache/arrow-rs/pull/5630) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jupiter](https://github.com/jupiter))
- Implement `filter` kernel for byte view arrays. [\#5624](https://github.com/apache/arrow-rs/pull/5624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- feat: encode FixedSizeBinary in JSON as hex string [\#5622](https://github.com/apache/arrow-rs/pull/5622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- Update Flight crate README version [\#5621](https://github.com/apache/arrow-rs/pull/5621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([phillipleblanc](https://github.com/phillipleblanc))
- feat: support reading and writing`StringView` and `BinaryView` in parquet \(part 1\) [\#5618](https://github.com/apache/arrow-rs/pull/5618) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Use FixedSizeListArray::new in FixedSizeListBuilder [\#5612](https://github.com/apache/arrow-rs/pull/5612) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- String to decimal conversion written using E/scientific notation [\#5611](https://github.com/apache/arrow-rs/pull/5611) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Nekit2217](https://github.com/Nekit2217))
- Account for Timezone when Casting Timestamp to Date32 [\#5605](https://github.com/apache/arrow-rs/pull/5605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Lordworms](https://github.com/Lordworms))
- Update prost-build requirement from =0.12.3 to =0.12.4 [\#5604](https://github.com/apache/arrow-rs/pull/5604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix panic when displaying dates on 32-bit platforms [\#5603](https://github.com/apache/arrow-rs/pull/5603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ivanceras](https://github.com/ivanceras))
- Implement `take` kernel for byte view array. [\#5602](https://github.com/apache/arrow-rs/pull/5602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Add tests for Arrow Flight support for `StringViewArray` and `BinaryViewArray` [\#5601](https://github.com/apache/arrow-rs/pull/5601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([XiangpengHao](https://github.com/XiangpengHao))
- test: Add a test for RowFilter with nested type [\#5600](https://github.com/apache/arrow-rs/pull/5600) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Minor: Add docs for GenericBinaryBuilder, links to `GenericStringBuilder` [\#5597](https://github.com/apache/arrow-rs/pull/5597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Bump chrono-tz from 0.8 to 0.9 [\#5596](https://github.com/apache/arrow-rs/pull/5596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Update brotli requirement from 3.3 to 4.0 [\#5586](https://github.com/apache/arrow-rs/pull/5586) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `UnionArray::into_parts` [\#5585](https://github.com/apache/arrow-rs/pull/5585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Expose ArrowReaderMetadata::try\_new [\#5583](https://github.com/apache/arrow-rs/pull/5583) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Add `try_filter_leaves` to propagate error from filter closure [\#5575](https://github.com/apache/arrow-rs/pull/5575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- filter for run end array [\#5573](https://github.com/apache/arrow-rs/pull/5573) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fabianmurariu](https://github.com/fabianmurariu))
- Pin zstd-sys to `v2.0.9` in parquet [\#5567](https://github.com/apache/arrow-rs/pull/5567) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Split arrow\_cast::cast::string into it's own submodule [\#5563](https://github.com/apache/arrow-rs/pull/5563) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Correct example code for column \(\#5560\) [\#5561](https://github.com/apache/arrow-rs/pull/5561) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zgershkoff](https://github.com/zgershkoff))
- Split arrow\_cast::cast::dictionary into it's own submodule [\#5555](https://github.com/apache/arrow-rs/pull/5555) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Split arrow\_cast::cast::decimal into it's own submodule [\#5552](https://github.com/apache/arrow-rs/pull/5552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Fix new clippy lints for Rust 1.77 [\#5544](https://github.com/apache/arrow-rs/pull/5544) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: correctly encode ticket [\#5543](https://github.com/apache/arrow-rs/pull/5543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([freddieptf](https://github.com/freddieptf))
- feat: implemented with\_field\(\) for FixedSizeListBuilder [\#5541](https://github.com/apache/arrow-rs/pull/5541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([istvan-fodor](https://github.com/istvan-fodor))
- Split arrow\_cast::cast::list into it's own submodule [\#5537](https://github.com/apache/arrow-rs/pull/5537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Bump black from 22.10.0 to 24.3.0 in /parquet/pytest [\#5535](https://github.com/apache/arrow-rs/pull/5535) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add OffsetBufferBuilder [\#5532](https://github.com/apache/arrow-rs/pull/5532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add IPC StreamDecoder [\#5531](https://github.com/apache/arrow-rs/pull/5531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- IPC format support for StringViewArray and BinaryViewArray [\#5525](https://github.com/apache/arrow-rs/pull/5525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- parquet: Use specific error variant when codec is disabled [\#5521](https://github.com/apache/arrow-rs/pull/5521) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- impl `From<ScalarBuffer<T>>` for `Vec<T>` [\#5518](https://github.com/apache/arrow-rs/pull/5518) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
