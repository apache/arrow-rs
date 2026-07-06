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

## [59.1.0](https://github.com/apache/arrow-rs/tree/59.1.0) (2026-07-03)

[Full Changelog](https://github.com/apache/arrow-rs/compare/59.0.0...59.1.0)

**Implemented enhancements:**

- Fast path for nested `DictionaryArray` casting [\#10247](https://github.com/apache/arrow-rs/issues/10247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet/arrow: reading multiple nested columns fails with "Not all children array length are the same!" when a list continues across DataPageV2 page boundary [\#10243](https://github.com/apache/arrow-rs/issues/10243) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add product aggregate kernel to arrow-rs [\#10150](https://github.com/apache/arrow-rs/issues/10150) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Stricter `DataType` parsing [\#10146](https://github.com/apache/arrow-rs/issues/10146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support validating CSV headers against Schema [\#10143](https://github.com/apache/arrow-rs/issues/10143) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-ipc: Supports compression level configuration for arrow-ipc writer [\#10132](https://github.com/apache/arrow-rs/issues/10132) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] `VariantArray` field API naming [\#10093](https://github.com/apache/arrow-rs/issues/10093)
- Add `StructArray::field_` APIs symmetric to `StructArray::column_` ones [\#10092](https://github.com/apache/arrow-rs/issues/10092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-buffer: implement Saturating, CheckedShl, Not num-traits for i256 [\#10087](https://github.com/apache/arrow-rs/issues/10087) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- feat: native concat for `MapArray` [\#10047](https://github.com/apache/arrow-rs/issues/10047) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Add `variant_to_arrow` `Dictionary/REE` type support [\#10013](https://github.com/apache/arrow-rs/issues/10013)

**Fixed bugs:**

- arrow-row on fixed size binary/list with size 0 and no nulls return wrong length [\#10270](https://github.com/apache/arrow-rs/issues/10270)
- casting list to 0-size fixedsizelist can cause incorrect output length [\#10227](https://github.com/apache/arrow-rs/issues/10227) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Buffer count mismatched with metadata when encoding records with dictionary of dictionaries [\#10213](https://github.com/apache/arrow-rs/issues/10213) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `Buffer::into_mutable` is not consistent regarding sliced data and can lead to panics [\#10117](https://github.com/apache/arrow-rs/issues/10117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet\_derive: cannot read or write columns whose name is a Rust keyword \(raw identifiers like r\#type become column "r\#type"\) [\#10112](https://github.com/apache/arrow-rs/issues/10112)
- parquet: fix OffsetBuffer panic on corrupt input [\#10107](https://github.com/apache/arrow-rs/issues/10107) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet geospatial conversion uses metadata key "algorithm" instead of "edges" in geoarrow metadata [\#9929](https://github.com/apache/arrow-rs/issues/9929) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- doc: More comments to `concat_batches` [\#10178](https://github.com/apache/arrow-rs/pull/10178) ([2010YOUY01](https://github.com/2010YOUY01))
- Minor: improve PageStore docs with a temp-file spilling example [\#10074](https://github.com/apache/arrow-rs/pull/10074) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- perf: interleave\_list for List\<Primitive\> could be optimized? [\#10022](https://github.com/apache/arrow-rs/issues/10022) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- perf\(arrow-ord\): Avoid full index materialization for small-limit lexsorts [\#9990](https://github.com/apache/arrow-rs/issues/9990) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace conversion of binary-\>string in arrow-row from arraydata to direct construction [\#10261](https://github.com/apache/arrow-rs/pull/10261) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- \[arrow-flight encode path\]re-use flatbufferbuilder [\#10220](https://github.com/apache/arrow-rs/pull/10220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- \[10125\] arrow-flight decode path optimizations \(add `skip_validation` to arrow-flight\) [\#10206](https://github.com/apache/arrow-rs/pull/10206) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- Improve performance of `concat_elements` ByteViewArray concatenation [\#10161](https://github.com/apache/arrow-rs/pull/10161) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- \[arrow-flight\] Optimize flight, remove some allocations, add dictionary focused benchmarks [\#10126](https://github.com/apache/arrow-rs/pull/10126) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- optimize\(concat\): concat map implementation [\#10048](https://github.com/apache/arrow-rs/pull/10048) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))
- Reduce copies in Arrow IPC writer [\#10044](https://github.com/apache/arrow-rs/pull/10044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- perf\(interleave\): Optimize list interleave\_list when child is primitive [\#10025](https://github.com/apache/arrow-rs/pull/10025) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mapleFU](https://github.com/mapleFU))

**Closed issues:**

- Soundness: Unsound alignment contract in public `FromBytes` trait and `BitReader::get_batch` [\#10164](https://github.com/apache/arrow-rs/issues/10164) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- ParquetPushDecoder: expose the next row-group index that try\_next\_reader will yield [\#10148](https://github.com/apache/arrow-rs/issues/10148) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- arrow-ipc: Extend writer benchmarks to include dictionaries [\#10119](https://github.com/apache/arrow-rs/issues/10119) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- bench\(parquet\): benchmark for nested list write [\#10083](https://github.com/apache/arrow-rs/issues/10083) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support i256 implement From\<i128\> [\#10080](https://github.com/apache/arrow-rs/issues/10080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- fix\(arrow-row\): allow to convert non empty fixed size binary/list array with size length 0 and no nulls [\#10271](https://github.com/apache/arrow-rs/pull/10271) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- chore: Fix audit CI run by ignore quick-xml audit advisories [\#10267](https://github.com/apache/arrow-rs/pull/10267) ([alamb](https://github.com/alamb))
- fix main: parquet test compilation failure [\#10266](https://github.com/apache/arrow-rs/pull/10266) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- minor: drive-by refactors for dicts in substring & filter [\#10264](https://github.com/apache/arrow-rs/pull/10264) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add validated row decode benchmark [\#10259](https://github.com/apache/arrow-rs/pull/10259) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- arrow-cast: Add optimized path for unnesting a dict [\#10248](https://github.com/apache/arrow-rs/pull/10248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- feat: support uuid from fixed type of length 16 [\#10241](https://github.com/apache/arrow-rs/pull/10241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ariel-miculas](https://github.com/ariel-miculas))
- chore\(deps\): bump actions/cache from 6.0.0 to 6.1.0 [\#10240](https://github.com/apache/arrow-rs/pull/10240) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: Rename parquet feature flag 'flate2-rust\_backened' to 'flate2-rust\_backend' [\#10239](https://github.com/apache/arrow-rs/pull/10239) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dannycjones](https://github.com/dannycjones))
- chore: Make clippy::question\_mark happy [\#10231](https://github.com/apache/arrow-rs/pull/10231) ([Tpt](https://github.com/Tpt))
- fix\(ipc\): reject dictionary-encoded dictionary values [\#10230](https://github.com/apache/arrow-rs/pull/10230) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([goutamadwant](https://github.com/goutamadwant))
- Replace `ArrayData` with direct `Array` construction in `arrow-row` [\#10229](https://github.com/apache/arrow-rs/pull/10229) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- fix: casting list to fixedsizelist didn't respect input length [\#10228](https://github.com/apache/arrow-rs/pull/10228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- chore: Fix clippy::byte\_char\_slices \(use byte strings instead of explicit arrays\) [\#10225](https://github.com/apache/arrow-rs/pull/10225) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Tpt](https://github.com/Tpt))
- nit: arrow-pyarrow: Use string interning [\#10224](https://github.com/apache/arrow-rs/pull/10224) ([Tpt](https://github.com/Tpt))
- Support concatenation of mixed FixedSizeBinary via `concat_elements_dyn` [\#10222](https://github.com/apache/arrow-rs/pull/10222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pepijnve](https://github.com/pepijnve))
- rename Compression struct [\#10221](https://github.com/apache/arrow-rs/pull/10221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- chore\(deps\): bump the all-other-cargo-deps group across 1 directory with 16 updates [\#10218](https://github.com/apache/arrow-rs/pull/10218) ([dependabot[bot]](https://github.com/apps/dependabot))
- chore\(deps\): bump actions/setup-python from 6.2.0 to 6.3.0 [\#10210](https://github.com/apache/arrow-rs/pull/10210) ([dependabot[bot]](https://github.com/apps/dependabot))
- \[10125\] Introduce mult-batch decode benchmarks [\#10207](https://github.com/apache/arrow-rs/pull/10207) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- chore\(deps\): bump actions/cache from 5.0.5 to 6.0.0 [\#10203](https://github.com/apache/arrow-rs/pull/10203) ([dependabot[bot]](https://github.com/apps/dependabot))
- introduce decode benchmarks [\#10202](https://github.com/apache/arrow-rs/pull/10202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- Fix `merge_kernels` benchmark panic due to not wrapping with `Scalar` [\#10199](https://github.com/apache/arrow-rs/pull/10199) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Benchmarks and performance improvement for parquet boolean reader [\#10196](https://github.com/apache/arrow-rs/pull/10196) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- add stale PR workflow [\#10194](https://github.com/apache/arrow-rs/pull/10194) ([Jefffrey](https://github.com/Jefffrey))
- chore: group minor/patch dependabot updates [\#10193](https://github.com/apache/arrow-rs/pull/10193) ([Jefffrey](https://github.com/Jefffrey))
- chore\(deps\): bump http from 1.4.0 to 1.4.2 [\#10191](https://github.com/apache/arrow-rs/pull/10191) ([dependabot[bot]](https://github.com/apps/dependabot))
- chore\(deps\): bump syn from 2.0.117 to 2.0.118 [\#10190](https://github.com/apache/arrow-rs/pull/10190) ([dependabot[bot]](https://github.com/apps/dependabot))
- chore\(deps\): bump chrono from 0.4.44 to 0.4.45 [\#10188](https://github.com/apache/arrow-rs/pull/10188) ([dependabot[bot]](https://github.com/apps/dependabot))
- chore\(deps\): bump uuid from 1.23.1 to 1.23.3 [\#10186](https://github.com/apache/arrow-rs/pull/10186) ([dependabot[bot]](https://github.com/apps/dependabot))
- chore: run `cargo update` to bump quinn [\#10181](https://github.com/apache/arrow-rs/pull/10181) ([Jefffrey](https://github.com/Jefffrey))
- test: cover signed integers and bool in BitReader::get\_batch test [\#10180](https://github.com/apache/arrow-rs/pull/10180) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- \[arrow-select\] perf: Replace `ArrayData` with direct `Array` construction in take kernels [\#10176](https://github.com/apache/arrow-rs/pull/10176) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Return PyValueError for nullable PyArrow struct imports [\#10174](https://github.com/apache/arrow-rs/pull/10174) ([fallintoplace](https://github.com/fallintoplace))
- Fix Variant time microsecond JSON formatting [\#10173](https://github.com/apache/arrow-rs/pull/10173) ([fallintoplace](https://github.com/fallintoplace))
- Split traits for plain and bitpacked decoding and fix soundness issue in BitReader::get\_batch [\#10172](https://github.com/apache/arrow-rs/pull/10172) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- fix: switch generic usages of `i128` to `IntervalMonthDayNano` for MonthDayNano type [\#10171](https://github.com/apache/arrow-rs/pull/10171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- chore: specify `--locked` when cargo installing `cargo-audit` [\#10170](https://github.com/apache/arrow-rs/pull/10170) ([Jefffrey](https://github.com/Jefffrey))
- chore: Fix clippy::useless\_borrows\_in\_formatting [\#10163](https://github.com/apache/arrow-rs/pull/10163) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Tpt](https://github.com/Tpt))
- fix\(arrow-cast\): respect cast safety for overflowing temporal casts [\#10162](https://github.com/apache/arrow-rs/pull/10162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SAY-5](https://github.com/SAY-5))
- chore\(deps\): bump actions/checkout from 6 to 7 [\#10159](https://github.com/apache/arrow-rs/pull/10159) ([dependabot[bot]](https://github.com/apps/dependabot))
- feat\(parquet\): add ParquetPushDecoder::peek\_next\_row\_group\(\) [\#10158](https://github.com/apache/arrow-rs/pull/10158) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- feat\(pyarrow\) `FromPyArrow` on `Vec<T>`: allow any iterable for input [\#10155](https://github.com/apache/arrow-rs/pull/10155) ([Tpt](https://github.com/Tpt))
- nit: pyarrow: simplify class validation error creation [\#10154](https://github.com/apache/arrow-rs/pull/10154) ([Tpt](https://github.com/Tpt))
- \[Variant\] add doc reference to `VariantArrayBuilder` [\#10152](https://github.com/apache/arrow-rs/pull/10152) ([sdf-jkl](https://github.com/sdf-jkl))
- feat: Adds product aggregate compute kernel [\#10151](https://github.com/apache/arrow-rs/pull/10151) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([devanbenz](https://github.com/devanbenz))
- Stricter datatype parsing for decimals, fixedsizelists and time32/64 [\#10147](https://github.com/apache/arrow-rs/pull/10147) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- feat\(arrow\_csv\): add header validation option [\#10144](https://github.com/apache/arrow-rs/pull/10144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiNiHa](https://github.com/XiNiHa))
- \[Parquet\] route dictionary page through the PageStore [\#10142](https://github.com/apache/arrow-rs/pull/10142) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liamzwbao](https://github.com/liamzwbao))
- chore: update pyo3 dependency to 0.29 [\#10134](https://github.com/apache/arrow-rs/pull/10134) ([timsaucer](https://github.com/timsaucer))
- feat\(ipc\): Supports compression level configuration [\#10133](https://github.com/apache/arrow-rs/pull/10133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wForget](https://github.com/wForget))
- fix: write error for dbg output of out of range timestamps [\#10130](https://github.com/apache/arrow-rs/pull/10130) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- \[Variant\] `VariantArray` field API naming [\#10124](https://github.com/apache/arrow-rs/pull/10124) ([sdf-jkl](https://github.com/sdf-jkl))
- feat\(arrow\_array\): add helper function to create MapArray from `Vec<Option<Vec<(Key, Option<Value>)>>>` for tests [\#10123](https://github.com/apache/arrow-rs/pull/10123) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([rluvaton](https://github.com/rluvaton))
- perf\(arrow-ipc\): Add writer benchmarks for dictionaries [\#10122](https://github.com/apache/arrow-rs/pull/10122) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JakeDern](https://github.com/JakeDern))
- feat: support `MapArray` in lengths kernel [\#10121](https://github.com/apache/arrow-rs/pull/10121) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- feat: add `OffsetBuffer::subtract` to allow to shift offsets by value [\#10120](https://github.com/apache/arrow-rs/pull/10120) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- fix: `Buffer::into_mutable` return error instead of panic for converting owned sliced when not start at 0 and fix returned Mutable length [\#10118](https://github.com/apache/arrow-rs/pull/10118) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- chore: update `Bytes` visibility to correctly reflect the actual visibility [\#10115](https://github.com/apache/arrow-rs/pull/10115) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- fix\(parquet\_derive\): support raw identifiers as column names [\#10113](https://github.com/apache/arrow-rs/pull/10113) ([cbmixx](https://github.com/cbmixx))
- removed clippy ignore statment [\#10111](https://github.com/apache/arrow-rs/pull/10111) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- Add `StructArray::field_` APIs symmetric to `StructArray::column_` ones [\#10110](https://github.com/apache/arrow-rs/pull/10110) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sdf-jkl](https://github.com/sdf-jkl))
- fix\(parquet\): return error instead of panicking in pad\_nulls on corrupt input [\#10108](https://github.com/apache/arrow-rs/pull/10108) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thepenguinco](https://github.com/thepenguinco))
- Minor: Add interleave tests for List\<Decimal128\> and List\<Timestamp\(tz\)\> [\#10099](https://github.com/apache/arrow-rs/pull/10099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add arrow-flight test coverage for IPC compression [\#10097](https://github.com/apache/arrow-rs/pull/10097) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- chore\(deps\): bump pyspark from 3.3.2 to 3.4.4 in /parquet/pytest [\#10091](https://github.com/apache/arrow-rs/pull/10091) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- refactor\(parquet\): bundle array reader recursion args into `ReaderArgs` [\#10089](https://github.com/apache/arrow-rs/pull/10089) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- arrow-buffer: implement Saturating, Checked num-traits for i256 [\#10088](https://github.com/apache/arrow-rs/pull/10088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([theirix](https://github.com/theirix))
- bench\(parquet\): add nested list writer benchmarks [\#10084](https://github.com/apache/arrow-rs/pull/10084) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- Implement From\<i128\> for i256 [\#10081](https://github.com/apache/arrow-rs/pull/10081) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- test\(parquet\): drop confusing `main` reference in page-roundtrip test comment [\#10072](https://github.com/apache/arrow-rs/pull/10072) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- ci: Split miri tests into 4 parallel shards [\#10067](https://github.com/apache/arrow-rs/pull/10067) ([AdamGS](https://github.com/AdamGS))
- Add tests and fix corner cases for Parquet/GeoArrow extension type conversion [\#10065](https://github.com/apache/arrow-rs/pull/10065) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([paleolimbot](https://github.com/paleolimbot))
- Support writing REE arrays directly to Parquet [\#10064](https://github.com/apache/arrow-rs/pull/10064) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- test\(arrow-select\): additional tests for inline-view filter fast path \(tests for \#9755\) [\#10054](https://github.com/apache/arrow-rs/pull/10054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- test\(arrow-select\): add take\_bytes coverage for sliced values and nullable offset overflow [\#10053](https://github.com/apache/arrow-rs/pull/10053) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Consolidate `filter_null_mask` into `FilterPredicate::filter_nulls` [\#10049](https://github.com/apache/arrow-rs/pull/10049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Add `VariantBuilder` values check [\#10016](https://github.com/apache/arrow-rs/pull/10016) ([sdf-jkl](https://github.com/sdf-jkl))
- \[Variant\] Preserve `UUID` extension type metadata for Parquet writer [\#10015](https://github.com/apache/arrow-rs/pull/10015) ([sdf-jkl](https://github.com/sdf-jkl))
- feat\(parquet-variant\): add Dictionary and REE variant\_to\_arrow support [\#10014](https://github.com/apache/arrow-rs/pull/10014) ([mneetika](https://github.com/mneetika))
- perf\(arrow-ord\): Avoid full index materialization for small-limit lexsorts [\#9991](https://github.com/apache/arrow-rs/pull/9991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
