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


## [59.2.0](https://github.com/apache/arrow-rs/tree/59.2.0) - (2026-07-31)

[Full Changelog](https://github.com/apache/arrow-rs/compare/59.1.0...59.2.0)

### Enhancements
- Chore: add unchecked array builder methods (#10440) by @Rich-T-kid in [#10440](https://github.com/apache/arrow-rs/pull/10440)
- Feat(arrow-ipc): add sans-IO stream encoder (#10277) by @Phoenix500526 in [#10277](https://github.com/apache/arrow-rs/pull/10277)
- Support suffix white space in arrow-cast parse (#10396) by @Rich-T-kid in [#10396](https://github.com/apache/arrow-rs/pull/10396)
- Feat(coalesce): add size function (#10331) by @rluvaton in [#10331](https://github.com/apache/arrow-rs/pull/10331)
- Fix(parquet): support mask filtering across skipped pages (#10288) by @hhhizzz in [#10288](https://github.com/apache/arrow-rs/pull/10288)
- Support white space prefixed parse for ints and floats (#10374) by @Rich-T-kid in [#10374](https://github.com/apache/arrow-rs/pull/10374)
- Make more of i256 available in const code (#10363) by @AdamGS in [#10363](https://github.com/apache/arrow-rs/pull/10363)
- Expose builder buffer capacity accessors (#10342) by @Weijun-H in [#10342](https://github.com/apache/arrow-rs/pull/10342)
- Add interval multiplication by i64 (#10336) by @peterxcli in [#10336](https://github.com/apache/arrow-rs/pull/10336)
- Make `parquet-index` work with column paths (#10330) by @korowa in [#10330](https://github.com/apache/arrow-rs/pull/10330)
- Feat(arrow-csv): add support for parsing `Float16` (#10343) by @Glatzel in [#10343](https://github.com/apache/arrow-rs/pull/10343)
- [Variant] Add `variant_to_arrow` `Map` type support (#10307) by @sdf-jkl in [#10307](https://github.com/apache/arrow-rs/pull/10307)

### Bug fixes
- Fix(arrow-json): validate ListView child nullability (#10486) by @dk3yyyy in [#10486](https://github.com/apache/arrow-rs/pull/10486)
- Fix(arrow-json): validate map value nullability (#10475) by @subotac in [#10475](https://github.com/apache/arrow-rs/pull/10475)
- Avro: bound VLQDecoder::long against overlong varints (#10407) by @STiFLeR7 in [#10407](https://github.com/apache/arrow-rs/pull/10407)
- Arrow-row: Fix decode_fixed_size_list to apply the corrected_type step for dictionary children (#10414) by @zhuqi-lucas in [#10414](https://github.com/apache/arrow-rs/pull/10414)
- [Variant] make `value` mandatory field for `VariantArray`/`ShreddingState` (#10318) by @sdf-jkl in [#10318](https://github.com/apache/arrow-rs/pull/10318)
- Fix off by one error for slice accounting (#10406) by @Rich-T-kid in [#10406](https://github.com/apache/arrow-rs/pull/10406)
- Fix: `GenericByteViewArray::gc()` drops inline views on the multi-buffer slow path (#10287) by @adriangb in [#10287](https://github.com/apache/arrow-rs/pull/10287)
- Fix(arrow-json): render coerced f32 as its value in the string decoder (#10386) by @hareshkh in [#10386](https://github.com/apache/arrow-rs/pull/10386)
- Fix(arrow-cast): make `b64_encode` reject invalid UTF-8 from misbehaving `Engine` impls (#10324) by @bit2swaz in [#10324](https://github.com/apache/arrow-rs/pull/10324)
- Fix: take FFI_ArrowArrayStream errno values from libc (#10299) by @fornwall in [#10299](https://github.com/apache/arrow-rs/pull/10299)
- Fix(arrow-data): allow full dictionary key range when concatenating (#10323) by @raphaelroshan in [#10323](https://github.com/apache/arrow-rs/pull/10323)
- Don't panic on invalid c ffi schema name (#10328) by @robert3005 in [#10328](https://github.com/apache/arrow-rs/pull/10328)
- Fix(REE): check upfront if sorting empty array or 0 limit (#10293) by @Jefffrey in [#10293](https://github.com/apache/arrow-rs/pull/10293)
- Fix(arrow-avro): bound untrusted OCF block size and item counts (#10237) by @miniex in [#10237](https://github.com/apache/arrow-rs/pull/10237)
- Fix(arrow-array): disallow creating `MapArray` with nullable key field (#10272) by @rluvaton in [#10272](https://github.com/apache/arrow-rs/pull/10272)
- Fix: don't panic on `ArrayData::try_new` on bad input even when `force_validate` feature is on (#10282) by @rluvaton in [#10282](https://github.com/apache/arrow-rs/pull/10282)

### Performance improvements
- Perf(parquet): slice up contiguous buffer for decimals and fsb (#10364) by @MassivePizza in [#10364](https://github.com/apache/arrow-rs/pull/10364)
- Feat(parquet): `RowSelection` can be backed by a `BooleanBuffer` (#10141) by @haohuaijin in [#10141](https://github.com/apache/arrow-rs/pull/10141)
- Perf(parquet): use Cursor in ZSTDCodec to avoid Vec alloc and copy (#10345) by @MassivePizza in [#10345](https://github.com/apache/arrow-rs/pull/10345)
- Optimize(parquet): Nested list batching child.write calls (#10085) by @mapleFU in [#10085](https://github.com/apache/arrow-rs/pull/10085)
- Perf(parquet): splice buffered pages with `write_all` instead of `io::copy` (adapts #10052) (#10353) by @adriangb in [#10353](https://github.com/apache/arrow-rs/pull/10353)
- Hoist calls for null_sentinel (#10356) by @Rich-T-kid in [#10356](https://github.com/apache/arrow-rs/pull/10356)
- Perf: speed up substring_by_char with an ASCII fast path and single-pass bounds (#10334) by @andygrove in [#10334](https://github.com/apache/arrow-rs/pull/10334)
- Cache encoded field name in FieldEncoder (#10296) by @MassivePizza in [#10296](https://github.com/apache/arrow-rs/pull/10296)
- Perf:  allow users to skip utf8 validation (#10319) by @Rich-T-kid in [#10319](https://github.com/apache/arrow-rs/pull/10319)
- Perf: Improve decimal addition and subtraction when scale is equal  (#10333) by @AdamGS in [#10333](https://github.com/apache/arrow-rs/pull/10333)
- Optimize(interleave): implement interleave for FixedSizeList/Map type (#10046) by @mapleFU in [#10046](https://github.com/apache/arrow-rs/pull/10046)
- Perf: Pre-size buffer allocations to avoid intermediate allocations (#10262) by @Rich-T-kid in [#10262](https://github.com/apache/arrow-rs/pull/10262)
- Perf: create dictionary reader config and default unsafeflag to false (#10260) by @Rich-T-kid in [#10260](https://github.com/apache/arrow-rs/pull/10260)
- Perf: Introduce zero copy path when tonic returns an aligned buffer (#10273) by @Rich-T-kid in [#10273](https://github.com/apache/arrow-rs/pull/10273)
- Validate short view strings in separate buffer in arrow-row (#10250) by @Jefffrey in [#10250](https://github.com/apache/arrow-rs/pull/10250)

### Documentation updates
- Docs: clarify decimal negative scale behavior (#10304) by @ByteBaker in [#10304](https://github.com/apache/arrow-rs/pull/10304)
- Docs: fix mutableArrayData comments (#10326) by @Rich-T-kid in [#10326](https://github.com/apache/arrow-rs/pull/10326)
- Docs: trim release schedule for released versions (#10280) by @alamb in [#10280](https://github.com/apache/arrow-rs/pull/10280)
- Align parquet-geospatial crate docs with README (#10302) by @paleolimbot in [#10302](https://github.com/apache/arrow-rs/pull/10302)

### Miscellaneous
- Arrow-avro: Deprecate object_store integration (#10484) by @brancz in [#10484](https://github.com/apache/arrow-rs/pull/10484)
- Parquet: Remove explicit `object_store` integration (#10354) by @brancz in [#10354](https://github.com/apache/arrow-rs/pull/10354)
- Fix(parquet): restore opaque return type for `RowSelection::iter` (#10450) by @haohuaijin in [#10450](https://github.com/apache/arrow-rs/pull/10450)
- Refactor(parquet): split `arrow_reader/selection` into smaller modules (#10434) by @haohuaijin in [#10434](https://github.com/apache/arrow-rs/pull/10434)
- Chore: deduplicate filter nulls code in coalesce/filter kernel (#10348) by @Jefffrey in [#10348](https://github.com/apache/arrow-rs/pull/10348)
- Chore: remove parquet dependency from parquet_derive (#10327) by @ByteBaker in [#10327](https://github.com/apache/arrow-rs/pull/10327)
- Remove the unmaintained paste dependency from arrow (#10303) by @Phoenix500526 in [#10303](https://github.com/apache/arrow-rs/pull/10303)
- Chore: formalize the default map field names to match default arrow spec (#10297) by @rluvaton in [#10297](https://github.com/apache/arrow-rs/pull/10297)

