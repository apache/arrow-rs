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


## [59.2.0](https://github.com/apache/arrow-rs/tree/59.2.0) - (2026-08-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/59.1.0...59.2.0)

### Enhancements
- arrow-avro: Deprecate object_store integration by @brancz in [#10484](https://github.com/apache/arrow-rs/pull/10484)
- chore(arrow): add unchecked array builder methods by @Rich-T-kid in [#10440](https://github.com/apache/arrow-rs/pull/10440)
- feat(arrow-ipc): add sans-IO stream encoder by @Phoenix500526 in [#10277](https://github.com/apache/arrow-rs/pull/10277)
- chore(arrow-cast): support suffix white space in arrow-cast parse by @Rich-T-kid in [#10396](https://github.com/apache/arrow-rs/pull/10396)
- feat(coalesce): add size function by @rluvaton in [#10331](https://github.com/apache/arrow-rs/pull/10331)
- fix(parquet): support mask filtering across skipped pages by @hhhizzz in [#10288](https://github.com/apache/arrow-rs/pull/10288)
- chore(arrow-cast): support white space prefixed parse for ints and floats by @Rich-T-kid in [#10374](https://github.com/apache/arrow-rs/pull/10374)
- chore(arrow-buffer): Make more of i256 available in const code by @AdamGS in [#10363](https://github.com/apache/arrow-rs/pull/10363)
- feat(arrow-array): Expose builder buffer capacity accessors by @Weijun-H in [#10342](https://github.com/apache/arrow-rs/pull/10342)
- feat(arrow-arith): Add interval multiplication by i64 by @peterxcli in [#10336](https://github.com/apache/arrow-rs/pull/10336)
- fix: Make `parquet-index` work with column paths by @korowa in [#10330](https://github.com/apache/arrow-rs/pull/10330)
- feat(arrow-csv): add support for parsing `Float16` by @Glatzel in [#10343](https://github.com/apache/arrow-rs/pull/10343)
- Remove the unmaintained paste dependency from arrow by @Phoenix500526 in [#10303](https://github.com/apache/arrow-rs/pull/10303)
- chore: formalize the default map field names to match default arrow spec by @rluvaton in [#10297](https://github.com/apache/arrow-rs/pull/10297)
- feat(variant): Add `variant_to_arrow` `Map` type support by @sdf-jkl in [#10307](https://github.com/apache/arrow-rs/pull/10307)

### Bug fixes
- fix(arrow-schema): Persist dictionary ordered flag on FFI schema import by @borchero in [#10514](https://github.com/apache/arrow-rs/pull/10514)
- fix(arrow-json): validate ListView child nullability by @dk3yyyy in [#10486](https://github.com/apache/arrow-rs/pull/10486)
- fix(arrow-json): validate map value nullability by @subotac in [#10475](https://github.com/apache/arrow-rs/pull/10475)
- avro: bound VLQDecoder::long against overlong varints by @STiFLeR7 in [#10407](https://github.com/apache/arrow-rs/pull/10407)
- arrow-row: Fix decode_fixed_size_list to apply the corrected_type step for dictionary children by @zhuqi-lucas in [#10414](https://github.com/apache/arrow-rs/pull/10414)
- [Variant] make `value` mandatory field for `VariantArray`/`ShreddingState` by @sdf-jkl in [#10318](https://github.com/apache/arrow-rs/pull/10318)
- fix off by one error for slice accounting by @Rich-T-kid in [#10406](https://github.com/apache/arrow-rs/pull/10406)
- fix: `GenericByteViewArray::gc()` drops inline views on the multi-buffer slow path by @adriangb in [#10287](https://github.com/apache/arrow-rs/pull/10287)
- fix(arrow-json): render coerced f32 as its value in the string decoder by @hareshkh in [#10386](https://github.com/apache/arrow-rs/pull/10386)
- fix(arrow-cast): make `b64_encode` reject invalid UTF-8 from misbehaving `Engine` impls by @bit2swaz in [#10324](https://github.com/apache/arrow-rs/pull/10324)
- fix: take FFI_ArrowArrayStream errno values from libc by @fornwall in [#10299](https://github.com/apache/arrow-rs/pull/10299)
- fix(arrow-data): allow full dictionary key range when concatenating by @raphaelroshan in [#10323](https://github.com/apache/arrow-rs/pull/10323)
- Don't panic on invalid c ffi schema name by @robert3005 in [#10328](https://github.com/apache/arrow-rs/pull/10328)
- fix(REE): check upfront if sorting empty array or 0 limit by @Jefffrey in [#10293](https://github.com/apache/arrow-rs/pull/10293)
- fix(arrow-avro): bound untrusted OCF block size and item counts by @miniex in [#10237](https://github.com/apache/arrow-rs/pull/10237)
- fix(arrow-array): disallow creating `MapArray` with nullable key field by @rluvaton in [#10272](https://github.com/apache/arrow-rs/pull/10272)
- fix: don't panic on `ArrayData::try_new` on bad input even when `force_validate` feature is on by @rluvaton in [#10282](https://github.com/apache/arrow-rs/pull/10282)

### Performance improvements
- perf(parquet): slice up contiguous buffer for decimals and fsb by @MassivePizza in [#10364](https://github.com/apache/arrow-rs/pull/10364)
- feat(parquet): `RowSelection` can be backed by a `BooleanBuffer` by @haohuaijin in [#10141](https://github.com/apache/arrow-rs/pull/10141)
- perf(parquet): use Cursor in ZSTDCodec to avoid Vec alloc and copy by @MassivePizza in [#10345](https://github.com/apache/arrow-rs/pull/10345)
- optimize(parquet): Nested list batching child.write calls by @mapleFU in [#10085](https://github.com/apache/arrow-rs/pull/10085)
- perf(parquet): splice buffered pages with `write_all` instead of `io::copy` (adapts #10052) by @adriangb in [#10353](https://github.com/apache/arrow-rs/pull/10353)
- hoist calls for null_sentinel by @Rich-T-kid in [#10356](https://github.com/apache/arrow-rs/pull/10356)
- perf: speed up substring_by_char with an ASCII fast path and single-pass bounds by @andygrove in [#10334](https://github.com/apache/arrow-rs/pull/10334)
- Cache encoded field name in FieldEncoder by @MassivePizza in [#10296](https://github.com/apache/arrow-rs/pull/10296)
- perf:  allow users to skip utf8 validation in arrow-row by @Rich-T-kid in [#10319](https://github.com/apache/arrow-rs/pull/10319)
- perf: Improve decimal addition and subtraction when scale is equal by @AdamGS in [#10333](https://github.com/apache/arrow-rs/pull/10333)
- optimize(interleave): implement interleave for FixedSizeList/Map type by @mapleFU in [#10046](https://github.com/apache/arrow-rs/pull/10046)
- Perf: Pre-size buffer allocations to avoid intermediate allocations by @Rich-T-kid in [#10262](https://github.com/apache/arrow-rs/pull/10262)
- Perf: create dictionary reader config and default unsafeflag to false by @Rich-T-kid in [#10260](https://github.com/apache/arrow-rs/pull/10260)
- Perf: Introduce zero copy path when tonic returns an aligned buffer by @Rich-T-kid in [#10273](https://github.com/apache/arrow-rs/pull/10273)
- Validate short view strings in separate buffer in arrow-row by @Jefffrey in [#10250](https://github.com/apache/arrow-rs/pull/10250)

### Documentation updates
- chore(parquet): add link to ticket in object_store deprecation message by @alamb in [#10502](https://github.com/apache/arrow-rs/pull/10502)
- chore(avro): add link to ticket in object_store deprecation message by @alamb in [#10503](https://github.com/apache/arrow-rs/pull/10503)
- docs: clarify decimal negative scale behavior by @ByteBaker in [#10304](https://github.com/apache/arrow-rs/pull/10304)
- Docs: fix mutableArrayData comments by @Rich-T-kid in [#10326](https://github.com/apache/arrow-rs/pull/10326)
- docs: trim release schedule for released versions by @alamb in [#10280](https://github.com/apache/arrow-rs/pull/10280)
- Align parquet-geospatial crate docs with README by @paleolimbot in [#10302](https://github.com/apache/arrow-rs/pull/10302)

### Miscellaneous
- Revert "chore: formalize the default map field names to match default arrow spec (#10297)" by @alamb in [#10506](https://github.com/apache/arrow-rs/pull/10506)
- parquet: deprecate explicit `object_store` integration by @brancz in [#10354](https://github.com/apache/arrow-rs/pull/10354)
- fix(parquet): restore opaque return type for `RowSelection::iter` by @haohuaijin in [#10450](https://github.com/apache/arrow-rs/pull/10450)
- refactor(parquet): split `arrow_reader/selection` into smaller modules by @haohuaijin in [#10434](https://github.com/apache/arrow-rs/pull/10434)
- chore: deduplicate filter nulls code in coalesce/filter kernel by @Jefffrey in [#10348](https://github.com/apache/arrow-rs/pull/10348)
- chore: remove parquet dependency from parquet_derive by @ByteBaker in [#10327](https://github.com/apache/arrow-rs/pull/10327)

