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

## [53.1.0](https://github.com/apache/arrow-rs/tree/53.1.0) (2024-10-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.0.0...53.1.0)

**Implemented enhancements:**

- Write null counts in Parquet statistics when they are known to be zero [\#6502](https://github.com/apache/arrow-rs/issues/6502) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make it easier to find / work with `ByteView` [\#6478](https://github.com/apache/arrow-rs/issues/6478) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update lexical-core version due to soundness issues with current version [\#6468](https://github.com/apache/arrow-rs/issues/6468)
- Add builder style API for manipulating `ParquetMetaData` [\#6465](https://github.com/apache/arrow-rs/issues/6465) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ArrayData.align_buffers` should support `Struct` data type / child data [\#6461](https://github.com/apache/arrow-rs/issues/6461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a method to return the number of skipped rows in a `RowSelection` [\#6428](https://github.com/apache/arrow-rs/issues/6428) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Bump lexical-core to 1.0 [\#6397](https://github.com/apache/arrow-rs/issues/6397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add union\_extract kernel [\#6386](https://github.com/apache/arrow-rs/issues/6386) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- implement `regexp_is_match_utf8` and `regexp_is_match_utf8_scalar` for `StringViewArray` [\#6370](https://github.com/apache/arrow-rs/issues/6370) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for BinaryView in arrow\_string::length [\#6358](https://github.com/apache/arrow-rs/issues/6358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `as_union` to `AsArray` [\#6351](https://github.com/apache/arrow-rs/issues/6351)
- Ability to append non contiguous strings to `StringBuilder` [\#6347](https://github.com/apache/arrow-rs/issues/6347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Catalog DB Schema subcommands to `flight_sql_client` [\#6331](https://github.com/apache/arrow-rs/issues/6331) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add support for Utf8View in arrow\_string::length [\#6305](https://github.com/apache/arrow-rs/issues/6305) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reading FIXED\_LEN\_BYTE\_ARRAY columns with nulls is inefficient [\#6296](https://github.com/apache/arrow-rs/issues/6296) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optionally verify 32-bit CRC checksum when decoding parquet pages [\#6289](https://github.com/apache/arrow-rs/issues/6289) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speed up `pad_nulls` for `FixedLenByteArrayBuffer` [\#6297](https://github.com/apache/arrow-rs/pull/6297) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve performance of set\_bits by avoiding to set individual bits [\#6288](https://github.com/apache/arrow-rs/pull/6288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuyukitanimura](https://github.com/kazuyukitanimura))

**Fixed bugs:**

- BitIterator panics when retrieving length [\#6480](https://github.com/apache/arrow-rs/issues/6480) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Flight data retrieved via Python client \(wrapping C++\) cannot be used by Rust Arrow [\#6471](https://github.com/apache/arrow-rs/issues/6471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CI integration test failing: Archery test With other arrows [\#6448](https://github.com/apache/arrow-rs/issues/6448) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- IPC not respecting not preserving dict ID [\#6443](https://github.com/apache/arrow-rs/issues/6443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Failing CI: Prost requires Rust 1.71.1 [\#6436](https://github.com/apache/arrow-rs/issues/6436) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Invalid struct arrays in IPC data causes panic during read [\#6416](https://github.com/apache/arrow-rs/issues/6416) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- REE Dicts cannot be encoded/decoded with streaming IPC [\#6398](https://github.com/apache/arrow-rs/issues/6398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reading json `map` with non-nullable value schema doesn't error if values are actually null [\#6391](https://github.com/apache/arrow-rs/issues/6391)
- StringViewBuilder with deduplication does not clear observed values [\#6384](https://github.com/apache/arrow-rs/issues/6384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast from Decimal\(p, s\) to dictionary-encoded Decimal\(p, s\) loses precision and scale [\#6381](https://github.com/apache/arrow-rs/issues/6381) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- LocalFileSystem `list` operation returns objects in wrong order [\#6375](https://github.com/apache/arrow-rs/issues/6375)
- `compute::binary_mut` returns `Err(PrimitiveArray<T>)` only with certain arrays [\#6374](https://github.com/apache/arrow-rs/issues/6374) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Exporting Binary/Utf8View from arrow-rs to pyarrow fails [\#6366](https://github.com/apache/arrow-rs/issues/6366) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- warning: methods `as_any` and `next_batch` are never used in `parquet` crate [\#6143](https://github.com/apache/arrow-rs/issues/6143) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- chore: add docs, part of \#37 [\#6496](https://github.com/apache/arrow-rs/pull/6496) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([ByteBaker](https://github.com/ByteBaker))
- Minor: improve `ChunkedReader` docs [\#6477](https://github.com/apache/arrow-rs/pull/6477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Add some missing documentation to fix CI errors [\#6445](https://github.com/apache/arrow-rs/pull/6445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Fix doc "bit width" to "byte width" [\#6434](https://github.com/apache/arrow-rs/pull/6434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- chore: add docs, part of \#37 [\#6433](https://github.com/apache/arrow-rs/pull/6433) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ByteBaker](https://github.com/ByteBaker))
- chore: add docs, part of \#37 [\#6424](https://github.com/apache/arrow-rs/pull/6424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ByteBaker](https://github.com/ByteBaker))
- Rephrase doc comment [\#6421](https://github.com/apache/arrow-rs/pull/6421) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([waynexia](https://github.com/waynexia))
- Remove "NOT YET FULLY SUPPORTED" comment from DataType::Utf8View/BinaryView [\#6380](https://github.com/apache/arrow-rs/pull/6380) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `GenericStringBuilder` documentation [\#6372](https://github.com/apache/arrow-rs/pull/6372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Columnar json writer for arrow-json [\#6411](https://github.com/apache/arrow-rs/issues/6411)
- Primitive `binary`/`unary` are not as fast as they could be [\#6364](https://github.com/apache/arrow-rs/issues/6364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Different numeric type may be able to compare [\#6357](https://github.com/apache/arrow-rs/issues/6357)

**Merged pull requests:**

- fix: override `size_hint` for `BitIterator` to return the exact remaining size [\#6495](https://github.com/apache/arrow-rs/pull/6495) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Beihao-Zhou](https://github.com/Beihao-Zhou))
- Minor: Fix path in format command in CONTRIBUTING.md [\#6494](https://github.com/apache/arrow-rs/pull/6494) ([etseidl](https://github.com/etseidl))
- Write null counts in Parquet statistics when they are known [\#6490](https://github.com/apache/arrow-rs/pull/6490) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add configuration option to `StatisticsConverter` to control interpretation of missing null counts in Parquet statistics  [\#6485](https://github.com/apache/arrow-rs/pull/6485) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- fix: check overflow numbers while inferring type for csv files [\#6481](https://github.com/apache/arrow-rs/pull/6481) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([CookiePieWw](https://github.com/CookiePieWw))
- Add better documentation, examples and builer-style API to `ByteView` [\#6479](https://github.com/apache/arrow-rs/pull/6479) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add take\_arrays util for getting entries from 2d arrays [\#6475](https://github.com/apache/arrow-rs/pull/6475) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([akurmustafa](https://github.com/akurmustafa))
- Deprecate `MetadataLoader` [\#6474](https://github.com/apache/arrow-rs/pull/6474) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update tonic-build requirement from =0.12.2 to =0.12.3 [\#6473](https://github.com/apache/arrow-rs/pull/6473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Align buffers from Python \(FFI\) [\#6472](https://github.com/apache/arrow-rs/pull/6472) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([EnricoMi](https://github.com/EnricoMi))
- Add `ParquetMetaDataBuilder` [\#6466](https://github.com/apache/arrow-rs/pull/6466) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Make `ArrayData.align_buffers` align child data buffers recursively [\#6462](https://github.com/apache/arrow-rs/pull/6462) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([EnricoMi](https://github.com/EnricoMi))
- Minor: Silence compiler warnings for `parquet::file::metadata::reader` [\#6457](https://github.com/apache/arrow-rs/pull/6457) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Minor: Error rather than panic for unsupported for dictionary `cast`ing [\#6456](https://github.com/apache/arrow-rs/pull/6456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([goldmedal](https://github.com/goldmedal))
- Support cast between Durations + between Durations all numeric types [\#6452](https://github.com/apache/arrow-rs/pull/6452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tisonkun](https://github.com/tisonkun))
- Deprecate methods from footer.rs in favor of `ParquetMetaDataReader` [\#6451](https://github.com/apache/arrow-rs/pull/6451) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Workaround for missing Parquet page indexes in `ParquetMetadaReader` [\#6450](https://github.com/apache/arrow-rs/pull/6450) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix CI by disabling newly failing rust \<\> nanoarrow integration test in CI [\#6449](https://github.com/apache/arrow-rs/pull/6449) ([alamb](https://github.com/alamb))
- Add `IpcSchemaEncoder`, deprecate ipc schema functions, Fix IPC not respecting not preserving dict ID [\#6444](https://github.com/apache/arrow-rs/pull/6444) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([brancz](https://github.com/brancz))
- Add additional documentation and builder APIs to `SortOptions` [\#6441](https://github.com/apache/arrow-rs/pull/6441) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update prost-build requirement from =0.13.2 to =0.13.3 [\#6440](https://github.com/apache/arrow-rs/pull/6440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump arrow-flight MSRV to 1.71.1 [\#6437](https://github.com/apache/arrow-rs/pull/6437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([gstvg](https://github.com/gstvg))
- Silence warnings that `as_any` and `next_batch` are never used [\#6432](https://github.com/apache/arrow-rs/pull/6432) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add `ParquetMetaDataReader` [\#6431](https://github.com/apache/arrow-rs/pull/6431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add RowSelection::skipped\_row\_count [\#6429](https://github.com/apache/arrow-rs/pull/6429) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- perf: Faster decimal precision overflow checks [\#6419](https://github.com/apache/arrow-rs/pull/6419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- fix: don't panic in IPC reader if struct child arrays have different lengths [\#6417](https://github.com/apache/arrow-rs/pull/6417) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- Reduce integration test matrix [\#6407](https://github.com/apache/arrow-rs/pull/6407) ([kou](https://github.com/kou))
- Move lifetime of `take_iter` from iterator to its items [\#6403](https://github.com/apache/arrow-rs/pull/6403) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dariocurr](https://github.com/dariocurr))
- Update lexical-core requirement from 0.8 to 1.0 \(to resolve RUSTSEC-2023-0086\) [\#6402](https://github.com/apache/arrow-rs/pull/6402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dariocurr](https://github.com/dariocurr))
- Fix encoding/decoding REE Dicts when using streaming IPC [\#6399](https://github.com/apache/arrow-rs/pull/6399) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- fix: binary\_mut should work if only one input array has null buffer [\#6396](https://github.com/apache/arrow-rs/pull/6396) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `set_bits` fuzz test [\#6394](https://github.com/apache/arrow-rs/pull/6394) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- impl `From<ScalarBuffer<T>>` for `Buffer` [\#6389](https://github.com/apache/arrow-rs/pull/6389) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Add `union_extract` kernel [\#6387](https://github.com/apache/arrow-rs/pull/6387) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Clear string-tracking hash table when ByteView deduplication is enabled [\#6385](https://github.com/apache/arrow-rs/pull/6385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([shanesveller](https://github.com/shanesveller))
- fix: Stop losing precision and scale when casting decimal to dictionary [\#6383](https://github.com/apache/arrow-rs/pull/6383) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Add `ARROW_VERSION` const [\#6379](https://github.com/apache/arrow-rs/pull/6379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- parquet writer: Raise an error when the row\_group\_index overflows i16 [\#6378](https://github.com/apache/arrow-rs/pull/6378) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- Implement native support StringViewArray for `regexp_is_match` and `regexp_is_match_scalar` function, deprecate `regexp_is_match_utf8` and `regexp_is_match_utf8_scalar` [\#6376](https://github.com/apache/arrow-rs/pull/6376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- Update chrono-tz requirement from 0.9 to 0.10 [\#6371](https://github.com/apache/arrow-rs/pull/6371) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support StringViewArray interop with python: fix lingering C Data Interface issues for \*ViewArray [\#6368](https://github.com/apache/arrow-rs/pull/6368) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- stop panic in `MetadataLoader` on invalid data [\#6367](https://github.com/apache/arrow-rs/pull/6367) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([samuelcolvin](https://github.com/samuelcolvin))
-  Add support for BinaryView in arrow\_string::length  [\#6359](https://github.com/apache/arrow-rs/pull/6359) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Omega359](https://github.com/Omega359))
- impl `From<Vec<T>>` for `Buffer` [\#6355](https://github.com/apache/arrow-rs/pull/6355) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Add breaking change from \#6043 to `CHANGELOG` [\#6354](https://github.com/apache/arrow-rs/pull/6354) ([mbrobbel](https://github.com/mbrobbel))
- Benchmark for bit\_mask \(set\_bits\) [\#6353](https://github.com/apache/arrow-rs/pull/6353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuyukitanimura](https://github.com/kazuyukitanimura))
- Update prost-build requirement from =0.13.1 to =0.13.2 [\#6350](https://github.com/apache/arrow-rs/pull/6350) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: clippy warnings from nightly rust 1.82 [\#6348](https://github.com/apache/arrow-rs/pull/6348) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Add support for Utf8View in arrow\_string::length [\#6345](https://github.com/apache/arrow-rs/pull/6345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Omega359](https://github.com/Omega359))
- feat: add catalog/schema subcommands to flight\_sql\_client. [\#6332](https://github.com/apache/arrow-rs/pull/6332) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([nathanielc](https://github.com/nathanielc))
- Manually run fmt on all files under parquet [\#6328](https://github.com/apache/arrow-rs/pull/6328) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Implement UnionArray logical\_nulls [\#6303](https://github.com/apache/arrow-rs/pull/6303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Parquet: Verify 32-bit CRC checksum when decoding pages [\#6290](https://github.com/apache/arrow-rs/pull/6290) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([xmakro](https://github.com/xmakro))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
