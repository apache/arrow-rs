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

## [58.2.0](https://github.com/apache/arrow-rs/tree/58.2.0) (2026-04-28)

[Full Changelog](https://github.com/apache/arrow-rs/compare/58.1.0...58.2.0)

**Implemented enhancements:**

- Expose ColumnCloseResult on ArrowColumnChunk [\#9774](https://github.com/apache/arrow-rs/issues/9774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Expose FFI data structures fields [\#9771](https://github.com/apache/arrow-rs/issues/9771) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- short-circuit last predicate in `RowFilter` when `with_limit(N)` is set [\#9765](https://github.com/apache/arrow-rs/issues/9765) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- vectorise dict-index bounds check [\#9747](https://github.com/apache/arrow-rs/issues/9747) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Refactor `RleEncoder::flush_bit_packed_run` [\#9734](https://github.com/apache/arrow-rs/issues/9734) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add benchmark for cast from/to decimals [\#9728](https://github.com/apache/arrow-rs/issues/9728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a security policy for arrow-rs [\#9727](https://github.com/apache/arrow-rs/issues/9727) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support `FixedSizeList` in arrow-json reader [\#9714](https://github.com/apache/arrow-rs/issues/9714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Add `VariantArrayBuilder::append_nulls` API [\#9684](https://github.com/apache/arrow-rs/issues/9684)
- \[Json\] RunEndEncoded decoder optimization [\#9645](https://github.com/apache/arrow-rs/issues/9645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] `variant_get(..., List<_>)` non-Struct types support [\#9615](https://github.com/apache/arrow-rs/issues/9615)
- \[Variant\] Add unshredded `Struct` fast-path for `variant_get(..., Struct)` [\#9596](https://github.com/apache/arrow-rs/issues/9596)
- Allow setting custom line terminator for CSV writer [\#9571](https://github.com/apache/arrow-rs/issues/9571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Variant\] Align cast logic for `variant_get` to cast kernel for numeric/bool types [\#9564](https://github.com/apache/arrow-rs/issues/9564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ci: use ubuntu-slim where applicable [\#9536](https://github.com/apache/arrow-rs/issues/9536)
- Publicly export `arrow_string::Predicate` and its methods? [\#9480](https://github.com/apache/arrow-rs/issues/9480)
- Don't create CompressionContext when no compression is selected \[IPC\] [\#9463](https://github.com/apache/arrow-rs/issues/9463) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: Raw level buffering causes unbounded memory growth for sparse columns [\#9446](https://github.com/apache/arrow-rs/issues/9446) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parallel Parquet Reading [\#9381](https://github.com/apache/arrow-rs/issues/9381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- \[Variant\] `unshred_variant` panics on malformed bytes despite returning `Result` [\#9740](https://github.com/apache/arrow-rs/issues/9740)
- RecordBatch::normalize\(\) does not propagate top level null bitmap into the results [\#9732](https://github.com/apache/arrow-rs/issues/9732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect accounting in `DictEncoder::estimated_memory_size` [\#9719](https://github.com/apache/arrow-rs/issues/9719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- arrow-ipc writer does not comply with spec for empty variable-size arrays [\#9716](https://github.com/apache/arrow-rs/issues/9716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when reading corrupt parquet file with truncated data instead of ParquetError [\#9705](https://github.com/apache/arrow-rs/issues/9705) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- NOTICE.txt is inaccurate [\#9703](https://github.com/apache/arrow-rs/issues/9703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unnecessary dependency on regex crate [\#9672](https://github.com/apache/arrow-rs/issues/9672)
- \[arrow-avro\] Avro reader produces incorrect results when reader schema and writer schema differ [\#9655](https://github.com/apache/arrow-rs/issues/9655) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet docs are broken on docs.rs [\#9649](https://github.com/apache/arrow-rs/issues/9649)
- \[Parquet\] ArrowWriter with CDC panics on nested ListArrays [\#9637](https://github.com/apache/arrow-rs/issues/9637) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Use release KEYS file for verification instead of dev KEYS [\#9603](https://github.com/apache/arrow-rs/issues/9603)
- IPC reader: handling of dictionaries with only null values [\#9595](https://github.com/apache/arrow-rs/issues/9595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet RleDecoder::get\_batch\_with\_dict panics on oob dictionary indices [\#9434](https://github.com/apache/arrow-rs/issues/9434) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- docs\(variant\): link VariantArray doc to official Parquet Variant extension type [\#9779](https://github.com/apache/arrow-rs/pull/9779) ([mcharrel](https://github.com/mcharrel))
- Document Security Policy [\#9730](https://github.com/apache/arrow-rs/pull/9730) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Docs: add example of how to read parquet row groups in parallel [\#9396](https://github.com/apache/arrow-rs/pull/9396) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- parquet: avoid decode and heap allocation on terminal skip in DeltaBitPackDecoder [\#9784](https://github.com/apache/arrow-rs/issues/9784) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet: O\(1\) skip for bw=0 miniblocks in DeltaBitPackDecoder [\#9783](https://github.com/apache/arrow-rs/issues/9783) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove per-message flush overhead in Arrow IPC writer [\#9762](https://github.com/apache/arrow-rs/issues/9762) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `GenericListViewArray::new_unchecked` and refactor ListView json decoder [\#9646](https://github.com/apache/arrow-rs/issues/9646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support nested REE in arrow-ord `partition` function [\#9640](https://github.com/apache/arrow-rs/issues/9640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Parquet\] Remove the BIT\_PACKED encoder [\#9635](https://github.com/apache/arrow-rs/issues/9635) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Pre-reserve output capacity in ByteView/ByteArray dictionary decoding [\#9587](https://github.com/apache/arrow-rs/issues/9587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Fuse RLE decoding and view gathering for StringView dictionary decoding [\#9582](https://github.com/apache/arrow-rs/issues/9582) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use branchless index clamping and add get\_batch\_direct to RleDecoder [\#9581](https://github.com/apache/arrow-rs/issues/9581) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Reduce per-byte overhead in VLQ integer decoding [\#9580](https://github.com/apache/arrow-rs/issues/9580) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- feat\(parquet\): batch RLE runs in level encoder via scan-ahead [\#9830](https://github.com/apache/arrow-rs/pull/9830) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- fix: lazy-init zstd compression contexts to avoid unnecessary FFI calls [\#9808](https://github.com/apache/arrow-rs/pull/9808) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbutrovich](https://github.com/mbutrovich))
- parquet: O\(1\) skip for bw=0 miniblocks in DeltaBitPackDecoder [\#9786](https://github.com/apache/arrow-rs/pull/9786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sahuagin](https://github.com/sahuagin))
- chore: add benchmark for row filters with LIMIT short-circuit [\#9767](https://github.com/apache/arrow-rs/pull/9767) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([haohuaijin](https://github.com/haohuaijin))
- Push `LIMIT` / `OFFSET` into the last `RowFilter` predicate and skip unused row groups [\#9766](https://github.com/apache/arrow-rs/pull/9766) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([haohuaijin](https://github.com/haohuaijin))
- feat\(ipc\): Remove per-message flush in IPC writer hot path [\#9763](https://github.com/apache/arrow-rs/pull/9763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))
- perf\(parquet\): Defer fixed length byte array buffer alloc and skip zero-batch init [\#9756](https://github.com/apache/arrow-rs/pull/9756) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([lyang24](https://github.com/lyang24))
- feat\(parquet\): batch consecutive null/empty rows in `write_list` [\#9752](https://github.com/apache/arrow-rs/pull/9752) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- Remove `len` field from buffer builder [\#9750](https://github.com/apache/arrow-rs/pull/9750) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([cetra3](https://github.com/cetra3))
- perf\(parquet\): Vectorize dict-index bounds check in RleDecoder::get\_batch\_with\_dict \(up to -7.9%\) [\#9746](https://github.com/apache/arrow-rs/pull/9746) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- feat\(parquet\): precompute `offset_index_disabled` at build-time [\#9724](https://github.com/apache/arrow-rs/pull/9724) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- \[Parquet\] Improve dictionary decoder by unrolling loops [\#9662](https://github.com/apache/arrow-rs/pull/9662) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- \[Json\] Use `partition` and `take` in RunEndEncoded decoder [\#9658](https://github.com/apache/arrow-rs/pull/9658) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Improve take performance on List arrays [\#9643](https://github.com/apache/arrow-rs/pull/9643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- \[Json\] Replace `ArrayData` with typed Array construction in json-reader [\#9497](https://github.com/apache/arrow-rs/pull/9497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- feat\(parquet\): stream-encode definition/repetition levels incrementally [\#9447](https://github.com/apache/arrow-rs/pull/9447) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))

**Closed issues:**

- Incorrect buffer skipping for V4 Union types in IPC `skip_field` [\#9828](https://github.com/apache/arrow-rs/issues/9828) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace wildcard match in `skip_field` with explicit `DataType` handling [\#9821](https://github.com/apache/arrow-rs/issues/9821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Column projection misalignment for ListView / LargeListView in IPC reader [\#9805](https://github.com/apache/arrow-rs/issues/9805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Avoid panic on malformed compressed buffer prefix in IPC [\#9801](https://github.com/apache/arrow-rs/issues/9801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DeltaByteArrayDecoder panics on invalid prefix lengths [\#9796](https://github.com/apache/arrow-rs/issues/9796) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use NullBufferBuilder when reading json [\#9781](https://github.com/apache/arrow-rs/issues/9781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Perfectly shredded arrays with top-level null values loss nullability when `typed_value` is extracted [\#9701](https://github.com/apache/arrow-rs/issues/9701)
- \[Parquet Metadata\] API to determine page-index presence separately from page-index load [\#9693](https://github.com/apache/arrow-rs/issues/9693)
- Union cast is incorrect for duplicate field names [\#9664](https://github.com/apache/arrow-rs/issues/9664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- List and ListView are missing `take` benchmarks [\#9627](https://github.com/apache/arrow-rs/issues/9627) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support RunEndEncoded arrays in comparison kernels \(eq, lt, etc.\) [\#9620](https://github.com/apache/arrow-rs/issues/9620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- variant\_get should follow JSONpath semantics [\#9606](https://github.com/apache/arrow-rs/issues/9606)
- GenericByteViewArray: support finding total length of all strings [\#9435](https://github.com/apache/arrow-rs/issues/9435) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- support length\(\) on Run-end encoding arrays [\#9838](https://github.com/apache/arrow-rs/pull/9838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rich-T-kid](https://github.com/Rich-T-kid))
- fix\(ipc\): correct skip\_field handling for V4 Union [\#9829](https://github.com/apache/arrow-rs/pull/9829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))
- fix\(ipc\): replace wildcard in skip\_field with explicit DataType handling [\#9822](https://github.com/apache/arrow-rs/pull/9822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))
- Prevent buffer builder length overflow in `MutableBuffer::extend_zeros` [\#9820](https://github.com/apache/arrow-rs/pull/9820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Prevent repeat slice length overflow [\#9819](https://github.com/apache/arrow-rs/pull/9819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Prevent BitChunks length overflow [\#9818](https://github.com/apache/arrow-rs/pull/9818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Prevent Rows row index overflow [\#9817](https://github.com/apache/arrow-rs/pull/9817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Prevent ArrayData validation length overflow [\#9816](https://github.com/apache/arrow-rs/pull/9816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Json\] Remove arrow-data dependency from arrow-json [\#9812](https://github.com/apache/arrow-rs/pull/9812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- Replace `BooleanBufferBuilder` with `NullBufferBuilder` in arrow-json if applicable [\#9811](https://github.com/apache/arrow-rs/pull/9811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- refactor\(ipc\): derive Default for CompressionContext [\#9809](https://github.com/apache/arrow-rs/pull/9809) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbutrovich](https://github.com/mbutrovich))
- fix\(ipc\): reader misalignment when skipping ListView / LargeListView columns [\#9806](https://github.com/apache/arrow-rs/pull/9806) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))
- fix\(ipc\): Avoid panic on malformed compressed buffer prefix [\#9802](https://github.com/apache/arrow-rs/pull/9802) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pchintar](https://github.com/pchintar))
- parquet: fix panic in DeltaByteArrayDecoder on invalid prefix lengths [\#9797](https://github.com/apache/arrow-rs/pull/9797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([pchintar](https://github.com/pchintar))
- feat\(parquet\): fuse level encoding with counting and histogram updates [\#9795](https://github.com/apache/arrow-rs/pull/9795) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- Expose ColumnCloseResult on ArrowColumnChunk [\#9773](https://github.com/apache/arrow-rs/pull/9773) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([leoyvens](https://github.com/leoyvens))
- feat: make FFI structs fields `pub` [\#9772](https://github.com/apache/arrow-rs/pull/9772) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ashdnazg](https://github.com/ashdnazg))
- chore: Refine the error message for List to non List cast [\#9757](https://github.com/apache/arrow-rs/pull/9757) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- refactor\(parquet\): replace magic `8` literals with named constants [\#9751](https://github.com/apache/arrow-rs/pull/9751) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- feat\(ipc\): add with\_skip\_validation to StreamDecoder  [\#9749](https://github.com/apache/arrow-rs/pull/9749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pantShrey](https://github.com/pantShrey))
- remove panics in unshred variant [\#9741](https://github.com/apache/arrow-rs/pull/9741) ([friendlymatthew](https://github.com/friendlymatthew))
- Add benchmark for ListView interleave [\#9738](https://github.com/apache/arrow-rs/pull/9738) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vegarsti](https://github.com/vegarsti))
- arrow-arith: fix 'occured' -\> 'occurred' in arity.rs comments [\#9736](https://github.com/apache/arrow-rs/pull/9736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SAY-5](https://github.com/SAY-5))
- Refactor `RleEncoder::flush_bit_packed_run` to make flow clearer [\#9735](https://github.com/apache/arrow-rs/pull/9735) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix RecordBatch::normalize\(\) null bitmap bug and add StructArray::flatten\(\) [\#9733](https://github.com/apache/arrow-rs/pull/9733) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sqd](https://github.com/sqd))
- Add benchmark for cast from/to decimals [\#9729](https://github.com/apache/arrow-rs/pull/9729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([klion26](https://github.com/klion26))
- refactor\(arrow-avro\): use `Decoder::flush_block` in async reader [\#9726](https://github.com/apache/arrow-rs/pull/9726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzabaluev](https://github.com/mzabaluev))
- fix: ParquetError when reading corrupt parquet file with truncated data instead of Panic [\#9725](https://github.com/apache/arrow-rs/pull/9725) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([xuzifu666](https://github.com/xuzifu666))
- feat\(parquet\): add wide-schema writer overhead benchmark [\#9723](https://github.com/apache/arrow-rs/pull/9723) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- fix: correct accounting in `DictEncoder::estimated_memory_size`, `Interner::estimated_memory_size` [\#9720](https://github.com/apache/arrow-rs/pull/9720) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mzabaluev](https://github.com/mzabaluev))
- arrow-ipc: Write 0 offset buffer for length-0 variable-size arrays [\#9717](https://github.com/apache/arrow-rs/pull/9717) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([atwam](https://github.com/atwam))
- \[Json\] Support `FixedSizeList` in json decoder [\#9715](https://github.com/apache/arrow-rs/pull/9715) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- chore\(deps\): bump actions/upload-pages-artifact from 4 to 5 [\#9713](https://github.com/apache/arrow-rs/pull/9713) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix clippy warning in fixed\_size\_binary\_array.rs [\#9712](https://github.com/apache/arrow-rs/pull/9712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- feat: add `has_non_empty_nulls` helper function in `OffsetBuffer` [\#9711](https://github.com/apache/arrow-rs/pull/9711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- chore\(deps\): bump pytest from 7.2.0 to 9.0.3 in /parquet/pytest [\#9706](https://github.com/apache/arrow-rs/pull/9706) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fedora license audit [\#9704](https://github.com/apache/arrow-rs/pull/9704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([michel-slm](https://github.com/michel-slm))
- \[Variant\] Take top-level nulls into consideration when extracting perfectly shredded children [\#9702](https://github.com/apache/arrow-rs/pull/9702) ([AdamGS](https://github.com/AdamGS))
- feat\(parquet\): add `push_decoder` benchmark for `PushBuffers` overhead [\#9696](https://github.com/apache/arrow-rs/pull/9696) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- Add mutable bitwise operations to `BooleanArray` and `NullBuffer::union_many` [\#9692](https://github.com/apache/arrow-rs/pull/9692) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbutrovich](https://github.com/mbutrovich))
- chore\(deps\): update hashbrown requirement from 0.16.0 to 0.17.0 [\#9691](https://github.com/apache/arrow-rs/pull/9691) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- chore\(deps\): bump actions/github-script from 8 to 9 [\#9690](https://github.com/apache/arrow-rs/pull/9690) ([dependabot[bot]](https://github.com/apps/dependabot))
- minor: Re-enable CDC bench [\#9686](https://github.com/apache/arrow-rs/pull/9686) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- \[Variant\] Add `VariantArrayBuilder::append_nulls` API  [\#9685](https://github.com/apache/arrow-rs/pull/9685) ([sdf-jkl](https://github.com/sdf-jkl))
- feat\(parquet\): add struct-column writer benchmarks [\#9679](https://github.com/apache/arrow-rs/pull/9679) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- \[Arrow\] Add API to check if `Field` has a valid `ExtensionType` [\#9677](https://github.com/apache/arrow-rs/pull/9677) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sdf-jkl](https://github.com/sdf-jkl))
- \[Variant\] `variant_get` should follow JSONPath semantics for Field path element [\#9676](https://github.com/apache/arrow-rs/pull/9676) ([sdf-jkl](https://github.com/sdf-jkl))
- ParquetMetaDataPushDecoder API to clear all buffered ranges [\#9673](https://github.com/apache/arrow-rs/pull/9673) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nathanb9](https://github.com/nathanb9))
- Fix union cast incorrectness for duplicate field names [\#9666](https://github.com/apache/arrow-rs/pull/9666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- chore: re-export `MAX_INLINE_VIEW_LEN` from `arrow_data` [\#9665](https://github.com/apache/arrow-rs/pull/9665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- No longer allow BIT\_PACKED level encoding in Parquet writer [\#9656](https://github.com/apache/arrow-rs/pull/9656) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- feat\(parquet\): add sparse-column writer benchmarks [\#9654](https://github.com/apache/arrow-rs/pull/9654) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HippoBaro](https://github.com/HippoBaro))
- Support `GenericListViewArray::new_unchecked` and refactor `ListView` json decoder [\#9648](https://github.com/apache/arrow-rs/pull/9648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- \[Json\] Add json reader benchmarks for ListView [\#9647](https://github.com/apache/arrow-rs/pull/9647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- fix\(parquet\): fix CDC panic on nested ListArrays with null entries  [\#9644](https://github.com/apache/arrow-rs/pull/9644) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kszucs](https://github.com/kszucs))
- Add a test for reading nested REE data in json [\#9634](https://github.com/apache/arrow-rs/pull/9634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Variant\] Fix `variant_get` to return `List<T>` instead of `List<Struct>` [\#9631](https://github.com/apache/arrow-rs/pull/9631) ([liamzwbao](https://github.com/liamzwbao))
- ci: use ubuntu-slim runner for lightweight CI jobs [\#9630](https://github.com/apache/arrow-rs/pull/9630) ([CuteChuanChuan](https://github.com/CuteChuanChuan))
- Add bloom filter folding to automatically size SBBF filters [\#9628](https://github.com/apache/arrow-rs/pull/9628) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Add List and ListView take benchmarks [\#9626](https://github.com/apache/arrow-rs/pull/9626) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- ParquetPushDecoder API to clear all buffered ranges [\#9624](https://github.com/apache/arrow-rs/pull/9624) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nathanb9](https://github.com/nathanb9))
- fix: handle missing dictionary batch for null-only columns in IPC reader [\#9623](https://github.com/apache/arrow-rs/pull/9623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([joaquinhuigomez](https://github.com/joaquinhuigomez))
- Fix `MutableBuffer::clear` [\#9622](https://github.com/apache/arrow-rs/pull/9622) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rafferty97](https://github.com/Rafferty97))
- feat\[arrow-ord\]: suppport REE comparisons [\#9621](https://github.com/apache/arrow-rs/pull/9621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- chore\(deps\): update sha2 requirement from 0.10 to 0.11 [\#9618](https://github.com/apache/arrow-rs/pull/9618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Expose option to set line terminator for CSV writer [\#9617](https://github.com/apache/arrow-rs/pull/9617) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([svranesevic](https://github.com/svranesevic))
- \[Json\] Add json reader benchmarks for Map and REE [\#9616](https://github.com/apache/arrow-rs/pull/9616) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))
- deps: fix `object_store` breakage for 0.13.2 [\#9612](https://github.com/apache/arrow-rs/pull/9612) ([mzabaluev-flarion](https://github.com/mzabaluev-flarion))
- \[Variant\] Support Binary/LargeBinary children  [\#9610](https://github.com/apache/arrow-rs/pull/9610) ([AdamGS](https://github.com/AdamGS))
- fix: use writer types in Skipper for resolved named record types [\#9605](https://github.com/apache/arrow-rs/pull/9605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ariel-miculas](https://github.com/ariel-miculas))
- feat\(parquet\): derive `PartialEq` and `Eq` for `CdcOptions` [\#9602](https://github.com/apache/arrow-rs/pull/9602) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kszucs](https://github.com/kszucs))
- Add  `finish_preserve_values` to `ArrayBuilder` trait [\#9601](https://github.com/apache/arrow-rs/pull/9601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adamreichold](https://github.com/adamreichold))
- \[Variant\] extend shredded null handling for arrays [\#9599](https://github.com/apache/arrow-rs/pull/9599) ([sdf-jkl](https://github.com/sdf-jkl))
- \[Variant\]  Add unshredded `Struct` fast-path for `variant_get(..., Struct)` [\#9597](https://github.com/apache/arrow-rs/pull/9597) ([sdf-jkl](https://github.com/sdf-jkl))
- Pre-reserve output capacity in ByteView/ByteArray dictionary decoding [\#9590](https://github.com/apache/arrow-rs/pull/9590) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- \[Variant\] Align cast logic for variant\_get to cast kernel for numeric/bool types [\#9563](https://github.com/apache/arrow-rs/pull/9563) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([klion26](https://github.com/klion26))
- Add support to cast from `UnionArray` [\#9544](https://github.com/apache/arrow-rs/pull/9544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- Support `ListView` codec in arrow-json [\#9503](https://github.com/apache/arrow-rs/pull/9503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liamzwbao](https://github.com/liamzwbao))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
