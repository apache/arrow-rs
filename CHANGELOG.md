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

## [51.0.0](https://github.com/apache/arrow-rs/tree/51.0.0) (2024-03-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/50.0.0...51.0.0)

**Breaking changes:**

- Remove internal buffering from AsyncArrowWriter \(\#5484\) [\#5485](https://github.com/apache/arrow-rs/pull/5485) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make ArrayBuilder also Sync [\#5353](https://github.com/apache/arrow-rs/pull/5353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dvic](https://github.com/dvic))
- Raw JSON writer \(~10x faster\) \(\#5314\)  [\#5318](https://github.com/apache/arrow-rs/pull/5318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Prototype Arrow over HTTP in Rust [\#5496](https://github.com/apache/arrow-rs/issues/5496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add DataType::ListView and DataType::LargeListView [\#5492](https://github.com/apache/arrow-rs/issues/5492) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve documentation around handling of dictionary arrays in arrow flight [\#5487](https://github.com/apache/arrow-rs/issues/5487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Better memory limiting in parquet `ArrowWriter`  [\#5484](https://github.com/apache/arrow-rs/issues/5484) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Creating Non-Nullable Lists and Maps within a Struct [\#5482](https://github.com/apache/arrow-rs/issues/5482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DISCUSSION\] Better borrow propagation \(e.g. `RecordBatch::schema()` to return `&SchemaRef` vs `SchemaRef`\) [\#5463](https://github.com/apache/arrow-rs/issues/5463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Build Scalar with ArrayRef [\#5459](https://github.com/apache/arrow-rs/issues/5459)
- AsyncArrowWriter doesn't limit underlying ArrowWriter to respect buffer-size [\#5450](https://github.com/apache/arrow-rs/issues/5450) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Refine `Display` implementation for `FlightError` [\#5438](https://github.com/apache/arrow-rs/issues/5438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Better ergonomics for `FixedSizeList` and `LargeList` [\#5372](https://github.com/apache/arrow-rs/issues/5372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update Flight proto [\#5367](https://github.com/apache/arrow-rs/issues/5367) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support check similar datatype but with different magnitudes [\#5358](https://github.com/apache/arrow-rs/issues/5358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Buffer memory usage for custom allocations is reported as 0 [\#5346](https://github.com/apache/arrow-rs/issues/5346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Can the ArrayBuilder trait be made Sync? [\#5344](https://github.com/apache/arrow-rs/issues/5344) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support cast 'UTF8' to `FixedSizeList` [\#5339](https://github.com/apache/arrow-rs/issues/5339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Creating Non-Nullable Lists with ListBuilder [\#5330](https://github.com/apache/arrow-rs/issues/5330) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `ParquetRecordBatchStreamBuilder::new()` panics instead of erroring out when opening a corrupted file [\#5315](https://github.com/apache/arrow-rs/issues/5315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Raw JSON Writer [\#5314](https://github.com/apache/arrow-rs/issues/5314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for more fused boolean operations [\#5297](https://github.com/apache/arrow-rs/issues/5297) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: Allow disabling embed `ARROW_SCHEMA_META_KEY` added by the `ArrowWriter` [\#5296](https://github.com/apache/arrow-rs/issues/5296) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting strings like '2001-01-01 01:01:01' to Date32 [\#5280](https://github.com/apache/arrow-rs/issues/5280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Temporal Extract/Date Part Kernel [\#5266](https://github.com/apache/arrow-rs/issues/5266) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support for extracting hours/minutes/seconds/etc. from `Time32`/`Time64` type in temporal kernels [\#5261](https://github.com/apache/arrow-rs/issues/5261) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: add method to get both the inner writer and the file metadata when closing SerializedFileWriter [\#5253](https://github.com/apache/arrow-rs/issues/5253) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Release arrow-rs version 50.0.0 [\#5234](https://github.com/apache/arrow-rs/issues/5234)

**Fixed bugs:**

- Empty String Parses as Zero in Unreleased Arrow [\#5504](https://github.com/apache/arrow-rs/issues/5504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unused import in nightly rust [\#5476](https://github.com/apache/arrow-rs/issues/5476) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Error `The data type type List .. has no natural order` when using `arrow::compute::lexsort_to_indices` with list and more than one column [\#5454](https://github.com/apache/arrow-rs/issues/5454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Wrong size assertion in arrow\_buffer::builder::NullBufferBuilder::new\_from\_buffer [\#5445](https://github.com/apache/arrow-rs/issues/5445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Inconsistency between comments and code implementation [\#5430](https://github.com/apache/arrow-rs/issues/5430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- OOB access in `Buffer::from_iter` [\#5412](https://github.com/apache/arrow-rs/issues/5412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast kernel doesn't return null for string to integral cases when overflowing under safe option enabled [\#5397](https://github.com/apache/arrow-rs/issues/5397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make ffi consume variable layout arrays with empty offsets [\#5391](https://github.com/apache/arrow-rs/issues/5391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordBatch conversion from pyarrow loses Schema's metadata [\#5354](https://github.com/apache/arrow-rs/issues/5354) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Debug output of Time32/Time64 arrays with invalid values has confusing nulls [\#5336](https://github.com/apache/arrow-rs/issues/5336) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Removing a column from a `RecordBatch` drops schema metadata [\#5327](https://github.com/apache/arrow-rs/issues/5327) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when read an empty parquet file [\#5304](https://github.com/apache/arrow-rs/issues/5304) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- How to enable statistics for string columns? [\#5270](https://github.com/apache/arrow-rs/issues/5270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `concat::tests::test_string_dictionary_merge failure` fails on Mac /  has different results in different platforms [\#5255](https://github.com/apache/arrow-rs/issues/5255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Minor: Add doc comments to `GenericByteViewArray` [\#5512](https://github.com/apache/arrow-rs/pull/5512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve docs for logical and physical nulls even more [\#5434](https://github.com/apache/arrow-rs/pull/5434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add example of converting RecordBatches to JSON objects [\#5364](https://github.com/apache/arrow-rs/pull/5364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- improve float to string cast by ~20%-40% [\#5401](https://github.com/apache/arrow-rs/pull/5401) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))

**Closed issues:**

- Add `StringViewArray` implementation and layout and basic construction + tests [\#5469](https://github.com/apache/arrow-rs/issues/5469) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `DataType::Utf8View` and `DataType::BinaryView` [\#5468](https://github.com/apache/arrow-rs/issues/5468) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Deprecate array\_to\_json\_array [\#5515](https://github.com/apache/arrow-rs/pull/5515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix integer parsing of empty strings \(\#5504\) [\#5505](https://github.com/apache/arrow-rs/pull/5505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: clarifying comments in struct\_builder.rs \#5494  [\#5499](https://github.com/apache/arrow-rs/pull/5499) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([istvan-fodor](https://github.com/istvan-fodor))
- Update proc-macro2 requirement from =1.0.78 to =1.0.79 [\#5498](https://github.com/apache/arrow-rs/pull/5498) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add DataType::ListView and DataType::LargeListView [\#5493](https://github.com/apache/arrow-rs/pull/5493) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Better document parquet pushdown [\#5491](https://github.com/apache/arrow-rs/pull/5491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix NullBufferBuilder::new\_from\_buffer wrong size assertion [\#5489](https://github.com/apache/arrow-rs/pull/5489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Support dictionary encoding in structures for `FlightDataEncoder`,  add documentation for `arrow_flight::encode::Dictionary` [\#5488](https://github.com/apache/arrow-rs/pull/5488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([thinkharderdev](https://github.com/thinkharderdev))
- Add MapBuilder::with\_values\_field to support non-nullable values \(\#5482\) [\#5483](https://github.com/apache/arrow-rs/pull/5483) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lasantosr](https://github.com/lasantosr))
- feat: initial support string\_view and binary\_view,  supports layout and basic construction + tests [\#5481](https://github.com/apache/arrow-rs/pull/5481) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ariesdevil](https://github.com/ariesdevil))
- Add more comprehensive documentation on testing and benchmarking to CONTRIBUTING.md [\#5478](https://github.com/apache/arrow-rs/pull/5478) ([monkwire](https://github.com/monkwire))
- Remove unused import detected by nightly rust [\#5477](https://github.com/apache/arrow-rs/pull/5477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add RecordBatch::schema\_ref [\#5474](https://github.com/apache/arrow-rs/pull/5474) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([monkwire](https://github.com/monkwire))
- Provide access to inner Write for parquet writers [\#5471](https://github.com/apache/arrow-rs/pull/5471) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add DataType::Utf8View and DataType::BinaryView [\#5470](https://github.com/apache/arrow-rs/pull/5470) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Update base64 requirement from 0.21 to 0.22 [\#5467](https://github.com/apache/arrow-rs/pull/5467) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Minor: Fix formatting typo in `Field::new_list_field` [\#5464](https://github.com/apache/arrow-rs/pull/5464) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix test\_string\_dictionary\_merge \(\#5255\) [\#5461](https://github.com/apache/arrow-rs/pull/5461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Vec::from\_iter in Buffer::from\_iter [\#5460](https://github.com/apache/arrow-rs/pull/5460) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Document parquet writer memory limiting \(\#5450\) [\#5457](https://github.com/apache/arrow-rs/pull/5457) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Document UnionArray Panics [\#5456](https://github.com/apache/arrow-rs/pull/5456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- fix: lexsort\_to\_indices unsupported mixed types with list [\#5455](https://github.com/apache/arrow-rs/pull/5455) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Refine `Display` and `Source` implementation for error types [\#5439](https://github.com/apache/arrow-rs/pull/5439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([BugenZhao](https://github.com/BugenZhao))
- Improve debug output of Time32/Time64 arrays [\#5428](https://github.com/apache/arrow-rs/pull/5428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Miri fix: Rename invalid\_mut to without\_provenance\_mut [\#5418](https://github.com/apache/arrow-rs/pull/5418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Ensure addition/multiplications in when allocating buffers don't overflow [\#5417](https://github.com/apache/arrow-rs/pull/5417) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Update Flight proto: PollFlightInfo & expiration time [\#5413](https://github.com/apache/arrow-rs/pull/5413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Jefffrey](https://github.com/Jefffrey))
- Add tests for serializing lists of dictionary encoded values to json [\#5399](https://github.com/apache/arrow-rs/pull/5399) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Return null for overflow when casting string to integer under safe option enabled [\#5398](https://github.com/apache/arrow-rs/pull/5398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Propagate error instead of panic for `take_bytes` [\#5395](https://github.com/apache/arrow-rs/pull/5395) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve like kernel by ~2% [\#5390](https://github.com/apache/arrow-rs/pull/5390) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Enable running arrow-array and arrow-arith with miri and avoid strict provenance warning [\#5387](https://github.com/apache/arrow-rs/pull/5387) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Update to chrono 0.4.34 [\#5385](https://github.com/apache/arrow-rs/pull/5385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return error instead of panic when reading invalid Parquet metadata [\#5382](https://github.com/apache/arrow-rs/pull/5382) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- Update tonic requirement from 0.10.0 to 0.11.0 [\#5380](https://github.com/apache/arrow-rs/pull/5380) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update tonic-build requirement from =0.10.2 to =0.11.0 [\#5379](https://github.com/apache/arrow-rs/pull/5379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix latest clippy lints [\#5376](https://github.com/apache/arrow-rs/pull/5376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: utility functions for creating `FixedSizeList` and `LargeList` dtypes [\#5373](https://github.com/apache/arrow-rs/pull/5373) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([universalmind303](https://github.com/universalmind303))
- Minor\(docs\): update master to main for DataFusion/Ballista [\#5363](https://github.com/apache/arrow-rs/pull/5363) ([caicancai](https://github.com/caicancai))
- Return an error instead of a panic when reading a corrupted Parquet file with mismatched column counts [\#5362](https://github.com/apache/arrow-rs/pull/5362) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- feat: support casting FixedSizeList with new child type [\#5360](https://github.com/apache/arrow-rs/pull/5360) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Add more debugging info to StructBuilder validate\_content [\#5357](https://github.com/apache/arrow-rs/pull/5357) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- pyarrow: Preserve RecordBatch's schema metadata [\#5355](https://github.com/apache/arrow-rs/pull/5355) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([atwam](https://github.com/atwam))
- Mark Encoding::BIT\_PACKED as deprecated and document its compatibility issues [\#5348](https://github.com/apache/arrow-rs/pull/5348) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- Track the size of custom allocations for use via Array::get\_buffer\_memory\_size [\#5347](https://github.com/apache/arrow-rs/pull/5347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- fix: Return an error on type mismatch rather than panic \(\#4995\) [\#5341](https://github.com/apache/arrow-rs/pull/5341) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([carols10cents](https://github.com/carols10cents))
- Minor: support cast values to fixedsizelist [\#5340](https://github.com/apache/arrow-rs/pull/5340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Enhance Time32/Time64 support in date\_part [\#5337](https://github.com/apache/arrow-rs/pull/5337) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- feat: add `take_record_batch`. [\#5333](https://github.com/apache/arrow-rs/pull/5333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Add ListBuilder::with\_field to support non nullable list fields \(\#5330\) [\#5331](https://github.com/apache/arrow-rs/pull/5331) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't omit schema metadata when removing column [\#5328](https://github.com/apache/arrow-rs/pull/5328) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Update proc-macro2 requirement from =1.0.76 to =1.0.78 [\#5324](https://github.com/apache/arrow-rs/pull/5324) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Enhance Date64 type documentation [\#5323](https://github.com/apache/arrow-rs/pull/5323) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- fix panic when decode a group with no child [\#5322](https://github.com/apache/arrow-rs/pull/5322) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Liyixin95](https://github.com/Liyixin95))
- Minor/Doc Expand FlightSqlServiceClient::handshake doc [\#5321](https://github.com/apache/arrow-rs/pull/5321) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([devinjdangelo](https://github.com/devinjdangelo))
- Refactor temporal extract date part kernels [\#5319](https://github.com/apache/arrow-rs/pull/5319) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add JSON writer benchmarks \(\#5314\) [\#5317](https://github.com/apache/arrow-rs/pull/5317) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Bump actions/cache from 3 to 4 [\#5308](https://github.com/apache/arrow-rs/pull/5308) ([dependabot[bot]](https://github.com/apps/dependabot))
- Avro block decompression [\#5306](https://github.com/apache/arrow-rs/pull/5306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Result into error in case of endianness mismatches [\#5301](https://github.com/apache/arrow-rs/pull/5301) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pangiole](https://github.com/pangiole))
- parquet: Add ArrowWriterOptions to skip embedding the arrow metadata [\#5299](https://github.com/apache/arrow-rs/pull/5299) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([evenyag](https://github.com/evenyag))
- Add support for more fused boolean operations [\#5298](https://github.com/apache/arrow-rs/pull/5298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RTEnzyme](https://github.com/RTEnzyme))
- Support Parquet  Byte Stream Split Encoding [\#5293](https://github.com/apache/arrow-rs/pull/5293) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mwlon](https://github.com/mwlon))
- Extend string parsing support for Date32 [\#5282](https://github.com/apache/arrow-rs/pull/5282) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Bring some methods over from ArrowWriter to the async version [\#5251](https://github.com/apache/arrow-rs/pull/5251) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
