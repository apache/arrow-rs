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

## [47.0.0](https://github.com/apache/arrow-rs/tree/47.0.0) (2023-09-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/46.0.0...47.0.0)

**Breaking changes:**

- Make FixedSizeBinaryArray value\_data return a reference [\#4820](https://github.com/apache/arrow-rs/issues/4820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update prost to v0.12.1 [\#4825](https://github.com/apache/arrow-rs/pull/4825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- feat: FixedSizeBinaryArray::value\_data return reference [\#4821](https://github.com/apache/arrow-rs/pull/4821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Stateless Row Encoding / Don't Preserve Dictionaries in `RowConverter` \(\#4811\) [\#4819](https://github.com/apache/arrow-rs/pull/4819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- fix: entries field is non-nullable [\#4808](https://github.com/apache/arrow-rs/pull/4808) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Fix flight sql do put handling, add bind parameter support to FlightSQL cli client [\#4797](https://github.com/apache/arrow-rs/pull/4797) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([suremarc](https://github.com/suremarc))
- Remove unused dyn\_cmp\_dict feature [\#4766](https://github.com/apache/arrow-rs/pull/4766) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add underlying `std::io::Error` to `IoError` and add `IpcError` variant [\#4726](https://github.com/apache/arrow-rs/pull/4726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alexandreyc](https://github.com/alexandreyc))

**Implemented enhancements:**

- Row Format Adapative Block Size  [\#4812](https://github.com/apache/arrow-rs/issues/4812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Stateless Row Conversion [\#4811](https://github.com/apache/arrow-rs/issues/4811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add option to specify custom null values for CSV reader [\#4794](https://github.com/apache/arrow-rs/issues/4794) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet::record::RowIter cannot be customized with batch\_size and defaults to 1024 [\#4782](https://github.com/apache/arrow-rs/issues/4782) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `DynScalar` abstraction \(something that makes it easy to create scalar `Datum`s\) [\#4781](https://github.com/apache/arrow-rs/issues/4781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `Datum` is not exported as part of `arrow` \(it is only exported in `arrow_array`\) [\#4780](https://github.com/apache/arrow-rs/issues/4780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `Scalar` is not exported as part of `arrow` \(it is only exported in `arrow_array`\) [\#4779](https://github.com/apache/arrow-rs/issues/4779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support IntoPyArrow for impl RecordBatchReader [\#4730](https://github.com/apache/arrow-rs/issues/4730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Datum Based String Kernels [\#4595](https://github.com/apache/arrow-rs/issues/4595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- MapArray::new\_from\_strings creates nullable entries field [\#4807](https://github.com/apache/arrow-rs/issues/4807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- pyarrow module can't roundtrip tensor arrays [\#4805](https://github.com/apache/arrow-rs/issues/4805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `concat_batches` errors with "schema mismatch" error when only metadata differs [\#4799](https://github.com/apache/arrow-rs/issues/4799) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- panic in `cmp` kernels with DictionaryArrays:  `Option::unwrap()` on a `None` value' [\#4788](https://github.com/apache/arrow-rs/issues/4788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- stream ffi panics if schema metadata values aren't valid utf8 [\#4750](https://github.com/apache/arrow-rs/issues/4750) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Regression: Incorrect Sorting of `*ListArray` in 46.0.0 [\#4746](https://github.com/apache/arrow-rs/issues/4746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Row is no longer comparable after reuse [\#4741](https://github.com/apache/arrow-rs/issues/4741) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DoPut FlightSQL handler inadvertently consumes schema at start of Request\<Streaming\<FlightData\>\> [\#4658](https://github.com/apache/arrow-rs/issues/4658)
- Return error when converting schema  [\#4752](https://github.com/apache/arrow-rs/pull/4752) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Implement PyArrowType for `Box<dyn RecordBatchReader + Send>` [\#4751](https://github.com/apache/arrow-rs/pull/4751) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))

**Closed issues:**

- Building arrow-rust for target wasm32-wasi falied to compile packed\_simd\_2 [\#4717](https://github.com/apache/arrow-rs/issues/4717)

**Merged pull requests:**

- Respect FormatOption::nulls for NullArray [\#4836](https://github.com/apache/arrow-rs/pull/4836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix merge\_dictionary\_values in selection kernels [\#4833](https://github.com/apache/arrow-rs/pull/4833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix like scalar null [\#4832](https://github.com/apache/arrow-rs/pull/4832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- More chrono deprecations [\#4822](https://github.com/apache/arrow-rs/pull/4822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
-  Adaptive Row Block Size \(\#4812\) [\#4818](https://github.com/apache/arrow-rs/pull/4818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.66 to =1.0.67 [\#4816](https://github.com/apache/arrow-rs/pull/4816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Do not check schema for equality in concat\_batches [\#4815](https://github.com/apache/arrow-rs/pull/4815) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: export record batch through stream [\#4806](https://github.com/apache/arrow-rs/pull/4806) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Improve CSV Reader Benchmark Coverage of Small Primitives [\#4803](https://github.com/apache/arrow-rs/pull/4803) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- csv: Add option to specify custom null values [\#4795](https://github.com/apache/arrow-rs/pull/4795) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vrongmeal](https://github.com/vrongmeal))
- Expand docstring and add example to `Scalar` [\#4793](https://github.com/apache/arrow-rs/pull/4793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Re-export array crate root \(\#4780\) \(\#4779\) [\#4791](https://github.com/apache/arrow-rs/pull/4791) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix DictionaryArray::normalized\_keys \(\#4788\) [\#4789](https://github.com/apache/arrow-rs/pull/4789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow custom tree builder for parquet::record::RowIter [\#4783](https://github.com/apache/arrow-rs/pull/4783) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([YuraKotov](https://github.com/YuraKotov))
- Bump actions/checkout from 3 to 4 [\#4767](https://github.com/apache/arrow-rs/pull/4767) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: avoid panic if offset index not exists. [\#4761](https://github.com/apache/arrow-rs/pull/4761) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Relax constraints on PyArrowType [\#4757](https://github.com/apache/arrow-rs/pull/4757) ([tustvold](https://github.com/tustvold))
- Chrono deprecations [\#4748](https://github.com/apache/arrow-rs/pull/4748) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix List Sorting, Revert Removal of Rank Kernels [\#4747](https://github.com/apache/arrow-rs/pull/4747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Clear row buffer before reuse [\#4742](https://github.com/apache/arrow-rs/pull/4742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Datum based like kernels \(\#4595\) [\#4732](https://github.com/apache/arrow-rs/pull/4732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- feat: expose DoGet response headers & trailers [\#4727](https://github.com/apache/arrow-rs/pull/4727) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Cleanup length and bit\_length kernels [\#4718](https://github.com/apache/arrow-rs/pull/4718) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
