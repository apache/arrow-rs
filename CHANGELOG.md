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

## [35.0.0](https://github.com/apache/arrow-rs/tree/35.0.0) (2023-03-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/34.0.0...35.0.0)

**Breaking changes:**

- Add RunEndBuffer \(\#1799\) [\#3817](https://github.com/apache/arrow-rs/pull/3817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Restrict DictionaryArray to ArrowDictionaryKeyType [\#3813](https://github.com/apache/arrow-rs/pull/3813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- refactor: assorted `FlightSqlServiceClient` improvements [\#3788](https://github.com/apache/arrow-rs/pull/3788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- minor: make Parquet CLI input args consistent [\#3786](https://github.com/apache/arrow-rs/pull/3786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XinyuZeng](https://github.com/XinyuZeng))
- Return Buffers from ArrayData::buffers instead of slice \(\#1799\) [\#3783](https://github.com/apache/arrow-rs/pull/3783) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use NullBuffer in ArrayData \(\#3775\) [\#3778](https://github.com/apache/arrow-rs/pull/3778) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Support timestamp/time and date types in json decoder [\#3834](https://github.com/apache/arrow-rs/issues/3834) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support decoding decimals in new raw json decoder [\#3819](https://github.com/apache/arrow-rs/issues/3819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Timezone Aware Timestamp Parsing [\#3794](https://github.com/apache/arrow-rs/issues/3794) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Preallocate buffers for FixedSizeBinary array creation [\#3792](https://github.com/apache/arrow-rs/issues/3792) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make Parquet CLI args consistent [\#3785](https://github.com/apache/arrow-rs/issues/3785) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Creates PrimitiveDictionaryBuilder from provided keys and values builders [\#3776](https://github.com/apache/arrow-rs/issues/3776) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use NullBuffer in ArrayData  [\#3775](https://github.com/apache/arrow-rs/issues/3775) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support unary\_dict\_mut in arth  [\#3710](https://github.com/apache/arrow-rs/issues/3710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support cast \<\> String to interval [\#3643](https://github.com/apache/arrow-rs/issues/3643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Zero-Copy Conversion from Vec to/from MutableBuffer [\#3516](https://github.com/apache/arrow-rs/issues/3516) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Timestamp Unit Casts are Unchecked [\#3833](https://github.com/apache/arrow-rs/issues/3833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- regexp\_match skips first match when returning match [\#3803](https://github.com/apache/arrow-rs/issues/3803) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast to timestamp with time zone returns timestamp [\#3800](https://github.com/apache/arrow-rs/issues/3800) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Schema-level metadata is not encoded in Flight responses [\#3779](https://github.com/apache/arrow-rs/issues/3779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Closed issues:**

- FlightSQL CLI client: simple test [\#3814](https://github.com/apache/arrow-rs/issues/3814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Merged pull requests:**

- refactor: timestamp overflow check [\#3840](https://github.com/apache/arrow-rs/pull/3840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Prep for 35.0.0 [\#3836](https://github.com/apache/arrow-rs/pull/3836) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Support timestamp/time and date json decoding [\#3835](https://github.com/apache/arrow-rs/pull/3835) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Make dictionary preservation optional in row encoding [\#3831](https://github.com/apache/arrow-rs/pull/3831) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move prettyprint to arrow-cast [\#3828](https://github.com/apache/arrow-rs/pull/3828) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Support decoding decimals in raw decoder [\#3820](https://github.com/apache/arrow-rs/pull/3820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Add ArrayDataLayout, port validation \(\#1799\) [\#3818](https://github.com/apache/arrow-rs/pull/3818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- test: add test for FlightSQL CLI client [\#3816](https://github.com/apache/arrow-rs/pull/3816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Add regexp\_match docs [\#3812](https://github.com/apache/arrow-rs/pull/3812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix: Ensure Flight schema includes parent metadata [\#3811](https://github.com/apache/arrow-rs/pull/3811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([stuartcarnie](https://github.com/stuartcarnie))
- fix: regexp\_match skips first match [\#3807](https://github.com/apache/arrow-rs/pull/3807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- fix: change uft8 to timestamp with timezone [\#3806](https://github.com/apache/arrow-rs/pull/3806) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Support reading decimal arrays from json [\#3805](https://github.com/apache/arrow-rs/pull/3805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Add unary\_dict\_mut [\#3804](https://github.com/apache/arrow-rs/pull/3804) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Faster timestamp parsing \(~70-90% faster\) [\#3801](https://github.com/apache/arrow-rs/pull/3801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add concat\_elements\_bytes [\#3798](https://github.com/apache/arrow-rs/pull/3798) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Timezone aware timestamp parsing \(\#3794\) [\#3795](https://github.com/apache/arrow-rs/pull/3795) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preallocate buffers for FixedSizeBinary array creation [\#3793](https://github.com/apache/arrow-rs/pull/3793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
- feat: simple flight sql CLI client [\#3789](https://github.com/apache/arrow-rs/pull/3789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Creates PrimitiveDictionaryBuilder from provided keys and values builders [\#3777](https://github.com/apache/arrow-rs/pull/3777) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- ArrayData Enumeration for Remaining Layouts [\#3769](https://github.com/apache/arrow-rs/pull/3769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.11.7 to =0.11.8 [\#3767](https://github.com/apache/arrow-rs/pull/3767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Implement concat\_elements\_dyn kernel [\#3763](https://github.com/apache/arrow-rs/pull/3763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Support for casting `Utf8` and `LargeUtf8` --\>  `Interval` [\#3762](https://github.com/apache/arrow-rs/pull/3762) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- into\_inner\(\) for CSV Writer [\#3759](https://github.com/apache/arrow-rs/pull/3759) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Zero-copy Vec conversion \(\#3516\) \(\#1176\) [\#3756](https://github.com/apache/arrow-rs/pull/3756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ArrayData Enumeration for Primitive, Binary and UTF8 [\#3749](https://github.com/apache/arrow-rs/pull/3749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `into_primitive_dict_builder` to `DictionaryArray` [\#3715](https://github.com/apache/arrow-rs/pull/3715) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
