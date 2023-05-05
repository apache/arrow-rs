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

## [39.0.0](https://github.com/apache/arrow-rs/tree/39.0.0) (2023-05-05)

[Full Changelog](https://github.com/apache/arrow-rs/compare/38.0.0...39.0.0)

**Breaking changes:**

- Cleanup ChunkReader \(\#4118\) [\#4156](https://github.com/apache/arrow-rs/pull/4156) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove Type from NativeIndex [\#4146](https://github.com/apache/arrow-rs/pull/4146) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Don't Duplicate Offset Index on RowGroupMetadata [\#4142](https://github.com/apache/arrow-rs/pull/4142) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Return BooleanBuffer from BooleanBufferBuilder [\#4140](https://github.com/apache/arrow-rs/pull/4140) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup CSV schema inference \(\#4129\) \(\#4130\) [\#4133](https://github.com/apache/arrow-rs/pull/4133) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove deprecated parquet ArrowReader [\#4125](https://github.com/apache/arrow-rs/pull/4125) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: construct `StructArray` w/ `FieldRef` [\#4116](https://github.com/apache/arrow-rs/pull/4116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Ignore Field Metadata in equals\_datatype for Dictionary, RunEndEncoded, Map and Union [\#4111](https://github.com/apache/arrow-rs/pull/4111) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add StructArray Constructors \(\#3879\) [\#4064](https://github.com/apache/arrow-rs/pull/4064) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- InMemory append API [\#4152](https://github.com/apache/arrow-rs/issues/4152)
- Fixed point decimal multiplication for DictionaryArray [\#4135](https://github.com/apache/arrow-rs/issues/4135)
- Remove Seek Requirement from CSV ReaderBuilder [\#4130](https://github.com/apache/arrow-rs/issues/4130)
- Inconsistent CSV Inference and Parsing DateTime Handling [\#4129](https://github.com/apache/arrow-rs/issues/4129)
- Support accessing ipc Reader/Writer inner by reference [\#4121](https://github.com/apache/arrow-rs/issues/4121)
- \[object\_store\] Retry requests on connection error [\#4119](https://github.com/apache/arrow-rs/issues/4119)
- Add Type Declarations for All Primitive Tensors and Buffer Builders [\#4112](https://github.com/apache/arrow-rs/issues/4112)
- Support `Interval + Timestamp` and `Interval + Date` in addition to `Timestamp + Interval` and `Interval + Date` [\#4094](https://github.com/apache/arrow-rs/issues/4094)
- Enable setting FlightDescriptor on FlightDataEncoderBuilder [\#3855](https://github.com/apache/arrow-rs/issues/3855) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Parquet Page Index Reader Assumes Consecutive Offsets [\#4149](https://github.com/apache/arrow-rs/issues/4149)
- Equality of nested data types [\#4110](https://github.com/apache/arrow-rs/issues/4110)

**Documentation updates:**

- Improve Documentation of Parquet ChunkReader [\#4118](https://github.com/apache/arrow-rs/issues/4118)

**Closed issues:**

- add specific error log for empty JSON array [\#4105](https://github.com/apache/arrow-rs/issues/4105)

**Merged pull requests:**

- Support Compression in parquet-fromcsv [\#4160](https://github.com/apache/arrow-rs/pull/4160) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([suxiaogang223](https://github.com/suxiaogang223))
- feat: support bitwise shift left/right with scalars [\#4159](https://github.com/apache/arrow-rs/pull/4159) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Cleanup reading page index \(\#4149\) \(\#4090\) [\#4151](https://github.com/apache/arrow-rs/pull/4151) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: support `bitwise` shift left/right [\#4148](https://github.com/apache/arrow-rs/pull/4148) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Don't hardcode port in FlightSQL tests [\#4145](https://github.com/apache/arrow-rs/pull/4145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Better flight SQL example codes [\#4144](https://github.com/apache/arrow-rs/pull/4144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([sundy-li](https://github.com/sundy-li))
- chore: clean the code by using `as_primitive` [\#4143](https://github.com/apache/arrow-rs/pull/4143) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- docs: fix the wrong ln command in CONTRIBUTING.md [\#4139](https://github.com/apache/arrow-rs/pull/4139) ([SteveLauC](https://github.com/SteveLauC))
- Infer Float64 for JSON Numerics Beyond Bounds of i64 [\#4138](https://github.com/apache/arrow-rs/pull/4138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SteveLauC](https://github.com/SteveLauC))
- Support fixed point multiplication for DictionaryArray of Decimals [\#4136](https://github.com/apache/arrow-rs/pull/4136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make arrow\_json::ReaderBuilder method names consistent [\#4128](https://github.com/apache/arrow-rs/pull/4128) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add get\_{ref, mut} to arrow\_ipc Reader and Writer [\#4122](https://github.com/apache/arrow-rs/pull/4122) ([sticnarf](https://github.com/sticnarf))
- feat: support `Interval` + `Timestamp` and `Interval` + `Date` [\#4117](https://github.com/apache/arrow-rs/pull/4117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Support NullArray in JSON Reader [\#4114](https://github.com/apache/arrow-rs/pull/4114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jiangzhx](https://github.com/jiangzhx))
- Add Type Declarations for All Primitive Tensors and Buffer Builders [\#4113](https://github.com/apache/arrow-rs/pull/4113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Update regex-syntax requirement from 0.6.27 to 0.7.1 [\#4107](https://github.com/apache/arrow-rs/pull/4107) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: set FlightDescriptor on FlightDataEncoderBuilder [\#4101](https://github.com/apache/arrow-rs/pull/4101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Weijun-H](https://github.com/Weijun-H))
- optimize cast for same decimal type and same scale [\#4088](https://github.com/apache/arrow-rs/pull/4088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
