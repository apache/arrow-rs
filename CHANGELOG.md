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

## [40.0.0](https://github.com/apache/arrow-rs/tree/40.0.0) (2023-05-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/39.0.0...40.0.0)

**Breaking changes:**

- Prefetch page index \(\#4090\) [\#4216](https://github.com/apache/arrow-rs/pull/4216) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add RecordBatchWriter trait and implement it for CSV, JSON, IPC and P… [\#4206](https://github.com/apache/arrow-rs/pull/4206) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Remove powf\_scalar kernel [\#4187](https://github.com/apache/arrow-rs/pull/4187) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow format specification in cast [\#4169](https://github.com/apache/arrow-rs/pull/4169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([parthchandra](https://github.com/parthchandra))

**Implemented enhancements:**

- ObjectStore with\_url Should Handle Path [\#4199](https://github.com/apache/arrow-rs/issues/4199)
- Support `Interval` +/- `Interval` [\#4178](https://github.com/apache/arrow-rs/issues/4178) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\] add compression info to `print_column_chunk_metadata()` [\#4172](https://github.com/apache/arrow-rs/issues/4172) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Allow cast to take in a format specification [\#4168](https://github.com/apache/arrow-rs/issues/4168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support extended pow arithmetic [\#4166](https://github.com/apache/arrow-rs/issues/4166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Preload page index for async ParquetObjectReader [\#4090](https://github.com/apache/arrow-rs/issues/4090) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Subtracting `Timestamp` from `Timestamp`  should produce a `Duration` \(not `Timestamp`\)  [\#3964](https://github.com/apache/arrow-rs/issues/3964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Arrow Arithmetic: Subtract timestamps [\#4244](https://github.com/apache/arrow-rs/pull/4244) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mr-brobot](https://github.com/mr-brobot))
- Update proc-macro2 requirement from =1.0.57 to =1.0.58 [\#4236](https://github.com/apache/arrow-rs/pull/4236) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix Nightly Clippy Lints [\#4233](https://github.com/apache/arrow-rs/pull/4233) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: use all primitive types in test\_layouts [\#4229](https://github.com/apache/arrow-rs/pull/4229) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add close method to RecordBatchWriter trait [\#4228](https://github.com/apache/arrow-rs/pull/4228) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Update proc-macro2 requirement from =1.0.56 to =1.0.57 [\#4219](https://github.com/apache/arrow-rs/pull/4219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Feat docs [\#4215](https://github.com/apache/arrow-rs/pull/4215) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Folyd](https://github.com/Folyd))
- feat: Support bitwise and boolean aggregate functions [\#4210](https://github.com/apache/arrow-rs/pull/4210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Document how to sort a RecordBatch [\#4204](https://github.com/apache/arrow-rs/pull/4204) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix incorrect cast Timestamp with Timezone [\#4201](https://github.com/apache/arrow-rs/pull/4201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([aprimadi](https://github.com/aprimadi))
- Add implementation of `RecordBatchReader` for CSV reader [\#4195](https://github.com/apache/arrow-rs/pull/4195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Add Sliced ListArray test \(\#3748\) [\#4186](https://github.com/apache/arrow-rs/pull/4186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- refactor: simplify can\_cast\_types code. [\#4185](https://github.com/apache/arrow-rs/pull/4185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Minor: support new types in struct\_builder.rs [\#4177](https://github.com/apache/arrow-rs/pull/4177) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- feat: add compression info to print\_column\_chunk\_metadata\(\) [\#4176](https://github.com/apache/arrow-rs/pull/4176) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SteveLauC](https://github.com/SteveLauC))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
