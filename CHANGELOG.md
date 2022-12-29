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

## [30.0.0](https://github.com/apache/arrow-rs/tree/30.0.0) (2022-12-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/29.0.0...30.0.0)

**Breaking changes:**

- Infer Parquet JSON Logical and Converted Type as UTF-8 [\#3376](https://github.com/apache/arrow-rs/pull/3376) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use custom Any instead of prost\_types [\#3360](https://github.com/apache/arrow-rs/pull/3360) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Use bytes in arrow-flight [\#3359](https://github.com/apache/arrow-rs/pull/3359) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Add derived implementations of Clone and Debug for `ParquetObjectReader` [\#3381](https://github.com/apache/arrow-rs/issues/3381)
- Speed up TrackedWrite [\#3366](https://github.com/apache/arrow-rs/issues/3366)
- Is it possible for ArrowWriter to write key\_value\_metadata after write all records [\#3356](https://github.com/apache/arrow-rs/issues/3356)
- Add UnionArray test to arrow-pyarrow integration test [\#3346](https://github.com/apache/arrow-rs/issues/3346)
- Add a constant prefix object store wrapper [\#3328](https://github.com/apache/arrow-rs/issues/3328)
- Document / Deprecate arrow\_flight::utils::flight\_data\_from\_arrow\_batch [\#3312](https://github.com/apache/arrow-rs/issues/3312) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[FlightSQL\] Support HTTPs [\#3309](https://github.com/apache/arrow-rs/issues/3309) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support UnionArray in ffi [\#3304](https://github.com/apache/arrow-rs/issues/3304)
- Add support for content-type while uploading files through ObjectStore API [\#3300](https://github.com/apache/arrow-rs/issues/3300)
- Add HttpStore [\#3294](https://github.com/apache/arrow-rs/issues/3294)
- Add support for Azure Data Lake Storage Gen2 \(aka: ADLS Gen2\) in Object Store library [\#3283](https://github.com/apache/arrow-rs/issues/3283)
- Support casting from String to Decimal [\#3280](https://github.com/apache/arrow-rs/issues/3280)
- Allow ArrowCSV writer to control the display of NULL values [\#3268](https://github.com/apache/arrow-rs/issues/3268) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release Arrow `29.0.0` \(next release after `28.0.0`\) [\#3216](https://github.com/apache/arrow-rs/issues/3216)

**Fixed bugs:**

- FlightSQL example is broken [\#3386](https://github.com/apache/arrow-rs/issues/3386)
- object\_store\(aws\): percent encoding test fails with latest localstack [\#3379](https://github.com/apache/arrow-rs/issues/3379)
- CSV Reader Bounds Incorrectly Handles Header [\#3364](https://github.com/apache/arrow-rs/issues/3364)
- Incorrect output string from `try_to_type`  [\#3350](https://github.com/apache/arrow-rs/issues/3350)
- Decimal arithmetic computation fails to run because decimal type equality [\#3344](https://github.com/apache/arrow-rs/issues/3344)
- Pretty print not implemented for Map [\#3322](https://github.com/apache/arrow-rs/issues/3322)
- ILIKE Kernels Inconsistent Case Folding [\#3311](https://github.com/apache/arrow-rs/issues/3311)

**Documentation updates:**

- minor: Improve arrow-flight docs [\#3372](https://github.com/apache/arrow-rs/pull/3372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Ends ParquetRecordBatchStream when polling on StreamState::Error [\#3404](https://github.com/apache/arrow-rs/pull/3404) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- fix clippy issues [\#3398](https://github.com/apache/arrow-rs/pull/3398) ([Jimexist](https://github.com/Jimexist))
- Upgrade multiversion to 0.7.1 [\#3396](https://github.com/apache/arrow-rs/pull/3396) ([viirya](https://github.com/viirya))
- Make FlightSQL Support HTTPs [\#3388](https://github.com/apache/arrow-rs/pull/3388) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Fix broken FlightSQL example [\#3387](https://github.com/apache/arrow-rs/pull/3387) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Update prost-build [\#3385](https://github.com/apache/arrow-rs/pull/3385) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Split out arrow-arith \(\#2594\) [\#3384](https://github.com/apache/arrow-rs/pull/3384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add derive for Clone and Debug for `ParquetObjectReader` [\#3382](https://github.com/apache/arrow-rs/pull/3382) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kszlim](https://github.com/kszlim))
- Initial Mid-level `FlightClient` [\#3378](https://github.com/apache/arrow-rs/pull/3378) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Document all features on docs.rs [\#3377](https://github.com/apache/arrow-rs/pull/3377) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Split out arrow-row \(\#2594\) [\#3375](https://github.com/apache/arrow-rs/pull/3375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove unnecessary flush calls on TrackedWrite [\#3374](https://github.com/apache/arrow-rs/pull/3374) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.47 to =1.0.49 [\#3369](https://github.com/apache/arrow-rs/pull/3369) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add CSV build\_buffered \(\#3338\) [\#3368](https://github.com/apache/arrow-rs/pull/3368) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add append\_key\_value\_metadata [\#3367](https://github.com/apache/arrow-rs/pull/3367) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jiacai2050](https://github.com/jiacai2050))
- Add csv-core based reader \(\#3338\) [\#3365](https://github.com/apache/arrow-rs/pull/3365) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Put BufWriter into TrackedWrite [\#3361](https://github.com/apache/arrow-rs/pull/3361) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add CSV reader benchmark \(\#3338\) [\#3357](https://github.com/apache/arrow-rs/pull/3357) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use ArrayData::ptr\_eq in DictionaryTracker [\#3354](https://github.com/apache/arrow-rs/pull/3354) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate flight\_data\_from\_arrow\_batch [\#3353](https://github.com/apache/arrow-rs/pull/3353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Dandandan](https://github.com/Dandandan))
- Fix incorrect output string from try\_to\_type [\#3351](https://github.com/apache/arrow-rs/pull/3351) ([viirya](https://github.com/viirya))
- Fix unary\_dyn for decimal scalar arithmetic computation [\#3345](https://github.com/apache/arrow-rs/pull/3345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add UnionArray test to arrow-pyarrow integration test [\#3343](https://github.com/apache/arrow-rs/pull/3343) ([viirya](https://github.com/viirya))
- feat: configure null value in arrow csv writer [\#3342](https://github.com/apache/arrow-rs/pull/3342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Optimize bulk writing of all blocks of bloom filter [\#3340](https://github.com/apache/arrow-rs/pull/3340) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add MapArray to pretty print [\#3339](https://github.com/apache/arrow-rs/pull/3339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Update prost-build 0.11.4 [\#3334](https://github.com/apache/arrow-rs/pull/3334) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Faster Parquet Bloom Writer [\#3333](https://github.com/apache/arrow-rs/pull/3333) ([tustvold](https://github.com/tustvold))
- Add bloom filter benchmark for parquet writer [\#3323](https://github.com/apache/arrow-rs/pull/3323) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add ASCII fast path for ILIKE scalar \(90% faster\) [\#3306](https://github.com/apache/arrow-rs/pull/3306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support UnionArray in ffi [\#3305](https://github.com/apache/arrow-rs/pull/3305) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support casting from String to Decimal [\#3281](https://github.com/apache/arrow-rs/pull/3281) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
