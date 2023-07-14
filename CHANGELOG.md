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

## [44.0.0](https://github.com/apache/arrow-rs/tree/44.0.0) (2023-07-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/43.0.0...44.0.0)

**Breaking changes:**

- Use Parser for cast kernel \(\#4512\) [\#4513](https://github.com/apache/arrow-rs/pull/4513) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Datum based arithmetic kernels \(\#3999\)  [\#4465](https://github.com/apache/arrow-rs/pull/4465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- eq\_dyn\_binary\_scalar should support FixedSizeBinary types [\#4491](https://github.com/apache/arrow-rs/issues/4491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Port Tests from Deprecated Arithmetic Kernels [\#4480](https://github.com/apache/arrow-rs/issues/4480) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatchReader for Boxed trait object [\#4474](https://github.com/apache/arrow-rs/issues/4474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `Date` - `Date` kernel [\#4383](https://github.com/apache/arrow-rs/issues/4383) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Default FlightSqlService Implementations [\#4372](https://github.com/apache/arrow-rs/issues/4372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Parquet: `AsyncArrowWriter` to a file corrupts the footer for large columns [\#4526](https://github.com/apache/arrow-rs/issues/4526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[object\_store\] Failure to send bytes to azure [\#4522](https://github.com/apache/arrow-rs/issues/4522)
- Cannot cast string '2021-01-02' to value of Date64 type [\#4512](https://github.com/apache/arrow-rs/issues/4512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect Interval Subtraction [\#4489](https://github.com/apache/arrow-rs/issues/4489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Interval Negation Incorrect [\#4488](https://github.com/apache/arrow-rs/issues/4488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: AsyncArrowWriter inner buffer is not correctly limited and causes OOM [\#4477](https://github.com/apache/arrow-rs/issues/4477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Fix AsyncArrowWriter flush for large buffer sizes \(\#4526\) [\#4527](https://github.com/apache/arrow-rs/pull/4527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cleanup cast\_primitive\_to\_list [\#4511](https://github.com/apache/arrow-rs/pull/4511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Bump actions/upload-pages-artifact from 1 to 2 [\#4508](https://github.com/apache/arrow-rs/pull/4508) ([dependabot[bot]](https://github.com/apps/dependabot))
- Support Date - Date \(\#4383\) [\#4504](https://github.com/apache/arrow-rs/pull/4504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Bump actions/labeler from 4.2.0 to 4.3.0 [\#4501](https://github.com/apache/arrow-rs/pull/4501) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.63 to =1.0.64 [\#4500](https://github.com/apache/arrow-rs/pull/4500) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add negate kernels \(\#4488\) [\#4494](https://github.com/apache/arrow-rs/pull/4494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Datum Arithmetic tests, Fix Interval Substraction \(\#4480\) [\#4493](https://github.com/apache/arrow-rs/pull/4493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- support FixedSizeBinary types in eq\_dyn\_binary\_scalar/neq\_dyn\_binary\_scalar [\#4492](https://github.com/apache/arrow-rs/pull/4492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
- Add default implementations to the FlightSqlService trait [\#4485](https://github.com/apache/arrow-rs/pull/4485) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([rossjones](https://github.com/rossjones))
- add num-complex requirement [\#4482](https://github.com/apache/arrow-rs/pull/4482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mwlon](https://github.com/mwlon))
- fix incorrect buffer size limiting in parquet async writer [\#4478](https://github.com/apache/arrow-rs/pull/4478) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([richox](https://github.com/richox))
- feat: support RecordBatchReader on boxed trait objects [\#4475](https://github.com/apache/arrow-rs/pull/4475) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Improve in-place primitive sorts by 13-67% [\#4473](https://github.com/apache/arrow-rs/pull/4473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Add Scalar/Datum abstraction \(\#1047\) [\#4393](https://github.com/apache/arrow-rs/pull/4393) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
