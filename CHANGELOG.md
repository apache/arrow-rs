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

## [43.0.0](https://github.com/apache/arrow-rs/tree/43.0.0) (2023-06-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/42.0.0...43.0.0)

**Breaking changes:**

- Simplify ffi import/export [\#4447](https://github.com/apache/arrow-rs/pull/4447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Virgiel](https://github.com/Virgiel))
- Return Result from Parquet Row APIs [\#4428](https://github.com/apache/arrow-rs/pull/4428) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Remove Binary Dictionary Arithmetic Support [\#4407](https://github.com/apache/arrow-rs/pull/4407) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Request: a way to copy a `Row` to `Rows` [\#4466](https://github.com/apache/arrow-rs/issues/4466) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reuse schema when importing from FFI [\#4444](https://github.com/apache/arrow-rs/issues/4444) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[FlightSQL\] Allow implementations of `FlightSqlService` to handle custom actions and commands [\#4439](https://github.com/apache/arrow-rs/issues/4439)
- Support `NullBuilder` [\#4429](https://github.com/apache/arrow-rs/issues/4429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Regression in  in parquet `42.0.0` : Bad parquet column indexes for All Null Columns, resulting in `Parquet error: StructArrayReader out of sync` on read [\#4459](https://github.com/apache/arrow-rs/issues/4459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Regression in 42.0.0: Parsing fractional intervals without leading 0 is not supported [\#4424](https://github.com/apache/arrow-rs/issues/4424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- doc: deploy crate docs to GitHub pages [\#4436](https://github.com/apache/arrow-rs/pull/4436) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xxchan](https://github.com/xxchan))

**Merged pull requests:**

- Append Row to Rows \(\#4466\) [\#4470](https://github.com/apache/arrow-rs/pull/4470) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat\(flight-sql\): Allow implementations of FlightSqlService to handle custom actions and commands [\#4463](https://github.com/apache/arrow-rs/pull/4463) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([amartins23](https://github.com/amartins23))
- Docs: Add clearer API doc links [\#4461](https://github.com/apache/arrow-rs/pull/4461) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Fix empty offset index for all null columns \(\#4459\) [\#4460](https://github.com/apache/arrow-rs/pull/4460) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Bump peaceiris/actions-gh-pages from 3.9.2 to 3.9.3 [\#4455](https://github.com/apache/arrow-rs/pull/4455) ([dependabot[bot]](https://github.com/apps/dependabot))
- Convince the compiler to auto-vectorize the range check in parquet DictionaryBuffer [\#4453](https://github.com/apache/arrow-rs/pull/4453) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- fix docs deployment [\#4452](https://github.com/apache/arrow-rs/pull/4452) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xxchan](https://github.com/xxchan))
- Update indexmap requirement from 1.9 to 2.0 [\#4451](https://github.com/apache/arrow-rs/pull/4451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.60 to =1.0.63 [\#4450](https://github.com/apache/arrow-rs/pull/4450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/deploy-pages from 1 to 2 [\#4449](https://github.com/apache/arrow-rs/pull/4449) ([dependabot[bot]](https://github.com/apps/dependabot))
- Revise error message in From\<Buffer\> for ScalarBuffer [\#4446](https://github.com/apache/arrow-rs/pull/4446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- minor: remove useless mut [\#4443](https://github.com/apache/arrow-rs/pull/4443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- unify substring for binary&utf8 [\#4442](https://github.com/apache/arrow-rs/pull/4442) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Casting fixedsizelist to list/largelist [\#4433](https://github.com/apache/arrow-rs/pull/4433) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jayzhan211](https://github.com/jayzhan211))
- feat: support `NullBuilder` [\#4430](https://github.com/apache/arrow-rs/pull/4430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Remove Float64 -\> Float32 cast in IPC Reader [\#4427](https://github.com/apache/arrow-rs/pull/4427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ming08108](https://github.com/ming08108))
- Parse intervals like `.5` the same as `0.5` [\#4425](https://github.com/apache/arrow-rs/pull/4425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat: add strict mode to json reader [\#4421](https://github.com/apache/arrow-rs/pull/4421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([blinkseb](https://github.com/blinkseb))
- Add DictionaryArray::occupancy [\#4415](https://github.com/apache/arrow-rs/pull/4415) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
