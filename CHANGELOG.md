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

## [54.3.0](https://github.com/apache/arrow-rs/tree/54.3.0) (2025-03-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.4.1...54.3.0)

**Implemented enhancements:**

- Support reading parquet with modular encryption [\#7296](https://github.com/apache/arrow-rs/issues/7296) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add example for how to read/write encrypted parquet files [\#7281](https://github.com/apache/arrow-rs/issues/7281) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Have writer return parsed `ParquetMetadata` [\#7254](https://github.com/apache/arrow-rs/issues/7254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- feat: Support Utf8View in  JSON reader [\#7244](https://github.com/apache/arrow-rs/issues/7244) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- StructBuilder should provide a way to get a &dyn ArrayBuilder of a field builder [\#7193](https://github.com/apache/arrow-rs/issues/7193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support div\_wrapping/rem\_wrapping for numeric arithmetic kernels [\#7158](https://github.com/apache/arrow-rs/issues/7158) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Incorrect IPC schema encoding for multiple dictionaries [\#7058](https://github.com/apache/arrow-rs/issues/7058) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Documentation updates:**

- Add example for how to read encrypted parquet files [\#7283](https://github.com/apache/arrow-rs/pull/7283) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rok](https://github.com/rok))
- Update the relative path of the test data in docs [\#7221](https://github.com/apache/arrow-rs/pull/7221) ([Ziy1-Tan](https://github.com/Ziy1-Tan))
- Minor: fix doc and remove unused code [\#7194](https://github.com/apache/arrow-rs/pull/7194) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lewiszlw](https://github.com/lewiszlw))
- doc: modify wrong comment [\#7190](https://github.com/apache/arrow-rs/pull/7190) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([YichiZhang0613](https://github.com/YichiZhang0613))
- doc: fix IPC file reader/writer docs [\#7178](https://github.com/apache/arrow-rs/pull/7178) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))

**Performance improvements:**

- Improve RleDecoder performance [\#7195](https://github.com/apache/arrow-rs/pull/7195) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Improve arrow-json deserialization performance by 30% [\#7157](https://github.com/apache/arrow-rs/pull/7157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mwylde](https://github.com/mwylde))
- Add `with_skip_validation` flag to IPC `StreamReader`, `FileReader` and `FileDecoder` [\#7120](https://github.com/apache/arrow-rs/pull/7120) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Minor: run `test_decimal_list` again [\#7282](https://github.com/apache/arrow-rs/pull/7282) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Move Parquet encryption tests into the arrow\_reader integration tests [\#7279](https://github.com/apache/arrow-rs/pull/7279) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Include license and notice files in published crates, part 2 [\#7275](https://github.com/apache/arrow-rs/pull/7275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ankane](https://github.com/ankane))
- feat: Support Utf8View in JSON reader [\#7263](https://github.com/apache/arrow-rs/pull/7263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- feat: use `force_validate` feature flag when creating an arrays [\#7241](https://github.com/apache/arrow-rs/pull/7241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- fix: take on empty struct array returns empty array [\#7224](https://github.com/apache/arrow-rs/pull/7224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([westonpace](https://github.com/westonpace))
- fix: correct `bloom_filter_position` description [\#7223](https://github.com/apache/arrow-rs/pull/7223) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([romanz](https://github.com/romanz))
- Minor: Move `make_builder` into mod.rs [\#7218](https://github.com/apache/arrow-rs/pull/7218) ([lewiszlw](https://github.com/lewiszlw))
- Expose `field_builders` in `StructBuilder` [\#7217](https://github.com/apache/arrow-rs/pull/7217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lewiszlw](https://github.com/lewiszlw))
- Minor: Fix json StructMode docs links [\#7215](https://github.com/apache/arrow-rs/pull/7215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- \[main\] Bump arrow version to 54.2.1 \(\#7207\) [\#7212](https://github.com/apache/arrow-rs/pull/7212) ([alamb](https://github.com/alamb))
- feat: add `downcast_integer_array` macro helper [\#7211](https://github.com/apache/arrow-rs/pull/7211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Remove zstd pin [\#7199](https://github.com/apache/arrow-rs/pull/7199) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- fix: Use chrono's quarter\(\) to avoid conflict [\#7198](https://github.com/apache/arrow-rs/pull/7198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yutannihilation](https://github.com/yutannihilation))
- Fix some Clippy 1.85 warnings [\#7167](https://github.com/apache/arrow-rs/pull/7167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- feat: add to concat different data types error message the data types [\#7166](https://github.com/apache/arrow-rs/pull/7166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Add Week ISO, Year ISO computation [\#7163](https://github.com/apache/arrow-rs/pull/7163) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kosiew](https://github.com/kosiew))
- fix: create\_random\_batch fails with timestamp types having a timezone [\#7162](https://github.com/apache/arrow-rs/pull/7162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([niebayes](https://github.com/niebayes))
- Avoid overflow of remainder [\#7159](https://github.com/apache/arrow-rs/pull/7159) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wForget](https://github.com/wForget))
- fix: Data type inference for NaN, inf and -inf in csv files [\#7150](https://github.com/apache/arrow-rs/pull/7150) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Mottl](https://github.com/Mottl))
- Preserve null dictionary values in `interleave` and `concat` kernels [\#7144](https://github.com/apache/arrow-rs/pull/7144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Support casting `Date` to a time zone-specific timestamp [\#7141](https://github.com/apache/arrow-rs/pull/7141) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([friendlymatthew](https://github.com/friendlymatthew))
- Minor: Add doctest to ArrayDataBuilder::build\_unchecked  [\#7139](https://github.com/apache/arrow-rs/pull/7139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- arrow-ord: add support for nested types to `partition` [\#7131](https://github.com/apache/arrow-rs/pull/7131) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asubiotto](https://github.com/asubiotto))
- Update prost-build requirement from =0.13.4 to =0.13.5 [\#7127](https://github.com/apache/arrow-rs/pull/7127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Avoid use of `flatbuffers::size_prefixed_root`, fix validation error in arrow-flight [\#7109](https://github.com/apache/arrow-rs/pull/7109) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([bkietz](https://github.com/bkietz))
- Optimise decimal casting for infallible conversions [\#7021](https://github.com/apache/arrow-rs/pull/7021) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([aweltsch](https://github.com/aweltsch))

## [53.4.1](https://github.com/apache/arrow-rs/tree/53.4.1) (2025-03-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/54.2.1...53.4.1)

**Fixed bugs:**

- Take empty struct array would get array with length 0 [\#7225](https://github.com/apache/arrow-rs/issues/7225)

**Closed issues:**

- Release arrow-rs / parquet patch version 54.2.1 \(Feb 2025\) \(HOTFIX\) [\#7209](https://github.com/apache/arrow-rs/issues/7209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
