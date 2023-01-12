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

## [31.0.0](https://github.com/apache/arrow-rs/tree/31.0.0) (2023-01-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/30.0.1...31.0.0)

**Breaking changes:**

- support RFC3339 style timestamps in `arrow-json`  [\#3449](https://github.com/apache/arrow-rs/pull/3449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JayjeetAtGithub](https://github.com/JayjeetAtGithub))
- Improve arrow flight batch splitting and naming [\#3444](https://github.com/apache/arrow-rs/pull/3444) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Parquet record API: timestamp as signed integer [\#3437](https://github.com/apache/arrow-rs/pull/3437) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ByteBaker](https://github.com/ByteBaker))
- Support decimal int32/64 for writer [\#3431](https://github.com/apache/arrow-rs/pull/3431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))

**Implemented enhancements:**

- Support casting  Date32 to timestamp [\#3504](https://github.com/apache/arrow-rs/issues/3504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting strings like `'2001-01-01'` to timestamp [\#3492](https://github.com/apache/arrow-rs/issues/3492)
- Allow providing service account key directly when building GCP object store client [\#3488](https://github.com/apache/arrow-rs/issues/3488)
- CLI to "rewrite" parquet files [\#3476](https://github.com/apache/arrow-rs/issues/3476)
- Add more dictionary value type support to `build_compare` [\#3465](https://github.com/apache/arrow-rs/issues/3465)
- Allow `concat_batches` to take non owned RecordBatch [\#3456](https://github.com/apache/arrow-rs/issues/3456)
- Release Arrow `30.0.1` \(maintenance release for `30.0.0`\) [\#3455](https://github.com/apache/arrow-rs/issues/3455)
- Add string comparisons \(starts\_with, ends\_with, and contains\) to kernel [\#3442](https://github.com/apache/arrow-rs/issues/3442)
- make\_builder Loses Timezone and Decimal Scale Information [\#3435](https://github.com/apache/arrow-rs/issues/3435)
- Use RFC3339 style timestamps in arrow-json [\#3416](https://github.com/apache/arrow-rs/issues/3416)
- ArrayData`get_slice_memory_size`   or similar [\#3407](https://github.com/apache/arrow-rs/issues/3407)

**Fixed bugs:**

- Sliced batch w/ bool column doesn't roundtrip through IPC [\#3496](https://github.com/apache/arrow-rs/issues/3496) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- take kernel on List array introduces nulls instead of empty lists [\#3471](https://github.com/apache/arrow-rs/issues/3471)
- Infinite Loop If Skipping More CSV Lines than Present [\#3469](https://github.com/apache/arrow-rs/issues/3469)

**Closed issues:**

- object\_store: temporary aws credentials not refreshed? [\#3446](https://github.com/apache/arrow-rs/issues/3446)

**Merged pull requests:**

- Additional nullif re-export [\#3515](https://github.com/apache/arrow-rs/pull/3515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Enable cast Date32 to Timestamp [\#3508](https://github.com/apache/arrow-rs/pull/3508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Update prost-build requirement from =0.11.5 to =0.11.6 [\#3507](https://github.com/apache/arrow-rs/pull/3507) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- minor fix for the comments [\#3505](https://github.com/apache/arrow-rs/pull/3505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Fix DataTypeLayout for LargeList [\#3503](https://github.com/apache/arrow-rs/pull/3503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add string comparisons \(starts\_with, ends\_with, and contains\) to kernel [\#3502](https://github.com/apache/arrow-rs/pull/3502) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([snmvaughan](https://github.com/snmvaughan))
- Add a function to get memory size of array slice [\#3501](https://github.com/apache/arrow-rs/pull/3501) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Fix IPCWriter for Sliced BooleanArray [\#3498](https://github.com/apache/arrow-rs/pull/3498) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Fix: Added support to cast string without time [\#3494](https://github.com/apache/arrow-rs/pull/3494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gaelwjl](https://github.com/gaelwjl))
- Fix negative interval prettyprint [\#3491](https://github.com/apache/arrow-rs/pull/3491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Fixes a broken link in the arrow lib.rs rustdoc [\#3487](https://github.com/apache/arrow-rs/pull/3487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- Refactoring build\_compare for decimal and using downcast\_primitive [\#3484](https://github.com/apache/arrow-rs/pull/3484) ([viirya](https://github.com/viirya))
- Add tests for record batch size splitting logic in FlightClient [\#3481](https://github.com/apache/arrow-rs/pull/3481) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- change `concat_batches` parameter to non owned reference [\#3480](https://github.com/apache/arrow-rs/pull/3480) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- feat: add `parquet-rewrite` CLI [\#3477](https://github.com/apache/arrow-rs/pull/3477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- Preserve empty list array elements in take kernel [\#3473](https://github.com/apache/arrow-rs/pull/3473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonmmease](https://github.com/jonmmease))
- Add a test for stream writer for writing sliced array [\#3472](https://github.com/apache/arrow-rs/pull/3472) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix CSV infinite loop and improve error messages [\#3470](https://github.com/apache/arrow-rs/pull/3470) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add more dictionary value type support to `build_compare` [\#3466](https://github.com/apache/arrow-rs/pull/3466) ([viirya](https://github.com/viirya))
- Add tests for `FlightClient::{list_flights, list_actions, do_action, get_schema}` [\#3463](https://github.com/apache/arrow-rs/pull/3463) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Minor: add ticket links to failing ipc integration tests [\#3461](https://github.com/apache/arrow-rs/pull/3461) ([alamb](https://github.com/alamb))
- feat: `column_name` based index access for `RecordBatch` and `StructArray` [\#3458](https://github.com/apache/arrow-rs/pull/3458) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Support Decimal256 in FFI [\#3453](https://github.com/apache/arrow-rs/pull/3453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove multiversion dependency [\#3452](https://github.com/apache/arrow-rs/pull/3452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Re-export nullif kernel [\#3451](https://github.com/apache/arrow-rs/pull/3451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Meaningful error message for map builder with null keys [\#3450](https://github.com/apache/arrow-rs/pull/3450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Parquet writer v2: clear buffer after page flush [\#3447](https://github.com/apache/arrow-rs/pull/3447) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([askoa](https://github.com/askoa))
- Verify ArrayData::data\_type compatible in PrimitiveArray::from [\#3440](https://github.com/apache/arrow-rs/pull/3440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preserve DataType metadata in make\_builder [\#3438](https://github.com/apache/arrow-rs/pull/3438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Consolidate arrow ipc tests and increase coverage [\#3427](https://github.com/apache/arrow-rs/pull/3427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Generic bytes dictionary builder [\#3426](https://github.com/apache/arrow-rs/pull/3426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Minor: Improve docs for arrow-ipc, remove clippy ignore [\#3421](https://github.com/apache/arrow-rs/pull/3421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- refactor: convert `*like_dyn`, `*like_utf8_scalar_dyn` and  `*like_dict` functions to macros [\#3411](https://github.com/apache/arrow-rs/pull/3411) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Add parquet-index binary [\#3405](https://github.com/apache/arrow-rs/pull/3405) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Complete mid-level `FlightClient` [\#3402](https://github.com/apache/arrow-rs/pull/3402) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Implement `RecordBatch` \<--\> `FlightData` encode/decode + tests [\#3391](https://github.com/apache/arrow-rs/pull/3391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Provide `into_builder` for bytearray [\#3326](https://github.com/apache/arrow-rs/pull/3326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
