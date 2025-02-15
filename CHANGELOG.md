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

## [54.2.0](https://github.com/apache/arrow-rs/tree/54.2.0) (2025-02-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/54.1.0...54.2.0)

**Implemented enhancements:**

- Casting from Utf8View to Dict\(k, Utf8View\) [\#7114](https://github.com/apache/arrow-rs/issues/7114)
- Support creating map arrays with key metadata [\#7100](https://github.com/apache/arrow-rs/issues/7100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\] Print Parquet BasicTypeInfo id when present [\#7081](https://github.com/apache/arrow-rs/issues/7081) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add arrow-ipc benchmarks for the IPC reader and writer [\#6968](https://github.com/apache/arrow-rs/issues/6968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- NullBufferBuilder::allocated_size Returns Size in Bits [\#7121](https://github.com/apache/arrow-rs/issues/7121) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Regression in 54.0.0\]. Decimal cast to smaller precision gives invalid \(off-by-one\) result in some cases [\#7069](https://github.com/apache/arrow-rs/issues/7069) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Minor: Fix deprecated note to point to the correct const [\#7067](https://github.com/apache/arrow-rs/issues/7067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- incorrect error message for reading definition levels [\#7056](https://github.com/apache/arrow-rs/issues/7056) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- First None in ListArray panics in `cast_with_options` [\#7043](https://github.com/apache/arrow-rs/issues/7043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Minor: Clarify documentation on `NullBufferBuilder::allocated_size` [\#7089](https://github.com/apache/arrow-rs/pull/7089) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Update release schedule [\#7086](https://github.com/apache/arrow-rs/pull/7086) ([alamb](https://github.com/alamb))
- Improve `ListArray` documentation for slices [\#7039](https://github.com/apache/arrow-rs/pull/7039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- fix: NullBufferBuilder::allocated_size should return Size in Bytes [\#7122](https://github.com/apache/arrow-rs/pull/7122) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([shuozel](https://github.com/shuozel))
- minor: fix deprecated_note [\#7105](https://github.com/apache/arrow-rs/pull/7105) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Chen-Yuan-Lai](https://github.com/Chen-Yuan-Lai))
- Minor: Fix ArrayDataBuilder::build_unchecked docs [\#7103](https://github.com/apache/arrow-rs/pull/7103) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Support setting key field in MapBuilder [\#7101](https://github.com/apache/arrow-rs/pull/7101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rshkv](https://github.com/rshkv))
- Add tests that arrow IPC data is validated [\#7096](https://github.com/apache/arrow-rs/pull/7096) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Print Parquet BasicTypeInfo id when present [\#7094](https://github.com/apache/arrow-rs/pull/7094) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([devinrsmith](https://github.com/devinrsmith))
- Expose record boundary information in JSON decoder [\#7092](https://github.com/apache/arrow-rs/pull/7092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([scovich](https://github.com/scovich))
- Benchmarks for Arrow IPC reader [\#7091](https://github.com/apache/arrow-rs/pull/7091) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Benchmarks for Arrow IPC writer [\#7090](https://github.com/apache/arrow-rs/pull/7090) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add another decimal cast edge test case [\#7078](https://github.com/apache/arrow-rs/pull/7078) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- minor: re-export `OffsetBufferBuilder` in `arrow` crate [\#7077](https://github.com/apache/arrow-rs/pull/7077) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Support converting large dates \(i.e. +10999-12-31\) from string to Date32 [\#7074](https://github.com/apache/arrow-rs/pull/7074) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phillipleblanc](https://github.com/phillipleblanc))
- fix: issue introduced in \#6833 - less than equal check for scale in decimal conversion [\#7070](https://github.com/apache/arrow-rs/pull/7070) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([himadripal](https://github.com/himadripal))
- perf: inline `from_iter` for `ScalarBuffer` [\#7066](https://github.com/apache/arrow-rs/pull/7066) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([0ax1](https://github.com/0ax1))
- fix: first none/empty list in `ListArray` panics in `cast_with_options` [\#7065](https://github.com/apache/arrow-rs/pull/7065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([irenjj](https://github.com/irenjj))
- Minor: add ticket reference for todo [\#7064](https://github.com/apache/arrow-rs/pull/7064) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Refactor some decimal-related code and tests [\#7062](https://github.com/apache/arrow-rs/pull/7062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([CurtHagenlocher](https://github.com/CurtHagenlocher))
- fix error message for reading definition levels [\#7057](https://github.com/apache/arrow-rs/pull/7057) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Update release schedule README.md [\#7053](https://github.com/apache/arrow-rs/pull/7053) ([alamb](https://github.com/alamb))
- Support both 0x01 and 0x02 as type for list of booleans in thrift metadata [\#7052](https://github.com/apache/arrow-rs/pull/7052) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- Refactor arrow-ipc: Move `create_*_array` methods into `RecordBatchDecoder` [\#7029](https://github.com/apache/arrow-rs/pull/7029) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Refactor arrow-ipc: Rename `ArrayReader` to `RecodeBatchDecoder` [\#7028](https://github.com/apache/arrow-rs/pull/7028) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Introduce `UnsafeFlag` to manage disabling `ArrayData` validation [\#7027](https://github.com/apache/arrow-rs/pull/7027) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

\* _This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)_
