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

## [33.0.0](https://github.com/apache/arrow-rs/tree/33.0.0) (2023-02-09)

[Full Changelog](https://github.com/apache/arrow-rs/compare/32.0.0...33.0.0)

**Breaking changes:**

- Use ArrayFormatter in Cast Kernel [\#3668](https://github.com/apache/arrow-rs/pull/3668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use dyn Array in cast kernels [\#3667](https://github.com/apache/arrow-rs/pull/3667) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return references from FixedSizeListArray and MapArray [\#3652](https://github.com/apache/arrow-rs/pull/3652) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Lazy array display \(\#3638\) [\#3647](https://github.com/apache/arrow-rs/pull/3647) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use array\_value\_to\_string in arrow-csv [\#3514](https://github.com/apache/arrow-rs/pull/3514) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JayjeetAtGithub](https://github.com/JayjeetAtGithub))

**Implemented enhancements:**

- object\_store: support encoded path as input [\#3651](https://github.com/apache/arrow-rs/issues/3651)
- Add modulus\_dyn and modulus\_scalar\_dyn [\#3648](https://github.com/apache/arrow-rs/issues/3648)
- A trait for append\_value and append\_null on ArrayBuilders [\#3644](https://github.com/apache/arrow-rs/issues/3644)
- Improve error messge "batches\[0\] schema is different with argument schema" [\#3628](https://github.com/apache/arrow-rs/issues/3628)
- Specified version of helper function to cast binary to string [\#3623](https://github.com/apache/arrow-rs/issues/3623)
- Casting generic binary to generic string [\#3606](https://github.com/apache/arrow-rs/issues/3606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `array_value_to_string` in `arrow-csv`  [\#3483](https://github.com/apache/arrow-rs/issues/3483)

**Fixed bugs:**

- ArrowArray::try\_from\_raw Misleading Signature [\#3684](https://github.com/apache/arrow-rs/issues/3684)
- PyArrowConvert Leaks Memory [\#3683](https://github.com/apache/arrow-rs/issues/3683)
- FFI Fails to Account For Offsets [\#3671](https://github.com/apache/arrow-rs/issues/3671)
- Regression in CSV reader error handling [\#3656](https://github.com/apache/arrow-rs/issues/3656)
- UnionArray Child and Value Fail to Account for non-contiguous Type IDs [\#3653](https://github.com/apache/arrow-rs/issues/3653)
- Panic when accessing RecordBatch from pyarrow [\#3646](https://github.com/apache/arrow-rs/issues/3646)
- Multiplication for decimals is incorrect [\#3645](https://github.com/apache/arrow-rs/issues/3645)
- Inconsistent output between pretty print and CSV writer for Arrow [\#3513](https://github.com/apache/arrow-rs/issues/3513)

**Closed issues:**

- Release `32.0.0` of `arrow`/`arrow-flight`/`parquet`/`parquet-derive` \(next release after `31.0.0`\) [\#3584](https://github.com/apache/arrow-rs/issues/3584)

**Merged pull requests:**

- Cleanup FFI interface \(\#3684\) \(\#3683\) [\#3685](https://github.com/apache/arrow-rs/pull/3685) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix: take\_run benchmark parameter [\#3679](https://github.com/apache/arrow-rs/pull/3679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Minor: Add some examples to Date\*Array and Time\*Array [\#3678](https://github.com/apache/arrow-rs/pull/3678) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add ArrayData::new\_null and DataType::primitive\_width [\#3676](https://github.com/apache/arrow-rs/pull/3676) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix FFI which fails to account for offsets [\#3675](https://github.com/apache/arrow-rs/pull/3675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Date64Array docs [\#3670](https://github.com/apache/arrow-rs/pull/3670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.50 to =1.0.51 [\#3669](https://github.com/apache/arrow-rs/pull/3669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add timezone accessor for Timestamp\*Array [\#3666](https://github.com/apache/arrow-rs/pull/3666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster timezone cast [\#3665](https://github.com/apache/arrow-rs/pull/3665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement std::fmt::Write for StringBuilder \(\#3638\) [\#3659](https://github.com/apache/arrow-rs/pull/3659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Include line and field number in CSV UTF-8 error \(\#3656\) [\#3657](https://github.com/apache/arrow-rs/pull/3657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Handle non-contiguous type\_ids in UnionArray \(\#3653\) [\#3654](https://github.com/apache/arrow-rs/pull/3654) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add modulus\_dyn and modulus\_scalar\_dyn [\#3649](https://github.com/apache/arrow-rs/pull/3649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve error messge with detailed schema [\#3637](https://github.com/apache/arrow-rs/pull/3637) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Veeupup](https://github.com/Veeupup))
- Add limit to ArrowReaderBuilder to push limit down to parquet reader [\#3633](https://github.com/apache/arrow-rs/pull/3633) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- chore: delete wrong comment and refactor set\_metadata in `Field` [\#3630](https://github.com/apache/arrow-rs/pull/3630) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chunshao90](https://github.com/chunshao90))
- Fix typo in comment [\#3627](https://github.com/apache/arrow-rs/pull/3627) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kjschiroo](https://github.com/kjschiroo))
- Minor: Update doc strings about Page Index / Column Index [\#3625](https://github.com/apache/arrow-rs/pull/3625) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Specified version of helper function to cast binary to string [\#3624](https://github.com/apache/arrow-rs/pull/3624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat: take kernel for RunArray [\#3622](https://github.com/apache/arrow-rs/pull/3622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Remove BitSliceIterator specialization from try\_for\_each\_valid\_idx [\#3621](https://github.com/apache/arrow-rs/pull/3621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Reduce PrimitiveArray::try\_unary codegen [\#3619](https://github.com/apache/arrow-rs/pull/3619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Reduce Dictionary Builder Codegen [\#3616](https://github.com/apache/arrow-rs/pull/3616) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Add test for dictionary encoding of batches [\#3608](https://github.com/apache/arrow-rs/pull/3608) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Casting generic binary to generic string [\#3607](https://github.com/apache/arrow-rs/pull/3607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add ArrayAccessor, Iterator, Extend and benchmarks for RunArray [\#3603](https://github.com/apache/arrow-rs/pull/3603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
