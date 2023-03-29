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

## [36.0.0](https://github.com/apache/arrow-rs/tree/36.0.0) (2023-03-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/35.0.0...36.0.0)

**Breaking changes:**

- Use dyn Array in sort kernels [\#3931](https://github.com/apache/arrow-rs/pull/3931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Enforce struct nullability in JSON raw reader \(\#3900\) \(\#3904\) [\#3906](https://github.com/apache/arrow-rs/pull/3906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return ScalarBuffer from PrimitiveArray::values \(\#3879\) [\#3896](https://github.com/apache/arrow-rs/pull/3896) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use BooleanBuffer in BooleanArray \(\#3879\) [\#3895](https://github.com/apache/arrow-rs/pull/3895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Seal ArrowPrimitiveType [\#3882](https://github.com/apache/arrow-rs/pull/3882) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support compression levels [\#3847](https://github.com/apache/arrow-rs/pull/3847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([spebern](https://github.com/spebern))

**Implemented enhancements:**

- Improve speed of parsing string to Times [\#3919](https://github.com/apache/arrow-rs/issues/3919) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- feat: add comparison/sort support for Float16 [\#3914](https://github.com/apache/arrow-rs/issues/3914)
- Pinned version in arrow-flight's build-dependencies are causing conflicts [\#3876](https://github.com/apache/arrow-rs/issues/3876)
- Add compression options \(levels\) [\#3844](https://github.com/apache/arrow-rs/issues/3844) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use Unsigned Integer for Fixed Size DataType [\#3815](https://github.com/apache/arrow-rs/issues/3815)
- Common trait for RecordBatch and StructArray [\#3764](https://github.com/apache/arrow-rs/issues/3764) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow precision loss on multiplying decimal arrays [\#3689](https://github.com/apache/arrow-rs/issues/3689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Raw JSON Reader Allows Non-Nullable Struct Children to Contain Nulls [\#3904](https://github.com/apache/arrow-rs/issues/3904)
- Nullable field with nested not nullable map in json [\#3900](https://github.com/apache/arrow-rs/issues/3900)
- parquet\_derive doesn't support Vec\<u8\> [\#3864](https://github.com/apache/arrow-rs/issues/3864) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[REGRESSION\] Parsing timestamps with lower case time separator [\#3863](https://github.com/apache/arrow-rs/issues/3863) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[REGRESSION\] Parsing timestamps with leap seconds [\#3861](https://github.com/apache/arrow-rs/issues/3861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[REGRESSION\] Parsing timestamps with fractional seconds / microseconds / milliseconds / nanoseconds [\#3859](https://github.com/apache/arrow-rs/issues/3859) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CSV Reader Doesn't set Timezone [\#3841](https://github.com/apache/arrow-rs/issues/3841)
- PyArrowConvert Leaks Memory [\#3683](https://github.com/apache/arrow-rs/issues/3683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Derive RunArray Clone [\#3932](https://github.com/apache/arrow-rs/pull/3932) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move protoc generation to binary crate, unpin prost/tonic build \(\#3876\) [\#3927](https://github.com/apache/arrow-rs/pull/3927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Fix JSON Temporal Encoding of Multiple Batches [\#3924](https://github.com/apache/arrow-rs/pull/3924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- Cleanup uses of Array::data\_ref \(\#3880\) [\#3918](https://github.com/apache/arrow-rs/pull/3918) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support microsecond and nanosecond in interval parsing [\#3916](https://github.com/apache/arrow-rs/pull/3916) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat: add comparison/sort support for Float16 [\#3915](https://github.com/apache/arrow-rs/pull/3915) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add AsArray trait for more ergonomic downcasting [\#3912](https://github.com/apache/arrow-rs/pull/3912) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add OffsetBuffer::new [\#3910](https://github.com/apache/arrow-rs/pull/3910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add PrimitiveArray::new \(\#3879\) [\#3909](https://github.com/apache/arrow-rs/pull/3909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support timezones in CSV reader \(\#3841\) [\#3908](https://github.com/apache/arrow-rs/pull/3908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve ScalarBuffer debug output [\#3907](https://github.com/apache/arrow-rs/pull/3907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.52 to =1.0.53 [\#3905](https://github.com/apache/arrow-rs/pull/3905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Re-export parquet compression level structs [\#3903](https://github.com/apache/arrow-rs/pull/3903) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix parsing timestamps of exactly 32 characters [\#3902](https://github.com/apache/arrow-rs/pull/3902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add iterators to BooleanBuffer and NullBuffer [\#3901](https://github.com/apache/arrow-rs/pull/3901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Array equality for &dyn Array \(\#3880\) [\#3899](https://github.com/apache/arrow-rs/pull/3899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BooleanArray::new \(\#3879\) [\#3898](https://github.com/apache/arrow-rs/pull/3898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Revert structured ArrayData \(\#3877\) [\#3894](https://github.com/apache/arrow-rs/pull/3894) ([tustvold](https://github.com/tustvold))
- Fix pyarrow memory leak \(\#3683\) [\#3893](https://github.com/apache/arrow-rs/pull/3893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: add examples for `ListBuilder` and `GenericListBuilder` [\#3891](https://github.com/apache/arrow-rs/pull/3891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update syn requirement from 1.0 to 2.0 [\#3890](https://github.com/apache/arrow-rs/pull/3890) ([dependabot[bot]](https://github.com/apps/dependabot))
- Use of `mul_checked` to avoid silent overflow in interval arithmetic [\#3886](https://github.com/apache/arrow-rs/pull/3886) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Flesh out NullBuffer abstraction \(\#3880\) [\#3885](https://github.com/apache/arrow-rs/pull/3885) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement Bit Operations for i256 [\#3884](https://github.com/apache/arrow-rs/pull/3884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Flatten arrow\_buffer [\#3883](https://github.com/apache/arrow-rs/pull/3883) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Array::to\_data and Array::nulls \(\#3880\) [\#3881](https://github.com/apache/arrow-rs/pull/3881) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Added support for byte vectors and slices to parquet\_derive \(\#3864\) [\#3878](https://github.com/apache/arrow-rs/pull/3878) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([waymost](https://github.com/waymost))
- chore: remove LevelDecoder [\#3872](https://github.com/apache/arrow-rs/pull/3872) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Weijun-H](https://github.com/Weijun-H))
- Parse timestamps with leap seconds \(\#3861\) [\#3862](https://github.com/apache/arrow-rs/pull/3862) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster time parsing \(~93% faster\) [\#3860](https://github.com/apache/arrow-rs/pull/3860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Parse timestamps with arbitrary seconds fraction [\#3858](https://github.com/apache/arrow-rs/pull/3858) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BitIterator [\#3856](https://github.com/apache/arrow-rs/pull/3856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve decimal parsing performance [\#3854](https://github.com/apache/arrow-rs/pull/3854) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Update proc-macro2 requirement from =1.0.51 to =1.0.52 [\#3853](https://github.com/apache/arrow-rs/pull/3853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update bitflags requirement from 1.2.1 to 2.0.0 [\#3852](https://github.com/apache/arrow-rs/pull/3852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add offset pushdown to parquet [\#3848](https://github.com/apache/arrow-rs/pull/3848) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add timezone support to JSON reader [\#3845](https://github.com/apache/arrow-rs/pull/3845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow precision loss on multiplying decimal arrays [\#3690](https://github.com/apache/arrow-rs/pull/3690) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
