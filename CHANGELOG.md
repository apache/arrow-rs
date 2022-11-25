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

## [28.0.0](https://github.com/apache/arrow-rs/tree/28.0.0) (2022-11-25)

[Full Changelog](https://github.com/apache/arrow-rs/compare/27.0.0...28.0.0)

**Breaking changes:**

- StructArray::columns return slice [\#3186](https://github.com/apache/arrow-rs/pull/3186) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return slice from GenericByteArray::value\_data [\#3171](https://github.com/apache/arrow-rs/pull/3171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support decimal negative scale [\#3152](https://github.com/apache/arrow-rs/pull/3152) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- refactor: convert `Field::metadata` to `HashMap` [\#3148](https://github.com/apache/arrow-rs/pull/3148) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Don't Skip Serializing Empty Metadata \(\#3082\) [\#3126](https://github.com/apache/arrow-rs/pull/3126) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Add Decimal128, Decimal256, Float16 to DataType::is\_numeric [\#3121](https://github.com/apache/arrow-rs/pull/3121) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Upgrade to thrift 0.17 and fix issues [\#3104](https://github.com/apache/arrow-rs/pull/3104) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Fix prettyprint for Interval second fractions [\#3093](https://github.com/apache/arrow-rs/pull/3093) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Remove Option from `Field::metadata` [\#3091](https://github.com/apache/arrow-rs/pull/3091) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))

**Implemented enhancements:**

- Add iterator to RowSelection [\#3172](https://github.com/apache/arrow-rs/issues/3172) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- create an integration test set for parquet crate against pyspark for working with bloom filters [\#3167](https://github.com/apache/arrow-rs/issues/3167)
- Row Format Size Tracking [\#3160](https://github.com/apache/arrow-rs/issues/3160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add ArrayBuilder::finish\_cloned\(\) [\#3154](https://github.com/apache/arrow-rs/issues/3154) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Optimize memory usage of json reader [\#3150](https://github.com/apache/arrow-rs/issues/3150)
- Add `Field::size` and `DataType::size` [\#3147](https://github.com/apache/arrow-rs/issues/3147) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add like\_utf8\_scalar\_dyn kernel [\#3145](https://github.com/apache/arrow-rs/issues/3145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support comparison for decimal128 array with scalar in kernel [\#3140](https://github.com/apache/arrow-rs/issues/3140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- audit and create a document for bloom filter configurations [\#3138](https://github.com/apache/arrow-rs/issues/3138) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Should be the rounding vs truncation when cast decimal to smaller scale  [\#3137](https://github.com/apache/arrow-rs/issues/3137)
- Upgrade chrono to 0.4.23 [\#3120](https://github.com/apache/arrow-rs/issues/3120)
- Implements more temporal kernels using time\_fraction\_dyn [\#3108](https://github.com/apache/arrow-rs/issues/3108) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Upgrade to thrift 0.17 [\#3105](https://github.com/apache/arrow-rs/issues/3105) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Be able to parse time formatted strings [\#3100](https://github.com/apache/arrow-rs/issues/3100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve "Fail to merge schema" error messages [\#3095](https://github.com/apache/arrow-rs/issues/3095) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose `SortingColumn` when reading and writing parquet metadata [\#3090](https://github.com/apache/arrow-rs/issues/3090) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Change Field::metadata to HashMap [\#3086](https://github.com/apache/arrow-rs/issues/3086) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support bloom filter reading and writing for parquet [\#3023](https://github.com/apache/arrow-rs/issues/3023) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- API to take back ownership of an ArrayRef [\#2901](https://github.com/apache/arrow-rs/issues/2901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Specialized Interleave Kernel [\#2864](https://github.com/apache/arrow-rs/issues/2864) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- arithmatic overflow leads to segfault in `concat_batches` [\#3123](https://github.com/apache/arrow-rs/issues/3123) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clippy failing on master : error: use of deprecated associated function chrono::NaiveDate::from\_ymd: use from\_ymd\_opt\(\) instead [\#3097](https://github.com/apache/arrow-rs/issues/3097) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pretty print for interval types has wrong formatting [\#3092](https://github.com/apache/arrow-rs/issues/3092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Field is not serializable with binary formats [\#3082](https://github.com/apache/arrow-rs/issues/3082) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Decimal Casts are Unchecked [\#2986](https://github.com/apache/arrow-rs/issues/2986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reading parquet files with a corrupt ARROW:schema panics [\#2855](https://github.com/apache/arrow-rs/issues/2855) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Closed issues:**

- Release Arrow `27.0.0` \(next release after `26.0.0`\) [\#3045](https://github.com/apache/arrow-rs/issues/3045) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Perf about ParquetRecordBatchStream vs ParquetRecordBatchReader [\#2916](https://github.com/apache/arrow-rs/issues/2916)

**Merged pull requests:**

- Derive clone for arrays [\#3184](https://github.com/apache/arrow-rs/pull/3184) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Row decode cleanups [\#3180](https://github.com/apache/arrow-rs/pull/3180) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update zstd requirement from 0.11.1 to 0.12.0 [\#3178](https://github.com/apache/arrow-rs/pull/3178) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Move decimal constants from `arrow-data` to `arrow-schema` crate [\#3177](https://github.com/apache/arrow-rs/pull/3177) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- bloom filter part V: add an integration with pytest against pyspark [\#3176](https://github.com/apache/arrow-rs/pull/3176) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Bloom filter config tweaks \(\#3023\) [\#3175](https://github.com/apache/arrow-rs/pull/3175) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add RowParser [\#3174](https://github.com/apache/arrow-rs/pull/3174) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `RowSelection::iter()`, `Into<Vec<RowSelector>>` and example [\#3173](https://github.com/apache/arrow-rs/pull/3173) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add read parquet examples [\#3170](https://github.com/apache/arrow-rs/pull/3170) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([xudong963](https://github.com/xudong963))
- Faster BinaryArray to StringArray conversion \(~67%\) [\#3168](https://github.com/apache/arrow-rs/pull/3168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove unnecessary downcasts in builders [\#3166](https://github.com/apache/arrow-rs/pull/3166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- bloom filter part IV: adjust writer properties, bloom filter properties, and incorporate into column encoder [\#3165](https://github.com/apache/arrow-rs/pull/3165) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Fix parquet decimal precision [\#3164](https://github.com/apache/arrow-rs/pull/3164) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Add Row size methods \(\#3160\) [\#3163](https://github.com/apache/arrow-rs/pull/3163) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Prevent precision=0 for decimal type [\#3162](https://github.com/apache/arrow-rs/pull/3162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Remove unnecessary Buffer::from\_slice\_ref reference [\#3161](https://github.com/apache/arrow-rs/pull/3161) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add finish\_cloned to ArrayBuilder [\#3158](https://github.com/apache/arrow-rs/pull/3158) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Check overflow in MutableArrayData extend offsets \(\#3123\) [\#3157](https://github.com/apache/arrow-rs/pull/3157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Extend Decimal256 as Primitive [\#3156](https://github.com/apache/arrow-rs/pull/3156) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Doc improvements [\#3155](https://github.com/apache/arrow-rs/pull/3155) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Add collect.rs example [\#3153](https://github.com/apache/arrow-rs/pull/3153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Implement Neg for i256 [\#3151](https://github.com/apache/arrow-rs/pull/3151) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: `{Field,DataType}::size` [\#3149](https://github.com/apache/arrow-rs/pull/3149) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add like\_utf8\_scalar\_dyn kernel [\#3146](https://github.com/apache/arrow-rs/pull/3146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- comparison op: decimal128 array with scalar [\#3141](https://github.com/apache/arrow-rs/pull/3141) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Cast: should get the round result for decimal to a decimal with smaller scale [\#3139](https://github.com/apache/arrow-rs/pull/3139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Fix Panic on Reading Corrupt Parquet Schema \(\#2855\) [\#3130](https://github.com/apache/arrow-rs/pull/3130) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Clippy parquet fixes [\#3124](https://github.com/apache/arrow-rs/pull/3124) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Add GenericByteBuilder \(\#2969\) [\#3122](https://github.com/apache/arrow-rs/pull/3122) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- parquet bloom filter part III: add sbbf writer, remove `bloom` default feature, add reader properties [\#3119](https://github.com/apache/arrow-rs/pull/3119) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Add downcast\_array \(\#2901\) [\#3117](https://github.com/apache/arrow-rs/pull/3117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add COW conversion for Buffer and PrimitiveArray and unary\_mut [\#3115](https://github.com/apache/arrow-rs/pull/3115) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Include field name in merge error message [\#3113](https://github.com/apache/arrow-rs/pull/3113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Add PrimitiveArray::unary\_opt [\#3110](https://github.com/apache/arrow-rs/pull/3110) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implements more temporal kernels using time\_fraction\_dyn [\#3107](https://github.com/apache/arrow-rs/pull/3107) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- cast: support unsigned numeric type to decimal128 [\#3106](https://github.com/apache/arrow-rs/pull/3106) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Expose `SortingColumn` in parquet files [\#3103](https://github.com/apache/arrow-rs/pull/3103) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([askoa](https://github.com/askoa))
- parquet bloom filter part II: read sbbf bitset from row group reader, update API, and add cli demo [\#3102](https://github.com/apache/arrow-rs/pull/3102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Parse Time32/Time64 from formatted string [\#3101](https://github.com/apache/arrow-rs/pull/3101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Cleanup temporal \_internal functions [\#3099](https://github.com/apache/arrow-rs/pull/3099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve schema mismatch error message [\#3098](https://github.com/apache/arrow-rs/pull/3098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Fix clippy by avoiding deprecated functions in chrono [\#3096](https://github.com/apache/arrow-rs/pull/3096) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Minor: Add diagrams and documentation to row format [\#3094](https://github.com/apache/arrow-rs/pull/3094) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Use ArrowNativeTypeOp instead of total\_cmp directly [\#3087](https://github.com/apache/arrow-rs/pull/3087) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Check overflow while casting between decimal types [\#3076](https://github.com/apache/arrow-rs/pull/3076) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- add bloom filter implementation based on split block \(sbbf\) spec [\#3057](https://github.com/apache/arrow-rs/pull/3057) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Add FixedSizeBinaryArray::try\_from\_sparse\_iter\_with\_size [\#3054](https://github.com/apache/arrow-rs/pull/3054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
