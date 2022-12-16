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

# Historical Changelog

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
- create an integration test set for parquet crate against pyspark for working with bloom filters [\#3167](https://github.com/apache/arrow-rs/issues/3167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Row Format Size Tracking [\#3160](https://github.com/apache/arrow-rs/issues/3160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add ArrayBuilder::finish\_cloned\(\) [\#3154](https://github.com/apache/arrow-rs/issues/3154) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Optimize memory usage of json reader [\#3150](https://github.com/apache/arrow-rs/issues/3150)
- Add `Field::size` and `DataType::size` [\#3147](https://github.com/apache/arrow-rs/issues/3147) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add like\_utf8\_scalar\_dyn kernel [\#3145](https://github.com/apache/arrow-rs/issues/3145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support comparison for decimal128 array with scalar in kernel [\#3140](https://github.com/apache/arrow-rs/issues/3140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- audit and create a document for bloom filter configurations [\#3138](https://github.com/apache/arrow-rs/issues/3138) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Should be the rounding vs truncation when cast decimal to smaller scale  [\#3137](https://github.com/apache/arrow-rs/issues/3137) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
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

**Closed issues:**

- Release Arrow `27.0.0` \(next release after `26.0.0`\) [\#3045](https://github.com/apache/arrow-rs/issues/3045) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Perf about ParquetRecordBatchStream vs ParquetRecordBatchReader [\#2916](https://github.com/apache/arrow-rs/issues/2916)

**Merged pull requests:**

- Improve regex related kernels by upto 85% [\#3192](https://github.com/apache/arrow-rs/pull/3192) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
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
## [27.0.0](https://github.com/apache/arrow-rs/tree/27.0.0) (2022-11-11)

[Full Changelog](https://github.com/apache/arrow-rs/compare/26.0.0...27.0.0)

**Breaking changes:**

- Recurse into Dictionary value type in DataType::is\_nested [\#3083](https://github.com/apache/arrow-rs/pull/3083) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- early type checks in `RowConverter` [\#3080](https://github.com/apache/arrow-rs/pull/3080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add Decimal128 and Decimal256 to downcast\_primitive [\#3056](https://github.com/apache/arrow-rs/pull/3056) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace remaining \_generic temporal kernels with \_dyn kernels [\#3046](https://github.com/apache/arrow-rs/pull/3046) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace year\_generic with year\_dyn [\#3041](https://github.com/apache/arrow-rs/pull/3041) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Validate decimal256 with i256 directly [\#3025](https://github.com/apache/arrow-rs/pull/3025) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Hadoop LZ4 Support for LZ4 Codec [\#3013](https://github.com/apache/arrow-rs/pull/3013) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
- Replace hour\_generic with hour\_dyn [\#3006](https://github.com/apache/arrow-rs/pull/3006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Accept any &dyn Array in nullif kernel [\#2940](https://github.com/apache/arrow-rs/pull/2940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Row Format: Option to detach/own a row [\#3078](https://github.com/apache/arrow-rs/issues/3078) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Row Format: API to check if datatypes are supported [\#3077](https://github.com/apache/arrow-rs/issues/3077) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Deprecate Buffer::count\_set\_bits [\#3067](https://github.com/apache/arrow-rs/issues/3067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Decimal128 and Decimal256 to downcast\_primitive [\#3055](https://github.com/apache/arrow-rs/issues/3055) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improved UX of  creating `TimestampNanosecondArray` with timezones [\#3042](https://github.com/apache/arrow-rs/issues/3042) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast decimal256 to signed integer [\#3039](https://github.com/apache/arrow-rs/issues/3039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting Date64 to Timestamp [\#3037](https://github.com/apache/arrow-rs/issues/3037) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Check overflow when casting floating point value to decimal256 [\#3032](https://github.com/apache/arrow-rs/issues/3032) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare i256 in validate\_decimal256\_precision [\#3024](https://github.com/apache/arrow-rs/issues/3024) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Check overflow when casting floating point value to decimal128 [\#3020](https://github.com/apache/arrow-rs/issues/3020) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add macro downcast\_temporal\_array [\#3008](https://github.com/apache/arrow-rs/issues/3008) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace hour\_generic with hour\_dyn [\#3005](https://github.com/apache/arrow-rs/issues/3005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace temporal \_generic kernels with dyn [\#3004](https://github.com/apache/arrow-rs/issues/3004) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `RowSelection::intersection` [\#3003](https://github.com/apache/arrow-rs/issues/3003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- I would like to round rather than truncate when casting f64 to decimal [\#2997](https://github.com/apache/arrow-rs/issues/2997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow::compute::kernels::temporal should support nanoseconds [\#2995](https://github.com/apache/arrow-rs/issues/2995) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release Arrow `26.0.0` \(next release after `25.0.0`\) [\#2953](https://github.com/apache/arrow-rs/issues/2953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add timezone offset for debug format of Timestamp with Timezone [\#2917](https://github.com/apache/arrow-rs/issues/2917) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support merge RowSelectors when creating RowSelection [\#2858](https://github.com/apache/arrow-rs/issues/2858) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Inconsistent Nan Handling Between Scalar and Non-Scalar Comparison Kernels [\#3074](https://github.com/apache/arrow-rs/issues/3074) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Debug format for timestamp ignores timezone [\#3069](https://github.com/apache/arrow-rs/issues/3069) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Row format decode loses timezone [\#3063](https://github.com/apache/arrow-rs/issues/3063) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- binary operator produces incorrect result on arrays with resized null buffer [\#3061](https://github.com/apache/arrow-rs/issues/3061) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RLEDecoder Panics on Null Padded Pages [\#3035](https://github.com/apache/arrow-rs/issues/3035) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nullif with incorrect valid\_count [\#3031](https://github.com/apache/arrow-rs/issues/3031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RLEDecoder::get\_batch\_with\_dict may panic on bit-packed runs longer than 1024 [\#3029](https://github.com/apache/arrow-rs/issues/3029) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Converted type is None according to Parquet Tools then utilizing logical types [\#3017](https://github.com/apache/arrow-rs/issues/3017)
- CompressionCodec LZ4 incompatible with C++ implementation [\#2988](https://github.com/apache/arrow-rs/issues/2988) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Mark parquet predicate pushdown as complete [\#2987](https://github.com/apache/arrow-rs/pull/2987) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Merged pull requests:**

- Improved UX of  creating `TimestampNanosecondArray` with timezones [\#3088](https://github.com/apache/arrow-rs/pull/3088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([src255](https://github.com/src255))
- Remove unused range module [\#3085](https://github.com/apache/arrow-rs/pull/3085) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make intersect\_row\_selections a member function [\#3084](https://github.com/apache/arrow-rs/pull/3084) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update hashbrown requirement from 0.12 to 0.13 [\#3081](https://github.com/apache/arrow-rs/pull/3081) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: add `OwnedRow` [\#3079](https://github.com/apache/arrow-rs/pull/3079) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Use ArrowNativeTypeOp on non-scalar comparison kernels [\#3075](https://github.com/apache/arrow-rs/pull/3075) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add missing inline to ArrowNativeTypeOp [\#3073](https://github.com/apache/arrow-rs/pull/3073) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix debug information for Timestamp with Timezone  [\#3072](https://github.com/apache/arrow-rs/pull/3072) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Deprecate Buffer::count\_set\_bits \(\#3067\) [\#3071](https://github.com/apache/arrow-rs/pull/3071) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add compare to ArrowNativeTypeOp [\#3070](https://github.com/apache/arrow-rs/pull/3070) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Improve docstrings on WriterPropertiesBuilder [\#3068](https://github.com/apache/arrow-rs/pull/3068) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Faster f64 inequality [\#3065](https://github.com/apache/arrow-rs/pull/3065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix row format decode loses timezone \(\#3063\) [\#3064](https://github.com/apache/arrow-rs/pull/3064) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix null\_count computation in binary [\#3062](https://github.com/apache/arrow-rs/pull/3062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Faster f64 equality [\#3060](https://github.com/apache/arrow-rs/pull/3060) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update arrow-flight subcrates \(\#3044\) [\#3052](https://github.com/apache/arrow-rs/pull/3052) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Minor: Remove cloning ArrayData in with\_precision\_and\_scale [\#3050](https://github.com/apache/arrow-rs/pull/3050) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split out arrow-json \(\#3044\) [\#3049](https://github.com/apache/arrow-rs/pull/3049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move `intersect_row_selections` from datafusion to arrow-rs. [\#3047](https://github.com/apache/arrow-rs/pull/3047) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Split out arrow-csv \(\#2594\) [\#3044](https://github.com/apache/arrow-rs/pull/3044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move reader\_parser to arrow-cast \(\#3022\) [\#3043](https://github.com/apache/arrow-rs/pull/3043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cast decimal256 to signed integer [\#3040](https://github.com/apache/arrow-rs/pull/3040) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Enable casting from Date64 to Timestamp [\#3038](https://github.com/apache/arrow-rs/pull/3038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Fix decoding long and/or padded RLE data \(\#3029\) \(\#3035\) [\#3036](https://github.com/apache/arrow-rs/pull/3036) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix nullif when existing array has no nulls [\#3034](https://github.com/apache/arrow-rs/pull/3034) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Check overflow when casting floating point value to decimal256 [\#3033](https://github.com/apache/arrow-rs/pull/3033) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update parquet to depend on arrow subcrates [\#3028](https://github.com/apache/arrow-rs/pull/3028) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make various i256 methods const [\#3026](https://github.com/apache/arrow-rs/pull/3026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-ipc [\#3022](https://github.com/apache/arrow-rs/pull/3022) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Check overflow while casting floating point value to decimal128 [\#3021](https://github.com/apache/arrow-rs/pull/3021) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update arrow-flight [\#3019](https://github.com/apache/arrow-rs/pull/3019) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Move ArrowNativeTypeOp to arrow-array \(\#2594\) [\#3018](https://github.com/apache/arrow-rs/pull/3018) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support cast timestamp to time [\#3016](https://github.com/apache/arrow-rs/pull/3016) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([naosense](https://github.com/naosense))
- Add filter example [\#3014](https://github.com/apache/arrow-rs/pull/3014) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Check overflow when casting integer to decimal [\#3009](https://github.com/apache/arrow-rs/pull/3009) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add macro downcast\_temporal\_array [\#3007](https://github.com/apache/arrow-rs/pull/3007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Parquet Writer: Make column descriptor public on the writer [\#3002](https://github.com/apache/arrow-rs/pull/3002) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([pier-oliviert](https://github.com/pier-oliviert))
- Update chrono-tz requirement from 0.7 to 0.8 [\#3001](https://github.com/apache/arrow-rs/pull/3001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Round instead of Truncate while casting float to decimal [\#3000](https://github.com/apache/arrow-rs/pull/3000) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Support Predicate Pushdown for Parquet Lists \(\#2108\) [\#2999](https://github.com/apache/arrow-rs/pull/2999) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out arrow-cast \(\#2594\) [\#2998](https://github.com/apache/arrow-rs/pull/2998) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- `arrow::compute::kernels::temporal` should support nanoseconds  [\#2996](https://github.com/apache/arrow-rs/pull/2996) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Add `RowSelection::from_selectors_and_combine` to  merge RowSelectors  [\#2994](https://github.com/apache/arrow-rs/pull/2994) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Simplify Single-Column Dictionary Sort [\#2993](https://github.com/apache/arrow-rs/pull/2993) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Add entry to changelog for 26.0.0 RC2 fix [\#2992](https://github.com/apache/arrow-rs/pull/2992) ([alamb](https://github.com/alamb))
- Fix ignored limit on `lexsort_to_indices` [\#2991](https://github.com/apache/arrow-rs/pull/2991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add clone and equal functions for CastOptions [\#2985](https://github.com/apache/arrow-rs/pull/2985) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- minor: remove redundant prefix [\#2983](https://github.com/apache/arrow-rs/pull/2983) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([jackwener](https://github.com/jackwener))
- Compare dictionary decimal arrays [\#2982](https://github.com/apache/arrow-rs/pull/2982) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Compare dictionary and non-dictionary decimal arrays [\#2980](https://github.com/apache/arrow-rs/pull/2980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add decimal comparison kernel support [\#2978](https://github.com/apache/arrow-rs/pull/2978) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Move concat kernel to arrow-select \(\#2594\) [\#2976](https://github.com/apache/arrow-rs/pull/2976) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Specialize interleave for byte arrays \(\#2864\) [\#2975](https://github.com/apache/arrow-rs/pull/2975) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use unary function for numeric to decimal cast [\#2973](https://github.com/apache/arrow-rs/pull/2973) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Specialize filter kernel for binary arrays \(\#2969\) [\#2971](https://github.com/apache/arrow-rs/pull/2971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Combine take\_utf8 and take\_binary \(\#2969\) [\#2970](https://github.com/apache/arrow-rs/pull/2970) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster Scalar Dictionary Comparison ~10% [\#2968](https://github.com/apache/arrow-rs/pull/2968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move `byte_size` from datafusion::physical\_expr [\#2965](https://github.com/apache/arrow-rs/pull/2965) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([avantgardnerio](https://github.com/avantgardnerio))
- Pass decompressed size to parquet Codec::decompress \(\#2956\) [\#2959](https://github.com/apache/arrow-rs/pull/2959) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
- Add Decimal Arithmetic [\#2881](https://github.com/apache/arrow-rs/pull/2881) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [26.0.0](https://github.com/apache/arrow-rs/tree/26.0.0) (2022-10-28)

[Full Changelog](https://github.com/apache/arrow-rs/compare/25.0.0...26.0.0)

**Breaking changes:**

- Cast Timestamps to RFC3339 strings [\#2934](https://github.com/apache/arrow-rs/issues/2934)
- Remove Unused NativeDecimalType [\#2945](https://github.com/apache/arrow-rs/pull/2945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Format Timestamps as RFC3339 [\#2939](https://github.com/apache/arrow-rs/pull/2939) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Update flatbuffers to resolve RUSTSEC-2021-0122 [\#2895](https://github.com/apache/arrow-rs/pull/2895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- replace `from_timestamp` by `from_timestamp_opt` [\#2894](https://github.com/apache/arrow-rs/pull/2894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))

**Implemented enhancements:**

- Optimized way to count the numbers of `true` and `false` values in a BooleanArray [\#2963](https://github.com/apache/arrow-rs/issues/2963) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add pow to i256 [\#2954](https://github.com/apache/arrow-rs/issues/2954) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Generic Code over \[Large\]BinaryArray and \[Large\]StringArray [\#2946](https://github.com/apache/arrow-rs/issues/2946) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Page Row Count Limit [\#2941](https://github.com/apache/arrow-rs/issues/2941) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- prettyprint to show timezone offset for timestamp with timezone [\#2937](https://github.com/apache/arrow-rs/issues/2937) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast numeric to decimal256 [\#2922](https://github.com/apache/arrow-rs/issues/2922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `freeze_with_dictionary` API to `MutableArrayData` [\#2914](https://github.com/apache/arrow-rs/issues/2914) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support decimal256 array in sort kernels [\#2911](https://github.com/apache/arrow-rs/issues/2911) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support `[+/-]hhmm` and `[+/-]hh` as fixedoffset timezone format [\#2910](https://github.com/apache/arrow-rs/issues/2910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cleanup decimal sort function [\#2907](https://github.com/apache/arrow-rs/issues/2907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- replace `from_timestamp` by `from_timestamp_opt` [\#2892](https://github.com/apache/arrow-rs/issues/2892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Move Primitive arity kernels to arrow-array [\#2787](https://github.com/apache/arrow-rs/issues/2787) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- add overflow-checking for negative arithmetic kernel [\#2662](https://github.com/apache/arrow-rs/issues/2662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Subtle compatibility issue with serve\_arrow [\#2952](https://github.com/apache/arrow-rs/issues/2952)
- error\[E0599\]: no method named `total_cmp` found for struct `f16` in the current scope [\#2926](https://github.com/apache/arrow-rs/issues/2926) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fail at rowSelection `and_then` method [\#2925](https://github.com/apache/arrow-rs/issues/2925) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Ordering not implemented for FixedSizeBinary types [\#2904](https://github.com/apache/arrow-rs/issues/2904) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet API: Could not convert timestamp before unix epoch to string/json [\#2897](https://github.com/apache/arrow-rs/issues/2897) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Overly Pessimistic RLE Size Estimation [\#2889](https://github.com/apache/arrow-rs/issues/2889) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Memory alignment error in `RawPtrBox::new` [\#2882](https://github.com/apache/arrow-rs/issues/2882) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compilation error under chrono-tz feature [\#2878](https://github.com/apache/arrow-rs/issues/2878) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- AHash Statically Allocates 64 bytes [\#2875](https://github.com/apache/arrow-rs/issues/2875) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `parquet::arrow::arrow_writer::ArrowWriter` ignores page size properties [\#2853](https://github.com/apache/arrow-rs/issues/2853) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Document crate topology \(\#2594\) [\#2913](https://github.com/apache/arrow-rs/pull/2913) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- SerializedFileWriter comments about multiple call on consumed self [\#2935](https://github.com/apache/arrow-rs/issues/2935) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Pointer freed error when deallocating ArrayData with shared memory buffer [\#2874](https://github.com/apache/arrow-rs/issues/2874)
- Release Arrow `25.0.0` \(next release after `24.0.0`\) [\#2820](https://github.com/apache/arrow-rs/issues/2820) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Replace DecimalArray with PrimitiveArray [\#2637](https://github.com/apache/arrow-rs/issues/2637) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix ignored limit on lexsort\_to\_indices (#2991) [\#2991](https://github.com/apache/arrow-rs/pull/2991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix GenericListArray::try\_new\_from\_array\_data error message \(\#526\) [\#2961](https://github.com/apache/arrow-rs/pull/2961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix take string on sliced indices [\#2960](https://github.com/apache/arrow-rs/pull/2960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BooleanArray::true\_count and BooleanArray::false\_count [\#2957](https://github.com/apache/arrow-rs/pull/2957) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add pow to i256 [\#2955](https://github.com/apache/arrow-rs/pull/2955) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix datatype for timestamptz debug fmt [\#2948](https://github.com/apache/arrow-rs/pull/2948) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Add GenericByteArray \(\#2946\) [\#2947](https://github.com/apache/arrow-rs/pull/2947) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Specialize interleave string ~2-3x faster [\#2944](https://github.com/apache/arrow-rs/pull/2944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Added support for LZ4\_RAW compression. \(\#1604\) [\#2943](https://github.com/apache/arrow-rs/pull/2943) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
- Add optional page row count limit for parquet `WriterProperties` \(\#2941\) [\#2942](https://github.com/apache/arrow-rs/pull/2942) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cleanup orphaned doc comments \(\#2935\) [\#2938](https://github.com/apache/arrow-rs/pull/2938) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- support more fixedoffset tz format [\#2936](https://github.com/apache/arrow-rs/pull/2936) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Benchmark with prepared row converter [\#2930](https://github.com/apache/arrow-rs/pull/2930) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add lexsort benchmark \(\#2871\) [\#2929](https://github.com/apache/arrow-rs/pull/2929) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve panic messages for RowSelection::and\_then \(\#2925\) [\#2928](https://github.com/apache/arrow-rs/pull/2928) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update required half from 2.0 --\> 2.1 [\#2927](https://github.com/apache/arrow-rs/pull/2927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Cast numeric to decimal256 [\#2923](https://github.com/apache/arrow-rs/pull/2923) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cleanup generated proto code [\#2921](https://github.com/apache/arrow-rs/pull/2921) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Deprecate TimestampArray from\_vec and from\_opt\_vec [\#2919](https://github.com/apache/arrow-rs/pull/2919) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support decimal256 array in sort kernels [\#2912](https://github.com/apache/arrow-rs/pull/2912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add timezone abstraction [\#2909](https://github.com/apache/arrow-rs/pull/2909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup decimal sort function [\#2908](https://github.com/apache/arrow-rs/pull/2908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify TimestampArray from\_vec with timezone [\#2906](https://github.com/apache/arrow-rs/pull/2906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement ord for FixedSizeBinary types [\#2905](https://github.com/apache/arrow-rs/pull/2905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
- Update chrono-tz requirement from 0.6 to 0.7 [\#2903](https://github.com/apache/arrow-rs/pull/2903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Parquet record api support timestamp before epoch [\#2899](https://github.com/apache/arrow-rs/pull/2899) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AnthonyPoncet](https://github.com/AnthonyPoncet))
- Specialize interleave integer [\#2898](https://github.com/apache/arrow-rs/pull/2898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support overflow-checking variant of negate kernel [\#2893](https://github.com/apache/arrow-rs/pull/2893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Respect Page Size Limits in ArrowWriter \(\#2853\) [\#2890](https://github.com/apache/arrow-rs/pull/2890) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Improve row format docs [\#2888](https://github.com/apache/arrow-rs/pull/2888) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add FixedSizeList::from\_iter\_primitive [\#2887](https://github.com/apache/arrow-rs/pull/2887) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify ListArray::from\_iter\_primitive [\#2886](https://github.com/apache/arrow-rs/pull/2886) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out value selection kernels into arrow-select \(\#2594\) [\#2885](https://github.com/apache/arrow-rs/pull/2885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Increase default IPC alignment to 64 \(\#2883\) [\#2884](https://github.com/apache/arrow-rs/pull/2884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Copying inappropriately aligned buffer in ipc reader [\#2883](https://github.com/apache/arrow-rs/pull/2883) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Validate decimal IPC read \(\#2387\) [\#2880](https://github.com/apache/arrow-rs/pull/2880) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix compilation error under `chrono-tz` feature [\#2879](https://github.com/apache/arrow-rs/pull/2879) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Don't validate decimal precision in ArrayData \(\#2637\) [\#2873](https://github.com/apache/arrow-rs/pull/2873) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add downcast\_integer and downcast\_primitive [\#2872](https://github.com/apache/arrow-rs/pull/2872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Filter DecimalArray as PrimitiveArray ~5x Faster \(\#2637\) [\#2870](https://github.com/apache/arrow-rs/pull/2870) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Treat DecimalArray as PrimitiveArray in row format [\#2866](https://github.com/apache/arrow-rs/pull/2866) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [25.0.0](https://github.com/apache/arrow-rs/tree/25.0.0) (2022-10-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/24.0.0...25.0.0)

**Breaking changes:**

- Make DecimalArray as PrimitiveArray [\#2857](https://github.com/apache/arrow-rs/pull/2857) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix timestamp parsing while no explicit timezone given [\#2814](https://github.com/apache/arrow-rs/pull/2814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Support Arbitrary Number of Arrays in downcast\_primitive\_array [\#2809](https://github.com/apache/arrow-rs/pull/2809) ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Restore Integration test JSON schema serialization  [\#2876](https://github.com/apache/arrow-rs/issues/2876) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix various invalid\_html\_tags clippy error [\#2861](https://github.com/apache/arrow-rs/issues/2861) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Replace complicated temporal macro with generic functions [\#2851](https://github.com/apache/arrow-rs/issues/2851) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add NaN handling in dyn scalar comparison kernels [\#2829](https://github.com/apache/arrow-rs/issues/2829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variant of sum kernel [\#2821](https://github.com/apache/arrow-rs/issues/2821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update to Clap 4 [\#2817](https://github.com/apache/arrow-rs/issues/2817) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Safe API to Operate on Dictionary Values [\#2797](https://github.com/apache/arrow-rs/issues/2797) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add modulus op into `ArrowNativeTypeOp` [\#2753](https://github.com/apache/arrow-rs/issues/2753) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow creating of TimeUnit instances without direct dependency on parquet-format [\#2708](https://github.com/apache/arrow-rs/issues/2708) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Arrow Row Format [\#2677](https://github.com/apache/arrow-rs/issues/2677) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Don't try to infer nulls in CSV schema inference [\#2859](https://github.com/apache/arrow-rs/issues/2859) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `parquet::arrow::arrow_writer::ArrowWriter` ignores page size properties [\#2853](https://github.com/apache/arrow-rs/issues/2853) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Introducing ArrowNativeTypeOp made it impossible to call kernels from generics [\#2839](https://github.com/apache/arrow-rs/issues/2839) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unsound ArrayData to Array Conversions [\#2834](https://github.com/apache/arrow-rs/issues/2834) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Regression: `the trait bound for<'de> arrow::datatypes::Schema: serde::de::Deserialize<'de> is not satisfied` [\#2825](https://github.com/apache/arrow-rs/issues/2825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- convert string to timestamp shouldn't apply local timezone offset if there's no explicit timezone info in the string [\#2813](https://github.com/apache/arrow-rs/issues/2813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add pub api for checking column index is sorted [\#2848](https://github.com/apache/arrow-rs/issues/2848) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Take decimal as primitive \(\#2637\) [\#2869](https://github.com/apache/arrow-rs/pull/2869) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-integration-test crate [\#2868](https://github.com/apache/arrow-rs/pull/2868) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Decimal cleanup \(\#2637\) [\#2865](https://github.com/apache/arrow-rs/pull/2865) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix various invalid\_html\_tags clippy errors [\#2862](https://github.com/apache/arrow-rs/pull/2862) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Don't try to infer nullability in CSV reader [\#2860](https://github.com/apache/arrow-rs/pull/2860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Fix page size on dictionary fallback [\#2854](https://github.com/apache/arrow-rs/pull/2854) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Replace complicated temporal macro with generic functions [\#2850](https://github.com/apache/arrow-rs/pull/2850) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- \[feat\] Add pub api for checking column index is sorted. [\#2849](https://github.com/apache/arrow-rs/pull/2849) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- parquet: Add `snap` option to README [\#2847](https://github.com/apache/arrow-rs/pull/2847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([exyi](https://github.com/exyi))
- Cleanup cast kernel [\#2846](https://github.com/apache/arrow-rs/pull/2846) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify ArrowNativeType [\#2841](https://github.com/apache/arrow-rs/pull/2841) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Expose ArrowNativeTypeOp trait to make it useful for type bound [\#2840](https://github.com/apache/arrow-rs/pull/2840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `interleave` kernel \(\#1523\) [\#2838](https://github.com/apache/arrow-rs/pull/2838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Handle empty offsets buffer \(\#1824\) [\#2836](https://github.com/apache/arrow-rs/pull/2836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Validate ArrayData type when converting to Array \(\#2834\) [\#2835](https://github.com/apache/arrow-rs/pull/2835) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Derive ArrowPrimitiveType for Decimal128Type and Decimal256Type \(\#2637\) [\#2833](https://github.com/apache/arrow-rs/pull/2833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add NaN handling in dyn scalar comparison kernels [\#2830](https://github.com/apache/arrow-rs/pull/2830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify OrderPreservingInterner allocation strategy ~97% faster \(\#2677\) [\#2827](https://github.com/apache/arrow-rs/pull/2827) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Convert rows to arrays \(\#2677\) [\#2826](https://github.com/apache/arrow-rs/pull/2826) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add overflow-checking variant of sum kernel [\#2822](https://github.com/apache/arrow-rs/pull/2822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update Clap dependency to version 4 [\#2819](https://github.com/apache/arrow-rs/pull/2819) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jgoday](https://github.com/jgoday))
- Fix i256 checked multiplication [\#2818](https://github.com/apache/arrow-rs/pull/2818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add string\_dictionary benches for row format \(\#2677\) [\#2816](https://github.com/apache/arrow-rs/pull/2816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add OrderPreservingInterner::lookup \(\#2677\) [\#2815](https://github.com/apache/arrow-rs/pull/2815) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify FixedLengthEncoding [\#2812](https://github.com/apache/arrow-rs/pull/2812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement ArrowNumericType for Float16Type [\#2810](https://github.com/apache/arrow-rs/pull/2810) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add DictionaryArray::with\_values to make it easier to operate on dictionary values [\#2798](https://github.com/apache/arrow-rs/pull/2798) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add i256 \(\#2637\) [\#2781](https://github.com/apache/arrow-rs/pull/2781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add modulus ops into `ArrowNativeTypeOp` [\#2756](https://github.com/apache/arrow-rs/pull/2756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- feat: cast List / LargeList to Utf8 / LargeUtf8 [\#2588](https://github.com/apache/arrow-rs/pull/2588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gandronchik](https://github.com/gandronchik))

## [24.0.0](https://github.com/apache/arrow-rs/tree/24.0.0) (2022-09-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/23.0.0...24.0.0)

**Breaking changes:**

- Cleanup `ArrowNativeType` \(\#1918\) [\#2793](https://github.com/apache/arrow-rs/pull/2793) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `ArrowNativeType::FromStr` [\#2775](https://github.com/apache/arrow-rs/pull/2775) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out `arrow-array`  crate \(\#2594\) [\#2769](https://github.com/apache/arrow-rs/pull/2769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `dyn_arith_dict` feature flag [\#2760](https://github.com/apache/arrow-rs/pull/2760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out `arrow-data` into a separate crate [\#2746](https://github.com/apache/arrow-rs/pull/2746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-schema \(\#2594\) [\#2711](https://github.com/apache/arrow-rs/pull/2711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Include field name in Parquet PrimitiveTypeBuilder error messages [\#2804](https://github.com/apache/arrow-rs/issues/2804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add PrimitiveArray::reinterpret\_cast [\#2785](https://github.com/apache/arrow-rs/issues/2785)
- BinaryBuilder and StringBuilder initialization parameters in struct\_builder may be wrong [\#2783](https://github.com/apache/arrow-rs/issues/2783) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide scalar dyn kernel which produces null for division by zero [\#2767](https://github.com/apache/arrow-rs/issues/2767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide dyn kernel which produces null for division by zero [\#2763](https://github.com/apache/arrow-rs/issues/2763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of checked kernels on non-null data [\#2747](https://github.com/apache/arrow-rs/issues/2747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variants of arithmetic dyn kernels [\#2739](https://github.com/apache/arrow-rs/issues/2739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The `binary` function should not panic on unequaled array length. [\#2721](https://github.com/apache/arrow-rs/issues/2721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- min compute kernel is incorrect with sliced buffers in arrow 23 [\#2779](https://github.com/apache/arrow-rs/issues/2779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `try_unary_dict` should check value type of dictionary array [\#2754](https://github.com/apache/arrow-rs/issues/2754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add back JSON import/export for schema [\#2762](https://github.com/apache/arrow-rs/issues/2762)
- null casting and coercion for Decimal128  [\#2761](https://github.com/apache/arrow-rs/issues/2761)
- Json decoder behavior changed from versions 21 to 21 and returns non-sensical num\_rows for RecordBatch [\#2722](https://github.com/apache/arrow-rs/issues/2722) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release Arrow `23.0.0` \(next release after `22.0.0`\) [\#2665](https://github.com/apache/arrow-rs/issues/2665) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Merged pull requests:**

- add field name to parquet PrimitiveTypeBuilder error messages [\#2805](https://github.com/apache/arrow-rs/pull/2805) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([andygrove](https://github.com/andygrove))
- Add struct equality test case \(\#514\) [\#2791](https://github.com/apache/arrow-rs/pull/2791) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move unary kernels to arrow-array \(\#2787\) [\#2789](https://github.com/apache/arrow-rs/pull/2789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Disable test harness for string\_dictionary\_builder benchmark [\#2788](https://github.com/apache/arrow-rs/pull/2788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add PrimitiveArray::reinterpret\_cast \(\#2785\) [\#2786](https://github.com/apache/arrow-rs/pull/2786) ([tustvold](https://github.com/tustvold))
- Fix BinaryBuilder and StringBuilder Capacity Allocation in StructBuilder [\#2784](https://github.com/apache/arrow-rs/pull/2784) ([chunshao90](https://github.com/chunshao90))
- Fix min/max computation for sliced arrays \(\#2779\) [\#2780](https://github.com/apache/arrow-rs/pull/2780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix Backwards Compatible Parquet List Encodings \(\#1915\) [\#2774](https://github.com/apache/arrow-rs/pull/2774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- MINOR: Fix clippy for rust 1.64.0 [\#2772](https://github.com/apache/arrow-rs/pull/2772) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- MINOR: Fix clippy for rust 1.64.0 [\#2771](https://github.com/apache/arrow-rs/pull/2771) ([viirya](https://github.com/viirya))
- Add divide scalar dyn kernel which produces null for division by zero [\#2768](https://github.com/apache/arrow-rs/pull/2768) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add divide dyn kernel which produces null for division by zero [\#2764](https://github.com/apache/arrow-rs/pull/2764) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add value type check in try\_unary\_dict [\#2755](https://github.com/apache/arrow-rs/pull/2755) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix `verify_release_candidate.sh` for new arrow subcrates [\#2752](https://github.com/apache/arrow-rs/pull/2752) ([alamb](https://github.com/alamb))
- Fix: Issue 2721 : binary function should not panic but return error w… [\#2750](https://github.com/apache/arrow-rs/pull/2750) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([aksharau](https://github.com/aksharau))
- Speed up checked kernels for non-null data \(~1.4-5x faster\) [\#2749](https://github.com/apache/arrow-rs/pull/2749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Add overflow-checking variants of arithmetic dyn kernels [\#2740](https://github.com/apache/arrow-rs/pull/2740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Trim parquet row selection [\#2705](https://github.com/apache/arrow-rs/pull/2705) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

## [23.0.0](https://github.com/apache/arrow-rs/tree/24.0.0) (2022-09-16)

[Full Changelog](https://github.com/apache/arrow-rs/compare/22.0.0...23.0.0)

**Breaking changes:**

- Move JSON Test Format To integration-testing [\#2724](https://github.com/apache/arrow-rs/pull/2724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-buffer crate \(\#2594\) [\#2693](https://github.com/apache/arrow-rs/pull/2693) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify DictionaryBuilder constructors \(\#2684\) \(\#2054\) [\#2685](https://github.com/apache/arrow-rs/pull/2685) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate RecordBatch::concat replace with concat\_batches \(\#2594\) [\#2683](https://github.com/apache/arrow-rs/pull/2683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add overflow-checking variant for primitive arithmetic kernels and explicitly define overflow behavior [\#2643](https://github.com/apache/arrow-rs/pull/2643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update thrift v0.16 and vendor parquet-format \(\#2502\) [\#2626](https://github.com/apache/arrow-rs/pull/2626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update flight definitions including backwards-incompatible change to GetSchema [\#2586](https://github.com/apache/arrow-rs/pull/2586) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([liukun4515](https://github.com/liukun4515))

**Implemented enhancements:**

- Cleanup like and nlike utf8 kernels [\#2744](https://github.com/apache/arrow-rs/issues/2744) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speedup eq and neq kernels for utf8 arrays [\#2742](https://github.com/apache/arrow-rs/issues/2742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- API for more ergonomic construction of `RecordBatchOptions` [\#2728](https://github.com/apache/arrow-rs/issues/2728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Automate updates to `CHANGELOG-old.md` [\#2726](https://github.com/apache/arrow-rs/issues/2726)
- Don't check the `DivideByZero` error for float modulus [\#2720](https://github.com/apache/arrow-rs/issues/2720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `try_binary` should not panic on unequaled array length. [\#2715](https://github.com/apache/arrow-rs/issues/2715) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add benchmark for bitwise operation [\#2714](https://github.com/apache/arrow-rs/issues/2714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variants of arithmetic scalar dyn kernels [\#2712](https://github.com/apache/arrow-rs/issues/2712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide\_opt kernel which produce null values on division by zero error [\#2709](https://github.com/apache/arrow-rs/issues/2709) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `DataType` function to detect nested types [\#2704](https://github.com/apache/arrow-rs/issues/2704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support of sorting dictionary of other primitive types [\#2700](https://github.com/apache/arrow-rs/issues/2700) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sort indices of dictionary string values [\#2697](https://github.com/apache/arrow-rs/issues/2697) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support empty projection in `RecordBatch::project` [\#2690](https://github.com/apache/arrow-rs/issues/2690) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support sorting dictionary encoded primitive integer arrays [\#2679](https://github.com/apache/arrow-rs/issues/2679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use BitIndexIterator in min\_max\_helper [\#2674](https://github.com/apache/arrow-rs/issues/2674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support building comparator for dictionaries of primitive integer values [\#2672](https://github.com/apache/arrow-rs/issues/2672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change max/min string macro to generic helper function `min_max_helper` [\#2657](https://github.com/apache/arrow-rs/issues/2657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variant of arithmetic scalar kernels [\#2651](https://github.com/apache/arrow-rs/issues/2651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with binary array [\#2644](https://github.com/apache/arrow-rs/issues/2644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variant for primitive arithmetic kernels [\#2642](https://github.com/apache/arrow-rs/issues/2642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `downcast_primitive_array` in arithmetic kernels [\#2639](https://github.com/apache/arrow-rs/issues/2639) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DictionaryArray in temporal kernels [\#2622](https://github.com/apache/arrow-rs/issues/2622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Inline Generated Thift Code Into Parquet Crate [\#2502](https://github.com/apache/arrow-rs/issues/2502) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Escape contains patterns for utf8 like kernels [\#2745](https://github.com/apache/arrow-rs/issues/2745) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Float Array should not panic on `DivideByZero` in the `Divide` kernel [\#2719](https://github.com/apache/arrow-rs/issues/2719) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DictionaryBuilders can Create Invalid DictionaryArrays [\#2684](https://github.com/apache/arrow-rs/issues/2684) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow` crate does not build with `features = ["ffi"]` and `default_features = false`. [\#2670](https://github.com/apache/arrow-rs/issues/2670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Invalid results with `RowSelector` having `row_count` of 0 [\#2669](https://github.com/apache/arrow-rs/issues/2669) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- clippy error: unresolved import `crate::array::layout` [\#2659](https://github.com/apache/arrow-rs/issues/2659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast the numeric without the `CastOptions` [\#2648](https://github.com/apache/arrow-rs/issues/2648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Explicitly define overflow behavior for primitive arithmetic kernels [\#2641](https://github.com/apache/arrow-rs/issues/2641) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- update the `flight.proto` and fix schema to SchemaResult [\#2571](https://github.com/apache/arrow-rs/issues/2571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Panic when first data page is skipped using ColumnChunkData::Sparse [\#2543](https://github.com/apache/arrow-rs/issues/2543) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `SchemaResult` in IPC deviates from other implementations [\#2445](https://github.com/apache/arrow-rs/issues/2445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Closed issues:**

- Implement collect for int values [\#2696](https://github.com/apache/arrow-rs/issues/2696) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Speedup string equal/not equal to empty string, cleanup like/ilike kernels, fix escape bug [\#2743](https://github.com/apache/arrow-rs/pull/2743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Partially flatten arrow-buffer [\#2737](https://github.com/apache/arrow-rs/pull/2737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Automate updates to `CHANGELOG-old.md` [\#2732](https://github.com/apache/arrow-rs/pull/2732) ([iajoiner](https://github.com/iajoiner))
- Update read parquet example in parquet/arrow home [\#2730](https://github.com/apache/arrow-rs/pull/2730) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([datapythonista](https://github.com/datapythonista))
- Better construction of RecordBatchOptions [\#2729](https://github.com/apache/arrow-rs/pull/2729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- benchmark: bitwise operation [\#2718](https://github.com/apache/arrow-rs/pull/2718) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Update `try_binary` and `checked_ops`, and remove `math_checked_op` [\#2717](https://github.com/apache/arrow-rs/pull/2717) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support bitwise op in kernel: or,xor,not [\#2716](https://github.com/apache/arrow-rs/pull/2716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Add overflow-checking variants of arithmetic scalar dyn kernels [\#2713](https://github.com/apache/arrow-rs/pull/2713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add divide\_opt kernel which produce null values on division by zero error [\#2710](https://github.com/apache/arrow-rs/pull/2710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add DataType::is\_nested\(\) [\#2707](https://github.com/apache/arrow-rs/pull/2707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kfastov](https://github.com/kfastov))
- Update criterion requirement from 0.3 to 0.4 [\#2706](https://github.com/apache/arrow-rs/pull/2706) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support bitwise and operation in the kernel [\#2703](https://github.com/apache/arrow-rs/pull/2703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Add support of sorting dictionary of other primitive arrays [\#2701](https://github.com/apache/arrow-rs/pull/2701) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Clarify docs of binary and string builders [\#2699](https://github.com/apache/arrow-rs/pull/2699) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([datapythonista](https://github.com/datapythonista))
- Sort indices of dictionary string values [\#2698](https://github.com/apache/arrow-rs/pull/2698) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add support for empty projection in RecordBatch::project [\#2691](https://github.com/apache/arrow-rs/pull/2691) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Temporarily disable Golang integration tests re-enable JS [\#2689](https://github.com/apache/arrow-rs/pull/2689) ([tustvold](https://github.com/tustvold))
- Verify valid UTF-8 when converting byte array \(\#2205\) [\#2686](https://github.com/apache/arrow-rs/pull/2686) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support sorting dictionary encoded primitive integer arrays [\#2680](https://github.com/apache/arrow-rs/pull/2680) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Skip RowSelectors with zero rows [\#2678](https://github.com/apache/arrow-rs/pull/2678) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([askoa](https://github.com/askoa))
- Faster Null Path Selection in ArrayData Equality [\#2676](https://github.com/apache/arrow-rs/pull/2676) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dhruv9vats](https://github.com/dhruv9vats))
- Use BitIndexIterator in min\_max\_helper [\#2675](https://github.com/apache/arrow-rs/pull/2675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support building comparator for dictionaries of primitive integer values [\#2673](https://github.com/apache/arrow-rs/pull/2673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- json feature always requires base64 feature [\#2668](https://github.com/apache/arrow-rs/pull/2668) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([eagletmt](https://github.com/eagletmt))
- Add try\_unary, binary, try\_binary kernels ~90% faster [\#2666](https://github.com/apache/arrow-rs/pull/2666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use downcast\_dictionary\_array in unary\_dyn [\#2663](https://github.com/apache/arrow-rs/pull/2663) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- optimize the `numeric_cast_with_error` [\#2661](https://github.com/apache/arrow-rs/pull/2661) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- ffi feature also requires layout [\#2660](https://github.com/apache/arrow-rs/pull/2660) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Change max/min string macro to generic helper function min\_max\_helper [\#2658](https://github.com/apache/arrow-rs/pull/2658) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix flaky test `test_fuzz_async_reader_selection` [\#2656](https://github.com/apache/arrow-rs/pull/2656) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- MINOR: Ignore flaky test test\_fuzz\_async\_reader\_selection [\#2655](https://github.com/apache/arrow-rs/pull/2655) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- MutableBuffer::typed\_data - shared ref access to the typed slice [\#2652](https://github.com/apache/arrow-rs/pull/2652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([medwards](https://github.com/medwards))
- Overflow-checking variant of arithmetic scalar kernels [\#2650](https://github.com/apache/arrow-rs/pull/2650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- support `CastOption` for casting numeric [\#2649](https://github.com/apache/arrow-rs/pull/2649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Help LLVM vectorize comparison kernel ~50-80% faster [\#2646](https://github.com/apache/arrow-rs/pull/2646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support comparison between dictionary array and binary array [\#2645](https://github.com/apache/arrow-rs/pull/2645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use `downcast_primitive_array` in arithmetic kernels [\#2640](https://github.com/apache/arrow-rs/pull/2640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fully qualifying parquet items [\#2638](https://github.com/apache/arrow-rs/pull/2638) ([dingxiangfei2009](https://github.com/dingxiangfei2009))
- Support DictionaryArray in temporal kernels [\#2623](https://github.com/apache/arrow-rs/pull/2623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Comparable Row Format [\#2593](https://github.com/apache/arrow-rs/pull/2593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix bug in page skipping [\#2552](https://github.com/apache/arrow-rs/pull/2552) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))

## [22.0.0](https://github.com/apache/arrow-rs/tree/22.0.0) (2022-09-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/21.0.0...22.0.0)

**Breaking changes:**

- Use `total_cmp` for floating value ordering and remove `nan_ordering` feature flag [\#2614](https://github.com/apache/arrow-rs/pull/2614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Gate dyn comparison of dictionary arrays behind `dyn_cmp_dict` [\#2597](https://github.com/apache/arrow-rs/pull/2597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move JsonSerializable to json module \(\#2300\) [\#2595](https://github.com/apache/arrow-rs/pull/2595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Decimal precision scale datatype change [\#2532](https://github.com/apache/arrow-rs/pull/2532) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor PrimitiveBuilder Constructors [\#2518](https://github.com/apache/arrow-rs/pull/2518) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactoring DecimalBuilder constructors [\#2517](https://github.com/apache/arrow-rs/pull/2517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor FixedSizeBinaryBuilder Constructors [\#2516](https://github.com/apache/arrow-rs/pull/2516) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor BooleanBuilder Constructors [\#2515](https://github.com/apache/arrow-rs/pull/2515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor UnionBuilder Constructors [\#2488](https://github.com/apache/arrow-rs/pull/2488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))

**Implemented enhancements:**

- Add  Macros to assist with static dispatch [\#2635](https://github.com/apache/arrow-rs/issues/2635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support comparison between DictionaryArray and BooleanArray [\#2617](https://github.com/apache/arrow-rs/issues/2617) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `total_cmp` for floating value ordering and remove `nan_ordering` feature flag [\#2613](https://github.com/apache/arrow-rs/issues/2613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support empty projection in CSV, JSON readers [\#2603](https://github.com/apache/arrow-rs/issues/2603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support SQL-compliant NaN ordering between for DictionaryArray and non-DictionaryArray [\#2599](https://github.com/apache/arrow-rs/issues/2599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `dyn_cmp_dict` feature flag to gate dyn comparison of dictionary arrays [\#2596](https://github.com/apache/arrow-rs/issues/2596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add max\_dyn and min\_dyn for max/min for dictionary array [\#2584](https://github.com/apache/arrow-rs/issues/2584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow FlightSQL implementers to extend `do_get()` [\#2581](https://github.com/apache/arrow-rs/issues/2581) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support SQL-compliant behavior on `eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#2569](https://github.com/apache/arrow-rs/issues/2569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add sql-compliant feature for enabling sql-compliant kernel behavior [\#2568](https://github.com/apache/arrow-rs/issues/2568)
- Calculate `sum` for dictionary array [\#2565](https://github.com/apache/arrow-rs/issues/2565) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add test for float nan comparison [\#2556](https://github.com/apache/arrow-rs/issues/2556) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with string array [\#2548](https://github.com/apache/arrow-rs/issues/2548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with primitive array in `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#2538](https://github.com/apache/arrow-rs/issues/2538) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with primitive array in `eq_dyn` and `neq_dyn` [\#2535](https://github.com/apache/arrow-rs/issues/2535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- UnionBuilder Create Children With Capacity [\#2523](https://github.com/apache/arrow-rs/issues/2523) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up `like_utf8_scalar` for `%pat%` [\#2519](https://github.com/apache/arrow-rs/issues/2519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace macro with TypedDictionaryArray in comparison kernels [\#2513](https://github.com/apache/arrow-rs/issues/2513) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use same codebase for boolean kernels [\#2507](https://github.com/apache/arrow-rs/issues/2507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use u8 for Decimal Precision and Scale [\#2496](https://github.com/apache/arrow-rs/issues/2496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Integrate skip row without pageIndex in SerializedPageReader in Fuzz Test [\#2475](https://github.com/apache/arrow-rs/issues/2475) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Avoid unecessary copies in Arrow IPC reader [\#2437](https://github.com/apache/arrow-rs/issues/2437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add GenericColumnReader::skip\_records Missing OffsetIndex Fallback [\#2433](https://github.com/apache/arrow-rs/issues/2433) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Reading PageIndex with ParquetRecordBatchStream [\#2430](https://github.com/apache/arrow-rs/issues/2430) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Specialize FixedLenByteArrayReader for Parquet [\#2318](https://github.com/apache/arrow-rs/issues/2318) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make JSON support Optional via Feature Flag [\#2300](https://github.com/apache/arrow-rs/issues/2300) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Casting timestamp array to string should not ignore timezone [\#2607](https://github.com/apache/arrow-rs/issues/2607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Ilike\_ut8\_scalar kernals have incorrect logic [\#2544](https://github.com/apache/arrow-rs/issues/2544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Always validate the array data when creating array in IPC reader [\#2541](https://github.com/apache/arrow-rs/issues/2541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Int96Converter Truncates Timestamps [\#2480](https://github.com/apache/arrow-rs/issues/2480) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error Reading Page Index When Not Available  [\#2434](https://github.com/apache/arrow-rs/issues/2434) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ParquetFileArrowReader::get_record_reader[_by_colum]` `batch_size` overallocates [\#2321](https://github.com/apache/arrow-rs/issues/2321) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Document All Arrow Features in docs.rs [\#2633](https://github.com/apache/arrow-rs/issues/2633) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add support for CAST from `Interval(DayTime)` to `Timestamp(Nanosecond, None)` [\#2606](https://github.com/apache/arrow-rs/issues/2606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Why do we check for null in TypedDictionaryArray value function [\#2564](https://github.com/apache/arrow-rs/issues/2564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add the `length` field for `Buffer` [\#2524](https://github.com/apache/arrow-rs/issues/2524) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Avoid large over allocate buffer in async reader [\#2512](https://github.com/apache/arrow-rs/issues/2512) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rewriting Decimal Builders using `const_generic`. [\#2390](https://github.com/apache/arrow-rs/issues/2390) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rewrite Decimal Array using `const_generic` [\#2384](https://github.com/apache/arrow-rs/issues/2384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Add downcast macros \(\#2635\) [\#2636](https://github.com/apache/arrow-rs/pull/2636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Document all arrow features in docs.rs \(\#2633\) [\#2634](https://github.com/apache/arrow-rs/pull/2634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Document dyn\_cmp\_dict [\#2624](https://github.com/apache/arrow-rs/pull/2624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support comparison between DictionaryArray and BooleanArray [\#2618](https://github.com/apache/arrow-rs/pull/2618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cast timestamp array to string array with timezone [\#2608](https://github.com/apache/arrow-rs/pull/2608) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support empty projection in CSV and JSON readers [\#2604](https://github.com/apache/arrow-rs/pull/2604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make JSON support optional via a feature flag \(\#2300\) [\#2601](https://github.com/apache/arrow-rs/pull/2601) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support SQL-compliant NaN ordering for DictionaryArray and non-DictionaryArray [\#2600](https://github.com/apache/arrow-rs/pull/2600) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split out integration test plumbing \(\#2594\) \(\#2300\) [\#2598](https://github.com/apache/arrow-rs/pull/2598) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Refactor Binary Builder and String Builder Constructors [\#2592](https://github.com/apache/arrow-rs/pull/2592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Dictionary like scalar kernels [\#2591](https://github.com/apache/arrow-rs/pull/2591) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Validate dictionary key in TypedDictionaryArray \(\#2578\) [\#2589](https://github.com/apache/arrow-rs/pull/2589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add max\_dyn and min\_dyn for max/min for dictionary array [\#2585](https://github.com/apache/arrow-rs/pull/2585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Code cleanup of array value functions [\#2583](https://github.com/apache/arrow-rs/pull/2583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Allow overriding of do\_get & export useful macro [\#2582](https://github.com/apache/arrow-rs/pull/2582) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- MINOR: Upgrade to pyo3 0.17 [\#2576](https://github.com/apache/arrow-rs/pull/2576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Support SQL-compliant NaN behavior on eq\_dyn, neq\_dyn, lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn [\#2570](https://github.com/apache/arrow-rs/pull/2570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add sum\_dyn to calculate sum for dictionary array [\#2566](https://github.com/apache/arrow-rs/pull/2566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- struct UnionBuilder will create child buffers with capacity [\#2560](https://github.com/apache/arrow-rs/pull/2560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kastolars](https://github.com/kastolars))
- Don't panic on RleValueEncoder::flush\_buffer if empty \(\#2558\) [\#2559](https://github.com/apache/arrow-rs/pull/2559) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add the `length` field for Buffer and use more `Buffer` in IPC reader to avoid memory copy. [\#2557](https://github.com/apache/arrow-rs/pull/2557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([HaoYang670](https://github.com/HaoYang670))
- Add test for float nan comparison [\#2555](https://github.com/apache/arrow-rs/pull/2555) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Compare dictionary array with string array [\#2549](https://github.com/apache/arrow-rs/pull/2549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Always validate the array data \(except the `Decimal`\) when creating array in IPC reader [\#2547](https://github.com/apache/arrow-rs/pull/2547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- MINOR: Fix test\_row\_type\_validation test [\#2546](https://github.com/apache/arrow-rs/pull/2546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix ilike\_utf8\_scalar kernals [\#2545](https://github.com/apache/arrow-rs/pull/2545) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- fix typo [\#2540](https://github.com/apache/arrow-rs/pull/2540) ([00Masato](https://github.com/00Masato))
- Compare dictionary array and primitive array in lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn kernels [\#2539](https://github.com/apache/arrow-rs/pull/2539) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- \[MINOR\]Avoid large over allocate buffer in async reader [\#2537](https://github.com/apache/arrow-rs/pull/2537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Compare dictionary with primitive array in `eq_dyn` and `neq_dyn` [\#2533](https://github.com/apache/arrow-rs/pull/2533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add iterator for FixedSizeBinaryArray [\#2531](https://github.com/apache/arrow-rs/pull/2531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- add bench: decimal with byte array and fixed length byte array [\#2529](https://github.com/apache/arrow-rs/pull/2529) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Add FixedLengthByteArrayReader Remove ComplexObjectArrayReader [\#2528](https://github.com/apache/arrow-rs/pull/2528) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out byte array decoders \(\#2318\) [\#2527](https://github.com/apache/arrow-rs/pull/2527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use offset index in ParquetRecordBatchStream [\#2526](https://github.com/apache/arrow-rs/pull/2526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Clean the `create_array` in IPC reader. [\#2525](https://github.com/apache/arrow-rs/pull/2525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove DecimalByteArrayConvert \(\#2480\) [\#2522](https://github.com/apache/arrow-rs/pull/2522) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Improve performance of `%pat%` \(\>3x speedup\) [\#2521](https://github.com/apache/arrow-rs/pull/2521) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- remove len field from MapBuilder [\#2520](https://github.com/apache/arrow-rs/pull/2520) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
-  Replace macro with TypedDictionaryArray in comparison kernels [\#2514](https://github.com/apache/arrow-rs/pull/2514) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Avoid large over allocate buffer in sync reader [\#2511](https://github.com/apache/arrow-rs/pull/2511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Avoid useless memory copies in IPC reader. [\#2510](https://github.com/apache/arrow-rs/pull/2510) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor boolean kernels to use same codebase [\#2508](https://github.com/apache/arrow-rs/pull/2508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove Int96Converter \(\#2480\) [\#2481](https://github.com/apache/arrow-rs/pull/2481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

## [21.0.0](https://github.com/apache/arrow-rs/tree/21.0.0) (2022-08-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/20.0.0...21.0.0)

**Breaking changes:**

- Return structured `ColumnCloseResult` \(\#2465\) [\#2466](https://github.com/apache/arrow-rs/pull/2466) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Push `ChunkReader` into `SerializedPageReader` \(\#2463\) [\#2464](https://github.com/apache/arrow-rs/pull/2464) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Revise FromIterator for Decimal128Array to use Into instead of Borrow [\#2442](https://github.com/apache/arrow-rs/pull/2442) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use Fixed-Length Array in BasicDecimal new and raw\_value [\#2405](https://github.com/apache/arrow-rs/pull/2405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove deprecated ParquetWriter [\#2380](https://github.com/apache/arrow-rs/pull/2380) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove deprecated SliceableCursor and InMemoryWriteableCursor [\#2378](https://github.com/apache/arrow-rs/pull/2378) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- add into\_inner method to ArrowWriter [\#2491](https://github.com/apache/arrow-rs/issues/2491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove byteorder dependency [\#2472](https://github.com/apache/arrow-rs/issues/2472) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Return Structured ColumnCloseResult from GenericColumnWriter::close [\#2465](https://github.com/apache/arrow-rs/issues/2465) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Push `ChunkReader` into `SerializedPageReader` [\#2463](https://github.com/apache/arrow-rs/issues/2463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support SerializedPageReader::skip\_page without OffsetIndex [\#2459](https://github.com/apache/arrow-rs/issues/2459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Time64/Time32 comparison [\#2457](https://github.com/apache/arrow-rs/issues/2457) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Revise FromIterator for Decimal128Array to use Into instead of Borrow [\#2441](https://github.com/apache/arrow-rs/issues/2441) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `RowFilter` within`ParquetRecordBatchReader` [\#2431](https://github.com/apache/arrow-rs/issues/2431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove the field `StructBuilder::len` [\#2429](https://github.com/apache/arrow-rs/issues/2429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Standardize creation and configuration of parquet --\> Arrow readers \( `ParquetRecordBatchReaderBuilder`\) [\#2427](https://github.com/apache/arrow-rs/issues/2427) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use `OffsetIndex` to Prune IO in `ParquetRecordBatchStream` [\#2426](https://github.com/apache/arrow-rs/issues/2426) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `peek_next_page` and `skip_next_page` in `InMemoryPageReader` [\#2406](https://github.com/apache/arrow-rs/issues/2406) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from `Utf8`/`LargeUtf8` to `Binary`/`LargeBinary` [\#2402](https://github.com/apache/arrow-rs/issues/2402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting between `Decimal128` and `Decimal256` arrays [\#2375](https://github.com/apache/arrow-rs/issues/2375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Combine multiple selections into the same batch size in `skip_records` [\#2358](https://github.com/apache/arrow-rs/issues/2358) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add API to change timezone for timestamp array [\#2346](https://github.com/apache/arrow-rs/issues/2346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change the output of `read_buffer` Arrow IPC API to return `Result<_>` [\#2342](https://github.com/apache/arrow-rs/issues/2342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow `skip_records` in `GenericColumnReader` to skip across row groups [\#2331](https://github.com/apache/arrow-rs/issues/2331) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize the validation of `Decimal256` [\#2320](https://github.com/apache/arrow-rs/issues/2320) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement Skip for `DeltaBitPackDecoder` [\#2281](https://github.com/apache/arrow-rs/issues/2281) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Changes to `ParquetRecordBatchStream` to support row filtering in DataFusion [\#2270](https://github.com/apache/arrow-rs/issues/2270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `ArrayReader::skip_records` API [\#2197](https://github.com/apache/arrow-rs/issues/2197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Panic in SerializedPageReader without offset index [\#2503](https://github.com/apache/arrow-rs/issues/2503) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- MapArray columns don't handle null values correctly [\#2484](https://github.com/apache/arrow-rs/issues/2484) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- There is no compiler error when using an invalid Decimal type. [\#2440](https://github.com/apache/arrow-rs/issues/2440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Flight SQL Server sends incorrect response for `DoPutUpdateResult` [\#2403](https://github.com/apache/arrow-rs/issues/2403) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- `AsyncFileReader`No Longer Object-Safe [\#2372](https://github.com/apache/arrow-rs/issues/2372) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StructBuilder Does not Verify Child Lengths [\#2252](https://github.com/apache/arrow-rs/issues/2252) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Combine `DecimalArray` validation [\#2447](https://github.com/apache/arrow-rs/issues/2447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix bug in page skipping [\#2504](https://github.com/apache/arrow-rs/pull/2504) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Fix `MapArrayReader` \(\#2484\) \(\#1699\) \(\#1561\) [\#2500](https://github.com/apache/arrow-rs/pull/2500) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add API to Retrieve Finished Writer from Parquet Writer [\#2498](https://github.com/apache/arrow-rs/pull/2498) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jiacai2050](https://github.com/jiacai2050))
- Derive Copy,Clone for BasicDecimal [\#2495](https://github.com/apache/arrow-rs/pull/2495) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- remove byteorder dependency from parquet [\#2486](https://github.com/apache/arrow-rs/pull/2486) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- parquet-read: add support to read parquet data from stdin [\#2482](https://github.com/apache/arrow-rs/pull/2482) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nvartolomei](https://github.com/nvartolomei))
- Remove Position trait \(\#1163\) [\#2479](https://github.com/apache/arrow-rs/pull/2479) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add ChunkReader::get\_bytes [\#2478](https://github.com/apache/arrow-rs/pull/2478) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- RFC: Simplify decimal \(\#2440\) [\#2477](https://github.com/apache/arrow-rs/pull/2477) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Parquet OffsetIndex to prune IO with RowSelection [\#2473](https://github.com/apache/arrow-rs/pull/2473) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Remove unnecessary Option from Int96 [\#2471](https://github.com/apache/arrow-rs/pull/2471) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- remove len field from StructBuilder [\#2468](https://github.com/apache/arrow-rs/pull/2468) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Make Parquet reader filter APIs public \(\#1792\) [\#2467](https://github.com/apache/arrow-rs/pull/2467) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- enable ipc compression feature for integration test [\#2462](https://github.com/apache/arrow-rs/pull/2462) ([liukun4515](https://github.com/liukun4515))
- Simplify implementation of Schema [\#2461](https://github.com/apache/arrow-rs/pull/2461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support skip\_page missing OffsetIndex Fallback in SerializedPageReader [\#2460](https://github.com/apache/arrow-rs/pull/2460) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- support time32/time64 comparison [\#2458](https://github.com/apache/arrow-rs/pull/2458) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Utf8array casting [\#2456](https://github.com/apache/arrow-rs/pull/2456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Remove outdated license text [\#2455](https://github.com/apache/arrow-rs/pull/2455) ([alamb](https://github.com/alamb))
- Support RowFilter within ParquetRecordBatchReader \(\#2431\) [\#2452](https://github.com/apache/arrow-rs/pull/2452) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- benchmark: decimal builder and vec to decimal array [\#2450](https://github.com/apache/arrow-rs/pull/2450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Collocate Decimal Array Validation Logic [\#2446](https://github.com/apache/arrow-rs/pull/2446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Minor: Move From trait for Decimal256 impl to decimal.rs [\#2443](https://github.com/apache/arrow-rs/pull/2443) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- decimal benchmark: arrow reader decimal from parquet int32 and int64 [\#2438](https://github.com/apache/arrow-rs/pull/2438) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- MINOR: Simplify `split_second` function [\#2436](https://github.com/apache/arrow-rs/pull/2436) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add ParquetRecordBatchReaderBuilder \(\#2427\) [\#2435](https://github.com/apache/arrow-rs/pull/2435) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: refine validation for decimal128 array [\#2428](https://github.com/apache/arrow-rs/pull/2428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Benchmark of casting decimal arrays [\#2424](https://github.com/apache/arrow-rs/pull/2424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Test non-annotated repeated fields \(\#2394\) [\#2422](https://github.com/apache/arrow-rs/pull/2422) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix \#2416 Automatic version updates for github actions with dependabot [\#2417](https://github.com/apache/arrow-rs/pull/2417) ([iemejia](https://github.com/iemejia))
- Add validation logic for StructBuilder::finish [\#2413](https://github.com/apache/arrow-rs/pull/2413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- test: add test for reading decimal value from primitive array reader [\#2411](https://github.com/apache/arrow-rs/pull/2411) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Upgrade ahash to 0.8 [\#2410](https://github.com/apache/arrow-rs/pull/2410) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Support peek\_next\_page and skip\_next\_page in InMemoryPageReader [\#2407](https://github.com/apache/arrow-rs/pull/2407) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Fix DoPutUpdateResult [\#2404](https://github.com/apache/arrow-rs/pull/2404) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Implement Skip for DeltaBitPackDecoder [\#2393](https://github.com/apache/arrow-rs/pull/2393) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- fix: Don't instantiate the scalar composition code quadratically for dictionaries [\#2391](https://github.com/apache/arrow-rs/pull/2391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Marwes](https://github.com/Marwes))
- MINOR: Remove unused trait and some cleanup [\#2389](https://github.com/apache/arrow-rs/pull/2389) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Decouple parquet fuzz tests from converter \(\#1661\) [\#2386](https://github.com/apache/arrow-rs/pull/2386) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rewrite `Decimal` and `DecimalArray` using `const_generic` [\#2383](https://github.com/apache/arrow-rs/pull/2383) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Simplify BitReader \(~5-10% faster\) [\#2381](https://github.com/apache/arrow-rs/pull/2381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix parquet clippy lints \(\#1254\) [\#2377](https://github.com/apache/arrow-rs/pull/2377) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cast between `Decimal128` and `Decimal256` arrays [\#2376](https://github.com/apache/arrow-rs/pull/2376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- support compression for IPC with revamped feature flags [\#2369](https://github.com/apache/arrow-rs/pull/2369) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement AsyncFileReader for `Box<dyn AsyncFileReader>` [\#2368](https://github.com/apache/arrow-rs/pull/2368) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove get\_byte\_ranges where bound [\#2366](https://github.com/apache/arrow-rs/pull/2366) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: Make read\_num\_bytes a function instead of a macro [\#2364](https://github.com/apache/arrow-rs/pull/2364) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- refactor: Group metrics into page and column metrics structs [\#2363](https://github.com/apache/arrow-rs/pull/2363) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- Speed up `Decimal256` validation based on bytes comparison and add benchmark test [\#2360](https://github.com/apache/arrow-rs/pull/2360) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Combine multiple selections into the same batch size in skip\_records [\#2359](https://github.com/apache/arrow-rs/pull/2359) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add API to change timezone for timestamp array [\#2347](https://github.com/apache/arrow-rs/pull/2347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Clean the code in `field.rs` and add more tests [\#2345](https://github.com/apache/arrow-rs/pull/2345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add Parquet RowFilter API [\#2335](https://github.com/apache/arrow-rs/pull/2335) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make skip\_records in complex\_object\_array can skip cross row groups [\#2332](https://github.com/apache/arrow-rs/pull/2332) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Integrate Record Skipping into Column Reader Fuzz Test [\#2315](https://github.com/apache/arrow-rs/pull/2315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))

## [20.0.0](https://github.com/apache/arrow-rs/tree/20.0.0) (2022-08-05)

[Full Changelog](https://github.com/apache/arrow-rs/compare/19.0.0...20.0.0)

**Breaking changes:**

- Add more const evaluation for `GenericBinaryArray` and `GenericListArray`: add `PREFIX` and data type constructor [\#2327](https://github.com/apache/arrow-rs/pull/2327) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Make FFI support optional, change APIs to be `safe` \(\#2302\) [\#2303](https://github.com/apache/arrow-rs/pull/2303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `test_utils` from default features \(\#2298\) [\#2299](https://github.com/apache/arrow-rs/pull/2299) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `DataType::Decimal` to `DataType::Decimal128` [\#2229](https://github.com/apache/arrow-rs/pull/2229) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `Decimal128Iter` and `Decimal256Iter` and do maximum precision/scale check [\#2140](https://github.com/apache/arrow-rs/pull/2140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Add the constant data type constructors for `ListArray` [\#2311](https://github.com/apache/arrow-rs/issues/2311) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update `FlightSqlService` trait to pass session info along [\#2308](https://github.com/apache/arrow-rs/issues/2308) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Optimize `take_bits` for non-null indices [\#2306](https://github.com/apache/arrow-rs/issues/2306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make FFI support optional via Feature Flag `ffi` [\#2302](https://github.com/apache/arrow-rs/issues/2302) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Mark `ffi::ArrowArray::try_new` is safe [\#2301](https://github.com/apache/arrow-rs/issues/2301) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove test\_utils from default arrow-rs features [\#2298](https://github.com/apache/arrow-rs/issues/2298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove `JsonEqual` trait [\#2296](https://github.com/apache/arrow-rs/issues/2296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Move `with_precision_and_scale` to `Decimal` array traits [\#2291](https://github.com/apache/arrow-rs/issues/2291) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve readability and maybe performance of string --\> numeric/time/date/timetamp cast kernels [\#2285](https://github.com/apache/arrow-rs/issues/2285) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add vectorized unpacking for 8, 16, and 64 bit integers [\#2276](https://github.com/apache/arrow-rs/issues/2276) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use initial capacity for interner hashmap [\#2273](https://github.com/apache/arrow-rs/issues/2273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Impl FromIterator for Decimal256Array [\#2248](https://github.com/apache/arrow-rs/issues/2248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Separate `ArrayReader::next_batch`with `ArrayReader::read_records` and `ArrayReader::consume_batch` [\#2236](https://github.com/apache/arrow-rs/issues/2236) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rename `DataType::Decimal` to `DataType::Decimal128` [\#2228](https://github.com/apache/arrow-rs/issues/2228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Automatically Grow Parquet BitWriter Buffer [\#2226](https://github.com/apache/arrow-rs/issues/2226) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `append_option` support to `Decimal128Builder` and `Decimal256Builder` [\#2224](https://github.com/apache/arrow-rs/issues/2224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Split the `FixedSizeBinaryArray` and `FixedSizeListArray` from `array_binary.rs` and `array_list.rs` [\#2217](https://github.com/apache/arrow-rs/issues/2217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Don't `Box` Values in `PrimitiveDictionaryBuilder` [\#2215](https://github.com/apache/arrow-rs/issues/2215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use BitChunks in equal\_bits [\#2186](https://github.com/apache/arrow-rs/issues/2186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Hash` for `Schema` [\#2182](https://github.com/apache/arrow-rs/issues/2182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- read decimal data type from parquet file with binary physical type [\#2159](https://github.com/apache/arrow-rs/issues/2159) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `GenericStringBuilder` should use `GenericBinaryBuilder` [\#2156](https://github.com/apache/arrow-rs/issues/2156) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update Rust version to 1.62 [\#2143](https://github.com/apache/arrow-rs/issues/2143) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Check precision and scale against maximum value when constructing `Decimal128` and `Decimal256` [\#2139](https://github.com/apache/arrow-rs/issues/2139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `ArrayAccessor` in `Decimal128Iter` and `Decimal256Iter` [\#2138](https://github.com/apache/arrow-rs/issues/2138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `ArrayAccessor` and `FromIterator` in Cast Kernels [\#2137](https://github.com/apache/arrow-rs/issues/2137) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `TypedDictionaryArray` for more ergonomic interaction with `DictionaryArray` [\#2136](https://github.com/apache/arrow-rs/issues/2136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `ArrayAccessor` in Comparison Kernels [\#2135](https://github.com/apache/arrow-rs/issues/2135) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `peek_next_page()` and s`kip_next_page` in `InMemoryColumnChunkReader` [\#2129](https://github.com/apache/arrow-rs/issues/2129) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Lazily materialize the null buffer builder for all array builders. [\#2125](https://github.com/apache/arrow-rs/issues/2125) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Do value validation for `Decimal256` [\#2112](https://github.com/apache/arrow-rs/issues/2112) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `skip_def_levels`  for `ColumnLevelDecoder` [\#2107](https://github.com/apache/arrow-rs/issues/2107) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add integration test for scan rows  with selection [\#2106](https://github.com/apache/arrow-rs/issues/2106) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support for casting from Utf8/String to `Time32` / `Time64` [\#2053](https://github.com/apache/arrow-rs/issues/2053) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update prost and tonic related crates [\#2268](https://github.com/apache/arrow-rs/pull/2268) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([carols10cents](https://github.com/carols10cents))

**Fixed bugs:**

- temporal conversion functions cannot work on negative input properly [\#2325](https://github.com/apache/arrow-rs/issues/2325) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- IPC writer should truncate string array with all empty string [\#2312](https://github.com/apache/arrow-rs/issues/2312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Error order for comparing `Decimal128` or `Decimal256` [\#2256](https://github.com/apache/arrow-rs/issues/2256) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix maximum and minimum for decimal values for precision greater than 38 [\#2246](https://github.com/apache/arrow-rs/issues/2246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `IntervalMonthDayNanoType::make_value()` does not match C implementation [\#2234](https://github.com/apache/arrow-rs/issues/2234) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `FlightSqlService` trait does not allow `impl`s to do handshake [\#2210](https://github.com/apache/arrow-rs/issues/2210) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- `EnabledStatistics::None` not working [\#2185](https://github.com/apache/arrow-rs/issues/2185) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Boolean ArrayData Equality Incorrect Slice Handling [\#2184](https://github.com/apache/arrow-rs/issues/2184) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Publicly export MapFieldNames [\#2118](https://github.com/apache/arrow-rs/issues/2118) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Update instructions on How to join the slack \#arrow-rust channel -- or maybe try to switch to discord?? [\#2192](https://github.com/apache/arrow-rs/issues/2192)
- \[Minor\] Improve arrow and parquet READMEs, document parquet feature flags [\#2324](https://github.com/apache/arrow-rs/pull/2324) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve speed of writing string dictionaries to parquet by skipping a copy\(\#1764\)  [\#2322](https://github.com/apache/arrow-rs/pull/2322) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- Fix wrong logic in calculate\_row\_count when skipping values [\#2328](https://github.com/apache/arrow-rs/issues/2328) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support filter for parquet data type [\#2126](https://github.com/apache/arrow-rs/issues/2126) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make skip value in ByteArrayDecoderDictionary avoid decoding [\#2088](https://github.com/apache/arrow-rs/issues/2088) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- fix: Fix skip error in calculate\_row\_count. [\#2329](https://github.com/apache/arrow-rs/pull/2329) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- temporal conversion functions should work on negative input properly [\#2326](https://github.com/apache/arrow-rs/pull/2326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Increase DeltaBitPackEncoder miniblock size to 64 for 64-bit integers  \(\#2282\) [\#2319](https://github.com/apache/arrow-rs/pull/2319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove JsonEqual [\#2317](https://github.com/apache/arrow-rs/pull/2317) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix: IPC writer should truncate string array with all empty string [\#2314](https://github.com/apache/arrow-rs/pull/2314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JasonLi-cn](https://github.com/JasonLi-cn))
- Pass pull `Request<FlightDescriptor>` to `FlightSqlService` `impl`s  [\#2309](https://github.com/apache/arrow-rs/pull/2309) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Speedup take\_boolean / take\_bits for non-null indices \(~4 - 5x speedup\) [\#2307](https://github.com/apache/arrow-rs/pull/2307) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Add typed dictionary \(\#2136\) [\#2297](https://github.com/apache/arrow-rs/pull/2297) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- \[Minor\] Improve types shown in cast error messages [\#2295](https://github.com/apache/arrow-rs/pull/2295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Move `with_precision_and_scale` to `BasicDecimalArray` trait [\#2292](https://github.com/apache/arrow-rs/pull/2292) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace the `fn get_data_type` by `const DATA_TYPE` in BinaryArray and StringArray [\#2289](https://github.com/apache/arrow-rs/pull/2289) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Clean up string casts and improve performance [\#2284](https://github.com/apache/arrow-rs/pull/2284) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Minor\] Add tests for temporal cast error paths [\#2283](https://github.com/apache/arrow-rs/pull/2283) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add unpack8, unpack16, unpack64 \(\#2276\) ~10-50% faster [\#2278](https://github.com/apache/arrow-rs/pull/2278) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix bugs in the `from_list` function. [\#2277](https://github.com/apache/arrow-rs/pull/2277) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- fix: use signed comparator to compare decimal128 and decimal256 [\#2275](https://github.com/apache/arrow-rs/pull/2275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Use initial capacity for interner hashmap [\#2272](https://github.com/apache/arrow-rs/pull/2272) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Remove fallibility from paruqet RleEncoder \(\#2226\) [\#2259](https://github.com/apache/arrow-rs/pull/2259) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix escaped like wildcards in `like_utf8` / `nlike_utf8` kernels [\#2258](https://github.com/apache/arrow-rs/pull/2258) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([daniel-martinez-maqueda-sap](https://github.com/daniel-martinez-maqueda-sap))
- Add tests for reading nested decimal arrays from parquet [\#2254](https://github.com/apache/arrow-rs/pull/2254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: Implement string cast operations for Time32 and Time64 [\#2251](https://github.com/apache/arrow-rs/pull/2251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([stuartcarnie](https://github.com/stuartcarnie))
- move `FixedSizeList` to `array_fixed_size_list.rs` [\#2250](https://github.com/apache/arrow-rs/pull/2250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Impl FromIterator for Decimal256Array [\#2247](https://github.com/apache/arrow-rs/pull/2247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix max and min value for decimal precision greater than 38 [\#2245](https://github.com/apache/arrow-rs/pull/2245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make `Schema::fields` and `Schema::metadata` `pub` \(public\) [\#2239](https://github.com/apache/arrow-rs/pull/2239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Minor\] Improve Schema metadata mismatch error [\#2238](https://github.com/apache/arrow-rs/pull/2238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Separate ArrayReader::next\_batch with read\_records and consume\_batch [\#2237](https://github.com/apache/arrow-rs/pull/2237) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Update `IntervalMonthDayNanoType::make_value()` to conform to specifications [\#2235](https://github.com/apache/arrow-rs/pull/2235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([avantgardnerio](https://github.com/avantgardnerio))
- Disable value validation for Decimal256 case [\#2232](https://github.com/apache/arrow-rs/pull/2232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Automatically grow parquet BitWriter \(\#2226\) \(~10% faster\) [\#2231](https://github.com/apache/arrow-rs/pull/2231) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Only trigger `arrow` CI on changes to arrow [\#2227](https://github.com/apache/arrow-rs/pull/2227) ([alamb](https://github.com/alamb))
- Add append\_option support to decimal builders [\#2225](https://github.com/apache/arrow-rs/pull/2225) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bphillips-exos](https://github.com/bphillips-exos))
- Optimized writing of byte array to parquet \(\#1764\) \(2x faster\) [\#2221](https://github.com/apache/arrow-rs/pull/2221) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Increase test coverage of ArrowWriter [\#2220](https://github.com/apache/arrow-rs/pull/2220) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update instructions on how to join the Slack channel [\#2219](https://github.com/apache/arrow-rs/pull/2219) ([HaoYang670](https://github.com/HaoYang670))
- Move `FixedSizeBinaryArray` to `array_fixed_size_binary.rs` [\#2218](https://github.com/apache/arrow-rs/pull/2218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Avoid boxing in PrimitiveDictionaryBuilder [\#2216](https://github.com/apache/arrow-rs/pull/2216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- remove redundant CI benchmark check, cleanups [\#2212](https://github.com/apache/arrow-rs/pull/2212) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update `FlightSqlService` trait to proxy handshake [\#2211](https://github.com/apache/arrow-rs/pull/2211) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- parquet: export json api with `serde_json` feature name [\#2209](https://github.com/apache/arrow-rs/pull/2209) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([flisky](https://github.com/flisky))
- Cleanup record skipping logic and tests \(\#2158\) [\#2199](https://github.com/apache/arrow-rs/pull/2199) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use BitChunks in equal\_bits [\#2194](https://github.com/apache/arrow-rs/pull/2194) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix disabling parquet statistics \(\#2185\) [\#2191](https://github.com/apache/arrow-rs/pull/2191) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Change CI names to match crate names [\#2189](https://github.com/apache/arrow-rs/pull/2189) ([alamb](https://github.com/alamb))
- Fix offset handling in boolean\_equal \(\#2184\) [\#2187](https://github.com/apache/arrow-rs/pull/2187) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement `Hash` for `Schema` [\#2183](https://github.com/apache/arrow-rs/pull/2183) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Let the `StringBuilder` use `BinaryBuilder` [\#2181](https://github.com/apache/arrow-rs/pull/2181) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use ArrayAccessor and FromIterator in Cast Kernels [\#2169](https://github.com/apache/arrow-rs/pull/2169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split most arrow specific CI checks into their own workflows \(reduce common CI time to 21 minutes\) [\#2168](https://github.com/apache/arrow-rs/pull/2168) ([alamb](https://github.com/alamb))
- Remove another attempt to cache target directory in action.yaml [\#2167](https://github.com/apache/arrow-rs/pull/2167) ([alamb](https://github.com/alamb))
- Run actions on push to master, pull requests [\#2166](https://github.com/apache/arrow-rs/pull/2166) ([alamb](https://github.com/alamb))
- Break parquet\_derive and arrow\_flight tests into their own workflows [\#2165](https://github.com/apache/arrow-rs/pull/2165) ([alamb](https://github.com/alamb))
- \[minor\] use type aliases refine code. [\#2161](https://github.com/apache/arrow-rs/pull/2161) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- parquet reader: Support reading decimals from parquet `BYTE_ARRAY` type [\#2160](https://github.com/apache/arrow-rs/pull/2160) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Add integration test for scan rows with selection [\#2158](https://github.com/apache/arrow-rs/pull/2158) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Use ArrayAccessor in Comparison Kernels [\#2157](https://github.com/apache/arrow-rs/pull/2157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Implement `peek\_next\_page` and `skip\_next\_page` for `InMemoryColumnCh… [\#2155](https://github.com/apache/arrow-rs/pull/2155) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Avoid decoding unneeded values in ByteArrayDecoderDictionary [\#2154](https://github.com/apache/arrow-rs/pull/2154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Only run integration tests when `arrow` changes [\#2152](https://github.com/apache/arrow-rs/pull/2152) ([alamb](https://github.com/alamb))
- Break out docs CI job to its own github action [\#2151](https://github.com/apache/arrow-rs/pull/2151) ([alamb](https://github.com/alamb))
- Do not pretend to cache rust build artifacts, speed up CI by ~20% [\#2150](https://github.com/apache/arrow-rs/pull/2150) ([alamb](https://github.com/alamb))
- Update rust version to 1.62 [\#2144](https://github.com/apache/arrow-rs/pull/2144) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Make MapFieldNames public \(\#2118\) [\#2134](https://github.com/apache/arrow-rs/pull/2134) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ArrayAccessor trait, remove duplication in array iterators \(\#1948\) [\#2133](https://github.com/apache/arrow-rs/pull/2133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Lazily materialize the null buffer builder for all array builders. [\#2127](https://github.com/apache/arrow-rs/pull/2127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Faster parquet DictEncoder \(~20%\) [\#2123](https://github.com/apache/arrow-rs/pull/2123) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add validation for Decimal256 [\#2113](https://github.com/apache/arrow-rs/pull/2113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support skip\_def\_levels for ColumnLevelDecoder [\#2111](https://github.com/apache/arrow-rs/pull/2111) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Donate `object_store` code from object\_store\_rs to arrow-rs [\#2081](https://github.com/apache/arrow-rs/pull/2081) ([alamb](https://github.com/alamb))
- Improve `validate_utf8` performance [\#2048](https://github.com/apache/arrow-rs/pull/2048) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))

## [19.0.0](https://github.com/apache/arrow-rs/tree/19.0.0) (2022-07-22)

[Full Changelog](https://github.com/apache/arrow-rs/compare/18.0.0...19.0.0)

**Breaking changes:**

- Rename `DecimalArray``/DecimalBuilder` to `Decimal128Array`/`Decimal128Builder` [\#2101](https://github.com/apache/arrow-rs/issues/2101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change builder `append` methods to be infallible where possible [\#2103](https://github.com/apache/arrow-rs/pull/2103) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Return reference from `UnionArray::child` \(\#2035\) [\#2099](https://github.com/apache/arrow-rs/pull/2099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `preserve_order` feature from `serde_json` dependency \(\#2095\) [\#2098](https://github.com/apache/arrow-rs/pull/2098) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `weekday` and `weekday0` kernels to to `num_days_from_monday` and `num_days_since_sunday` [\#2066](https://github.com/apache/arrow-rs/pull/2066) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove `null_count` from `write_batch_with_statistics` [\#2047](https://github.com/apache/arrow-rs/pull/2047) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Use `total_cmp` from std  [\#2130](https://github.com/apache/arrow-rs/issues/2130) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Permit parallel fetching of column chunks in `ParquetRecordBatchStream` [\#2110](https://github.com/apache/arrow-rs/issues/2110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `GenericBinaryBuilder` should use buffer builders directly. [\#2104](https://github.com/apache/arrow-rs/issues/2104) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pass `generate_decimal256_case` arrow integration test [\#2093](https://github.com/apache/arrow-rs/issues/2093) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rename `weekday` and `weekday0` kernels to to `num_days_from_monday` and `days_since_sunday` [\#2065](https://github.com/apache/arrow-rs/issues/2065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Improve performance of `filter_dict` [\#2062](https://github.com/apache/arrow-rs/issues/2062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `set_bits` [\#2060](https://github.com/apache/arrow-rs/issues/2060) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Lazily materialize the null buffer builder of `BooleanBuilder` [\#2058](https://github.com/apache/arrow-rs/issues/2058) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `BooleanArray::from_iter` should omit validity buffer if all values are valid [\#2055](https://github.com/apache/arrow-rs/issues/2055) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FFI\_ArrowSchema should set `DICTIONARY_ORDERED` flag if a field's dictionary is ordered [\#2049](https://github.com/apache/arrow-rs/issues/2049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `peek_next_page()` and `skip_next_page` in `SerializedPageReader` [\#2043](https://github.com/apache/arrow-rs/issues/2043) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support FFI / C Data Interface for `MapType` [\#2037](https://github.com/apache/arrow-rs/issues/2037) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The `DecimalArrayBuilder` should use `FixedSizedBinaryBuilder` [\#2026](https://github.com/apache/arrow-rs/issues/2026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Enable `serialized_reader` read specific Page by passing row ranges. [\#1976](https://github.com/apache/arrow-rs/issues/1976) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- `type_id` and `value_offset` are incorrect for sliced `UnionArray` [\#2086](https://github.com/apache/arrow-rs/issues/2086) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Boolean `take` kernel does not handle null indices correctly [\#2057](https://github.com/apache/arrow-rs/issues/2057) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Don't double-count nulls in `write_batch_with_statistics` [\#2046](https://github.com/apache/arrow-rs/issues/2046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Writer Ignores Statistics specification in `WriterProperties` [\#2014](https://github.com/apache/arrow-rs/issues/2014) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Improve docstrings + examples for `as_primitive_array` cast functions [\#2114](https://github.com/apache/arrow-rs/pull/2114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Why does `serde_json` specify the `preserve_order` feature in `arrow` package [\#2095](https://github.com/apache/arrow-rs/issues/2095) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `skip_values` in DictionaryDecoder [\#2079](https://github.com/apache/arrow-rs/issues/2079) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support skip\_values in ColumnValueDecoderImpl  [\#2078](https://github.com/apache/arrow-rs/issues/2078) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `skip_values` in `ByteArrayColumnValueDecoder` [\#2072](https://github.com/apache/arrow-rs/issues/2072) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Several `Builder::append` methods returning results even though they are infallible [\#2071](https://github.com/apache/arrow-rs/issues/2071)
- Improve formatting of logical plans containing subqueries [\#2059](https://github.com/apache/arrow-rs/issues/2059)
- Return reference from `UnionArray::child`  [\#2035](https://github.com/apache/arrow-rs/issues/2035)
- support write page index [\#1777](https://github.com/apache/arrow-rs/issues/1777) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Use `total_cmp` from std [\#2131](https://github.com/apache/arrow-rs/pull/2131) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- fix clippy [\#2124](https://github.com/apache/arrow-rs/pull/2124) ([alamb](https://github.com/alamb))
- Fix logical merge conflict: `match` arms have incompatible types [\#2121](https://github.com/apache/arrow-rs/pull/2121) ([alamb](https://github.com/alamb))
- Update `GenericBinaryBuilder` to use buffer builders directly. [\#2117](https://github.com/apache/arrow-rs/pull/2117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Simplify null mask preservation in parquet reader [\#2116](https://github.com/apache/arrow-rs/pull/2116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add get\_byte\_ranges method to AsyncFileReader trait [\#2115](https://github.com/apache/arrow-rs/pull/2115) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- add test for skip\_values in DictionaryDecoder and fix it [\#2105](https://github.com/apache/arrow-rs/pull/2105) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Define Decimal128Builder and Decimal128Array [\#2102](https://github.com/apache/arrow-rs/pull/2102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support skip\_values in DictionaryDecoder [\#2100](https://github.com/apache/arrow-rs/pull/2100) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Pass generate\_decimal256\_case integration test, add `DataType::Decimal256` [\#2094](https://github.com/apache/arrow-rs/pull/2094) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- `DecimalBuilder` should use `FixedSizeBinaryBuilder` [\#2092](https://github.com/apache/arrow-rs/pull/2092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Array writer indirection [\#2091](https://github.com/apache/arrow-rs/pull/2091) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove doc hidden from GenericColumnReader [\#2090](https://github.com/apache/arrow-rs/pull/2090) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support skip\_values in ColumnValueDecoderImpl  [\#2089](https://github.com/apache/arrow-rs/pull/2089) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- type\_id and value\_offset are incorrect for sliced UnionArray [\#2087](https://github.com/apache/arrow-rs/pull/2087) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add IPC truncation test case for StructArray [\#2083](https://github.com/apache/arrow-rs/pull/2083) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve performance of set\_bits by using copy\_from\_slice instead of setting individual bytes [\#2077](https://github.com/apache/arrow-rs/pull/2077) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Support skip\_values in ByteArrayColumnValueDecoder [\#2076](https://github.com/apache/arrow-rs/pull/2076) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Lazily materialize the null buffer builder of boolean builder [\#2073](https://github.com/apache/arrow-rs/pull/2073) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix windows CI \(\#2069\) [\#2070](https://github.com/apache/arrow-rs/pull/2070) ([tustvold](https://github.com/tustvold))
- Test utf8\_validation checks char boundaries [\#2068](https://github.com/apache/arrow-rs/pull/2068) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat\(compute\): Support doy \(day of year\) for temporal [\#2067](https://github.com/apache/arrow-rs/pull/2067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ovr](https://github.com/ovr))
- Support nullable indices in boolean take kernel and some optimizations [\#2064](https://github.com/apache/arrow-rs/pull/2064) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Improve performance of filter\_dict [\#2063](https://github.com/apache/arrow-rs/pull/2063) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Ignore null buffer when creating ArrayData if null count is zero [\#2056](https://github.com/apache/arrow-rs/pull/2056) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- feat\(compute\): Support week0 \(PostgreSQL behaviour\) for temporal [\#2052](https://github.com/apache/arrow-rs/pull/2052) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ovr](https://github.com/ovr))
- Set DICTIONARY\_ORDERED flag for FFI\_ArrowSchema [\#2050](https://github.com/apache/arrow-rs/pull/2050) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Generify parquet write path \(\#1764\) [\#2045](https://github.com/apache/arrow-rs/pull/2045) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support peek\_next\_page\(\) and skip\_next\_page in serialized\_reader. [\#2044](https://github.com/apache/arrow-rs/pull/2044) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Support MapType in FFI [\#2042](https://github.com/apache/arrow-rs/pull/2042) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add support of converting `FixedSizeBinaryArray` to `DecimalArray` [\#2041](https://github.com/apache/arrow-rs/pull/2041) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Truncate IPC record batch [\#2040](https://github.com/apache/arrow-rs/pull/2040) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Refine the List builder [\#2034](https://github.com/apache/arrow-rs/pull/2034) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add more tests of RecordReader Batch Size Edge Cases \(\#2025\) [\#2032](https://github.com/apache/arrow-rs/pull/2032) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add support for adding intervals to dates [\#2031](https://github.com/apache/arrow-rs/pull/2031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([avantgardnerio](https://github.com/avantgardnerio))

## [18.0.0](https://github.com/apache/arrow-rs/tree/18.0.0) (2022-07-08)

[Full Changelog](https://github.com/apache/arrow-rs/compare/17.0.0...18.0.0)

**Breaking changes:**

- Fix several bugs in parquet writer statistics generation, add `EnabledStatistics` to control level of statistics generated [\#2022](https://github.com/apache/arrow-rs/pull/2022) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add page index reader test for all types and support empty index. [\#2012](https://github.com/apache/arrow-rs/pull/2012) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add `Decimal256Builder` and `Decimal256Array`; Decimal arrays now implement `BasicDecimalArray` trait [\#2000](https://github.com/apache/arrow-rs/pull/2000) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify `ColumnReader::read_batch` [\#1995](https://github.com/apache/arrow-rs/pull/1995) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `PrimitiveBuilder::finish_dict` \(\#1978\) [\#1980](https://github.com/apache/arrow-rs/pull/1980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Disallow cast from other datatypes to `NullType` [\#1942](https://github.com/apache/arrow-rs/pull/1942) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Add column index writer for parquet [\#1935](https://github.com/apache/arrow-rs/pull/1935) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))

**Implemented enhancements:**

- Add `DataType::Dictionary` support to `subtract_scalar`, `multiply_scalar`, `divide_scalar` [\#2019](https://github.com/apache/arrow-rs/issues/2019) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DictionaryArray in `add_scalar` kernel [\#2017](https://github.com/apache/arrow-rs/issues/2017) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Enable column page index read test for all types [\#2010](https://github.com/apache/arrow-rs/issues/2010) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Simplify `FixedSizeBinaryBuilder` [\#2007](https://github.com/apache/arrow-rs/issues/2007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `Decimal256Builder` and `Decimal256Array` [\#1999](https://github.com/apache/arrow-rs/issues/1999) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Support `DictionaryArray` in `unary` kernel [\#1989](https://github.com/apache/arrow-rs/issues/1989) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add kernel to quickly compute comparisons on `Array`s [\#1987](https://github.com/apache/arrow-rs/issues/1987) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `DictionaryArray` in `divide` kernel [\#1982](https://github.com/apache/arrow-rs/issues/1982) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Into<ArrayData>` for `T: Array` [\#1979](https://github.com/apache/arrow-rs/issues/1979) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Support `DictionaryArray` in `multiply` kernel [\#1972](https://github.com/apache/arrow-rs/issues/1972) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `DictionaryArray` in `subtract` kernel [\#1970](https://github.com/apache/arrow-rs/issues/1970) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Declare `DecimalArray::length` as a constant [\#1967](https://github.com/apache/arrow-rs/issues/1967) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `DictionaryArray` in `add` kernel [\#1950](https://github.com/apache/arrow-rs/issues/1950) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add builder style methods to `Field` [\#1934](https://github.com/apache/arrow-rs/issues/1934) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make `StringDictionaryBuilder` faster [\#1851](https://github.com/apache/arrow-rs/issues/1851) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `concat_elements_utf8` should accept arbitrary number of input arrays [\#1748](https://github.com/apache/arrow-rs/issues/1748) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Array reader for list columns fails to decode if batches fall on row group boundaries [\#2025](https://github.com/apache/arrow-rs/issues/2025) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ColumnWriterImpl::write_batch_with_statistics` incorrect distinct count in statistics [\#2016](https://github.com/apache/arrow-rs/issues/2016) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ColumnWriterImpl::write_batch_with_statistics` can write incorrect page statistics [\#2015](https://github.com/apache/arrow-rs/issues/2015) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `RowFormatter` is not part of the public api [\#2008](https://github.com/apache/arrow-rs/issues/2008) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Infinite Loop possible in `ColumnReader::read_batch` For Corrupted Files [\#1997](https://github.com/apache/arrow-rs/issues/1997) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `PrimitiveBuilder::finish_dict` does not validate dictionary offsets [\#1978](https://github.com/apache/arrow-rs/issues/1978) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect `n_buffers` in `FFI_ArrowArray` [\#1959](https://github.com/apache/arrow-rs/issues/1959) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `DecimalArray::from_fixed_size_list_array` fails when `offset > 0` [\#1958](https://github.com/apache/arrow-rs/issues/1958) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect \(but ignored\) metadata written after ColumnChunk [\#1946](https://github.com/apache/arrow-rs/issues/1946) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Send` + `Sync` impl for `Allocation` may  not be sound unless `Allocation` is `Send` + `Sync` as well [\#1944](https://github.com/apache/arrow-rs/issues/1944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Disallow cast from other datatypes to `NullType` [\#1923](https://github.com/apache/arrow-rs/issues/1923) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- The doc of `FixedSizeListArray::value_length` is incorrect. [\#1908](https://github.com/apache/arrow-rs/issues/1908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Column chunk statistics of `min_bytes` and  `max_bytes` return wrong size [\#2021](https://github.com/apache/arrow-rs/issues/2021) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[Discussion\] Refactor the `Decimal`s by using constant generic. [\#2001](https://github.com/apache/arrow-rs/issues/2001)
- Move `DecimalArray` to a new file [\#1985](https://github.com/apache/arrow-rs/issues/1985) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Support `DictionaryArray` in `multiply` kernel [\#1974](https://github.com/apache/arrow-rs/issues/1974)
- close function instead of mutable reference [\#1969](https://github.com/apache/arrow-rs/issues/1969) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Incorrect `null_count` of DictionaryArray [\#1962](https://github.com/apache/arrow-rs/issues/1962) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support multi diskRanges for ChunkReader [\#1955](https://github.com/apache/arrow-rs/issues/1955) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Persisting Arrow timestamps with Parquet produces missing `TIMESTAMP` in schema [\#1920](https://github.com/apache/arrow-rs/issues/1920) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Sperate get\_next\_page\_header from get\_next\_page in PageReader [\#1834](https://github.com/apache/arrow-rs/issues/1834) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Consistent case in Index enumeration [\#2029](https://github.com/apache/arrow-rs/pull/2029) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix record delimiting on row group boundaries \(\#2025\) [\#2027](https://github.com/apache/arrow-rs/pull/2027) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add builder style APIs For `Field`: `with_name`, `with_data_type` and `with_nullable` [\#2024](https://github.com/apache/arrow-rs/pull/2024) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add dictionary support to subtract\_scalar, multiply\_scalar, divide\_scalar [\#2020](https://github.com/apache/arrow-rs/pull/2020) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support DictionaryArray in add\_scalar kernel [\#2018](https://github.com/apache/arrow-rs/pull/2018) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Refine the `FixedSizeBinaryBuilder` [\#2013](https://github.com/apache/arrow-rs/pull/2013) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add RowFormatter to record public API [\#2009](https://github.com/apache/arrow-rs/pull/2009) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([FabioBatSilva](https://github.com/FabioBatSilva))
- Fix parquet test\_common feature flags [\#2003](https://github.com/apache/arrow-rs/pull/2003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Stub out Skip Records API \(\#1792\) [\#1998](https://github.com/apache/arrow-rs/pull/1998) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Implement `Into<ArrayData>` for `T: Array` [\#1992](https://github.com/apache/arrow-rs/pull/1992) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([heyrutvik](https://github.com/heyrutvik))
- Add unary\_cmp [\#1991](https://github.com/apache/arrow-rs/pull/1991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support DictionaryArray in unary kernel [\#1990](https://github.com/apache/arrow-rs/pull/1990) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Refine `FixedSizeListBuilder` [\#1988](https://github.com/apache/arrow-rs/pull/1988) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Move `DecimalArray` to array\_decimal.rs [\#1986](https://github.com/apache/arrow-rs/pull/1986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- MINOR: Fix clippy error after updating rust toolchain [\#1984](https://github.com/apache/arrow-rs/pull/1984) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Support dictionary array for divide kernel [\#1983](https://github.com/apache/arrow-rs/pull/1983) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support dictionary array for subtract and multiply kernel [\#1971](https://github.com/apache/arrow-rs/pull/1971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Declare the value\_length of decimal array as a `const` [\#1968](https://github.com/apache/arrow-rs/pull/1968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix the behavior of `from_fixed_size_list` when offset \> 0 [\#1964](https://github.com/apache/arrow-rs/pull/1964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Calculate n\_buffers in FFI\_ArrowArray by data layout [\#1960](https://github.com/apache/arrow-rs/pull/1960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix the doc of `FixedSizeListArray::value_length` [\#1957](https://github.com/apache/arrow-rs/pull/1957) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use InMemoryColumnChunkReader \(~20% faster\) [\#1956](https://github.com/apache/arrow-rs/pull/1956) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Unpin clap \(\#1867\) [\#1954](https://github.com/apache/arrow-rs/pull/1954) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Set is\_adjusted\_to\_utc if any timezone set \(\#1932\) [\#1953](https://github.com/apache/arrow-rs/pull/1953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add add\_dyn for DictionaryArray support [\#1951](https://github.com/apache/arrow-rs/pull/1951) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- write `ColumnMetadata` after the column chunk data, not the `ColumnChunk` [\#1947](https://github.com/apache/arrow-rs/pull/1947) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Require Send+Sync bounds for Allocation trait [\#1945](https://github.com/apache/arrow-rs/pull/1945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
-  Faster StringDictionaryBuilder \(~60% faster\) \(\#1851\)  [\#1861](https://github.com/apache/arrow-rs/pull/1861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Arbitrary size concat elements utf8 [\#1787](https://github.com/apache/arrow-rs/pull/1787) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))

## [17.0.0](https://github.com/apache/arrow-rs/tree/17.0.0) (2022-06-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/16.0.0...17.0.0)

**Breaking changes:**

- Add validation to `RecordBatch` for non-nullable fields containing null values [\#1890](https://github.com/apache/arrow-rs/pull/1890) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Rename `ArrayData::validate_dict_offsets` to `ArrayData::validate_values` [\#1889](https://github.com/apache/arrow-rs/pull/1889) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([frolovdev](https://github.com/frolovdev))
-  Add `Decimal128` API and use it in DecimalArray and DecimalBuilder [\#1871](https://github.com/apache/arrow-rs/pull/1871) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Mark typed buffer APIs `safe` \(\#996\) \(\#1027\) [\#1866](https://github.com/apache/arrow-rs/pull/1866) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- add a small doc example showing `ArrowWriter` being used with a cursor [\#1927](https://github.com/apache/arrow-rs/issues/1927) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `cast` to/from `NULL` and `DataType::Decimal` [\#1921](https://github.com/apache/arrow-rs/issues/1921) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `Decimal256` API [\#1913](https://github.com/apache/arrow-rs/issues/1913) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `DictionaryArray::key` function [\#1911](https://github.com/apache/arrow-rs/issues/1911) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support specifying capacities for `ListArrays` in `MutableArrayData` [\#1884](https://github.com/apache/arrow-rs/issues/1884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Explicitly declare the features used for each dependency [\#1876](https://github.com/apache/arrow-rs/issues/1876) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add Decimal128 API and use it in DecimalArray and DecimalBuilder [\#1870](https://github.com/apache/arrow-rs/issues/1870) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `PrimitiveArray::from_iter` should omit validity buffer if all values are valid [\#1856](https://github.com/apache/arrow-rs/issues/1856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `from(v: Vec<Option<&[u8]>>)` and `from(v: Vec<&[u8]>)` for `FixedSizedBInaryArray` [\#1852](https://github.com/apache/arrow-rs/issues/1852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `Vec`-inspired APIs to `BufferBuilder` [\#1850](https://github.com/apache/arrow-rs/issues/1850) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- PyArrow intergation test for C Stream Interface [\#1847](https://github.com/apache/arrow-rs/issues/1847) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `nilike` support in `comparison` [\#1845](https://github.com/apache/arrow-rs/issues/1845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Split up `arrow::array::builder` module [\#1843](https://github.com/apache/arrow-rs/issues/1843) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `quarter` support in `temporal` kernels [\#1835](https://github.com/apache/arrow-rs/issues/1835) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rename `ArrayData::validate_dictionary_offset` to `ArrayData::validate_values` [\#1812](https://github.com/apache/arrow-rs/issues/1812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clean up the testing code for `substring` kernel [\#1801](https://github.com/apache/arrow-rs/issues/1801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up `substring_by_char` kernel [\#1800](https://github.com/apache/arrow-rs/issues/1800) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- unable to write parquet file with UTC timestamp [\#1932](https://github.com/apache/arrow-rs/issues/1932) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Incorrect max and min decimals [\#1916](https://github.com/apache/arrow-rs/issues/1916) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `dynamic_types` example does not print the projection [\#1902](https://github.com/apache/arrow-rs/issues/1902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `log2(0)` panicked at `'attempt to subtract with overflow', parquet/src/util/bit_util.rs:148:5` [\#1901](https://github.com/apache/arrow-rs/issues/1901) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Final slicing in `combine_option_bitmap` needs to use bit slices [\#1899](https://github.com/apache/arrow-rs/issues/1899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Dictionary IPC writer  writes incorrect schema [\#1892](https://github.com/apache/arrow-rs/issues/1892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Creating a `RecordBatch` with null values in non-nullable fields does not cause an error [\#1888](https://github.com/apache/arrow-rs/issues/1888) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Upgrade `regex` dependency [\#1874](https://github.com/apache/arrow-rs/issues/1874) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Miri reports leaks in ffi tests [\#1872](https://github.com/apache/arrow-rs/issues/1872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- AVX512 + simd binary and/or kernels slower than autovectorized version [\#1829](https://github.com/apache/arrow-rs/issues/1829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Blog post about arrow 10.0.0 - 16.0.0 [\#1808](https://github.com/apache/arrow-rs/issues/1808)
- Add README for the compute module. [\#1940](https://github.com/apache/arrow-rs/pull/1940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- minor: clarify docstring on `DictionaryArray::lookup_key` [\#1910](https://github.com/apache/arrow-rs/pull/1910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- minor: add a diagram to docstring for DictionaryArray [\#1909](https://github.com/apache/arrow-rs/pull/1909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Closes \#1902: Print the original and projected RecordBatch in dynamic\_types example [\#1903](https://github.com/apache/arrow-rs/pull/1903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([martin-g](https://github.com/martin-g))

**Closed issues:**

- how read/write REPEATED [\#1886](https://github.com/apache/arrow-rs/issues/1886) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Handling Unsupported Arrow Types in Parquet [\#1666](https://github.com/apache/arrow-rs/issues/1666) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Set adjusted to UTC if UTC timezone \(\#1932\) [\#1937](https://github.com/apache/arrow-rs/pull/1937) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split up parquet::arrow::array\_reader \(\#1483\) [\#1933](https://github.com/apache/arrow-rs/pull/1933) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add ArrowWriter doctest \(\#1927\) [\#1930](https://github.com/apache/arrow-rs/pull/1930) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update indexmap dependency [\#1929](https://github.com/apache/arrow-rs/pull/1929) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Complete and fixup  split of `arrow::array::builder` module \(\#1843\) [\#1928](https://github.com/apache/arrow-rs/pull/1928) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- MINOR: Replace `checked_add/sub().unwrap()` with `+/-` [\#1924](https://github.com/apache/arrow-rs/pull/1924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support casting `NULL` to/from `Decimal` [\#1922](https://github.com/apache/arrow-rs/pull/1922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Update half requirement from 1.8 to 2.0 [\#1919](https://github.com/apache/arrow-rs/pull/1919) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix max and min decimal for max precision [\#1917](https://github.com/apache/arrow-rs/pull/1917) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `Decimal256` API [\#1914](https://github.com/apache/arrow-rs/pull/1914) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `DictionaryArray::key` function [\#1912](https://github.com/apache/arrow-rs/pull/1912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix misaligned reference and logic error in crc32 [\#1906](https://github.com/apache/arrow-rs/pull/1906) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([saethlin](https://github.com/saethlin))
- Refine the `bit_util` of Parquet. [\#1905](https://github.com/apache/arrow-rs/pull/1905) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HaoYang670](https://github.com/HaoYang670))
- Use bit\_slice in combine\_option\_bitmap [\#1900](https://github.com/apache/arrow-rs/pull/1900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Issue \#1876: Explicitly declare the used features for each dependency in integration\_testing [\#1898](https://github.com/apache/arrow-rs/pull/1898) ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet\_derive\_test [\#1897](https://github.com/apache/arrow-rs/pull/1897) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet\_derive [\#1896](https://github.com/apache/arrow-rs/pull/1896) ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet [\#1895](https://github.com/apache/arrow-rs/pull/1895) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([martin-g](https://github.com/martin-g))
- Minor: Add examples to docstring for `weekday` [\#1894](https://github.com/apache/arrow-rs/pull/1894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Correct nullable in read\_dictionary [\#1893](https://github.com/apache/arrow-rs/pull/1893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Feature add weekday temporal kernel [\#1891](https://github.com/apache/arrow-rs/pull/1891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nl5887](https://github.com/nl5887))
- Support specifying list capacities for `MutableArrayData` [\#1885](https://github.com/apache/arrow-rs/pull/1885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet [\#1881](https://github.com/apache/arrow-rs/pull/1881) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in arrow-flight [\#1880](https://github.com/apache/arrow-rs/pull/1880) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([martin-g](https://github.com/martin-g))
- Split up arrow::array::builder module \(\#1843\) [\#1879](https://github.com/apache/arrow-rs/pull/1879) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([DaltonModlin](https://github.com/DaltonModlin))
- Fix memory leak in ffi test [\#1878](https://github.com/apache/arrow-rs/pull/1878) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Issue \#1876 - Explicitly declare the used features for each dependency [\#1877](https://github.com/apache/arrow-rs/pull/1877) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([martin-g](https://github.com/martin-g))
- Fixes \#1874 - Upgrade `regex` dependency to 1.5.6 [\#1875](https://github.com/apache/arrow-rs/pull/1875) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([martin-g](https://github.com/martin-g))
- Do not print exit code from miri, instead it should be the return value of the script [\#1873](https://github.com/apache/arrow-rs/pull/1873) ([jhorstmann](https://github.com/jhorstmann))
- Update vendored gRPC [\#1869](https://github.com/apache/arrow-rs/pull/1869) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Expose `BitSliceIterator` and `BitIndexIterator` \(\#1864\) [\#1865](https://github.com/apache/arrow-rs/pull/1865) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Exclude some long-running tests when running under miri [\#1863](https://github.com/apache/arrow-rs/pull/1863) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add vec-inspired APIs to BufferBuilder \(\#1850\) [\#1860](https://github.com/apache/arrow-rs/pull/1860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Omit validity buffer in PrimitiveArray::from\_iter when all values are valid [\#1859](https://github.com/apache/arrow-rs/pull/1859) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add two `from` methods for `FixedSizeBinaryArray` [\#1854](https://github.com/apache/arrow-rs/pull/1854) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Clean up the test code of `substring` kernel. [\#1853](https://github.com/apache/arrow-rs/pull/1853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add PyArrow integration test for C Stream Interface [\#1848](https://github.com/apache/arrow-rs/pull/1848) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `nilike` support in `comparison` [\#1846](https://github.com/apache/arrow-rs/pull/1846) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([MazterQyou](https://github.com/MazterQyou))
- MINOR: Remove version check from `test_command_help` [\#1844](https://github.com/apache/arrow-rs/pull/1844) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Implement UnionArray FieldData using Type Erasure [\#1842](https://github.com/apache/arrow-rs/pull/1842) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `quarter` support in `temporal` [\#1836](https://github.com/apache/arrow-rs/pull/1836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([MazterQyou](https://github.com/MazterQyou))
- speed up `substring_by_char` by about 2.5x [\#1832](https://github.com/apache/arrow-rs/pull/1832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove simd and avx512 bitwise kernels in favor of autovectorization [\#1830](https://github.com/apache/arrow-rs/pull/1830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Refactor parquet::arrow module [\#1827](https://github.com/apache/arrow-rs/pull/1827) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- docs: remove experimental marker on C Stream Interface [\#1821](https://github.com/apache/arrow-rs/pull/1821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Separate Page IO from Page Decode [\#1810](https://github.com/apache/arrow-rs/pull/1810) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))


## [16.0.0](https://github.com/apache/arrow-rs/tree/16.0.0) (2022-06-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/15.0.0...16.0.0)

**Breaking changes:**

- Seal `ArrowNativeType` and `OffsetSizeTrait` for safety \(\#1028\) [\#1819](https://github.com/apache/arrow-rs/pull/1819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve API for `csv::infer_file_schema` by removing redundant ref  [\#1776](https://github.com/apache/arrow-rs/pull/1776) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- List equality method should work on empty offset `ListArray` [\#1817](https://github.com/apache/arrow-rs/issues/1817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Command line tool for convert CSV to Parquet [\#1797](https://github.com/apache/arrow-rs/issues/1797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC writer should write validity buffer for `UnionArray` in V4 IPC message [\#1793](https://github.com/apache/arrow-rs/issues/1793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add function for row alignment with page mask [\#1790](https://github.com/apache/arrow-rs/issues/1790) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rust IPC Read should be able to read V4 UnionType Array [\#1788](https://github.com/apache/arrow-rs/issues/1788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `combine_option_bitmap` should accept arbitrary number of input arrays. [\#1780](https://github.com/apache/arrow-rs/issues/1780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `substring_by_char` kernels for slicing on character boundaries [\#1768](https://github.com/apache/arrow-rs/issues/1768) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support reading `PageIndex` from column metadata [\#1761](https://github.com/apache/arrow-rs/issues/1761) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from `DataType::Utf8` to `DataType::Boolean` [\#1740](https://github.com/apache/arrow-rs/issues/1740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make current position available in `FileWriter`. [\#1691](https://github.com/apache/arrow-rs/issues/1691) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing parquet to `stdout` [\#1687](https://github.com/apache/arrow-rs/issues/1687) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Incorrect Offset Validation for Sliced List Array Children [\#1814](https://github.com/apache/arrow-rs/issues/1814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Snappy Codec overwrites Existing Data in Decompression Buffer [\#1806](https://github.com/apache/arrow-rs/issues/1806) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `flight_data_to_arrow_batch` does not support `RecordBatch`es with no columns [\#1783](https://github.com/apache/arrow-rs/issues/1783) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- parquet does not compile with `features=["zstd"]` [\#1630](https://github.com/apache/arrow-rs/issues/1630) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Update arrow module docs [\#1840](https://github.com/apache/arrow-rs/pull/1840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update safety disclaimer [\#1837](https://github.com/apache/arrow-rs/pull/1837) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update ballista readme link [\#1765](https://github.com/apache/arrow-rs/pull/1765) ([tustvold](https://github.com/tustvold))
- Move changelog archive to `CHANGELOG-old.md` [\#1759](https://github.com/apache/arrow-rs/pull/1759) ([alamb](https://github.com/alamb))

**Closed issues:**

- `DataType::Decimal` Non-Compliant? [\#1779](https://github.com/apache/arrow-rs/issues/1779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Further simplify the offset validation [\#1770](https://github.com/apache/arrow-rs/issues/1770) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Best way to convert arrow to Rust native type [\#1760](https://github.com/apache/arrow-rs/issues/1760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Why `Parquet` is a part of `Arrow`? [\#1715](https://github.com/apache/arrow-rs/issues/1715) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Make equals\_datatype method public, enabling other modules [\#1838](https://github.com/apache/arrow-rs/pull/1838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nl5887](https://github.com/nl5887))
- \[Minor\] Clarify `PageIterator` Documentation [\#1831](https://github.com/apache/arrow-rs/pull/1831) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Update MIRI pin [\#1828](https://github.com/apache/arrow-rs/pull/1828) ([tustvold](https://github.com/tustvold))
- Change to use `resolver v2`, test more feature flag combinations in CI, fix errors \(\#1630\) [\#1822](https://github.com/apache/arrow-rs/pull/1822) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ScalarBuffer abstraction \(\#1811\) [\#1820](https://github.com/apache/arrow-rs/pull/1820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix list equal for empty offset list array [\#1818](https://github.com/apache/arrow-rs/pull/1818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Decimal and List ArrayData Validation \(\#1813\) \(\#1814\) [\#1816](https://github.com/apache/arrow-rs/pull/1816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't overwrite existing data on snappy decompress \(\#1806\) [\#1807](https://github.com/apache/arrow-rs/pull/1807) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rename `arrow/benches/string_kernels.rs` to `arrow/benches/substring_kernels.rs` [\#1805](https://github.com/apache/arrow-rs/pull/1805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add public API for decoding parquet footer [\#1804](https://github.com/apache/arrow-rs/pull/1804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add AsyncFileReader trait [\#1803](https://github.com/apache/arrow-rs/pull/1803) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- add parquet-fromcsv \(\#1\) [\#1798](https://github.com/apache/arrow-rs/pull/1798) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kazuk](https://github.com/kazuk))
- Use IPC row count info in IPC reader [\#1796](https://github.com/apache/arrow-rs/pull/1796) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix typos in the Memory and Buffers section of the docs home [\#1795](https://github.com/apache/arrow-rs/pull/1795) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([datapythonista](https://github.com/datapythonista))
- Write validity buffer for UnionArray in V4 IPC message [\#1794](https://github.com/apache/arrow-rs/pull/1794) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat:Add function for row alignment with page mask [\#1791](https://github.com/apache/arrow-rs/pull/1791) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Read and skip validity buffer of UnionType Array for V4 ipc message [\#1789](https://github.com/apache/arrow-rs/pull/1789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Add `Substring_by_char` [\#1784](https://github.com/apache/arrow-rs/pull/1784) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add `ParquetFileArrowReader::try_new` [\#1782](https://github.com/apache/arrow-rs/pull/1782) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Arbitrary size combine option bitmap [\#1781](https://github.com/apache/arrow-rs/pull/1781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))
- Implement `ChunkReader` for `Bytes`, deprecate `SliceableCursor` [\#1775](https://github.com/apache/arrow-rs/pull/1775) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Access metadata of flushed row groups on write \(\#1691\) [\#1774](https://github.com/apache/arrow-rs/pull/1774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Simplify ParquetFileArrowReader Metadata API [\#1773](https://github.com/apache/arrow-rs/pull/1773) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- MINOR: Unpin nightly version as packed\_simd releases new version [\#1771](https://github.com/apache/arrow-rs/pull/1771) ([viirya](https://github.com/viirya))
- Update comfy-table requirement from 5.0 to 6.0 [\#1769](https://github.com/apache/arrow-rs/pull/1769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Optionally disable `validate_decimal_precision` check in `DecimalBuilder.append_value` for interop test [\#1767](https://github.com/apache/arrow-rs/pull/1767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Minor: Clean up the code of MutableArrayData [\#1763](https://github.com/apache/arrow-rs/pull/1763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support reading PageIndex from parquet metadata, prepare for skipping pages at reading [\#1762](https://github.com/apache/arrow-rs/pull/1762) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Support casting `Utf8` to `Boolean` [\#1738](https://github.com/apache/arrow-rs/pull/1738) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([MazterQyou](https://github.com/MazterQyou))


## [15.0.0](https://github.com/apache/arrow-rs/tree/15.0.0) (2022-05-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/14.0.0...15.0.0)

**Breaking changes:**

- Change `ArrayDataBuilder::null_bit_buffer` to accept `Option<Buffer>` rather than `Buffer` [\#1739](https://github.com/apache/arrow-rs/pull/1739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove `null_count` from `ArrayData::try_new()` [\#1721](https://github.com/apache/arrow-rs/pull/1721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Change parquet writers to use standard `std:io::Write` rather custom `ParquetWriter` trait \(\#1717\) \(\#1163\) [\#1719](https://github.com/apache/arrow-rs/pull/1719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add explicit column mask for selection in parquet: `ProjectionMask` \(\#1701\) [\#1716](https://github.com/apache/arrow-rs/pull/1716) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add type\_ids in Union datatype [\#1703](https://github.com/apache/arrow-rs/pull/1703) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Parquet Reader's Arrow Schema Inference [\#1682](https://github.com/apache/arrow-rs/pull/1682) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Rename the `string` kernel to `concatenate_elements` [\#1747](https://github.com/apache/arrow-rs/issues/1747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `ArrayDataBuilder::null_bit_buffer` should accept `Option<Buffer>` as input type [\#1737](https://github.com/apache/arrow-rs/issues/1737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix schema comparison for non\_canonical\_map when running flight test [\#1730](https://github.com/apache/arrow-rs/issues/1730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support in aggregate kernel for `BinaryArray` [\#1724](https://github.com/apache/arrow-rs/issues/1724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix incorrect null\_count in `generate_unions_case` integration test [\#1712](https://github.com/apache/arrow-rs/issues/1712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Keep type ids in Union datatype to follow Arrow spec and integrate with other implementations [\#1690](https://github.com/apache/arrow-rs/issues/1690) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Reading Alternative List Representations to Arrow From Parquet [\#1680](https://github.com/apache/arrow-rs/issues/1680) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speed up the offsets checking [\#1675](https://github.com/apache/arrow-rs/issues/1675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Separate Parquet -\> Arrow Schema Conversion From ArrayBuilder [\#1655](https://github.com/apache/arrow-rs/issues/1655) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `leaf_columns` argument to `ArrowReader::get_record_reader_by_columns` [\#1653](https://github.com/apache/arrow-rs/issues/1653) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement `string_concat` kernel  [\#1540](https://github.com/apache/arrow-rs/issues/1540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve Unit Test Coverage of ArrayReaderBuilder [\#1484](https://github.com/apache/arrow-rs/issues/1484) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Parquet write failure \(from record batches\) when data is nested two levels deep  [\#1744](https://github.com/apache/arrow-rs/issues/1744) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC reader may break on projection [\#1735](https://github.com/apache/arrow-rs/issues/1735) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Latest nightly fails to build with feature simd [\#1734](https://github.com/apache/arrow-rs/issues/1734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Trying to write parquet file in parallel results in corrupt file [\#1717](https://github.com/apache/arrow-rs/issues/1717) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Roundtrip failure when using DELTA\_BINARY\_PACKED [\#1708](https://github.com/apache/arrow-rs/issues/1708) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ArrayData::try_new` cannot always return expected error. [\#1707](https://github.com/apache/arrow-rs/issues/1707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  "out of order projection is not supported" after Fix Parquet Arrow Schema Inference [\#1701](https://github.com/apache/arrow-rs/issues/1701) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rust is not interoperability with C++ for IPC schemas with dictionaries [\#1694](https://github.com/apache/arrow-rs/issues/1694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect Repeated Field Schema Inference [\#1681](https://github.com/apache/arrow-rs/issues/1681) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Treats Embedded Arrow Schema as Authoritative [\#1663](https://github.com/apache/arrow-rs/issues/1663) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet\_to\_arrow\_schema\_by\_columns Incorrectly Handles Nested Types [\#1654](https://github.com/apache/arrow-rs/issues/1654) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Inconsistent Arrow Schema When Projecting Nested Parquet File [\#1652](https://github.com/apache/arrow-rs/issues/1652) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StructArrayReader Cannot Handle Nested Lists [\#1651](https://github.com/apache/arrow-rs/issues/1651) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Bug \(`substring` kernel\): The null buffer is not aligned when `offset != 0` [\#1639](https://github.com/apache/arrow-rs/issues/1639) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Parquet command line tool does not install "globally" [\#1710](https://github.com/apache/arrow-rs/issues/1710) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Improve integration test document to follow Arrow C++ repo CI [\#1742](https://github.com/apache/arrow-rs/pull/1742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Merged pull requests:**

- Test for list array equality with different offsets [\#1756](https://github.com/apache/arrow-rs/pull/1756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Rename `string_concat` to `concat_elements_utf8` [\#1754](https://github.com/apache/arrow-rs/pull/1754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Rename the `string` kernel to `concat_elements`. [\#1752](https://github.com/apache/arrow-rs/pull/1752) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support writing nested lists to parquet [\#1746](https://github.com/apache/arrow-rs/pull/1746) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Pin nightly version to bypass packed\_simd build error [\#1743](https://github.com/apache/arrow-rs/pull/1743) ([viirya](https://github.com/viirya))
- Fix projection in IPC reader [\#1736](https://github.com/apache/arrow-rs/pull/1736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([iyupeng](https://github.com/iyupeng))
- `cargo install` installs not globally [\#1732](https://github.com/apache/arrow-rs/pull/1732) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kazuk](https://github.com/kazuk))
- Fix schema comparison for non\_canonical\_map when running flight test [\#1731](https://github.com/apache/arrow-rs/pull/1731) ([viirya](https://github.com/viirya))
- Add `min_binary` and `max_binary` aggregate kernels [\#1725](https://github.com/apache/arrow-rs/pull/1725) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix parquet benchmarks [\#1723](https://github.com/apache/arrow-rs/pull/1723) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix BitReader::get\_batch zero extension \(\#1708\) [\#1722](https://github.com/apache/arrow-rs/pull/1722) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Implementation string concat [\#1720](https://github.com/apache/arrow-rs/pull/1720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))
- Check the length of `null_bit_buffer` in `ArrayData::try_new()` [\#1714](https://github.com/apache/arrow-rs/pull/1714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix incorrect null\_count in `generate_unions_case` integration test [\#1713](https://github.com/apache/arrow-rs/pull/1713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix: Null buffer accounts for `offset` in `substring` kernel. [\#1704](https://github.com/apache/arrow-rs/pull/1704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Minor: Refine `OffsetSizeTrait` to extend `num::Integer`  [\#1702](https://github.com/apache/arrow-rs/pull/1702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix StructArrayReader handling nested lists \(\#1651\)  [\#1700](https://github.com/apache/arrow-rs/pull/1700) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Speed up the offsets checking [\#1684](https://github.com/apache/arrow-rs/pull/1684) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))

## [14.0.0](https://github.com/apache/arrow-rs/tree/14.0.0) (2022-05-13)

[Full Changelog](https://github.com/apache/arrow-rs/compare/13.0.0...14.0.0)

**Breaking changes:**

- Use `bytes` in parquet rather than custom Buffer implementation \(\#1474\) [\#1683](https://github.com/apache/arrow-rs/pull/1683) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rename  `OffsetSize::fn is_large` to `const OffsetSize::IS_LARGE` [\#1664](https://github.com/apache/arrow-rs/pull/1664) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove `StringOffsetTrait` and `BinaryOffsetTrait` [\#1645](https://github.com/apache/arrow-rs/pull/1645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix `generate_nested_dictionary_case` integration test failure  [\#1636](https://github.com/apache/arrow-rs/pull/1636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Add support for `DataType::Duration` in ffi interface [\#1688](https://github.com/apache/arrow-rs/issues/1688) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix `generate_unions_case` integration test  [\#1676](https://github.com/apache/arrow-rs/issues/1676) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Add `DictionaryArray` support for `bit_length` kernel [\#1673](https://github.com/apache/arrow-rs/issues/1673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Add `DictionaryArray` support for `length` kernel [\#1672](https://github.com/apache/arrow-rs/issues/1672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- flight\_client\_scenarios integration test should receive schema from flight data [\#1669](https://github.com/apache/arrow-rs/issues/1669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unpin Flatbuffer version dependency [\#1667](https://github.com/apache/arrow-rs/issues/1667) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add dictionary array support for substring function [\#1656](https://github.com/apache/arrow-rs/issues/1656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Exclude dict\_id and dict\_is\_ordered from equality comparison of `Field` [\#1646](https://github.com/apache/arrow-rs/issues/1646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove `StringOffsetTrait` and `BinaryOffsetTrait` [\#1644](https://github.com/apache/arrow-rs/issues/1644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add tests and examples for `UnionArray::from(data: ArrayData)` [\#1643](https://github.com/apache/arrow-rs/issues/1643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add methods `pub fn offsets_buffer`, `pub fn types_ids_buffer`and `pub fn data_buffer` for `ArrayDataBuilder` [\#1640](https://github.com/apache/arrow-rs/issues/1640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix `generate_nested_dictionary_case` integration test failure for Rust cases [\#1635](https://github.com/apache/arrow-rs/issues/1635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose `ArrowWriter` row group flush in public API [\#1626](https://github.com/apache/arrow-rs/issues/1626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `substring` support for `FixedSizeBinaryArray` [\#1618](https://github.com/apache/arrow-rs/issues/1618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add PrettyPrint for `UnionArray`s [\#1594](https://github.com/apache/arrow-rs/issues/1594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add SIMD support for the `length` kernel [\#1489](https://github.com/apache/arrow-rs/issues/1489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support dictionary arrays in length and bit\_length [\#1674](https://github.com/apache/arrow-rs/pull/1674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add dictionary array support for substring function [\#1665](https://github.com/apache/arrow-rs/pull/1665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Add `DecimalType` support in `new_null_array ` [\#1659](https://github.com/apache/arrow-rs/pull/1659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))

**Fixed bugs:**

- Docs.rs build is broken  [\#1695](https://github.com/apache/arrow-rs/issues/1695)
- Interoperability with C++ for IPC schemas with dictionaries [\#1694](https://github.com/apache/arrow-rs/issues/1694)
- `UnionArray::is_null` incorrect [\#1625](https://github.com/apache/arrow-rs/issues/1625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Published Parquet documentation missing `arrow::async_reader` [\#1617](https://github.com/apache/arrow-rs/issues/1617) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Files written with Julia's Arrow.jl in IPC format cannot be read by arrow-rs [\#1335](https://github.com/apache/arrow-rs/issues/1335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Correct arrow-flight readme version [\#1641](https://github.com/apache/arrow-rs/pull/1641) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Make `OffsetSizeTrait::IS_LARGE` as a const value [\#1658](https://github.com/apache/arrow-rs/issues/1658)
- Question: Why are there 3 types of `OffsetSizeTrait`s? [\#1638](https://github.com/apache/arrow-rs/issues/1638)
- Written Parquet file way bigger than input files  [\#1627](https://github.com/apache/arrow-rs/issues/1627)
- Ensure there is a single zero in the offsets buffer for an empty ListArray. [\#1620](https://github.com/apache/arrow-rs/issues/1620)
- Filtering `UnionArray` Changes DataType [\#1595](https://github.com/apache/arrow-rs/issues/1595)

**Merged pull requests:**

- Fix docs.rs build [\#1696](https://github.com/apache/arrow-rs/pull/1696) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- support duration in ffi [\#1689](https://github.com/apache/arrow-rs/pull/1689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ryan-jacobs1](https://github.com/ryan-jacobs1))
- fix bench command line options [\#1685](https://github.com/apache/arrow-rs/pull/1685) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- Enable branch protection [\#1679](https://github.com/apache/arrow-rs/pull/1679) ([tustvold](https://github.com/tustvold))
- Fix logical merge conflict in \#1588 [\#1678](https://github.com/apache/arrow-rs/pull/1678) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix generate\_unions\_case for Rust case [\#1677](https://github.com/apache/arrow-rs/pull/1677) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Receive schema from flight data [\#1670](https://github.com/apache/arrow-rs/pull/1670) ([viirya](https://github.com/viirya))
- unpin flatbuffers dependency version [\#1668](https://github.com/apache/arrow-rs/pull/1668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Cheappie](https://github.com/Cheappie))
- Remove parquet dictionary converters \(\#1661\) [\#1662](https://github.com/apache/arrow-rs/pull/1662) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Minor: simplify the function `GenericListArray::get_type` [\#1650](https://github.com/apache/arrow-rs/pull/1650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Pretty Print `UnionArray`s [\#1648](https://github.com/apache/arrow-rs/pull/1648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))
- Exclude `dict_id` and `dict_is_ordered` from equality comparison of `Field` [\#1647](https://github.com/apache/arrow-rs/pull/1647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- expose row-group flush in public api [\#1634](https://github.com/apache/arrow-rs/pull/1634) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Cheappie](https://github.com/Cheappie))
- Add `substring` support for `FixedSizeBinaryArray` [\#1633](https://github.com/apache/arrow-rs/pull/1633) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix UnionArray is\_null [\#1632](https://github.com/apache/arrow-rs/pull/1632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Do not assume dictionaries exists in footer [\#1631](https://github.com/apache/arrow-rs/pull/1631) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pcjentsch](https://github.com/pcjentsch))
- Add support for nested list arrays from parquet to arrow arrays \(\#993\) [\#1588](https://github.com/apache/arrow-rs/pull/1588) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add `async` into doc features [\#1349](https://github.com/apache/arrow-rs/pull/1349) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HaoYang670](https://github.com/HaoYang670))


## [13.0.0](https://github.com/apache/arrow-rs/tree/13.0.0) (2022-04-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/12.0.0...13.0.0)

**Breaking changes:**

- Update `parquet::basic::LogicalType` to be more idomatic [\#1612](https://github.com/apache/arrow-rs/pull/1612) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tfeda](https://github.com/tfeda))
- Fix Null Mask Handling in `ArrayData`,  `UnionArray`, and `MapArray` [\#1589](https://github.com/apache/arrow-rs/pull/1589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Replace `&Option<T>`  with `Option<&T>` in several `arrow` and `parquet` APIs [\#1571](https://github.com/apache/arrow-rs/pull/1571) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))

**Implemented enhancements:**

- Read/write nested dictionary under fixed size list in ipc stream reader/write [\#1609](https://github.com/apache/arrow-rs/issues/1609) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `BinaryArray` in `substring`  kernel [\#1593](https://github.com/apache/arrow-rs/issues/1593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read/write nested dictionary under large list in ipc stream reader/write [\#1584](https://github.com/apache/arrow-rs/issues/1584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read/write nested dictionary under map in ipc stream reader/write [\#1582](https://github.com/apache/arrow-rs/issues/1582) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Clone` for JSON `DecoderOptions` [\#1580](https://github.com/apache/arrow-rs/issues/1580) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add utf-8 validation checking to `substring` kernel [\#1575](https://github.com/apache/arrow-rs/issues/1575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting to/from `DataType::Null` in `cast` kernel [\#1572](https://github.com/apache/arrow-rs/pull/1572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([WinkerDu](https://github.com/WinkerDu))

**Fixed bugs:**

- Parquet schema should allow scale == precision for decimal type [\#1606](https://github.com/apache/arrow-rs/issues/1606) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- ListArray::from\(ArrayData\) dereferences invalid pointer when offsets are empty [\#1601](https://github.com/apache/arrow-rs/issues/1601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ArrayData Equality Incorrect Null Mask Offset Handling [\#1599](https://github.com/apache/arrow-rs/issues/1599)
- Filtering UnionArray Incorrect Handles Runs [\#1598](https://github.com/apache/arrow-rs/issues/1598)
- \[Safety\] Filtering Dense UnionArray Produces Invalid Offsets [\#1596](https://github.com/apache/arrow-rs/issues/1596)
- \[Safety\] UnionBuilder Doesn't Check Types [\#1591](https://github.com/apache/arrow-rs/issues/1591)
- Union Layout Should Not Support Separate Validity Mask [\#1590](https://github.com/apache/arrow-rs/issues/1590)
- Incorrect nullable flag when reading maps \( test\_read\_maps fails when `force_validate` is active\)  [\#1587](https://github.com/apache/arrow-rs/issues/1587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Output of `ipc::reader::tests::projection_should_work` fails validation [\#1548](https://github.com/apache/arrow-rs/issues/1548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect min/max statistics for decimals with byte-array notation [\#1532](https://github.com/apache/arrow-rs/issues/1532)

**Documentation updates:**

- Minor: Clarify docs on `UnionBuilder::append_null` [\#1628](https://github.com/apache/arrow-rs/pull/1628) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Dense UnionArray Offsets Are i32 not i8 [\#1597](https://github.com/apache/arrow-rs/issues/1597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace `&Option<T>` with `Option<&T>` in some APIs [\#1556](https://github.com/apache/arrow-rs/issues/1556) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve ergonomics of `parquet::basic::LogicalType`  [\#1554](https://github.com/apache/arrow-rs/issues/1554) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Mark the current `substring` function as `unsafe` and rename it. [\#1541](https://github.com/apache/arrow-rs/issues/1541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Requirements for Async Parquet API [\#1473](https://github.com/apache/arrow-rs/issues/1473) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Nit: use the standard function `div_ceil`  [\#1629](https://github.com/apache/arrow-rs/pull/1629) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update flatbuffers requirement from =2.1.1 to =2.1.2 [\#1622](https://github.com/apache/arrow-rs/pull/1622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix decimals min max statistics [\#1621](https://github.com/apache/arrow-rs/pull/1621) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([atefsawaed](https://github.com/atefsawaed))
- Add example readme [\#1615](https://github.com/apache/arrow-rs/pull/1615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve docs and examples links on main readme [\#1614](https://github.com/apache/arrow-rs/pull/1614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Read/Write nested dictionaries under FixedSizeList in IPC [\#1610](https://github.com/apache/arrow-rs/pull/1610) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `substring` support for binary [\#1608](https://github.com/apache/arrow-rs/pull/1608) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Parquet: schema validation should allow scale == precision for decimal type [\#1607](https://github.com/apache/arrow-rs/pull/1607) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sunchao](https://github.com/sunchao))
- Don't access and validate offset buffer in ListArray::from\(ArrayData\) [\#1602](https://github.com/apache/arrow-rs/pull/1602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix map nullable flag in `ParquetTypeConverter` [\#1592](https://github.com/apache/arrow-rs/pull/1592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary under large list in ipc stream reader/writer [\#1585](https://github.com/apache/arrow-rs/pull/1585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary under map in ipc stream reader/writer [\#1583](https://github.com/apache/arrow-rs/pull/1583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Derive `Clone` and `PartialEq` for json `DecoderOptions` [\#1581](https://github.com/apache/arrow-rs/pull/1581) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add utf-8 validation checking for `substring` [\#1577](https://github.com/apache/arrow-rs/pull/1577) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use `Option<T>` rather than `Option<&T>` for copy types in substring kernel [\#1576](https://github.com/apache/arrow-rs/pull/1576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use littleendian arrow files for `projection_should_work` [\#1573](https://github.com/apache/arrow-rs/pull/1573) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))


## [12.0.0](https://github.com/apache/arrow-rs/tree/12.0.0) (2022-04-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/11.1.0...12.0.0)

**Breaking changes:**

- Add `ArrowReaderOptions` to `ParquetFileArrowReader`, add option to skip decoding arrow metadata from parquet \(\#1459\) [\#1558](https://github.com/apache/arrow-rs/pull/1558) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support `RecordBatch` with zero columns but non zero row count, add field to `RecordBatchOptions` \(\#1536\) [\#1552](https://github.com/apache/arrow-rs/pull/1552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Consolidate JSON Reader options and `DecoderOptions` [\#1539](https://github.com/apache/arrow-rs/pull/1539) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update `prost`, `prost-derive` and `prost-types` to 0.10, `tonic`, and `tonic-build` to `0.7` [\#1510](https://github.com/apache/arrow-rs/pull/1510) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Add Json `DecoderOptions` and support custom `format_string` for each field  [\#1451](https://github.com/apache/arrow-rs/pull/1451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))

**Implemented enhancements:**

- Read/write nested dictionary in ipc stream reader/writer [\#1565](https://github.com/apache/arrow-rs/issues/1565) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `FixedSizeBinary` in the Arrow C data interface [\#1553](https://github.com/apache/arrow-rs/issues/1553) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Empty Column Projection in `ParquetRecordBatchReader` [\#1537](https://github.com/apache/arrow-rs/issues/1537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `RecordBatch` with zero columns but non zero row count [\#1536](https://github.com/apache/arrow-rs/issues/1536) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `Date32`/`Date64`\<--\> `String`/`LargeString` in `cast` kernel [\#1535](https://github.com/apache/arrow-rs/issues/1535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support creating arrays from externally owned memory like `Vec` or `String` [\#1516](https://github.com/apache/arrow-rs/issues/1516) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up the `substring` kernel [\#1511](https://github.com/apache/arrow-rs/issues/1511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Handle Parquet Files With Inconsistent Timestamp Units [\#1459](https://github.com/apache/arrow-rs/issues/1459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Error Infering Schema for LogicalType::UNKNOWN [\#1557](https://github.com/apache/arrow-rs/issues/1557) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Read dictionary from nested struct in ipc stream reader panics [\#1549](https://github.com/apache/arrow-rs/issues/1549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `filter` produces invalid sparse `UnionArray`s [\#1547](https://github.com/apache/arrow-rs/issues/1547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Documentation for `GenericListBuilder` is not exposed.  [\#1518](https://github.com/apache/arrow-rs/issues/1518) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- cannot read parquet file  [\#1515](https://github.com/apache/arrow-rs/issues/1515) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `substring` kernel panics when chars \> U+0x007F [\#1478](https://github.com/apache/arrow-rs/issues/1478) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Hang due to infinite loop when reading some parquet files with RLE encoding and bit packing [\#1458](https://github.com/apache/arrow-rs/issues/1458) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Improve JSON reader documentation [\#1559](https://github.com/apache/arrow-rs/pull/1559) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve doc string for `substring` kernel [\#1529](https://github.com/apache/arrow-rs/pull/1529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Expose documentation of `GenericListBuilder` [\#1525](https://github.com/apache/arrow-rs/pull/1525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comath](https://github.com/comath))
- Add a diagram to `take` kernel documentation [\#1524](https://github.com/apache/arrow-rs/pull/1524) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Interesting benchmark results of `min_max_helper` [\#1400](https://github.com/apache/arrow-rs/issues/1400)

**Merged pull requests:**

- Fix incorrect `into_buffers` for UnionArray [\#1567](https://github.com/apache/arrow-rs/pull/1567) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary in ipc stream reader/writer [\#1566](https://github.com/apache/arrow-rs/pull/1566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support FixedSizeBinary and FixedSizeList for the C data interface [\#1564](https://github.com/apache/arrow-rs/pull/1564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Split out ListArrayReader into separate module \(\#1483\) [\#1563](https://github.com/apache/arrow-rs/pull/1563) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out `MapArray` into separate module \(\#1483\) [\#1562](https://github.com/apache/arrow-rs/pull/1562) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support empty projection in `ParquetRecordBatchReader` [\#1560](https://github.com/apache/arrow-rs/pull/1560) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- fix infinite loop in not fully packed bit-packed runs [\#1555](https://github.com/apache/arrow-rs/pull/1555) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add test for creating FixedSizeBinaryArray::try\_from\_sparse\_iter failed when given all Nones [\#1551](https://github.com/apache/arrow-rs/pull/1551) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix reading dictionaries from nested structs in ipc `StreamReader` [\#1550](https://github.com/apache/arrow-rs/pull/1550) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dispanser](https://github.com/dispanser))
- Add support for Date32/64 \<--\> String/LargeString in `cast` kernel [\#1534](https://github.com/apache/arrow-rs/pull/1534) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- fix clippy errors in 1.60 [\#1527](https://github.com/apache/arrow-rs/pull/1527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Mark `remove-old-releases.sh` executable [\#1522](https://github.com/apache/arrow-rs/pull/1522) ([alamb](https://github.com/alamb))
- Delete duplicate code in the `sort` kernel [\#1519](https://github.com/apache/arrow-rs/pull/1519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix reading nested lists from parquet files  [\#1517](https://github.com/apache/arrow-rs/pull/1517) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Speed up the `substring` kernel by about 2x [\#1512](https://github.com/apache/arrow-rs/pull/1512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add `new_from_strings` to create `MapArrays` [\#1507](https://github.com/apache/arrow-rs/pull/1507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Decouple buffer deallocation from ffi and allow creating buffers from rust vec [\#1494](https://github.com/apache/arrow-rs/pull/1494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))

## [11.1.0](https://github.com/apache/arrow-rs/tree/11.1.0) (2022-03-31)

[Full Changelog](https://github.com/apache/arrow-rs/compare/11.0.0...11.1.0)

**Implemented enhancements:**

- Implement `size_hint` and `ExactSizedIterator` for DecimalArray [\#1505](https://github.com/apache/arrow-rs/issues/1505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support calculate length by chars for `StringArray` [\#1493](https://github.com/apache/arrow-rs/issues/1493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `length` kernel support for `ListArray` [\#1470](https://github.com/apache/arrow-rs/issues/1470) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The length kernel should work with `BinaryArray`s [\#1464](https://github.com/apache/arrow-rs/issues/1464) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FFI for Arrow C Stream Interface [\#1348](https://github.com/apache/arrow-rs/issues/1348) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `DictionaryArray::try_new()` [\#1313](https://github.com/apache/arrow-rs/issues/1313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- MIRI error in math\_checked\_divide\_op/try\_from\_trusted\_len\_iter [\#1496](https://github.com/apache/arrow-rs/issues/1496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Writer Incorrect Definition Levels for Nested NullArray [\#1480](https://github.com/apache/arrow-rs/issues/1480) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FFI: ArrowArray::try\_from\_raw shouldn't clone [\#1425](https://github.com/apache/arrow-rs/issues/1425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet reader fails to read null list. [\#1399](https://github.com/apache/arrow-rs/issues/1399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- A small mistake in the doc of `BinaryArray` and `LargeBinaryArray` [\#1455](https://github.com/apache/arrow-rs/issues/1455) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- A small mistake in the doc of `GenericBinaryArray::take_iter_unchecked` [\#1454](https://github.com/apache/arrow-rs/issues/1454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add links in the doc of `BinaryOffsetSizeTrait` [\#1453](https://github.com/apache/arrow-rs/issues/1453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The doc of `FixedSizeBinaryArray` is confusing. [\#1452](https://github.com/apache/arrow-rs/issues/1452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clarify docs that SlicesIterator ignores null values [\#1504](https://github.com/apache/arrow-rs/pull/1504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update the doc of `BinaryArray` and `LargeBinaryArray` [\#1471](https://github.com/apache/arrow-rs/pull/1471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))

**Closed issues:**

- `packed_simd` v.s. `portable_simd`, which should be used? [\#1492](https://github.com/apache/arrow-rs/issues/1492)
- Cleanup: Use Arrow take kernel Within parquet ListArrayReader [\#1482](https://github.com/apache/arrow-rs/issues/1482) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Implement `size_hint` and `ExactSizedIterator` for `DecimalArray` [\#1506](https://github.com/apache/arrow-rs/pull/1506) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `StringArray::num_chars` for calculating number of characters [\#1503](https://github.com/apache/arrow-rs/pull/1503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Workaround nightly miri error in `try_from_trusted_len_iter` [\#1497](https://github.com/apache/arrow-rs/pull/1497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- update doc of array\_binary and array\_string [\#1491](https://github.com/apache/arrow-rs/pull/1491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use Arrow take kernel within ListArrayReader [\#1490](https://github.com/apache/arrow-rs/pull/1490) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add `length` kernel support for List Array [\#1488](https://github.com/apache/arrow-rs/pull/1488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support sort for `Decimal` data type [\#1487](https://github.com/apache/arrow-rs/pull/1487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Fix reading/writing nested null arrays \(\#1480\) \(\#1036\) \(\#1399\) [\#1481](https://github.com/apache/arrow-rs/pull/1481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
-  Implement ArrayEqual for UnionArray [\#1469](https://github.com/apache/arrow-rs/pull/1469) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support the `length` kernel on Binary Array [\#1465](https://github.com/apache/arrow-rs/pull/1465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove Clone and copy source structs internally [\#1449](https://github.com/apache/arrow-rs/pull/1449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Parquet reader for null lists [\#1448](https://github.com/apache/arrow-rs/pull/1448) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Improve performance of DictionaryArray::try\_new\(\)  [\#1435](https://github.com/apache/arrow-rs/pull/1435) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Add FFI for Arrow C Stream Interface [\#1384](https://github.com/apache/arrow-rs/pull/1384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

## [11.0.0](https://github.com/apache/arrow-rs/tree/11.0.0) (2022-03-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/10.0.0...11.0.0)

**Breaking changes:**

- Replace `filter_row_groups` with `ReadOptions` in parquet SerializedFileReader  [\#1389](https://github.com/apache/arrow-rs/pull/1389) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yjshen](https://github.com/yjshen))
- Implement projection for arrow `IPC Reader` file / streams [\#1339](https://github.com/apache/arrow-rs/pull/1339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Dandandan](https://github.com/Dandandan))

**Implemented enhancements:**

- Fix generate\_interval\_case integration test failure [\#1445](https://github.com/apache/arrow-rs/issues/1445)
- Make the doc examples of `ListArray` and `LargeListArray` more readable [\#1433](https://github.com/apache/arrow-rs/issues/1433)
- Redundant `if` and `abs` in `shift()` [\#1427](https://github.com/apache/arrow-rs/issues/1427)
- Improve substring kernel performance [\#1422](https://github.com/apache/arrow-rs/issues/1422) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add missing value\_unchecked\(\) of `FixedSizeBinaryArray` [\#1419](https://github.com/apache/arrow-rs/issues/1419)
- Remove duplicate bound check in function `shift` [\#1408](https://github.com/apache/arrow-rs/issues/1408)
- Support dictionary array in C data interface [\#1397](https://github.com/apache/arrow-rs/issues/1397)
- filter kernel should work with `UnionArray`s [\#1394](https://github.com/apache/arrow-rs/issues/1394) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- filter kernel should work with `FixedSizeListArrays`s [\#1393](https://github.com/apache/arrow-rs/issues/1393) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add doc examples for creating FixedSizeListArray [\#1392](https://github.com/apache/arrow-rs/issues/1392) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update `rust-version` to 1.59 [\#1377](https://github.com/apache/arrow-rs/issues/1377)
- Arrow IPC projection support [\#1338](https://github.com/apache/arrow-rs/issues/1338)
- Implement basic FlightSQL Server [\#1386](https://github.com/apache/arrow-rs/pull/1386) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([wangfenjin](https://github.com/wangfenjin))

**Fixed bugs:**

- DictionaryArray::try\_new ignores validity bitmap of the keys [\#1429](https://github.com/apache/arrow-rs/issues/1429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The doc of `GenericListArray` is confusing [\#1424](https://github.com/apache/arrow-rs/issues/1424)
- DeltaBitPackDecoder Incorrectly Handles Non-Zero MiniBlock Bit Width Padding [\#1417](https://github.com/apache/arrow-rs/issues/1417) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- DeltaBitPackEncoder Pads Miniblock BitWidths With Arbitrary Values [\#1416](https://github.com/apache/arrow-rs/issues/1416) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Possible unaligned write with MutableBuffer::push [\#1410](https://github.com/apache/arrow-rs/issues/1410) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Integration Test is failing on master branch [\#1398](https://github.com/apache/arrow-rs/issues/1398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Rewrite doc of `GenericListArray` [\#1450](https://github.com/apache/arrow-rs/pull/1450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix integration doc about build.ninja location [\#1438](https://github.com/apache/arrow-rs/pull/1438) ([viirya](https://github.com/viirya))

**Merged pull requests:**

- Rewrite doc example of `ListArray` and `LargeListArray` [\#1447](https://github.com/apache/arrow-rs/pull/1447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix generate\_interval\_case in integration test [\#1446](https://github.com/apache/arrow-rs/pull/1446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix generate\_decimal128\_case in integration test [\#1440](https://github.com/apache/arrow-rs/pull/1440) ([viirya](https://github.com/viirya))
- `filter` kernel should work with FixedSizeListArrays [\#1434](https://github.com/apache/arrow-rs/pull/1434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support nullable keys in DictionaryArray::try\_new [\#1430](https://github.com/apache/arrow-rs/pull/1430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- remove redundant if/clamp\_min/abs [\#1428](https://github.com/apache/arrow-rs/pull/1428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Add doc example for creating `FixedSizeListArray` [\#1426](https://github.com/apache/arrow-rs/pull/1426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Directly write to MutableBuffer in substring [\#1423](https://github.com/apache/arrow-rs/pull/1423) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix possibly unaligned writes in MutableBuffer [\#1421](https://github.com/apache/arrow-rs/pull/1421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add value\_unchecked\(\) and unit test [\#1420](https://github.com/apache/arrow-rs/pull/1420) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Fix DeltaBitPack MiniBlock Bit Width Padding [\#1418](https://github.com/apache/arrow-rs/pull/1418) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update zstd requirement from 0.10 to 0.11 [\#1415](https://github.com/apache/arrow-rs/pull/1415) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Set `default-features = false` for `zstd` in the parquet crate to support `wasm32-unknown-unknown` [\#1414](https://github.com/apache/arrow-rs/pull/1414) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Add support for `UnionArray` in`filter` kernel [\#1412](https://github.com/apache/arrow-rs/pull/1412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove duplicate bound check in the function `shift` [\#1409](https://github.com/apache/arrow-rs/pull/1409) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add dictionary support for C data interface [\#1407](https://github.com/apache/arrow-rs/pull/1407) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Fix a small spelling mistake in docs. [\#1406](https://github.com/apache/arrow-rs/pull/1406) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add unit test to check `FixedSizeBinaryArray` input all none [\#1405](https://github.com/apache/arrow-rs/pull/1405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Move csv Parser trait and its implementations to utils module [\#1385](https://github.com/apache/arrow-rs/pull/1385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))

## [10.0.0](https://github.com/apache/arrow-rs/tree/10.0.0) (2022-03-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/9.1.0...10.0.0)

**Breaking changes:**

- Remove existing has\_ methods for optional fields in `ColumnChunkMetaData` [\#1346](https://github.com/apache/arrow-rs/pull/1346) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Remove redundant `has_` methods in `ColumnChunkMetaData` [\#1345](https://github.com/apache/arrow-rs/pull/1345) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))

**Implemented enhancements:**

- Add extract month and day  in temporal.rs [\#1387](https://github.com/apache/arrow-rs/issues/1387)
- Add clone to `IpcWriteOptions` [\#1381](https://github.com/apache/arrow-rs/issues/1381) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `MapArray` in `filter` kernel  [\#1378](https://github.com/apache/arrow-rs/issues/1378) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `week` temporal kernel [\#1375](https://github.com/apache/arrow-rs/issues/1375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `compare_dict_op` [\#1371](https://github.com/apache/arrow-rs/issues/1371) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for LargeUtf8 in json writer [\#1357](https://github.com/apache/arrow-rs/issues/1357) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `arrow::array::builder::MapBuilder` public [\#1354](https://github.com/apache/arrow-rs/issues/1354) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor `StructArray::from` [\#1351](https://github.com/apache/arrow-rs/issues/1351) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor `RecordBatch::validate_new_batch` [\#1350](https://github.com/apache/arrow-rs/issues/1350) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove redundant has\_ methods for optional column metadata fields [\#1344](https://github.com/apache/arrow-rs/issues/1344) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `write` method to JsonWriter [\#1340](https://github.com/apache/arrow-rs/issues/1340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor the code of `Bitmap::new` [\#1337](https://github.com/apache/arrow-rs/issues/1337) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Use DictionaryArray's iterator in `compare_dict_op` [\#1329](https://github.com/apache/arrow-rs/issues/1329) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add  `as_decimal_array(arr: &dyn Array) -> &DecimalArray` [\#1312](https://github.com/apache/arrow-rs/issues/1312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- More ergonomic / idiomatic primitive array creation from iterators [\#1298](https://github.com/apache/arrow-rs/issues/1298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement DictionaryArray support in `eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#1201](https://github.com/apache/arrow-rs/issues/1201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- `cargo clippy` fails on the `master` branch [\#1362](https://github.com/apache/arrow-rs/issues/1362) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `ArrowArray::try_from_raw` should not assume the pointers are from Arc [\#1333](https://github.com/apache/arrow-rs/issues/1333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix CSV Writer::new to accept delimiter and make WriterBuilder::build use it   [\#1328](https://github.com/apache/arrow-rs/issues/1328) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make bounds configurable via builder when reading CSV [\#1327](https://github.com/apache/arrow-rs/issues/1327) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `with_datetime_format()` to CSV WriterBuilder  [\#1272](https://github.com/apache/arrow-rs/issues/1272) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Performance improvements:**

- Improve performance of `min` and `max` aggregation kernels without nulls [\#1373](https://github.com/apache/arrow-rs/issues/1373) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Consider removing redundant has\_XXX metadata functions in `ColumnChunkMetadata` [\#1332](https://github.com/apache/arrow-rs/issues/1332)

**Merged pull requests:**

- Support extract `day` and `month` in temporal.rs [\#1388](https://github.com/apache/arrow-rs/pull/1388) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add write method to Json Writer [\#1383](https://github.com/apache/arrow-rs/pull/1383) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- Derive `Clone` for  `IpcWriteOptions` [\#1382](https://github.com/apache/arrow-rs/pull/1382) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- feat: support maps in MutableArrayData [\#1379](https://github.com/apache/arrow-rs/pull/1379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))
- Support extract `week` in temporal.rs [\#1376](https://github.com/apache/arrow-rs/pull/1376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Speed up the function `min_max_string` [\#1374](https://github.com/apache/arrow-rs/pull/1374) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Improve performance if dictionary kernels, add benchmark and add `take_iter_unchecked` [\#1372](https://github.com/apache/arrow-rs/pull/1372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update pyo3 requirement from 0.15 to 0.16 [\#1369](https://github.com/apache/arrow-rs/pull/1369) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update contributing guide [\#1368](https://github.com/apache/arrow-rs/pull/1368) ([HaoYang670](https://github.com/HaoYang670))
- Allow primitive array creation from iterators of PrimitiveTypes \(as well as `Option`\) [\#1367](https://github.com/apache/arrow-rs/pull/1367) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update flatbuffers requirement from =2.1.0 to =2.1.1 [\#1364](https://github.com/apache/arrow-rs/pull/1364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix clippy lints [\#1363](https://github.com/apache/arrow-rs/pull/1363) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor `RecordBatch::validate_new_batch` [\#1361](https://github.com/apache/arrow-rs/pull/1361) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor `StructArray::from` [\#1360](https://github.com/apache/arrow-rs/pull/1360) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update flatbuffers requirement from =2.0.0 to =2.1.0 [\#1359](https://github.com/apache/arrow-rs/pull/1359) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: add LargeUtf8 support in json writer [\#1358](https://github.com/apache/arrow-rs/pull/1358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tiphaineruy](https://github.com/tiphaineruy))
- Add `as_decimal_array` function [\#1356](https://github.com/apache/arrow-rs/pull/1356) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Publicly export arrow::array::MapBuilder [\#1355](https://github.com/apache/arrow-rs/pull/1355) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tjwilson90](https://github.com/tjwilson90))
- Add with\_datetime\_format to csv WriterBuilder [\#1347](https://github.com/apache/arrow-rs/pull/1347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Refactor `Bitmap::new` [\#1343](https://github.com/apache/arrow-rs/pull/1343) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove delimiter from csv Writer [\#1342](https://github.com/apache/arrow-rs/pull/1342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Make bounds configurable in csv ReaderBuilder [\#1341](https://github.com/apache/arrow-rs/pull/1341) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- `ArrowArray::try_from_raw` should not assume the pointers are from Arc [\#1334](https://github.com/apache/arrow-rs/pull/1334) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use DictionaryArray's iterator in `compare_dict_op` [\#1330](https://github.com/apache/arrow-rs/pull/1330) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Implement DictionaryArray support in neq\_dyn, lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn [\#1326](https://github.com/apache/arrow-rs/pull/1326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Arrow Rust + Conbench Integration [\#1289](https://github.com/apache/arrow-rs/pull/1289) ([dianaclarke](https://github.com/dianaclarke))

## [9.1.0](https://github.com/apache/arrow-rs/tree/9.1.0) (2022-02-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/9.0.2...9.1.0)

**Implemented enhancements:**

- Exposing page encoding stats [\#1321](https://github.com/apache/arrow-rs/issues/1321)
- Improve filter performance by special casing high and low selectivity predicates [\#1288](https://github.com/apache/arrow-rs/issues/1288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up  `DeltaBitPackDecoder` [\#1281](https://github.com/apache/arrow-rs/issues/1281) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Fix all clippy lints in arrow crate [\#1255](https://github.com/apache/arrow-rs/issues/1255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose page encoding `ColumnChunkMetadata` [\#1322](https://github.com/apache/arrow-rs/pull/1322) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Expose column index and offset index in `ColumnChunkMetadata` [\#1318](https://github.com/apache/arrow-rs/pull/1318) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Expose bloom filter offset in `ColumnChunkMetadata` [\#1309](https://github.com/apache/arrow-rs/pull/1309) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Add `DictionaryArray::try_new()` to create dictionaries from pre existing arrays [\#1300](https://github.com/apache/arrow-rs/pull/1300) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `DictionaryArray::keys_iter`, and `take_iter` for other array types [\#1296](https://github.com/apache/arrow-rs/pull/1296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make `rle` decoder public under `experimental` feature [\#1271](https://github.com/apache/arrow-rs/pull/1271) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Add `DictionaryArray` support in `eq_dyn` kernel [\#1263](https://github.com/apache/arrow-rs/pull/1263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Fixed bugs:**

- `len` is not a parameter of `MutableArrayData::extend` [\#1316](https://github.com/apache/arrow-rs/issues/1316)
- module `data_type` is private in Rust Parquet 8.0.0 [\#1302](https://github.com/apache/arrow-rs/issues/1302) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Test failure: bit\_chunk\_iterator [\#1294](https://github.com/apache/arrow-rs/issues/1294)
- csv\_writer benchmark fails with "no such file or directory" [\#1292](https://github.com/apache/arrow-rs/issues/1292)

**Documentation updates:**

- Fix warnings in `cargo doc` [\#1268](https://github.com/apache/arrow-rs/pull/1268) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Vectorize DeltaBitPackDecoder, up to 5x faster decoding [\#1284](https://github.com/apache/arrow-rs/pull/1284) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Skip zero-ing primitive nulls [\#1280](https://github.com/apache/arrow-rs/pull/1280) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add specialized filter kernels in `compute` module \(up to 10x faster\) [\#1248](https://github.com/apache/arrow-rs/pull/1248) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- Expose column and offset index metadata offset [\#1317](https://github.com/apache/arrow-rs/issues/1317)
- Expose bloom filter metadata offset [\#1308](https://github.com/apache/arrow-rs/issues/1308)
- Improve ergonomics to construct `DictionaryArrays` from `Key` and `Value` arrays [\#1299](https://github.com/apache/arrow-rs/issues/1299)
- Make it easier to iterate over `DictionaryArray` [\#1295](https://github.com/apache/arrow-rs/issues/1295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- (WON'T FIX) Don't Interwine Bit and Byte Aligned Operations in `BitReader` [\#1282](https://github.com/apache/arrow-rs/issues/1282)
- how to create arrow::array from streamReader [\#1278](https://github.com/apache/arrow-rs/issues/1278)
- Remove scientific notation when converting floats to strings. [\#983](https://github.com/apache/arrow-rs/issues/983)

**Merged pull requests:**

- Update the document of function `MutableArrayData::extend` [\#1336](https://github.com/apache/arrow-rs/pull/1336) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix clippy lint `dead_code` [\#1324](https://github.com/apache/arrow-rs/pull/1324) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- fix test bug and ensure that bloom filter metadata is serialized in `to_thrift` [\#1320](https://github.com/apache/arrow-rs/pull/1320) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Enable more clippy lints in arrow  [\#1315](https://github.com/apache/arrow-rs/pull/1315) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix clippy lint `clippy::type_complexity` [\#1310](https://github.com/apache/arrow-rs/pull/1310) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix clippy lint `clippy::float_equality_without_abs` [\#1305](https://github.com/apache/arrow-rs/pull/1305) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix clippy `clippy::vec_init_then_push` lint [\#1303](https://github.com/apache/arrow-rs/pull/1303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix failing csv\_writer bench [\#1293](https://github.com/apache/arrow-rs/pull/1293) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Changes for 9.0.2  [\#1291](https://github.com/apache/arrow-rs/pull/1291) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Fix bitmask creation also for simd comparisons with scalar [\#1290](https://github.com/apache/arrow-rs/pull/1290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix simd comparison kernels [\#1286](https://github.com/apache/arrow-rs/pull/1286) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Restrict Decoder to compatible types \(\#1276\) [\#1277](https://github.com/apache/arrow-rs/pull/1277) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix some clippy lints in parquet crate, rename `LevelEncoder` variants to conform to Rust standards [\#1273](https://github.com/apache/arrow-rs/pull/1273) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HaoYang670](https://github.com/HaoYang670))
- Use new DecimalArray creation API in arrow crate [\#1249](https://github.com/apache/arrow-rs/pull/1249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `DecimalArray` API ergonomics: add `iter()`, `FromIterator`, `with_precision_and_scale` [\#1223](https://github.com/apache/arrow-rs/pull/1223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))


## [9.0.2](https://github.com/apache/arrow-rs/tree/9.0.2) (2022-02-09)

[Full Changelog](https://github.com/apache/arrow-rs/compare/8.0.0...9.0.2)

**Breaking changes:**

- Add  `Send` + `Sync` to `DataType`, `RowGroupReader`, `FileReader`, `ChunkReader`. [\#1264](https://github.com/apache/arrow-rs/issues/1264)
- Rename the function `Bitmap::len` to `Bitmap::bit_len` to clarify its meaning [\#1242](https://github.com/apache/arrow-rs/pull/1242) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove unused / broken `memory-check` feature [\#1222](https://github.com/apache/arrow-rs/pull/1222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Potentially buffer  multiple `RecordBatches` before writing a parquet row group in `ArrowWriter` [\#1214](https://github.com/apache/arrow-rs/pull/1214) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Add `async` arrow parquet reader [\#1154](https://github.com/apache/arrow-rs/pull/1154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `Bitmap::len` to `Bitmap::bit_len` [\#1233](https://github.com/apache/arrow-rs/issues/1233)
- Extend CSV schema inference to allow scientific notation for floating point types [\#1215](https://github.com/apache/arrow-rs/issues/1215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Multiple RecordBatch to Parquet Row Group [\#1211](https://github.com/apache/arrow-rs/issues/1211)
- Add doc examples for `eq_dyn` etc. [\#1202](https://github.com/apache/arrow-rs/issues/1202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add comparison kernels for `BinaryArray` [\#1108](https://github.com/apache/arrow-rs/issues/1108)
- `impl ArrowNativeType for i128`  [\#1098](https://github.com/apache/arrow-rs/issues/1098)
- Remove `Copy` trait bound from dyn scalar kernels [\#1243](https://github.com/apache/arrow-rs/pull/1243) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- Add `into_inner` for IPC `FileWriter` [\#1236](https://github.com/apache/arrow-rs/pull/1236) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- \[Minor\]Re-export `array::builder::make_builder` to make it available for downstream [\#1235](https://github.com/apache/arrow-rs/pull/1235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))

**Fixed bugs:**

- Parquet v8.0.0 panics when reading all null column to NullArray [\#1245](https://github.com/apache/arrow-rs/issues/1245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Get `Unknown configuration option rust-version` when running the rust format command [\#1240](https://github.com/apache/arrow-rs/issues/1240)
- `Bitmap` Length Validation is Incorrect [\#1231](https://github.com/apache/arrow-rs/issues/1231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Writing sliced `ListArray` or `MapArray` ignore offsets [\#1226](https://github.com/apache/arrow-rs/issues/1226) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove broken `memory-tracking` crate feature [\#1171](https://github.com/apache/arrow-rs/issues/1171)
- Revert making `parquet::data_type` and `parquet::arrow::schema` experimental [\#1244](https://github.com/apache/arrow-rs/pull/1244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Documentation updates:**

- Update parquet crate documentation and examples [\#1253](https://github.com/apache/arrow-rs/pull/1253) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Refresh parquet readme / contributing guide [\#1252](https://github.com/apache/arrow-rs/pull/1252) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add docs examples for dynamically compare functions  [\#1250](https://github.com/apache/arrow-rs/pull/1250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add Rust Docs examples for UnionArray [\#1241](https://github.com/apache/arrow-rs/pull/1241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Improve documentation for Bitmap [\#1237](https://github.com/apache/arrow-rs/pull/1237) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve performance for arithmetic kernels with `simd` feature enabled \(except for division/modulo\) [\#1221](https://github.com/apache/arrow-rs/pull/1221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Do not concatenate identical dictionaries [\#1219](https://github.com/apache/arrow-rs/pull/1219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preserve dictionary encoding when decoding parquet into Arrow arrays, 60x perf improvement \(\#171\) [\#1180](https://github.com/apache/arrow-rs/pull/1180) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- `UnalignedBitChunkIterator` to that iterates through already aligned `u64` blocks [\#1227](https://github.com/apache/arrow-rs/issues/1227)
- Remove unused `ArrowArrayReader` in parquet  [\#1197](https://github.com/apache/arrow-rs/issues/1197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Upgrade clap to 3.0.0 [\#1261](https://github.com/apache/arrow-rs/pull/1261) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Update chrono-tz requirement from 0.4 to 0.6 [\#1259](https://github.com/apache/arrow-rs/pull/1259) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update zstd requirement from 0.9 to 0.10 [\#1257](https://github.com/apache/arrow-rs/pull/1257) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix NullArrayReader \(\#1245\) [\#1246](https://github.com/apache/arrow-rs/pull/1246) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- dyn compare for binary array [\#1238](https://github.com/apache/arrow-rs/pull/1238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove arrow array reader \(\#1197\) [\#1234](https://github.com/apache/arrow-rs/pull/1234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix null bitmap length validation \(\#1231\) [\#1232](https://github.com/apache/arrow-rs/pull/1232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster bitmask iteration [\#1228](https://github.com/apache/arrow-rs/pull/1228) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add non utf8 values into the test cases of BinaryArray comparison [\#1220](https://github.com/apache/arrow-rs/pull/1220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update DECIMAL\_RE to allow scientific notation in auto inferred schemas [\#1216](https://github.com/apache/arrow-rs/pull/1216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pjmore](https://github.com/pjmore))
- Fix simd comparison kernels [\#1286](https://github.com/apache/arrow-rs/pull/1286) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix bitmask creation also for simd comparisons with scalar [\#1290](https://github.com/apache/arrow-rs/pull/1290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))

## [8.0.0](https://github.com/apache/arrow-rs/tree/8.0.0) (2022-01-20)

[Full Changelog](https://github.com/apache/arrow-rs/compare/7.0.0...8.0.0)

**Breaking changes:**

- Return error from JSON writer rather than panic [\#1205](https://github.com/apache/arrow-rs/pull/1205) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Remove `ArrowSignedNumericType ` to Simplify and reduce code duplication in arithmetic kernels [\#1161](https://github.com/apache/arrow-rs/pull/1161) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Restrict RecordReader and friends to scalar types \(\#1132\) [\#1155](https://github.com/apache/arrow-rs/pull/1155) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Move more parquet functionality behind experimental feature flag \(\#1032\)  [\#1134](https://github.com/apache/arrow-rs/pull/1134) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Parquet reader should be able to read structs within list [\#1186](https://github.com/apache/arrow-rs/issues/1186) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Disable serde\_json `arbitrary_precision` feature flag [\#1174](https://github.com/apache/arrow-rs/issues/1174) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Simplify and reduce code duplication in arithmetic.rs [\#1160](https://github.com/apache/arrow-rs/issues/1160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Return `Err` from JSON writer rather than `panic!` for unsupported types [\#1157](https://github.com/apache/arrow-rs/issues/1157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `scalar` mathematics kernels for `Array` and scalar value [\#1153](https://github.com/apache/arrow-rs/issues/1153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `DecimalArray` in sort kernel [\#1137](https://github.com/apache/arrow-rs/issues/1137)
- Parquet Fuzz Tests [\#1053](https://github.com/apache/arrow-rs/issues/1053)
- BooleanBufferBuilder Append Packed [\#1038](https://github.com/apache/arrow-rs/issues/1038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet Performance Optimization: StructArrayReader Redundant Level & Bitmap Computation [\#1034](https://github.com/apache/arrow-rs/issues/1034) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Reduce Public Parquet API [\#1032](https://github.com/apache/arrow-rs/issues/1032) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `from_iter_values` for binary array [\#1188](https://github.com/apache/arrow-rs/pull/1188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Add support for `MapArray` in json writer [\#1149](https://github.com/apache/arrow-rs/pull/1149) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))

**Fixed bugs:**

- Empty string arrays with no nulls are not equal [\#1208](https://github.com/apache/arrow-rs/issues/1208) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pretty print a `RecordBatch` containing `Float16` triggers a panic [\#1193](https://github.com/apache/arrow-rs/issues/1193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Writing structs nested in lists produces an incorrect output [\#1184](https://github.com/apache/arrow-rs/issues/1184) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Undefined behavior for `GenericStringArray::from_iter_values` if reported iterator upper bound is incorrect [\#1144](https://github.com/apache/arrow-rs/issues/1144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Interval comparisons with `simd` feature asserts [\#1136](https://github.com/apache/arrow-rs/issues/1136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordReader Permits Illegal Types [\#1132](https://github.com/apache/arrow-rs/issues/1132) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Security fixes:**

- Fix undefined behavor in GenericStringArray::from\_iter\_values [\#1145](https://github.com/apache/arrow-rs/pull/1145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
-  parquet: Optimized ByteArrayReader, Add UTF-8 Validation \(\#1040\)  [\#1082](https://github.com/apache/arrow-rs/pull/1082) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Documentation updates:**

- Update parquet crate readme [\#1192](https://github.com/apache/arrow-rs/pull/1192) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Document safety justification of some uses of `from_trusted_len_iter` [\#1148](https://github.com/apache/arrow-rs/pull/1148) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve parquet reading performance for columns with nulls by preserving bitmask when possible \(\#1037\) [\#1054](https://github.com/apache/arrow-rs/pull/1054) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve parquet performance: Skip levels computation for required struct arrays in parquet [\#1035](https://github.com/apache/arrow-rs/pull/1035) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- Generify ColumnReaderImpl and RecordReader [\#1040](https://github.com/apache/arrow-rs/issues/1040) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Preserve BitMask [\#1037](https://github.com/apache/arrow-rs/issues/1037)

**Merged pull requests:**

- fix a bug in variable sized equality [\#1209](https://github.com/apache/arrow-rs/pull/1209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))
- Pin WASM / packed SIMD tests to nightly-2022-01-17 [\#1204](https://github.com/apache/arrow-rs/pull/1204) ([alamb](https://github.com/alamb))
- feat: add support for casting Duration/Interval to Int64Array [\#1196](https://github.com/apache/arrow-rs/pull/1196) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([e-dard](https://github.com/e-dard))
- Add comparison support for fully qualified BinaryArray [\#1195](https://github.com/apache/arrow-rs/pull/1195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix in display of `Float16Array` [\#1194](https://github.com/apache/arrow-rs/pull/1194) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))
- update nightly version for miri [\#1189](https://github.com/apache/arrow-rs/pull/1189) ([Jimexist](https://github.com/Jimexist))
- feat\(parquet\): support for reading structs nested within lists [\#1187](https://github.com/apache/arrow-rs/pull/1187) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([helgikrs](https://github.com/helgikrs))
- fix: Fix a bug in how definition levels are calculated for nested structs in a list [\#1185](https://github.com/apache/arrow-rs/pull/1185) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([helgikrs](https://github.com/helgikrs))
- Truncate bitmask on BooleanBufferBuilder::resize:  [\#1183](https://github.com/apache/arrow-rs/pull/1183) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ticket reference for false positive in clippy [\#1181](https://github.com/apache/arrow-rs/pull/1181) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix record formatting in 1.58 [\#1178](https://github.com/apache/arrow-rs/pull/1178) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Serialize i128 as JSON string [\#1175](https://github.com/apache/arrow-rs/pull/1175) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support DecimalType in `sort` and `take` kernels [\#1172](https://github.com/apache/arrow-rs/pull/1172) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Fix new clippy lints introduced in Rust 1.58 [\#1170](https://github.com/apache/arrow-rs/pull/1170) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix compilation error with simd feature [\#1169](https://github.com/apache/arrow-rs/pull/1169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix bug while writing parquet with empty lists of structs [\#1166](https://github.com/apache/arrow-rs/pull/1166) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([helgikrs](https://github.com/helgikrs))
- Use tempfile for parquet tests [\#1165](https://github.com/apache/arrow-rs/pull/1165) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove left over dev/README.md file from arrow/arrow-rs split [\#1162](https://github.com/apache/arrow-rs/pull/1162) ([alamb](https://github.com/alamb))
- Add multiply\_scalar kernel [\#1159](https://github.com/apache/arrow-rs/pull/1159) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fuzz test different parquet encodings [\#1156](https://github.com/apache/arrow-rs/pull/1156) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add subtract\_scalar kernel [\#1152](https://github.com/apache/arrow-rs/pull/1152) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add add\_scalar kernel [\#1151](https://github.com/apache/arrow-rs/pull/1151) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Move simd right out of for\_each loop [\#1150](https://github.com/apache/arrow-rs/pull/1150) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Internal Remove `GenericStringArray::from_vec` and `GenericStringArray::from_opt_vec` [\#1147](https://github.com/apache/arrow-rs/pull/1147) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement SIMD comparison operations for types with less than 4 lanes \(i128\) [\#1146](https://github.com/apache/arrow-rs/pull/1146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Extends parquet fuzz tests to also tests nulls, dictionaries and row groups with multiple pages  \(\#1053\) [\#1110](https://github.com/apache/arrow-rs/pull/1110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
-  Generify ColumnReaderImpl and RecordReader \(\#1040\)  [\#1041](https://github.com/apache/arrow-rs/pull/1041) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- BooleanBufferBuilder::append\_packed \(\#1038\) [\#1039](https://github.com/apache/arrow-rs/pull/1039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [7.0.0](https://github.com/apache/arrow-rs/tree/7.0.0) (2022-1-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/6.5.0...7.0.0)

### Arrow

**Breaking changes:**
- `pretty_format_batches` now returns `Result<impl Display>` rather than `String`: [#975](https://github.com/apache/arrow-rs/pull/975)
- `MutableBuffer::typed_data_mut` is marked `unsafe`: [#1029](https://github.com/apache/arrow-rs/pull/1029)
- UnionArray updated match latest Arrow spec, added `UnionMode`, `UnionArray::new()` marked `unsafe`: [#885](https://github.com/apache/arrow-rs/pull/885)

**New Features:**
- Support for `Float16Array` types [#888](https://github.com/apache/arrow-rs/pull/888)
- IPC support for `UnionArray` [#654](https://github.com/apache/arrow-rs/issues/654)
- Dynamic comparison kernels for scalars (e.g. `eq_dyn_scalar`), including `DictionaryArray`: [#1113](https://github.com/apache/arrow-rs/issues/1113)

**Enhancements:**
- Added `Schema::with_metadata` and `Field::with_metadata` [#1092](https://github.com/apache/arrow-rs/pull/1092)
- Support for custom datetime format for inference and parsing csv files [#1112](https://github.com/apache/arrow-rs/pull/1112)
- Implement `Array` for `ArrayRef` for easier use [#1129](https://github.com/apache/arrow-rs/pull/1129)
- Pretty printing display support for `FixedSizeBinaryArray` [#1097](https://github.com/apache/arrow-rs/pull/1097)
- Dependency Upgrades: `pyo3`, `parquet-format`, `prost`, `tonic`
- Avoid allocating vector of indices in `lexicographical_partition_ranges`[#998](https://github.com/apache/arrow-rs/pull/998)

### Parquet

**Fixed bugs:**
- (parquet) Fix reading of dictionary encoded pages with null values: [#1130](https://github.com/apache/arrow-rs/pull/1130)


# Changelog

## [6.5.0](https://github.com/apache/arrow-rs/tree/6.5.0) (2021-12-23)

[Full Changelog](https://github.com/apache/arrow-rs/compare/6.4.0...6.5.0)

* [092fc64bbb019244887ebd0d9c9a2d3e3a9aebc0](https://github.com/apache/arrow-rs/commit/092fc64bbb019244887ebd0d9c9a2d3e3a9aebc0) support cast decimal to decimal ([#1084](https://github.com/apache/arrow-rs/pull/1084)) ([#1093](https://github.com/apache/arrow-rs/pull/1093))
* [01459762ed18b504e00e7b2818fce91f19188b1e](https://github.com/apache/arrow-rs/commit/01459762ed18b504e00e7b2818fce91f19188b1e) Fix like regex escaping ([#1085](https://github.com/apache/arrow-rs/pull/1085)) ([#1090](https://github.com/apache/arrow-rs/pull/1090))
* [7c748bfccbc2eac0c1138378736b70dcb7e26a5b](https://github.com/apache/arrow-rs/commit/7c748bfccbc2eac0c1138378736b70dcb7e26a5b) support cast decimal to signed numeric ([#1073](https://github.com/apache/arrow-rs/pull/1073)) ([#1089](https://github.com/apache/arrow-rs/pull/1089))
* [bd3600b6483c253ae57a38928a636d39a6b7cb02](https://github.com/apache/arrow-rs/commit/bd3600b6483c253ae57a38928a636d39a6b7cb02) parquet: Use constant for RLE decoder buffer size ([#1070](https://github.com/apache/arrow-rs/pull/1070)) ([#1088](https://github.com/apache/arrow-rs/pull/1088))
* [2b5c53ecd92468fd95328637a15de7f35b6fcf28](https://github.com/apache/arrow-rs/commit/2b5c53ecd92468fd95328637a15de7f35b6fcf28) Box RleDecoder index buffer ([#1061](https://github.com/apache/arrow-rs/pull/1061)) ([#1062](https://github.com/apache/arrow-rs/pull/1062)) ([#1081](https://github.com/apache/arrow-rs/pull/1081))
* [78721bc1a467177679ad6196b994759cf4d73377](https://github.com/apache/arrow-rs/commit/78721bc1a467177679ad6196b994759cf4d73377) BooleanBufferBuilder correct buffer length ([#1051](https://github.com/apache/arrow-rs/pull/1051)) ([#1052](https://github.com/apache/arrow-rs/pull/1052)) ([#1080](https://github.com/apache/arrow-rs/pull/1080))
* [3a5e3541d3a4db61a828011ed95c8539adf1d57c](https://github.com/apache/arrow-rs/commit/3a5e3541d3a4db61a828011ed95c8539adf1d57c) support cast signed numeric to decimal ([#1044](https://github.com/apache/arrow-rs/pull/1044)) ([#1079](https://github.com/apache/arrow-rs/pull/1079))
* [000bdb3053098255d43288aa3e8665e8b1892a6c](https://github.com/apache/arrow-rs/commit/000bdb3053098255d43288aa3e8665e8b1892a6c) fix(compute): LIKE escape parenthesis ([#1042](https://github.com/apache/arrow-rs/pull/1042)) ([#1078](https://github.com/apache/arrow-rs/pull/1078))
* [e0abdb9e62772a2f853974e68e744246e7f47569](https://github.com/apache/arrow-rs/commit/e0abdb9e62772a2f853974e68e744246e7f47569) Add Schema::project and RecordBatch::project functions  ([#1033](https://github.com/apache/arrow-rs/pull/1033)) ([#1077](https://github.com/apache/arrow-rs/pull/1077))
* [31911a4d6328d889d98796b896412b3997f73e13](https://github.com/apache/arrow-rs/commit/31911a4d6328d889d98796b896412b3997f73e13) Remove outdated safety example from doc ([#1050](https://github.com/apache/arrow-rs/pull/1050)) ([#1058](https://github.com/apache/arrow-rs/pull/1058))
* [71ac8620993a65a7f1f57278c3495556625356b3](https://github.com/apache/arrow-rs/commit/71ac8620993a65a7f1f57278c3495556625356b3) Use existing array type in `take` kernel ([#1046](https://github.com/apache/arrow-rs/pull/1046)) ([#1057](https://github.com/apache/arrow-rs/pull/1057))
* [1c5902376b7f7d56cb5249db4f98a6a370ead919](https://github.com/apache/arrow-rs/commit/1c5902376b7f7d56cb5249db4f98a6a370ead919) Extract method to drive PageIterator -> RecordReader ([#1031](https://github.com/apache/arrow-rs/pull/1031)) ([#1056](https://github.com/apache/arrow-rs/pull/1056))
* [7ca39361f8733b86bc0cef5ed5d74093e2c6b14d](https://github.com/apache/arrow-rs/commit/7ca39361f8733b86bc0cef5ed5d74093e2c6b14d) Clarify governance of arrow crate ([#1030](https://github.com/apache/arrow-rs/pull/1030)) ([#1055](https://github.com/apache/arrow-rs/pull/1055))


## [6.4.0](https://github.com/apache/arrow-rs/tree/6.4.0) (2021-12-10)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.3.0...6.4.0)


* [049f48559f578243935b6e512d06c4c2df360bf1](https://github.com/apache/arrow-rs/commit/049f48559f578243935b6e512d06c4c2df360bf1) Force new cargo and target caching to fix CI ([#1023](https://github.com/apache/arrow-rs/pull/1023)) ([#1024](https://github.com/apache/arrow-rs/pull/1024))
* [ef37da3b60f71a52d5ad67e9ca810dca38b29f00](https://github.com/apache/arrow-rs/commit/ef37da3b60f71a52d5ad67e9ca810dca38b29f00) Fix a broken link and some missing styling in the main arrow crate docs ([#1013](https://github.com/apache/arrow-rs/pull/1013)) ([#1019](https://github.com/apache/arrow-rs/pull/1019))
* [f2c746a9b968714cfe05d35fcee8658371acd899](https://github.com/apache/arrow-rs/commit/f2c746a9b968714cfe05d35fcee8658371acd899) Remove out of date comment ([#1008](https://github.com/apache/arrow-rs/pull/1008)) ([#1018](https://github.com/apache/arrow-rs/pull/1018))
* [557fc11e3b2a09a680c0cfbf38d27b13101b63fe](https://github.com/apache/arrow-rs/commit/557fc11e3b2a09a680c0cfbf38d27b13101b63fe) Remove unneeded `rc` feature of serde ([#990](https://github.com/apache/arrow-rs/pull/990)) ([#1016](https://github.com/apache/arrow-rs/pull/1016))
* [b28385e096b1cf8f5fb2773d49b160f93d94fbac](https://github.com/apache/arrow-rs/commit/b28385e096b1cf8f5fb2773d49b160f93d94fbac) Docstrings for Timestamp*Array. ([#988](https://github.com/apache/arrow-rs/pull/988)) ([#1015](https://github.com/apache/arrow-rs/pull/1015))
* [a92672e40217670d2566a85d70b0b59fffac594c](https://github.com/apache/arrow-rs/commit/a92672e40217670d2566a85d70b0b59fffac594c) Add full data validation for ArrayData::try_new() ([#1007](https://github.com/apache/arrow-rs/pull/1007))
* [6c8b2936d7b07e1e2f5d1d48eea425a385382dfb](https://github.com/apache/arrow-rs/commit/6c8b2936d7b07e1e2f5d1d48eea425a385382dfb) Add boolean comparison to scalar kernels for less then, greater than ([#977](https://github.com/apache/arrow-rs/pull/977)) ([#1005](https://github.com/apache/arrow-rs/pull/1005))
* [14d140aeca608a23a8a6b2c251c8f53ffd377e61](https://github.com/apache/arrow-rs/commit/14d140aeca608a23a8a6b2c251c8f53ffd377e61) Fix some typos in code and comments ([#985](https://github.com/apache/arrow-rs/pull/985)) ([#1006](https://github.com/apache/arrow-rs/pull/1006))
* [b4507f562fb0eddfb79840871cd2733dc0e337cd](https://github.com/apache/arrow-rs/commit/b4507f562fb0eddfb79840871cd2733dc0e337cd) Fix warnings introduced by Rust/Clippy 1.57.0 ([#1004](https://github.com/apache/arrow-rs/pull/1004))


## [6.3.0](https://github.com/apache/arrow-rs/tree/6.3.0) (2021-11-26)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.2.0...6.3.0)


**Changes:**
* [7e51df015ce851a5de444ca08b57b38e7ee959a3](https://github.com/apache/arrow-rs/commit/7e51df015ce851a5de444ca08b57b38e7ee959a3) add more error test case and change the code style ([#952](https://github.com/apache/arrow-rs/pull/952)) ([#976](https://github.com/apache/arrow-rs/pull/976))
* [6c570cfe98d6a7a4ec74b139b733c5c72ed10015](https://github.com/apache/arrow-rs/commit/6c570cfe98d6a7a4ec74b139b733c5c72ed10015) Support read decimal data from csv reader if user provide the schema with decimal data type ([#941](https://github.com/apache/arrow-rs/pull/941)) ([#974](https://github.com/apache/arrow-rs/pull/974))
* [4fa0d4d7f7d9ca0a3da2a6dfe3eae6dc2d51a79a](https://github.com/apache/arrow-rs/commit/4fa0d4d7f7d9ca0a3da2a6dfe3eae6dc2d51a79a) Adding Pretty Print Support For Fixed Size List ([#958](https://github.com/apache/arrow-rs/pull/958)) ([#968](https://github.com/apache/arrow-rs/pull/968))
* [9d453a3128013c03e8ed854ded76b15cc6f28be4](https://github.com/apache/arrow-rs/commit/9d453a3128013c03e8ed854ded76b15cc6f28be4) Fix bug in temporal utilities due to DST being ignored. ([#955](https://github.com/apache/arrow-rs/pull/955)) ([#967](https://github.com/apache/arrow-rs/pull/967))
* [1b9fd9e3fb2653236513bb7dda5aa2fa14d1d831](https://github.com/apache/arrow-rs/commit/1b9fd9e3fb2653236513bb7dda5aa2fa14d1d831) Inferring 2. as Float64 for issue [#929](https://github.com/apache/arrow-rs/pull/929) ([#950](https://github.com/apache/arrow-rs/pull/950)) ([#966](https://github.com/apache/arrow-rs/pull/966))
* [e6c5e1c877bd94b3d6e545567f901d9962257cf8](https://github.com/apache/arrow-rs/commit/e6c5e1c877bd94b3d6e545567f901d9962257cf8) Fix CI for latest nightly ([#970](https://github.com/apache/arrow-rs/pull/970)) ([#973](https://github.com/apache/arrow-rs/pull/973))
* [c96e8de457442806e18944f0b26dd06ba4cb1aee](https://github.com/apache/arrow-rs/commit/c96e8de457442806e18944f0b26dd06ba4cb1aee) Fix primitive sort when input contains more nulls than the given sort limit ([#954](https://github.com/apache/arrow-rs/pull/954)) ([#965](https://github.com/apache/arrow-rs/pull/965))
* [094037d418381584178db1d886cad3b5024b414a](https://github.com/apache/arrow-rs/commit/094037d418381584178db1d886cad3b5024b414a) Update comfy-table to 5.0 ([#957](https://github.com/apache/arrow-rs/pull/957)) ([#964](https://github.com/apache/arrow-rs/pull/964))
* [9f635021eee6786c5377c891218c5f88ebce07c3](https://github.com/apache/arrow-rs/commit/9f635021eee6786c5377c891218c5f88ebce07c3) Fix csv writing of timestamps to show timezone. ([#849](https://github.com/apache/arrow-rs/pull/849)) ([#963](https://github.com/apache/arrow-rs/pull/963))
* [f7deba4c3a050a52608462ee8a827bb8f6364140](https://github.com/apache/arrow-rs/commit/f7deba4c3a050a52608462ee8a827bb8f6364140) Adding ability to parse float from number with leading decimal ([#831](https://github.com/apache/arrow-rs/pull/831)) ([#962](https://github.com/apache/arrow-rs/pull/962))
* [59f96e842d05b63882f7ba285c66a9739761cf84](https://github.com/apache/arrow-rs/commit/59f96e842d05b63882f7ba285c66a9739761cf84) add ilike comparitor ([#874](https://github.com/apache/arrow-rs/pull/874)) ([#961](https://github.com/apache/arrow-rs/pull/961))
* [54023c8a5543c9f9fa4955afa01189029f3e96f5](https://github.com/apache/arrow-rs/commit/54023c8a5543c9f9fa4955afa01189029f3e96f5) Remove unpassable cargo publish check from verify-release-candidate.sh ([#882](https://github.com/apache/arrow-rs/pull/882)) ([#949](https://github.com/apache/arrow-rs/pull/949))



## [6.2.0](https://github.com/apache/arrow-rs/tree/6.2.0) (2021-11-12)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.1.0...6.2.0)

**Features / Fixes:**


* [4037933e43cad9e4de027039ce14caa65f78300a](https://github.com/apache/arrow-rs/commit/4037933e43cad9e4de027039ce14caa65f78300a) Fix validation for offsets of StructArrays ([#942](https://github.com/apache/arrow-rs/pull/942)) ([#946](https://github.com/apache/arrow-rs/pull/946))
* [1af9ca5d363d870550026a7b1abcb749befbb371](https://github.com/apache/arrow-rs/commit/1af9ca5d363d870550026a7b1abcb749befbb371) implement take kernel for null arrays ([#939](https://github.com/apache/arrow-rs/pull/939)) ([#944](https://github.com/apache/arrow-rs/pull/944))
* [320de1c20aefbf204f6888e2ad3663863afeba9f](https://github.com/apache/arrow-rs/commit/320de1c20aefbf204f6888e2ad3663863afeba9f) add checker for appending i128 to decimal builder ([#928](https://github.com/apache/arrow-rs/pull/928)) ([#943](https://github.com/apache/arrow-rs/pull/943))
* [dff14113884ad4246a8cafb9be579ebdb4e1481f](https://github.com/apache/arrow-rs/commit/dff14113884ad4246a8cafb9be579ebdb4e1481f) Validate arguments to ArrayData::new and null bit buffer and buffers ([#810](https://github.com/apache/arrow-rs/pull/810)) ([#936](https://github.com/apache/arrow-rs/pull/936))
* [c3eae1ec56303b97c9e15263063a6a13122ef194](https://github.com/apache/arrow-rs/commit/c3eae1ec56303b97c9e15263063a6a13122ef194) fix some warning about unused variables in panic tests ([#894](https://github.com/apache/arrow-rs/pull/894)) ([#933](https://github.com/apache/arrow-rs/pull/933))
* [e80bb018450f13a30811ffd244c42917d8bf8a62](https://github.com/apache/arrow-rs/commit/e80bb018450f13a30811ffd244c42917d8bf8a62) fix some clippy warnings ([#896](https://github.com/apache/arrow-rs/pull/896)) ([#930](https://github.com/apache/arrow-rs/pull/930))
* [bde89463b627be3f60b5569d038ca36c434da71d](https://github.com/apache/arrow-rs/commit/bde89463b627be3f60b5569d038ca36c434da71d) feat(ipc): add support for deserializing messages with nested dictionary fields ([#923](https://github.com/apache/arrow-rs/pull/923)) ([#931](https://github.com/apache/arrow-rs/pull/931))
* [792544b5fb7b84224ef9745ecb9f330663c14fb4](https://github.com/apache/arrow-rs/commit/792544b5fb7b84224ef9745ecb9f330663c14fb4) refactor regexp_is_match_utf8_scalar to try to mitigate miri failures ([#895](https://github.com/apache/arrow-rs/pull/895)) ([#932](https://github.com/apache/arrow-rs/pull/932))
* [3f0e252811cbb6e3f7c774959787dcfec985d03e](https://github.com/apache/arrow-rs/commit/3f0e252811cbb6e3f7c774959787dcfec985d03e) Automatically retry failed MIRI runs to work around intermittent failures  ([#934](https://github.com/apache/arrow-rs/pull/934))
* [c9a9515c46d560ced00e23ff57cb10a1c97573cb](https://github.com/apache/arrow-rs/commit/c9a9515c46d560ced00e23ff57cb10a1c97573cb) Update mod.rs ([#909](https://github.com/apache/arrow-rs/pull/909)) ([#919](https://github.com/apache/arrow-rs/pull/919))
* [64ed79ece67141b92dc45b8a1d43cb9d909aa6a9](https://github.com/apache/arrow-rs/commit/64ed79ece67141b92dc45b8a1d43cb9d909aa6a9) Mark boolean kernels public ([#913](https://github.com/apache/arrow-rs/pull/913)) ([#920](https://github.com/apache/arrow-rs/pull/920))
* [8b95fe0bbf03588c5cc00f67365c5b0dac4d7a34](https://github.com/apache/arrow-rs/commit/8b95fe0bbf03588c5cc00f67365c5b0dac4d7a34) doc example  mistype ([#904](https://github.com/apache/arrow-rs/pull/904)) ([#918](https://github.com/apache/arrow-rs/pull/918))
* [34c5eab4862cab16fdfd5f5ed6c68dce6298dfa4](https://github.com/apache/arrow-rs/commit/34c5eab4862cab16fdfd5f5ed6c68dce6298dfa4) allow null array to be cast to all other types ([#884](https://github.com/apache/arrow-rs/pull/884)) ([#917](https://github.com/apache/arrow-rs/pull/917))
* [3c69752e55ed0c58f5a8faed918a22b45cd93766](https://github.com/apache/arrow-rs/commit/3c69752e55ed0c58f5a8faed918a22b45cd93766) Fix instances of UB that cause tests to not pass under miri ([#878](https://github.com/apache/arrow-rs/pull/878)) ([#916](https://github.com/apache/arrow-rs/pull/916))
* [85402148c3af03d0855e81f855715ea98a7491c5](https://github.com/apache/arrow-rs/commit/85402148c3af03d0855e81f855715ea98a7491c5) feat(ipc): Support writing dictionaries nested in structs and unions ([#870](https://github.com/apache/arrow-rs/pull/870)) ([#915](https://github.com/apache/arrow-rs/pull/915))
* [03d95e626cb0e654775fefa77786674ea41be4a2](https://github.com/apache/arrow-rs/commit/03d95e626cb0e654775fefa77786674ea41be4a2) Fix references to changelog ([#905](https://github.com/apache/arrow-rs/pull/905))


## [6.1.0](https://github.com/apache/arrow-rs/tree/6.1.0) (2021-10-29)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.0.0...6.1.0)

**Features / Fixes:**

* [b42649b0088fe7762c713a41a23c1abdf8d0496d](https://github.com/apache/arrow-rs/commit/b42649b0088fe7762c713a41a23c1abdf8d0496d) implement eq_dyn and neq_dyn ([#858](https://github.com/apache/arrow-rs/pull/858)) ([#867](https://github.com/apache/arrow-rs/pull/867))
* [01743f3f10a377c1ca857cd554acbf84155766d8](https://github.com/apache/arrow-rs/commit/01743f3f10a377c1ca857cd554acbf84155766d8) fix: fix a bug in offset calculation for unions ([#863](https://github.com/apache/arrow-rs/pull/863)) ([#871](https://github.com/apache/arrow-rs/pull/871))
* [8bfff793a23f0e71008c7a9eea7a54d6b913ecff](https://github.com/apache/arrow-rs/commit/8bfff793a23f0e71008c7a9eea7a54d6b913ecff) add lt_bool, lt_eq_bool, gt_bool, gt_eq_bool ([#860](https://github.com/apache/arrow-rs/pull/860)) ([#868](https://github.com/apache/arrow-rs/pull/868))
* [8845e91d4ab584c822e9ee903db7069551b124af](https://github.com/apache/arrow-rs/commit/8845e91d4ab584c822e9ee903db7069551b124af) fix(ipc): Support serializing structs containing dictionaries ([#848](https://github.com/apache/arrow-rs/pull/848)) ([#865](https://github.com/apache/arrow-rs/pull/865))
* [620282a0d9fdd2a8ed7e8313d17ba3dec64c80e5](https://github.com/apache/arrow-rs/commit/620282a0d9fdd2a8ed7e8313d17ba3dec64c80e5) Implement boolean equality kernels ([#844](https://github.com/apache/arrow-rs/pull/844)) ([#857](https://github.com/apache/arrow-rs/pull/857))
* [94cddcacf785be982e69689291ce034ef00220b4](https://github.com/apache/arrow-rs/commit/94cddcacf785be982e69689291ce034ef00220b4) Cherry pick fix parquet_derive with default features (and fix cargo publish) ([#856](https://github.com/apache/arrow-rs/pull/856))
* [733fd583ddb3dbe6b4d58a809c444ee16ac0eae8](https://github.com/apache/arrow-rs/commit/733fd583ddb3dbe6b4d58a809c444ee16ac0eae8) Use kernel utility for parsing timestamps in csv reader. ([#832](https://github.com/apache/arrow-rs/pull/832)) ([#853](https://github.com/apache/arrow-rs/pull/853))
* [2cc64937a153f632796915d2d9869d5c2a501d28](https://github.com/apache/arrow-rs/commit/2cc64937a153f632796915d2d9869d5c2a501d28) [Minor] Fix clippy errors with new rust version (1.56) and float formatting with nightly ([#845](https://github.com/apache/arrow-rs/pull/845)) ([#850](https://github.com/apache/arrow-rs/pull/850))

**Other:**
* [bfac9e5a027e3bd78b7a1ec90c75a3e385bd66bb](https://github.com/apache/arrow-rs/commit/bfac9e5a027e3bd78b7a1ec90c75a3e385bd66bb) Test out new tarpaulin version ([#852](https://github.com/apache/arrow-rs/pull/852)) ([#866](https://github.com/apache/arrow-rs/pull/866))
* [809350ced392cfc78d8a1a46228d4ffc25dea9ff](https://github.com/apache/arrow-rs/commit/809350ced392cfc78d8a1a46228d4ffc25dea9ff) Update README.md ([#834](https://github.com/apache/arrow-rs/pull/834)) ([#854](https://github.com/apache/arrow-rs/pull/854))
* [70582f40dd21f5c710c4946266d0563a92b92337](https://github.com/apache/arrow-rs/commit/70582f40dd21f5c710c4946266d0563a92b92337) [MINOR] Delete temp file from docs ([#836](https://github.com/apache/arrow-rs/pull/836)) ([#855](https://github.com/apache/arrow-rs/pull/855))
* [a721e00014015a7e598946b6efb9b1da8080ec85](https://github.com/apache/arrow-rs/commit/a721e00014015a7e598946b6efb9b1da8080ec85) Force fresh cargo cache key in CI ([#839](https://github.com/apache/arrow-rs/pull/839)) ([#851](https://github.com/apache/arrow-rs/pull/851))


## [6.0.0](https://github.com/apache/arrow-rs/tree/6.0.0) (2021-10-13)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.5.0...6.0.0)

**Breaking changes:**

- Replace `ArrayData::new()` with `ArrayData::try_new()` and `unsafe ArrayData::new_unchecked` [\#822](https://github.com/apache/arrow-rs/pull/822) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update Bitmap::len to return bits rather than bytes [\#749](https://github.com/apache/arrow-rs/pull/749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- use sort\_unstable\_by in primitive sorting [\#552](https://github.com/apache/arrow-rs/pull/552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- New MapArray support [\#491](https://github.com/apache/arrow-rs/pull/491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nevi-me](https://github.com/nevi-me))

**Implemented enhancements:**

- Improve parquet binary writer speed by reducing allocations [\#819](https://github.com/apache/arrow-rs/issues/819)
- Expose buffer operations [\#808](https://github.com/apache/arrow-rs/issues/808)
- Add doc examples of writing parquet files using `ArrowWriter` [\#788](https://github.com/apache/arrow-rs/issues/788)

**Fixed bugs:**

- JSON reader can create null struct children on empty lists [\#825](https://github.com/apache/arrow-rs/issues/825)
- Incorrect null count for cast kernel for list arrays [\#815](https://github.com/apache/arrow-rs/issues/815)
- `minute` and `second` temporal kernels do not respect timezone [\#500](https://github.com/apache/arrow-rs/issues/500)
- Fix data corruption in json decoder f64-to-i64 cast [\#652](https://github.com/apache/arrow-rs/pull/652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xianwill](https://github.com/xianwill))

**Documentation updates:**

- Doctest for PrimitiveArray using from\_iter\_values. [\#694](https://github.com/apache/arrow-rs/pull/694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Doctests for BinaryArray and LargeBinaryArray. [\#625](https://github.com/apache/arrow-rs/pull/625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Add links in docstrings [\#605](https://github.com/apache/arrow-rs/pull/605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))


## [5.5.0](https://github.com/apache/arrow-rs/tree/5.5.0) (2021-09-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.4.0...5.5.0)

**Implemented enhancements:**

- parquet should depend on a small set of arrow features [\#800](https://github.com/apache/arrow-rs/issues/800)
- Support equality on RecordBatch [\#735](https://github.com/apache/arrow-rs/issues/735)

**Fixed bugs:**

- Converting from string to timestamp uses microseconds instead of milliseconds [\#780](https://github.com/apache/arrow-rs/issues/780)
- Document has no link to `RowColumIter` [\#762](https://github.com/apache/arrow-rs/issues/762)
- length on slices with null doesn't work [\#744](https://github.com/apache/arrow-rs/issues/744)

## [5.4.0](https://github.com/apache/arrow-rs/tree/5.4.0) (2021-09-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.3.0...5.4.0)

**Implemented enhancements:**

- Upgrade lexical-core to 0.8 [\#747](https://github.com/apache/arrow-rs/issues/747)
- `append_nulls` and `append_trusted_len_iter` for PrimitiveBuilder [\#725](https://github.com/apache/arrow-rs/issues/725)
- Optimize MutableArrayData::extend for null buffers [\#397](https://github.com/apache/arrow-rs/issues/397)

**Fixed bugs:**

- Arithmetic with scalars doesn't work on slices [\#742](https://github.com/apache/arrow-rs/issues/742)
- Comparisons with scalar don't work on slices [\#740](https://github.com/apache/arrow-rs/issues/740)
- `unary` kernel doesn't respect offset [\#738](https://github.com/apache/arrow-rs/issues/738)
- `new_null_array` creates invalid struct arrays [\#734](https://github.com/apache/arrow-rs/issues/734)
- --no-default-features is broken for parquet [\#733](https://github.com/apache/arrow-rs/issues/733) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Bitmap::len` returns the number of bytes, not bits. [\#730](https://github.com/apache/arrow-rs/issues/730)
- Decimal logical type is formatted incorrectly by print\_schema [\#713](https://github.com/apache/arrow-rs/issues/713)
- parquet\_derive does not support chrono time values [\#711](https://github.com/apache/arrow-rs/issues/711)
- Numeric overflow when formatting Decimal type [\#710](https://github.com/apache/arrow-rs/issues/710)
- The integration tests are not running [\#690](https://github.com/apache/arrow-rs/issues/690)

**Closed issues:**

- Question: Is there no way to create a DictionaryArray with a pre-arranged mapping? [\#729](https://github.com/apache/arrow-rs/issues/729)

## [5.3.0](https://github.com/apache/arrow-rs/tree/5.3.0) (2021-08-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.2.0...5.3.0)

**Implemented enhancements:**

- Add optimized filter kernel for regular expression matching [\#697](https://github.com/apache/arrow-rs/issues/697)
- Can't cast from timestamp array to string array [\#587](https://github.com/apache/arrow-rs/issues/587)

**Fixed bugs:**

- 'Encoding DELTA\_BYTE\_ARRAY is not supported' with parquet arrow readers [\#708](https://github.com/apache/arrow-rs/issues/708)
- Support reading json string into binary data type. [\#701](https://github.com/apache/arrow-rs/issues/701)

**Closed issues:**

- Resolve Issues with `prettytable-rs` dependency [\#69](https://github.com/apache/arrow-rs/issues/69) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [5.2.0](https://github.com/apache/arrow-rs/tree/5.2.0) (2021-08-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.1.0...5.2.0)

**Implemented enhancements:**

- Make rand an optional dependency [\#671](https://github.com/apache/arrow-rs/issues/671)
- Remove undefined behavior in `value` method of boolean and primitive arrays [\#645](https://github.com/apache/arrow-rs/issues/645)
- Avoid materialization of indices in filter\_record\_batch for single arrays [\#636](https://github.com/apache/arrow-rs/issues/636)
- Add a note about arrow crate security / safety [\#627](https://github.com/apache/arrow-rs/issues/627)
- Allow the creation of String arrays from an interator of &Option\<&str\> [\#598](https://github.com/apache/arrow-rs/issues/598)
- Support arrow map datatype [\#395](https://github.com/apache/arrow-rs/issues/395)

**Fixed bugs:**

- Parquet fixed length byte array columns write byte array statistics [\#660](https://github.com/apache/arrow-rs/issues/660) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet boolean columns write Int32 statistics [\#659](https://github.com/apache/arrow-rs/issues/659) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Writing Parquet with a boolean column fails [\#657](https://github.com/apache/arrow-rs/issues/657)
- JSON decoder data corruption for large i64/u64 [\#653](https://github.com/apache/arrow-rs/issues/653)
- Incorrect min/max statistics for strings in parquet files [\#641](https://github.com/apache/arrow-rs/issues/641) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Closed issues:**

- Release candidate verifying script seems work on macOS [\#640](https://github.com/apache/arrow-rs/issues/640)
- Update CONTRIBUTING  [\#342](https://github.com/apache/arrow-rs/issues/342)

## [5.1.0](https://github.com/apache/arrow-rs/tree/5.1.0) (2021-07-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.0.0...5.1.0)

**Implemented enhancements:**

- Make FFI\_ArrowArray empty\(\) public [\#602](https://github.com/apache/arrow-rs/issues/602)
- exponential sort can be used to speed up lexico partition kernel [\#586](https://github.com/apache/arrow-rs/issues/586)
- Implement sort\(\) for binary array [\#568](https://github.com/apache/arrow-rs/issues/568)
- primitive sorting can be improved and more consistent with and without `limit` if sorted unstably [\#553](https://github.com/apache/arrow-rs/issues/553)

**Fixed bugs:**

- Confusing memory usage with CSV reader [\#623](https://github.com/apache/arrow-rs/issues/623)
- FFI implementation deviates from specification for array release  [\#595](https://github.com/apache/arrow-rs/issues/595)
- Parquet file content is different if `~/.cargo` is in a git checkout [\#589](https://github.com/apache/arrow-rs/issues/589)
- Ensure output of MIRI is checked for success [\#581](https://github.com/apache/arrow-rs/issues/581)
- MIRI failure in `array::ffi::tests::test_struct` and other ffi tests [\#580](https://github.com/apache/arrow-rs/issues/580)
- ListArray equality check may return wrong result [\#570](https://github.com/apache/arrow-rs/issues/570)
- cargo audit failed [\#561](https://github.com/apache/arrow-rs/issues/561)
- ArrayData::slice\(\) does not work for nested types such as StructArray [\#554](https://github.com/apache/arrow-rs/issues/554)

**Documentation updates:**

- More examples of how to construct Arrays [\#301](https://github.com/apache/arrow-rs/issues/301)

**Closed issues:**

- Implement StringBuilder::append\_option [\#263](https://github.com/apache/arrow-rs/issues/263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [5.0.0](https://github.com/apache/arrow-rs/tree/5.0.0) (2021-07-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.4.0...5.0.0)

**Breaking changes:**

- Remove lifetime from DynComparator [\#543](https://github.com/apache/arrow-rs/issues/543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Simplify interactions with arrow flight APIs [\#376](https://github.com/apache/arrow-rs/issues/376) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- refactor: remove lifetime from DynComparator [\#542](https://github.com/apache/arrow-rs/pull/542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([e-dard](https://github.com/e-dard))
- use iterator for partition kernel instead of generating vec [\#438](https://github.com/apache/arrow-rs/pull/438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Remove DictionaryArray::keys\_array method [\#419](https://github.com/apache/arrow-rs/pull/419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- simplify interactions with arrow flight APIs [\#377](https://github.com/apache/arrow-rs/pull/377) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([garyanaplan](https://github.com/garyanaplan))
- return reference from DictionaryArray::values\(\) \(\#313\) [\#314](https://github.com/apache/arrow-rs/pull/314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Allow creation of StringArrays from Vec\<String\> [\#519](https://github.com/apache/arrow-rs/issues/519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatch::concat [\#461](https://github.com/apache/arrow-rs/issues/461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatch::slice\(\) to slice RecordBatches  [\#460](https://github.com/apache/arrow-rs/issues/460) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a RecordBatch::split to split large batches into a set of smaller batches [\#343](https://github.com/apache/arrow-rs/issues/343)
- generate parquet schema from rust struct [\#539](https://github.com/apache/arrow-rs/pull/539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Implement `RecordBatch::concat` [\#537](https://github.com/apache/arrow-rs/pull/537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Implement function slice for RecordBatch [\#490](https://github.com/apache/arrow-rs/pull/490) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([b41sh](https://github.com/b41sh))
- add lexicographically partition points and ranges [\#424](https://github.com/apache/arrow-rs/pull/424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- allow to read non-standard CSV [\#326](https://github.com/apache/arrow-rs/pull/326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- parquet: Speed up `BitReader`/`DeltaBitPackDecoder` [\#325](https://github.com/apache/arrow-rs/pull/325) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kornholi](https://github.com/kornholi))
- ARROW-12343: \[Rust\] Support auto-vectorization for min/max [\#9](https://github.com/apache/arrow-rs/pull/9) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- ARROW-12411: \[Rust\] Create RecordBatches from Iterators [\#7](https://github.com/apache/arrow-rs/pull/7) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Fixed bugs:**

- Error building on master - error: cyclic package dependency: package `ahash v0.7.4` depends on itself. Cycle [\#544](https://github.com/apache/arrow-rs/issues/544)
- IPC reader panics with out of bounds error [\#541](https://github.com/apache/arrow-rs/issues/541)
- Take kernel doesn't handle nulls and structs correctly [\#530](https://github.com/apache/arrow-rs/issues/530) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- master fails to compile with `default-features=false` [\#529](https://github.com/apache/arrow-rs/issues/529)
- README developer instructions out of date [\#523](https://github.com/apache/arrow-rs/issues/523)
- Update rustc and packed\_simd in CI before 5.0 release [\#517](https://github.com/apache/arrow-rs/issues/517)
- Incorrect memory usage calculation for dictionary arrays [\#503](https://github.com/apache/arrow-rs/issues/503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- sliced null buffers lead to incorrect result in take kernel \(and probably on other places\) [\#502](https://github.com/apache/arrow-rs/issues/502)
- Cast of utf8 types and list container types don't respect offset [\#334](https://github.com/apache/arrow-rs/issues/334) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- fix take kernel null handling on structs [\#531](https://github.com/apache/arrow-rs/pull/531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Correct array memory usage calculation for dictionary arrays [\#505](https://github.com/apache/arrow-rs/pull/505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- parquet: improve BOOLEAN writing logic and report error on encoding fail [\#443](https://github.com/apache/arrow-rs/pull/443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([garyanaplan](https://github.com/garyanaplan))
- Fix bug with null buffer offset in boolean not kernel [\#418](https://github.com/apache/arrow-rs/pull/418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- respect offset in utf8 and list casts [\#335](https://github.com/apache/arrow-rs/pull/335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Fix comparison of dictionaries with different values arrays \(\#332\) [\#333](https://github.com/apache/arrow-rs/pull/333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ensure null-counts are written for all-null columns [\#307](https://github.com/apache/arrow-rs/pull/307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- fix invalid null handling in filter [\#296](https://github.com/apache/arrow-rs/pull/296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- fix NaN handling in parquet statistics [\#256](https://github.com/apache/arrow-rs/pull/256) ([crepererum](https://github.com/crepererum))

**Documentation updates:**

- Improve arrow's crate's readme on crates.io [\#463](https://github.com/apache/arrow-rs/issues/463)
- Clean up README.md in advance of the 5.0 release [\#536](https://github.com/apache/arrow-rs/pull/536) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix readme instructions to reflect new structure [\#524](https://github.com/apache/arrow-rs/pull/524) ([marcvanheerden](https://github.com/marcvanheerden))
- Improve docs for NullArray, new\_null\_array and new\_empty\_array [\#240](https://github.com/apache/arrow-rs/pull/240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Fix default arrow build [\#533](https://github.com/apache/arrow-rs/pull/533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add tests for building applications using arrow with different feature flags [\#532](https://github.com/apache/arrow-rs/pull/532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove unused futures dependency from arrow-flight [\#528](https://github.com/apache/arrow-rs/pull/528) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- CI: update rust nightly and packed\_simd [\#525](https://github.com/apache/arrow-rs/pull/525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Support `StringArray` creation from String Vec [\#522](https://github.com/apache/arrow-rs/pull/522) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Fix parquet benchmark schema [\#513](https://github.com/apache/arrow-rs/pull/513) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix parquet definition levels [\#511](https://github.com/apache/arrow-rs/pull/511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix for primitive and boolean take kernel for nullable indices with an offset [\#509](https://github.com/apache/arrow-rs/pull/509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Bump flatbuffers [\#499](https://github.com/apache/arrow-rs/pull/499) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([PsiACE](https://github.com/PsiACE))
- implement second/minute helpers for temporal [\#493](https://github.com/apache/arrow-rs/pull/493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ovr](https://github.com/ovr))
- special case concatenating single element array shortcut [\#492](https://github.com/apache/arrow-rs/pull/492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- update docs to reflect recent changes \(joins and window functions\) [\#489](https://github.com/apache/arrow-rs/pull/489) ([Jimexist](https://github.com/Jimexist))
- Update rand, proc-macro and zstd dependencies [\#488](https://github.com/apache/arrow-rs/pull/488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Doctest for GenericListArray. [\#474](https://github.com/apache/arrow-rs/pull/474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- remove stale comment on `ArrayData` equality and update unit tests [\#472](https://github.com/apache/arrow-rs/pull/472) ([Jimexist](https://github.com/Jimexist))
- remove unused patch file [\#471](https://github.com/apache/arrow-rs/pull/471) ([Jimexist](https://github.com/Jimexist))
- fix clippy warnings for rust 1.53 [\#470](https://github.com/apache/arrow-rs/pull/470) ([Jimexist](https://github.com/Jimexist))
- Fix PR labeler [\#468](https://github.com/apache/arrow-rs/pull/468) ([Dandandan](https://github.com/Dandandan))
- Tweak dev backporting docs [\#466](https://github.com/apache/arrow-rs/pull/466) ([alamb](https://github.com/alamb))
- Unvendor Archery [\#459](https://github.com/apache/arrow-rs/pull/459) ([kszucs](https://github.com/kszucs))
- Add sort boolean benchmark [\#457](https://github.com/apache/arrow-rs/pull/457) ([alamb](https://github.com/alamb))
- Add C data interface for decimal128 and timestamp [\#453](https://github.com/apache/arrow-rs/pull/453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alippai](https://github.com/alippai))
- Implement the Iterator trait for the json Reader. [\#451](https://github.com/apache/arrow-rs/pull/451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([LaurentMazare](https://github.com/LaurentMazare))
- Update release docs + release email template [\#450](https://github.com/apache/arrow-rs/pull/450) ([alamb](https://github.com/alamb))
- remove clippy unnecessary wraps suppresions in cast kernel [\#449](https://github.com/apache/arrow-rs/pull/449) ([Jimexist](https://github.com/Jimexist))
- Use partition for bool sort [\#448](https://github.com/apache/arrow-rs/pull/448) ([Jimexist](https://github.com/Jimexist))
- remove unnecessary wraps in sort [\#445](https://github.com/apache/arrow-rs/pull/445) ([Jimexist](https://github.com/Jimexist))
- Python FFI bridge for Schema, Field and DataType  [\#439](https://github.com/apache/arrow-rs/pull/439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszucs](https://github.com/kszucs))
- Update release Readme.md [\#436](https://github.com/apache/arrow-rs/pull/436) ([alamb](https://github.com/alamb))
- Derive Eq and PartialEq for SortOptions [\#425](https://github.com/apache/arrow-rs/pull/425) ([tustvold](https://github.com/tustvold))
- refactor lexico sort for future code reuse [\#423](https://github.com/apache/arrow-rs/pull/423) ([Jimexist](https://github.com/Jimexist))
- Reenable MIRI check on PRs [\#421](https://github.com/apache/arrow-rs/pull/421) ([alamb](https://github.com/alamb))
- Sort by float lists [\#420](https://github.com/apache/arrow-rs/pull/420) ([medwards](https://github.com/medwards))
- Fix out of bounds read in bit chunk iterator [\#416](https://github.com/apache/arrow-rs/pull/416) ([jhorstmann](https://github.com/jhorstmann))
- Doctests for DecimalArray. [\#414](https://github.com/apache/arrow-rs/pull/414) ([novemberkilo](https://github.com/novemberkilo))
- Add Decimal to CsvWriter and improve debug display [\#406](https://github.com/apache/arrow-rs/pull/406) ([alippai](https://github.com/alippai))
- MINOR: update install instruction [\#400](https://github.com/apache/arrow-rs/pull/400) ([alippai](https://github.com/alippai))
- use prettier to auto format md files [\#398](https://github.com/apache/arrow-rs/pull/398) ([Jimexist](https://github.com/Jimexist))
- window::shift to work for all array types [\#388](https://github.com/apache/arrow-rs/pull/388) ([Jimexist](https://github.com/Jimexist))
- add more tests for window::shift and handle boundary cases [\#386](https://github.com/apache/arrow-rs/pull/386) ([Jimexist](https://github.com/Jimexist))
- Implement faster arrow array reader [\#384](https://github.com/apache/arrow-rs/pull/384) ([yordan-pavlov](https://github.com/yordan-pavlov))
- Add set\_bit to BooleanBufferBuilder to allow mutating bit in index [\#383](https://github.com/apache/arrow-rs/pull/383) ([boazberman](https://github.com/boazberman))
- make sure that only concat preallocates buffers [\#382](https://github.com/apache/arrow-rs/pull/382) ([ritchie46](https://github.com/ritchie46))
- Respect max rowgroup size in Arrow writer [\#381](https://github.com/apache/arrow-rs/pull/381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix typo in release script, update release location [\#380](https://github.com/apache/arrow-rs/pull/380) ([alamb](https://github.com/alamb))
- Doctests for FixedSizeBinaryArray [\#378](https://github.com/apache/arrow-rs/pull/378) ([novemberkilo](https://github.com/novemberkilo))
- Simplify shift kernel using new\_null\_array [\#370](https://github.com/apache/arrow-rs/pull/370) ([Dandandan](https://github.com/Dandandan))
- allow `SliceableCursor` to be constructed from an `Arc` directly [\#369](https://github.com/apache/arrow-rs/pull/369) ([crepererum](https://github.com/crepererum))
- Add doctest for ArrayBuilder [\#367](https://github.com/apache/arrow-rs/pull/367) ([alippai](https://github.com/alippai))
- Fix version in readme [\#365](https://github.com/apache/arrow-rs/pull/365) ([domoritz](https://github.com/domoritz))
- Remove superfluous space [\#363](https://github.com/apache/arrow-rs/pull/363) ([domoritz](https://github.com/domoritz))
- Add crate badges [\#362](https://github.com/apache/arrow-rs/pull/362) ([domoritz](https://github.com/domoritz))
- Disable MIRI check until it runs cleanly on CI [\#360](https://github.com/apache/arrow-rs/pull/360) ([alamb](https://github.com/alamb))
- Only register Flight.proto with cargo if it exists [\#351](https://github.com/apache/arrow-rs/pull/351) ([tustvold](https://github.com/tustvold))
- Reduce memory usage of concat \(large\)utf8 [\#348](https://github.com/apache/arrow-rs/pull/348) ([ritchie46](https://github.com/ritchie46))
- Fix filter UB and add fast path [\#341](https://github.com/apache/arrow-rs/pull/341) ([ritchie46](https://github.com/ritchie46))
- Automatic cherry-pick script [\#339](https://github.com/apache/arrow-rs/pull/339) ([alamb](https://github.com/alamb))
- Doctests for BooleanArray. [\#338](https://github.com/apache/arrow-rs/pull/338) ([novemberkilo](https://github.com/novemberkilo))
- feature gate ipc reader/writer [\#336](https://github.com/apache/arrow-rs/pull/336) ([ritchie46](https://github.com/ritchie46))
- Add ported Rust release verification script [\#331](https://github.com/apache/arrow-rs/pull/331) ([wesm](https://github.com/wesm))
- Doctests for StringArray and LargeStringArray. [\#330](https://github.com/apache/arrow-rs/pull/330) ([novemberkilo](https://github.com/novemberkilo))
- inline PrimitiveArray::value [\#329](https://github.com/apache/arrow-rs/pull/329) ([ritchie46](https://github.com/ritchie46))
- Enable wasm32 as a target architecture for the SIMD feature  [\#324](https://github.com/apache/arrow-rs/pull/324) ([roee88](https://github.com/roee88))
- Fix undefined behavior in FFI and enable MIRI checks on CI [\#323](https://github.com/apache/arrow-rs/pull/323) ([roee88](https://github.com/roee88))
- Mutablebuffer::shrink\_to\_fit [\#318](https://github.com/apache/arrow-rs/pull/318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Add \(simd\) modulus op [\#317](https://github.com/apache/arrow-rs/pull/317) ([gangliao](https://github.com/gangliao))
- feature gate csv functionality [\#312](https://github.com/apache/arrow-rs/pull/312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- \[Minor\] Version upgrades [\#304](https://github.com/apache/arrow-rs/pull/304) ([Dandandan](https://github.com/Dandandan))
- Remove old release scripts [\#293](https://github.com/apache/arrow-rs/pull/293) ([alamb](https://github.com/alamb))
- Add Send to the ArrayBuilder trait [\#291](https://github.com/apache/arrow-rs/pull/291) ([Max-Meldrum](https://github.com/Max-Meldrum))
- Added changelog generator script and configuration. [\#289](https://github.com/apache/arrow-rs/pull/289) ([jorgecarleitao](https://github.com/jorgecarleitao))
- manually bump development version [\#288](https://github.com/apache/arrow-rs/pull/288) ([nevi-me](https://github.com/nevi-me))
- Fix FFI and add support for Struct type [\#287](https://github.com/apache/arrow-rs/pull/287) ([roee88](https://github.com/roee88))
- Fix subtraction underflow when sorting string arrays with many nulls [\#285](https://github.com/apache/arrow-rs/pull/285) ([medwards](https://github.com/medwards))
- Speed up bound checking in `take` [\#281](https://github.com/apache/arrow-rs/pull/281) ([Dandandan](https://github.com/Dandandan))
- Update PR template by commenting out instructions [\#278](https://github.com/apache/arrow-rs/pull/278) ([nevi-me](https://github.com/nevi-me))
- Added Decimal support to pretty-print display utility \(\#230\) [\#273](https://github.com/apache/arrow-rs/pull/273) ([mgill25](https://github.com/mgill25))
- Fix null struct and list roundtrip [\#270](https://github.com/apache/arrow-rs/pull/270) ([nevi-me](https://github.com/nevi-me))
- 1.52 clippy fixes [\#267](https://github.com/apache/arrow-rs/pull/267) ([nevi-me](https://github.com/nevi-me))
- Fix typo in csv/reader.rs [\#265](https://github.com/apache/arrow-rs/pull/265) ([domoritz](https://github.com/domoritz))
- Fix empty Schema::metadata deserialization error [\#260](https://github.com/apache/arrow-rs/pull/260) ([hulunbier](https://github.com/hulunbier))
- update datafusion and ballista doc links [\#259](https://github.com/apache/arrow-rs/pull/259) ([Jimexist](https://github.com/Jimexist))
- support full u32 and u64 roundtrip through parquet [\#258](https://github.com/apache/arrow-rs/pull/258) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- \[MINOR\] Added env to run rust in integration. [\#253](https://github.com/apache/arrow-rs/pull/253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- \[Minor\] Made integration tests always run. [\#248](https://github.com/apache/arrow-rs/pull/248) ([jorgecarleitao](https://github.com/jorgecarleitao))
- fix parquet max\_definition for non-null structs [\#246](https://github.com/apache/arrow-rs/pull/246) ([nevi-me](https://github.com/nevi-me))
- Disabled rebase needed until demonstrate working. [\#243](https://github.com/apache/arrow-rs/pull/243) ([jorgecarleitao](https://github.com/jorgecarleitao))
- pin flatbuffers to 0.8.4 [\#239](https://github.com/apache/arrow-rs/pull/239) ([ritchie46](https://github.com/ritchie46))
- sort\_primitive result is capped to the min of limit or values.len [\#236](https://github.com/apache/arrow-rs/pull/236) ([medwards](https://github.com/medwards))
- Read list field correctly [\#234](https://github.com/apache/arrow-rs/pull/234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix code examples for RecordBatch::try\_from\_iter [\#231](https://github.com/apache/arrow-rs/pull/231) ([alamb](https://github.com/alamb))
- Support string dictionaries in csv reader \(\#228\) [\#229](https://github.com/apache/arrow-rs/pull/229) ([tustvold](https://github.com/tustvold))
- support LargeUtf8 in sort kernel [\#26](https://github.com/apache/arrow-rs/pull/26) ([ritchie46](https://github.com/ritchie46))
- Removed unused files [\#22](https://github.com/apache/arrow-rs/pull/22) ([jorgecarleitao](https://github.com/jorgecarleitao))
- ARROW-12504: Buffer::from\_slice\_ref set correct capacity [\#18](https://github.com/apache/arrow-rs/pull/18) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add GitHub templates [\#17](https://github.com/apache/arrow-rs/pull/17) ([andygrove](https://github.com/andygrove))
- ARROW-12493: Add support for writing dictionary arrays to CSV and JSON [\#16](https://github.com/apache/arrow-rs/pull/16) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ARROW-12426: \[Rust\] Fix concatentation of arrow dictionaries [\#15](https://github.com/apache/arrow-rs/pull/15) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update repository and homepage urls [\#14](https://github.com/apache/arrow-rs/pull/14) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Added rebase-needed bot [\#13](https://github.com/apache/arrow-rs/pull/13) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added Integration tests against arrow [\#10](https://github.com/apache/arrow-rs/pull/10) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [4.4.0](https://github.com/apache/arrow-rs/tree/4.4.0) (2021-06-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.3.0...4.4.0)

**Breaking changes:**

- migrate partition kernel to use Iterator trait [\#437](https://github.com/apache/arrow-rs/issues/437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove DictionaryArray::keys\_array [\#391](https://github.com/apache/arrow-rs/issues/391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Implemented enhancements:**

- sort kernel boolean sort can be O\(n\) [\#447](https://github.com/apache/arrow-rs/issues/447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- C data interface for decimal128, timestamp, date32 and date64 [\#413](https://github.com/apache/arrow-rs/issues/413)
- Add Decimal to CsvWriter [\#405](https://github.com/apache/arrow-rs/issues/405)
- Use iterators to increase performance of creating Arrow arrays [\#200](https://github.com/apache/arrow-rs/issues/200) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Release Audit Tool \(RAT\) is not being triggered [\#481](https://github.com/apache/arrow-rs/issues/481)
- Security Vulnerabilities: flatbuffers: `read_scalar` and `read_scalar_at` allow transmuting values without `unsafe` blocks [\#476](https://github.com/apache/arrow-rs/issues/476)
- Clippy broken after upgrade to rust 1.53 [\#467](https://github.com/apache/arrow-rs/issues/467)
- Pull Request Labeler is not working [\#462](https://github.com/apache/arrow-rs/issues/462)
- Arrow 4.3 release: error\[E0658\]: use of unstable library feature 'partition\_point': new API [\#456](https://github.com/apache/arrow-rs/issues/456)
- parquet reading hangs when row\_group contains more than 2048 rows of data [\#349](https://github.com/apache/arrow-rs/issues/349)
- Fail to build arrow  [\#247](https://github.com/apache/arrow-rs/issues/247)
- JSON reader does not implement iterator [\#193](https://github.com/apache/arrow-rs/issues/193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Security fixes:**

- Ensure a successful MIRI Run on CI [\#227](https://github.com/apache/arrow-rs/issues/227)

**Closed issues:**

- sort kernel has a lot of unnecessary wrapping [\#446](https://github.com/apache/arrow-rs/issues/446)
- \[Parquet\] Plain encoded boolean column chunks limited to 2048 values [\#48](https://github.com/apache/arrow-rs/issues/48) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

## [4.3.0](https://github.com/apache/arrow-rs/tree/4.3.0) (2021-06-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.2.0...4.3.0)

**Implemented enhancements:**

- Add partitioning kernel for sorted arrays [\#428](https://github.com/apache/arrow-rs/issues/428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement sort by float lists [\#427](https://github.com/apache/arrow-rs/issues/427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Derive Eq and PartialEq for SortOptions [\#426](https://github.com/apache/arrow-rs/issues/426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- use prettier and github action to normalize markdown document syntax [\#399](https://github.com/apache/arrow-rs/issues/399)
- window::shift can work for more than just primitive array type [\#392](https://github.com/apache/arrow-rs/issues/392)
- Doctest for ArrayBuilder [\#366](https://github.com/apache/arrow-rs/issues/366)

**Fixed bugs:**

- Boolean `not` kernel does not take offset of null buffer into account [\#417](https://github.com/apache/arrow-rs/issues/417)
- my contribution not marged in 4.2 release  [\#394](https://github.com/apache/arrow-rs/issues/394)
- window::shift shall properly handle boundary cases [\#387](https://github.com/apache/arrow-rs/issues/387)
- Parquet `WriterProperties.max_row_group_size` not wired up [\#257](https://github.com/apache/arrow-rs/issues/257)
- Out of bound reads in chunk iterator [\#198](https://github.com/apache/arrow-rs/issues/198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [4.2.0](https://github.com/apache/arrow-rs/tree/4.2.0) (2021-05-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.1.0...4.2.0)

**Breaking changes:**

- DictionaryArray::values\(\) clones the underlying ArrayRef [\#313](https://github.com/apache/arrow-rs/issues/313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Implemented enhancements:**

- Simplify shift kernel using null array [\#371](https://github.com/apache/arrow-rs/issues/371)
- Provide `Arc`-based constructor for `parquet::util::cursor::SliceableCursor` [\#368](https://github.com/apache/arrow-rs/issues/368)
- Add badges to crates [\#361](https://github.com/apache/arrow-rs/issues/361)
- Consider inlining PrimitiveArray::value [\#328](https://github.com/apache/arrow-rs/issues/328)
- Implement automated release verification script [\#327](https://github.com/apache/arrow-rs/issues/327)
- Add wasm32 to the list of target architectures of the simd feature [\#316](https://github.com/apache/arrow-rs/issues/316)
- add with\_escape for csv::ReaderBuilder [\#315](https://github.com/apache/arrow-rs/issues/315) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- IPC feature gate [\#310](https://github.com/apache/arrow-rs/issues/310)
- csv feature gate [\#309](https://github.com/apache/arrow-rs/issues/309) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `shrink_to` / `shrink_to_fit` to `MutableBuffer` [\#297](https://github.com/apache/arrow-rs/issues/297)

**Fixed bugs:**

- Incorrect crate setup instructions [\#364](https://github.com/apache/arrow-rs/issues/364)
- Arrow-flight only register rerun-if-changed if file exists [\#350](https://github.com/apache/arrow-rs/issues/350)
- Dictionary Comparison Uses Wrong Values Array [\#332](https://github.com/apache/arrow-rs/issues/332)
- Undefined behavior in FFI implementation [\#322](https://github.com/apache/arrow-rs/issues/322)
- All-null column get wrong parquet null-counts [\#306](https://github.com/apache/arrow-rs/issues/306) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Filter has inconsistent null handling [\#295](https://github.com/apache/arrow-rs/issues/295)

## [4.1.0](https://github.com/apache/arrow-rs/tree/4.1.0) (2021-05-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.0.0...4.1.0)

**Implemented enhancements:**

- Add Send to ArrayBuilder [\#290](https://github.com/apache/arrow-rs/issues/290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of bound checking option [\#280](https://github.com/apache/arrow-rs/issues/280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- extend compute kernel arity to include nullary functions [\#276](https://github.com/apache/arrow-rs/issues/276)
- Implement FFI / CDataInterface for Struct Arrays [\#251](https://github.com/apache/arrow-rs/issues/251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for pretty-printing Decimal numbers [\#230](https://github.com/apache/arrow-rs/issues/230) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CSV Reader String Dictionary Support [\#228](https://github.com/apache/arrow-rs/issues/228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Builder interface for adding Arrays to record batches [\#210](https://github.com/apache/arrow-rs/issues/210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support auto-vectorization for min/max [\#209](https://github.com/apache/arrow-rs/issues/209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support LargeUtf8 in sort kernel [\#25](https://github.com/apache/arrow-rs/issues/25) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

-  no method named `select_nth_unstable_by` found for mutable reference `&mut [T]`  [\#283](https://github.com/apache/arrow-rs/issues/283)
- Rust 1.52 Clippy error [\#266](https://github.com/apache/arrow-rs/issues/266)
- NaNs can break parquet statistics [\#255](https://github.com/apache/arrow-rs/issues/255) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- u64::MAX does not roundtrip through parquet [\#254](https://github.com/apache/arrow-rs/issues/254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Integration tests failing to compile \(flatbuffer\) [\#249](https://github.com/apache/arrow-rs/issues/249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix compatibility quirks between arrow and parquet structs [\#245](https://github.com/apache/arrow-rs/issues/245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Unable to write non-null Arrow structs to Parquet [\#244](https://github.com/apache/arrow-rs/issues/244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- schema: missing field `metadata` when deserialize [\#241](https://github.com/apache/arrow-rs/issues/241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow does not compile due to flatbuffers upgrade [\#238](https://github.com/apache/arrow-rs/issues/238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sort with limit panics for the limit includes some but not all nulls, for large arrays [\#235](https://github.com/apache/arrow-rs/issues/235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-rs contains a copy of the "format" directory [\#233](https://github.com/apache/arrow-rs/issues/233) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix SEGFAULT/ SIGILL in child-data ffi [\#206](https://github.com/apache/arrow-rs/issues/206) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read list field correctly in \<struct\<list\>\> [\#167](https://github.com/apache/arrow-rs/issues/167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FFI listarray lead to undefined behavior.  [\#20](https://github.com/apache/arrow-rs/issues/20)

**Security fixes:**

- Fix MIRI build on CI [\#226](https://github.com/apache/arrow-rs/issues/226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Get MIRI running again [\#224](https://github.com/apache/arrow-rs/issues/224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Comment out the instructions in the PR template [\#277](https://github.com/apache/arrow-rs/issues/277)
- Update links to datafusion and ballista in README.md [\#19](https://github.com/apache/arrow-rs/issues/19)
- Update "repository" in Cargo.toml [\#12](https://github.com/apache/arrow-rs/issues/12)

**Closed issues:**

- Arrow Aligned Vec [\#268](https://github.com/apache/arrow-rs/issues/268)
- \[Rust\]: Tracking issue for AVX-512 [\#220](https://github.com/apache/arrow-rs/issues/220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Umbrella issue for clippy integration [\#217](https://github.com/apache/arrow-rs/issues/217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support sort [\#215](https://github.com/apache/arrow-rs/issues/215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support stable Rust [\#214](https://github.com/apache/arrow-rs/issues/214) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove Rust and point integration tests to arrow-rs repo [\#211](https://github.com/apache/arrow-rs/issues/211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ArrayData buffers are inconsistent accross implementations [\#207](https://github.com/apache/arrow-rs/issues/207)
- 3.0.1 patch release [\#204](https://github.com/apache/arrow-rs/issues/204)
- Document patch release process [\#202](https://github.com/apache/arrow-rs/issues/202)
- Simplify Offset [\#186](https://github.com/apache/arrow-rs/issues/186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Typed Bytes [\#185](https://github.com/apache/arrow-rs/issues/185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[CI\]docker-compose setup should enable caching [\#175](https://github.com/apache/arrow-rs/issues/175)
- Improve take primitive performance [\#174](https://github.com/apache/arrow-rs/issues/174)
- \[CI\] Try out buildkite [\#165](https://github.com/apache/arrow-rs/issues/165) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update assignees in JIRA where missing [\#160](https://github.com/apache/arrow-rs/issues/160)
- \[Rust\]: From\<ArrayDataRef\> implementations should validate data type [\#103](https://github.com/apache/arrow-rs/issues/103) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Verify that projection push down does not remove aliases columns [\#99](https://github.com/apache/arrow-rs/issues/99) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Implement modulus expression [\#98](https://github.com/apache/arrow-rs/issues/98) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Add constant folding to expressions during logically planning [\#96](https://github.com/apache/arrow-rs/issues/96) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] DataFrame.collect should return RecordBatchReader [\#95](https://github.com/apache/arrow-rs/issues/95) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Add FORMAT to explain plan and an easy to visualize format [\#94](https://github.com/apache/arrow-rs/issues/94) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement metrics framework [\#90](https://github.com/apache/arrow-rs/issues/90) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement micro benchmarks for each operator [\#89](https://github.com/apache/arrow-rs/issues/89) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement pretty print for physical query plan [\#88](https://github.com/apache/arrow-rs/issues/88) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Archery\] Support rust clippy in the lint command [\#83](https://github.com/apache/arrow-rs/issues/83)
- \[rust\]\[datafusion\] optimize count\(\*\) queries on parquet sources [\#75](https://github.com/apache/arrow-rs/issues/75) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Improve like/nlike performance [\#71](https://github.com/apache/arrow-rs/issues/71) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement optimizer rule to remove redundant projections [\#56](https://github.com/apache/arrow-rs/issues/56) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Parquet data source does not support complex types [\#39](https://github.com/apache/arrow-rs/issues/39) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Merge utils from Parquet and Arrow [\#32](https://github.com/apache/arrow-rs/issues/32) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add benchmarks for Parquet [\#30](https://github.com/apache/arrow-rs/issues/30) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Mark methods that do not perform bounds checking as unsafe [\#28](https://github.com/apache/arrow-rs/issues/28) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Test issue [\#24](https://github.com/apache/arrow-rs/issues/24) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- This is a test issue [\#11](https://github.com/apache/arrow-rs/issues/11)
