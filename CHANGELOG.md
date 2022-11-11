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

## [27.0.0](https://github.com/apache/arrow-rs/tree/27.0.0) (2022-11-11)

[Full Changelog](https://github.com/apache/arrow-rs/compare/26.0.0...27.0.0)

**Breaking changes:**

- Recurse into Dictionary value type in DataType::is\_nested [\#3083](https://github.com/apache/arrow-rs/pull/3083) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- early type checks in `RowConverter` [\#3080](https://github.com/apache/arrow-rs/pull/3080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add Decimal128 and Decimal256 to downcast\_primitive [\#3056](https://github.com/apache/arrow-rs/pull/3056) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Validate decimal256 with i256 directly [\#3025](https://github.com/apache/arrow-rs/pull/3025) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace hour\_generic with hour\_dyn [\#3006](https://github.com/apache/arrow-rs/pull/3006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Accept any &dyn Array in nullif kernel [\#2940](https://github.com/apache/arrow-rs/pull/2940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Row Format: Option to detach/own a row [\#3078](https://github.com/apache/arrow-rs/issues/3078)
- Row Format: API to check if datatypes are supported [\#3077](https://github.com/apache/arrow-rs/issues/3077)
- Deprecate Buffer::count\_set\_bits [\#3067](https://github.com/apache/arrow-rs/issues/3067)
- Add Decimal128 and Decimal256 to downcast\_primitive [\#3055](https://github.com/apache/arrow-rs/issues/3055)
- Cast decimal256 to signed integer [\#3039](https://github.com/apache/arrow-rs/issues/3039)
- Support casting Date64 to Timestamp [\#3037](https://github.com/apache/arrow-rs/issues/3037)
- Check overflow when casting floating point value to decimal256 [\#3032](https://github.com/apache/arrow-rs/issues/3032)
- Compare i256 in validate\_decimal256\_precision [\#3024](https://github.com/apache/arrow-rs/issues/3024)
- Check overflow when casting floating point value to decimal128 [\#3020](https://github.com/apache/arrow-rs/issues/3020)
- Some clippy errors after updating rust toolchain [\#3011](https://github.com/apache/arrow-rs/issues/3011)
- Add macro downcast\_temporal\_array [\#3008](https://github.com/apache/arrow-rs/issues/3008)
- Replace hour\_generic with hour\_dyn [\#3005](https://github.com/apache/arrow-rs/issues/3005)
- Replace temporal \_generic kernels with dyn [\#3004](https://github.com/apache/arrow-rs/issues/3004)
- Add `RowSelection::intersection` [\#3003](https://github.com/apache/arrow-rs/issues/3003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- I would like to round rather than truncate when casting f64 to decimal [\#2997](https://github.com/apache/arrow-rs/issues/2997)
- arrow::compute::kernels::temporal should support nanoseconds [\#2995](https://github.com/apache/arrow-rs/issues/2995)
- Release Arrow `26.0.0` \(next release after `25.0.0`\) [\#2953](https://github.com/apache/arrow-rs/issues/2953)
- Add timezone offset for debug format of Timestamp with Timezone [\#2917](https://github.com/apache/arrow-rs/issues/2917)
- Support merge RowSelectors when creating RowSelection [\#2858](https://github.com/apache/arrow-rs/issues/2858)

**Fixed bugs:**

- Inconsistent Nan Handling Between Scalar and Non-Scalar Comparison Kernels [\#3074](https://github.com/apache/arrow-rs/issues/3074)
- Debug format for timestamp ignores timezone [\#3069](https://github.com/apache/arrow-rs/issues/3069)
- Row format decode loses timezone [\#3063](https://github.com/apache/arrow-rs/issues/3063)
- binary operator produces incorrect result on arrays with resized null buffer [\#3061](https://github.com/apache/arrow-rs/issues/3061)
- RLEDecoder Panics on Null Padded Pages [\#3035](https://github.com/apache/arrow-rs/issues/3035) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nullif with incorrect valid\_count [\#3031](https://github.com/apache/arrow-rs/issues/3031)
- RLEDecoder::get\_batch\_with\_dict may panic on bit-packed runs longer than 1024 [\#3029](https://github.com/apache/arrow-rs/issues/3029)
- Converted type is None according to Parquet Tools then utilizing logical types [\#3017](https://github.com/apache/arrow-rs/issues/3017)
- CompressionCodec LZ4 incompatible with C++ implementation [\#2988](https://github.com/apache/arrow-rs/issues/2988)

**Documentation updates:**

- Mark parquet predicate pushdown as complete [\#2987](https://github.com/apache/arrow-rs/pull/2987) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Merged pull requests:**

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
- Replace remaining \_generic temporal kernels with \_dyn kernels [\#3046](https://github.com/apache/arrow-rs/pull/3046) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split out arrow-csv \(\#2594\) [\#3044](https://github.com/apache/arrow-rs/pull/3044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move reader\_parser to arrow-cast \(\#3022\) [\#3043](https://github.com/apache/arrow-rs/pull/3043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Replace year\_generic with year\_dyn [\#3041](https://github.com/apache/arrow-rs/pull/3041) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
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
- Hadoop LZ4 Support for LZ4 Codec [\#3013](https://github.com/apache/arrow-rs/pull/3013) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
