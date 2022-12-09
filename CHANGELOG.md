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

## [29.0.0](https://github.com/apache/arrow-rs/tree/29.0.0) (2022-12-09)

[Full Changelog](https://github.com/apache/arrow-rs/compare/28.0.0...29.0.0)

**Breaking changes:**

- Minor: Allow `Field::new` and `Field::new_with_dict` to take existing `String` as well as `&str` [\#3288](https://github.com/apache/arrow-rs/pull/3288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- update `&Option<T>` to `Option<&T>` [\#3249](https://github.com/apache/arrow-rs/pull/3249) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Hide `*_dict_scalar` kernels behind `*_dyn` kernels [\#3202](https://github.com/apache/arrow-rs/pull/3202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Support writing BloomFilter in arrow\_writer [\#3275](https://github.com/apache/arrow-rs/issues/3275) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from unsigned numeric to Decimal256 [\#3272](https://github.com/apache/arrow-rs/issues/3272) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting from Decimal256 to float types [\#3266](https://github.com/apache/arrow-rs/issues/3266) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make arithmetic kernels supports DictionaryArray of DecimalType [\#3254](https://github.com/apache/arrow-rs/issues/3254) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Casting from Decimal256 to unsigned numeric [\#3239](https://github.com/apache/arrow-rs/issues/3239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- precision is not considered when cast value to decimal [\#3223](https://github.com/apache/arrow-rs/issues/3223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use RegexSet in arrow\_csv::infer\_field\_schema [\#3211](https://github.com/apache/arrow-rs/issues/3211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement FlightSQL Client [\#3206](https://github.com/apache/arrow-rs/issues/3206) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add binary\_mut and try\_binary\_mut [\#3143](https://github.com/apache/arrow-rs/issues/3143) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add try\_unary\_mut [\#3133](https://github.com/apache/arrow-rs/issues/3133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Skip null buffer when importing FFI ArrowArray struct if no null buffer in the spec [\#3290](https://github.com/apache/arrow-rs/issues/3290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- using ahash `compile-time-rng` kills reproducible builds [\#3271](https://github.com/apache/arrow-rs/issues/3271) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Decimal128 to Decimal256 Overflows [\#3265](https://github.com/apache/arrow-rs/issues/3265) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `nullif` panics on empty array [\#3261](https://github.com/apache/arrow-rs/issues/3261) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Some more inconsistency between can\_cast\_types  and cast\_with\_options [\#3250](https://github.com/apache/arrow-rs/issues/3250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Enable casting between Dictionary of DecimalArray and DecimalArray [\#3237](https://github.com/apache/arrow-rs/issues/3237) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- new\_null\_array Panics creating StructArray with non-nullable fields [\#3226](https://github.com/apache/arrow-rs/issues/3226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- bool should cast from/to Float16Type as `can_cast_types` returns true [\#3221](https://github.com/apache/arrow-rs/issues/3221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Utf8 and LargeUtf8 cannot cast from/to Float16 but can\_cast\_types returns true [\#3220](https://github.com/apache/arrow-rs/issues/3220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Re-enable some tests in `arrow-cast` crate [\#3219](https://github.com/apache/arrow-rs/issues/3219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Off-by-one buffer size error triggers Panic when constructing RecordBatch from IPC bytes \(should return an Error\) [\#3215](https://github.com/apache/arrow-rs/issues/3215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow to and from pyarrow conversion results in changes in schema [\#3136](https://github.com/apache/arrow-rs/issues/3136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- better document when we need `LargeUtf8` instead of `Utf8` [\#3228](https://github.com/apache/arrow-rs/issues/3228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Use BufWriter when writing bloom filters and limit tests \(\#3318\) [\#3319](https://github.com/apache/arrow-rs/pull/3319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use take for dictionary like comparisons [\#3313](https://github.com/apache/arrow-rs/pull/3313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update versions to  29.0.0 and update CHANGELOG [\#3315](https://github.com/apache/arrow-rs/pull/3315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- refactor: Merge similar functions `ilike_scalar` and `nilike_scalar` [\#3303](https://github.com/apache/arrow-rs/pull/3303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Split out arrow-ord \(\#2594\) [\#3299](https://github.com/apache/arrow-rs/pull/3299) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-string \(\#2594\) [\#3295](https://github.com/apache/arrow-rs/pull/3295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Skip null buffer when importing FFI ArrowArray struct if no null buffer in the spec [\#3293](https://github.com/apache/arrow-rs/pull/3293) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Don't use dangling NonNull as sentinel [\#3289](https://github.com/apache/arrow-rs/pull/3289) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Set bloom filter on byte array [\#3284](https://github.com/apache/arrow-rs/pull/3284) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Fix ipc schema custom\_metadata serialization [\#3282](https://github.com/apache/arrow-rs/pull/3282) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Disable const-random ahash feature on non-WASM \(\#3271\) [\#3277](https://github.com/apache/arrow-rs/pull/3277) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- fix\(ffi\): handle null data buffers from empty arrays [\#3276](https://github.com/apache/arrow-rs/pull/3276) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Support casting from unsigned numeric to Decimal256 [\#3273](https://github.com/apache/arrow-rs/pull/3273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add parquet-layout binary [\#3269](https://github.com/apache/arrow-rs/pull/3269) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support casting from Decimal256 to float types [\#3267](https://github.com/apache/arrow-rs/pull/3267) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify decimal cast logic [\#3264](https://github.com/apache/arrow-rs/pull/3264) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix panic on nullif empty array \(\#3261\) [\#3263](https://github.com/apache/arrow-rs/pull/3263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BooleanArray::from\_unary and BooleanArray::from\_binary [\#3258](https://github.com/apache/arrow-rs/pull/3258) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Remove parquet build script [\#3257](https://github.com/apache/arrow-rs/pull/3257) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make arithmetic kernels supports DictionaryArray of DecimalType [\#3255](https://github.com/apache/arrow-rs/pull/3255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support List and LargeList in Row format \(\#3159\) [\#3251](https://github.com/apache/arrow-rs/pull/3251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't recurse to children in ArrayData::try\_new [\#3248](https://github.com/apache/arrow-rs/pull/3248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Validate dictionaries read over IPC [\#3247](https://github.com/apache/arrow-rs/pull/3247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix MapBuilder example [\#3246](https://github.com/apache/arrow-rs/pull/3246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Loosen nullability restrictions added in \#3205 \(\#3226\) [\#3244](https://github.com/apache/arrow-rs/pull/3244) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Better document implications of offsets \(\#3228\) [\#3243](https://github.com/apache/arrow-rs/pull/3243) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add new API to validate the precision for decimal array [\#3242](https://github.com/apache/arrow-rs/pull/3242) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Move nullif to arrow-select \(\#2594\) [\#3241](https://github.com/apache/arrow-rs/pull/3241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Casting from Decimal256 to unsigned numeric [\#3240](https://github.com/apache/arrow-rs/pull/3240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Enable casting between Dictionary of DecimalArray and DecimalArray [\#3238](https://github.com/apache/arrow-rs/pull/3238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove unwraps from 'create\_primitive\_array' [\#3232](https://github.com/apache/arrow-rs/pull/3232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([aarashy](https://github.com/aarashy))
- Fix CI build by upgrading tonic-build to 0.8.4 [\#3231](https://github.com/apache/arrow-rs/pull/3231) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Remove negative scale check [\#3230](https://github.com/apache/arrow-rs/pull/3230) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update prost-build requirement from =0.11.2 to =0.11.3 [\#3225](https://github.com/apache/arrow-rs/pull/3225) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Get the round result for decimal to a decimal with smaller scale  [\#3224](https://github.com/apache/arrow-rs/pull/3224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Move tests which require chrono-tz feature from `arrow-cast` to `arrow` [\#3222](https://github.com/apache/arrow-rs/pull/3222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- add test cases for extracing week with/without timezone [\#3218](https://github.com/apache/arrow-rs/pull/3218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Use RegexSet for matching DataType [\#3217](https://github.com/apache/arrow-rs/pull/3217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Update tonic-build to 0.8.3 [\#3214](https://github.com/apache/arrow-rs/pull/3214) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Support StructArray in Row Format \(\#3159\) [\#3212](https://github.com/apache/arrow-rs/pull/3212) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Infer timestamps from CSV files [\#3209](https://github.com/apache/arrow-rs/pull/3209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- fix bug: cast decimal256 to other decimal with no-safe [\#3208](https://github.com/apache/arrow-rs/pull/3208) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- FlightSQL Client & integration test [\#3207](https://github.com/apache/arrow-rs/pull/3207) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Ensure StructArrays check nullability of fields [\#3205](https://github.com/apache/arrow-rs/pull/3205) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Remove special case ArrayData equality for decimals [\#3204](https://github.com/apache/arrow-rs/pull/3204) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add a cast test case for decimal negative scale [\#3203](https://github.com/apache/arrow-rs/pull/3203) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Move zip and shift kernels to arrow-select [\#3201](https://github.com/apache/arrow-rs/pull/3201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate limit kernel [\#3200](https://github.com/apache/arrow-rs/pull/3200) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use SlicesIterator for ArrayData Equality [\#3198](https://github.com/apache/arrow-rs/pull/3198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add \_dyn kernels of like, ilike, nlike, nilike kernels for dictionary support [\#3197](https://github.com/apache/arrow-rs/pull/3197) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Adding scalar nlike\_dyn, ilike\_dyn, nilike\_dyn kernels [\#3195](https://github.com/apache/arrow-rs/pull/3195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Use self capture in DataType [\#3190](https://github.com/apache/arrow-rs/pull/3190) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- To pyarrow with schema [\#3188](https://github.com/apache/arrow-rs/pull/3188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- Support Duration in array\_value\_to\_string [\#3183](https://github.com/apache/arrow-rs/pull/3183) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Support `FixedSizeBinary` in Row format [\#3182](https://github.com/apache/arrow-rs/pull/3182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add binary\_mut and try\_binary\_mut [\#3144](https://github.com/apache/arrow-rs/pull/3144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add try\_unary\_mut [\#3134](https://github.com/apache/arrow-rs/pull/3134) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
