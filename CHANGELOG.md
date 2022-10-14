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

## [25.0.0](https://github.com/apache/arrow-rs/tree/25.0.0) (2022-10-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/24.0.0...25.0.0)

**Breaking changes:**

- fix timestamp parsing while no explicit timezone given [\#2814](https://github.com/apache/arrow-rs/pull/2814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Support Arbitrary Number of Arrays in downcast\_primitive\_array [\#2809](https://github.com/apache/arrow-rs/pull/2809) ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Integration test JSON schema serialization format was removed   [\#2876](https://github.com/apache/arrow-rs/issues/2876) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix various invalid\_html\_tags clippy error [\#2861](https://github.com/apache/arrow-rs/issues/2861)
- Replace complicated temporal macro with generic functions [\#2851](https://github.com/apache/arrow-rs/issues/2851)
- Add NaN handling in dyn scalar comparison kernels [\#2829](https://github.com/apache/arrow-rs/issues/2829)
- Add overflow-checking variant of sum kernel [\#2821](https://github.com/apache/arrow-rs/issues/2821)
- Update to Clap 4 [\#2817](https://github.com/apache/arrow-rs/issues/2817)
- Safe API to Operate on Dictionary Values [\#2797](https://github.com/apache/arrow-rs/issues/2797) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add modulus op into `ArrowNativeTypeOp` [\#2753](https://github.com/apache/arrow-rs/issues/2753) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow creating of TimeUnit instances without direct dependency on parquet-format [\#2708](https://github.com/apache/arrow-rs/issues/2708) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Arrow Row Format [\#2677](https://github.com/apache/arrow-rs/issues/2677)

**Fixed bugs:**

- Don't try to infer nulls in CSV schema inference [\#2859](https://github.com/apache/arrow-rs/issues/2859)
- `parquet::arrow::arrow_writer::ArrowWriter` ignores page size properties [\#2853](https://github.com/apache/arrow-rs/issues/2853)
- Introducing ArrowNativeTypeOp made it impossible to call kernels from generics [\#2839](https://github.com/apache/arrow-rs/issues/2839)
- Unsound ArrayData to Array Conversions [\#2834](https://github.com/apache/arrow-rs/issues/2834)
- Regression: `the trait bound for<'de> arrow::datatypes::Schema: serde::de::Deserialize<'de> is not satisfied` [\#2825](https://github.com/apache/arrow-rs/issues/2825)
- convert string to timestamp shouldn't apply local timezone offset if there's no explicit timezone info in the string [\#2813](https://github.com/apache/arrow-rs/issues/2813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add pub api for checking column index is sorted [\#2848](https://github.com/apache/arrow-rs/issues/2848)

**Merged pull requests:**

- Take decimal as primitive \(\#2637\) [\#2869](https://github.com/apache/arrow-rs/pull/2869) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-integration-test crate [\#2868](https://github.com/apache/arrow-rs/pull/2868) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Decimal cleanup \(\#2637\) [\#2865](https://github.com/apache/arrow-rs/pull/2865) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix various invalid\_html\_tags clippy errors [\#2862](https://github.com/apache/arrow-rs/pull/2862) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Don't try to infer nullability in CSV reader [\#2860](https://github.com/apache/arrow-rs/pull/2860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make DecimalArray as PrimitiveArray [\#2857](https://github.com/apache/arrow-rs/pull/2857) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix page size on dictionary fallback [\#2854](https://github.com/apache/arrow-rs/pull/2854) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Replace complicated temporal macro with generic functions [\#2850](https://github.com/apache/arrow-rs/pull/2850) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- \[feat\] Add pub api for checking column index is sorted. [\#2849](https://github.com/apache/arrow-rs/pull/2849) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- parquet: Add `snap` option to README [\#2847](https://github.com/apache/arrow-rs/pull/2847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([exyi](https://github.com/exyi))
- Cleanup cast kernel [\#2846](https://github.com/apache/arrow-rs/pull/2846) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify ArrowNativeType [\#2841](https://github.com/apache/arrow-rs/pull/2841) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Expose ArrowNativeTypeOp trait to make it useful for type bound [\#2840](https://github.com/apache/arrow-rs/pull/2840) ([viirya](https://github.com/viirya))
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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
