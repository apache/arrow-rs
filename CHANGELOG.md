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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
