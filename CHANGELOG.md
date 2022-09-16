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

## [23.0.0](https://github.com/apache/arrow-rs/tree/23.0.0) (2022-09-16)

[Full Changelog](https://github.com/apache/arrow-rs/compare/22.0.0...23.0.0)

**Breaking changes:**

- DictionaryBuilders can Create Invalid DictionaryArrays [\#2684](https://github.com/apache/arrow-rs/issues/2684) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- update the `flight.proto` and fix schema to SchemaResult [\#2571](https://github.com/apache/arrow-rs/issues/2571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- `SchemaResult` in IPC deviates from other implementations [\#2445](https://github.com/apache/arrow-rs/issues/2445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
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
- `arrow` crate does not build with `features = ["ffi"]` and `default_features = false`. [\#2670](https://github.com/apache/arrow-rs/issues/2670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Invalid results with `RowSelector` having `row_count` of 0 [\#2669](https://github.com/apache/arrow-rs/issues/2669) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- clippy error: unresolved import `crate::array::layout` [\#2659](https://github.com/apache/arrow-rs/issues/2659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast the numeric without the `CastOptions` [\#2648](https://github.com/apache/arrow-rs/issues/2648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Explicitly define overflow behavior for primitive arithmetic kernels [\#2641](https://github.com/apache/arrow-rs/issues/2641) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when first data page is skipped using ColumnChunkData::Sparse [\#2543](https://github.com/apache/arrow-rs/issues/2543) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
