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

## [24.0.0](https://github.com/apache/arrow-rs/tree/24.0.0) (2022-09-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/23.0.0...24.0.0)

**Breaking changes:**

- Cleanup ArrowNativeType \(\#1918\) [\#2793](https://github.com/apache/arrow-rs/pull/2793) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove ArrowNativeType: FromStr [\#2775](https://github.com/apache/arrow-rs/pull/2775) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out `arrow-array`  crate \(\#2594\) [\#2769](https://github.com/apache/arrow-rs/pull/2769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add dyn\_arith\_dict feature flag [\#2760](https://github.com/apache/arrow-rs/pull/2760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out `arrow-data` into a separate crate [\#2746](https://github.com/apache/arrow-rs/pull/2746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-schema \(\#2594\) [\#2711](https://github.com/apache/arrow-rs/pull/2711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Include field name in Parquet PrimitiveTypeBuilder error messages [\#2804](https://github.com/apache/arrow-rs/issues/2804)
- Add PrimitiveArray::reinterpret\_cast [\#2785](https://github.com/apache/arrow-rs/issues/2785)
- BinaryBuilder and StringBuilder initialization parameters in struct\_builder may be wrong [\#2783](https://github.com/apache/arrow-rs/issues/2783)
- Add divide scalar dyn kernel which produces null for division by zero [\#2767](https://github.com/apache/arrow-rs/issues/2767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide dyn kernel which produces null for division by zero [\#2763](https://github.com/apache/arrow-rs/issues/2763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of checked kernels on non-null data [\#2747](https://github.com/apache/arrow-rs/issues/2747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variants of arithmetic dyn kernels [\#2739](https://github.com/apache/arrow-rs/issues/2739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use memmap for local files in object\_store [\#2738](https://github.com/apache/arrow-rs/issues/2738)
- The `binary` function should not panic on unequaled array length. [\#2721](https://github.com/apache/arrow-rs/issues/2721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- min compute kernel is incorrect with sliced buffers in arrow 23 [\#2779](https://github.com/apache/arrow-rs/issues/2779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add back JSON import/export for schema [\#2762](https://github.com/apache/arrow-rs/issues/2762)
- null casting and coercion for Decimal128  [\#2761](https://github.com/apache/arrow-rs/issues/2761)
- `try_unary_dict` should check value type of dictionary array [\#2754](https://github.com/apache/arrow-rs/issues/2754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Json decoder behavior changed from versions 21 to 21 and returns non-sensical num\_rows for RecordBatch [\#2722](https://github.com/apache/arrow-rs/issues/2722)

**Closed issues:**

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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
