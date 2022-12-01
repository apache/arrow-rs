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

## [object_store_0.5.2](https://github.com/apache/arrow-rs/tree/object_store_0.5.2) (2022-12-01)

[Full Changelog](https://github.com/apache/arrow-rs/compare/26.0.0_rc1...object_store_0.5.2)

**Implemented enhancements:**

- Casting from Decimal256 to unsigned numeric [\#3239](https://github.com/apache/arrow-rs/issues/3239)
- better document when we need `LargeUtf8` instead of `Utf8` [\#3228](https://github.com/apache/arrow-rs/issues/3228)
- precision is not considered when cast value to decimal [\#3223](https://github.com/apache/arrow-rs/issues/3223)
- Use RegexSet in arrow\_csv::infer\_field\_schema [\#3211](https://github.com/apache/arrow-rs/issues/3211)
- Optimize memory usage of json reader [\#3150](https://github.com/apache/arrow-rs/issues/3150)
- Add binary\_mut and try\_binary\_mut [\#3143](https://github.com/apache/arrow-rs/issues/3143)
- Add try\_unary\_mut [\#3133](https://github.com/apache/arrow-rs/issues/3133)
- Upgrade chrono to 0.4.23 [\#3120](https://github.com/apache/arrow-rs/issues/3120)
- socks5 proxy support for the object\_store crate [\#2989](https://github.com/apache/arrow-rs/issues/2989) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)]
- Compare dictionary decimal arrays [\#2981](https://github.com/apache/arrow-rs/issues/2981)
- Compare dictionary and non-dictionary decimal arrays [\#2979](https://github.com/apache/arrow-rs/issues/2979)
- Decimal Comparison kernels [\#2977](https://github.com/apache/arrow-rs/issues/2977)
- Use unary function for numeric to decimal cast [\#2972](https://github.com/apache/arrow-rs/issues/2972)
- Allow cloning and comparing CastOptions [\#2966](https://github.com/apache/arrow-rs/issues/2966)
- Add a way to get `byte_size` [\#2964](https://github.com/apache/arrow-rs/issues/2964)
- Pass Decompressed Size to Parquet Codec::decompress [\#2956](https://github.com/apache/arrow-rs/issues/2956)

**Fixed bugs:**

- object\_store\(aws\): EntityTooSmall error on multi-part upload [\#3233](https://github.com/apache/arrow-rs/issues/3233)
- bool should cast from/to Float16Type as `can_cast_types` returns true [\#3221](https://github.com/apache/arrow-rs/issues/3221)
- Utf8 and LargeUtf8 cannot cast from/to Float16 but can\_cast\_types returns true [\#3220](https://github.com/apache/arrow-rs/issues/3220)
- Re-enable some tests in `arrow-cast` crate [\#3219](https://github.com/apache/arrow-rs/issues/3219)
- arrow to and from pyarrow conversion results in changes in schema [\#3136](https://github.com/apache/arrow-rs/issues/3136)
- Converted type is None according to Parquet Tools then utilizing logical types [\#3017](https://github.com/apache/arrow-rs/issues/3017)
- Regression in 26.0.0 RC1: limit is not applied for multi-column sorts `lexsort_to_indices` [\#2990](https://github.com/apache/arrow-rs/issues/2990)
- Implicity setting of converted type is missing then setting [\#2984](https://github.com/apache/arrow-rs/issues/2984)

**Closed issues:**

- Release Arrow `28.0.0` \(next release after `27.0.0`\) [\#3118](https://github.com/apache/arrow-rs/issues/3118)

**Merged pull requests:**

- fix\(object\_store,aws,gcp\): multipart upload enforce size limit of 5 MiB not 5MB [\#3234](https://github.com/apache/arrow-rs/pull/3234) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([wjones127](https://github.com/wjones127))
- Final 28.0.0 CHANGELOG updates [\#3194](https://github.com/apache/arrow-rs/pull/3194) ([alamb](https://github.com/alamb))
- object\_store: add support for using proxy\_url for connection testing [\#3109](https://github.com/apache/arrow-rs/pull/3109) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([sum12](https://github.com/sum12))
- Minor: Add entry to changelog for 26.0.0 RC2 fix [\#2992](https://github.com/apache/arrow-rs/pull/2992) ([alamb](https://github.com/alamb))
- Update AWS SDK [\#2974](https://github.com/apache/arrow-rs/pull/2974) [[object-store](https://github.com/apache/arrow-rs/labels/object-store)] ([tustvold](https://github.com/tustvold))

## [26.0.0_rc1](https://github.com/apache/arrow-rs/tree/26.0.0_rc1) (2022-10-28)

[Full Changelog](https://github.com/apache/arrow-rs/compare/object_store_0.5.1...26.0.0_rc1)

**Breaking changes:**

- Cast Timestamps to RFC3339 strings [\#2934](https://github.com/apache/arrow-rs/issues/2934)

**Fixed bugs:**

- Subtle compatibility issue with serve\_arrow [\#2952](https://github.com/apache/arrow-rs/issues/2952)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
