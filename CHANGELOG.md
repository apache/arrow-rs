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

## [18.0.0](https://github.com/apache/arrow-rs/tree/18.0.0) (2022-07-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/17.0.0...18.0.0)

**Breaking changes:**

- Simplify ColumnReader::read\_batch [\#1995](https://github.com/apache/arrow-rs/pull/1995) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove PrimitiveBuilder::finish\_dict \(\#1978\) [\#1980](https://github.com/apache/arrow-rs/pull/1980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- add column index writer for parquet [\#1935](https://github.com/apache/arrow-rs/pull/1935) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))

**Implemented enhancements:**

- Add dictionary support to subtract\_scalar, multiply\_scalar, divide\_scalar [\#2019](https://github.com/apache/arrow-rs/issues/2019)
- Support DictionaryArray in add\_scalar kernel [\#2017](https://github.com/apache/arrow-rs/issues/2017)
- Simplify the `FixedSizeBinaryBuilder` [\#2007](https://github.com/apache/arrow-rs/issues/2007)
- Fix New Clippy Lints [\#1996](https://github.com/apache/arrow-rs/issues/1996)
-  Support DictionaryArray in unary kernel [\#1989](https://github.com/apache/arrow-rs/issues/1989)
- Kernel to quickly compute comparisons of arrays [\#1987](https://github.com/apache/arrow-rs/issues/1987) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Move the `DecimalArray` to a new file. [\#1985](https://github.com/apache/arrow-rs/issues/1985)
- Implement Into\<ArrayData\> for T: Array [\#1979](https://github.com/apache/arrow-rs/issues/1979)
-  Support DictionaryArray in multiply kernel [\#1974](https://github.com/apache/arrow-rs/issues/1974)
-  Support DictionaryArray in multiply kernel [\#1972](https://github.com/apache/arrow-rs/issues/1972)
- Support DictionaryArray in subtract kernel [\#1970](https://github.com/apache/arrow-rs/issues/1970)
- Declare `DecimalArray::length` as a constant [\#1967](https://github.com/apache/arrow-rs/issues/1967)
- Support multi diskRanges for ChunkReader [\#1955](https://github.com/apache/arrow-rs/issues/1955)
- Support DictionaryArray in add kernel [\#1950](https://github.com/apache/arrow-rs/issues/1950)
- Faster StringDictionaryBuilder [\#1851](https://github.com/apache/arrow-rs/issues/1851)
- Sperate get\_next\_page\_header from get\_next\_page in PageReader [\#1834](https://github.com/apache/arrow-rs/issues/1834)
- `concat_elements_utf8` should accept arbitrary number of input arrays.  [\#1748](https://github.com/apache/arrow-rs/issues/1748)

**Fixed bugs:**

- RowFormatter is not part of the public api [\#2008](https://github.com/apache/arrow-rs/issues/2008)
- Prevent Infinite Loop in ColumnReader::read\_batch For Corrupted Files [\#1997](https://github.com/apache/arrow-rs/issues/1997) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- CI continues failed on /usr/bin/ld: final link failed: No space left on device [\#1993](https://github.com/apache/arrow-rs/issues/1993)
- PrimitiveBuilder::finish\_dict does not validate dictionary offsets [\#1978](https://github.com/apache/arrow-rs/issues/1978)
- Incorrect null\_count of DictionaryArray [\#1962](https://github.com/apache/arrow-rs/issues/1962)
- Incorrect n\_buffers in FFI\_ArrowArray [\#1959](https://github.com/apache/arrow-rs/issues/1959)
- `DecimalArray::from_fixed_size_list_array` fails when `offset > 0` [\#1958](https://github.com/apache/arrow-rs/issues/1958)
- Write error ColumnChunk to the Parquet File instead of ColumnMetaData [\#1946](https://github.com/apache/arrow-rs/issues/1946)
- Send + Sync impl for Allocation might be unsound [\#1944](https://github.com/apache/arrow-rs/issues/1944)
- Disallow cast from other datatypes to NullType [\#1923](https://github.com/apache/arrow-rs/issues/1923)
- The doc of `FixedSizeListArray::value_length` is incorrect. [\#1908](https://github.com/apache/arrow-rs/issues/1908)

**Closed issues:**

- Column chunk statistics of min\_bytes and  max\_bytes return wrong size [\#2021](https://github.com/apache/arrow-rs/issues/2021)
- close function instead of mutable reference [\#1969](https://github.com/apache/arrow-rs/issues/1969)
- Persisting Arrow timestamps with Parquet produces missing `TIMESTAMP` in schema [\#1920](https://github.com/apache/arrow-rs/issues/1920) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Add dictionary support to subtract\_scalar, multiply\_scalar, divide\_scalar [\#2020](https://github.com/apache/arrow-rs/pull/2020) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support DictionaryArray in add\_scalar kernel [\#2018](https://github.com/apache/arrow-rs/pull/2018) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Refine the `FixedSizeBinaryBuilder` [\#2013](https://github.com/apache/arrow-rs/pull/2013) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add RowFormatter to record public API [\#2009](https://github.com/apache/arrow-rs/pull/2009) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([FabioBatSilva](https://github.com/FabioBatSilva))
- Stub out Skip Records API \(\#1792\) [\#1998](https://github.com/apache/arrow-rs/pull/1998) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Implements Into\<ArrayData\> for T: Array [\#1992](https://github.com/apache/arrow-rs/pull/1992) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([heyrutvik](https://github.com/heyrutvik))
- Add unary\_cmp [\#1991](https://github.com/apache/arrow-rs/pull/1991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support DictionaryArray in unary kernel [\#1990](https://github.com/apache/arrow-rs/pull/1990) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Refine `FixedSizeListBuilder` [\#1988](https://github.com/apache/arrow-rs/pull/1988) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Move `DecimalArray` to array\_decimal.rs [\#1986](https://github.com/apache/arrow-rs/pull/1986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- MINOR: Fix clippy error after updating rust toolchain [\#1984](https://github.com/apache/arrow-rs/pull/1984) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Support dictionary array for subtract and multiply kernel [\#1971](https://github.com/apache/arrow-rs/pull/1971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Declare the value\_length of decimal array as a `const` [\#1968](https://github.com/apache/arrow-rs/pull/1968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix the behavior of `from_fixed_size_list` when offset \> 0 [\#1964](https://github.com/apache/arrow-rs/pull/1964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Calculate n\_buffers in FFI\_ArrowArray by data layout [\#1960](https://github.com/apache/arrow-rs/pull/1960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix the doc of `FixedSizeListArray::value_length` [\#1957](https://github.com/apache/arrow-rs/pull/1957) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use InMemoryColumnChunkReader \(~20% faster\) [\#1956](https://github.com/apache/arrow-rs/pull/1956) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Unpin clap \(\#1867\) [\#1954](https://github.com/apache/arrow-rs/pull/1954) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Set is\_adjusted\_to\_utc if any timezone set \(\#1932\) [\#1953](https://github.com/apache/arrow-rs/pull/1953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add add\_dyn for DictionaryArray support [\#1951](https://github.com/apache/arrow-rs/pull/1951) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- write columnmetadata to the behind of the column chunk data, not the ColumnChunk [\#1947](https://github.com/apache/arrow-rs/pull/1947) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Require Send+Sync bounds for Allocation trait [\#1945](https://github.com/apache/arrow-rs/pull/1945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Disallow cast from other datatypes to NullType [\#1942](https://github.com/apache/arrow-rs/pull/1942) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
-  Faster StringDictionaryBuilder \(~60% faster\) \(\#1851\)  [\#1861](https://github.com/apache/arrow-rs/pull/1861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Arbitrary size concat elements utf8 [\#1787](https://github.com/apache/arrow-rs/pull/1787) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
