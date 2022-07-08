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

- Array reader for list columns fails to decode if batches fall on row group boundaries [\#2025](https://github.com/apache/arrow-rs/issues/2025)
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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
