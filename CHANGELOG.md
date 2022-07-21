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

## [19.0.0](https://github.com/apache/arrow-rs/tree/19.0.0) (2022-07-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/18.0.0...19.0.0)

**Breaking changes:**

- Rename `DecimalArray``/DecimalBuilder` to `Decimal128Array`/`Decimal128Builder` [\#2101](https://github.com/apache/arrow-rs/issues/2101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change builder `append` methods to be infallible where possible [\#2103](https://github.com/apache/arrow-rs/pull/2103) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Return reference from `UnionArray::child` \(\#2035\) [\#2099](https://github.com/apache/arrow-rs/pull/2099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `preserve_order` feature from `serde_json` dependency \(\#2095\) [\#2098](https://github.com/apache/arrow-rs/pull/2098) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `weekday` and `weekday0` kernels to to `num_days_from_monday` and `days_since_sunday` [\#2066](https://github.com/apache/arrow-rs/pull/2066) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove `null_count` from `write_batch_with_statistics` [\#2047](https://github.com/apache/arrow-rs/pull/2047) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Parallel fetching of column chunks in ParquetRecordBatchStream [\#2110](https://github.com/apache/arrow-rs/issues/2110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Pass generate\_decimal256\_case integration test [\#2093](https://github.com/apache/arrow-rs/issues/2093) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rename `weekday` and `weekday0` kernels to to `num_days_from_monday` and `days_since_sunday` [\#2065](https://github.com/apache/arrow-rs/issues/2065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Improve performance of `filter_dict` [\#2062](https://github.com/apache/arrow-rs/issues/2062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `set_bits` [\#2060](https://github.com/apache/arrow-rs/issues/2060) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Lazily materialize the null buffer builder of `BooleanBuilder` [\#2058](https://github.com/apache/arrow-rs/issues/2058) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `BooleanArray::from_iter` should omit validity buffer if all values are valid [\#2055](https://github.com/apache/arrow-rs/issues/2055) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FFI\_ArrowSchema should set `DICTIONARY_ORDERED` flag if a field's dictionary is ordered [\#2049](https://github.com/apache/arrow-rs/issues/2049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `peek_next_page()` and `skip_next_page` in `SerializedPageReader` [\#2043](https://github.com/apache/arrow-rs/issues/2043) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support FFI / C Data Interface for `MapType` [\#2037](https://github.com/apache/arrow-rs/issues/2037) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The `DecimalArrayBuilder` should use `FixedSizedBinaryBuilder` [\#2026](https://github.com/apache/arrow-rs/issues/2026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- `type_id` and `value_offset` are incorrect for sliced `UnionArray` [\#2086](https://github.com/apache/arrow-rs/issues/2086) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Boolean `take` kernel does not handle null indices correctly [\#2057](https://github.com/apache/arrow-rs/issues/2057) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Don't double-count nulls in `write_batch_with_statistics` [\#2046](https://github.com/apache/arrow-rs/issues/2046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Writer Ignores Statistics specification in `WriterProperties` [\#2014](https://github.com/apache/arrow-rs/issues/2014) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
