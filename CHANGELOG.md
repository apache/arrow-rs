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

## [20.0.0](https://github.com/apache/arrow-rs/tree/20.0.0) (2022-08-05)

[Full Changelog](https://github.com/apache/arrow-rs/compare/19.0.0...20.0.0)

**Breaking changes:**

- Add more const evaluation for `GenericBinaryArray` and `GenericListArray`: add `PREFIX` and data type constructor [\#2327](https://github.com/apache/arrow-rs/pull/2327) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Make FFI support optional, change APIs to be `safe` \(\#2302\) [\#2303](https://github.com/apache/arrow-rs/pull/2303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `test_utils` from default features \(\#2298\) [\#2299](https://github.com/apache/arrow-rs/pull/2299) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `DataType::Decimal` to `DataType::Decimal128` [\#2229](https://github.com/apache/arrow-rs/pull/2229) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `Decimal128Iter` and `Decimal256Iter` and do maximum precision/scale check [\#2140](https://github.com/apache/arrow-rs/pull/2140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Add the constant data type constructors for `ListArray` [\#2311](https://github.com/apache/arrow-rs/issues/2311) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update `FlightSqlService` trait to pass session info along [\#2308](https://github.com/apache/arrow-rs/issues/2308) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Optimize `take_bits` for non-null indices [\#2306](https://github.com/apache/arrow-rs/issues/2306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make FFI support optional via Feature Flag `ffi` [\#2302](https://github.com/apache/arrow-rs/issues/2302) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Mark `ffi::ArrowArray::try_new` is safe [\#2301](https://github.com/apache/arrow-rs/issues/2301) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove test\_utils from default arrow-rs features [\#2298](https://github.com/apache/arrow-rs/issues/2298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove `JsonEqual` trait [\#2296](https://github.com/apache/arrow-rs/issues/2296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Move `with_precision_and_scale` to `Decimal` array traits [\#2291](https://github.com/apache/arrow-rs/issues/2291) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve readability and maybe performance of string --\> numeric/time/date/timetamp cast kernels [\#2285](https://github.com/apache/arrow-rs/issues/2285) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add vectorized unpacking for 8, 16, and 64 bit integers [\#2276](https://github.com/apache/arrow-rs/issues/2276) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use initial capacity for interner hashmap [\#2273](https://github.com/apache/arrow-rs/issues/2273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Impl FromIterator for Decimal256Array [\#2248](https://github.com/apache/arrow-rs/issues/2248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Separate `ArrayReader::next_batch`with `ArrayReader::read_records` and `ArrayReader::consume_batch` [\#2236](https://github.com/apache/arrow-rs/issues/2236) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rename `DataType::Decimal` to `DataType::Decimal128` [\#2228](https://github.com/apache/arrow-rs/issues/2228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Automatically Grow Parquet BitWriter Buffer [\#2226](https://github.com/apache/arrow-rs/issues/2226) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `append_option` support to `Decimal128Builder` and `Decimal256Builder` [\#2224](https://github.com/apache/arrow-rs/issues/2224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Split the `FixedSizeBinaryArray` and `FixedSizeListArray` from `array_binary.rs` and `array_list.rs` [\#2217](https://github.com/apache/arrow-rs/issues/2217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Don't `Box` Values in `PrimitiveDictionaryBuilder` [\#2215](https://github.com/apache/arrow-rs/issues/2215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use BitChunks in equal\_bits [\#2186](https://github.com/apache/arrow-rs/issues/2186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Hash` for `Schema` [\#2182](https://github.com/apache/arrow-rs/issues/2182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- read decimal data type from parquet file with binary physical type [\#2159](https://github.com/apache/arrow-rs/issues/2159) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `GenericStringBuilder` should use `GenericBinaryBuilder` [\#2156](https://github.com/apache/arrow-rs/issues/2156) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update Rust version to 1.62 [\#2143](https://github.com/apache/arrow-rs/issues/2143) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Check precision and scale against maximum value when constructing `Decimal128` and `Decimal256` [\#2139](https://github.com/apache/arrow-rs/issues/2139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `ArrayAccessor` in `Decimal128Iter` and `Decimal256Iter` [\#2138](https://github.com/apache/arrow-rs/issues/2138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `ArrayAccessor` and `FromIterator` in Cast Kernels [\#2137](https://github.com/apache/arrow-rs/issues/2137) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `TypedDictionaryArray` for more ergonomic interaction with `DictionaryArray` [\#2136](https://github.com/apache/arrow-rs/issues/2136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `ArrayAccessor` in Comparison Kernels [\#2135](https://github.com/apache/arrow-rs/issues/2135) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `peek_next_page()` and s`kip_next_page` in `InMemoryColumnChunkReader` [\#2129](https://github.com/apache/arrow-rs/issues/2129) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Lazily materialize the null buffer builder for all array builders. [\#2125](https://github.com/apache/arrow-rs/issues/2125) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Do value validation for `Decimal256` [\#2112](https://github.com/apache/arrow-rs/issues/2112) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `skip_def_levels`  for `ColumnLevelDecoder` [\#2107](https://github.com/apache/arrow-rs/issues/2107) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add integration test for scan rows  with selection [\#2106](https://github.com/apache/arrow-rs/issues/2106) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support for casting from Utf8/String to `Time32` / `Time64` [\#2053](https://github.com/apache/arrow-rs/issues/2053) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update prost and tonic related crates [\#2268](https://github.com/apache/arrow-rs/pull/2268) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([carols10cents](https://github.com/carols10cents))

**Fixed bugs:**

- temporal conversion functions cannot work on negative input properly [\#2325](https://github.com/apache/arrow-rs/issues/2325) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- IPC writer should truncate string array with all empty string [\#2312](https://github.com/apache/arrow-rs/issues/2312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Error order for comparing `Decimal128` or `Decimal256` [\#2256](https://github.com/apache/arrow-rs/issues/2256) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix maximum and minimum for decimal values for precision greater than 38 [\#2246](https://github.com/apache/arrow-rs/issues/2246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `IntervalMonthDayNanoType::make_value()` does not match C implementation [\#2234](https://github.com/apache/arrow-rs/issues/2234) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `FlightSqlService` trait does not allow `impl`s to do handshake [\#2210](https://github.com/apache/arrow-rs/issues/2210) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- `EnabledStatistics::None` not working [\#2185](https://github.com/apache/arrow-rs/issues/2185) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Boolean ArrayData Equality Incorrect Slice Handling [\#2184](https://github.com/apache/arrow-rs/issues/2184) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Publicly export MapFieldNames [\#2118](https://github.com/apache/arrow-rs/issues/2118) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Update instructions on How to join the slack \#arrow-rust channel -- or maybe try to switch to discord?? [\#2192](https://github.com/apache/arrow-rs/issues/2192)
- \[Minor\] Improve arrow and parquet READMEs, document parquet feature flags [\#2324](https://github.com/apache/arrow-rs/pull/2324) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve speed of writing string dictionaries to parquet by skipping a copy\(\#1764\)  [\#2322](https://github.com/apache/arrow-rs/pull/2322) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- Fix wrong logic in calculate\_row\_count when skipping values [\#2328](https://github.com/apache/arrow-rs/issues/2328) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support filter for parquet data type [\#2126](https://github.com/apache/arrow-rs/issues/2126) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make skip value in ByteArrayDecoderDictionary avoid decoding [\#2088](https://github.com/apache/arrow-rs/issues/2088) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- fix: Fix skip error in calculate\_row\_count. [\#2329](https://github.com/apache/arrow-rs/pull/2329) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- temporal conversion functions should work on negative input properly [\#2326](https://github.com/apache/arrow-rs/pull/2326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Increase DeltaBitPackEncoder miniblock size to 64 for 64-bit integers  \(\#2282\) [\#2319](https://github.com/apache/arrow-rs/pull/2319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove JsonEqual [\#2317](https://github.com/apache/arrow-rs/pull/2317) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix: IPC writer should truncate string array with all empty string [\#2314](https://github.com/apache/arrow-rs/pull/2314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JasonLi-cn](https://github.com/JasonLi-cn))
- Pass pull `Request<FlightDescriptor>` to `FlightSqlService` `impl`s  [\#2309](https://github.com/apache/arrow-rs/pull/2309) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Speedup take\_boolean / take\_bits for non-null indices \(~4 - 5x speedup\) [\#2307](https://github.com/apache/arrow-rs/pull/2307) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Add typed dictionary \(\#2136\) [\#2297](https://github.com/apache/arrow-rs/pull/2297) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- \[Minor\] Improve types shown in cast error messages [\#2295](https://github.com/apache/arrow-rs/pull/2295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Move `with_precision_and_scale` to `BasicDecimalArray` trait [\#2292](https://github.com/apache/arrow-rs/pull/2292) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace the `fn get_data_type` by `const DATA_TYPE` in BinaryArray and StringArray [\#2289](https://github.com/apache/arrow-rs/pull/2289) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Clean up string casts and improve performance [\#2284](https://github.com/apache/arrow-rs/pull/2284) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Minor\] Add tests for temporal cast error paths [\#2283](https://github.com/apache/arrow-rs/pull/2283) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add unpack8, unpack16, unpack64 \(\#2276\) ~10-50% faster [\#2278](https://github.com/apache/arrow-rs/pull/2278) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix bugs in the `from_list` function. [\#2277](https://github.com/apache/arrow-rs/pull/2277) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- fix: use signed comparator to compare decimal128 and decimal256 [\#2275](https://github.com/apache/arrow-rs/pull/2275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Use initial capacity for interner hashmap [\#2272](https://github.com/apache/arrow-rs/pull/2272) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Remove fallibility from paruqet RleEncoder \(\#2226\) [\#2259](https://github.com/apache/arrow-rs/pull/2259) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix escaped like wildcards in `like_utf8` / `nlike_utf8` kernels [\#2258](https://github.com/apache/arrow-rs/pull/2258) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([daniel-martinez-maqueda-sap](https://github.com/daniel-martinez-maqueda-sap))
- Add tests for reading nested decimal arrays from parquet [\#2254](https://github.com/apache/arrow-rs/pull/2254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: Implement string cast operations for Time32 and Time64 [\#2251](https://github.com/apache/arrow-rs/pull/2251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([stuartcarnie](https://github.com/stuartcarnie))
- move `FixedSizeList` to `array_fixed_size_list.rs` [\#2250](https://github.com/apache/arrow-rs/pull/2250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Impl FromIterator for Decimal256Array [\#2247](https://github.com/apache/arrow-rs/pull/2247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix max and min value for decimal precision greater than 38 [\#2245](https://github.com/apache/arrow-rs/pull/2245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make `Schema::fields` and `Schema::metadata` `pub` \(public\) [\#2239](https://github.com/apache/arrow-rs/pull/2239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- \[Minor\] Improve Schema metadata mismatch error [\#2238](https://github.com/apache/arrow-rs/pull/2238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Separate ArrayReader::next\_batch with read\_records and consume\_batch [\#2237](https://github.com/apache/arrow-rs/pull/2237) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Update `IntervalMonthDayNanoType::make_value()` to conform to specifications [\#2235](https://github.com/apache/arrow-rs/pull/2235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([avantgardnerio](https://github.com/avantgardnerio))
- Disable value validation for Decimal256 case [\#2232](https://github.com/apache/arrow-rs/pull/2232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Automatically grow parquet BitWriter \(\#2226\) \(~10% faster\) [\#2231](https://github.com/apache/arrow-rs/pull/2231) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Only trigger `arrow` CI on changes to arrow [\#2227](https://github.com/apache/arrow-rs/pull/2227) ([alamb](https://github.com/alamb))
- Add append\_option support to decimal builders [\#2225](https://github.com/apache/arrow-rs/pull/2225) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bphillips-exos](https://github.com/bphillips-exos))
- Optimized writing of byte array to parquet \(\#1764\) \(2x faster\) [\#2221](https://github.com/apache/arrow-rs/pull/2221) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Increase test coverage of ArrowWriter [\#2220](https://github.com/apache/arrow-rs/pull/2220) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update instructions on how to join the Slack channel [\#2219](https://github.com/apache/arrow-rs/pull/2219) ([HaoYang670](https://github.com/HaoYang670))
- Move `FixedSizeBinaryArray` to `array_fixed_size_binary.rs` [\#2218](https://github.com/apache/arrow-rs/pull/2218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Avoid boxing in PrimitiveDictionaryBuilder [\#2216](https://github.com/apache/arrow-rs/pull/2216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- remove redundant CI benchmark check, cleanups [\#2212](https://github.com/apache/arrow-rs/pull/2212) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update `FlightSqlService` trait to proxy handshake [\#2211](https://github.com/apache/arrow-rs/pull/2211) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- parquet: export json api with `serde_json` feature name [\#2209](https://github.com/apache/arrow-rs/pull/2209) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([flisky](https://github.com/flisky))
- Cleanup record skipping logic and tests \(\#2158\) [\#2199](https://github.com/apache/arrow-rs/pull/2199) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use BitChunks in equal\_bits [\#2194](https://github.com/apache/arrow-rs/pull/2194) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix disabling parquet statistics \(\#2185\) [\#2191](https://github.com/apache/arrow-rs/pull/2191) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Change CI names to match crate names [\#2189](https://github.com/apache/arrow-rs/pull/2189) ([alamb](https://github.com/alamb))
- Fix offset handling in boolean\_equal \(\#2184\) [\#2187](https://github.com/apache/arrow-rs/pull/2187) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement `Hash` for `Schema` [\#2183](https://github.com/apache/arrow-rs/pull/2183) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Let the `StringBuilder` use `BinaryBuilder` [\#2181](https://github.com/apache/arrow-rs/pull/2181) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use ArrayAccessor and FromIterator in Cast Kernels [\#2169](https://github.com/apache/arrow-rs/pull/2169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split most arrow specific CI checks into their own workflows \(reduce common CI time to 21 minutes\) [\#2168](https://github.com/apache/arrow-rs/pull/2168) ([alamb](https://github.com/alamb))
- Remove another attempt to cache target directory in action.yaml [\#2167](https://github.com/apache/arrow-rs/pull/2167) ([alamb](https://github.com/alamb))
- Run actions on push to master, pull requests [\#2166](https://github.com/apache/arrow-rs/pull/2166) ([alamb](https://github.com/alamb))
- Break parquet\_derive and arrow\_flight tests into their own workflows [\#2165](https://github.com/apache/arrow-rs/pull/2165) ([alamb](https://github.com/alamb))
- \[minor\] use type aliases refine code. [\#2161](https://github.com/apache/arrow-rs/pull/2161) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- parquet reader: Support reading decimals from parquet `BYTE_ARRAY` type [\#2160](https://github.com/apache/arrow-rs/pull/2160) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Add integration test for scan rows with selection [\#2158](https://github.com/apache/arrow-rs/pull/2158) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Use ArrayAccessor in Comparison Kernels [\#2157](https://github.com/apache/arrow-rs/pull/2157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Implement `peek\_next\_page` and `skip\_next\_page` for `InMemoryColumnCh… [\#2155](https://github.com/apache/arrow-rs/pull/2155) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Avoid decoding unneeded values in ByteArrayDecoderDictionary [\#2154](https://github.com/apache/arrow-rs/pull/2154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Only run integration tests when `arrow` changes [\#2152](https://github.com/apache/arrow-rs/pull/2152) ([alamb](https://github.com/alamb))
- Break out docs CI job to its own github action [\#2151](https://github.com/apache/arrow-rs/pull/2151) ([alamb](https://github.com/alamb))
- Do not pretend to cache rust build artifacts, speed up CI by ~20% [\#2150](https://github.com/apache/arrow-rs/pull/2150) ([alamb](https://github.com/alamb))
- Update rust version to 1.62 [\#2144](https://github.com/apache/arrow-rs/pull/2144) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Make MapFieldNames public \(\#2118\) [\#2134](https://github.com/apache/arrow-rs/pull/2134) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ArrayAccessor trait, remove duplication in array iterators \(\#1948\) [\#2133](https://github.com/apache/arrow-rs/pull/2133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Lazily materialize the null buffer builder for all array builders. [\#2127](https://github.com/apache/arrow-rs/pull/2127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Faster parquet DictEncoder \(~20%\) [\#2123](https://github.com/apache/arrow-rs/pull/2123) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add validation for Decimal256 [\#2113](https://github.com/apache/arrow-rs/pull/2113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support skip\_def\_levels for ColumnLevelDecoder [\#2111](https://github.com/apache/arrow-rs/pull/2111) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Donate `object_store` code from object\_store\_rs to arrow-rs [\#2081](https://github.com/apache/arrow-rs/pull/2081) ([alamb](https://github.com/alamb))
- Improve `validate_utf8` performance [\#2048](https://github.com/apache/arrow-rs/pull/2048) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
