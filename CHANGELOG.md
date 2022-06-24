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

## [17.0.0](https://github.com/apache/arrow-rs/tree/17.0.0) (2022-06-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/16.0.0...17.0.0)

**Breaking changes:**

- Add validation to `RecordBatch` for non-nullable fields containing null values [\#1890](https://github.com/apache/arrow-rs/pull/1890) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Rename `ArrayData::validate_dict_offsets` to `ArrayData::validate_values` [\#1889](https://github.com/apache/arrow-rs/pull/1889) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([frolovdev](https://github.com/frolovdev))
-  Add `Decimal128` API and use it in DecimalArray and DecimalBuilder [\#1871](https://github.com/apache/arrow-rs/pull/1871) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Mark typed buffer APIs `safe` \(\#996\) \(\#1027\) [\#1866](https://github.com/apache/arrow-rs/pull/1866) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- add a small doc example showing `ArrowWriter` being used with a cursor [\#1927](https://github.com/apache/arrow-rs/issues/1927) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `cast` to/from `NULL` and `DataType::Decimal` [\#1921](https://github.com/apache/arrow-rs/issues/1921) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `Decimal256` API [\#1913](https://github.com/apache/arrow-rs/issues/1913) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `DictionaryArray::key` function [\#1911](https://github.com/apache/arrow-rs/issues/1911) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support specifying capacities for `ListArrays` in `MutableArrayData` [\#1884](https://github.com/apache/arrow-rs/issues/1884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Explicitly declare the features used for each dependency [\#1876](https://github.com/apache/arrow-rs/issues/1876) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add Decimal128 API and use it in DecimalArray and DecimalBuilder [\#1870](https://github.com/apache/arrow-rs/issues/1870) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `PrimitiveArray::from_iter` should omit validity buffer if all values are valid [\#1856](https://github.com/apache/arrow-rs/issues/1856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `from(v: Vec<Option<&[u8]>>)` and `from(v: Vec<&[u8]>)` for `FixedSizedBInaryArray` [\#1852](https://github.com/apache/arrow-rs/issues/1852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `Vec`-inspired APIs to `BufferBuilder` [\#1850](https://github.com/apache/arrow-rs/issues/1850) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- PyArrow intergation test for C Stream Interface [\#1847](https://github.com/apache/arrow-rs/issues/1847) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `nilike` support in `comparison` [\#1845](https://github.com/apache/arrow-rs/issues/1845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Split up `arrow::array::builder` module [\#1843](https://github.com/apache/arrow-rs/issues/1843) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `quarter` support in `temporal` kernels [\#1835](https://github.com/apache/arrow-rs/issues/1835) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rename `ArrayData::validate_dictionary_offset` to `ArrayData::validate_values` [\#1812](https://github.com/apache/arrow-rs/issues/1812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clean up the testing code for `substring` kernel [\#1801](https://github.com/apache/arrow-rs/issues/1801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up `substring_by_char` kernel [\#1800](https://github.com/apache/arrow-rs/issues/1800) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- unable to write parquet file with UTC timestamp [\#1932](https://github.com/apache/arrow-rs/issues/1932) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Incorrect max and min decimals [\#1916](https://github.com/apache/arrow-rs/issues/1916) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `dynamic_types` example does not print the projection [\#1902](https://github.com/apache/arrow-rs/issues/1902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `log2(0)` panicked at `'attempt to subtract with overflow', parquet/src/util/bit_util.rs:148:5` [\#1901](https://github.com/apache/arrow-rs/issues/1901) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Final slicing in `combine_option_bitmap` needs to use bit slices [\#1899](https://github.com/apache/arrow-rs/issues/1899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Dictionary IPC writer  writes incorrect schema [\#1892](https://github.com/apache/arrow-rs/issues/1892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Creating a `RecordBatch` with null values in non-nullable fields does not cause an error [\#1888](https://github.com/apache/arrow-rs/issues/1888) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Upgrade `regex` dependency [\#1874](https://github.com/apache/arrow-rs/issues/1874) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Miri reports leaks in ffi tests [\#1872](https://github.com/apache/arrow-rs/issues/1872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- AVX512 + simd binary and/or kernels slower than autovectorized version [\#1829](https://github.com/apache/arrow-rs/issues/1829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Blog post about arrow 10.0.0 - 16.0.0 [\#1808](https://github.com/apache/arrow-rs/issues/1808)
- Add README for the compute module. [\#1940](https://github.com/apache/arrow-rs/pull/1940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- minor: clarify docstring on `DictionaryArray::lookup_key` [\#1910](https://github.com/apache/arrow-rs/pull/1910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- minor: add a diagram to docstring for DictionaryArray [\#1909](https://github.com/apache/arrow-rs/pull/1909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Closes \#1902: Print the original and projected RecordBatch in dynamic\_types example [\#1903](https://github.com/apache/arrow-rs/pull/1903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([martin-g](https://github.com/martin-g))

**Closed issues:**

- how read/write REPEATED [\#1886](https://github.com/apache/arrow-rs/issues/1886) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Handling Unsupported Arrow Types in Parquet [\#1666](https://github.com/apache/arrow-rs/issues/1666) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Set adjusted to UTC if UTC timezone \(\#1932\) [\#1937](https://github.com/apache/arrow-rs/pull/1937) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split up parquet::arrow::array\_reader \(\#1483\) [\#1933](https://github.com/apache/arrow-rs/pull/1933) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add ArrowWriter doctest \(\#1927\) [\#1930](https://github.com/apache/arrow-rs/pull/1930) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update indexmap dependency [\#1929](https://github.com/apache/arrow-rs/pull/1929) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Complete and fixup  split of `arrow::array::builder` module \(\#1843\) [\#1928](https://github.com/apache/arrow-rs/pull/1928) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- MINOR: Replace `checked_add/sub().unwrap()` with `+/-` [\#1924](https://github.com/apache/arrow-rs/pull/1924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support casting `NULL` to/from `Decimal` [\#1922](https://github.com/apache/arrow-rs/pull/1922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Update half requirement from 1.8 to 2.0 [\#1919](https://github.com/apache/arrow-rs/pull/1919) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix max and min decimal for max precision [\#1917](https://github.com/apache/arrow-rs/pull/1917) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `Decimal256` API [\#1914](https://github.com/apache/arrow-rs/pull/1914) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `DictionaryArray::key` function [\#1912](https://github.com/apache/arrow-rs/pull/1912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix misaligned reference and logic error in crc32 [\#1906](https://github.com/apache/arrow-rs/pull/1906) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([saethlin](https://github.com/saethlin))
- Refine the `bit_util` of Parquet. [\#1905](https://github.com/apache/arrow-rs/pull/1905) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HaoYang670](https://github.com/HaoYang670))
- Use bit\_slice in combine\_option\_bitmap [\#1900](https://github.com/apache/arrow-rs/pull/1900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Issue \#1876: Explicitly declare the used features for each dependency in integration\_testing [\#1898](https://github.com/apache/arrow-rs/pull/1898) ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet\_derive\_test [\#1897](https://github.com/apache/arrow-rs/pull/1897) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet\_derive [\#1896](https://github.com/apache/arrow-rs/pull/1896) ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet [\#1895](https://github.com/apache/arrow-rs/pull/1895) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([martin-g](https://github.com/martin-g))
- Minor: Add examples to docstring for `weekday` [\#1894](https://github.com/apache/arrow-rs/pull/1894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Correct nullable in read\_dictionary [\#1893](https://github.com/apache/arrow-rs/pull/1893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Feature add weekday temporal kernel [\#1891](https://github.com/apache/arrow-rs/pull/1891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nl5887](https://github.com/nl5887))
- Support specifying list capacities for `MutableArrayData` [\#1885](https://github.com/apache/arrow-rs/pull/1885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Issue \#1876: Explicitly declare the used features for each dependency in parquet [\#1881](https://github.com/apache/arrow-rs/pull/1881) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([martin-g](https://github.com/martin-g))
- Issue \#1876: Explicitly declare the used features for each dependency in arrow-flight [\#1880](https://github.com/apache/arrow-rs/pull/1880) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([martin-g](https://github.com/martin-g))
- Split up arrow::array::builder module \(\#1843\) [\#1879](https://github.com/apache/arrow-rs/pull/1879) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([DaltonModlin](https://github.com/DaltonModlin))
- Fix memory leak in ffi test [\#1878](https://github.com/apache/arrow-rs/pull/1878) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Issue \#1876 - Explicitly declare the used features for each dependency [\#1877](https://github.com/apache/arrow-rs/pull/1877) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([martin-g](https://github.com/martin-g))
- Fixes \#1874 - Upgrade `regex` dependency to 1.5.6 [\#1875](https://github.com/apache/arrow-rs/pull/1875) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([martin-g](https://github.com/martin-g))
- Do not print exit code from miri, instead it should be the return value of the script [\#1873](https://github.com/apache/arrow-rs/pull/1873) ([jhorstmann](https://github.com/jhorstmann))
- Update vendored gRPC [\#1869](https://github.com/apache/arrow-rs/pull/1869) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Expose `BitSliceIterator` and `BitIndexIterator` \(\#1864\) [\#1865](https://github.com/apache/arrow-rs/pull/1865) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Exclude some long-running tests when running under miri [\#1863](https://github.com/apache/arrow-rs/pull/1863) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add vec-inspired APIs to BufferBuilder \(\#1850\) [\#1860](https://github.com/apache/arrow-rs/pull/1860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Omit validity buffer in PrimitiveArray::from\_iter when all values are valid [\#1859](https://github.com/apache/arrow-rs/pull/1859) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add two `from` methods for `FixedSizeBinaryArray` [\#1854](https://github.com/apache/arrow-rs/pull/1854) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Clean up the test code of `substring` kernel. [\#1853](https://github.com/apache/arrow-rs/pull/1853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add PyArrow integration test for C Stream Interface [\#1848](https://github.com/apache/arrow-rs/pull/1848) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `nilike` support in `comparison` [\#1846](https://github.com/apache/arrow-rs/pull/1846) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([MazterQyou](https://github.com/MazterQyou))
- MINOR: Remove version check from `test_command_help` [\#1844](https://github.com/apache/arrow-rs/pull/1844) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Implement UnionArray FieldData using Type Erasure [\#1842](https://github.com/apache/arrow-rs/pull/1842) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `quarter` support in `temporal` [\#1836](https://github.com/apache/arrow-rs/pull/1836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([MazterQyou](https://github.com/MazterQyou))
- speed up `substring_by_char` by about 2.5x [\#1832](https://github.com/apache/arrow-rs/pull/1832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove simd and avx512 bitwise kernels in favor of autovectorization [\#1830](https://github.com/apache/arrow-rs/pull/1830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Refactor parquet::arrow module [\#1827](https://github.com/apache/arrow-rs/pull/1827) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- docs: remove experimental marker on C Stream Interface [\#1821](https://github.com/apache/arrow-rs/pull/1821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Separate Page IO from Page Decode [\#1810](https://github.com/apache/arrow-rs/pull/1810) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
