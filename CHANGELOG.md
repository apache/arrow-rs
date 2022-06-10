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

## [16.0.0](https://github.com/apache/arrow-rs/tree/16.0.0) (2022-06-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/15.0.0...16.0.0)

**Breaking changes:**

- Seal `ArrowNativeType` and `OffsetSizeTrait` for safety \(\#1028\) [\#1819](https://github.com/apache/arrow-rs/pull/1819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve API for `csv::infer_file_schema` by removing redundant ref  [\#1776](https://github.com/apache/arrow-rs/pull/1776) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- List equality method should work on empty offset `ListArray` [\#1817](https://github.com/apache/arrow-rs/issues/1817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Command line tool for convert CSV to Parquet [\#1797](https://github.com/apache/arrow-rs/issues/1797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC writer should write validity buffer for `UnionArray` in V4 IPC message [\#1793](https://github.com/apache/arrow-rs/issues/1793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add function for row alignment with page mask [\#1790](https://github.com/apache/arrow-rs/issues/1790) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rust IPC Read should be able to read V4 UnionType Array [\#1788](https://github.com/apache/arrow-rs/issues/1788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `combine_option_bitmap` should accept arbitrary number of input arrays. [\#1780](https://github.com/apache/arrow-rs/issues/1780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `substring_by_char` kernels for slicing on character boundaries [\#1768](https://github.com/apache/arrow-rs/issues/1768) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support reading `PageIndex` from column metadata [\#1761](https://github.com/apache/arrow-rs/issues/1761) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from `DataType::Utf8` to `DataType::Boolean` [\#1740](https://github.com/apache/arrow-rs/issues/1740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make current position available in `FileWriter`. [\#1691](https://github.com/apache/arrow-rs/issues/1691) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing parquet to `stdout` [\#1687](https://github.com/apache/arrow-rs/issues/1687) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Incorrect Offset Validation for Sliced List Array Children [\#1814](https://github.com/apache/arrow-rs/issues/1814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Snappy Codec overwrites Existing Data in Decompression Buffer [\#1806](https://github.com/apache/arrow-rs/issues/1806) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `flight_data_to_arrow_batch` does not support `RecordBatch`es with no columns [\#1783](https://github.com/apache/arrow-rs/issues/1783) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- parquet does not compile with `features=["zstd"]` [\#1630](https://github.com/apache/arrow-rs/issues/1630) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Update arrow module docs [\#1840](https://github.com/apache/arrow-rs/pull/1840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update safety disclaimer [\#1837](https://github.com/apache/arrow-rs/pull/1837) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update ballista readme link [\#1765](https://github.com/apache/arrow-rs/pull/1765) ([tustvold](https://github.com/tustvold))
- Move changelog archive to `CHANGELOG-old.md` [\#1759](https://github.com/apache/arrow-rs/pull/1759) ([alamb](https://github.com/alamb))

**Closed issues:**

- `DataType::Decimal` Non-Compliant? [\#1779](https://github.com/apache/arrow-rs/issues/1779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Further simplify the offset validation [\#1770](https://github.com/apache/arrow-rs/issues/1770) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Best way to convert arrow to Rust native type [\#1760](https://github.com/apache/arrow-rs/issues/1760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Why `Parquet` is a part of `Arrow`? [\#1715](https://github.com/apache/arrow-rs/issues/1715) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Make equals\_datatype method public, enabling other modules [\#1838](https://github.com/apache/arrow-rs/pull/1838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nl5887](https://github.com/nl5887))
- \[Minor\] Clarify `PageIterator` Documentation [\#1831](https://github.com/apache/arrow-rs/pull/1831) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Update MIRI pin [\#1828](https://github.com/apache/arrow-rs/pull/1828) ([tustvold](https://github.com/tustvold))
- Change to use `resolver v2`, test more feature flag combinations in CI, fix errors \(\#1630\) [\#1822](https://github.com/apache/arrow-rs/pull/1822) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ScalarBuffer abstraction \(\#1811\) [\#1820](https://github.com/apache/arrow-rs/pull/1820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix list equal for empty offset list array [\#1818](https://github.com/apache/arrow-rs/pull/1818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Decimal and List ArrayData Validation \(\#1813\) \(\#1814\) [\#1816](https://github.com/apache/arrow-rs/pull/1816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't overwrite existing data on snappy decompress \(\#1806\) [\#1807](https://github.com/apache/arrow-rs/pull/1807) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rename `arrow/benches/string_kernels.rs` to `arrow/benches/substring_kernels.rs` [\#1805](https://github.com/apache/arrow-rs/pull/1805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add public API for decoding parquet footer [\#1804](https://github.com/apache/arrow-rs/pull/1804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add AsyncFileReader trait [\#1803](https://github.com/apache/arrow-rs/pull/1803) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- add parquet-fromcsv \(\#1\) [\#1798](https://github.com/apache/arrow-rs/pull/1798) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kazuk](https://github.com/kazuk))
- Use IPC row count info in IPC reader [\#1796](https://github.com/apache/arrow-rs/pull/1796) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix typos in the Memory and Buffers section of the docs home [\#1795](https://github.com/apache/arrow-rs/pull/1795) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([datapythonista](https://github.com/datapythonista))
- Write validity buffer for UnionArray in V4 IPC message [\#1794](https://github.com/apache/arrow-rs/pull/1794) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat:Add function for row alignment with page mask [\#1791](https://github.com/apache/arrow-rs/pull/1791) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Read and skip validity buffer of UnionType Array for V4 ipc message [\#1789](https://github.com/apache/arrow-rs/pull/1789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Add `Substring_by_char` [\#1784](https://github.com/apache/arrow-rs/pull/1784) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add `ParquetFileArrowReader::try_new` [\#1782](https://github.com/apache/arrow-rs/pull/1782) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Arbitrary size combine option bitmap [\#1781](https://github.com/apache/arrow-rs/pull/1781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))
- Implement `ChunkReader` for `Bytes`, deprecate `SliceableCursor` [\#1775](https://github.com/apache/arrow-rs/pull/1775) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Access metadata of flushed row groups on write \(\#1691\) [\#1774](https://github.com/apache/arrow-rs/pull/1774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Simplify ParquetFileArrowReader Metadata API [\#1773](https://github.com/apache/arrow-rs/pull/1773) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- MINOR: Unpin nightly version as packed\_simd releases new version [\#1771](https://github.com/apache/arrow-rs/pull/1771) ([viirya](https://github.com/viirya))
- Update comfy-table requirement from 5.0 to 6.0 [\#1769](https://github.com/apache/arrow-rs/pull/1769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Optionally disable `validate_decimal_precision` check in `DecimalBuilder.append_value` for interop test [\#1767](https://github.com/apache/arrow-rs/pull/1767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Minor: Clean up the code of MutableArrayData [\#1763](https://github.com/apache/arrow-rs/pull/1763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support reading PageIndex from parquet metadata, prepare for skipping pages at reading [\#1762](https://github.com/apache/arrow-rs/pull/1762) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Support casting `Utf8` to `Boolean` [\#1738](https://github.com/apache/arrow-rs/pull/1738) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([MazterQyou](https://github.com/MazterQyou))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
