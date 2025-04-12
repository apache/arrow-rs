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

## [55.0.0](https://github.com/apache/arrow-rs/tree/55.0.0) (2025-04-08)

[Full Changelog](https://github.com/apache/arrow-rs/compare/54.3.1...55.0.0)

**Breaking changes:**

- Change Parquet API interaction to use `u64` \(support files larger than 4GB in WASM\) [\#7371](https://github.com/apache/arrow-rs/pull/7371) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Remove  `AsyncFileReader::get_metadata_with_options`, add `options` to `AsyncFileReader::get_metadata` [\#7342](https://github.com/apache/arrow-rs/pull/7342) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([corwinjoy](https://github.com/corwinjoy))
- Parquet: Support reading Parquet metadata via suffix range requests [\#7334](https://github.com/apache/arrow-rs/pull/7334) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Upgrade to `object_store` to `0.12.0` [\#7328](https://github.com/apache/arrow-rs/pull/7328) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- Upgrade `pyo3` to `0.24` [\#7324](https://github.com/apache/arrow-rs/pull/7324) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Reapply Box `FlightErrror::tonic` to reduce size \(fixes nightly clippy\) [\#7277](https://github.com/apache/arrow-rs/pull/7277) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Improve parquet gzip compression performance using zlib-rs [\#7200](https://github.com/apache/arrow-rs/pull/7200) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Fix: `date_part` to extract only the requested part \(not the overall interval\) [\#7189](https://github.com/apache/arrow-rs/pull/7189) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- chore: upgrade flatbuffer version to `25.2.10` [\#7134](https://github.com/apache/arrow-rs/pull/7134) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tisonkun](https://github.com/tisonkun))
- Add hooks to json encoder to override default encoding or add support for unsupported types [\#7015](https://github.com/apache/arrow-rs/pull/7015) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))

**Implemented enhancements:**

- Improve the performance of `concat` [\#7357](https://github.com/apache/arrow-rs/issues/7357) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pushdown predictions to Parquet in-memory row group fetches [\#7348](https://github.com/apache/arrow-rs/issues/7348) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Improve CSV parsing errors: Print the row that makes csv parsing fails [\#7344](https://github.com/apache/arrow-rs/issues/7344) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support ColumnMetaData `encoding_stats` in Parquet Writing [\#7341](https://github.com/apache/arrow-rs/issues/7341) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing Parquet with modular encryption [\#7327](https://github.com/apache/arrow-rs/issues/7327) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Use U64 Instead of Usize \(wasm support for files greater than 4GB\) [\#7238](https://github.com/apache/arrow-rs/issues/7238) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support different TimeUnits and timezones when reading Timestamps from INT96 [\#7220](https://github.com/apache/arrow-rs/issues/7220) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- New clippy failures in code base with release of rustc 1.86 [\#7381](https://github.com/apache/arrow-rs/issues/7381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix bug in `ParquetMetaDataReader` and add test of suffix metadata reads with encryption [\#7372](https://github.com/apache/arrow-rs/pull/7372) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))

**Documentation updates:**

- Improve documentation on `ArrayData::offset` [\#7385](https://github.com/apache/arrow-rs/pull/7385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve documentation for `AsyncFileReader::get_metadata` [\#7380](https://github.com/apache/arrow-rs/pull/7380) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve documentation on implementing Parquet predicate pushdown [\#7370](https://github.com/apache/arrow-rs/pull/7370) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add documentation and examples for pretty printing, make `pretty_format_columns_with_options` pub [\#7346](https://github.com/apache/arrow-rs/pull/7346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve documentation on writing parquet, including multiple threads [\#7321](https://github.com/apache/arrow-rs/pull/7321) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- chore: apply clippy suggestions newly introduced in rust 1.86 [\#7382](https://github.com/apache/arrow-rs/pull/7382) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([westonpace](https://github.com/westonpace))
- bench: add more {boolean, string, int} benchmarks for concat kernel [\#7376](https://github.com/apache/arrow-rs/pull/7376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Add more examples of using Parquet encryption [\#7374](https://github.com/apache/arrow-rs/pull/7374) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Clean up `ArrowReaderMetadata::load_async` [\#7369](https://github.com/apache/arrow-rs/pull/7369) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- bump pyo3 for RUSTSEC-2025-0020 [\#7368](https://github.com/apache/arrow-rs/pull/7368) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([onursatici](https://github.com/onursatici))
- Test int96 Parquet file from Spark [\#7367](https://github.com/apache/arrow-rs/pull/7367) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbutrovich](https://github.com/mbutrovich))
- fix: respect offset/length when converting ArrayData to StructArray [\#7366](https://github.com/apache/arrow-rs/pull/7366) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([westonpace](https://github.com/westonpace))
- Print row, data present, expected type, and row number in error messages for arrow-csv [\#7361](https://github.com/apache/arrow-rs/pull/7361) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psiayn](https://github.com/psiayn))
- Use rust builtins for round\_upto\_multiple\_of\_64 and ceil [\#7358](https://github.com/apache/arrow-rs/pull/7358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Write parquet PageEncodingStats [\#7354](https://github.com/apache/arrow-rs/pull/7354) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- Move `sysinfo` to `dev-dependencies` [\#7353](https://github.com/apache/arrow-rs/pull/7353) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- chore\(deps\): update sysinfo requirement from 0.33.0 to 0.34.0 [\#7352](https://github.com/apache/arrow-rs/pull/7352) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add additional benchmarks for utf8view comparison kernels [\#7351](https://github.com/apache/arrow-rs/pull/7351) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhuqi-lucas](https://github.com/zhuqi-lucas))
- Upgrade to twox-hash 2.0 [\#7347](https://github.com/apache/arrow-rs/pull/7347) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- refactor: apply borrowed chunk reader to Sbbf::read\_from\_column\_chunk [\#7345](https://github.com/apache/arrow-rs/pull/7345) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ethe](https://github.com/ethe))
- Merge changelog and version from 54.3.1 into main [\#7340](https://github.com/apache/arrow-rs/pull/7340) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([timsaucer](https://github.com/timsaucer))
- Remove `object-store` label from `.asf.yaml` [\#7339](https://github.com/apache/arrow-rs/pull/7339) ([mbrobbel](https://github.com/mbrobbel))
- Encapsulate encryption code more in readers [\#7337](https://github.com/apache/arrow-rs/pull/7337) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Bump MSRV to 1.81 [\#7336](https://github.com/apache/arrow-rs/pull/7336) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Add an option to show column type [\#7335](https://github.com/apache/arrow-rs/pull/7335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([blaginin](https://github.com/blaginin))
- Add missing type annotation [\#7326](https://github.com/apache/arrow-rs/pull/7326) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- Minor: Improve parallel parquet encoding example [\#7323](https://github.com/apache/arrow-rs/pull/7323) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- feat: allow if expressions for fallbacks in downcast macro [\#7322](https://github.com/apache/arrow-rs/pull/7322) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Minor: rename `ParquetRecordBatchStream::reader` to `ParquetRecordBatchStream::reader_factory` [\#7319](https://github.com/apache/arrow-rs/pull/7319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- bugfix: correct offsets when serializing a list of fixed sized list and non-zero start offset [\#7318](https://github.com/apache/arrow-rs/pull/7318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([timsaucer](https://github.com/timsaucer))
- Remove object\_store references in Readme.md [\#7317](https://github.com/apache/arrow-rs/pull/7317) ([alamb](https://github.com/alamb))
- Adopt MSRV policy [\#7314](https://github.com/apache/arrow-rs/pull/7314) ([psvri](https://github.com/psvri))
- fix: correct array length validation error message [\#7313](https://github.com/apache/arrow-rs/pull/7313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wkalt](https://github.com/wkalt))
- chore: remove trailing space in debug print [\#7311](https://github.com/apache/arrow-rs/pull/7311) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xxchan](https://github.com/xxchan))
- Improve `concat` performance, and add `append_array` for some array builder implementations [\#7309](https://github.com/apache/arrow-rs/pull/7309) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- feat: add `append_buffer` for `NullBufferBuilder` [\#7308](https://github.com/apache/arrow-rs/pull/7308) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- MINOR: fix incorrect method name in deprecate node [\#7306](https://github.com/apache/arrow-rs/pull/7306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Allow retrieving Parquet decryption keys using the key metadata [\#7286](https://github.com/apache/arrow-rs/pull/7286) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adamreeve](https://github.com/adamreeve))
- Support different TimeUnits and timezones when reading Timestamps from INT96 [\#7285](https://github.com/apache/arrow-rs/pull/7285) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbutrovich](https://github.com/mbutrovich))
- Add Parquet Modular encryption support \(write\) [\#7111](https://github.com/apache/arrow-rs/pull/7111) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rok](https://github.com/rok))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
