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

## [54.0.0](https://github.com/apache/arrow-rs/tree/54.0.0) (2024-12-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.3.0...54.0.0)

**Breaking changes:**

- avoid redundant parsing of repeated value in RleDecoder [\#6834](https://github.com/apache/arrow-rs/pull/6834) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Handling nullable DictionaryArray in CSV parser [\#6830](https://github.com/apache/arrow-rs/pull/6830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([edmondop](https://github.com/edmondop))
- fix\(flightsql\): remove Any encoding of DoPutUpdateResult [\#6825](https://github.com/apache/arrow-rs/pull/6825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([davisp](https://github.com/davisp))
- arrow-ipc: Default to not preserving dict IDs [\#6788](https://github.com/apache/arrow-rs/pull/6788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Remove some very old deprecated functions [\#6774](https://github.com/apache/arrow-rs/pull/6774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- update to pyo3 0.23.0 [\#6745](https://github.com/apache/arrow-rs/pull/6745) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Remove APIs deprecated since v 4.4.0 [\#6722](https://github.com/apache/arrow-rs/pull/6722) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- Return `None` when Parquet page indexes are not present in file [\#6639](https://github.com/apache/arrow-rs/pull/6639) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add `ParquetError::NeedMoreData` mark `ParquetError` as `non_exhaustive` [\#6630](https://github.com/apache/arrow-rs/pull/6630) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Remove APIs deprecated since v 2.0.0 [\#6609](https://github.com/apache/arrow-rs/pull/6609) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))

**Implemented enhancements:**

- Parquet schema hint doesn't support integer types upcasting [\#6891](https://github.com/apache/arrow-rs/issues/6891)
- Parquet UTF-8 max statistics are overly pessimistic [\#6867](https://github.com/apache/arrow-rs/issues/6867) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add builder support for Int8 keys [\#6844](https://github.com/apache/arrow-rs/issues/6844) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Formalize the name of the nested `Field` in a list [\#6784](https://github.com/apache/arrow-rs/issues/6784) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Allow disabling the writing of Parquet Offset Index [\#6778](https://github.com/apache/arrow-rs/issues/6778) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `parquet::record::make_row` is not exposed to users, leaving no option to users to manually create `Row` objects [\#6761](https://github.com/apache/arrow-rs/issues/6761) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Avoid `from_num_days_from_ce_opt` calls in `timestamp_s_to_datetime` if we don't need [\#6746](https://github.com/apache/arrow-rs/issues/6746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Temporal -\> Utf8View casting [\#6734](https://github.com/apache/arrow-rs/issues/6734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Option To Coerce List Type on Parquet Write [\#6733](https://github.com/apache/arrow-rs/issues/6733) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Numeric -\> Utf8View casting [\#6714](https://github.com/apache/arrow-rs/issues/6714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Utf8View \<=\> boolean casting [\#6713](https://github.com/apache/arrow-rs/issues/6713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- `Buffer::bit_slice` loses length with byte-aligned offsets [\#6895](https://github.com/apache/arrow-rs/issues/6895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet arrow writer doesn't track memory size correctly for fixed sized lists [\#6839](https://github.com/apache/arrow-rs/issues/6839) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Casting Decimal128 to Decimal128 with smaller precision produces incorrect results in some cases [\#6833](https://github.com/apache/arrow-rs/issues/6833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Should empty nullable dictionary be parsed as null from arrow-csv? [\#6821](https://github.com/apache/arrow-rs/issues/6821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Array take doesn't make fields nullable [\#6809](https://github.com/apache/arrow-rs/issues/6809)
- Arrow Flight Encodes a Slice's List Offsets If the slice offset is starts with zero [\#6803](https://github.com/apache/arrow-rs/issues/6803) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet readers incorrectly interpret legacy nested lists [\#6756](https://github.com/apache/arrow-rs/issues/6756) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- filter\_bits under-allocates resulting boolean buffer [\#6750](https://github.com/apache/arrow-rs/issues/6750) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Multi-language support issues with Arrow FlightSQL client's execute\_update and execute\_ingest methods [\#6545](https://github.com/apache/arrow-rs/issues/6545) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Documentation updates:**

- Should we document at what rate deprecated APIs are removed? [\#6851](https://github.com/apache/arrow-rs/issues/6851) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix docstring for `Format::with_header` in `arrow-csv` [\#6856](https://github.com/apache/arrow-rs/pull/6856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Add deprecation / API removal policy [\#6852](https://github.com/apache/arrow-rs/pull/6852) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: add example for creating `SchemaDescriptor` [\#6841](https://github.com/apache/arrow-rs/pull/6841) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- chore: enrich panic context when BooleanBuffer fails to create [\#6810](https://github.com/apache/arrow-rs/pull/6810) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tisonkun](https://github.com/tisonkun))

**Closed issues:**

- \[FlightSQL\] GetCatalogsBuilder does not sort the catalog names [\#6807](https://github.com/apache/arrow-rs/issues/6807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add a lint to automatically check for unused dependencies [\#6796](https://github.com/apache/arrow-rs/issues/6796) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Merged pull requests:**

- doc: add comment for timezone string [\#6899](https://github.com/apache/arrow-rs/pull/6899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xxchan](https://github.com/xxchan))
- docs: fix typo [\#6890](https://github.com/apache/arrow-rs/pull/6890) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rluvaton](https://github.com/rluvaton))
- Minor: Fix deprecation notice for `arrow_to_parquet_schema` [\#6889](https://github.com/apache/arrow-rs/pull/6889) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add Field::with\_dict\_is\_ordered [\#6885](https://github.com/apache/arrow-rs/pull/6885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Deprecate "max statistics size" property in `WriterProperties` [\#6884](https://github.com/apache/arrow-rs/pull/6884) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add deprecation warnings for everything related to `dict_id` [\#6873](https://github.com/apache/arrow-rs/pull/6873) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([brancz](https://github.com/brancz))
- Enable matching temporal as from\_type to Utf8View [\#6872](https://github.com/apache/arrow-rs/pull/6872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kev1n8](https://github.com/Kev1n8))
- Enable string-based column projections from Parquet files [\#6871](https://github.com/apache/arrow-rs/pull/6871) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improvements to UTF-8 statistics truncation [\#6870](https://github.com/apache/arrow-rs/pull/6870) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- fix: make GetCatalogsBuilder sort catalog names  [\#6864](https://github.com/apache/arrow-rs/pull/6864) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([niebayes](https://github.com/niebayes))
- add buffered data\_pages to parquet column writer total bytes estimation [\#6862](https://github.com/apache/arrow-rs/pull/6862) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([onursatici](https://github.com/onursatici))
- Update prost-build requirement from =0.13.3 to =0.13.4 [\#6860](https://github.com/apache/arrow-rs/pull/6860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Minor: add comments explaining bad MSRV, output in json [\#6857](https://github.com/apache/arrow-rs/pull/6857) ([alamb](https://github.com/alamb))
- perf: Use Cow in get\_format\_string in FFI\_ArrowSchema [\#6853](https://github.com/apache/arrow-rs/pull/6853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- chore: add cast\_decimal benchmark [\#6850](https://github.com/apache/arrow-rs/pull/6850) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- arrow-array::builder: support Int8, Int16 and Int64 keys [\#6845](https://github.com/apache/arrow-rs/pull/6845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ajwerner](https://github.com/ajwerner))
- Add `ArrowToParquetSchemaConverter`, deprecate `arrow_to_parquet_schema` [\#6840](https://github.com/apache/arrow-rs/pull/6840) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Remove APIs deprecated in 50.0.0 [\#6838](https://github.com/apache/arrow-rs/pull/6838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- fix: decimal conversion looses value on lower precision [\#6836](https://github.com/apache/arrow-rs/pull/6836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([himadripal](https://github.com/himadripal))
- Update sysinfo requirement from 0.32.0 to 0.33.0 [\#6835](https://github.com/apache/arrow-rs/pull/6835) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Optionally coerce names of maps and lists to match Parquet specification [\#6828](https://github.com/apache/arrow-rs/pull/6828) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Remove deprecated unary\_dyn and try\_unary\_dyn [\#6824](https://github.com/apache/arrow-rs/pull/6824) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Remove deprecated flight\_data\_from\_arrow\_batch [\#6823](https://github.com/apache/arrow-rs/pull/6823) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- \[arrow-cast\] Support cast boolean from/to string view [\#6822](https://github.com/apache/arrow-rs/pull/6822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- Hook up Avro Decoder [\#6820](https://github.com/apache/arrow-rs/pull/6820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix arrow-avro compilation without default features [\#6819](https://github.com/apache/arrow-rs/pull/6819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Support shrink to empty [\#6817](https://github.com/apache/arrow-rs/pull/6817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- \[arrow-cast\] Support cast numeric to string view \(alternate\) [\#6816](https://github.com/apache/arrow-rs/pull/6816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Hide implicit optional dependency features in arrow-flight [\#6806](https://github.com/apache/arrow-rs/pull/6806) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- fix: Encoding of List offsets was incorrect when slice offsets begin with zero [\#6805](https://github.com/apache/arrow-rs/pull/6805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HawaiianSpork](https://github.com/HawaiianSpork))
- Enable unused\_crate\_dependencies Rust lint, remove unused dependencies [\#6804](https://github.com/apache/arrow-rs/pull/6804) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- Minor: Fix docstrings for `ColumnProperties::statistics_enabled` property [\#6798](https://github.com/apache/arrow-rs/pull/6798) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add option to disable writing of Parquet offset index [\#6797](https://github.com/apache/arrow-rs/pull/6797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Remove unused dependencies [\#6792](https://github.com/apache/arrow-rs/pull/6792) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- Add `Array::shrink_to_fit(&mut self)` [\#6790](https://github.com/apache/arrow-rs/pull/6790) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([emilk](https://github.com/emilk))
- Formalize the default nested list field name to `item` [\#6785](https://github.com/apache/arrow-rs/pull/6785) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([gruuya](https://github.com/gruuya))
- Improve UnionArray logical\_nulls tests [\#6781](https://github.com/apache/arrow-rs/pull/6781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Improve list builder usage example in docs [\#6775](https://github.com/apache/arrow-rs/pull/6775) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Update proc-macro2 requirement from =1.0.89 to =1.0.92 [\#6772](https://github.com/apache/arrow-rs/pull/6772) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Allow NullBuffer construction directly from array  [\#6769](https://github.com/apache/arrow-rs/pull/6769) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Include license and notice files in published crates [\#6767](https://github.com/apache/arrow-rs/pull/6767) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([ankane](https://github.com/ankane))
- fix: remove redundant `bit_util::ceil` [\#6766](https://github.com/apache/arrow-rs/pull/6766) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([miroim](https://github.com/miroim))
- Remove 'make\_row', expose a 'Row::new' method instead. [\#6763](https://github.com/apache/arrow-rs/pull/6763) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jonded94](https://github.com/jonded94))
- Read nested Parquet 2-level lists correctly [\#6757](https://github.com/apache/arrow-rs/pull/6757) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Split `timestamp_s_to_datetime` to `date` and `time` to avoid unnecessary computation [\#6755](https://github.com/apache/arrow-rs/pull/6755) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jayzhan211](https://github.com/jayzhan211))
- More trivial implementation of `Box<dyn AsyncArrowWriter>` and `Box<dyn AsyncArrowReader>` [\#6748](https://github.com/apache/arrow-rs/pull/6748) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ethe](https://github.com/ethe))
- Update cache action to v4 [\#6744](https://github.com/apache/arrow-rs/pull/6744) ([findepi](https://github.com/findepi))
- Remove redundant implementation of `StringArrayType` [\#6743](https://github.com/apache/arrow-rs/pull/6743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- Fix Dictionary logical nulls for RunArray/UnionArray Values [\#6740](https://github.com/apache/arrow-rs/pull/6740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Allow reading Parquet maps that lack a `values` field [\#6730](https://github.com/apache/arrow-rs/pull/6730) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve default implementation of Array::is\_nullable [\#6721](https://github.com/apache/arrow-rs/pull/6721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Fix Buffer::bit\_slice losing length with byte-aligned offsets [\#6707](https://github.com/apache/arrow-rs/pull/6707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
