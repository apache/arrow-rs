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

# Historical Changelog

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

- Parquet schema hint doesn't support integer types upcasting [\#6891](https://github.com/apache/arrow-rs/issues/6891) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
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

## [53.3.0](https://github.com/apache/arrow-rs/tree/53.3.0) (2024-11-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.2.0...53.3.0)

- Signed decimal e-notation parsing bug [\#6728](https://github.com/apache/arrow-rs/issues/6728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for Utf8View -\> numeric in can\_cast\_types [\#6715](https://github.com/apache/arrow-rs/issues/6715)
- IPC file writer produces incorrect footer when not preserving dict ID [\#6710](https://github.com/apache/arrow-rs/issues/6710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet from\_thrift\_helper incorrectly checks index [\#6693](https://github.com/apache/arrow-rs/issues/6693) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Primitive REPEATED fields not contained in LIST annotated groups aren't read as lists by record reader [\#6648](https://github.com/apache/arrow-rs/issues/6648) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- DictionaryHandling does not recurse into Map fields [\#6644](https://github.com/apache/arrow-rs/issues/6644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Array writer output empty when no record is written [\#6613](https://github.com/apache/arrow-rs/issues/6613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Archery Integration Test with c\# failing on main [\#6577](https://github.com/apache/arrow-rs/issues/6577) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Potential unsoundness in `filter_run_end_array` [\#6569](https://github.com/apache/arrow-rs/issues/6569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet reader can generate incorrect validity buffer information for nested structures [\#6510](https://github.com/apache/arrow-rs/issues/6510) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- arrow-array ffi: FFI\_ArrowArray.null\_count is always interpreted as unsigned and initialized during conversion from C to Rust. [\#6497](https://github.com/apache/arrow-rs/issues/6497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Minor: Document pattern for accessing views in StringView [\#6673](https://github.com/apache/arrow-rs/pull/6673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve Array::is\_nullable documentation [\#6615](https://github.com/apache/arrow-rs/pull/6615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Minor: improve docs for ByteViewArray-\>ByteArray From impl [\#6610](https://github.com/apache/arrow-rs/pull/6610) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Speed up `filter_run_end_array` [\#6712](https://github.com/apache/arrow-rs/pull/6712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))

**Closed issues:**

- Incorrect like results for pattern starting/ending with `%` percent and containing escape characters [\#6702](https://github.com/apache/arrow-rs/issues/6702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix signed decimal e-notation parsing [\#6729](https://github.com/apache/arrow-rs/pull/6729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Clean up some arrow-flight tests and duplicated code [\#6725](https://github.com/apache/arrow-rs/pull/6725) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))
- Update PR template section about API breaking changes [\#6723](https://github.com/apache/arrow-rs/pull/6723) ([findepi](https://github.com/findepi))
- Support for casting `StringViewArray` to `DecimalArray` [\#6720](https://github.com/apache/arrow-rs/pull/6720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- File writer preserve dict bug [\#6711](https://github.com/apache/arrow-rs/pull/6711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Add filter\_kernel benchmark for run array [\#6706](https://github.com/apache/arrow-rs/pull/6706) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- Fix string view ILIKE checks with NULL values [\#6705](https://github.com/apache/arrow-rs/pull/6705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Implement logical\_null\_count for more array types [\#6704](https://github.com/apache/arrow-rs/pull/6704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Fix LIKE with escapes [\#6703](https://github.com/apache/arrow-rs/pull/6703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Speed up `filter_bytes` [\#6699](https://github.com/apache/arrow-rs/pull/6699) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Minor: fix misleading comment in byte view [\#6695](https://github.com/apache/arrow-rs/pull/6695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jayzhan211](https://github.com/jayzhan211))
- minor fix on checking index [\#6694](https://github.com/apache/arrow-rs/pull/6694) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Undo run end filter performance regression [\#6691](https://github.com/apache/arrow-rs/pull/6691) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- Reimplement `PartialEq` of `GenericByteViewArray` compares by logical value [\#6689](https://github.com/apache/arrow-rs/pull/6689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- feat: expose known\_schema from FlightDataEncoder [\#6688](https://github.com/apache/arrow-rs/pull/6688) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([nathanielc](https://github.com/nathanielc))
- Update hashbrown requirement from 0.14.2 to 0.15.1 [\#6684](https://github.com/apache/arrow-rs/pull/6684) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support Duration in JSON Reader [\#6683](https://github.com/apache/arrow-rs/pull/6683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Check predicate and values are the same length for run end array filter safety [\#6675](https://github.com/apache/arrow-rs/pull/6675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- \[ffi\] Fix arrow-array null\_count error during conversion from C to Rust [\#6674](https://github.com/apache/arrow-rs/pull/6674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adbmal](https://github.com/adbmal))
- Support `Utf8View` for `bit_length` kernel [\#6671](https://github.com/apache/arrow-rs/pull/6671) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([austin362667](https://github.com/austin362667))
- Fix string view LIKE checks with NULL values [\#6662](https://github.com/apache/arrow-rs/pull/6662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Improve documentation for `nullif` kernel [\#6658](https://github.com/apache/arrow-rs/pull/6658) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve test\_auth error message when contains\(\) fails [\#6657](https://github.com/apache/arrow-rs/pull/6657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- Let std::fmt::Debug for StructArray output Null/Validity info [\#6655](https://github.com/apache/arrow-rs/pull/6655) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XinyuZeng](https://github.com/XinyuZeng))
- Include offending line number when processing CSV file fails [\#6653](https://github.com/apache/arrow-rs/pull/6653) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- feat: add write\_bytes for GenericBinaryBuilder [\#6652](https://github.com/apache/arrow-rs/pull/6652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tisonkun](https://github.com/tisonkun))
- feat: Support Utf8View in JSON serialization [\#6651](https://github.com/apache/arrow-rs/pull/6651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonmmease](https://github.com/jonmmease))
- fix: include chrono-tz in flight sql cli [\#6650](https://github.com/apache/arrow-rs/pull/6650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Handle primitive REPEATED field not contained in LIST annotated group [\#6649](https://github.com/apache/arrow-rs/pull/6649) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Implement `append_n` for `BooleanBuilder` [\#6646](https://github.com/apache/arrow-rs/pull/6646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- fix: recurse into Map datatype when hydrating dictionaries [\#6645](https://github.com/apache/arrow-rs/pull/6645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([nathanielc](https://github.com/nathanielc))
- fix: enable TLS roots for flight CLI client [\#6640](https://github.com/apache/arrow-rs/pull/6640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- doc: Clarify take kernel semantics [\#6632](https://github.com/apache/arrow-rs/pull/6632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Return error rather than panic when too many row groups are written [\#6629](https://github.com/apache/arrow-rs/pull/6629) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix test feature selection so all feature combinations work as expected [\#6626](https://github.com/apache/arrow-rs/pull/6626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([itsjunetime](https://github.com/itsjunetime))
- Add Parquet RowSelection benchmark [\#6623](https://github.com/apache/arrow-rs/pull/6623) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Optimize `take_bits` to optimize `take_boolean` / `take_primitive` / `take_byte_view`: up to -25% [\#6622](https://github.com/apache/arrow-rs/pull/6622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make downcast macros hygenic \(\#6400\) [\#6620](https://github.com/apache/arrow-rs/pull/6620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.88 to =1.0.89 [\#6618](https://github.com/apache/arrow-rs/pull/6618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix arrow-json writer empty [\#6614](https://github.com/apache/arrow-rs/pull/6614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gwik](https://github.com/gwik))
- Add `ParquetObjectReader::with_runtime` [\#6612](https://github.com/apache/arrow-rs/pull/6612) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([itsjunetime](https://github.com/itsjunetime))
- Re-enable `C#` arrow flight integration test [\#6611](https://github.com/apache/arrow-rs/pull/6611) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

## [53.3.0](https://github.com/apache/arrow-rs/tree/53.3.0) (2024-11-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.2.0...53.3.0)

**Implemented enhancements:**

- `PartialEq` of GenericByteViewArray \(StringViewArray / ByteViewArray\) that compares on equality rather than logical value [\#6679](https://github.com/apache/arrow-rs/issues/6679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Need a mechanism to handle schema changes due to dictionary hydration in FlightSQL server implementations [\#6672](https://github.com/apache/arrow-rs/issues/6672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support encoding Utf8View columns to JSON [\#6642](https://github.com/apache/arrow-rs/issues/6642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `append_n` for `BooleanBuilder` [\#6634](https://github.com/apache/arrow-rs/issues/6634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Some take optimizations [\#6621](https://github.com/apache/arrow-rs/issues/6621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Error Instead of Panic On Attempting to Write More Than 32769 Row Groups [\#6591](https://github.com/apache/arrow-rs/issues/6591) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make casting from a timestamp without timezone to a timestamp with timezone configurable [\#6555](https://github.com/apache/arrow-rs/issues/6555)
- Add `record_batch!` macro for easy record batch creation [\#6553](https://github.com/apache/arrow-rs/issues/6553) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `Binary` --\> `Utf8View` casting [\#6531](https://github.com/apache/arrow-rs/issues/6531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `downcast_primitive_array` and `downcast_dictionary_array` are not hygienic wrt imports [\#6400](https://github.com/apache/arrow-rs/issues/6400) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement interleave\_record\_batch [\#6731](https://github.com/apache/arrow-rs/pull/6731) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- feat: `record_batch!` macro [\#6588](https://github.com/apache/arrow-rs/pull/6588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ByteBaker](https://github.com/ByteBaker))

**Fixed bugs:**

- Signed decimal e-notation parsing bug [\#6728](https://github.com/apache/arrow-rs/issues/6728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for Utf8View -\> numeric in can\_cast\_types [\#6715](https://github.com/apache/arrow-rs/issues/6715)
- IPC file writer produces incorrect footer when not preserving dict ID [\#6710](https://github.com/apache/arrow-rs/issues/6710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet from\_thrift\_helper incorrectly checks index [\#6693](https://github.com/apache/arrow-rs/issues/6693) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Primitive REPEATED fields not contained in LIST annotated groups aren't read as lists by record reader [\#6648](https://github.com/apache/arrow-rs/issues/6648) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- DictionaryHandling does not recurse into Map fields [\#6644](https://github.com/apache/arrow-rs/issues/6644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Array writer output empty when no record is written [\#6613](https://github.com/apache/arrow-rs/issues/6613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Archery Integration Test with c\# failing on main [\#6577](https://github.com/apache/arrow-rs/issues/6577) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Potential unsoundness in `filter_run_end_array` [\#6569](https://github.com/apache/arrow-rs/issues/6569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet reader can generate incorrect validity buffer information for nested structures [\#6510](https://github.com/apache/arrow-rs/issues/6510) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- arrow-array ffi: FFI\_ArrowArray.null\_count is always interpreted as unsigned and initialized during conversion from C to Rust. [\#6497](https://github.com/apache/arrow-rs/issues/6497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Minor: Document pattern for accessing views in StringView [\#6673](https://github.com/apache/arrow-rs/pull/6673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve Array::is\_nullable documentation [\#6615](https://github.com/apache/arrow-rs/pull/6615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Minor: improve docs for ByteViewArray-\>ByteArray From impl [\#6610](https://github.com/apache/arrow-rs/pull/6610) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Speed up `filter_run_end_array` [\#6712](https://github.com/apache/arrow-rs/pull/6712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))

**Closed issues:**

- Incorrect like results for pattern starting/ending with `%` percent and containing escape characters [\#6702](https://github.com/apache/arrow-rs/issues/6702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix signed decimal e-notation parsing [\#6729](https://github.com/apache/arrow-rs/pull/6729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Clean up some arrow-flight tests and duplicated code [\#6725](https://github.com/apache/arrow-rs/pull/6725) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))
- Update PR template section about API breaking changes [\#6723](https://github.com/apache/arrow-rs/pull/6723) ([findepi](https://github.com/findepi))
- Support for casting `StringViewArray` to `DecimalArray` [\#6720](https://github.com/apache/arrow-rs/pull/6720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- File writer preserve dict bug [\#6711](https://github.com/apache/arrow-rs/pull/6711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- Add filter\_kernel benchmark for run array [\#6706](https://github.com/apache/arrow-rs/pull/6706) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- Fix string view ILIKE checks with NULL values [\#6705](https://github.com/apache/arrow-rs/pull/6705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Implement logical\_null\_count for more array types [\#6704](https://github.com/apache/arrow-rs/pull/6704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Fix LIKE with escapes [\#6703](https://github.com/apache/arrow-rs/pull/6703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Speed up `filter_bytes` [\#6699](https://github.com/apache/arrow-rs/pull/6699) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Minor: fix misleading comment in byte view [\#6695](https://github.com/apache/arrow-rs/pull/6695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jayzhan211](https://github.com/jayzhan211))
- minor fix on checking index [\#6694](https://github.com/apache/arrow-rs/pull/6694) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Undo run end filter performance regression [\#6691](https://github.com/apache/arrow-rs/pull/6691) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- Reimplement `PartialEq` of `GenericByteViewArray` compares by logical value [\#6689](https://github.com/apache/arrow-rs/pull/6689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- feat: expose known\_schema from FlightDataEncoder [\#6688](https://github.com/apache/arrow-rs/pull/6688) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([nathanielc](https://github.com/nathanielc))
- Update hashbrown requirement from 0.14.2 to 0.15.1 [\#6684](https://github.com/apache/arrow-rs/pull/6684) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support Duration in JSON Reader [\#6683](https://github.com/apache/arrow-rs/pull/6683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Check predicate and values are the same length for run end array filter safety [\#6675](https://github.com/apache/arrow-rs/pull/6675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- \[ffi\] Fix arrow-array null\_count error during conversion from C to Rust [\#6674](https://github.com/apache/arrow-rs/pull/6674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adbmal](https://github.com/adbmal))
- Support `Utf8View` for `bit_length` kernel [\#6671](https://github.com/apache/arrow-rs/pull/6671) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([austin362667](https://github.com/austin362667))
- Fix string view LIKE checks with NULL values [\#6662](https://github.com/apache/arrow-rs/pull/6662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Improve documentation for `nullif` kernel [\#6658](https://github.com/apache/arrow-rs/pull/6658) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve test\_auth error message when contains\(\) fails [\#6657](https://github.com/apache/arrow-rs/pull/6657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([findepi](https://github.com/findepi))
- Let std::fmt::Debug for StructArray output Null/Validity info [\#6655](https://github.com/apache/arrow-rs/pull/6655) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XinyuZeng](https://github.com/XinyuZeng))
- Include offending line number when processing CSV file fails [\#6653](https://github.com/apache/arrow-rs/pull/6653) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- feat: add write\_bytes for GenericBinaryBuilder [\#6652](https://github.com/apache/arrow-rs/pull/6652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tisonkun](https://github.com/tisonkun))
- feat: Support Utf8View in JSON serialization [\#6651](https://github.com/apache/arrow-rs/pull/6651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonmmease](https://github.com/jonmmease))
- fix: include chrono-tz in flight sql cli [\#6650](https://github.com/apache/arrow-rs/pull/6650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Handle primitive REPEATED field not contained in LIST annotated group [\#6649](https://github.com/apache/arrow-rs/pull/6649) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Implement `append_n` for `BooleanBuilder` [\#6646](https://github.com/apache/arrow-rs/pull/6646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([delamarch3](https://github.com/delamarch3))
- fix: recurse into Map datatype when hydrating dictionaries [\#6645](https://github.com/apache/arrow-rs/pull/6645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([nathanielc](https://github.com/nathanielc))
- fix: enable TLS roots for flight CLI client [\#6640](https://github.com/apache/arrow-rs/pull/6640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- doc: Clarify take kernel semantics [\#6632](https://github.com/apache/arrow-rs/pull/6632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Return error rather than panic when too many row groups are written [\#6629](https://github.com/apache/arrow-rs/pull/6629) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix test feature selection so all feature combinations work as expected [\#6626](https://github.com/apache/arrow-rs/pull/6626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([itsjunetime](https://github.com/itsjunetime))
- Add Parquet RowSelection benchmark [\#6623](https://github.com/apache/arrow-rs/pull/6623) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Optimize `take_bits` to optimize `take_boolean` / `take_primitive` / `take_byte_view`: up to -25% [\#6622](https://github.com/apache/arrow-rs/pull/6622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make downcast macros hygenic \(\#6400\) [\#6620](https://github.com/apache/arrow-rs/pull/6620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.88 to =1.0.89 [\#6618](https://github.com/apache/arrow-rs/pull/6618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix arrow-json writer empty [\#6614](https://github.com/apache/arrow-rs/pull/6614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gwik](https://github.com/gwik))
- Add `ParquetObjectReader::with_runtime` [\#6612](https://github.com/apache/arrow-rs/pull/6612) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([itsjunetime](https://github.com/itsjunetime))
- Re-enable `C#` arrow flight integration test [\#6611](https://github.com/apache/arrow-rs/pull/6611) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add Array::logical\_null\_count for inspecting number of null values [\#6608](https://github.com/apache/arrow-rs/pull/6608) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Added casting from Binary/LargeBinary to Utf8View [\#6592](https://github.com/apache/arrow-rs/pull/6592) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ngli-me](https://github.com/ngli-me))
- Parquet AsyncReader: Don't panic when empty offset\_index is Some\(\[\]\) [\#6582](https://github.com/apache/arrow-rs/pull/6582) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jroddev](https://github.com/jroddev))
- Skip writing down null buffers for non-nullable primitive arrays [\#6524](https://github.com/apache/arrow-rs/pull/6524) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([bkirwi](https://github.com/bkirwi))
## [53.2.0](https://github.com/apache/arrow-rs/tree/53.2.0) (2024-10-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.1.0...53.2.0)

**Implemented enhancements:**

- Implement arrow\_json encoder for Decimal128 & Decimal256 DataTypes [\#6605](https://github.com/apache/arrow-rs/issues/6605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DataType::FixedSizeList in make\_builder within struct\_builder.rs [\#6594](https://github.com/apache/arrow-rs/issues/6594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DataType::Dictionary in `make_builder` within struct\_builder.rs [\#6589](https://github.com/apache/arrow-rs/issues/6589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Interval parsing from string - accept "mon" and "mons" token [\#6548](https://github.com/apache/arrow-rs/issues/6548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `AsyncArrowWriter` API to get the total size of a written parquet file [\#6530](https://github.com/apache/arrow-rs/issues/6530) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `append_many` for Dictionary builders [\#6529](https://github.com/apache/arrow-rs/issues/6529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Missing tonic `GRPC_STATUS` with tonic 0.12.1 [\#6515](https://github.com/apache/arrow-rs/issues/6515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add example of how to use parquet metadata reader APIs for a local cache [\#6504](https://github.com/apache/arrow-rs/issues/6504) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove reliance on `raw-entry` feature of Hashbrown [\#6498](https://github.com/apache/arrow-rs/issues/6498) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Improve page index metadata loading in `SerializedFileReader::new_with_options` [\#6491](https://github.com/apache/arrow-rs/issues/6491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Release arrow-rs / parquet minor version `53.1.0` \(October 2024\) [\#6340](https://github.com/apache/arrow-rs/issues/6340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Compilation fail where `c_char = u8` [\#6571](https://github.com/apache/arrow-rs/issues/6571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow flight CI test failing on `master` [\#6568](https://github.com/apache/arrow-rs/issues/6568) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Documentation updates:**

- Minor: Document SIMD rationale and tips [\#6554](https://github.com/apache/arrow-rs/pull/6554) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Casting to and from unions [\#6247](https://github.com/apache/arrow-rs/issues/6247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Minor: more comments for `RecordBatch.get_array_memory_size()` [\#6607](https://github.com/apache/arrow-rs/pull/6607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([2010YOUY01](https://github.com/2010YOUY01))
- Implement arrow\_json encoder for Decimal128 & Decimal256 [\#6606](https://github.com/apache/arrow-rs/pull/6606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([phillipleblanc](https://github.com/phillipleblanc))
- Add support for building FixedSizeListBuilder in struct\_builder's mak… [\#6595](https://github.com/apache/arrow-rs/pull/6595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszlim](https://github.com/kszlim))
- Add limited support for dictionary builders in `make_builders` for stru… [\#6593](https://github.com/apache/arrow-rs/pull/6593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszlim](https://github.com/kszlim))
- Fix CI with new valid certificates and add script for future usage [\#6585](https://github.com/apache/arrow-rs/pull/6585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))
- Update proc-macro2 requirement from =1.0.87 to =1.0.88 [\#6579](https://github.com/apache/arrow-rs/pull/6579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix clippy complaints [\#6573](https://github.com/apache/arrow-rs/pull/6573) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([itsjunetime](https://github.com/itsjunetime))
- Use c\_char instead of i8 to compile on platforms where c\_char = u8 [\#6572](https://github.com/apache/arrow-rs/pull/6572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([itsjunetime](https://github.com/itsjunetime))
- Bump pyspark from 3.3.1 to 3.3.2 in /parquet/pytest [\#6564](https://github.com/apache/arrow-rs/pull/6564) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- `unsafe` improvements [\#6551](https://github.com/apache/arrow-rs/pull/6551) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ssbr](https://github.com/ssbr))
- Update README.md [\#6550](https://github.com/apache/arrow-rs/pull/6550) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Abdullahsab3](https://github.com/Abdullahsab3))
- Fix string '0' cast to decimal with scale 0 [\#6547](https://github.com/apache/arrow-rs/pull/6547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([findepi](https://github.com/findepi))
- Add finish to `AsyncArrowWriter::finish` [\#6543](https://github.com/apache/arrow-rs/pull/6543) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add append\_nulls to dictionary builders [\#6542](https://github.com/apache/arrow-rs/pull/6542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- Improve UnionArray::is\_nullable [\#6540](https://github.com/apache/arrow-rs/pull/6540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow to read parquet binary column as UTF8 type [\#6539](https://github.com/apache/arrow-rs/pull/6539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([goldmedal](https://github.com/goldmedal))
- Use HashTable instead of raw\_entry\_mut [\#6537](https://github.com/apache/arrow-rs/pull/6537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add append\_many to dictionary arrays to allow adding repeated values [\#6534](https://github.com/apache/arrow-rs/pull/6534) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- Adds documentation and example recommending Vec\<ArrayRef\> over ChunkedArray [\#6527](https://github.com/apache/arrow-rs/pull/6527) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([efredine](https://github.com/efredine))
- Update proc-macro2 requirement from =1.0.86 to =1.0.87 [\#6526](https://github.com/apache/arrow-rs/pull/6526) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `ColumnChunkMetadataBuilder` clear APIs [\#6523](https://github.com/apache/arrow-rs/pull/6523) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update sysinfo requirement from 0.31.2 to 0.32.0 [\#6521](https://github.com/apache/arrow-rs/pull/6521) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update Tonic to 0.12.3 [\#6517](https://github.com/apache/arrow-rs/pull/6517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([cisaacson](https://github.com/cisaacson))
- Detect missing page indexes while reading Parquet metadata [\#6507](https://github.com/apache/arrow-rs/pull/6507) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Use ParquetMetaDataReader to load page indexes in `SerializedFileReader::new_with_options` [\#6506](https://github.com/apache/arrow-rs/pull/6506) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve parquet `MetadataFetch` and `AsyncFileReader` docs [\#6505](https://github.com/apache/arrow-rs/pull/6505) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix arrow-json encoding with dictionary including nulls [\#6503](https://github.com/apache/arrow-rs/pull/6503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Update brotli requirement from 6.0 to 7.0 [\#6499](https://github.com/apache/arrow-rs/pull/6499) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Benchmark both scenarios, with records skipped and without skipping, for delta-bin-packed primitive arrays with half nulls. [\#6489](https://github.com/apache/arrow-rs/pull/6489) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- Add round trip tests for reading/writing parquet metadata [\#6463](https://github.com/apache/arrow-rs/pull/6463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
## [53.1.0](https://github.com/apache/arrow-rs/tree/53.1.0) (2024-10-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.0.0...53.1.0)

**Implemented enhancements:**

- Write null counts in Parquet statistics when they are known to be zero [\#6502](https://github.com/apache/arrow-rs/issues/6502) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make it easier to find / work with `ByteView` [\#6478](https://github.com/apache/arrow-rs/issues/6478) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update lexical-core version due to soundness issues with current version [\#6468](https://github.com/apache/arrow-rs/issues/6468)
- Add builder style API for manipulating `ParquetMetaData` [\#6465](https://github.com/apache/arrow-rs/issues/6465) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ArrayData.align_buffers` should support `Struct` data type / child data [\#6461](https://github.com/apache/arrow-rs/issues/6461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a method to return the number of skipped rows in a `RowSelection` [\#6428](https://github.com/apache/arrow-rs/issues/6428) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Bump lexical-core to 1.0 [\#6397](https://github.com/apache/arrow-rs/issues/6397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add union\_extract kernel [\#6386](https://github.com/apache/arrow-rs/issues/6386) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- implement `regexp_is_match_utf8` and `regexp_is_match_utf8_scalar` for `StringViewArray` [\#6370](https://github.com/apache/arrow-rs/issues/6370) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for BinaryView in arrow\_string::length [\#6358](https://github.com/apache/arrow-rs/issues/6358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `as_union` to `AsArray` [\#6351](https://github.com/apache/arrow-rs/issues/6351)
- Ability to append non contiguous strings to `StringBuilder` [\#6347](https://github.com/apache/arrow-rs/issues/6347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Catalog DB Schema subcommands to `flight_sql_client` [\#6331](https://github.com/apache/arrow-rs/issues/6331) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add support for Utf8View in arrow\_string::length [\#6305](https://github.com/apache/arrow-rs/issues/6305) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reading FIXED\_LEN\_BYTE\_ARRAY columns with nulls is inefficient [\#6296](https://github.com/apache/arrow-rs/issues/6296) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optionally verify 32-bit CRC checksum when decoding parquet pages [\#6289](https://github.com/apache/arrow-rs/issues/6289) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speed up `pad_nulls` for `FixedLenByteArrayBuffer` [\#6297](https://github.com/apache/arrow-rs/pull/6297) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve performance of set\_bits by avoiding to set individual bits [\#6288](https://github.com/apache/arrow-rs/pull/6288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuyukitanimura](https://github.com/kazuyukitanimura))

**Fixed bugs:**

- BitIterator panics when retrieving length [\#6480](https://github.com/apache/arrow-rs/issues/6480) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Flight data retrieved via Python client \(wrapping C++\) cannot be used by Rust Arrow [\#6471](https://github.com/apache/arrow-rs/issues/6471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CI integration test failing: Archery test With other arrows [\#6448](https://github.com/apache/arrow-rs/issues/6448) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- IPC not respecting not preserving dict ID [\#6443](https://github.com/apache/arrow-rs/issues/6443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Failing CI: Prost requires Rust 1.71.1 [\#6436](https://github.com/apache/arrow-rs/issues/6436) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Invalid struct arrays in IPC data causes panic during read [\#6416](https://github.com/apache/arrow-rs/issues/6416) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- REE Dicts cannot be encoded/decoded with streaming IPC [\#6398](https://github.com/apache/arrow-rs/issues/6398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reading json `map` with non-nullable value schema doesn't error if values are actually null [\#6391](https://github.com/apache/arrow-rs/issues/6391)
- StringViewBuilder with deduplication does not clear observed values [\#6384](https://github.com/apache/arrow-rs/issues/6384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast from Decimal\(p, s\) to dictionary-encoded Decimal\(p, s\) loses precision and scale [\#6381](https://github.com/apache/arrow-rs/issues/6381) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- LocalFileSystem `list` operation returns objects in wrong order [\#6375](https://github.com/apache/arrow-rs/issues/6375)
- `compute::binary_mut` returns `Err(PrimitiveArray<T>)` only with certain arrays [\#6374](https://github.com/apache/arrow-rs/issues/6374) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Exporting Binary/Utf8View from arrow-rs to pyarrow fails [\#6366](https://github.com/apache/arrow-rs/issues/6366) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- warning: methods `as_any` and `next_batch` are never used in `parquet` crate [\#6143](https://github.com/apache/arrow-rs/issues/6143) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- chore: add docs, part of \#37 [\#6496](https://github.com/apache/arrow-rs/pull/6496) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([ByteBaker](https://github.com/ByteBaker))
- Minor: improve `ChunkedReader` docs [\#6477](https://github.com/apache/arrow-rs/pull/6477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Add some missing documentation to fix CI errors [\#6445](https://github.com/apache/arrow-rs/pull/6445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Fix doc "bit width" to "byte width" [\#6434](https://github.com/apache/arrow-rs/pull/6434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- chore: add docs, part of \#37 [\#6433](https://github.com/apache/arrow-rs/pull/6433) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ByteBaker](https://github.com/ByteBaker))
- chore: add docs, part of \#37 [\#6424](https://github.com/apache/arrow-rs/pull/6424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ByteBaker](https://github.com/ByteBaker))
- Rephrase doc comment [\#6421](https://github.com/apache/arrow-rs/pull/6421) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([waynexia](https://github.com/waynexia))
- Remove "NOT YET FULLY SUPPORTED" comment from DataType::Utf8View/BinaryView [\#6380](https://github.com/apache/arrow-rs/pull/6380) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `GenericStringBuilder` documentation [\#6372](https://github.com/apache/arrow-rs/pull/6372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Columnar json writer for arrow-json [\#6411](https://github.com/apache/arrow-rs/issues/6411)
- Primitive `binary`/`unary` are not as fast as they could be [\#6364](https://github.com/apache/arrow-rs/issues/6364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Different numeric type may be able to compare [\#6357](https://github.com/apache/arrow-rs/issues/6357)

**Merged pull requests:**

- fix: override `size_hint` for `BitIterator` to return the exact remaining size [\#6495](https://github.com/apache/arrow-rs/pull/6495) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Beihao-Zhou](https://github.com/Beihao-Zhou))
- Minor: Fix path in format command in CONTRIBUTING.md [\#6494](https://github.com/apache/arrow-rs/pull/6494) ([etseidl](https://github.com/etseidl))
- Write null counts in Parquet statistics when they are known [\#6490](https://github.com/apache/arrow-rs/pull/6490) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add configuration option to `StatisticsConverter` to control interpretation of missing null counts in Parquet statistics  [\#6485](https://github.com/apache/arrow-rs/pull/6485) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- fix: check overflow numbers while inferring type for csv files [\#6481](https://github.com/apache/arrow-rs/pull/6481) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([CookiePieWw](https://github.com/CookiePieWw))
- Add better documentation, examples and builer-style API to `ByteView` [\#6479](https://github.com/apache/arrow-rs/pull/6479) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add take\_arrays util for getting entries from 2d arrays [\#6475](https://github.com/apache/arrow-rs/pull/6475) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([akurmustafa](https://github.com/akurmustafa))
- Deprecate `MetadataLoader` [\#6474](https://github.com/apache/arrow-rs/pull/6474) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update tonic-build requirement from =0.12.2 to =0.12.3 [\#6473](https://github.com/apache/arrow-rs/pull/6473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Align buffers from Python \(FFI\) [\#6472](https://github.com/apache/arrow-rs/pull/6472) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([EnricoMi](https://github.com/EnricoMi))
- Add `ParquetMetaDataBuilder` [\#6466](https://github.com/apache/arrow-rs/pull/6466) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Make `ArrayData.align_buffers` align child data buffers recursively [\#6462](https://github.com/apache/arrow-rs/pull/6462) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([EnricoMi](https://github.com/EnricoMi))
- Minor: Silence compiler warnings for `parquet::file::metadata::reader` [\#6457](https://github.com/apache/arrow-rs/pull/6457) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Minor: Error rather than panic for unsupported for dictionary `cast`ing [\#6456](https://github.com/apache/arrow-rs/pull/6456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([goldmedal](https://github.com/goldmedal))
- Support cast between Durations + between Durations all numeric types [\#6452](https://github.com/apache/arrow-rs/pull/6452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tisonkun](https://github.com/tisonkun))
- Deprecate methods from footer.rs in favor of `ParquetMetaDataReader` [\#6451](https://github.com/apache/arrow-rs/pull/6451) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Workaround for missing Parquet page indexes in `ParquetMetadaReader` [\#6450](https://github.com/apache/arrow-rs/pull/6450) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix CI by disabling newly failing rust \<\> nanoarrow integration test in CI [\#6449](https://github.com/apache/arrow-rs/pull/6449) ([alamb](https://github.com/alamb))
- Add `IpcSchemaEncoder`, deprecate ipc schema functions, Fix IPC not respecting not preserving dict ID [\#6444](https://github.com/apache/arrow-rs/pull/6444) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([brancz](https://github.com/brancz))
- Add additional documentation and builder APIs to `SortOptions` [\#6441](https://github.com/apache/arrow-rs/pull/6441) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update prost-build requirement from =0.13.2 to =0.13.3 [\#6440](https://github.com/apache/arrow-rs/pull/6440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump arrow-flight MSRV to 1.71.1 [\#6437](https://github.com/apache/arrow-rs/pull/6437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([gstvg](https://github.com/gstvg))
- Silence warnings that `as_any` and `next_batch` are never used [\#6432](https://github.com/apache/arrow-rs/pull/6432) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add `ParquetMetaDataReader` [\#6431](https://github.com/apache/arrow-rs/pull/6431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add RowSelection::skipped\_row\_count [\#6429](https://github.com/apache/arrow-rs/pull/6429) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- perf: Faster decimal precision overflow checks [\#6419](https://github.com/apache/arrow-rs/pull/6419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- fix: don't panic in IPC reader if struct child arrays have different lengths [\#6417](https://github.com/apache/arrow-rs/pull/6417) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- Reduce integration test matrix [\#6407](https://github.com/apache/arrow-rs/pull/6407) ([kou](https://github.com/kou))
- Move lifetime of `take_iter` from iterator to its items [\#6403](https://github.com/apache/arrow-rs/pull/6403) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dariocurr](https://github.com/dariocurr))
- Update lexical-core requirement from 0.8 to 1.0 \(to resolve RUSTSEC-2023-0086\) [\#6402](https://github.com/apache/arrow-rs/pull/6402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dariocurr](https://github.com/dariocurr))
- Fix encoding/decoding REE Dicts when using streaming IPC [\#6399](https://github.com/apache/arrow-rs/pull/6399) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([brancz](https://github.com/brancz))
- fix: binary\_mut should work if only one input array has null buffer [\#6396](https://github.com/apache/arrow-rs/pull/6396) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `set_bits` fuzz test [\#6394](https://github.com/apache/arrow-rs/pull/6394) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- impl `From<ScalarBuffer<T>>` for `Buffer` [\#6389](https://github.com/apache/arrow-rs/pull/6389) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Add `union_extract` kernel [\#6387](https://github.com/apache/arrow-rs/pull/6387) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Clear string-tracking hash table when ByteView deduplication is enabled [\#6385](https://github.com/apache/arrow-rs/pull/6385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([shanesveller](https://github.com/shanesveller))
- fix: Stop losing precision and scale when casting decimal to dictionary [\#6383](https://github.com/apache/arrow-rs/pull/6383) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Add `ARROW_VERSION` const [\#6379](https://github.com/apache/arrow-rs/pull/6379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- parquet writer: Raise an error when the row\_group\_index overflows i16 [\#6378](https://github.com/apache/arrow-rs/pull/6378) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- Implement native support StringViewArray for `regexp_is_match` and `regexp_is_match_scalar` function, deprecate `regexp_is_match_utf8` and `regexp_is_match_utf8_scalar` [\#6376](https://github.com/apache/arrow-rs/pull/6376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tlm365](https://github.com/tlm365))
- Update chrono-tz requirement from 0.9 to 0.10 [\#6371](https://github.com/apache/arrow-rs/pull/6371) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support StringViewArray interop with python: fix lingering C Data Interface issues for \*ViewArray [\#6368](https://github.com/apache/arrow-rs/pull/6368) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- stop panic in `MetadataLoader` on invalid data [\#6367](https://github.com/apache/arrow-rs/pull/6367) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([samuelcolvin](https://github.com/samuelcolvin))
-  Add support for BinaryView in arrow\_string::length  [\#6359](https://github.com/apache/arrow-rs/pull/6359) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Omega359](https://github.com/Omega359))
- impl `From<Vec<T>>` for `Buffer` [\#6355](https://github.com/apache/arrow-rs/pull/6355) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Add breaking change from \#6043 to `CHANGELOG` [\#6354](https://github.com/apache/arrow-rs/pull/6354) ([mbrobbel](https://github.com/mbrobbel))
- Benchmark for bit\_mask \(set\_bits\) [\#6353](https://github.com/apache/arrow-rs/pull/6353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuyukitanimura](https://github.com/kazuyukitanimura))
- Update prost-build requirement from =0.13.1 to =0.13.2 [\#6350](https://github.com/apache/arrow-rs/pull/6350) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: clippy warnings from nightly rust 1.82 [\#6348](https://github.com/apache/arrow-rs/pull/6348) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Add support for Utf8View in arrow\_string::length [\#6345](https://github.com/apache/arrow-rs/pull/6345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Omega359](https://github.com/Omega359))
- feat: add catalog/schema subcommands to flight\_sql\_client. [\#6332](https://github.com/apache/arrow-rs/pull/6332) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([nathanielc](https://github.com/nathanielc))
- Manually run fmt on all files under parquet [\#6328](https://github.com/apache/arrow-rs/pull/6328) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Implement UnionArray logical\_nulls [\#6303](https://github.com/apache/arrow-rs/pull/6303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Parquet: Verify 32-bit CRC checksum when decoding pages [\#6290](https://github.com/apache/arrow-rs/pull/6290) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([xmakro](https://github.com/xmakro))
- 
## [53.0.0](https://github.com/apache/arrow-rs/tree/53.0.0) (2024-08-31)

[Full Changelog](https://github.com/apache/arrow-rs/compare/52.2.0...53.0.0)

**Breaking changes:**

- parquet\_derive: Match fields by name, support reading selected fields rather than all [\#6269](https://github.com/apache/arrow-rs/pull/6269) ([double-free](https://github.com/double-free))
- Update parquet object\_store dependency to 0.11.0 [\#6264](https://github.com/apache/arrow-rs/pull/6264) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- parquet Statistics - deprecate `has_*` APIs and add `_opt` functions that return `Option<T>` [\#6216](https://github.com/apache/arrow-rs/pull/6216) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Michael-J-Ward](https://github.com/Michael-J-Ward))
- Expose bulk ingest in flight sql client and server [\#6201](https://github.com/apache/arrow-rs/pull/6201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([djanderson](https://github.com/djanderson))
- Upgrade protobuf definitions to flightsql 17.0 \(\#6133\) [\#6169](https://github.com/apache/arrow-rs/pull/6169) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Remove automatic buffering in `ipc::reader::FileReader` for for consistent buffering [\#6132](https://github.com/apache/arrow-rs/pull/6132) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([V0ldek](https://github.com/V0ldek))
- No longer write Parquet column metadata after column chunks \*and\* in the footer [\#6117](https://github.com/apache/arrow-rs/pull/6117) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Remove `impl<T: AsRef<[u8]>> From<T> for Buffer` that easily accidentally copies data [\#6043](https://github.com/apache/arrow-rs/pull/6043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))

**Implemented enhancements:**

- Derive `PartialEq` and `Eq` for `parquet::arrow::ProjectionMask` [\#6329](https://github.com/apache/arrow-rs/issues/6329) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Allow converting empty `pyarrow.RecordBatch` to `arrow::RecordBatch` [\#6318](https://github.com/apache/arrow-rs/issues/6318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet writer should not write any min/max data to ColumnIndex when all values are null [\#6315](https://github.com/apache/arrow-rs/issues/6315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: Add `union` method to `RowSelection` [\#6307](https://github.com/apache/arrow-rs/issues/6307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing `UTC adjusted time` arrow array to parquet [\#6277](https://github.com/apache/arrow-rs/issues/6277) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- A better way to resize the buffer for the snappy encode/decode [\#6276](https://github.com/apache/arrow-rs/issues/6276) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet\_derive: support reading selected columns from parquet file [\#6268](https://github.com/apache/arrow-rs/issues/6268)
- Tests for invalid parquet files [\#6261](https://github.com/apache/arrow-rs/issues/6261) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement `date_part` for `Duration` [\#6245](https://github.com/apache/arrow-rs/issues/6245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Avoid unnecessary null buffer construction when converting arrays to a different type [\#6243](https://github.com/apache/arrow-rs/issues/6243) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `parquet_opendal` in related projects [\#6235](https://github.com/apache/arrow-rs/issues/6235)
- Look into optimizing reading FixedSizeBinary arrays from parquet [\#6219](https://github.com/apache/arrow-rs/issues/6219) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add benchmarks for `BYTE_STREAM_SPLIT` encoded Parquet `FIXED_LEN_BYTE_ARRAY` data [\#6203](https://github.com/apache/arrow-rs/issues/6203) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make it easy to write parquet to object\_store -- Implement `AsyncFileWriter` for a type that implements `obj_store::MultipartUpload` for `AsyncArrowWriter` [\#6200](https://github.com/apache/arrow-rs/issues/6200) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove test duplication in parquet statistics tets [\#6185](https://github.com/apache/arrow-rs/issues/6185) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support BinaryView Types in C Schema FFI [\#6170](https://github.com/apache/arrow-rs/issues/6170) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- speedup take\_byte\_view kernel [\#6167](https://github.com/apache/arrow-rs/issues/6167) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `StringView` and `BinaryView` statistics in `StatisticsConverter` [\#6164](https://github.com/apache/arrow-rs/issues/6164) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting `BinaryView` --\> `Utf8` and `LargeUtf8` [\#6162](https://github.com/apache/arrow-rs/issues/6162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `filter` kernel specially for `FixedSizeByteArray` [\#6153](https://github.com/apache/arrow-rs/issues/6153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `LevelHistogram` throughout Parquet metadata [\#6134](https://github.com/apache/arrow-rs/issues/6134) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support DoPutStatementIngest from Arrow Flight SQL 17.0 [\#6124](https://github.com/apache/arrow-rs/issues/6124) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- ColumnMetaData should no longer be written inline with data [\#6115](https://github.com/apache/arrow-rs/issues/6115) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement date\_part for `Interval`  [\#6113](https://github.com/apache/arrow-rs/issues/6113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Into<Arc<dyn Array>>` for `ArrayData` [\#6104](https://github.com/apache/arrow-rs/issues/6104)
- Allow flushing or non-buffered writes from `arrow::ipc::writer::StreamWriter` [\#6099](https://github.com/apache/arrow-rs/issues/6099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Default block\_size for `StringViewArray` [\#6094](https://github.com/apache/arrow-rs/issues/6094) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove `Statistics::has_min_max_set` and `ValueStatistics::has_min_max_set` and use `Option` instead [\#6093](https://github.com/apache/arrow-rs/issues/6093) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Upgrade arrow-flight to tonic 0.12 [\#6072](https://github.com/apache/arrow-rs/issues/6072)
- Improve speed of row converter by skipping utf8 checks [\#6058](https://github.com/apache/arrow-rs/issues/6058) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Extend support for BYTE\_STREAM\_SPLIT to FIXED\_LEN\_BYTE\_ARRAY, INT32, and INT64 primitive types [\#6048](https://github.com/apache/arrow-rs/issues/6048) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Release arrow-rs / parquet minor version `52.2.0` \(August 2024\) [\#5998](https://github.com/apache/arrow-rs/issues/5998) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Invalid `ColumnIndex` written in parquet [\#6310](https://github.com/apache/arrow-rs/issues/6310) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- comparison\_kernels benchmarks panic [\#6283](https://github.com/apache/arrow-rs/issues/6283) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Printing schema metadata includes possibly incorrect compression level [\#6270](https://github.com/apache/arrow-rs/issues/6270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Don't panic when creating `Field` from `FFI_ArrowSchema` with no name [\#6251](https://github.com/apache/arrow-rs/issues/6251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- lexsort\_to\_indices should not fallback to non-lexical sort if the datatype is not supported [\#6226](https://github.com/apache/arrow-rs/issues/6226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Statistics null\_count does not distinguish between `0` and not specified [\#6215](https://github.com/apache/arrow-rs/issues/6215) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Using a take kernel on a dense union can result in reaching "unreachable" code [\#6206](https://github.com/apache/arrow-rs/issues/6206) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Adding sub day seconds to Date64 is ignored. [\#6198](https://github.com/apache/arrow-rs/issues/6198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- mismatch between parquet type `is_optional` codes and comment [\#6191](https://github.com/apache/arrow-rs/issues/6191) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Minor: improve filter documentation [\#6317](https://github.com/apache/arrow-rs/pull/6317) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Improve comments on GenericByteViewArray::bytes\_iter\(\), prefix\_iter\(\) and suffix\_iter\(\) [\#6306](https://github.com/apache/arrow-rs/pull/6306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: improve `RowFilter` and `ArrowPredicate` docs [\#6301](https://github.com/apache/arrow-rs/pull/6301) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve documentation for `MutableArrayData` [\#6272](https://github.com/apache/arrow-rs/pull/6272) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add examples to `StringViewBuilder` and `BinaryViewBuilder` [\#6240](https://github.com/apache/arrow-rs/pull/6240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- minor: enhance document for ParquetField [\#6239](https://github.com/apache/arrow-rs/pull/6239) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mapleFU](https://github.com/mapleFU))
- Minor: Improve Type documentation [\#6224](https://github.com/apache/arrow-rs/pull/6224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Update `DateType::Date64` docs [\#6223](https://github.com/apache/arrow-rs/pull/6223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add \(more\) Parquet Metadata Documentation [\#6184](https://github.com/apache/arrow-rs/pull/6184) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add additional documentation and examples to `ArrayAccessor` [\#6141](https://github.com/apache/arrow-rs/pull/6141) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: improve comments in temporal.rs tests [\#6140](https://github.com/apache/arrow-rs/pull/6140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Update release schedule in README [\#6125](https://github.com/apache/arrow-rs/pull/6125) ([alamb](https://github.com/alamb))

**Closed issues:**

- Simplify take octokit workflow [\#6279](https://github.com/apache/arrow-rs/issues/6279)
- Make the bearer token visible in FlightSqlServiceClient [\#6253](https://github.com/apache/arrow-rs/issues/6253) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Port `take` workflow to use `oktokit` [\#6242](https://github.com/apache/arrow-rs/issues/6242)
- Remove `SchemaBuilder` dependency from `StructArray` constructors [\#6138](https://github.com/apache/arrow-rs/issues/6138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Derive PartialEq and Eq for parquet::arrow::ProjectionMask [\#6330](https://github.com/apache/arrow-rs/pull/6330) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Support zero column `RecordBatch`es in pyarrow integration \(use RecordBatchOptions when converting a pyarrow RecordBatch\) [\#6320](https://github.com/apache/arrow-rs/pull/6320) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Michael-J-Ward](https://github.com/Michael-J-Ward))
- Fix writing of invalid  Parquet ColumnIndex when row group contains null pages [\#6319](https://github.com/apache/arrow-rs/pull/6319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Pass empty vectors as min/max for all null pages when building ColumnIndex [\#6316](https://github.com/apache/arrow-rs/pull/6316) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update tonic-build requirement from =0.12.0 to =0.12.2 [\#6314](https://github.com/apache/arrow-rs/pull/6314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Parquet: add `union` method to `RowSelection` [\#6308](https://github.com/apache/arrow-rs/pull/6308) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sdd](https://github.com/sdd))
- Specialize filter for structs and sparse unions [\#6304](https://github.com/apache/arrow-rs/pull/6304) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Err on `try_from_le_slice` [\#6295](https://github.com/apache/arrow-rs/pull/6295) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([samuelcolvin](https://github.com/samuelcolvin))
- fix reference in doctest to size\_of which is not imported by default [\#6286](https://github.com/apache/arrow-rs/pull/6286) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rtyler](https://github.com/rtyler))
- Support writing UTC adjusted time arrays to parquet [\#6278](https://github.com/apache/arrow-rs/pull/6278) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([aykut-bozkurt](https://github.com/aykut-bozkurt))
- Minor: `pub use ByteView` in arrow and improve documentation [\#6275](https://github.com/apache/arrow-rs/pull/6275) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix accessing name from ffi schema [\#6273](https://github.com/apache/arrow-rs/pull/6273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Do not print compression level in schema printer [\#6271](https://github.com/apache/arrow-rs/pull/6271) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ttencate](https://github.com/ttencate))
- ci: use octokit to add assignee [\#6267](https://github.com/apache/arrow-rs/pull/6267) ([dsgibbons](https://github.com/dsgibbons))
- Add tests for bad parquet files [\#6262](https://github.com/apache/arrow-rs/pull/6262) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add `Statistics::distinct_count_opt` and deprecate `Statistics::distinct_count` [\#6259](https://github.com/apache/arrow-rs/pull/6259) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: move `FallibleRequestStream` and `FallibleTonicResponseStream` to a module [\#6258](https://github.com/apache/arrow-rs/pull/6258) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Make the bearer token visible in FlightSqlServiceClient [\#6254](https://github.com/apache/arrow-rs/pull/6254) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([ccciudatu](https://github.com/ccciudatu))
- Use `unary()` for array conversion in Parquet array readers, speed up `Decimal128`, `Decimal256` and `Float16`  [\#6252](https://github.com/apache/arrow-rs/pull/6252) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Update tower requirement from 0.4.13 to 0.5.0 [\#6250](https://github.com/apache/arrow-rs/pull/6250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Implement date\_part for durations [\#6246](https://github.com/apache/arrow-rs/pull/6246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nrc](https://github.com/nrc))
- Remove unnecessary null buffer construction when converting arrays to a different type [\#6244](https://github.com/apache/arrow-rs/pull/6244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([etseidl](https://github.com/etseidl))
- Implement PartialEq for GenericByteViewArray [\#6241](https://github.com/apache/arrow-rs/pull/6241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Remove non standard footer from LICENSE.txt / reference to Apache Aurora [\#6237](https://github.com/apache/arrow-rs/pull/6237) ([alamb](https://github.com/alamb))
- docs: Add parquet\_opendal in related projects [\#6236](https://github.com/apache/arrow-rs/pull/6236) ([Xuanwo](https://github.com/Xuanwo))
- Avoid infinite loop in bad parquet by checking the number of rep levels  [\#6232](https://github.com/apache/arrow-rs/pull/6232) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Specialize Prefix/Suffix Match for `Like/ILike` between Array and Scalar for StringViewArray [\#6231](https://github.com/apache/arrow-rs/pull/6231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xinlifoobar](https://github.com/xinlifoobar))
- fix: lexsort\_to\_indices should not fallback to non-lexical sort if the datatype is not supported [\#6225](https://github.com/apache/arrow-rs/pull/6225) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Modest improvement to FixedLenByteArray BYTE\_STREAM\_SPLIT arrow decoder [\#6222](https://github.com/apache/arrow-rs/pull/6222) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve performance of `FixedLengthBinary` decoding  [\#6220](https://github.com/apache/arrow-rs/pull/6220) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Update documentation for Parquet BYTE\_STREAM\_SPLIT encoding [\#6212](https://github.com/apache/arrow-rs/pull/6212) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Improve interval parsing [\#6211](https://github.com/apache/arrow-rs/pull/6211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- minor: Suggest take on interleave docs [\#6210](https://github.com/apache/arrow-rs/pull/6210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- fix: Correctly handle take on dense union of a single selected type [\#6209](https://github.com/apache/arrow-rs/pull/6209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Add time dictionary coercions [\#6208](https://github.com/apache/arrow-rs/pull/6208) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([adriangb](https://github.com/adriangb))
- fix\(arrow\): restrict the range of temporal values produced via `data_gen` [\#6205](https://github.com/apache/arrow-rs/pull/6205) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kyle-mccarthy](https://github.com/kyle-mccarthy))
- Add benchmarks for `BYTE_STREAM_SPLIT` encoded Parquet `FIXED_LEN_BYTE_ARRAY` data  [\#6204](https://github.com/apache/arrow-rs/pull/6204) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Move `ParquetMetadataWriter` to its own module, update documentation [\#6202](https://github.com/apache/arrow-rs/pull/6202) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add `ThriftMetadataWriter` for writing Parquet metadata [\#6197](https://github.com/apache/arrow-rs/pull/6197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Update zstd-sys requirement from \>=2.0.0, \<2.0.13 to \>=2.0.0, \<2.0.14 [\#6196](https://github.com/apache/arrow-rs/pull/6196) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix parquet type `is_optional` comments [\#6192](https://github.com/apache/arrow-rs/pull/6192) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jp0317](https://github.com/jp0317))
- Remove duplicated statistics tests in parquet [\#6190](https://github.com/apache/arrow-rs/pull/6190) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Kev1n8](https://github.com/Kev1n8))
- Benchmarks for `bool_and` [\#6189](https://github.com/apache/arrow-rs/pull/6189) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Fix typo in documentation of Float64Array [\#6188](https://github.com/apache/arrow-rs/pull/6188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mesejo](https://github.com/mesejo))
- Make it clear that `StatisticsConverter` can not panic [\#6187](https://github.com/apache/arrow-rs/pull/6187) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- add filter benchmark for `FixedSizeBinaryArray` [\#6186](https://github.com/apache/arrow-rs/pull/6186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chloro-pn](https://github.com/chloro-pn))
- Update sysinfo requirement from 0.30.12 to 0.31.2 [\#6182](https://github.com/apache/arrow-rs/pull/6182) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add support for `StringView` and `BinaryView` statistics in `StatisticsConverter` [\#6181](https://github.com/apache/arrow-rs/pull/6181) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Kev1n8](https://github.com/Kev1n8))
- Support casting between BinaryView \<--\> Utf8 and LargeUtf8 [\#6180](https://github.com/apache/arrow-rs/pull/6180) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xinlifoobar](https://github.com/xinlifoobar))
- Implement specialized filter kernel for `FixedSizeByteArray` [\#6178](https://github.com/apache/arrow-rs/pull/6178) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chloro-pn](https://github.com/chloro-pn))
- Support `StringView` and `BinaryView` in CDataInterface  [\#6171](https://github.com/apache/arrow-rs/pull/6171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- Optimize `take` kernel for `BinaryViewArray` and `StringViewArray` [\#6168](https://github.com/apache/arrow-rs/pull/6168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- Support Parquet `BYTE_STREAM_SPLIT` for INT32, INT64, and FIXED\_LEN\_BYTE\_ARRAY primitive types [\#6159](https://github.com/apache/arrow-rs/pull/6159) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fix comparison kernel benchmarks [\#6147](https://github.com/apache/arrow-rs/pull/6147) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- improve `LIKE` regex performance up to 12x [\#6145](https://github.com/apache/arrow-rs/pull/6145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Optimize `min_boolean` and `bool_and` [\#6144](https://github.com/apache/arrow-rs/pull/6144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Reduce bounds check in `RowIter`, add `unsafe Rows::row_unchecked` [\#6142](https://github.com/apache/arrow-rs/pull/6142) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Minor: Simplify `StructArray` constructors [\#6139](https://github.com/apache/arrow-rs/pull/6139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Rafferty97](https://github.com/Rafferty97))
- Implement exponential block size growing strategy for `StringViewBuilder` [\#6136](https://github.com/apache/arrow-rs/pull/6136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Use `LevelHistogram` in `PageIndex` [\#6135](https://github.com/apache/arrow-rs/pull/6135) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Add ArrowError::ArithmeticError [\#6130](https://github.com/apache/arrow-rs/pull/6130) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Improve `LIKE` performance for "contains" style queries [\#6128](https://github.com/apache/arrow-rs/pull/6128) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Add `BooleanArray::new_from_packed` and `BooleanArray::new_from_u8` [\#6127](https://github.com/apache/arrow-rs/pull/6127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chloro-pn](https://github.com/chloro-pn))
- improvements to `(i)starts_with` and `(i)ends_with` performance [\#6118](https://github.com/apache/arrow-rs/pull/6118) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Fix Clippy for the Rust 1.80 release [\#6116](https://github.com/apache/arrow-rs/pull/6116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- added a flush method to IPC writers [\#6108](https://github.com/apache/arrow-rs/pull/6108) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([V0ldek](https://github.com/V0ldek))
- Add support for level histograms added in PARQUET-2261 to `ParquetMetaData` [\#6105](https://github.com/apache/arrow-rs/pull/6105) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Implement date\_part for intervals [\#6071](https://github.com/apache/arrow-rs/pull/6071) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nrc](https://github.com/nrc))
- feat\(parquet\): Implement AsyncFileWriter for `object_store::buffered::BufWriter` [\#6013](https://github.com/apache/arrow-rs/pull/6013) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Xuanwo](https://github.com/Xuanwo))
## [52.2.0](https://github.com/apache/arrow-rs/tree/52.2.0) (2024-07-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/52.1.0...52.2.0)

**Implemented enhancements:**

- Faster min/max for string/binary view arrays [\#6088](https://github.com/apache/arrow-rs/issues/6088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting to/from Utf8View [\#6076](https://github.com/apache/arrow-rs/issues/6076) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Min/max support for String/BinaryViewArray [\#6052](https://github.com/apache/arrow-rs/issues/6052) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of constructing `ByteView`s for small strings [\#6034](https://github.com/apache/arrow-rs/issues/6034) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fast UTF-8 validation when reading StringViewArray from Parquet [\#5995](https://github.com/apache/arrow-rs/issues/5995) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize StringView row decoding [\#5945](https://github.com/apache/arrow-rs/issues/5945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implementing `deduplicate` / `intern` functionality for StringView [\#5910](https://github.com/apache/arrow-rs/issues/5910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `FlightSqlServiceClient::new_from_inner` [\#6003](https://github.com/apache/arrow-rs/pull/6003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Complete `StringViewArray` and `BinaryViewArray` parquet decoder:  [\#6004](https://github.com/apache/arrow-rs/pull/6004) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add begin/end\_transaction methods in FlightSqlServiceClient [\#6026](https://github.com/apache/arrow-rs/pull/6026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Read Parquet statistics as arrow `Arrays`  [\#6046](https://github.com/apache/arrow-rs/pull/6046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([efredine](https://github.com/efredine))

**Fixed bugs:**

- Panic in `ParquetMetadata::memory_size` if no min/max set [\#6091](https://github.com/apache/arrow-rs/issues/6091) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- BinaryViewArray doesn't roundtrip a single `Some(&[])` through parquet [\#6086](https://github.com/apache/arrow-rs/issues/6086) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet `ColumnIndex` for null columns is written even when statistics are disabled [\#6010](https://github.com/apache/arrow-rs/issues/6010) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Fix typo in GenericByteViewArray documentation [\#6054](https://github.com/apache/arrow-rs/pull/6054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([progval](https://github.com/progval))
- Minor: Improve parquet PageIndex documentation [\#6042](https://github.com/apache/arrow-rs/pull/6042) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Potential performance improvements for reading Parquet to StringViewArray/BinaryViewArray [\#5904](https://github.com/apache/arrow-rs/issues/5904) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Faster `GenericByteView` construction [\#6102](https://github.com/apache/arrow-rs/pull/6102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add benchmark to track byte-view construction performance [\#6101](https://github.com/apache/arrow-rs/pull/6101) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Optimize `bool_or` using `max_boolean` [\#6100](https://github.com/apache/arrow-rs/pull/6100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- Optimize `max_boolean` by operating on u64 chunks [\#6098](https://github.com/apache/arrow-rs/pull/6098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([simonvandel](https://github.com/simonvandel))
- fix panic in `ParquetMetadata::memory_size`: check has\_min\_max\_set before invoking min\(\)/max\(\) [\#6092](https://github.com/apache/arrow-rs/pull/6092) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Fischer0522](https://github.com/Fischer0522))
- Implement specialized min/max for `GenericBinaryView` \(`StringView` and `BinaryView`\) [\#6089](https://github.com/apache/arrow-rs/pull/6089) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add PartialEq to ParquetMetaData and FileMetadata [\#6082](https://github.com/apache/arrow-rs/pull/6082) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([adriangb](https://github.com/adriangb))
- Enable casting from Utf8View [\#6077](https://github.com/apache/arrow-rs/pull/6077) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([a10y](https://github.com/a10y))
- StringView support in arrow-csv [\#6062](https://github.com/apache/arrow-rs/pull/6062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([2010YOUY01](https://github.com/2010YOUY01))
- Implement min max support for string/binary view types [\#6053](https://github.com/apache/arrow-rs/pull/6053) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Minor: clarify the relationship between `file::metadata` and `format` in docs [\#6049](https://github.com/apache/arrow-rs/pull/6049) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor API adjustments for StringViewBuilder [\#6047](https://github.com/apache/arrow-rs/pull/6047) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add parquet `StatisticsConverter` for arrow reader [\#6046](https://github.com/apache/arrow-rs/pull/6046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([efredine](https://github.com/efredine))
- Directly decode String/BinaryView types from arrow-row format [\#6044](https://github.com/apache/arrow-rs/pull/6044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Clean up unused code for view types in offset buffer [\#6040](https://github.com/apache/arrow-rs/pull/6040) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Avoid using Buffer api that accidentally copies data [\#6039](https://github.com/apache/arrow-rs/pull/6039) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([XiangpengHao](https://github.com/XiangpengHao))
- MINOR: Fix `hashbrown` version in `arrow-array`, remove from `arrow-row` [\#6035](https://github.com/apache/arrow-rs/pull/6035) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Improve performance reading `ByteViewArray` from parquet by removing an implicit copy [\#6031](https://github.com/apache/arrow-rs/pull/6031) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add begin/end\_transaction methods in FlightSqlServiceClient [\#6026](https://github.com/apache/arrow-rs/pull/6026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Unsafe improvements: core `parquet` crate. [\#6024](https://github.com/apache/arrow-rs/pull/6024) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([veluca93](https://github.com/veluca93))
- Additional tests for parquet reader utf8 validation [\#6023](https://github.com/apache/arrow-rs/pull/6023) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update zstd-sys requirement from \>=2.0.0, \<2.0.12 to \>=2.0.0, \<2.0.13 [\#6019](https://github.com/apache/arrow-rs/pull/6019) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix doc ci in latest rust nightly version [\#6012](https://github.com/apache/arrow-rs/pull/6012) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Rachelint](https://github.com/Rachelint))
- Do not write `ColumnIndex` for null columns when not writing page statistics [\#6011](https://github.com/apache/arrow-rs/pull/6011) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([etseidl](https://github.com/etseidl))
- Fast utf8 validation when loading string view from parquet [\#6009](https://github.com/apache/arrow-rs/pull/6009) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Deduplicate strings/binarys when building view types [\#6005](https://github.com/apache/arrow-rs/pull/6005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Complete `StringViewArray` and `BinaryViewArray` parquet decoder:  implement delta byte array and delta length byte array encoding [\#6004](https://github.com/apache/arrow-rs/pull/6004) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add `FlightSqlServiceClient::new_from_inner` [\#6003](https://github.com/apache/arrow-rs/pull/6003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Rename `Schema::all_fields` to `flattened_fields` [\#6001](https://github.com/apache/arrow-rs/pull/6001) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([lewiszlw](https://github.com/lewiszlw))
- Refine documentation and examples for `DataType` [\#5997](https://github.com/apache/arrow-rs/pull/5997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- implement `DataType::try_form(&str)` [\#5994](https://github.com/apache/arrow-rs/pull/5994) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([samuelcolvin](https://github.com/samuelcolvin))
- Implement dictionary support for reading ByteView from parquet [\#5973](https://github.com/apache/arrow-rs/pull/5973) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
## [52.1.0](https://github.com/apache/arrow-rs/tree/52.1.0) (2024-07-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/52.0.0...52.1.0)


**Implemented enhancements:**

- Implement `eq` comparison for StructArray [\#5960](https://github.com/apache/arrow-rs/issues/5960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- A new feature as a workaround hack to unavailable offset support in Arrow Java [\#5959](https://github.com/apache/arrow-rs/issues/5959) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `min_bytes` and `max_bytes` to `PageIndex` [\#5949](https://github.com/apache/arrow-rs/issues/5949) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error message in ArrowNativeTypeOp::neg\_checked doesn't include the operation [\#5944](https://github.com/apache/arrow-rs/issues/5944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add object\_store\_opendal as related projects [\#5925](https://github.com/apache/arrow-rs/issues/5925)
- Opaque retry errors make debugging difficult [\#5923](https://github.com/apache/arrow-rs/issues/5923)
- Implement arrow-row en/decoding for GenericByteView types [\#5921](https://github.com/apache/arrow-rs/issues/5921) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The arrow-rs repo is very large [\#5908](https://github.com/apache/arrow-rs/issues/5908)
- \[DISCUSS\] Release arrow-rs / parquet patch release `52.0.1` [\#5906](https://github.com/apache/arrow-rs/issues/5906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `compare_op` for `GenericBinaryView`  [\#5897](https://github.com/apache/arrow-rs/issues/5897) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- New null with view types are not supported [\#5893](https://github.com/apache/arrow-rs/issues/5893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cleanup ByteView construction [\#5878](https://github.com/apache/arrow-rs/issues/5878) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `cast` kernel support for `StringViewArray` and `BinaryViewArray` `\<--\> `DictionaryArray` [\#5861](https://github.com/apache/arrow-rs/issues/5861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet::ArrowWriter show allow writing Bloom filters before the end of the file [\#5859](https://github.com/apache/arrow-rs/issues/5859) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- API to get memory usage for parquet ArrowWriter [\#5851](https://github.com/apache/arrow-rs/issues/5851) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support writing `IntervalMonthDayNanoArray` to parquet via Arrow Writer  [\#5849](https://github.com/apache/arrow-rs/issues/5849) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Write parquet statistics for `IntervalDayTimeArray` , `IntervalMonthDayNanoArray` and `IntervalYearMonthArray` [\#5847](https://github.com/apache/arrow-rs/issues/5847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `RowSelection::from_consecutive_ranges` public [\#5846](https://github.com/apache/arrow-rs/issues/5846) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Schema::try_merge` should be able to merge List of any data type with List of Null data type [\#5843](https://github.com/apache/arrow-rs/issues/5843) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a way to move `fields` out of parquet `Row` [\#5841](https://github.com/apache/arrow-rs/issues/5841) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `TimeUnit` and `IntervalUnit` `Copy` [\#5839](https://github.com/apache/arrow-rs/issues/5839) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Limit Parquet Page Row Count By Default to reduce writer memory requirements with highly compressable columns [\#5797](https://github.com/apache/arrow-rs/issues/5797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Report / blog on parquet metadata sizes for "large" \(1000+\) numbers of columns [\#5770](https://github.com/apache/arrow-rs/issues/5770) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Structured ByteView Access \(underlying StringView/BinaryView representation\) [\#5736](https://github.com/apache/arrow-rs/issues/5736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\_derive\] support OPTIONAL \(def\_level = 1\) columns by default [\#5716](https://github.com/apache/arrow-rs/issues/5716)
- Maps cast to other Maps with different Elements, Key and Value Names [\#5702](https://github.com/apache/arrow-rs/issues/5702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Provide Arrow Schema Hint to Parquet Reader [\#5657](https://github.com/apache/arrow-rs/issues/5657) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Wrong error type in case of invalid amount in Interval components [\#5986](https://github.com/apache/arrow-rs/issues/5986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Empty and Null structarray fails to IPC roundtrip  [\#5920](https://github.com/apache/arrow-rs/issues/5920)
- FixedSizeList got out of range when the total length of the underlying values over i32::MAX [\#5901](https://github.com/apache/arrow-rs/issues/5901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Out of range when extending on a slice of string array imported through FFI [\#5896](https://github.com/apache/arrow-rs/issues/5896) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- cargo msrv test is failing on main for `object_store` [\#5864](https://github.com/apache/arrow-rs/issues/5864) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- chore: update RunArray reference in run\_iterator.rs [\#5892](https://github.com/apache/arrow-rs/pull/5892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Minor: Clarify when page index structures are read [\#5886](https://github.com/apache/arrow-rs/pull/5886) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Improve Parquet reader/writer properties docs [\#5863](https://github.com/apache/arrow-rs/pull/5863) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Refine documentation for `unary_mut` and `binary_mut` [\#5798](https://github.com/apache/arrow-rs/pull/5798) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Implement benchmarks for `compare_op` for `GenericBinaryView` [\#5903](https://github.com/apache/arrow-rs/issues/5903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- fix: error in case of invalid amount interval component [\#5987](https://github.com/apache/arrow-rs/pull/5987) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([DDtKey](https://github.com/DDtKey))
- Minor: fix clippy complaint in parquet\_derive [\#5984](https://github.com/apache/arrow-rs/pull/5984) ([alamb](https://github.com/alamb))
- Reduce repo size by removing accumulative commits in CI job [\#5982](https://github.com/apache/arrow-rs/pull/5982) ([Owen-CH-Leung](https://github.com/Owen-CH-Leung))
- Add operation in ArrowNativeTypeOp::neg\_check error message \(\#5944\) [\#5980](https://github.com/apache/arrow-rs/pull/5980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zhao-gang](https://github.com/zhao-gang))
- Implement directly build byte view array on top of parquet buffer [\#5972](https://github.com/apache/arrow-rs/pull/5972) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Handle flight dictionary ID assignment automatically [\#5971](https://github.com/apache/arrow-rs/pull/5971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([thinkharderdev](https://github.com/thinkharderdev))
- Add view buffer for parquet reader [\#5970](https://github.com/apache/arrow-rs/pull/5970) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add benchmark for reading binary/binary view from parquet [\#5968](https://github.com/apache/arrow-rs/pull/5968) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- feat\(5851\): ArrowWriter memory usage [\#5967](https://github.com/apache/arrow-rs/pull/5967) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- Add ParquetMetadata::memory\_size size estimation [\#5965](https://github.com/apache/arrow-rs/pull/5965) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Fix FFI array offset handling [\#5964](https://github.com/apache/arrow-rs/pull/5964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement sort for String/BinaryViewArray [\#5963](https://github.com/apache/arrow-rs/pull/5963) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Improve error message for unsupported nested comparison [\#5961](https://github.com/apache/arrow-rs/pull/5961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- chore\(5797\): change default parquet data\_page\_row\_limit to 20k [\#5957](https://github.com/apache/arrow-rs/pull/5957) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([wiedld](https://github.com/wiedld))
- Document process for PRs with breaking changes [\#5953](https://github.com/apache/arrow-rs/pull/5953) ([alamb](https://github.com/alamb))
- Minor: fixup contribution guide about clippy [\#5952](https://github.com/apache/arrow-rs/pull/5952) ([alamb](https://github.com/alamb))
- feat: add max\_bytes and min\_bytes on PageIndex [\#5950](https://github.com/apache/arrow-rs/pull/5950) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tshauck](https://github.com/tshauck))
- test: Add unit test for extending slice of list array [\#5948](https://github.com/apache/arrow-rs/pull/5948) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- minor: row format benches for bool & nullable int [\#5943](https://github.com/apache/arrow-rs/pull/5943) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([korowa](https://github.com/korowa))
- Better document support for nested comparison [\#5942](https://github.com/apache/arrow-rs/pull/5942) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Provide Arrow Schema Hint to Parquet Reader - Alternative 2 [\#5939](https://github.com/apache/arrow-rs/pull/5939) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([efredine](https://github.com/efredine))
- `like` benchmark for StringView [\#5936](https://github.com/apache/arrow-rs/pull/5936) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix typo in benchmark name `egexp` --\> `regexp` [\#5935](https://github.com/apache/arrow-rs/pull/5935) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Revert "Write Bloom filters between row groups instead of the end " [\#5932](https://github.com/apache/arrow-rs/pull/5932) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Implement like/ilike etc for StringViewArray [\#5931](https://github.com/apache/arrow-rs/pull/5931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- docs: Fix broken links of object\_store\_opendal README [\#5929](https://github.com/apache/arrow-rs/pull/5929) ([Xuanwo](https://github.com/Xuanwo))
- Expose `IntervalMonthDayNano` and `IntervalDayTime` and update docs [\#5928](https://github.com/apache/arrow-rs/pull/5928) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update proc-macro2 requirement from =1.0.85 to =1.0.86 [\#5927](https://github.com/apache/arrow-rs/pull/5927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- docs: Add object\_store\_opendal as related projects [\#5926](https://github.com/apache/arrow-rs/pull/5926) ([Xuanwo](https://github.com/Xuanwo))
- Add eq benchmark for StringArray/StringViewArray [\#5924](https://github.com/apache/arrow-rs/pull/5924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Implement arrow-row encoding/decoding for view types [\#5922](https://github.com/apache/arrow-rs/pull/5922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- fix\(ipc\): set correct row count when reading struct arrays with zero fields [\#5918](https://github.com/apache/arrow-rs/pull/5918) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Update zstd-sys requirement from \>=2.0.0, \<2.0.10 to \>=2.0.0, \<2.0.12 [\#5913](https://github.com/apache/arrow-rs/pull/5913) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: prevent potential out-of-range access in FixedSizeListArray [\#5902](https://github.com/apache/arrow-rs/pull/5902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([BubbleCal](https://github.com/BubbleCal))
- Implement compare operations for view types [\#5900](https://github.com/apache/arrow-rs/pull/5900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- minor: use as\_primitive replace downcast\_ref [\#5898](https://github.com/apache/arrow-rs/pull/5898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- fix: Adjust FFI\_ArrowArray offset based on the offset of offset buffer [\#5895](https://github.com/apache/arrow-rs/pull/5895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- implement `new_null_array` for view types [\#5894](https://github.com/apache/arrow-rs/pull/5894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- chore: add view type single column tests [\#5891](https://github.com/apache/arrow-rs/pull/5891) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ariesdevil](https://github.com/ariesdevil))
- Minor: expose timestamp\_tz\_format for csv writing [\#5890](https://github.com/apache/arrow-rs/pull/5890) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tmi](https://github.com/tmi))
- chore: implement parquet error handling for object\_store [\#5889](https://github.com/apache/arrow-rs/pull/5889) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([abhiaagarwal](https://github.com/abhiaagarwal))
- Document when the ParquetRecordBatchReader will re-read metadata [\#5887](https://github.com/apache/arrow-rs/pull/5887) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add simple GC for view array types [\#5885](https://github.com/apache/arrow-rs/pull/5885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Update for new clippy rules [\#5881](https://github.com/apache/arrow-rs/pull/5881) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- clean up ByteView construction [\#5879](https://github.com/apache/arrow-rs/pull/5879) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Avoid copy/allocation when read view types from parquet [\#5877](https://github.com/apache/arrow-rs/pull/5877) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XiangpengHao](https://github.com/XiangpengHao))
- Document parquet ArrowWriter type limitations [\#5875](https://github.com/apache/arrow-rs/pull/5875) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Benchmark for casting view to dict arrays \(and the reverse\) [\#5874](https://github.com/apache/arrow-rs/pull/5874) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Implement Take for Dense UnionArray [\#5873](https://github.com/apache/arrow-rs/pull/5873) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gstvg](https://github.com/gstvg))
- Improve performance of casting `StringView`/`BinaryView` to `DictionaryArray` [\#5872](https://github.com/apache/arrow-rs/pull/5872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Improve performance of casting `DictionaryArray` to `StringViewArray` [\#5871](https://github.com/apache/arrow-rs/pull/5871) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- fix: msrv CI for object\_store [\#5866](https://github.com/apache/arrow-rs/pull/5866) ([korowa](https://github.com/korowa))
- parquet: Fix warning about unused import [\#5865](https://github.com/apache/arrow-rs/pull/5865) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- Preallocate for `FixedSizeList` in `concat` [\#5862](https://github.com/apache/arrow-rs/pull/5862) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([judahrand](https://github.com/judahrand))
- Faster primitive arrays encoding into row format [\#5858](https://github.com/apache/arrow-rs/pull/5858) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([korowa](https://github.com/korowa))
- Added panic message to docs. [\#5857](https://github.com/apache/arrow-rs/pull/5857) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SeeRightThroughMe](https://github.com/SeeRightThroughMe))
- feat: call try\_merge recursively for list field  [\#5852](https://github.com/apache/arrow-rs/pull/5852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mnpw](https://github.com/mnpw))
- Minor: refine row selection example more [\#5850](https://github.com/apache/arrow-rs/pull/5850) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Make RowSelection's from\_consecutive\_ranges public [\#5848](https://github.com/apache/arrow-rs/pull/5848) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([advancedxy](https://github.com/advancedxy))
- Add exposing fields from parquet row [\#5842](https://github.com/apache/arrow-rs/pull/5842) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SHaaD94](https://github.com/SHaaD94))
- Derive `Copy` for `TimeUnit` and `IntervalUnit` [\#5840](https://github.com/apache/arrow-rs/pull/5840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- feat: support reading OPTIONAL column in parquet\_derive [\#5717](https://github.com/apache/arrow-rs/pull/5717) ([double-free](https://github.com/double-free))
- Add the ability for Maps to cast to another case where the field names are different [\#5703](https://github.com/apache/arrow-rs/pull/5703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HawaiianSpork](https://github.com/HawaiianSpork))
## [52.0.0](https://github.com/apache/arrow-rs/tree/52.0.0) (2024-06-03)

[Full Changelog](https://github.com/apache/arrow-rs/compare/51.0.0...52.0.0)

**Breaking changes:**

- chore: Make binary\_mut kernel accept different type for second arg [\#5833](https://github.com/apache/arrow-rs/pull/5833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix\(flightsql\): remove Any encoding of `DoPutPreparedStatementResult` [\#5817](https://github.com/apache/arrow-rs/pull/5817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([erratic-pattern](https://github.com/erratic-pattern))
- Encode UUID as FixedLenByteArray in parquet\_derive [\#5773](https://github.com/apache/arrow-rs/pull/5773) ([conradludgate](https://github.com/conradludgate))
- Structured interval types for `IntervalMonthDayNano` or `IntervalDayTime`  \(\#3125\) \(\#5654\) [\#5769](https://github.com/apache/arrow-rs/pull/5769) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fallible stream for arrow-flight do\_exchange call \(\#3462\) [\#5698](https://github.com/apache/arrow-rs/pull/5698) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([opensourcegeek](https://github.com/opensourcegeek))
- Update object\_store dependency in arrow to `0.10.0` [\#5675](https://github.com/apache/arrow-rs/pull/5675) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove deprecated JSON writer [\#5651](https://github.com/apache/arrow-rs/pull/5651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Change `UnionArray` constructors [\#5623](https://github.com/apache/arrow-rs/pull/5623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mbrobbel](https://github.com/mbrobbel))
- Update py03 from 0.20 to 0.21 [\#5566](https://github.com/apache/arrow-rs/pull/5566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Optionally require alignment when reading IPC, respect alignment when writing [\#5554](https://github.com/apache/arrow-rs/pull/5554) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([hzuo](https://github.com/hzuo))

**Implemented enhancements:**

- Serialize `Binary` and `LargeBinary` as HEX with JSON Writer [\#5783](https://github.com/apache/arrow-rs/issues/5783) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Some optimizations in arrow\_buffer::util::bit\_util do more harm than good [\#5771](https://github.com/apache/arrow-rs/issues/5771) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support skipping comments in CSV files [\#5758](https://github.com/apache/arrow-rs/issues/5758) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  `parquet-derive` should be included in repository README. [\#5751](https://github.com/apache/arrow-rs/issues/5751)
- proposal: Make AsyncArrowWriter accepts AsyncFileWriter trait instead [\#5738](https://github.com/apache/arrow-rs/issues/5738) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nested nullable fields do not get treated as nullable in data\_gen [\#5712](https://github.com/apache/arrow-rs/issues/5712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Optionally support flexible column lengths [\#5678](https://github.com/apache/arrow-rs/issues/5678) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow Flight SQL example server: do\_handshake should include auth header [\#5665](https://github.com/apache/arrow-rs/issues/5665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add support for the "r+" datatype in the C Data interface / `RunArray` [\#5631](https://github.com/apache/arrow-rs/issues/5631) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Serialize `FixedSizeBinary` as HEX with JSON Writer [\#5620](https://github.com/apache/arrow-rs/issues/5620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cleanup UnionArray Constructors [\#5613](https://github.com/apache/arrow-rs/issues/5613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Zero Copy Support [\#5593](https://github.com/apache/arrow-rs/issues/5593)
- ObjectStore bulk delete [\#5591](https://github.com/apache/arrow-rs/issues/5591)
- Retry on Broken Connection [\#5589](https://github.com/apache/arrow-rs/issues/5589)
- `StreamReader` is not zero-copy [\#5584](https://github.com/apache/arrow-rs/issues/5584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Create `ArrowReaderMetadata` from externalized metadata [\#5582](https://github.com/apache/arrow-rs/issues/5582) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `filter` in `filter_leaves` API propagate error [\#5574](https://github.com/apache/arrow-rs/issues/5574) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `List` in `compare_op` [\#5572](https://github.com/apache/arrow-rs/issues/5572)
- Make FixedSizedList Json serializable [\#5568](https://github.com/apache/arrow-rs/issues/5568) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-ord: Support sortting StructArray [\#5559](https://github.com/apache/arrow-rs/issues/5559) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add scientific notation decimal parsing in `parse_decimal` [\#5549](https://github.com/apache/arrow-rs/issues/5549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `take` kernel support for `StringViewArray` and `BinaryViewArray` [\#5511](https://github.com/apache/arrow-rs/issues/5511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `filter` kernel support for `StringViewArray` and `BinaryViewArray` [\#5510](https://github.com/apache/arrow-rs/issues/5510) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Display support for `StringViewArray` and `BinaryViewArray` [\#5509](https://github.com/apache/arrow-rs/issues/5509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Arrow Flight format support for `StringViewArray` and `BinaryViewArray` [\#5507](https://github.com/apache/arrow-rs/issues/5507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
-  IPC format support for `StringViewArray` and `BinaryViewArray` [\#5506](https://github.com/apache/arrow-rs/issues/5506) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- List Row Encoding Sorts Incorrectly [\#5807](https://github.com/apache/arrow-rs/issues/5807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Schema Root Message Name Ignored by parquet-fromcsv [\#5804](https://github.com/apache/arrow-rs/issues/5804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Compute data buffer length by using start and end values in offset buffer [\#5756](https://github.com/apache/arrow-rs/issues/5756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: ByteArrayEncoder allocates large unused FallbackEncoder for Parquet 2 [\#5755](https://github.com/apache/arrow-rs/issues/5755) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The CI pipeline `Archery test With other arrow` is broken [\#5742](https://github.com/apache/arrow-rs/issues/5742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unable to parse scientific notation string to decimal when scale is 0 [\#5739](https://github.com/apache/arrow-rs/issues/5739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Stateless prepared statements wrap `DoPutPreparedStatementResult` with `Any` which is differs from Go implementation [\#5731](https://github.com/apache/arrow-rs/issues/5731) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- "Rustdocs are clean \(amd64, nightly\)" CI check is failing [\#5725](https://github.com/apache/arrow-rs/issues/5725) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- "Archery test With other arrows" integration tests are failing  [\#5719](https://github.com/apache/arrow-rs/issues/5719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet\_derive: invalid examples/documentation [\#5687](https://github.com/apache/arrow-rs/issues/5687)
- Arrow FLight SQL: invalid location in get\_flight\_info\_prepared\_statement [\#5669](https://github.com/apache/arrow-rs/issues/5669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Rust Interval definition incorrect [\#5654](https://github.com/apache/arrow-rs/issues/5654) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DECIMAL regex in csv reader does not accept positive exponent specifier [\#5648](https://github.com/apache/arrow-rs/issues/5648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- panic when casting `ListArray` to `FixedSizeList` [\#5642](https://github.com/apache/arrow-rs/issues/5642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FixedSizeListArray::try\_new Errors on Entirely Null Array With Size 0 [\#5614](https://github.com/apache/arrow-rs/issues/5614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `parquet / Build wasm32 (pull_request)` CI check failing on main [\#5565](https://github.com/apache/arrow-rs/issues/5565) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Documentation fix: example in parquet/src/column/mod.rs is incorrect [\#5560](https://github.com/apache/arrow-rs/issues/5560) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC code writes data with insufficient alignment [\#5553](https://github.com/apache/arrow-rs/issues/5553) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Cannot access example Flight SQL Server from dbeaver [\#5540](https://github.com/apache/arrow-rs/issues/5540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- parquet: "not yet implemented" error when codec is actually implemented but disabled [\#5520](https://github.com/apache/arrow-rs/issues/5520) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Minor: Improve arrow\_cast documentation [\#5825](https://github.com/apache/arrow-rs/pull/5825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Improve `ArrowReaderBuilder::with_row_selection` docs [\#5824](https://github.com/apache/arrow-rs/pull/5824) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Add examples for ColumnPath::from [\#5813](https://github.com/apache/arrow-rs/pull/5813) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Minor: Clarify docs on `EnabledStatistics` [\#5812](https://github.com/apache/arrow-rs/pull/5812) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add parquet-derive to repository README [\#5795](https://github.com/apache/arrow-rs/pull/5795) ([konjac](https://github.com/konjac))
- Refine ParquetRecordBatchReaderBuilder docs [\#5774](https://github.com/apache/arrow-rs/pull/5774) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- docs: add sizing explanation to bloom filter docs in parquet [\#5705](https://github.com/apache/arrow-rs/pull/5705) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([hiltontj](https://github.com/hiltontj))

**Closed issues:**

- `binary_mut` kernel requires both args to be the same type \(which is inconsistent with `binary`\) [\#5818](https://github.com/apache/arrow-rs/issues/5818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when displaying debug the results via log::info in the browser. [\#5599](https://github.com/apache/arrow-rs/issues/5599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- feat: impl \*Assign ops for types in arrow-buffer [\#5832](https://github.com/apache/arrow-rs/pull/5832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Relax zstd-sys Version Pin [\#5829](https://github.com/apache/arrow-rs/pull/5829) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([waynexia](https://github.com/waynexia))
- Minor: Document timestamp with/without cast behavior [\#5826](https://github.com/apache/arrow-rs/pull/5826) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: invalid examples/documentation in parquet\_derive doc [\#5823](https://github.com/apache/arrow-rs/pull/5823) ([Weijun-H](https://github.com/Weijun-H))
- Check length of `FIXED_LEN_BYTE_ARRAY` for `uuid` logical parquet type [\#5821](https://github.com/apache/arrow-rs/pull/5821) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mbrobbel](https://github.com/mbrobbel))
- Allow overriding the inferred parquet schema root [\#5814](https://github.com/apache/arrow-rs/pull/5814) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Revisit List Row Encoding \(\#5807\) [\#5811](https://github.com/apache/arrow-rs/pull/5811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.83 to =1.0.84 [\#5805](https://github.com/apache/arrow-rs/pull/5805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix typo continuation maker -\> marker [\#5802](https://github.com/apache/arrow-rs/pull/5802) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([djanderson](https://github.com/djanderson))
- fix: serialization of decimal [\#5801](https://github.com/apache/arrow-rs/pull/5801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Allow constructing ByteViewArray from existing blocks [\#5796](https://github.com/apache/arrow-rs/pull/5796) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Push SortOptions into DynComparator Allowing Nested Comparisons \(\#5426\) [\#5792](https://github.com/apache/arrow-rs/pull/5792) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix incorrect URL to Parquet CPP types.h [\#5790](https://github.com/apache/arrow-rs/pull/5790) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.82 to =1.0.83 [\#5789](https://github.com/apache/arrow-rs/pull/5789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update prost-build requirement from =0.12.4 to =0.12.6 [\#5788](https://github.com/apache/arrow-rs/pull/5788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Refine parquet documentation on types and metadata [\#5786](https://github.com/apache/arrow-rs/pull/5786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- feat\(arrow-json\): encode `Binary` and `LargeBinary` types as hex when writing JSON [\#5785](https://github.com/apache/arrow-rs/pull/5785) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- fix broken link to ballista crate in README.md [\#5784](https://github.com/apache/arrow-rs/pull/5784) ([navicore](https://github.com/navicore))
- feat\(arrow-csv\): support encoding of binary in CSV writer [\#5782](https://github.com/apache/arrow-rs/pull/5782) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- Fix documentation for parquet `parse_metadata`, `decode_metadata` and `decode_footer` [\#5781](https://github.com/apache/arrow-rs/pull/5781) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Support casting a `FixedSizedList<T>[1]` to `T` [\#5779](https://github.com/apache/arrow-rs/pull/5779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sadboy](https://github.com/sadboy))
- \[parquet\] Set the default size of BitWriter in DeltaBitPackEncoder to 1MB [\#5776](https://github.com/apache/arrow-rs/pull/5776) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- Remove harmful table lookup optimization for bitmap operations [\#5772](https://github.com/apache/arrow-rs/pull/5772) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- Remove deprecated comparison kernels \(\#4733\) [\#5768](https://github.com/apache/arrow-rs/pull/5768) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add environment variable definitions to run the nanoarrow integration tests [\#5764](https://github.com/apache/arrow-rs/pull/5764) ([paleolimbot](https://github.com/paleolimbot))
- Downgrade to Rust 1.77 in integration pipeline to fix CI \(\#5719\) [\#5761](https://github.com/apache/arrow-rs/pull/5761) ([tustvold](https://github.com/tustvold))
- Expose boolean builder contents [\#5760](https://github.com/apache/arrow-rs/pull/5760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- Allow specifying comment character for CSV reader [\#5759](https://github.com/apache/arrow-rs/pull/5759) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bbannier](https://github.com/bbannier))
- Expose the null buffer of every builder that has one [\#5754](https://github.com/apache/arrow-rs/pull/5754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- feat: Make AsyncArrowWriter accepts AsyncFileWriter [\#5753](https://github.com/apache/arrow-rs/pull/5753) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Xuanwo](https://github.com/Xuanwo))
- Improve repository readme [\#5752](https://github.com/apache/arrow-rs/pull/5752) ([alamb](https://github.com/alamb))
- Document object store release cadence [\#5750](https://github.com/apache/arrow-rs/pull/5750) ([alamb](https://github.com/alamb))
- Compute data buffer length by using start and end values in offset buffer [\#5741](https://github.com/apache/arrow-rs/pull/5741) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix: parse string of scientific notation to decimal when the scale is 0 [\#5740](https://github.com/apache/arrow-rs/pull/5740) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Minor: avoid \(likely unreachable\) panic in FlightClient [\#5734](https://github.com/apache/arrow-rs/pull/5734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Update proc-macro2 requirement from =1.0.81 to =1.0.82 [\#5732](https://github.com/apache/arrow-rs/pull/5732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Improve error message for timestamp queries outside supported range [\#5730](https://github.com/apache/arrow-rs/pull/5730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Abdi-29](https://github.com/Abdi-29))
- Refactor to share code between do\_put and do\_exchange calls [\#5728](https://github.com/apache/arrow-rs/pull/5728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([opensourcegeek](https://github.com/opensourcegeek))
- Update brotli requirement from 5.0 to 6.0 [\#5726](https://github.com/apache/arrow-rs/pull/5726) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix `GenericListBuilder` test typo [\#5724](https://github.com/apache/arrow-rs/pull/5724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Deprecate NullBuilder capacity, as it behaves in a surprising way [\#5721](https://github.com/apache/arrow-rs/pull/5721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HadrienG2](https://github.com/HadrienG2))
- Fix nested nullability when randomly generating arrays [\#5713](https://github.com/apache/arrow-rs/pull/5713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- Fix up clippy for Rust 1.78 [\#5710](https://github.com/apache/arrow-rs/pull/5710) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Support casting `StringView`/`BinaryView` --\> `StringArray`/`BinaryArray`. [\#5704](https://github.com/apache/arrow-rs/pull/5704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Fix documentation around handling of nulls in cmp kernels [\#5697](https://github.com/apache/arrow-rs/pull/5697) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Support casting `StringArray`/`BinaryArray` --\> `StringView` / `BinaryView` [\#5686](https://github.com/apache/arrow-rs/pull/5686) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Add support for flexible column lengths [\#5679](https://github.com/apache/arrow-rs/pull/5679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Posnet](https://github.com/Posnet))
- Move ffi stream and utils from arrow to arrow-array [\#5670](https://github.com/apache/arrow-rs/pull/5670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Arrow Flight SQL example JDBC driver incompatibility [\#5666](https://github.com/apache/arrow-rs/pull/5666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([istvan-fodor](https://github.com/istvan-fodor))
- Add `ListView` & `LargeListView` basic construction and validation [\#5664](https://github.com/apache/arrow-rs/pull/5664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Update proc-macro2 requirement from =1.0.80 to =1.0.81 [\#5659](https://github.com/apache/arrow-rs/pull/5659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Modify decimal regex to accept positive exponent specifier [\#5649](https://github.com/apache/arrow-rs/pull/5649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jdcasale](https://github.com/jdcasale))
- feat: JSON encoding of `FixedSizeList` [\#5646](https://github.com/apache/arrow-rs/pull/5646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- Update proc-macro2 requirement from =1.0.79 to =1.0.80 [\#5644](https://github.com/apache/arrow-rs/pull/5644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: panic when casting `ListArray` to `FixedSizeList` [\#5643](https://github.com/apache/arrow-rs/pull/5643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonahgao](https://github.com/jonahgao))
- Add more invalid utf8 parquet reader tests [\#5639](https://github.com/apache/arrow-rs/pull/5639) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Update brotli requirement from 4.0 to 5.0 [\#5637](https://github.com/apache/arrow-rs/pull/5637) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update flatbuffers requirement from 23.1.21 to 24.3.25 [\#5636](https://github.com/apache/arrow-rs/pull/5636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Increase `BinaryViewArray` test coverage [\#5635](https://github.com/apache/arrow-rs/pull/5635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- PrettyPrint support for `StringViewArray` and `BinaryViewArray` [\#5634](https://github.com/apache/arrow-rs/pull/5634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat\(ffi\): add run end encoded arrays [\#5632](https://github.com/apache/arrow-rs/pull/5632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([notfilippo](https://github.com/notfilippo))
- Accept parquet schemas without explicitly required Map keys [\#5630](https://github.com/apache/arrow-rs/pull/5630) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jupiter](https://github.com/jupiter))
- Implement `filter` kernel for byte view arrays. [\#5624](https://github.com/apache/arrow-rs/pull/5624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- feat: encode FixedSizeBinary in JSON as hex string [\#5622](https://github.com/apache/arrow-rs/pull/5622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([hiltontj](https://github.com/hiltontj))
- Update Flight crate README version [\#5621](https://github.com/apache/arrow-rs/pull/5621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([phillipleblanc](https://github.com/phillipleblanc))
- feat: support reading and writing`StringView` and `BinaryView` in parquet \(part 1\) [\#5618](https://github.com/apache/arrow-rs/pull/5618) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Use FixedSizeListArray::new in FixedSizeListBuilder [\#5612](https://github.com/apache/arrow-rs/pull/5612) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- String to decimal conversion written using E/scientific notation [\#5611](https://github.com/apache/arrow-rs/pull/5611) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Nekit2217](https://github.com/Nekit2217))
- Account for Timezone when Casting Timestamp to Date32 [\#5605](https://github.com/apache/arrow-rs/pull/5605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Lordworms](https://github.com/Lordworms))
- Update prost-build requirement from =0.12.3 to =0.12.4 [\#5604](https://github.com/apache/arrow-rs/pull/5604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix panic when displaying dates on 32-bit platforms [\#5603](https://github.com/apache/arrow-rs/pull/5603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ivanceras](https://github.com/ivanceras))
- Implement `take` kernel for byte view array. [\#5602](https://github.com/apache/arrow-rs/pull/5602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Add tests for Arrow Flight support for `StringViewArray` and `BinaryViewArray` [\#5601](https://github.com/apache/arrow-rs/pull/5601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([XiangpengHao](https://github.com/XiangpengHao))
- test: Add a test for RowFilter with nested type [\#5600](https://github.com/apache/arrow-rs/pull/5600) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Minor: Add docs for GenericBinaryBuilder, links to `GenericStringBuilder` [\#5597](https://github.com/apache/arrow-rs/pull/5597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Bump chrono-tz from 0.8 to 0.9 [\#5596](https://github.com/apache/arrow-rs/pull/5596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Update brotli requirement from 3.3 to 4.0 [\#5586](https://github.com/apache/arrow-rs/pull/5586) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `UnionArray::into_parts` [\#5585](https://github.com/apache/arrow-rs/pull/5585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Expose ArrowReaderMetadata::try\_new [\#5583](https://github.com/apache/arrow-rs/pull/5583) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Add `try_filter_leaves` to propagate error from filter closure [\#5575](https://github.com/apache/arrow-rs/pull/5575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- filter for run end array [\#5573](https://github.com/apache/arrow-rs/pull/5573) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fabianmurariu](https://github.com/fabianmurariu))
- Pin zstd-sys to `v2.0.9` in parquet [\#5567](https://github.com/apache/arrow-rs/pull/5567) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Split arrow\_cast::cast::string into it's own submodule [\#5563](https://github.com/apache/arrow-rs/pull/5563) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Correct example code for column \(\#5560\) [\#5561](https://github.com/apache/arrow-rs/pull/5561) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zgershkoff](https://github.com/zgershkoff))
- Split arrow\_cast::cast::dictionary into it's own submodule [\#5555](https://github.com/apache/arrow-rs/pull/5555) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Split arrow\_cast::cast::decimal into it's own submodule [\#5552](https://github.com/apache/arrow-rs/pull/5552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Fix new clippy lints for Rust 1.77 [\#5544](https://github.com/apache/arrow-rs/pull/5544) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: correctly encode ticket [\#5543](https://github.com/apache/arrow-rs/pull/5543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([freddieptf](https://github.com/freddieptf))
- feat: implemented with\_field\(\) for FixedSizeListBuilder [\#5541](https://github.com/apache/arrow-rs/pull/5541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([istvan-fodor](https://github.com/istvan-fodor))
- Split arrow\_cast::cast::list into it's own submodule [\#5537](https://github.com/apache/arrow-rs/pull/5537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Bump black from 22.10.0 to 24.3.0 in /parquet/pytest [\#5535](https://github.com/apache/arrow-rs/pull/5535) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add OffsetBufferBuilder [\#5532](https://github.com/apache/arrow-rs/pull/5532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add IPC StreamDecoder [\#5531](https://github.com/apache/arrow-rs/pull/5531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- IPC format support for StringViewArray and BinaryViewArray [\#5525](https://github.com/apache/arrow-rs/pull/5525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- parquet: Use specific error variant when codec is disabled [\#5521](https://github.com/apache/arrow-rs/pull/5521) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([progval](https://github.com/progval))
- impl `From<ScalarBuffer<T>>` for `Vec<T>` [\#5518](https://github.com/apache/arrow-rs/pull/5518) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
## [51.0.0](https://github.com/apache/arrow-rs/tree/51.0.0) (2024-03-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/50.0.0...51.0.0)

**Breaking changes:**

- Remove internal buffering from AsyncArrowWriter \(\#5484\) [\#5485](https://github.com/apache/arrow-rs/pull/5485) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make ArrayBuilder also Sync [\#5353](https://github.com/apache/arrow-rs/pull/5353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dvic](https://github.com/dvic))
- Raw JSON writer \(~10x faster\) \(\#5314\)  [\#5318](https://github.com/apache/arrow-rs/pull/5318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Prototype Arrow over HTTP in Rust [\#5496](https://github.com/apache/arrow-rs/issues/5496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add DataType::ListView and DataType::LargeListView [\#5492](https://github.com/apache/arrow-rs/issues/5492) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve documentation around handling of dictionary arrays in arrow flight [\#5487](https://github.com/apache/arrow-rs/issues/5487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Better memory limiting in parquet `ArrowWriter`  [\#5484](https://github.com/apache/arrow-rs/issues/5484) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Creating Non-Nullable Lists and Maps within a Struct [\#5482](https://github.com/apache/arrow-rs/issues/5482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DISCUSSION\] Better borrow propagation \(e.g. `RecordBatch::schema()` to return `&SchemaRef` vs `SchemaRef`\) [\#5463](https://github.com/apache/arrow-rs/issues/5463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Build Scalar with ArrayRef [\#5459](https://github.com/apache/arrow-rs/issues/5459)
- AsyncArrowWriter doesn't limit underlying ArrowWriter to respect buffer-size [\#5450](https://github.com/apache/arrow-rs/issues/5450) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Refine `Display` implementation for `FlightError` [\#5438](https://github.com/apache/arrow-rs/issues/5438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Better ergonomics for `FixedSizeList` and `LargeList` [\#5372](https://github.com/apache/arrow-rs/issues/5372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update Flight proto [\#5367](https://github.com/apache/arrow-rs/issues/5367) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support check similar datatype but with different magnitudes [\#5358](https://github.com/apache/arrow-rs/issues/5358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Buffer memory usage for custom allocations is reported as 0 [\#5346](https://github.com/apache/arrow-rs/issues/5346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Can the ArrayBuilder trait be made Sync? [\#5344](https://github.com/apache/arrow-rs/issues/5344) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support cast 'UTF8' to `FixedSizeList` [\#5339](https://github.com/apache/arrow-rs/issues/5339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Creating Non-Nullable Lists with ListBuilder [\#5330](https://github.com/apache/arrow-rs/issues/5330) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `ParquetRecordBatchStreamBuilder::new()` panics instead of erroring out when opening a corrupted file [\#5315](https://github.com/apache/arrow-rs/issues/5315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Raw JSON Writer [\#5314](https://github.com/apache/arrow-rs/issues/5314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for more fused boolean operations [\#5297](https://github.com/apache/arrow-rs/issues/5297) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: Allow disabling embed `ARROW_SCHEMA_META_KEY` added by the `ArrowWriter` [\#5296](https://github.com/apache/arrow-rs/issues/5296) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting strings like '2001-01-01 01:01:01' to Date32 [\#5280](https://github.com/apache/arrow-rs/issues/5280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Temporal Extract/Date Part Kernel [\#5266](https://github.com/apache/arrow-rs/issues/5266) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support for extracting hours/minutes/seconds/etc. from `Time32`/`Time64` type in temporal kernels [\#5261](https://github.com/apache/arrow-rs/issues/5261) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: add method to get both the inner writer and the file metadata when closing SerializedFileWriter [\#5253](https://github.com/apache/arrow-rs/issues/5253) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Release arrow-rs version 50.0.0 [\#5234](https://github.com/apache/arrow-rs/issues/5234)

**Fixed bugs:**

- Empty String Parses as Zero in Unreleased Arrow [\#5504](https://github.com/apache/arrow-rs/issues/5504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unused import in nightly rust [\#5476](https://github.com/apache/arrow-rs/issues/5476) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Error `The data type type List .. has no natural order` when using `arrow::compute::lexsort_to_indices` with list and more than one column [\#5454](https://github.com/apache/arrow-rs/issues/5454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Wrong size assertion in arrow\_buffer::builder::NullBufferBuilder::new\_from\_buffer [\#5445](https://github.com/apache/arrow-rs/issues/5445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Inconsistency between comments and code implementation [\#5430](https://github.com/apache/arrow-rs/issues/5430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- OOB access in `Buffer::from_iter` [\#5412](https://github.com/apache/arrow-rs/issues/5412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast kernel doesn't return null for string to integral cases when overflowing under safe option enabled [\#5397](https://github.com/apache/arrow-rs/issues/5397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make ffi consume variable layout arrays with empty offsets [\#5391](https://github.com/apache/arrow-rs/issues/5391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordBatch conversion from pyarrow loses Schema's metadata [\#5354](https://github.com/apache/arrow-rs/issues/5354) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Debug output of Time32/Time64 arrays with invalid values has confusing nulls [\#5336](https://github.com/apache/arrow-rs/issues/5336) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Removing a column from a `RecordBatch` drops schema metadata [\#5327](https://github.com/apache/arrow-rs/issues/5327) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when read an empty parquet file [\#5304](https://github.com/apache/arrow-rs/issues/5304) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- How to enable statistics for string columns? [\#5270](https://github.com/apache/arrow-rs/issues/5270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `concat::tests::test_string_dictionary_merge failure` fails on Mac /  has different results in different platforms [\#5255](https://github.com/apache/arrow-rs/issues/5255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Minor: Add doc comments to `GenericByteViewArray` [\#5512](https://github.com/apache/arrow-rs/pull/5512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve docs for logical and physical nulls even more [\#5434](https://github.com/apache/arrow-rs/pull/5434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add example of converting RecordBatches to JSON objects [\#5364](https://github.com/apache/arrow-rs/pull/5364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- improve float to string cast by ~20%-40% [\#5401](https://github.com/apache/arrow-rs/pull/5401) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))

**Closed issues:**

- Add `StringViewArray` implementation and layout and basic construction + tests [\#5469](https://github.com/apache/arrow-rs/issues/5469) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `DataType::Utf8View` and `DataType::BinaryView` [\#5468](https://github.com/apache/arrow-rs/issues/5468) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Deprecate array\_to\_json\_array [\#5515](https://github.com/apache/arrow-rs/pull/5515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix integer parsing of empty strings \(\#5504\) [\#5505](https://github.com/apache/arrow-rs/pull/5505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: clarifying comments in struct\_builder.rs \#5494  [\#5499](https://github.com/apache/arrow-rs/pull/5499) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([istvan-fodor](https://github.com/istvan-fodor))
- Update proc-macro2 requirement from =1.0.78 to =1.0.79 [\#5498](https://github.com/apache/arrow-rs/pull/5498) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add DataType::ListView and DataType::LargeListView [\#5493](https://github.com/apache/arrow-rs/pull/5493) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Better document parquet pushdown [\#5491](https://github.com/apache/arrow-rs/pull/5491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix NullBufferBuilder::new\_from\_buffer wrong size assertion [\#5489](https://github.com/apache/arrow-rs/pull/5489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Support dictionary encoding in structures for `FlightDataEncoder`,  add documentation for `arrow_flight::encode::Dictionary` [\#5488](https://github.com/apache/arrow-rs/pull/5488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([thinkharderdev](https://github.com/thinkharderdev))
- Add MapBuilder::with\_values\_field to support non-nullable values \(\#5482\) [\#5483](https://github.com/apache/arrow-rs/pull/5483) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lasantosr](https://github.com/lasantosr))
- feat: initial support string\_view and binary\_view,  supports layout and basic construction + tests [\#5481](https://github.com/apache/arrow-rs/pull/5481) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ariesdevil](https://github.com/ariesdevil))
- Add more comprehensive documentation on testing and benchmarking to CONTRIBUTING.md [\#5478](https://github.com/apache/arrow-rs/pull/5478) ([monkwire](https://github.com/monkwire))
- Remove unused import detected by nightly rust [\#5477](https://github.com/apache/arrow-rs/pull/5477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([XiangpengHao](https://github.com/XiangpengHao))
- Add RecordBatch::schema\_ref [\#5474](https://github.com/apache/arrow-rs/pull/5474) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([monkwire](https://github.com/monkwire))
- Provide access to inner Write for parquet writers [\#5471](https://github.com/apache/arrow-rs/pull/5471) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add DataType::Utf8View and DataType::BinaryView [\#5470](https://github.com/apache/arrow-rs/pull/5470) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([XiangpengHao](https://github.com/XiangpengHao))
- Update base64 requirement from 0.21 to 0.22 [\#5467](https://github.com/apache/arrow-rs/pull/5467) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Minor: Fix formatting typo in `Field::new_list_field` [\#5464](https://github.com/apache/arrow-rs/pull/5464) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix test\_string\_dictionary\_merge \(\#5255\) [\#5461](https://github.com/apache/arrow-rs/pull/5461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Vec::from\_iter in Buffer::from\_iter [\#5460](https://github.com/apache/arrow-rs/pull/5460) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- Document parquet writer memory limiting \(\#5450\) [\#5457](https://github.com/apache/arrow-rs/pull/5457) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Document UnionArray Panics [\#5456](https://github.com/apache/arrow-rs/pull/5456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Kikkon](https://github.com/Kikkon))
- fix: lexsort\_to\_indices unsupported mixed types with list [\#5455](https://github.com/apache/arrow-rs/pull/5455) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Refine `Display` and `Source` implementation for error types [\#5439](https://github.com/apache/arrow-rs/pull/5439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([BugenZhao](https://github.com/BugenZhao))
- Improve debug output of Time32/Time64 arrays [\#5428](https://github.com/apache/arrow-rs/pull/5428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([monkwire](https://github.com/monkwire))
- Miri fix: Rename invalid\_mut to without\_provenance\_mut [\#5418](https://github.com/apache/arrow-rs/pull/5418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Ensure addition/multiplications in when allocating buffers don't overflow [\#5417](https://github.com/apache/arrow-rs/pull/5417) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Update Flight proto: PollFlightInfo & expiration time [\#5413](https://github.com/apache/arrow-rs/pull/5413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Jefffrey](https://github.com/Jefffrey))
- Add tests for serializing lists of dictionary encoded values to json [\#5399](https://github.com/apache/arrow-rs/pull/5399) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Return null for overflow when casting string to integer under safe option enabled [\#5398](https://github.com/apache/arrow-rs/pull/5398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Propagate error instead of panic for `take_bytes` [\#5395](https://github.com/apache/arrow-rs/pull/5395) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve like kernel by ~2% [\#5390](https://github.com/apache/arrow-rs/pull/5390) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Enable running arrow-array and arrow-arith with miri and avoid strict provenance warning [\#5387](https://github.com/apache/arrow-rs/pull/5387) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Update to chrono 0.4.34 [\#5385](https://github.com/apache/arrow-rs/pull/5385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return error instead of panic when reading invalid Parquet metadata [\#5382](https://github.com/apache/arrow-rs/pull/5382) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- Update tonic requirement from 0.10.0 to 0.11.0 [\#5380](https://github.com/apache/arrow-rs/pull/5380) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update tonic-build requirement from =0.10.2 to =0.11.0 [\#5379](https://github.com/apache/arrow-rs/pull/5379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix latest clippy lints [\#5376](https://github.com/apache/arrow-rs/pull/5376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: utility functions for creating `FixedSizeList` and `LargeList` dtypes [\#5373](https://github.com/apache/arrow-rs/pull/5373) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([universalmind303](https://github.com/universalmind303))
- Minor\(docs\): update master to main for DataFusion/Ballista [\#5363](https://github.com/apache/arrow-rs/pull/5363) ([caicancai](https://github.com/caicancai))
- Return an error instead of a panic when reading a corrupted Parquet file with mismatched column counts [\#5362](https://github.com/apache/arrow-rs/pull/5362) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- feat: support casting FixedSizeList with new child type [\#5360](https://github.com/apache/arrow-rs/pull/5360) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Add more debugging info to StructBuilder validate\_content [\#5357](https://github.com/apache/arrow-rs/pull/5357) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- pyarrow: Preserve RecordBatch's schema metadata [\#5355](https://github.com/apache/arrow-rs/pull/5355) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([atwam](https://github.com/atwam))
- Mark Encoding::BIT\_PACKED as deprecated and document its compatibility issues [\#5348](https://github.com/apache/arrow-rs/pull/5348) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- Track the size of custom allocations for use via Array::get\_buffer\_memory\_size [\#5347](https://github.com/apache/arrow-rs/pull/5347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- fix: Return an error on type mismatch rather than panic \(\#4995\) [\#5341](https://github.com/apache/arrow-rs/pull/5341) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([carols10cents](https://github.com/carols10cents))
- Minor: support cast values to fixedsizelist [\#5340](https://github.com/apache/arrow-rs/pull/5340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Enhance Time32/Time64 support in date\_part [\#5337](https://github.com/apache/arrow-rs/pull/5337) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- feat: add `take_record_batch`. [\#5333](https://github.com/apache/arrow-rs/pull/5333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Add ListBuilder::with\_field to support non nullable list fields \(\#5330\) [\#5331](https://github.com/apache/arrow-rs/pull/5331) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't omit schema metadata when removing column [\#5328](https://github.com/apache/arrow-rs/pull/5328) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kylebarron](https://github.com/kylebarron))
- Update proc-macro2 requirement from =1.0.76 to =1.0.78 [\#5324](https://github.com/apache/arrow-rs/pull/5324) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Enhance Date64 type documentation [\#5323](https://github.com/apache/arrow-rs/pull/5323) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- fix panic when decode a group with no child [\#5322](https://github.com/apache/arrow-rs/pull/5322) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Liyixin95](https://github.com/Liyixin95))
- Minor/Doc Expand FlightSqlServiceClient::handshake doc [\#5321](https://github.com/apache/arrow-rs/pull/5321) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([devinjdangelo](https://github.com/devinjdangelo))
- Refactor temporal extract date part kernels [\#5319](https://github.com/apache/arrow-rs/pull/5319) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Add JSON writer benchmarks \(\#5314\) [\#5317](https://github.com/apache/arrow-rs/pull/5317) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Bump actions/cache from 3 to 4 [\#5308](https://github.com/apache/arrow-rs/pull/5308) ([dependabot[bot]](https://github.com/apps/dependabot))
- Avro block decompression [\#5306](https://github.com/apache/arrow-rs/pull/5306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Result into error in case of endianness mismatches [\#5301](https://github.com/apache/arrow-rs/pull/5301) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pangiole](https://github.com/pangiole))
- parquet: Add ArrowWriterOptions to skip embedding the arrow metadata [\#5299](https://github.com/apache/arrow-rs/pull/5299) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([evenyag](https://github.com/evenyag))
- Add support for more fused boolean operations [\#5298](https://github.com/apache/arrow-rs/pull/5298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([RTEnzyme](https://github.com/RTEnzyme))
- Support Parquet  Byte Stream Split Encoding [\#5293](https://github.com/apache/arrow-rs/pull/5293) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mwlon](https://github.com/mwlon))
- Extend string parsing support for Date32 [\#5282](https://github.com/apache/arrow-rs/pull/5282) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Bring some methods over from ArrowWriter to the async version [\#5251](https://github.com/apache/arrow-rs/pull/5251) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
## [50.0.0](https://github.com/apache/arrow-rs/tree/50.0.0) (2024-01-08)

[Full Changelog](https://github.com/apache/arrow-rs/compare/49.0.0...50.0.0)

**Breaking changes:**

- Make regexp\_match take scalar pattern and flag [\#5245](https://github.com/apache/arrow-rs/pull/5245) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use Vec in ColumnReader \(\#5177\) [\#5193](https://github.com/apache/arrow-rs/pull/5193) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove SIMD Feature [\#5184](https://github.com/apache/arrow-rs/pull/5184) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Total Ordering for Aggregates and Refactor for Better Auto-Vectorization [\#5100](https://github.com/apache/arrow-rs/pull/5100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Allow the `zip` compute function to operator on `Scalar` values via `Datum` [\#5086](https://github.com/apache/arrow-rs/pull/5086) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Nathan-Fenner](https://github.com/Nathan-Fenner))
- Improve C Data Interface and Add Integration Testing Entrypoints [\#5080](https://github.com/apache/arrow-rs/pull/5080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pitrou](https://github.com/pitrou))
- Parquet: read/write f16 for Arrow [\#5003](https://github.com/apache/arrow-rs/pull/5003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))

**Implemented enhancements:**

- Support get offsets or blocks info from arrow file.  [\#5252](https://github.com/apache/arrow-rs/issues/5252) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make regexp\_match take scalar pattern and flag [\#5246](https://github.com/apache/arrow-rs/issues/5246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cannot access pen state website on arrow-row [\#5238](https://github.com/apache/arrow-rs/issues/5238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordBatch with\_schema's error message is hard to read [\#5227](https://github.com/apache/arrow-rs/issues/5227) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support cast between StructArray. [\#5219](https://github.com/apache/arrow-rs/issues/5219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove nightly-only simd feature and related code in ArrowNumericType [\#5185](https://github.com/apache/arrow-rs/issues/5185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use Vec instead of Slice in ColumnReader [\#5177](https://github.com/apache/arrow-rs/issues/5177) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Request to Memmap Arrow IPC files on disk [\#5153](https://github.com/apache/arrow-rs/issues/5153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- GenericColumnReader::read\_records Yields Truncated Records [\#5150](https://github.com/apache/arrow-rs/issues/5150) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nested Schema Projection [\#5148](https://github.com/apache/arrow-rs/issues/5148) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support specifying `quote` and `escape` in Csv `WriterBuilder` [\#5146](https://github.com/apache/arrow-rs/issues/5146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting of Float16 with other numeric types [\#5138](https://github.com/apache/arrow-rs/issues/5138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: read parquet metadata with page index in async and with size hints [\#5129](https://github.com/apache/arrow-rs/issues/5129) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Cast from floating/timestamp to timestamp/floating [\#5122](https://github.com/apache/arrow-rs/issues/5122) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Casting List To/From LargeList in Cast Kernel [\#5113](https://github.com/apache/arrow-rs/issues/5113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose a path for converting `bytes::Bytes` into `arrow_buffer::Buffer` without copy [\#5104](https://github.com/apache/arrow-rs/issues/5104) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- API inconsistency of ListBuilder make it hard to use as nested builder [\#5098](https://github.com/apache/arrow-rs/issues/5098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: don't truncate min/max statistics for float16 and decimal when writing file [\#5075](https://github.com/apache/arrow-rs/issues/5075) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: derive boundary order when writing columns [\#5074](https://github.com/apache/arrow-rs/issues/5074) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support new Arrow PyCapsule Interface for Python FFI [\#5067](https://github.com/apache/arrow-rs/issues/5067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `48.0.1 ` arrow patch release [\#5050](https://github.com/apache/arrow-rs/issues/5050) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Binary columns do not receive truncated statistics [\#5037](https://github.com/apache/arrow-rs/issues/5037) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Re-evaluate Explicit SIMD Aggregations [\#5032](https://github.com/apache/arrow-rs/issues/5032) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Min/Max Kernels Should Use Total Ordering [\#5031](https://github.com/apache/arrow-rs/issues/5031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow `zip` compute kernel to take `Scalar` / `Datum`  [\#5011](https://github.com/apache/arrow-rs/issues/5011) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Float16/Half-float logical type to Parquet [\#4986](https://github.com/apache/arrow-rs/issues/4986) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- feat: cast \(Large\)List to FixedSizeList [\#5081](https://github.com/apache/arrow-rs/pull/5081) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Update Parquet Encoding Documentation [\#5051](https://github.com/apache/arrow-rs/issues/5051) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- json schema inference can't handle null field turned into object field in subsequent rows [\#5215](https://github.com/apache/arrow-rs/issues/5215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Invalid trailing content after `Z` in timezone is ignored [\#5182](https://github.com/apache/arrow-rs/issues/5182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Take panics on a fixed size list array when given null indices [\#5169](https://github.com/apache/arrow-rs/issues/5169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- EnabledStatistics::Page  does not take effect on ByteArrayEncoder [\#5162](https://github.com/apache/arrow-rs/issues/5162) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: ColumnOrder not being written when writing parquet files [\#5152](https://github.com/apache/arrow-rs/issues/5152) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet: Interval columns shouldn't write min/max stats [\#5145](https://github.com/apache/arrow-rs/issues/5145) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
-  cast `Utf8` to decimal failure [\#5127](https://github.com/apache/arrow-rs/issues/5127) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- coerce\_primitive not honored when decoding from serde object [\#5095](https://github.com/apache/arrow-rs/issues/5095) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unsound MutableArrayData Constructor [\#5091](https://github.com/apache/arrow-rs/issues/5091) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RowGroupReader.get\_row\_iter\(\) fails with Path ColumnPath not found [\#5064](https://github.com/apache/arrow-rs/issues/5064) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- cast format 'yyyymmdd'  to Date32 give a error [\#5044](https://github.com/apache/arrow-rs/issues/5044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Performance improvements:**

- ArrowArrayStreamReader imports FFI\_ArrowSchema on each iteration [\#5103](https://github.com/apache/arrow-rs/issues/5103) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Working example of list\_flights with ObjectStore [\#5116](https://github.com/apache/arrow-rs/issues/5116)
- \(object\_store\) Error broken pipe on S3 multipart upload [\#5106](https://github.com/apache/arrow-rs/issues/5106)

**Merged pull requests:**

- Update parquet object\_store dependency to 0.9.0 [\#5290](https://github.com/apache/arrow-rs/pull/5290) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.75 to =1.0.76 [\#5289](https://github.com/apache/arrow-rs/pull/5289) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Enable JS tests again [\#5287](https://github.com/apache/arrow-rs/pull/5287) ([domoritz](https://github.com/domoritz))
- Update proc-macro2 requirement from =1.0.74 to =1.0.75 [\#5279](https://github.com/apache/arrow-rs/pull/5279) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.73 to =1.0.74 [\#5271](https://github.com/apache/arrow-rs/pull/5271) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.71 to =1.0.73 [\#5265](https://github.com/apache/arrow-rs/pull/5265) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update docs for datatypes [\#5260](https://github.com/apache/arrow-rs/pull/5260) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Don't suppress errors in ArrowArrayStreamReader [\#5256](https://github.com/apache/arrow-rs/pull/5256) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add IPC FileDecoder [\#5249](https://github.com/apache/arrow-rs/pull/5249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- optimize the next function of ArrowArrayStreamReader [\#5248](https://github.com/apache/arrow-rs/pull/5248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- ci: Fail Miri CI on first failure [\#5243](https://github.com/apache/arrow-rs/pull/5243) ([Jefffrey](https://github.com/Jefffrey))
- Remove 'unwrap' from Result [\#5241](https://github.com/apache/arrow-rs/pull/5241) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Update arrow-row docs URL [\#5239](https://github.com/apache/arrow-rs/pull/5239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([thomas-k-cameron](https://github.com/thomas-k-cameron))
- Improve regexp kernels performance by avoiding cloning Regex [\#5235](https://github.com/apache/arrow-rs/pull/5235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.70 to =1.0.71 [\#5231](https://github.com/apache/arrow-rs/pull/5231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Minor: Improve comments and errors for ArrowPredicate [\#5230](https://github.com/apache/arrow-rs/pull/5230) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Bump actions/upload-pages-artifact from 2 to 3 [\#5229](https://github.com/apache/arrow-rs/pull/5229) ([dependabot[bot]](https://github.com/apps/dependabot))
- make with\_schema's error more readable [\#5228](https://github.com/apache/arrow-rs/pull/5228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([shuoli84](https://github.com/shuoli84))
- Use `try_new` when casting between structs to propagate error [\#5226](https://github.com/apache/arrow-rs/pull/5226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat\(cast\): support cast between struct [\#5221](https://github.com/apache/arrow-rs/pull/5221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([my-vegetable-has-exploded](https://github.com/my-vegetable-has-exploded))
- Add `entries` to `MapBuilder` to return both key and value array builders [\#5218](https://github.com/apache/arrow-rs/pull/5218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix\(json\): fix inferring object after field was null [\#5216](https://github.com/apache/arrow-rs/pull/5216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kskalski](https://github.com/kskalski))
- Support MapBuilder in make\_builder [\#5210](https://github.com/apache/arrow-rs/pull/5210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- impl `From<OffsetBuffer<T>>` for `ScalarBuffer<T>` [\#5203](https://github.com/apache/arrow-rs/pull/5203) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- impl `From<BufferBuilder<T>>` for `Buffer` [\#5202](https://github.com/apache/arrow-rs/pull/5202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- impl `From<BufferBuilder<T>>` for `ScalarBuffer<T>` [\#5201](https://github.com/apache/arrow-rs/pull/5201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- feat: Support  quote and escape in Csv WriterBuilder [\#5196](https://github.com/apache/arrow-rs/pull/5196) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([my-vegetable-has-exploded](https://github.com/my-vegetable-has-exploded))
- chore: simplify cast\_string\_to\_interval [\#5195](https://github.com/apache/arrow-rs/pull/5195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Clarify interval comparison behavior with documentation and tests [\#5192](https://github.com/apache/arrow-rs/pull/5192) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `BooleanArray::into_parts` method [\#5191](https://github.com/apache/arrow-rs/pull/5191) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Fix deprecated note for `Buffer::from_raw_parts` [\#5190](https://github.com/apache/arrow-rs/pull/5190) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Fix: Ensure Timestamp Parsing Rejects Characters After 'Z [\#5189](https://github.com/apache/arrow-rs/pull/5189) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([razeghi71](https://github.com/razeghi71))
- Simplify parquet statistics generation [\#5183](https://github.com/apache/arrow-rs/pull/5183) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Parquet: Ensure page statistics are written only when conifgured from the Arrow Writer [\#5181](https://github.com/apache/arrow-rs/pull/5181) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- Blockwise IO in IPC FileReader \(\#5153\) [\#5179](https://github.com/apache/arrow-rs/pull/5179) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Replace ScalarBuffer in Parquet with Vec \(\#1849\) \(\#5177\) [\#5178](https://github.com/apache/arrow-rs/pull/5178) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Bump actions/setup-python from 4 to 5 [\#5175](https://github.com/apache/arrow-rs/pull/5175) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add `LargeListBuilder` to `make_builder` [\#5171](https://github.com/apache/arrow-rs/pull/5171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix: ensure take\_fixed\_size\_list can handle null indices [\#5170](https://github.com/apache/arrow-rs/pull/5170) ([westonpace](https://github.com/westonpace))
- Removing redundant `as casts` in parquet [\#5168](https://github.com/apache/arrow-rs/pull/5168) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Bump actions/labeler from 4.3.0 to 5.0.0 [\#5167](https://github.com/apache/arrow-rs/pull/5167) ([dependabot[bot]](https://github.com/apps/dependabot))
- improve: make RunArray displayable [\#5166](https://github.com/apache/arrow-rs/pull/5166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yukkit](https://github.com/yukkit))
- ci: Add cargo audit CI action [\#5160](https://github.com/apache/arrow-rs/pull/5160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Parquet: write column\_orders in FileMetaData [\#5158](https://github.com/apache/arrow-rs/pull/5158) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Adding `is_null` datatype shortcut method [\#5157](https://github.com/apache/arrow-rs/pull/5157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Parquet: don't truncate f16/decimal min/max stats [\#5154](https://github.com/apache/arrow-rs/pull/5154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Support nested schema projection \(\#5148\) [\#5149](https://github.com/apache/arrow-rs/pull/5149) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Parquet: omit min/max for interval columns when writing stats [\#5147](https://github.com/apache/arrow-rs/pull/5147) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Deprecate Fields::remove and Schema::remove [\#5144](https://github.com/apache/arrow-rs/pull/5144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support casting of Float16 with other numeric types [\#5139](https://github.com/apache/arrow-rs/pull/5139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Parquet: Make `MetadataLoader` public [\#5137](https://github.com/apache/arrow-rs/pull/5137) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- Add FileReaderBuilder for arrow-ipc to allow reading large no. of column files [\#5136](https://github.com/apache/arrow-rs/pull/5136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Parquet: clear metadata and project fields of ParquetRecordBatchStream::schema [\#5135](https://github.com/apache/arrow-rs/pull/5135) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- JSON: write struct array nulls as null [\#5133](https://github.com/apache/arrow-rs/pull/5133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Update proc-macro2 requirement from =1.0.69 to =1.0.70 [\#5131](https://github.com/apache/arrow-rs/pull/5131) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix negative decimal string [\#5128](https://github.com/apache/arrow-rs/pull/5128) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cleanup list casting and support nested lists \(\#5113\) [\#5124](https://github.com/apache/arrow-rs/pull/5124) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cast from numeric/timestamp to timestamp/numeric [\#5123](https://github.com/apache/arrow-rs/pull/5123) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve cast docs [\#5114](https://github.com/apache/arrow-rs/pull/5114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.12.2 to =0.12.3 [\#5112](https://github.com/apache/arrow-rs/pull/5112) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Parquet: derive boundary order when writing [\#5110](https://github.com/apache/arrow-rs/pull/5110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Implementing `ArrayBuilder` for `Box<dyn ArrayBuilder>` [\#5109](https://github.com/apache/arrow-rs/pull/5109) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix 'ColumnPath not found' error reading Parquet files with nested REPEATED fields [\#5102](https://github.com/apache/arrow-rs/pull/5102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- fix: coerce\_primitive for serde decoded data [\#5101](https://github.com/apache/arrow-rs/pull/5101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fansehep](https://github.com/fansehep))
- Extend aggregation benchmarks [\#5096](https://github.com/apache/arrow-rs/pull/5096) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Expand parquet crate overview doc [\#5093](https://github.com/apache/arrow-rs/pull/5093) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mmaitre314](https://github.com/mmaitre314))
- Ensure arrays passed to MutableArrayData have same type \(\#5091\) [\#5092](https://github.com/apache/arrow-rs/pull/5092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.12.1 to =0.12.2 [\#5088](https://github.com/apache/arrow-rs/pull/5088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add FFI from\_raw [\#5082](https://github.com/apache/arrow-rs/pull/5082) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- \[fix \#5044\] Support converting 'yyyymmdd' format to date [\#5078](https://github.com/apache/arrow-rs/pull/5078) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Tangruilin](https://github.com/Tangruilin))
- Enable truncation of binary statistics columns [\#5076](https://github.com/apache/arrow-rs/pull/5076) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([emcake](https://github.com/emcake))
## [49.0.0](https://github.com/apache/arrow-rs/tree/49.0.0) (2023-11-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/48.0.0...49.0.0)

**Breaking changes:**

- Return row count when inferring schema from JSON [\#5008](https://github.com/apache/arrow-rs/pull/5008) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([asayers](https://github.com/asayers))
- Update object\_store 0.8.0 [\#5043](https://github.com/apache/arrow-rs/pull/5043) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Cast from integer/timestamp to timestamp/integer [\#5039](https://github.com/apache/arrow-rs/issues/5039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting from integer to binary [\#5014](https://github.com/apache/arrow-rs/issues/5014) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Return row count when inferring schema from JSON [\#5007](https://github.com/apache/arrow-rs/issues/5007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[FlightSQL\] Allow custom commands in get-flight-info [\#4996](https://github.com/apache/arrow-rs/issues/4996) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support `RecordBatch::remove_column()` and `Schema::remove_field()` [\#4952](https://github.com/apache/arrow-rs/issues/4952) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow_json`: support `binary` deserialization [\#4945](https://github.com/apache/arrow-rs/issues/4945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support StructArray in Cast Kernel [\#4908](https://github.com/apache/arrow-rs/issues/4908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- There exists a `ParquetRecordWriter` proc macro in `parquet_derive`, but `ParquetRecordReader` is missing [\#4772](https://github.com/apache/arrow-rs/issues/4772) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Regression when serializing large json numbers [\#5038](https://github.com/apache/arrow-rs/issues/5038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RowSelection::intersection Produces Invalid RowSelection [\#5036](https://github.com/apache/arrow-rs/issues/5036) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Incorrect comment on arrow::compute::kernels::sort::sort\_to\_indices [\#5029](https://github.com/apache/arrow-rs/issues/5029) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- chore: Update docs to refer to non deprecated function \(`partition`\) [\#5027](https://github.com/apache/arrow-rs/pull/5027) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Parquet f32/f64 handle signed zeros in statistics [\#5048](https://github.com/apache/arrow-rs/pull/5048) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jefffrey](https://github.com/Jefffrey))
- Fix serialization of large integers in JSON \(\#5038\) [\#5042](https://github.com/apache/arrow-rs/pull/5042) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix RowSelection::intersection \(\#5036\) [\#5041](https://github.com/apache/arrow-rs/pull/5041) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cast from integer/timestamp to timestamp/integer [\#5040](https://github.com/apache/arrow-rs/pull/5040) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- doc: update comment on sort\_to\_indices to reflect correct ordering [\#5033](https://github.com/apache/arrow-rs/pull/5033) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([westonpace](https://github.com/westonpace))
- Support casting from integer to binary [\#5015](https://github.com/apache/arrow-rs/pull/5015) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update tracing-log requirement from 0.1 to 0.2 [\#4998](https://github.com/apache/arrow-rs/pull/4998) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat\(flight-sql\): Allow custom commands in get-flight-info [\#4997](https://github.com/apache/arrow-rs/pull/4997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([amartins23](https://github.com/amartins23))
- \[MINOR\] No need to jump to web pages [\#4994](https://github.com/apache/arrow-rs/pull/4994) ([smallzhongfeng](https://github.com/smallzhongfeng))
- Support metadata in SchemaBuilder [\#4987](https://github.com/apache/arrow-rs/pull/4987) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: support schema change by idx and reverse [\#4985](https://github.com/apache/arrow-rs/pull/4985) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fansehep](https://github.com/fansehep))
- Bump actions/setup-node from 3 to 4 [\#4982](https://github.com/apache/arrow-rs/pull/4982) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add arrow\_cast::base64 and document usage in arrow\_json [\#4975](https://github.com/apache/arrow-rs/pull/4975) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add SchemaBuilder::remove \(\#4952\) [\#4964](https://github.com/apache/arrow-rs/pull/4964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `Field::remove()`, `Schema::remove()`, and `RecordBatch::remove_column()` APIs [\#4959](https://github.com/apache/arrow-rs/pull/4959) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Folyd](https://github.com/Folyd))
- Add `RecordReader` trait and proc macro to implement it for a struct [\#4773](https://github.com/apache/arrow-rs/pull/4773) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Joseph-Rance](https://github.com/Joseph-Rance))
## [48.0.0](https://github.com/apache/arrow-rs/tree/48.0.0) (2023-10-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/47.0.0...48.0.0)

**Breaking changes:**

- Evaluate null\_regex for string type in csv \(now such values will be parsed as `Null` rather than `""`\) [\#4942](https://github.com/apache/arrow-rs/pull/4942) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([haohuaijin](https://github.com/haohuaijin))
- fix\(csv\)!: infer null for empty column. [\#4910](https://github.com/apache/arrow-rs/pull/4910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kskalski](https://github.com/kskalski))
- feat: log headers/trailers in flight CLI \(+ minor fixes\) [\#4898](https://github.com/apache/arrow-rs/pull/4898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- fix\(arrow-json\)!: include null fields in schema inference with a type of Null [\#4894](https://github.com/apache/arrow-rs/pull/4894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kskalski](https://github.com/kskalski))
- Mark OnCloseRowGroup Send [\#4893](https://github.com/apache/arrow-rs/pull/4893) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([devinjdangelo](https://github.com/devinjdangelo))
- Specialize Thrift Decoding \(~40% Faster\) \(\#4891\) [\#4892](https://github.com/apache/arrow-rs/pull/4892) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make ArrowRowGroupWriter Public and SerializedRowGroupWriter Send [\#4850](https://github.com/apache/arrow-rs/pull/4850) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([devinjdangelo](https://github.com/devinjdangelo))

**Implemented enhancements:**

- Allow schema fields to merge with `Null` datatype [\#4901](https://github.com/apache/arrow-rs/issues/4901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add option to FlightDataEncoder to always send dictionaries [\#4895](https://github.com/apache/arrow-rs/issues/4895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Rework Thrift Encoding / Decoding of Parquet Metadata [\#4891](https://github.com/apache/arrow-rs/issues/4891) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Plans for supporting Extension Array to support Fixed shape tensor Array [\#4890](https://github.com/apache/arrow-rs/issues/4890)
- Implement Take for UnionArray [\#4882](https://github.com/apache/arrow-rs/issues/4882) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Check precision overflow for casting floating to decimal [\#4865](https://github.com/apache/arrow-rs/issues/4865) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace lexical [\#4774](https://github.com/apache/arrow-rs/issues/4774) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add read access to settings in `csv::WriterBuilder` [\#4735](https://github.com/apache/arrow-rs/issues/4735) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve the performance of "DictionaryValue" row encoding [\#4712](https://github.com/apache/arrow-rs/issues/4712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Should we make blank values and empty string to `None` in csv? [\#4939](https://github.com/apache/arrow-rs/issues/4939) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[FlightSQL\] SubstraitPlan structure is not exported [\#4932](https://github.com/apache/arrow-rs/issues/4932) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Loading page index breaks skipping of pages with nested types [\#4921](https://github.com/apache/arrow-rs/issues/4921) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- CSV schema inference assumes `Utf8` for empty columns [\#4903](https://github.com/apache/arrow-rs/issues/4903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: Field Ids are not read from a Parquet file without serialized arrow schema [\#4877](https://github.com/apache/arrow-rs/issues/4877) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- make\_primitive\_scalar function loses DataType Internal information [\#4851](https://github.com/apache/arrow-rs/issues/4851) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- StructBuilder doesn't handle nulls correctly for empty structs [\#4842](https://github.com/apache/arrow-rs/issues/4842) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `NullArray::is_null()` returns `false` incorrectly [\#4835](https://github.com/apache/arrow-rs/issues/4835) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- cast\_string\_to\_decimal should check precision overflow [\#4829](https://github.com/apache/arrow-rs/issues/4829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Null fields are omitted by `infer_json_schema_from_seekable` [\#4814](https://github.com/apache/arrow-rs/issues/4814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Support for reading JSON Array to Arrow [\#4905](https://github.com/apache/arrow-rs/issues/4905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Assume Pages Delimit Records When Offset Index Loaded \(\#4921\) [\#4943](https://github.com/apache/arrow-rs/pull/4943) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update pyo3 requirement from 0.19 to 0.20 [\#4941](https://github.com/apache/arrow-rs/pull/4941) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add `FileWriter` schema getter [\#4940](https://github.com/apache/arrow-rs/pull/4940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([haixuanTao](https://github.com/haixuanTao))
- feat: support parsing for parquet writer option [\#4938](https://github.com/apache/arrow-rs/pull/4938) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([fansehep](https://github.com/fansehep))
- Export `SubstraitPlan` structure in arrow\_flight::sql \(\#4932\) [\#4933](https://github.com/apache/arrow-rs/pull/4933) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([amartins23](https://github.com/amartins23))
- Update zstd requirement from 0.12.0 to 0.13.0 [\#4923](https://github.com/apache/arrow-rs/pull/4923) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: add method for async read bloom filter [\#4917](https://github.com/apache/arrow-rs/pull/4917) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([hengfeiyang](https://github.com/hengfeiyang))
- Minor: Clarify rationale for `FlightDataEncoder` API, add examples [\#4916](https://github.com/apache/arrow-rs/pull/4916) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Update regex-syntax requirement from 0.7.1 to 0.8.0 [\#4914](https://github.com/apache/arrow-rs/pull/4914) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: document & streamline flight SQL CLI [\#4912](https://github.com/apache/arrow-rs/pull/4912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Support Arbitrary JSON values in JSON Reader \(\#4905\) [\#4911](https://github.com/apache/arrow-rs/pull/4911) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup CSV WriterBuilder, Default to AutoSI Second Precision \(\#4735\) [\#4909](https://github.com/apache/arrow-rs/pull/4909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.68 to =1.0.69 [\#4907](https://github.com/apache/arrow-rs/pull/4907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- chore: add csv example [\#4904](https://github.com/apache/arrow-rs/pull/4904) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fansehep](https://github.com/fansehep))
- feat\(schema\): allow null fields to be merged with other datatypes [\#4902](https://github.com/apache/arrow-rs/pull/4902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kskalski](https://github.com/kskalski))
- Update proc-macro2 requirement from =1.0.67 to =1.0.68 [\#4900](https://github.com/apache/arrow-rs/pull/4900) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add option to `FlightDataEncoder` to always resend batch dictionaries [\#4896](https://github.com/apache/arrow-rs/pull/4896) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- Fix integration tests [\#4889](https://github.com/apache/arrow-rs/pull/4889) ([tustvold](https://github.com/tustvold))
- Support Parsing Avro File Headers [\#4888](https://github.com/apache/arrow-rs/pull/4888) ([tustvold](https://github.com/tustvold))
- Support parquet bloom filter length [\#4885](https://github.com/apache/arrow-rs/pull/4885) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([letian-jiang](https://github.com/letian-jiang))
- Replace lz4 with lz4\_flex Allowing Compilation for WASM [\#4884](https://github.com/apache/arrow-rs/pull/4884) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement Take for UnionArray [\#4883](https://github.com/apache/arrow-rs/pull/4883) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([avantgardnerio](https://github.com/avantgardnerio))
- Update tonic-build requirement from =0.10.1 to =0.10.2 [\#4881](https://github.com/apache/arrow-rs/pull/4881) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- parquet: Read field IDs from Parquet Schema [\#4878](https://github.com/apache/arrow-rs/pull/4878) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Samrose-Ahmed](https://github.com/Samrose-Ahmed))
- feat: improve flight CLI error handling [\#4873](https://github.com/apache/arrow-rs/pull/4873) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Support Encoding Parquet Columns in Parallel [\#4871](https://github.com/apache/arrow-rs/pull/4871) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Check precision overflow for casting floating to decimal [\#4866](https://github.com/apache/arrow-rs/pull/4866) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make align\_buffers as public API [\#4863](https://github.com/apache/arrow-rs/pull/4863) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Enable new integration tests \(\#4828\) [\#4862](https://github.com/apache/arrow-rs/pull/4862) ([tustvold](https://github.com/tustvold))
- Faster Serde Integration \(~80% faster\) [\#4861](https://github.com/apache/arrow-rs/pull/4861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix: make\_primitive\_scalar bug [\#4852](https://github.com/apache/arrow-rs/pull/4852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JasonLi-cn](https://github.com/JasonLi-cn))
- Update tonic-build requirement from =0.10.0 to =0.10.1 [\#4846](https://github.com/apache/arrow-rs/pull/4846) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Allow Constructing Non-Empty StructArray with no Fields \(\#4842\) [\#4845](https://github.com/apache/arrow-rs/pull/4845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Refine documentation to `Array::is_null` [\#4838](https://github.com/apache/arrow-rs/pull/4838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: add missing precision overflow checking for `cast_string_to_decimal` [\#4830](https://github.com/apache/arrow-rs/pull/4830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonahgao](https://github.com/jonahgao))
## [47.0.0](https://github.com/apache/arrow-rs/tree/47.0.0) (2023-09-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/46.0.0...47.0.0)

**Breaking changes:**

- Make FixedSizeBinaryArray value\_data return a reference [\#4820](https://github.com/apache/arrow-rs/issues/4820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update prost to v0.12.1 [\#4825](https://github.com/apache/arrow-rs/pull/4825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- feat: FixedSizeBinaryArray::value\_data return reference [\#4821](https://github.com/apache/arrow-rs/pull/4821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Stateless Row Encoding / Don't Preserve Dictionaries in `RowConverter` \(\#4811\) [\#4819](https://github.com/apache/arrow-rs/pull/4819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- fix: entries field is non-nullable [\#4808](https://github.com/apache/arrow-rs/pull/4808) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Fix flight sql do put handling, add bind parameter support to FlightSQL cli client [\#4797](https://github.com/apache/arrow-rs/pull/4797) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([suremarc](https://github.com/suremarc))
- Remove unused dyn\_cmp\_dict feature [\#4766](https://github.com/apache/arrow-rs/pull/4766) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add underlying `std::io::Error` to `IoError` and add `IpcError` variant [\#4726](https://github.com/apache/arrow-rs/pull/4726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alexandreyc](https://github.com/alexandreyc))

**Implemented enhancements:**

- Row Format Adapative Block Size  [\#4812](https://github.com/apache/arrow-rs/issues/4812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Stateless Row Conversion [\#4811](https://github.com/apache/arrow-rs/issues/4811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add option to specify custom null values for CSV reader [\#4794](https://github.com/apache/arrow-rs/issues/4794) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet::record::RowIter cannot be customized with batch\_size and defaults to 1024 [\#4782](https://github.com/apache/arrow-rs/issues/4782) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `DynScalar` abstraction \(something that makes it easy to create scalar `Datum`s\) [\#4781](https://github.com/apache/arrow-rs/issues/4781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `Datum` is not exported as part of `arrow` \(it is only exported in `arrow_array`\) [\#4780](https://github.com/apache/arrow-rs/issues/4780) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `Scalar` is not exported as part of `arrow` \(it is only exported in `arrow_array`\) [\#4779](https://github.com/apache/arrow-rs/issues/4779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support IntoPyArrow for impl RecordBatchReader [\#4730](https://github.com/apache/arrow-rs/issues/4730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Datum Based String Kernels [\#4595](https://github.com/apache/arrow-rs/issues/4595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- MapArray::new\_from\_strings creates nullable entries field [\#4807](https://github.com/apache/arrow-rs/issues/4807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- pyarrow module can't roundtrip tensor arrays [\#4805](https://github.com/apache/arrow-rs/issues/4805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `concat_batches` errors with "schema mismatch" error when only metadata differs [\#4799](https://github.com/apache/arrow-rs/issues/4799) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- panic in `cmp` kernels with DictionaryArrays:  `Option::unwrap()` on a `None` value' [\#4788](https://github.com/apache/arrow-rs/issues/4788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- stream ffi panics if schema metadata values aren't valid utf8 [\#4750](https://github.com/apache/arrow-rs/issues/4750) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Regression: Incorrect Sorting of `*ListArray` in 46.0.0 [\#4746](https://github.com/apache/arrow-rs/issues/4746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Row is no longer comparable after reuse [\#4741](https://github.com/apache/arrow-rs/issues/4741) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DoPut FlightSQL handler inadvertently consumes schema at start of Request\<Streaming\<FlightData\>\> [\#4658](https://github.com/apache/arrow-rs/issues/4658)
- Return error when converting schema  [\#4752](https://github.com/apache/arrow-rs/pull/4752) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Implement PyArrowType for `Box<dyn RecordBatchReader + Send>` [\#4751](https://github.com/apache/arrow-rs/pull/4751) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))

**Closed issues:**

- Building arrow-rust for target wasm32-wasi falied to compile packed\_simd\_2 [\#4717](https://github.com/apache/arrow-rs/issues/4717)

**Merged pull requests:**

- Respect FormatOption::nulls for NullArray [\#4836](https://github.com/apache/arrow-rs/pull/4836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix merge\_dictionary\_values in selection kernels [\#4833](https://github.com/apache/arrow-rs/pull/4833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix like scalar null [\#4832](https://github.com/apache/arrow-rs/pull/4832) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- More chrono deprecations [\#4822](https://github.com/apache/arrow-rs/pull/4822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
-  Adaptive Row Block Size \(\#4812\) [\#4818](https://github.com/apache/arrow-rs/pull/4818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.66 to =1.0.67 [\#4816](https://github.com/apache/arrow-rs/pull/4816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Do not check schema for equality in concat\_batches [\#4815](https://github.com/apache/arrow-rs/pull/4815) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- fix: export record batch through stream [\#4806](https://github.com/apache/arrow-rs/pull/4806) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Improve CSV Reader Benchmark Coverage of Small Primitives [\#4803](https://github.com/apache/arrow-rs/pull/4803) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- csv: Add option to specify custom null values [\#4795](https://github.com/apache/arrow-rs/pull/4795) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([vrongmeal](https://github.com/vrongmeal))
- Expand docstring and add example to `Scalar` [\#4793](https://github.com/apache/arrow-rs/pull/4793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Re-export array crate root \(\#4780\) \(\#4779\) [\#4791](https://github.com/apache/arrow-rs/pull/4791) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix DictionaryArray::normalized\_keys \(\#4788\) [\#4789](https://github.com/apache/arrow-rs/pull/4789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow custom tree builder for parquet::record::RowIter [\#4783](https://github.com/apache/arrow-rs/pull/4783) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([YuraKotov](https://github.com/YuraKotov))
- Bump actions/checkout from 3 to 4 [\#4767](https://github.com/apache/arrow-rs/pull/4767) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: avoid panic if offset index not exists. [\#4761](https://github.com/apache/arrow-rs/pull/4761) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Relax constraints on PyArrowType [\#4757](https://github.com/apache/arrow-rs/pull/4757) ([tustvold](https://github.com/tustvold))
- Chrono deprecations [\#4748](https://github.com/apache/arrow-rs/pull/4748) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix List Sorting, Revert Removal of Rank Kernels [\#4747](https://github.com/apache/arrow-rs/pull/4747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Clear row buffer before reuse [\#4742](https://github.com/apache/arrow-rs/pull/4742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Datum based like kernels \(\#4595\) [\#4732](https://github.com/apache/arrow-rs/pull/4732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- feat: expose DoGet response headers & trailers [\#4727](https://github.com/apache/arrow-rs/pull/4727) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Cleanup length and bit\_length kernels [\#4718](https://github.com/apache/arrow-rs/pull/4718) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
## [46.0.0](https://github.com/apache/arrow-rs/tree/46.0.0) (2023-08-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/45.0.0...46.0.0)

**Breaking changes:**

- API improvement: `batches_to_flight_data` forces clone [\#4656](https://github.com/apache/arrow-rs/issues/4656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add AnyDictionary Abstraction and Take ArrayRef in DictionaryArray::with\_values [\#4707](https://github.com/apache/arrow-rs/pull/4707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup parquet type builders [\#4706](https://github.com/apache/arrow-rs/pull/4706) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Take kernel dyn Array [\#4705](https://github.com/apache/arrow-rs/pull/4705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve ergonomics of Scalar [\#4704](https://github.com/apache/arrow-rs/pull/4704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Datum based comparison kernels \(\#4596\) [\#4701](https://github.com/apache/arrow-rs/pull/4701) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Improve `Array` Logical Nullability [\#4691](https://github.com/apache/arrow-rs/pull/4691) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Validate ArrayData Buffer Alignment and Automatically Align IPC buffers \(\#4255\) [\#4681](https://github.com/apache/arrow-rs/pull/4681) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- More intuitive bool-to-string casting [\#4666](https://github.com/apache/arrow-rs/pull/4666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fsdvh](https://github.com/fsdvh))
- enhancement: batches\_to\_flight\_data use a schema ref as param. [\#4665](https://github.com/apache/arrow-rs/pull/4665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([jackwener](https://github.com/jackwener))
- fix: from\_thrift avoid panic when stats in invalid. [\#4642](https://github.com/apache/arrow-rs/pull/4642) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jackwener](https://github.com/jackwener))
- bug: Add some missing field in row group metadata: ordinal, total co… [\#4636](https://github.com/apache/arrow-rs/pull/4636) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liurenjie1024](https://github.com/liurenjie1024))
- Remove deprecated limit kernel [\#4597](https://github.com/apache/arrow-rs/pull/4597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- parquet: support setting the field\_id with an ArrowWriter [\#4702](https://github.com/apache/arrow-rs/issues/4702) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support references in i256 arithmetic ops [\#4694](https://github.com/apache/arrow-rs/issues/4694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Precision-Loss Decimal Arithmetic [\#4664](https://github.com/apache/arrow-rs/issues/4664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Faster i256 Division [\#4663](https://github.com/apache/arrow-rs/issues/4663) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `concat_batches` for 0 columns [\#4661](https://github.com/apache/arrow-rs/issues/4661) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `filter_record_batch` should support filtering record batch without columns [\#4647](https://github.com/apache/arrow-rs/issues/4647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve speed of `lexicographical_partition_ranges` [\#4614](https://github.com/apache/arrow-rs/issues/4614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- object\_store: multipart ranges for HTTP [\#4612](https://github.com/apache/arrow-rs/issues/4612)
- Add Rank Function [\#4606](https://github.com/apache/arrow-rs/issues/4606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Datum Based Comparison Kernels [\#4596](https://github.com/apache/arrow-rs/issues/4596) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Convenience method to create `DataType::List` correctly [\#4544](https://github.com/apache/arrow-rs/issues/4544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove Deprecated Arithmetic Kernels [\#4481](https://github.com/apache/arrow-rs/issues/4481) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Equality kernel where null==null gives true  [\#4438](https://github.com/apache/arrow-rs/issues/4438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Parquet ArrowWriter Ignores Nulls in Dictionary Values [\#4690](https://github.com/apache/arrow-rs/issues/4690) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Schema Nullability Validation Fails to Account for Dictionary Nulls [\#4689](https://github.com/apache/arrow-rs/issues/4689) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Comparison Kernels Ignore Nulls in Dictionary Values [\#4688](https://github.com/apache/arrow-rs/issues/4688) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Casting List to String Ignores Format Options [\#4669](https://github.com/apache/arrow-rs/issues/4669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Double free in C Stream Interface [\#4659](https://github.com/apache/arrow-rs/issues/4659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CI Failing On Packed SIMD [\#4651](https://github.com/apache/arrow-rs/issues/4651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `RowInterner::size()` much too low for high cardinality dictionary columns [\#4645](https://github.com/apache/arrow-rs/issues/4645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Decimal PrimitiveArray change datatype after try\_unary  [\#4644](https://github.com/apache/arrow-rs/issues/4644)
- Better explanation in docs for Dictionary field encoding using RowConverter [\#4639](https://github.com/apache/arrow-rs/issues/4639) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `List(FixedSizeBinary)` array equality check may return wrong result [\#4637](https://github.com/apache/arrow-rs/issues/4637) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow::compute::nullif` panics if `NullArray` is provided [\#4634](https://github.com/apache/arrow-rs/issues/4634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Empty lists in FixedSizeListArray::try\_new is not handled [\#4623](https://github.com/apache/arrow-rs/issues/4623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Bounds checking in `MutableBuffer::set_null_bits` can be bypassed [\#4620](https://github.com/apache/arrow-rs/issues/4620) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- TypedDictionaryArray Misleading Null Behaviour [\#4616](https://github.com/apache/arrow-rs/issues/4616) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- bug: Parquet writer missing row group metadata fields such as `compressed_size`, `file offset`. [\#4610](https://github.com/apache/arrow-rs/issues/4610) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `new_null_array` generates an invalid union array [\#4600](https://github.com/apache/arrow-rs/issues/4600) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Footer parsing fails for very large parquet file. [\#4592](https://github.com/apache/arrow-rs/issues/4592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- bug\(parquet\): Disabling global statistics but enabling for particular column breaks reading [\#4587](https://github.com/apache/arrow-rs/issues/4587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `arrow::compute::concat` panics for dense union arrays with non-trivial type IDs [\#4578](https://github.com/apache/arrow-rs/issues/4578) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- \[object\_store\] when Create a AmazonS3 instance  work with MinIO without set endpoint got error MissingRegion [\#4617](https://github.com/apache/arrow-rs/issues/4617)

**Merged pull requests:**

- Add distinct kernels \(\#960\) \(\#4438\) [\#4716](https://github.com/apache/arrow-rs/pull/4716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update parquet object\_store 0.7 [\#4715](https://github.com/apache/arrow-rs/pull/4715) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support Field ID in ArrowWriter \(\#4702\) [\#4710](https://github.com/apache/arrow-rs/pull/4710) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove rank kernels [\#4703](https://github.com/apache/arrow-rs/pull/4703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support references in i256 arithmetic ops [\#4692](https://github.com/apache/arrow-rs/pull/4692) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cleanup DynComparator \(\#2654\) [\#4687](https://github.com/apache/arrow-rs/pull/4687) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Separate metadata fetch from `ArrowReaderBuilder` construction \(\#4674\) [\#4676](https://github.com/apache/arrow-rs/pull/4676) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- cleanup some assert\(\) with error propagation [\#4673](https://github.com/apache/arrow-rs/pull/4673) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Faster i256 Division \(2-100x\) \(\#4663\) [\#4672](https://github.com/apache/arrow-rs/pull/4672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix MSRV CI [\#4671](https://github.com/apache/arrow-rs/pull/4671) ([tustvold](https://github.com/tustvold))
- Fix equality of nested nullable FixedSizeBinary \(\#4637\) [\#4670](https://github.com/apache/arrow-rs/pull/4670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use ArrayFormatter in cast kernel [\#4668](https://github.com/apache/arrow-rs/pull/4668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Improve API docs for FlightSQL metadata builders [\#4667](https://github.com/apache/arrow-rs/pull/4667) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Support `concat_batches` for 0 columns [\#4662](https://github.com/apache/arrow-rs/pull/4662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- fix ownership of c stream error [\#4660](https://github.com/apache/arrow-rs/pull/4660) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Minor: Fix illustration for dict encoding [\#4657](https://github.com/apache/arrow-rs/pull/4657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JayjeetAtGithub](https://github.com/JayjeetAtGithub))
- minor: move comment to the correct location [\#4655](https://github.com/apache/arrow-rs/pull/4655) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Update packed\_simd and run miri tests on simd code [\#4654](https://github.com/apache/arrow-rs/pull/4654) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- impl `From<Vec<T>>` for `BufferBuilder` and `MutableBuffer` [\#4650](https://github.com/apache/arrow-rs/pull/4650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Filter record batch with 0 columns [\#4648](https://github.com/apache/arrow-rs/pull/4648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Account for child `Bucket` size in OrderPreservingInterner [\#4646](https://github.com/apache/arrow-rs/pull/4646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement `Default`,`Extend` and `FromIterator` for `BufferBuilder` [\#4638](https://github.com/apache/arrow-rs/pull/4638) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- fix\(select\): handle `NullArray` in `nullif` [\#4635](https://github.com/apache/arrow-rs/pull/4635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Move `BufferBuilder` to `arrow-buffer` [\#4630](https://github.com/apache/arrow-rs/pull/4630) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- allow zero sized empty fixed [\#4626](https://github.com/apache/arrow-rs/pull/4626) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([smiklos](https://github.com/smiklos))
- fix: compute\_dictionary\_mapping use wrong offsetSize [\#4625](https://github.com/apache/arrow-rs/pull/4625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- impl `FromIterator` for `MutableBuffer` [\#4624](https://github.com/apache/arrow-rs/pull/4624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- expand docs for FixedSizeListArray [\#4622](https://github.com/apache/arrow-rs/pull/4622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([smiklos](https://github.com/smiklos))
- fix\(buffer\): panic on end index overflow in `MutableBuffer::set_null_bits` [\#4621](https://github.com/apache/arrow-rs/pull/4621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- impl `Default` for `arrow_buffer::buffer::MutableBuffer` [\#4619](https://github.com/apache/arrow-rs/pull/4619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Minor: improve docs and add example for lexicographical\_partition\_ranges [\#4615](https://github.com/apache/arrow-rs/pull/4615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Cleanup sort [\#4613](https://github.com/apache/arrow-rs/pull/4613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add rank function \(\#4606\) [\#4609](https://github.com/apache/arrow-rs/pull/4609) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add more docs and examples for ListArray and OffsetsBuffer [\#4607](https://github.com/apache/arrow-rs/pull/4607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Simplify dictionary sort [\#4605](https://github.com/apache/arrow-rs/pull/4605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Consolidate sort benchmarks [\#4604](https://github.com/apache/arrow-rs/pull/4604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't Reorder Nulls in sort\_to\_indices \(\#4545\) [\#4603](https://github.com/apache/arrow-rs/pull/4603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix\(data\): create child arrays of correct length when building a sparse union null array [\#4601](https://github.com/apache/arrow-rs/pull/4601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Use u32 metadata\_len when parsing footer of parquet. [\#4599](https://github.com/apache/arrow-rs/pull/4599) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Berrysoft](https://github.com/Berrysoft))
- fix\(data\): map type ID to child index before indexing a union child array [\#4598](https://github.com/apache/arrow-rs/pull/4598) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kawadakk](https://github.com/kawadakk))
- Remove deprecated arithmetic kernels \(\#4481\) [\#4594](https://github.com/apache/arrow-rs/pull/4594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Test Disabled Page Statistics \(\#4587\) [\#4589](https://github.com/apache/arrow-rs/pull/4589) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cleanup ArrayData::buffers [\#4583](https://github.com/apache/arrow-rs/pull/4583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use contains\_nulls in ArrayData equality of byte arrays [\#4582](https://github.com/apache/arrow-rs/pull/4582) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Vectorized lexicographical\_partition\_ranges \(~80% faster\) [\#4575](https://github.com/apache/arrow-rs/pull/4575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- chore: add datatype new\_list [\#4561](https://github.com/apache/arrow-rs/pull/4561) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([fansehep](https://github.com/fansehep))
## [45.0.0](https://github.com/apache/arrow-rs/tree/45.0.0) (2023-07-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/44.0.0...45.0.0)

**Breaking changes:**

- Fix timezoned timestamp arithmetic [\#4546](https://github.com/apache/arrow-rs/pull/4546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))

**Implemented enhancements:**

- Use FormatOptions in Const Contexts [\#4580](https://github.com/apache/arrow-rs/issues/4580) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Human Readable Duration Display [\#4554](https://github.com/apache/arrow-rs/issues/4554) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `BooleanBuilder`: Add `validity_slice` method for accessing validity bits [\#4535](https://github.com/apache/arrow-rs/issues/4535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `FixedSizedListArray` for `length` kernel [\#4517](https://github.com/apache/arrow-rs/issues/4517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `RowCoverter::convert` that targets an existing `Rows` [\#4479](https://github.com/apache/arrow-rs/issues/4479) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Panic `assertion failed: idx < self.len` when casting DictionaryArrays with nulls [\#4576](https://github.com/apache/arrow-rs/issues/4576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-arith is\_null is buggy with NullArray [\#4565](https://github.com/apache/arrow-rs/issues/4565) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect Interval to Duration Casting [\#4553](https://github.com/apache/arrow-rs/issues/4553) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Too large validity buffer pre-allocation in `FixedSizeListBuilder::new` [\#4549](https://github.com/apache/arrow-rs/issues/4549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Like with wildcards fail to match fields with new lines. [\#4547](https://github.com/apache/arrow-rs/issues/4547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Timestamp Interval Arithmetic Ignores Timezone [\#4457](https://github.com/apache/arrow-rs/issues/4457) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- refactor: simplify hour\_dyn\(\) with time\_fraction\_dyn\(\) [\#4588](https://github.com/apache/arrow-rs/pull/4588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Move from\_iter\_values to GenericByteArray [\#4586](https://github.com/apache/arrow-rs/pull/4586) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Mark GenericByteArray::new\_unchecked unsafe [\#4584](https://github.com/apache/arrow-rs/pull/4584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Configurable Duration Display [\#4581](https://github.com/apache/arrow-rs/pull/4581) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix take\_bytes Null and Overflow Handling \(\#4576\) [\#4579](https://github.com/apache/arrow-rs/pull/4579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move chrono-tz arithmetic tests to integration [\#4571](https://github.com/apache/arrow-rs/pull/4571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Write Page Offset Index For All-Nan Pages [\#4567](https://github.com/apache/arrow-rs/pull/4567) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([MachaelLee](https://github.com/MachaelLee))
- support NullArray un arith/boolean kernel [\#4566](https://github.com/apache/arrow-rs/pull/4566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([smiklos](https://github.com/smiklos))
- Remove Sync from arrow-flight example [\#4564](https://github.com/apache/arrow-rs/pull/4564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Fix interval to duration casting \(\#4553\) [\#4562](https://github.com/apache/arrow-rs/pull/4562) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- docs: fix wrong parameter name [\#4559](https://github.com/apache/arrow-rs/pull/4559) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SteveLauC](https://github.com/SteveLauC))
- Fix FixedSizeListBuilder capacity \(\#4549\) [\#4552](https://github.com/apache/arrow-rs/pull/4552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- docs: fix wrong inline code snippet in parquet document [\#4550](https://github.com/apache/arrow-rs/pull/4550) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SteveLauC](https://github.com/SteveLauC))
- fix multiline wildcard likes \(fixes \#4547\) [\#4548](https://github.com/apache/arrow-rs/pull/4548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nl5887](https://github.com/nl5887))
- Provide default `is_empty` impl for `arrow::array::ArrayBuilder` [\#4543](https://github.com/apache/arrow-rs/pull/4543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- Add RowConverter::append \(\#4479\) [\#4541](https://github.com/apache/arrow-rs/pull/4541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Clarify GenericColumnReader::read\_records [\#4540](https://github.com/apache/arrow-rs/pull/4540) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Initial loongarch port [\#4538](https://github.com/apache/arrow-rs/pull/4538) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xiangzhai](https://github.com/xiangzhai))
- Update proc-macro2 requirement from =1.0.64 to =1.0.66 [\#4537](https://github.com/apache/arrow-rs/pull/4537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- add a validity slice access for boolean array builders [\#4536](https://github.com/apache/arrow-rs/pull/4536) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ChristianBeilschmidt](https://github.com/ChristianBeilschmidt))
- use new num version instead of explicit num-complex dependency [\#4532](https://github.com/apache/arrow-rs/pull/4532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mwlon](https://github.com/mwlon))
- feat: Support `FixedSizedListArray` for `length` kernel [\#4520](https://github.com/apache/arrow-rs/pull/4520) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
## [44.0.0](https://github.com/apache/arrow-rs/tree/44.0.0) (2023-07-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/43.0.0...44.0.0)

**Breaking changes:**

- Use Parser for cast kernel \(\#4512\) [\#4513](https://github.com/apache/arrow-rs/pull/4513) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Datum based arithmetic kernels \(\#3999\)  [\#4465](https://github.com/apache/arrow-rs/pull/4465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- eq\_dyn\_binary\_scalar should support FixedSizeBinary types [\#4491](https://github.com/apache/arrow-rs/issues/4491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Port Tests from Deprecated Arithmetic Kernels [\#4480](https://github.com/apache/arrow-rs/issues/4480) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatchReader for Boxed trait object [\#4474](https://github.com/apache/arrow-rs/issues/4474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `Date` - `Date` kernel [\#4383](https://github.com/apache/arrow-rs/issues/4383) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Default FlightSqlService Implementations [\#4372](https://github.com/apache/arrow-rs/issues/4372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Parquet: `AsyncArrowWriter` to a file corrupts the footer for large columns [\#4526](https://github.com/apache/arrow-rs/issues/4526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[object\_store\] Failure to send bytes to azure [\#4522](https://github.com/apache/arrow-rs/issues/4522)
- Cannot cast string '2021-01-02' to value of Date64 type [\#4512](https://github.com/apache/arrow-rs/issues/4512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect Interval Subtraction [\#4489](https://github.com/apache/arrow-rs/issues/4489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Interval Negation Incorrect [\#4488](https://github.com/apache/arrow-rs/issues/4488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet: AsyncArrowWriter inner buffer is not correctly limited and causes OOM [\#4477](https://github.com/apache/arrow-rs/issues/4477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Fix AsyncArrowWriter flush for large buffer sizes \(\#4526\) [\#4527](https://github.com/apache/arrow-rs/pull/4527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cleanup cast\_primitive\_to\_list [\#4511](https://github.com/apache/arrow-rs/pull/4511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Bump actions/upload-pages-artifact from 1 to 2 [\#4508](https://github.com/apache/arrow-rs/pull/4508) ([dependabot[bot]](https://github.com/apps/dependabot))
- Support Date - Date \(\#4383\) [\#4504](https://github.com/apache/arrow-rs/pull/4504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Bump actions/labeler from 4.2.0 to 4.3.0 [\#4501](https://github.com/apache/arrow-rs/pull/4501) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.63 to =1.0.64 [\#4500](https://github.com/apache/arrow-rs/pull/4500) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add negate kernels \(\#4488\) [\#4494](https://github.com/apache/arrow-rs/pull/4494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Datum Arithmetic tests, Fix Interval Substraction \(\#4480\) [\#4493](https://github.com/apache/arrow-rs/pull/4493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- support FixedSizeBinary types in eq\_dyn\_binary\_scalar/neq\_dyn\_binary\_scalar [\#4492](https://github.com/apache/arrow-rs/pull/4492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
- Add default implementations to the FlightSqlService trait [\#4485](https://github.com/apache/arrow-rs/pull/4485) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([rossjones](https://github.com/rossjones))
- add num-complex requirement [\#4482](https://github.com/apache/arrow-rs/pull/4482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mwlon](https://github.com/mwlon))
- fix incorrect buffer size limiting in parquet async writer [\#4478](https://github.com/apache/arrow-rs/pull/4478) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([richox](https://github.com/richox))
- feat: support RecordBatchReader on boxed trait objects [\#4475](https://github.com/apache/arrow-rs/pull/4475) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Improve in-place primitive sorts by 13-67% [\#4473](https://github.com/apache/arrow-rs/pull/4473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Add Scalar/Datum abstraction \(\#1047\) [\#4393](https://github.com/apache/arrow-rs/pull/4393) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
## [43.0.0](https://github.com/apache/arrow-rs/tree/43.0.0) (2023-06-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/42.0.0...43.0.0)

**Breaking changes:**

- Simplify ffi import/export [\#4447](https://github.com/apache/arrow-rs/pull/4447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Virgiel](https://github.com/Virgiel))
- Return Result from Parquet Row APIs [\#4428](https://github.com/apache/arrow-rs/pull/4428) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Remove Binary Dictionary Arithmetic Support [\#4407](https://github.com/apache/arrow-rs/pull/4407) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Request: a way to copy a `Row` to `Rows` [\#4466](https://github.com/apache/arrow-rs/issues/4466) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reuse schema when importing from FFI [\#4444](https://github.com/apache/arrow-rs/issues/4444) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[FlightSQL\] Allow implementations of `FlightSqlService` to handle custom actions and commands [\#4439](https://github.com/apache/arrow-rs/issues/4439)
- Support `NullBuilder` [\#4429](https://github.com/apache/arrow-rs/issues/4429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Regression in  in parquet `42.0.0` : Bad parquet column indexes for All Null Columns, resulting in `Parquet error: StructArrayReader out of sync` on read [\#4459](https://github.com/apache/arrow-rs/issues/4459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Regression in 42.0.0: Parsing fractional intervals without leading 0 is not supported [\#4424](https://github.com/apache/arrow-rs/issues/4424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- doc: deploy crate docs to GitHub pages [\#4436](https://github.com/apache/arrow-rs/pull/4436) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xxchan](https://github.com/xxchan))

**Merged pull requests:**

- Append Row to Rows \(\#4466\) [\#4470](https://github.com/apache/arrow-rs/pull/4470) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat\(flight-sql\): Allow implementations of FlightSqlService to handle custom actions and commands [\#4463](https://github.com/apache/arrow-rs/pull/4463) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([amartins23](https://github.com/amartins23))
- Docs: Add clearer API doc links [\#4461](https://github.com/apache/arrow-rs/pull/4461) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Fix empty offset index for all null columns \(\#4459\) [\#4460](https://github.com/apache/arrow-rs/pull/4460) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Bump peaceiris/actions-gh-pages from 3.9.2 to 3.9.3 [\#4455](https://github.com/apache/arrow-rs/pull/4455) ([dependabot[bot]](https://github.com/apps/dependabot))
- Convince the compiler to auto-vectorize the range check in parquet DictionaryBuffer [\#4453](https://github.com/apache/arrow-rs/pull/4453) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jhorstmann](https://github.com/jhorstmann))
- fix docs deployment [\#4452](https://github.com/apache/arrow-rs/pull/4452) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xxchan](https://github.com/xxchan))
- Update indexmap requirement from 1.9 to 2.0 [\#4451](https://github.com/apache/arrow-rs/pull/4451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update proc-macro2 requirement from =1.0.60 to =1.0.63 [\#4450](https://github.com/apache/arrow-rs/pull/4450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/deploy-pages from 1 to 2 [\#4449](https://github.com/apache/arrow-rs/pull/4449) ([dependabot[bot]](https://github.com/apps/dependabot))
- Revise error message in From\<Buffer\> for ScalarBuffer [\#4446](https://github.com/apache/arrow-rs/pull/4446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- minor: remove useless mut [\#4443](https://github.com/apache/arrow-rs/pull/4443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- unify substring for binary&utf8 [\#4442](https://github.com/apache/arrow-rs/pull/4442) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Casting fixedsizelist to list/largelist [\#4433](https://github.com/apache/arrow-rs/pull/4433) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jayzhan211](https://github.com/jayzhan211))
- feat: support `NullBuilder` [\#4430](https://github.com/apache/arrow-rs/pull/4430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Remove Float64 -\> Float32 cast in IPC Reader [\#4427](https://github.com/apache/arrow-rs/pull/4427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ming08108](https://github.com/ming08108))
- Parse intervals like `.5` the same as `0.5` [\#4425](https://github.com/apache/arrow-rs/pull/4425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat: add strict mode to json reader [\#4421](https://github.com/apache/arrow-rs/pull/4421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([blinkseb](https://github.com/blinkseb))
- Add DictionaryArray::occupancy [\#4415](https://github.com/apache/arrow-rs/pull/4415) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
## [42.0.0](https://github.com/apache/arrow-rs/tree/42.0.0) (2023-06-16)

[Full Changelog](https://github.com/apache/arrow-rs/compare/41.0.0...42.0.0)

**Breaking changes:**

- Remove 64-bit to 32-bit Cast from IPC Reader [\#4412](https://github.com/apache/arrow-rs/pull/4412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ming08108](https://github.com/ming08108))
- Truncate Min/Max values in the Column Index [\#4389](https://github.com/apache/arrow-rs/pull/4389) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AdamGS](https://github.com/AdamGS))
- feat\(flight\): harmonize server metadata APIs [\#4384](https://github.com/apache/arrow-rs/pull/4384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([roeap](https://github.com/roeap))
- Move record delimiting into ColumnReader \(\#4365\) [\#4376](https://github.com/apache/arrow-rs/pull/4376) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Changed array\_to\_json\_array to take &dyn Array [\#4370](https://github.com/apache/arrow-rs/pull/4370) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dadepo](https://github.com/dadepo))
- Make PrimitiveArray::with\_timezone consuming [\#4366](https://github.com/apache/arrow-rs/pull/4366) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Add doc example of constructing a MapArray [\#4385](https://github.com/apache/arrow-rs/issues/4385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `millisecond` and `microsecond` functions [\#4374](https://github.com/apache/arrow-rs/issues/4374) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Changed array\_to\_json\_array to take &dyn Array [\#4369](https://github.com/apache/arrow-rs/issues/4369) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- compute::ord kernel for getting min and max of two scalar/array values [\#4347](https://github.com/apache/arrow-rs/issues/4347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release 41.0.0 of arrow/arrow-flight/parquet/parquet-derive [\#4346](https://github.com/apache/arrow-rs/issues/4346)
- Refactor CAST tests to use new cast array syntax [\#4336](https://github.com/apache/arrow-rs/issues/4336) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- pass bytes directly to parquet's KeyValue [\#4317](https://github.com/apache/arrow-rs/issues/4317)
- PyArrow conversions could return TypeError if provided incorrect Python type [\#4312](https://github.com/apache/arrow-rs/issues/4312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Have array\_to\_json\_array support Map [\#4297](https://github.com/apache/arrow-rs/issues/4297) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FlightSQL: Add helpers to create `CommandGetXdbcTypeInfo` responses \(`XdbcInfoValue` and builders\) [\#4257](https://github.com/apache/arrow-rs/issues/4257) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Have array\_to\_json\_array support FixedSizeList  [\#4248](https://github.com/apache/arrow-rs/issues/4248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Truncate ColumnIndex ByteArray Statistics [\#4126](https://github.com/apache/arrow-rs/issues/4126) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Arrow compute kernel regards selection vector [\#4095](https://github.com/apache/arrow-rs/issues/4095) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Wrongly calculated data compressed length in IPC writer [\#4410](https://github.com/apache/arrow-rs/issues/4410) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Take Kernel Handles Nullable Indices Incorrectly [\#4404](https://github.com/apache/arrow-rs/issues/4404) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- StructBuilder::new Doesn't Validate Builder DataTypes [\#4397](https://github.com/apache/arrow-rs/issues/4397) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet error: Not all children array length are the same! when using RowSelection to read a parquet file [\#4396](https://github.com/apache/arrow-rs/issues/4396)
- RecordReader::skip\_records Is Incorrect for Repeated Columns [\#4368](https://github.com/apache/arrow-rs/issues/4368) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- List-of-String Array panics in the presence of row filters [\#4365](https://github.com/apache/arrow-rs/issues/4365) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Fail to read block compressed gzip files with parquet-fromcsv [\#4173](https://github.com/apache/arrow-rs/issues/4173) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Closed issues:**

- Have a parquet file not able to be deduped via arrow-rs, complains about Decimal precision? [\#4356](https://github.com/apache/arrow-rs/issues/4356)
- Question: Could we move `dict_id, dict_is_ordered` into DataType? [\#4325](https://github.com/apache/arrow-rs/issues/4325)

**Merged pull requests:**

- Fix reading gzip file with multiple gzip headers in parquet-fromcsv. [\#4419](https://github.com/apache/arrow-rs/pull/4419) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ghuls](https://github.com/ghuls))
- Cleanup nullif kernel [\#4416](https://github.com/apache/arrow-rs/pull/4416) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix bug in IPC logic that determines if the buffer should be compressed or not [\#4411](https://github.com/apache/arrow-rs/pull/4411) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([lwpyr](https://github.com/lwpyr))
- Faster unpacking of Int32Type dictionary [\#4406](https://github.com/apache/arrow-rs/pull/4406) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve `take` kernel performance on primitive arrays, fix bad null index handling  \(\#4404\) [\#4405](https://github.com/apache/arrow-rs/pull/4405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- More take benchmarks [\#4403](https://github.com/apache/arrow-rs/pull/4403) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `BooleanBuffer::new_unset` and `BooleanBuffer::new_set` and `BooleanArray::new_null` constructors [\#4402](https://github.com/apache/arrow-rs/pull/4402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add PrimitiveBuilder type constructors [\#4401](https://github.com/apache/arrow-rs/pull/4401) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- StructBuilder Validate Child Data \(\#4397\) [\#4400](https://github.com/apache/arrow-rs/pull/4400) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster UTF-8 truncation [\#4399](https://github.com/apache/arrow-rs/pull/4399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Minor: Derive `Hash` impls for `CastOptions` and `FormatOptions` [\#4395](https://github.com/apache/arrow-rs/pull/4395) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix typo in README [\#4394](https://github.com/apache/arrow-rs/pull/4394) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([okue](https://github.com/okue))
- Improve parquet `WriterProperites` and `ReaderProperties` docs [\#4392](https://github.com/apache/arrow-rs/pull/4392) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Cleanup downcast macros [\#4391](https://github.com/apache/arrow-rs/pull/4391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.59 to =1.0.60 [\#4388](https://github.com/apache/arrow-rs/pull/4388) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Consolidate ByteArray::from\_iterator [\#4386](https://github.com/apache/arrow-rs/pull/4386) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add MapArray constructors and doc example [\#4382](https://github.com/apache/arrow-rs/pull/4382) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Documentation Improvements [\#4381](https://github.com/apache/arrow-rs/pull/4381) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add NullBuffer and BooleanBuffer From conversions [\#4380](https://github.com/apache/arrow-rs/pull/4380) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add more examples of constructing Boolean, Primitive, String,  and Decimal Arrays, and From impl for i256 [\#4379](https://github.com/apache/arrow-rs/pull/4379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add ListArrayReader benchmarks [\#4378](https://github.com/apache/arrow-rs/pull/4378) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update comfy-table requirement from 6.0 to 7.0 [\#4377](https://github.com/apache/arrow-rs/pull/4377) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: Add`microsecond` and `millisecond` kernels [\#4375](https://github.com/apache/arrow-rs/pull/4375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Update hashbrown requirement from 0.13 to 0.14 [\#4373](https://github.com/apache/arrow-rs/pull/4373) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- minor: use as\_boolean to resolve TODO [\#4367](https://github.com/apache/arrow-rs/pull/4367) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Have array\_to\_json\_array support MapArray [\#4364](https://github.com/apache/arrow-rs/pull/4364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dadepo](https://github.com/dadepo))
- deprecate: as\_decimal\_array [\#4363](https://github.com/apache/arrow-rs/pull/4363) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add support for FixedSizeList in array\_to\_json\_array [\#4361](https://github.com/apache/arrow-rs/pull/4361) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dadepo](https://github.com/dadepo))
- refact: use as\_primitive in cast.rs test [\#4360](https://github.com/apache/arrow-rs/pull/4360) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- feat\(flight\): add xdbc type info helpers [\#4359](https://github.com/apache/arrow-rs/pull/4359) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([roeap](https://github.com/roeap))
- Minor: float16 to json [\#4358](https://github.com/apache/arrow-rs/pull/4358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Raise TypeError on PyArrow import [\#4316](https://github.com/apache/arrow-rs/pull/4316) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Arrow Cast: Fixed Point Arithmetic for Interval Parsing [\#4291](https://github.com/apache/arrow-rs/pull/4291) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mr-brobot](https://github.com/mr-brobot))
## [41.0.0](https://github.com/apache/arrow-rs/tree/41.0.0) (2023-06-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/40.0.0...41.0.0)

**Breaking changes:**

- Rename list contains kernels to in\_list \(\#4289\) [\#4342](https://github.com/apache/arrow-rs/pull/4342) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move BooleanBufferBuilder and NullBufferBuilder to arrow\_buffer [\#4338](https://github.com/apache/arrow-rs/pull/4338) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add separate row\_count and level\_count to PageMetadata \(\#4321\)  [\#4326](https://github.com/apache/arrow-rs/pull/4326) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Treat legacy TIMSETAMP\_X converted types as UTC [\#4309](https://github.com/apache/arrow-rs/pull/4309) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sergiimk](https://github.com/sergiimk))
- Simplify parquet PageIterator [\#4306](https://github.com/apache/arrow-rs/pull/4306) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add Builder style APIs and docs for `FlightData`,` FlightInfo`, `FlightEndpoint`, `Locaation` and `Ticket` [\#4294](https://github.com/apache/arrow-rs/pull/4294) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Make GenericColumnWriter Send [\#4287](https://github.com/apache/arrow-rs/pull/4287) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: update flight-sql to latest specs [\#4250](https://github.com/apache/arrow-rs/pull/4250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([roeap](https://github.com/roeap))
- feat\(api!\): make ArrowArrayStreamReader Send [\#4232](https://github.com/apache/arrow-rs/pull/4232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))

**Implemented enhancements:**

- Make SerializedRowGroupReader::new\(\) Public [\#4330](https://github.com/apache/arrow-rs/issues/4330) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speed up i256 division and remainder operations [\#4302](https://github.com/apache/arrow-rs/issues/4302) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- export function parquet\_to\_array\_schema\_and\_fields [\#4298](https://github.com/apache/arrow-rs/issues/4298) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FLightSQL: add helpers to create `CommandGetCatalogs`, `CommandGetSchemas`, and `CommandGetTables` requests [\#4295](https://github.com/apache/arrow-rs/issues/4295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Make ColumnWriter Send [\#4286](https://github.com/apache/arrow-rs/issues/4286) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add Builder for `FlightInfo` to make it easier to create new requests [\#4281](https://github.com/apache/arrow-rs/issues/4281) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support Writing/Reading Decimal256 to/from Parquet [\#4264](https://github.com/apache/arrow-rs/issues/4264) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FlightSQL: Add helpers to create `CommandGetSqlInfo` responses \(`SqlInfoValue` and builders\) [\#4256](https://github.com/apache/arrow-rs/issues/4256) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Update flight-sql implementation to latest specs [\#4249](https://github.com/apache/arrow-rs/issues/4249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Make ArrowArrayStreamReader Send [\#4222](https://github.com/apache/arrow-rs/issues/4222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support writing FixedSizeList to Parquet [\#4214](https://github.com/apache/arrow-rs/issues/4214) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Cast between `Intervals` [\#4181](https://github.com/apache/arrow-rs/issues/4181) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Splice Parquet Data [\#4155](https://github.com/apache/arrow-rs/issues/4155) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- CSV Schema More Flexible Timestamp Inference [\#4131](https://github.com/apache/arrow-rs/issues/4131) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Doc for arrow\_flight::sql is missing enums that are Xdbc related [\#4339](https://github.com/apache/arrow-rs/issues/4339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- concat\_batches panics with total\_len \<= bit\_len assertion for records with lists [\#4324](https://github.com/apache/arrow-rs/issues/4324) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect PageMetadata Row Count returned for V1 DataPage [\#4321](https://github.com/apache/arrow-rs/issues/4321) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[parquet\] Not following the spec for TIMESTAMP\_MILLIS legacy converted types [\#4308](https://github.com/apache/arrow-rs/issues/4308) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- ambiguous glob re-exports of contains\_utf8 [\#4289](https://github.com/apache/arrow-rs/issues/4289) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- flight\_sql\_client --header "key: value" yields a value with a leading whitespace [\#4270](https://github.com/apache/arrow-rs/issues/4270) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Casting Timestamp to date is off by one day for dates before 1970-01-01 [\#4211](https://github.com/apache/arrow-rs/issues/4211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Don't infer 16-byte decimal as decimal256 [\#4349](https://github.com/apache/arrow-rs/pull/4349) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix MutableArrayData::extend\_nulls \(\#1230\) [\#4343](https://github.com/apache/arrow-rs/pull/4343) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update FlightSQL metadata locations, names and docs [\#4341](https://github.com/apache/arrow-rs/pull/4341) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- chore: expose Xdbc related FlightSQL enums [\#4340](https://github.com/apache/arrow-rs/pull/4340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([appletreeisyellow](https://github.com/appletreeisyellow))
- Update pyo3 requirement from 0.18 to 0.19 [\#4335](https://github.com/apache/arrow-rs/pull/4335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Skip unnecessary null checks in MutableArrayData [\#4333](https://github.com/apache/arrow-rs/pull/4333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add read parquet by custom rowgroup examples [\#4332](https://github.com/apache/arrow-rs/pull/4332) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sundy-li](https://github.com/sundy-li))
- Make SerializedRowGroupReader::new\(\) public [\#4331](https://github.com/apache/arrow-rs/pull/4331) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([burmecia](https://github.com/burmecia))
- Don't split record across pages \(\#3680\) [\#4327](https://github.com/apache/arrow-rs/pull/4327) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- fix date conversion if timestamp below unixtimestamp [\#4323](https://github.com/apache/arrow-rs/pull/4323) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Short-circuit on exhausted page in skip\_records [\#4320](https://github.com/apache/arrow-rs/pull/4320) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Handle trailing padding when skipping repetition levels \(\#3911\) [\#4319](https://github.com/apache/arrow-rs/pull/4319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use `page_size` consistently, deprecate `pagesize` in parquet WriterProperties [\#4313](https://github.com/apache/arrow-rs/pull/4313) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add roundtrip tests for Decimal256 and fix issues \(\#4264\) [\#4311](https://github.com/apache/arrow-rs/pull/4311) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Expose page-level arrow reader API \(\#4298\) [\#4307](https://github.com/apache/arrow-rs/pull/4307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Speed up i256 division and remainder operations [\#4303](https://github.com/apache/arrow-rs/pull/4303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat\(flight\): support int32\_to\_int32\_list\_map in sql infos [\#4300](https://github.com/apache/arrow-rs/pull/4300) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([roeap](https://github.com/roeap))
- feat\(flight\): add helpers to handle `CommandGetCatalogs`, `CommandGetSchemas`, and `CommandGetTables` requests [\#4296](https://github.com/apache/arrow-rs/pull/4296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([roeap](https://github.com/roeap))
- Improve docs and tests for `SqlInfoList [\#4293](https://github.com/apache/arrow-rs/pull/4293) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- minor: fix arrow\_row docs.rs links [\#4292](https://github.com/apache/arrow-rs/pull/4292) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([roeap](https://github.com/roeap))
- Update proc-macro2 requirement from =1.0.58 to =1.0.59 [\#4290](https://github.com/apache/arrow-rs/pull/4290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Improve `ArrowWriter` memory usage: Buffer Pages in ArrowWriter instead of RecordBatch \(\#3871\) [\#4280](https://github.com/apache/arrow-rs/pull/4280) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Minor: Add more docstrings in arrow-flight [\#4279](https://github.com/apache/arrow-rs/pull/4279) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Add `Debug` impls for `ArrowWriter` and `SerializedFileWriter` [\#4278](https://github.com/apache/arrow-rs/pull/4278) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Expose `RecordBatchWriter` to `arrow` crate [\#4277](https://github.com/apache/arrow-rs/pull/4277) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Update criterion requirement from 0.4 to 0.5 [\#4275](https://github.com/apache/arrow-rs/pull/4275) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add parquet-concat [\#4274](https://github.com/apache/arrow-rs/pull/4274) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Convert FixedSizeListArray to GenericListArray [\#4273](https://github.com/apache/arrow-rs/pull/4273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: support 'Decimal256' for parquet [\#4272](https://github.com/apache/arrow-rs/pull/4272) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Weijun-H](https://github.com/Weijun-H))
- Strip leading whitespace from flight\_sql\_client custom header values [\#4271](https://github.com/apache/arrow-rs/pull/4271) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([mkmik](https://github.com/mkmik))
- Add Append Column API \(\#4155\) [\#4269](https://github.com/apache/arrow-rs/pull/4269) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Derive Default for WriterProperties [\#4268](https://github.com/apache/arrow-rs/pull/4268) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Parquet Reader/writer for fixed-size list arrays  [\#4267](https://github.com/apache/arrow-rs/pull/4267) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dexterduck](https://github.com/dexterduck))
- feat\(flight\): add sql-info helpers [\#4266](https://github.com/apache/arrow-rs/pull/4266) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([roeap](https://github.com/roeap))
- Convert parquet metadata back to builders [\#4265](https://github.com/apache/arrow-rs/pull/4265) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add constructors for FixedSize array types \(\#3879\) [\#4263](https://github.com/apache/arrow-rs/pull/4263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Extract IPC ArrayReader struct [\#4259](https://github.com/apache/arrow-rs/pull/4259) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update object\_store requirement from 0.5 to 0.6 [\#4258](https://github.com/apache/arrow-rs/pull/4258) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support Absolute Timestamps in CSV Schema Inference \(\#4131\) [\#4217](https://github.com/apache/arrow-rs/pull/4217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: cast between `Intervals` [\#4182](https://github.com/apache/arrow-rs/pull/4182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
## [40.0.0](https://github.com/apache/arrow-rs/tree/40.0.0) (2023-05-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/39.0.0...40.0.0)

**Breaking changes:**

- Prefetch page index \(\#4090\) [\#4216](https://github.com/apache/arrow-rs/pull/4216) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add RecordBatchWriter trait and implement it for CSV, JSON, IPC and P… [\#4206](https://github.com/apache/arrow-rs/pull/4206) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Remove powf\_scalar kernel [\#4187](https://github.com/apache/arrow-rs/pull/4187) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow format specification in cast [\#4169](https://github.com/apache/arrow-rs/pull/4169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([parthchandra](https://github.com/parthchandra))

**Implemented enhancements:**

- ObjectStore with\_url Should Handle Path [\#4199](https://github.com/apache/arrow-rs/issues/4199)
- Support `Interval` +/- `Interval` [\#4178](https://github.com/apache/arrow-rs/issues/4178) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[parquet\] add compression info to `print_column_chunk_metadata()` [\#4172](https://github.com/apache/arrow-rs/issues/4172) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Allow cast to take in a format specification [\#4168](https://github.com/apache/arrow-rs/issues/4168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support extended pow arithmetic [\#4166](https://github.com/apache/arrow-rs/issues/4166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Preload page index for async ParquetObjectReader [\#4090](https://github.com/apache/arrow-rs/issues/4090) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Subtracting `Timestamp` from `Timestamp`  should produce a `Duration` \(not `Timestamp`\)  [\#3964](https://github.com/apache/arrow-rs/issues/3964) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Arrow Arithmetic: Subtract timestamps [\#4244](https://github.com/apache/arrow-rs/pull/4244) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mr-brobot](https://github.com/mr-brobot))
- Update proc-macro2 requirement from =1.0.57 to =1.0.58 [\#4236](https://github.com/apache/arrow-rs/pull/4236) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix Nightly Clippy Lints [\#4233](https://github.com/apache/arrow-rs/pull/4233) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: use all primitive types in test\_layouts [\#4229](https://github.com/apache/arrow-rs/pull/4229) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add close method to RecordBatchWriter trait [\#4228](https://github.com/apache/arrow-rs/pull/4228) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Update proc-macro2 requirement from =1.0.56 to =1.0.57 [\#4219](https://github.com/apache/arrow-rs/pull/4219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Feat docs [\#4215](https://github.com/apache/arrow-rs/pull/4215) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Folyd](https://github.com/Folyd))
- feat: Support bitwise and boolean aggregate functions [\#4210](https://github.com/apache/arrow-rs/pull/4210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Document how to sort a RecordBatch [\#4204](https://github.com/apache/arrow-rs/pull/4204) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix incorrect cast Timestamp with Timezone [\#4201](https://github.com/apache/arrow-rs/pull/4201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([aprimadi](https://github.com/aprimadi))
- Add implementation of `RecordBatchReader` for CSV reader [\#4195](https://github.com/apache/arrow-rs/pull/4195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alexandreyc](https://github.com/alexandreyc))
- Add Sliced ListArray test \(\#3748\) [\#4186](https://github.com/apache/arrow-rs/pull/4186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- refactor: simplify can\_cast\_types code. [\#4185](https://github.com/apache/arrow-rs/pull/4185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Minor: support new types in struct\_builder.rs [\#4177](https://github.com/apache/arrow-rs/pull/4177) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- feat: add compression info to print\_column\_chunk\_metadata\(\) [\#4176](https://github.com/apache/arrow-rs/pull/4176) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([SteveLauC](https://github.com/SteveLauC))
## [39.0.0](https://github.com/apache/arrow-rs/tree/39.0.0) (2023-05-05)

[Full Changelog](https://github.com/apache/arrow-rs/compare/38.0.0...39.0.0)

**Breaking changes:**

- Allow creating unbuffered streamreader [\#4165](https://github.com/apache/arrow-rs/pull/4165) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ming08108](https://github.com/ming08108))
- Cleanup ChunkReader \(\#4118\) [\#4156](https://github.com/apache/arrow-rs/pull/4156) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove Type from NativeIndex [\#4146](https://github.com/apache/arrow-rs/pull/4146) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Don't Duplicate Offset Index on RowGroupMetadata [\#4142](https://github.com/apache/arrow-rs/pull/4142) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Return BooleanBuffer from BooleanBufferBuilder [\#4140](https://github.com/apache/arrow-rs/pull/4140) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup CSV schema inference \(\#4129\) \(\#4130\) [\#4133](https://github.com/apache/arrow-rs/pull/4133) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove deprecated parquet ArrowReader [\#4125](https://github.com/apache/arrow-rs/pull/4125) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: construct `StructArray` w/ `FieldRef` [\#4116](https://github.com/apache/arrow-rs/pull/4116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Ignore Field Metadata in equals\_datatype for Dictionary, RunEndEncoded, Map and Union [\#4111](https://github.com/apache/arrow-rs/pull/4111) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add StructArray Constructors \(\#3879\) [\#4064](https://github.com/apache/arrow-rs/pull/4064) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Release 39.0.0 of arrow/arrow-flight/parquet/parquet-derive \(next release after 38.0.0\) [\#4170](https://github.com/apache/arrow-rs/issues/4170) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Fixed point decimal multiplication for DictionaryArray [\#4135](https://github.com/apache/arrow-rs/issues/4135) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove Seek Requirement from CSV ReaderBuilder [\#4130](https://github.com/apache/arrow-rs/issues/4130) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Inconsistent CSV Inference and Parsing DateTime Handling [\#4129](https://github.com/apache/arrow-rs/issues/4129) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support accessing ipc Reader/Writer inner by reference [\#4121](https://github.com/apache/arrow-rs/issues/4121)
- Add Type Declarations for All Primitive Tensors and Buffer Builders [\#4112](https://github.com/apache/arrow-rs/issues/4112) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `Interval + Timestamp` and `Interval + Date` in addition to `Timestamp + Interval` and `Interval + Date` [\#4094](https://github.com/apache/arrow-rs/issues/4094) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Enable setting FlightDescriptor on FlightDataEncoderBuilder [\#3855](https://github.com/apache/arrow-rs/issues/3855) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Parquet Page Index Reader Assumes Consecutive Offsets [\#4149](https://github.com/apache/arrow-rs/issues/4149) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Equality of nested data types [\#4110](https://github.com/apache/arrow-rs/issues/4110) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Improve Documentation of Parquet ChunkReader [\#4118](https://github.com/apache/arrow-rs/issues/4118)

**Closed issues:**

- add specific error log for empty JSON array [\#4105](https://github.com/apache/arrow-rs/issues/4105) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Prep for 39.0.0 [\#4171](https://github.com/apache/arrow-rs/pull/4171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Support Compression in parquet-fromcsv [\#4160](https://github.com/apache/arrow-rs/pull/4160) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([suxiaogang223](https://github.com/suxiaogang223))
- feat: support bitwise shift left/right with scalars [\#4159](https://github.com/apache/arrow-rs/pull/4159) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Cleanup reading page index \(\#4149\) \(\#4090\) [\#4151](https://github.com/apache/arrow-rs/pull/4151) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: support `bitwise` shift left/right [\#4148](https://github.com/apache/arrow-rs/pull/4148) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Don't hardcode port in FlightSQL tests [\#4145](https://github.com/apache/arrow-rs/pull/4145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Better flight SQL example codes [\#4144](https://github.com/apache/arrow-rs/pull/4144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([sundy-li](https://github.com/sundy-li))
- chore: clean the code by using `as_primitive` [\#4143](https://github.com/apache/arrow-rs/pull/4143) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- docs: fix the wrong ln command in CONTRIBUTING.md [\#4139](https://github.com/apache/arrow-rs/pull/4139) ([SteveLauC](https://github.com/SteveLauC))
- Infer Float64 for JSON Numerics Beyond Bounds of i64 [\#4138](https://github.com/apache/arrow-rs/pull/4138) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SteveLauC](https://github.com/SteveLauC))
- Support fixed point multiplication for DictionaryArray of Decimals [\#4136](https://github.com/apache/arrow-rs/pull/4136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make arrow\_json::ReaderBuilder method names consistent [\#4128](https://github.com/apache/arrow-rs/pull/4128) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add get\_{ref, mut} to arrow\_ipc Reader and Writer [\#4122](https://github.com/apache/arrow-rs/pull/4122) ([sticnarf](https://github.com/sticnarf))
- feat: support `Interval` + `Timestamp` and `Interval` + `Date` [\#4117](https://github.com/apache/arrow-rs/pull/4117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Support NullArray in JSON Reader [\#4114](https://github.com/apache/arrow-rs/pull/4114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jiangzhx](https://github.com/jiangzhx))
- Add Type Declarations for All Primitive Tensors and Buffer Builders [\#4113](https://github.com/apache/arrow-rs/pull/4113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Update regex-syntax requirement from 0.6.27 to 0.7.1 [\#4107](https://github.com/apache/arrow-rs/pull/4107) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: set FlightDescriptor on FlightDataEncoderBuilder [\#4101](https://github.com/apache/arrow-rs/pull/4101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Weijun-H](https://github.com/Weijun-H))
- optimize cast for same decimal type and same scale [\#4088](https://github.com/apache/arrow-rs/pull/4088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))

## [38.0.0](https://github.com/apache/arrow-rs/tree/38.0.0) (2023-04-21)

[Full Changelog](https://github.com/apache/arrow-rs/compare/37.0.0...38.0.0)

**Breaking changes:**

- Remove DataType from PrimitiveArray constructors [\#4098](https://github.com/apache/arrow-rs/pull/4098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Into\<Arc\<str\>\> for PrimitiveArray::with\_timezone [\#4097](https://github.com/apache/arrow-rs/pull/4097) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Store StructArray entries in MapArray [\#4085](https://github.com/apache/arrow-rs/pull/4085) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add DictionaryArray Constructors \(\#3879\)  [\#4068](https://github.com/apache/arrow-rs/pull/4068) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Relax JSON schema inference generics [\#4063](https://github.com/apache/arrow-rs/pull/4063) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove ArrayData from Array \(\#3880\) [\#4061](https://github.com/apache/arrow-rs/pull/4061) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add CommandGetXdbcTypeInfo to Flight SQL Server [\#4055](https://github.com/apache/arrow-rs/pull/4055) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([c-thiel](https://github.com/c-thiel))
- Remove old JSON Reader and Decoder \(\#3610\) [\#4052](https://github.com/apache/arrow-rs/pull/4052) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use BufRead for JSON Schema Inference [\#4041](https://github.com/apache/arrow-rs/pull/4041) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([WenyXu](https://github.com/WenyXu))

**Implemented enhancements:**

- Support dyn\_compare\_scalar for Decimal256 [\#4083](https://github.com/apache/arrow-rs/issues/4083) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Better JSON Reader Error Messages [\#4076](https://github.com/apache/arrow-rs/issues/4076) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Additional data type groups [\#4056](https://github.com/apache/arrow-rs/issues/4056) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Async JSON reader [\#4043](https://github.com/apache/arrow-rs/issues/4043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Field::contains Should Recurse into DataType [\#4029](https://github.com/apache/arrow-rs/issues/4029) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Prevent UnionArray with Repeated Type IDs [\#3982](https://github.com/apache/arrow-rs/issues/3982) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support  `Timestamp` `+`/`-` `Interval` types [\#3963](https://github.com/apache/arrow-rs/issues/3963) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- First-Class Array Abstractions [\#3880](https://github.com/apache/arrow-rs/issues/3880) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Update readme to remove reference to Jira [\#4091](https://github.com/apache/arrow-rs/issues/4091)
- OffsetBuffer::new Rejects 0 Offsets [\#4066](https://github.com/apache/arrow-rs/issues/4066) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet AsyncArrowWriter not shutting down inner async writer.  [\#4058](https://github.com/apache/arrow-rs/issues/4058) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Flight SQL Server missing command type.googleapis.com/arrow.flight.protocol.sql.CommandGetXdbcTypeInfo [\#4054](https://github.com/apache/arrow-rs/issues/4054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- RawJsonReader Errors with Empty Schema [\#4053](https://github.com/apache/arrow-rs/issues/4053) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RawJsonReader Integer Truncation [\#4049](https://github.com/apache/arrow-rs/issues/4049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sparse UnionArray Equality Incorrect Offset Handling [\#4044](https://github.com/apache/arrow-rs/issues/4044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Write blog about improvements in JSON and CSV processing [\#4062](https://github.com/apache/arrow-rs/issues/4062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Parquet reader of Int96 columns and coercion to timestamps [\#4075](https://github.com/apache/arrow-rs/issues/4075)
- Serializing timestamp from int \(json raw decoder\) [\#4069](https://github.com/apache/arrow-rs/issues/4069) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting to/from Interval and Duration [\#3998](https://github.com/apache/arrow-rs/issues/3998) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix Docs Typos [\#4100](https://github.com/apache/arrow-rs/pull/4100) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([rnarkk](https://github.com/rnarkk))
- Update tonic-build requirement from =0.9.1 to =0.9.2 [\#4099](https://github.com/apache/arrow-rs/pull/4099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Increase minimum chrono version to 0.4.24 [\#4093](https://github.com/apache/arrow-rs/pull/4093) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Simplify reference to GitHub issues [\#4092](https://github.com/apache/arrow-rs/pull/4092) ([bkmgit](https://github.com/bkmgit))
- \[Minor\]: Add `Hash` trait to SortOptions. [\#4089](https://github.com/apache/arrow-rs/pull/4089) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mustafasrepo](https://github.com/mustafasrepo))
- Include byte offsets in parquet-layout [\#4086](https://github.com/apache/arrow-rs/pull/4086) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- feat: Support dyn\_compare\_scalar for Decimal256 [\#4084](https://github.com/apache/arrow-rs/pull/4084) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add ByteArray constructors \(\#3879\)  [\#4081](https://github.com/apache/arrow-rs/pull/4081) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.11.8 to =0.11.9 [\#4080](https://github.com/apache/arrow-rs/pull/4080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Improve JSON decoder errors \(\#4076\) [\#4079](https://github.com/apache/arrow-rs/pull/4079) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix Timestamp Numeric Truncation in JSON Reader [\#4074](https://github.com/apache/arrow-rs/pull/4074) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Serialize numeric to tape \(\#4069\) [\#4073](https://github.com/apache/arrow-rs/pull/4073) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Prevent UnionArray with Repeated Type IDs [\#4070](https://github.com/apache/arrow-rs/pull/4070) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Add PrimitiveArray::try\_new \(\#3879\) [\#4067](https://github.com/apache/arrow-rs/pull/4067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ListArray Constructors \(\#3879\)  [\#4065](https://github.com/apache/arrow-rs/pull/4065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Shutdown parquet async writer [\#4059](https://github.com/apache/arrow-rs/pull/4059) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kindly](https://github.com/kindly))
- feat: additional data type groups [\#4057](https://github.com/apache/arrow-rs/pull/4057) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Fix precision loss in Raw JSON decoder \(\#4049\) [\#4051](https://github.com/apache/arrow-rs/pull/4051) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use lexical\_core in CSV and JSON parser \(~25% faster\) [\#4050](https://github.com/apache/arrow-rs/pull/4050) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add offsets accessors to variable length arrays \(\#3879\) [\#4048](https://github.com/apache/arrow-rs/pull/4048) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Document Async decoder usage \(\#4043\) \(\#78\) [\#4046](https://github.com/apache/arrow-rs/pull/4046) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix sparse union array equality \(\#4044\) [\#4045](https://github.com/apache/arrow-rs/pull/4045) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: DataType::contains support nested type [\#4042](https://github.com/apache/arrow-rs/pull/4042) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- feat: Support Timestamp +/- Interval types [\#4038](https://github.com/apache/arrow-rs/pull/4038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Fix object\_store CI [\#4037](https://github.com/apache/arrow-rs/pull/4037) ([tustvold](https://github.com/tustvold))
- feat: cast from/to interval and duration [\#4020](https://github.com/apache/arrow-rs/pull/4020) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))

## [37.0.0](https://github.com/apache/arrow-rs/tree/37.0.0) (2023-04-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/36.0.0...37.0.0)

**Breaking changes:**

- Fix timestamp handling in cast kernel \(\#1936\) \(\#4033\) [\#4034](https://github.com/apache/arrow-rs/pull/4034) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update tonic 0.9.1 [\#4011](https://github.com/apache/arrow-rs/pull/4011) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Use FieldRef in DataType \(\#3955\) [\#3983](https://github.com/apache/arrow-rs/pull/3983) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Store Timezone as Arc\<str\> [\#3976](https://github.com/apache/arrow-rs/pull/3976) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Panic instead of discarding nulls converting StructArray to RecordBatch -  \(\#3951\) [\#3953](https://github.com/apache/arrow-rs/pull/3953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix\(flight\_sql\): PreparedStatement has no token for auth. [\#3948](https://github.com/apache/arrow-rs/pull/3948) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([youngsofun](https://github.com/youngsofun))
- Add Strongly Typed Array Slice \(\#3929\) [\#3930](https://github.com/apache/arrow-rs/pull/3930) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Zero-Copy Conversion between Vec and MutableBuffer [\#3920](https://github.com/apache/arrow-rs/pull/3920) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Support Decimals cast to Utf8/LargeUtf [\#3991](https://github.com/apache/arrow-rs/issues/3991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Date32/Date64 minus Interval [\#3962](https://github.com/apache/arrow-rs/issues/3962) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Reduce Cloning of Field [\#3955](https://github.com/apache/arrow-rs/issues/3955) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support Deserializing Serde DataTypes to Arrow [\#3949](https://github.com/apache/arrow-rs/issues/3949) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add multiply\_fixed\_point [\#3946](https://github.com/apache/arrow-rs/issues/3946) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Strongly Typed Array Slicing [\#3929](https://github.com/apache/arrow-rs/issues/3929) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make it easier to match FlightSQL messages [\#3874](https://github.com/apache/arrow-rs/issues/3874) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support Casting Between Binary / LargeBinary and FixedSizeBinary [\#3826](https://github.com/apache/arrow-rs/issues/3826) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Incorrect Overflow Casting String to Timestamp [\#4033](https://github.com/apache/arrow-rs/issues/4033)
- f16::ZERO and f16::ONE are mixed up [\#4016](https://github.com/apache/arrow-rs/issues/4016) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Handle overflow precision when casting from integer to decimal [\#3995](https://github.com/apache/arrow-rs/issues/3995) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- PrimitiveDictionaryBuilder.finish should use actual value type [\#3971](https://github.com/apache/arrow-rs/issues/3971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordBatch From StructArray Silently Discards Nulls [\#3952](https://github.com/apache/arrow-rs/issues/3952) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- I256 Checked Subtraction Overflows for i256::MINUS\_ONE [\#3942](https://github.com/apache/arrow-rs/issues/3942) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- I256 Checked Multiply Overflows for i256::MIN [\#3941](https://github.com/apache/arrow-rs/issues/3941) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Remove non-existent `js` feature from README [\#4000](https://github.com/apache/arrow-rs/issues/4000) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support take on MapArray [\#3875](https://github.com/apache/arrow-rs/issues/3875) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Prep for 37.0.0 [\#4031](https://github.com/apache/arrow-rs/pull/4031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Add RecordBatch::with\_schema [\#4028](https://github.com/apache/arrow-rs/pull/4028) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Only require compatible batch schema in ArrowWriter [\#4027](https://github.com/apache/arrow-rs/pull/4027) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add Fields::contains [\#4026](https://github.com/apache/arrow-rs/pull/4026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: add methods "is\_positive" and "signum" to i256 [\#4024](https://github.com/apache/arrow-rs/pull/4024) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Deprecate Array::data \(\#3880\) [\#4019](https://github.com/apache/arrow-rs/pull/4019) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add tests for ArrowNativeTypeOp [\#4018](https://github.com/apache/arrow-rs/pull/4018) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- fix: f16::ZERO and f16::ONE are mixed up [\#4017](https://github.com/apache/arrow-rs/pull/4017) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Minor: Float16Tensor [\#4013](https://github.com/apache/arrow-rs/pull/4013) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add FlightSQL module docs and links to `arrow-flight` crates [\#4012](https://github.com/apache/arrow-rs/pull/4012) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Update proc-macro2 requirement from =1.0.54 to =1.0.56 [\#4008](https://github.com/apache/arrow-rs/pull/4008) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Cleanup Primitive take [\#4006](https://github.com/apache/arrow-rs/pull/4006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate combine\_option\_bitmap [\#4005](https://github.com/apache/arrow-rs/pull/4005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: add tests for BooleanBuffer [\#4004](https://github.com/apache/arrow-rs/pull/4004) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- feat: support to read/write customized metadata in ipc files [\#4003](https://github.com/apache/arrow-rs/pull/4003) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([framlog](https://github.com/framlog))
- Cleanup more uses of Array::data \(\#3880\) [\#4002](https://github.com/apache/arrow-rs/pull/4002) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove js feature from README [\#4001](https://github.com/apache/arrow-rs/pull/4001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([akazukin5151](https://github.com/akazukin5151))
- feat: add the implementation BitXor to BooleanBuffer [\#3997](https://github.com/apache/arrow-rs/pull/3997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Handle precision overflow when casting from integer to decimal [\#3996](https://github.com/apache/arrow-rs/pull/3996) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support CAST from Decimal datatype to String [\#3994](https://github.com/apache/arrow-rs/pull/3994) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Add Field Constructors for Complex Fields [\#3992](https://github.com/apache/arrow-rs/pull/3992) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- fix: remove unused type parameters. [\#3986](https://github.com/apache/arrow-rs/pull/3986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([youngsofun](https://github.com/youngsofun))
- Add UnionFields \(\#3955\) [\#3981](https://github.com/apache/arrow-rs/pull/3981) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup Fields Serde [\#3980](https://github.com/apache/arrow-rs/pull/3980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support Rust structures --\> `RecordBatch` by adding `Serde` support to `RawDecoder` \(\#3949\) [\#3979](https://github.com/apache/arrow-rs/pull/3979) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Convert string\_to\_timestamp\_nanos to doctest [\#3978](https://github.com/apache/arrow-rs/pull/3978) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix documentation of string\_to\_timestamp\_nanos [\#3977](https://github.com/apache/arrow-rs/pull/3977) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([byteink](https://github.com/byteink))
- add Date32/Date64 support to subtract\_dyn [\#3974](https://github.com/apache/arrow-rs/pull/3974) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([SinanGncgl](https://github.com/SinanGncgl))
- PrimitiveDictionaryBuilder.finish should use actual value type [\#3972](https://github.com/apache/arrow-rs/pull/3972) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.53 to =1.0.54 [\#3968](https://github.com/apache/arrow-rs/pull/3968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Async writer tweaks [\#3967](https://github.com/apache/arrow-rs/pull/3967) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix reading ipc files with unordered projections [\#3966](https://github.com/apache/arrow-rs/pull/3966) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([framlog](https://github.com/framlog))
- Add Fields abstraction \(\#3955\) [\#3965](https://github.com/apache/arrow-rs/pull/3965) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- feat: cast between `Binary`/`LargeBinary` and `FixedSizeBinary` [\#3961](https://github.com/apache/arrow-rs/pull/3961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- feat: support async writer \(\#1269\) [\#3957](https://github.com/apache/arrow-rs/pull/3957) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ShiKaiWi](https://github.com/ShiKaiWi))
- Add ListBuilder::append\_value \(\#3949\) [\#3954](https://github.com/apache/arrow-rs/pull/3954) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve array builder documentation \(\#3949\) [\#3951](https://github.com/apache/arrow-rs/pull/3951) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster i256 parsing [\#3950](https://github.com/apache/arrow-rs/pull/3950) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add multiply\_fixed\_point [\#3945](https://github.com/apache/arrow-rs/pull/3945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat: enable metadata import/export through C data interface [\#3944](https://github.com/apache/arrow-rs/pull/3944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Fix checked i256 arithmetic \(\#3942\) \(\#3941\) [\#3943](https://github.com/apache/arrow-rs/pull/3943) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Avoid memory copies in take\_list [\#3940](https://github.com/apache/arrow-rs/pull/3940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster decimal parsing \(30-60%\) [\#3939](https://github.com/apache/arrow-rs/pull/3939) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Fix: FlightSqlClient panic when execute\_update. [\#3938](https://github.com/apache/arrow-rs/pull/3938) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([youngsofun](https://github.com/youngsofun))
- Cleanup row count handling in JSON writer [\#3934](https://github.com/apache/arrow-rs/pull/3934) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add typed buffers to UnionArray \(\#3880\) [\#3933](https://github.com/apache/arrow-rs/pull/3933) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add take for MapArray [\#3925](https://github.com/apache/arrow-rs/pull/3925) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Deprecate Array::data\_ref \(\#3880\) [\#3923](https://github.com/apache/arrow-rs/pull/3923) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Zero-copy conversion from Vec to PrimitiveArray [\#3917](https://github.com/apache/arrow-rs/pull/3917) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Add Commands enum to decode prost messages to strong type [\#3887](https://github.com/apache/arrow-rs/pull/3887) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([stuartcarnie](https://github.com/stuartcarnie))
## [36.0.0](https://github.com/apache/arrow-rs/tree/36.0.0) (2023-03-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/35.0.0...36.0.0)

**Breaking changes:**

- Use dyn Array in sort kernels [\#3931](https://github.com/apache/arrow-rs/pull/3931) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Enforce struct nullability in JSON raw reader \(\#3900\) \(\#3904\) [\#3906](https://github.com/apache/arrow-rs/pull/3906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return ScalarBuffer from PrimitiveArray::values \(\#3879\) [\#3896](https://github.com/apache/arrow-rs/pull/3896) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use BooleanBuffer in BooleanArray \(\#3879\) [\#3895](https://github.com/apache/arrow-rs/pull/3895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Seal ArrowPrimitiveType [\#3882](https://github.com/apache/arrow-rs/pull/3882) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support compression levels [\#3847](https://github.com/apache/arrow-rs/pull/3847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([spebern](https://github.com/spebern))

**Implemented enhancements:**

- Improve speed of parsing string to Times [\#3919](https://github.com/apache/arrow-rs/issues/3919) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- feat: add comparison/sort support for Float16 [\#3914](https://github.com/apache/arrow-rs/issues/3914)
- Pinned version in arrow-flight's build-dependencies are causing conflicts [\#3876](https://github.com/apache/arrow-rs/issues/3876)
- Add compression options \(levels\) [\#3844](https://github.com/apache/arrow-rs/issues/3844) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use Unsigned Integer for Fixed Size DataType [\#3815](https://github.com/apache/arrow-rs/issues/3815)
- Common trait for RecordBatch and StructArray [\#3764](https://github.com/apache/arrow-rs/issues/3764) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow precision loss on multiplying decimal arrays [\#3689](https://github.com/apache/arrow-rs/issues/3689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Raw JSON Reader Allows Non-Nullable Struct Children to Contain Nulls [\#3904](https://github.com/apache/arrow-rs/issues/3904)
- Nullable field with nested not nullable map in json [\#3900](https://github.com/apache/arrow-rs/issues/3900)
- parquet\_derive doesn't support Vec\<u8\> [\#3864](https://github.com/apache/arrow-rs/issues/3864) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- \[REGRESSION\] Parsing timestamps with lower case time separator [\#3863](https://github.com/apache/arrow-rs/issues/3863) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[REGRESSION\] Parsing timestamps with leap seconds [\#3861](https://github.com/apache/arrow-rs/issues/3861) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[REGRESSION\] Parsing timestamps with fractional seconds / microseconds / milliseconds / nanoseconds [\#3859](https://github.com/apache/arrow-rs/issues/3859) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CSV Reader Doesn't set Timezone [\#3841](https://github.com/apache/arrow-rs/issues/3841)
- PyArrowConvert Leaks Memory [\#3683](https://github.com/apache/arrow-rs/issues/3683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Derive RunArray Clone [\#3932](https://github.com/apache/arrow-rs/pull/3932) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move protoc generation to binary crate, unpin prost/tonic build \(\#3876\) [\#3927](https://github.com/apache/arrow-rs/pull/3927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Fix JSON Temporal Encoding of Multiple Batches [\#3924](https://github.com/apache/arrow-rs/pull/3924) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- Cleanup uses of Array::data\_ref \(\#3880\) [\#3918](https://github.com/apache/arrow-rs/pull/3918) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support microsecond and nanosecond in interval parsing [\#3916](https://github.com/apache/arrow-rs/pull/3916) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- feat: add comparison/sort support for Float16 [\#3915](https://github.com/apache/arrow-rs/pull/3915) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([izveigor](https://github.com/izveigor))
- Add AsArray trait for more ergonomic downcasting [\#3912](https://github.com/apache/arrow-rs/pull/3912) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add OffsetBuffer::new [\#3910](https://github.com/apache/arrow-rs/pull/3910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add PrimitiveArray::new \(\#3879\) [\#3909](https://github.com/apache/arrow-rs/pull/3909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support timezones in CSV reader \(\#3841\) [\#3908](https://github.com/apache/arrow-rs/pull/3908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve ScalarBuffer debug output [\#3907](https://github.com/apache/arrow-rs/pull/3907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.52 to =1.0.53 [\#3905](https://github.com/apache/arrow-rs/pull/3905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Re-export parquet compression level structs [\#3903](https://github.com/apache/arrow-rs/pull/3903) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix parsing timestamps of exactly 32 characters [\#3902](https://github.com/apache/arrow-rs/pull/3902) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add iterators to BooleanBuffer and NullBuffer [\#3901](https://github.com/apache/arrow-rs/pull/3901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Array equality for &dyn Array \(\#3880\) [\#3899](https://github.com/apache/arrow-rs/pull/3899) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BooleanArray::new \(\#3879\) [\#3898](https://github.com/apache/arrow-rs/pull/3898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Revert structured ArrayData \(\#3877\) [\#3894](https://github.com/apache/arrow-rs/pull/3894) ([tustvold](https://github.com/tustvold))
- Fix pyarrow memory leak \(\#3683\) [\#3893](https://github.com/apache/arrow-rs/pull/3893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: add examples for `ListBuilder` and `GenericListBuilder` [\#3891](https://github.com/apache/arrow-rs/pull/3891) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update syn requirement from 1.0 to 2.0 [\#3890](https://github.com/apache/arrow-rs/pull/3890) ([dependabot[bot]](https://github.com/apps/dependabot))
- Use of `mul_checked` to avoid silent overflow in interval arithmetic [\#3886](https://github.com/apache/arrow-rs/pull/3886) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Flesh out NullBuffer abstraction \(\#3880\) [\#3885](https://github.com/apache/arrow-rs/pull/3885) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement Bit Operations for i256 [\#3884](https://github.com/apache/arrow-rs/pull/3884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Flatten arrow\_buffer [\#3883](https://github.com/apache/arrow-rs/pull/3883) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add Array::to\_data and Array::nulls \(\#3880\) [\#3881](https://github.com/apache/arrow-rs/pull/3881) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Added support for byte vectors and slices to parquet\_derive \(\#3864\) [\#3878](https://github.com/apache/arrow-rs/pull/3878) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([waymost](https://github.com/waymost))
- chore: remove LevelDecoder [\#3872](https://github.com/apache/arrow-rs/pull/3872) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Weijun-H](https://github.com/Weijun-H))
- Parse timestamps with leap seconds \(\#3861\) [\#3862](https://github.com/apache/arrow-rs/pull/3862) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster time parsing \(~93% faster\) [\#3860](https://github.com/apache/arrow-rs/pull/3860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Parse timestamps with arbitrary seconds fraction [\#3858](https://github.com/apache/arrow-rs/pull/3858) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BitIterator [\#3856](https://github.com/apache/arrow-rs/pull/3856) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve decimal parsing performance [\#3854](https://github.com/apache/arrow-rs/pull/3854) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Update proc-macro2 requirement from =1.0.51 to =1.0.52 [\#3853](https://github.com/apache/arrow-rs/pull/3853) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update bitflags requirement from 1.2.1 to 2.0.0 [\#3852](https://github.com/apache/arrow-rs/pull/3852) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add offset pushdown to parquet [\#3848](https://github.com/apache/arrow-rs/pull/3848) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add timezone support to JSON reader [\#3845](https://github.com/apache/arrow-rs/pull/3845) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Allow precision loss on multiplying decimal arrays [\#3690](https://github.com/apache/arrow-rs/pull/3690) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

## [35.0.0](https://github.com/apache/arrow-rs/tree/35.0.0) (2023-03-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/34.0.0...35.0.0)

**Breaking changes:**

- Add RunEndBuffer \(\#1799\) [\#3817](https://github.com/apache/arrow-rs/pull/3817) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Restrict DictionaryArray to ArrowDictionaryKeyType [\#3813](https://github.com/apache/arrow-rs/pull/3813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- refactor: assorted `FlightSqlServiceClient` improvements [\#3788](https://github.com/apache/arrow-rs/pull/3788) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- minor: make Parquet CLI input args consistent [\#3786](https://github.com/apache/arrow-rs/pull/3786) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([XinyuZeng](https://github.com/XinyuZeng))
- Return Buffers from ArrayData::buffers instead of slice \(\#1799\) [\#3783](https://github.com/apache/arrow-rs/pull/3783) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use NullBuffer in ArrayData \(\#3775\) [\#3778](https://github.com/apache/arrow-rs/pull/3778) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Support timestamp/time and date types in json decoder [\#3834](https://github.com/apache/arrow-rs/issues/3834) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support decoding decimals in new raw json decoder [\#3819](https://github.com/apache/arrow-rs/issues/3819) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Timezone Aware Timestamp Parsing [\#3794](https://github.com/apache/arrow-rs/issues/3794) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Preallocate buffers for FixedSizeBinary array creation [\#3792](https://github.com/apache/arrow-rs/issues/3792) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make Parquet CLI args consistent [\#3785](https://github.com/apache/arrow-rs/issues/3785) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Creates PrimitiveDictionaryBuilder from provided keys and values builders [\#3776](https://github.com/apache/arrow-rs/issues/3776) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use NullBuffer in ArrayData  [\#3775](https://github.com/apache/arrow-rs/issues/3775) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support unary\_dict\_mut in arth  [\#3710](https://github.com/apache/arrow-rs/issues/3710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support cast \<\> String to interval [\#3643](https://github.com/apache/arrow-rs/issues/3643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Zero-Copy Conversion from Vec to/from MutableBuffer [\#3516](https://github.com/apache/arrow-rs/issues/3516) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Timestamp Unit Casts are Unchecked [\#3833](https://github.com/apache/arrow-rs/issues/3833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- regexp\_match skips first match when returning match [\#3803](https://github.com/apache/arrow-rs/issues/3803) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast to timestamp with time zone returns timestamp [\#3800](https://github.com/apache/arrow-rs/issues/3800) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Schema-level metadata is not encoded in Flight responses [\#3779](https://github.com/apache/arrow-rs/issues/3779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Closed issues:**

- FlightSQL CLI client: simple test [\#3814](https://github.com/apache/arrow-rs/issues/3814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Merged pull requests:**

- refactor: timestamp overflow check [\#3840](https://github.com/apache/arrow-rs/pull/3840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Prep for 35.0.0 [\#3836](https://github.com/apache/arrow-rs/pull/3836) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Support timestamp/time and date json decoding [\#3835](https://github.com/apache/arrow-rs/pull/3835) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Make dictionary preservation optional in row encoding [\#3831](https://github.com/apache/arrow-rs/pull/3831) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move prettyprint to arrow-cast [\#3828](https://github.com/apache/arrow-rs/pull/3828) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Support decoding decimals in raw decoder [\#3820](https://github.com/apache/arrow-rs/pull/3820) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Add ArrayDataLayout, port validation \(\#1799\) [\#3818](https://github.com/apache/arrow-rs/pull/3818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- test: add test for FlightSQL CLI client [\#3816](https://github.com/apache/arrow-rs/pull/3816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Add regexp\_match docs [\#3812](https://github.com/apache/arrow-rs/pull/3812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix: Ensure Flight schema includes parent metadata [\#3811](https://github.com/apache/arrow-rs/pull/3811) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([stuartcarnie](https://github.com/stuartcarnie))
- fix: regexp\_match skips first match [\#3807](https://github.com/apache/arrow-rs/pull/3807) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- fix: change uft8 to timestamp with timezone [\#3806](https://github.com/apache/arrow-rs/pull/3806) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Support reading decimal arrays from json [\#3805](https://github.com/apache/arrow-rs/pull/3805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([spebern](https://github.com/spebern))
- Add unary\_dict\_mut [\#3804](https://github.com/apache/arrow-rs/pull/3804) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Faster timestamp parsing \(~70-90% faster\) [\#3801](https://github.com/apache/arrow-rs/pull/3801) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add concat\_elements\_bytes [\#3798](https://github.com/apache/arrow-rs/pull/3798) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Timezone aware timestamp parsing \(\#3794\) [\#3795](https://github.com/apache/arrow-rs/pull/3795) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preallocate buffers for FixedSizeBinary array creation [\#3793](https://github.com/apache/arrow-rs/pull/3793) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
- feat: simple flight sql CLI client [\#3789](https://github.com/apache/arrow-rs/pull/3789) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([crepererum](https://github.com/crepererum))
- Creates PrimitiveDictionaryBuilder from provided keys and values builders [\#3777](https://github.com/apache/arrow-rs/pull/3777) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- ArrayData Enumeration for Remaining Layouts [\#3769](https://github.com/apache/arrow-rs/pull/3769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.11.7 to =0.11.8 [\#3767](https://github.com/apache/arrow-rs/pull/3767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Implement concat\_elements\_dyn kernel [\#3763](https://github.com/apache/arrow-rs/pull/3763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Support for casting `Utf8` and `LargeUtf8` --\>  `Interval` [\#3762](https://github.com/apache/arrow-rs/pull/3762) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- into\_inner\(\) for CSV Writer [\#3759](https://github.com/apache/arrow-rs/pull/3759) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Weijun-H](https://github.com/Weijun-H))
- Zero-copy Vec conversion \(\#3516\) \(\#1176\) [\#3756](https://github.com/apache/arrow-rs/pull/3756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ArrayData Enumeration for Primitive, Binary and UTF8 [\#3749](https://github.com/apache/arrow-rs/pull/3749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `into_primitive_dict_builder` to `DictionaryArray` [\#3715](https://github.com/apache/arrow-rs/pull/3715) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

## [34.0.0](https://github.com/apache/arrow-rs/tree/34.0.0) (2023-02-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/33.0.0...34.0.0)

**Breaking changes:**

- Infer 2020-03-19 00:00:00 as timestamp not Date64 in CSV \(\#3744\) [\#3746](https://github.com/apache/arrow-rs/pull/3746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement fallible streams for `FlightClient::do_put` [\#3464](https://github.com/apache/arrow-rs/pull/3464) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Implemented enhancements:**

- Support casting string to timestamp with microsecond resolution [\#3751](https://github.com/apache/arrow-rs/issues/3751)
- Add datatime/interval/duration into comparison kernels [\#3729](https://github.com/apache/arrow-rs/issues/3729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ! \(not\) operator overload for SortOptions [\#3726](https://github.com/apache/arrow-rs/issues/3726) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet: convert Bytes to ByteArray directly [\#3719](https://github.com/apache/arrow-rs/issues/3719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement simple RecordBatchReader [\#3704](https://github.com/apache/arrow-rs/issues/3704)
- Is possible to implement GenericListArray::from\_iter ? [\#3702](https://github.com/apache/arrow-rs/issues/3702)
- `take_run` improvements [\#3701](https://github.com/apache/arrow-rs/issues/3701) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `as_mut_any`  in Array trait  [\#3655](https://github.com/apache/arrow-rs/issues/3655)
- `Array` --\> `Display` formatter that supports more options and is configurable [\#3638](https://github.com/apache/arrow-rs/issues/3638) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-csv: support decimal256 [\#3474](https://github.com/apache/arrow-rs/issues/3474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- CSV reader infers Date64 type for fields like "2020-03-19 00:00:00" that it can't parse to Date64 [\#3744](https://github.com/apache/arrow-rs/issues/3744) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Update to 34.0.0 and update changelog  [\#3757](https://github.com/apache/arrow-rs/pull/3757) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Update MIRI for split crates \(\#2594\) [\#3754](https://github.com/apache/arrow-rs/pull/3754) ([tustvold](https://github.com/tustvold))
- Update prost-build requirement from =0.11.6 to =0.11.7 [\#3753](https://github.com/apache/arrow-rs/pull/3753) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Enable casting of string to timestamp with microsecond resolution [\#3752](https://github.com/apache/arrow-rs/pull/3752) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Use Typed Buffers in Arrays \(\#1811\) \(\#1176\) [\#3743](https://github.com/apache/arrow-rs/pull/3743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup arithmetic kernel type constraints [\#3739](https://github.com/apache/arrow-rs/pull/3739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Make dictionary kernels optional for comparison benchmark [\#3738](https://github.com/apache/arrow-rs/pull/3738) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support String Coercion in Raw JSON Reader [\#3736](https://github.com/apache/arrow-rs/pull/3736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rguerreiromsft](https://github.com/rguerreiromsft))
- replace for loop by try\_for\_each [\#3734](https://github.com/apache/arrow-rs/pull/3734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([suxiaogang223](https://github.com/suxiaogang223))
- feat: implement generic record batch reader [\#3733](https://github.com/apache/arrow-rs/pull/3733) ([wjones127](https://github.com/wjones127))
- \[minor\] fix doc test fail [\#3732](https://github.com/apache/arrow-rs/pull/3732) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add datetime/interval/duration into dyn scalar comparison [\#3730](https://github.com/apache/arrow-rs/pull/3730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Using Borrow\<Value\> on infer\_json\_schema\_from\_iterator [\#3728](https://github.com/apache/arrow-rs/pull/3728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([rguerreiromsft](https://github.com/rguerreiromsft))
- Not operator overload for SortOptions [\#3727](https://github.com/apache/arrow-rs/pull/3727) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([berkaysynnada](https://github.com/berkaysynnada))
- fix: encoding batch with no columns [\#3724](https://github.com/apache/arrow-rs/pull/3724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([wangrunji0408](https://github.com/wangrunji0408))
- feat: impl `Ord`/`PartialOrd` for `SortOptions` [\#3723](https://github.com/apache/arrow-rs/pull/3723) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add From\<Bytes\> for ByteArray [\#3720](https://github.com/apache/arrow-rs/pull/3720) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Deprecate old JSON reader \(\#3610\) [\#3718](https://github.com/apache/arrow-rs/pull/3718) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add pretty format with options [\#3717](https://github.com/apache/arrow-rs/pull/3717) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove unreachable decimal take [\#3716](https://github.com/apache/arrow-rs/pull/3716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Feat: arrow csv decimal256 [\#3711](https://github.com/apache/arrow-rs/pull/3711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([suxiaogang223](https://github.com/suxiaogang223))
- perf: `take_run` improvements [\#3705](https://github.com/apache/arrow-rs/pull/3705) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Add raw MapArrayReader [\#3703](https://github.com/apache/arrow-rs/pull/3703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Sort kernel for `RunArray` [\#3695](https://github.com/apache/arrow-rs/pull/3695) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- perf: Remove sorting to yield sorted\_rank [\#3693](https://github.com/apache/arrow-rs/pull/3693) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- fix: Handle sliced array in run array iterator [\#3681](https://github.com/apache/arrow-rs/pull/3681) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))

## [33.0.0](https://github.com/apache/arrow-rs/tree/33.0.0) (2023-02-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/32.0.0...33.0.0)

**Breaking changes:**

- Use ArrayFormatter in Cast Kernel [\#3668](https://github.com/apache/arrow-rs/pull/3668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use dyn Array in cast kernels [\#3667](https://github.com/apache/arrow-rs/pull/3667) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return references from FixedSizeListArray and MapArray [\#3652](https://github.com/apache/arrow-rs/pull/3652) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Lazy array display \(\#3638\) [\#3647](https://github.com/apache/arrow-rs/pull/3647) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use array\_value\_to\_string in arrow-csv [\#3514](https://github.com/apache/arrow-rs/pull/3514) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JayjeetAtGithub](https://github.com/JayjeetAtGithub))

**Implemented enhancements:**

- Support UTF8 cast to Timestamp with timezone [\#3664](https://github.com/apache/arrow-rs/issues/3664)
- Add modulus\_dyn and modulus\_scalar\_dyn [\#3648](https://github.com/apache/arrow-rs/issues/3648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- A trait for append\_value and append\_null on ArrayBuilders [\#3644](https://github.com/apache/arrow-rs/issues/3644)
- Improve error message "batches\[0\] schema is different with argument schema" [\#3628](https://github.com/apache/arrow-rs/issues/3628) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Specified version of helper function to cast binary to string [\#3623](https://github.com/apache/arrow-rs/issues/3623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Casting generic binary to generic string [\#3606](https://github.com/apache/arrow-rs/issues/3606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `array_value_to_string` in `arrow-csv`  [\#3483](https://github.com/apache/arrow-rs/issues/3483) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- ArrowArray::try\_from\_raw Misleading Signature [\#3684](https://github.com/apache/arrow-rs/issues/3684) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- PyArrowConvert Leaks Memory [\#3683](https://github.com/apache/arrow-rs/issues/3683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow-csv reader cannot produce RecordBatch even if the bytes are necessary [\#3674](https://github.com/apache/arrow-rs/issues/3674)
- FFI Fails to Account For Offsets [\#3671](https://github.com/apache/arrow-rs/issues/3671) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Regression in CSV reader error handling [\#3656](https://github.com/apache/arrow-rs/issues/3656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- UnionArray Child and Value Fail to Account for non-contiguous Type IDs [\#3653](https://github.com/apache/arrow-rs/issues/3653) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Panic when accessing RecordBatch from pyarrow [\#3646](https://github.com/apache/arrow-rs/issues/3646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Multiplication for decimals is incorrect [\#3645](https://github.com/apache/arrow-rs/issues/3645)
- Inconsistent output between pretty print and CSV writer for Arrow [\#3513](https://github.com/apache/arrow-rs/issues/3513) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Release 33.0.0 of arrow/arrow-flight/parquet/parquet-derive \(next release after 32.0.0\)  [\#3682](https://github.com/apache/arrow-rs/issues/3682)
- Release `32.0.0` of `arrow`/`arrow-flight`/`parquet`/`parquet-derive` \(next release after `31.0.0`\) [\#3584](https://github.com/apache/arrow-rs/issues/3584) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Merged pull requests:**

- Move FFI to sub-crates [\#3687](https://github.com/apache/arrow-rs/pull/3687) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update to 33.0.0 and update changelog [\#3686](https://github.com/apache/arrow-rs/pull/3686) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Cleanup FFI interface \(\#3684\) \(\#3683\) [\#3685](https://github.com/apache/arrow-rs/pull/3685) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix: take\_run benchmark parameter [\#3679](https://github.com/apache/arrow-rs/pull/3679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Minor: Add some examples to Date\*Array and Time\*Array [\#3678](https://github.com/apache/arrow-rs/pull/3678) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add CSV Decoder::capacity \(\#3674\) [\#3677](https://github.com/apache/arrow-rs/pull/3677) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ArrayData::new\_null and DataType::primitive\_width [\#3676](https://github.com/apache/arrow-rs/pull/3676) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix FFI which fails to account for offsets [\#3675](https://github.com/apache/arrow-rs/pull/3675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support UTF8 cast to Timestamp with timezone [\#3673](https://github.com/apache/arrow-rs/pull/3673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Fix Date64Array docs [\#3670](https://github.com/apache/arrow-rs/pull/3670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.50 to =1.0.51 [\#3669](https://github.com/apache/arrow-rs/pull/3669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add timezone accessor for Timestamp\*Array [\#3666](https://github.com/apache/arrow-rs/pull/3666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster timezone cast [\#3665](https://github.com/apache/arrow-rs/pull/3665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat + fix: IPC support for run encoded array. [\#3662](https://github.com/apache/arrow-rs/pull/3662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Implement std::fmt::Write for StringBuilder \(\#3638\) [\#3659](https://github.com/apache/arrow-rs/pull/3659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Include line and field number in CSV UTF-8 error \(\#3656\) [\#3657](https://github.com/apache/arrow-rs/pull/3657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Handle non-contiguous type\_ids in UnionArray \(\#3653\) [\#3654](https://github.com/apache/arrow-rs/pull/3654) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add modulus\_dyn and modulus\_scalar\_dyn [\#3649](https://github.com/apache/arrow-rs/pull/3649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve error message with detailed schema [\#3637](https://github.com/apache/arrow-rs/pull/3637) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Veeupup](https://github.com/Veeupup))
- Add limit to ArrowReaderBuilder to push limit down to parquet reader [\#3633](https://github.com/apache/arrow-rs/pull/3633) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- chore: delete wrong comment and refactor set\_metadata in `Field` [\#3630](https://github.com/apache/arrow-rs/pull/3630) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([chunshao90](https://github.com/chunshao90))
- Fix typo in comment [\#3627](https://github.com/apache/arrow-rs/pull/3627) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kjschiroo](https://github.com/kjschiroo))
- Minor: Update doc strings about Page Index / Column Index [\#3625](https://github.com/apache/arrow-rs/pull/3625) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Specified version of helper function to cast binary to string [\#3624](https://github.com/apache/arrow-rs/pull/3624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- feat: take kernel for RunArray [\#3622](https://github.com/apache/arrow-rs/pull/3622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Remove BitSliceIterator specialization from try\_for\_each\_valid\_idx [\#3621](https://github.com/apache/arrow-rs/pull/3621) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Reduce PrimitiveArray::try\_unary codegen [\#3619](https://github.com/apache/arrow-rs/pull/3619) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Reduce Dictionary Builder Codegen [\#3616](https://github.com/apache/arrow-rs/pull/3616) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Add test for dictionary encoding of batches [\#3608](https://github.com/apache/arrow-rs/pull/3608) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Casting generic binary to generic string [\#3607](https://github.com/apache/arrow-rs/pull/3607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add ArrayAccessor, Iterator, Extend and benchmarks for RunArray [\#3603](https://github.com/apache/arrow-rs/pull/3603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))

## [32.0.0](https://github.com/apache/arrow-rs/tree/32.0.0) (2023-01-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/31.0.0...32.0.0)

**Breaking changes:**

- Allow `StringArray` construction with `Vec<Option<String>>` [\#3602](https://github.com/apache/arrow-rs/pull/3602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sinistersnare](https://github.com/sinistersnare))
- Use native types in PageIndex \(\#3575\) [\#3578](https://github.com/apache/arrow-rs/pull/3578) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add external variant to ParquetError \(\#3285\) [\#3574](https://github.com/apache/arrow-rs/pull/3574) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Return reference from ListArray::values [\#3561](https://github.com/apache/arrow-rs/pull/3561) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: Add `RunEndEncodedArray` [\#3553](https://github.com/apache/arrow-rs/pull/3553) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))

**Implemented enhancements:**

- There should be a `From<Vec<Option<String>>>` impl for `GenericStringArray<OffsetSize>` [\#3599](https://github.com/apache/arrow-rs/issues/3599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FlightDataEncoder Optionally send Schema even when no record batches [\#3591](https://github.com/apache/arrow-rs/issues/3591) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Use Native Types in PageIndex [\#3575](https://github.com/apache/arrow-rs/issues/3575) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Packing array into dictionary of generic byte array [\#3571](https://github.com/apache/arrow-rs/issues/3571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Error::Source` for ArrowError and FlightError [\#3566](https://github.com/apache/arrow-rs/issues/3566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[FlightSQL\] Allow access to underlying FlightClient [\#3551](https://github.com/apache/arrow-rs/issues/3551) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Arrow CSV writer should not fail when cannot cast the value [\#3547](https://github.com/apache/arrow-rs/issues/3547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Deprecated Min Max Statistics When ColumnOrder Signed [\#3526](https://github.com/apache/arrow-rs/issues/3526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Improve Performance of JSON Reader [\#3441](https://github.com/apache/arrow-rs/issues/3441)
- Support footer kv metadata for IPC file [\#3432](https://github.com/apache/arrow-rs/issues/3432)
- Add `External` variant to ParquetError [\#3285](https://github.com/apache/arrow-rs/issues/3285) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Nullif of NULL Predicate is not NULL [\#3589](https://github.com/apache/arrow-rs/issues/3589)
- BooleanBufferBuilder Fails to Clear Set Bits On Truncate [\#3587](https://github.com/apache/arrow-rs/issues/3587) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `nullif` incorrectly calculates `null_count`, sometimes panics with subtraction overflow error [\#3579](https://github.com/apache/arrow-rs/issues/3579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Meet warning when use pyarrow [\#3543](https://github.com/apache/arrow-rs/issues/3543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect row group total\_byte\_size written to parquet file [\#3530](https://github.com/apache/arrow-rs/issues/3530) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Overflow when casting timestamps prior to the epoch [\#3512](https://github.com/apache/arrow-rs/issues/3512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Panic on Key Overflow in Dictionary Builders [\#3562](https://github.com/apache/arrow-rs/issues/3562) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Bumping version gives compilation error \(arrow-array\) [\#3525](https://github.com/apache/arrow-rs/issues/3525)

**Merged pull requests:**

- Add Push-Based CSV Decoder [\#3604](https://github.com/apache/arrow-rs/pull/3604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update to flatbuffers 23.1.21 [\#3597](https://github.com/apache/arrow-rs/pull/3597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster BooleanBufferBuilder::append\_n for true values [\#3596](https://github.com/apache/arrow-rs/pull/3596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support sending schemas for empty streams [\#3594](https://github.com/apache/arrow-rs/pull/3594) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Faster ListArray to StringArray conversion [\#3593](https://github.com/apache/arrow-rs/pull/3593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add conversion from StringArray to BinaryArray [\#3592](https://github.com/apache/arrow-rs/pull/3592) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix nullif null count \(\#3579\) [\#3590](https://github.com/apache/arrow-rs/pull/3590) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Clear bits in BooleanBufferBuilder \(\#3587\) [\#3588](https://github.com/apache/arrow-rs/pull/3588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Iterate all dictionary key types in cast test [\#3585](https://github.com/apache/arrow-rs/pull/3585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Propagate EOF Error from AsyncRead [\#3576](https://github.com/apache/arrow-rs/pull/3576) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Sach1nAgarwal](https://github.com/Sach1nAgarwal))
- Show row\_counts also for \(FixedLen\)ByteArray [\#3573](https://github.com/apache/arrow-rs/pull/3573) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([bmmeijers](https://github.com/bmmeijers))
- Packing array into dictionary of generic byte array [\#3572](https://github.com/apache/arrow-rs/pull/3572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove unwrap on datetime cast for CSV writer [\#3570](https://github.com/apache/arrow-rs/pull/3570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Implement `std::error::Error::source` for `ArrowError` and `FlightError` [\#3567](https://github.com/apache/arrow-rs/pull/3567) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Improve GenericBytesBuilder offset overflow panic message \(\#139\) [\#3564](https://github.com/apache/arrow-rs/pull/3564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement Extend for ArrayBuilder \(\#1841\) [\#3563](https://github.com/apache/arrow-rs/pull/3563) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update pyarrow method call with kwargs [\#3560](https://github.com/apache/arrow-rs/pull/3560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Frankonly](https://github.com/Frankonly))
- Update pyo3 requirement from 0.17 to 0.18 [\#3557](https://github.com/apache/arrow-rs/pull/3557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Expose Inner FlightServiceClient on FlightSqlServiceClient \(\#3551\) [\#3556](https://github.com/apache/arrow-rs/pull/3556) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Fix final page row count in parquet-index binary [\#3554](https://github.com/apache/arrow-rs/pull/3554) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Parquet Avoid Reading 8 Byte Footer Twice from AsyncRead [\#3550](https://github.com/apache/arrow-rs/pull/3550) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Sach1nAgarwal](https://github.com/Sach1nAgarwal))
- Improve concat kernel capacity estimation [\#3546](https://github.com/apache/arrow-rs/pull/3546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update proc-macro2 requirement from =1.0.49 to =1.0.50 [\#3545](https://github.com/apache/arrow-rs/pull/3545) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update pyarrow method call to avoid warning [\#3544](https://github.com/apache/arrow-rs/pull/3544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Frankonly](https://github.com/Frankonly))
- Enable casting between Utf8/LargeUtf8 and Binary/LargeBinary [\#3542](https://github.com/apache/arrow-rs/pull/3542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use GHA concurrency groups \(\#3495\) [\#3538](https://github.com/apache/arrow-rs/pull/3538) ([tustvold](https://github.com/tustvold))
- set sum of uncompressed column size as row group size for parquet files [\#3531](https://github.com/apache/arrow-rs/pull/3531) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sidred](https://github.com/sidred))
- Minor: Add documentation about memory use for ArrayData [\#3529](https://github.com/apache/arrow-rs/pull/3529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Upgrade to clap 4.1 + fix test [\#3528](https://github.com/apache/arrow-rs/pull/3528) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Write backwards compatible row group statistics \(\#3526\) [\#3527](https://github.com/apache/arrow-rs/pull/3527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- No panic on timestamp buffer overflow [\#3519](https://github.com/apache/arrow-rs/pull/3519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Support casting from binary to dictionary of binary [\#3482](https://github.com/apache/arrow-rs/pull/3482) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add Raw JSON Reader \(~2.5x faster\) [\#3479](https://github.com/apache/arrow-rs/pull/3479) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [31.0.0](https://github.com/apache/arrow-rs/tree/31.0.0) (2023-01-13)

[Full Changelog](https://github.com/apache/arrow-rs/compare/30.0.1...31.0.0)

**Breaking changes:**

- support RFC3339 style timestamps in `arrow-json`  [\#3449](https://github.com/apache/arrow-rs/pull/3449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([JayjeetAtGithub](https://github.com/JayjeetAtGithub))
- Improve arrow flight batch splitting and naming [\#3444](https://github.com/apache/arrow-rs/pull/3444) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Parquet record API: timestamp as signed integer [\#3437](https://github.com/apache/arrow-rs/pull/3437) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ByteBaker](https://github.com/ByteBaker))
- Support decimal int32/64 for writer [\#3431](https://github.com/apache/arrow-rs/pull/3431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))

**Implemented enhancements:**

- Support casting  Date32 to timestamp [\#3504](https://github.com/apache/arrow-rs/issues/3504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting strings like `'2001-01-01'` to timestamp [\#3492](https://github.com/apache/arrow-rs/issues/3492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CLI to "rewrite" parquet files [\#3476](https://github.com/apache/arrow-rs/issues/3476) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add more dictionary value type support to `build_compare` [\#3465](https://github.com/apache/arrow-rs/issues/3465)
- Allow `concat_batches` to take non owned RecordBatch [\#3456](https://github.com/apache/arrow-rs/issues/3456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release Arrow `30.0.1` \(maintenance release for `30.0.0`\) [\#3455](https://github.com/apache/arrow-rs/issues/3455)
- Add string comparisons \(starts\_with, ends\_with, and contains\) to kernel [\#3442](https://github.com/apache/arrow-rs/issues/3442) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- make\_builder Loses Timezone and Decimal Scale Information [\#3435](https://github.com/apache/arrow-rs/issues/3435) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use RFC3339 style timestamps in arrow-json [\#3416](https://github.com/apache/arrow-rs/issues/3416) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ArrayData`get_slice_memory_size`   or similar [\#3407](https://github.com/apache/arrow-rs/issues/3407) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Fixed bugs:**

- Unable to read CSV with null boolean value [\#3521](https://github.com/apache/arrow-rs/issues/3521) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make consistent behavior on zeros equality on floating point types [\#3509](https://github.com/apache/arrow-rs/issues/3509)
- Sliced batch w/ bool column doesn't roundtrip through IPC [\#3496](https://github.com/apache/arrow-rs/issues/3496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- take kernel on List array introduces nulls instead of empty lists [\#3471](https://github.com/apache/arrow-rs/issues/3471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Infinite Loop If Skipping More CSV Lines than Present [\#3469](https://github.com/apache/arrow-rs/issues/3469) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix reading null booleans from CSV [\#3523](https://github.com/apache/arrow-rs/pull/3523) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- minor fix: use the unified decimal type builder [\#3522](https://github.com/apache/arrow-rs/pull/3522) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Update version to `31.0.0` and add changelog [\#3518](https://github.com/apache/arrow-rs/pull/3518) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([iajoiner](https://github.com/iajoiner))
- Additional nullif re-export [\#3515](https://github.com/apache/arrow-rs/pull/3515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Make consistent behavior on zeros equality on floating point types [\#3510](https://github.com/apache/arrow-rs/pull/3510) ([viirya](https://github.com/viirya))
- Enable cast Date32 to Timestamp [\#3508](https://github.com/apache/arrow-rs/pull/3508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Update prost-build requirement from =0.11.5 to =0.11.6 [\#3507](https://github.com/apache/arrow-rs/pull/3507) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- minor fix for the comments [\#3505](https://github.com/apache/arrow-rs/pull/3505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Fix DataTypeLayout for LargeList [\#3503](https://github.com/apache/arrow-rs/pull/3503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add string comparisons \(starts\_with, ends\_with, and contains\) to kernel [\#3502](https://github.com/apache/arrow-rs/pull/3502) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([snmvaughan](https://github.com/snmvaughan))
- Add a function to get memory size of array slice [\#3501](https://github.com/apache/arrow-rs/pull/3501) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Fix IPCWriter for Sliced BooleanArray [\#3498](https://github.com/apache/arrow-rs/pull/3498) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Fix: Added support to cast string without time [\#3494](https://github.com/apache/arrow-rs/pull/3494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gaelwjl](https://github.com/gaelwjl))
- Fix negative interval prettyprint [\#3491](https://github.com/apache/arrow-rs/pull/3491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Fixes a broken link in the arrow lib.rs rustdoc [\#3487](https://github.com/apache/arrow-rs/pull/3487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([AdamGS](https://github.com/AdamGS))
- Refactoring build\_compare for decimal and using downcast\_primitive [\#3484](https://github.com/apache/arrow-rs/pull/3484) ([viirya](https://github.com/viirya))
- Add tests for record batch size splitting logic in FlightClient [\#3481](https://github.com/apache/arrow-rs/pull/3481) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- change `concat_batches` parameter to non owned reference [\#3480](https://github.com/apache/arrow-rs/pull/3480) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- feat: add `parquet-rewrite` CLI [\#3477](https://github.com/apache/arrow-rs/pull/3477) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- Preserve empty list array elements in take kernel [\#3473](https://github.com/apache/arrow-rs/pull/3473) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jonmmease](https://github.com/jonmmease))
- Add a test for stream writer for writing sliced array [\#3472](https://github.com/apache/arrow-rs/pull/3472) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix CSV infinite loop and improve error messages [\#3470](https://github.com/apache/arrow-rs/pull/3470) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add more dictionary value type support to `build_compare` [\#3466](https://github.com/apache/arrow-rs/pull/3466) ([viirya](https://github.com/viirya))
- Add tests for `FlightClient::{list_flights, list_actions, do_action, get_schema}` [\#3463](https://github.com/apache/arrow-rs/pull/3463) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Minor: add ticket links to failing ipc integration tests [\#3461](https://github.com/apache/arrow-rs/pull/3461) ([alamb](https://github.com/alamb))
- feat: `column_name` based index access for `RecordBatch` and `StructArray` [\#3458](https://github.com/apache/arrow-rs/pull/3458) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Support Decimal256 in FFI [\#3453](https://github.com/apache/arrow-rs/pull/3453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove multiversion dependency [\#3452](https://github.com/apache/arrow-rs/pull/3452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Re-export nullif kernel [\#3451](https://github.com/apache/arrow-rs/pull/3451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Meaningful error message for map builder with null keys [\#3450](https://github.com/apache/arrow-rs/pull/3450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Parquet writer v2: clear buffer after page flush [\#3447](https://github.com/apache/arrow-rs/pull/3447) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([askoa](https://github.com/askoa))
- Verify ArrayData::data\_type compatible in PrimitiveArray::from [\#3440](https://github.com/apache/arrow-rs/pull/3440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preserve DataType metadata in make\_builder [\#3438](https://github.com/apache/arrow-rs/pull/3438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Consolidate arrow ipc tests and increase coverage [\#3427](https://github.com/apache/arrow-rs/pull/3427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Generic bytes dictionary builder [\#3426](https://github.com/apache/arrow-rs/pull/3426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Minor: Improve docs for arrow-ipc, remove clippy ignore [\#3421](https://github.com/apache/arrow-rs/pull/3421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- refactor: convert `*like_dyn`, `*like_utf8_scalar_dyn` and  `*like_dict` functions to macros [\#3411](https://github.com/apache/arrow-rs/pull/3411) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Add parquet-index binary [\#3405](https://github.com/apache/arrow-rs/pull/3405) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Complete mid-level `FlightClient` [\#3402](https://github.com/apache/arrow-rs/pull/3402) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Implement `RecordBatch` \<--\> `FlightData` encode/decode + tests [\#3391](https://github.com/apache/arrow-rs/pull/3391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Provide `into_builder` for bytearray [\#3326](https://github.com/apache/arrow-rs/pull/3326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

## [30.0.1](https://github.com/apache/arrow-rs/tree/30.0.1) (2023-01-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/30.0.0...30.0.1)

**Implemented enhancements:**

- Generic bytes dictionary builder [\#3425](https://github.com/apache/arrow-rs/issues/3425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Derive Clone for the builders in object-store. [\#3419](https://github.com/apache/arrow-rs/issues/3419)
- Mid-level `ArrowFlight` Client [\#3371](https://github.com/apache/arrow-rs/issues/3371) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Improve performance of the CSV parser [\#3338](https://github.com/apache/arrow-rs/issues/3338) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- `nullif` kernel no longer exported [\#3454](https://github.com/apache/arrow-rs/issues/3454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- PrimitiveArray from ArrayData Unsound For IntervalArray [\#3439](https://github.com/apache/arrow-rs/issues/3439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- LZ4-compressed PQ files unreadable by Pandas and ClickHouse [\#3433](https://github.com/apache/arrow-rs/issues/3433) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Record API: Cannot convert date before Unix epoch to json [\#3430](https://github.com/apache/arrow-rs/issues/3430) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet-fromcsv with writer version v2 does not stop [\#3408](https://github.com/apache/arrow-rs/issues/3408) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

## [30.0.0](https://github.com/apache/arrow-rs/tree/30.0.0) (2022-12-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/29.0.0...30.0.0)

**Breaking changes:**

- Infer Parquet JSON Logical and Converted Type as UTF-8 [\#3376](https://github.com/apache/arrow-rs/pull/3376) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use custom Any instead of prost\_types [\#3360](https://github.com/apache/arrow-rs/pull/3360) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Use bytes in arrow-flight [\#3359](https://github.com/apache/arrow-rs/pull/3359) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Add derived implementations of Clone and Debug for `ParquetObjectReader` [\#3381](https://github.com/apache/arrow-rs/issues/3381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speed up TrackedWrite [\#3366](https://github.com/apache/arrow-rs/issues/3366) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Is it possible for ArrowWriter to write key\_value\_metadata after write all records [\#3356](https://github.com/apache/arrow-rs/issues/3356) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add UnionArray test to arrow-pyarrow integration test [\#3346](https://github.com/apache/arrow-rs/issues/3346)
- Document / Deprecate arrow\_flight::utils::flight\_data\_from\_arrow\_batch [\#3312](https://github.com/apache/arrow-rs/issues/3312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- \[FlightSQL\] Support HTTPs [\#3309](https://github.com/apache/arrow-rs/issues/3309) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support UnionArray in ffi [\#3304](https://github.com/apache/arrow-rs/issues/3304) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for Azure Data Lake Storage Gen2 \(aka: ADLS Gen2\) in Object Store library [\#3283](https://github.com/apache/arrow-rs/issues/3283)
- Support casting from String to Decimal [\#3280](https://github.com/apache/arrow-rs/issues/3280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow ArrowCSV writer to control the display of NULL values [\#3268](https://github.com/apache/arrow-rs/issues/3268) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- FlightSQL example is broken [\#3386](https://github.com/apache/arrow-rs/issues/3386) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- CSV Reader Bounds Incorrectly Handles Header [\#3364](https://github.com/apache/arrow-rs/issues/3364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect output string from `try_to_type`  [\#3350](https://github.com/apache/arrow-rs/issues/3350)
- Decimal arithmetic computation fails to run because decimal type equality [\#3344](https://github.com/apache/arrow-rs/issues/3344) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pretty print not implemented for Map [\#3322](https://github.com/apache/arrow-rs/issues/3322) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ILIKE Kernels Inconsistent Case Folding [\#3311](https://github.com/apache/arrow-rs/issues/3311) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- minor: Improve arrow-flight docs [\#3372](https://github.com/apache/arrow-rs/pull/3372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Version 30.0.0 release notes and changelog [\#3406](https://github.com/apache/arrow-rs/pull/3406) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Ends ParquetRecordBatchStream when polling on StreamState::Error [\#3404](https://github.com/apache/arrow-rs/pull/3404) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- fix clippy issues [\#3398](https://github.com/apache/arrow-rs/pull/3398) ([Jimexist](https://github.com/Jimexist))
- Upgrade multiversion to 0.7.1 [\#3396](https://github.com/apache/arrow-rs/pull/3396) ([viirya](https://github.com/viirya))
- Make FlightSQL Support HTTPs [\#3388](https://github.com/apache/arrow-rs/pull/3388) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Fix broken FlightSQL example [\#3387](https://github.com/apache/arrow-rs/pull/3387) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Update prost-build [\#3385](https://github.com/apache/arrow-rs/pull/3385) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Split out arrow-arith \(\#2594\) [\#3384](https://github.com/apache/arrow-rs/pull/3384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add derive for Clone and Debug for `ParquetObjectReader` [\#3382](https://github.com/apache/arrow-rs/pull/3382) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kszlim](https://github.com/kszlim))
- Initial Mid-level `FlightClient` [\#3378](https://github.com/apache/arrow-rs/pull/3378) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Document all features on docs.rs [\#3377](https://github.com/apache/arrow-rs/pull/3377) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Split out arrow-row \(\#2594\) [\#3375](https://github.com/apache/arrow-rs/pull/3375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove unnecessary flush calls on TrackedWrite [\#3374](https://github.com/apache/arrow-rs/pull/3374) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Update proc-macro2 requirement from =1.0.47 to =1.0.49 [\#3369](https://github.com/apache/arrow-rs/pull/3369) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Add CSV build\_buffered \(\#3338\) [\#3368](https://github.com/apache/arrow-rs/pull/3368) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: add append\_key\_value\_metadata [\#3367](https://github.com/apache/arrow-rs/pull/3367) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jiacai2050](https://github.com/jiacai2050))
- Add csv-core based reader \(\#3338\) [\#3365](https://github.com/apache/arrow-rs/pull/3365) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Put BufWriter into TrackedWrite [\#3361](https://github.com/apache/arrow-rs/pull/3361) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add CSV reader benchmark \(\#3338\) [\#3357](https://github.com/apache/arrow-rs/pull/3357) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use ArrayData::ptr\_eq in DictionaryTracker [\#3354](https://github.com/apache/arrow-rs/pull/3354) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate flight\_data\_from\_arrow\_batch [\#3353](https://github.com/apache/arrow-rs/pull/3353) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Dandandan](https://github.com/Dandandan))
- Fix incorrect output string from try\_to\_type [\#3351](https://github.com/apache/arrow-rs/pull/3351) ([viirya](https://github.com/viirya))
- Fix unary\_dyn for decimal scalar arithmetic computation [\#3345](https://github.com/apache/arrow-rs/pull/3345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add UnionArray test to arrow-pyarrow integration test [\#3343](https://github.com/apache/arrow-rs/pull/3343) ([viirya](https://github.com/viirya))
- feat: configure null value in arrow csv writer [\#3342](https://github.com/apache/arrow-rs/pull/3342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Optimize bulk writing of all blocks of bloom filter [\#3340](https://github.com/apache/arrow-rs/pull/3340) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add MapArray to pretty print [\#3339](https://github.com/apache/arrow-rs/pull/3339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Update prost-build 0.11.4 [\#3334](https://github.com/apache/arrow-rs/pull/3334) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Faster Parquet Bloom Writer [\#3333](https://github.com/apache/arrow-rs/pull/3333) ([tustvold](https://github.com/tustvold))
- Add bloom filter benchmark for parquet writer [\#3323](https://github.com/apache/arrow-rs/pull/3323) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add ASCII fast path for ILIKE scalar \(90% faster\) [\#3306](https://github.com/apache/arrow-rs/pull/3306) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support UnionArray in ffi [\#3305](https://github.com/apache/arrow-rs/pull/3305) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support casting from String to Decimal [\#3281](https://github.com/apache/arrow-rs/pull/3281) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- add more integration test for parquet bloom filter round trip tests [\#3210](https://github.com/apache/arrow-rs/pull/3210) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
## [29.0.0](https://github.com/apache/arrow-rs/tree/29.0.0) (2022-12-09)

[Full Changelog](https://github.com/apache/arrow-rs/compare/28.0.0...29.0.0)

**Breaking changes:**

- Minor: Allow `Field::new` and `Field::new_with_dict` to take existing `String` as well as `&str` [\#3288](https://github.com/apache/arrow-rs/pull/3288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- update `&Option<T>` to `Option<&T>` [\#3249](https://github.com/apache/arrow-rs/pull/3249) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Hide `*_dict_scalar` kernels behind `*_dyn` kernels [\#3202](https://github.com/apache/arrow-rs/pull/3202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Support writing BloomFilter in arrow\_writer [\#3275](https://github.com/apache/arrow-rs/issues/3275) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from unsigned numeric to Decimal256 [\#3272](https://github.com/apache/arrow-rs/issues/3272) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting from Decimal256 to float types [\#3266](https://github.com/apache/arrow-rs/issues/3266) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make arithmetic kernels supports DictionaryArray of DecimalType [\#3254](https://github.com/apache/arrow-rs/issues/3254) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Casting from Decimal256 to unsigned numeric [\#3239](https://github.com/apache/arrow-rs/issues/3239) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- precision is not considered when cast value to decimal [\#3223](https://github.com/apache/arrow-rs/issues/3223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use RegexSet in arrow\_csv::infer\_field\_schema [\#3211](https://github.com/apache/arrow-rs/issues/3211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement FlightSQL Client [\#3206](https://github.com/apache/arrow-rs/issues/3206) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add binary\_mut and try\_binary\_mut [\#3143](https://github.com/apache/arrow-rs/issues/3143) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add try\_unary\_mut [\#3133](https://github.com/apache/arrow-rs/issues/3133) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Skip null buffer when importing FFI ArrowArray struct if no null buffer in the spec [\#3290](https://github.com/apache/arrow-rs/issues/3290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- using ahash `compile-time-rng` kills reproducible builds [\#3271](https://github.com/apache/arrow-rs/issues/3271) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Decimal128 to Decimal256 Overflows [\#3265](https://github.com/apache/arrow-rs/issues/3265) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `nullif` panics on empty array [\#3261](https://github.com/apache/arrow-rs/issues/3261) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Some more inconsistency between can\_cast\_types  and cast\_with\_options [\#3250](https://github.com/apache/arrow-rs/issues/3250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Enable casting between Dictionary of DecimalArray and DecimalArray [\#3237](https://github.com/apache/arrow-rs/issues/3237) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- new\_null\_array Panics creating StructArray with non-nullable fields [\#3226](https://github.com/apache/arrow-rs/issues/3226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- bool should cast from/to Float16Type as `can_cast_types` returns true [\#3221](https://github.com/apache/arrow-rs/issues/3221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Utf8 and LargeUtf8 cannot cast from/to Float16 but can\_cast\_types returns true [\#3220](https://github.com/apache/arrow-rs/issues/3220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Re-enable some tests in `arrow-cast` crate [\#3219](https://github.com/apache/arrow-rs/issues/3219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Off-by-one buffer size error triggers Panic when constructing RecordBatch from IPC bytes \(should return an Error\) [\#3215](https://github.com/apache/arrow-rs/issues/3215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow to and from pyarrow conversion results in changes in schema [\#3136](https://github.com/apache/arrow-rs/issues/3136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- better document when we need `LargeUtf8` instead of `Utf8` [\#3228](https://github.com/apache/arrow-rs/issues/3228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Use BufWriter when writing bloom filters and limit tests \(\#3318\) [\#3319](https://github.com/apache/arrow-rs/pull/3319) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use take for dictionary like comparisons [\#3313](https://github.com/apache/arrow-rs/pull/3313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update versions to  29.0.0 and update CHANGELOG [\#3315](https://github.com/apache/arrow-rs/pull/3315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- refactor: Merge similar functions `ilike_scalar` and `nilike_scalar` [\#3303](https://github.com/apache/arrow-rs/pull/3303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Split out arrow-ord \(\#2594\) [\#3299](https://github.com/apache/arrow-rs/pull/3299) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-string \(\#2594\) [\#3295](https://github.com/apache/arrow-rs/pull/3295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Skip null buffer when importing FFI ArrowArray struct if no null buffer in the spec [\#3293](https://github.com/apache/arrow-rs/pull/3293) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Don't use dangling NonNull as sentinel [\#3289](https://github.com/apache/arrow-rs/pull/3289) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Set bloom filter on byte array [\#3284](https://github.com/apache/arrow-rs/pull/3284) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Fix ipc schema custom\_metadata serialization [\#3282](https://github.com/apache/arrow-rs/pull/3282) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Disable const-random ahash feature on non-WASM \(\#3271\) [\#3277](https://github.com/apache/arrow-rs/pull/3277) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- fix\(ffi\): handle null data buffers from empty arrays [\#3276](https://github.com/apache/arrow-rs/pull/3276) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wjones127](https://github.com/wjones127))
- Support casting from unsigned numeric to Decimal256 [\#3273](https://github.com/apache/arrow-rs/pull/3273) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add parquet-layout binary [\#3269](https://github.com/apache/arrow-rs/pull/3269) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support casting from Decimal256 to float types [\#3267](https://github.com/apache/arrow-rs/pull/3267) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify decimal cast logic [\#3264](https://github.com/apache/arrow-rs/pull/3264) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix panic on nullif empty array \(\#3261\) [\#3263](https://github.com/apache/arrow-rs/pull/3263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BooleanArray::from\_unary and BooleanArray::from\_binary [\#3258](https://github.com/apache/arrow-rs/pull/3258) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Remove parquet build script [\#3257](https://github.com/apache/arrow-rs/pull/3257) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make arithmetic kernels supports DictionaryArray of DecimalType [\#3255](https://github.com/apache/arrow-rs/pull/3255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support List and LargeList in Row format \(\#3159\) [\#3251](https://github.com/apache/arrow-rs/pull/3251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Don't recurse to children in ArrayData::try\_new [\#3248](https://github.com/apache/arrow-rs/pull/3248) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Validate dictionaries read over IPC [\#3247](https://github.com/apache/arrow-rs/pull/3247) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix MapBuilder example [\#3246](https://github.com/apache/arrow-rs/pull/3246) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Loosen nullability restrictions added in \#3205 \(\#3226\) [\#3244](https://github.com/apache/arrow-rs/pull/3244) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Better document implications of offsets \(\#3228\) [\#3243](https://github.com/apache/arrow-rs/pull/3243) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add new API to validate the precision for decimal array [\#3242](https://github.com/apache/arrow-rs/pull/3242) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Move nullif to arrow-select \(\#2594\) [\#3241](https://github.com/apache/arrow-rs/pull/3241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Casting from Decimal256 to unsigned numeric [\#3240](https://github.com/apache/arrow-rs/pull/3240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Enable casting between Dictionary of DecimalArray and DecimalArray [\#3238](https://github.com/apache/arrow-rs/pull/3238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove unwraps from 'create\_primitive\_array' [\#3232](https://github.com/apache/arrow-rs/pull/3232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([aarashy](https://github.com/aarashy))
- Fix CI build by upgrading tonic-build to 0.8.4 [\#3231](https://github.com/apache/arrow-rs/pull/3231) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Remove negative scale check [\#3230](https://github.com/apache/arrow-rs/pull/3230) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update prost-build requirement from =0.11.2 to =0.11.3 [\#3225](https://github.com/apache/arrow-rs/pull/3225) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Get the round result for decimal to a decimal with smaller scale  [\#3224](https://github.com/apache/arrow-rs/pull/3224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Move tests which require chrono-tz feature from `arrow-cast` to `arrow` [\#3222](https://github.com/apache/arrow-rs/pull/3222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- add test cases for extracting week with/without timezone [\#3218](https://github.com/apache/arrow-rs/pull/3218) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Use RegexSet for matching DataType [\#3217](https://github.com/apache/arrow-rs/pull/3217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Update tonic-build to 0.8.3 [\#3214](https://github.com/apache/arrow-rs/pull/3214) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Support StructArray in Row Format \(\#3159\) [\#3212](https://github.com/apache/arrow-rs/pull/3212) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Infer timestamps from CSV files [\#3209](https://github.com/apache/arrow-rs/pull/3209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- fix bug: cast decimal256 to other decimal with no-safe [\#3208](https://github.com/apache/arrow-rs/pull/3208) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- FlightSQL Client & integration test [\#3207](https://github.com/apache/arrow-rs/pull/3207) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Ensure StructArrays check nullability of fields [\#3205](https://github.com/apache/arrow-rs/pull/3205) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Remove special case ArrayData equality for decimals [\#3204](https://github.com/apache/arrow-rs/pull/3204) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add a cast test case for decimal negative scale [\#3203](https://github.com/apache/arrow-rs/pull/3203) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Move zip and shift kernels to arrow-select [\#3201](https://github.com/apache/arrow-rs/pull/3201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate limit kernel [\#3200](https://github.com/apache/arrow-rs/pull/3200) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use SlicesIterator for ArrayData Equality [\#3198](https://github.com/apache/arrow-rs/pull/3198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add \_dyn kernels of like, ilike, nlike, nilike kernels for dictionary support [\#3197](https://github.com/apache/arrow-rs/pull/3197) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Adding scalar nlike\_dyn, ilike\_dyn, nilike\_dyn kernels [\#3195](https://github.com/apache/arrow-rs/pull/3195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Use self capture in DataType [\#3190](https://github.com/apache/arrow-rs/pull/3190) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- To pyarrow with schema [\#3188](https://github.com/apache/arrow-rs/pull/3188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([doki23](https://github.com/doki23))
- Support Duration in array\_value\_to\_string [\#3183](https://github.com/apache/arrow-rs/pull/3183) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Support `FixedSizeBinary` in Row format [\#3182](https://github.com/apache/arrow-rs/pull/3182) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add binary\_mut and try\_binary\_mut [\#3144](https://github.com/apache/arrow-rs/pull/3144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add try\_unary\_mut [\#3134](https://github.com/apache/arrow-rs/pull/3134) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
## [28.0.0](https://github.com/apache/arrow-rs/tree/28.0.0) (2022-11-25)

[Full Changelog](https://github.com/apache/arrow-rs/compare/27.0.0...28.0.0)

**Breaking changes:**

- StructArray::columns return slice [\#3186](https://github.com/apache/arrow-rs/pull/3186) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Return slice from GenericByteArray::value\_data [\#3171](https://github.com/apache/arrow-rs/pull/3171) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support decimal negative scale [\#3152](https://github.com/apache/arrow-rs/pull/3152) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- refactor: convert `Field::metadata` to `HashMap` [\#3148](https://github.com/apache/arrow-rs/pull/3148) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Don't Skip Serializing Empty Metadata \(\#3082\) [\#3126](https://github.com/apache/arrow-rs/pull/3126) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Add Decimal128, Decimal256, Float16 to DataType::is\_numeric [\#3121](https://github.com/apache/arrow-rs/pull/3121) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Upgrade to thrift 0.17 and fix issues [\#3104](https://github.com/apache/arrow-rs/pull/3104) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Fix prettyprint for Interval second fractions [\#3093](https://github.com/apache/arrow-rs/pull/3093) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Remove Option from `Field::metadata` [\#3091](https://github.com/apache/arrow-rs/pull/3091) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))

**Implemented enhancements:**

- Add iterator to RowSelection [\#3172](https://github.com/apache/arrow-rs/issues/3172) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- create an integration test set for parquet crate against pyspark for working with bloom filters [\#3167](https://github.com/apache/arrow-rs/issues/3167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Row Format Size Tracking [\#3160](https://github.com/apache/arrow-rs/issues/3160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add ArrayBuilder::finish\_cloned\(\) [\#3154](https://github.com/apache/arrow-rs/issues/3154) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Optimize memory usage of json reader [\#3150](https://github.com/apache/arrow-rs/issues/3150)
- Add `Field::size` and `DataType::size` [\#3147](https://github.com/apache/arrow-rs/issues/3147) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add like\_utf8\_scalar\_dyn kernel [\#3145](https://github.com/apache/arrow-rs/issues/3145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support comparison for decimal128 array with scalar in kernel [\#3140](https://github.com/apache/arrow-rs/issues/3140) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- audit and create a document for bloom filter configurations [\#3138](https://github.com/apache/arrow-rs/issues/3138) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Should be the rounding vs truncation when cast decimal to smaller scale  [\#3137](https://github.com/apache/arrow-rs/issues/3137) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Upgrade chrono to 0.4.23 [\#3120](https://github.com/apache/arrow-rs/issues/3120)
- Implements more temporal kernels using time\_fraction\_dyn [\#3108](https://github.com/apache/arrow-rs/issues/3108) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Upgrade to thrift 0.17 [\#3105](https://github.com/apache/arrow-rs/issues/3105) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Be able to parse time formatted strings [\#3100](https://github.com/apache/arrow-rs/issues/3100) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve "Fail to merge schema" error messages [\#3095](https://github.com/apache/arrow-rs/issues/3095) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose `SortingColumn` when reading and writing parquet metadata [\#3090](https://github.com/apache/arrow-rs/issues/3090) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Change Field::metadata to HashMap [\#3086](https://github.com/apache/arrow-rs/issues/3086) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support bloom filter reading and writing for parquet [\#3023](https://github.com/apache/arrow-rs/issues/3023) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- API to take back ownership of an ArrayRef [\#2901](https://github.com/apache/arrow-rs/issues/2901) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Specialized Interleave Kernel [\#2864](https://github.com/apache/arrow-rs/issues/2864) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- arithmetic overflow leads to segfault in `concat_batches` [\#3123](https://github.com/apache/arrow-rs/issues/3123) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clippy failing on master : error: use of deprecated associated function chrono::NaiveDate::from\_ymd: use from\_ymd\_opt\(\) instead [\#3097](https://github.com/apache/arrow-rs/issues/3097) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pretty print for interval types has wrong formatting [\#3092](https://github.com/apache/arrow-rs/issues/3092) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Field is not serializable with binary formats [\#3082](https://github.com/apache/arrow-rs/issues/3082) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Decimal Casts are Unchecked [\#2986](https://github.com/apache/arrow-rs/issues/2986) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Release Arrow `27.0.0` \(next release after `26.0.0`\) [\#3045](https://github.com/apache/arrow-rs/issues/3045) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Perf about ParquetRecordBatchStream vs ParquetRecordBatchReader [\#2916](https://github.com/apache/arrow-rs/issues/2916)

**Merged pull requests:**

- Improve regex related kernels by upto 85% [\#3192](https://github.com/apache/arrow-rs/pull/3192) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Derive clone for arrays [\#3184](https://github.com/apache/arrow-rs/pull/3184) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Row decode cleanups [\#3180](https://github.com/apache/arrow-rs/pull/3180) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update zstd requirement from 0.11.1 to 0.12.0 [\#3178](https://github.com/apache/arrow-rs/pull/3178) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Move decimal constants from `arrow-data` to `arrow-schema` crate [\#3177](https://github.com/apache/arrow-rs/pull/3177) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mbrobbel](https://github.com/mbrobbel))
- bloom filter part V: add an integration with pytest against pyspark [\#3176](https://github.com/apache/arrow-rs/pull/3176) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Bloom filter config tweaks \(\#3023\) [\#3175](https://github.com/apache/arrow-rs/pull/3175) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add RowParser [\#3174](https://github.com/apache/arrow-rs/pull/3174) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `RowSelection::iter()`, `Into<Vec<RowSelector>>` and example [\#3173](https://github.com/apache/arrow-rs/pull/3173) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add read parquet examples [\#3170](https://github.com/apache/arrow-rs/pull/3170) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([xudong963](https://github.com/xudong963))
- Faster BinaryArray to StringArray conversion \(~67%\) [\#3168](https://github.com/apache/arrow-rs/pull/3168) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove unnecessary downcasts in builders [\#3166](https://github.com/apache/arrow-rs/pull/3166) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- bloom filter part IV: adjust writer properties, bloom filter properties, and incorporate into column encoder [\#3165](https://github.com/apache/arrow-rs/pull/3165) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Fix parquet decimal precision [\#3164](https://github.com/apache/arrow-rs/pull/3164) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Add Row size methods \(\#3160\) [\#3163](https://github.com/apache/arrow-rs/pull/3163) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Prevent precision=0 for decimal type [\#3162](https://github.com/apache/arrow-rs/pull/3162) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Remove unnecessary Buffer::from\_slice\_ref reference [\#3161](https://github.com/apache/arrow-rs/pull/3161) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add finish\_cloned to ArrayBuilder [\#3158](https://github.com/apache/arrow-rs/pull/3158) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Check overflow in MutableArrayData extend offsets \(\#3123\) [\#3157](https://github.com/apache/arrow-rs/pull/3157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Extend Decimal256 as Primitive [\#3156](https://github.com/apache/arrow-rs/pull/3156) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Doc improvements [\#3155](https://github.com/apache/arrow-rs/pull/3155) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Add collect.rs example [\#3153](https://github.com/apache/arrow-rs/pull/3153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Implement Neg for i256 [\#3151](https://github.com/apache/arrow-rs/pull/3151) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- feat: `{Field,DataType}::size` [\#3149](https://github.com/apache/arrow-rs/pull/3149) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add like\_utf8\_scalar\_dyn kernel [\#3146](https://github.com/apache/arrow-rs/pull/3146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- comparison op: decimal128 array with scalar [\#3141](https://github.com/apache/arrow-rs/pull/3141) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Cast: should get the round result for decimal to a decimal with smaller scale [\#3139](https://github.com/apache/arrow-rs/pull/3139) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Fix Panic on Reading Corrupt Parquet Schema \(\#2855\) [\#3130](https://github.com/apache/arrow-rs/pull/3130) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- Clippy parquet fixes [\#3124](https://github.com/apache/arrow-rs/pull/3124) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Add GenericByteBuilder \(\#2969\) [\#3122](https://github.com/apache/arrow-rs/pull/3122) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- parquet bloom filter part III: add sbbf writer, remove `bloom` default feature, add reader properties [\#3119](https://github.com/apache/arrow-rs/pull/3119) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Add downcast\_array \(\#2901\) [\#3117](https://github.com/apache/arrow-rs/pull/3117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add COW conversion for Buffer and PrimitiveArray and unary\_mut [\#3115](https://github.com/apache/arrow-rs/pull/3115) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Include field name in merge error message [\#3113](https://github.com/apache/arrow-rs/pull/3113) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Add PrimitiveArray::unary\_opt [\#3110](https://github.com/apache/arrow-rs/pull/3110) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implements more temporal kernels using time\_fraction\_dyn [\#3107](https://github.com/apache/arrow-rs/pull/3107) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- cast: support unsigned numeric type to decimal128 [\#3106](https://github.com/apache/arrow-rs/pull/3106) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Expose `SortingColumn` in parquet files [\#3103](https://github.com/apache/arrow-rs/pull/3103) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([askoa](https://github.com/askoa))
- parquet bloom filter part II: read sbbf bitset from row group reader, update API, and add cli demo [\#3102](https://github.com/apache/arrow-rs/pull/3102) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Parse Time32/Time64 from formatted string [\#3101](https://github.com/apache/arrow-rs/pull/3101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jefffrey](https://github.com/Jefffrey))
- Cleanup temporal \_internal functions [\#3099](https://github.com/apache/arrow-rs/pull/3099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Improve schema mismatch error message [\#3098](https://github.com/apache/arrow-rs/pull/3098) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- Fix clippy by avoiding deprecated functions in chrono [\#3096](https://github.com/apache/arrow-rs/pull/3096) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Minor: Add diagrams and documentation to row format [\#3094](https://github.com/apache/arrow-rs/pull/3094) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Minor: Use ArrowNativeTypeOp instead of total\_cmp directly [\#3087](https://github.com/apache/arrow-rs/pull/3087) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Check overflow while casting between decimal types [\#3076](https://github.com/apache/arrow-rs/pull/3076) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- add bloom filter implementation based on split block \(sbbf\) spec [\#3057](https://github.com/apache/arrow-rs/pull/3057) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Add FixedSizeBinaryArray::try\_from\_sparse\_iter\_with\_size [\#3054](https://github.com/apache/arrow-rs/pull/3054) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
## [27.0.0](https://github.com/apache/arrow-rs/tree/27.0.0) (2022-11-11)

[Full Changelog](https://github.com/apache/arrow-rs/compare/26.0.0...27.0.0)

**Breaking changes:**

- Recurse into Dictionary value type in DataType::is\_nested [\#3083](https://github.com/apache/arrow-rs/pull/3083) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- early type checks in `RowConverter` [\#3080](https://github.com/apache/arrow-rs/pull/3080) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Add Decimal128 and Decimal256 to downcast\_primitive [\#3056](https://github.com/apache/arrow-rs/pull/3056) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace remaining \_generic temporal kernels with \_dyn kernels [\#3046](https://github.com/apache/arrow-rs/pull/3046) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Replace year\_generic with year\_dyn [\#3041](https://github.com/apache/arrow-rs/pull/3041) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Validate decimal256 with i256 directly [\#3025](https://github.com/apache/arrow-rs/pull/3025) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Hadoop LZ4 Support for LZ4 Codec [\#3013](https://github.com/apache/arrow-rs/pull/3013) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
- Replace hour\_generic with hour\_dyn [\#3006](https://github.com/apache/arrow-rs/pull/3006) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Accept any &dyn Array in nullif kernel [\#2940](https://github.com/apache/arrow-rs/pull/2940) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Row Format: Option to detach/own a row [\#3078](https://github.com/apache/arrow-rs/issues/3078) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Row Format: API to check if datatypes are supported [\#3077](https://github.com/apache/arrow-rs/issues/3077) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Deprecate Buffer::count\_set\_bits [\#3067](https://github.com/apache/arrow-rs/issues/3067) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Decimal128 and Decimal256 to downcast\_primitive [\#3055](https://github.com/apache/arrow-rs/issues/3055) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improved UX of  creating `TimestampNanosecondArray` with timezones [\#3042](https://github.com/apache/arrow-rs/issues/3042) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast decimal256 to signed integer [\#3039](https://github.com/apache/arrow-rs/issues/3039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting Date64 to Timestamp [\#3037](https://github.com/apache/arrow-rs/issues/3037) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Check overflow when casting floating point value to decimal256 [\#3032](https://github.com/apache/arrow-rs/issues/3032) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare i256 in validate\_decimal256\_precision [\#3024](https://github.com/apache/arrow-rs/issues/3024) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Check overflow when casting floating point value to decimal128 [\#3020](https://github.com/apache/arrow-rs/issues/3020) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add macro downcast\_temporal\_array [\#3008](https://github.com/apache/arrow-rs/issues/3008) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace hour\_generic with hour\_dyn [\#3005](https://github.com/apache/arrow-rs/issues/3005) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace temporal \_generic kernels with dyn [\#3004](https://github.com/apache/arrow-rs/issues/3004) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `RowSelection::intersection` [\#3003](https://github.com/apache/arrow-rs/issues/3003) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- I would like to round rather than truncate when casting f64 to decimal [\#2997](https://github.com/apache/arrow-rs/issues/2997) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow::compute::kernels::temporal should support nanoseconds [\#2995](https://github.com/apache/arrow-rs/issues/2995) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Release Arrow `26.0.0` \(next release after `25.0.0`\) [\#2953](https://github.com/apache/arrow-rs/issues/2953) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Add timezone offset for debug format of Timestamp with Timezone [\#2917](https://github.com/apache/arrow-rs/issues/2917) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support merge RowSelectors when creating RowSelection [\#2858](https://github.com/apache/arrow-rs/issues/2858) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Inconsistent Nan Handling Between Scalar and Non-Scalar Comparison Kernels [\#3074](https://github.com/apache/arrow-rs/issues/3074) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Debug format for timestamp ignores timezone [\#3069](https://github.com/apache/arrow-rs/issues/3069) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Row format decode loses timezone [\#3063](https://github.com/apache/arrow-rs/issues/3063) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- binary operator produces incorrect result on arrays with resized null buffer [\#3061](https://github.com/apache/arrow-rs/issues/3061) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RLEDecoder Panics on Null Padded Pages [\#3035](https://github.com/apache/arrow-rs/issues/3035) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Nullif with incorrect valid\_count [\#3031](https://github.com/apache/arrow-rs/issues/3031) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RLEDecoder::get\_batch\_with\_dict may panic on bit-packed runs longer than 1024 [\#3029](https://github.com/apache/arrow-rs/issues/3029) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Converted type is None according to Parquet Tools then utilizing logical types [\#3017](https://github.com/apache/arrow-rs/issues/3017)
- CompressionCodec LZ4 incompatible with C++ implementation [\#2988](https://github.com/apache/arrow-rs/issues/2988) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Mark parquet predicate pushdown as complete [\#2987](https://github.com/apache/arrow-rs/pull/2987) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Merged pull requests:**

- Improved UX of  creating `TimestampNanosecondArray` with timezones [\#3088](https://github.com/apache/arrow-rs/pull/3088) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([src255](https://github.com/src255))
- Remove unused range module [\#3085](https://github.com/apache/arrow-rs/pull/3085) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make intersect\_row\_selections a member function [\#3084](https://github.com/apache/arrow-rs/pull/3084) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update hashbrown requirement from 0.12 to 0.13 [\#3081](https://github.com/apache/arrow-rs/pull/3081) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: add `OwnedRow` [\#3079](https://github.com/apache/arrow-rs/pull/3079) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([crepererum](https://github.com/crepererum))
- Use ArrowNativeTypeOp on non-scalar comparison kernels [\#3075](https://github.com/apache/arrow-rs/pull/3075) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add missing inline to ArrowNativeTypeOp [\#3073](https://github.com/apache/arrow-rs/pull/3073) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- fix debug information for Timestamp with Timezone  [\#3072](https://github.com/apache/arrow-rs/pull/3072) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Deprecate Buffer::count\_set\_bits \(\#3067\) [\#3071](https://github.com/apache/arrow-rs/pull/3071) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add compare to ArrowNativeTypeOp [\#3070](https://github.com/apache/arrow-rs/pull/3070) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Improve docstrings on WriterPropertiesBuilder [\#3068](https://github.com/apache/arrow-rs/pull/3068) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Faster f64 inequality [\#3065](https://github.com/apache/arrow-rs/pull/3065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix row format decode loses timezone \(\#3063\) [\#3064](https://github.com/apache/arrow-rs/pull/3064) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix null\_count computation in binary [\#3062](https://github.com/apache/arrow-rs/pull/3062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Faster f64 equality [\#3060](https://github.com/apache/arrow-rs/pull/3060) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update arrow-flight subcrates \(\#3044\) [\#3052](https://github.com/apache/arrow-rs/pull/3052) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Minor: Remove cloning ArrayData in with\_precision\_and\_scale [\#3050](https://github.com/apache/arrow-rs/pull/3050) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split out arrow-json \(\#3044\) [\#3049](https://github.com/apache/arrow-rs/pull/3049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move `intersect_row_selections` from datafusion to arrow-rs. [\#3047](https://github.com/apache/arrow-rs/pull/3047) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Split out arrow-csv \(\#2594\) [\#3044](https://github.com/apache/arrow-rs/pull/3044) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move reader\_parser to arrow-cast \(\#3022\) [\#3043](https://github.com/apache/arrow-rs/pull/3043) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cast decimal256 to signed integer [\#3040](https://github.com/apache/arrow-rs/pull/3040) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Enable casting from Date64 to Timestamp [\#3038](https://github.com/apache/arrow-rs/pull/3038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gruuya](https://github.com/gruuya))
- Fix decoding long and/or padded RLE data \(\#3029\) \(\#3035\) [\#3036](https://github.com/apache/arrow-rs/pull/3036) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix nullif when existing array has no nulls [\#3034](https://github.com/apache/arrow-rs/pull/3034) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Check overflow when casting floating point value to decimal256 [\#3033](https://github.com/apache/arrow-rs/pull/3033) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update parquet to depend on arrow subcrates [\#3028](https://github.com/apache/arrow-rs/pull/3028) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make various i256 methods const [\#3026](https://github.com/apache/arrow-rs/pull/3026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-ipc [\#3022](https://github.com/apache/arrow-rs/pull/3022) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Check overflow while casting floating point value to decimal128 [\#3021](https://github.com/apache/arrow-rs/pull/3021) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update arrow-flight [\#3019](https://github.com/apache/arrow-rs/pull/3019) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Move ArrowNativeTypeOp to arrow-array \(\#2594\) [\#3018](https://github.com/apache/arrow-rs/pull/3018) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support cast timestamp to time [\#3016](https://github.com/apache/arrow-rs/pull/3016) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([naosense](https://github.com/naosense))
- Add filter example [\#3014](https://github.com/apache/arrow-rs/pull/3014) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Check overflow when casting integer to decimal [\#3009](https://github.com/apache/arrow-rs/pull/3009) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add macro downcast\_temporal\_array [\#3007](https://github.com/apache/arrow-rs/pull/3007) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Parquet Writer: Make column descriptor public on the writer [\#3002](https://github.com/apache/arrow-rs/pull/3002) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([pier-oliviert](https://github.com/pier-oliviert))
- Update chrono-tz requirement from 0.7 to 0.8 [\#3001](https://github.com/apache/arrow-rs/pull/3001) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Round instead of Truncate while casting float to decimal [\#3000](https://github.com/apache/arrow-rs/pull/3000) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Support Predicate Pushdown for Parquet Lists \(\#2108\) [\#2999](https://github.com/apache/arrow-rs/pull/2999) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out arrow-cast \(\#2594\) [\#2998](https://github.com/apache/arrow-rs/pull/2998) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- `arrow::compute::kernels::temporal` should support nanoseconds  [\#2996](https://github.com/apache/arrow-rs/pull/2996) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comphead](https://github.com/comphead))
- Add `RowSelection::from_selectors_and_combine` to  merge RowSelectors  [\#2994](https://github.com/apache/arrow-rs/pull/2994) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Simplify Single-Column Dictionary Sort [\#2993](https://github.com/apache/arrow-rs/pull/2993) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Minor: Add entry to changelog for 26.0.0 RC2 fix [\#2992](https://github.com/apache/arrow-rs/pull/2992) ([alamb](https://github.com/alamb))
- Fix ignored limit on `lexsort_to_indices` [\#2991](https://github.com/apache/arrow-rs/pull/2991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add clone and equal functions for CastOptions [\#2985](https://github.com/apache/arrow-rs/pull/2985) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- minor: remove redundant prefix [\#2983](https://github.com/apache/arrow-rs/pull/2983) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([jackwener](https://github.com/jackwener))
- Compare dictionary decimal arrays [\#2982](https://github.com/apache/arrow-rs/pull/2982) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Compare dictionary and non-dictionary decimal arrays [\#2980](https://github.com/apache/arrow-rs/pull/2980) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add decimal comparison kernel support [\#2978](https://github.com/apache/arrow-rs/pull/2978) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Move concat kernel to arrow-select \(\#2594\) [\#2976](https://github.com/apache/arrow-rs/pull/2976) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Specialize interleave for byte arrays \(\#2864\) [\#2975](https://github.com/apache/arrow-rs/pull/2975) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use unary function for numeric to decimal cast [\#2973](https://github.com/apache/arrow-rs/pull/2973) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Specialize filter kernel for binary arrays \(\#2969\) [\#2971](https://github.com/apache/arrow-rs/pull/2971) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Combine take\_utf8 and take\_binary \(\#2969\) [\#2970](https://github.com/apache/arrow-rs/pull/2970) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster Scalar Dictionary Comparison ~10% [\#2968](https://github.com/apache/arrow-rs/pull/2968) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move `byte_size` from datafusion::physical\_expr [\#2965](https://github.com/apache/arrow-rs/pull/2965) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([avantgardnerio](https://github.com/avantgardnerio))
- Pass decompressed size to parquet Codec::decompress \(\#2956\) [\#2959](https://github.com/apache/arrow-rs/pull/2959) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
- Add Decimal Arithmetic [\#2881](https://github.com/apache/arrow-rs/pull/2881) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [26.0.0](https://github.com/apache/arrow-rs/tree/26.0.0) (2022-10-28)

[Full Changelog](https://github.com/apache/arrow-rs/compare/25.0.0...26.0.0)

**Breaking changes:**

- Cast Timestamps to RFC3339 strings [\#2934](https://github.com/apache/arrow-rs/issues/2934)
- Remove Unused NativeDecimalType [\#2945](https://github.com/apache/arrow-rs/pull/2945) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Format Timestamps as RFC3339 [\#2939](https://github.com/apache/arrow-rs/pull/2939) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Update flatbuffers to resolve RUSTSEC-2021-0122 [\#2895](https://github.com/apache/arrow-rs/pull/2895) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- replace `from_timestamp` by `from_timestamp_opt` [\#2894](https://github.com/apache/arrow-rs/pull/2894) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))

**Implemented enhancements:**

- Optimized way to count the numbers of `true` and `false` values in a BooleanArray [\#2963](https://github.com/apache/arrow-rs/issues/2963) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add pow to i256 [\#2954](https://github.com/apache/arrow-rs/issues/2954) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Generic Code over \[Large\]BinaryArray and \[Large\]StringArray [\#2946](https://github.com/apache/arrow-rs/issues/2946) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Page Row Count Limit [\#2941](https://github.com/apache/arrow-rs/issues/2941) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- prettyprint to show timezone offset for timestamp with timezone [\#2937](https://github.com/apache/arrow-rs/issues/2937) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast numeric to decimal256 [\#2922](https://github.com/apache/arrow-rs/issues/2922) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `freeze_with_dictionary` API to `MutableArrayData` [\#2914](https://github.com/apache/arrow-rs/issues/2914) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support decimal256 array in sort kernels [\#2911](https://github.com/apache/arrow-rs/issues/2911) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- support `[+/-]hhmm` and `[+/-]hh` as fixedoffset timezone format [\#2910](https://github.com/apache/arrow-rs/issues/2910) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cleanup decimal sort function [\#2907](https://github.com/apache/arrow-rs/issues/2907) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- replace `from_timestamp` by `from_timestamp_opt` [\#2892](https://github.com/apache/arrow-rs/issues/2892) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Move Primitive arity kernels to arrow-array [\#2787](https://github.com/apache/arrow-rs/issues/2787) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- add overflow-checking for negative arithmetic kernel [\#2662](https://github.com/apache/arrow-rs/issues/2662) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Subtle compatibility issue with serve\_arrow [\#2952](https://github.com/apache/arrow-rs/issues/2952)
- error\[E0599\]: no method named `total_cmp` found for struct `f16` in the current scope [\#2926](https://github.com/apache/arrow-rs/issues/2926) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fail at rowSelection `and_then` method [\#2925](https://github.com/apache/arrow-rs/issues/2925) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Ordering not implemented for FixedSizeBinary types [\#2904](https://github.com/apache/arrow-rs/issues/2904) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet API: Could not convert timestamp before unix epoch to string/json [\#2897](https://github.com/apache/arrow-rs/issues/2897) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Overly Pessimistic RLE Size Estimation [\#2889](https://github.com/apache/arrow-rs/issues/2889) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Memory alignment error in `RawPtrBox::new` [\#2882](https://github.com/apache/arrow-rs/issues/2882) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compilation error under chrono-tz feature [\#2878](https://github.com/apache/arrow-rs/issues/2878) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- AHash Statically Allocates 64 bytes [\#2875](https://github.com/apache/arrow-rs/issues/2875) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `parquet::arrow::arrow_writer::ArrowWriter` ignores page size properties [\#2853](https://github.com/apache/arrow-rs/issues/2853) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Document crate topology \(\#2594\) [\#2913](https://github.com/apache/arrow-rs/pull/2913) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- SerializedFileWriter comments about multiple call on consumed self [\#2935](https://github.com/apache/arrow-rs/issues/2935) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Pointer freed error when deallocating ArrayData with shared memory buffer [\#2874](https://github.com/apache/arrow-rs/issues/2874)
- Release Arrow `25.0.0` \(next release after `24.0.0`\) [\#2820](https://github.com/apache/arrow-rs/issues/2820) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Replace DecimalArray with PrimitiveArray [\#2637](https://github.com/apache/arrow-rs/issues/2637) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix ignored limit on lexsort\_to\_indices (#2991) [\#2991](https://github.com/apache/arrow-rs/pull/2991) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix GenericListArray::try\_new\_from\_array\_data error message \(\#526\) [\#2961](https://github.com/apache/arrow-rs/pull/2961) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix take string on sliced indices [\#2960](https://github.com/apache/arrow-rs/pull/2960) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add BooleanArray::true\_count and BooleanArray::false\_count [\#2957](https://github.com/apache/arrow-rs/pull/2957) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add pow to i256 [\#2955](https://github.com/apache/arrow-rs/pull/2955) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix datatype for timestamptz debug fmt [\#2948](https://github.com/apache/arrow-rs/pull/2948) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Add GenericByteArray \(\#2946\) [\#2947](https://github.com/apache/arrow-rs/pull/2947) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Specialize interleave string ~2-3x faster [\#2944](https://github.com/apache/arrow-rs/pull/2944) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Added support for LZ4\_RAW compression. \(\#1604\) [\#2943](https://github.com/apache/arrow-rs/pull/2943) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([marioloko](https://github.com/marioloko))
- Add optional page row count limit for parquet `WriterProperties` \(\#2941\) [\#2942](https://github.com/apache/arrow-rs/pull/2942) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cleanup orphaned doc comments \(\#2935\) [\#2938](https://github.com/apache/arrow-rs/pull/2938) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- support more fixedoffset tz format [\#2936](https://github.com/apache/arrow-rs/pull/2936) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Benchmark with prepared row converter [\#2930](https://github.com/apache/arrow-rs/pull/2930) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add lexsort benchmark \(\#2871\) [\#2929](https://github.com/apache/arrow-rs/pull/2929) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve panic messages for RowSelection::and\_then \(\#2925\) [\#2928](https://github.com/apache/arrow-rs/pull/2928) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update required half from 2.0 --\> 2.1 [\#2927](https://github.com/apache/arrow-rs/pull/2927) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Cast numeric to decimal256 [\#2923](https://github.com/apache/arrow-rs/pull/2923) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cleanup generated proto code [\#2921](https://github.com/apache/arrow-rs/pull/2921) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([tustvold](https://github.com/tustvold))
- Deprecate TimestampArray from\_vec and from\_opt\_vec [\#2919](https://github.com/apache/arrow-rs/pull/2919) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support decimal256 array in sort kernels [\#2912](https://github.com/apache/arrow-rs/pull/2912) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add timezone abstraction [\#2909](https://github.com/apache/arrow-rs/pull/2909) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Cleanup decimal sort function [\#2908](https://github.com/apache/arrow-rs/pull/2908) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify TimestampArray from\_vec with timezone [\#2906](https://github.com/apache/arrow-rs/pull/2906) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement ord for FixedSizeBinary types [\#2905](https://github.com/apache/arrow-rs/pull/2905) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([maxburke](https://github.com/maxburke))
- Update chrono-tz requirement from 0.6 to 0.7 [\#2903](https://github.com/apache/arrow-rs/pull/2903) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Parquet record api support timestamp before epoch [\#2899](https://github.com/apache/arrow-rs/pull/2899) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([AnthonyPoncet](https://github.com/AnthonyPoncet))
- Specialize interleave integer [\#2898](https://github.com/apache/arrow-rs/pull/2898) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support overflow-checking variant of negate kernel [\#2893](https://github.com/apache/arrow-rs/pull/2893) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Respect Page Size Limits in ArrowWriter \(\#2853\) [\#2890](https://github.com/apache/arrow-rs/pull/2890) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Improve row format docs [\#2888](https://github.com/apache/arrow-rs/pull/2888) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add FixedSizeList::from\_iter\_primitive [\#2887](https://github.com/apache/arrow-rs/pull/2887) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify ListArray::from\_iter\_primitive [\#2886](https://github.com/apache/arrow-rs/pull/2886) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out value selection kernels into arrow-select \(\#2594\) [\#2885](https://github.com/apache/arrow-rs/pull/2885) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Increase default IPC alignment to 64 \(\#2883\) [\#2884](https://github.com/apache/arrow-rs/pull/2884) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Copying inappropriately aligned buffer in ipc reader [\#2883](https://github.com/apache/arrow-rs/pull/2883) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Validate decimal IPC read \(\#2387\) [\#2880](https://github.com/apache/arrow-rs/pull/2880) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix compilation error under `chrono-tz` feature [\#2879](https://github.com/apache/arrow-rs/pull/2879) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Don't validate decimal precision in ArrayData \(\#2637\) [\#2873](https://github.com/apache/arrow-rs/pull/2873) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add downcast\_integer and downcast\_primitive [\#2872](https://github.com/apache/arrow-rs/pull/2872) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Filter DecimalArray as PrimitiveArray ~5x Faster \(\#2637\) [\#2870](https://github.com/apache/arrow-rs/pull/2870) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Treat DecimalArray as PrimitiveArray in row format [\#2866](https://github.com/apache/arrow-rs/pull/2866) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [25.0.0](https://github.com/apache/arrow-rs/tree/25.0.0) (2022-10-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/24.0.0...25.0.0)

**Breaking changes:**

- Make DecimalArray as PrimitiveArray [\#2857](https://github.com/apache/arrow-rs/pull/2857) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- fix timestamp parsing while no explicit timezone given [\#2814](https://github.com/apache/arrow-rs/pull/2814) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Support Arbitrary Number of Arrays in downcast\_primitive\_array [\#2809](https://github.com/apache/arrow-rs/pull/2809) ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Restore Integration test JSON schema serialization  [\#2876](https://github.com/apache/arrow-rs/issues/2876) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix various invalid\_html\_tags clippy error [\#2861](https://github.com/apache/arrow-rs/issues/2861) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Replace complicated temporal macro with generic functions [\#2851](https://github.com/apache/arrow-rs/issues/2851) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add NaN handling in dyn scalar comparison kernels [\#2829](https://github.com/apache/arrow-rs/issues/2829) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variant of sum kernel [\#2821](https://github.com/apache/arrow-rs/issues/2821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update to Clap 4 [\#2817](https://github.com/apache/arrow-rs/issues/2817) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Safe API to Operate on Dictionary Values [\#2797](https://github.com/apache/arrow-rs/issues/2797) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add modulus op into `ArrowNativeTypeOp` [\#2753](https://github.com/apache/arrow-rs/issues/2753) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow creating of TimeUnit instances without direct dependency on parquet-format [\#2708](https://github.com/apache/arrow-rs/issues/2708) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Arrow Row Format [\#2677](https://github.com/apache/arrow-rs/issues/2677) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Don't try to infer nulls in CSV schema inference [\#2859](https://github.com/apache/arrow-rs/issues/2859) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `parquet::arrow::arrow_writer::ArrowWriter` ignores page size properties [\#2853](https://github.com/apache/arrow-rs/issues/2853) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Introducing ArrowNativeTypeOp made it impossible to call kernels from generics [\#2839](https://github.com/apache/arrow-rs/issues/2839) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unsound ArrayData to Array Conversions [\#2834](https://github.com/apache/arrow-rs/issues/2834) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Regression: `the trait bound for<'de> arrow::datatypes::Schema: serde::de::Deserialize<'de> is not satisfied` [\#2825](https://github.com/apache/arrow-rs/issues/2825) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- convert string to timestamp shouldn't apply local timezone offset if there's no explicit timezone info in the string [\#2813](https://github.com/apache/arrow-rs/issues/2813) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add pub api for checking column index is sorted [\#2848](https://github.com/apache/arrow-rs/issues/2848) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Take decimal as primitive \(\#2637\) [\#2869](https://github.com/apache/arrow-rs/pull/2869) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-integration-test crate [\#2868](https://github.com/apache/arrow-rs/pull/2868) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Decimal cleanup \(\#2637\) [\#2865](https://github.com/apache/arrow-rs/pull/2865) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix various invalid\_html\_tags clippy errors [\#2862](https://github.com/apache/arrow-rs/pull/2862) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))
- Don't try to infer nullability in CSV reader [\#2860](https://github.com/apache/arrow-rs/pull/2860) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Fix page size on dictionary fallback [\#2854](https://github.com/apache/arrow-rs/pull/2854) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Replace complicated temporal macro with generic functions [\#2850](https://github.com/apache/arrow-rs/pull/2850) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- \[feat\] Add pub api for checking column index is sorted. [\#2849](https://github.com/apache/arrow-rs/pull/2849) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- parquet: Add `snap` option to README [\#2847](https://github.com/apache/arrow-rs/pull/2847) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([exyi](https://github.com/exyi))
- Cleanup cast kernel [\#2846](https://github.com/apache/arrow-rs/pull/2846) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify ArrowNativeType [\#2841](https://github.com/apache/arrow-rs/pull/2841) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Expose ArrowNativeTypeOp trait to make it useful for type bound [\#2840](https://github.com/apache/arrow-rs/pull/2840) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `interleave` kernel \(\#1523\) [\#2838](https://github.com/apache/arrow-rs/pull/2838) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Handle empty offsets buffer \(\#1824\) [\#2836](https://github.com/apache/arrow-rs/pull/2836) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Validate ArrayData type when converting to Array \(\#2834\) [\#2835](https://github.com/apache/arrow-rs/pull/2835) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Derive ArrowPrimitiveType for Decimal128Type and Decimal256Type \(\#2637\) [\#2833](https://github.com/apache/arrow-rs/pull/2833) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add NaN handling in dyn scalar comparison kernels [\#2830](https://github.com/apache/arrow-rs/pull/2830) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Simplify OrderPreservingInterner allocation strategy ~97% faster \(\#2677\) [\#2827](https://github.com/apache/arrow-rs/pull/2827) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Convert rows to arrays \(\#2677\) [\#2826](https://github.com/apache/arrow-rs/pull/2826) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add overflow-checking variant of sum kernel [\#2822](https://github.com/apache/arrow-rs/pull/2822) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update Clap dependency to version 4 [\#2819](https://github.com/apache/arrow-rs/pull/2819) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jgoday](https://github.com/jgoday))
- Fix i256 checked multiplication [\#2818](https://github.com/apache/arrow-rs/pull/2818) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add string\_dictionary benches for row format \(\#2677\) [\#2816](https://github.com/apache/arrow-rs/pull/2816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add OrderPreservingInterner::lookup \(\#2677\) [\#2815](https://github.com/apache/arrow-rs/pull/2815) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify FixedLengthEncoding [\#2812](https://github.com/apache/arrow-rs/pull/2812) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Implement ArrowNumericType for Float16Type [\#2810](https://github.com/apache/arrow-rs/pull/2810) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add DictionaryArray::with\_values to make it easier to operate on dictionary values [\#2798](https://github.com/apache/arrow-rs/pull/2798) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add i256 \(\#2637\) [\#2781](https://github.com/apache/arrow-rs/pull/2781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add modulus ops into `ArrowNativeTypeOp` [\#2756](https://github.com/apache/arrow-rs/pull/2756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- feat: cast List / LargeList to Utf8 / LargeUtf8 [\#2588](https://github.com/apache/arrow-rs/pull/2588) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gandronchik](https://github.com/gandronchik))

## [24.0.0](https://github.com/apache/arrow-rs/tree/24.0.0) (2022-09-30)

[Full Changelog](https://github.com/apache/arrow-rs/compare/23.0.0...24.0.0)

**Breaking changes:**

- Cleanup `ArrowNativeType` \(\#1918\) [\#2793](https://github.com/apache/arrow-rs/pull/2793) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `ArrowNativeType::FromStr` [\#2775](https://github.com/apache/arrow-rs/pull/2775) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out `arrow-array`  crate \(\#2594\) [\#2769](https://github.com/apache/arrow-rs/pull/2769) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add `dyn_arith_dict` feature flag [\#2760](https://github.com/apache/arrow-rs/pull/2760) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out `arrow-data` into a separate crate [\#2746](https://github.com/apache/arrow-rs/pull/2746) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-schema \(\#2594\) [\#2711](https://github.com/apache/arrow-rs/pull/2711) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Include field name in Parquet PrimitiveTypeBuilder error messages [\#2804](https://github.com/apache/arrow-rs/issues/2804) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add PrimitiveArray::reinterpret\_cast [\#2785](https://github.com/apache/arrow-rs/issues/2785)
- BinaryBuilder and StringBuilder initialization parameters in struct\_builder may be wrong [\#2783](https://github.com/apache/arrow-rs/issues/2783) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide scalar dyn kernel which produces null for division by zero [\#2767](https://github.com/apache/arrow-rs/issues/2767) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide dyn kernel which produces null for division by zero [\#2763](https://github.com/apache/arrow-rs/issues/2763) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of checked kernels on non-null data [\#2747](https://github.com/apache/arrow-rs/issues/2747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variants of arithmetic dyn kernels [\#2739](https://github.com/apache/arrow-rs/issues/2739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The `binary` function should not panic on unequaled array length. [\#2721](https://github.com/apache/arrow-rs/issues/2721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- min compute kernel is incorrect with sliced buffers in arrow 23 [\#2779](https://github.com/apache/arrow-rs/issues/2779) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `try_unary_dict` should check value type of dictionary array [\#2754](https://github.com/apache/arrow-rs/issues/2754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add back JSON import/export for schema [\#2762](https://github.com/apache/arrow-rs/issues/2762)
- null casting and coercion for Decimal128  [\#2761](https://github.com/apache/arrow-rs/issues/2761)
- Json decoder behavior changed from versions 21 to 21 and returns non-sensical num\_rows for RecordBatch [\#2722](https://github.com/apache/arrow-rs/issues/2722) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
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

## [23.0.0](https://github.com/apache/arrow-rs/tree/24.0.0) (2022-09-16)

[Full Changelog](https://github.com/apache/arrow-rs/compare/22.0.0...23.0.0)

**Breaking changes:**

- Move JSON Test Format To integration-testing [\#2724](https://github.com/apache/arrow-rs/pull/2724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Split out arrow-buffer crate \(\#2594\) [\#2693](https://github.com/apache/arrow-rs/pull/2693) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Simplify DictionaryBuilder constructors \(\#2684\) \(\#2054\) [\#2685](https://github.com/apache/arrow-rs/pull/2685) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Deprecate RecordBatch::concat replace with concat\_batches \(\#2594\) [\#2683](https://github.com/apache/arrow-rs/pull/2683) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add overflow-checking variant for primitive arithmetic kernels and explicitly define overflow behavior [\#2643](https://github.com/apache/arrow-rs/pull/2643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update thrift v0.16 and vendor parquet-format \(\#2502\) [\#2626](https://github.com/apache/arrow-rs/pull/2626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update flight definitions including backwards-incompatible change to GetSchema [\#2586](https://github.com/apache/arrow-rs/pull/2586) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([liukun4515](https://github.com/liukun4515))

**Implemented enhancements:**

- Cleanup like and nlike utf8 kernels [\#2744](https://github.com/apache/arrow-rs/issues/2744) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speedup eq and neq kernels for utf8 arrays [\#2742](https://github.com/apache/arrow-rs/issues/2742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- API for more ergonomic construction of `RecordBatchOptions` [\#2728](https://github.com/apache/arrow-rs/issues/2728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Automate updates to `CHANGELOG-old.md` [\#2726](https://github.com/apache/arrow-rs/issues/2726)
- Don't check the `DivideByZero` error for float modulus [\#2720](https://github.com/apache/arrow-rs/issues/2720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `try_binary` should not panic on unequaled array length. [\#2715](https://github.com/apache/arrow-rs/issues/2715) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add benchmark for bitwise operation [\#2714](https://github.com/apache/arrow-rs/issues/2714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variants of arithmetic scalar dyn kernels [\#2712](https://github.com/apache/arrow-rs/issues/2712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add divide\_opt kernel which produce null values on division by zero error [\#2709](https://github.com/apache/arrow-rs/issues/2709) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `DataType` function to detect nested types [\#2704](https://github.com/apache/arrow-rs/issues/2704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support of sorting dictionary of other primitive types [\#2700](https://github.com/apache/arrow-rs/issues/2700) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sort indices of dictionary string values [\#2697](https://github.com/apache/arrow-rs/issues/2697) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support empty projection in `RecordBatch::project` [\#2690](https://github.com/apache/arrow-rs/issues/2690) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support sorting dictionary encoded primitive integer arrays [\#2679](https://github.com/apache/arrow-rs/issues/2679) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use BitIndexIterator in min\_max\_helper [\#2674](https://github.com/apache/arrow-rs/issues/2674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support building comparator for dictionaries of primitive integer values [\#2672](https://github.com/apache/arrow-rs/issues/2672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change max/min string macro to generic helper function `min_max_helper` [\#2657](https://github.com/apache/arrow-rs/issues/2657) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variant of arithmetic scalar kernels [\#2651](https://github.com/apache/arrow-rs/issues/2651) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with binary array [\#2644](https://github.com/apache/arrow-rs/issues/2644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add overflow-checking variant for primitive arithmetic kernels [\#2642](https://github.com/apache/arrow-rs/issues/2642) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `downcast_primitive_array` in arithmetic kernels [\#2639](https://github.com/apache/arrow-rs/issues/2639) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support DictionaryArray in temporal kernels [\#2622](https://github.com/apache/arrow-rs/issues/2622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Inline Generated Thift Code Into Parquet Crate [\#2502](https://github.com/apache/arrow-rs/issues/2502) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Escape contains patterns for utf8 like kernels [\#2745](https://github.com/apache/arrow-rs/issues/2745) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Float Array should not panic on `DivideByZero` in the `Divide` kernel [\#2719](https://github.com/apache/arrow-rs/issues/2719) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- DictionaryBuilders can Create Invalid DictionaryArrays [\#2684](https://github.com/apache/arrow-rs/issues/2684) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `arrow` crate does not build with `features = ["ffi"]` and `default_features = false`. [\#2670](https://github.com/apache/arrow-rs/issues/2670) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Invalid results with `RowSelector` having `row_count` of 0 [\#2669](https://github.com/apache/arrow-rs/issues/2669) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- clippy error: unresolved import `crate::array::layout` [\#2659](https://github.com/apache/arrow-rs/issues/2659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Cast the numeric without the `CastOptions` [\#2648](https://github.com/apache/arrow-rs/issues/2648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Explicitly define overflow behavior for primitive arithmetic kernels [\#2641](https://github.com/apache/arrow-rs/issues/2641) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- update the `flight.proto` and fix schema to SchemaResult [\#2571](https://github.com/apache/arrow-rs/issues/2571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Panic when first data page is skipped using ColumnChunkData::Sparse [\#2543](https://github.com/apache/arrow-rs/issues/2543) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `SchemaResult` in IPC deviates from other implementations [\#2445](https://github.com/apache/arrow-rs/issues/2445) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]

**Closed issues:**

- Implement collect for int values [\#2696](https://github.com/apache/arrow-rs/issues/2696) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Speedup string equal/not equal to empty string, cleanup like/ilike kernels, fix escape bug [\#2743](https://github.com/apache/arrow-rs/pull/2743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Partially flatten arrow-buffer [\#2737](https://github.com/apache/arrow-rs/pull/2737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Automate updates to `CHANGELOG-old.md` [\#2732](https://github.com/apache/arrow-rs/pull/2732) ([iajoiner](https://github.com/iajoiner))
- Update read parquet example in parquet/arrow home [\#2730](https://github.com/apache/arrow-rs/pull/2730) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([datapythonista](https://github.com/datapythonista))
- Better construction of RecordBatchOptions [\#2729](https://github.com/apache/arrow-rs/pull/2729) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([askoa](https://github.com/askoa))
- benchmark: bitwise operation [\#2718](https://github.com/apache/arrow-rs/pull/2718) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Update `try_binary` and `checked_ops`, and remove `math_checked_op` [\#2717](https://github.com/apache/arrow-rs/pull/2717) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support bitwise op in kernel: or,xor,not [\#2716](https://github.com/apache/arrow-rs/pull/2716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Add overflow-checking variants of arithmetic scalar dyn kernels [\#2713](https://github.com/apache/arrow-rs/pull/2713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add divide\_opt kernel which produce null values on division by zero error [\#2710](https://github.com/apache/arrow-rs/pull/2710) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add DataType::is\_nested\(\) [\#2707](https://github.com/apache/arrow-rs/pull/2707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kfastov](https://github.com/kfastov))
- Update criterion requirement from 0.3 to 0.4 [\#2706](https://github.com/apache/arrow-rs/pull/2706) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Support bitwise and operation in the kernel [\#2703](https://github.com/apache/arrow-rs/pull/2703) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Add support of sorting dictionary of other primitive arrays [\#2701](https://github.com/apache/arrow-rs/pull/2701) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Clarify docs of binary and string builders [\#2699](https://github.com/apache/arrow-rs/pull/2699) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([datapythonista](https://github.com/datapythonista))
- Sort indices of dictionary string values [\#2698](https://github.com/apache/arrow-rs/pull/2698) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add support for empty projection in RecordBatch::project [\#2691](https://github.com/apache/arrow-rs/pull/2691) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Temporarily disable Golang integration tests re-enable JS [\#2689](https://github.com/apache/arrow-rs/pull/2689) ([tustvold](https://github.com/tustvold))
- Verify valid UTF-8 when converting byte array \(\#2205\) [\#2686](https://github.com/apache/arrow-rs/pull/2686) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support sorting dictionary encoded primitive integer arrays [\#2680](https://github.com/apache/arrow-rs/pull/2680) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Skip RowSelectors with zero rows [\#2678](https://github.com/apache/arrow-rs/pull/2678) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([askoa](https://github.com/askoa))
- Faster Null Path Selection in ArrayData Equality [\#2676](https://github.com/apache/arrow-rs/pull/2676) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dhruv9vats](https://github.com/dhruv9vats))
- Use BitIndexIterator in min\_max\_helper [\#2675](https://github.com/apache/arrow-rs/pull/2675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support building comparator for dictionaries of primitive integer values [\#2673](https://github.com/apache/arrow-rs/pull/2673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- json feature always requires base64 feature [\#2668](https://github.com/apache/arrow-rs/pull/2668) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([eagletmt](https://github.com/eagletmt))
- Add try\_unary, binary, try\_binary kernels ~90% faster [\#2666](https://github.com/apache/arrow-rs/pull/2666) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use downcast\_dictionary\_array in unary\_dyn [\#2663](https://github.com/apache/arrow-rs/pull/2663) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- optimize the `numeric_cast_with_error` [\#2661](https://github.com/apache/arrow-rs/pull/2661) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- ffi feature also requires layout [\#2660](https://github.com/apache/arrow-rs/pull/2660) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Change max/min string macro to generic helper function min\_max\_helper [\#2658](https://github.com/apache/arrow-rs/pull/2658) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix flaky test `test_fuzz_async_reader_selection` [\#2656](https://github.com/apache/arrow-rs/pull/2656) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- MINOR: Ignore flaky test test\_fuzz\_async\_reader\_selection [\#2655](https://github.com/apache/arrow-rs/pull/2655) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- MutableBuffer::typed\_data - shared ref access to the typed slice [\#2652](https://github.com/apache/arrow-rs/pull/2652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([medwards](https://github.com/medwards))
- Overflow-checking variant of arithmetic scalar kernels [\#2650](https://github.com/apache/arrow-rs/pull/2650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- support `CastOption` for casting numeric [\#2649](https://github.com/apache/arrow-rs/pull/2649) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Help LLVM vectorize comparison kernel ~50-80% faster [\#2646](https://github.com/apache/arrow-rs/pull/2646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support comparison between dictionary array and binary array [\#2645](https://github.com/apache/arrow-rs/pull/2645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use `downcast_primitive_array` in arithmetic kernels [\#2640](https://github.com/apache/arrow-rs/pull/2640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fully qualifying parquet items [\#2638](https://github.com/apache/arrow-rs/pull/2638) ([dingxiangfei2009](https://github.com/dingxiangfei2009))
- Support DictionaryArray in temporal kernels [\#2623](https://github.com/apache/arrow-rs/pull/2623) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Comparable Row Format [\#2593](https://github.com/apache/arrow-rs/pull/2593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Fix bug in page skipping [\#2552](https://github.com/apache/arrow-rs/pull/2552) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))

## [22.0.0](https://github.com/apache/arrow-rs/tree/22.0.0) (2022-09-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/21.0.0...22.0.0)

**Breaking changes:**

- Use `total_cmp` for floating value ordering and remove `nan_ordering` feature flag [\#2614](https://github.com/apache/arrow-rs/pull/2614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Gate dyn comparison of dictionary arrays behind `dyn_cmp_dict` [\#2597](https://github.com/apache/arrow-rs/pull/2597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Move JsonSerializable to json module \(\#2300\) [\#2595](https://github.com/apache/arrow-rs/pull/2595) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Decimal precision scale datatype change [\#2532](https://github.com/apache/arrow-rs/pull/2532) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor PrimitiveBuilder Constructors [\#2518](https://github.com/apache/arrow-rs/pull/2518) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactoring DecimalBuilder constructors [\#2517](https://github.com/apache/arrow-rs/pull/2517) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor FixedSizeBinaryBuilder Constructors [\#2516](https://github.com/apache/arrow-rs/pull/2516) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor BooleanBuilder Constructors [\#2515](https://github.com/apache/arrow-rs/pull/2515) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Refactor UnionBuilder Constructors [\#2488](https://github.com/apache/arrow-rs/pull/2488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))

**Implemented enhancements:**

- Add  Macros to assist with static dispatch [\#2635](https://github.com/apache/arrow-rs/issues/2635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support comparison between DictionaryArray and BooleanArray [\#2617](https://github.com/apache/arrow-rs/issues/2617) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use `total_cmp` for floating value ordering and remove `nan_ordering` feature flag [\#2613](https://github.com/apache/arrow-rs/issues/2613) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support empty projection in CSV, JSON readers [\#2603](https://github.com/apache/arrow-rs/issues/2603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support SQL-compliant NaN ordering between for DictionaryArray and non-DictionaryArray [\#2599](https://github.com/apache/arrow-rs/issues/2599) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `dyn_cmp_dict` feature flag to gate dyn comparison of dictionary arrays [\#2596](https://github.com/apache/arrow-rs/issues/2596) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add max\_dyn and min\_dyn for max/min for dictionary array [\#2584](https://github.com/apache/arrow-rs/issues/2584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow FlightSQL implementers to extend `do_get()` [\#2581](https://github.com/apache/arrow-rs/issues/2581) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- Support SQL-compliant behavior on `eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#2569](https://github.com/apache/arrow-rs/issues/2569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add sql-compliant feature for enabling sql-compliant kernel behavior [\#2568](https://github.com/apache/arrow-rs/issues/2568)
- Calculate `sum` for dictionary array [\#2565](https://github.com/apache/arrow-rs/issues/2565) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add test for float nan comparison [\#2556](https://github.com/apache/arrow-rs/issues/2556) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with string array [\#2548](https://github.com/apache/arrow-rs/issues/2548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with primitive array in `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#2538](https://github.com/apache/arrow-rs/issues/2538) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Compare dictionary with primitive array in `eq_dyn` and `neq_dyn` [\#2535](https://github.com/apache/arrow-rs/issues/2535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- UnionBuilder Create Children With Capacity [\#2523](https://github.com/apache/arrow-rs/issues/2523) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up `like_utf8_scalar` for `%pat%` [\#2519](https://github.com/apache/arrow-rs/issues/2519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace macro with TypedDictionaryArray in comparison kernels [\#2513](https://github.com/apache/arrow-rs/issues/2513) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use same codebase for boolean kernels [\#2507](https://github.com/apache/arrow-rs/issues/2507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Use u8 for Decimal Precision and Scale [\#2496](https://github.com/apache/arrow-rs/issues/2496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Integrate skip row without pageIndex in SerializedPageReader in Fuzz Test [\#2475](https://github.com/apache/arrow-rs/issues/2475) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Avoid unnecessary copies in Arrow IPC reader [\#2437](https://github.com/apache/arrow-rs/issues/2437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add GenericColumnReader::skip\_records Missing OffsetIndex Fallback [\#2433](https://github.com/apache/arrow-rs/issues/2433) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Reading PageIndex with ParquetRecordBatchStream [\#2430](https://github.com/apache/arrow-rs/issues/2430) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Specialize FixedLenByteArrayReader for Parquet [\#2318](https://github.com/apache/arrow-rs/issues/2318) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make JSON support Optional via Feature Flag [\#2300](https://github.com/apache/arrow-rs/issues/2300) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- Casting timestamp array to string should not ignore timezone [\#2607](https://github.com/apache/arrow-rs/issues/2607) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Ilike\_ut8\_scalar kernels have incorrect logic [\#2544](https://github.com/apache/arrow-rs/issues/2544) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Always validate the array data when creating array in IPC reader [\#2541](https://github.com/apache/arrow-rs/issues/2541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Int96Converter Truncates Timestamps [\#2480](https://github.com/apache/arrow-rs/issues/2480) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Error Reading Page Index When Not Available  [\#2434](https://github.com/apache/arrow-rs/issues/2434) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ParquetFileArrowReader::get_record_reader[_by_column]` `batch_size` overallocates [\#2321](https://github.com/apache/arrow-rs/issues/2321) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Document All Arrow Features in docs.rs [\#2633](https://github.com/apache/arrow-rs/issues/2633) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Add support for CAST from `Interval(DayTime)` to `Timestamp(Nanosecond, None)` [\#2606](https://github.com/apache/arrow-rs/issues/2606) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Why do we check for null in TypedDictionaryArray value function [\#2564](https://github.com/apache/arrow-rs/issues/2564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add the `length` field for `Buffer` [\#2524](https://github.com/apache/arrow-rs/issues/2524) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Avoid large over allocate buffer in async reader [\#2512](https://github.com/apache/arrow-rs/issues/2512) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rewriting Decimal Builders using `const_generic`. [\#2390](https://github.com/apache/arrow-rs/issues/2390) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rewrite Decimal Array using `const_generic` [\#2384](https://github.com/apache/arrow-rs/issues/2384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Add downcast macros \(\#2635\) [\#2636](https://github.com/apache/arrow-rs/pull/2636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Document all arrow features in docs.rs \(\#2633\) [\#2634](https://github.com/apache/arrow-rs/pull/2634) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Document dyn\_cmp\_dict [\#2624](https://github.com/apache/arrow-rs/pull/2624) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support comparison between DictionaryArray and BooleanArray [\#2618](https://github.com/apache/arrow-rs/pull/2618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Cast timestamp array to string array with timezone [\#2608](https://github.com/apache/arrow-rs/pull/2608) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support empty projection in CSV and JSON readers [\#2604](https://github.com/apache/arrow-rs/pull/2604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Make JSON support optional via a feature flag \(\#2300\) [\#2601](https://github.com/apache/arrow-rs/pull/2601) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support SQL-compliant NaN ordering for DictionaryArray and non-DictionaryArray [\#2600](https://github.com/apache/arrow-rs/pull/2600) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Split out integration test plumbing \(\#2594\) \(\#2300\) [\#2598](https://github.com/apache/arrow-rs/pull/2598) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Refactor Binary Builder and String Builder Constructors [\#2592](https://github.com/apache/arrow-rs/pull/2592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Dictionary like scalar kernels [\#2591](https://github.com/apache/arrow-rs/pull/2591) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Validate dictionary key in TypedDictionaryArray \(\#2578\) [\#2589](https://github.com/apache/arrow-rs/pull/2589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add max\_dyn and min\_dyn for max/min for dictionary array [\#2585](https://github.com/apache/arrow-rs/pull/2585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Code cleanup of array value functions [\#2583](https://github.com/apache/arrow-rs/pull/2583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Allow overriding of do\_get & export useful macro [\#2582](https://github.com/apache/arrow-rs/pull/2582) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- MINOR: Upgrade to pyo3 0.17 [\#2576](https://github.com/apache/arrow-rs/pull/2576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Support SQL-compliant NaN behavior on eq\_dyn, neq\_dyn, lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn [\#2570](https://github.com/apache/arrow-rs/pull/2570) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add sum\_dyn to calculate sum for dictionary array [\#2566](https://github.com/apache/arrow-rs/pull/2566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- struct UnionBuilder will create child buffers with capacity [\#2560](https://github.com/apache/arrow-rs/pull/2560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kastolars](https://github.com/kastolars))
- Don't panic on RleValueEncoder::flush\_buffer if empty \(\#2558\) [\#2559](https://github.com/apache/arrow-rs/pull/2559) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add the `length` field for Buffer and use more `Buffer` in IPC reader to avoid memory copy. [\#2557](https://github.com/apache/arrow-rs/pull/2557) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([HaoYang670](https://github.com/HaoYang670))
- Add test for float nan comparison [\#2555](https://github.com/apache/arrow-rs/pull/2555) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Compare dictionary array with string array [\#2549](https://github.com/apache/arrow-rs/pull/2549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Always validate the array data \(except the `Decimal`\) when creating array in IPC reader [\#2547](https://github.com/apache/arrow-rs/pull/2547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- MINOR: Fix test\_row\_type\_validation test [\#2546](https://github.com/apache/arrow-rs/pull/2546) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix ilike\_utf8\_scalar kernels [\#2545](https://github.com/apache/arrow-rs/pull/2545) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- fix typo [\#2540](https://github.com/apache/arrow-rs/pull/2540) ([00Masato](https://github.com/00Masato))
- Compare dictionary array and primitive array in lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn kernels [\#2539](https://github.com/apache/arrow-rs/pull/2539) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- \[MINOR\]Avoid large over allocate buffer in async reader [\#2537](https://github.com/apache/arrow-rs/pull/2537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Compare dictionary with primitive array in `eq_dyn` and `neq_dyn` [\#2533](https://github.com/apache/arrow-rs/pull/2533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add iterator for FixedSizeBinaryArray [\#2531](https://github.com/apache/arrow-rs/pull/2531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- add bench: decimal with byte array and fixed length byte array [\#2529](https://github.com/apache/arrow-rs/pull/2529) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Add FixedLengthByteArrayReader Remove ComplexObjectArrayReader [\#2528](https://github.com/apache/arrow-rs/pull/2528) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out byte array decoders \(\#2318\) [\#2527](https://github.com/apache/arrow-rs/pull/2527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Use offset index in ParquetRecordBatchStream [\#2526](https://github.com/apache/arrow-rs/pull/2526) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Clean the `create_array` in IPC reader. [\#2525](https://github.com/apache/arrow-rs/pull/2525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove DecimalByteArrayConvert \(\#2480\) [\#2522](https://github.com/apache/arrow-rs/pull/2522) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Improve performance of `%pat%` \(\>3x speedup\) [\#2521](https://github.com/apache/arrow-rs/pull/2521) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- remove len field from MapBuilder [\#2520](https://github.com/apache/arrow-rs/pull/2520) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
-  Replace macro with TypedDictionaryArray in comparison kernels [\#2514](https://github.com/apache/arrow-rs/pull/2514) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Avoid large over allocate buffer in sync reader [\#2511](https://github.com/apache/arrow-rs/pull/2511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Avoid useless memory copies in IPC reader. [\#2510](https://github.com/apache/arrow-rs/pull/2510) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor boolean kernels to use same codebase [\#2508](https://github.com/apache/arrow-rs/pull/2508) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove Int96Converter \(\#2480\) [\#2481](https://github.com/apache/arrow-rs/pull/2481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

## [21.0.0](https://github.com/apache/arrow-rs/tree/21.0.0) (2022-08-18)

[Full Changelog](https://github.com/apache/arrow-rs/compare/20.0.0...21.0.0)

**Breaking changes:**

- Return structured `ColumnCloseResult` \(\#2465\) [\#2466](https://github.com/apache/arrow-rs/pull/2466) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Push `ChunkReader` into `SerializedPageReader` \(\#2463\) [\#2464](https://github.com/apache/arrow-rs/pull/2464) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Revise FromIterator for Decimal128Array to use Into instead of Borrow [\#2442](https://github.com/apache/arrow-rs/pull/2442) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use Fixed-Length Array in BasicDecimal new and raw\_value [\#2405](https://github.com/apache/arrow-rs/pull/2405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove deprecated ParquetWriter [\#2380](https://github.com/apache/arrow-rs/pull/2380) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove deprecated SliceableCursor and InMemoryWriteableCursor [\#2378](https://github.com/apache/arrow-rs/pull/2378) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- add into\_inner method to ArrowWriter [\#2491](https://github.com/apache/arrow-rs/issues/2491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove byteorder dependency [\#2472](https://github.com/apache/arrow-rs/issues/2472) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Return Structured ColumnCloseResult from GenericColumnWriter::close [\#2465](https://github.com/apache/arrow-rs/issues/2465) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Push `ChunkReader` into `SerializedPageReader` [\#2463](https://github.com/apache/arrow-rs/issues/2463) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support SerializedPageReader::skip\_page without OffsetIndex [\#2459](https://github.com/apache/arrow-rs/issues/2459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support Time64/Time32 comparison [\#2457](https://github.com/apache/arrow-rs/issues/2457) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Revise FromIterator for Decimal128Array to use Into instead of Borrow [\#2441](https://github.com/apache/arrow-rs/issues/2441) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `RowFilter` within`ParquetRecordBatchReader` [\#2431](https://github.com/apache/arrow-rs/issues/2431) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove the field `StructBuilder::len` [\#2429](https://github.com/apache/arrow-rs/issues/2429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Standardize creation and configuration of parquet --\> Arrow readers \( `ParquetRecordBatchReaderBuilder`\) [\#2427](https://github.com/apache/arrow-rs/issues/2427) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Use `OffsetIndex` to Prune IO in `ParquetRecordBatchStream` [\#2426](https://github.com/apache/arrow-rs/issues/2426) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `peek_next_page` and `skip_next_page` in `InMemoryPageReader` [\#2406](https://github.com/apache/arrow-rs/issues/2406) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support casting from `Utf8`/`LargeUtf8` to `Binary`/`LargeBinary` [\#2402](https://github.com/apache/arrow-rs/issues/2402) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting between `Decimal128` and `Decimal256` arrays [\#2375](https://github.com/apache/arrow-rs/issues/2375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Combine multiple selections into the same batch size in `skip_records` [\#2358](https://github.com/apache/arrow-rs/issues/2358) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add API to change timezone for timestamp array [\#2346](https://github.com/apache/arrow-rs/issues/2346) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change the output of `read_buffer` Arrow IPC API to return `Result<_>` [\#2342](https://github.com/apache/arrow-rs/issues/2342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Allow `skip_records` in `GenericColumnReader` to skip across row groups [\#2331](https://github.com/apache/arrow-rs/issues/2331) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Optimize the validation of `Decimal256` [\#2320](https://github.com/apache/arrow-rs/issues/2320) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement Skip for `DeltaBitPackDecoder` [\#2281](https://github.com/apache/arrow-rs/issues/2281) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Changes to `ParquetRecordBatchStream` to support row filtering in DataFusion [\#2270](https://github.com/apache/arrow-rs/issues/2270) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `ArrayReader::skip_records` API [\#2197](https://github.com/apache/arrow-rs/issues/2197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Panic in SerializedPageReader without offset index [\#2503](https://github.com/apache/arrow-rs/issues/2503) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- MapArray columns don't handle null values correctly [\#2484](https://github.com/apache/arrow-rs/issues/2484) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- There is no compiler error when using an invalid Decimal type. [\#2440](https://github.com/apache/arrow-rs/issues/2440) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Flight SQL Server sends incorrect response for `DoPutUpdateResult` [\#2403](https://github.com/apache/arrow-rs/issues/2403) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- `AsyncFileReader`No Longer Object-Safe [\#2372](https://github.com/apache/arrow-rs/issues/2372) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StructBuilder Does not Verify Child Lengths [\#2252](https://github.com/apache/arrow-rs/issues/2252) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Combine `DecimalArray` validation [\#2447](https://github.com/apache/arrow-rs/issues/2447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Merged pull requests:**

- Fix bug in page skipping [\#2504](https://github.com/apache/arrow-rs/pull/2504) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Fix `MapArrayReader` \(\#2484\) \(\#1699\) \(\#1561\) [\#2500](https://github.com/apache/arrow-rs/pull/2500) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add API to Retrieve Finished Writer from Parquet Writer [\#2498](https://github.com/apache/arrow-rs/pull/2498) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([jiacai2050](https://github.com/jiacai2050))
- Derive Copy,Clone for BasicDecimal [\#2495](https://github.com/apache/arrow-rs/pull/2495) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- remove byteorder dependency from parquet [\#2486](https://github.com/apache/arrow-rs/pull/2486) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([psvri](https://github.com/psvri))
- parquet-read: add support to read parquet data from stdin [\#2482](https://github.com/apache/arrow-rs/pull/2482) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nvartolomei](https://github.com/nvartolomei))
- Remove Position trait \(\#1163\) [\#2479](https://github.com/apache/arrow-rs/pull/2479) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add ChunkReader::get\_bytes [\#2478](https://github.com/apache/arrow-rs/pull/2478) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- RFC: Simplify decimal \(\#2440\) [\#2477](https://github.com/apache/arrow-rs/pull/2477) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use Parquet OffsetIndex to prune IO with RowSelection [\#2473](https://github.com/apache/arrow-rs/pull/2473) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([thinkharderdev](https://github.com/thinkharderdev))
- Remove unnecessary Option from Int96 [\#2471](https://github.com/apache/arrow-rs/pull/2471) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- remove len field from StructBuilder [\#2468](https://github.com/apache/arrow-rs/pull/2468) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Make Parquet reader filter APIs public \(\#1792\) [\#2467](https://github.com/apache/arrow-rs/pull/2467) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- enable ipc compression feature for integration test [\#2462](https://github.com/apache/arrow-rs/pull/2462) ([liukun4515](https://github.com/liukun4515))
- Simplify implementation of Schema [\#2461](https://github.com/apache/arrow-rs/pull/2461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support skip\_page missing OffsetIndex Fallback in SerializedPageReader [\#2460](https://github.com/apache/arrow-rs/pull/2460) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- support time32/time64 comparison [\#2458](https://github.com/apache/arrow-rs/pull/2458) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waitingkuo](https://github.com/waitingkuo))
- Utf8array casting [\#2456](https://github.com/apache/arrow-rs/pull/2456) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- Remove outdated license text [\#2455](https://github.com/apache/arrow-rs/pull/2455) ([alamb](https://github.com/alamb))
- Support RowFilter within ParquetRecordBatchReader \(\#2431\) [\#2452](https://github.com/apache/arrow-rs/pull/2452) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- benchmark: decimal builder and vec to decimal array [\#2450](https://github.com/apache/arrow-rs/pull/2450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Collocate Decimal Array Validation Logic [\#2446](https://github.com/apache/arrow-rs/pull/2446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Minor: Move From trait for Decimal256 impl to decimal.rs [\#2443](https://github.com/apache/arrow-rs/pull/2443) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- decimal benchmark: arrow reader decimal from parquet int32 and int64 [\#2438](https://github.com/apache/arrow-rs/pull/2438) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- MINOR: Simplify `split_second` function [\#2436](https://github.com/apache/arrow-rs/pull/2436) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add ParquetRecordBatchReaderBuilder \(\#2427\) [\#2435](https://github.com/apache/arrow-rs/pull/2435) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: refine validation for decimal128 array [\#2428](https://github.com/apache/arrow-rs/pull/2428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Benchmark of casting decimal arrays [\#2424](https://github.com/apache/arrow-rs/pull/2424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Test non-annotated repeated fields \(\#2394\) [\#2422](https://github.com/apache/arrow-rs/pull/2422) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix \#2416 Automatic version updates for github actions with dependabot [\#2417](https://github.com/apache/arrow-rs/pull/2417) ([iemejia](https://github.com/iemejia))
- Add validation logic for StructBuilder::finish [\#2413](https://github.com/apache/arrow-rs/pull/2413) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([psvri](https://github.com/psvri))
- test: add test for reading decimal value from primitive array reader [\#2411](https://github.com/apache/arrow-rs/pull/2411) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([liukun4515](https://github.com/liukun4515))
- Upgrade ahash to 0.8 [\#2410](https://github.com/apache/arrow-rs/pull/2410) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Support peek\_next\_page and skip\_next\_page in InMemoryPageReader [\#2407](https://github.com/apache/arrow-rs/pull/2407) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Fix DoPutUpdateResult [\#2404](https://github.com/apache/arrow-rs/pull/2404) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([avantgardnerio](https://github.com/avantgardnerio))
- Implement Skip for DeltaBitPackDecoder [\#2393](https://github.com/apache/arrow-rs/pull/2393) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- fix: Don't instantiate the scalar composition code quadratically for dictionaries [\#2391](https://github.com/apache/arrow-rs/pull/2391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Marwes](https://github.com/Marwes))
- MINOR: Remove unused trait and some cleanup [\#2389](https://github.com/apache/arrow-rs/pull/2389) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Decouple parquet fuzz tests from converter \(\#1661\) [\#2386](https://github.com/apache/arrow-rs/pull/2386) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rewrite `Decimal` and `DecimalArray` using `const_generic` [\#2383](https://github.com/apache/arrow-rs/pull/2383) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Simplify BitReader \(~5-10% faster\) [\#2381](https://github.com/apache/arrow-rs/pull/2381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix parquet clippy lints \(\#1254\) [\#2377](https://github.com/apache/arrow-rs/pull/2377) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Cast between `Decimal128` and `Decimal256` arrays [\#2376](https://github.com/apache/arrow-rs/pull/2376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- support compression for IPC with revamped feature flags [\#2369](https://github.com/apache/arrow-rs/pull/2369) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement AsyncFileReader for `Box<dyn AsyncFileReader>` [\#2368](https://github.com/apache/arrow-rs/pull/2368) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove get\_byte\_ranges where bound [\#2366](https://github.com/apache/arrow-rs/pull/2366) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- refactor: Make read\_num\_bytes a function instead of a macro [\#2364](https://github.com/apache/arrow-rs/pull/2364) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- refactor: Group metrics into page and column metrics structs [\#2363](https://github.com/apache/arrow-rs/pull/2363) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- Speed up `Decimal256` validation based on bytes comparison and add benchmark test [\#2360](https://github.com/apache/arrow-rs/pull/2360) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Combine multiple selections into the same batch size in skip\_records [\#2359](https://github.com/apache/arrow-rs/pull/2359) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add API to change timezone for timestamp array [\#2347](https://github.com/apache/arrow-rs/pull/2347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Clean the code in `field.rs` and add more tests [\#2345](https://github.com/apache/arrow-rs/pull/2345) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add Parquet RowFilter API [\#2335](https://github.com/apache/arrow-rs/pull/2335) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Make skip\_records in complex\_object\_array can skip cross row groups [\#2332](https://github.com/apache/arrow-rs/pull/2332) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Integrate Record Skipping into Column Reader Fuzz Test [\#2315](https://github.com/apache/arrow-rs/pull/2315) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Ted-Jiang](https://github.com/Ted-Jiang))

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

## [19.0.0](https://github.com/apache/arrow-rs/tree/19.0.0) (2022-07-22)

[Full Changelog](https://github.com/apache/arrow-rs/compare/18.0.0...19.0.0)

**Breaking changes:**

- Rename `DecimalArray``/DecimalBuilder` to `Decimal128Array`/`Decimal128Builder` [\#2101](https://github.com/apache/arrow-rs/issues/2101) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Change builder `append` methods to be infallible where possible [\#2103](https://github.com/apache/arrow-rs/pull/2103) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Return reference from `UnionArray::child` \(\#2035\) [\#2099](https://github.com/apache/arrow-rs/pull/2099) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Remove `preserve_order` feature from `serde_json` dependency \(\#2095\) [\#2098](https://github.com/apache/arrow-rs/pull/2098) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `weekday` and `weekday0` kernels to to `num_days_from_monday` and `num_days_since_sunday` [\#2066](https://github.com/apache/arrow-rs/pull/2066) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove `null_count` from `write_batch_with_statistics` [\#2047](https://github.com/apache/arrow-rs/pull/2047) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Use `total_cmp` from std  [\#2130](https://github.com/apache/arrow-rs/issues/2130) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Permit parallel fetching of column chunks in `ParquetRecordBatchStream` [\#2110](https://github.com/apache/arrow-rs/issues/2110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `GenericBinaryBuilder` should use buffer builders directly. [\#2104](https://github.com/apache/arrow-rs/issues/2104) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pass `generate_decimal256_case` arrow integration test [\#2093](https://github.com/apache/arrow-rs/issues/2093) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Rename `weekday` and `weekday0` kernels to to `num_days_from_monday` and `days_since_sunday` [\#2065](https://github.com/apache/arrow-rs/issues/2065) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Improve performance of `filter_dict` [\#2062](https://github.com/apache/arrow-rs/issues/2062) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `set_bits` [\#2060](https://github.com/apache/arrow-rs/issues/2060) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Lazily materialize the null buffer builder of `BooleanBuilder` [\#2058](https://github.com/apache/arrow-rs/issues/2058) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `BooleanArray::from_iter` should omit validity buffer if all values are valid [\#2055](https://github.com/apache/arrow-rs/issues/2055) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FFI\_ArrowSchema should set `DICTIONARY_ORDERED` flag if a field's dictionary is ordered [\#2049](https://github.com/apache/arrow-rs/issues/2049) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `peek_next_page()` and `skip_next_page` in `SerializedPageReader` [\#2043](https://github.com/apache/arrow-rs/issues/2043) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support FFI / C Data Interface for `MapType` [\#2037](https://github.com/apache/arrow-rs/issues/2037) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The `DecimalArrayBuilder` should use `FixedSizedBinaryBuilder` [\#2026](https://github.com/apache/arrow-rs/issues/2026) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Enable `serialized_reader` read specific Page by passing row ranges. [\#1976](https://github.com/apache/arrow-rs/issues/1976) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- `type_id` and `value_offset` are incorrect for sliced `UnionArray` [\#2086](https://github.com/apache/arrow-rs/issues/2086) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Boolean `take` kernel does not handle null indices correctly [\#2057](https://github.com/apache/arrow-rs/issues/2057) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Don't double-count nulls in `write_batch_with_statistics` [\#2046](https://github.com/apache/arrow-rs/issues/2046) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Writer Ignores Statistics specification in `WriterProperties` [\#2014](https://github.com/apache/arrow-rs/issues/2014) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Improve docstrings + examples for `as_primitive_array` cast functions [\#2114](https://github.com/apache/arrow-rs/pull/2114) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

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

- Use `total_cmp` from std [\#2131](https://github.com/apache/arrow-rs/pull/2131) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- fix clippy [\#2124](https://github.com/apache/arrow-rs/pull/2124) ([alamb](https://github.com/alamb))
- Fix logical merge conflict: `match` arms have incompatible types [\#2121](https://github.com/apache/arrow-rs/pull/2121) ([alamb](https://github.com/alamb))
- Update `GenericBinaryBuilder` to use buffer builders directly. [\#2117](https://github.com/apache/arrow-rs/pull/2117) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Simplify null mask preservation in parquet reader [\#2116](https://github.com/apache/arrow-rs/pull/2116) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
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

- Array reader for list columns fails to decode if batches fall on row group boundaries [\#2025](https://github.com/apache/arrow-rs/issues/2025) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
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
- Separate get\_next\_page\_header from get\_next\_page in PageReader [\#1834](https://github.com/apache/arrow-rs/issues/1834) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

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
- PyArrow integration test for C Stream Interface [\#1847](https://github.com/apache/arrow-rs/issues/1847) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
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


## [15.0.0](https://github.com/apache/arrow-rs/tree/15.0.0) (2022-05-27)

[Full Changelog](https://github.com/apache/arrow-rs/compare/14.0.0...15.0.0)

**Breaking changes:**

- Change `ArrayDataBuilder::null_bit_buffer` to accept `Option<Buffer>` rather than `Buffer` [\#1739](https://github.com/apache/arrow-rs/pull/1739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove `null_count` from `ArrayData::try_new()` [\#1721](https://github.com/apache/arrow-rs/pull/1721) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Change parquet writers to use standard `std:io::Write` rather custom `ParquetWriter` trait \(\#1717\) \(\#1163\) [\#1719](https://github.com/apache/arrow-rs/pull/1719) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add explicit column mask for selection in parquet: `ProjectionMask` \(\#1701\) [\#1716](https://github.com/apache/arrow-rs/pull/1716) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add type\_ids in Union datatype [\#1703](https://github.com/apache/arrow-rs/pull/1703) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Parquet Reader's Arrow Schema Inference [\#1682](https://github.com/apache/arrow-rs/pull/1682) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Rename the `string` kernel to `concatenate_elements` [\#1747](https://github.com/apache/arrow-rs/issues/1747) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `ArrayDataBuilder::null_bit_buffer` should accept `Option<Buffer>` as input type [\#1737](https://github.com/apache/arrow-rs/issues/1737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix schema comparison for non\_canonical\_map when running flight test [\#1730](https://github.com/apache/arrow-rs/issues/1730) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support in aggregate kernel for `BinaryArray` [\#1724](https://github.com/apache/arrow-rs/issues/1724) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix incorrect null\_count in `generate_unions_case` integration test [\#1712](https://github.com/apache/arrow-rs/issues/1712) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Keep type ids in Union datatype to follow Arrow spec and integrate with other implementations [\#1690](https://github.com/apache/arrow-rs/issues/1690) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Reading Alternative List Representations to Arrow From Parquet [\#1680](https://github.com/apache/arrow-rs/issues/1680) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Speed up the offsets checking [\#1675](https://github.com/apache/arrow-rs/issues/1675) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Separate Parquet -\> Arrow Schema Conversion From ArrayBuilder [\#1655](https://github.com/apache/arrow-rs/issues/1655) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `leaf_columns` argument to `ArrowReader::get_record_reader_by_columns` [\#1653](https://github.com/apache/arrow-rs/issues/1653) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Implement `string_concat` kernel  [\#1540](https://github.com/apache/arrow-rs/issues/1540) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve Unit Test Coverage of ArrayReaderBuilder [\#1484](https://github.com/apache/arrow-rs/issues/1484) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Parquet write failure \(from record batches\) when data is nested two levels deep  [\#1744](https://github.com/apache/arrow-rs/issues/1744) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- IPC reader may break on projection [\#1735](https://github.com/apache/arrow-rs/issues/1735) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Latest nightly fails to build with feature simd [\#1734](https://github.com/apache/arrow-rs/issues/1734) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Trying to write parquet file in parallel results in corrupt file [\#1717](https://github.com/apache/arrow-rs/issues/1717) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Roundtrip failure when using DELTA\_BINARY\_PACKED [\#1708](https://github.com/apache/arrow-rs/issues/1708) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `ArrayData::try_new` cannot always return expected error. [\#1707](https://github.com/apache/arrow-rs/issues/1707) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  "out of order projection is not supported" after Fix Parquet Arrow Schema Inference [\#1701](https://github.com/apache/arrow-rs/issues/1701) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Rust is not interoperability with C++ for IPC schemas with dictionaries [\#1694](https://github.com/apache/arrow-rs/issues/1694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect Repeated Field Schema Inference [\#1681](https://github.com/apache/arrow-rs/issues/1681) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Treats Embedded Arrow Schema as Authoritative [\#1663](https://github.com/apache/arrow-rs/issues/1663) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- parquet\_to\_arrow\_schema\_by\_columns Incorrectly Handles Nested Types [\#1654](https://github.com/apache/arrow-rs/issues/1654) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Inconsistent Arrow Schema When Projecting Nested Parquet File [\#1652](https://github.com/apache/arrow-rs/issues/1652) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- StructArrayReader Cannot Handle Nested Lists [\#1651](https://github.com/apache/arrow-rs/issues/1651) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Bug \(`substring` kernel\): The null buffer is not aligned when `offset != 0` [\#1639](https://github.com/apache/arrow-rs/issues/1639) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Parquet command line tool does not install "globally" [\#1710](https://github.com/apache/arrow-rs/issues/1710) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Improve integration test document to follow Arrow C++ repo CI [\#1742](https://github.com/apache/arrow-rs/pull/1742) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Merged pull requests:**

- Test for list array equality with different offsets [\#1756](https://github.com/apache/arrow-rs/pull/1756) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Rename `string_concat` to `concat_elements_utf8` [\#1754](https://github.com/apache/arrow-rs/pull/1754) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Rename the `string` kernel to `concat_elements`. [\#1752](https://github.com/apache/arrow-rs/pull/1752) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support writing nested lists to parquet [\#1746](https://github.com/apache/arrow-rs/pull/1746) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Pin nightly version to bypass packed\_simd build error [\#1743](https://github.com/apache/arrow-rs/pull/1743) ([viirya](https://github.com/viirya))
- Fix projection in IPC reader [\#1736](https://github.com/apache/arrow-rs/pull/1736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([iyupeng](https://github.com/iyupeng))
- `cargo install` installs not globally [\#1732](https://github.com/apache/arrow-rs/pull/1732) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kazuk](https://github.com/kazuk))
- Fix schema comparison for non\_canonical\_map when running flight test [\#1731](https://github.com/apache/arrow-rs/pull/1731) ([viirya](https://github.com/viirya))
- Add `min_binary` and `max_binary` aggregate kernels [\#1725](https://github.com/apache/arrow-rs/pull/1725) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix parquet benchmarks [\#1723](https://github.com/apache/arrow-rs/pull/1723) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix BitReader::get\_batch zero extension \(\#1708\) [\#1722](https://github.com/apache/arrow-rs/pull/1722) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Implementation string concat [\#1720](https://github.com/apache/arrow-rs/pull/1720) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ismail-Maj](https://github.com/Ismail-Maj))
- Check the length of `null_bit_buffer` in `ArrayData::try_new()` [\#1714](https://github.com/apache/arrow-rs/pull/1714) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix incorrect null\_count in `generate_unions_case` integration test [\#1713](https://github.com/apache/arrow-rs/pull/1713) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix: Null buffer accounts for `offset` in `substring` kernel. [\#1704](https://github.com/apache/arrow-rs/pull/1704) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Minor: Refine `OffsetSizeTrait` to extend `num::Integer`  [\#1702](https://github.com/apache/arrow-rs/pull/1702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix StructArrayReader handling nested lists \(\#1651\)  [\#1700](https://github.com/apache/arrow-rs/pull/1700) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Speed up the offsets checking [\#1684](https://github.com/apache/arrow-rs/pull/1684) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))

## [14.0.0](https://github.com/apache/arrow-rs/tree/14.0.0) (2022-05-13)

[Full Changelog](https://github.com/apache/arrow-rs/compare/13.0.0...14.0.0)

**Breaking changes:**

- Use `bytes` in parquet rather than custom Buffer implementation \(\#1474\) [\#1683](https://github.com/apache/arrow-rs/pull/1683) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Rename  `OffsetSize::fn is_large` to `const OffsetSize::IS_LARGE` [\#1664](https://github.com/apache/arrow-rs/pull/1664) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove `StringOffsetTrait` and `BinaryOffsetTrait` [\#1645](https://github.com/apache/arrow-rs/pull/1645) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix `generate_nested_dictionary_case` integration test failure  [\#1636](https://github.com/apache/arrow-rs/pull/1636) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([viirya](https://github.com/viirya))

**Implemented enhancements:**

- Add support for `DataType::Duration` in ffi interface [\#1688](https://github.com/apache/arrow-rs/issues/1688) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix `generate_unions_case` integration test  [\#1676](https://github.com/apache/arrow-rs/issues/1676) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Add `DictionaryArray` support for `bit_length` kernel [\#1673](https://github.com/apache/arrow-rs/issues/1673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Add `DictionaryArray` support for `length` kernel [\#1672](https://github.com/apache/arrow-rs/issues/1672) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- flight\_client\_scenarios integration test should receive schema from flight data [\#1669](https://github.com/apache/arrow-rs/issues/1669) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Unpin Flatbuffer version dependency [\#1667](https://github.com/apache/arrow-rs/issues/1667) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add dictionary array support for substring function [\#1656](https://github.com/apache/arrow-rs/issues/1656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Exclude dict\_id and dict\_is\_ordered from equality comparison of `Field` [\#1646](https://github.com/apache/arrow-rs/issues/1646) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove `StringOffsetTrait` and `BinaryOffsetTrait` [\#1644](https://github.com/apache/arrow-rs/issues/1644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add tests and examples for `UnionArray::from(data: ArrayData)` [\#1643](https://github.com/apache/arrow-rs/issues/1643) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add methods `pub fn offsets_buffer`, `pub fn types_ids_buffer`and `pub fn data_buffer` for `ArrayDataBuilder` [\#1640](https://github.com/apache/arrow-rs/issues/1640) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix `generate_nested_dictionary_case` integration test failure for Rust cases [\#1635](https://github.com/apache/arrow-rs/issues/1635) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose `ArrowWriter` row group flush in public API [\#1626](https://github.com/apache/arrow-rs/issues/1626) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `substring` support for `FixedSizeBinaryArray` [\#1618](https://github.com/apache/arrow-rs/issues/1618) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add PrettyPrint for `UnionArray`s [\#1594](https://github.com/apache/arrow-rs/issues/1594) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add SIMD support for the `length` kernel [\#1489](https://github.com/apache/arrow-rs/issues/1489) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support dictionary arrays in length and bit\_length [\#1674](https://github.com/apache/arrow-rs/pull/1674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add dictionary array support for substring function [\#1665](https://github.com/apache/arrow-rs/pull/1665) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Add `DecimalType` support in `new_null_array ` [\#1659](https://github.com/apache/arrow-rs/pull/1659) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))

**Fixed bugs:**

- Docs.rs build is broken  [\#1695](https://github.com/apache/arrow-rs/issues/1695)
- Interoperability with C++ for IPC schemas with dictionaries [\#1694](https://github.com/apache/arrow-rs/issues/1694)
- `UnionArray::is_null` incorrect [\#1625](https://github.com/apache/arrow-rs/issues/1625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Published Parquet documentation missing `arrow::async_reader` [\#1617](https://github.com/apache/arrow-rs/issues/1617) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Files written with Julia's Arrow.jl in IPC format cannot be read by arrow-rs [\#1335](https://github.com/apache/arrow-rs/issues/1335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Correct arrow-flight readme version [\#1641](https://github.com/apache/arrow-rs/pull/1641) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Make `OffsetSizeTrait::IS_LARGE` as a const value [\#1658](https://github.com/apache/arrow-rs/issues/1658)
- Question: Why are there 3 types of `OffsetSizeTrait`s? [\#1638](https://github.com/apache/arrow-rs/issues/1638)
- Written Parquet file way bigger than input files  [\#1627](https://github.com/apache/arrow-rs/issues/1627)
- Ensure there is a single zero in the offsets buffer for an empty ListArray. [\#1620](https://github.com/apache/arrow-rs/issues/1620)
- Filtering `UnionArray` Changes DataType [\#1595](https://github.com/apache/arrow-rs/issues/1595)

**Merged pull requests:**

- Fix docs.rs build [\#1696](https://github.com/apache/arrow-rs/pull/1696) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- support duration in ffi [\#1689](https://github.com/apache/arrow-rs/pull/1689) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ryan-jacobs1](https://github.com/ryan-jacobs1))
- fix bench command line options [\#1685](https://github.com/apache/arrow-rs/pull/1685) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- Enable branch protection [\#1679](https://github.com/apache/arrow-rs/pull/1679) ([tustvold](https://github.com/tustvold))
- Fix logical merge conflict in \#1588 [\#1678](https://github.com/apache/arrow-rs/pull/1678) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix generate\_unions\_case for Rust case [\#1677](https://github.com/apache/arrow-rs/pull/1677) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Receive schema from flight data [\#1670](https://github.com/apache/arrow-rs/pull/1670) ([viirya](https://github.com/viirya))
- unpin flatbuffers dependency version [\#1668](https://github.com/apache/arrow-rs/pull/1668) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Cheappie](https://github.com/Cheappie))
- Remove parquet dictionary converters \(\#1661\) [\#1662](https://github.com/apache/arrow-rs/pull/1662) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Minor: simplify the function `GenericListArray::get_type` [\#1650](https://github.com/apache/arrow-rs/pull/1650) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Pretty Print `UnionArray`s [\#1648](https://github.com/apache/arrow-rs/pull/1648) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))
- Exclude `dict_id` and `dict_is_ordered` from equality comparison of `Field` [\#1647](https://github.com/apache/arrow-rs/pull/1647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- expose row-group flush in public api [\#1634](https://github.com/apache/arrow-rs/pull/1634) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Cheappie](https://github.com/Cheappie))
- Add `substring` support for `FixedSizeBinaryArray` [\#1633](https://github.com/apache/arrow-rs/pull/1633) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix UnionArray is\_null [\#1632](https://github.com/apache/arrow-rs/pull/1632) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Do not assume dictionaries exists in footer [\#1631](https://github.com/apache/arrow-rs/pull/1631) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pcjentsch](https://github.com/pcjentsch))
- Add support for nested list arrays from parquet to arrow arrays \(\#993\) [\#1588](https://github.com/apache/arrow-rs/pull/1588) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add `async` into doc features [\#1349](https://github.com/apache/arrow-rs/pull/1349) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HaoYang670](https://github.com/HaoYang670))


## [13.0.0](https://github.com/apache/arrow-rs/tree/13.0.0) (2022-04-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/12.0.0...13.0.0)

**Breaking changes:**

- Update `parquet::basic::LogicalType` to be more idomatic [\#1612](https://github.com/apache/arrow-rs/pull/1612) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tfeda](https://github.com/tfeda))
- Fix Null Mask Handling in `ArrayData`,  `UnionArray`, and `MapArray` [\#1589](https://github.com/apache/arrow-rs/pull/1589) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Replace `&Option<T>`  with `Option<&T>` in several `arrow` and `parquet` APIs [\#1571](https://github.com/apache/arrow-rs/pull/1571) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tfeda](https://github.com/tfeda))

**Implemented enhancements:**

- Read/write nested dictionary under fixed size list in ipc stream reader/write [\#1609](https://github.com/apache/arrow-rs/issues/1609) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `BinaryArray` in `substring`  kernel [\#1593](https://github.com/apache/arrow-rs/issues/1593) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read/write nested dictionary under large list in ipc stream reader/write [\#1584](https://github.com/apache/arrow-rs/issues/1584) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read/write nested dictionary under map in ipc stream reader/write [\#1582](https://github.com/apache/arrow-rs/issues/1582) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement `Clone` for JSON `DecoderOptions` [\#1580](https://github.com/apache/arrow-rs/issues/1580) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add utf-8 validation checking to `substring` kernel [\#1575](https://github.com/apache/arrow-rs/issues/1575) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support casting to/from `DataType::Null` in `cast` kernel [\#1572](https://github.com/apache/arrow-rs/pull/1572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([WinkerDu](https://github.com/WinkerDu))

**Fixed bugs:**

- Parquet schema should allow scale == precision for decimal type [\#1606](https://github.com/apache/arrow-rs/issues/1606) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- ListArray::from\(ArrayData\) dereferences invalid pointer when offsets are empty [\#1601](https://github.com/apache/arrow-rs/issues/1601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ArrayData Equality Incorrect Null Mask Offset Handling [\#1599](https://github.com/apache/arrow-rs/issues/1599)
- Filtering UnionArray Incorrect Handles Runs [\#1598](https://github.com/apache/arrow-rs/issues/1598)
- \[Safety\] Filtering Dense UnionArray Produces Invalid Offsets [\#1596](https://github.com/apache/arrow-rs/issues/1596)
- \[Safety\] UnionBuilder Doesn't Check Types [\#1591](https://github.com/apache/arrow-rs/issues/1591)
- Union Layout Should Not Support Separate Validity Mask [\#1590](https://github.com/apache/arrow-rs/issues/1590)
- Incorrect nullable flag when reading maps \( test\_read\_maps fails when `force_validate` is active\)  [\#1587](https://github.com/apache/arrow-rs/issues/1587) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Output of `ipc::reader::tests::projection_should_work` fails validation [\#1548](https://github.com/apache/arrow-rs/issues/1548) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Incorrect min/max statistics for decimals with byte-array notation [\#1532](https://github.com/apache/arrow-rs/issues/1532)

**Documentation updates:**

- Minor: Clarify docs on `UnionBuilder::append_null` [\#1628](https://github.com/apache/arrow-rs/pull/1628) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Dense UnionArray Offsets Are i32 not i8 [\#1597](https://github.com/apache/arrow-rs/issues/1597) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Replace `&Option<T>` with `Option<&T>` in some APIs [\#1556](https://github.com/apache/arrow-rs/issues/1556) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve ergonomics of `parquet::basic::LogicalType`  [\#1554](https://github.com/apache/arrow-rs/issues/1554) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Mark the current `substring` function as `unsafe` and rename it. [\#1541](https://github.com/apache/arrow-rs/issues/1541) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Requirements for Async Parquet API [\#1473](https://github.com/apache/arrow-rs/issues/1473) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Nit: use the standard function `div_ceil`  [\#1629](https://github.com/apache/arrow-rs/pull/1629) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update flatbuffers requirement from =2.1.1 to =2.1.2 [\#1622](https://github.com/apache/arrow-rs/pull/1622) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix decimals min max statistics [\#1621](https://github.com/apache/arrow-rs/pull/1621) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([atefsawaed](https://github.com/atefsawaed))
- Add example readme [\#1615](https://github.com/apache/arrow-rs/pull/1615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve docs and examples links on main readme [\#1614](https://github.com/apache/arrow-rs/pull/1614) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Read/Write nested dictionaries under FixedSizeList in IPC [\#1610](https://github.com/apache/arrow-rs/pull/1610) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add `substring` support for binary [\#1608](https://github.com/apache/arrow-rs/pull/1608) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Parquet: schema validation should allow scale == precision for decimal type [\#1607](https://github.com/apache/arrow-rs/pull/1607) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sunchao](https://github.com/sunchao))
- Don't access and validate offset buffer in ListArray::from\(ArrayData\) [\#1602](https://github.com/apache/arrow-rs/pull/1602) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix map nullable flag in `ParquetTypeConverter` [\#1592](https://github.com/apache/arrow-rs/pull/1592) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary under large list in ipc stream reader/writer [\#1585](https://github.com/apache/arrow-rs/pull/1585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary under map in ipc stream reader/writer [\#1583](https://github.com/apache/arrow-rs/pull/1583) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Derive `Clone` and `PartialEq` for json `DecoderOptions` [\#1581](https://github.com/apache/arrow-rs/pull/1581) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add utf-8 validation checking for `substring` [\#1577](https://github.com/apache/arrow-rs/pull/1577) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use `Option<T>` rather than `Option<&T>` for copy types in substring kernel [\#1576](https://github.com/apache/arrow-rs/pull/1576) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Use littleendian arrow files for `projection_should_work` [\#1573](https://github.com/apache/arrow-rs/pull/1573) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))


## [12.0.0](https://github.com/apache/arrow-rs/tree/12.0.0) (2022-04-15)

[Full Changelog](https://github.com/apache/arrow-rs/compare/11.1.0...12.0.0)

**Breaking changes:**

- Add `ArrowReaderOptions` to `ParquetFileArrowReader`, add option to skip decoding arrow metadata from parquet \(\#1459\) [\#1558](https://github.com/apache/arrow-rs/pull/1558) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support `RecordBatch` with zero columns but non zero row count, add field to `RecordBatchOptions` \(\#1536\) [\#1552](https://github.com/apache/arrow-rs/pull/1552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Consolidate JSON Reader options and `DecoderOptions` [\#1539](https://github.com/apache/arrow-rs/pull/1539) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update `prost`, `prost-derive` and `prost-types` to 0.10, `tonic`, and `tonic-build` to `0.7` [\#1510](https://github.com/apache/arrow-rs/pull/1510) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Add Json `DecoderOptions` and support custom `format_string` for each field  [\#1451](https://github.com/apache/arrow-rs/pull/1451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))

**Implemented enhancements:**

- Read/write nested dictionary in ipc stream reader/writer [\#1565](https://github.com/apache/arrow-rs/issues/1565) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `FixedSizeBinary` in the Arrow C data interface [\#1553](https://github.com/apache/arrow-rs/issues/1553) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support Empty Column Projection in `ParquetRecordBatchReader` [\#1537](https://github.com/apache/arrow-rs/issues/1537) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Support `RecordBatch` with zero columns but non zero row count [\#1536](https://github.com/apache/arrow-rs/issues/1536) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for `Date32`/`Date64`\<--\> `String`/`LargeString` in `cast` kernel [\#1535](https://github.com/apache/arrow-rs/issues/1535) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support creating arrays from externally owned memory like `Vec` or `String` [\#1516](https://github.com/apache/arrow-rs/issues/1516) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up the `substring` kernel [\#1511](https://github.com/apache/arrow-rs/issues/1511) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Handle Parquet Files With Inconsistent Timestamp Units [\#1459](https://github.com/apache/arrow-rs/issues/1459) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Error Inferring Schema for LogicalType::UNKNOWN [\#1557](https://github.com/apache/arrow-rs/issues/1557) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Read dictionary from nested struct in ipc stream reader panics [\#1549](https://github.com/apache/arrow-rs/issues/1549) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `filter` produces invalid sparse `UnionArray`s [\#1547](https://github.com/apache/arrow-rs/issues/1547) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Documentation for `GenericListBuilder` is not exposed.  [\#1518](https://github.com/apache/arrow-rs/issues/1518) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- cannot read parquet file  [\#1515](https://github.com/apache/arrow-rs/issues/1515) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- The `substring` kernel panics when chars \> U+0x007F [\#1478](https://github.com/apache/arrow-rs/issues/1478) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Hang due to infinite loop when reading some parquet files with RLE encoding and bit packing [\#1458](https://github.com/apache/arrow-rs/issues/1458) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- Improve JSON reader documentation [\#1559](https://github.com/apache/arrow-rs/pull/1559) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve doc string for `substring` kernel [\#1529](https://github.com/apache/arrow-rs/pull/1529) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Expose documentation of `GenericListBuilder` [\#1525](https://github.com/apache/arrow-rs/pull/1525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([comath](https://github.com/comath))
- Add a diagram to `take` kernel documentation [\#1524](https://github.com/apache/arrow-rs/pull/1524) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Closed issues:**

- Interesting benchmark results of `min_max_helper` [\#1400](https://github.com/apache/arrow-rs/issues/1400)

**Merged pull requests:**

- Fix incorrect `into_buffers` for UnionArray [\#1567](https://github.com/apache/arrow-rs/pull/1567) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Read/write nested dictionary in ipc stream reader/writer [\#1566](https://github.com/apache/arrow-rs/pull/1566) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support FixedSizeBinary and FixedSizeList for the C data interface [\#1564](https://github.com/apache/arrow-rs/pull/1564) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Split out ListArrayReader into separate module \(\#1483\) [\#1563](https://github.com/apache/arrow-rs/pull/1563) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Split out `MapArray` into separate module \(\#1483\) [\#1562](https://github.com/apache/arrow-rs/pull/1562) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Support empty projection in `ParquetRecordBatchReader` [\#1560](https://github.com/apache/arrow-rs/pull/1560) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- fix infinite loop in not fully packed bit-packed runs [\#1555](https://github.com/apache/arrow-rs/pull/1555) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add test for creating FixedSizeBinaryArray::try\_from\_sparse\_iter failed when given all Nones [\#1551](https://github.com/apache/arrow-rs/pull/1551) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix reading dictionaries from nested structs in ipc `StreamReader` [\#1550](https://github.com/apache/arrow-rs/pull/1550) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dispanser](https://github.com/dispanser))
- Add support for Date32/64 \<--\> String/LargeString in `cast` kernel [\#1534](https://github.com/apache/arrow-rs/pull/1534) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- fix clippy errors in 1.60 [\#1527](https://github.com/apache/arrow-rs/pull/1527) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Mark `remove-old-releases.sh` executable [\#1522](https://github.com/apache/arrow-rs/pull/1522) ([alamb](https://github.com/alamb))
- Delete duplicate code in the `sort` kernel [\#1519](https://github.com/apache/arrow-rs/pull/1519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix reading nested lists from parquet files  [\#1517](https://github.com/apache/arrow-rs/pull/1517) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Speed up the `substring` kernel by about 2x [\#1512](https://github.com/apache/arrow-rs/pull/1512) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add `new_from_strings` to create `MapArrays` [\#1507](https://github.com/apache/arrow-rs/pull/1507) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Decouple buffer deallocation from ffi and allow creating buffers from rust vec [\#1494](https://github.com/apache/arrow-rs/pull/1494) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))

## [11.1.0](https://github.com/apache/arrow-rs/tree/11.1.0) (2022-03-31)

[Full Changelog](https://github.com/apache/arrow-rs/compare/11.0.0...11.1.0)

**Implemented enhancements:**

- Implement `size_hint` and `ExactSizedIterator` for DecimalArray [\#1505](https://github.com/apache/arrow-rs/issues/1505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support calculate length by chars for `StringArray` [\#1493](https://github.com/apache/arrow-rs/issues/1493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `length` kernel support for `ListArray` [\#1470](https://github.com/apache/arrow-rs/issues/1470) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The length kernel should work with `BinaryArray`s [\#1464](https://github.com/apache/arrow-rs/issues/1464) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- FFI for Arrow C Stream Interface [\#1348](https://github.com/apache/arrow-rs/issues/1348) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `DictionaryArray::try_new()` [\#1313](https://github.com/apache/arrow-rs/issues/1313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- MIRI error in math\_checked\_divide\_op/try\_from\_trusted\_len\_iter [\#1496](https://github.com/apache/arrow-rs/issues/1496) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet Writer Incorrect Definition Levels for Nested NullArray [\#1480](https://github.com/apache/arrow-rs/issues/1480) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FFI: ArrowArray::try\_from\_raw shouldn't clone [\#1425](https://github.com/apache/arrow-rs/issues/1425) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Parquet reader fails to read null list. [\#1399](https://github.com/apache/arrow-rs/issues/1399) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Documentation updates:**

- A small mistake in the doc of `BinaryArray` and `LargeBinaryArray` [\#1455](https://github.com/apache/arrow-rs/issues/1455) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- A small mistake in the doc of `GenericBinaryArray::take_iter_unchecked` [\#1454](https://github.com/apache/arrow-rs/issues/1454) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add links in the doc of `BinaryOffsetSizeTrait` [\#1453](https://github.com/apache/arrow-rs/issues/1453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The doc of `FixedSizeBinaryArray` is confusing. [\#1452](https://github.com/apache/arrow-rs/issues/1452) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Clarify docs that SlicesIterator ignores null values [\#1504](https://github.com/apache/arrow-rs/pull/1504) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update the doc of `BinaryArray` and `LargeBinaryArray` [\#1471](https://github.com/apache/arrow-rs/pull/1471) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))

**Closed issues:**

- `packed_simd` v.s. `portable_simd`, which should be used? [\#1492](https://github.com/apache/arrow-rs/issues/1492)
- Cleanup: Use Arrow take kernel Within parquet ListArrayReader [\#1482](https://github.com/apache/arrow-rs/issues/1482) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Implement `size_hint` and `ExactSizedIterator` for `DecimalArray` [\#1506](https://github.com/apache/arrow-rs/pull/1506) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `StringArray::num_chars` for calculating number of characters [\#1503](https://github.com/apache/arrow-rs/pull/1503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Workaround nightly miri error in `try_from_trusted_len_iter` [\#1497](https://github.com/apache/arrow-rs/pull/1497) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- update doc of array\_binary and array\_string [\#1491](https://github.com/apache/arrow-rs/pull/1491) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Use Arrow take kernel within ListArrayReader [\#1490](https://github.com/apache/arrow-rs/pull/1490) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Add `length` kernel support for List Array [\#1488](https://github.com/apache/arrow-rs/pull/1488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Support sort for `Decimal` data type [\#1487](https://github.com/apache/arrow-rs/pull/1487) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- Fix reading/writing nested null arrays \(\#1480\) \(\#1036\) \(\#1399\) [\#1481](https://github.com/apache/arrow-rs/pull/1481) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
-  Implement ArrayEqual for UnionArray [\#1469](https://github.com/apache/arrow-rs/pull/1469) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support the `length` kernel on Binary Array [\#1465](https://github.com/apache/arrow-rs/pull/1465) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove Clone and copy source structs internally [\#1449](https://github.com/apache/arrow-rs/pull/1449) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix Parquet reader for null lists [\#1448](https://github.com/apache/arrow-rs/pull/1448) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([viirya](https://github.com/viirya))
- Improve performance of DictionaryArray::try\_new\(\)  [\#1435](https://github.com/apache/arrow-rs/pull/1435) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Add FFI for Arrow C Stream Interface [\#1384](https://github.com/apache/arrow-rs/pull/1384) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

## [11.0.0](https://github.com/apache/arrow-rs/tree/11.0.0) (2022-03-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/10.0.0...11.0.0)

**Breaking changes:**

- Replace `filter_row_groups` with `ReadOptions` in parquet SerializedFileReader  [\#1389](https://github.com/apache/arrow-rs/pull/1389) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([yjshen](https://github.com/yjshen))
- Implement projection for arrow `IPC Reader` file / streams [\#1339](https://github.com/apache/arrow-rs/pull/1339) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Dandandan](https://github.com/Dandandan))

**Implemented enhancements:**

- Fix generate\_interval\_case integration test failure [\#1445](https://github.com/apache/arrow-rs/issues/1445)
- Make the doc examples of `ListArray` and `LargeListArray` more readable [\#1433](https://github.com/apache/arrow-rs/issues/1433)
- Redundant `if` and `abs` in `shift()` [\#1427](https://github.com/apache/arrow-rs/issues/1427)
- Improve substring kernel performance [\#1422](https://github.com/apache/arrow-rs/issues/1422) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add missing value\_unchecked\(\) of `FixedSizeBinaryArray` [\#1419](https://github.com/apache/arrow-rs/issues/1419)
- Remove duplicate bound check in function `shift` [\#1408](https://github.com/apache/arrow-rs/issues/1408)
- Support dictionary array in C data interface [\#1397](https://github.com/apache/arrow-rs/issues/1397)
- filter kernel should work with `UnionArray`s [\#1394](https://github.com/apache/arrow-rs/issues/1394) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- filter kernel should work with `FixedSizeListArrays`s [\#1393](https://github.com/apache/arrow-rs/issues/1393) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add doc examples for creating FixedSizeListArray [\#1392](https://github.com/apache/arrow-rs/issues/1392) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update `rust-version` to 1.59 [\#1377](https://github.com/apache/arrow-rs/issues/1377)
- Arrow IPC projection support [\#1338](https://github.com/apache/arrow-rs/issues/1338)
- Implement basic FlightSQL Server [\#1386](https://github.com/apache/arrow-rs/pull/1386) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([wangfenjin](https://github.com/wangfenjin))

**Fixed bugs:**

- DictionaryArray::try\_new ignores validity bitmap of the keys [\#1429](https://github.com/apache/arrow-rs/issues/1429) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- The doc of `GenericListArray` is confusing [\#1424](https://github.com/apache/arrow-rs/issues/1424)
- DeltaBitPackDecoder Incorrectly Handles Non-Zero MiniBlock Bit Width Padding [\#1417](https://github.com/apache/arrow-rs/issues/1417) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- DeltaBitPackEncoder Pads Miniblock BitWidths With Arbitrary Values [\#1416](https://github.com/apache/arrow-rs/issues/1416) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Possible unaligned write with MutableBuffer::push [\#1410](https://github.com/apache/arrow-rs/issues/1410) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Integration Test is failing on master branch [\#1398](https://github.com/apache/arrow-rs/issues/1398) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Rewrite doc of `GenericListArray` [\#1450](https://github.com/apache/arrow-rs/pull/1450) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix integration doc about build.ninja location [\#1438](https://github.com/apache/arrow-rs/pull/1438) ([viirya](https://github.com/viirya))

**Merged pull requests:**

- Rewrite doc example of `ListArray` and `LargeListArray` [\#1447](https://github.com/apache/arrow-rs/pull/1447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix generate\_interval\_case in integration test [\#1446](https://github.com/apache/arrow-rs/pull/1446) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix generate\_decimal128\_case in integration test [\#1440](https://github.com/apache/arrow-rs/pull/1440) ([viirya](https://github.com/viirya))
- `filter` kernel should work with FixedSizeListArrays [\#1434](https://github.com/apache/arrow-rs/pull/1434) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Support nullable keys in DictionaryArray::try\_new [\#1430](https://github.com/apache/arrow-rs/pull/1430) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- remove redundant if/clamp\_min/abs [\#1428](https://github.com/apache/arrow-rs/pull/1428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Add doc example for creating `FixedSizeListArray` [\#1426](https://github.com/apache/arrow-rs/pull/1426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Directly write to MutableBuffer in substring [\#1423](https://github.com/apache/arrow-rs/pull/1423) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fix possibly unaligned writes in MutableBuffer [\#1421](https://github.com/apache/arrow-rs/pull/1421) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Add value\_unchecked\(\) and unit test [\#1420](https://github.com/apache/arrow-rs/pull/1420) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Fix DeltaBitPack MiniBlock Bit Width Padding [\#1418](https://github.com/apache/arrow-rs/pull/1418) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Update zstd requirement from 0.10 to 0.11 [\#1415](https://github.com/apache/arrow-rs/pull/1415) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Set `default-features = false` for `zstd` in the parquet crate to support `wasm32-unknown-unknown` [\#1414](https://github.com/apache/arrow-rs/pull/1414) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kylebarron](https://github.com/kylebarron))
- Add support for `UnionArray` in`filter` kernel [\#1412](https://github.com/apache/arrow-rs/pull/1412) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Remove duplicate bound check in the function `shift` [\#1409](https://github.com/apache/arrow-rs/pull/1409) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add dictionary support for C data interface [\#1407](https://github.com/apache/arrow-rs/pull/1407) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sunchao](https://github.com/sunchao))
- Fix a small spelling mistake in docs. [\#1406](https://github.com/apache/arrow-rs/pull/1406) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add unit test to check `FixedSizeBinaryArray` input all none [\#1405](https://github.com/apache/arrow-rs/pull/1405) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jackwener](https://github.com/jackwener))
- Move csv Parser trait and its implementations to utils module [\#1385](https://github.com/apache/arrow-rs/pull/1385) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))

## [10.0.0](https://github.com/apache/arrow-rs/tree/10.0.0) (2022-03-04)

[Full Changelog](https://github.com/apache/arrow-rs/compare/9.1.0...10.0.0)

**Breaking changes:**

- Remove existing has\_ methods for optional fields in `ColumnChunkMetaData` [\#1346](https://github.com/apache/arrow-rs/pull/1346) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Remove redundant `has_` methods in `ColumnChunkMetaData` [\#1345](https://github.com/apache/arrow-rs/pull/1345) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))

**Implemented enhancements:**

- Add extract month and day  in temporal.rs [\#1387](https://github.com/apache/arrow-rs/issues/1387)
- Add clone to `IpcWriteOptions` [\#1381](https://github.com/apache/arrow-rs/issues/1381) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `MapArray` in `filter` kernel  [\#1378](https://github.com/apache/arrow-rs/issues/1378) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `week` temporal kernel [\#1375](https://github.com/apache/arrow-rs/issues/1375) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of `compare_dict_op` [\#1371](https://github.com/apache/arrow-rs/issues/1371) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for LargeUtf8 in json writer [\#1357](https://github.com/apache/arrow-rs/issues/1357) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Make `arrow::array::builder::MapBuilder` public [\#1354](https://github.com/apache/arrow-rs/issues/1354) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor `StructArray::from` [\#1351](https://github.com/apache/arrow-rs/issues/1351) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor `RecordBatch::validate_new_batch` [\#1350](https://github.com/apache/arrow-rs/issues/1350) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove redundant has\_ methods for optional column metadata fields [\#1344](https://github.com/apache/arrow-rs/issues/1344) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `write` method to JsonWriter [\#1340](https://github.com/apache/arrow-rs/issues/1340) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Refactor the code of `Bitmap::new` [\#1337](https://github.com/apache/arrow-rs/issues/1337) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
-  Use DictionaryArray's iterator in `compare_dict_op` [\#1329](https://github.com/apache/arrow-rs/issues/1329) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add  `as_decimal_array(arr: &dyn Array) -> &DecimalArray` [\#1312](https://github.com/apache/arrow-rs/issues/1312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- More ergonomic / idiomatic primitive array creation from iterators [\#1298](https://github.com/apache/arrow-rs/issues/1298) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement DictionaryArray support in `eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` [\#1201](https://github.com/apache/arrow-rs/issues/1201) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

- `cargo clippy` fails on the `master` branch [\#1362](https://github.com/apache/arrow-rs/issues/1362) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- `ArrowArray::try_from_raw` should not assume the pointers are from Arc [\#1333](https://github.com/apache/arrow-rs/issues/1333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix CSV Writer::new to accept delimiter and make WriterBuilder::build use it   [\#1328](https://github.com/apache/arrow-rs/issues/1328) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Make bounds configurable via builder when reading CSV [\#1327](https://github.com/apache/arrow-rs/issues/1327) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `with_datetime_format()` to CSV WriterBuilder  [\#1272](https://github.com/apache/arrow-rs/issues/1272) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Performance improvements:**

- Improve performance of `min` and `max` aggregation kernels without nulls [\#1373](https://github.com/apache/arrow-rs/issues/1373) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Closed issues:**

- Consider removing redundant has\_XXX metadata functions in `ColumnChunkMetadata` [\#1332](https://github.com/apache/arrow-rs/issues/1332)

**Merged pull requests:**

- Support extract `day` and `month` in temporal.rs [\#1388](https://github.com/apache/arrow-rs/pull/1388) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add write method to Json Writer [\#1383](https://github.com/apache/arrow-rs/pull/1383) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- Derive `Clone` for  `IpcWriteOptions` [\#1382](https://github.com/apache/arrow-rs/pull/1382) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- feat: support maps in MutableArrayData [\#1379](https://github.com/apache/arrow-rs/pull/1379) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))
- Support extract `week` in temporal.rs [\#1376](https://github.com/apache/arrow-rs/pull/1376) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Speed up the function `min_max_string` [\#1374](https://github.com/apache/arrow-rs/pull/1374) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Improve performance if dictionary kernels, add benchmark and add `take_iter_unchecked` [\#1372](https://github.com/apache/arrow-rs/pull/1372) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update pyo3 requirement from 0.15 to 0.16 [\#1369](https://github.com/apache/arrow-rs/pull/1369) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update contributing guide [\#1368](https://github.com/apache/arrow-rs/pull/1368) ([HaoYang670](https://github.com/HaoYang670))
- Allow primitive array creation from iterators of PrimitiveTypes \(as well as `Option`\) [\#1367](https://github.com/apache/arrow-rs/pull/1367) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Update flatbuffers requirement from =2.1.0 to =2.1.1 [\#1364](https://github.com/apache/arrow-rs/pull/1364) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix clippy lints [\#1363](https://github.com/apache/arrow-rs/pull/1363) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor `RecordBatch::validate_new_batch` [\#1361](https://github.com/apache/arrow-rs/pull/1361) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Refactor `StructArray::from` [\#1360](https://github.com/apache/arrow-rs/pull/1360) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update flatbuffers requirement from =2.0.0 to =2.1.0 [\#1359](https://github.com/apache/arrow-rs/pull/1359) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: add LargeUtf8 support in json writer [\#1358](https://github.com/apache/arrow-rs/pull/1358) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tiphaineruy](https://github.com/tiphaineruy))
- Add `as_decimal_array` function [\#1356](https://github.com/apache/arrow-rs/pull/1356) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Publicly export arrow::array::MapBuilder [\#1355](https://github.com/apache/arrow-rs/pull/1355) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tjwilson90](https://github.com/tjwilson90))
- Add with\_datetime\_format to csv WriterBuilder [\#1347](https://github.com/apache/arrow-rs/pull/1347) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Refactor `Bitmap::new` [\#1343](https://github.com/apache/arrow-rs/pull/1343) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove delimiter from csv Writer [\#1342](https://github.com/apache/arrow-rs/pull/1342) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Make bounds configurable in csv ReaderBuilder [\#1341](https://github.com/apache/arrow-rs/pull/1341) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- `ArrowArray::try_from_raw` should not assume the pointers are from Arc [\#1334](https://github.com/apache/arrow-rs/pull/1334) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Use DictionaryArray's iterator in `compare_dict_op` [\#1330](https://github.com/apache/arrow-rs/pull/1330) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Implement DictionaryArray support in neq\_dyn, lt\_dyn, lt\_eq\_dyn, gt\_dyn, gt\_eq\_dyn [\#1326](https://github.com/apache/arrow-rs/pull/1326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Arrow Rust + Conbench Integration [\#1289](https://github.com/apache/arrow-rs/pull/1289) ([dianaclarke](https://github.com/dianaclarke))

## [9.1.0](https://github.com/apache/arrow-rs/tree/9.1.0) (2022-02-19)

[Full Changelog](https://github.com/apache/arrow-rs/compare/9.0.2...9.1.0)

**Implemented enhancements:**

- Exposing page encoding stats [\#1321](https://github.com/apache/arrow-rs/issues/1321)
- Improve filter performance by special casing high and low selectivity predicates [\#1288](https://github.com/apache/arrow-rs/issues/1288) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Speed up  `DeltaBitPackDecoder` [\#1281](https://github.com/apache/arrow-rs/issues/1281) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Fix all clippy lints in arrow crate [\#1255](https://github.com/apache/arrow-rs/issues/1255) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Expose page encoding `ColumnChunkMetadata` [\#1322](https://github.com/apache/arrow-rs/pull/1322) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Expose column index and offset index in `ColumnChunkMetadata` [\#1318](https://github.com/apache/arrow-rs/pull/1318) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Expose bloom filter offset in `ColumnChunkMetadata` [\#1309](https://github.com/apache/arrow-rs/pull/1309) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Add `DictionaryArray::try_new()` to create dictionaries from pre existing arrays [\#1300](https://github.com/apache/arrow-rs/pull/1300) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add `DictionaryArray::keys_iter`, and `take_iter` for other array types [\#1296](https://github.com/apache/arrow-rs/pull/1296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Make `rle` decoder public under `experimental` feature [\#1271](https://github.com/apache/arrow-rs/pull/1271) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([zeevm](https://github.com/zeevm))
- Add `DictionaryArray` support in `eq_dyn` kernel [\#1263](https://github.com/apache/arrow-rs/pull/1263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))

**Fixed bugs:**

- `len` is not a parameter of `MutableArrayData::extend` [\#1316](https://github.com/apache/arrow-rs/issues/1316)
- module `data_type` is private in Rust Parquet 8.0.0 [\#1302](https://github.com/apache/arrow-rs/issues/1302) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Test failure: bit\_chunk\_iterator [\#1294](https://github.com/apache/arrow-rs/issues/1294)
- csv\_writer benchmark fails with "no such file or directory" [\#1292](https://github.com/apache/arrow-rs/issues/1292)

**Documentation updates:**

- Fix warnings in `cargo doc` [\#1268](https://github.com/apache/arrow-rs/pull/1268) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Vectorize DeltaBitPackDecoder, up to 5x faster decoding [\#1284](https://github.com/apache/arrow-rs/pull/1284) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Skip zero-ing primitive nulls [\#1280](https://github.com/apache/arrow-rs/pull/1280) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add specialized filter kernels in `compute` module \(up to 10x faster\) [\#1248](https://github.com/apache/arrow-rs/pull/1248) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- Expose column and offset index metadata offset [\#1317](https://github.com/apache/arrow-rs/issues/1317)
- Expose bloom filter metadata offset [\#1308](https://github.com/apache/arrow-rs/issues/1308)
- Improve ergonomics to construct `DictionaryArrays` from `Key` and `Value` arrays [\#1299](https://github.com/apache/arrow-rs/issues/1299)
- Make it easier to iterate over `DictionaryArray` [\#1295](https://github.com/apache/arrow-rs/issues/1295) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- (WON'T FIX) Don't Interwine Bit and Byte Aligned Operations in `BitReader` [\#1282](https://github.com/apache/arrow-rs/issues/1282)
- how to create arrow::array from streamReader [\#1278](https://github.com/apache/arrow-rs/issues/1278)
- Remove scientific notation when converting floats to strings. [\#983](https://github.com/apache/arrow-rs/issues/983)

**Merged pull requests:**

- Update the document of function `MutableArrayData::extend` [\#1336](https://github.com/apache/arrow-rs/pull/1336) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix clippy lint `dead_code` [\#1324](https://github.com/apache/arrow-rs/pull/1324) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- fix test bug and ensure that bloom filter metadata is serialized in `to_thrift` [\#1320](https://github.com/apache/arrow-rs/pull/1320) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([shanisolomon](https://github.com/shanisolomon))
- Enable more clippy lints in arrow  [\#1315](https://github.com/apache/arrow-rs/pull/1315) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix clippy lint `clippy::type_complexity` [\#1310](https://github.com/apache/arrow-rs/pull/1310) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix clippy lint `clippy::float_equality_without_abs` [\#1305](https://github.com/apache/arrow-rs/pull/1305) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix clippy `clippy::vec_init_then_push` lint [\#1303](https://github.com/apache/arrow-rs/pull/1303) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([gsserge](https://github.com/gsserge))
- Fix failing csv\_writer bench [\#1293](https://github.com/apache/arrow-rs/pull/1293) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([andygrove](https://github.com/andygrove))
- Changes for 9.0.2  [\#1291](https://github.com/apache/arrow-rs/pull/1291) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Fix bitmask creation also for simd comparisons with scalar [\#1290](https://github.com/apache/arrow-rs/pull/1290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix simd comparison kernels [\#1286](https://github.com/apache/arrow-rs/pull/1286) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Restrict Decoder to compatible types \(\#1276\) [\#1277](https://github.com/apache/arrow-rs/pull/1277) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix some clippy lints in parquet crate, rename `LevelEncoder` variants to conform to Rust standards [\#1273](https://github.com/apache/arrow-rs/pull/1273) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([HaoYang670](https://github.com/HaoYang670))
- Use new DecimalArray creation API in arrow crate [\#1249](https://github.com/apache/arrow-rs/pull/1249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Improve `DecimalArray` API ergonomics: add `iter()`, `FromIterator`, `with_precision_and_scale` [\#1223](https://github.com/apache/arrow-rs/pull/1223) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))


## [9.0.2](https://github.com/apache/arrow-rs/tree/9.0.2) (2022-02-09)

[Full Changelog](https://github.com/apache/arrow-rs/compare/8.0.0...9.0.2)

**Breaking changes:**

- Add  `Send` + `Sync` to `DataType`, `RowGroupReader`, `FileReader`, `ChunkReader`. [\#1264](https://github.com/apache/arrow-rs/issues/1264)
- Rename the function `Bitmap::len` to `Bitmap::bit_len` to clarify its meaning [\#1242](https://github.com/apache/arrow-rs/pull/1242) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove unused / broken `memory-check` feature [\#1222](https://github.com/apache/arrow-rs/pull/1222) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Potentially buffer  multiple `RecordBatches` before writing a parquet row group in `ArrowWriter` [\#1214](https://github.com/apache/arrow-rs/pull/1214) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Add `async` arrow parquet reader [\#1154](https://github.com/apache/arrow-rs/pull/1154) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Rename `Bitmap::len` to `Bitmap::bit_len` [\#1233](https://github.com/apache/arrow-rs/issues/1233)
- Extend CSV schema inference to allow scientific notation for floating point types [\#1215](https://github.com/apache/arrow-rs/issues/1215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Write Multiple RecordBatch to Parquet Row Group [\#1211](https://github.com/apache/arrow-rs/issues/1211)
- Add doc examples for `eq_dyn` etc. [\#1202](https://github.com/apache/arrow-rs/issues/1202) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add comparison kernels for `BinaryArray` [\#1108](https://github.com/apache/arrow-rs/issues/1108)
- `impl ArrowNativeType for i128`  [\#1098](https://github.com/apache/arrow-rs/issues/1098)
- Remove `Copy` trait bound from dyn scalar kernels [\#1243](https://github.com/apache/arrow-rs/pull/1243) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- Add `into_inner` for IPC `FileWriter` [\#1236](https://github.com/apache/arrow-rs/pull/1236) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))
- \[Minor\]Re-export `array::builder::make_builder` to make it available for downstream [\#1235](https://github.com/apache/arrow-rs/pull/1235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([yjshen](https://github.com/yjshen))

**Fixed bugs:**

- Parquet v8.0.0 panics when reading all null column to NullArray [\#1245](https://github.com/apache/arrow-rs/issues/1245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Get `Unknown configuration option rust-version` when running the rust format command [\#1240](https://github.com/apache/arrow-rs/issues/1240)
- `Bitmap` Length Validation is Incorrect [\#1231](https://github.com/apache/arrow-rs/issues/1231) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Writing sliced `ListArray` or `MapArray` ignore offsets [\#1226](https://github.com/apache/arrow-rs/issues/1226) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Remove broken `memory-tracking` crate feature [\#1171](https://github.com/apache/arrow-rs/issues/1171)
- Revert making `parquet::data_type` and `parquet::arrow::schema` experimental [\#1244](https://github.com/apache/arrow-rs/pull/1244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Documentation updates:**

- Update parquet crate documentation and examples [\#1253](https://github.com/apache/arrow-rs/pull/1253) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Refresh parquet readme / contributing guide [\#1252](https://github.com/apache/arrow-rs/pull/1252) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add docs examples for dynamically compare functions  [\#1250](https://github.com/apache/arrow-rs/pull/1250) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Add Rust Docs examples for UnionArray [\#1241](https://github.com/apache/arrow-rs/pull/1241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Improve documentation for Bitmap [\#1237](https://github.com/apache/arrow-rs/pull/1237) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve performance for arithmetic kernels with `simd` feature enabled \(except for division/modulo\) [\#1221](https://github.com/apache/arrow-rs/pull/1221) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Do not concatenate identical dictionaries [\#1219](https://github.com/apache/arrow-rs/pull/1219) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Preserve dictionary encoding when decoding parquet into Arrow arrays, 60x perf improvement \(\#171\) [\#1180](https://github.com/apache/arrow-rs/pull/1180) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- `UnalignedBitChunkIterator` to that iterates through already aligned `u64` blocks [\#1227](https://github.com/apache/arrow-rs/issues/1227)
- Remove unused `ArrowArrayReader` in parquet  [\#1197](https://github.com/apache/arrow-rs/issues/1197) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Merged pull requests:**

- Upgrade clap to 3.0.0 [\#1261](https://github.com/apache/arrow-rs/pull/1261) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Jimexist](https://github.com/Jimexist))
- Update chrono-tz requirement from 0.4 to 0.6 [\#1259](https://github.com/apache/arrow-rs/pull/1259) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Update zstd requirement from 0.9 to 0.10 [\#1257](https://github.com/apache/arrow-rs/pull/1257) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix NullArrayReader \(\#1245\) [\#1246](https://github.com/apache/arrow-rs/pull/1246) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- dyn compare for binary array [\#1238](https://github.com/apache/arrow-rs/pull/1238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Remove arrow array reader \(\#1197\) [\#1234](https://github.com/apache/arrow-rs/pull/1234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Fix null bitmap length validation \(\#1231\) [\#1232](https://github.com/apache/arrow-rs/pull/1232) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Faster bitmask iteration [\#1228](https://github.com/apache/arrow-rs/pull/1228) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add non utf8 values into the test cases of BinaryArray comparison [\#1220](https://github.com/apache/arrow-rs/pull/1220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Update DECIMAL\_RE to allow scientific notation in auto inferred schemas [\#1216](https://github.com/apache/arrow-rs/pull/1216) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([pjmore](https://github.com/pjmore))
- Fix simd comparison kernels [\#1286](https://github.com/apache/arrow-rs/pull/1286) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix bitmask creation also for simd comparisons with scalar [\#1290](https://github.com/apache/arrow-rs/pull/1290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))

## [8.0.0](https://github.com/apache/arrow-rs/tree/8.0.0) (2022-01-20)

[Full Changelog](https://github.com/apache/arrow-rs/compare/7.0.0...8.0.0)

**Breaking changes:**

- Return error from JSON writer rather than panic [\#1205](https://github.com/apache/arrow-rs/pull/1205) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Ted-Jiang](https://github.com/Ted-Jiang))
- Remove `ArrowSignedNumericType ` to Simplify and reduce code duplication in arithmetic kernels [\#1161](https://github.com/apache/arrow-rs/pull/1161) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Restrict RecordReader and friends to scalar types \(\#1132\) [\#1155](https://github.com/apache/arrow-rs/pull/1155) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Move more parquet functionality behind experimental feature flag \(\#1032\)  [\#1134](https://github.com/apache/arrow-rs/pull/1134) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Parquet reader should be able to read structs within list [\#1186](https://github.com/apache/arrow-rs/issues/1186) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Disable serde\_json `arbitrary_precision` feature flag [\#1174](https://github.com/apache/arrow-rs/issues/1174) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Simplify and reduce code duplication in arithmetic.rs [\#1160](https://github.com/apache/arrow-rs/issues/1160) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Return `Err` from JSON writer rather than `panic!` for unsupported types [\#1157](https://github.com/apache/arrow-rs/issues/1157) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `scalar` mathematics kernels for `Array` and scalar value [\#1153](https://github.com/apache/arrow-rs/issues/1153) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support `DecimalArray` in sort kernel [\#1137](https://github.com/apache/arrow-rs/issues/1137)
- Parquet Fuzz Tests [\#1053](https://github.com/apache/arrow-rs/issues/1053)
- BooleanBufferBuilder Append Packed [\#1038](https://github.com/apache/arrow-rs/issues/1038) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- parquet Performance Optimization: StructArrayReader Redundant Level & Bitmap Computation [\#1034](https://github.com/apache/arrow-rs/issues/1034) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Reduce Public Parquet API [\#1032](https://github.com/apache/arrow-rs/issues/1032) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add `from_iter_values` for binary array [\#1188](https://github.com/apache/arrow-rs/pull/1188) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Add support for `MapArray` in json writer [\#1149](https://github.com/apache/arrow-rs/pull/1149) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))

**Fixed bugs:**

- Empty string arrays with no nulls are not equal [\#1208](https://github.com/apache/arrow-rs/issues/1208) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Pretty print a `RecordBatch` containing `Float16` triggers a panic [\#1193](https://github.com/apache/arrow-rs/issues/1193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Writing structs nested in lists produces an incorrect output [\#1184](https://github.com/apache/arrow-rs/issues/1184) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Undefined behavior for `GenericStringArray::from_iter_values` if reported iterator upper bound is incorrect [\#1144](https://github.com/apache/arrow-rs/issues/1144) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Interval comparisons with `simd` feature asserts [\#1136](https://github.com/apache/arrow-rs/issues/1136) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- RecordReader Permits Illegal Types [\#1132](https://github.com/apache/arrow-rs/issues/1132) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Security fixes:**

- Fix undefined behavor in GenericStringArray::from\_iter\_values [\#1145](https://github.com/apache/arrow-rs/pull/1145) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
-  parquet: Optimized ByteArrayReader, Add UTF-8 Validation \(\#1040\)  [\#1082](https://github.com/apache/arrow-rs/pull/1082) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Documentation updates:**

- Update parquet crate readme [\#1192](https://github.com/apache/arrow-rs/pull/1192) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Document safety justification of some uses of `from_trusted_len_iter` [\#1148](https://github.com/apache/arrow-rs/pull/1148) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Performance improvements:**

- Improve parquet reading performance for columns with nulls by preserving bitmask when possible \(\#1037\) [\#1054](https://github.com/apache/arrow-rs/pull/1054) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Improve parquet performance: Skip levels computation for required struct arrays in parquet [\#1035](https://github.com/apache/arrow-rs/pull/1035) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))

**Closed issues:**

- Generify ColumnReaderImpl and RecordReader [\#1040](https://github.com/apache/arrow-rs/issues/1040) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet Preserve BitMask [\#1037](https://github.com/apache/arrow-rs/issues/1037)

**Merged pull requests:**

- fix a bug in variable sized equality [\#1209](https://github.com/apache/arrow-rs/pull/1209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))
- Pin WASM / packed SIMD tests to nightly-2022-01-17 [\#1204](https://github.com/apache/arrow-rs/pull/1204) ([alamb](https://github.com/alamb))
- feat: add support for casting Duration/Interval to Int64Array [\#1196](https://github.com/apache/arrow-rs/pull/1196) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([e-dard](https://github.com/e-dard))
- Add comparison support for fully qualified BinaryArray [\#1195](https://github.com/apache/arrow-rs/pull/1195) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([HaoYang670](https://github.com/HaoYang670))
- Fix in display of `Float16Array` [\#1194](https://github.com/apache/arrow-rs/pull/1194) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([helgikrs](https://github.com/helgikrs))
- update nightly version for miri [\#1189](https://github.com/apache/arrow-rs/pull/1189) ([Jimexist](https://github.com/Jimexist))
- feat\(parquet\): support for reading structs nested within lists [\#1187](https://github.com/apache/arrow-rs/pull/1187) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([helgikrs](https://github.com/helgikrs))
- fix: Fix a bug in how definition levels are calculated for nested structs in a list [\#1185](https://github.com/apache/arrow-rs/pull/1185) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([helgikrs](https://github.com/helgikrs))
- Truncate bitmask on BooleanBufferBuilder::resize:  [\#1183](https://github.com/apache/arrow-rs/pull/1183) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add ticket reference for false positive in clippy [\#1181](https://github.com/apache/arrow-rs/pull/1181) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix record formatting in 1.58 [\#1178](https://github.com/apache/arrow-rs/pull/1178) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Serialize i128 as JSON string [\#1175](https://github.com/apache/arrow-rs/pull/1175) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Support DecimalType in `sort` and `take` kernels [\#1172](https://github.com/apache/arrow-rs/pull/1172) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([liukun4515](https://github.com/liukun4515))
- Fix new clippy lints introduced in Rust 1.58 [\#1170](https://github.com/apache/arrow-rs/pull/1170) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix compilation error with simd feature [\#1169](https://github.com/apache/arrow-rs/pull/1169) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Fix bug while writing parquet with empty lists of structs [\#1166](https://github.com/apache/arrow-rs/pull/1166) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([helgikrs](https://github.com/helgikrs))
- Use tempfile for parquet tests [\#1165](https://github.com/apache/arrow-rs/pull/1165) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Remove left over dev/README.md file from arrow/arrow-rs split [\#1162](https://github.com/apache/arrow-rs/pull/1162) ([alamb](https://github.com/alamb))
- Add multiply\_scalar kernel [\#1159](https://github.com/apache/arrow-rs/pull/1159) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Fuzz test different parquet encodings [\#1156](https://github.com/apache/arrow-rs/pull/1156) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- Add subtract\_scalar kernel [\#1152](https://github.com/apache/arrow-rs/pull/1152) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Add add\_scalar kernel [\#1151](https://github.com/apache/arrow-rs/pull/1151) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Move simd right out of for\_each loop [\#1150](https://github.com/apache/arrow-rs/pull/1150) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([viirya](https://github.com/viirya))
- Internal Remove `GenericStringArray::from_vec` and `GenericStringArray::from_opt_vec` [\#1147](https://github.com/apache/arrow-rs/pull/1147) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Implement SIMD comparison operations for types with less than 4 lanes \(i128\) [\#1146](https://github.com/apache/arrow-rs/pull/1146) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Extends parquet fuzz tests to also tests nulls, dictionaries and row groups with multiple pages  \(\#1053\) [\#1110](https://github.com/apache/arrow-rs/pull/1110) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
-  Generify ColumnReaderImpl and RecordReader \(\#1040\)  [\#1041](https://github.com/apache/arrow-rs/pull/1041) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([tustvold](https://github.com/tustvold))
- BooleanBufferBuilder::append\_packed \(\#1038\) [\#1039](https://github.com/apache/arrow-rs/pull/1039) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

## [7.0.0](https://github.com/apache/arrow-rs/tree/7.0.0) (2022-1-07)

[Full Changelog](https://github.com/apache/arrow-rs/compare/6.5.0...7.0.0)

### Arrow

**Breaking changes:**
- `pretty_format_batches` now returns `Result<impl Display>` rather than `String`: [#975](https://github.com/apache/arrow-rs/pull/975)
- `MutableBuffer::typed_data_mut` is marked `unsafe`: [#1029](https://github.com/apache/arrow-rs/pull/1029)
- UnionArray updated match latest Arrow spec, added `UnionMode`, `UnionArray::new()` marked `unsafe`: [#885](https://github.com/apache/arrow-rs/pull/885)

**New Features:**
- Support for `Float16Array` types [#888](https://github.com/apache/arrow-rs/pull/888)
- IPC support for `UnionArray` [#654](https://github.com/apache/arrow-rs/issues/654)
- Dynamic comparison kernels for scalars (e.g. `eq_dyn_scalar`), including `DictionaryArray`: [#1113](https://github.com/apache/arrow-rs/issues/1113)

**Enhancements:**
- Added `Schema::with_metadata` and `Field::with_metadata` [#1092](https://github.com/apache/arrow-rs/pull/1092)
- Support for custom datetime format for inference and parsing csv files [#1112](https://github.com/apache/arrow-rs/pull/1112)
- Implement `Array` for `ArrayRef` for easier use [#1129](https://github.com/apache/arrow-rs/pull/1129)
- Pretty printing display support for `FixedSizeBinaryArray` [#1097](https://github.com/apache/arrow-rs/pull/1097)
- Dependency Upgrades: `pyo3`, `parquet-format`, `prost`, `tonic`
- Avoid allocating vector of indices in `lexicographical_partition_ranges`[#998](https://github.com/apache/arrow-rs/pull/998)

### Parquet

**Fixed bugs:**
- (parquet) Fix reading of dictionary encoded pages with null values: [#1130](https://github.com/apache/arrow-rs/pull/1130)


# Changelog

## [6.5.0](https://github.com/apache/arrow-rs/tree/6.5.0) (2021-12-23)

[Full Changelog](https://github.com/apache/arrow-rs/compare/6.4.0...6.5.0)

* [092fc64bbb019244887ebd0d9c9a2d3e3a9aebc0](https://github.com/apache/arrow-rs/commit/092fc64bbb019244887ebd0d9c9a2d3e3a9aebc0) support cast decimal to decimal ([#1084](https://github.com/apache/arrow-rs/pull/1084)) ([#1093](https://github.com/apache/arrow-rs/pull/1093))
* [01459762ed18b504e00e7b2818fce91f19188b1e](https://github.com/apache/arrow-rs/commit/01459762ed18b504e00e7b2818fce91f19188b1e) Fix like regex escaping ([#1085](https://github.com/apache/arrow-rs/pull/1085)) ([#1090](https://github.com/apache/arrow-rs/pull/1090))
* [7c748bfccbc2eac0c1138378736b70dcb7e26a5b](https://github.com/apache/arrow-rs/commit/7c748bfccbc2eac0c1138378736b70dcb7e26a5b) support cast decimal to signed numeric ([#1073](https://github.com/apache/arrow-rs/pull/1073)) ([#1089](https://github.com/apache/arrow-rs/pull/1089))
* [bd3600b6483c253ae57a38928a636d39a6b7cb02](https://github.com/apache/arrow-rs/commit/bd3600b6483c253ae57a38928a636d39a6b7cb02) parquet: Use constant for RLE decoder buffer size ([#1070](https://github.com/apache/arrow-rs/pull/1070)) ([#1088](https://github.com/apache/arrow-rs/pull/1088))
* [2b5c53ecd92468fd95328637a15de7f35b6fcf28](https://github.com/apache/arrow-rs/commit/2b5c53ecd92468fd95328637a15de7f35b6fcf28) Box RleDecoder index buffer ([#1061](https://github.com/apache/arrow-rs/pull/1061)) ([#1062](https://github.com/apache/arrow-rs/pull/1062)) ([#1081](https://github.com/apache/arrow-rs/pull/1081))
* [78721bc1a467177679ad6196b994759cf4d73377](https://github.com/apache/arrow-rs/commit/78721bc1a467177679ad6196b994759cf4d73377) BooleanBufferBuilder correct buffer length ([#1051](https://github.com/apache/arrow-rs/pull/1051)) ([#1052](https://github.com/apache/arrow-rs/pull/1052)) ([#1080](https://github.com/apache/arrow-rs/pull/1080))
* [3a5e3541d3a4db61a828011ed95c8539adf1d57c](https://github.com/apache/arrow-rs/commit/3a5e3541d3a4db61a828011ed95c8539adf1d57c) support cast signed numeric to decimal ([#1044](https://github.com/apache/arrow-rs/pull/1044)) ([#1079](https://github.com/apache/arrow-rs/pull/1079))
* [000bdb3053098255d43288aa3e8665e8b1892a6c](https://github.com/apache/arrow-rs/commit/000bdb3053098255d43288aa3e8665e8b1892a6c) fix(compute): LIKE escape parenthesis ([#1042](https://github.com/apache/arrow-rs/pull/1042)) ([#1078](https://github.com/apache/arrow-rs/pull/1078))
* [e0abdb9e62772a2f853974e68e744246e7f47569](https://github.com/apache/arrow-rs/commit/e0abdb9e62772a2f853974e68e744246e7f47569) Add Schema::project and RecordBatch::project functions  ([#1033](https://github.com/apache/arrow-rs/pull/1033)) ([#1077](https://github.com/apache/arrow-rs/pull/1077))
* [31911a4d6328d889d98796b896412b3997f73e13](https://github.com/apache/arrow-rs/commit/31911a4d6328d889d98796b896412b3997f73e13) Remove outdated safety example from doc ([#1050](https://github.com/apache/arrow-rs/pull/1050)) ([#1058](https://github.com/apache/arrow-rs/pull/1058))
* [71ac8620993a65a7f1f57278c3495556625356b3](https://github.com/apache/arrow-rs/commit/71ac8620993a65a7f1f57278c3495556625356b3) Use existing array type in `take` kernel ([#1046](https://github.com/apache/arrow-rs/pull/1046)) ([#1057](https://github.com/apache/arrow-rs/pull/1057))
* [1c5902376b7f7d56cb5249db4f98a6a370ead919](https://github.com/apache/arrow-rs/commit/1c5902376b7f7d56cb5249db4f98a6a370ead919) Extract method to drive PageIterator -> RecordReader ([#1031](https://github.com/apache/arrow-rs/pull/1031)) ([#1056](https://github.com/apache/arrow-rs/pull/1056))
* [7ca39361f8733b86bc0cef5ed5d74093e2c6b14d](https://github.com/apache/arrow-rs/commit/7ca39361f8733b86bc0cef5ed5d74093e2c6b14d) Clarify governance of arrow crate ([#1030](https://github.com/apache/arrow-rs/pull/1030)) ([#1055](https://github.com/apache/arrow-rs/pull/1055))


## [6.4.0](https://github.com/apache/arrow-rs/tree/6.4.0) (2021-12-10)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.3.0...6.4.0)


* [049f48559f578243935b6e512d06c4c2df360bf1](https://github.com/apache/arrow-rs/commit/049f48559f578243935b6e512d06c4c2df360bf1) Force new cargo and target caching to fix CI ([#1023](https://github.com/apache/arrow-rs/pull/1023)) ([#1024](https://github.com/apache/arrow-rs/pull/1024))
* [ef37da3b60f71a52d5ad67e9ca810dca38b29f00](https://github.com/apache/arrow-rs/commit/ef37da3b60f71a52d5ad67e9ca810dca38b29f00) Fix a broken link and some missing styling in the main arrow crate docs ([#1013](https://github.com/apache/arrow-rs/pull/1013)) ([#1019](https://github.com/apache/arrow-rs/pull/1019))
* [f2c746a9b968714cfe05d35fcee8658371acd899](https://github.com/apache/arrow-rs/commit/f2c746a9b968714cfe05d35fcee8658371acd899) Remove out of date comment ([#1008](https://github.com/apache/arrow-rs/pull/1008)) ([#1018](https://github.com/apache/arrow-rs/pull/1018))
* [557fc11e3b2a09a680c0cfbf38d27b13101b63fe](https://github.com/apache/arrow-rs/commit/557fc11e3b2a09a680c0cfbf38d27b13101b63fe) Remove unneeded `rc` feature of serde ([#990](https://github.com/apache/arrow-rs/pull/990)) ([#1016](https://github.com/apache/arrow-rs/pull/1016))
* [b28385e096b1cf8f5fb2773d49b160f93d94fbac](https://github.com/apache/arrow-rs/commit/b28385e096b1cf8f5fb2773d49b160f93d94fbac) Docstrings for Timestamp*Array. ([#988](https://github.com/apache/arrow-rs/pull/988)) ([#1015](https://github.com/apache/arrow-rs/pull/1015))
* [a92672e40217670d2566a85d70b0b59fffac594c](https://github.com/apache/arrow-rs/commit/a92672e40217670d2566a85d70b0b59fffac594c) Add full data validation for ArrayData::try_new() ([#1007](https://github.com/apache/arrow-rs/pull/1007))
* [6c8b2936d7b07e1e2f5d1d48eea425a385382dfb](https://github.com/apache/arrow-rs/commit/6c8b2936d7b07e1e2f5d1d48eea425a385382dfb) Add boolean comparison to scalar kernels for less then, greater than ([#977](https://github.com/apache/arrow-rs/pull/977)) ([#1005](https://github.com/apache/arrow-rs/pull/1005))
* [14d140aeca608a23a8a6b2c251c8f53ffd377e61](https://github.com/apache/arrow-rs/commit/14d140aeca608a23a8a6b2c251c8f53ffd377e61) Fix some typos in code and comments ([#985](https://github.com/apache/arrow-rs/pull/985)) ([#1006](https://github.com/apache/arrow-rs/pull/1006))
* [b4507f562fb0eddfb79840871cd2733dc0e337cd](https://github.com/apache/arrow-rs/commit/b4507f562fb0eddfb79840871cd2733dc0e337cd) Fix warnings introduced by Rust/Clippy 1.57.0 ([#1004](https://github.com/apache/arrow-rs/pull/1004))


## [6.3.0](https://github.com/apache/arrow-rs/tree/6.3.0) (2021-11-26)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.2.0...6.3.0)


**Changes:**
* [7e51df015ce851a5de444ca08b57b38e7ee959a3](https://github.com/apache/arrow-rs/commit/7e51df015ce851a5de444ca08b57b38e7ee959a3) add more error test case and change the code style ([#952](https://github.com/apache/arrow-rs/pull/952)) ([#976](https://github.com/apache/arrow-rs/pull/976))
* [6c570cfe98d6a7a4ec74b139b733c5c72ed10015](https://github.com/apache/arrow-rs/commit/6c570cfe98d6a7a4ec74b139b733c5c72ed10015) Support read decimal data from csv reader if user provide the schema with decimal data type ([#941](https://github.com/apache/arrow-rs/pull/941)) ([#974](https://github.com/apache/arrow-rs/pull/974))
* [4fa0d4d7f7d9ca0a3da2a6dfe3eae6dc2d51a79a](https://github.com/apache/arrow-rs/commit/4fa0d4d7f7d9ca0a3da2a6dfe3eae6dc2d51a79a) Adding Pretty Print Support For Fixed Size List ([#958](https://github.com/apache/arrow-rs/pull/958)) ([#968](https://github.com/apache/arrow-rs/pull/968))
* [9d453a3128013c03e8ed854ded76b15cc6f28be4](https://github.com/apache/arrow-rs/commit/9d453a3128013c03e8ed854ded76b15cc6f28be4) Fix bug in temporal utilities due to DST being ignored. ([#955](https://github.com/apache/arrow-rs/pull/955)) ([#967](https://github.com/apache/arrow-rs/pull/967))
* [1b9fd9e3fb2653236513bb7dda5aa2fa14d1d831](https://github.com/apache/arrow-rs/commit/1b9fd9e3fb2653236513bb7dda5aa2fa14d1d831) Inferring 2. as Float64 for issue [#929](https://github.com/apache/arrow-rs/pull/929) ([#950](https://github.com/apache/arrow-rs/pull/950)) ([#966](https://github.com/apache/arrow-rs/pull/966))
* [e6c5e1c877bd94b3d6e545567f901d9962257cf8](https://github.com/apache/arrow-rs/commit/e6c5e1c877bd94b3d6e545567f901d9962257cf8) Fix CI for latest nightly ([#970](https://github.com/apache/arrow-rs/pull/970)) ([#973](https://github.com/apache/arrow-rs/pull/973))
* [c96e8de457442806e18944f0b26dd06ba4cb1aee](https://github.com/apache/arrow-rs/commit/c96e8de457442806e18944f0b26dd06ba4cb1aee) Fix primitive sort when input contains more nulls than the given sort limit ([#954](https://github.com/apache/arrow-rs/pull/954)) ([#965](https://github.com/apache/arrow-rs/pull/965))
* [094037d418381584178db1d886cad3b5024b414a](https://github.com/apache/arrow-rs/commit/094037d418381584178db1d886cad3b5024b414a) Update comfy-table to 5.0 ([#957](https://github.com/apache/arrow-rs/pull/957)) ([#964](https://github.com/apache/arrow-rs/pull/964))
* [9f635021eee6786c5377c891218c5f88ebce07c3](https://github.com/apache/arrow-rs/commit/9f635021eee6786c5377c891218c5f88ebce07c3) Fix csv writing of timestamps to show timezone. ([#849](https://github.com/apache/arrow-rs/pull/849)) ([#963](https://github.com/apache/arrow-rs/pull/963))
* [f7deba4c3a050a52608462ee8a827bb8f6364140](https://github.com/apache/arrow-rs/commit/f7deba4c3a050a52608462ee8a827bb8f6364140) Adding ability to parse float from number with leading decimal ([#831](https://github.com/apache/arrow-rs/pull/831)) ([#962](https://github.com/apache/arrow-rs/pull/962))
* [59f96e842d05b63882f7ba285c66a9739761cf84](https://github.com/apache/arrow-rs/commit/59f96e842d05b63882f7ba285c66a9739761cf84) add ilike comparator ([#874](https://github.com/apache/arrow-rs/pull/874)) ([#961](https://github.com/apache/arrow-rs/pull/961))
* [54023c8a5543c9f9fa4955afa01189029f3e96f5](https://github.com/apache/arrow-rs/commit/54023c8a5543c9f9fa4955afa01189029f3e96f5) Remove unpassable cargo publish check from verify-release-candidate.sh ([#882](https://github.com/apache/arrow-rs/pull/882)) ([#949](https://github.com/apache/arrow-rs/pull/949))



## [6.2.0](https://github.com/apache/arrow-rs/tree/6.2.0) (2021-11-12)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.1.0...6.2.0)

**Features / Fixes:**


* [4037933e43cad9e4de027039ce14caa65f78300a](https://github.com/apache/arrow-rs/commit/4037933e43cad9e4de027039ce14caa65f78300a) Fix validation for offsets of StructArrays ([#942](https://github.com/apache/arrow-rs/pull/942)) ([#946](https://github.com/apache/arrow-rs/pull/946))
* [1af9ca5d363d870550026a7b1abcb749befbb371](https://github.com/apache/arrow-rs/commit/1af9ca5d363d870550026a7b1abcb749befbb371) implement take kernel for null arrays ([#939](https://github.com/apache/arrow-rs/pull/939)) ([#944](https://github.com/apache/arrow-rs/pull/944))
* [320de1c20aefbf204f6888e2ad3663863afeba9f](https://github.com/apache/arrow-rs/commit/320de1c20aefbf204f6888e2ad3663863afeba9f) add checker for appending i128 to decimal builder ([#928](https://github.com/apache/arrow-rs/pull/928)) ([#943](https://github.com/apache/arrow-rs/pull/943))
* [dff14113884ad4246a8cafb9be579ebdb4e1481f](https://github.com/apache/arrow-rs/commit/dff14113884ad4246a8cafb9be579ebdb4e1481f) Validate arguments to ArrayData::new and null bit buffer and buffers ([#810](https://github.com/apache/arrow-rs/pull/810)) ([#936](https://github.com/apache/arrow-rs/pull/936))
* [c3eae1ec56303b97c9e15263063a6a13122ef194](https://github.com/apache/arrow-rs/commit/c3eae1ec56303b97c9e15263063a6a13122ef194) fix some warning about unused variables in panic tests ([#894](https://github.com/apache/arrow-rs/pull/894)) ([#933](https://github.com/apache/arrow-rs/pull/933))
* [e80bb018450f13a30811ffd244c42917d8bf8a62](https://github.com/apache/arrow-rs/commit/e80bb018450f13a30811ffd244c42917d8bf8a62) fix some clippy warnings ([#896](https://github.com/apache/arrow-rs/pull/896)) ([#930](https://github.com/apache/arrow-rs/pull/930))
* [bde89463b627be3f60b5569d038ca36c434da71d](https://github.com/apache/arrow-rs/commit/bde89463b627be3f60b5569d038ca36c434da71d) feat(ipc): add support for deserializing messages with nested dictionary fields ([#923](https://github.com/apache/arrow-rs/pull/923)) ([#931](https://github.com/apache/arrow-rs/pull/931))
* [792544b5fb7b84224ef9745ecb9f330663c14fb4](https://github.com/apache/arrow-rs/commit/792544b5fb7b84224ef9745ecb9f330663c14fb4) refactor regexp_is_match_utf8_scalar to try to mitigate miri failures ([#895](https://github.com/apache/arrow-rs/pull/895)) ([#932](https://github.com/apache/arrow-rs/pull/932))
* [3f0e252811cbb6e3f7c774959787dcfec985d03e](https://github.com/apache/arrow-rs/commit/3f0e252811cbb6e3f7c774959787dcfec985d03e) Automatically retry failed MIRI runs to work around intermittent failures  ([#934](https://github.com/apache/arrow-rs/pull/934))
* [c9a9515c46d560ced00e23ff57cb10a1c97573cb](https://github.com/apache/arrow-rs/commit/c9a9515c46d560ced00e23ff57cb10a1c97573cb) Update mod.rs ([#909](https://github.com/apache/arrow-rs/pull/909)) ([#919](https://github.com/apache/arrow-rs/pull/919))
* [64ed79ece67141b92dc45b8a1d43cb9d909aa6a9](https://github.com/apache/arrow-rs/commit/64ed79ece67141b92dc45b8a1d43cb9d909aa6a9) Mark boolean kernels public ([#913](https://github.com/apache/arrow-rs/pull/913)) ([#920](https://github.com/apache/arrow-rs/pull/920))
* [8b95fe0bbf03588c5cc00f67365c5b0dac4d7a34](https://github.com/apache/arrow-rs/commit/8b95fe0bbf03588c5cc00f67365c5b0dac4d7a34) doc example  mistype ([#904](https://github.com/apache/arrow-rs/pull/904)) ([#918](https://github.com/apache/arrow-rs/pull/918))
* [34c5eab4862cab16fdfd5f5ed6c68dce6298dfa4](https://github.com/apache/arrow-rs/commit/34c5eab4862cab16fdfd5f5ed6c68dce6298dfa4) allow null array to be cast to all other types ([#884](https://github.com/apache/arrow-rs/pull/884)) ([#917](https://github.com/apache/arrow-rs/pull/917))
* [3c69752e55ed0c58f5a8faed918a22b45cd93766](https://github.com/apache/arrow-rs/commit/3c69752e55ed0c58f5a8faed918a22b45cd93766) Fix instances of UB that cause tests to not pass under miri ([#878](https://github.com/apache/arrow-rs/pull/878)) ([#916](https://github.com/apache/arrow-rs/pull/916))
* [85402148c3af03d0855e81f855715ea98a7491c5](https://github.com/apache/arrow-rs/commit/85402148c3af03d0855e81f855715ea98a7491c5) feat(ipc): Support writing dictionaries nested in structs and unions ([#870](https://github.com/apache/arrow-rs/pull/870)) ([#915](https://github.com/apache/arrow-rs/pull/915))
* [03d95e626cb0e654775fefa77786674ea41be4a2](https://github.com/apache/arrow-rs/commit/03d95e626cb0e654775fefa77786674ea41be4a2) Fix references to changelog ([#905](https://github.com/apache/arrow-rs/pull/905))


## [6.1.0](https://github.com/apache/arrow-rs/tree/6.1.0) (2021-10-29)


[Full Changelog](https://github.com/apache/arrow-rs/compare/6.0.0...6.1.0)

**Features / Fixes:**

* [b42649b0088fe7762c713a41a23c1abdf8d0496d](https://github.com/apache/arrow-rs/commit/b42649b0088fe7762c713a41a23c1abdf8d0496d) implement eq_dyn and neq_dyn ([#858](https://github.com/apache/arrow-rs/pull/858)) ([#867](https://github.com/apache/arrow-rs/pull/867))
* [01743f3f10a377c1ca857cd554acbf84155766d8](https://github.com/apache/arrow-rs/commit/01743f3f10a377c1ca857cd554acbf84155766d8) fix: fix a bug in offset calculation for unions ([#863](https://github.com/apache/arrow-rs/pull/863)) ([#871](https://github.com/apache/arrow-rs/pull/871))
* [8bfff793a23f0e71008c7a9eea7a54d6b913ecff](https://github.com/apache/arrow-rs/commit/8bfff793a23f0e71008c7a9eea7a54d6b913ecff) add lt_bool, lt_eq_bool, gt_bool, gt_eq_bool ([#860](https://github.com/apache/arrow-rs/pull/860)) ([#868](https://github.com/apache/arrow-rs/pull/868))
* [8845e91d4ab584c822e9ee903db7069551b124af](https://github.com/apache/arrow-rs/commit/8845e91d4ab584c822e9ee903db7069551b124af) fix(ipc): Support serializing structs containing dictionaries ([#848](https://github.com/apache/arrow-rs/pull/848)) ([#865](https://github.com/apache/arrow-rs/pull/865))
* [620282a0d9fdd2a8ed7e8313d17ba3dec64c80e5](https://github.com/apache/arrow-rs/commit/620282a0d9fdd2a8ed7e8313d17ba3dec64c80e5) Implement boolean equality kernels ([#844](https://github.com/apache/arrow-rs/pull/844)) ([#857](https://github.com/apache/arrow-rs/pull/857))
* [94cddcacf785be982e69689291ce034ef00220b4](https://github.com/apache/arrow-rs/commit/94cddcacf785be982e69689291ce034ef00220b4) Cherry pick fix parquet_derive with default features (and fix cargo publish) ([#856](https://github.com/apache/arrow-rs/pull/856))
* [733fd583ddb3dbe6b4d58a809c444ee16ac0eae8](https://github.com/apache/arrow-rs/commit/733fd583ddb3dbe6b4d58a809c444ee16ac0eae8) Use kernel utility for parsing timestamps in csv reader. ([#832](https://github.com/apache/arrow-rs/pull/832)) ([#853](https://github.com/apache/arrow-rs/pull/853))
* [2cc64937a153f632796915d2d9869d5c2a501d28](https://github.com/apache/arrow-rs/commit/2cc64937a153f632796915d2d9869d5c2a501d28) [Minor] Fix clippy errors with new rust version (1.56) and float formatting with nightly ([#845](https://github.com/apache/arrow-rs/pull/845)) ([#850](https://github.com/apache/arrow-rs/pull/850))

**Other:**
* [bfac9e5a027e3bd78b7a1ec90c75a3e385bd66bb](https://github.com/apache/arrow-rs/commit/bfac9e5a027e3bd78b7a1ec90c75a3e385bd66bb) Test out new tarpaulin version ([#852](https://github.com/apache/arrow-rs/pull/852)) ([#866](https://github.com/apache/arrow-rs/pull/866))
* [809350ced392cfc78d8a1a46228d4ffc25dea9ff](https://github.com/apache/arrow-rs/commit/809350ced392cfc78d8a1a46228d4ffc25dea9ff) Update README.md ([#834](https://github.com/apache/arrow-rs/pull/834)) ([#854](https://github.com/apache/arrow-rs/pull/854))
* [70582f40dd21f5c710c4946266d0563a92b92337](https://github.com/apache/arrow-rs/commit/70582f40dd21f5c710c4946266d0563a92b92337) [MINOR] Delete temp file from docs ([#836](https://github.com/apache/arrow-rs/pull/836)) ([#855](https://github.com/apache/arrow-rs/pull/855))
* [a721e00014015a7e598946b6efb9b1da8080ec85](https://github.com/apache/arrow-rs/commit/a721e00014015a7e598946b6efb9b1da8080ec85) Force fresh cargo cache key in CI ([#839](https://github.com/apache/arrow-rs/pull/839)) ([#851](https://github.com/apache/arrow-rs/pull/851))


## [6.0.0](https://github.com/apache/arrow-rs/tree/6.0.0) (2021-10-13)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.5.0...6.0.0)

**Breaking changes:**

- Replace `ArrayData::new()` with `ArrayData::try_new()` and `unsafe ArrayData::new_unchecked` [\#822](https://github.com/apache/arrow-rs/pull/822) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update Bitmap::len to return bits rather than bytes [\#749](https://github.com/apache/arrow-rs/pull/749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- use sort\_unstable\_by in primitive sorting [\#552](https://github.com/apache/arrow-rs/pull/552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- New MapArray support [\#491](https://github.com/apache/arrow-rs/pull/491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nevi-me](https://github.com/nevi-me))

**Implemented enhancements:**

- Improve parquet binary writer speed by reducing allocations [\#819](https://github.com/apache/arrow-rs/issues/819)
- Expose buffer operations [\#808](https://github.com/apache/arrow-rs/issues/808)
- Add doc examples of writing parquet files using `ArrowWriter` [\#788](https://github.com/apache/arrow-rs/issues/788)

**Fixed bugs:**

- JSON reader can create null struct children on empty lists [\#825](https://github.com/apache/arrow-rs/issues/825)
- Incorrect null count for cast kernel for list arrays [\#815](https://github.com/apache/arrow-rs/issues/815)
- `minute` and `second` temporal kernels do not respect timezone [\#500](https://github.com/apache/arrow-rs/issues/500)
- Fix data corruption in json decoder f64-to-i64 cast [\#652](https://github.com/apache/arrow-rs/pull/652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xianwill](https://github.com/xianwill))

**Documentation updates:**

- Doctest for PrimitiveArray using from\_iter\_values. [\#694](https://github.com/apache/arrow-rs/pull/694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Doctests for BinaryArray and LargeBinaryArray. [\#625](https://github.com/apache/arrow-rs/pull/625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Add links in docstrings [\#605](https://github.com/apache/arrow-rs/pull/605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))


## [5.5.0](https://github.com/apache/arrow-rs/tree/5.5.0) (2021-09-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.4.0...5.5.0)

**Implemented enhancements:**

- parquet should depend on a small set of arrow features [\#800](https://github.com/apache/arrow-rs/issues/800)
- Support equality on RecordBatch [\#735](https://github.com/apache/arrow-rs/issues/735)

**Fixed bugs:**

- Converting from string to timestamp uses microseconds instead of milliseconds [\#780](https://github.com/apache/arrow-rs/issues/780)
- Document has no link to `RowColumnIter` [\#762](https://github.com/apache/arrow-rs/issues/762)
- length on slices with null doesn't work [\#744](https://github.com/apache/arrow-rs/issues/744)

## [5.4.0](https://github.com/apache/arrow-rs/tree/5.4.0) (2021-09-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.3.0...5.4.0)

**Implemented enhancements:**

- Upgrade lexical-core to 0.8 [\#747](https://github.com/apache/arrow-rs/issues/747)
- `append_nulls` and `append_trusted_len_iter` for PrimitiveBuilder [\#725](https://github.com/apache/arrow-rs/issues/725)
- Optimize MutableArrayData::extend for null buffers [\#397](https://github.com/apache/arrow-rs/issues/397)

**Fixed bugs:**

- Arithmetic with scalars doesn't work on slices [\#742](https://github.com/apache/arrow-rs/issues/742)
- Comparisons with scalar don't work on slices [\#740](https://github.com/apache/arrow-rs/issues/740)
- `unary` kernel doesn't respect offset [\#738](https://github.com/apache/arrow-rs/issues/738)
- `new_null_array` creates invalid struct arrays [\#734](https://github.com/apache/arrow-rs/issues/734)
- --no-default-features is broken for parquet [\#733](https://github.com/apache/arrow-rs/issues/733) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Bitmap::len` returns the number of bytes, not bits. [\#730](https://github.com/apache/arrow-rs/issues/730)
- Decimal logical type is formatted incorrectly by print\_schema [\#713](https://github.com/apache/arrow-rs/issues/713)
- parquet\_derive does not support chrono time values [\#711](https://github.com/apache/arrow-rs/issues/711)
- Numeric overflow when formatting Decimal type [\#710](https://github.com/apache/arrow-rs/issues/710)
- The integration tests are not running [\#690](https://github.com/apache/arrow-rs/issues/690)

**Closed issues:**

- Question: Is there no way to create a DictionaryArray with a pre-arranged mapping? [\#729](https://github.com/apache/arrow-rs/issues/729)

## [5.3.0](https://github.com/apache/arrow-rs/tree/5.3.0) (2021-08-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.2.0...5.3.0)

**Implemented enhancements:**

- Add optimized filter kernel for regular expression matching [\#697](https://github.com/apache/arrow-rs/issues/697)
- Can't cast from timestamp array to string array [\#587](https://github.com/apache/arrow-rs/issues/587)

**Fixed bugs:**

- 'Encoding DELTA\_BYTE\_ARRAY is not supported' with parquet arrow readers [\#708](https://github.com/apache/arrow-rs/issues/708)
- Support reading json string into binary data type. [\#701](https://github.com/apache/arrow-rs/issues/701)

**Closed issues:**

- Resolve Issues with `prettytable-rs` dependency [\#69](https://github.com/apache/arrow-rs/issues/69) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [5.2.0](https://github.com/apache/arrow-rs/tree/5.2.0) (2021-08-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.1.0...5.2.0)

**Implemented enhancements:**

- Make rand an optional dependency [\#671](https://github.com/apache/arrow-rs/issues/671)
- Remove undefined behavior in `value` method of boolean and primitive arrays [\#645](https://github.com/apache/arrow-rs/issues/645)
- Avoid materialization of indices in filter\_record\_batch for single arrays [\#636](https://github.com/apache/arrow-rs/issues/636)
- Add a note about arrow crate security / safety [\#627](https://github.com/apache/arrow-rs/issues/627)
- Allow the creation of String arrays from an iterator of &Option\<&str\> [\#598](https://github.com/apache/arrow-rs/issues/598)
- Support arrow map datatype [\#395](https://github.com/apache/arrow-rs/issues/395)

**Fixed bugs:**

- Parquet fixed length byte array columns write byte array statistics [\#660](https://github.com/apache/arrow-rs/issues/660) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet boolean columns write Int32 statistics [\#659](https://github.com/apache/arrow-rs/issues/659) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Writing Parquet with a boolean column fails [\#657](https://github.com/apache/arrow-rs/issues/657)
- JSON decoder data corruption for large i64/u64 [\#653](https://github.com/apache/arrow-rs/issues/653)
- Incorrect min/max statistics for strings in parquet files [\#641](https://github.com/apache/arrow-rs/issues/641) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Closed issues:**

- Release candidate verifying script seems work on macOS [\#640](https://github.com/apache/arrow-rs/issues/640)
- Update CONTRIBUTING  [\#342](https://github.com/apache/arrow-rs/issues/342)

## [5.1.0](https://github.com/apache/arrow-rs/tree/5.1.0) (2021-07-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.0.0...5.1.0)

**Implemented enhancements:**

- Make FFI\_ArrowArray empty\(\) public [\#602](https://github.com/apache/arrow-rs/issues/602)
- exponential sort can be used to speed up lexico partition kernel [\#586](https://github.com/apache/arrow-rs/issues/586)
- Implement sort\(\) for binary array [\#568](https://github.com/apache/arrow-rs/issues/568)
- primitive sorting can be improved and more consistent with and without `limit` if sorted unstably [\#553](https://github.com/apache/arrow-rs/issues/553)

**Fixed bugs:**

- Confusing memory usage with CSV reader [\#623](https://github.com/apache/arrow-rs/issues/623)
- FFI implementation deviates from specification for array release  [\#595](https://github.com/apache/arrow-rs/issues/595)
- Parquet file content is different if `~/.cargo` is in a git checkout [\#589](https://github.com/apache/arrow-rs/issues/589)
- Ensure output of MIRI is checked for success [\#581](https://github.com/apache/arrow-rs/issues/581)
- MIRI failure in `array::ffi::tests::test_struct` and other ffi tests [\#580](https://github.com/apache/arrow-rs/issues/580)
- ListArray equality check may return wrong result [\#570](https://github.com/apache/arrow-rs/issues/570)
- cargo audit failed [\#561](https://github.com/apache/arrow-rs/issues/561)
- ArrayData::slice\(\) does not work for nested types such as StructArray [\#554](https://github.com/apache/arrow-rs/issues/554)

**Documentation updates:**

- More examples of how to construct Arrays [\#301](https://github.com/apache/arrow-rs/issues/301)

**Closed issues:**

- Implement StringBuilder::append\_option [\#263](https://github.com/apache/arrow-rs/issues/263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [5.0.0](https://github.com/apache/arrow-rs/tree/5.0.0) (2021-07-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.4.0...5.0.0)

**Breaking changes:**

- Remove lifetime from DynComparator [\#543](https://github.com/apache/arrow-rs/issues/543) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Simplify interactions with arrow flight APIs [\#376](https://github.com/apache/arrow-rs/issues/376) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)]
- refactor: remove lifetime from DynComparator [\#542](https://github.com/apache/arrow-rs/pull/542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([e-dard](https://github.com/e-dard))
- use iterator for partition kernel instead of generating vec [\#438](https://github.com/apache/arrow-rs/pull/438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Remove DictionaryArray::keys\_array method [\#419](https://github.com/apache/arrow-rs/pull/419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- simplify interactions with arrow flight APIs [\#377](https://github.com/apache/arrow-rs/pull/377) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([garyanaplan](https://github.com/garyanaplan))
- return reference from DictionaryArray::values\(\) \(\#313\) [\#314](https://github.com/apache/arrow-rs/pull/314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Allow creation of StringArrays from Vec\<String\> [\#519](https://github.com/apache/arrow-rs/issues/519) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatch::concat [\#461](https://github.com/apache/arrow-rs/issues/461) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement RecordBatch::slice\(\) to slice RecordBatches  [\#460](https://github.com/apache/arrow-rs/issues/460) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add a RecordBatch::split to split large batches into a set of smaller batches [\#343](https://github.com/apache/arrow-rs/issues/343)
- generate parquet schema from rust struct [\#539](https://github.com/apache/arrow-rs/pull/539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Implement `RecordBatch::concat` [\#537](https://github.com/apache/arrow-rs/pull/537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Implement function slice for RecordBatch [\#490](https://github.com/apache/arrow-rs/pull/490) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([b41sh](https://github.com/b41sh))
- add lexicographically partition points and ranges [\#424](https://github.com/apache/arrow-rs/pull/424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- allow to read non-standard CSV [\#326](https://github.com/apache/arrow-rs/pull/326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- parquet: Speed up `BitReader`/`DeltaBitPackDecoder` [\#325](https://github.com/apache/arrow-rs/pull/325) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kornholi](https://github.com/kornholi))
- ARROW-12343: \[Rust\] Support auto-vectorization for min/max [\#9](https://github.com/apache/arrow-rs/pull/9) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- ARROW-12411: \[Rust\] Create RecordBatches from Iterators [\#7](https://github.com/apache/arrow-rs/pull/7) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Fixed bugs:**

- Error building on master - error: cyclic package dependency: package `ahash v0.7.4` depends on itself. Cycle [\#544](https://github.com/apache/arrow-rs/issues/544)
- IPC reader panics with out of bounds error [\#541](https://github.com/apache/arrow-rs/issues/541)
- Take kernel doesn't handle nulls and structs correctly [\#530](https://github.com/apache/arrow-rs/issues/530) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- master fails to compile with `default-features=false` [\#529](https://github.com/apache/arrow-rs/issues/529)
- README developer instructions out of date [\#523](https://github.com/apache/arrow-rs/issues/523)
- Update rustc and packed\_simd in CI before 5.0 release [\#517](https://github.com/apache/arrow-rs/issues/517)
- Incorrect memory usage calculation for dictionary arrays [\#503](https://github.com/apache/arrow-rs/issues/503) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- sliced null buffers lead to incorrect result in take kernel \(and probably on other places\) [\#502](https://github.com/apache/arrow-rs/issues/502)
- Cast of utf8 types and list container types don't respect offset [\#334](https://github.com/apache/arrow-rs/issues/334) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- fix take kernel null handling on structs [\#531](https://github.com/apache/arrow-rs/pull/531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Correct array memory usage calculation for dictionary arrays [\#505](https://github.com/apache/arrow-rs/pull/505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- parquet: improve BOOLEAN writing logic and report error on encoding fail [\#443](https://github.com/apache/arrow-rs/pull/443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([garyanaplan](https://github.com/garyanaplan))
- Fix bug with null buffer offset in boolean not kernel [\#418](https://github.com/apache/arrow-rs/pull/418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- respect offset in utf8 and list casts [\#335](https://github.com/apache/arrow-rs/pull/335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Fix comparison of dictionaries with different values arrays \(\#332\) [\#333](https://github.com/apache/arrow-rs/pull/333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ensure null-counts are written for all-null columns [\#307](https://github.com/apache/arrow-rs/pull/307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- fix invalid null handling in filter [\#296](https://github.com/apache/arrow-rs/pull/296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- fix NaN handling in parquet statistics [\#256](https://github.com/apache/arrow-rs/pull/256) ([crepererum](https://github.com/crepererum))

**Documentation updates:**

- Improve arrow's crate's readme on crates.io [\#463](https://github.com/apache/arrow-rs/issues/463)
- Clean up README.md in advance of the 5.0 release [\#536](https://github.com/apache/arrow-rs/pull/536) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- fix readme instructions to reflect new structure [\#524](https://github.com/apache/arrow-rs/pull/524) ([marcvanheerden](https://github.com/marcvanheerden))
- Improve docs for NullArray, new\_null\_array and new\_empty\_array [\#240](https://github.com/apache/arrow-rs/pull/240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- Fix default arrow build [\#533](https://github.com/apache/arrow-rs/pull/533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add tests for building applications using arrow with different feature flags [\#532](https://github.com/apache/arrow-rs/pull/532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove unused futures dependency from arrow-flight [\#528](https://github.com/apache/arrow-rs/pull/528) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- CI: update rust nightly and packed\_simd [\#525](https://github.com/apache/arrow-rs/pull/525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Support `StringArray` creation from String Vec [\#522](https://github.com/apache/arrow-rs/pull/522) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Fix parquet benchmark schema [\#513](https://github.com/apache/arrow-rs/pull/513) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix parquet definition levels [\#511](https://github.com/apache/arrow-rs/pull/511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix for primitive and boolean take kernel for nullable indices with an offset [\#509](https://github.com/apache/arrow-rs/pull/509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Bump flatbuffers [\#499](https://github.com/apache/arrow-rs/pull/499) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([PsiACE](https://github.com/PsiACE))
- implement second/minute helpers for temporal [\#493](https://github.com/apache/arrow-rs/pull/493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ovr](https://github.com/ovr))
- special case concatenating single element array shortcut [\#492](https://github.com/apache/arrow-rs/pull/492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- update docs to reflect recent changes \(joins and window functions\) [\#489](https://github.com/apache/arrow-rs/pull/489) ([Jimexist](https://github.com/Jimexist))
- Update rand, proc-macro and zstd dependencies [\#488](https://github.com/apache/arrow-rs/pull/488) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Doctest for GenericListArray. [\#474](https://github.com/apache/arrow-rs/pull/474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- remove stale comment on `ArrayData` equality and update unit tests [\#472](https://github.com/apache/arrow-rs/pull/472) ([Jimexist](https://github.com/Jimexist))
- remove unused patch file [\#471](https://github.com/apache/arrow-rs/pull/471) ([Jimexist](https://github.com/Jimexist))
- fix clippy warnings for rust 1.53 [\#470](https://github.com/apache/arrow-rs/pull/470) ([Jimexist](https://github.com/Jimexist))
- Fix PR labeler [\#468](https://github.com/apache/arrow-rs/pull/468) ([Dandandan](https://github.com/Dandandan))
- Tweak dev backporting docs [\#466](https://github.com/apache/arrow-rs/pull/466) ([alamb](https://github.com/alamb))
- Unvendor Archery [\#459](https://github.com/apache/arrow-rs/pull/459) ([kszucs](https://github.com/kszucs))
- Add sort boolean benchmark [\#457](https://github.com/apache/arrow-rs/pull/457) ([alamb](https://github.com/alamb))
- Add C data interface for decimal128 and timestamp [\#453](https://github.com/apache/arrow-rs/pull/453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alippai](https://github.com/alippai))
- Implement the Iterator trait for the json Reader. [\#451](https://github.com/apache/arrow-rs/pull/451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([LaurentMazare](https://github.com/LaurentMazare))
- Update release docs + release email template [\#450](https://github.com/apache/arrow-rs/pull/450) ([alamb](https://github.com/alamb))
- remove clippy unnecessary wraps suppression in cast kernel [\#449](https://github.com/apache/arrow-rs/pull/449) ([Jimexist](https://github.com/Jimexist))
- Use partition for bool sort [\#448](https://github.com/apache/arrow-rs/pull/448) ([Jimexist](https://github.com/Jimexist))
- remove unnecessary wraps in sort [\#445](https://github.com/apache/arrow-rs/pull/445) ([Jimexist](https://github.com/Jimexist))
- Python FFI bridge for Schema, Field and DataType  [\#439](https://github.com/apache/arrow-rs/pull/439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszucs](https://github.com/kszucs))
- Update release Readme.md [\#436](https://github.com/apache/arrow-rs/pull/436) ([alamb](https://github.com/alamb))
- Derive Eq and PartialEq for SortOptions [\#425](https://github.com/apache/arrow-rs/pull/425) ([tustvold](https://github.com/tustvold))
- refactor lexico sort for future code reuse [\#423](https://github.com/apache/arrow-rs/pull/423) ([Jimexist](https://github.com/Jimexist))
- Reenable MIRI check on PRs [\#421](https://github.com/apache/arrow-rs/pull/421) ([alamb](https://github.com/alamb))
- Sort by float lists [\#420](https://github.com/apache/arrow-rs/pull/420) ([medwards](https://github.com/medwards))
- Fix out of bounds read in bit chunk iterator [\#416](https://github.com/apache/arrow-rs/pull/416) ([jhorstmann](https://github.com/jhorstmann))
- Doctests for DecimalArray. [\#414](https://github.com/apache/arrow-rs/pull/414) ([novemberkilo](https://github.com/novemberkilo))
- Add Decimal to CsvWriter and improve debug display [\#406](https://github.com/apache/arrow-rs/pull/406) ([alippai](https://github.com/alippai))
- MINOR: update install instruction [\#400](https://github.com/apache/arrow-rs/pull/400) ([alippai](https://github.com/alippai))
- use prettier to auto format md files [\#398](https://github.com/apache/arrow-rs/pull/398) ([Jimexist](https://github.com/Jimexist))
- window::shift to work for all array types [\#388](https://github.com/apache/arrow-rs/pull/388) ([Jimexist](https://github.com/Jimexist))
- add more tests for window::shift and handle boundary cases [\#386](https://github.com/apache/arrow-rs/pull/386) ([Jimexist](https://github.com/Jimexist))
- Implement faster arrow array reader [\#384](https://github.com/apache/arrow-rs/pull/384) ([yordan-pavlov](https://github.com/yordan-pavlov))
- Add set\_bit to BooleanBufferBuilder to allow mutating bit in index [\#383](https://github.com/apache/arrow-rs/pull/383) ([boazberman](https://github.com/boazberman))
- make sure that only concat preallocates buffers [\#382](https://github.com/apache/arrow-rs/pull/382) ([ritchie46](https://github.com/ritchie46))
- Respect max rowgroup size in Arrow writer [\#381](https://github.com/apache/arrow-rs/pull/381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix typo in release script, update release location [\#380](https://github.com/apache/arrow-rs/pull/380) ([alamb](https://github.com/alamb))
- Doctests for FixedSizeBinaryArray [\#378](https://github.com/apache/arrow-rs/pull/378) ([novemberkilo](https://github.com/novemberkilo))
- Simplify shift kernel using new\_null\_array [\#370](https://github.com/apache/arrow-rs/pull/370) ([Dandandan](https://github.com/Dandandan))
- allow `SliceableCursor` to be constructed from an `Arc` directly [\#369](https://github.com/apache/arrow-rs/pull/369) ([crepererum](https://github.com/crepererum))
- Add doctest for ArrayBuilder [\#367](https://github.com/apache/arrow-rs/pull/367) ([alippai](https://github.com/alippai))
- Fix version in readme [\#365](https://github.com/apache/arrow-rs/pull/365) ([domoritz](https://github.com/domoritz))
- Remove superfluous space [\#363](https://github.com/apache/arrow-rs/pull/363) ([domoritz](https://github.com/domoritz))
- Add crate badges [\#362](https://github.com/apache/arrow-rs/pull/362) ([domoritz](https://github.com/domoritz))
- Disable MIRI check until it runs cleanly on CI [\#360](https://github.com/apache/arrow-rs/pull/360) ([alamb](https://github.com/alamb))
- Only register Flight.proto with cargo if it exists [\#351](https://github.com/apache/arrow-rs/pull/351) ([tustvold](https://github.com/tustvold))
- Reduce memory usage of concat \(large\)utf8 [\#348](https://github.com/apache/arrow-rs/pull/348) ([ritchie46](https://github.com/ritchie46))
- Fix filter UB and add fast path [\#341](https://github.com/apache/arrow-rs/pull/341) ([ritchie46](https://github.com/ritchie46))
- Automatic cherry-pick script [\#339](https://github.com/apache/arrow-rs/pull/339) ([alamb](https://github.com/alamb))
- Doctests for BooleanArray. [\#338](https://github.com/apache/arrow-rs/pull/338) ([novemberkilo](https://github.com/novemberkilo))
- feature gate ipc reader/writer [\#336](https://github.com/apache/arrow-rs/pull/336) ([ritchie46](https://github.com/ritchie46))
- Add ported Rust release verification script [\#331](https://github.com/apache/arrow-rs/pull/331) ([wesm](https://github.com/wesm))
- Doctests for StringArray and LargeStringArray. [\#330](https://github.com/apache/arrow-rs/pull/330) ([novemberkilo](https://github.com/novemberkilo))
- inline PrimitiveArray::value [\#329](https://github.com/apache/arrow-rs/pull/329) ([ritchie46](https://github.com/ritchie46))
- Enable wasm32 as a target architecture for the SIMD feature  [\#324](https://github.com/apache/arrow-rs/pull/324) ([roee88](https://github.com/roee88))
- Fix undefined behavior in FFI and enable MIRI checks on CI [\#323](https://github.com/apache/arrow-rs/pull/323) ([roee88](https://github.com/roee88))
- Mutablebuffer::shrink\_to\_fit [\#318](https://github.com/apache/arrow-rs/pull/318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Add \(simd\) modulus op [\#317](https://github.com/apache/arrow-rs/pull/317) ([gangliao](https://github.com/gangliao))
- feature gate csv functionality [\#312](https://github.com/apache/arrow-rs/pull/312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- \[Minor\] Version upgrades [\#304](https://github.com/apache/arrow-rs/pull/304) ([Dandandan](https://github.com/Dandandan))
- Remove old release scripts [\#293](https://github.com/apache/arrow-rs/pull/293) ([alamb](https://github.com/alamb))
- Add Send to the ArrayBuilder trait [\#291](https://github.com/apache/arrow-rs/pull/291) ([Max-Meldrum](https://github.com/Max-Meldrum))
- Added changelog generator script and configuration. [\#289](https://github.com/apache/arrow-rs/pull/289) ([jorgecarleitao](https://github.com/jorgecarleitao))
- manually bump development version [\#288](https://github.com/apache/arrow-rs/pull/288) ([nevi-me](https://github.com/nevi-me))
- Fix FFI and add support for Struct type [\#287](https://github.com/apache/arrow-rs/pull/287) ([roee88](https://github.com/roee88))
- Fix subtraction underflow when sorting string arrays with many nulls [\#285](https://github.com/apache/arrow-rs/pull/285) ([medwards](https://github.com/medwards))
- Speed up bound checking in `take` [\#281](https://github.com/apache/arrow-rs/pull/281) ([Dandandan](https://github.com/Dandandan))
- Update PR template by commenting out instructions [\#278](https://github.com/apache/arrow-rs/pull/278) ([nevi-me](https://github.com/nevi-me))
- Added Decimal support to pretty-print display utility \(\#230\) [\#273](https://github.com/apache/arrow-rs/pull/273) ([mgill25](https://github.com/mgill25))
- Fix null struct and list roundtrip [\#270](https://github.com/apache/arrow-rs/pull/270) ([nevi-me](https://github.com/nevi-me))
- 1.52 clippy fixes [\#267](https://github.com/apache/arrow-rs/pull/267) ([nevi-me](https://github.com/nevi-me))
- Fix typo in csv/reader.rs [\#265](https://github.com/apache/arrow-rs/pull/265) ([domoritz](https://github.com/domoritz))
- Fix empty Schema::metadata deserialization error [\#260](https://github.com/apache/arrow-rs/pull/260) ([hulunbier](https://github.com/hulunbier))
- update datafusion and ballista doc links [\#259](https://github.com/apache/arrow-rs/pull/259) ([Jimexist](https://github.com/Jimexist))
- support full u32 and u64 roundtrip through parquet [\#258](https://github.com/apache/arrow-rs/pull/258) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- \[MINOR\] Added env to run rust in integration. [\#253](https://github.com/apache/arrow-rs/pull/253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- \[Minor\] Made integration tests always run. [\#248](https://github.com/apache/arrow-rs/pull/248) ([jorgecarleitao](https://github.com/jorgecarleitao))
- fix parquet max\_definition for non-null structs [\#246](https://github.com/apache/arrow-rs/pull/246) ([nevi-me](https://github.com/nevi-me))
- Disabled rebase needed until demonstrate working. [\#243](https://github.com/apache/arrow-rs/pull/243) ([jorgecarleitao](https://github.com/jorgecarleitao))
- pin flatbuffers to 0.8.4 [\#239](https://github.com/apache/arrow-rs/pull/239) ([ritchie46](https://github.com/ritchie46))
- sort\_primitive result is capped to the min of limit or values.len [\#236](https://github.com/apache/arrow-rs/pull/236) ([medwards](https://github.com/medwards))
- Read list field correctly [\#234](https://github.com/apache/arrow-rs/pull/234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix code examples for RecordBatch::try\_from\_iter [\#231](https://github.com/apache/arrow-rs/pull/231) ([alamb](https://github.com/alamb))
- Support string dictionaries in csv reader \(\#228\) [\#229](https://github.com/apache/arrow-rs/pull/229) ([tustvold](https://github.com/tustvold))
- support LargeUtf8 in sort kernel [\#26](https://github.com/apache/arrow-rs/pull/26) ([ritchie46](https://github.com/ritchie46))
- Removed unused files [\#22](https://github.com/apache/arrow-rs/pull/22) ([jorgecarleitao](https://github.com/jorgecarleitao))
- ARROW-12504: Buffer::from\_slice\_ref set correct capacity [\#18](https://github.com/apache/arrow-rs/pull/18) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add GitHub templates [\#17](https://github.com/apache/arrow-rs/pull/17) ([andygrove](https://github.com/andygrove))
- ARROW-12493: Add support for writing dictionary arrays to CSV and JSON [\#16](https://github.com/apache/arrow-rs/pull/16) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ARROW-12426: \[Rust\] Fix concatenation of arrow dictionaries [\#15](https://github.com/apache/arrow-rs/pull/15) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update repository and homepage urls [\#14](https://github.com/apache/arrow-rs/pull/14) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Dandandan](https://github.com/Dandandan))
- Added rebase-needed bot [\#13](https://github.com/apache/arrow-rs/pull/13) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added Integration tests against arrow [\#10](https://github.com/apache/arrow-rs/pull/10) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [4.4.0](https://github.com/apache/arrow-rs/tree/4.4.0) (2021-06-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.3.0...4.4.0)

**Breaking changes:**

- migrate partition kernel to use Iterator trait [\#437](https://github.com/apache/arrow-rs/issues/437) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove DictionaryArray::keys\_array [\#391](https://github.com/apache/arrow-rs/issues/391) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Implemented enhancements:**

- sort kernel boolean sort can be O\(n\) [\#447](https://github.com/apache/arrow-rs/issues/447) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- C data interface for decimal128, timestamp, date32 and date64 [\#413](https://github.com/apache/arrow-rs/issues/413)
- Add Decimal to CsvWriter [\#405](https://github.com/apache/arrow-rs/issues/405)
- Use iterators to increase performance of creating Arrow arrays [\#200](https://github.com/apache/arrow-rs/issues/200) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Fixed bugs:**

- Release Audit Tool \(RAT\) is not being triggered [\#481](https://github.com/apache/arrow-rs/issues/481)
- Security Vulnerabilities: flatbuffers: `read_scalar` and `read_scalar_at` allow transmuting values without `unsafe` blocks [\#476](https://github.com/apache/arrow-rs/issues/476)
- Clippy broken after upgrade to rust 1.53 [\#467](https://github.com/apache/arrow-rs/issues/467)
- Pull Request Labeler is not working [\#462](https://github.com/apache/arrow-rs/issues/462)
- Arrow 4.3 release: error\[E0658\]: use of unstable library feature 'partition\_point': new API [\#456](https://github.com/apache/arrow-rs/issues/456)
- parquet reading hangs when row\_group contains more than 2048 rows of data [\#349](https://github.com/apache/arrow-rs/issues/349)
- Fail to build arrow  [\#247](https://github.com/apache/arrow-rs/issues/247)
- JSON reader does not implement iterator [\#193](https://github.com/apache/arrow-rs/issues/193) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Security fixes:**

- Ensure a successful MIRI Run on CI [\#227](https://github.com/apache/arrow-rs/issues/227)

**Closed issues:**

- sort kernel has a lot of unnecessary wrapping [\#446](https://github.com/apache/arrow-rs/issues/446)
- \[Parquet\] Plain encoded boolean column chunks limited to 2048 values [\#48](https://github.com/apache/arrow-rs/issues/48) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

## [4.3.0](https://github.com/apache/arrow-rs/tree/4.3.0) (2021-06-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.2.0...4.3.0)

**Implemented enhancements:**

- Add partitioning kernel for sorted arrays [\#428](https://github.com/apache/arrow-rs/issues/428) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Implement sort by float lists [\#427](https://github.com/apache/arrow-rs/issues/427) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Derive Eq and PartialEq for SortOptions [\#426](https://github.com/apache/arrow-rs/issues/426) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- use prettier and github action to normalize markdown document syntax [\#399](https://github.com/apache/arrow-rs/issues/399)
- window::shift can work for more than just primitive array type [\#392](https://github.com/apache/arrow-rs/issues/392)
- Doctest for ArrayBuilder [\#366](https://github.com/apache/arrow-rs/issues/366)

**Fixed bugs:**

- Boolean `not` kernel does not take offset of null buffer into account [\#417](https://github.com/apache/arrow-rs/issues/417)
- my contribution not marged in 4.2 release  [\#394](https://github.com/apache/arrow-rs/issues/394)
- window::shift shall properly handle boundary cases [\#387](https://github.com/apache/arrow-rs/issues/387)
- Parquet `WriterProperties.max_row_group_size` not wired up [\#257](https://github.com/apache/arrow-rs/issues/257)
- Out of bound reads in chunk iterator [\#198](https://github.com/apache/arrow-rs/issues/198) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [4.2.0](https://github.com/apache/arrow-rs/tree/4.2.0) (2021-05-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.1.0...4.2.0)

**Breaking changes:**

- DictionaryArray::values\(\) clones the underlying ArrayRef [\#313](https://github.com/apache/arrow-rs/issues/313) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Implemented enhancements:**

- Simplify shift kernel using null array [\#371](https://github.com/apache/arrow-rs/issues/371)
- Provide `Arc`-based constructor for `parquet::util::cursor::SliceableCursor` [\#368](https://github.com/apache/arrow-rs/issues/368)
- Add badges to crates [\#361](https://github.com/apache/arrow-rs/issues/361)
- Consider inlining PrimitiveArray::value [\#328](https://github.com/apache/arrow-rs/issues/328)
- Implement automated release verification script [\#327](https://github.com/apache/arrow-rs/issues/327)
- Add wasm32 to the list of target architectures of the simd feature [\#316](https://github.com/apache/arrow-rs/issues/316)
- add with\_escape for csv::ReaderBuilder [\#315](https://github.com/apache/arrow-rs/issues/315) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- IPC feature gate [\#310](https://github.com/apache/arrow-rs/issues/310)
- csv feature gate [\#309](https://github.com/apache/arrow-rs/issues/309) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add `shrink_to` / `shrink_to_fit` to `MutableBuffer` [\#297](https://github.com/apache/arrow-rs/issues/297)

**Fixed bugs:**

- Incorrect crate setup instructions [\#364](https://github.com/apache/arrow-rs/issues/364)
- Arrow-flight only register rerun-if-changed if file exists [\#350](https://github.com/apache/arrow-rs/issues/350)
- Dictionary Comparison Uses Wrong Values Array [\#332](https://github.com/apache/arrow-rs/issues/332)
- Undefined behavior in FFI implementation [\#322](https://github.com/apache/arrow-rs/issues/322)
- All-null column get wrong parquet null-counts [\#306](https://github.com/apache/arrow-rs/issues/306) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Filter has inconsistent null handling [\#295](https://github.com/apache/arrow-rs/issues/295)

## [4.1.0](https://github.com/apache/arrow-rs/tree/4.1.0) (2021-05-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.0.0...4.1.0)

**Implemented enhancements:**

- Add Send to ArrayBuilder [\#290](https://github.com/apache/arrow-rs/issues/290) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Improve performance of bound checking option [\#280](https://github.com/apache/arrow-rs/issues/280) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- extend compute kernel arity to include nullary functions [\#276](https://github.com/apache/arrow-rs/issues/276)
- Implement FFI / CDataInterface for Struct Arrays [\#251](https://github.com/apache/arrow-rs/issues/251) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add support for pretty-printing Decimal numbers [\#230](https://github.com/apache/arrow-rs/issues/230) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- CSV Reader String Dictionary Support [\#228](https://github.com/apache/arrow-rs/issues/228) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Add Builder interface for adding Arrays to record batches [\#210](https://github.com/apache/arrow-rs/issues/210) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support auto-vectorization for min/max [\#209](https://github.com/apache/arrow-rs/issues/209) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support LargeUtf8 in sort kernel [\#25](https://github.com/apache/arrow-rs/issues/25) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Fixed bugs:**

-  no method named `select_nth_unstable_by` found for mutable reference `&mut [T]`  [\#283](https://github.com/apache/arrow-rs/issues/283)
- Rust 1.52 Clippy error [\#266](https://github.com/apache/arrow-rs/issues/266)
- NaNs can break parquet statistics [\#255](https://github.com/apache/arrow-rs/issues/255) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- u64::MAX does not roundtrip through parquet [\#254](https://github.com/apache/arrow-rs/issues/254) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Integration tests failing to compile \(flatbuffer\) [\#249](https://github.com/apache/arrow-rs/issues/249) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix compatibility quirks between arrow and parquet structs [\#245](https://github.com/apache/arrow-rs/issues/245) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Unable to write non-null Arrow structs to Parquet [\#244](https://github.com/apache/arrow-rs/issues/244) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- schema: missing field `metadata` when deserialize [\#241](https://github.com/apache/arrow-rs/issues/241) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Arrow does not compile due to flatbuffers upgrade [\#238](https://github.com/apache/arrow-rs/issues/238) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Sort with limit panics for the limit includes some but not all nulls, for large arrays [\#235](https://github.com/apache/arrow-rs/issues/235) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- arrow-rs contains a copy of the "format" directory [\#233](https://github.com/apache/arrow-rs/issues/233) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Fix SEGFAULT/ SIGILL in child-data ffi [\#206](https://github.com/apache/arrow-rs/issues/206) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Read list field correctly in \<struct\<list\>\> [\#167](https://github.com/apache/arrow-rs/issues/167) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- FFI listarray lead to undefined behavior.  [\#20](https://github.com/apache/arrow-rs/issues/20)

**Security fixes:**

- Fix MIRI build on CI [\#226](https://github.com/apache/arrow-rs/issues/226) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Get MIRI running again [\#224](https://github.com/apache/arrow-rs/issues/224) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

**Documentation updates:**

- Comment out the instructions in the PR template [\#277](https://github.com/apache/arrow-rs/issues/277)
- Update links to datafusion and ballista in README.md [\#19](https://github.com/apache/arrow-rs/issues/19)
- Update "repository" in Cargo.toml [\#12](https://github.com/apache/arrow-rs/issues/12)

**Closed issues:**

- Arrow Aligned Vec [\#268](https://github.com/apache/arrow-rs/issues/268)
- \[Rust\]: Tracking issue for AVX-512 [\#220](https://github.com/apache/arrow-rs/issues/220) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Umbrella issue for clippy integration [\#217](https://github.com/apache/arrow-rs/issues/217) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support sort [\#215](https://github.com/apache/arrow-rs/issues/215) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Support stable Rust [\#214](https://github.com/apache/arrow-rs/issues/214) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Remove Rust and point integration tests to arrow-rs repo [\#211](https://github.com/apache/arrow-rs/issues/211) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- ArrayData buffers are inconsistent across implementations [\#207](https://github.com/apache/arrow-rs/issues/207)
- 3.0.1 patch release [\#204](https://github.com/apache/arrow-rs/issues/204)
- Document patch release process [\#202](https://github.com/apache/arrow-rs/issues/202)
- Simplify Offset [\#186](https://github.com/apache/arrow-rs/issues/186) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Typed Bytes [\#185](https://github.com/apache/arrow-rs/issues/185) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[CI\]docker-compose setup should enable caching [\#175](https://github.com/apache/arrow-rs/issues/175)
- Improve take primitive performance [\#174](https://github.com/apache/arrow-rs/issues/174)
- \[CI\] Try out buildkite [\#165](https://github.com/apache/arrow-rs/issues/165) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Update assignees in JIRA where missing [\#160](https://github.com/apache/arrow-rs/issues/160)
- \[Rust\]: From\<ArrayDataRef\> implementations should validate data type [\#103](https://github.com/apache/arrow-rs/issues/103) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Verify that projection push down does not remove aliases columns [\#99](https://github.com/apache/arrow-rs/issues/99) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Implement modulus expression [\#98](https://github.com/apache/arrow-rs/issues/98) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Add constant folding to expressions during logically planning [\#96](https://github.com/apache/arrow-rs/issues/96) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] DataFrame.collect should return RecordBatchReader [\#95](https://github.com/apache/arrow-rs/issues/95) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Add FORMAT to explain plan and an easy to visualize format [\#94](https://github.com/apache/arrow-rs/issues/94) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement metrics framework [\#90](https://github.com/apache/arrow-rs/issues/90) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement micro benchmarks for each operator [\#89](https://github.com/apache/arrow-rs/issues/89) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement pretty print for physical query plan [\#88](https://github.com/apache/arrow-rs/issues/88) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Archery\] Support rust clippy in the lint command [\#83](https://github.com/apache/arrow-rs/issues/83)
- \[rust\]\[datafusion\] optimize count\(\*\) queries on parquet sources [\#75](https://github.com/apache/arrow-rs/issues/75) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[Rust\]\[DataFusion\] Improve like/nlike performance [\#71](https://github.com/apache/arrow-rs/issues/71) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Implement optimizer rule to remove redundant projections [\#56](https://github.com/apache/arrow-rs/issues/56) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- \[DataFusion\] Parquet data source does not support complex types [\#39](https://github.com/apache/arrow-rs/issues/39) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Merge utils from Parquet and Arrow [\#32](https://github.com/apache/arrow-rs/issues/32) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Add benchmarks for Parquet [\#30](https://github.com/apache/arrow-rs/issues/30) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Mark methods that do not perform bounds checking as unsafe [\#28](https://github.com/apache/arrow-rs/issues/28) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- Test issue [\#24](https://github.com/apache/arrow-rs/issues/24) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]
- This is a test issue [\#11](https://github.com/apache/arrow-rs/issues/11)
