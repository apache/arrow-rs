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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
