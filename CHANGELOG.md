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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
