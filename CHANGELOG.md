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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
