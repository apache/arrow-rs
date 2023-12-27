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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
