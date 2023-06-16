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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
