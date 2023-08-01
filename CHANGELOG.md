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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
