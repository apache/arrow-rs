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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
