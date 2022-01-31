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
* [59f96e842d05b63882f7ba285c66a9739761cf84](https://github.com/apache/arrow-rs/commit/59f96e842d05b63882f7ba285c66a9739761cf84) add ilike comparitor ([#874](https://github.com/apache/arrow-rs/pull/874)) ([#961](https://github.com/apache/arrow-rs/pull/961))
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
- Document has no link to `RowColumIter` [\#762](https://github.com/apache/arrow-rs/issues/762)
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
- Allow the creation of String arrays from an interator of &Option\<&str\> [\#598](https://github.com/apache/arrow-rs/issues/598)
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
- remove clippy unnecessary wraps suppresions in cast kernel [\#449](https://github.com/apache/arrow-rs/pull/449) ([Jimexist](https://github.com/Jimexist))
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
- ARROW-12426: \[Rust\] Fix concatentation of arrow dictionaries [\#15](https://github.com/apache/arrow-rs/pull/15) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
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
- ArrayData buffers are inconsistent accross implementations [\#207](https://github.com/apache/arrow-rs/issues/207)
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

For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)


\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
