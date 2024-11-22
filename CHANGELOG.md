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



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
