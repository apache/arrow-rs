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

For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

# Changelog

## [5.3.0](https://github.com/apache/arrow-rs/tree/5.3.0) (2021-08-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.2.0...5.3.0)

* [559db29d7f1a529487a4a9b3502d102ffd9ec932](https://github.com/apache/arrow-rs/commit/559db29d7f1a529487a4a9b3502d102ffd9ec932) fix edition 2021 ([#714](https://github.com/apache/arrow-rs/pull/714)) ([#719](https://github.com/apache/arrow-rs/pull/719))
* [c8d492c8282d4c8045028a5555186227e8f40963](https://github.com/apache/arrow-rs/commit/c8d492c8282d4c8045028a5555186227e8f40963) Support arrow readers for strings with DELTA_BYTE_ARRAY encoding ([#709](https://github.com/apache/arrow-rs/pull/709)) ([#718](https://github.com/apache/arrow-rs/pull/718))
* [446a4b7ae44eaa48eb34fa46b319a470fa3ec971](https://github.com/apache/arrow-rs/commit/446a4b7ae44eaa48eb34fa46b319a470fa3ec971) Implement `regexp_matches_utf8` ([#706](https://github.com/apache/arrow-rs/pull/706)) ([#717](https://github.com/apache/arrow-rs/pull/717))
* [4ca0d95fa71d9733b20cbcb4bad92d024e680589](https://github.com/apache/arrow-rs/commit/4ca0d95fa71d9733b20cbcb4bad92d024e680589) Cherry pick of Support binary data type in build_struct_array ([#705](https://github.com/apache/arrow-rs/pull/705))
* [412a5bde386c29e18d83570ce42fabfc9c207f27](https://github.com/apache/arrow-rs/commit/412a5bde386c29e18d83570ce42fabfc9c207f27) Doctest for PrimitiveArray using from_iter_values. ([#694](https://github.com/apache/arrow-rs/pull/694)) ([#700](https://github.com/apache/arrow-rs/pull/700))
* [1fb621063b8d54d95944268f603190bb15ae311a](https://github.com/apache/arrow-rs/commit/1fb621063b8d54d95944268f603190bb15ae311a) Add get_bit to BooleanBufferBuilder ([#693](https://github.com/apache/arrow-rs/pull/693)) ([#699](https://github.com/apache/arrow-rs/pull/699))
* [0534f43a98030b4e9bbdc57b752c8d3798d29fbc](https://github.com/apache/arrow-rs/commit/0534f43a98030b4e9bbdc57b752c8d3798d29fbc) allow casting from Timestamp based arrays to utf8 ([#664](https://github.com/apache/arrow-rs/pull/664)) ([#698](https://github.com/apache/arrow-rs/pull/698))
* [3c0a8bd81e50858b870345d2d8ece176b27af883](https://github.com/apache/arrow-rs/commit/3c0a8bd81e50858b870345d2d8ece176b27af883) Add note about changelog generation to README ([#639](https://github.com/apache/arrow-rs/pull/639)) ([#689](https://github.com/apache/arrow-rs/pull/689))

## [5.2.0](https://github.com/apache/arrow-rs/tree/5.2.0) (2021-08-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.1.0...5.2.0)

* [aed425968162b22e3ced31a81cc876d6dcdebca5](https://github.com/apache/arrow-rs/commit/aed425968162b22e3ced31a81cc876d6dcdebca5) Write boolean stats for boolean columns (not i32 stats) ([#661](https://github.com/apache/arrow-rs/pull/661)) ([#682](https://github.com/apache/arrow-rs/pull/682))
* [6a96f5fd9babd42d9f603a9b48242cf5f2f283b8](https://github.com/apache/arrow-rs/commit/6a96f5fd9babd42d9f603a9b48242cf5f2f283b8) Write FixedLenByteArray stats for FixedLenByteArray columns (not ByteArray stats) ([#662](https://github.com/apache/arrow-rs/pull/662)) ([#683](https://github.com/apache/arrow-rs/pull/683))
* [72f240735dc91b306689f7281e896385dc27f4c9](https://github.com/apache/arrow-rs/commit/72f240735dc91b306689f7281e896385dc27f4c9) Allow creation of String arrays from &Option<&str> iterators ([#680](https://github.com/apache/arrow-rs/pull/680)) ([#686](https://github.com/apache/arrow-rs/pull/686))
* [52bbc81c9c4cd8a05a0573709a0982a902524e87](https://github.com/apache/arrow-rs/commit/52bbc81c9c4cd8a05a0573709a0982a902524e87) Doctests for DictionaryArray::from_iter, PrimitiveDictionaryBuilder and DecimalBuilder. ([#673](https://github.com/apache/arrow-rs/pull/673)) ([#679](https://github.com/apache/arrow-rs/pull/679))
* [03af5e490a6ba0567eaebaff60ec155d2e3dc35f](https://github.com/apache/arrow-rs/commit/03af5e490a6ba0567eaebaff60ec155d2e3dc35f) Add some do comments to parquet bit_util ([#663](https://github.com/apache/arrow-rs/pull/663)) ([#678](https://github.com/apache/arrow-rs/pull/678))
* [be471fd7859dd6c885e537169aa099df4c63a9d8](https://github.com/apache/arrow-rs/commit/be471fd7859dd6c885e537169aa099df4c63a9d8) allocate enough bytes when writing booleans ([#658](https://github.com/apache/arrow-rs/pull/658)) ([#677](https://github.com/apache/arrow-rs/pull/677))
* [876f397a99821424681c7839de3ca729f525edd1](https://github.com/apache/arrow-rs/commit/876f397a99821424681c7839de3ca729f525edd1) Fix parquet string statistics generation ([#643](https://github.com/apache/arrow-rs/pull/643)) ([#676](https://github.com/apache/arrow-rs/pull/676))
* [ead64b7a2fb6c4e0e3c05c7e27aa2c043882f7c3](https://github.com/apache/arrow-rs/commit/ead64b7a2fb6c4e0e3c05c7e27aa2c043882f7c3) Remove undefined behavior in `value` method of boolean and primitive arrays ([#644](https://github.com/apache/arrow-rs/pull/644)) ([#668](https://github.com/apache/arrow-rs/pull/668))
* [107a604ba8086ccc92f3da835d618c744c476f70](https://github.com/apache/arrow-rs/commit/107a604ba8086ccc92f3da835d618c744c476f70) Speed up filter_record_batch with one array ([#637](https://github.com/apache/arrow-rs/pull/637)) ([#666](https://github.com/apache/arrow-rs/pull/666))
* [dace74b840baae239e6feb379ce152db01bdf155](https://github.com/apache/arrow-rs/commit/dace74b840baae239e6feb379ce152db01bdf155) Doctests for from_iter for BooleanArray & for BooleanBuilder. ([#647](https://github.com/apache/arrow-rs/pull/647)) ([#669](https://github.com/apache/arrow-rs/pull/669))
* [38e85eb7e2ec58824f515f5e8695b8137a1d275d](https://github.com/apache/arrow-rs/commit/38e85eb7e2ec58824f515f5e8695b8137a1d275d) Add human readable Format for parquet ByteArray ([#642](https://github.com/apache/arrow-rs/pull/642)) ([#667](https://github.com/apache/arrow-rs/pull/667))
* [51f2b2bf4a106530e0d6ffc0108e8215c5d413e3](https://github.com/apache/arrow-rs/commit/51f2b2bf4a106530e0d6ffc0108e8215c5d413e3) Fix data corruption in json decoder f64-to-i64 cast ([#652](https://github.com/apache/arrow-rs/pull/652)) ([#665](https://github.com/apache/arrow-rs/pull/665))

## [5.1.0](https://github.com/apache/arrow-rs/tree/5.1.0) (2021-07-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.0.0...5.1.0)

* 89932505dbe34904886fddb0da9c2e43ea6eaea3 Add links in docstrings (#605) (#633)
* bf2e4f0966dfeba320afc347d7df407f7c76c82a Fix clippy lints for Rust 1.54 (#631) (#634)
* 6d222fa9390d975a5dbf119102da235ad2116581 Doctests for BinaryArray and LargeBinaryArray. (#625) (#630)
* f241bc72467ae9c0f3a79a9ad27d47a3ca739c9e Cherry pick Sort binary to active_release (#621)
* ffe436a21b4c9deda2e019b4214f938c6384fcdb resolve unnecessary borrow clippy lints (#613) (#622)
* 239c253c0c05eb36b763ef9e67b736c1f1af4731 make FFI_ArrowArray::empty() public (#612) (#619)
* 552ef0b4963c5ff543900714b8ff927b6a818d7b Doctests for UnionBuilder and UnionArray. (#603) (#618)
* ec739c2c310a7f2e3624c1bd625af422473700dc fix dyn syntax (#592) (#617)
* 2c83a47a3764b5f7178815600f6276293242691d Fix newline in changelog (#588) (#616)
* d32533a61fe8e21ac994cc4f0a590149ad20320d Fix typo (#604) (#606)
* 4c9e94dd4492fac89a2994f0db71a7ac8a3b1628 Implement `StringBuilder::append_option` (#601) (#611)
* 4ee91492cc421f546f3babd6ebefe1f5b9d672fb Validate output of MIRI (#578) (#610)
* e0d1d5b7505eeccdccbcc02d7276007857a063d9 support struct array in pretty display (#579) (#609)
* 820530da1d5f8cd96b95442a2206ab844ca2f818 use exponential search for lexicographical_partition_ranges (#585) (#608)
* 15d3ca5dbbbb9bd00a206c2afbfe5cd1248c419f Remove Git SHA from created_by Parquet file metadata (#590) (#607)
* 8835adc3e481c3a3585c2139bc5e6ed0d2f5c1ee Update readme link to point at the right codecoverage location (#577) (#584)
* 952cae4c670a8c023ce8b77a4822819e83054fdf fix: undefined behavior in schema ffi (#582) (#583)
* 9dd0aec8f3d8d15ef59e13882cc317ce774b08ce Update triplet.rs (#556) (#574)
* c87a7bc1e8aa943d883801eaedd1d5eb0bb9312a Fix array equal check (#571) (#576)
* 4c21d6db67c7118baee4836683460085c7f054da Add len() to InMemoryWriteableCursor (#564) (#575)
* 5057839b51b3b90b41bebeb205deb38bd6380014 Doctest for StructArray. (#562) (#567)
* 332baca3ed24b516bb166f931db39f6bad19fab8 make has_min_max_set as pub fn (#559) (#566)
* 2998db4a07b5f9cd8da292d7e7e990a19b0b7535 Bump prost and tonic (#560) (#565)
