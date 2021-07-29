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

## [5.1.0](https://github.com/apache/arrow-rs/tree/5.0.1) (2021-07-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.0.0...5.1.0)

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
*
