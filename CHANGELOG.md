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

## [5.2.0](https://github.com/apache/arrow-rs/tree/5.2.0) (2021-08-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.1.0...5.2.0)

* [aed425968162b22e3ced31a81cc876d6dcdebca5](https://github.com/apache/arrow-rs/commit/aed425968162b22e3ced31a81cc876d6dcdebca5) Write boolean stats for boolean columns (not i32 stats) (#661) (#682)
* [6a96f5fd9babd42d9f603a9b48242cf5f2f283b8](https://github.com/apache/arrow-rs/commit/6a96f5fd9babd42d9f603a9b48242cf5f2f283b8) Write FixedLenByteArray stats for FixedLenByteArray columns (not ByteArray stats) (#662) (#683)
* [72f240735dc91b306689f7281e896385dc27f4c9](https://github.com/apache/arrow-rs/commit/72f240735dc91b306689f7281e896385dc27f4c9) Allow creation of String arrays from &Option<&str> iterators (#680) (#686)
* [52bbc81c9c4cd8a05a0573709a0982a902524e87](https://github.com/apache/arrow-rs/commit/52bbc81c9c4cd8a05a0573709a0982a902524e87) Doctests for DictionaryArray::from_iter, PrimitiveDictionaryBuilder and DecimalBuilder. (#673) (#679)
* [03af5e490a6ba0567eaebaff60ec155d2e3dc35f](https://github.com/apache/arrow-rs/commit/03af5e490a6ba0567eaebaff60ec155d2e3dc35f) Add some do comments to parquet bit_util (#663) (#678)
* [be471fd7859dd6c885e537169aa099df4c63a9d8](https://github.com/apache/arrow-rs/commit/be471fd7859dd6c885e537169aa099df4c63a9d8) allocate enough bytes when writing booleans (#658) (#677)
* [876f397a99821424681c7839de3ca729f525edd1](https://github.com/apache/arrow-rs/commit/876f397a99821424681c7839de3ca729f525edd1) Fix parquet string statistics generation (#643) (#676)
* [ead64b7a2fb6c4e0e3c05c7e27aa2c043882f7c3](https://github.com/apache/arrow-rs/commit/ead64b7a2fb6c4e0e3c05c7e27aa2c043882f7c3) Remove undefined behavior in `value` method of boolean and primitive arrays (#644) (#668)
* [107a604ba8086ccc92f3da835d618c744c476f70](https://github.com/apache/arrow-rs/commit/107a604ba8086ccc92f3da835d618c744c476f70) Speed up filter_record_batch with one array (#637) (#666)
* [dace74b840baae239e6feb379ce152db01bdf155](https://github.com/apache/arrow-rs/commit/dace74b840baae239e6feb379ce152db01bdf155) Doctests for from_iter for BooleanArray & for BooleanBuilder. (#647) (#669)
* [38e85eb7e2ec58824f515f5e8695b8137a1d275d](https://github.com/apache/arrow-rs/commit/38e85eb7e2ec58824f515f5e8695b8137a1d275d) Add human readable Format for parquet ByteArray (#642) (#667)
* [51f2b2bf4a106530e0d6ffc0108e8215c5d413e3](https://github.com/apache/arrow-rs/commit/51f2b2bf4a106530e0d6ffc0108e8215c5d413e3) Fix data corruption in json decoder f64-to-i64 cast (#652) (#665)
