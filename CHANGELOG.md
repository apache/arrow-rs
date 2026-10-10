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


## [59.3.1](https://github.com/apache/arrow-rs/tree/59.3.1) - (2026-10-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/59.3.0...59.3.1)

### Bug fixes
- [59_maintenance] fix(arrow-array): avoid returning corrupt array in `GenericByteArray::into_builder` by @Jefffrey in [#11446](https://github.com/apache/arrow-rs/pull/11446)
- [59_maintenance] fix(arrow-array): preserve type params in `PrimitiveArray::into_builder` by @Jefffrey in [#11448](https://github.com/apache/arrow-rs/pull/11448)
- [59_maintenance] fix(arrow-cast): avoid panicking during failed List -> ListView cast by @Jefffrey in [#11445](https://github.com/apache/arrow-rs/pull/11445)
- [59_maintenance] Fix filter_nulls for predicates selecting all or no rows by @Jefffrey in [#11444](https://github.com/apache/arrow-rs/pull/11444)
- [59_maintenance] fix: finish methods in `PrimitiveDictionaryBuilder` discard type params by @Jefffrey in [#11442](https://github.com/apache/arrow-rs/pull/11442)
- [59_maintenance] fix(arrow-array): `PrimitiveRunBuilder` was buggy after `finish` by @Jefffrey in [#11441](https://github.com/apache/arrow-rs/pull/11441)
- [59_maintenance] fix(parquet): keep virtual columns in the schema reported with a schema hint by @Jefffrey in [#11447](https://github.com/apache/arrow-rs/pull/11447)
- [59_maintenance] fix(arrow-data): fix gaps in ListView equality by @Jefffrey in [#11439](https://github.com/apache/arrow-rs/pull/11439)
- [59_maintenance] fix: Parquet `ByteArrayDecoderPlain::read` can silently fail to read the correct number of values by @Jefffrey in [#11440](https://github.com/apache/arrow-rs/pull/11440)
- [59_maintenance] fix(parquet): reject Thrift list sizes larger than remaining input by @Jefffrey in [#11353](https://github.com/apache/arrow-rs/pull/11353)

### Miscellaneous
- [59_maintenance] Identify the chronoutil-derived MIT code in the root LICENSE.txt by @Jefffrey in [#11358](https://github.com/apache/arrow-rs/pull/11358)
- [59_maintenance] fix: update rustls to address RUSTSEC-2026-0285 by @Jefffrey in [#11357](https://github.com/apache/arrow-rs/pull/11357)

