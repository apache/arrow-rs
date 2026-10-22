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


## [59.3.2](https://github.com/apache/arrow-rs/tree/59.3.2) - (2026-10-22)

[Full Changelog](https://github.com/apache/arrow-rs/compare/59.3.1...59.3.2)

### Bug fixes
- [59_maintenance] Write empty validity buffers for arrays without nulls in IPC writer by @alamb in [#11820](https://github.com/apache/arrow-rs/pull/11820)
- [59_maintenance] fix(arrow-buffer): prevent use-after-free in Buffer::shrink_to_fit when MemoryReservation::resize panics by @alamb in [#11787](https://github.com/apache/arrow-rs/pull/11787)
- [59_maintenance] fix(parquet): skip miniblocks wider than 64 values instead of erroring by @Jefffrey in [#11754](https://github.com/apache/arrow-rs/pull/11754)
- [59_maintenance] fix: account for offsets in runarrays when using extend for MutableArrayData by @Jefffrey in [#11671](https://github.com/apache/arrow-rs/pull/11671)
- [59_maintenance] fix: account for offset when building RunArray from ArrayData by @alamb in [#11739](https://github.com/apache/arrow-rs/pull/11739)
- [59_maintenance] fix: use correct length in runarray validation by @alamb in [#11740](https://github.com/apache/arrow-rs/pull/11740)
- [59_maintenance] fix: account for arraydata offset when comparing runarrays by @Jefffrey in [#11673](https://github.com/apache/arrow-rs/pull/11673)
- [59_maintenance] fix(arrow-schema): validate `UnionFields` when deserializing by @Jefffrey in [#11672](https://github.com/apache/arrow-rs/pull/11672)
- [59_maintenance] fix: implement `ArrayData::validate_values` for union types by @Jefffrey in [#11678](https://github.com/apache/arrow-rs/pull/11678)
- [59_maintenance] Fix sliced sparse union ArrayData conversion by @Jefffrey in [#11679](https://github.com/apache/arrow-rs/pull/11679)
- [59_maintenance] fix(arrow-data): account for FixedSizeList parent offset in MutableArrayData by @Jefffrey in [#11683](https://github.com/apache/arrow-rs/pull/11683)
- [59_maintenance] fix: `UnionFields::from_iter` should validate input by @Jefffrey in [#11670](https://github.com/apache/arrow-rs/pull/11670)
- [59_maintenance] fix(arrow-array): use the field's type id in dense union logical_nulls by @Jefffrey in [#11680](https://github.com/apache/arrow-rs/pull/11680)
- [59_maintenance] Parquet: list the dictionary page first in page_encoding_stats by @Jefffrey in [#11699](https://github.com/apache/arrow-rs/pull/11699)
- [59_maintenance] fix(arrow-data): check view nulls at the right index in `ArrayData` equality by @Jefffrey in [#11675](https://github.com/apache/arrow-rs/pull/11675)

