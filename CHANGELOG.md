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


## [58.4.1](https://github.com/apache/arrow-rs/tree/58.4.1) - (2026-10-22)

[Full Changelog](https://github.com/apache/arrow-rs/compare/58.4.0...58.4.1)

### Bug fixes
- [58_maintenance] Write empty validity buffers for arrays without nulls in IPC writer by @mailmindlin in [#11817](https://github.com/apache/arrow-rs/pull/11817)
- [58_maintenance] fix: account for offsets in runarrays when using extend for MutableArrayData by @Jefffrey in [#11761](https://github.com/apache/arrow-rs/pull/11761)
- [58_maintenance] fix(arrow-schema): validate `UnionFields` when deserializing by @Jefffrey in [#11760](https://github.com/apache/arrow-rs/pull/11760)
- [58_maintenance] fix: implement `ArrayData::validate_values` for union types by @Jefffrey in [#11762](https://github.com/apache/arrow-rs/pull/11762)
- [58_maintenance] fix(arrow-data): check view nulls at the right index in `ArrayData` equality by @Jefffrey in [#11758](https://github.com/apache/arrow-rs/pull/11758)
- [58_maintenance] fix(arrow-array): avoid returning corrupt array in `GenericByteArray::into_builder` by @Jefffrey in [#11707](https://github.com/apache/arrow-rs/pull/11707)
- [58_maintenance] fix(arrow-buffer): prevent use-after-free in Buffer::shrink_to_fit when MemoryReservation::resize panics by @alamb in [#11785](https://github.com/apache/arrow-rs/pull/11785)
- [58_maintenance] fix: account for arraydata offset when comparing runarrays by @Jefffrey in [#11755](https://github.com/apache/arrow-rs/pull/11755)
- [58_maintenance] fix(arrow-data): account for FixedSizeList parent offset in MutableArrayData by @Jefffrey in [#11756](https://github.com/apache/arrow-rs/pull/11756)
- [58_maintenance] fix: use correct length in runarray validation by @Jefffrey in [#11767](https://github.com/apache/arrow-rs/pull/11767)
- [58_maintenance] fix: account for offset when building RunArray from ArrayData by @Jefffrey in [#11766](https://github.com/apache/arrow-rs/pull/11766)
- [58_maintenance] fix: `UnionFields::from_iter` should validate input by @Jefffrey in [#11763](https://github.com/apache/arrow-rs/pull/11763)
- [58_maintenance] Fix sliced sparse union ArrayData conversion by @Jefffrey in [#11759](https://github.com/apache/arrow-rs/pull/11759)
- [58_maintenance] fix(arrow-array): use the field's type id in dense union logical_nulls by @Jefffrey in [#11757](https://github.com/apache/arrow-rs/pull/11757)
- [58_maintenance] fix(arrow-array): `PrimitiveRunBuilder` was buggy after `finish` by @Jefffrey in [#11704](https://github.com/apache/arrow-rs/pull/11704)
- [58_maintenance] fix(arrow-data): fix gaps in ListView equality by @Jefffrey in [#11703](https://github.com/apache/arrow-rs/pull/11703)
- [58_maintenance] fix(arrow-array): preserve type params in `PrimitiveArray::into_builder` by @Jefffrey in [#11765](https://github.com/apache/arrow-rs/pull/11765)
- [58_maintenance] fix: `Buffer::into_mutable` return error instead of panic for converting owned sliced when not start at 0 and fix returned Mutable length by @Jefffrey in [#11706](https://github.com/apache/arrow-rs/pull/11706)
- [58_maintenance] fix: finish methods in `PrimitiveDictionaryBuilder` discard type params by @Jefffrey in [#11705](https://github.com/apache/arrow-rs/pull/11705)

### Miscellaneous
- [58_maintenance] Upgrade legacy Thrift dependency to 0.23 by @beinan in [#11365](https://github.com/apache/arrow-rs/pull/11365)

