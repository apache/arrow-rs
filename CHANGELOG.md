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


## [57.3.2](https://github.com/apache/arrow-rs/tree/57.3.2) - (2026-10-22)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.3.1...57.3.2)

### Bug fixes
- [57_maintenance] fix: implement `ArrayData::validate_values` for union types by @Jefffrey in [#11814](https://github.com/apache/arrow-rs/pull/11814)
- [57_maintenance] fix(arrow-array): avoid returning corrupt array in `GenericByteArray::into_builder` by @Jefffrey in [#11816](https://github.com/apache/arrow-rs/pull/11816)
- [57_maintenance] fix: account for offsets in runarrays when using extend for MutableArrayData by @Jefffrey in [#11815](https://github.com/apache/arrow-rs/pull/11815)
- [57_maintenance] fix(arrow-schema): validate `UnionFields` when deserializing by @Jefffrey in [#11813](https://github.com/apache/arrow-rs/pull/11813)
- [57_maintenance] Fix sliced sparse union ArrayData conversion by @Jefffrey in [#11812](https://github.com/apache/arrow-rs/pull/11812)
- [57_maintenance] fix(arrow-data): account for FixedSizeList parent offset in MutableArrayData by @Jefffrey in [#11809](https://github.com/apache/arrow-rs/pull/11809)
- [57_maintenance] fix(arrow-array): preserve type params in `PrimitiveArray::into_builder` by @Jefffrey in [#11807](https://github.com/apache/arrow-rs/pull/11807)
- [57_maintenance] fix: use correct length in runarray validation by @Jefffrey in [#11801](https://github.com/apache/arrow-rs/pull/11801)
- [57_maintenance] fix(arrow-data): check view nulls at the right index in `ArrayData` equality by @Jefffrey in [#11811](https://github.com/apache/arrow-rs/pull/11811)
- [57_maintenance] fix: account for offset when building RunArray from ArrayData by @Jefffrey in [#11800](https://github.com/apache/arrow-rs/pull/11800)
- [57_maintenance] fix: `Buffer::into_mutable` return error instead of panic for converting owned sliced when not start at 0 and fix returned Mutable length by @Jefffrey in [#11806](https://github.com/apache/arrow-rs/pull/11806)
- [57_maintenance] fix(arrow-array): `PrimitiveRunBuilder` was buggy after `finish` by @Jefffrey in [#11804](https://github.com/apache/arrow-rs/pull/11804)
- [57_maintenance] fix(arrow-data): fix gaps in ListView equality by @Jefffrey in [#11802](https://github.com/apache/arrow-rs/pull/11802)
- [57_maintenance] fix(arrow-array): use the field's type id in dense union logical_nulls by @Jefffrey in [#11810](https://github.com/apache/arrow-rs/pull/11810)
- [57_maintenance] fix: finish methods in `PrimitiveDictionaryBuilder` discard type params by @Jefffrey in [#11805](https://github.com/apache/arrow-rs/pull/11805)
- [57_maintenance] fix(arrow-buffer): prevent use-after-free in Buffer::shrink_to_fit when MemoryReservation::resize panics by @alamb in [#11786](https://github.com/apache/arrow-rs/pull/11786)
- [57_maintenance] fix: `UnionFields::from_iter` should validate input by @alamb in [#11726](https://github.com/apache/arrow-rs/pull/11726)

