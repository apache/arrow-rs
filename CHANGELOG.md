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


## [59.3.0](https://github.com/apache/arrow-rs/tree/59.3.0) - (2026-08-25)

[Full Changelog](https://github.com/apache/arrow-rs/compare/59.2.0...59.3.0)

### Security fixes
- [59_maintenance] Backport `cargo audit` fix by updating `h2` dependency by @alamb in [#10834](https://github.com/apache/arrow-rs/pull/10834)

### Bug fixes
- [59_maintenance] Backport fix for concat_run_arrays with all-empty run arrays by @alamb in [#10828](https://github.com/apache/arrow-rs/pull/10828)
- [59_maintenance] Backport fix for DELTA_BYTE_ARRAY dedup with values larger than the page size limit by @alamb in [#10826](https://github.com/apache/arrow-rs/pull/10826)
- [59_maintenance] Backport fix for cached Mask reads crossing unloaded sparse pages by @alamb in [#10766](https://github.com/apache/arrow-rs/pull/10766)

