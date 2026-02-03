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

## [57.3.0](https://github.com/apache/arrow-rs/tree/57.3.0) (2026-02-02)

[Full Changelog](https://github.com/apache/arrow-rs/compare/57.2.0...57.3.0)

**Breaking changes:**

- Revert "Seal Array trait", mark `Array` as `unsafe` [#9313](https://github.com/apache/arrow-rs/pull/9313) ([alamb](https://github.com/alamb), [gabotechs](https://github.com/gabotechs))
- Mark `BufferBuilder::new_from_buffer` as unsafe [#9312](https://github.com/apache/arrow-rs/pull/9312) ([alamb](https://github.com/alamb), [Jefffrey](https://github.com/Jefffrey))

**Fixed bugs:**

- Fix string array equality when the values buffer is the same and only the offsets to access it differ [#9330](https://github.com/apache/arrow-rs/pull/9330) ([alamb](https://github.com/alamb), [jhorstmann](https://github.com/jhorstmann))
- Ensure `BufferBuilder::truncate` doesn't overset length [#9311](https://github.com/apache/arrow-rs/pull/9311) ([alamb](https://github.com/alamb), [Jefffrey](https://github.com/Jefffrey))
- [parquet] Provide only encrypted column stats in plaintext footer [#9310](https://github.com/apache/arrow-rs/pull/9310) ([alamb](https://github.com/alamb), [rok](https://github.com/rok), [adamreeve](https://github.com/adamreeve))
- [regression] Error with adaptive predicate pushdown: "Invalid offset …" [#9309](https://github.com/apache/arrow-rs/pull/9309) ([alamb](https://github.com/alamb), [erratic-pattern](https://github.com/erratic-pattern), [sdf-jkl](https://github.com/sdf-jkl))
