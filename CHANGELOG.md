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

## [53.4.0](https://github.com/apache/arrow-rs/tree/53.4.0) (2025-01-14)

[Full Changelog](https://github.com/apache/arrow-rs/compare/53.3.0...53.4.0)

**Merged pull requests:**

* fix clippy (#6791) (#6940)
* fix: decimal conversion looses value on lower precision (#6836) (#6936)
* perf: Use Cow in get_format_string in FFI_ArrowSchema (#6853) (#6937)
* fix: Encoding of List offsets was incorrect when slice offsets begin …
* [arrow-cast] Support cast numeric to string view (alternate) (#6816) (#…
* Enable matching temporal as from_type to Utf8View (#6872) (#6956)
* [arrow-cast] Support cast boolean from/to string view (#6822) (#6957)
* [53.0.0_maintenance] Fix CI (#6964)
* Add Array::shrink_to_fit(&mut self) to 53.4.0 (#6790) (#6817) (#6962)