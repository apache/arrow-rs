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

# Security Policy

This document outlines the security model for the Rust implementation of Apache Arrow (`arrow-rs`) and how to report vulnerabilities.

## Security Model

The `arrow-rs` project follows the [Apache Arrow Security Model]. Key aspects include:
- Reading data from untrusted sources (e.g., over a network or from a file) requires explicit validation.
- Failure to validate untrusted data before use may lead to security issues. This implementation provides APIs to validate Arrow data. For example, [`ArrayData::validate_full`] can be used to ensure that data conforms to the Arrow specification.

## Rust Safety and Undefined Behavior

We strive to uphold the [Rust Soundness Pledge].

- **Undefined Behavior (UB) is a bug:** Any instance of UB is a bug we are committed to fixing.
- **UB as a Security Issue:** Any **exploitable** UB triggered via safe APIs is a security issue. Other UB instances are bugs, and we welcome help fixing them.

## Reporting a Vulnerability

**Do not file a public issue.** Follow the [ASF security reporting process] by emailing [security@apache.org](mailto:security@apache.org).

Include in your report:
- A clear description and minimal reproducer.
- Affected crates and versions.
- Potential impact.

[Apache Arrow Security Model]: https://arrow.apache.org/docs/dev/format/Security.html
[`ArrayData::validate_full`]: https://docs.rs/arrow/latest/arrow/array/struct.ArrayData.html#method.validate_full
[Rust Soundness Pledge]: https://raphlinus.github.io/rust/2020/01/18/soundness-pledge.html
[ASF security reporting process]: https://www.apache.org/security/#reporting-a-vulnerability
