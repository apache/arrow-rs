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
- Failure to validate untrusted data before use may lead to security issues. 

This implementation provides APIs to validate Arrow data such as
[`ArrayData::validate_full`] to ensure that data conforms to the Arrow
specification.

Unexpected behavior (e.g., panics, crashes or infinite loops) triggered by
malformed input is generally considered a **bug**, not a security
vulnerability, unless it is **exploitable** by an attacker to

* Execute arbitrary code (Remote Code Execution);
* Exfiltrate sensitive information from process memory (Information Disclosure);

Similarly, undefined behavior using `safe` APIs is considered a bug unless it
can be exploited as above. 


## Reporting Bugs

We treat all bugs seriously, and welcome help fixing them. If you find a bug,
that does not meet the criteria for a security vulnerability, please report it
in the public issue tracker so we can fix it togther.

## Reporting a Vulnerability

For security vulnerabilities, we ask that you follow the responsible disclosure
process outlined below. This allows us to investigate and fix the issue before
it can be exploited in the wild.

**Do not file a public issue.** Follow the [ASF security reporting process] by emailing [security@apache.org](mailto:security@apache.org).

Include in your report:
- A clear description and minimal reproducer.
- Affected crates and versions.
- Potential impact.

[Apache Arrow Security Model]: https://arrow.apache.org/docs/dev/format/Security.html
[`ArrayData::validate_full`]: https://docs.rs/arrow/latest/arrow/array/struct.ArrayData.html#method.validate_full
[Rust Soundness Pledge]: https://raphlinus.github.io/rust/2020/01/18/soundness-pledge.html
[ASF security reporting process]: https://www.apache.org/security/#reporting-a-vulnerability
