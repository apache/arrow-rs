<!---
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
  SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# `arrow-encoded`

Experimental encoded in-memory arrays for [Apache Arrow]. An encoded array has a logical type, such as `Float64`, and stores its values in a physical encoding, such as ALP, that is decoded to a plain Arrow array on demand.

This crate is experimental: its API may change in any release.

See the [main repository README] and the [API documentation] for more details.

## Security

See the [Security Policy] for information on the security model and how to report vulnerabilities.

[Apache Arrow]: https://arrow.apache.org/
[main repository README]: https://github.com/apache/arrow-rs
[API documentation]: https://docs.rs/arrow-encoded/latest
[Security Policy]: https://github.com/apache/arrow-rs/blob/main/SECURITY.md
