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

# Native Rust implementation of Apache Arrow and Parquet

[![Coverage Status](https://codecov.io/gh/apache/arrow/rust/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/arrow?branch=master)

Welcome to the implementation of Arrow, the popular in-memory columnar format, in [Rust](https://www.rust-lang.org/).

This repo contains the following main components:

| Crate        | Description                                                        | Documentation                      |
| ------------ | ------------------------------------------------------------------ | ---------------------------------- |
| arrow        | Core functionality (memory layout, arrays, low level computations) | [(README)](arrow/README.md)        |
| parquet      | Support for Parquet columnar file format                           | [(README)](parquet/README.md)      |
| arrow-flight | Support for Arrow-Flight IPC protocol                              | [(README)](arrow-flight/README.md) |

There are two related crates in a different repository
| Crate | Description | Documentation |
| ------------ | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| DataFusion | In-memory query engine with SQL support | [(README)](https://github.com/apache/arrow-datafusion/blob/master/README.md) |
| Ballista | Distributed query execution | [(README)](https://github.com/apache/arrow-datafusion/blob/master/ballista/README.md) |

Collectively, these crates support a vast array of functionality for analytic computations in Rust.

For example, you can write an SQL query or a `DataFrame` (using the `datafusion` crate), run it against a parquet file (using the `parquet` crate), evaluate it in-memory using Arrow's columnar format (using the `arrow` crate), and send to another process (using the `arrow-flight` crate).

Generally speaking, the `arrow` crate offers functionality for using Arrow arrays, and `datafusion` offers most operations typically found in SQL, including `join`s and window functions.

You can find more details about each crate in their respective READMEs.

## Arrow Rust Community

The `dev@arrow.apache.org` mailing list serves as the core communication channel for the Arrow community. Instructions for signing up and links to the archives can be found at the [Arrow Community](https://arrow.apache.org/community/) page. All major announcements and communications happen there.

The Rust Arrow community also uses the official [ASF Slack](https://s.apache.org/slack-invite) for informal discussions and coordination. This is
a great place to meet other contributors and get guidance on where to contribute. Join us in the `#arrow-rust` channel.

Unlike other parts of the Arrow ecosystem, the Rust implementation uses [github issues](https://github.com/apache/arrow-rs/issues) as the system of record for new features
and bug fixes and this plays a critical role in the release process.

For design discussions we generally collaborate on Google documents and file a github issue linking to the document.
