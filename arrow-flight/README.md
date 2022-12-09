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

# Apache Arrow Flight

[![Crates.io](https://img.shields.io/crates/v/arrow-flight.svg)](https://crates.io/crates/arrow-flight)

## Usage

Add this to your Cargo.toml:

```toml
[dependencies]
arrow-flight = "29.0.0"
```

Apache Arrow Flight is a gRPC based protocol for exchanging Arrow data between processes. See the blog post [Introducing Apache Arrow Flight: A Framework for Fast Data Transport](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for more information.

This crate provides a Rust implementation of the [Flight.proto](../../format/Flight.proto) gRPC protocol and provides an example that demonstrates how to build a Flight server implemented with Tonic.
