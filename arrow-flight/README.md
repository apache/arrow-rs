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

See the [API documentation](https://docs.rs/arrow_flight/latest) for examples and the full API.

The API documentation for most recent, unreleased code is available [here](https://arrow.apache.org/rust/arrow_flight/index.html).

## Usage

Add this to your Cargo.toml:

```toml
[dependencies]
arrow-flight = "53.2.0"
```

Apache Arrow Flight is a gRPC based protocol for exchanging Arrow data between processes. See the blog post [Introducing Apache Arrow Flight: A Framework for Fast Data Transport](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for more information.

This crate provides a Rust implementation of the
[Flight.proto](../format/Flight.proto) gRPC protocol and
[examples](https://github.com/apache/arrow-rs/tree/master/arrow-flight/examples)
that demonstrate how to build a Flight server implemented with [tonic](https://docs.rs/crate/tonic/latest).

## Feature Flags

- `flight-sql-experimental`: Enables experimental support for
  [Apache Arrow FlightSQL], a protocol for interacting with SQL databases.

- `tls`: Enables `tls` on `tonic`

## CLI

This crates offers a basic [Apache Arrow FlightSQL] command line interface.

The client can be installed from the repository:

```console
$ cargo install --features=cli,flight-sql-experimental,tls --bin=flight_sql_client --path=. --locked
```

The client comes with extensive help text:

```console
$ flight_sql_client help
```

A query can be executed using:

```console
$ flight_sql_client --host example.com statement-query "SELECT 1;"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+
```

[apache arrow flightsql]: https://arrow.apache.org/docs/format/FlightSql.html
