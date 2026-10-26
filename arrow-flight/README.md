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
arrow-flight = "54.0.0"
```

Apache Arrow Flight is a gRPC based protocol for exchanging Arrow data between processes. See the blog post [Introducing Apache Arrow Flight: A Framework for Fast Data Transport](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for more information.

This crate provides a Rust implementation of the
[Flight.proto](../format/Flight.proto) gRPC protocol and
[examples](https://github.com/apache/arrow-rs/tree/main/arrow-flight/examples)
that demonstrate how to build a Flight server implemented with [tonic](https://docs.rs/crate/tonic/latest).

## Feature Flags

- `flight-sql`: Support for [Apache Arrow FlightSQL], a protocol for interacting with SQL databases.

You can enable TLS using the following features (not enabled by default)

- `tls-aws-lc`: enables [tonic feature] `tls-aws-lc`
- `tls-native-roots`: enables [tonic feature] `tls-native-roots`
- `tls-ring`: enables [tonic feature] `tls-ring`
- `tls-webpki`: enables [tonic feature] `tls-webpki-roots`

[tonic feature]: https://docs.rs/tonic/latest/tonic/#feature-flags

## CLI

This crates offers a basic [Apache Arrow FlightSQL] command line interface.

The client can be installed from the repository:

```console
$ cargo install --features=cli,flight-sql,tls --bin=flight_sql_client --path=. --locked
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

## Running the Flight SQL Example

The [Flight SQL server example](examples/flight_sql_server.rs) implements the
`GetFlightInfo` and `DoGet` requests needed to list database schemas. From the
repository root, start the server with:

```console
$ cargo run -p arrow-flight --example flight_sql_server --features flight-sql,tls-ring
Listening on 0.0.0.0:50051
```

In another terminal, use the bundled client to list schemas:

```console
$ cargo run -p arrow-flight --bin flight_sql_client --features cli,flight-sql,tls-ring -- \
    --host localhost --port 50051 db-schemas flight_sql --db-schema-filter '%'
+--------------+----------------+
| catalog_name | db_schema_name |
+--------------+----------------+
| flight_sql   | example        |
+--------------+----------------+
```

This request has two steps:

1. `get_flight_info_schemas` returns a `FlightInfo` containing the result
   schema and an endpoint ticket. The example uses `FlightInfo::try_with_schema`
   to encode the schema in the format required by the Flight protocol.
2. The client passes the ticket to `do_get_schemas`, which builds the metadata
   `RecordBatch` and uses `FlightDataEncoderBuilder` to produce a correctly
   framed stream of `FlightData` messages.

`FlightData.data_header` and `FlightData.data_body` are IPC message components,
not arbitrary schema and IPC stream bytes. Constructing them directly, for
example by putting `Schema::to_string()` in `data_header`, produces invalid
messages. Use `FlightInfo::try_with_schema` for `FlightInfo` schemas and
`FlightDataEncoderBuilder` for `DoGet` response streams.

## Security

See the [Security Policy] for information on the security model and how to report vulnerabilities.

[apache arrow flightsql]: https://arrow.apache.org/docs/format/FlightSql.html
[security policy]: https://github.com/apache/arrow-rs/blob/main/SECURITY.md
