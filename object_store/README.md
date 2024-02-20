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

# Rust Object Store

A focused, easy to use, idiomatic, high performance, `async` object
store library for interacting with object stores.

Using this crate, the same binary and code can easily run in multiple
clouds and local test environments, via a simple runtime configuration
change. Supported object stores include:

* [AWS S3](https://aws.amazon.com/s3/)
* [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)
* [Google Cloud Storage](https://cloud.google.com/storage)
* Local files
* Memory
* [HTTP/WebDAV Storage](https://datatracker.ietf.org/doc/html/rfc2518)
* Custom implementations

Originally developed by [InfluxData](https://www.influxdata.com/) and later donated to [Apache Arrow](https://arrow.apache.org/).

See [docs.rs](https://docs.rs/object_store) for usage instructions

## Support for `wasm32-unknown-unknown` target

It's possible to build `object_store` for the `wasm32-unknown-unknown` target, however the cloud storage features `aws`, `azure`, `gcp`, and `http` are not supported.

```
cargo build -p object_store --target wasm32-unknown-unknown
```