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

# `arrow-avro`

[![crates.io](https://img.shields.io/crates/v/arrow-avro.svg)](https://crates.io/crates/arrow-avro)
[![docs.rs](https://img.shields.io/docsrs/arrow-avro.svg)](https://docs.rs/arrow-avro/latest/arrow_avro/)

Transfer data between the [Apache Arrow] memory format and [Apache Avro].

This crate provides:

- a **reader** that decodes Avro
  - **Object Container Files (OCF)**,
  - **Avro Single‑Object Encoding (SOE)**, and
  - **Confluent Schema Registry wire format**  
  into Arrow `RecordBatch`es; and
- a **writer** that encodes Arrow `RecordBatch`es into Avro (**OCF** or **SOE**).

> The latest API docs for `main` (unreleased) are published on the Arrow website: **arrow_avro**.

[Apache Arrow]: https://arrow.apache.org/
[Apache Avro]: https://avro.apache.org/

---

## Install

```toml
[dependencies]
arrow-avro = "57.0.0"
````

Disable defaults and pick only what you need (see **Feature Flags**):

```toml
[dependencies]
arrow-avro = { version = "57.0.0", default-features = false, features = ["deflate", "snappy"] }
```

---

## Quick start

### Read an Avro OCF file into Arrow

```rust
use std::fs::File;
use std::io::BufReader;

use arrow_avro::reader::ReaderBuilder;
use arrow_array::RecordBatch;

fn main() -> anyhow::Result<()> {
    let file = BufReader::new(File::open("data/example.avro")?);
    let mut reader = ReaderBuilder::new().build(file)?;
    while let Some(batch) = reader.next() {
        let batch: RecordBatch = batch?;
        println!("rows: {}", batch.num_rows());
    }
    Ok(())
}
```

### Write Arrow to Avro OCF (in‑memory)

```rust
use std::sync::Arc;

use arrow_avro::writer::AvroWriter;
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

fn main() -> anyhow::Result<()> {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )?;

    let sink: Vec<u8> = Vec::new();
    let mut w = AvroWriter::new(sink, schema)?;
    w.write(&batch)?;
    w.finish()?;
    assert!(!w.into_inner().is_empty());
    Ok(())
}
```

See the crate docs for runnable SOE and Confluent round‑trip examples.

---

## Feature Flags (what they do and when to use them)

### Compression codecs (OCF block compression)

`arrow-avro` supports the Avro‑standard OCF codecs. The **defaults** include all five: `deflate`, `snappy`, `zstd`, `bzip2`, and `xz`.

| Feature   | Default | What it enables                                                     | When to use                                                                                                                            |
|-----------|--------:|---------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `deflate` |       ✅ | DEFLATE compression via `flate2` (pure‑Rust backend)                | Most compatible; widely supported; good compression, slower than Snappy.                                                               |
| `snappy`  |       ✅ | Snappy block compression via `snap` with CRC‑32 as required by Avro | Fastest decode/encode; common in streaming/data‑lake pipelines. (Avro requires a 4‑byte big‑endian CRC of the **uncompressed** block.) |
| `zstd`    |       ✅ | Zstandard block compression via `zstd`                              | Great compression/speed trade‑off on modern systems. May pull in a native library.                                                     |
| `bzip2`   |       ✅ | BZip2 block compression                                             | For compatibility with older datasets that used BZip2. Slower; larger deps.                                                            |
| `xz`      |       ✅ | XZ/LZMA block compression                                           | Highest compression for archival data; slowest; larger deps.                                                                           |

> Avro defines these codecs for OCF: `null` (no compression), `deflate`, `snappy`, `bzip2`, `xz`, and `zstandard` (recent spec versions).

**Notes**

* Only **OCF** uses these codecs (they compress per‑block). They do **not** apply to raw Avro frames used by Confluent wire format or SOE. The crate’s `compression` module is specifically for **OCF blocks**.
* `deflate` uses `flate2` with the `rust_backend` (no system zlib required).

### Schema fingerprints & custom logical type helpers

| Feature                     | Default | What it enables                                                                  | When to use                                                                                                         |    
|-----------------------------|--------:|----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `md5`                       |       ⬜ | `md5` dep for optional **MD5** schema fingerprints                               | If you want to compute MD5 fingerprints of writer schemas (i.e. for custom prefixing/validation).                   |   
| `sha256`                    |       ⬜ | `sha2` dep for optional **SHA‑256** schema fingerprints                          | If you prefer longer fingerprints; affects max prefix length (i.e. when framing).                                   |  
| `small_decimals`            |       ⬜ | Extra handling for **small decimal** logical types (`Decimal32` and `Decimal64`) | If your Avro `decimal` values are small and you want more compact Arrow representations.                            |
| `avro_custom_types`         |       ⬜ | Annotates Avro values using Arrow specific custom logical types                  | Enable when you need arrow-avro to reinterpret certain Avro fields as Arrow types that Avro doesn’t natively model. | 
| `canonical_extension_types` |       ⬜ | Re‑exports Arrow’s canonical extension types support from `arrow-schema`         | Enable if your workflow uses Arrow [canonical extension types] and you want `arrow-avro` to respect them.           | 

[canonical extension types]: https://arrow.apache.org/docs/format/CanonicalExtensions.html

**Lower‑level/internal toggles (rarely used directly)**

* `flate2`, `snap`, `crc`, `zstd`, `bzip2`, `xz` are optional **dependencies** wired to the user‑facing features above. You normally enable `deflate`/`snappy`/`zstd`/`bzip2`/`xz`, not these directly.

### Feature snippets

* Minimal, fast build (common pipelines):

  ```toml
  arrow-avro = { version = "56", default-features = false, features = ["deflate", "snappy"] }
  ```
* Include Zstandard too (modern data lakes):

  ```toml
  arrow-avro = { version = "56", default-features = false, features = ["deflate", "snappy", "zstd"] }
  ```
* Fingerprint helpers:

  ```toml
  arrow-avro = { version = "56", features = ["md5", "sha256"] }
  ```
  
---

## What formats are supported?

* **OCF (Object Container Files)**: self‑describing Avro files with header, optional compression, sync markers; reader and writer supported.
* **Confluent Schema Registry wire format**: 1‑byte magic `0x00` + 4‑byte BE schema ID + Avro body; supports decode + encode helpers.
* **Avro Single‑Object Encoding (SOE)**: 2‑byte magic `0xC3 0x01` + 8‑byte LE CRC‑64‑AVRO fingerprint + Avro body; supports decode + encode helpers.

---

## Examples

* Read/write OCF in memory and from files (see crate docs “OCF round‑trip”).
* Confluent wire‑format and SOE quickstarts are provided as runnable snippets in docs.

There are additional examples under `arrow-avro/examples/` in the repository.

---
