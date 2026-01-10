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

# `arrow-memory-size-derive`

[![crates.io](https://img.shields.io/crates/v/arrow-memory-size-derive.svg)](https://crates.io/crates/arrow-memory-size-derive)
[![docs.rs](https://img.shields.io/docsrs/arrow-memory-size-derive.svg)](https://docs.rs/arrow-memory-size-derive/latest/arrow_memory_size_derive/)

Derive macro for the `HeapSize` trait from [`arrow-memory-size`].

[`arrow-memory-size`]: https://crates.io/crates/arrow-memory-size

---

## Install

```toml
[dependencies]
arrow-memory-size = "57.0.0"
arrow-memory-size-derive = "57.0.0"
```

---

## Usage

```rust
use arrow_memory_size::HeapSize;
use arrow_memory_size_derive::HeapSize;

#[derive(HeapSize)]
struct MyStruct {
    name: String,
    data: Vec<u8>,
}

let s = MyStruct {
    name: "test".to_string(),
    data: vec![1, 2, 3],
};
println!("Heap size: {} bytes", s.heap_size());
```

## Field Attributes

- `#[heap_size(ignore)]` — Skip this field (contributes 0 to heap size)
- `#[heap_size(size = N)]` — Use a constant value N
- `#[heap_size(size_fn = path)]` — Call a custom function `fn(&FieldType) -> usize`

See the [`arrow-memory-size` README](../arrow-memory-size/README.md) for full documentation and examples.

---

## License

Licensed under the Apache License, Version 2.0.
