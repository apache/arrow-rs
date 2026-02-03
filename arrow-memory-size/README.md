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

# `arrow-memory-size`

[![crates.io](https://img.shields.io/crates/v/arrow-memory-size.svg)](https://crates.io/crates/arrow-memory-size)
[![docs.rs](https://img.shields.io/docsrs/arrow-memory-size.svg)](https://docs.rs/arrow-memory-size/latest/arrow_memory_size/)

Memory size estimation utilities for [Apache Arrow].

This crate provides the `HeapSize` trait for calculating heap memory usage of data structures.

[Apache Arrow]: https://arrow.apache.org/

## Why This Crate?

Several memory size estimation crates exist in the Rust ecosystem ([deepsize], [get-size2], etc.), but none has emerged as a clear standard. Rather than take a dependency on any of them, this crate provides a minimal `HeapSize` trait with a small API surface that can be implemented across the Arrow ecosystem.

Key motivations:

- **Minimal API**: Just two methods (`heap_size()` and `total_size()`)
- **Customizable semantics**: Behavior around `Arc`/`Rc` deduplication varies between crates and use cases; having our own trait allows us to make decisions appropriate for Arrow's needs
- **Arrow integration**: Implementations for all Arrow buffer and array types

[deepsize]: https://github.com/Aeledfyr/deepsize
[get-size2]: https://github.com/bircni/get-size2

## Crate Structure

- **`arrow-memory-size`**: Core trait + standard library implementations
- **`arrow-memory-size-derive`**: `#[derive(HeapSize)]` proc macro
- **`arrow-buffer`**: Implements `HeapSize` for buffer types (`Buffer`, `ScalarBuffer`, etc.)
- **`arrow-array`**: Implements `HeapSize` for array types (`PrimitiveArray`, `StringArray`, etc.)
- **`arrow`**: Re-exports `HeapSize` via `arrow::util::HeapSize` and `arrow::util::HeapSizeDerive`

---

## Install

If you already depend on `arrow`, the trait and derive macro are re-exported:

```rust
use arrow::util::{HeapSize, HeapSizeDerive};
```

Otherwise, add the crates directly:

```toml
[dependencies]
arrow-memory-size = "57.0.0"
arrow-memory-size-derive = "57.0.0"  # Optional, for derive macro
```

---

## Quick Start

### Basic Usage

```rust
use arrow_memory_size::HeapSize;

let v: Vec<String> = vec!["hello".to_string(), "world".to_string()];
let heap_bytes = v.heap_size();   // Only heap allocations
let total_bytes = v.total_size(); // Stack + heap
```

### Derive Macro

```rust
use arrow_memory_size::HeapSize;
use arrow_memory_size_derive::HeapSize;

#[derive(HeapSize)]
struct MyStruct {
    name: String,
    data: Vec<u8>,
    count: i32,
}

let s = MyStruct {
    name: "test".to_string(),
    data: vec![1, 2, 3],
    count: 42,
};
println!("Heap size: {} bytes", s.heap_size());
```

### Derive Macro Attributes

The derive macro supports field attributes for customization:

```rust,ignore
use arrow_memory_size::HeapSize;
use arrow_memory_size_derive::HeapSize;

fn custom_size(data: &ExternalType) -> usize {
    data.len() * 8
}

#[derive(HeapSize)]
struct MyStruct {
    // Skip this field (contributes 0)
    #[heap_size(ignore)]
    cached: u64,

    // Use a constant value
    #[heap_size(size = 1024)]
    fixed_buffer: *const u8,

    // Use a custom function
    #[heap_size(size_fn = custom_size)]
    external: ExternalType,
}
```

**Note:** The derive macro emits a compile error if any field contains `Arc` or `Rc` types, unless the field is marked with `#[heap_size(ignore)]`. This is intentional—shared reference semantics are complex and vary by use case, so they should be handled explicitly.

---

## Supported Types

### Standard Library (this crate)

| Type | Notes |
|------|-------|
| Primitives | `bool`, `i8`-`i128`, `u8`-`u128`, `f32`, `f64` — always 0 |
| `String` | Reports capacity |
| `Vec<T>` | Capacity × element size + nested heap |
| `HashMap<K, V>` | Approximation based on hashbrown internals |
| `HashSet<T>` | Approximation based on hashbrown internals |
| `BTreeMap<K, V>` | Approximation with node overhead |
| `BTreeSet<T>` | Approximation with node overhead |
| `Box<T>` | Size of T + nested heap |
| `Arc<T>` | Reference counts + size of T + nested heap |
| `Option<T>` | Nested heap if `Some` |
| Tuples | Up to 12 elements |
| Arrays `[T; N]` | Sum of element heap sizes |
| `Mutex<T>` | Uses `try_lock()`, returns 0 if locked |
| `RwLock<T>` | Uses `try_read()`, returns 0 if locked |

### Arrow Buffer Types (arrow-buffer crate)

| Type | Notes |
|------|-------|
| `Buffer` | Reports capacity |
| `ScalarBuffer<T>` | Reports inner buffer capacity |
| `OffsetBuffer<T>` | Reports inner buffer capacity |
| `NullBuffer` | Reports buffer capacity |
| `BooleanBuffer` | Reports inner buffer capacity |

### Arrow Array Types (arrow-array crate)

All array types delegate to `get_buffer_memory_size()`:

- `PrimitiveArray`, `BooleanArray`, `NullArray`
- `StringArray`, `LargeStringArray`, `StringViewArray`
- `BinaryArray`, `LargeBinaryArray`, `BinaryViewArray`
- `ListArray`, `LargeListArray`, `ListViewArray`, `FixedSizeListArray`
- `StructArray`, `MapArray`
- `UnionArray` (sparse and dense)
- `DictionaryArray`, `RunArray`

---

## Arc/Rc Handling

This crate counts `Arc<T>` and `Rc<T>` fully each time they appear—shared references will be counted multiple times. This is a deliberate choice: deduplication requires threading a context/tracker through all calls, which adds API complexity and may not be the right tradeoff for all use cases.

The derive macro enforces explicit handling by emitting compile errors for `Arc`/`Rc` fields unless they're marked with `#[heap_size(ignore)]`.

---

## Examples

### Measuring Arrow Arrays

```rust,ignore
use arrow_memory_size::HeapSize;
use arrow_array::{Int32Array, StringArray};

let int_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
println!("Int32Array heap: {} bytes", int_array.heap_size());

let string_array = StringArray::from(vec!["hello", "world"]);
println!("StringArray heap: {} bytes", string_array.heap_size());
```

### Complex Nested Structures

```rust,ignore
use arrow_memory_size::HeapSize;
use arrow_memory_size_derive::HeapSize;
use std::collections::HashMap;

#[derive(HeapSize)]
struct CacheEntry {
    key: String,
    data: Vec<u8>,
    metadata: HashMap<String, String>,
}

#[derive(HeapSize)]
struct Cache {
    entries: Vec<CacheEntry>,
    #[heap_size(ignore)]
    stats: CacheStats,  // Don't count internal bookkeeping
}
```

---

## License

Licensed under the Apache License, Version 2.0.
