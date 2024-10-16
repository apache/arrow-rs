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

## Developer's guide

Common information for all Rust libraries in this project, including
testing, code formatting, and lints, can be found in the main Arrow
Rust [README.md](../README.md).

Please refer to [lib.rs](src/lib.rs) for an introduction to this
specific crate and its current functionality.

## Guidelines in usage of `unsafe`

[`unsafe`](https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html) has a high maintenance cost because debugging and testing it is difficult, time consuming, often requires external tools (e.g. `valgrind`), and requires a higher-than-usual attention to details. Undefined behavior is particularly difficult to identify and test, and usage of `unsafe` is the [primary cause of undefined behavior](https://doc.rust-lang.org/reference/behavior-considered-undefined.html) in a program written in Rust.
For two real world examples of where `unsafe` has consumed time in the past in this project see [#8545](https://github.com/apache/arrow/pull/8645) and [8829](https://github.com/apache/arrow/pull/8829)
This crate only accepts the usage of `unsafe` code upon careful consideration, and strives to avoid it to the largest possible extent.

### When can `unsafe` be used?

Generally, `unsafe` should only be used when a `safe` counterpart is not available and there is no `safe` way to achieve additional performance in that area. The following is a summary of the current components of the crate that require `unsafe`:

- alloc, dealloc and realloc of buffers along cache lines
- Interpreting bytes as certain rust types, for access, representation and compute
- Foreign interfaces (C data interface)
- Inter-process communication (IPC)
- SIMD
- Performance (e.g. omit bounds checks, use of pointers to avoid bound checks)

#### cache-line aligned memory management

The arrow format recommends storing buffers aligned with cache lines, and this crate adopts this behavior.
However, Rust's global allocator does not allocate memory aligned with cache-lines. As such, many of the low-level operations related to memory management require `unsafe`.

#### Interpreting bytes

The arrow format is specified in bytes (`u8`), which can be logically represented as certain types
depending on the `DataType`.
For many operations, such as access, representation, numerical computation and string manipulation,
it is often necessary to interpret bytes as other physical types (e.g. `i32`).

Usage of `unsafe` for the purpose of interpreting bytes in their corresponding type (according to the arrow specification) is allowed. Specifically, the pointer to the byte slice must be aligned to the type that it intends to represent and the length of the slice is a multiple of the size of the target type of the transmutation.

#### FFI

The arrow format declares an ABI for zero-copy from and to libraries that implement the specification
(foreign interfaces). In Rust, receiving and sending pointers via FFI requires usage of `unsafe` due to
the impossibility of the compiler to derive the invariants (such as lifetime, null pointers, and pointer alignment) from the source code alone as they are part of the FFI contract.

#### IPC

The arrow format declares a IPC protocol, which this crate supports. IPC is equivalent to a FFI in that the rust compiler can't reason about the contract's invariants.

#### Performance

Some operations are significantly faster when `unsafe` is used.

A common usage of `unsafe` is to offer an API to access the `i`th element of an array (e.g. `UInt32Array`).
This requires accessing the values buffer e.g. `array.buffers()[0]`, picking the slice
`[i * size_of<i32>(), (i + 1) * size_of<i32>()]`, and then transmuting it to `i32`. In safe Rust,
this operation requires boundary checks that are detrimental to performance.

Usage of `unsafe` for performance reasons is justified only when all other alternatives have been exhausted and the performance benefits are sufficiently large (e.g. >~10%).

### Considerations when introducing `unsafe`

Usage of `unsafe` in this crate _must_:

- not expose a public API as `safe` when there are necessary invariants for that API to be defined behavior.
- have code documentation for why `safe` is not used / possible
- have code documentation about which invariant the user needs to enforce to ensure [soundness](https://rust-lang.github.io/unsafe-code-guidelines/glossary.html#soundness-of-code--of-a-library), or which
- invariant is being preserved.
- if applicable, use `debug_assert`s to relevant invariants (e.g. bound checks)

Example of code documentation:

```rust
// JUSTIFICATION
//  Benefit
//      Describe the benefit of using unsafe. E.g.
//      "30% performance degradation if the safe counterpart is used, see bench X."
//  Soundness
//      Describe why the code remains sound (according to the definition of rust's unsafe code guidelines). E.g.
//      "We bounded check these values at initialization and the array is immutable."
let ... = unsafe { ... };
```

When adding this documentation to existing code that is not sound and cannot trivially be fixed, we should file
specific JIRA issues and reference them in these code comments. For example:

```rust
//  Soundness
//      This is not sound because .... see https://issues.apache.org/jira/browse/ARROW-nnnnn
```

### Usage of SIMD / auto vectorization

This crate does not use SIMD intrinsics (e.g. [`std::simd`]) directly, but
instead relies on the Rust compiler's auto-vectorization capabilities, which are
built on LLVM.

SIMD intrinsics are difficult to maintain and can be difficult to reason about.
The auto-vectorizer in LLVM is quite good and often produces kernels that are
faster than using hand-written SIMD intrinsics. This crate used to contain
several kernels with hand-written SIMD instructions, which were removed after
discovering the auto-vectorized code was faster.

[`std::simd`]: https://doc.rust-lang.org/std/simd/index.html

#### Tips for auto vectorization

LLVM is relatively good at vectorizing vertical operations provided:

1. No conditionals within the loop body (e.g no checking for nulls on each row)
2. Not too much inlining (judicious use of `#[inline]` and `#[inline(never)]`) as the vectorizer gives up if the code is too complex
3. No [horizontal reductions] or data dependencies
4. Suitable SIMD instructions available in the target ISA (e.g. `target-cpu` `RUSTFLAGS` flag)

[horizontal reductions]: https://rust-lang.github.io/packed_simd/perf-guide/vert-hor-ops.html

The last point is especially important as the default `target-cpu` doesn't
support many SIMD instructions. See the Performance Tips section at the
end of <https://crates.io/crates/arrow>

To ensure your code is fully vectorized, we recommend using tools like
<https://rust.godbolt.org/> (again being sure `RUSTFLAGS` is set appropriately)
to analyze the resulting code, and only once you've exhausted auto vectorization
think of reaching for manual SIMD. Generally the hard part of vectorizing code
is structuring the algorithm in such a way that it can be vectorized, regardless
of what generates those instructions.

# Releases and publishing to crates.io

Please see the [release](../dev/release/README.md) for details on how to create arrow releases
