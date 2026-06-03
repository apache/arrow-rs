// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for the C Data Interface import path (`from_ffi`).
//!
//! This file lives in the `arrow` umbrella crate so that the `arrow`
//! crate's `force_validate` feature (which forwards to
//! `arrow-data/force_validate`) actually activates `arrow-data`'s
//! validation gate when the test runs. The `arrow-array` crate's own
//! `force_validate` feature is empty and does NOT forward to
//! `arrow-data`, so an inline `arrow-array` test cannot exercise the
//! realign-before-validate path under any standard CI invocation. CI runs
//! `cargo test -p arrow --features=force_validate,...,ffi` (arrow.yml), which
//! does activate the gate, so this is a CI-reachable regression guard for
//! <https://github.com/apache/arrow-rs/issues/10034>.

#![cfg(feature = "ffi")]

use arrow_array::Decimal128Array;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::{ArrowError, DataType};

use arrow::ffi::from_ffi_and_data_type;

/// Regression test for #10034: `from_ffi` must realign under-aligned but
/// protocol-legal C Data Interface buffers *before* validating them.
///
/// Under `force_validate` (active here via `arrow/force_validate ->
/// arrow-data/force_validate`), `ImportedArrowArray::consume` builds the
/// `ArrayData` via `ArrayDataBuilder::build`, which validates whenever
/// `force_validate` is set. If `consume` does not realign first (the pre-fix
/// behavior of `ArrayData::new_unchecked`), `build` rejects the
/// 8-byte-aligned (not 16-byte-aligned) `Decimal128` buffer with
/// `InvalidArgumentError("Misaligned buffers[0] ...")` before the import can
/// realign it. With the fix, `consume` realigns first and the import
/// succeeds in every feature configuration.
#[test]
fn test_decimal128_under_aligned_round_trip_force_validate() -> Result<(), ArrowError> {
    // Model an FFI producer that only guarantees the C Data Interface's
    // recommended 8-byte alignment (e.g. arrow-java): an i128 data buffer
    // that is 8-aligned but not 16-aligned.
    let aligned = Buffer::from_vec(vec![0_i128, 1_i128, 2_i128]);
    let under_aligned = aligned.slice(8);
    assert_eq!(under_aligned.as_ptr().align_offset(8), 0);
    assert_ne!(under_aligned.as_ptr().align_offset(16), 0);

    // Export the under-aligned bytes as a `UInt8` array (alignment 1, so it is
    // valid Arrow data even under `force_validate`), then re-import them as
    // `Decimal128`. This reproduces an under-aligned `Decimal128` buffer
    // arriving over the C Data Interface without having to construct an
    // (invalid) under-aligned `Decimal128` `ArrayData` on the producer side,
    // which `force_validate` would reject before export.
    let producer = ArrayData::builder(DataType::UInt8)
        .len(under_aligned.len())
        .add_buffer(under_aligned)
        .build()?;

    let mut array = FFI_ArrowArray::new(&producer);
    // Re-describe the exported 32 data bytes as 2 `Decimal128` elements. The
    // data pointer (`buffers[1]`) is unchanged and still only 8-aligned.
    array.length = 2;
    array.null_count = 0;

    // SAFETY: the exported buffer holds 32 bytes = 2 little-endian i128 values;
    // reinterpreting it as `Decimal128(10, 2)` of length 2 matches.
    let imported = unsafe { from_ffi_and_data_type(array, DataType::Decimal128(10, 2)) }?;
    // Import must realign the under-aligned buffer to satisfy arrow-rs's
    // 16-byte `i128` alignment invariant, regardless of `force_validate`.
    assert_eq!(imported.buffers()[0].as_ptr().align_offset(16), 0);
    let array = Decimal128Array::from(imported);

    // The little-endian byte layout of [0i128, 1, 2] sliced 8 bytes in yields
    // elements `1 << 64` and `2 << 64`.
    assert_eq!(array.len(), 2);
    assert_eq!(array.value(0), 1_i128 << 64);
    assert_eq!(array.value(1), 2_i128 << 64);
    Ok(())
}
