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
//! These live in the `arrow` umbrella crate because its `force_validate`
//! feature forwards to `arrow-data/force_validate`, activating the validation
//! gate that the realign-before-validate path must satisfy. The `arrow-array`
//! crate's own `force_validate` feature does not forward to `arrow-data`, so
//! an inline test there cannot reach that gate.

#![cfg(feature = "ffi")]

use arrow_array::Decimal128Array;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::{ArrowError, DataType};

use arrow::ffi::from_ffi_and_data_type;

/// `from_ffi` must realign under-aligned but protocol-legal C Data Interface
/// buffers *before* validating them. Under `force_validate`, importing an
/// 8-byte-aligned (not 16-byte-aligned) `Decimal128` buffer fails with a
/// "Misaligned buffers" error unless `consume` realigns first.
#[test]
fn test_decimal128_under_aligned_round_trip_force_validate() -> Result<(), ArrowError> {
    // An i128 buffer that is 8-aligned but not 16-aligned, modeling an FFI
    // producer that only guarantees the C Data Interface's recommended 8-byte
    // alignment (e.g. arrow-java).
    let aligned = Buffer::from_vec(vec![0_i128, 1_i128, 2_i128]);
    let under_aligned = aligned.slice(8);
    assert_eq!(under_aligned.as_ptr().align_offset(8), 0);
    assert_ne!(under_aligned.as_ptr().align_offset(16), 0);

    // Export the bytes as a `UInt8` array (alignment 1, valid even under
    // `force_validate`) then re-import as `Decimal128`, reproducing an
    // under-aligned buffer arriving over the C Data Interface.
    let producer = ArrayData::builder(DataType::UInt8)
        .len(under_aligned.len())
        .add_buffer(under_aligned)
        .build()?;

    let mut array = FFI_ArrowArray::new(&producer);
    // Re-describe the 32 data bytes as 2 `Decimal128` elements; the data
    // pointer is unchanged and still only 8-aligned.
    array.length = 2;
    array.null_count = 0;

    // SAFETY: 32 bytes = 2 little-endian i128 values, matching
    // `Decimal128(10, 2)` of length 2.
    let imported = unsafe { from_ffi_and_data_type(array, DataType::Decimal128(10, 2)) }?;
    // Import must realign the buffer to arrow-rs's 16-byte `i128` alignment.
    assert_eq!(imported.buffers()[0].as_ptr().align_offset(16), 0);
    let array = Decimal128Array::from(imported);

    // The little-endian byte layout of [0i128, 1, 2] sliced 8 bytes in yields
    // elements `1 << 64` and `2 << 64`.
    assert_eq!(array.len(), 2);
    assert_eq!(array.value(0), 1_i128 << 64);
    assert_eq!(array.value(1), 2_i128 << 64);
    Ok(())
}
