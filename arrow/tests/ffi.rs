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

//! C Data Interface tests that need `arrow-data`'s validation active.
//!
//! `arrow-array`'s own `force_validate` feature does not reach `arrow-data`,
//! and `cargo test -p arrow-array` never runs with `arrow-data/force_validate`,
//! so these tests live here: the `arrow` crate's `force_validate` forwards to
//! `arrow-data`, and CI runs `cargo test -p arrow --features force_validate,ffi`.

#![cfg(feature = "ffi")]

use arrow::array::{ArrayData, Decimal128Array};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::ffi::{FFI_ArrowArray, from_ffi_and_data_type};

/// An FFI producer may hand over a `Decimal128` buffer with only the C Data
/// Interface's recommended 8-byte alignment, not the 16-byte alignment arrow-rs
/// needs for `i128`. The import must realign it rather than reject it, even
/// under `force_validate`. Regression test for #10034.
#[test]
fn test_decimal128_under_aligned_import() {
    // The under-aligned bytes are carried by a `FixedSizeBinary(16)` array,
    // which only needs 1-byte alignment and so stays valid under
    // `force_validate`, then imported as `Decimal128`.
    let aligned = Buffer::from_vec(vec![0_i128, 1_i128, 2_i128]);
    let under_aligned = aligned.slice(8);
    assert_eq!(under_aligned.as_ptr().align_offset(8), 0);
    assert_ne!(under_aligned.as_ptr().align_offset(16), 0);

    let data = ArrayData::builder(DataType::FixedSizeBinary(16))
        .len(2)
        .add_buffer(under_aligned)
        .build()
        .unwrap();

    let array = FFI_ArrowArray::new(&data);
    let imported = unsafe { from_ffi_and_data_type(array, DataType::Decimal128(10, 2)) }.unwrap();
    let array = Decimal128Array::from(imported);

    // [0i128, 1, 2] sliced 8 bytes in yields `1 << 64` and `2 << 64`.
    assert_eq!(array.len(), 2);
    assert_eq!(array.value(0), 1_i128 << 64);
    assert_eq!(array.value(1), 2_i128 << 64);
}
