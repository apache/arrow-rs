// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr::null_mut;
use std::sync::Arc;

use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field};

const ROWS: usize = 8;
const LIST_SIZE: i32 = 8192;
const EXPANDED_BYTES: usize = ROWS * LIST_SIZE as usize / 8;

thread_local! {
    static FAIL_EXPANSION: Cell<bool> = const { Cell::new(false) };
}

struct FailingAllocator;

#[global_allocator]
static ALLOCATOR: FailingAllocator = FailingAllocator;

// SAFETY: Successful allocations and all deallocations are delegated to System.
unsafe impl GlobalAlloc for FailingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == EXPANDED_BYTES && FAIL_EXPANSION.get() {
            return null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn validate_with_failed_expansion(data: &ArrayData) -> Result<(), ArrowError> {
    struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            FAIL_EXPANSION.set(false);
        }
    }

    // Simulate failure of an expanded bitmap allocation on this test thread.
    let _reset = Reset;
    FAIL_EXPANSION.set(true);
    data.validate_full()
}

fn fixed_size_list(child_nulls: Option<NullBuffer>, offset: usize) -> ArrayData {
    // An empty struct has no value buffers, so only the null bitmaps take space.
    let child_type = DataType::Struct(Default::default());
    let child = ArrayData::builder(child_type.clone())
        .len((ROWS + offset) * LIST_SIZE as usize)
        .nulls(child_nulls)
        .build()
        .unwrap();

    ArrayData::builder(DataType::FixedSizeList(
        Arc::new(Field::new("item", child_type, false)),
        LIST_SIZE,
    ))
    .len(ROWS)
    .offset(offset)
    .nulls(Some(NullBuffer::new_null(ROWS)))
    .add_child_data(child)
    .build()
    .unwrap()
}

#[test]
fn test_fixed_size_list_skips_unnecessary_null_expansion() {
    let without_child_nulls = fixed_size_list(None, 0);
    let nulls_outside_slice = fixed_size_list(
        Some(NullBuffer::new(BooleanBuffer::collect_bool(
            (ROWS + 1) * LIST_SIZE as usize,
            |i| i != 0,
        ))),
        1,
    );

    for data in [without_child_nulls, nulls_outside_slice] {
        validate_with_failed_expansion(&data).unwrap();
    }
}

#[test]
fn test_fixed_size_list_null_expansion_allocation_failure() {
    let data = fixed_size_list(Some(NullBuffer::new_null(ROWS * LIST_SIZE as usize)), 0);

    assert!(matches!(
        validate_with_failed_expansion(&data),
        Err(ArrowError::MemoryError(_))
    ));
}
