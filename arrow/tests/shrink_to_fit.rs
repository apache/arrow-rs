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

use arrow::{
    array::{Array, ArrayRef, ListArray, PrimitiveArray},
    buffer::OffsetBuffer,
    datatypes::{Field, UInt8Type},
};

/// Test that `shrink_to_fit` frees memory after concatenating a large number of arrays.
#[test]
fn test_shrink_to_fit_after_concat() {
    let array_len = 6_000;
    let num_concats = 100;

    let primitive_array: PrimitiveArray<UInt8Type> = (0..array_len)
        .map(|v| (v % 255) as u8)
        .collect::<Vec<_>>()
        .into();
    let primitive_array: ArrayRef = Arc::new(primitive_array);

    let list_array: ArrayRef = Arc::new(ListArray::new(
        Field::new_list_field(primitive_array.data_type().clone(), false).into(),
        OffsetBuffer::from_lengths([primitive_array.len()]),
        primitive_array.clone(),
        None,
    ));

    // Num bytes allocated globally and by this thread, respectively.
    let (concatenated, _bytes_allocated_globally, bytes_allocated_by_this_thread) =
        memory_use(|| {
            let mut concatenated = concatenate(num_concats, list_array.clone());
            concatenated.shrink_to_fit(); // This is what we're testing!
            dbg!(concatenated.data_type());
            concatenated
        });
    let expected_len = num_concats * array_len;
    assert_eq!(bytes_used(concatenated.clone()), expected_len);
    eprintln!("The concatenated array is {expected_len} B long. Amount of memory used by this thread: {bytes_allocated_by_this_thread} B");

    assert!(
        expected_len <= bytes_allocated_by_this_thread,
        "We must allocate at least as much space as the concatenated array"
    );
    assert!(
        bytes_allocated_by_this_thread <= expected_len + expected_len / 100,
        "We shouldn't have more than 1% memory overhead. In fact, we are using {bytes_allocated_by_this_thread} B of memory for {expected_len} B of data"
    );
}

fn concatenate(num_times: usize, array: ArrayRef) -> ArrayRef {
    let mut concatenated = array.clone();
    for _ in 0..num_times - 1 {
        concatenated = arrow::compute::kernels::concat::concat(&[&*concatenated, &*array]).unwrap();
    }
    concatenated
}

fn bytes_used(array: ArrayRef) -> usize {
    let mut array = array;
    loop {
        match array.data_type() {
            arrow::datatypes::DataType::UInt8 => break,
            arrow::datatypes::DataType::List(_) => {
                let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                array = list.values().clone();
            }
            _ => unreachable!(),
        }
    }

    array.len()
}

// --- Memory tracking ---

use std::{
    alloc::Layout,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

static LIVE_BYTES_GLOBAL: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static LIVE_BYTES_IN_THREAD: AtomicUsize = const { AtomicUsize::new(0)  } ;
}

pub struct TrackingAllocator {
    allocator: std::alloc::System,
}

#[global_allocator]
pub static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator {
    allocator: std::alloc::System,
};

#[allow(unsafe_code)]
// SAFETY:
// We just do book-keeping and then let another allocator do all the actual work.
unsafe impl std::alloc::GlobalAlloc for TrackingAllocator {
    #[allow(clippy::let_and_return)]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY:
        // Just deferring
        let ptr = unsafe { self.allocator.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES_IN_THREAD.with(|bytes| bytes.fetch_add(layout.size(), Relaxed));
            LIVE_BYTES_GLOBAL.fetch_add(layout.size(), Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES_IN_THREAD.with(|bytes| bytes.fetch_sub(layout.size(), Relaxed));
        LIVE_BYTES_GLOBAL.fetch_sub(layout.size(), Relaxed);

        // SAFETY:
        // Just deferring
        unsafe { self.allocator.dealloc(ptr, layout) };
    }

    // No need to override `alloc_zeroed` or `realloc`,
    // since they both by default just defer to `alloc` and `dealloc`.
}

fn live_bytes_local() -> usize {
    LIVE_BYTES_IN_THREAD.with(|bytes| bytes.load(Relaxed))
}

fn live_bytes_global() -> usize {
    LIVE_BYTES_GLOBAL.load(Relaxed)
}

/// Returns `(num_bytes_allocated, num_bytes_allocated_by_this_thread)`.
fn memory_use<R>(run: impl Fn() -> R) -> (R, usize, usize) {
    let used_bytes_start_local = live_bytes_local();
    let used_bytes_start_global = live_bytes_global();
    let ret = run();
    let bytes_used_local = live_bytes_local() - used_bytes_start_local;
    let bytes_used_global = live_bytes_global() - used_bytes_start_global;
    (ret, bytes_used_global, bytes_used_local)
}
