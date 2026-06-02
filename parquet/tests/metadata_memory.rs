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

//! Tests calculation of the Parquet metadata heap size

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::file::metadata::PageIndexPolicy;
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct TrackingAllocator {
    pub bytes_allocated: AtomicUsize,
}

impl TrackingAllocator {
    pub const fn new() -> Self {
        Self {
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}

impl Default for TrackingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.bytes_allocated
            .fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.bytes_allocated
            .fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

#[test]
fn test_metadata_heap_memory() {
    // Run test cases sequentially so that heap allocations
    // are restricted to a single test case at a time.
    let test_data = arrow::util::test_util::parquet_test_data();
    let reader_options =
        ArrowReaderOptions::default().with_page_index_policy(PageIndexPolicy::Required);

    {
        let path = format!("{test_data}/alltypes_dictionary.parquet");
        verify_metadata_heap_memory(&path, || reader_options.clone());
    }

    {
        // Calculated heap size doesn't match exactly, possibly due to extra overhead not accounted
        // for in the HeapSize implementation for parquet::data_type::ByteArray.
        let path = format!("{test_data}/alltypes_tiny_pages_plain.parquet");
        verify_metadata_heap_memory(&path, || reader_options.clone());
    }

    {
        let path = format!("{test_data}/data_index_bloom_encoding_with_length.parquet");
        verify_metadata_heap_memory(&path, || reader_options.clone());
    }

    {
        let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");

        let footer_key = b"0123456789012345";
        let column_1_key = b"1234567890123450";
        let column_2_key = b"1234567890123451";

        // Delay creating the FileDecryptionProperties as their heap memory is included
        // in the heap size calculation.
        let get_options = || {
            let decryption_properties = FileDecryptionProperties::builder(footer_key.into())
                .with_column_key("double_field", column_1_key.into())
                .with_column_key("float_field", column_2_key.into())
                .build()
                .unwrap();
            reader_options
                .clone()
                .with_file_decryption_properties(decryption_properties)
        };

        verify_metadata_heap_memory(&path, get_options);
    }
}

fn verify_metadata_heap_memory<F>(path: &str, get_options: F)
where
    F: FnOnce() -> ArrowReaderOptions,
{
    let input_file = File::open(path).unwrap();

    let baseline = ALLOCATOR.bytes_allocated();

    let options = get_options();
    let reader_metadata = ArrowReaderMetadata::load(&input_file, options).unwrap();
    let metadata = Arc::clone(reader_metadata.metadata());
    drop(reader_metadata);

    let metadata_heap_size = metadata.memory_size();

    let arc_overhead = std::mem::size_of::<usize>() * 2;
    let allocated = ALLOCATOR.bytes_allocated() - baseline - arc_overhead;
    black_box(metadata);

    assert!(metadata_heap_size > 0);
    // Allow for some tolerance in the calculated heap size as this can be platform
    // dependant and not always exact.
    let rel_tol = 0.025;
    let min_size = ((allocated as f64) * (1.0 - rel_tol)) as usize;
    let max_size = ((allocated as f64) * (1.0 + rel_tol)) as usize;
    assert!(
        metadata_heap_size >= min_size && metadata_heap_size <= max_size,
        "Calculated heap size {} doesn't match the allocated size {} within a relative tolerance of {} for file {}",
        metadata_heap_size,
        allocated,
        rel_tol,
        path
    );
}
