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

//! Customizing [`Allocator`] for Arrow Array's underlying [`MutableBuffer`].
//!
//! This module requires the `allocator_api` feature and a nightly channel Rust toolchain.

#![cfg_attr(feature = "allocator_api", feature(allocator_api))]

fn main() {
    demo();
}

#[cfg(not(feature = "allocator_api"))]
fn demo() {
    println!("This example requires the `allocator_api` feature to be enabled.");
}

#[cfg(feature = "allocator_api")]
mod allocator {
    /// A simple allocator tracker that records and reports memory usage.
    #[derive(Clone)]
    pub struct AllocatorTracker<A>
    where
        A: std::alloc::Allocator + Clone,
    {
        usage: std::sync::Arc<std::sync::atomic::AtomicIsize>,
        alloc: A,
    }

    impl<A> AllocatorTracker<A>
    where
        A: std::alloc::Allocator + Clone,
    {
        pub fn new(alloc: A) -> Self {
            Self {
                usage: std::sync::Arc::new(std::sync::atomic::AtomicIsize::new(0)),
                alloc,
            }
        }

        pub fn report_usage(&self) -> isize {
            self.usage.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    unsafe impl<A> std::alloc::Allocator for AllocatorTracker<A>
    where
        A: std::alloc::Allocator + Clone,
    {
        fn allocate(
            &self,
            layout: std::alloc::Layout,
        ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
            let size = layout.size();
            self.usage
                .fetch_add(size as isize, std::sync::atomic::Ordering::Relaxed);

            self.alloc.allocate(layout)
        }

        unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
            let size = layout.size();
            self.usage
                .fetch_sub(size as isize, std::sync::atomic::Ordering::Relaxed);

            self.alloc.deallocate(ptr, layout)
        }
    }
}

#[cfg(feature = "allocator_api")]
fn demo() {
    use arrow::buffer::MutableBuffer;
    use std::alloc::Global;

    let allocator_tracker = allocator::AllocatorTracker::new(Global);

    // Creates a mutable buffer with customized allocator
    let mut buffer =
        MutableBuffer::<allocator::AllocatorTracker<std::alloc::Global>>::with_capacity_in(
            10,
            allocator_tracker.clone(),
        );
    println!(
        "Current memory usage: {} bytes",
        allocator_tracker.report_usage()
    );

    // Inherits allocator from Vec
    let vector = Vec::<u8, allocator::AllocatorTracker<std::alloc::Global>>::with_capacity_in(
        100,
        allocator_tracker.clone(),
    );
    let mut buffer = MutableBuffer::from(vector);
    buffer.reserve(100);
    println!(
        "Current memory usage: {} bytes",
        allocator_tracker.report_usage()
    );
}
