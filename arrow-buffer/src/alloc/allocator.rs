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

use std::{
    alloc::{Allocator, Layout},
    panic::UnwindSafe,
    ptr::NonNull,
};

pub struct AllocatorDeallocation<A: Allocator> {
    ptr: NonNull<u8>,
    layout: Layout,
    allocator: A,
}

impl<A: Allocator + UnwindSafe> UnwindSafe for AllocatorDeallocation<A> {}
unsafe impl<A: Allocator + Send> Send for AllocatorDeallocation<A> {}
unsafe impl<A: Allocator + Sync> Sync for AllocatorDeallocation<A> {}

impl<A: Allocator> Drop for AllocatorDeallocation<A> {
    fn drop(&mut self) {
        unsafe {
            self.allocator.deallocate(self.ptr, self.layout);
        }
    }
}

impl<A: Allocator> AllocatorDeallocation<A> {
    pub fn new(ptr: NonNull<u8>, layout: Layout, allocator: A) -> Self {
        Self {
            ptr,
            layout,
            allocator,
        }
    }
}
