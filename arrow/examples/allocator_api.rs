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

fn main() {
    demo();
}

#[cfg(not(feature = "allocator_api"))]
fn demo() {
    println!("This example requires the `allocator_api` feature to be enabled.");
}

#[cfg(feature = "allocator_api")]
fn demo() {
    use arrow::buffer::MutableBuffer;
    use std::alloc::Global;

    // Creates a mutable buffer with customized allocator
    let mut buffer = MutableBuffer::<u8>::with_capacity_in(10, Global);

    // Inherits allocator from Vec
    let vector = Vec::<u8>::with_capacity_in(100, Global);
    let mut buffer = MutableBuffer::from(vector);
    buffer.reserve(100);
}
