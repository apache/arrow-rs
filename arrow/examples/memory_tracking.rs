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

//! Example demonstrating the Array memory tracking functionality

use arrow_array::{Array, Int32Array, ListArray};
use arrow_buffer::{MemoryPool, TrackingMemoryPool};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

fn main() {
    let pool = TrackingMemoryPool::default();

    println!("Arrow Array Memory Tracking Example");
    println!("===================================");

    // Basic array memory tracking
    let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    array.claim(&pool);
    println!("Int32Array (5 elements): {} bytes", pool.used());

    // Nested array (recursive tracking)
    let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2, 4].into());
    let field = Arc::new(Field::new("item", DataType::Int32, false));
    let list_array = ListArray::new(field, offsets, Arc::new(array), None);

    let before_list = pool.used();
    list_array.claim(&pool);
    let after_list = pool.used();
    println!("ListArray (nested): +{} bytes", after_list - before_list);

    // No double-counting for derived arrays
    let large_array = Int32Array::from((0..1000).collect::<Vec<i32>>());
    large_array.claim(&pool);
    let original_usage = pool.used();
    println!("Original array (1000 elements): {original_usage} bytes");

    // Create and claim slices - should not increase memory usage
    let slice1 = large_array.slice(0, 100);
    let slice2 = large_array.slice(500, 200);

    slice1.claim(&pool);
    slice2.claim(&pool);
    let final_usage = pool.used();

    println!("After claiming 2 slices: {final_usage} bytes");
    println!(
        "Increase: {} bytes (slices share the same buffer!)",
        final_usage - original_usage
    );
}
