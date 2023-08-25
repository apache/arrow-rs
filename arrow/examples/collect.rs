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

//! `FromIterator` API is implemented for different array types to easily create them
//! from values.

use arrow::array::Array;
use arrow_array::types::Int32Type;
use arrow_array::{Float32Array, Int32Array, Int8Array, ListArray};

fn main() {
    // Primitive Arrays
    //
    // Primitive arrays are arrays of fixed-width primitive types (u8, u16, u32,
    // u64, i8, i16, i32, i64, f32, f64, etc.)

    // Create an Int8Array with 4 values
    let array: Int8Array = vec![1, 2, 3, 4].into_iter().collect();
    println!("{array:?}");

    // Arrays can also be built from `Vec<Option<T>>`. `None`
    // represents a null value in the array.
    let array: Int8Array = vec![Some(1_i8), Some(2), None, Some(3)]
        .into_iter()
        .collect();
    println!("{array:?}");
    assert!(array.is_null(2));

    let array: Float32Array = [Some(1.0_f32), Some(2.3), None].into_iter().collect();
    println!("{array:?}");
    assert_eq!(array.value(0), 1.0_f32);
    assert_eq!(array.value(1), 2.3_f32);
    assert!(array.is_null(2));

    // Although not implementing `FromIterator`, ListArrays provides `from_iter_primitive`
    // function to create ListArrays from `Vec<Option<Vec<Option<T>>>>`. The outer `None`
    // represents a null list, the inner `None` represents a null value in a list.
    let data = vec![
        Some(vec![]),
        None,
        Some(vec![Some(3), None, Some(5), Some(19)]),
        Some(vec![Some(6), Some(7)]),
    ];
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

    assert!(!list_array.is_valid(1));

    let list0 = list_array.value(0);
    let list2 = list_array.value(2);
    let list3 = list_array.value(3);

    assert_eq!(
        &[] as &[i32],
        list0
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
    );
    assert!(!list2
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .is_valid(1));
    assert_eq!(
        &[6, 7],
        list3
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
    );
}
