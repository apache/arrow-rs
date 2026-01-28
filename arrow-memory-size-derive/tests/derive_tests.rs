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

//! Integration tests for the HeapSize derive macro

use arrow_memory_size::HeapSize;
use arrow_memory_size_derive::HeapSize;

// =============================================================================
// Basic derive test structures
// =============================================================================

/// Test struct with named fields
#[derive(HeapSize)]
struct HeapSizeNamedFields {
    name: String,
    data: Vec<u8>,
    count: i32,
    optional: Option<String>,
}

/// Test tuple struct
#[derive(HeapSize)]
struct HeapSizeTuple(String, Vec<u8>, i32);

/// Test unit struct
#[derive(HeapSize)]
struct HeapSizeUnit;

/// Test empty struct with named fields
#[derive(HeapSize)]
struct HeapSizeEmpty {}

/// Test empty tuple struct
#[derive(HeapSize)]
struct HeapSizeEmptyTuple();

/// Test enum with various variant types
#[derive(HeapSize)]
enum HeapSizeEnum {
    Unit,
    Tuple(String, Vec<u8>),
    Named { name: String, value: i32 },
}

/// Test generic struct
#[derive(HeapSize)]
struct HeapSizeGeneric<T> {
    value: T,
    items: Vec<T>,
}

/// Test struct with Box
#[derive(HeapSize)]
struct HeapSizeWithBox {
    boxed: Box<String>,
}

/// Test struct with nested containers
#[derive(HeapSize)]
struct HeapSizeNested {
    data: Vec<Vec<String>>,
    map: std::collections::HashMap<String, Vec<u8>>,
}

// =============================================================================
// Basic derive tests
// =============================================================================

#[test]
fn test_heap_size_named_fields() {
    let s = HeapSizeNamedFields {
        name: "hello".to_string(),
        data: vec![1, 2, 3, 4, 5],
        count: 42,
        optional: Some("world".to_string()),
    };

    let size = s.heap_size();
    // Should include: String capacity + Vec capacity + Option<String> capacity
    // "hello" = 5 bytes, vec = 5 bytes, "world" = 5 bytes
    assert!(size >= 15, "heap_size should be at least 15, got {}", size);
}

#[test]
fn test_heap_size_tuple_struct() {
    let s = HeapSizeTuple("test".to_string(), vec![1, 2, 3], 0);
    let size = s.heap_size();
    // "test" = 4 bytes + vec = 3 bytes
    assert!(size >= 7, "heap_size should be at least 7, got {}", size);
}

#[test]
fn test_heap_size_unit_struct() {
    let s = HeapSizeUnit;
    assert_eq!(s.heap_size(), 0);
}

#[test]
fn test_heap_size_empty_struct() {
    let s = HeapSizeEmpty {};
    assert_eq!(s.heap_size(), 0);
}

#[test]
fn test_heap_size_empty_tuple() {
    let s = HeapSizeEmptyTuple();
    assert_eq!(s.heap_size(), 0);
}

#[test]
fn test_heap_size_enum_unit() {
    let e = HeapSizeEnum::Unit;
    assert_eq!(e.heap_size(), 0);
}

#[test]
fn test_heap_size_enum_tuple() {
    let e = HeapSizeEnum::Tuple("hello".to_string(), vec![1, 2, 3]);
    let size = e.heap_size();
    assert!(size >= 8, "heap_size should be at least 8, got {}", size);
}

#[test]
fn test_heap_size_enum_named() {
    let e = HeapSizeEnum::Named {
        name: "test".to_string(),
        value: 42,
    };
    let size = e.heap_size();
    // "test" = 4 bytes, i32 = 0 bytes on heap
    assert!(size >= 4, "heap_size should be at least 4, got {}", size);
}

#[test]
fn test_heap_size_generic() {
    let s = HeapSizeGeneric {
        value: "hello".to_string(),
        items: vec!["a".to_string(), "bb".to_string(), "ccc".to_string()],
    };
    let size = s.heap_size();
    // "hello" = 5, vec has 3 strings with capacity for 3 + "a"(1) + "bb"(2) + "ccc"(3)
    assert!(size >= 11, "heap_size should be at least 11, got {}", size);
}

#[test]
fn test_heap_size_with_box() {
    let s = HeapSizeWithBox {
        boxed: Box::new("hello".to_string()),
    };
    let size = s.heap_size();
    // Box overhead (size of String) + String heap allocation
    assert!(size > 0, "heap_size should be > 0, got {}", size);
}

#[test]
fn test_heap_size_nested() {
    let s = HeapSizeNested {
        data: vec![
            vec!["a".to_string()],
            vec!["b".to_string(), "c".to_string()],
        ],
        map: std::collections::HashMap::new(),
    };
    let size = s.heap_size();
    assert!(size > 0, "heap_size should be > 0, got {}", size);
}

#[test]
fn test_total_size() {
    let s = HeapSizeUnit;
    let total = s.total_size();
    assert_eq!(total, std::mem::size_of::<HeapSizeUnit>());
}

// =============================================================================
// Derive macro attribute tests
// =============================================================================

/// Test #[heap_size(ignore)] attribute
#[derive(HeapSize)]
struct WithIgnore {
    data: String,
    #[heap_size(ignore)]
    _ignored: Vec<u8>,
}

#[test]
fn test_heap_size_ignore_attribute() {
    let s = WithIgnore {
        data: "hello".to_string(),
        _ignored: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], // 10 bytes, but ignored
    };
    let size = s.heap_size();
    // Should only count the String, not the Vec
    assert!(
        size >= 5,
        "heap_size should be at least 5 for 'hello', got {}",
        size
    );
    // Should be less than if we counted the Vec too
    assert!(
        size < 20,
        "heap_size should not include ignored Vec, got {}",
        size
    );
}

/// Test #[heap_size(size = N)] attribute
#[derive(HeapSize)]
struct WithConstantSize {
    data: String,
    #[heap_size(size = 1024)]
    fixed: u64, // primitives normally have 0 heap size
}

#[test]
fn test_heap_size_constant_attribute() {
    let s = WithConstantSize {
        data: "hello".to_string(),
        fixed: 42,
    };
    let size = s.heap_size();
    // Should include 5 bytes for "hello" + 1024 constant
    assert!(
        size >= 1029,
        "heap_size should be at least 1029, got {}",
        size
    );
}

/// Custom function for size_fn attribute test
fn custom_size_fn(v: &Vec<u8>) -> usize {
    v.len() * 100 // Deliberately different from actual size
}

/// Test #[heap_size(size_fn = path)] attribute
#[derive(HeapSize)]
struct WithSizeFn {
    data: String,
    #[heap_size(size_fn = custom_size_fn)]
    custom: Vec<u8>,
}

#[test]
fn test_heap_size_size_fn_attribute() {
    let s = WithSizeFn {
        data: "hello".to_string(),
        custom: vec![1, 2, 3], // 3 elements * 100 = 300
    };
    let size = s.heap_size();
    // Should include 5 bytes for "hello" + 300 from custom_size_fn
    assert!(
        size >= 305,
        "heap_size should be at least 305, got {}",
        size
    );
}

/// Test #[heap_size(ignore)] allows Arc fields
#[derive(HeapSize)]
struct WithIgnoredArc {
    data: String,
    #[heap_size(ignore)]
    shared: std::sync::Arc<String>,
}

#[test]
fn test_heap_size_ignored_arc() {
    let s = WithIgnoredArc {
        data: "hello".to_string(),
        shared: std::sync::Arc::new("world".to_string()),
    };
    let size = s.heap_size();
    // Should only count the data String, Arc is ignored
    assert!(size >= 5, "heap_size should be at least 5, got {}", size);
}

/// Test enum with attributes
#[derive(HeapSize)]
#[allow(dead_code)]
enum EnumWithAttributes {
    Normal(String),
    WithIgnored {
        data: String,
        #[heap_size(ignore)]
        ignored: Vec<u8>,
    },
    WithConstant {
        #[heap_size(size = 500)]
        fixed: u8,
    },
}

#[test]
fn test_enum_with_ignore_attribute() {
    let e = EnumWithAttributes::WithIgnored {
        data: "test".to_string(),
        ignored: vec![1, 2, 3, 4, 5],
    };
    let size = e.heap_size();
    // Should only count "test" (4 bytes)
    assert!(size >= 4, "heap_size should be at least 4, got {}", size);
    assert!(
        size < 15,
        "heap_size should not include ignored Vec, got {}",
        size
    );
}

#[test]
fn test_enum_with_constant_attribute() {
    let e = EnumWithAttributes::WithConstant { fixed: 42 };
    let size = e.heap_size();
    assert_eq!(size, 500, "heap_size should be exactly 500, got {}", size);
}

/// Test tuple struct with attributes
#[derive(HeapSize)]
struct TupleWithAttributes(
    String,
    #[heap_size(ignore)] Vec<u8>,
    #[heap_size(size = 200)] u32,
);

#[test]
fn test_tuple_struct_with_attributes() {
    let s = TupleWithAttributes("hello".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 42);
    let size = s.heap_size();
    // Should be 5 (string) + 0 (ignored) + 200 (constant) = 205
    assert!(
        size >= 205,
        "heap_size should be at least 205, got {}",
        size
    );
    assert!(
        size < 220,
        "heap_size should not include ignored Vec, got {}",
        size
    );
}
