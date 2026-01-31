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

//! [`HeapSize`] implementations for arrow-array types

use arrow_memory_size::HeapSize;

use crate::Array;
use crate::types::{ArrowDictionaryKeyType, ArrowPrimitiveType, RunEndIndexType};
use crate::{
    BinaryArray, BinaryViewArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray,
    ListArray, ListViewArray, MapArray, NullArray, PrimitiveArray, RunArray, StringArray,
    StringViewArray, StructArray, UnionArray,
};

// Note: A blanket implementation `impl<T: Array> HeapSize for T` would be ideal,
// but is not possible due to Rust's orphan rules (E0210) since HeapSize is defined
// in a separate crate.
//
// Note: HeapSize cannot be implemented for ArrayRef (Arc<dyn Array>) here due to
// Rust's orphan rules. Use array.get_buffer_memory_size() directly instead.

/// Implements HeapSize for array types that delegate to get_buffer_memory_size()
macro_rules! impl_heap_size {
    ($($ty:ty),*) => {
        $(
            impl HeapSize for $ty {
                fn heap_size(&self) -> usize {
                    self.get_buffer_memory_size()
                }
            }
        )*
    };
}

impl_heap_size!(
    BooleanArray,
    NullArray,
    StringArray,
    LargeStringArray,
    BinaryArray,
    LargeBinaryArray,
    StringViewArray,
    BinaryViewArray,
    FixedSizeBinaryArray,
    ListArray,
    LargeListArray,
    ListViewArray,
    LargeListViewArray,
    FixedSizeListArray,
    StructArray,
    MapArray,
    UnionArray
);

impl<T: ArrowPrimitiveType> HeapSize for PrimitiveArray<T> {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl<K: ArrowDictionaryKeyType> HeapSize for DictionaryArray<K> {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl<R: RunEndIndexType> HeapSize for RunArray<R> {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Int32Array;

    #[test]
    fn test_primitive_array_heap_size() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let size = array.heap_size();
        assert!(size >= 5 * std::mem::size_of::<i32>());
    }

    #[test]
    fn test_string_array_heap_size() {
        let array = StringArray::from(vec!["hello", "world"]);
        let size = array.heap_size();
        // Should include offset buffer + data buffer
        assert!(size > 0);
    }

    #[test]
    fn test_boolean_array_heap_size() {
        let array = BooleanArray::from(vec![true, false, true]);
        let size = array.heap_size();
        assert!(size > 0);
    }

    #[test]
    fn test_null_array_heap_size() {
        let array = NullArray::new(100);
        assert_eq!(array.heap_size(), 0);
    }

    #[test]
    fn test_struct_array_heap_size() {
        use crate::builder::StructBuilder;
        use arrow_schema::{DataType, Field, Fields};

        let fields = Fields::from(vec![Field::new("a", DataType::Int32, false)]);
        let mut builder = StructBuilder::from_fields(fields, 10);
        builder
            .field_builder::<crate::builder::Int32Builder>(0)
            .unwrap()
            .append_value(1);
        builder.append(true);
        let array = builder.finish();
        let size = array.heap_size();
        assert!(size > 0);
    }
}
