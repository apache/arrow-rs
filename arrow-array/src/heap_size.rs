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

// Note: HeapSize cannot be implemented for ArrayRef (Arc<dyn Array>) here due to
// Rust's orphan rules. Use array.get_buffer_memory_size() directly instead.

// =============================================================================
// Primitive and Boolean Arrays
// =============================================================================

impl<T: ArrowPrimitiveType> HeapSize for PrimitiveArray<T> {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for BooleanArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for NullArray {
    fn heap_size(&self) -> usize {
        // NullArray has no buffers
        0
    }
}

// =============================================================================
// String and Binary Arrays
// =============================================================================

impl HeapSize for StringArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for LargeStringArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for BinaryArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for LargeBinaryArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for StringViewArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for BinaryViewArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for FixedSizeBinaryArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

// =============================================================================
// List Arrays
// =============================================================================

impl HeapSize for ListArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for LargeListArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for ListViewArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for LargeListViewArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for FixedSizeListArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

// =============================================================================
// Complex/Nested Arrays
// =============================================================================

impl HeapSize for StructArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for MapArray {
    fn heap_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl HeapSize for UnionArray {
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
