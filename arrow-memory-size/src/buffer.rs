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

//! HeapSize implementations for arrow-buffer types

use crate::HeapSize;
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};

impl HeapSize for Buffer {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T: arrow_buffer::ArrowNativeType> HeapSize for ScalarBuffer<T> {
    fn heap_size(&self) -> usize {
        self.inner().capacity()
    }
}

impl<T: arrow_buffer::ArrowNativeType> HeapSize for OffsetBuffer<T> {
    fn heap_size(&self) -> usize {
        self.inner().inner().capacity()
    }
}

impl HeapSize for NullBuffer {
    fn heap_size(&self) -> usize {
        self.buffer().capacity()
    }
}

impl HeapSize for BooleanBuffer {
    fn heap_size(&self) -> usize {
        self.inner().capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_heap_size() {
        let buf = Buffer::from(vec![1u8, 2, 3, 4, 5]);
        assert!(buf.heap_size() >= 5);
    }

    #[test]
    fn test_scalar_buffer_heap_size() {
        let buf: ScalarBuffer<i32> = vec![1, 2, 3, 4, 5].into();
        assert!(buf.heap_size() >= 5 * std::mem::size_of::<i32>());
    }

    #[test]
    fn test_null_buffer_heap_size() {
        let buf = NullBuffer::new_null(100);
        assert!(buf.heap_size() > 0);
    }

    #[test]
    fn test_boolean_buffer_heap_size() {
        let buf = BooleanBuffer::new_set(100);
        assert!(buf.heap_size() > 0);
    }
}
