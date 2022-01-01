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

use arrow::array::BooleanBufferBuilder;
use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;
use std::ops::Range;

use crate::column::reader::decoder::ColumnLevelDecoderImpl;
use crate::schema::types::ColumnDescPtr;

use super::{
    buffer::{BufferQueue, TypedBuffer},
    MIN_BATCH_SIZE,
};

pub struct DefinitionLevelBuffer {
    buffer: TypedBuffer<i16>,
    builder: BooleanBufferBuilder,
    max_level: i16,
}

impl BufferQueue for DefinitionLevelBuffer {
    type Output = Buffer;
    type Slice = [i16];

    fn split_off(&mut self, len: usize) -> Self::Output {
        self.buffer.split_off(len)
    }

    fn spare_capacity_mut(&mut self, batch_size: usize) -> &mut Self::Slice {
        assert_eq!(self.buffer.len(), self.builder.len());
        self.buffer.spare_capacity_mut(batch_size)
    }

    fn set_len(&mut self, len: usize) {
        self.buffer.set_len(len);
        let buf = self.buffer.as_slice();

        let range = self.builder.len()..len;
        self.builder.reserve(range.end - range.start);
        for i in &buf[range] {
            self.builder.append(*i == self.max_level)
        }
    }
}

impl DefinitionLevelBuffer {
    pub fn new(desc: &ColumnDescPtr) -> Self {
        Self {
            buffer: TypedBuffer::new(),
            builder: BooleanBufferBuilder::new(0),
            max_level: desc.max_def_level(),
        }
    }

    /// Split `len` levels out of `self`
    pub fn split_bitmask(&mut self, len: usize) -> Bitmap {
        let old_len = self.builder.len();
        let num_left_values = old_len - len;
        let new_bitmap_builder =
            BooleanBufferBuilder::new(MIN_BATCH_SIZE.max(num_left_values));

        let old_bitmap =
            std::mem::replace(&mut self.builder, new_bitmap_builder).finish();
        let old_bitmap = Bitmap::from(old_bitmap);

        for i in len..old_len {
            self.builder.append(old_bitmap.is_set(i));
        }

        old_bitmap
    }

    /// Returns an iterator of the valid positions in `range` in descending order
    pub fn rev_valid_positions_iter(
        &self,
        range: Range<usize>,
    ) -> impl Iterator<Item = usize> + '_ {
        let max_def_level = self.max_level;
        let slice = self.buffer.as_slice();
        range.rev().filter(move |x| slice[*x] == max_def_level)
    }
}

pub type DefinitionLevelDecoder = ColumnLevelDecoderImpl;
