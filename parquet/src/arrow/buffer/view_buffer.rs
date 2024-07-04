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

use crate::arrow::record_reader::buffer::ValuesBuffer;
use arrow_array::{builder::make_view, make_array, ArrayRef};
use arrow_buffer::Buffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowType;

/// A buffer of view type byte arrays that can be converted into
/// `GenericByteViewArray`
///
/// Note this does not reuse `GenericByteViewBuilder` due to the need to call `pad_nulls`
/// and reuse the existing logic for Vec in the parquet crate
#[derive(Debug, Default)]
pub struct ViewBuffer {
    pub views: Vec<u128>,
    pub buffers: Vec<Buffer>,
}

impl ViewBuffer {
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    pub fn append_block(&mut self, block: Buffer) -> u32 {
        let block_id = self.buffers.len() as u32;
        self.buffers.push(block);
        block_id
    }

    /// # Safety
    /// This method is only safe when:
    /// - `block` is a valid index, i.e., the return value of `append_block`
    /// - `offset` and `offset + len` are valid indices into the buffer
    /// - The `(offset, offset + len)` is valid value for the native type.
    pub unsafe fn append_view_unchecked(&mut self, block: u32, offset: u32, len: u32) {
        let b = self.buffers.get_unchecked(block as usize);
        let end = offset.saturating_add(len);
        let b = b.get_unchecked(offset as usize..end as usize);

        let view = make_view(b, block, offset);

        self.views.push(view);
    }

    /// Directly append a view to the view array.
    /// This is used when we create a StringViewArray from a dictionary whose values are StringViewArray.
    ///
    /// # Safety
    /// The `view` must be a valid view as per the ByteView spec.
    pub unsafe fn append_raw_view_unchecked(&mut self, view: &u128) {
        self.views.push(*view);
    }

    /// Converts this into an [`ArrayRef`] with the provided `data_type` and `null_buffer`
    #[allow(unused)]
    pub fn into_array(self, null_buffer: Option<Buffer>, data_type: &ArrowType) -> ArrayRef {
        let len = self.views.len();
        let views = Buffer::from_vec(self.views);
        match data_type {
            ArrowType::Utf8View => {
                let builder = ArrayDataBuilder::new(ArrowType::Utf8View)
                    .len(len)
                    .add_buffer(views)
                    .add_buffers(self.buffers)
                    .null_bit_buffer(null_buffer);
                // We have checked that the data is utf8 when building the buffer, so it is safe
                let array = unsafe { builder.build_unchecked() };
                make_array(array)
            }
            ArrowType::BinaryView => {
                let builder = ArrayDataBuilder::new(ArrowType::BinaryView)
                    .len(len)
                    .add_buffer(views)
                    .add_buffers(self.buffers)
                    .null_bit_buffer(null_buffer);
                let array = unsafe { builder.build_unchecked() };
                make_array(array)
            }
            _ => panic!("Unsupported data type: {:?}", data_type),
        }
    }
}

impl ValuesBuffer for ViewBuffer {
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        self.views
            .pad_nulls(read_offset, values_read, levels_read, valid_mask);
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::Array;

    use super::*;

    #[test]
    fn test_view_buffer_empty() {
        let buffer = ViewBuffer::default();
        let array = buffer.into_array(None, &ArrowType::Utf8View);
        let strings = array
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(strings.len(), 0);
    }

    #[test]
    fn test_view_buffer_append_view() {
        let mut buffer = ViewBuffer::default();
        let string_buffer = Buffer::from(&b"0123456789long string to test string view"[..]);
        let block_id = buffer.append_block(string_buffer);

        unsafe {
            buffer.append_view_unchecked(block_id, 0, 1);
            buffer.append_view_unchecked(block_id, 1, 9);
            buffer.append_view_unchecked(block_id, 10, 31);
        }

        let array = buffer.into_array(None, &ArrowType::Utf8View);
        let string_array = array
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(
            string_array.iter().collect::<Vec<_>>(),
            vec![
                Some("0"),
                Some("123456789"),
                Some("long string to test string view"),
            ]
        );
    }

    #[test]
    fn test_view_buffer_pad_null() {
        let mut buffer = ViewBuffer::default();
        let string_buffer = Buffer::from(&b"0123456789long string to test string view"[..]);
        let block_id = buffer.append_block(string_buffer);

        unsafe {
            buffer.append_view_unchecked(block_id, 0, 1);
            buffer.append_view_unchecked(block_id, 1, 9);
            buffer.append_view_unchecked(block_id, 10, 31);
        }

        let valid = [true, false, false, true, false, false, true];
        let valid_mask = Buffer::from_iter(valid.iter().copied());

        buffer.pad_nulls(1, 2, valid.len() - 1, valid_mask.as_slice());

        let array = buffer.into_array(Some(valid_mask), &ArrowType::Utf8View);
        let strings = array
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();

        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![
                Some("0"),
                None,
                None,
                Some("123456789"),
                None,
                None,
                Some("long string to test string view"),
            ]
        );
    }
}
