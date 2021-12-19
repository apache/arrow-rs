use crate::arrow::record_reader::buffer::{
    BufferQueue, ScalarBuffer, ScalarValue, ValuesBuffer,
};
use crate::column::reader::decoder::ValuesBufferSlice;
use crate::errors::{ParquetError, Result};
use arrow::array::{make_array, ArrayDataBuilder, ArrayRef, OffsetSizeTrait};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowNativeType, DataType as ArrowType};
use std::ops::Range;

pub struct OffsetBuffer<I: ScalarValue> {
    pub offsets: ScalarBuffer<I>,
    pub values: ScalarBuffer<u8>,
}

impl<I: ScalarValue> Default for OffsetBuffer<I> {
    fn default() -> Self {
        let mut offsets = ScalarBuffer::new();
        offsets.resize(1);
        Self {
            offsets,
            values: ScalarBuffer::new(),
        }
    }
}

impl<I: OffsetSizeTrait + ScalarValue> OffsetBuffer<I> {
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn try_push(&mut self, data: &[u8], validate_utf8: bool) -> Result<()> {
        if validate_utf8 {
            if let Err(e) = std::str::from_utf8(data) {
                return Err(ParquetError::General(format!(
                    "encountered non UTF-8 data: {}",
                    e
                )));
            }
        }

        self.values.extend_from_slice(data);

        let index_offset = I::from_usize(self.values.len())
            .ok_or_else(|| general_err!("index overflow decoding byte array"))?;

        self.offsets.push(index_offset);
        Ok(())
    }

    pub fn extend_from_dictionary<K: ArrowNativeType, V: ArrowNativeType>(
        &mut self,
        keys: &[K],
        dict_offsets: &[V],
        dict_values: &[u8],
    ) -> Result<()> {
        for key in keys {
            let index = key.to_usize().unwrap();
            if index + 1 >= dict_offsets.len() {
                return Err(general_err!("invalid offset in byte array: {}", index));
            }
            let start_offset = dict_offsets[index].to_usize().unwrap();
            let end_offset = dict_offsets[index + 1].to_usize().unwrap();

            // Dictionary values are verified when decoding dictionary page
            self.try_push(&dict_values[start_offset..end_offset], false)?;
        }
        Ok(())
    }

    pub fn into_array(
        self,
        null_buffer: Option<Buffer>,
        data_type: ArrowType,
    ) -> ArrayRef {
        let mut array_data_builder = ArrayDataBuilder::new(data_type)
            .len(self.len())
            .add_buffer(self.offsets.into())
            .add_buffer(self.values.into());

        if let Some(buffer) = null_buffer {
            array_data_builder = array_data_builder.null_bit_buffer(buffer);
        }

        let data = match cfg!(debug_assertions) {
            true => array_data_builder.build().unwrap(),
            false => unsafe { array_data_builder.build_unchecked() }
        };

        make_array(data)
    }
}

impl<I: OffsetSizeTrait + ScalarValue> BufferQueue for OffsetBuffer<I> {
    type Output = Self;
    type Slice = Self;

    fn split_off(&mut self, len: usize) -> Self::Output {
        let remaining_offsets = self.offsets.len() - len - 1;
        let offsets = self.offsets.as_slice();

        let end_offset = offsets[len];

        let mut new_offsets = ScalarBuffer::new();
        new_offsets.reserve(remaining_offsets + 1);
        for v in &offsets[len..] {
            new_offsets.push(*v - end_offset)
        }

        self.offsets.resize(len + 1);

        Self {
            offsets: std::mem::replace(&mut self.offsets, new_offsets),
            values: self.values.take(end_offset.to_usize().unwrap()),
        }
    }

    fn spare_capacity_mut(&mut self, _batch_size: usize) -> &mut Self::Slice {
        self
    }

    fn set_len(&mut self, len: usize) {
        assert_eq!(self.offsets.len(), len + 1);
    }
}

impl<I: OffsetSizeTrait + ScalarValue> ValuesBuffer for OffsetBuffer<I> {
    fn pad_nulls(
        &mut self,
        values_range: Range<usize>,
        levels_range: Range<usize>,
        rev_position_iter: impl Iterator<Item = usize>,
    ) {
        assert_eq!(values_range.start, levels_range.start);
        assert_eq!(self.offsets.len(), values_range.end + 1);
        self.offsets.resize(levels_range.end + 1);

        let offsets = self.offsets.as_slice_mut();

        let values_start = values_range.start;
        let mut last_offset = levels_range.end + 1;

        for (value_pos, level_pos) in values_range.rev().zip(rev_position_iter) {
            assert!(level_pos >= value_pos);
            assert!(level_pos < last_offset);

            if level_pos == value_pos {
                // Pad leading nulls if necessary
                if level_pos != last_offset {
                    let value = offsets[last_offset];
                    for x in &mut offsets[level_pos + 1..last_offset] {
                        *x = value;
                    }
                }

                // We are done
                return;
            }

            // Fill in any nulls
            let value_end = offsets[value_pos + 1];
            let value_start = offsets[value_pos];

            for x in &mut offsets[level_pos + 1..last_offset] {
                *x = value_end;
            }

            offsets[level_pos] = value_start;
            last_offset = level_pos;
        }

        // Pad leading nulls up to `last_offset`
        let value = offsets[values_start];
        for x in &mut offsets[values_start + 1..last_offset] {
            *x = value
        }
    }
}

impl<I: ScalarValue> ValuesBufferSlice for OffsetBuffer<I> {
    fn capacity(&self) -> usize {
        usize::MAX
    }
}
