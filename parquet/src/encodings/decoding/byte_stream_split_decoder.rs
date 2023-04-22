use crate::data_type::{DataType, SliceAsBytes};
use crate::{basic::Encoding, errors::Result};

use super::Decoder;

use crate::util::memory::ByteBufferPtr;

use std::marker::PhantomData;

pub struct ByteStreamSplitDecoder<T: DataType> {
    _phantom: PhantomData<T>,
    encoded_bytes: ByteBufferPtr,
    total_num_values: usize,
    cur_byte_idx: usize,
}

impl<T: DataType> ByteStreamSplitDecoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            _phantom: PhantomData,
            encoded_bytes: ByteBufferPtr::new(vec![]),
            total_num_values: 0,
            cur_byte_idx: 0,
        }
    }
}

impl<T: DataType> Decoder<T> for ByteStreamSplitDecoder<T> {
    fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
        self.encoded_bytes = data;
        self.total_num_values = num_values;
        self.cur_byte_idx = 0;

        println!();
        Ok(())
    }

    fn get(&mut self, buffer: &mut [<T as DataType>::T]) -> Result<usize> {
        let total_remaining_values = self.values_left();
        let num_values_to_decode = buffer.len().min(total_remaining_values);
        dbg!(
            self.total_num_values,
            total_remaining_values,
            self.cur_byte_idx,
            num_values_to_decode,
        );

        let num_streams = T::get_type_size();

        // TODO explain safety
        let raw_out_bytes = unsafe { <T as DataType>::T::slice_as_bytes_mut(buffer) };

        let byte_stride = num_values_to_decode;

        let mut start = self.cur_byte_idx;

        // For each value, compute a chunk that is the indexes into self.encoded_bytes to fetch and combine into the output byte

        (0..12).cycle().chun(T::get_type_size())

        for value_idx in 0..num_values_to_decode {
            usize::
            // go through each byte at a time of the value
            for b_idx in 0..T::get_type_size() {
                let encoded_byte_idx = 
                raw_out_bytes[value_idx * T::get_type_size() + b_idx] =
                    self.encoded_bytes[start + value_idx + (b_idx * T::get_type_size())];
            }
        }

        // for out_index in (0..raw_out_bytes.len()).step_by(T::get_type_size()) {
        //     for i in 0..T::get_type_size() {
        //         raw_out_bytes[out_index + i] =
        //             self.encoded_bytes[self.cur_byte_idx + (i * T::get_type_size())];
        //     }
        // }

        // // TODO will it be better to take iterate stream first, because of locality?
        // for i in self.cur_byte_idx..self.cur_byte_idx + num_values_to_decode {
        //     for b in 0..num_streams {
        //         // dbg!(byte_index);
        //         // TODO avoid get_type_size mul every iteration
        //         let encoded_byte_index = (b * byte_stride) + i;
        //         let out_byte_index = i * num_streams + b;
        //         // dbg!(encoded_byte_index, out_byte_index, b, byte_stride);
        //         raw_out_bytes[out_byte_index] = self.encoded_bytes[encoded_byte_index];
        //     }
        // }

        // TODO I think there's an inherent flaw in the design of Decoding, where the
        // Or maybe we can actually do it by remember the byte we are at in self.buffer

        self.cur_byte_idx += num_values_to_decode;

        Ok(num_values_to_decode)
    }

    fn values_left(&self) -> usize {
        self.total_num_values - self.cur_byte_idx
    }

    fn encoding(&self) -> Encoding {
        Encoding::BYTE_STREAM_SPLIT
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        todo!()
        // let values_skipped = 0;
        // let new = self.cur_value_idx + num_values;
        // if new > self.num_values {
        //     return Ok()
        // }
        // self.cur_value_idx.s + num_values
        // self.cur_value_idx.a += num_values;
        // Ok()
    }
}
