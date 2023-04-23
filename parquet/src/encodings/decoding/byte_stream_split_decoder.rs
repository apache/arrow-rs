use crate::data_type::{DataType, SliceAsBytes};
use crate::{basic::Encoding, errors::Result};

use super::Decoder;

use crate::util::memory::ByteBufferPtr;

use std::marker::PhantomData;

pub struct ByteStreamSplitDecoder<T: DataType> {
    _phantom: PhantomData<T>,
    encoded_bytes: ByteBufferPtr,
    total_num_values: usize,
    values_decoded: usize,
}

impl<T: DataType> ByteStreamSplitDecoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            _phantom: PhantomData,
            encoded_bytes: ByteBufferPtr::new(vec![]),
            total_num_values: 0,
            values_decoded: 0,
        }
    }
}

impl<T: DataType> Decoder<T> for ByteStreamSplitDecoder<T> {
    fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
        self.encoded_bytes = data;
        self.total_num_values = num_values;
        self.values_decoded = 0;

        println!();
        Ok(())
    }

    fn get(&mut self, buffer: &mut [<T as DataType>::T]) -> Result<usize> {
        let total_remaining_values = self.values_left();
        let num_values = buffer.len().min(total_remaining_values);
        dbg!(
            self.total_num_values,
            total_remaining_values,
            self.values_decoded,
            num_values,
        );

        // TODO explain safety
        let raw_out_bytes = unsafe { <T as DataType>::T::slice_as_bytes_mut(buffer) };

        // For each value, compute a chunk that is the indexes into self.encoded_bytes to fetch and combine into the output byte

        // TODO it might be better to go through one byte stream at a time for memory locality

        let num_values = num_values;
        let num_streams = T::get_type_size();
        let byte_stream_length = self.encoded_bytes.len() / num_streams;
        let values_decoded = self.values_decoded;

        // go through each value to decode
        for out_value_idx in 0..num_values {
            // go through each byte stream of that value
            for byte_stream_idx in 0..num_streams {
                let idx_in_encoded_data = (byte_stream_idx * byte_stream_length)
                    + (values_decoded + out_value_idx);

                raw_out_bytes[(out_value_idx * num_streams) + byte_stream_idx] =
                    self.encoded_bytes[idx_in_encoded_data];
            }
        }

        // let encoded_indices_iter = (0..num_values).cartesian_product(0..num_streams).map(
        //     |(value_idx, byte_idx)| {
        //         (start
        //             + value_idx
        //             + (byte_idx * (num_streams - 1)) % self.encoded_bytes.len())
        //     },
        // );
        // for (out_byte_idx, encoded_idx) in (0..num_bytes).zip(encoded_indices_iter) {
        //     println!("{out_byte_idx} {encoded_idx}");
        //     raw_out_bytes[out_byte_idx] = self.encoded_bytes[encoded_idx];
        // }

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

        self.values_decoded += num_values;

        Ok(num_values)
    }

    fn values_left(&self) -> usize {
        self.total_num_values - self.values_decoded
    }

    fn encoding(&self) -> Encoding {
        Encoding::BYTE_STREAM_SPLIT
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let to_skip = usize::min(self.values_left(), num_values);
        self.values_decoded += to_skip;
        Ok(to_skip)
    }
}
