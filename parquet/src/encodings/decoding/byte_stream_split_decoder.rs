use crate::data_type::{DataType, SliceAsBytes};
use crate::{basic::Encoding, errors::Result};

use super::Decoder;

use bytes::Bytes;
use std::marker::PhantomData;

pub struct ByteStreamSplitDecoder<T: DataType> {
    _phantom: PhantomData<T>,
    encoded_bytes: Bytes,
    total_num_values: usize,
    values_decoded: usize,
}

impl<T: DataType> ByteStreamSplitDecoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            _phantom: PhantomData,
            encoded_bytes: Bytes::new(),
            total_num_values: 0,
            values_decoded: 0,
        }
    }
}

impl<T: DataType> Decoder<T> for ByteStreamSplitDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        self.encoded_bytes = data;
        self.total_num_values = num_values;
        self.values_decoded = 0;

        Ok(())
    }

    fn get(&mut self, buffer: &mut [<T as DataType>::T]) -> Result<usize> {
        let total_remaining_values = self.values_left();
        let num_values = buffer.len().min(total_remaining_values);

        // SAFETY: f32 and f64 has no constraints on their internal representation, so we can modify it as we want
        let raw_out_bytes = unsafe { <T as DataType>::T::slice_as_bytes_mut(buffer) };

        let num_values = num_values;
        let num_streams = T::get_type_size();
        let byte_stream_length = self.encoded_bytes.len() / num_streams;
        let values_decoded = self.values_decoded;

        // go through each value to decode
        for out_value_idx in 0..num_values {
            // go through each byte stream of that value
            for byte_stream_idx in 0..num_streams {
                let idx_in_encoded_data =
                    (byte_stream_idx * byte_stream_length) + (values_decoded + out_value_idx);

                raw_out_bytes[(out_value_idx * num_streams) + byte_stream_idx] =
                    self.encoded_bytes[idx_in_encoded_data];
            }
        }

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
