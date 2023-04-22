use crate::basic::{Encoding, Type};
use crate::data_type::SliceAsBytes;
use crate::data_type::{AsBytes, DataType};
use crate::util::memory::ByteBufferPtr;

use crate::errors::Result;

use super::Encoder;

use std::marker::PhantomData;

pub struct ByteStreamSplitEncoder<T> {
    buffer: Vec<u8>,
    _p: PhantomData<T>,
}

impl<T: DataType> ByteStreamSplitEncoder<T> {
    pub(crate) fn new() -> Self {
        // println!("ByteStreamSplitEncoder constructed");
        Self {
            buffer: Vec::new(),
            _p: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for ByteStreamSplitEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        // println!("ByteStreamSplitEncoder.put");
        // dbg!(values.len());
        // println!("before self.buffer: {:x?}", self.buffer);
        self.buffer
            .extend(<T as DataType>::T::slice_as_bytes(values));
        ensure_phys_ty!(
            Type::FLOAT | Type::DOUBLE,
            "ByteStreamSplitEncoder only supports FloatType or DoubleType"
        );

        // TODO should this instead use T::encode?

        // TODO chunk to try to make compiler auto-vectorize
        // println!("after self.buffer: {:x?}", self.buffer);

        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::BYTE_STREAM_SPLIT
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.buffer.len()
    }

    fn flush_buffer(&mut self) -> Result<ByteBufferPtr> {
        let mut encoded = vec![0; self.buffer.len()];
        // Have to do all work in flush buffer, as we need the whole byte stream
        let num_streams = T::get_type_size();
        let num_values = self.buffer.len() / T::get_type_size();
        for i in 0..num_values {
            for j in 0..num_streams {
                let byte_in_value = self.buffer[i * num_streams + j];
                encoded[j * num_values + i] = byte_in_value;
            }
        }
        self.buffer.clear();
        Ok(encoded.into())
    }
}
