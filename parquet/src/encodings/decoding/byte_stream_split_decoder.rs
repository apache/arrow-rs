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

use std::marker::PhantomData;

use bytes::Bytes;

use crate::basic::Encoding;
use crate::data_type::{DataType, SliceAsBytes};
use crate::errors::{ParquetError, Result};

use super::Decoder;

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

// Here we assume src contains the full data (which it must, since we're
// can only know where to split the streams once all data is collected),
// but dst can be just a slice starting from the given index.
// We iterate over the output bytes and fill them in from their strided
// input byte locations.
fn join_streams_const<const TYPE_SIZE: usize>(
    src: &[u8],
    dst: &mut [u8],
    stride: usize,
    values_decoded: usize,
) {
    let sub_src = &src[values_decoded..];
    for i in 0..dst.len() / TYPE_SIZE {
        for j in 0..TYPE_SIZE {
            dst[i * TYPE_SIZE + j] = sub_src[i + j * stride];
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
        let buffer = &mut buffer[..num_values];

        // SAFETY: f32 and f64 has no constraints on their internal representation, so we can modify it as we want
        let raw_out_bytes = unsafe { <T as DataType>::T::slice_as_bytes_mut(buffer) };
        let type_size = T::get_type_size();
        let stride = self.encoded_bytes.len() / type_size;
        match type_size {
            4 => join_streams_const::<4>(
                &self.encoded_bytes,
                raw_out_bytes,
                stride,
                self.values_decoded,
            ),
            8 => join_streams_const::<8>(
                &self.encoded_bytes,
                raw_out_bytes,
                stride,
                self.values_decoded,
            ),
            _ => {
                return Err(general_err!(
                    "byte stream split unsupported for data types of size {} bytes",
                    type_size
                ));
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
