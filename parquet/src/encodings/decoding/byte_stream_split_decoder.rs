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

use crate::basic::{Encoding, Type};
use crate::data_type::private::ParquetValueType;
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

// Like the above, but type_size is not known at compile time.
fn join_streams_variable(
    src: &[u8],
    dst: &mut [u8],
    stride: usize,
    type_size: usize,
    values_decoded: usize,
) {
    let sub_src = &src[values_decoded..];
    for i in 0..dst.len() / type_size {
        for j in 0..type_size {
            dst[i * type_size + j] = sub_src[i + j * stride];
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

        // SAFETY: i/f32 and i/f64 has no constraints on their internal representation, so we can modify it as we want
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

pub struct VariableWidthByteStreamSplitDecoder<T: DataType> {
    _phantom: PhantomData<T>,
    encoded_bytes: Bytes,
    total_num_values: usize,
    values_decoded: usize,
    type_width: usize,
}

impl<T: DataType> VariableWidthByteStreamSplitDecoder<T> {
    pub(crate) fn new(type_length: i32) -> Self {
        Self {
            _phantom: PhantomData,
            encoded_bytes: Bytes::new(),
            total_num_values: 0,
            values_decoded: 0,
            type_width: type_length as usize,
        }
    }
}

impl<T: DataType> Decoder<T> for VariableWidthByteStreamSplitDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        // Rough check that all data elements are the same length
        if data.len() % self.type_width != 0 {
            return Err(general_err!(
                "Input data length is not a multiple of type width {}",
                self.type_width
            ));
        }

        match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => {
                self.encoded_bytes = data;
                self.total_num_values = num_values;
                self.values_decoded = 0;
                Ok(())
            }
            _ => Err(general_err!(
                "VariableWidthByteStreamSplitDecoder only supports FixedLenByteArrayType"
            )),
        }
    }

    fn get(&mut self, buffer: &mut [<T as DataType>::T]) -> Result<usize> {
        let total_remaining_values = self.values_left();
        let num_values = buffer.len().min(total_remaining_values);
        let buffer = &mut buffer[..num_values];
        let type_size = self.type_width;

        // Since this is FIXED_LEN_BYTE_ARRAY data, we can't use slice_as_bytes_mut. Instead we'll
        // have to do some data copies.
        let mut tmp_vec = vec![0_u8; num_values * type_size];
        let raw_out_bytes = tmp_vec.as_mut_slice();

        let stride = self.encoded_bytes.len() / type_size;
        match type_size {
            2 => join_streams_const::<2>(
                &self.encoded_bytes,
                raw_out_bytes,
                stride,
                self.values_decoded,
            ),
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
            16 => join_streams_const::<16>(
                &self.encoded_bytes,
                raw_out_bytes,
                stride,
                self.values_decoded,
            ),
            _ => join_streams_variable(
                &self.encoded_bytes,
                raw_out_bytes,
                stride,
                type_size,
                self.values_decoded,
            ),
        }
        self.values_decoded += num_values;

        // create a buffer from the vec so far (and leave a new Vec in its place)
        let vec_with_data = std::mem::take(&mut tmp_vec);
        // convert Vec to Bytes (which is a ref counted wrapper)
        let bytes_with_data = Bytes::from(vec_with_data);
        for (i, bi) in buffer.iter_mut().enumerate().take(num_values) {
            // Get a view into the data, without also copying the bytes
            let data = bytes_with_data.slice(i * type_size..(i + 1) * type_size);
            bi.set_from_bytes(data);
        }

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
