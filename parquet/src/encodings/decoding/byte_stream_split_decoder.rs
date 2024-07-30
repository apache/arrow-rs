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
use crate::data_type::{DataType, FixedLenByteArray, SliceAsBytes};
use crate::errors::Result;

use super::Decoder;

pub struct ByteStreamSplitDecoder<T: DataType> {
    _phantom: PhantomData<T>,
    encoded_bytes: Bytes,
    total_num_values: usize,
    values_decoded: usize,
    type_width: usize,
}

impl<T: DataType> ByteStreamSplitDecoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            _phantom: PhantomData,
            encoded_bytes: Bytes::new(),
            total_num_values: 0,
            values_decoded: 0,
            type_width: 0,
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

fn join_streams(
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

        let type_size = match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => self.type_width,
            _ => T::get_type_size(),
        };

        // If this is FIXED_LEN_BYTE_ARRAY data, we can't use slice_as_bytes_mut. Instead we'll
        // have to do some data copies.
        let mut tmp_vec = match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => vec![0_u8; num_values * type_size],
            _ => Vec::new(),
        };

        // SAFETY: f32 and f64 has no constraints on their internal representation, so we can modify it as we want
        let raw_out_bytes = match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => tmp_vec.as_mut_slice(),
            _ => unsafe { <T as DataType>::T::slice_as_bytes_mut(buffer) },
        };

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
            _ => join_streams(
                &self.encoded_bytes,
                raw_out_bytes,
                stride,
                type_size,
                self.values_decoded,
            ),
        }
        self.values_decoded += num_values;

        // FIXME(ets): there's got to be a better way to do this
        if T::get_physical_type() == Type::FIXED_LEN_BYTE_ARRAY  {
            for i in 0..num_values {
                if let Some(bi) = buffer[i].as_mut_any().downcast_mut::<FixedLenByteArray>() {
                    bi.set_data(Bytes::copy_from_slice(&tmp_vec[i * type_size..(i+1) * type_size]));
                }
            }
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

    fn set_type_width(&mut self, type_width: usize) {
        self.type_width = type_width
    }
}
