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

use crate::basic::{Encoding, Type};
use crate::data_type::{AsBytes, DataType, SliceAsBytes};

use crate::errors::Result;

use super::Encoder;

use bytes::Bytes;
use std::marker::PhantomData;

pub struct ByteStreamSplitEncoder<T> {
    buffer: Vec<u8>,
    type_width: usize,
    _p: PhantomData<T>,
}

impl<T: DataType> ByteStreamSplitEncoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            buffer: Vec::new(),
            type_width: 0,
            _p: PhantomData,
        }
    }
}

// Here we assume src contains the full data (which it must, since we're
// can only know where to split the streams once all data is collected).
// We iterate over the input bytes and write them to their strided output
// byte locations.
fn split_streams_const<const TYPE_SIZE: usize>(src: &[u8], dst: &mut [u8]) {
    let stride = src.len() / TYPE_SIZE;
    for i in 0..stride {
        for j in 0..TYPE_SIZE {
            dst[i + j * stride] = src[i * TYPE_SIZE + j];
        }
    }
}

fn split_streams(src: &[u8], dst: &mut [u8], type_size: usize) {
    let stride = src.len() / type_size;
    for i in 0..stride {
        for j in 0..type_size {
            dst[i + j * stride] = src[i * type_size + j];
        }
    }
}

impl<T: DataType> Encoder<T> for ByteStreamSplitEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        // FixedLenByteArray is implemented as ByteArray, so there may be gaps making
        // slice_as_bytes untenable
        match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => values.iter().for_each(|x| {
                let bytes = x.as_bytes();
                if bytes.len() != self.type_width {
                    panic!(
                        "Mismatched FixedLenByteArray sizes: {} != {}",
                        bytes.len(),
                        self.type_width
                    );
                }
                self.buffer.extend(bytes)
            }),
            _ => self
                .buffer
                .extend(<T as DataType>::T::slice_as_bytes(values)),
        }

        ensure_phys_ty!(
            Type::FLOAT | Type::DOUBLE | Type::INT32 | Type::INT64 | Type::FIXED_LEN_BYTE_ARRAY,
            "ByteStreamSplitEncoder does not support Int96, Boolean, or ByteArray types"
        );

        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::BYTE_STREAM_SPLIT
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.buffer.len()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        let mut encoded = vec![0; self.buffer.len()];
        let type_size = match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => self.type_width,
            _ => T::get_type_size(),
        };
        // match some common type width's. 2 for FLOAT16, 4 for INT32 and FLOAT,
        // 8 for INT64 and DOUBLE, 16 for UUID.
        match type_size {
            2 => split_streams_const::<2>(&self.buffer, &mut encoded),
            4 => split_streams_const::<4>(&self.buffer, &mut encoded),
            8 => split_streams_const::<8>(&self.buffer, &mut encoded),
            16 => split_streams_const::<16>(&self.buffer, &mut encoded),
            _ => split_streams(&self.buffer, &mut encoded, type_size),
        }

        self.buffer.clear();
        Ok(encoded.into())
    }

    /// return the estimated memory size of this encoder.
    fn estimated_memory_size(&self) -> usize {
        self.buffer.capacity() * std::mem::size_of::<u8>()
    }

    fn set_type_width(&mut self, type_width: usize) {
        self.type_width = type_width
    }
}
