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
use crate::data_type::DataType;
use crate::data_type::SliceAsBytes;

use crate::errors::{ParquetError, Result};

use super::Encoder;

use bytes::Bytes;
use std::marker::PhantomData;

pub struct ByteStreamSplitEncoder<T> {
    buffer: Vec<u8>,
    _p: PhantomData<T>,
}

impl<T: DataType> ByteStreamSplitEncoder<T> {
    pub(crate) fn new() -> Self {
        Self {
            buffer: Vec::new(),
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

impl<T: DataType> Encoder<T> for ByteStreamSplitEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.buffer
            .extend(<T as DataType>::T::slice_as_bytes(values));
        ensure_phys_ty!(
            Type::FLOAT | Type::DOUBLE,
            "ByteStreamSplitEncoder only supports FloatType or DoubleType"
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
        let type_size = T::get_type_size();
        match type_size {
            4 => split_streams_const::<4>(&self.buffer, &mut encoded),
            8 => split_streams_const::<8>(&self.buffer, &mut encoded),
            _ => {
                return Err(general_err!(
                    "byte stream split unsupported for data types of size {} bytes",
                    type_size
                ));
            }
        }

        self.buffer.clear();
        Ok(encoded.into())
    }
}
