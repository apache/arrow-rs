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

use crate::errors::{ParquetError, Result};

use super::Encoder;

use bytes::BufMut;
use std::cmp;
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

// Here we assume src contains the full data (which it must, since we
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

// Like above, but type_size is not known at compile time.
fn split_streams_variable(src: &[u8], dst: &mut [u8], type_size: usize) {
    const BLOCK_SIZE: usize = 4;
    let stride = src.len() / type_size;
    for j in (0..type_size).step_by(BLOCK_SIZE) {
        let jrange = cmp::min(BLOCK_SIZE, type_size - j);
        for i in 0..stride {
            for jj in 0..jrange {
                dst[i + (j + jj) * stride] = src[i * type_size + j + jj];
            }
        }
    }
}

impl<T: DataType> Encoder<T> for ByteStreamSplitEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.buffer
            .extend(<T as DataType>::T::slice_as_bytes(values));

        ensure_phys_ty!(
            Type::FLOAT | Type::DOUBLE | Type::INT32 | Type::INT64,
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

    fn flush_to(&mut self, out: &mut Vec<u8>) -> Result<()> {
        let start = out.len();
        out.resize(start + self.buffer.len(), 0);
        let encoded = &mut out[start..];

        let type_size = T::get_type_size();
        match type_size {
            4 => split_streams_const::<4>(&self.buffer, encoded),
            8 => split_streams_const::<8>(&self.buffer, encoded),
            _ => {
                return Err(general_err!(
                    "byte stream split unsupported for data types of size {} bytes",
                    type_size
                ));
            }
        }

        self.buffer.clear();
        Ok(())
    }

    /// return the estimated memory size of this encoder.
    fn estimated_memory_size(&self) -> usize {
        self.buffer.capacity() * std::mem::size_of::<u8>()
    }
}

pub struct VariableWidthByteStreamSplitEncoder<T> {
    buffer: Vec<u8>,
    type_width: usize,
    _p: PhantomData<T>,
}

impl<T: DataType> VariableWidthByteStreamSplitEncoder<T> {
    pub(crate) fn new(type_length: i32) -> Self {
        Self {
            buffer: Vec::new(),
            type_width: type_length as usize,
            _p: PhantomData,
        }
    }
}

#[cold]
#[inline(never)]
fn mismatched_byte_array_size(bytes: usize, type_size: usize) {
    panic!("Mismatched FixedLenByteArray sizes: {bytes} != {type_size}");
}

fn put_fixed<T: DataType, const TYPE_SIZE: usize>(dst: &mut [u8], values: &[T::T]) {
    for (out, x) in dst.chunks_exact_mut(TYPE_SIZE).zip(values.iter()) {
        let bytes = x.as_bytes();
        if bytes.len() != TYPE_SIZE {
            mismatched_byte_array_size(bytes.len(), TYPE_SIZE);
        }
        out.copy_from_slice(bytes);
    }
}

fn put_variable<T: DataType>(dst: &mut [u8], values: &[T::T], type_width: usize) {
    for (out, x) in dst.chunks_exact_mut(type_width).zip(values.iter()) {
        let bytes = x.as_bytes();
        if bytes.len() != type_width {
            mismatched_byte_array_size(bytes.len(), type_width);
        }
        out.copy_from_slice(bytes);
    }
}

impl<T: DataType> Encoder<T> for VariableWidthByteStreamSplitEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        ensure_phys_ty!(
            Type::FIXED_LEN_BYTE_ARRAY,
            "VariableWidthByteStreamSplitEncoder only supports FixedLenByteArray types"
        );

        // FixedLenByteArray is implemented as ByteArray, so there may be gaps making
        // slice_as_bytes untenable
        let idx = self.buffer.len();
        let data_len = values.len() * self.type_width;
        // Extend the size of buffer to allow direct access
        self.buffer.put_bytes(0_u8, data_len);
        // Get a slice of the buffer corresponding to the location of the new data
        let out_buf = &mut self.buffer[idx..idx + data_len];

        // Now copy `values` into the buffer. For `type_width` <= 8 use a fixed size when
        // performing the copy as it is significantly faster.
        match self.type_width {
            2 => put_fixed::<T, 2>(out_buf, values),
            3 => put_fixed::<T, 3>(out_buf, values),
            4 => put_fixed::<T, 4>(out_buf, values),
            5 => put_fixed::<T, 5>(out_buf, values),
            6 => put_fixed::<T, 6>(out_buf, values),
            7 => put_fixed::<T, 7>(out_buf, values),
            8 => put_fixed::<T, 8>(out_buf, values),
            12 => put_fixed::<T, 12>(out_buf, values),
            16 => put_fixed::<T, 16>(out_buf, values),
            _ => put_variable::<T>(out_buf, values, self.type_width),
        }

        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::BYTE_STREAM_SPLIT
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.buffer.len()
    }

    fn flush_to(&mut self, out: &mut Vec<u8>) -> Result<()> {
        let src = &self.buffer[..];
        let start = out.len();
        out.resize(start + src.len(), 0);
        let dst = &mut out[start..];

        let type_size = match T::get_physical_type() {
            Type::FIXED_LEN_BYTE_ARRAY => self.type_width,
            _ => T::get_type_size(),
        };
        // split_streams_const() is faster up to type_width == 8
        match type_size {
            2 => split_streams_const::<2>(src, dst),
            3 => split_streams_const::<3>(src, dst),
            4 => split_streams_const::<4>(src, dst),
            5 => split_streams_const::<5>(src, dst),
            6 => split_streams_const::<6>(src, dst),
            7 => split_streams_const::<7>(src, dst),
            8 => split_streams_const::<8>(src, dst),
            12 => split_streams_const::<12>(src, dst),
            16 => split_streams_const::<16>(src, dst),
            _ => split_streams_variable(src, dst, type_size),
        }

        self.buffer.clear();
        Ok(())
    }

    /// return the estimated memory size of this encoder.
    fn estimated_memory_size(&self) -> usize {
        self.buffer.capacity() * std::mem::size_of::<u8>()
    }
}
