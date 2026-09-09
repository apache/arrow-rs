// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Static ALP dispatch: only floating-point physical types can construct an encoder.

use super::alp_encoder::AlpEncoder;
use super::{Encoder, unsupported_column_encoding};
use crate::basic::Encoding;
use crate::data_type::{ByteArray, DataType, FixedLenByteArray, Int96};
use crate::errors::Result;
use bytes::Bytes;

pub trait AlpRunEncoder<T: DataType>: Encoder<T> {
    fn put_repeated(&mut self, value: T::T, count: usize) -> Result<()>;
}

impl<T: DataType> AlpRunEncoder<T> for AlpEncoder<T>
where
    T::T: crate::encodings::alp::AlpFloat,
{
    fn put_repeated(&mut self, value: T::T, count: usize) -> Result<()> {
        AlpEncoder::put_repeated(self, value, count)
    }
}

pub trait AlpValue: Sized {
    type Encoder<D>: AlpRunEncoder<D> + 'static
    where
        D: DataType<T = Self>;

    fn new_alp_encoder<D: DataType<T = Self>>() -> Result<Self::Encoder<D>>;
}

/// An uninhabited ALP variant for non-floating physical types. No dynamic dispatch,
/// conversion buffer, or encoder state is added to their write paths.
pub enum UnsupportedAlp {}

impl<T: DataType> Encoder<T> for UnsupportedAlp {
    fn put(&mut self, _values: &[T::T]) -> Result<()> {
        unreachable!("ALP is not supported for this physical type")
    }
    fn encoding(&self) -> Encoding {
        unreachable!("ALP is not supported for this physical type")
    }
    fn estimated_data_encoded_size(&self) -> usize {
        unreachable!("ALP is not supported for this physical type")
    }
    fn estimated_memory_size(&self) -> usize {
        unreachable!("ALP is not supported for this physical type")
    }
    fn flush_buffer(&mut self) -> Result<Bytes> {
        unreachable!("ALP is not supported for this physical type")
    }
}

impl<T: DataType> AlpRunEncoder<T> for UnsupportedAlp {
    fn put_repeated(&mut self, _value: T::T, _count: usize) -> Result<()> {
        unreachable!("ALP is not supported for this physical type")
    }
}

macro_rules! unsupported_alp {
    ($($ty:ty),+ $(,)?) => {$(
        impl AlpValue for $ty {
            type Encoder<D> = UnsupportedAlp where D: DataType<T = Self>;

            fn new_alp_encoder<D: DataType<T = Self>>() -> Result<Self::Encoder<D>> {
                Err(unsupported_column_encoding(Encoding::ALP, D::get_physical_type()))
            }
        }
    )+};
}

unsupported_alp!(bool, i32, i64, Int96, ByteArray, FixedLenByteArray);

macro_rules! floating_alp {
    ($($ty:ty),+ $(,)?) => {$(
        impl AlpValue for $ty {
            type Encoder<D> = AlpEncoder<D> where D: DataType<T = Self>;

            fn new_alp_encoder<D: DataType<T = Self>>() -> Result<Self::Encoder<D>> {
                Ok(AlpEncoder::new())
            }
        }
    )+};
}

floating_alp!(f32, f64);
