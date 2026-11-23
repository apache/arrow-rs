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

//! Scalar conversions shared by shredded Variant readers.

use arrow::array::{
    Array, AsArray, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, DecimalType, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time64MicrosecondType, TimestampMicrosecondType,
    TimestampNanosecondType,
};
use arrow::error::{ArrowError, Result};
use arrow::temporal_conversions::time64us_to_time;
use chrono::{DateTime, Utc};
use parquet_variant::{Variant, VariantDecimalType};
use uuid::Uuid;

pub(crate) trait DecodePrimitive: Array {
    fn decode_primitive(&self, index: usize) -> Result<Variant<'_, '_>>;
}

// Decode an array value, optionally converting it first.
macro_rules! impl_decode_primitive {
    ($array_type:ty $(, |$v:ident| $transform:expr)? ) => {
        impl DecodePrimitive for $array_type {
            fn decode_primitive(
                &self,
                index: usize,
            ) -> Result<Variant<'_, '_>> {
                let value = self.value(index);
                $(
                    let $v = value;
                    let value = $transform;
                )?
                Ok(value.into())
            }
        }
    };
}

impl_decode_primitive!(BooleanArray);
impl_decode_primitive!(StringArray);
impl_decode_primitive!(StringViewArray);
impl_decode_primitive!(LargeStringArray);
impl_decode_primitive!(BinaryArray);
impl_decode_primitive!(BinaryViewArray);
impl_decode_primitive!(LargeBinaryArray);
impl_decode_primitive!(PrimitiveArray<Int8Type>);
impl_decode_primitive!(PrimitiveArray<Int16Type>);
impl_decode_primitive!(PrimitiveArray<Int32Type>);
impl_decode_primitive!(PrimitiveArray<Int64Type>);
impl_decode_primitive!(PrimitiveArray<Float32Type>);
impl_decode_primitive!(PrimitiveArray<Float64Type>);

impl_decode_primitive!(PrimitiveArray<Date32Type>, |days_since_epoch| {
    Date32Type::to_naive_date_opt(days_since_epoch).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("Invalid Date32 value: {days_since_epoch}"))
    })?
});

impl_decode_primitive!(
    PrimitiveArray<Time64MicrosecondType>,
    |micros_since_midnight| {
        time64us_to_time(micros_since_midnight).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid Time64 microsecond value: {micros_since_midnight}"
            ))
        })?
    }
);

// FixedSizeBinary(16) guarantees a valid UUID byte length.
impl_decode_primitive!(FixedSizeBinaryArray, |bytes| {
    Uuid::from_slice(bytes).unwrap()
});

/// Converts Arrow timestamps to `DateTime<Utc>`.
pub(crate) trait TimestampType: ArrowPrimitiveType<Native = i64> {
    fn to_datetime_utc(value: i64) -> Result<DateTime<Utc>>;
}

impl TimestampType for TimestampMicrosecondType {
    fn to_datetime_utc(micros: i64) -> Result<DateTime<Utc>> {
        DateTime::from_timestamp_micros(micros).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid timestamp microsecond value: {micros}"
            ))
        })
    }
}

impl TimestampType for TimestampNanosecondType {
    fn to_datetime_utc(nanos: i64) -> Result<DateTime<Utc>> {
        Ok(DateTime::from_timestamp_nanos(nanos))
    }
}

pub(crate) fn decode_timestamp<T: TimestampType>(
    array: &dyn Array,
    index: usize,
) -> Result<Variant<'_, '_>> {
    let dt = T::to_datetime_utc(array.as_primitive::<T>().value(index))?;
    if matches!(array.data_type(), DataType::Timestamp(_, Some(_))) {
        Ok(dt.into())
    } else {
        Ok(dt.naive_utc().into())
    }
}

pub(crate) fn decode_decimal<A: DecimalType, V: VariantDecimalType<Native = A::Native>>(
    array: &dyn Array,
    index: usize,
) -> Result<Variant<'_, '_>> {
    let array = array.as_primitive::<A>();
    Ok(V::try_new_with_signed_scale(array.value(index), array.scale())?.into())
}
