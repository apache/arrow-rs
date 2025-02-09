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

pub use arrow_buffer::BufferBuilder;
pub use arrow_buffer::OffsetBufferBuilder;

use half::f16;

use crate::types::*;

/// Buffer builder for signed 8-bit integer type.
pub type Int8BufferBuilder = BufferBuilder<i8>;
/// Buffer builder for signed 16-bit integer type.
pub type Int16BufferBuilder = BufferBuilder<i16>;
/// Buffer builder for signed 32-bit integer type.
pub type Int32BufferBuilder = BufferBuilder<i32>;
/// Buffer builder for signed 64-bit integer type.
pub type Int64BufferBuilder = BufferBuilder<i64>;
/// Buffer builder for usigned 8-bit integer type.
pub type UInt8BufferBuilder = BufferBuilder<u8>;
/// Buffer builder for usigned 16-bit integer type.
pub type UInt16BufferBuilder = BufferBuilder<u16>;
/// Buffer builder for usigned 32-bit integer type.
pub type UInt32BufferBuilder = BufferBuilder<u32>;
/// Buffer builder for usigned 64-bit integer type.
pub type UInt64BufferBuilder = BufferBuilder<u64>;
/// Buffer builder for 16-bit floating point type.
pub type Float16BufferBuilder = BufferBuilder<f16>;
/// Buffer builder for 32-bit floating point type.
pub type Float32BufferBuilder = BufferBuilder<f32>;
/// Buffer builder for 64-bit floating point type.
pub type Float64BufferBuilder = BufferBuilder<f64>;

/// Buffer builder for 128-bit decimal type.
pub type Decimal128BufferBuilder = BufferBuilder<<Decimal128Type as ArrowPrimitiveType>::Native>;
/// Buffer builder for 256-bit decimal type.
pub type Decimal256BufferBuilder = BufferBuilder<<Decimal256Type as ArrowPrimitiveType>::Native>;

/// Buffer builder for timestamp type of second unit.
pub type TimestampSecondBufferBuilder =
    BufferBuilder<<TimestampSecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for timestamp type of millisecond unit.
pub type TimestampMillisecondBufferBuilder =
    BufferBuilder<<TimestampMillisecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for timestamp type of microsecond unit.
pub type TimestampMicrosecondBufferBuilder =
    BufferBuilder<<TimestampMicrosecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for timestamp type of nanosecond unit.
pub type TimestampNanosecondBufferBuilder =
    BufferBuilder<<TimestampNanosecondType as ArrowPrimitiveType>::Native>;

/// Buffer builder for 32-bit date type.
pub type Date32BufferBuilder = BufferBuilder<<Date32Type as ArrowPrimitiveType>::Native>;
/// Buffer builder for 64-bit date type.
pub type Date64BufferBuilder = BufferBuilder<<Date64Type as ArrowPrimitiveType>::Native>;

/// Buffer builder for 32-bit elaspsed time since midnight of second unit.
pub type Time32SecondBufferBuilder =
    BufferBuilder<<Time32SecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for 32-bit elaspsed time since midnight of millisecond unit.
pub type Time32MillisecondBufferBuilder =
    BufferBuilder<<Time32MillisecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for 64-bit elaspsed time since midnight of microsecond unit.
pub type Time64MicrosecondBufferBuilder =
    BufferBuilder<<Time64MicrosecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for 64-bit elaspsed time since midnight of nanosecond unit.
pub type Time64NanosecondBufferBuilder =
    BufferBuilder<<Time64NanosecondType as ArrowPrimitiveType>::Native>;

/// Buffer builder for “calendar” interval in months.
pub type IntervalYearMonthBufferBuilder =
    BufferBuilder<<IntervalYearMonthType as ArrowPrimitiveType>::Native>;
/// Buffer builder for “calendar” interval in days and milliseconds.
pub type IntervalDayTimeBufferBuilder =
    BufferBuilder<<IntervalDayTimeType as ArrowPrimitiveType>::Native>;
/// Buffer builder “calendar” interval in months, days, and nanoseconds.
pub type IntervalMonthDayNanoBufferBuilder =
    BufferBuilder<<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native>;

/// Buffer builder for elaspsed time of second unit.
pub type DurationSecondBufferBuilder =
    BufferBuilder<<DurationSecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for elaspsed time of milliseconds unit.
pub type DurationMillisecondBufferBuilder =
    BufferBuilder<<DurationMillisecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for elaspsed time of microseconds unit.
pub type DurationMicrosecondBufferBuilder =
    BufferBuilder<<DurationMicrosecondType as ArrowPrimitiveType>::Native>;
/// Buffer builder for elaspsed time of nanoseconds unit.
pub type DurationNanosecondBufferBuilder =
    BufferBuilder<<DurationNanosecondType as ArrowPrimitiveType>::Native>;

#[cfg(test)]
mod tests {
    use crate::builder::{ArrayBuilder, Int32BufferBuilder, Int8Builder, UInt8BufferBuilder};
    use crate::Array;

    #[test]
    fn test_builder_i32_empty() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(0, b.len());
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32_alloc_zero_bytes() {
        let mut b = Int32BufferBuilder::new(0);
        b.append(123);
        let a = b.finish();
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b = Int32BufferBuilder::new(5);
        for i in 0..5 {
            b.append(i);
        }
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(20, a.len());
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        for i in 0..20 {
            b.append(i);
        }
        assert_eq!(32, b.capacity());
        let a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_builder_finish() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(16, b.capacity());
        for i in 0..10 {
            b.append(i);
        }
        let mut a = b.finish();
        assert_eq!(40, a.len());
        assert_eq!(0, b.len());
        assert_eq!(0, b.capacity());

        // Try build another buffer after cleaning up.
        for i in 0..20 {
            b.append(i)
        }
        assert_eq!(32, b.capacity());
        a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_reserve() {
        let mut b = UInt8BufferBuilder::new(2);
        assert_eq!(64, b.capacity());
        b.reserve(64);
        assert_eq!(64, b.capacity());
        b.reserve(65);
        assert_eq!(128, b.capacity());

        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        b.reserve(16);
        assert_eq!(16, b.capacity());
        b.reserve(17);
        assert_eq!(32, b.capacity());
    }

    #[test]
    fn test_append_slice() {
        let mut b = UInt8BufferBuilder::new(0);
        b.append_slice(b"Hello, ");
        b.append_slice(b"World!");
        let buffer = b.finish();
        assert_eq!(13, buffer.len());

        let mut b = Int32BufferBuilder::new(0);
        b.append_slice(&[32, 54]);
        let buffer = b.finish();
        assert_eq!(8, buffer.len());
    }

    #[test]
    fn test_append_values() {
        let mut a = Int8Builder::new();
        a.append_value(1);
        a.append_null();
        a.append_value(-2);
        assert_eq!(a.len(), 3);

        // append values
        let values = &[1, 2, 3, 4];
        let is_valid = &[true, true, false, true];
        a.append_values(values, is_valid);

        assert_eq!(a.len(), 7);
        let array = a.finish();
        assert_eq!(array.value(0), 1);
        assert!(array.is_null(1));
        assert_eq!(array.value(2), -2);
        assert_eq!(array.value(3), 1);
        assert_eq!(array.value(4), 2);
        assert!(array.is_null(5));
        assert_eq!(array.value(6), 4);
    }
}
