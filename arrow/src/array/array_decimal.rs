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

use crate::array::ArrayAccessor;
use num::BigInt;
use std::borrow::Borrow;
use std::convert::From;
use std::fmt;
use std::{any::Any, iter::FromIterator};

use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData, FixedSizeListArray,
};
use super::{BasicDecimalIter, BooleanBufferBuilder, FixedSizeBinaryArray};
#[allow(deprecated)]
pub use crate::array::DecimalIter;
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::{validate_decimal256_precision_with_lt_bytes, DataType};
use crate::datatypes::{
    validate_decimal_precision, DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use crate::error::{ArrowError, Result};
use crate::util::decimal::{BasicDecimal, Decimal256};

/// `Decimal128Array` stores fixed width decimal numbers,
/// with a fixed precision and scale.
///
/// # Examples
///
/// ```
///    use arrow::array::{Array, BasicDecimalArray, Decimal128Array};
///    use arrow::datatypes::DataType;
///
///    // Create a DecimalArray with the default precision and scale
///    let decimal_array: Decimal128Array = vec![
///       Some(8_887_000_000),
///       None,
///       Some(-8_887_000_000),
///     ]
///     .into_iter().collect();
///
///    // set precision and scale so values are interpreted
///    // as `8887.000000`, `Null`, and `-8887.000000`
///    let decimal_array = decimal_array
///     .with_precision_and_scale(23, 6)
///     .unwrap();
///
///    assert_eq!(&DataType::Decimal128(23, 6), decimal_array.data_type());
///    assert_eq!(8_887_000_000_i128, decimal_array.value(0).as_i128());
///    assert_eq!("8887.000000", decimal_array.value_as_string(0));
///    assert_eq!(3, decimal_array.len());
///    assert_eq!(1, decimal_array.null_count());
///    assert_eq!(32, decimal_array.value_offset(2));
///    assert_eq!(16, decimal_array.value_length());
///    assert_eq!(23, decimal_array.precision());
///    assert_eq!(6, decimal_array.scale());
/// ```
///
pub type Decimal128Array = BasicDecimalArray<16>;

pub type Decimal256Array = BasicDecimalArray<32>;

pub struct BasicDecimalArray<const BYTE_WIDTH: usize> {
    data: ArrayData,
    value_data: RawPtrBox<u8>,
    precision: usize,
    scale: usize,
}

impl<const BYTE_WIDTH: usize> BasicDecimalArray<BYTE_WIDTH> {
    pub const VALUE_LENGTH: i32 = BYTE_WIDTH as i32;
    const DEFAULT_TYPE: DataType = BasicDecimal::<BYTE_WIDTH>::DEFAULT_TYPE;
    pub const MAX_PRECISION: usize = BasicDecimal::<BYTE_WIDTH>::MAX_PRECISION;
    pub const MAX_SCALE: usize = BasicDecimal::<BYTE_WIDTH>::MAX_SCALE;
    const TYPE_CONSTRUCTOR: fn(usize, usize) -> DataType =
        BasicDecimal::<BYTE_WIDTH>::TYPE_CONSTRUCTOR;

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    /// Return the precision (total digits) that can be stored by this array
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// Return the scale (digits after the decimal) that can be stored by this array
    pub fn scale(&self) -> usize {
        self.scale
    }

    /// Returns the element at index `i`.
    pub fn value(&self, i: usize) -> BasicDecimal<BYTE_WIDTH> {
        assert!(i < self.data().len(), "Out of bounds access");

        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`.
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, i: usize) -> BasicDecimal<BYTE_WIDTH> {
        let data = self.data();
        let offset = i + data.offset();
        let raw_val = {
            let pos = self.value_offset_at(offset);
            std::slice::from_raw_parts(
                self.raw_value_data_ptr().offset(pos as isize),
                Self::VALUE_LENGTH as usize,
            )
        };
        BasicDecimal::<BYTE_WIDTH>::new(self.precision(), self.scale(), raw_val)
    }

    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data().offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub fn value_length(&self) -> i32 {
        Self::VALUE_LENGTH
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data().buffers()[0].clone()
    }

    #[inline]
    pub fn value_offset_at(&self, i: usize) -> i32 {
        Self::VALUE_LENGTH * i as i32
    }

    #[inline]
    pub fn value_as_string(&self, row: usize) -> String {
        self.value(row).to_string()
    }

    /// Build a decimal array from [`FixedSizeBinaryArray`].
    ///
    /// NB: This function does not validate that each value is in the permissible
    /// range for a decimal
    pub fn from_fixed_size_binary_array(
        v: FixedSizeBinaryArray,
        precision: usize,
        scale: usize,
    ) -> Self {
        assert!(
            v.value_length() == Self::VALUE_LENGTH,
            "Value length of the array ({}) must equal to the byte width of the decimal ({})",
            v.value_length(),
            Self::VALUE_LENGTH,
        );
        let data_type = if Self::VALUE_LENGTH == 16 {
            DataType::Decimal128(precision, scale)
        } else {
            DataType::Decimal256(precision, scale)
        };
        let builder = v.into_data().into_builder().data_type(data_type);

        let array_data = unsafe { builder.build_unchecked() };
        Self::from(array_data)
    }

    /// Build a decimal array from [`FixedSizeListArray`].
    ///
    /// NB: This function does not validate that each value is in the permissible
    /// range for a decimal.
    #[deprecated(note = "please use `from_fixed_size_binary_array` instead")]
    pub fn from_fixed_size_list_array(
        v: FixedSizeListArray,
        precision: usize,
        scale: usize,
    ) -> Self {
        assert_eq!(
            v.data_ref().child_data().len(),
            1,
            "DecimalArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        let child_data = &v.data_ref().child_data()[0];

        assert_eq!(
            child_data.child_data().len(),
            0,
            "DecimalArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            child_data.data_type(),
            &DataType::UInt8,
            "DecimalArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );
        assert!(
            v.value_length() == Self::VALUE_LENGTH,
            "Value length of the array ({}) must equal to the byte width of the decimal ({})",
            v.value_length(),
            Self::VALUE_LENGTH,
        );
        assert_eq!(
            v.data_ref().child_data()[0].null_count(),
            0,
            "The child array cannot contain null values."
        );

        let list_offset = v.offset();
        let child_offset = child_data.offset();
        let data_type = if Self::VALUE_LENGTH == 16 {
            DataType::Decimal128(precision, scale)
        } else {
            DataType::Decimal256(precision, scale)
        };
        let builder = ArrayData::builder(data_type)
            .len(v.len())
            .add_buffer(child_data.buffers()[0].slice(child_offset))
            .null_bit_buffer(v.data_ref().null_buffer().cloned())
            .offset(list_offset);

        let array_data = unsafe { builder.build_unchecked() };
        Self::from(array_data)
    }

    /// The default precision and scale used when not specified.
    pub const fn default_type() -> DataType {
        Self::DEFAULT_TYPE
    }

    fn raw_value_data_ptr(&self) -> *const u8 {
        self.value_data.as_ptr()
    }
}

impl Decimal128Array {
    /// Creates a [Decimal128Array] with default precision and scale,
    /// based on an iterator of `i128` values without nulls
    pub fn from_iter_values<I: IntoIterator<Item = i128>>(iter: I) -> Self {
        let val_buf: Buffer = iter.into_iter().collect();
        let data = unsafe {
            ArrayData::new_unchecked(
                Self::default_type(),
                val_buf.len() / std::mem::size_of::<i128>(),
                None,
                None,
                0,
                vec![val_buf],
                vec![],
            )
        };
        Decimal128Array::from(data)
    }

    // Validates decimal values in this array can be properly interpreted
    // with the specified precision.
    fn validate_decimal_precision(&self, precision: usize) -> Result<()> {
        for v in self.iter().flatten() {
            validate_decimal_precision(v.as_i128(), precision)?;
        }
        Ok(())
    }

    /// Returns a Decimal array with the same data as self, with the
    /// specified precision.
    ///
    /// Returns an Error if:
    /// 1. `precision` is larger than [`Self::MAX_PRECISION`]
    /// 2. `scale` is larger than [`Self::MAX_SCALE`];
    /// 3. `scale` is > `precision`
    pub fn with_precision_and_scale(self, precision: usize, scale: usize) -> Result<Self>
    where
        Self: Sized,
    {
        if precision > Self::MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "precision {} is greater than max {}",
                precision,
                Self::MAX_PRECISION
            )));
        }
        if scale > Self::MAX_SCALE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than max {}",
                scale,
                Self::MAX_SCALE
            )));
        }
        if scale > precision {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than precision {}",
                scale, precision
            )));
        }

        // Ensure that all values are within the requested
        // precision. For performance, only check if the precision is
        // decreased
        if precision < self.precision {
            self.validate_decimal_precision(precision)?;
        }

        let data_type = Self::TYPE_CONSTRUCTOR(self.precision, self.scale);
        assert_eq!(self.data().data_type(), &data_type);

        // safety: self.data is valid DataType::Decimal as checked above
        let new_data_type = Self::TYPE_CONSTRUCTOR(precision, scale);

        Ok(self.data().clone().with_data_type(new_data_type).into())
    }
}

impl Decimal256Array {
    // Validates decimal values in this array can be properly interpreted
    // with the specified precision.
    fn validate_decimal_precision(&self, precision: usize) -> Result<()> {
        (0..self.len()).try_for_each(|idx| {
            if self.is_valid(idx) {
                let raw_val = unsafe {
                    let pos = self.value_offset(idx);
                    std::slice::from_raw_parts(
                        self.raw_value_data_ptr().offset(pos as isize),
                        Self::VALUE_LENGTH as usize,
                    )
                };
                validate_decimal256_precision_with_lt_bytes(raw_val, precision)
            } else {
                Ok(())
            }
        })
    }

    /// Returns a Decimal array with the same data as self, with the
    /// specified precision.
    ///
    /// Returns an Error if:
    /// 1. `precision` is larger than [`Self::MAX_PRECISION`]
    /// 2. `scale` is larger than [`Self::MAX_SCALE`];
    /// 3. `scale` is > `precision`
    pub fn with_precision_and_scale(self, precision: usize, scale: usize) -> Result<Self>
    where
        Self: Sized,
    {
        if precision > Self::MAX_PRECISION {
            return Err(ArrowError::InvalidArgumentError(format!(
                "precision {} is greater than max {}",
                precision,
                Self::MAX_PRECISION
            )));
        }
        if scale > Self::MAX_SCALE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than max {}",
                scale,
                Self::MAX_SCALE
            )));
        }
        if scale > precision {
            return Err(ArrowError::InvalidArgumentError(format!(
                "scale {} is greater than precision {}",
                scale, precision
            )));
        }

        // Ensure that all values are within the requested
        // precision. For performance, only check if the precision is
        // decreased
        if precision < self.precision {
            self.validate_decimal_precision(precision)?;
        }

        let data_type = Self::TYPE_CONSTRUCTOR(self.precision, self.scale);
        assert_eq!(self.data().data_type(), &data_type);

        // safety: self.data is valid DataType::Decimal as checked above
        let new_data_type = Self::TYPE_CONSTRUCTOR(precision, scale);

        Ok(self.data().clone().with_data_type(new_data_type).into())
    }
}

impl<const BYTE_WIDTH: usize> From<ArrayData> for BasicDecimalArray<BYTE_WIDTH> {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DecimalArray data should contain 1 buffer only (values)"
        );
        let values = data.buffers()[0].as_ptr();
        let (precision, scale) = match (data.data_type(), BYTE_WIDTH) {
            (DataType::Decimal128(precision, scale), 16)
            | (DataType::Decimal256(precision, scale), 32) => (*precision, *scale),
            _ => panic!("Expected data type to be Decimal"),
        };
        Self {
            data,
            value_data: unsafe { RawPtrBox::new(values) },
            precision,
            scale,
        }
    }
}

impl<'a> Decimal128Array {
    /// Constructs a new iterator that iterates `Decimal128` values as i128 values.
    /// This is kept mostly for back-compatibility purpose.
    /// Suggests to use `iter()` that returns `Decimal128Iter`.
    #[allow(deprecated)]
    pub fn i128_iter(&'a self) -> DecimalIter<'a> {
        DecimalIter::<'a>::new(self)
    }
}

impl From<BigInt> for Decimal256 {
    fn from(bigint: BigInt) -> Self {
        Decimal256::from_big_int(&bigint, DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE)
            .unwrap()
    }
}

fn build_decimal_array_from<const BYTE_WIDTH: usize>(
    null_buf: BooleanBufferBuilder,
    buffer: Buffer,
) -> BasicDecimalArray<BYTE_WIDTH> {
    let data = unsafe {
        ArrayData::new_unchecked(
            BasicDecimalArray::<BYTE_WIDTH>::default_type(),
            null_buf.len(),
            None,
            Some(null_buf.into()),
            0,
            vec![buffer],
            vec![],
        )
    };
    BasicDecimalArray::<BYTE_WIDTH>::from(data)
}

impl<Ptr: Into<Decimal256>> FromIterator<Option<Ptr>> for Decimal256Array {
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let size_hint = upper.unwrap_or(lower);

        let mut null_buf = BooleanBufferBuilder::new(size_hint);

        let mut buffer = MutableBuffer::with_capacity(size_hint);

        iter.for_each(|item| {
            if let Some(a) = item {
                null_buf.append(true);
                buffer.extend_from_slice(Into::into(a).raw_value());
            } else {
                null_buf.append(false);
                buffer.extend_zeros(32);
            }
        });

        build_decimal_array_from::<32>(null_buf, buffer.into())
    }
}

impl<Ptr: Borrow<Option<i128>>> FromIterator<Ptr> for Decimal128Array {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let size_hint = upper.unwrap_or(lower);

        let mut null_buf = BooleanBufferBuilder::new(size_hint);

        let buffer: Buffer = iter
            .map(|item| {
                if let Some(a) = item.borrow() {
                    null_buf.append(true);
                    *a
                } else {
                    null_buf.append(false);
                    // arbitrary value for NULL
                    0
                }
            })
            .collect();

        build_decimal_array_from::<16>(null_buf, buffer)
    }
}

impl<const BYTE_WIDTH: usize> Array for BasicDecimalArray<BYTE_WIDTH> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }
}

impl<const BYTE_WIDTH: usize> From<BasicDecimalArray<BYTE_WIDTH>> for ArrayData {
    fn from(array: BasicDecimalArray<BYTE_WIDTH>) -> Self {
        array.data
    }
}

impl<const BYTE_WIDTH: usize> fmt::Debug for BasicDecimalArray<BYTE_WIDTH> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Decimal{}Array<{}, {}>\n[\n",
            BYTE_WIDTH * 8,
            self.precision,
            self.scale
        )?;
        print_long_array(self, f, |array, index, f| {
            let formatted_decimal = array.value_as_string(index);

            write!(f, "{}", formatted_decimal)
        })?;
        write!(f, "]")
    }
}

impl<'a, const BYTE_WIDTH: usize> ArrayAccessor for &'a BasicDecimalArray<BYTE_WIDTH> {
    type Item = BasicDecimal<BYTE_WIDTH>;

    fn value(&self, index: usize) -> Self::Item {
        BasicDecimalArray::<BYTE_WIDTH>::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        BasicDecimalArray::<BYTE_WIDTH>::value_unchecked(self, index)
    }
}

impl<'a, const BYTE_WIDTH: usize> IntoIterator for &'a BasicDecimalArray<BYTE_WIDTH> {
    type Item = Option<BasicDecimal<BYTE_WIDTH>>;
    type IntoIter = BasicDecimalIter<'a, BYTE_WIDTH>;

    fn into_iter(self) -> Self::IntoIter {
        BasicDecimalIter::<'a, BYTE_WIDTH>::new(self)
    }
}

impl<'a, const BYTE_WIDTH: usize> BasicDecimalArray<BYTE_WIDTH> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BasicDecimalIter<'a, BYTE_WIDTH> {
        BasicDecimalIter::<'a, BYTE_WIDTH>::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::Decimal256Builder;
    use crate::{array::Decimal128Builder, datatypes::Field};
    use num::{BigInt, Num};

    use super::*;

    #[test]
    fn test_decimal_array() {
        // let val_8887: [u8; 16] = [192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // let val_neg_8887: [u8; 16] = [64, 36, 75, 238, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255];
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal128(38, 6))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build()
            .unwrap();
        let decimal_array = Decimal128Array::from(array_data);
        assert_eq!(8_887_000_000_i128, decimal_array.value(0).into());
        assert_eq!(-8_887_000_000_i128, decimal_array.value(1).into());
        assert_eq!(16, decimal_array.value_length());
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_decimal_append_error_value() {
        let mut decimal_builder = Decimal128Builder::new(10, 5, 3);
        let mut result = decimal_builder.append_value(123456);
        let mut error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: 123456 is too large to store in a Decimal128 of precision 5. Max is 99999",
            error.to_string()
        );

        unsafe {
            decimal_builder.disable_value_validation();
        }
        result = decimal_builder.append_value(123456);
        assert!(result.is_ok());
        decimal_builder.append_value(12345).unwrap();
        let arr = decimal_builder.finish();
        assert_eq!("12.345", arr.value_as_string(1));

        decimal_builder = Decimal128Builder::new(10, 2, 1);
        result = decimal_builder.append_value(100);
        error = result.unwrap_err();
        assert_eq!(
            "Invalid argument error: 100 is too large to store in a Decimal128 of precision 2. Max is 99",
            error.to_string()
        );

        unsafe {
            decimal_builder.disable_value_validation();
        }
        result = decimal_builder.append_value(100);
        assert!(result.is_ok());
        decimal_builder.append_value(99).unwrap();
        result = decimal_builder.append_value(-100);
        assert!(result.is_ok());
        decimal_builder.append_value(-99).unwrap();
        let arr = decimal_builder.finish();
        assert_eq!("9.9", arr.value_as_string(1));
        assert_eq!("-9.9", arr.value_as_string(3));
    }

    #[test]
    fn test_decimal_from_iter_values() {
        let array = Decimal128Array::from_iter_values(vec![-100, 0, 101].into_iter());
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(-100_i128, array.value(0).into());
        assert!(!array.is_null(0));
        assert_eq!(0_i128, array.value(1).into());
        assert!(!array.is_null(1));
        assert_eq!(101_i128, array.value(2).into());
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_decimal_from_iter() {
        let array: Decimal128Array =
            vec![Some(-100), None, Some(101)].into_iter().collect();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal128(38, 10));
        assert_eq!(-100_i128, array.value(0).into());
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(101_i128, array.value(2).into());
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_decimal_iter() {
        let data = vec![Some(-100), None, Some(101)];
        let array: Decimal128Array = data.clone().into_iter().collect();

        let collected: Vec<_> = array.iter().map(|d| d.map(|v| v.as_i128())).collect();
        assert_eq!(data, collected);
    }

    #[test]
    fn test_decimal_into_iter() {
        let data = vec![Some(-100), None, Some(101)];
        let array: Decimal128Array = data.clone().into_iter().collect();

        let collected: Vec<_> =
            array.into_iter().map(|d| d.map(|v| v.as_i128())).collect();
        assert_eq!(data, collected);
    }

    #[test]
    fn test_decimal_iter_sized() {
        let data = vec![Some(-100), None, Some(101)];
        let array: Decimal128Array = data.into_iter().collect();
        let mut iter = array.into_iter();

        // is exact sized
        assert_eq!(array.len(), 3);

        // size_hint is reported correctly
        assert_eq!(iter.size_hint(), (3, Some(3)));
        iter.next().unwrap();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        iter.next().unwrap();
        iter.next().unwrap();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert!(iter.next().is_none());
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_decimal_array_value_as_string() {
        let arr = [123450, -123450, 100, -100, 10, -10, 0]
            .into_iter()
            .map(Some)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(6, 3)
            .unwrap();

        assert_eq!("123.450", arr.value_as_string(0));
        assert_eq!("-123.450", arr.value_as_string(1));
        assert_eq!("0.100", arr.value_as_string(2));
        assert_eq!("-0.100", arr.value_as_string(3));
        assert_eq!("0.010", arr.value_as_string(4));
        assert_eq!("-0.010", arr.value_as_string(5));
        assert_eq!("0.000", arr.value_as_string(6));
    }

    #[test]
    fn test_decimal_array_with_precision_and_scale() {
        let arr = Decimal128Array::from_iter_values([12345, 456, 7890, -123223423432432])
            .with_precision_and_scale(20, 2)
            .unwrap();

        assert_eq!(arr.data_type(), &DataType::Decimal128(20, 2));
        assert_eq!(arr.precision(), 20);
        assert_eq!(arr.scale(), 2);

        let actual: Vec<_> = (0..arr.len()).map(|i| arr.value_as_string(i)).collect();
        let expected = vec!["123.45", "4.56", "78.90", "-1232234234324.32"];

        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic(
        expected = "-123223423432432 is too small to store in a Decimal128 of precision 5. Min is -99999"
    )]
    fn test_decimal_array_with_precision_and_scale_out_of_range() {
        Decimal128Array::from_iter_values([12345, 456, 7890, -123223423432432])
            // precision is too small to hold value
            .with_precision_and_scale(5, 2)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "precision 40 is greater than max 38")]
    fn test_decimal_array_with_precision_and_scale_invalid_precision() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(40, 2)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "scale 40 is greater than max 38")]
    fn test_decimal_array_with_precision_and_scale_invalid_scale() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(20, 40)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "scale 10 is greater than precision 4")]
    fn test_decimal_array_with_precision_and_scale_invalid_precision_and_scale() {
        Decimal128Array::from_iter_values([12345, 456])
            .with_precision_and_scale(4, 10)
            .unwrap();
    }

    #[test]
    fn test_decimal_array_fmt_debug() {
        let arr = [Some(8887000000), Some(-8887000000), None]
            .iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        assert_eq!(
            "Decimal128Array<23, 6>\n[\n  8887.000000,\n  -8887.000000,\n  null,\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_decimal_array_from_fixed_size_binary() {
        let value_data = ArrayData::builder(DataType::FixedSizeBinary(16))
            .offset(1)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&[99999_i128, 2, 34, 560]))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b1010])))
            .build()
            .unwrap();

        let binary_array = FixedSizeBinaryArray::from(value_data);
        let decimal = Decimal128Array::from_fixed_size_binary_array(binary_array, 38, 1);

        assert_eq!(decimal.len(), 3);
        assert_eq!(decimal.value_as_string(0), "0.2".to_string());
        assert!(decimal.is_null(1));
        assert_eq!(decimal.value_as_string(2), "56.0".to_string());
    }

    #[test]
    #[should_panic(
        expected = "Value length of the array (8) must equal to the byte width of the decimal (16)"
    )]
    fn test_decimal_array_from_fixed_size_binary_wrong_length() {
        let value_data = ArrayData::builder(DataType::FixedSizeBinary(8))
            .offset(1)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&[99999_i64, 2, 34, 560]))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b1010])))
            .build()
            .unwrap();

        let binary_array = FixedSizeBinaryArray::from(value_data);
        let _ = Decimal128Array::from_fixed_size_binary_array(binary_array, 38, 1);
    }

    #[test]
    #[allow(deprecated)]
    fn test_decimal_array_from_fixed_size_list() {
        let value_data = ArrayData::builder(DataType::UInt8)
            .offset(16)
            .len(48)
            .add_buffer(Buffer::from_slice_ref(&[99999_i128, 12, 34, 56]))
            .build()
            .unwrap();

        let null_buffer = Buffer::from_slice_ref(&[0b101]);

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::UInt8, false)),
            16,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .null_bit_buffer(Some(null_buffer))
            .offset(1)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);
        let decimal = Decimal128Array::from_fixed_size_list_array(list_array, 38, 0);

        assert_eq!(decimal.len(), 2);
        assert!(decimal.is_null(0));
        assert_eq!(decimal.value_as_string(1), "56".to_string());
    }

    #[test]
    #[allow(deprecated)]
    #[should_panic(expected = "The child array cannot contain null values.")]
    fn test_decimal_array_from_fixed_size_list_with_child_nulls_failed() {
        let value_data = ArrayData::builder(DataType::UInt8)
            .len(16)
            .add_buffer(Buffer::from_slice_ref(&[12_i128]))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b1010101010101010])))
            .build()
            .unwrap();

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::UInt8, false)),
            16,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(1)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);
        drop(Decimal128Array::from_fixed_size_list_array(
            list_array, 38, 0,
        ));
    }

    #[test]
    #[allow(deprecated)]
    #[should_panic(
        expected = "Value length of the array (8) must equal to the byte width of the decimal (16)"
    )]
    fn test_decimal_array_from_fixed_size_list_with_wrong_length() {
        let value_data = ArrayData::builder(DataType::UInt8)
            .len(16)
            .add_buffer(Buffer::from_slice_ref(&[12_i128]))
            .null_bit_buffer(Some(Buffer::from_slice_ref(&[0b1010101010101010])))
            .build()
            .unwrap();

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::UInt8, false)),
            8,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);
        drop(Decimal128Array::from_fixed_size_list_array(
            list_array, 38, 0,
        ));
    }

    #[test]
    fn test_decimal256_iter() {
        let mut builder = Decimal256Builder::new(30, 76, 6);
        let value = BigInt::from_str_radix("12345", 10).unwrap();
        let decimal1 = Decimal256::from_big_int(&value, 76, 6).unwrap();
        builder.append_value(&decimal1).unwrap();

        builder.append_null();

        let value = BigInt::from_str_radix("56789", 10).unwrap();
        let decimal2 = Decimal256::from_big_int(&value, 76, 6).unwrap();
        builder.append_value(&decimal2).unwrap();

        let array: Decimal256Array = builder.finish();

        let collected: Vec<_> = array.iter().collect();
        assert_eq!(vec![Some(decimal1), None, Some(decimal2)], collected);
    }

    #[test]
    fn test_from_iter_decimal256array() {
        let value1 = BigInt::from_str_radix("12345", 10).unwrap();
        let value2 = BigInt::from_str_radix("56789", 10).unwrap();

        let array: Decimal256Array =
            vec![Some(value1.clone()), None, Some(value2.clone())]
                .into_iter()
                .collect();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Decimal256(76, 10));
        assert_eq!(
            Decimal256::from_big_int(
                &value1,
                DECIMAL256_MAX_PRECISION,
                DECIMAL_DEFAULT_SCALE
            )
            .unwrap(),
            array.value(0)
        );
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(
            Decimal256::from_big_int(
                &value2,
                DECIMAL256_MAX_PRECISION,
                DECIMAL_DEFAULT_SCALE
            )
            .unwrap(),
            array.value(2)
        );
        assert!(!array.is_null(2));
    }
}
