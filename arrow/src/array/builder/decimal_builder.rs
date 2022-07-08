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

use std::any::Any;
use std::sync::Arc;

use crate::array::array_decimal::{BasicDecimalArray, Decimal256Array};
use crate::array::ArrayRef;
use crate::array::DecimalArray;
use crate::array::UInt8Builder;
use crate::array::{ArrayBuilder, FixedSizeListBuilder};

use crate::error::{ArrowError, Result};

use crate::datatypes::validate_decimal_precision;
use crate::util::decimal::{BasicDecimal, Decimal256};

/// Array Builder for [`DecimalArray`]
///
/// See [`DecimalArray`] for example.
///
#[derive(Debug)]
pub struct DecimalBuilder {
    builder: FixedSizeListBuilder<UInt8Builder>,
    precision: usize,
    scale: usize,

    /// Should i128 values be validated for compatibility with scale and precision?
    /// defaults to true
    value_validation: bool,
}

/// Array Builder for [`Decimal256Array`]
///
/// See [`Decimal256Array`] for example.
#[derive(Debug)]
pub struct Decimal256Builder {
    builder: FixedSizeListBuilder<UInt8Builder>,
    precision: usize,
    scale: usize,
}

impl DecimalBuilder {
    /// Creates a new `DecimalBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize, precision: usize, scale: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        let byte_width = 16;
        Self {
            builder: FixedSizeListBuilder::new(values_builder, byte_width),
            precision,
            scale,
            value_validation: true,
        }
    }

    /// Disable validation
    ///
    /// # Safety
    ///
    /// After disabling validation, caller must ensure that appended values are compatible
    /// for the specified precision and scale.
    pub unsafe fn disable_value_validation(&mut self) {
        self.value_validation = false;
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl Into<i128>) -> Result<()> {
        let value = if self.value_validation {
            validate_decimal_precision(value.into(), self.precision)?
        } else {
            value.into()
        };

        let value_as_bytes = Self::from_i128_to_fixed_size_bytes(
            value,
            self.builder.value_length() as usize,
        )?;
        if self.builder.value_length() != value_as_bytes.len() as i32 {
            return Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as DecimalBuilder value lengths".to_string()
            ));
        }
        self.builder
            .values()
            .append_slice(value_as_bytes.as_slice())?;
        self.builder.append(true)
    }

    pub(crate) fn from_i128_to_fixed_size_bytes(v: i128, size: usize) -> Result<Vec<u8>> {
        if size > 16 {
            return Err(ArrowError::InvalidArgumentError(
                "DecimalBuilder only supports values up to 16 bytes.".to_string(),
            ));
        }
        let res = v.to_le_bytes();
        let start_byte = 16 - size;
        Ok(res[start_byte..16].to_vec())
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        let length: usize = self.builder.value_length() as usize;
        self.builder.values().append_slice(&vec![0u8; length][..])?;
        self.builder.append(false)
    }

    /// Builds the `DecimalArray` and reset this builder.
    pub fn finish(&mut self) -> DecimalArray {
        DecimalArray::from_fixed_size_list_array(
            self.builder.finish(),
            self.precision,
            self.scale,
        )
    }
}

impl ArrayBuilder for DecimalBuilder {
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl Decimal256Builder {
    /// Creates a new `Decimal256Builder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize, precision: usize, scale: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        let byte_width = 32;
        Self {
            builder: FixedSizeListBuilder::new(values_builder, byte_width),
            precision,
            scale,
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: &Decimal256) -> Result<()> {
        let value_as_bytes = value.raw_value();

        if self.precision != value.precision() || self.scale != value.scale() {
            return Err(ArrowError::InvalidArgumentError(
                "Decimal value does not have the same precision or scale as Decimal256Builder".to_string()
            ));
        }

        if self.builder.value_length() != value_as_bytes.len() as i32 {
            return Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as Decimal256Builder value lengths".to_string()
            ));
        }
        self.builder.values().append_slice(value_as_bytes)?;
        self.builder.append(true)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        let length: usize = self.builder.value_length() as usize;
        self.builder.values().append_slice(&vec![0u8; length][..])?;
        self.builder.append(false)
    }

    /// Builds the `Decimal256Array` and reset this builder.
    pub fn finish(&mut self) -> Decimal256Array {
        Decimal256Array::from_fixed_size_list_array(
            self.builder.finish(),
            self.precision,
            self.scale,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::array_decimal::BasicDecimalArray;
    use crate::array::Array;
    use crate::datatypes::DataType;
    use crate::util::decimal::Decimal128;

    #[test]
    fn test_decimal_builder() {
        let mut builder = DecimalBuilder::new(30, 38, 6);

        builder.append_value(8_887_000_000_i128).unwrap();
        builder.append_null().unwrap();
        builder.append_value(-8_887_000_000_i128).unwrap();
        let decimal_array: DecimalArray = builder.finish();

        assert_eq!(&DataType::Decimal(38, 6), decimal_array.data_type());
        assert_eq!(3, decimal_array.len());
        assert_eq!(1, decimal_array.null_count());
        assert_eq!(32, decimal_array.value_offset(2));
        assert_eq!(16, decimal_array.value_length());
    }

    #[test]
    fn test_decimal_builder_with_decimal128() {
        let mut builder = DecimalBuilder::new(30, 38, 6);

        builder
            .append_value(Decimal128::new_from_i128(30, 38, 8_887_000_000_i128))
            .unwrap();
        builder.append_null().unwrap();
        builder
            .append_value(Decimal128::new_from_i128(30, 38, -8_887_000_000_i128))
            .unwrap();
        let decimal_array: DecimalArray = builder.finish();

        assert_eq!(&DataType::Decimal(38, 6), decimal_array.data_type());
        assert_eq!(3, decimal_array.len());
        assert_eq!(1, decimal_array.null_count());
        assert_eq!(32, decimal_array.value_offset(2));
        assert_eq!(16, decimal_array.value_length());
    }

    #[test]
    fn test_decimal256_builder() {
        let mut builder = Decimal256Builder::new(30, 40, 6);

        let mut bytes = vec![0; 32];
        bytes[0..16].clone_from_slice(&8_887_000_000_i128.to_le_bytes());
        let value = Decimal256::try_new_from_bytes(40, 6, bytes.as_slice()).unwrap();
        builder.append_value(&value).unwrap();

        builder.append_null().unwrap();

        bytes = vec![255; 32];
        let value = Decimal256::try_new_from_bytes(40, 6, bytes.as_slice()).unwrap();
        builder.append_value(&value).unwrap();

        bytes = vec![0; 32];
        bytes[0..16].clone_from_slice(&0_i128.to_le_bytes());
        bytes[15] = 128;
        let value = Decimal256::try_new_from_bytes(40, 6, bytes.as_slice()).unwrap();
        builder.append_value(&value).unwrap();

        let decimal_array: Decimal256Array = builder.finish();

        assert_eq!(&DataType::Decimal(40, 6), decimal_array.data_type());
        assert_eq!(4, decimal_array.len());
        assert_eq!(1, decimal_array.null_count());
        assert_eq!(64, decimal_array.value_offset(2));
        assert_eq!(32, decimal_array.value_length());

        assert_eq!(decimal_array.value(0).to_string(), "8887.000000");
        assert!(decimal_array.is_null(1));
        assert_eq!(decimal_array.value(2).to_string(), "-0.000001");
        assert_eq!(
            decimal_array.value(3).to_string(),
            "170141183460469231731687303715884.105728"
        );
    }

    #[test]
    #[should_panic(
        expected = "Decimal value does not have the same precision or scale as Decimal256Builder"
    )]
    fn test_decimal256_builder_unmatched_precision_scale() {
        let mut builder = Decimal256Builder::new(30, 10, 6);

        let mut bytes = vec![0; 32];
        bytes[0..16].clone_from_slice(&8_887_000_000_i128.to_le_bytes());
        let value = Decimal256::try_new_from_bytes(40, 6, bytes.as_slice()).unwrap();
        builder.append_value(&value).unwrap();
    }
}
