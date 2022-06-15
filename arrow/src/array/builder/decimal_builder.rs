use std::any::Any;
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

pub use crate::array::builder::GenericBinaryBuilder;

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

impl<OffsetSize: OffsetSizeTrait> ArrayBuilder for GenericBinaryBuilder<OffsetSize> {
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

impl<OffsetSize: OffsetSizeTrait> ArrayBuilder for GenericStringBuilder<OffsetSize> {
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
        let a = GenericStringBuilder::<OffsetSize>::finish(self);
        Arc::new(a)
    }
}

impl ArrayBuilder for FixedSizeBinaryBuilder {
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

impl<OffsetSize: OffsetSizeTrait> GenericBinaryBuilder<OffsetSize> {
    /// Creates a new `GenericBinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: GenericListBuilder::new(values_builder),
        }
    }

    /// Appends a single byte value into the builder's values array.
    ///
    /// Note, when appending individual byte values you must call `append` to delimit each
    /// distinct list value.
    #[inline]
    pub fn append_byte(&mut self, value: u8) -> Result<()> {
        self.builder.values().append_value(value)?;
        Ok(())
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<()> {
        self.builder.values().append_slice(value.as_ref())?;
        self.builder.append(true)?;
        Ok(())
    }

    /// Finish the current variable-length list array slot.
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Builds the `BinaryArray` and reset this builder.
    pub fn finish(&mut self) -> GenericBinaryArray<OffsetSize> {
        GenericBinaryArray::<OffsetSize>::from(self.builder.finish())
    }
}

impl<OffsetSize: OffsetSizeTrait> GenericStringBuilder<OffsetSize> {
    /// Creates a new `StringBuilder`,
    /// `capacity` is the number of bytes of string data to pre-allocate space for in this builder
    pub fn new(capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: GenericListBuilder::new(values_builder),
        }
    }

    /// Creates a new `StringBuilder`,
    /// `data_capacity` is the number of bytes of string data to pre-allocate space for in this builder
    /// `item_capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(data_capacity);
        Self {
            builder: GenericListBuilder::with_capacity(values_builder, item_capacity),
        }
    }

    /// Appends a string into the builder.
    ///
    /// Automatically calls the `append` method to delimit the string appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<str>) -> Result<()> {
        self.builder
            .values()
            .append_slice(value.as_ref().as_bytes())?;
        self.builder.append(true)?;
        Ok(())
    }

    /// Finish the current variable-length list array slot.
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Append an `Option` value to the array.
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<str>>) -> Result<()> {
        match value {
            None => self.append_null()?,
            Some(v) => self.append_value(v)?,
        };
        Ok(())
    }

    /// Builds the `StringArray` and reset this builder.
    pub fn finish(&mut self) -> GenericStringArray<OffsetSize> {
        GenericStringArray::<OffsetSize>::from(self.builder.finish())
    }
}

impl FixedSizeBinaryBuilder {
    /// Creates a new `BinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize, byte_width: i32) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: FixedSizeListBuilder::new(values_builder, byte_width),
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<[u8]>) -> Result<()> {
        if self.builder.value_length() != value.as_ref().len() as i32 {
            return Err(ArrowError::InvalidArgumentError(
                "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths".to_string()
            ));
        }
        self.builder.values().append_slice(value.as_ref())?;
        self.builder.append(true)
    }

    /// Append a null value to the array.
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        let length: usize = self.builder.value_length() as usize;
        self.builder.values().append_slice(&vec![0u8; length][..])?;
        self.builder.append(false)
    }

    /// Builds the `FixedSizeBinaryArray` and reset this builder.
    pub fn finish(&mut self) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::from(self.builder.finish())
    }
}

impl DecimalBuilder {
    /// Creates a new `BinaryBuilder`, `capacity` is the number of bytes in the values
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
    pub fn append_value(&mut self, value: i128) -> Result<()> {
        let value = if self.value_validation {
            validate_decimal_precision(value, self.precision)?
        } else {
            value
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::bitmap::Bitmap;
    use crate::buffer::Buffer;
    use crate::error::Result;

    #[test]
    fn test_decimal_builder() {
        let mut builder = DecimalBuilder::new(30, 38, 6);

        builder.append_value(8_887_000_000).unwrap();
        builder.append_null().unwrap();
        builder.append_value(-8_887_000_000).unwrap();
        let decimal_array: DecimalArray = builder.finish();

        assert_eq!(&DataType::Decimal(38, 6), decimal_array.data_type());
        assert_eq!(3, decimal_array.len());
        assert_eq!(1, decimal_array.null_count());
        assert_eq!(32, decimal_array.value_offset(2));
        assert_eq!(16, decimal_array.value_length());
    }
}
