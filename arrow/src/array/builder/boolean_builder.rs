use std::any::Any;
use std::sync::Arc;

use crate::datatypes::*;

use crate::error::{ArrowError, Result};

pub use crate::array::ArrayBuilder;
pub use crate::array::ArrayData;
pub use crate::array::ArrayRef;
pub use crate::array::BooleanArray;

pub use crate::array::builder::boolean_buffer_builder::BooleanBufferBuilder;

///  Array builder for fixed-width primitive types
///
/// # Example
///
/// Create a `BooleanArray` from a `BooleanBuilder`
///
/// ```
///     use arrow::array::{Array, BooleanArray, BooleanBuilder};
///
///     let mut b = BooleanBuilder::new(4);
///     b.append_value(true);
///     b.append_null();
///     b.append_value(false);
///     b.append_value(true);
///     let arr = b.finish();
///
///     assert_eq!(4, arr.len());
///     assert_eq!(1, arr.null_count());
///     assert_eq!(true, arr.value(0));
///     assert!(arr.is_valid(0));
///     assert!(!arr.is_null(0));
///     assert!(!arr.is_valid(1));
///     assert!(arr.is_null(1));
///     assert_eq!(false, arr.value(2));
///     assert!(arr.is_valid(2));
///     assert!(!arr.is_null(2));
///     assert_eq!(true, arr.value(3));
///     assert!(arr.is_valid(3));
///     assert!(!arr.is_null(3));
/// ```
#[derive(Debug)]
pub struct BooleanBuilder {
    values_builder: BooleanBufferBuilder,
    bitmap_builder: BooleanBufferBuilder,
}

impl BooleanBuilder {
    /// Creates a new primitive array builder
    pub fn new(capacity: usize) -> Self {
        Self {
            values_builder: BooleanBufferBuilder::new(capacity),
            bitmap_builder: BooleanBufferBuilder::new(capacity),
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: bool) -> Result<()> {
        self.bitmap_builder.append(true);
        self.values_builder.append(v);
        Ok(())
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) -> Result<()> {
        self.bitmap_builder.append(false);
        self.values_builder.advance(1);
        Ok(())
    }

    /// Appends an `Option<T>` into the builder
    #[inline]
    pub fn append_option(&mut self, v: Option<bool>) -> Result<()> {
        match v {
            None => self.append_null()?,
            Some(v) => self.append_value(v)?,
        };
        Ok(())
    }

    /// Appends a slice of type `T` into the builder
    #[inline]
    pub fn append_slice(&mut self, v: &[bool]) -> Result<()> {
        self.bitmap_builder.append_n(v.len(), true);
        self.values_builder.append_slice(v);
        Ok(())
    }

    /// Appends values from a slice of type `T` and a validity boolean slice
    #[inline]
    pub fn append_values(&mut self, values: &[bool], is_valid: &[bool]) -> Result<()> {
        if values.len() != is_valid.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Value and validity lengths must be equal".to_string(),
            ));
        }
        self.bitmap_builder.append_slice(is_valid);
        self.values_builder.append_slice(values);
        Ok(())
    }

    /// Builds the [BooleanArray] and reset this builder.
    pub fn finish(&mut self) -> BooleanArray {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = len - null_bit_buffer.count_set_bits();
        let builder = ArrayData::builder(DataType::Boolean)
            .len(len)
            .add_buffer(self.values_builder.finish())
            .null_bit_buffer((null_count > 0).then(|| null_bit_buffer));

        let array_data = unsafe { builder.build_unchecked() };
        BooleanArray::from(array_data)
    }
}

impl ArrayBuilder for BooleanBuilder {
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
        self.values_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.values_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
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
    fn test_boolean_array_builder_append_slice() {
        let arr1 =
            BooleanArray::from(vec![Some(true), Some(false), None, None, Some(false)]);

        let mut builder = BooleanArray::builder(0);
        builder.append_slice(&[true, false]).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(false).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_boolean_array_builder_append_slice_large() {
        let arr1 = BooleanArray::from(vec![true; 513]);

        let mut builder = BooleanArray::builder(512);
        builder.append_slice(&[true; 513]).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1, arr2);
    }
}
