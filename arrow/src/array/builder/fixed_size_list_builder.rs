use std::any::Any;
use std::sync::Arc;

use crate::datatypes::*;
use crate::error::Result;

pub use crate::array::builder::BooleanBufferBuilder;
pub use crate::array::builder::BufferBuilder;
pub use crate::array::ArrayBuilder;
pub use crate::array::ArrayData;
pub use crate::array::ArrayRef;
pub use crate::array::FixedSizeListArray;
pub use crate::array::OffsetSizeTrait;

pub use crate::array::Int32BufferBuilder;

///  Array builder for `ListArray`
#[derive(Debug)]
pub struct FixedSizeListBuilder<T: ArrayBuilder> {
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: usize,
    list_len: i32,
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T> {
    /// Creates a new `FixedSizeListBuilder` from a given values array builder
    /// `length` is the number of values within each array
    pub fn new(values_builder: T, length: i32) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, length, capacity)
    }

    /// Creates a new `FixedSizeListBuilder` from a given values array builder
    /// `length` is the number of values within each array
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, length: i32, capacity: usize) -> Self {
        let mut offsets_builder = Int32BufferBuilder::new(capacity + 1);
        offsets_builder.append(0);
        Self {
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len: 0,
            list_len: length,
        }
    }
}

impl<T: ArrayBuilder> ArrayBuilder for FixedSizeListBuilder<T>
where
    T: 'static,
{
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
        self.len
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call `append` to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    pub fn value_length(&self) -> i32 {
        self.list_len
    }

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.bitmap_builder.append(is_valid);
        self.len += 1;
        Ok(())
    }

    /// Builds the `FixedSizeListBuilder` and reset this builder.
    pub fn finish(&mut self) -> FixedSizeListArray {
        let len = self.len();
        self.len = 0;
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        // check that values_data length is multiple of len if we have data
        if len != 0 {
            assert!(
                values_data.len() / len == self.list_len as usize,
                "Values of FixedSizeList must have equal lengths, values have length {} and list has {}",
                values_data.len() / len,
                self.list_len
            );
        }

        let null_bit_buffer = self.bitmap_builder.finish();
        let array_data = ArrayData::builder(DataType::FixedSizeList(
            Box::new(Field::new("item", values_data.data_type().clone(), true)),
            self.list_len,
        ))
        .len(len)
        .add_child_data(values_data.clone())
        .null_bit_buffer(Some(null_bit_buffer));

        let array_data = unsafe { array_data.build_unchecked() };

        FixedSizeListArray::from(array_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::bitmap::Bitmap;
    use crate::buffer::Buffer;
    use crate::error::Result;

    use crate::array::builder::FixedSizeBinaryArray;
    use crate::array::builder::FixedSizeBinaryBuilder;
    use crate::array::Int32Array;
    use crate::array::Int32Builder;

    #[test]
    fn test_fixed_size_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7, null]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.values().append_null().unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length());
    }

    #[test]
    fn test_fixed_size_list_array_builder_empty() {
        let values_builder = Int32Array::builder(5);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        let arr = builder.finish();
        assert_eq!(0, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_fixed_size_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        builder.values().append_slice(&[1, 2, 3]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_slice(&[4, 5, 6]).unwrap();
        builder.append(true).unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.values().append_slice(&[7, 8, 9]).unwrap();
        builder.append(true).unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_fixed_size_binary_builder() {
        let mut builder = FixedSizeBinaryBuilder::new(15, 5);

        //  [b"hello", null, "arrow"]
        builder.append_value(b"hello").unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"arrow").unwrap();
        let fixed_size_binary_array: FixedSizeBinaryArray = builder.finish();

        assert_eq!(
            &DataType::FixedSizeBinary(5),
            fixed_size_binary_array.data_type()
        );
        assert_eq!(3, fixed_size_binary_array.len());
        assert_eq!(1, fixed_size_binary_array.null_count());
        assert_eq!(10, fixed_size_binary_array.value_offset(2));
        assert_eq!(5, fixed_size_binary_array.value_length());
    }
}
