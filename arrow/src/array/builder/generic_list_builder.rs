use std::any::Any;
use std::sync::Arc;

use crate::datatypes::*;
use crate::error::Result;

pub use crate::array::builder::BooleanBufferBuilder;
pub use crate::array::builder::BufferBuilder;
pub use crate::array::ArrayBuilder;
pub use crate::array::ArrayData;
pub use crate::array::ArrayRef;
pub use crate::array::GenericListArray;
pub use crate::array::OffsetSizeTrait;
pub use crate::array::UInt8Builder;

///  Array builder for `ListArray`
#[derive(Debug)]
pub struct GenericListBuilder<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> {
    offsets_builder: BufferBuilder<OffsetSize>,
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: OffsetSize,
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T> {
    /// Creates a new `ListArrayBuilder` from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new `ListArrayBuilder` from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(capacity + 1);
        let len = OffsetSize::zero();
        offsets_builder.append(len);
        Self {
            offsets_builder,
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> ArrayBuilder
    for GenericListBuilder<OffsetSize, T>
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
        self.len.to_usize().unwrap()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len == OffsetSize::zero()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T>
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

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.offsets_builder
            .append(OffsetSize::from_usize(self.values_builder.len()).unwrap());
        self.bitmap_builder.append(is_valid);
        self.len += OffsetSize::one();
        Ok(())
    }

    /// Builds the `ListArray` and reset this builder.
    pub fn finish(&mut self) -> GenericListArray<OffsetSize> {
        let len = self.len();
        self.len = OffsetSize::zero();
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.bitmap_builder.finish();
        self.offsets_builder.append(self.len);
        let field = Box::new(Field::new(
            "item",
            values_data.data_type().clone(),
            true, // TODO: find a consistent way of getting this
        ));
        let data_type = if OffsetSize::IS_LARGE {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };
        let array_data = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(values_data.clone())
            .null_bit_buffer(Some(null_bit_buffer));

        let array_data = unsafe { array_data.build_unchecked() };

        GenericListArray::<OffsetSize>::from(array_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::array::{Int32Array, Int32Builder};
    use crate::bitmap::Bitmap;
    use crate::buffer::Buffer;
    use crate::error::Result;

    pub use crate::array::builder::{
        BinaryBuilder, GenericStringBuilder, LargeBinaryBuilder, LargeListBuilder,
        LargeStringBuilder, ListBuilder, StringBuilder,
    };

    #[test]
    fn test_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_value(4).unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        let values = list_array.values().data().buffers()[0].clone();
        assert_eq!(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]), values);
        assert_eq!(
            Buffer::from_slice_ref(&[0, 3, 6, 8]),
            list_array.data().buffers()[0].clone()
        );
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_large_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = LargeListBuilder::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_value(4).unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        let values = list_array.values().data().buffers()[0].clone();
        assert_eq!(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]), values);
        assert_eq!(
            Buffer::from_slice_ref(&[0i64, 3, 6, 8]),
            list_array.data().buffers()[0].clone()
        );
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_list_array_builder_nulls() {
        let values_builder = Int32Builder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(3, list_array.value_offsets()[2]);
        assert_eq!(3, list_array.value_length(2));
    }

    #[test]
    fn test_large_list_array_builder_nulls() {
        let values_builder = Int32Builder::new(10);
        let mut builder = LargeListBuilder::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(3, list_array.value_offsets()[2]);
        assert_eq!(3, list_array.value_length(2));
    }

    #[test]
    fn test_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = ListBuilder::new(values_builder);

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
    fn test_list_list_array_builder() {
        let primitive_builder = Int32Builder::new(10);
        let values_builder = ListBuilder::new(primitive_builder);
        let mut builder = ListBuilder::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder.values().values().append_value(1).unwrap();
        builder.values().values().append_value(2).unwrap();
        builder.values().append(true).unwrap();
        builder.values().values().append_value(3).unwrap();
        builder.values().values().append_value(4).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.values().values().append_value(5).unwrap();
        builder.values().values().append_value(6).unwrap();
        builder.values().values().append_value(7).unwrap();
        builder.values().append(true).unwrap();
        builder.values().append(false).unwrap();
        builder.values().values().append_value(8).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.append(false).unwrap();

        builder.values().values().append_value(9).unwrap();
        builder.values().values().append_value(10).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        let list_array = builder.finish();

        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[0, 2, 5, 5, 6]),
            list_array.data().buffers()[0].clone()
        );

        assert_eq!(6, list_array.values().data().len());
        assert_eq!(1, list_array.values().data().null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[0, 2, 4, 7, 7, 8, 10]),
            list_array.values().data().buffers()[0].clone()
        );

        assert_eq!(10, list_array.values().data().child_data()[0].len());
        assert_eq!(0, list_array.values().data().child_data()[0].null_count());
        assert_eq!(
            Buffer::from_slice_ref(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            list_array.values().data().child_data()[0].buffers()[0].clone()
        );
    }

    #[test]
    fn test_binary_array_builder() {
        let mut builder = BinaryBuilder::new(20);

        builder.append_byte(b'h').unwrap();
        builder.append_byte(b'e').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_byte(b'w').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append_byte(b'r').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'd').unwrap();
        builder.append(true).unwrap();

        let binary_array = builder.finish();

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(5, binary_array.value_length(2));
    }

    #[test]
    fn test_large_binary_array_builder() {
        let mut builder = LargeBinaryBuilder::new(20);

        builder.append_byte(b'h').unwrap();
        builder.append_byte(b'e').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_byte(b'w').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append_byte(b'r').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'd').unwrap();
        builder.append(true).unwrap();

        let binary_array = builder.finish();

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(5, binary_array.value_length(2));
    }

    #[test]
    fn test_string_array_builder() {
        let mut builder = StringBuilder::new(20);

        builder.append_value("hello").unwrap();
        builder.append(true).unwrap();
        builder.append_value("world").unwrap();

        let string_array = builder.finish();

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("world", string_array.value(2));
        assert_eq!(5, string_array.value_offsets()[2]);
        assert_eq!(5, string_array.value_length(2));
    }

    #[test]
    fn test_string_array_builder_finish() {
        let mut builder = StringBuilder::new(10);

        builder.append_value("hello").unwrap();
        builder.append_value("world").unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.append_value("arrow").unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_string_array_builder_append_string() {
        let mut builder = StringBuilder::new(20);

        let var = "hello".to_owned();
        builder.append_value(&var).unwrap();
        builder.append(true).unwrap();
        builder.append_value("world").unwrap();

        let string_array = builder.finish();

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("world", string_array.value(2));
        assert_eq!(5, string_array.value_offsets()[2]);
        assert_eq!(5, string_array.value_length(2));
    }

    #[test]
    fn test_string_array_builder_append_option() {
        let mut builder = StringBuilder::new(20);
        builder.append_option(Some("hello")).unwrap();
        builder.append_option(None::<&str>).unwrap();
        builder.append_option(None::<String>).unwrap();
        builder.append_option(Some("world")).unwrap();

        let string_array = builder.finish();

        assert_eq!(4, string_array.len());
        assert_eq!("hello", string_array.value(0));
        assert!(string_array.is_null(1));
        assert!(string_array.is_null(2));
        assert_eq!("world", string_array.value(3));
    }
}
