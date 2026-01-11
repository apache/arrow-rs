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

use crate::builder::ArrayBuilder;
use crate::types::{ByteArrayType, GenericBinaryType, GenericStringType};
use crate::{Array, ArrayRef, GenericByteArray, OffsetSizeTrait};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer, NullBufferBuilder, ScalarBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;
use std::any::Any;
use std::sync::Arc;

/// Builder for [`GenericByteArray`]
///
/// For building strings, see docs on [`GenericStringBuilder`].
/// For building binary, see docs on [`GenericBinaryBuilder`].
pub struct GenericByteBuilder<T: ByteArrayType> {
    value_builder: Vec<u8>,
    offsets_builder: Vec<T::Offset>,
    null_buffer_builder: NullBufferBuilder,
}

impl<T: ByteArrayType> GenericByteBuilder<T> {
    /// Creates a new [`GenericByteBuilder`].
    pub fn new() -> Self {
        Self::with_capacity(1024, 1024)
    }

    /// Creates a new [`GenericByteBuilder`].
    ///
    /// - `item_capacity` is the number of items to pre-allocate.
    ///   The size of the preallocated buffer of offsets is the number of items plus one.
    /// - `data_capacity` is the total number of bytes of data to pre-allocate
    ///   (for all items, not per item).
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let mut offsets_builder = Vec::with_capacity(item_capacity + 1);
        offsets_builder.push(T::Offset::from_usize(0).unwrap());
        Self {
            value_builder: Vec::with_capacity(data_capacity),
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(item_capacity),
        }
    }

    /// Creates a new  [`GenericByteBuilder`] from buffers.
    ///
    /// # Safety
    ///
    /// This doesn't verify buffer contents as it assumes the buffers are from
    /// existing and valid [`GenericByteArray`].
    pub unsafe fn new_from_buffer(
        offsets_buffer: MutableBuffer,
        value_buffer: MutableBuffer,
        null_buffer: Option<MutableBuffer>,
    ) -> Self {
        let offsets_builder: Vec<T::Offset> =
            ScalarBuffer::<T::Offset>::from(offsets_buffer).into();
        let value_builder: Vec<u8> = ScalarBuffer::<u8>::from(value_buffer).into();

        let null_buffer_builder = null_buffer
            .map(|buffer| NullBufferBuilder::new_from_buffer(buffer, offsets_builder.len() - 1))
            .unwrap_or_else(|| NullBufferBuilder::new_with_len(offsets_builder.len() - 1));

        Self {
            offsets_builder,
            value_builder,
            null_buffer_builder,
        }
    }

    #[inline]
    fn next_offset(&self) -> T::Offset {
        T::Offset::from_usize(self.value_builder.len()).expect("byte array offset overflow")
    }

    /// Appends a value into the builder.
    ///
    /// See the [GenericStringBuilder] documentation for examples of
    /// incrementally building string values with multiple `write!` calls.
    ///
    /// # Panics
    ///
    /// Panics if the resulting length of [`Self::values_slice`] would exceed
    /// `T::Offset::MAX` bytes.
    ///
    /// For example, this can happen with [`StringArray`] or [`BinaryArray`]
    /// where the total length of all values exceeds 2GB
    ///
    /// [`StringArray`]: crate::StringArray
    /// [`BinaryArray`]: crate::BinaryArray
    #[inline]
    pub fn append_value(&mut self, value: impl AsRef<T::Native>) {
        self.value_builder
            .extend_from_slice(value.as_ref().as_ref());
        self.null_buffer_builder.append(true);
        self.offsets_builder.push(self.next_offset());
    }

    /// Append an `Option` value into the builder.
    ///
    /// - A `None` value will append a null value.
    /// - A `Some` value will append the value.
    ///
    /// See [`Self::append_value`] for more panic information.
    #[inline]
    pub fn append_option(&mut self, value: Option<impl AsRef<T::Native>>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        };
    }

    /// Append a null value into the builder.
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append(false);
        self.offsets_builder.push(self.next_offset());
    }

    /// Appends `n` `null`s into the builder.
    #[inline]
    pub fn append_nulls(&mut self, n: usize) {
        self.null_buffer_builder.append_n_nulls(n);
        let next_offset = self.next_offset();
        self.offsets_builder
            .extend(std::iter::repeat_n(next_offset, n));
    }

    /// Appends array values and null to this builder as is
    /// (this means that underlying null values are copied as is).
    #[inline]
    pub fn append_array(&mut self, array: &GenericByteArray<T>) -> Result<(), ArrowError> {
        use num_traits::CheckedAdd;
        if array.len() == 0 {
            return Ok(());
        }

        let offsets = array.offsets();

        // If the offsets are contiguous, we can append them directly avoiding the need to align
        // for example, when the first appended array is not sliced (starts at offset 0)
        if self.next_offset() == offsets[0] {
            self.offsets_builder.extend_from_slice(&offsets[1..]);
        } else {
            // Shifting all the offsets
            let shift: T::Offset = self.next_offset() - offsets[0];

            if shift.checked_add(&offsets[offsets.len() - 1]).is_none() {
                return Err(ArrowError::OffsetOverflowError(
                    shift.as_usize() + offsets[offsets.len() - 1].as_usize(),
                ));
            }

            self.offsets_builder
                .extend(offsets[1..].iter().map(|&offset| offset + shift));
        }

        // Append underlying values, starting from the first offset and ending at the last offset
        self.value_builder.extend_from_slice(
            &array.values().as_slice()[offsets[0].as_usize()..offsets[array.len()].as_usize()],
        );

        if let Some(null_buffer) = array.nulls() {
            self.null_buffer_builder.append_buffer(null_buffer);
        } else {
            self.null_buffer_builder.append_n_non_nulls(array.len());
        }
        Ok(())
    }

    /// Builds the [`GenericByteArray`] and reset this builder.
    pub fn finish(&mut self) -> GenericByteArray<T> {
        let array_type = T::DATA_TYPE;
        let array_builder = ArrayDataBuilder::new(array_type)
            .len(self.len())
            .add_buffer(std::mem::take(&mut self.offsets_builder).into())
            .add_buffer(std::mem::take(&mut self.value_builder).into())
            .nulls(self.null_buffer_builder.finish());

        self.offsets_builder.push(self.next_offset());
        let array_data = unsafe { array_builder.build_unchecked() };
        GenericByteArray::from(array_data)
    }

    /// Builds the [`GenericByteArray`] without resetting the builder.
    pub fn finish_cloned(&self) -> GenericByteArray<T> {
        let array_type = T::DATA_TYPE;
        let offset_buffer = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let value_buffer = Buffer::from_slice_ref(self.value_builder.as_slice());
        let array_builder = ArrayDataBuilder::new(array_type)
            .len(self.len())
            .add_buffer(offset_buffer)
            .add_buffer(value_buffer)
            .nulls(self.null_buffer_builder.finish_cloned());

        let array_data = unsafe { array_builder.build_unchecked() };
        GenericByteArray::from(array_data)
    }

    /// Returns the current values buffer as a slice
    pub fn values_slice(&self) -> &[u8] {
        self.value_builder.as_slice()
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[T::Offset] {
        self.offsets_builder.as_slice()
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
    }

    /// Returns the current null buffer as a mutable slice
    pub fn validity_slice_mut(&mut self) -> Option<&mut [u8]> {
        self.null_buffer_builder.as_slice_mut()
    }
}

impl<T: ByteArrayType> std::fmt::Debug for GenericByteBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}Builder", T::Offset::PREFIX, T::PREFIX)?;
        f.debug_struct("")
            .field("value_builder", &self.value_builder)
            .field("offsets_builder", &self.offsets_builder)
            .field("null_buffer_builder", &self.null_buffer_builder)
            .finish()
    }
}

impl<T: ByteArrayType> Default for GenericByteBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ByteArrayType> ArrayBuilder for GenericByteBuilder<T> {
    /// Returns the number of binary slots in the builder
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

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
}

impl<T: ByteArrayType, V: AsRef<T::Native>> Extend<Option<V>> for GenericByteBuilder<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = Option<V>>>(&mut self, iter: I) {
        for v in iter {
            self.append_option(v)
        }
    }
}

/// Array builder for [`GenericStringArray`][crate::GenericStringArray]
///
/// Values can be appended using [`GenericByteBuilder::append_value`], and nulls with
/// [`GenericByteBuilder::append_null`].
///
/// This builder also implements [`std::fmt::Write`] with any written data
/// included in the next appended value. This allows using [`std::fmt::Display`]
/// with standard Rust idioms like `write!` and `writeln!` to write data
/// directly to the builder without intermediate allocations.
///
/// # Example writing strings with `append_value`
/// ```
/// # use arrow_array::builder::GenericStringBuilder;
/// let mut builder = GenericStringBuilder::<i32>::new();
///
/// // Write one string value
/// builder.append_value("foobarbaz");
///
/// // Write a second string
/// builder.append_value("v2");
///
/// let array = builder.finish();
/// assert_eq!(array.value(0), "foobarbaz");
/// assert_eq!(array.value(1), "v2");
/// ```
///
/// # Example incrementally writing strings with `std::fmt::Write`
///
/// ```
/// # use std::fmt::Write;
/// # use arrow_array::builder::GenericStringBuilder;
/// let mut builder = GenericStringBuilder::<i32>::new();
///
/// // Write data in multiple `write!` calls
/// write!(builder, "foo").unwrap();
/// write!(builder, "bar").unwrap();
/// // The next call to append_value finishes the current string
/// // including all previously written strings.
/// builder.append_value("baz");
///
/// // Write second value with a single write call
/// write!(builder, "v2").unwrap();
/// // finish the value by calling append_value with an empty string
/// builder.append_value("");
///
/// let array = builder.finish();
/// assert_eq!(array.value(0), "foobarbaz");
/// assert_eq!(array.value(1), "v2");
/// ```
pub type GenericStringBuilder<O> = GenericByteBuilder<GenericStringType<O>>;

impl<O: OffsetSizeTrait> std::fmt::Write for GenericStringBuilder<O> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.value_builder.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

/// A byte size value representing the number of bytes to allocate per string in [`GenericStringBuilder`]
///
/// To create a [`GenericStringBuilder`] using `.with_capacity` we are required to provide: \
/// - `item_capacity` - the row count \
/// - `data_capacity` - total string byte count \
///
/// We will use the `AVERAGE_STRING_LENGTH` * row_count for `data_capacity`. \
///
/// These capacities are preallocation hints used to improve performance,
/// but consequences of passing a hint too large or too small should be negligible.
const AVERAGE_STRING_LENGTH: usize = 16;
/// Trait for string-like array builders
///
/// This trait provides unified interface for builders that append string-like data
/// such as [`GenericStringBuilder<O>`] and [`crate::builder::StringViewBuilder`]
pub trait StringLikeArrayBuilder: ArrayBuilder {
    /// Returns a human-readable type name for the builder.
    fn type_name() -> &'static str;

    /// Creates a new builder with the given row capacity.
    fn with_capacity(capacity: usize) -> Self;

    /// Appends a non-null string value to the builder.
    fn append_value(&mut self, value: &str);

    /// Appends a null value to the builder.
    fn append_null(&mut self);
}

impl<O: OffsetSizeTrait> StringLikeArrayBuilder for GenericStringBuilder<O> {
    fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity, capacity * AVERAGE_STRING_LENGTH)
    }
    fn append_value(&mut self, value: &str) {
        Self::append_value(self, value);
    }
    fn append_null(&mut self) {
        Self::append_null(self);
    }
}

/// A byte size value representing the number of bytes to allocate per binary in [`GenericBinaryBuilder`]
///
/// To create a [`GenericBinaryBuilder`] using `.with_capacity` we are required to provide: \
/// - `item_capacity` - the row count \
/// - `data_capacity` - total binary byte count \
///
/// We will use the `AVERAGE_BINARY_LENGTH` * row_count for `data_capacity`. \
///
/// These capacities are preallocation hints used to improve performance,
/// but consequences of passing a hint too large or too small should be negligible.
const AVERAGE_BINARY_LENGTH: usize = 128;
/// Trait for binary-like array builders
///
/// This trait provides unified interface for builders that append binary-like data
/// such as [`GenericBinaryBuilder<O>`] and [`crate::builder::BinaryViewBuilder`]
pub trait BinaryLikeArrayBuilder: ArrayBuilder {
    /// Returns a human-readable type name for the builder.
    fn type_name() -> &'static str;

    /// Creates a new builder with the given row capacity.
    fn with_capacity(capacity: usize) -> Self;

    /// Appends a non-null string value to the builder.
    fn append_value(&mut self, value: &[u8]);

    /// Appends a null value to the builder.
    fn append_null(&mut self);
}

impl<O: OffsetSizeTrait> BinaryLikeArrayBuilder for GenericBinaryBuilder<O> {
    fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
    fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity, capacity * AVERAGE_BINARY_LENGTH)
    }
    fn append_value(&mut self, value: &[u8]) {
        Self::append_value(self, value);
    }
    fn append_null(&mut self) {
        Self::append_null(self);
    }
}

///  Array builder for [`GenericBinaryArray`][crate::GenericBinaryArray]
///
/// Values can be appended using [`GenericByteBuilder::append_value`], and nulls with
/// [`GenericByteBuilder::append_null`].
///
/// # Example
/// ```
/// # use arrow_array::builder::GenericBinaryBuilder;
/// let mut builder = GenericBinaryBuilder::<i32>::new();
///
/// // Write data
/// builder.append_value("foo");
///
/// // Write second value
/// builder.append_value(&[0,1,2]);
///
/// let array = builder.finish();
/// // binary values
/// assert_eq!(array.value(0), b"foo");
/// assert_eq!(array.value(1), b"\x00\x01\x02");
/// ```
///
/// # Example incrementally writing bytes with `write_bytes`
///
/// ```
/// # use std::io::Write;
/// # use arrow_array::builder::GenericBinaryBuilder;
/// let mut builder = GenericBinaryBuilder::<i32>::new();
///
/// // Write data in multiple `write_bytes` calls
/// write!(builder, "foo").unwrap();
/// write!(builder, "bar").unwrap();
/// // The next call to append_value finishes the current string
/// // including all previously written strings.
/// builder.append_value("baz");
///
/// // Write second value with a single write call
/// write!(builder, "v2").unwrap();
/// // finish the value by calling append_value with an empty string
/// builder.append_value("");
///
/// let array = builder.finish();
/// assert_eq!(array.value(0), "foobarbaz".as_bytes());
/// assert_eq!(array.value(1), "v2".as_bytes());
/// ```
pub type GenericBinaryBuilder<O> = GenericByteBuilder<GenericBinaryType<O>>;

impl<O: OffsetSizeTrait> std::io::Write for GenericBinaryBuilder<O> {
    fn write(&mut self, bs: &[u8]) -> std::io::Result<usize> {
        self.value_builder.extend_from_slice(bs);
        Ok(bs.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GenericStringArray;
    use crate::array::Array;
    use arrow_buffer::NullBuffer;
    use std::fmt::Write as _;
    use std::io::Write as _;

    fn _test_generic_binary_builder<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");

        let array = builder.finish();

        assert_eq!(4, array.len());
        assert_eq!(1, array.null_count());
        assert_eq!(b"hello", array.value(0));
        assert_eq!([] as [u8; 0], array.value(1));
        assert!(array.is_null(2));
        assert_eq!(b"rust", array.value(3));
        assert_eq!(O::from_usize(5).unwrap(), array.value_offsets()[2]);
        assert_eq!(O::from_usize(4).unwrap(), array.value_length(3));
    }

    #[test]
    fn test_binary_builder() {
        _test_generic_binary_builder::<i32>()
    }

    #[test]
    fn test_large_binary_builder() {
        _test_generic_binary_builder::<i64>()
    }

    fn _test_generic_binary_builder_all_nulls<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();
        builder.append_null();
        builder.append_null();
        builder.append_null();
        builder.append_nulls(2);
        assert_eq!(5, builder.len());
        assert!(!builder.is_empty());

        let array = builder.finish();
        assert_eq!(5, array.null_count());
        assert_eq!(5, array.len());
        assert!(array.is_null(0));
        assert!(array.is_null(1));
        assert!(array.is_null(2));
        assert!(array.is_null(3));
        assert!(array.is_null(4));
    }

    #[test]
    fn test_binary_builder_all_nulls() {
        _test_generic_binary_builder_all_nulls::<i32>()
    }

    #[test]
    fn test_large_binary_builder_all_nulls() {
        _test_generic_binary_builder_all_nulls::<i64>()
    }

    fn _test_generic_binary_builder_reset<O: OffsetSizeTrait>() {
        let mut builder = GenericBinaryBuilder::<O>::new();

        builder.append_value(b"hello");
        builder.append_value(b"");
        builder.append_null();
        builder.append_value(b"rust");
        builder.finish();

        assert!(builder.is_empty());

        builder.append_value(b"parquet");
        builder.append_null();
        builder.append_value(b"arrow");
        builder.append_value(b"");
        builder.append_nulls(2);
        builder.append_value(b"hi");
        let array = builder.finish();

        assert_eq!(7, array.len());
        assert_eq!(3, array.null_count());
        assert_eq!(b"parquet", array.value(0));
        assert!(array.is_null(1));
        assert!(array.is_null(4));
        assert!(array.is_null(5));
        assert_eq!(b"arrow", array.value(2));
        assert_eq!(b"", array.value(1));
        assert_eq!(b"hi", array.value(6));

        assert_eq!(O::zero(), array.value_offsets()[0]);
        assert_eq!(O::from_usize(7).unwrap(), array.value_offsets()[2]);
        assert_eq!(O::from_usize(14).unwrap(), array.value_offsets()[7]);
        assert_eq!(O::from_usize(5).unwrap(), array.value_length(2));
    }

    #[test]
    fn test_binary_builder_reset() {
        _test_generic_binary_builder_reset::<i32>()
    }

    #[test]
    fn test_large_binary_builder_reset() {
        _test_generic_binary_builder_reset::<i64>()
    }

    fn _test_generic_string_array_builder<O: OffsetSizeTrait>() {
        let mut builder = GenericStringBuilder::<O>::new();
        let owned = "arrow".to_owned();

        builder.append_value("hello");
        builder.append_value("");
        builder.append_value(&owned);
        builder.append_null();
        builder.append_option(Some("rust"));
        builder.append_option(None::<&str>);
        builder.append_option(None::<String>);
        builder.append_nulls(2);
        builder.append_value("parquet");
        assert_eq!(10, builder.len());

        assert_eq!(
            GenericStringArray::<O>::from(vec![
                Some("hello"),
                Some(""),
                Some("arrow"),
                None,
                Some("rust"),
                None,
                None,
                None,
                None,
                Some("parquet")
            ]),
            builder.finish()
        );
    }

    #[test]
    fn test_string_array_builder() {
        _test_generic_string_array_builder::<i32>()
    }

    #[test]
    fn test_large_string_array_builder() {
        _test_generic_string_array_builder::<i64>()
    }

    fn _test_generic_string_array_builder_finish<O: OffsetSizeTrait>() {
        let mut builder = GenericStringBuilder::<O>::with_capacity(3, 11);

        builder.append_value("hello");
        builder.append_value("rust");
        builder.append_null();

        builder.finish();
        assert!(builder.is_empty());
        assert_eq!(&[O::zero()], builder.offsets_slice());

        builder.append_value("arrow");
        builder.append_value("parquet");
        let arr = builder.finish();
        // array should not have null buffer because there is not `null` value.
        assert!(arr.nulls().is_none());
        assert_eq!(GenericStringArray::<O>::from(vec!["arrow", "parquet"]), arr,)
    }

    #[test]
    fn test_string_array_builder_finish() {
        _test_generic_string_array_builder_finish::<i32>()
    }

    #[test]
    fn test_large_string_array_builder_finish() {
        _test_generic_string_array_builder_finish::<i64>()
    }

    fn _test_generic_string_array_builder_finish_cloned<O: OffsetSizeTrait>() {
        let mut builder = GenericStringBuilder::<O>::with_capacity(3, 11);

        builder.append_value("hello");
        builder.append_value("rust");
        builder.append_null();

        let mut arr = builder.finish_cloned();
        assert!(!builder.is_empty());
        assert_eq!(3, arr.len());

        builder.append_value("arrow");
        builder.append_value("parquet");
        arr = builder.finish();

        assert!(arr.nulls().is_some());
        assert_eq!(&[O::zero()], builder.offsets_slice());
        assert_eq!(5, arr.len());
    }

    #[test]
    fn test_string_array_builder_finish_cloned() {
        _test_generic_string_array_builder_finish_cloned::<i32>()
    }

    #[test]
    fn test_large_string_array_builder_finish_cloned() {
        _test_generic_string_array_builder_finish_cloned::<i64>()
    }

    #[test]
    fn test_extend() {
        let mut builder = GenericStringBuilder::<i32>::new();
        builder.extend(["a", "b", "c", "", "a", "b", "c"].into_iter().map(Some));
        builder.extend(["d", "cupcakes", "hello"].into_iter().map(Some));
        let array = builder.finish();
        assert_eq!(array.value_offsets(), &[0, 1, 2, 3, 3, 4, 5, 6, 7, 15, 20]);
        assert_eq!(array.value_data(), b"abcabcdcupcakeshello");
    }

    #[test]
    fn test_write_str() {
        let mut builder = GenericStringBuilder::<i32>::new();
        write!(builder, "foo").unwrap();
        builder.append_value("");
        writeln!(builder, "bar").unwrap();
        builder.append_value("");
        write!(builder, "fiz").unwrap();
        write!(builder, "buz").unwrap();
        builder.append_value("");
        let a = builder.finish();
        let r: Vec<_> = a.iter().flatten().collect();
        assert_eq!(r, &["foo", "bar\n", "fizbuz"])
    }

    #[test]
    fn test_write_bytes() {
        let mut builder = GenericBinaryBuilder::<i32>::new();
        write!(builder, "foo").unwrap();
        builder.append_value("");
        writeln!(builder, "bar").unwrap();
        builder.append_value("");
        write!(builder, "fiz").unwrap();
        write!(builder, "buz").unwrap();
        builder.append_value("");
        let a = builder.finish();
        let r: Vec<_> = a.iter().flatten().collect();
        assert_eq!(
            r,
            &["foo".as_bytes(), "bar\n".as_bytes(), "fizbuz".as_bytes()]
        )
    }

    #[test]
    fn test_append_array_without_nulls() {
        let input = vec![
            "hello", "world", "how", "are", "you", "doing", "today", "I", "am", "doing", "well",
            "thank", "you", "for", "asking",
        ];
        let arr1 = GenericStringArray::<i32>::from(input[..3].to_vec());
        let arr2 = GenericStringArray::<i32>::from(input[3..7].to_vec());
        let arr3 = GenericStringArray::<i32>::from(input[7..].to_vec());

        let mut builder = GenericStringBuilder::<i32>::new();
        builder.append_array(&arr1).unwrap();
        builder.append_array(&arr2).unwrap();
        builder.append_array(&arr3).unwrap();

        let actual = builder.finish();
        let expected = GenericStringArray::<i32>::from(input);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_append_array_with_nulls() {
        let input = vec![
            Some("hello"),
            None,
            Some("how"),
            None,
            None,
            None,
            None,
            Some("I"),
            Some("am"),
            Some("doing"),
            Some("well"),
        ];
        let arr1 = GenericStringArray::<i32>::from(input[..3].to_vec());
        let arr2 = GenericStringArray::<i32>::from(input[3..7].to_vec());
        let arr3 = GenericStringArray::<i32>::from(input[7..].to_vec());

        let mut builder = GenericStringBuilder::<i32>::new();
        builder.append_array(&arr1).unwrap();
        builder.append_array(&arr2).unwrap();
        builder.append_array(&arr3).unwrap();

        let actual = builder.finish();
        let expected = GenericStringArray::<i32>::from(input);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_append_empty_array() {
        let arr = GenericStringArray::<i32>::from(Vec::<&str>::new());
        let mut builder = GenericStringBuilder::<i32>::new();
        builder.append_array(&arr).unwrap();
        let result = builder.finish();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_append_array_with_offset_not_starting_at_0() {
        let input = vec![
            Some("hello"),
            None,
            Some("how"),
            None,
            None,
            None,
            None,
            Some("I"),
            Some("am"),
            Some("doing"),
            Some("well"),
        ];
        let full_array = GenericStringArray::<i32>::from(input);
        let sliced = full_array.slice(1, 4);

        assert_ne!(sliced.offsets()[0].as_usize(), 0);
        assert_ne!(sliced.offsets().last(), full_array.offsets().last());

        let mut builder = GenericStringBuilder::<i32>::new();
        builder.append_array(&sliced).unwrap();
        let actual = builder.finish();

        let expected = GenericStringArray::<i32>::from(vec![None, Some("how"), None, None]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_append_underlying_null_values_added_as_is() {
        let input_1_array_with_nulls = {
            let input = vec![
                "hello", "world", "how", "are", "you", "doing", "today", "I", "am",
            ];
            let (offsets, buffer, _) = GenericStringArray::<i32>::from(input).into_parts();

            GenericStringArray::<i32>::new(
                offsets,
                buffer,
                Some(NullBuffer::from(&[
                    true, false, true, false, false, true, true, true, false,
                ])),
            )
        };
        let input_2_array_with_nulls = {
            let input = vec!["doing", "well", "thank", "you", "for", "asking"];
            let (offsets, buffer, _) = GenericStringArray::<i32>::from(input).into_parts();

            GenericStringArray::<i32>::new(
                offsets,
                buffer,
                Some(NullBuffer::from(&[false, false, true, false, true, true])),
            )
        };

        let mut builder = GenericStringBuilder::<i32>::new();
        builder.append_array(&input_1_array_with_nulls).unwrap();
        builder.append_array(&input_2_array_with_nulls).unwrap();

        let actual = builder.finish();
        let expected = GenericStringArray::<i32>::from(vec![
            Some("hello"),
            None, // world
            Some("how"),
            None, // are
            None, // you
            Some("doing"),
            Some("today"),
            Some("I"),
            None, // am
            None, // doing
            None, // well
            Some("thank"),
            None, // "you",
            Some("for"),
            Some("asking"),
        ]);

        assert_eq!(actual, expected);

        let expected_underlying_buffer = Buffer::from(
            [
                "hello", "world", "how", "are", "you", "doing", "today", "I", "am", "doing",
                "well", "thank", "you", "for", "asking",
            ]
            .join("")
            .as_bytes(),
        );
        assert_eq!(actual.values(), &expected_underlying_buffer);
    }

    #[test]
    fn append_array_with_continues_indices() {
        let input = vec![
            "hello", "world", "how", "are", "you", "doing", "today", "I", "am", "doing", "well",
            "thank", "you", "for", "asking",
        ];
        let full_array = GenericStringArray::<i32>::from(input);
        let slice1 = full_array.slice(0, 3);
        let slice2 = full_array.slice(3, 4);
        let slice3 = full_array.slice(7, full_array.len() - 7);

        let mut builder = GenericStringBuilder::<i32>::new();
        builder.append_array(&slice1).unwrap();
        builder.append_array(&slice2).unwrap();
        builder.append_array(&slice3).unwrap();

        let actual = builder.finish();

        assert_eq!(actual, full_array);
    }

    #[test]
    fn test_append_array_offset_overflow_precise() {
        let mut builder = GenericStringBuilder::<i32>::new();

        let initial_string = "x".repeat(i32::MAX as usize - 100);
        builder.append_value(&initial_string);

        let overflow_string = "y".repeat(200);
        let overflow_array = GenericStringArray::<i32>::from(vec![overflow_string.as_str()]);

        let result = builder.append_array(&overflow_array);

        assert!(matches!(result, Err(ArrowError::OffsetOverflowError(_))));
    }
}
