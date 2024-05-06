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

use arrow_buffer::{Buffer, ToByteSlice};
use arrow_schema::ArrowError;
use std::fmt::Formatter;
use std::ops::Range;

/// A `View` is a `u128` value that represents a single value in a
/// [`GenericByteViewArray`].
///
/// Depending on the array type, the value may be a utf8 string or simply bytes.
/// The layout of the u128 is different depending on the length of the bytes
/// stored at that location:
///
/// # 12 or fewer bytes [`InlineView`]
///
/// Values with 12 or fewer bytes are stored directly inlined in the `u128`. See
/// [`InlineView`] for field access.
///
/// ```text
///                      ┌───────────────────────────────────────────┬──────────────┐
///                      │                   data                    │    length    │
///  Strings, len <= 12  │             (padded with \0)              │    (u32)     │
///   (InlineView)       │                                           │              │
///                      └───────────────────────────────────────────┴──────────────┘
///                      127                                        31             0  bit
///                                                                                   offset
/// ```
///
/// # More than 12 bytes [`OffsetView`]
///
/// Values with more than 12 bytes store the first 4 bytes inline, an offset and
/// buffer index that reference the actual data (including the first 4 bytes) in
/// an externally managed buffer. See [`OffsetView`] for field access.
///
/// ```text
///                      ┌──────────────┬─────────────┬──────────────┬──────────────┐
///                      │buffer offset │ buffer index│ data prefix  │    length    │
///  Strings, len > 12   │    (u32)     │    (u32)    │  (4 bytes)   │    (u32)     │
///   (OffsetView)       │              │             │              │              │
///                      └──────────────┴─────────────┴──────────────┴──────────────┘
///                      127            95            63             31            0  bit
///                                                                                   offset
/// ```
///
/// See Also:
/// * [`OwnedView`]: An owned variant of [`View`], used for constructing views
///
/// [`GenericByteViewArray`]: https://docs.rs/arrow/latest/arrow/array/struct.GenericByteViewArray.html
///
/// # Notes
/// Equality is based on the bitwise value of the view, not the data it logically points to
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum View<'a> {
    /// Entire string is inlined
    Inline(InlineView<'a>),
    /// String is stored in buffer, 4 byte prefix stored inline
    Offset(OffsetView<'a>),
}

impl<'a> View<'a> {
    /// Create a new `View` representing the contents of a `u128`
    #[inline(always)]
    pub fn new(v: &'a u128) -> Self {
        let len = *v as u32;
        if len <= 12 {
            Self::Inline(InlineView::from(v))
        } else {
            Self::Offset(OffsetView::from(v))
        }
    }

    /// Convert the view to a `u128`
    pub fn to_u128(&self) -> u128 {
        match self {
            Self::Inline(inline) => inline.to_u128(),
            Self::Offset(offset) => offset.to_u128(),
        }
    }

    /// Return an [`OwnedView`] representing this view
    pub fn to_owned(&self) -> OwnedView {
        OwnedView::new(self.to_u128())
    }
}

impl<'a> From<&'a u128> for View<'a> {
    #[inline(always)]
    fn from(v: &'a u128) -> Self {
        Self::new(v)
    }
}

/// Owned variant of [`View`] for constructing views from a string or byte slice.
///
/// # Example
/// ```
/// # use arrow_data::OwnedView;
/// // contruct a view from a string
/// let view = OwnedView::new_from_str("hello");
/// assert!(matches!(view, OwnedView::Inline(_)));
/// ```
///
/// ```
/// # use arrow_data::OwnedView;
/// // contruct a view from a longer string
/// let view = OwnedView::new_from_str("hello my name is crumple faced fish");
/// assert!(matches!(view, OwnedView::Offset(_)));
/// ```
///
/// # Notes
/// Equality is based on the bitwise value of the view, not the data it logically points to
#[derive(PartialEq)]
pub enum OwnedView {
    /// [`InlineView`]: Data is inlined (12 or fewer bytes)
    Inline(u128),
    /// [`OffsetView`]: Data is stored in a buffer (more than 12 bytes)
    Offset(u128),
}

impl OwnedView {
    /// Create a new `OwnedView` from a preexisting u128 that represents a view.
    ///
    /// Note no validation is done on the u128 (e.g. no length checking)
    pub fn new(v: u128) -> Self {
        let len = v as u32;
        if len <= 12 {
            Self::Inline(v)
        } else {
            Self::Offset(v)
        }
    }

    /// Create a new view from a string
    ///
    /// See [`OwnedView::new_from_bytes`] for more details
    pub fn new_from_str(value: &str) -> Self {
        Self::new_from_bytes(value.as_bytes())
    }

    /// Construct an `OwnedView` from a byte slice
    ///
    /// This function constructs the appropriate view type to represent this
    /// value, inlining the value or prefix as appropriate.
    ///
    /// # Notes:
    /// * Does not manage any buffers / offsets
    /// * A created [`OwnedView::Offset`] has buffer index and offset set to zero
    #[inline(always)]
    pub fn new_from_bytes(v: &[u8]) -> Self {
        let length: u32 = v.len().try_into().unwrap();
        let mut view_buffer = [0; 16];
        view_buffer[0..4].copy_from_slice(&length.to_le_bytes());

        if length <= 12 {
            // copy all values
            view_buffer[4..4 + v.len()].copy_from_slice(v);
            Self::Inline(u128::from_le_bytes(view_buffer))
        } else {
            // copy 4 byte prefix
            view_buffer[4..8].copy_from_slice(&v[0..4]);
            Self::Offset(u128::from_le_bytes(view_buffer))
        }
    }

    // Convert this `OwnedView` to a `View`
    pub fn as_view(&self) -> View {
        match self {
            Self::Inline(inline) => View::Inline(InlineView::from(inline)),
            Self::Offset(offset) => View::Offset(OffsetView::from(offset)),
        }
    }
}

impl std::fmt::Debug for OwnedView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // format with hex bytes
        match self {
            Self::Inline(inline) => write!(f, "OwnedView::Inline({inline:#20x})"),
            Self::Offset(offset) => write!(f, "OwnedView::Offset({offset:#20x})"),
        }
    }
}

impl From<&str> for OwnedView {
    fn from(value: &str) -> Self {
        Self::new_from_str(value)
    }
}

impl From<&[u8]> for OwnedView {
    fn from(value: &[u8]) -> Self {
        Self::new_from_bytes(value)
    }
}

impl From<u128> for OwnedView {
    fn from(value: u128) -> Self {
        Self::new(value)
    }
}

/// A view for data where the variable length data is less than or equal to 12.
///
/// See documentation on [`View`] for details.
///
/// # Notes
/// Note there is no validation done when converting to/from u128
///
/// Equality is based on the bitwise value of the view, not the data it
/// logically points to
#[derive(Copy, Clone, PartialEq)]
pub struct InlineView<'a>(&'a u128);

impl<'a> std::fmt::Debug for InlineView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // format with hex bytes
        write!(f, "InlineView({:#020x})", self.0)
    }
}

impl<'a> InlineView<'a> {
    /// Create a new inline view from a u128
    #[inline(always)]
    pub fn new_from_u128(v: &'a u128) -> Self {
        Self(v)
    }

    /// Return a reference to the u128
    pub fn as_u128(self) -> &'a u128 {
        self.0
    }

    /// Convert this view to a u128
    pub fn to_u128(&self) -> u128 {
        *self.0
    }

    /// Return the length of the data, in bytes
    #[inline(always)]
    pub fn len(&self) -> usize {
        // take first 4 bytes
        let len = *self.0 as u32;
        len as usize
    }

    /// Return true if the length of the data is zero
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access the value of the data, as bytes
    ///
    /// # Panics
    /// If the length is greater than 12 (aka if this view is invalid)
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.to_byte_slice()[4..4 + self.len()]
    }

    /// Access the value of the data, as bytes, unchecked
    ///
    /// # Safety
    /// Undefined behavior if the length is greater than 12
    #[inline(always)]
    pub unsafe fn as_bytes_unchecked(&self) -> &[u8] {
        self.get_bytes_unchecked(self.0)
    }

    /// Access the value of `v`, as bytes, unchecked described by this view
    ///
    /// This method can be used to access the inlined bytes described by this
    /// view directly from a reference to the underlying `u128`.
    ///
    /// # Safety
    /// Undefined behavior if the length is greater than 12
    #[inline(always)]
    pub unsafe fn get_bytes_unchecked<'b>(&self, v: &'b u128) -> &'b [u8] {
        v.to_byte_slice().get_unchecked(4..4 + self.len())
    }
}

impl<'a> From<&'a u128> for InlineView<'a> {
    #[inline(always)]
    fn from(v: &'a u128) -> Self {
        Self::new_from_u128(v)
    }
}

impl<'a> From<InlineView<'a>> for &'a u128 {
    #[inline(always)]
    fn from(view: InlineView<'a>) -> Self {
        view.as_u128()
    }
}

impl<'a> From<InlineView<'a>> for u128 {
    #[inline(always)]
    fn from(view: InlineView) -> Self {
        view.to_u128()
    }
}

/// A view for data where the length variable length data is greater than
/// 12 bytes.
///
/// See documentation on [`View`] for details.
///
/// # Notes
/// There is no validation done when converting to/from u128
///
/// # See Also
/// * [`View`] to determine the correct view type for a given `u128`
/// * [`OffsetViewBuilder`] for modifying the buffer index and offset of an `OffsetView`
#[derive(Copy, Clone, PartialEq)]
pub struct OffsetView<'a>(&'a u128);

impl<'a> std::fmt::Debug for OffsetView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // format with hex bytes
        write!(f, "OffsetView({:#020x})", self.0)
    }
}

impl<'a> OffsetView<'a> {
    /// Create a new inline view from a u128
    #[inline(always)]
    pub fn new_from_u128(v: &'a u128) -> Self {
        Self(v)
    }

    /// Return a reference to the inner u128
    pub fn as_u128(self) -> &'a u128 {
        self.0
    }

    /// Convert this view to a u128
    pub fn to_u128(&self) -> u128 {
        *self.0
    }

    /// Return the length of the data, in bytes
    #[inline(always)]
    pub fn len(&self) -> usize {
        // take first 4 bytes
        let len = *self.0 as u32;
        len as usize
    }

    /// Return true if the view represents an empty string
    ///
    /// # Notes
    ///
    /// Since an `OffsetView` is always greater than 12 bytes, this function
    /// always returns false.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Return the prefix of the data (always 4 bytes)
    #[inline(always)]
    pub fn prefix_as_bytes(&self) -> &[u8] {
        &self.0.to_byte_slice()[4..8]
    }

    /// Return the buffer index
    #[inline(always)]
    pub fn buffer_index(&self) -> u32 {
        (((*self.0) & 0x00000000_ffffffff_00000000_00000000) >> 64) as u32
    }

    /// Return the offset into the buffer
    #[inline(always)]
    pub fn offset(&self) -> u32 {
        (((*self.0) & 0xffffffff_00000000_00000000_00000000) >> 96) as u32
    }

    /// Return the range of the data in the offset buffer
    #[inline(always)]
    pub fn range(&self) -> Range<usize> {
        let offset = self.offset() as usize;
        offset..(offset + self.len())
    }

    /// Return a builder for modifying this view
    pub fn into_builder(&self) -> OffsetViewBuilder {
        OffsetViewBuilder::new_from_u128(*self.0)
    }
}

impl<'a> From<&'a u128> for OffsetView<'a> {
    #[inline(always)]
    fn from(v: &'a u128) -> Self {
        Self::new_from_u128(v)
    }
}

impl<'a> From<OffsetView<'a>> for &'a u128 {
    #[inline(always)]
    fn from(view: OffsetView<'a>) -> Self {
        view.as_u128()
    }
}

impl<'a> From<OffsetView<'a>> for u128 {
    #[inline(always)]
    fn from(view: OffsetView) -> Self {
        view.to_u128()
    }
}

/// Builder for [`OffsetView`]s
///
/// This builder can help set offset and buffer index of an `OffsetView`.
///
/// Note that the builder does not permit changing the length or prefix of the
/// view. To change the length or prefix, create a new `OffsetView` using
/// [`OwnedView::new_from_bytes`].
#[derive(Clone, PartialEq)]
pub struct OffsetViewBuilder(u128);

impl std::fmt::Debug for OffsetViewBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OffsetViewBuilder({:#020x})", self.0)
    }
}

impl OffsetViewBuilder {
    fn new_from_u128(v: u128) -> Self {
        Self(v)
    }

    /// Retrieve the u128 as a OffsetView
    pub fn as_offset_view(&self) -> OffsetView<'_> {
        OffsetView::new_from_u128(&self.0)
    }

    /// Set the buffer index
    pub fn with_buffer_index(self, buffer_index: u32) -> Self {
        Self(self.0 | ((buffer_index as u128) << 64))
    }

    /// Set the offset
    pub fn with_offset(self, offset: u32) -> Self {
        Self(self.0 | ((offset as u128) << 96))
    }

    /// Return the inner u128, consuming the builder
    pub fn build(self) -> u128 {
        self.0
    }
}

impl From<u128> for OffsetViewBuilder {
    fn from(v: u128) -> Self {
        Self::new_from_u128(v)
    }
}

impl From<OffsetViewBuilder> for u128 {
    fn from(builder: OffsetViewBuilder) -> Self {
        builder.build()
    }
}

/// A view for data where the variable length data has 12 or fewer bytes. See
/// [`View`] for details.
///
/// Note: equality for `ByteView` is based on the bitwise value of the view, not
/// the data it logically points to
#[derive(Copy, Clone, Default, PartialEq)]
#[repr(C)]
pub struct ByteView {
    /// The length of the string/bytes.
    pub length: u32,
    /// First 4 bytes of string/bytes data.
    pub prefix: u32,
    /// The buffer index.
    pub buffer_index: u32,
    /// The offset into the buffer.
    pub offset: u32,
}

impl ByteView {
    #[inline(always)]
    pub fn as_u128(self) -> u128 {
        (self.length as u128)
            | ((self.prefix as u128) << 32)
            | ((self.buffer_index as u128) << 64)
            | ((self.offset as u128) << 96)
    }
}

impl From<u128> for ByteView {
    #[inline]
    fn from(value: u128) -> Self {
        Self {
            length: value as u32,
            prefix: (value >> 32) as u32,
            buffer_index: (value >> 64) as u32,
            offset: (value >> 96) as u32,
        }
    }
}

impl From<ByteView> for u128 {
    #[inline]
    fn from(value: ByteView) -> Self {
        value.as_u128()
    }
}

/// Validates the combination of `views` and `buffers` is a valid BinaryView
pub fn validate_binary_view(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError> {
    validate_view_impl(views, buffers, |_, _| Ok(()))
}

/// Validates the combination of `views` and `buffers` is a valid StringView
pub fn validate_string_view(views: &[u128], buffers: &[Buffer]) -> Result<(), ArrowError> {
    validate_view_impl(views, buffers, |idx, b| {
        std::str::from_utf8(b).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "Encountered non-UTF-8 data at index {idx}: {e}"
            ))
        })?;
        Ok(())
    })
}

fn validate_view_impl<F>(views: &[u128], buffers: &[Buffer], f: F) -> Result<(), ArrowError>
where
    F: Fn(usize, &[u8]) -> Result<(), ArrowError>,
{
    for (idx, v) in views.iter().enumerate() {
        let len = *v as u32;
        if len <= 12 {
            if len < 12 && (v >> (32 + len * 8)) != 0 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "View at index {idx} contained non-zero padding for string of length {len}",
                )));
            }
            f(idx, &v.to_le_bytes()[4..4 + len as usize])?;
        } else {
            let view = ByteView::from(*v);
            let data = buffers.get(view.buffer_index as usize).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Invalid buffer index at {idx}: got index {} but only has {} buffers",
                    view.buffer_index,
                    buffers.len()
                ))
            })?;

            let start = view.offset as usize;
            let end = start + len as usize;
            let b = data.get(start..end).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Invalid buffer slice at {idx}: got {start}..{end} but buffer {} has length {}",
                    view.buffer_index,
                    data.len()
                ))
            })?;

            if !b.starts_with(&view.prefix.to_le_bytes()) {
                return Err(ArrowError::InvalidArgumentError(
                    "Mismatch between embedded prefix and data".to_string(),
                ));
            }

            f(idx, b)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn construction_empty() {
        let s = "";
        let v = 0;
        let owned = OwnedView::new_from_str(s);
        assert_eq!(owned, OwnedView::Inline(v));
        assert_eq!(owned, OwnedView::from(v));
        assert_eq!(owned, OwnedView::from(s));
    }

    #[test]
    fn construction_small() {
        let s = "hello";
        // (7 bytes 0 padding, "hello", 5)
        let v = 0x00000000_0000006f_6c6c6568_00000005u128;
        let owned = OwnedView::new_from_str(s);
        assert_eq!(owned, OwnedView::Inline(v));
        assert_eq!(owned, OwnedView::from(v));
        assert_eq!(owned, OwnedView::from(s))
    }

    #[test]
    fn access_empty() {
        let owned = OwnedView::new_from_str("");
        let View::Inline(inline) = owned.as_view() else {
            panic!("unexpected view");
        };

        assert_eq!(inline.len(), 0);
        assert!(inline.is_empty());
        assert_eq!(inline.as_bytes(), []);
    }

    #[test]
    fn access_small() {
        let owned = OwnedView::new_from_str("hello");
        let View::Inline(inline) = owned.as_view() else {
            panic!("unexpected view");
        };
        assert_eq!(inline.len(), 5);
        assert!(!inline.is_empty());
        assert_eq!(inline.as_bytes(), "hello".as_bytes());

        // test accessing as a str (maybe make this unsafe or encapsulate in type system)
    }

    #[test]
    #[should_panic(expected = "range end index 19 out of range for slice of length 16")]
    fn access_small_invalid() {
        // use invalid length 20
        // (7 bytes 0 padding, "hello", 15)
        let v = 0x00000000_0000006f_6c6c6568_0000000fu128;
        let inline = InlineView(&v);
        inline.as_bytes();
    }

    #[test]
    fn construction_large() {
        let s = "hello world here I am";
        // len = 21 (in hex is 0x15)
        // prefix = "hell" (0x6c6c6568)
        // offset/buffer_index = 0
        let v = 0x00000000_00000000_6c6c6568_00000015u128;
        let owned = OwnedView::new_from_str(s);
        let View::Offset(offset) = owned.as_view() else {
            panic!("unexpected view");
        };
        assert_eq!(offset, OffsetView(&v));
        assert_eq!(offset.prefix_as_bytes(), "hell".as_bytes());
    }

    #[test]
    fn access_large() {
        // len = 0xdeadbeef
        // prefix = "frob" (0x66 0x72 0x6f 0x62)
        // offset = 0x12345678
        // buffer_index = 0x87654321
        let v = 0x12345678_87654321_626f7266_deadbeefu128;
        let offset = OffsetView(&v);
        assert_eq!(offset.len(), 0xdeadbeef);
        assert_eq!(offset.buffer_index(), 0x87654321);
        assert_eq!(offset.offset(), 0x12345678);
        assert_eq!(offset.prefix_as_bytes(), "frob".as_bytes());
    }

    #[test]
    fn modification_large() {
        // len = 34 (0x22)
        let v = 0x00000000_00000000_87654321_00000022u128;
        let builder = OffsetViewBuilder::new_from_u128(v);

        let offset = builder.as_offset_view();
        assert_eq!(offset.len(), 34);
        assert_eq!(offset.buffer_index(), 0);
        assert_eq!(offset.offset(), 0);
        assert_eq!(offset.prefix_as_bytes(), [0x21, 0x43, 0x65, 0x87]);

        // modify the buffer index
        let builder = builder.with_buffer_index(0x12345678);
        let offset = builder.as_offset_view();
        assert_eq!(offset.buffer_index(), 0x12345678);
        assert_eq!(offset.offset(), 0);
        assert_eq!(offset.prefix_as_bytes(), [0x21, 0x43, 0x65, 0x87]);
        assert_eq!(offset.to_u128(), 0x00000000_12345678_87654321_00000022u128);

        // modify the offset
        let builder = builder.with_offset(0xfeedbeef);
        let offset = builder.as_offset_view();
        assert_eq!(offset.buffer_index(), 0x12345678);
        assert_eq!(offset.offset(), 0xfeedbeef);
        assert_eq!(offset.prefix_as_bytes(), [0x21, 0x43, 0x65, 0x87]);
        assert_eq!(offset.to_u128(), 0xfeedbeef_12345678_87654321_00000022u128);
    }
}
