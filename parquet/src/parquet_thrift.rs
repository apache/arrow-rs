// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Structs used for encoding and decoding Parquet Thrift objects.
//!
//! These include:
//! * [`ThriftCompactInputProtocol`]: Trait implemented by Thrift decoders.
//!     * [`ThriftSliceInputProtocol`]: Thrift decoder that takes a slice of bytes as input.
//!     * [`ThriftReadInputProtocol`]: Thrift decoder that takes a [`Read`] as input.
//! * [`ReadThrift`]: Trait implemented by serializable objects.
//! * [`ThriftCompactOutputProtocol`]: Thrift encoder.
//! * [`WriteThrift`]: Trait implemented by serializable objects.
//! * [`WriteThriftField`]: Trait implemented by serializable objects that are fields in Thrift structs.

#[cfg(test)]
use crate::file::metadata::extensions::ThriftStruct;
use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::{
    errors::{ParquetError, Result},
    file::metadata::extensions::{FooterScope, WriteState},
    write_thrift_field,
};
use std::io::Error;
use std::num::TryFromIntError;
use std::str::Utf8Error;

/// Maximum recursion budget for skipping one unknown Thrift value.
///
/// This is a defensive implementation limit, not a Parquet/Thrift wire-format
/// restriction. Known Parquet metadata nests much less deeply, but unknown fields
/// may contain additional structs or collections. The budget starts at the
/// skipped value, excluding any enclosing structures already decoded normally.
pub(crate) const MAX_SKIP_DEPTH: i8 = 64;

#[derive(Debug)]
pub(crate) enum ThriftProtocolError {
    Eof,
    IO(Error),
    InvalidFieldType(u8),
    InvalidElementType(u8),
    FieldDeltaOverflow { field_delta: u8, last_field_id: i16 },
    InvalidBoolean(u8),
    IntegerOverflow,
    Utf8Error,
    SkipDepth(FieldType),
    SkipUnsupportedType(FieldType),
}

impl From<ThriftProtocolError> for ParquetError {
    #[inline(never)]
    fn from(e: ThriftProtocolError) -> Self {
        match e {
            ThriftProtocolError::Eof => eof_err!("Unexpected EOF"),
            ThriftProtocolError::IO(e) => e.into(),
            ThriftProtocolError::InvalidFieldType(value) => match FieldType::try_from(value) {
                Ok(fld_type) => general_err!("Unexpected struct field type {:?}", fld_type),
                Err(_) => general_err!("Unexpected struct field type {}", value),
            },
            ThriftProtocolError::InvalidElementType(value) => {
                general_err!("Unexpected list/set element type {}", value)
            }
            ThriftProtocolError::FieldDeltaOverflow {
                field_delta,
                last_field_id,
            } => general_err!("cannot add {} to {}", field_delta, last_field_id),
            ThriftProtocolError::InvalidBoolean(value) => {
                general_err!("cannot convert {} into bool", value)
            }
            ThriftProtocolError::IntegerOverflow => {
                general_err!("integer overflow decoding thrift value")
            }
            ThriftProtocolError::Utf8Error => general_err!("invalid utf8"),
            ThriftProtocolError::SkipDepth(field_type) => {
                general_err!("cannot parse past {:?}", field_type)
            }
            ThriftProtocolError::SkipUnsupportedType(field_type) => {
                general_err!("cannot skip field type {:?}", field_type)
            }
        }
    }
}

impl From<Utf8Error> for ThriftProtocolError {
    fn from(_: Utf8Error) -> Self {
        // ignore error payload to reduce the size of ThriftProtocolError
        Self::Utf8Error
    }
}

impl From<Error> for ThriftProtocolError {
    fn from(e: Error) -> Self {
        Self::IO(e)
    }
}

impl From<TryFromIntError> for ThriftProtocolError {
    fn from(_: TryFromIntError) -> Self {
        // ignore error payload to reduce the size of ThriftProtocolError
        Self::IntegerOverflow
    }
}

pub type ThriftProtocolResult<T> = Result<T, ThriftProtocolError>;

/// Wrapper for thrift `double` fields. This is used to provide
/// an implementation of `Eq` for floats. This implementation
/// uses IEEE 754 total order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedF64(f64);

impl From<f64> for OrderedF64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl From<OrderedF64> for f64 {
    fn from(value: OrderedF64) -> Self {
        value.0
    }
}

impl Eq for OrderedF64 {} // Marker trait, requires PartialEq

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Thrift compact protocol types for struct fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldType {
    Stop = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Byte = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
    Uuid = 13,
}

impl TryFrom<u8> for FieldType {
    type Error = ThriftProtocolError;
    fn try_from(value: u8) -> ThriftProtocolResult<Self> {
        match value {
            0 => Ok(Self::Stop),
            1 => Ok(Self::BooleanTrue),
            2 => Ok(Self::BooleanFalse),
            3 => Ok(Self::Byte),
            4 => Ok(Self::I16),
            5 => Ok(Self::I32),
            6 => Ok(Self::I64),
            7 => Ok(Self::Double),
            8 => Ok(Self::Binary),
            9 => Ok(Self::List),
            10 => Ok(Self::Set),
            11 => Ok(Self::Map),
            12 => Ok(Self::Struct),
            13 => Ok(Self::Uuid),
            _ => Err(ThriftProtocolError::InvalidFieldType(value)),
        }
    }
}

impl From<ElementType> for FieldType {
    fn from(value: ElementType) -> Self {
        match value {
            ElementType::Bool => Self::BooleanTrue,
            ElementType::Byte => Self::Byte,
            ElementType::I16 => Self::I16,
            ElementType::I32 => Self::I32,
            ElementType::I64 => Self::I64,
            ElementType::Double => Self::Double,
            ElementType::Binary => Self::Binary,
            ElementType::List => Self::List,
            ElementType::Set => Self::Set,
            ElementType::Map => Self::Map,
            ElementType::Struct => Self::Struct,
            ElementType::Uuid => Self::Uuid,
        }
    }
}

// Thrift compact protocol types for list elements
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElementType {
    Bool = 2,
    Byte = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
    Uuid = 13,
}

impl TryFrom<u8> for ElementType {
    type Error = ThriftProtocolError;
    fn try_from(value: u8) -> ThriftProtocolResult<Self> {
        match value {
            // For historical and compatibility reasons, a reader should be capable to deal with both cases.
            // The only valid value in the original spec was 2, but due to an widespread implementation bug
            // the defacto standard across large parts of the library became 1 instead.
            // As a result, both values are now allowed.
            // https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#list-and-set
            1 | 2 => Ok(Self::Bool),
            3 => Ok(Self::Byte),
            4 => Ok(Self::I16),
            5 => Ok(Self::I32),
            6 => Ok(Self::I64),
            7 => Ok(Self::Double),
            8 => Ok(Self::Binary),
            9 => Ok(Self::List),
            10 => Ok(Self::Set),
            11 => Ok(Self::Map),
            12 => Ok(Self::Struct),
            13 => Ok(Self::Uuid),
            _ => Err(ThriftProtocolError::InvalidElementType(value)),
        }
    }
}

/// Struct used to describe a [thrift struct] field during decoding.
///
/// [thrift struct]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#struct-encoding
pub(crate) struct FieldIdentifier {
    /// The type for the field.
    pub(crate) field_type: FieldType,
    /// The field's `id`. May be computed from delta or directly decoded.
    pub(crate) id: i16,
}

impl FieldIdentifier {
    pub(crate) fn bool_val(&self) -> ThriftProtocolResult<bool> {
        match self.field_type {
            FieldType::BooleanTrue => Ok(true),
            FieldType::BooleanFalse => Ok(false),
            _ => Err(ThriftProtocolError::InvalidFieldType(self.field_type as u8)),
        }
    }
}

/// Struct used to describe a [thrift list].
///
/// [thrift list]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#list-and-set
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListIdentifier {
    /// The type for each element in the list.
    pub(crate) element_type: ElementType,
    /// Number of elements contained in the list.
    pub(crate) size: i32,
}

/// Low-level object used to deserialize structs encoded with the Thrift [compact] protocol.
///
/// Implementation of this trait must provide the low-level functions `read_byte`, `read_bytes`,
/// `skip_bytes`, and `read_double`. These primitives are used by the default functions provided
/// here to perform deserialization.
///
/// [compact]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
pub(crate) trait ThriftCompactInputProtocol<'a> {
    /// Read a single byte from the input.
    fn read_byte(&mut self) -> ThriftProtocolResult<u8>;

    /// Read a Thrift encoded [binary] from the input.
    ///
    /// [binary]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#binary-encoding
    fn read_bytes(&mut self) -> ThriftProtocolResult<&'a [u8]>;

    fn read_bytes_owned(&mut self) -> ThriftProtocolResult<Vec<u8>>;

    /// Skip the next `n` bytes of input.
    fn skip_bytes(&mut self, n: usize) -> ThriftProtocolResult<()>;

    /// Remaining unread bytes, if this protocol is backed by a finite buffer.
    ///
    /// Used to reject Thrift collection sizes that cannot fit in the remaining
    /// input before allocating.
    fn remaining_bytes(&self) -> Option<usize> {
        None
    }

    /// Read a ULEB128 encoded unsigned varint from the input.
    fn read_vlq(&mut self) -> ThriftProtocolResult<u64> {
        // try the happy path first
        let byte = self.read_byte()?;
        if byte & 0x80 == 0 {
            return Ok(byte as u64);
        }
        let mut in_progress = (byte & 0x7f) as u64;
        let mut shift = 7;
        loop {
            let byte = self.read_byte()?;
            in_progress |= ((byte & 0x7F) as u64).wrapping_shl(shift);
            if byte & 0x80 == 0 {
                return Ok(in_progress);
            }
            shift += 7;
        }
    }

    /// Read a binary length, bounded to the signed 32-bit range used by Java and C++ Thrift.
    fn read_binary_len(&mut self) -> ThriftProtocolResult<usize> {
        Ok(i32::try_from(self.read_vlq()?)? as usize)
    }

    /// Read a zig-zag encoded signed varint from the input.
    fn read_zig_zag(&mut self) -> ThriftProtocolResult<i64> {
        let val = self.read_vlq()?;
        Ok((val >> 1) as i64 ^ -((val & 1) as i64))
    }

    /// Read the [`ListIdentifier`] for a Thrift encoded list.
    fn read_list_begin(&mut self) -> ThriftProtocolResult<ListIdentifier> {
        let header = self.read_byte()?;
        // some parquet writers will have an element_type of 0 for an empty list.
        // account for that and return a bogus but valid element_type.
        if header == 0 {
            return Ok(ListIdentifier {
                element_type: ElementType::Byte,
                size: 0,
            });
        }
        let element_type = ElementType::try_from(header & 0x0f)?;

        let possible_element_count = (header & 0xF0) >> 4;
        let element_count = if possible_element_count != 15 {
            // high bits set high if count and type encoded separately
            possible_element_count as i32
        } else {
            // The list size on the wire is an unsigned varint, but we represent
            // it as `i32` (matching Java's `int` and the Thrift schema).
            // A varint that decodes above `i32::MAX` is malformed input — reject
            // it here at the protocol layer rather than letting the cast wrap
            // into a negative size that downstream allocation code has to
            // re-validate.
            i32::try_from(self.read_vlq()?)?
        };

        Ok(ListIdentifier {
            element_type,
            size: element_count,
        })
    }

    // Full field ids are uncommon.
    // Not inlining this method reduces the code size of `read_field_begin`, which then ideally gets
    // inlined everywhere.
    #[cold]
    fn read_full_field_id(&mut self) -> ThriftProtocolResult<i16> {
        self.read_i16()
    }

    /// Read the [`FieldIdentifier`] for a field in a Thrift encoded struct.
    fn read_field_begin(&mut self, last_field_id: i16) -> ThriftProtocolResult<FieldIdentifier> {
        // we can read at least one byte, which is:
        // - the type
        // - the field delta and the type
        let field_type = self.read_byte()?;
        self.read_field_begin_with_byte(last_field_id, field_type)
    }

    #[inline(always)]
    fn read_field_begin_with_byte(
        &mut self,
        last_field_id: i16,
        field_type: u8,
    ) -> ThriftProtocolResult<FieldIdentifier> {
        if field_type & 0xf == 0 {
            return Ok(FieldIdentifier {
                field_type: FieldType::Stop,
                id: 0,
            });
        }

        let field_delta = (field_type & 0xf0) >> 4;
        let field_type = FieldType::try_from(field_type & 0xf)?;

        let id = if field_delta != 0 {
            last_field_id.checked_add(field_delta as i16).ok_or(
                ThriftProtocolError::FieldDeltaOverflow {
                    field_delta,
                    last_field_id,
                },
            )?
        } else {
            self.read_full_field_id()?
        };

        Ok(FieldIdentifier { field_type, id })
    }

    /// This is a specialized version of [`Self::read_field_begin`], solely for use in parsing
    /// simple structs. This function assumes that the delta field will always be less than 0xf,
    /// fields will be in order, and no boolean fields will be read.
    /// This also skips validation of the field type.
    ///
    /// Returns a tuple of `(field_type, field_delta)`.
    fn read_field_header(&mut self) -> ThriftProtocolResult<(u8, u8)> {
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xf0) >> 4;
        let field_type = field_type & 0xf;
        Ok((field_type, field_delta))
    }

    /// Read a boolean list element. This should not be used for struct fields. For the latter,
    /// use the [`FieldIdentifier::bool_val`] field.
    fn read_bool(&mut self) -> ThriftProtocolResult<bool> {
        let b = self.read_byte()?;
        // Previous versions of the thrift specification said to use 0 and 1 inside collections,
        // but that differed from existing implementations.
        // The specification was updated in https://github.com/apache/thrift/commit/2c29c5665bc442e703480bb0ee60fe925ffe02e8.
        // At least the go implementation seems to have followed the previously documented values.
        match b {
            0x01 => Ok(true),
            0x00 | 0x02 => Ok(false),
            _ => Err(ThriftProtocolError::InvalidBoolean(b)),
        }
    }

    /// Read a Thrift [binary] as a UTF-8 encoded string.
    ///
    /// [binary]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#binary-encoding
    fn read_string(&mut self) -> ThriftProtocolResult<&'a str> {
        let slice = self.read_bytes()?;
        Ok(std::str::from_utf8(slice)?)
    }

    /// Read an `i8`.
    fn read_i8(&mut self) -> ThriftProtocolResult<i8> {
        Ok(self.read_byte()? as _)
    }

    /// Read an `i16`.
    fn read_i16(&mut self) -> ThriftProtocolResult<i16> {
        Ok(self.read_zig_zag()? as _)
    }

    /// Read an `i32`.
    fn read_i32(&mut self) -> ThriftProtocolResult<i32> {
        Ok(self.read_zig_zag()? as _)
    }

    /// Read an `i64`.
    fn read_i64(&mut self) -> ThriftProtocolResult<i64> {
        self.read_zig_zag()
    }

    /// Read a Thrift `double` as `f64`.
    fn read_double(&mut self) -> ThriftProtocolResult<f64>;

    /// Skip a ULEB128 encoded varint.
    fn skip_vlq(&mut self) -> ThriftProtocolResult<()> {
        loop {
            let byte = self.read_byte()?;
            if byte & 0x80 == 0 {
                return Ok(());
            }
        }
    }

    /// Skip a thrift [binary].
    ///
    /// [binary]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#binary-encoding
    fn skip_binary(&mut self) -> ThriftProtocolResult<()> {
        let len = self.read_binary_len()?;
        self.skip_bytes(len)
    }

    /// Skip a value by type up to [`MAX_SKIP_DEPTH`] levels.
    fn skip(&mut self, field_type: FieldType) -> ThriftProtocolResult<()> {
        self.skip_till_depth(field_type, MAX_SKIP_DEPTH)
    }

    /// Skip a nominally empty struct, including forward-compatible extension fields.
    fn skip_empty_struct(&mut self) -> Result<()> {
        let byte = self.read_byte()?;
        // Empty structs have no extensions and leave the enclosing path unchanged.
        if byte & 0xf == 0 {
            return Ok(());
        }
        self.skip_nonempty_marker(byte)
    }

    #[cold]
    #[inline(never)]
    fn skip_nonempty_marker(&mut self, byte: u8) -> Result<()> {
        let mut field = self.read_field_begin_with_byte(0, byte)?;
        loop {
            self.skip_till_depth(field.field_type, MAX_SKIP_DEPTH - 1)?;
            field = self.read_field_begin(field.id)?;
            if field.field_type == FieldType::Stop {
                return Ok(());
            }
        }
    }

    /// Skip a field with type `field_type` recursively up to `depth` levels.
    fn skip_till_depth(&mut self, field_type: FieldType, depth: i8) -> ThriftProtocolResult<()> {
        if depth == 0 {
            return Err(ThriftProtocolError::SkipDepth(field_type));
        }

        match field_type {
            // boolean field has no data
            FieldType::BooleanFalse | FieldType::BooleanTrue => Ok(()),
            FieldType::Byte => self.read_i8().map(|_| ()),
            FieldType::I16 => self.skip_vlq(),
            FieldType::I32 => self.skip_vlq(),
            FieldType::I64 => self.skip_vlq(),
            FieldType::Double => self.skip_bytes(8),
            FieldType::Binary => self.skip_binary(),
            // see https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#struct
            FieldType::Struct => {
                loop {
                    // we don't need field id for skipping, so always pass 0 for last id
                    let field_ident = self.read_field_begin(0)?;
                    if field_ident.field_type == FieldType::Stop {
                        break;
                    }
                    self.skip_till_depth(field_ident.field_type, depth - 1)?;
                }
                Ok(())
            }
            // lists and sets are encoded the same
            // see https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#list-and-set
            FieldType::List | FieldType::Set => {
                let list_ident = self.read_list_begin()?;
                if list_ident.element_type == ElementType::Bool {
                    self.skip_bool_list(list_ident.size, depth - 1)
                } else {
                    let field_type = list_ident.element_type.into();
                    for _ in 0..list_ident.size {
                        self.skip_till_depth(field_type, depth - 1)?;
                    }
                    Ok(())
                }
            }
            // see https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#map
            FieldType::Map => self.skip_map(depth),
            // see https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#universal-unique-identifier-encoding
            FieldType::Uuid => self.skip_bytes(16),
            FieldType::Stop => Err(ThriftProtocolError::SkipUnsupportedType(field_type)),
        }
    }

    /// Skip boolean list or set payloads after reading their header.
    #[cold]
    fn skip_bool_list(&mut self, size: i32, depth: i8) -> ThriftProtocolResult<()> {
        if size > 0 && depth == 0 {
            return Err(ThriftProtocolError::SkipDepth(FieldType::BooleanTrue));
        }
        self.skip_bytes(size as usize)
    }

    /// Skip maps, which are not used by the current Parquet metadata schema.
    #[cold]
    fn skip_map(&mut self, depth: i8) -> ThriftProtocolResult<()> {
        let size = i32::try_from(self.read_vlq()?)?;
        if size > 0 {
            let kv = self.read_byte()?;
            let key_type = ElementType::try_from(kv >> 4)?;
            let val_type = ElementType::try_from(kv & 0xf)?;
            for _ in 0..size {
                self.skip_element_till_depth(key_type, depth - 1)?;
                self.skip_element_till_depth(val_type, depth - 1)?;
            }
        }
        Ok(())
    }

    /// Collection booleans have a payload byte, unlike inline struct fields.
    fn skip_element_till_depth(
        &mut self,
        element_type: ElementType,
        depth: i8,
    ) -> ThriftProtocolResult<()> {
        if depth == 0 {
            return Err(ThriftProtocolError::SkipDepth(element_type.into()));
        }
        match element_type {
            ElementType::Bool => self.skip_bytes(1),
            other => self.skip_till_depth(other.into(), depth),
        }
    }
}

/// A high performance Thrift reader that reads from a slice of bytes.
pub(crate) struct ThriftSliceInputProtocol<'a> {
    buf: &'a [u8],
}

impl<'a> ThriftSliceInputProtocol<'a> {
    /// Create a new `ThriftSliceInputProtocol` using the bytes in `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Return the current buffer as a slice.
    pub fn as_slice(&self) -> &'a [u8] {
        self.buf
    }
}

impl<'b, 'a: 'b> ThriftCompactInputProtocol<'b> for ThriftSliceInputProtocol<'a> {
    #[inline]
    fn read_byte(&mut self) -> ThriftProtocolResult<u8> {
        let ret = *self.buf.first().ok_or(ThriftProtocolError::Eof)?;
        self.buf = &self.buf[1..];
        Ok(ret)
    }

    fn read_bytes(&mut self) -> ThriftProtocolResult<&'b [u8]> {
        let len = self.read_binary_len()?;
        let ret = self.buf.get(..len).ok_or(ThriftProtocolError::Eof)?;
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    fn read_bytes_owned(&mut self) -> ThriftProtocolResult<Vec<u8>> {
        Ok(self.read_bytes()?.to_vec())
    }

    #[inline]
    fn skip_bytes(&mut self, n: usize) -> ThriftProtocolResult<()> {
        self.buf.get(..n).ok_or(ThriftProtocolError::Eof)?;
        self.buf = &self.buf[n..];
        Ok(())
    }

    fn read_double(&mut self) -> ThriftProtocolResult<f64> {
        let slice = self.buf.get(..8).ok_or(ThriftProtocolError::Eof)?;
        self.buf = &self.buf[8..];
        match slice.try_into() {
            Ok(slice) => Ok(f64::from_le_bytes(slice)),
            Err(_) => unreachable!(),
        }
    }

    fn remaining_bytes(&self) -> Option<usize> {
        Some(self.buf.len())
    }
}

/// A Thrift input protocol that wraps a [`Read`] object.
///
/// Note that this is only intended for use in reading Parquet page headers. This will panic
/// if Thrift `binary` data is encountered because a slice of that data cannot be returned.
pub(crate) struct ThriftReadInputProtocol<R: Read> {
    reader: R,
}

impl<R: Read> ThriftReadInputProtocol<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<'a, R: Read> ThriftCompactInputProtocol<'a> for ThriftReadInputProtocol<R> {
    #[inline]
    fn read_byte(&mut self) -> ThriftProtocolResult<u8> {
        let mut buf = [0_u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_bytes(&mut self) -> ThriftProtocolResult<&'a [u8]> {
        unimplemented!()
    }

    fn read_bytes_owned(&mut self) -> ThriftProtocolResult<Vec<u8>> {
        let len = self.read_binary_len()?;
        let mut v = Vec::with_capacity(len.min(8 * 1024));
        let copied = std::io::copy(&mut self.reader.by_ref().take(len as u64), &mut v)?;
        if copied != len as u64 {
            return Err(ThriftProtocolError::Eof);
        }
        Ok(v)
    }

    fn skip_bytes(&mut self, n: usize) -> ThriftProtocolResult<()> {
        let skipped = std::io::copy(
            &mut self.reader.by_ref().take(n as u64),
            &mut std::io::sink(),
        )?;
        if skipped != n as u64 {
            return Err(ThriftProtocolError::Eof);
        }
        Ok(())
    }

    fn read_double(&mut self) -> ThriftProtocolResult<f64> {
        let mut buf = [0_u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }
}

/// Trait implemented for objects that can be deserialized from a Thrift input stream.
/// Implementations are provided for Thrift primitive types.
pub(crate) trait ReadThrift<'a, R: ThriftCompactInputProtocol<'a>> {
    /// Read an object of type `Self` from the input protocol object.
    fn read_thrift(prot: &mut R) -> Result<Self>
    where
        Self: Sized;
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for bool {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_bool()?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i8 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_i8()?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i16 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_i16()?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i32 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_i32()?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i64 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_i64()?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for OrderedF64 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(OrderedF64(prot.read_double()?))
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for &'a str {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_string()?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for String {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(String::from_utf8(prot.read_bytes_owned()?)?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for &'a [u8] {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(prot.read_bytes()?)
    }
}

/// Read a Thrift encoded [list] from the input protocol object.
///
/// [list]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#list-and-set
pub(crate) fn read_thrift_vec<'a, T, R>(prot: &mut R) -> Result<Vec<T>>
where
    R: ThriftCompactInputProtocol<'a>,
    T: ReadThrift<'a, R> + WriteThrift,
{
    let list_ident = prot.read_list_begin()?;
    validate_list_type(T::ELEMENT_TYPE, &list_ident)?;
    let size = list_ident.size as usize;
    check_list_size(prot.remaining_bytes(), size)?;
    let mut res = Vec::with_capacity(size);
    for _ in 0..list_ident.size {
        let val = T::read_thrift(prot)?;
        res.push(val);
    }
    Ok(res)
}

pub(crate) fn validate_list_type(expected: ElementType, got: &ListIdentifier) -> Result<()> {
    // An empty list decodes no elements, and some writers (e.g. fastparquet)
    // encode it as a bare 0x00 header with no meaningful element type.
    if got.element_type != expected && got.size != 0 {
        return Err(general_err!(
            "Expected list element type of {:?} but got {:?}",
            expected,
            got.element_type
        ));
    }
    Ok(())
}

/// Reject a declared list size that cannot fit the remaining input. Each list
/// element is at least one byte on the wire, so a size past the remaining
/// bytes is a malformed header; bounding it before reserving stops a huge
/// allocation. `remaining` is `None` when the protocol is not backed by a
/// finite buffer, in which case there is nothing to check against.
pub(crate) fn check_list_size(remaining: Option<usize>, size: usize) -> Result<()> {
    if let Some(remaining) = remaining
        && size > remaining
    {
        return Err(general_err!(
            "Thrift list size {size} exceeds remaining input length {remaining}"
        ));
    }
    Ok(())
}

/////////////////////////
// thrift compact output

/// Low-level object used to serialize structs to the Thrift [compact output] protocol.
///
/// This struct serves as a wrapper around a [`Write`] object, to which thrift encoded data
/// will written. The implementation provides functions to write Thrift primitive types, as well
/// as functions used in the encoding of lists and structs. This should rarely be used directly,
/// but is instead intended for use by implementers of [`WriteThrift`] and [`WriteThriftField`].
///
/// The root binding selects `TRACK` once. Both modes share the serializers, but
/// the untracked mode can eliminate hook traversal from primitive operations.
///
/// [compact output]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
pub(crate) struct ThriftCompactOutputProtocol<'s, 'a, W: Write, const TRACK: bool = false> {
    writer: W,
    write_path_in_schema: bool,
    write_rg_ordinal: bool,
    metadata_extensions: Option<&'s mut WriteState<'a>>,
}

impl<'s, 'a, W: Write> ThriftCompactOutputProtocol<'s, 'a, W, false> {
    /// Create a new `ThriftCompactOutputProtocol` wrapping the byte sink `writer`.
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            write_path_in_schema: true,
            write_rg_ordinal: true,
            metadata_extensions: None,
        }
    }

    pub(crate) fn with_root_extension(writer: W, state: &'s mut WriteState<'a>) -> Self {
        Self {
            metadata_extensions: Some(state),
            ..Self::new(writer)
        }
    }
}

impl<'s, 'a, W: Write> ThriftCompactOutputProtocol<'s, 'a, W, true> {
    pub(crate) fn with_metadata_extensions(writer: W, state: &'s mut WriteState<'a>) -> Self {
        Self {
            writer,
            write_path_in_schema: true,
            write_rg_ordinal: true,
            metadata_extensions: Some(state),
        }
    }
}

impl<W: Write, const TRACK: bool> ThriftCompactOutputProtocol<'_, '_, W, TRACK> {
    /// Supply ownership already known by a typed footer serializer. This is a
    /// scoped hint, not a path update or an eagerly constructed public context.
    /// Typed serializer scopes expose existing owner indices without resolving
    /// the structural path.
    #[inline]
    pub(crate) fn with_footer_scope<T>(
        &mut self,
        scope: FooterScope,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if !TRACK {
            return f(self);
        }
        let previous = self
            .metadata_extensions
            .as_deref_mut()
            .map(|s| s.replace_scope(scope));
        let result = f(self);
        if let (Some(state), Some(previous)) = (self.metadata_extensions.as_deref_mut(), previous) {
            state.replace_scope(previous);
        }
        result
    }
    /// Borrow the same transport and format flags, with observation erased.
    pub(crate) fn untracked(&mut self) -> ThriftCompactOutputProtocol<'_, '_, &mut W> {
        ThriftCompactOutputProtocol {
            writer: &mut self.writer,
            write_path_in_schema: self.write_path_in_schema,
            write_rg_ordinal: self.write_rg_ordinal,
            metadata_extensions: None,
        }
    }

    pub(crate) fn write_footer_element(
        &mut self,
        scope: FooterScope,
        index: usize,
        object: &impl WriteThrift,
    ) -> Result<()> {
        if TRACK
            && self
                .metadata_extensions
                .as_ref()
                .is_some_and(|s| !s.descendants().any(|t| t.accepts_scope(scope)))
        {
            return object.write_thrift(&mut self.untracked());
        }
        self.with_list_element(index, |writer| {
            writer.with_footer_scope(scope, |writer| object.write_thrift(writer))
        })
    }

    #[inline]
    pub(crate) fn write_footer_column(
        &mut self,
        index: usize,
        sorting: bool,
        object: &impl WriteThrift,
    ) -> Result<()> {
        let row_group = if TRACK {
            self.metadata_extensions
                .as_deref()
                .and_then(|s| s.row_group())
        } else {
            None
        };
        if let Some(row_group) = row_group {
            let scope = if sorting {
                FooterScope::SortingColumn { row_group, index }
            } else {
                FooterScope::Column {
                    row_group,
                    column: index,
                }
            };
            self.write_footer_element(scope, index, object)
        } else {
            self.with_list_element(index, |writer| object.write_thrift(writer))
        }
    }

    pub(crate) fn schema_extensions(&self) -> bool {
        TRACK
            && self
                .metadata_extensions
                .as_ref()
                .is_some_and(|s| s.descendants().any(|t| t.accepts_schema()))
    }

    /// Borrow schema ownership for this element and all its nested annotations.
    /// The scoped protocol shares the parent's structural tracking, not a copy.
    pub(crate) fn write_schema_element(
        &mut self,
        element: &impl WriteThrift,
        index: usize,
        column: Option<usize>,
    ) -> Result<()> {
        if !TRACK {
            return element.write_thrift(self);
        }
        if self.metadata_extensions.as_ref().is_some_and(|state| {
            !state.descendants().any(|t| {
                t.accepts_context(crate::file::metadata::extensions::MetadataContext {
                    column_ordinal: column,
                    ..Default::default()
                })
            })
        }) {
            return element.write_thrift(&mut self.untracked());
        }
        self.with_list_element(index, |writer| {
            let Some(state) = writer.metadata_extensions.as_deref_mut() else {
                return element.write_thrift(writer);
            };
            let mut state = state.with_schema(index, column);
            let mut scoped = ThriftCompactOutputProtocol::<_, true> {
                writer: &mut writer.writer,
                write_path_in_schema: writer.write_path_in_schema,
                write_rg_ordinal: writer.write_rg_ordinal,
                metadata_extensions: Some(&mut state),
            };
            element.write_thrift(&mut scoped)
        })
    }

    #[cfg(debug_assertions)]
    pub(crate) fn assert_metadata_extensions_balanced(&self) {
        if TRACK && let Some(state) = &self.metadata_extensions {
            state.assert_balanced();
        }
    }

    pub(crate) fn write_struct_begin(&mut self) -> Result<()> {
        if TRACK && let Some(state) = &mut self.metadata_extensions {
            state.begin()?;
        }
        Ok(())
    }

    pub(crate) fn write_union_begin(&mut self) -> Result<()> {
        self.write_struct_begin()
    }

    pub(crate) fn with_list_element<T>(
        &mut self,
        index: usize,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if !TRACK {
            return f(self);
        }
        let n = self
            .metadata_extensions
            .as_mut()
            .map(|s| s.element(index))
            .transpose()?;
        let result = f(self);
        if let (Some(state), Some(n)) = (&mut self.metadata_extensions, n) {
            state.truncate(n);
        }
        result
    }

    // TODO(ets): at some point there should probably be a properties object
    // to control aspects of thrift output. But since this is the only option to date
    // I'm choosing a simpler API.
    /// Control the writing of the `path_in_schema` element of the `ColumnMetaData`
    pub(crate) fn set_write_path_in_schema(&mut self, val: bool) {
        self.write_path_in_schema = val;
    }

    /// Indicate whether or not to emit `path_in_schema`.
    pub(crate) fn write_path_in_schema(&self) -> bool {
        self.write_path_in_schema
    }

    /// Control the writing of the `ordinal` element of the `RowGroup` struct.
    ///
    /// The Thrift `ordinal` field on the `RowGroup` struct is `i16`, but the
    /// Thrift compact protocol allows for up to 2^31 elements in a list. If
    /// more than 2^15 row groups are to be written, this can be set to `false`
    /// to prevent writing the ordinal for some row groups but not others.
    pub(crate) fn set_write_row_group_ordinal(&mut self, val: bool) {
        self.write_rg_ordinal = val;
    }

    /// Indicate whether or not to emit `ordinal`.
    pub(crate) fn write_row_group_ordinal(&self) -> bool {
        self.write_rg_ordinal
    }

    /// Write a single byte to the output stream.
    fn write_byte(&mut self, b: u8) -> Result<()> {
        self.writer.write_all(&[b])?;
        Ok(())
    }

    /// Write the given `u64` as a ULEB128 encoded varint.
    fn write_vlq(&mut self, val: u64) -> Result<()> {
        let mut v = val;
        while v > 0x7f {
            self.write_byte(v as u8 | 0x80)?;
            v >>= 7;
        }
        self.write_byte(v as u8)
    }

    /// Write the given `i64` as a zig-zag encoded varint.
    fn write_zig_zag(&mut self, val: i64) -> Result<()> {
        let s = (val < 0) as i64;
        self.write_vlq((((val ^ -s) << 1) + s) as u64)
    }

    /// Used to mark the start of a Thrift struct field of type `field_type`. `last_field_id`
    /// is used to compute a delta to the given `field_id` per the compact protocol [spec].
    ///
    /// [spec]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#struct-encoding
    #[inline(always)]
    pub(crate) fn write_field_begin(
        &mut self,
        field_type: FieldType,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<()> {
        // Only compound values can lead to a nested struct's physical path.
        if TRACK
            && matches!(
                field_type,
                FieldType::Struct | FieldType::List | FieldType::Set | FieldType::Map
            )
            && let Some(state) = &mut self.metadata_extensions
        {
            state.field(field_id)?;
        }
        let delta = field_id.wrapping_sub(last_field_id);
        if delta > 0 && delta <= 0xf {
            self.write_byte(((delta as u8) << 4) | field_type as u8)
        } else {
            self.write_byte(field_type as u8)?;
            self.write_i16(field_id)
        }
    }

    /// Used to indicate the start of a list of `element_type` elements.
    pub(crate) fn write_list_begin(&mut self, element_type: ElementType, len: usize) -> Result<()> {
        if len < 15 {
            self.write_byte(((len as u8) << 4) | element_type as u8)
        } else {
            self.write_byte(0xf0u8 | element_type as u8)?;
            self.write_vlq(len as _)
        }
    }

    /// Used to mark the end of a struct. This must be called after all fields of the struct have
    /// been written.
    pub(crate) fn write_struct_end(&mut self) -> Result<()> {
        self.write_end(true, false)
    }

    /// Finish an independently encoded root. Root-only hooks use the untracked
    /// protocol for every child and are invoked exactly here, without a depth counter.
    pub(crate) fn write_root_end(&mut self) -> Result<()> {
        self.write_end(true, true)
    }

    /// Union envelopes have ancestry but never receive an extension payload.
    pub(crate) fn write_union_end(&mut self) -> Result<()> {
        self.write_end(false, false)
    }

    fn write_end(&mut self, ordinary_struct: bool, root: bool) -> Result<()> {
        if (TRACK || root)
            && let Some(state) = self.metadata_extensions.take()
        {
            // The hook only supplies bytes. Validate and write them afterward,
            // while its borrowed payload is alive, on the ordinary error path.
            let payload = if TRACK {
                state.end(ordinary_struct)
            } else {
                state.root_payload()
            };
            let result = payload.and_then(|payload| {
                if let Some(payload) = payload {
                    self.write_metadata_extension_header(payload.len())?;
                    self.writer.write_all(payload)?;
                }
                Ok(())
            });
            self.metadata_extensions = Some(state);
            result?;
        }
        self.write_byte(0)
    }

    /// Validate the payload length before emitting any extension framing.
    fn write_metadata_extension_header(&mut self, length: usize) -> Result<()> {
        if length > i32::MAX as usize {
            return Err(general_err!(
                "Metadata extension payload exceeds i32::MAX bytes"
            ));
        }
        // Explicit signed field ID: ZigZag(32767), not ULEB128(32767).
        self.write_byte(FieldType::Binary as u8)?;
        self.write_i16(i16::MAX)?;
        self.write_vlq(length as u64)
    }

    /// Serialize a slice of `u8`s. This will encode a length, and then write the bytes without
    /// further encoding.
    pub(crate) fn write_bytes(&mut self, val: &[u8]) -> Result<()> {
        self.write_vlq(val.len() as u64)?;
        self.writer.write_all(val)?;
        Ok(())
    }

    /// Short-cut method used to encode structs that have no fields (often used in Thrift unions).
    /// This simply encodes the field id and then immediately writes the end-of-struct marker.
    pub(crate) fn write_empty_struct(&mut self, field_id: i16, last_field_id: i16) -> Result<i16> {
        self.write_field_begin(FieldType::Struct, field_id, last_field_id)?;
        self.write_struct_begin()?;
        self.write_struct_end()?;
        Ok(field_id)
    }

    /// Write a boolean value.
    pub(crate) fn write_bool(&mut self, val: bool) -> Result<()> {
        match val {
            true => self.write_byte(1),
            false => self.write_byte(2),
        }
    }

    /// Write a zig-zag encoded `i8` value.
    pub(crate) fn write_i8(&mut self, val: i8) -> Result<()> {
        self.write_byte(val as u8)
    }

    /// Write a zig-zag encoded `i16` value.
    pub(crate) fn write_i16(&mut self, val: i16) -> Result<()> {
        self.write_zig_zag(val as _)
    }

    /// Write a zig-zag encoded `i32` value.
    pub(crate) fn write_i32(&mut self, val: i32) -> Result<()> {
        self.write_zig_zag(val as _)
    }

    /// Write a zig-zag encoded `i64` value.
    pub(crate) fn write_i64(&mut self, val: i64) -> Result<()> {
        self.write_zig_zag(val)
    }

    /// Write a double value.
    pub(crate) fn write_double(&mut self, val: f64) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        Ok(())
    }
}

/// Trait implemented by objects that are to be serialized to a Thrift [compact output] protocol
/// stream. Implementations are also provided for primitive Thrift types.
///
/// [compact output]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
pub(crate) trait WriteThrift {
    /// The [`ElementType`] to use when a list of this object is written.
    const ELEMENT_TYPE: ElementType;

    /// Serialize this object to the given `writer`.
    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()>;
}

/// Implementation for a vector of thrift serializable objects that implement [`WriteThrift`].
/// This will write the necessary list header and then serialize the elements one-at-a-time.
impl<T> WriteThrift for Vec<T>
where
    T: WriteThrift,
{
    const ELEMENT_TYPE: ElementType = ElementType::List;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_list_begin(T::ELEMENT_TYPE, self.len())?;
        for (index, item) in self.iter().enumerate() {
            if matches!(T::ELEMENT_TYPE, ElementType::Struct | ElementType::List) {
                writer.with_list_element(index, |writer| item.write_thrift(writer))?;
            } else {
                item.write_thrift(writer)?;
            }
        }
        Ok(())
    }
}

impl WriteThrift for bool {
    const ELEMENT_TYPE: ElementType = ElementType::Bool;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_bool(*self)
    }
}

impl WriteThrift for i8 {
    const ELEMENT_TYPE: ElementType = ElementType::Byte;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_i8(*self)
    }
}

impl WriteThrift for i16 {
    const ELEMENT_TYPE: ElementType = ElementType::I16;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_i16(*self)
    }
}

impl WriteThrift for i32 {
    const ELEMENT_TYPE: ElementType = ElementType::I32;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_i32(*self)
    }
}

impl WriteThrift for i64 {
    const ELEMENT_TYPE: ElementType = ElementType::I64;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_i64(*self)
    }
}

impl WriteThrift for OrderedF64 {
    const ELEMENT_TYPE: ElementType = ElementType::Double;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_double(self.0)
    }
}

impl WriteThrift for f64 {
    const ELEMENT_TYPE: ElementType = ElementType::Double;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_double(*self)
    }
}

impl WriteThrift for &[u8] {
    const ELEMENT_TYPE: ElementType = ElementType::Binary;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_bytes(self)
    }
}

impl WriteThrift for &str {
    const ELEMENT_TYPE: ElementType = ElementType::Binary;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_bytes(self.as_bytes())
    }
}

impl WriteThrift for String {
    const ELEMENT_TYPE: ElementType = ElementType::Binary;

    fn write_thrift<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
    ) -> Result<()> {
        writer.write_bytes(self.as_bytes())
    }
}

/// Trait implemented by objects that are fields of Thrift structs.
///
/// For example, given the Thrift struct definition
/// ```ignore
/// struct MyStruct {
///   1: required i32 field1
///   2: optional bool field2
///   3: optional OtherStruct field3
/// }
/// ```
///
/// which becomes in Rust
/// ```ignore
/// # struct OtherStruct {}
/// struct MyStruct {
///   field1: i32,
///   field2: Option<bool>,
///   field3: Option<OtherStruct>,
/// }
/// ```
/// the impl of `WriteThrift` for `MyStruct` will use the `WriteThriftField` impls for `i32`,
/// `bool`, and `OtherStruct`.
///
/// ```ignore
/// impl WriteThrift for MyStruct {
///   const ELEMENT_TYPE: ElementType = ElementType::Struct;
///
///   fn write_thrift<W: Write, const TRACK: bool>(&self, writer: &mut ThriftCompactOutputProtocol<W, TRACK>) -> Result<()> {
///     writer.write_struct_begin()?;
///     let mut last_field_id = 0i16;
///     last_field_id = self.field1.write_thrift_field(writer, 1, last_field_id)?;
///     if self.field2.is_some() {
///       // if field2 is `None` then this assignment won't happen and last_field_id will remain
///       // `1` when writing `field3`
///       last_field_id = self.field2.write_thrift_field(writer, 2, last_field_id)?;
///     }
///     if self.field3.is_some() {
///       // no need to assign last_field_id since this is the final field.
///       self.field3.write_thrift_field(writer, 3, last_field_id)?;
///     }
///     writer.write_struct_end()
///   }
/// }
/// ```
///
pub(crate) trait WriteThriftField {
    /// Used to write struct fields (which may be primitive or IDL defined types). This will
    /// write the field marker for the given `field_id`, using `last_field_id` to compute the
    /// field delta used by the Thrift [compact protocol]. On success this will return `field_id`
    /// to be used in chaining.
    ///
    /// [compact protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#struct-encoding
    fn write_thrift_field<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16>;
}

// bool struct fields are written differently to bool values
impl WriteThriftField for bool {
    fn write_thrift_field<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        // boolean only writes the field header
        match *self {
            true => writer.write_field_begin(FieldType::BooleanTrue, field_id, last_field_id)?,
            false => writer.write_field_begin(FieldType::BooleanFalse, field_id, last_field_id)?,
        }
        Ok(field_id)
    }
}

write_thrift_field!(i8, FieldType::Byte);
write_thrift_field!(i16, FieldType::I16);
write_thrift_field!(i32, FieldType::I32);
write_thrift_field!(i64, FieldType::I64);
write_thrift_field!(OrderedF64, FieldType::Double);
write_thrift_field!(f64, FieldType::Double);
write_thrift_field!(String, FieldType::Binary);

impl WriteThriftField for &[u8] {
    fn write_thrift_field<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Binary, field_id, last_field_id)?;
        writer.write_bytes(self)?;
        Ok(field_id)
    }
}

impl WriteThriftField for &str {
    fn write_thrift_field<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Binary, field_id, last_field_id)?;
        writer.write_bytes(self.as_bytes())?;
        Ok(field_id)
    }
}

impl<T> WriteThriftField for Vec<T>
where
    T: WriteThrift,
{
    fn write_thrift_field<W: Write, const TRACK: bool>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::List, field_id, last_field_id)?;
        self.write_thrift(writer)?;
        Ok(field_id)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::basic::{TimeUnit, Type};
    use crate::file::metadata::extensions::{
        MetadataExtensionHook, MetadataSite, SiteInfo, ThriftPathSegment, write_with_hook,
    };

    use super::*;
    use std::fmt::Debug;
    use std::io::{ErrorKind, Result as IoResult};

    pub(crate) fn test_roundtrip<T>(val: T)
    where
        T: for<'a> ReadThrift<'a, ThriftSliceInputProtocol<'a>> + WriteThrift + PartialEq + Debug,
    {
        let mut buf = Vec::<u8>::new();
        {
            let mut writer = ThriftCompactOutputProtocol::new(&mut buf);
            val.write_thrift(&mut writer).unwrap();
        }

        let mut prot = ThriftSliceInputProtocol::new(&buf);
        let read_val = T::read_thrift(&mut prot).unwrap();
        assert_eq!(val, read_val);
    }

    #[test]
    fn test_binary_length_bound() {
        let oversized = [0x80, 0x80, 0x80, 0x80, 8]; // i32::MAX + 1
        let maximum = [0xff, 0xff, 0xff, 0xff, 7]; // i32::MAX, no payload
        for bytes in [&oversized, &maximum] {
            let check = |result: ThriftProtocolResult<()>| {
                if bytes == &oversized {
                    assert!(matches!(result, Err(ThriftProtocolError::IntegerOverflow)));
                } else {
                    assert!(matches!(result, Err(ThriftProtocolError::Eof)));
                }
            };
            check(
                ThriftSliceInputProtocol::new(bytes)
                    .read_bytes()
                    .map(|_| ()),
            );
            check(
                ThriftSliceInputProtocol::new(bytes)
                    .read_bytes_owned()
                    .map(|_| ()),
            );
            check(ThriftSliceInputProtocol::new(bytes).skip_binary());
            check(
                ThriftReadInputProtocol::new(bytes.as_slice())
                    .read_bytes_owned()
                    .map(|_| ()),
            );
            check(ThriftReadInputProtocol::new(bytes.as_slice()).skip_binary());
        }
    }

    struct FragmentedReader<'a>(&'a [u8]);

    impl Read for FragmentedReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            let len = buf.len().min(2);
            self.0.read(&mut buf[..len])
        }
    }

    #[test]
    fn test_read_bytes_owned_complete_and_fragmented() {
        let large_payload = vec![7; 16 * 1024];
        for payload in [b"".as_slice(), b"abcde".as_slice(), &large_payload] {
            let mut data = Vec::new();
            ThriftCompactOutputProtocol::new(&mut data)
                .write_bytes(payload)
                .unwrap();
            data.push(42);
            let mut slice = ThriftSliceInputProtocol::new(&data);
            let mut stream = ThriftReadInputProtocol::new(data.as_slice());
            let mut fragmented = ThriftReadInputProtocol::new(FragmentedReader(&data));
            assert_eq!(slice.read_bytes_owned().unwrap(), payload);
            assert_eq!(stream.read_bytes_owned().unwrap(), payload);
            assert_eq!(fragmented.read_bytes_owned().unwrap(), payload);
            assert_eq!(slice.read_byte().unwrap(), 42);
            assert_eq!(stream.read_byte().unwrap(), 42);
            assert_eq!(fragmented.read_byte().unwrap(), 42);
        }
    }

    #[test]
    fn test_read_bytes_owned_truncated_payload() {
        for len in [5, 1024 * 1024, i32::MAX as u64] {
            let mut data = Vec::new();
            ThriftCompactOutputProtocol::new(&mut data)
                .write_vlq(len)
                .unwrap();
            data.extend_from_slice(b"abc");
            let mut stream = ThriftReadInputProtocol::new(FragmentedReader(&data));
            assert!(matches!(
                stream.read_bytes_owned(),
                Err(ThriftProtocolError::Eof)
            ));
        }
    }

    #[test]
    fn test_read_bytes_owned_preserves_io_error() {
        struct FailingReader;
        impl Read for FailingReader {
            fn read(&mut self, _: &mut [u8]) -> IoResult<usize> {
                Err(Error::new(ErrorKind::PermissionDenied, "payload error"))
            }
        }
        let reader = b"\x05ab".as_slice().chain(FailingReader);
        let mut stream = ThriftReadInputProtocol::new(reader);
        assert!(
            matches!(stream.read_bytes_owned(), Err(ThriftProtocolError::IO(e)) if e.kind() == ErrorKind::PermissionDenied)
        );
    }

    #[test]
    fn test_enum_roundtrip() {
        test_roundtrip(Type::BOOLEAN);
        test_roundtrip(Type::INT32);
        test_roundtrip(Type::INT64);
        test_roundtrip(Type::INT96);
        test_roundtrip(Type::FLOAT);
        test_roundtrip(Type::DOUBLE);
        test_roundtrip(Type::BYTE_ARRAY);
        test_roundtrip(Type::FIXED_LEN_BYTE_ARRAY);
    }

    #[test]
    fn test_union_all_empty_roundtrip() {
        test_roundtrip(TimeUnit::MILLIS);
        test_roundtrip(TimeUnit::MICROS);
        test_roundtrip(TimeUnit::NANOS);
    }

    #[test]
    fn test_metadata_extension_callbacks_and_payloads() {
        use crate::basic::{Encoding, PageType};
        use crate::file::metadata::thrift::{DictionaryPageHeader, PageHeader};
        use crate::file::metadata::thrift_fields::page_header;
        use ThriftPathSegment::Field;

        let header = PageHeader {
            r#type: PageType::DICTIONARY_PAGE,
            uncompressed_page_size: 4,
            compressed_page_size: 4,
            crc: None,
            data_page_header: None,
            index_page_header: None,
            dictionary_page_header: Some(DictionaryPageHeader {
                num_values: 1,
                encoding: Encoding::PLAIN,
                is_sorted: None,
            }),
            data_page_header_v2: None,
        };
        let root = SiteInfo::page_header(3, 2, 0, PageType::DICTIONARY_PAGE);
        #[derive(Default)]
        struct Payload {
            paths: Vec<Vec<ThriftPathSegment>>,
            buffer: Vec<u8>,
        }
        impl MetadataExtensionHook for Payload {
            fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
                let info = site.context()?;
                assert_eq!(site.root(), ThriftStruct::DictionaryPage);
                assert_eq!(info.page_ordinal, Some(0));
                assert_eq!(info.row_group_ordinal, Some(3));
                assert_eq!(info.column_ordinal, Some(2));
                let path = crate::file::metadata::extensions::test_util::coordinates(site)?;
                self.paths.push(path.clone());
                // Reuse hook-owned storage; each payload must be consumed before
                // the next callback overwrites it.
                self.buffer.clear();
                self.buffer
                    .extend_from_slice(if path.is_empty() { b"root" } else { b"child" });
                Ok(Some(&self.buffer))
            }
        }
        let mut plain = Vec::new();
        header
            .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut plain))
            .unwrap();
        let mut hook = Payload::default();
        let mut bytes = Vec::new();
        write_with_hook(&header, &mut bytes, &mut hook, root).unwrap();
        assert_eq!(
            hook.paths,
            [vec![Field(page_header::DICTIONARY_PAGE_HEADER)], vec![]]
        );
        // The child is the final field: replace its STOP and the parent's STOP
        // with binary field 32767, payload length, payload, and STOP at each level.
        assert!(plain.ends_with(&[0, 0]));
        let mut expected = plain[..plain.len() - 2].to_vec();
        expected.extend_from_slice(&[8, 0xfe, 0xff, 3, 5]);
        expected.extend_from_slice(b"child");
        expected.extend_from_slice(&[0, 8, 0xfe, 0xff, 3, 4]);
        expected.extend_from_slice(b"root");
        expected.push(0);
        assert_eq!(bytes, expected);
        let mut reader = ThriftSliceInputProtocol::new(&bytes);
        assert_eq!(PageHeader::read_thrift(&mut reader).unwrap(), header);
        assert!(reader.as_slice().is_empty());

        let mut no_op = |_: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> { Ok(None) };
        bytes.clear();
        write_with_hook(&header, &mut bytes, &mut no_op, root).unwrap();
        assert_eq!(bytes, plain);
    }

    #[test]
    fn test_metadata_extension_empty_union_member() {
        use crate::file::metadata::thrift_fields::time_unit;
        let mut calls = 0;
        let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
            // Only the ordinary empty member, never the union envelope.
            assert_eq!(site.root(), ThriftStruct::FileCryptoMetaData);
            assert_eq!(
                crate::file::metadata::extensions::test_util::coordinates(site)?,
                &[ThriftPathSegment::Field(time_unit::MILLIS)]
            );
            calls += 1;
            Ok(Some(b""))
        };
        let mut bytes = Vec::new();
        write_with_hook(
            &TimeUnit::MILLIS,
            &mut bytes,
            &mut hook,
            SiteInfo::new(ThriftStruct::FileCryptoMetaData),
        )
        .unwrap();
        assert_eq!(calls, 1);
        // A present zero-length binary field, followed by member and union STOPs.
        assert_eq!(bytes, [0x1c, 8, 0xfe, 0xff, 3, 0, 0, 0]);
        let mut reader = ThriftSliceInputProtocol::new(&bytes);
        assert_eq!(
            TimeUnit::read_thrift(&mut reader).unwrap(),
            TimeUnit::MILLIS
        );
        assert!(reader.as_slice().is_empty());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn test_metadata_extension_payload_is_encrypted() {
        use crate::encryption::ciphers::{
            BlockDecryptor, BlockEncryptor, RingGcmBlockDecryptor, RingGcmBlockEncryptor,
        };
        use crate::encryption::encrypt::encrypt_thrift;

        let key = [0u8; 16];
        let aad = b"metadata-extension-test";
        let payload = b"encrypted-extension-payload";
        let mut encryptor: Box<dyn BlockEncryptor> =
            Box::new(RingGcmBlockEncryptor::new(&key).unwrap());
        let mut calls = 0;
        let mut ciphertext = Vec::new();
        {
            let mut hook = |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
                let info = site.context()?;
                assert_eq!(site.root(), ThriftStruct::ColumnMetaData);
                assert_eq!(info.row_group_ordinal, Some(0));
                assert_eq!(info.column_ordinal, None);
                calls += 1;
                Ok(Some(payload))
            };
            encrypt_thrift(
                |buffer| {
                    write_with_hook(
                        &TimeUnit::MILLIS,
                        buffer,
                        &mut hook,
                        SiteInfo::column(ThriftStruct::ColumnMetaData, Some(0), None),
                    )
                },
                &mut encryptor,
                &mut ciphertext,
                aad,
            )
            .unwrap();
        }
        assert_eq!(calls, 1);
        assert!(
            !ciphertext
                .windows(payload.len())
                .any(|bytes| bytes == payload)
        );
        let plaintext = RingGcmBlockDecryptor::new(&key)
            .unwrap()
            .decrypt(&ciphertext, aad)
            .unwrap();
        let mut expected = vec![0x1c, 8, 0xfe, 0xff, 3, payload.len() as u8];
        expected.extend_from_slice(payload);
        expected.extend_from_slice(&[0, 0]);
        assert_eq!(plaintext, expected);
    }

    #[test]
    fn test_metadata_extension_length_validation() {
        // Exercise the serializer's length guard without allocating a >2 GiB slice.
        let mut bytes = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut bytes);
        assert!(
            writer
                .write_metadata_extension_header(i32::MAX as usize + 1)
                .is_err()
        );
        assert!(bytes.is_empty()); // No field framing was emitted on invalid length.
        let mut writer = ThriftCompactOutputProtocol::new(&mut bytes);
        writer
            .write_metadata_extension_header(i32::MAX as usize)
            .unwrap();
        let mut reader = ThriftSliceInputProtocol::new(&bytes);
        let field = reader.read_field_begin(0).unwrap();
        assert_eq!(field.id, i16::MAX);
        assert_eq!(field.field_type, FieldType::Binary);
        assert_eq!(reader.read_vlq().unwrap(), i32::MAX as u64);
    }

    #[test]
    fn test_decode_empty_list() {
        let data = vec![0u8; 1];
        let mut prot = ThriftSliceInputProtocol::new(&data);
        let header = prot.read_list_begin().expect("error reading list header");
        assert_eq!(header.size, 0);
        assert_eq!(header.element_type, ElementType::Byte);
    }

    #[test]
    fn test_read_empty_list_with_zero_header() {
        let data = [0u8];
        let mut prot = ThriftSliceInputProtocol::new(&data);
        let result = read_thrift_vec::<i32, ThriftSliceInputProtocol>(&mut prot).unwrap();
        assert!(result.is_empty());
    }

    /// A Thrift list header whose `size` varint decodes above `i32::MAX`
    /// must be rejected at the protocol layer rather than wrapping into a
    /// negative `i32` and being smuggled into downstream allocation code.
    #[test]
    fn test_read_list_begin_size_above_i32_max_returns_err() {
        // List header: element_type=8 (Binary), 0xF=follow-up varint.
        // Varint 80 80 80 80 08 decodes to 0x8000_0000 = i32::MAX + 1.
        let mut data: Vec<u8> = vec![0xF8];
        data.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x08]);
        let mut prot = ThriftSliceInputProtocol::new(&data);
        let result = prot.read_list_begin();
        assert!(result.is_err(), "expected error, got {result:?}");
    }

    // Unknown fields must leave the following known field aligned.
    crate::thrift_struct! {
        struct KnownField {
            2: required i32 value;
        }
    }

    #[test]
    fn test_skip_boolean_collections_before_known_field() {
        for collection_type in [FieldType::List, FieldType::Set] {
            // False then true must not be mistaken for inline field headers.
            // Known field 2 follows with i32(42).
            let data = [0x10 | collection_type as u8, 0x22, 2, 1, 0x15, 84, 0];
            let mut protocol = ThriftSliceInputProtocol::new(&data);
            assert_eq!(KnownField::read_thrift(&mut protocol).unwrap().value, 42);
            assert!(protocol.as_slice().is_empty());
        }
    }

    #[test]
    fn test_skip_map_before_known_field() {
        // Unknown field 1: map<bool, i32> with two entries. Known field 2: i32(42).
        let data = [0x1b, 2, 0x25, 1, 14, 2, 16, 0x15, 84, 0];
        let mut protocol = ThriftSliceInputProtocol::new(&data);
        assert_eq!(KnownField::read_thrift(&mut protocol).unwrap().value, 42);
        assert!(protocol.as_slice().is_empty());
    }

    #[test]
    fn test_read_list_wrong_type() {
        // list header: 4 elements of `Boolean`
        let data = [0x42, 0x01];
        let mut prot = ThriftSliceInputProtocol::new(&data);
        // try to read as list<i32>
        let result = read_thrift_vec::<i32, ThriftSliceInputProtocol>(&mut prot);
        println!("{result:?}");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected list element type of I32 but got Bool")
        );
    }

    #[test]
    fn test_read_thrift_vec_roundtrip_i32() {
        // 2-element list of i32: header 0x25 (count=2, type=I32), two zigzag zeros.
        let data = [0x25, 0x00, 0x00];
        let mut prot = ThriftSliceInputProtocol::new(&data);
        let result = read_thrift_vec::<i32, ThriftSliceInputProtocol>(&mut prot).unwrap();
        assert_eq!(result, vec![0, 0]);
    }

    #[test]
    fn test_read_thrift_vec_size_exceeds_remaining_returns_err() {
        // Header 0xE5: 14 i32 elements, no payload. After reading the header
        // zero bytes remain, so this must error instead of reserving 14 slots
        // (and, for a larger declared size, gigabytes).
        let data = [0xE5];
        let mut prot = ThriftSliceInputProtocol::new(&data);
        let result = read_thrift_vec::<i32, ThriftSliceInputProtocol>(&mut prot);
        assert!(result.is_err(), "expected error, got {result:?}");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Thrift list size 14 exceeds remaining input length 0")
        );
    }

    #[test]
    fn test_read_thrift_vec_huge_declared_size_returns_err() {
        // Compact list header: 0xF5 = I32 elements, size follows as a varint.
        // 0xfc 0xfc 0xfc 0x33 decodes to 109_002_364 — the schema-list case
        // from #10920 — with only two leftover bytes.
        let data = [0xF5, 0xfc, 0xfc, 0xfc, 0x33, 0x00, 0x00];
        let mut prot = ThriftSliceInputProtocol::new(&data);
        let result = read_thrift_vec::<i32, ThriftSliceInputProtocol>(&mut prot);
        assert!(result.is_err(), "expected error, got {result:?}");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Thrift list size 109002364 exceeds remaining input length 2"),
            "{err}"
        );
    }
}
