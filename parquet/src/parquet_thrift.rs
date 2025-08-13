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

//! experimental replacement for thrift decoder
// this is a copy of TCompactSliceInputProtocol, but modified
// to not allocate byte arrays or strings.
#![allow(dead_code)]

use std::cmp::Ordering;

use crate::errors::{ParquetError, Result};

// Couldn't implement thrift structs with f64 do to lack of Eq
// for f64. This is a hacky workaround for now...there are other
// wrappers out there that should probably be used instead.
// thrift seems to re-export an impl from ordered-float
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedF64(f64);

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
}

impl TryFrom<u8> for FieldType {
    type Error = ParquetError;
    fn try_from(value: u8) -> Result<Self> {
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
            _ => Err(general_err!("Unexpected struct field type{}", value)),
        }
    }
}

impl TryFrom<ElementType> for FieldType {
    type Error = ParquetError;
    fn try_from(value: ElementType) -> std::result::Result<Self, Self::Error> {
        match value {
            ElementType::Bool => Ok(Self::BooleanTrue),
            ElementType::Byte => Ok(Self::Byte),
            ElementType::I16 => Ok(Self::I16),
            ElementType::I32 => Ok(Self::I32),
            ElementType::I64 => Ok(Self::I64),
            ElementType::Double => Ok(Self::Double),
            ElementType::Binary => Ok(Self::Binary),
            ElementType::List => Ok(Self::List),
            ElementType::Struct => Ok(Self::Struct),
            _ => Err(general_err!("Unexpected list element type{:?}", value)),
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
}

impl TryFrom<u8> for ElementType {
    type Error = ParquetError;
    fn try_from(value: u8) -> Result<Self> {
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
            _ => Err(general_err!("Unexpected list/set element type{}", value)),
        }
    }
}

pub(crate) struct FieldIdentifier {
    pub(crate) field_type: FieldType,
    pub(crate) id: i16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListIdentifier {
    pub(crate) element_type: ElementType,
    pub(crate) size: i32,
}

/// A more performant implementation of [`TCompactInputProtocol`] that reads a slice
///
/// [`TCompactInputProtocol`]: thrift::protocol::TCompactInputProtocol
pub(crate) struct ThriftCompactInputProtocol<'a> {
    buf: &'a [u8],
    // Identifier of the last field deserialized for a struct.
    last_read_field_id: i16,
    // Stack of the last read field ids (a new entry is added each time a nested struct is read).
    read_field_id_stack: Vec<i16>,
    // Boolean value for a field.
    // Saved because boolean fields and their value are encoded in a single byte,
    // and reading the field only occurs after the field id is read.
    pending_read_bool_value: Option<bool>,
}

impl<'b, 'a: 'b> ThriftCompactInputProtocol<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            last_read_field_id: 0,
            read_field_id_stack: Vec::with_capacity(16),
            pending_read_bool_value: None,
        }
    }

    pub fn reset_buffer(&mut self, buf: &'a [u8]) {
        self.buf = buf;
        self.last_read_field_id = 0;
        self.read_field_id_stack.clear();
        self.pending_read_bool_value = None;
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.buf
    }

    fn read_vlq(&mut self) -> Result<u64> {
        let mut in_progress = 0;
        let mut shift = 0;
        loop {
            let byte = self.read_byte()?;
            in_progress |= ((byte & 0x7F) as u64).wrapping_shl(shift);
            shift += 7;
            if byte & 0x80 == 0 {
                return Ok(in_progress);
            }
        }
    }

    fn read_zig_zag(&mut self) -> Result<i64> {
        let val = self.read_vlq()?;
        Ok((val >> 1) as i64 ^ -((val & 1) as i64))
    }

    fn read_list_set_begin(&mut self) -> Result<(ElementType, i32)> {
        let header = self.read_byte()?;
        let element_type = ElementType::try_from(header & 0x0f)?;

        let possible_element_count = (header & 0xF0) >> 4;
        let element_count = if possible_element_count != 15 {
            // high bits set high if count and type encoded separately
            possible_element_count as i32
        } else {
            self.read_vlq()? as _
        };

        Ok((element_type, element_count))
    }

    pub(crate) fn read_struct_begin(&mut self) -> Result<()> {
        self.read_field_id_stack.push(self.last_read_field_id);
        self.last_read_field_id = 0;
        Ok(())
    }

    pub(crate) fn read_struct_end(&mut self) -> Result<()> {
        self.last_read_field_id = self
            .read_field_id_stack
            .pop()
            .expect("should have previous field ids");
        Ok(())
    }

    pub(crate) fn read_field_begin(&mut self) -> Result<FieldIdentifier> {
        // we can read at least one byte, which is:
        // - the type
        // - the field delta and the type
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xf0) >> 4;
        let field_type = FieldType::try_from(field_type & 0xf)?;

        match field_type {
            FieldType::Stop => Ok(FieldIdentifier {
                field_type: FieldType::Stop,
                id: 0,
            }),
            _ => {
                // special handling for bools
                if field_type == FieldType::BooleanFalse {
                    self.pending_read_bool_value = Some(false);
                } else if field_type == FieldType::BooleanTrue {
                    self.pending_read_bool_value = Some(true);
                }
                if field_delta != 0 {
                    self.last_read_field_id = self
                        .last_read_field_id
                        .checked_add(field_delta as i16)
                        .map_or_else(
                            || {
                                Err(general_err!(format!(
                                    "cannot add {} to {}",
                                    field_delta, self.last_read_field_id
                                )))
                            },
                            Ok,
                        )?;
                } else {
                    self.last_read_field_id = self.read_i16()?;
                };

                Ok(FieldIdentifier {
                    field_type,
                    id: self.last_read_field_id,
                })
            }
        }
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool> {
        match self.pending_read_bool_value.take() {
            Some(b) => Ok(b),
            None => {
                let b = self.read_byte()?;
                // Previous versions of the thrift specification said to use 0 and 1 inside collections,
                // but that differed from existing implementations.
                // The specification was updated in https://github.com/apache/thrift/commit/2c29c5665bc442e703480bb0ee60fe925ffe02e8.
                // At least the go implementation seems to have followed the previously documented values.
                match b {
                    0x01 => Ok(true),
                    0x00 | 0x02 => Ok(false),
                    unkn => Err(general_err!(format!("cannot convert {unkn} into bool"))),
                }
            }
        }
    }

    pub(crate) fn read_bytes(&mut self) -> Result<&'b [u8]> {
        let len = self.read_vlq()? as usize;
        let ret = self.buf.get(..len).ok_or_else(eof_error)?;
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    pub(crate) fn read_string(&mut self) -> Result<&'b str> {
        let slice = self.read_bytes()?;
        Ok(std::str::from_utf8(slice)?)
    }

    pub(crate) fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_byte()? as _)
    }

    pub(crate) fn read_i16(&mut self) -> Result<i16> {
        Ok(self.read_zig_zag()? as _)
    }

    pub(crate) fn read_i32(&mut self) -> Result<i32> {
        Ok(self.read_zig_zag()? as _)
    }

    pub(crate) fn read_i64(&mut self) -> Result<i64> {
        self.read_zig_zag()
    }

    pub(crate) fn read_double(&mut self) -> Result<f64> {
        let slice = self.buf.get(..8).ok_or_else(eof_error)?;
        self.buf = &self.buf[8..];
        match slice.try_into() {
            Ok(slice) => Ok(f64::from_le_bytes(slice)),
            Err(_) => Err(general_err!("Unexpected error converting slice")),
        }
    }

    pub(crate) fn read_list_begin(&mut self) -> Result<ListIdentifier> {
        let (element_type, element_count) = self.read_list_set_begin()?;
        Ok(ListIdentifier {
            element_type,
            size: element_count,
        })
    }

    pub(crate) fn read_list_end(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn read_byte(&mut self) -> Result<u8> {
        let ret = *self.buf.first().ok_or_else(eof_error)?;
        self.buf = &self.buf[1..];
        Ok(ret)
    }

    #[inline]
    fn skip_bytes(&mut self, n: usize) -> Result<()> {
        self.buf.get(..n).ok_or_else(eof_error)?;
        self.buf = &self.buf[n..];
        Ok(())
    }

    fn skip_vlq(&mut self) -> Result<()> {
        loop {
            let byte = self.read_byte()?;
            if byte & 0x80 == 0 {
                return Ok(());
            }
        }
    }

    fn skip_binary(&mut self) -> Result<()> {
        let len = self.read_vlq()? as usize;
        self.skip_bytes(len)
    }

    /// Skip a field with type `field_type` recursively until the default
    /// maximum skip depth is reached.
    pub(crate) fn skip(&mut self, field_type: FieldType) -> Result<()> {
        // TODO: magic number
        self.skip_till_depth(field_type, 64)
    }

    /// Empty structs in unions consist of a single byte of 0 for the field stop record.
    /// This skips that byte without pushing to the field id stack.
    pub(crate) fn skip_empty_struct(&mut self) -> Result<()> {
        let b = self.read_byte()?;
        if b != 0 {
            Err(general_err!("Empty struct has fields"))
        } else {
            Ok(())
        }
    }

    /// Skip a field with type `field_type` recursively up to `depth` levels.
    fn skip_till_depth(&mut self, field_type: FieldType, depth: i8) -> Result<()> {
        if depth == 0 {
            return Err(general_err!(format!("cannot parse past {:?}", field_type)));
        }

        match field_type {
            FieldType::BooleanFalse | FieldType::BooleanTrue => self.read_bool().map(|_| ()),
            FieldType::Byte => self.read_i8().map(|_| ()),
            FieldType::I16 => self.skip_vlq().map(|_| ()),
            FieldType::I32 => self.skip_vlq().map(|_| ()),
            FieldType::I64 => self.skip_vlq().map(|_| ()),
            FieldType::Double => self.skip_bytes(8).map(|_| ()),
            FieldType::Binary => self.skip_binary().map(|_| ()),
            FieldType::Struct => {
                self.read_struct_begin()?;
                loop {
                    let field_ident = self.read_field_begin()?;
                    if field_ident.field_type == FieldType::Stop {
                        break;
                    }
                    self.skip_till_depth(field_ident.field_type, depth - 1)?;
                }
                self.read_struct_end()
            }
            FieldType::List => {
                let list_ident = self.read_list_begin()?;
                for _ in 0..list_ident.size {
                    let element_type = FieldType::try_from(list_ident.element_type)?;
                    self.skip_till_depth(element_type, depth - 1)?;
                }
                self.read_list_end()
            }
            // no list or map types in parquet format
            u => Err(general_err!(format!("cannot skip field type {:?}", &u))),
        }
    }
}

fn eof_error() -> ParquetError {
    eof_err!("Unexpected EOF")
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for bool {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_bool()
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for i8 {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_i8()
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for i16 {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_i16()
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for i32 {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_i32()
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for i64 {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_i64()
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for OrderedF64 {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        Ok(OrderedF64(prot.read_double()?))
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for &'a str {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_string()
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for String {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        Ok(prot.read_string()?.to_owned())
    }
}

impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for &'a [u8] {
    type Error = ParquetError;
    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
        prot.read_bytes()
    }
}

impl<'a, T> TryFrom<&mut ThriftCompactInputProtocol<'a>> for Vec<T>
where
    T: for<'b> TryFrom<&'b mut ThriftCompactInputProtocol<'a>>,
    ParquetError: for<'b> From<<T as TryFrom<&'b mut ThriftCompactInputProtocol<'a>>>::Error>,
{
    type Error = ParquetError;

    fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self, Self::Error> {
        let list_ident = prot.read_list_begin()?;
        let mut res = Vec::with_capacity(list_ident.size as usize);
        for _ in 0..list_ident.size {
            let val = T::try_from(prot)?;
            res.push(val);
        }

        Ok(res)
    }
}
