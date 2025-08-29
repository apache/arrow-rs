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

use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::errors::{ParquetError, Result};

// Couldn't implement thrift structs with f64 do to lack of Eq
// for f64. This is a hacky workaround for now...there are other
// wrappers out there that should probably be used instead.
// thrift seems to re-export an impl from ordered-float
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
    pub(crate) bool_val: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListIdentifier {
    pub(crate) element_type: ElementType,
    pub(crate) size: i32,
}

pub(crate) trait ThriftCompactInputProtocol<'a> {
    fn read_byte(&mut self) -> Result<u8>;

    fn read_bytes(&mut self) -> Result<&'a [u8]>;

    fn read_bytes_owned(&mut self) -> Result<Vec<u8>>;

    fn skip_bytes(&mut self, n: usize) -> Result<()>;

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

    fn read_list_begin(&mut self) -> Result<ListIdentifier> {
        let header = self.read_byte()?;
        let element_type = ElementType::try_from(header & 0x0f)?;

        let possible_element_count = (header & 0xF0) >> 4;
        let element_count = if possible_element_count != 15 {
            // high bits set high if count and type encoded separately
            possible_element_count as i32
        } else {
            self.read_vlq()? as _
        };

        Ok(ListIdentifier {
            element_type,
            size: element_count,
        })
    }

    fn read_field_begin(&mut self, last_field_id: i16) -> Result<FieldIdentifier> {
        // we can read at least one byte, which is:
        // - the type
        // - the field delta and the type
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xf0) >> 4;
        let field_type = FieldType::try_from(field_type & 0xf)?;
        let mut bool_val: Option<bool> = None;

        match field_type {
            FieldType::Stop => Ok(FieldIdentifier {
                field_type: FieldType::Stop,
                id: 0,
                bool_val,
            }),
            _ => {
                // special handling for bools
                if field_type == FieldType::BooleanFalse {
                    bool_val = Some(false);
                } else if field_type == FieldType::BooleanTrue {
                    bool_val = Some(true);
                }
                let field_id = if field_delta != 0 {
                    last_field_id.checked_add(field_delta as i16).map_or_else(
                        || {
                            Err(general_err!(format!(
                                "cannot add {} to {}",
                                field_delta, last_field_id
                            )))
                        },
                        Ok,
                    )?
                } else {
                    self.read_i16()?
                };

                Ok(FieldIdentifier {
                    field_type,
                    id: field_id,
                    bool_val,
                })
            }
        }
    }

    // This is a specialized version of read_field_begin, solely for use in parsing
    // PageLocation structs in the offset index. This function assumes that the delta
    // field will always be less than 0xf, fields will be in order, and no boolean fields
    // will be read. This also skips validation of the field type.
    //
    // Returns a tuple of (field_type, field_delta)
    fn read_field_header(&mut self) -> Result<(u8, u8)> {
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xf0) >> 4;
        let field_type = field_type & 0xf;
        Ok((field_type, field_delta))
    }

    // not to be used for bool struct fields, just for bool arrays
    fn read_bool(&mut self) -> Result<bool> {
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

    fn read_string(&mut self) -> Result<&'a str> {
        let slice = self.read_bytes()?;
        Ok(std::str::from_utf8(slice)?)
    }

    fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_byte()? as _)
    }

    fn read_i16(&mut self) -> Result<i16> {
        Ok(self.read_zig_zag()? as _)
    }

    fn read_i32(&mut self) -> Result<i32> {
        Ok(self.read_zig_zag()? as _)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.read_zig_zag()
    }

    fn read_double(&mut self) -> Result<f64>;

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
    fn skip(&mut self, field_type: FieldType) -> Result<()> {
        // TODO: magic number
        self.skip_till_depth(field_type, 64)
    }

    /// Empty structs in unions consist of a single byte of 0 for the field stop record.
    /// This skips that byte without pushing to the field id stack.
    fn skip_empty_struct(&mut self) -> Result<()> {
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
            // boolean field has no data
            FieldType::BooleanFalse | FieldType::BooleanTrue => Ok(()),
            FieldType::Byte => self.read_i8().map(|_| ()),
            FieldType::I16 => self.skip_vlq().map(|_| ()),
            FieldType::I32 => self.skip_vlq().map(|_| ()),
            FieldType::I64 => self.skip_vlq().map(|_| ()),
            FieldType::Double => self.skip_bytes(8).map(|_| ()),
            FieldType::Binary => self.skip_binary().map(|_| ()),
            FieldType::Struct => {
                let mut last_field_id = 0i16;
                loop {
                    let field_ident = self.read_field_begin(last_field_id)?;
                    if field_ident.field_type == FieldType::Stop {
                        break;
                    }
                    self.skip_till_depth(field_ident.field_type, depth - 1)?;
                    last_field_id = field_ident.id;
                }
                Ok(())
            }
            FieldType::List => {
                let list_ident = self.read_list_begin()?;
                for _ in 0..list_ident.size {
                    let element_type = FieldType::try_from(list_ident.element_type)?;
                    self.skip_till_depth(element_type, depth - 1)?;
                }
                Ok(())
            }
            // no list or map types in parquet format
            u => Err(general_err!(format!("cannot skip field type {:?}", &u))),
        }
    }
}

pub(crate) struct ThriftSliceInputProtocol<'a> {
    buf: &'a [u8],
}

impl<'a> ThriftSliceInputProtocol<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub fn reset_buffer(&mut self, buf: &'a [u8]) {
        self.buf = buf;
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.buf
    }
}

impl<'b, 'a: 'b> ThriftCompactInputProtocol<'b> for ThriftSliceInputProtocol<'a> {
    #[inline]
    fn read_byte(&mut self) -> Result<u8> {
        let ret = *self.buf.first().ok_or_else(eof_error)?;
        self.buf = &self.buf[1..];
        Ok(ret)
    }

    fn read_bytes(&mut self) -> Result<&'b [u8]> {
        let len = self.read_vlq()? as usize;
        let ret = self.buf.get(..len).ok_or_else(eof_error)?;
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    fn read_bytes_owned(&mut self) -> Result<Vec<u8>> {
        Ok(self.read_bytes()?.to_vec())
    }

    #[inline]
    fn skip_bytes(&mut self, n: usize) -> Result<()> {
        self.buf.get(..n).ok_or_else(eof_error)?;
        self.buf = &self.buf[n..];
        Ok(())
    }

    fn read_double(&mut self) -> Result<f64> {
        let slice = self.buf.get(..8).ok_or_else(eof_error)?;
        self.buf = &self.buf[8..];
        match slice.try_into() {
            Ok(slice) => Ok(f64::from_le_bytes(slice)),
            Err(_) => Err(general_err!("Unexpected error converting slice")),
        }
    }
}

fn eof_error() -> ParquetError {
    eof_err!("Unexpected EOF")
}

// input protocol that's only intended for use in reading page headers. not fully implemented
// so this shouldn't be used generally.
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
    fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0_u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        unimplemented!()
    }

    fn read_bytes_owned(&mut self) -> Result<Vec<u8>> {
        let len = self.read_vlq()? as usize;
        let mut v = Vec::with_capacity(len);
        std::io::copy(&mut self.reader.by_ref().take(len as u64), &mut v)?;
        Ok(v)
    }

    fn skip_bytes(&mut self, n: usize) -> Result<()> {
        std::io::copy(
            &mut self.reader.by_ref().take(n as u64),
            &mut std::io::sink(),
        )?;
        Ok(())
    }

    fn read_double(&mut self) -> Result<f64> {
        let mut buf = [0_u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }
}

pub(crate) trait ReadThrift<'a, R: ThriftCompactInputProtocol<'a>> {
    // used to read generated enums and structs
    fn read_thrift(prot: &mut R) -> Result<Self>
    where
        Self: Sized;
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for bool {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_bool()
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i8 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_i8()
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i16 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_i16()
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i32 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_i32()
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for i64 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_i64()
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for OrderedF64 {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(OrderedF64(prot.read_double()?))
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for &'a str {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_string()
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for String {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        Ok(String::from_utf8(prot.read_bytes_owned()?)?)
    }
}

impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for &'a [u8] {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        prot.read_bytes()
    }
}

pub(crate) fn read_thrift_vec<'a, T, R>(prot: &mut R) -> Result<Vec<T>>
where
    R: ThriftCompactInputProtocol<'a>,
    T: ReadThrift<'a, R>,
{
    let list_ident = prot.read_list_begin()?;
    let mut res = Vec::with_capacity(list_ident.size as usize);
    for _ in 0..list_ident.size {
        let val = T::read_thrift(prot)?;
        res.push(val);
    }
    Ok(res)
}

/////////////////////////
// thrift compact output

pub(crate) struct ThriftCompactOutputProtocol<W: Write> {
    writer: W,
}

impl<W: Write> ThriftCompactOutputProtocol<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self { writer }
    }

    pub(crate) fn inner(&self) -> &W {
        &self.writer
    }

    fn write_byte(&mut self, b: u8) -> Result<()> {
        self.writer.write_all(&[b])?;
        Ok(())
    }

    fn write_vlq(&mut self, val: u64) -> Result<()> {
        let mut v = val;
        while v > 0x7f {
            self.write_byte(v as u8 | 0x80)?;
            v >>= 7;
        }
        self.write_byte(v as u8)
    }

    fn write_zig_zag(&mut self, val: i64) -> Result<()> {
        let s = (val < 0) as i64;
        self.write_vlq((((val ^ -s) << 1) + s) as u64)
    }

    pub(crate) fn write_field_begin(
        &mut self,
        field_type: FieldType,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<()> {
        let delta = field_id.wrapping_sub(last_field_id);
        if delta > 0 && delta <= 0xf {
            self.write_byte((delta as u8) << 4 | field_type as u8)
        } else {
            self.write_byte(field_type as u8)?;
            self.write_i16(field_id)
        }
    }

    pub(crate) fn write_list_begin(&mut self, element_type: ElementType, len: usize) -> Result<()> {
        if len < 15 {
            self.write_byte((len as u8) << 4 | element_type as u8)
        } else {
            self.write_byte(0xf0u8 | element_type as u8)?;
            self.write_vlq(len as _)
        }
    }

    pub(crate) fn write_struct_end(&mut self) -> Result<()> {
        self.write_byte(0)
    }

    pub(crate) fn write_bytes(&mut self, val: &[u8]) -> Result<()> {
        self.write_vlq(val.len() as u64)?;
        self.writer.write_all(val)?;
        Ok(())
    }

    pub(crate) fn write_empty_struct(&mut self, field_id: i16, last_field_id: i16) -> Result<i16> {
        self.write_field_begin(FieldType::Struct, field_id, last_field_id)?;
        self.write_struct_end()?;
        Ok(last_field_id)
    }

    pub(crate) fn write_bool(&mut self, val: bool) -> Result<()> {
        match val {
            true => self.write_byte(1),
            false => self.write_byte(2),
        }
    }

    pub(crate) fn write_i8(&mut self, val: i8) -> Result<()> {
        self.write_byte(val as u8)
    }

    pub(crate) fn write_i16(&mut self, val: i16) -> Result<()> {
        self.write_zig_zag(val as _)
    }

    pub(crate) fn write_i32(&mut self, val: i32) -> Result<()> {
        self.write_zig_zag(val as _)
    }

    pub(crate) fn write_i64(&mut self, val: i64) -> Result<()> {
        self.write_zig_zag(val as _)
    }

    pub(crate) fn write_double(&mut self, val: f64) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        Ok(())
    }
}

pub(crate) trait WriteThrift {
    const ELEMENT_TYPE: ElementType;

    // used to write generated enums and structs
    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()>;
}

impl<T> WriteThrift for Vec<T>
where
    T: WriteThrift,
{
    const ELEMENT_TYPE: ElementType = ElementType::List;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_list_begin(T::ELEMENT_TYPE, self.len())?;
        for item in self {
            item.write_thrift(writer)?;
        }
        Ok(())
    }
}

impl WriteThrift for bool {
    const ELEMENT_TYPE: ElementType = ElementType::Bool;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_bool(*self)
    }
}

impl WriteThrift for i8 {
    const ELEMENT_TYPE: ElementType = ElementType::Byte;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_i8(*self)
    }
}

impl WriteThrift for i16 {
    const ELEMENT_TYPE: ElementType = ElementType::I16;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_i16(*self)
    }
}

impl WriteThrift for i32 {
    const ELEMENT_TYPE: ElementType = ElementType::I32;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_i32(*self)
    }
}

impl WriteThrift for i64 {
    const ELEMENT_TYPE: ElementType = ElementType::I64;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_i64(*self)
    }
}

impl WriteThrift for OrderedF64 {
    const ELEMENT_TYPE: ElementType = ElementType::Double;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_double(self.0)
    }
}

impl WriteThrift for &[u8] {
    const ELEMENT_TYPE: ElementType = ElementType::Binary;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_bytes(self)
    }
}

impl WriteThrift for &str {
    const ELEMENT_TYPE: ElementType = ElementType::Binary;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_bytes(self.as_bytes())
    }
}

impl WriteThrift for String {
    const ELEMENT_TYPE: ElementType = ElementType::Binary;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        writer.write_bytes(self.as_bytes())
    }
}

pub(crate) trait WriteThriftField {
    // used to write struct fields (which may be basic types or generated types).
    // write the field header and field value. returns `field_id`.
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16>;
}

impl WriteThriftField for bool {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
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

impl WriteThriftField for i8 {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Byte, field_id, last_field_id)?;
        writer.write_i8(*self)?;
        Ok(field_id)
    }
}

impl WriteThriftField for i16 {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::I16, field_id, last_field_id)?;
        writer.write_i16(*self)?;
        Ok(field_id)
    }
}

impl WriteThriftField for i32 {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::I32, field_id, last_field_id)?;
        writer.write_i32(*self)?;
        Ok(field_id)
    }
}

impl WriteThriftField for i64 {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::I64, field_id, last_field_id)?;
        writer.write_i64(*self)?;
        Ok(field_id)
    }
}

impl WriteThriftField for OrderedF64 {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Double, field_id, last_field_id)?;
        writer.write_double(self.0)?;
        Ok(field_id)
    }
}

impl WriteThriftField for &[u8] {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Binary, field_id, last_field_id)?;
        writer.write_bytes(self)?;
        Ok(field_id)
    }
}

impl WriteThriftField for &str {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
        field_id: i16,
        last_field_id: i16,
    ) -> Result<i16> {
        writer.write_field_begin(FieldType::Binary, field_id, last_field_id)?;
        writer.write_bytes(self.as_bytes())?;
        Ok(field_id)
    }
}

impl WriteThriftField for String {
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
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
    fn write_thrift_field<W: Write>(
        &self,
        writer: &mut ThriftCompactOutputProtocol<W>,
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

    use super::*;
    use std::fmt::Debug;

    pub(crate) fn test_roundtrip<T>(val: T)
    where
        T: for<'a> ReadThrift<'a, ThriftSliceInputProtocol<'a>>
            + WriteThrift
            + PartialEq
            + Debug,
    {
        let buf = Vec::<u8>::new();
        let mut writer = ThriftCompactOutputProtocol::new(buf);
        val.write_thrift(&mut writer).unwrap();

        //println!("serialized: {:x?}", writer.inner());

        let mut prot = ThriftSliceInputProtocol::new(writer.inner());
        let read_val = T::read_thrift(&mut prot).unwrap();
        assert_eq!(val, read_val);
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
}
