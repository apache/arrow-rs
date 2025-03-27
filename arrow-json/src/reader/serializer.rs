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

use crate::reader::tape::TapeElement;
use lexical_core::FormattedSize;
use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct,
};
use serde::{Serialize, Serializer};

#[derive(Debug)]
pub struct SerializerError(String);

impl std::error::Error for SerializerError {}

impl std::fmt::Display for SerializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::ser::Error for SerializerError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self(msg.to_string())
    }
}

/// [`Serializer`] for [`TapeElement`]
///
/// Heavily based on <https://serde.rs/impl-serializer.html>
pub struct TapeSerializer<'a> {
    elements: &'a mut Vec<TapeElement>,

    /// A buffer of parsed string data
    bytes: &'a mut Vec<u8>,

    /// Offsets into `data`
    offsets: &'a mut Vec<usize>,
}

impl<'a> TapeSerializer<'a> {
    pub fn new(
        elements: &'a mut Vec<TapeElement>,
        bytes: &'a mut Vec<u8>,
        offsets: &'a mut Vec<usize>,
    ) -> Self {
        Self {
            elements,
            bytes,
            offsets,
        }
    }

    fn serialize_number(&mut self, v: &[u8]) {
        self.bytes.extend_from_slice(v);
        let idx = self.offsets.len() - 1;
        self.elements.push(TapeElement::Number(idx as _));
        self.offsets.push(self.bytes.len());
    }
}

impl<'a, 'b> Serializer for &'a mut TapeSerializer<'b> {
    type Ok = ();

    type Error = SerializerError;

    type SerializeSeq = ListSerializer<'a, 'b>;
    type SerializeTuple = ListSerializer<'a, 'b>;
    type SerializeTupleStruct = ListSerializer<'a, 'b>;
    type SerializeTupleVariant = Impossible<(), SerializerError>;
    type SerializeMap = ObjectSerializer<'a, 'b>;
    type SerializeStruct = ObjectSerializer<'a, 'b>;
    type SerializeStructVariant = Impossible<(), SerializerError>;

    fn serialize_bool(self, v: bool) -> Result<(), SerializerError> {
        self.elements.push(match v {
            true => TapeElement::True,
            false => TapeElement::False,
        });
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<(), SerializerError> {
        self.serialize_i32(v as _)
    }

    fn serialize_i16(self, v: i16) -> Result<(), SerializerError> {
        self.serialize_i32(v as _)
    }

    fn serialize_i32(self, v: i32) -> Result<(), SerializerError> {
        self.elements.push(TapeElement::I32(v));
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<(), SerializerError> {
        let low = v as i32;
        let high = (v >> 32) as i32;
        self.elements.push(TapeElement::I64(high));
        self.elements.push(TapeElement::I32(low));
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), SerializerError> {
        self.serialize_i32(v as _)
    }

    fn serialize_u16(self, v: u16) -> Result<(), SerializerError> {
        self.serialize_i32(v as _)
    }

    fn serialize_u32(self, v: u32) -> Result<(), SerializerError> {
        match i32::try_from(v) {
            Ok(v) => self.serialize_i32(v),
            Err(_) => self.serialize_i64(v as _),
        }
    }

    fn serialize_u64(self, v: u64) -> Result<(), SerializerError> {
        match i64::try_from(v) {
            Ok(v) => self.serialize_i64(v),
            Err(_) => {
                let mut buffer = [0_u8; u64::FORMATTED_SIZE];
                let s = lexical_core::write(v, &mut buffer);
                self.serialize_number(s);
                Ok(())
            }
        }
    }

    fn serialize_f32(self, v: f32) -> Result<(), SerializerError> {
        self.elements.push(TapeElement::F32(v.to_bits()));
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), SerializerError> {
        let bits = v.to_bits();
        self.elements.push(TapeElement::F64((bits >> 32) as u32));
        self.elements.push(TapeElement::F32(bits as u32));
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), SerializerError> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<(), SerializerError> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), SerializerError> {
        self.bytes.extend_from_slice(v);
        let idx = self.offsets.len() - 1;
        self.elements.push(TapeElement::String(idx as _));
        self.offsets.push(self.bytes.len());
        Ok(())
    }

    fn serialize_none(self) -> Result<(), SerializerError> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), SerializerError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), SerializerError> {
        self.elements.push(TapeElement::Null);
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), SerializerError> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<(), SerializerError> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), SerializerError>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), SerializerError>
    where
        T: ?Sized + Serialize,
    {
        let mut serializer = self.serialize_map(Some(1))?;
        serializer.serialize_key(variant)?;
        serializer.serialize_value(value)?;
        serializer.finish();
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, SerializerError> {
        Ok(ListSerializer::new(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, SerializerError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, SerializerError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, SerializerError> {
        Err(SerializerError(format!(
            "serializing tuple variants is not currently supported: {name}::{variant}"
        )))
    }

    // Maps are represented in JSON as `{ K: V, K: V, ... }`.
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, SerializerError> {
        Ok(ObjectSerializer::new(self))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, SerializerError> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, SerializerError> {
        Err(SerializerError(format!(
            "serializing struct variants is not currently supported: {name}::{variant}"
        )))
    }
}

pub struct ObjectSerializer<'a, 'b> {
    serializer: &'a mut TapeSerializer<'b>,
    start: usize,
}

impl<'a, 'b> ObjectSerializer<'a, 'b> {
    fn new(serializer: &'a mut TapeSerializer<'b>) -> Self {
        let start = serializer.elements.len();
        serializer.elements.push(TapeElement::StartObject(0));
        Self { serializer, start }
    }

    fn finish(self) {
        let end = self.serializer.elements.len() as _;
        self.serializer.elements[self.start] = TapeElement::StartObject(end);

        let end = TapeElement::EndObject(self.start as _);
        self.serializer.elements.push(end);
    }
}

impl SerializeMap for ObjectSerializer<'_, '_> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        key.serialize(&mut *self.serializer)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.finish();
        Ok(())
    }
}

impl SerializeStruct for ObjectSerializer<'_, '_> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        key.serialize(&mut *self.serializer)?;
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.finish();
        Ok(())
    }
}

pub struct ListSerializer<'a, 'b> {
    serializer: &'a mut TapeSerializer<'b>,
    start: usize,
}

impl<'a, 'b> ListSerializer<'a, 'b> {
    fn new(serializer: &'a mut TapeSerializer<'b>) -> Self {
        let start = serializer.elements.len();
        serializer.elements.push(TapeElement::StartList(0));
        Self { serializer, start }
    }

    fn finish(self) {
        let end = self.serializer.elements.len() as _;
        self.serializer.elements[self.start] = TapeElement::StartList(end);

        let end = TapeElement::EndList(self.start as _);
        self.serializer.elements.push(end);
    }
}

impl SerializeSeq for ListSerializer<'_, '_> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.finish();
        Ok(())
    }
}

impl SerializeTuple for ListSerializer<'_, '_> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.finish();
        Ok(())
    }
}

impl SerializeTupleStruct for ListSerializer<'_, '_> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.finish();
        Ok(())
    }
}
