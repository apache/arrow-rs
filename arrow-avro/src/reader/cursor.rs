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

use crate::errors::AvroError;
use crate::reader::vlq;

/// A wrapper around a byte slice, providing low-level decoding for Avro
///
/// <https://avro.apache.org/docs/1.11.1/specification/#encodings>
#[derive(Debug)]
pub(crate) struct AvroCursor<'a> {
    buf: &'a [u8],
    start_len: usize,
}

impl<'a> AvroCursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            start_len: buf.len(),
        }
    }

    /// Returns the current cursor position
    #[inline]
    pub(crate) fn position(&self) -> usize {
        self.start_len - self.buf.len()
    }

    /// Read a single `u8`
    #[inline]
    pub(crate) fn get_u8(&mut self) -> Result<u8, AvroError> {
        match self.buf.first().copied() {
            Some(x) => {
                self.buf = &self.buf[1..];
                Ok(x)
            }
            None => Err(AvroError::EOF("Unexpected EOF".to_string())),
        }
    }

    #[inline]
    pub(crate) fn get_bool(&mut self) -> Result<bool, AvroError> {
        Ok(self.get_u8()? != 0)
    }

    pub(crate) fn read_vlq(&mut self) -> Result<u64, AvroError> {
        let (val, offset) = vlq::read_varint(self.buf)
            .ok_or_else(|| AvroError::ParseError("bad varint".to_string()))?;
        self.buf = &self.buf[offset..];
        Ok(val)
    }

    #[inline]
    pub(crate) fn get_int(&mut self) -> Result<i32, AvroError> {
        let varint = self.read_vlq()?;
        let val: u32 = varint
            .try_into()
            .map_err(|_| AvroError::ParseError("varint overflow".to_string()))?;
        Ok((val >> 1) as i32 ^ -((val & 1) as i32))
    }

    #[inline]
    pub(crate) fn get_long(&mut self) -> Result<i64, AvroError> {
        let val = self.read_vlq()?;
        Ok((val >> 1) as i64 ^ -((val & 1) as i64))
    }

    pub(crate) fn get_bytes(&mut self) -> Result<&'a [u8], AvroError> {
        let len: usize = self
            .get_long()?
            .try_into()
            .map_err(|_| AvroError::ParseError("offset overflow reading avro bytes".to_string()))?;

        if self.buf.len() < len {
            return Err(AvroError::EOF("Unexpected EOF reading bytes".to_string()));
        }
        let ret = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    #[inline]
    pub(crate) fn get_float(&mut self) -> Result<f32, AvroError> {
        if self.buf.len() < 4 {
            return Err(AvroError::EOF("Unexpected EOF reading float".to_string()));
        }
        let ret = f32::from_le_bytes(self.buf[..4].try_into().unwrap());
        self.buf = &self.buf[4..];
        Ok(ret)
    }

    #[inline]
    pub(crate) fn get_double(&mut self) -> Result<f64, AvroError> {
        if self.buf.len() < 8 {
            return Err(AvroError::EOF("Unexpected EOF reading double".to_string()));
        }
        let ret = f64::from_le_bytes(self.buf[..8].try_into().unwrap());
        self.buf = &self.buf[8..];
        Ok(ret)
    }

    /// Read exactly `n` bytes from the buffer (e.g. for Avro `fixed`).
    pub(crate) fn get_fixed(&mut self, n: usize) -> Result<&'a [u8], AvroError> {
        if self.buf.len() < n {
            return Err(AvroError::EOF("Unexpected EOF reading fixed".to_string()));
        }
        let ret = &self.buf[..n];
        self.buf = &self.buf[n..];
        Ok(ret)
    }

    pub(crate) fn skip_int(&mut self) -> Result<(), AvroError> {
        let offset = vlq::skip_varint(self.buf)
            .ok_or_else(|| AvroError::ParseError("bad varint".to_string()))?;
        // Check if the skipped encoded value would fail a conversion to i32;
        // skip_varint only cares about fitting in a 64-bit value.
        match offset {
            ..5 => {}
            5 if self.buf[4] < 0x10 => {}
            _ => return Err(AvroError::ParseError("varint overflow".to_owned())),
        }
        self.buf = &self.buf[offset..];
        Ok(())
    }

    pub(crate) fn skip_long(&mut self) -> Result<(), AvroError> {
        let offset = vlq::skip_varint(self.buf)
            .ok_or_else(|| AvroError::ParseError("bad varint".to_string()))?;
        // skip_varint invalidates encodings that are out of range for i64,
        // so we are good.
        self.buf = &self.buf[offset..];
        Ok(())
    }
}
