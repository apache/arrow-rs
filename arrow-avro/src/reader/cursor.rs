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

use arrow_schema::ArrowError;

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
    pub(crate) fn get_u8(&mut self) -> Result<u8, ArrowError> {
        match self.buf.first().copied() {
            Some(x) => {
                self.buf = &self.buf[1..];
                Ok(x)
            }
            None => Err(ArrowError::ParseError("Unexpected EOF".to_string())),
        }
    }

    #[inline]
    pub(crate) fn get_bool(&mut self) -> Result<bool, ArrowError> {
        Ok(self.get_u8()? != 0)
    }

    #[inline]
    pub(crate) fn get_int(&mut self) -> Result<i32, ArrowError> {
        let mut in_progress = 0;
        let mut shift = 0;

        while let Some(byte) = self.buf.first().copied() {
            self.buf = &self.buf[1..];
            in_progress |= ((byte & 0x7F) as u32) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                let val = in_progress;
                in_progress = 0;
                shift = 0;
                return Ok((val >> 1) as i32 ^ -((val & 1) as i32));
            }
        }
        Err(ArrowError::ParseError(
            "Unexpected EOF reading int".to_string(),
        ))
    }

    #[inline]
    pub(crate) fn get_long(&mut self) -> Result<i64, ArrowError> {
        let mut in_progress = 0;
        let mut shift = 0;

        while let Some(byte) = self.buf.first().copied() {
            self.buf = &self.buf[1..];
            in_progress |= ((byte & 0x7F) as u64) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                let val = in_progress;
                in_progress = 0;
                shift = 0;
                return Ok((val >> 1) as i64 ^ -((val & 1) as i64));
            }
        }
        Err(ArrowError::ParseError(
            "Unexpected EOF reading long".to_string(),
        ))
    }

    pub(crate) fn get_bytes(&mut self) -> Result<&'a [u8], ArrowError> {
        let len: usize = self.get_long()?.try_into().map_err(|_| {
            ArrowError::ParseError("offset overflow reading avro bytes".to_string())
        })?;

        if (self.buf.len() < len) {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading bytes".to_string(),
            ));
        }
        let ret = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    #[inline]
    pub(crate) fn get_float(&mut self) -> Result<f32, ArrowError> {
        if (self.buf.len() < 4) {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading float".to_string(),
            ));
        }
        let ret = f32::from_le_bytes(self.buf[..4].try_into().unwrap());
        self.buf = &self.buf[4..];
        Ok(ret)
    }

    #[inline]
    pub(crate) fn get_double(&mut self) -> Result<f64, ArrowError> {
        if (self.buf.len() < 8) {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading float".to_string(),
            ));
        }
        let ret = f64::from_le_bytes(self.buf[..8].try_into().unwrap());
        self.buf = &self.buf[8..];
        Ok(ret)
    }
}
