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
use crate::reader::vlq::read_varint;
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

    pub(crate) fn read_vlq(&mut self) -> Result<u64, ArrowError> {
        let (val, offset) = read_varint(self.buf)
            .ok_or_else(|| ArrowError::ParseError("bad varint".to_string()))?;
        self.buf = &self.buf[offset..];
        Ok(val)
    }

    /// Decode a zig-zag encoded Avro int (32-bit).
    #[inline]
    pub(crate) fn get_int(&mut self) -> Result<i32, ArrowError> {
        let varint = self.read_vlq()?;
        let val: u32 = varint
            .try_into()
            .map_err(|_| ArrowError::ParseError("varint overflow".to_string()))?;
        Ok((val >> 1) as i32 ^ -((val & 1) as i32))
    }

    /// Decode a zig-zag encoded Avro long (64-bit).
    #[inline]
    pub(crate) fn get_long(&mut self) -> Result<i64, ArrowError> {
        let val = self.read_vlq()?;
        Ok((val >> 1) as i64 ^ -((val & 1) as i64))
    }

    /// Read a variable-length byte array from Avro (where the length is stored as an Avro long).
    pub(crate) fn get_bytes(&mut self) -> Result<&'a [u8], ArrowError> {
        let len: usize = self.get_long()?.try_into().map_err(|_| {
            ArrowError::ParseError("offset overflow reading avro bytes".to_string())
        })?;

        if self.buf.len() < len {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading bytes".to_string(),
            ));
        }
        let ret = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    /// Read a little-endian 32-bit float
    #[inline]
    pub(crate) fn get_float(&mut self) -> Result<f32, ArrowError> {
        if self.buf.len() < 4 {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading float".to_string(),
            ));
        }
        let ret = f32::from_le_bytes(self.buf[..4].try_into().unwrap());
        self.buf = &self.buf[4..];
        Ok(ret)
    }

    /// Read a little-endian 64-bit float
    #[inline]
    pub(crate) fn get_double(&mut self) -> Result<f64, ArrowError> {
        if self.buf.len() < 8 {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading double".to_string(),
            ));
        }
        let ret = f64::from_le_bytes(self.buf[..8].try_into().unwrap());
        self.buf = &self.buf[8..];
        Ok(ret)
    }

    /// Read exactly `n` bytes from the buffer (e.g. for Avro `fixed`).
    pub(crate) fn get_fixed(&mut self, n: usize) -> Result<&'a [u8], ArrowError> {
        if self.buf.len() < n {
            return Err(ArrowError::ParseError(
                "Unexpected EOF reading fixed".to_string(),
            ));
        }
        let ret = &self.buf[..n];
        self.buf = &self.buf[n..];
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::ArrowError;

    #[test]
    fn test_new_and_position() {
        let data = [1, 2, 3, 4];
        let cursor = AvroCursor::new(&data);
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn test_get_u8_ok() {
        let data = [0x12, 0x34, 0x56];
        let mut cursor = AvroCursor::new(&data);
        assert_eq!(cursor.get_u8().unwrap(), 0x12);
        assert_eq!(cursor.position(), 1);
        assert_eq!(cursor.get_u8().unwrap(), 0x34);
        assert_eq!(cursor.position(), 2);
        assert_eq!(cursor.get_u8().unwrap(), 0x56);
        assert_eq!(cursor.position(), 3);
    }

    #[test]
    fn test_get_u8_eof() {
        let data = [];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_u8();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("Unexpected EOF"))
        );
    }

    #[test]
    fn test_get_bool_ok() {
        let data = [0x00, 0x01, 0xFF];
        let mut cursor = AvroCursor::new(&data);
        assert!(!cursor.get_bool().unwrap()); // 0x00 -> false
        assert!(cursor.get_bool().unwrap()); // 0x01 -> true
        assert!(cursor.get_bool().unwrap()); // 0xFF -> true (non-zero)
    }

    #[test]
    fn test_get_bool_eof() {
        let data = [];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_bool();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("Unexpected EOF"))
        );
    }

    #[test]
    fn test_read_vlq_ok() {
        let data = [0x80, 0x01, 0x05];
        let mut cursor = AvroCursor::new(&data);
        let val1 = cursor.read_vlq().unwrap();
        assert_eq!(val1, 128);
        let val2 = cursor.read_vlq().unwrap();
        assert_eq!(val2, 5);
    }

    #[test]
    fn test_read_vlq_bad_varint() {
        let data = [0x80];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.read_vlq();
        assert!(matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("bad varint")));
    }

    #[test]
    fn test_get_int_ok() {
        let data = [0x04, 0x03]; // encodes +2, -2
        let mut cursor = AvroCursor::new(&data);
        assert_eq!(cursor.get_int().unwrap(), 2);
        assert_eq!(cursor.get_int().unwrap(), -2);
    }

    #[test]
    fn test_get_int_overflow() {
        let data = [0x80, 0x80, 0x80, 0x80, 0x10];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_int();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("varint overflow"))
        );
    }

    #[test]
    fn test_get_long_ok() {
        let data = [0x04, 0x03, 0xAC, 0x02];
        let mut cursor = AvroCursor::new(&data);
        assert_eq!(cursor.get_long().unwrap(), 2);
        assert_eq!(cursor.get_long().unwrap(), -2);
        assert_eq!(cursor.get_long().unwrap(), 150);
    }

    #[test]
    fn test_get_long_eof() {
        let data = [0x80];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_long();
        assert!(matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("bad varint")));
    }

    #[test]
    fn test_get_bytes_ok() {
        let data = [0x06, 0xAA, 0xBB, 0xCC, 0x05, 0x01];
        let mut cursor = AvroCursor::new(&data);
        let bytes = cursor.get_bytes().unwrap();
        assert_eq!(bytes, [0xAA, 0xBB, 0xCC]);
        assert_eq!(cursor.position(), 4);
    }

    #[test]
    fn test_get_bytes_overflow() {
        let data = [0xAC, 0x02, 0x01, 0x02, 0x03];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_bytes();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("Unexpected EOF reading bytes"))
        );
    }

    #[test]
    fn test_get_bytes_negative_length() {
        let data = [0x01];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_bytes();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("offset overflow"))
        );
    }

    #[test]
    fn test_get_float_ok() {
        let data = [0x00, 0x00, 0x80, 0x3F, 0x01];
        let mut cursor = AvroCursor::new(&data);
        let val = cursor.get_float().unwrap();
        assert!((val - 1.0).abs() < f32::EPSILON);
        assert_eq!(cursor.position(), 4);
    }

    #[test]
    fn test_get_float_eof() {
        let data = [0x00, 0x00, 0x80];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_float();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("Unexpected EOF reading float"))
        );
    }

    #[test]
    fn test_get_double_ok() {
        let data = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x99];
        let mut cursor = AvroCursor::new(&data);
        let val = cursor.get_double().unwrap();
        assert!((val - 1.0).abs() < f64::EPSILON);
        assert_eq!(cursor.position(), 8);
    }

    #[test]
    fn test_get_double_eof() {
        let data = [0x00, 0x00, 0x00, 0x00]; // only 4 bytes
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_double();
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("Unexpected EOF reading double"))
        );
    }

    #[test]
    fn test_get_fixed_ok() {
        let data = [0x11, 0x22, 0x33, 0x44];
        let mut cursor = AvroCursor::new(&data);
        let val = cursor.get_fixed(2).unwrap();
        assert_eq!(val, [0x11, 0x22]);
        assert_eq!(cursor.position(), 2);

        let val = cursor.get_fixed(2).unwrap();
        assert_eq!(val, [0x33, 0x44]);
        assert_eq!(cursor.position(), 4);
    }

    #[test]
    fn test_get_fixed_eof() {
        let data = [0x11, 0x22];
        let mut cursor = AvroCursor::new(&data);
        let result = cursor.get_fixed(3);
        assert!(
            matches!(result, Err(ArrowError::ParseError(msg)) if msg.contains("Unexpected EOF reading fixed"))
        );
    }
}
