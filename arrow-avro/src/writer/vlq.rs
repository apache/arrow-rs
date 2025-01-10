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

/// Encoder for zig-zag encoded variable length integers
///
/// This complements the VLQ decoding logic used by Avro. Zig-zag encoding maps signed integers
/// to unsigned integers so that small magnitudes (both positive and negative) produce smaller varints.
/// After zig-zag encoding, values are encoded as a series of bytes where the lower 7 bits are data
/// and the high bit indicates if another byte follows.
///
/// See also:
/// <https://avro.apache.org/docs/1.11.1/specification/#primitive-types-1>
/// <https://protobuf.dev/programming-guides/encoding/#varints>
#[derive(Debug, Default)]
pub struct VLQEncoder;

impl VLQEncoder {
    /// Encode a signed 64-bit integer `value` into `output` using Avro's zig-zag varint encoding.
    ///
    /// Zig-zag encoding:
    /// ```text
    /// encoded = (value << 1) ^ (value >> 63)
    /// ```
    ///
    /// Then `encoded` is written as a variable-length integer (varint):
    /// - Extract 7 bits at a time
    /// - If more bits remain, set the MSB of the current byte to 1 and continue
    /// - Otherwise, MSB is 0 and this is the last byte
    pub fn long(&mut self, value: i64, output: &mut Vec<u8>) {
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        self.encode_varint(zigzag, output);
    }

    fn encode_varint(&self, mut val: u64, output: &mut Vec<u8>) {
        while (val & !0x7F) != 0 {
            output.push(((val & 0x7F) as u8) | 0x80);
            val >>= 7;
        }
        output.push(val as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_varint(buf: &mut &[u8]) -> Option<u64> {
        let mut value = 0_u64;
        for i in 0..10 {
            let b = buf.get(i).copied()?;
            let lower_7 = (b & 0x7F) as u64;
            value |= lower_7 << (7 * i);
            if b & 0x80 == 0 {
                *buf = &buf[i + 1..];
                return Some(value);
            }
        }
        None // more than 10 bytes or not terminated properly
    }

    fn decode_zigzag(val: u64) -> i64 {
        ((val >> 1) as i64) ^ -((val & 1) as i64)
    }

    fn decode_long(buf: &mut &[u8]) -> Option<i64> {
        let val = decode_varint(buf)?;
        Some(decode_zigzag(val))
    }

    fn round_trip(value: i64) {
        let mut encoder = VLQEncoder;
        let mut buf = Vec::new();
        encoder.long(value, &mut buf);
        let mut slice = buf.as_slice();
        let decoded = decode_long(&mut slice).expect("Failed to decode value");
        assert_eq!(decoded, value, "Round-trip mismatch for value {}", value);
        assert!(slice.is_empty(), "Not all bytes consumed");
    }

    #[test]
    fn test_round_trip() {
        round_trip(0);
        round_trip(1);
        round_trip(-1);
        round_trip(12345678);
        round_trip(-12345678);
        round_trip(i64::MAX);
        round_trip(i64::MIN);
    }

    #[test]
    fn test_random_values() {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let val: i64 = rng.gen();
            round_trip(val);
        }
    }
}
