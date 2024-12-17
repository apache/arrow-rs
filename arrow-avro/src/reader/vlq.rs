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

/// Decoder for zig-zag encoded variable length (VLW) integers
///
/// See also:
/// <https://avro.apache.org/docs/1.11.1/specification/#primitive-types-1>
/// <https://protobuf.dev/programming-guides/encoding/#varints>
#[derive(Debug, Default)]
pub struct VLQDecoder {
    /// Scratch space for decoding VLQ integers
    in_progress: u64,
    shift: u32,
}

impl VLQDecoder {
    /// Decode a signed long from `buf`
    pub fn long(&mut self, buf: &mut &[u8]) -> Option<i64> {
        while let Some(byte) = buf.first().copied() {
            *buf = &buf[1..];
            self.in_progress |= ((byte & 0x7F) as u64) << self.shift;
            self.shift += 7;
            if byte & 0x80 == 0 {
                let val = self.in_progress;
                self.in_progress = 0;
                self.shift = 0;
                return Some((val >> 1) as i64 ^ -((val & 1) as i64));
            }
        }
        None
    }
}

/// Read a varint from `buf` returning the decoded `u64` and the number of bytes read
#[inline]
pub(crate) fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let first = *buf.first()?;
    if first < 0x80 {
        return Some((first as u64, 1));
    }

    if let Some(array) = buf.get(..10) {
        return read_varint_array(array.try_into().unwrap());
    }

    read_varint_slow(buf)
}

/// Based on
/// - <https://github.com/tokio-rs/prost/blob/master/prost/src/encoding/varint.rs#L71>
/// - <https://github.com/google/protobuf/blob/3.3.x/src/google/protobuf/io/coded_stream.cc#L365-L406>
/// - <https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358>
#[inline]
fn read_varint_array(buf: [u8; 10]) -> Option<(u64, usize)> {
    let mut in_progress = 0_u64;
    for (idx, b) in buf.into_iter().take(9).enumerate() {
        in_progress += (b as u64) << (7 * idx);
        if b < 0x80 {
            return Some((in_progress, idx + 1));
        }
        in_progress -= 0x80 << (7 * idx);
    }

    let b = buf[9] as u64;
    in_progress += b << (7 * 9);
    (b < 0x02).then_some((in_progress, 10))
}

#[inline(never)]
#[cold]
fn read_varint_slow(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0;
    for (count, byte) in buf.iter().take(10).enumerate() {
        let byte = buf[count];
        value |= u64::from(byte & 0x7F) << (count * 7);
        if byte <= 0x7F {
            // Check for u64::MAX overflow. See [`ConsumeVarint`][1] for details.
            // [1]: https://github.com/protocolbuffers/protobuf-go/blob/v1.27.1/encoding/protowire/wire.go#L358
            return (count != 9 || byte < 2).then_some((value, count + 1));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_var(mut n: u64, dst: &mut [u8]) -> usize {
        let mut i = 0;

        while n >= 0x80 {
            dst[i] = 0x80 | (n as u8);
            i += 1;
            n >>= 7;
        }

        dst[i] = n as u8;
        i + 1
    }

    fn varint_test(a: u64) {
        let mut buf = [0_u8; 10];
        let len = encode_var(a, &mut buf);
        assert_eq!(read_varint(&buf[..len]).unwrap(), (a, len));
        assert_eq!(read_varint(&buf).unwrap(), (a, len));
    }

    #[test]
    fn test_varint() {
        varint_test(0);
        varint_test(4395932);
        varint_test(u64::MAX);

        for _ in 0..1000 {
            varint_test(rand::random());
        }
    }
}
