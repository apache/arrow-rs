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
