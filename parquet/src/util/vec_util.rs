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

/// Resize the `buf` to a new len `n` without initialization.
///
/// Replacing `resize` on the `buf` with `reserve` and `set_len` can skip the initialization
/// cost for a good performance.
/// And the `set_len` here is safe because the element of the vector is byte whose destruction
/// does nothing.
// TODO: remove this clippy allow lint if it is used by a module without feature gate.
#[allow(dead_code)]
pub fn resize_buffer_without_init(buf: &mut Vec<u8>, n: usize) {
    if n > buf.capacity() {
        buf.reserve(n - buf.len());
    }
    unsafe { buf.set_len(n) };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resize_and_check(source: Vec<u8>, new_len: usize) {
        let mut new = source.clone();
        resize_buffer_without_init(&mut new, new_len);

        assert_eq!(new.len(), new_len);
        if new.len() > source.len() {
            assert_eq!(&new[..source.len()], &source[..]);
        } else {
            assert_eq!(&new[..], &source[..new.len()]);
        }
    }

    #[test]
    fn test_resize_buffer_without_init() {
        let cases = [
            (vec![1, 2, 3], 10),
            (vec![1, 2, 3], 3),
            (vec![1, 2, 3, 4, 5], 3),
            (vec![], 10),
            (vec![], 0),
        ];

        for (vector, resize_len) in cases {
            resize_and_check(vector, resize_len);
        }
    }
}
