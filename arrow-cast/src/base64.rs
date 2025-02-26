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

//! Functions for converting data in [`GenericBinaryArray`] such as [`StringArray`] to/from base64 encoded strings
//!
//! [`StringArray`]: arrow_array::StringArray

use arrow_array::{Array, GenericBinaryArray, GenericStringArray, OffsetSizeTrait};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::ArrowError;
use base64::encoded_len;
use base64::engine::Config;

pub use base64::prelude::*;

/// Bas64 encode each element of `array` with the provided [`Engine`]
pub fn b64_encode<E: Engine, O: OffsetSizeTrait>(
    engine: &E,
    array: &GenericBinaryArray<O>,
) -> GenericStringArray<O> {
    let lengths = array.offsets().windows(2).map(|w| {
        let len = w[1].as_usize() - w[0].as_usize();
        encoded_len(len, engine.config().encode_padding()).unwrap()
    });
    let offsets = OffsetBuffer::<O>::from_lengths(lengths);
    let buffer_len = offsets.last().unwrap().as_usize();
    let mut buffer = vec![0_u8; buffer_len];
    let mut offset = 0;

    for i in 0..array.len() {
        let len = engine
            .encode_slice(array.value(i), &mut buffer[offset..])
            .unwrap();
        offset += len;
    }
    assert_eq!(offset, buffer_len);

    // Safety: Base64 is valid UTF-8
    unsafe {
        GenericStringArray::new_unchecked(offsets, Buffer::from_vec(buffer), array.nulls().cloned())
    }
}

/// Base64 decode each element of `array` with the provided [`Engine`]
pub fn b64_decode<E: Engine, O: OffsetSizeTrait>(
    engine: &E,
    array: &GenericBinaryArray<O>,
) -> Result<GenericBinaryArray<O>, ArrowError> {
    let estimated_len = array.values().len(); // This is an overestimate
    let mut buffer = vec![0; estimated_len];

    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(O::usize_as(0));
    let mut offset = 0;

    for v in array.iter() {
        if let Some(v) = v {
            let len = engine.decode_slice(v, &mut buffer[offset..]).unwrap();
            // This cannot overflow as `len` is less than `v.len()` and `a` is valid
            offset += len;
        }
        offsets.push(O::usize_as(offset));
    }

    // Safety: offsets monotonically increasing by construction
    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

    Ok(GenericBinaryArray::new(
        offsets,
        Buffer::from_vec(buffer),
        array.nulls().cloned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::BinaryArray;
    use rand::{rng, Rng};

    fn test_engine<E: Engine>(e: &E, a: &BinaryArray) {
        let encoded = b64_encode(e, a);
        encoded.to_data().validate_full().unwrap();

        let to_decode = encoded.into();
        let decoded = b64_decode(e, &to_decode).unwrap();
        decoded.to_data().validate_full().unwrap();

        assert_eq!(&decoded, a);
    }

    #[test]
    fn test_b64() {
        let mut rng = rng();
        let len = rng.random_range(1024..1050);
        let data: BinaryArray = (0..len)
            .map(|_| {
                let len = rng.random_range(0..16);
                Some((0..len).map(|_| rng.random()).collect::<Vec<u8>>())
            })
            .collect();

        test_engine(&BASE64_STANDARD, &data);
        test_engine(&BASE64_STANDARD_NO_PAD, &data);
    }
}
