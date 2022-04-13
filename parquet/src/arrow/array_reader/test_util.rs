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

use std::sync::Arc;

use crate::basic::{ConvertedType, Encoding, Type as PhysicalType};
use crate::data_type::{ByteArray, ByteArrayType};
use crate::encodings::encoding::{get_encoder, DictEncoder, Encoder};
use crate::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type};
use crate::util::memory::{ByteBufferPtr, MemTracker};

/// Returns a descriptor for a UTF-8 column
pub fn utf8_column() -> ColumnDescPtr {
    let t = Type::primitive_type_builder("col", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .build()
        .unwrap();

    Arc::new(ColumnDescriptor::new(
        Arc::new(t),
        1,
        0,
        ColumnPath::new(vec![]),
    ))
}

/// Encode `data` with the provided `encoding`
pub fn encode_byte_array(encoding: Encoding, data: &[ByteArray]) -> ByteBufferPtr {
    let descriptor = utf8_column();
    let mem_tracker = Arc::new(MemTracker::new());
    let mut encoder =
        get_encoder::<ByteArrayType>(descriptor, encoding, mem_tracker).unwrap();

    encoder.put(data).unwrap();
    encoder.flush_buffer().unwrap()
}

/// Returns the encoded dictionary and value data
pub fn encode_dictionary(data: &[ByteArray]) -> (ByteBufferPtr, ByteBufferPtr) {
    let mut dict_encoder =
        DictEncoder::<ByteArrayType>::new(utf8_column(), Arc::new(MemTracker::new()));

    dict_encoder.put(data).unwrap();
    let encoded_rle = dict_encoder.flush_buffer().unwrap();
    let encoded_dictionary = dict_encoder.write_dict().unwrap();

    (encoded_dictionary, encoded_rle)
}

/// Encodes `data` in all the possible encodings
///
/// Returns an array of data with its associated encoding, along with an encoded dictionary
pub fn byte_array_all_encodings(
    data: Vec<impl Into<ByteArray>>,
) -> (Vec<(Encoding, ByteBufferPtr)>, ByteBufferPtr) {
    let data: Vec<_> = data.into_iter().map(Into::into).collect();
    let (encoded_dictionary, encoded_rle) = encode_dictionary(&data);

    // A column chunk with all the encodings!
    let pages = vec![
        (Encoding::PLAIN, encode_byte_array(Encoding::PLAIN, &data)),
        (
            Encoding::DELTA_BYTE_ARRAY,
            encode_byte_array(Encoding::DELTA_BYTE_ARRAY, &data),
        ),
        (
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            encode_byte_array(Encoding::DELTA_LENGTH_BYTE_ARRAY, &data),
        ),
        (Encoding::PLAIN_DICTIONARY, encoded_rle.clone()),
        (Encoding::RLE_DICTIONARY, encoded_rle),
    ];

    (pages, encoded_dictionary)
}
