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

use bytes::Bytes;
use std::sync::Arc;

use crate::arrow::array_reader::ArrayReader;
use crate::basic::{ConvertedType, Encoding, Type as PhysicalType};
use crate::column::page::{PageIterator, PageReader};
use crate::data_type::{ByteArray, ByteArrayType, Int32Type};
use crate::encodings::encoding::{DictEncoder, Encoder, get_encoder};
use crate::errors::Result;
use crate::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type};

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
pub fn encode_byte_array(encoding: Encoding, data: &[ByteArray]) -> Bytes {
    let desc = utf8_column();
    let mut encoder = get_encoder::<ByteArrayType>(encoding, &desc).unwrap();

    encoder.put(data).unwrap();
    encoder.flush_buffer().unwrap()
}

/// Returns the encoded dictionary and value data
pub fn encode_dictionary(data: &[ByteArray]) -> (Bytes, Bytes) {
    let mut dict_encoder = DictEncoder::<ByteArrayType>::new(utf8_column());

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
) -> (Vec<(Encoding, Bytes)>, Bytes) {
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

/// Build a real `PrimitiveArrayReader<Int32Type>` from raw non-null values
/// and definition/repetition levels. This exercises the full production
/// `RecordReader` code path, including selective padding when
/// `padding_threshold` is set.
///
/// `values` must contain only the non-null values (entries where
/// `def_levels[i] == max_def_level`), in order, as Parquet encodes them.
///
/// `padding_threshold` controls null filtering: `None` for full padding
/// (top-level columns), `Some(threshold)` for selective padding (list
/// children — typically the parent list's def_level).
pub fn make_int32_page_reader(
    values: &[i32],
    def_levels: &[i16],
    rep_levels: &[i16],
    max_def_level: i16,
    max_rep_level: i16,
    padding_threshold: Option<i16>,
) -> Box<dyn ArrayReader> {
    use crate::arrow::array_reader::PrimitiveArrayReader;
    use crate::arrow::arrow_reader::DEFAULT_BATCH_SIZE;
    use crate::util::InMemoryPageIterator;
    use crate::util::test_common::page_util::{DataPageBuilder, DataPageBuilderImpl};

    let leaf_type = Type::primitive_type_builder("leaf", PhysicalType::INT32)
        .build()
        .unwrap();

    let desc = Arc::new(ColumnDescriptor::new(
        Arc::new(leaf_type),
        max_def_level,
        max_rep_level,
        ColumnPath::new(vec![]),
    ));

    let mut pb = DataPageBuilderImpl::new(desc.clone(), def_levels.len() as u32, true);
    if max_rep_level > 0 {
        pb.add_rep_levels(max_rep_level, rep_levels);
    }
    if max_def_level > 0 {
        pb.add_def_levels(max_def_level, def_levels);
    }
    pb.add_values::<Int32Type>(Encoding::PLAIN, values);

    let pages = vec![vec![pb.consume()]];
    let page_iter = InMemoryPageIterator::new(pages);
    Box::new(
        PrimitiveArrayReader::<Int32Type>::new(
            Box::new(page_iter),
            desc,
            None,
            DEFAULT_BATCH_SIZE,
            padding_threshold,
        )
        .unwrap(),
    )
}

/// Iterator for testing reading empty columns
#[derive(Default)]
pub struct EmptyPageIterator {}

impl Iterator for EmptyPageIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl PageIterator for EmptyPageIterator {}
