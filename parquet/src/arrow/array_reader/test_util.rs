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

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;
use std::any::Any;
use std::sync::Arc;

use crate::arrow::array_reader::ArrayReader;
use crate::basic::{ConvertedType, Encoding, Type as PhysicalType};
use crate::column::page::{PageIterator, PageReader};
use crate::data_type::{ByteArray, ByteArrayType};
use crate::encodings::encoding::{get_encoder, DictEncoder, Encoder};
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

/// Array reader for test.
pub struct InMemoryArrayReader {
    data_type: ArrowType,
    array: ArrayRef,
    def_levels: Option<Vec<i16>>,
    rep_levels: Option<Vec<i16>>,
    last_idx: usize,
    cur_idx: usize,
    need_consume_records: usize,
}

impl InMemoryArrayReader {
    pub fn new(
        data_type: ArrowType,
        array: ArrayRef,
        def_levels: Option<Vec<i16>>,
        rep_levels: Option<Vec<i16>>,
    ) -> Self {
        assert!(def_levels
            .as_ref()
            .map(|d| d.len() == array.len())
            .unwrap_or(true));

        assert!(rep_levels
            .as_ref()
            .map(|r| r.len() == array.len())
            .unwrap_or(true));

        Self {
            data_type,
            array,
            def_levels,
            rep_levels,
            cur_idx: 0,
            last_idx: 0,
            need_consume_records: 0,
        }
    }
}

impl ArrayReader for InMemoryArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        assert_ne!(batch_size, 0);
        // This replicates the logical normally performed by
        // RecordReader to delimit semantic records
        let read = match &self.rep_levels {
            Some(rep_levels) => {
                let rep_levels = &rep_levels[self.cur_idx..];
                let mut levels_read = 0;
                let mut records_read = 0;
                while levels_read < rep_levels.len() && records_read < batch_size {
                    if rep_levels[levels_read] == 0 {
                        records_read += 1; // Start of new record
                    }
                    levels_read += 1;
                }

                // Find end of current record
                while levels_read < rep_levels.len() && rep_levels[levels_read] != 0 {
                    levels_read += 1
                }
                levels_read
            }
            None => batch_size.min(self.array.len() - self.cur_idx),
        };
        self.need_consume_records += read;
        Ok(read)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let batch_size = self.need_consume_records;
        assert_ne!(batch_size, 0);
        self.last_idx = self.cur_idx;
        self.cur_idx += batch_size;
        self.need_consume_records = 0;
        Ok(self.array.slice(self.last_idx, batch_size))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let array = self.next_batch(num_records)?;
        Ok(array.len())
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels
            .as_ref()
            .map(|l| &l[self.last_idx..self.cur_idx])
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels
            .as_ref()
            .map(|l| &l[self.last_idx..self.cur_idx])
    }
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
