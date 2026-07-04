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

use arrow::array::AsArray;
use arrow_array::{
    Array, ArrayRef, GenericBinaryArray, GenericStringArray, OffsetSizeTrait, RecordBatch,
    StringViewArray,
};
use arrow_schema::{ArrowError, DataType};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderBuilder, ParquetRecordBatchReader},
    },
    basic::Encoding,
    file::properties::WriterProperties,
};

#[test]
fn test_invalid_utf8() {
    // a parquet file with 1 column with invalid utf8
    let data = vec![
        80, 65, 82, 49, 21, 6, 21, 22, 21, 22, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 4, 21, 0, 18,
        28, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111, 0, 0, 0, 3, 1,
        5, 0, 0, 0, 104, 101, 255, 108, 111, 38, 110, 28, 21, 12, 25, 37, 6, 0, 25, 24, 2, 99, 49,
        21, 0, 22, 2, 22, 102, 22, 102, 38, 8, 60, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5,
        104, 101, 255, 108, 111, 0, 0, 0, 21, 4, 25, 44, 72, 4, 114, 111, 111, 116, 21, 2, 0, 21,
        12, 37, 2, 24, 2, 99, 49, 37, 0, 76, 28, 0, 0, 0, 22, 2, 25, 28, 25, 28, 38, 110, 28, 21,
        12, 25, 37, 6, 0, 25, 24, 2, 99, 49, 21, 0, 22, 2, 22, 102, 22, 102, 38, 8, 60, 54, 0, 40,
        5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111, 0, 0, 0, 22, 102, 22, 2, 0, 40,
        44, 65, 114, 114, 111, 119, 50, 32, 45, 32, 78, 97, 116, 105, 118, 101, 32, 82, 117, 115,
        116, 32, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 32, 111, 102,
        32, 65, 114, 114, 111, 119, 0, 130, 0, 0, 0, 80, 65, 82, 49,
    ];

    let file = Bytes::from(data);
    let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 10).unwrap();

    let error = record_batch_reader.next().unwrap().unwrap_err();

    assert!(
        error.to_string().contains("invalid utf-8 sequence"),
        "{}",
        error
    );
}

#[test]
fn test_invalid_utf8_string_array() {
    test_invalid_utf8_string_array_inner::<i32>();
}

#[test]
fn test_invalid_utf8_large_string_array() {
    test_invalid_utf8_string_array_inner::<i64>();
}

fn test_invalid_utf8_string_array_inner<O: OffsetSizeTrait>() {
    let cases = [
        invalid_utf8_first_char::<O>(),
        invalid_utf8_first_char_long_strings::<O>(),
        invalid_utf8_later_char::<O>(),
        invalid_utf8_later_char_long_strings::<O>(),
        invalid_utf8_later_char_really_long_strings::<O>(),
        invalid_utf8_later_char_really_long_strings2::<O>(),
    ];
    for array in &cases {
        for encoding in STRING_ENCODINGS {
            // data is not valid utf8 we can not construct a correct StringArray
            // safely, so purposely create an invalid StringArray
            let array = unsafe {
                GenericStringArray::<O>::new_unchecked(
                    array.offsets().clone(),
                    array.values().clone(),
                    array.nulls().cloned(),
                )
            };
            let data_type = array.data_type().clone();
            let data = write_to_parquet_with_encoding(Arc::new(array), *encoding);
            let err = read_from_parquet(data).unwrap_err();
            let expected_err = "Parquet argument error: Parquet error: encountered non UTF-8 data";
            assert!(
                err.to_string().contains(expected_err),
                "data type: {data_type}, expected: {expected_err}, got: {err}"
            );
        }
    }
}

#[test]
fn test_invalid_utf8_string_view_array() {
    let cases = [
        invalid_utf8_first_char::<i32>(),
        invalid_utf8_first_char_long_strings::<i32>(),
        invalid_utf8_later_char::<i32>(),
        invalid_utf8_later_char_long_strings::<i32>(),
        invalid_utf8_later_char_really_long_strings::<i32>(),
        invalid_utf8_later_char_really_long_strings2::<i32>(),
    ];

    for encoding in STRING_ENCODINGS {
        for array in &cases {
            let array = arrow_cast::cast(&array, &DataType::BinaryView).unwrap();
            let array = array.as_binary_view();

            // data is not valid utf8 we can not construct a correct StringArray
            // safely, so purposely create an invalid StringViewArray
            let array = unsafe {
                StringViewArray::new_unchecked(
                    array.views().clone(),
                    array.data_buffers().to_vec(),
                    array.nulls().cloned(),
                )
            };

            let data_type = array.data_type().clone();
            let data = write_to_parquet_with_encoding(Arc::new(array), *encoding);
            let err = read_from_parquet(data).unwrap_err();
            let expected_err = "Parquet argument error: Parquet error: encountered non UTF-8 data";
            assert!(
                err.to_string().contains(expected_err),
                "data type: {data_type}, expected: {expected_err}, got: {err}"
            );
        }
    }
}

/// Encodings suitable for string data
const STRING_ENCODINGS: &[Option<Encoding>] = &[
    None,
    Some(Encoding::PLAIN),
    Some(Encoding::DELTA_LENGTH_BYTE_ARRAY),
    Some(Encoding::DELTA_BYTE_ARRAY),
];

/// Invalid Utf-8 sequence in the first character
/// <https://stackoverflow.com/questions/1301402/example-invalid-utf8-string>
const INVALID_UTF8_FIRST_CHAR: &[u8] = &[0xa0, 0xa1, 0x20, 0x20];

/// Invalid Utf=8 sequence in NOT the first character
/// <https://stackoverflow.com/questions/1301402/example-invalid-utf8-string>
const INVALID_UTF8_LATER_CHAR: &[u8] = &[0x20, 0x20, 0x20, 0xa0, 0xa1, 0x20, 0x20];

/// returns a BinaryArray with invalid UTF8 data in the first character
fn invalid_utf8_first_char<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
    let valid: &[u8] = b"   ";
    let invalid = INVALID_UTF8_FIRST_CHAR;
    GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(invalid)])
}

/// Returns a BinaryArray with invalid UTF8 data in the first character of a
/// string larger than 12 bytes which is handled specially when reading
/// `ByteViewArray`s
fn invalid_utf8_first_char_long_strings<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
    let valid: &[u8] = b"   ";
    let mut invalid = vec![];
    invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
    invalid.extend_from_slice(INVALID_UTF8_FIRST_CHAR);
    GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(&invalid)])
}

/// returns a BinaryArray with invalid UTF8 data in a character other than
/// the first (this is checked in a special codepath)
fn invalid_utf8_later_char<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
    let valid: &[u8] = b"   ";
    let invalid: &[u8] = INVALID_UTF8_LATER_CHAR;
    GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(invalid)])
}

/// returns a BinaryArray with invalid UTF8 data in a character other than
/// the first in a string larger than 12 bytes which is handled specially
/// when reading `ByteViewArray`s (this is checked in a special codepath)
fn invalid_utf8_later_char_long_strings<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
    let valid: &[u8] = b"   ";
    let mut invalid = vec![];
    invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
    invalid.extend_from_slice(INVALID_UTF8_LATER_CHAR);
    GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(&invalid)])
}

/// returns a BinaryArray with invalid UTF8 data in a character other than
/// the first in a string larger than 128 bytes which is handled specially
/// when reading `ByteViewArray`s (this is checked in a special codepath)
fn invalid_utf8_later_char_really_long_strings<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
    let valid: &[u8] = b"   ";
    let mut invalid = vec![];
    for _ in 0..10 {
        // each instance is 38 bytes
        invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
    }
    invalid.extend_from_slice(INVALID_UTF8_LATER_CHAR);
    GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(&invalid)])
}

/// returns a BinaryArray with small invalid UTF8 data followed by a large
/// invalid UTF8 data in a character other than the first in a string larger
fn invalid_utf8_later_char_really_long_strings2<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
    let valid: &[u8] = b"   ";
    let mut valid_long = vec![];
    for _ in 0..10 {
        // each instance is 38 bytes
        valid_long.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
    }
    let invalid = INVALID_UTF8_LATER_CHAR;
    GenericBinaryArray::<O>::from_iter(vec![
        None,
        Some(valid),
        Some(invalid),
        None,
        Some(&valid_long),
        Some(valid),
    ])
}

/// writes the array into a single column parquet file with the specified
/// encoding.
///
/// If no encoding is specified, use default (dictionary) encoding
fn write_to_parquet_with_encoding(array: ArrayRef, encoding: Option<Encoding>) -> Vec<u8> {
    let batch = RecordBatch::try_from_iter(vec![("c", array)]).unwrap();
    let mut data = vec![];
    let schema = batch.schema();
    let props = encoding.map(|encoding| {
        WriterProperties::builder()
            // must disable dictionary encoding to actually use encoding
            .set_dictionary_enabled(false)
            .set_encoding(encoding)
            .build()
    });

    {
        let mut writer = ArrowWriter::try_new(&mut data, schema, props).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();
    };
    data
}

/// read the parquet file into a record batch
fn read_from_parquet(data: Vec<u8>) -> Result<Vec<RecordBatch>, ArrowError> {
    let reader = ArrowReaderBuilder::try_new(bytes::Bytes::from(data))
        .unwrap()
        .build()
        .unwrap();

    reader.collect()
}
