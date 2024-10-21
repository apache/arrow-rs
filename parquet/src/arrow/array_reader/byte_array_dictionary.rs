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

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;

use crate::arrow::array_reader::byte_array::{ByteArrayDecoder, ByteArrayDecoderPlain};
use crate::arrow::array_reader::{read_records, skip_records, ArrayReader};
use crate::arrow::buffer::{dictionary_buffer::DictionaryBuffer, offset_buffer::OffsetBuffer};
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{ConvertedType, Encoding};
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::encodings::rle::RleDecoder;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::FromBytes;

/// A macro to reduce verbosity of [`make_byte_array_dictionary_reader`]
macro_rules! make_reader {
    (
        ($pages:expr, $column_desc:expr, $data_type:expr) => match ($k:expr, $v:expr) {
            $(($key_arrow:pat, $value_arrow:pat) => ($key_type:ty, $value_type:ty),)+
        }
    ) => {
        match (($k, $v)) {
            $(
                ($key_arrow, $value_arrow) => {
                    let reader = GenericRecordReader::new($column_desc);
                    Ok(Box::new(ByteArrayDictionaryReader::<$key_type, $value_type>::new(
                        $pages, $data_type, reader,
                    )))
                }
            )+
            _ => Err(general_err!(
                "unsupported data type for byte array dictionary reader - {}",
                $data_type
            )),
        }
    }
}

/// Returns an [`ArrayReader`] that decodes the provided byte array column
///
/// This will attempt to preserve any dictionary encoding present in the parquet data
///
/// It will be unable to preserve the dictionary encoding if:
///
/// * A single read spans across multiple column chunks
/// * A column chunk contains non-dictionary encoded pages
///
/// It is therefore recommended that if `pages` contains data from multiple column chunks,
/// that the read batch size used is a divisor of the row group size
///
pub fn make_byte_array_dictionary_reader(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
    arrow_type: Option<ArrowType>,
) -> Result<Box<dyn ArrayReader>> {
    // Check if Arrow type is specified, else create it from Parquet type
    let data_type = match arrow_type {
        Some(t) => t,
        None => parquet_to_arrow_field(column_desc.as_ref())?
            .data_type()
            .clone(),
    };

    match &data_type {
        ArrowType::Dictionary(key_type, value_type) => {
            make_reader! {
                (pages, column_desc, data_type) => match (key_type.as_ref(), value_type.as_ref()) {
                    (ArrowType::UInt8, ArrowType::Binary | ArrowType::Utf8) => (u8, i32),
                    (ArrowType::UInt8, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (u8, i64),
                    (ArrowType::Int8, ArrowType::Binary | ArrowType::Utf8) => (i8, i32),
                    (ArrowType::Int8, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (i8, i64),
                    (ArrowType::UInt16, ArrowType::Binary | ArrowType::Utf8) => (u16, i32),
                    (ArrowType::UInt16, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (u16, i64),
                    (ArrowType::Int16, ArrowType::Binary | ArrowType::Utf8) => (i16, i32),
                    (ArrowType::Int16, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (i16, i64),
                    (ArrowType::UInt32, ArrowType::Binary | ArrowType::Utf8) => (u32, i32),
                    (ArrowType::UInt32, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (u32, i64),
                    (ArrowType::Int32, ArrowType::Binary | ArrowType::Utf8) => (i32, i32),
                    (ArrowType::Int32, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (i32, i64),
                    (ArrowType::UInt64, ArrowType::Binary | ArrowType::Utf8) => (u64, i32),
                    (ArrowType::UInt64, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (u64, i64),
                    (ArrowType::Int64, ArrowType::Binary | ArrowType::Utf8) => (i64, i32),
                    (ArrowType::Int64, ArrowType::LargeBinary | ArrowType::LargeUtf8) => (i64, i64),
                }
            }
        }
        _ => Err(general_err!(
            "invalid non-dictionary data type for byte array dictionary reader - {}",
            data_type
        )),
    }
}

/// An [`ArrayReader`] for dictionary encoded variable length byte arrays
///
/// Will attempt to preserve any dictionary encoding present in the parquet data
struct ByteArrayDictionaryReader<K: ArrowNativeType, V: OffsetSizeTrait> {
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: GenericRecordReader<DictionaryBuffer<K, V>, DictionaryDecoder<K, V>>,
}

impl<K, V> ByteArrayDictionaryReader<K, V>
where
    K: FromBytes + Ord + ArrowNativeType,
    V: OffsetSizeTrait,
{
    fn new(
        pages: Box<dyn PageIterator>,
        data_type: ArrowType,
        record_reader: GenericRecordReader<DictionaryBuffer<K, V>, DictionaryDecoder<K, V>>,
    ) -> Self {
        Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            record_reader,
        }
    }
}

impl<K, V> ArrayReader for ByteArrayDictionaryReader<K, V>
where
    K: FromBytes + Ord + ArrowNativeType,
    V: OffsetSizeTrait,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let buffer = self.record_reader.consume_record_data();
        let null_buffer = self.record_reader.consume_bitmap_buffer();
        let array = buffer.into_array(null_buffer, &self.data_type)?;

        self.def_levels_buffer = self.record_reader.consume_def_levels();
        self.rep_levels_buffer = self.record_reader.consume_rep_levels();
        self.record_reader.reset();

        Ok(array)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        skip_records(&mut self.record_reader, self.pages.as_mut(), num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_deref()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_deref()
    }
}

/// If the data is dictionary encoded decode the key data directly, so that the dictionary
/// encoding can be preserved. Otherwise fallback to decoding using [`ByteArrayDecoder`]
/// and compute a fresh dictionary in [`ByteArrayDictionaryReader::next_batch`]
enum MaybeDictionaryDecoder {
    Dict {
        decoder: RleDecoder,
        /// This is a maximum as the null count is not always known, e.g. value data from
        /// a v1 data page
        max_remaining_values: usize,
    },
    Fallback(ByteArrayDecoder),
}

/// A [`ColumnValueDecoder`] for dictionary encoded variable length byte arrays
struct DictionaryDecoder<K, V> {
    /// The current dictionary
    dict: Option<ArrayRef>,

    /// Dictionary decoder
    decoder: Option<MaybeDictionaryDecoder>,

    validate_utf8: bool,

    value_type: ArrowType,

    phantom: PhantomData<(K, V)>,
}

impl<K, V> ColumnValueDecoder for DictionaryDecoder<K, V>
where
    K: FromBytes + Ord + ArrowNativeType,
    V: OffsetSizeTrait,
{
    type Buffer = DictionaryBuffer<K, V>;

    fn new(col: &ColumnDescPtr) -> Self {
        let validate_utf8 = col.converted_type() == ConvertedType::UTF8;

        let value_type = match (V::IS_LARGE, col.converted_type() == ConvertedType::UTF8) {
            (true, true) => ArrowType::LargeUtf8,
            (true, false) => ArrowType::LargeBinary,
            (false, true) => ArrowType::Utf8,
            (false, false) => ArrowType::Binary,
        };

        Self {
            dict: None,
            decoder: None,
            validate_utf8,
            value_type,
            phantom: Default::default(),
        }
    }

    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        encoding: Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        if !matches!(
            encoding,
            Encoding::PLAIN | Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
        ) {
            return Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ));
        }

        if K::from_usize(num_values as usize).is_none() {
            return Err(general_err!("dictionary too large for index type"));
        }

        let len = num_values as usize;
        let mut buffer = OffsetBuffer::<V>::default();
        let mut decoder = ByteArrayDecoderPlain::new(buf, len, Some(len), self.validate_utf8);
        decoder.read(&mut buffer, usize::MAX)?;

        let array = buffer.into_array(None, self.value_type.clone());
        self.dict = Some(Arc::new(array));
        Ok(())
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        let decoder = match encoding {
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                let bit_width = data[0];
                let mut decoder = RleDecoder::new(bit_width);
                decoder.set_data(data.slice(1..));
                MaybeDictionaryDecoder::Dict {
                    decoder,
                    max_remaining_values: num_values.unwrap_or(num_levels),
                }
            }
            _ => MaybeDictionaryDecoder::Fallback(ByteArrayDecoder::new(
                encoding,
                data,
                num_levels,
                num_values,
                self.validate_utf8,
            )?),
        };

        self.decoder = Some(decoder);
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        match self.decoder.as_mut().expect("decoder set") {
            MaybeDictionaryDecoder::Fallback(decoder) => {
                decoder.read(out.spill_values()?, num_values, None)
            }
            MaybeDictionaryDecoder::Dict {
                decoder,
                max_remaining_values,
            } => {
                let len = num_values.min(*max_remaining_values);

                let dict = self
                    .dict
                    .as_ref()
                    .ok_or_else(|| general_err!("missing dictionary page for column"))?;

                assert_eq!(dict.data_type(), &self.value_type);

                if dict.is_empty() {
                    return Ok(0); // All data must be NULL
                }

                match out.as_keys(dict) {
                    Some(keys) => {
                        // Happy path - can just copy keys
                        // Keys will be validated on conversion to arrow

                        // TODO: Push vec into decoder (#5177)
                        let start = keys.len();
                        keys.resize(start + len, K::default());
                        let len = decoder.get_batch(&mut keys[start..])?;
                        keys.truncate(start + len);
                        *max_remaining_values -= len;
                        Ok(len)
                    }
                    None => {
                        // Sad path - need to recompute dictionary
                        //
                        // This either means we crossed into a new column chunk whilst
                        // reading this batch, or encountered non-dictionary encoded data
                        let values = out.spill_values()?;
                        let mut keys = vec![K::default(); len];
                        let len = decoder.get_batch(&mut keys)?;

                        assert_eq!(dict.data_type(), &self.value_type);

                        let data = dict.to_data();
                        let dict_buffers = data.buffers();
                        let dict_offsets = dict_buffers[0].typed_data::<V>();
                        let dict_values = dict_buffers[1].as_slice();

                        values.extend_from_dictionary(&keys[..len], dict_offsets, dict_values)?;
                        *max_remaining_values -= len;
                        Ok(len)
                    }
                }
            }
        }
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        match self.decoder.as_mut().expect("decoder set") {
            MaybeDictionaryDecoder::Fallback(decoder) => decoder.skip::<V>(num_values, None),
            MaybeDictionaryDecoder::Dict {
                decoder,
                max_remaining_values,
            } => {
                let num_values = num_values.min(*max_remaining_values);
                *max_remaining_values -= num_values;
                decoder.skip(num_values)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::compute::cast;
    use arrow_array::{Array, StringArray};
    use arrow_buffer::Buffer;

    use crate::arrow::array_reader::test_util::{
        byte_array_all_encodings, encode_dictionary, utf8_column,
    };
    use crate::arrow::record_reader::buffer::ValuesBuffer;
    use crate::data_type::ByteArray;

    use super::*;

    fn utf8_dictionary() -> ArrowType {
        ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8))
    }

    #[test]
    fn test_dictionary_preservation() {
        let data_type = utf8_dictionary();

        let data: Vec<_> = vec!["0", "1", "0", "1", "2", "1", "2"]
            .into_iter()
            .map(ByteArray::from)
            .collect();
        let (dict, encoded) = encode_dictionary(&data);

        let column_desc = utf8_column();
        let mut decoder = DictionaryDecoder::<i32, i32>::new(&column_desc);

        decoder
            .set_dict(dict, 3, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        decoder
            .set_data(Encoding::RLE_DICTIONARY, encoded, 14, Some(data.len()))
            .unwrap();

        let mut output = DictionaryBuffer::<i32, i32>::default();
        assert_eq!(decoder.read(&mut output, 3).unwrap(), 3);

        let mut valid = vec![false, false, true, true, false, true];
        let valid_buffer = Buffer::from_iter(valid.iter().cloned());
        output.pad_nulls(0, 3, valid.len(), valid_buffer.as_slice());

        assert!(matches!(output, DictionaryBuffer::Dict { .. }));

        assert_eq!(decoder.read(&mut output, 4).unwrap(), 4);

        valid.extend_from_slice(&[false, false, true, true, false, true, true, false]);
        let valid_buffer = Buffer::from_iter(valid.iter().cloned());
        output.pad_nulls(6, 4, 8, valid_buffer.as_slice());

        assert!(matches!(output, DictionaryBuffer::Dict { .. }));

        let array = output.into_array(Some(valid_buffer), &data_type).unwrap();
        assert_eq!(array.data_type(), &data_type);

        let array = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), 14);

        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![
                None,
                None,
                Some("0"),
                Some("1"),
                None,
                Some("0"),
                None,
                None,
                Some("1"),
                Some("2"),
                None,
                Some("1"),
                Some("2"),
                None
            ]
        )
    }

    #[test]
    fn test_dictionary_preservation_skip() {
        let data_type = utf8_dictionary();

        let data: Vec<_> = vec!["0", "1", "0", "1", "2", "1", "2"]
            .into_iter()
            .map(ByteArray::from)
            .collect();
        let (dict, encoded) = encode_dictionary(&data);

        let column_desc = utf8_column();
        let mut decoder = DictionaryDecoder::<i32, i32>::new(&column_desc);

        decoder
            .set_dict(dict, 3, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        decoder
            .set_data(Encoding::RLE_DICTIONARY, encoded, 7, Some(data.len()))
            .unwrap();

        let mut output = DictionaryBuffer::<i32, i32>::default();

        // read two skip one
        assert_eq!(decoder.read(&mut output, 2).unwrap(), 2);
        assert_eq!(decoder.skip_values(1).unwrap(), 1);

        assert!(matches!(output, DictionaryBuffer::Dict { .. }));

        // read two skip one
        assert_eq!(decoder.read(&mut output, 2).unwrap(), 2);
        assert_eq!(decoder.skip_values(1).unwrap(), 1);

        // read one and test on skip at the end
        assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
        assert_eq!(decoder.skip_values(4).unwrap(), 0);

        let valid = [true, true, true, true, true];
        let valid_buffer = Buffer::from_iter(valid.iter().cloned());
        output.pad_nulls(0, 5, 5, valid_buffer.as_slice());

        assert!(matches!(output, DictionaryBuffer::Dict { .. }));

        let array = output.into_array(Some(valid_buffer), &data_type).unwrap();
        assert_eq!(array.data_type(), &data_type);

        let array = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), 5);

        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("0"), Some("1"), Some("1"), Some("2"), Some("2"),]
        )
    }

    #[test]
    fn test_dictionary_fallback() {
        let data_type = utf8_dictionary();
        let data = vec!["hello", "world", "a", "b"];

        let (pages, encoded_dictionary) = byte_array_all_encodings(data.clone());
        let num_encodings = pages.len();

        let column_desc = utf8_column();
        let mut decoder = DictionaryDecoder::<i32, i32>::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        // Read all pages into single buffer
        let mut output = DictionaryBuffer::<i32, i32>::default();

        for (encoding, page) in pages {
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();
            assert_eq!(decoder.read(&mut output, 1024).unwrap(), 4);
        }
        let array = output.into_array(None, &data_type).unwrap();
        assert_eq!(array.data_type(), &data_type);

        let array = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), data.len() * num_encodings);

        // Should have a copy of `data` for each encoding
        for i in 0..num_encodings {
            assert_eq!(
                strings
                    .iter()
                    .skip(i * data.len())
                    .take(data.len())
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>(),
                data
            )
        }
    }

    #[test]
    fn test_dictionary_skip_fallback() {
        let data_type = utf8_dictionary();
        let data = vec!["hello", "world", "a", "b"];

        let (pages, encoded_dictionary) = byte_array_all_encodings(data.clone());
        let num_encodings = pages.len();

        let column_desc = utf8_column();
        let mut decoder = DictionaryDecoder::<i32, i32>::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        // Read all pages into single buffer
        let mut output = DictionaryBuffer::<i32, i32>::default();

        for (encoding, page) in pages {
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();
            decoder.skip_values(2).expect("skipping two values");
            assert_eq!(decoder.read(&mut output, 1024).unwrap(), 2);
        }
        let array = output.into_array(None, &data_type).unwrap();
        assert_eq!(array.data_type(), &data_type);

        let array = cast(&array, &ArrowType::Utf8).unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), (data.len() - 2) * num_encodings);

        // Should have a copy of `data` for each encoding
        for i in 0..num_encodings {
            assert_eq!(
                &strings
                    .iter()
                    .skip(i * (data.len() - 2))
                    .take(data.len() - 2)
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>(),
                &data[2..]
            )
        }
    }

    #[test]
    fn test_too_large_dictionary() {
        let data: Vec<_> = (0..128)
            .map(|x| ByteArray::from(x.to_string().as_str()))
            .collect();
        let (dictionary, _) = encode_dictionary(&data);

        let column_desc = utf8_column();

        let mut decoder = DictionaryDecoder::<i8, i32>::new(&column_desc);
        let err = decoder
            .set_dict(dictionary.clone(), 128, Encoding::RLE_DICTIONARY, false)
            .unwrap_err()
            .to_string();

        assert!(err.contains("dictionary too large for index type"));

        let mut decoder = DictionaryDecoder::<i16, i32>::new(&column_desc);
        decoder
            .set_dict(dictionary, 128, Encoding::RLE_DICTIONARY, false)
            .unwrap();
    }

    #[test]
    fn test_nulls() {
        let data_type = utf8_dictionary();
        let (pages, encoded_dictionary) = byte_array_all_encodings(Vec::<&str>::new());

        let column_desc = utf8_column();
        let mut decoder = DictionaryDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::PLAIN_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages.clone() {
            let mut output = DictionaryBuffer::<i32, i32>::default();
            decoder.set_data(encoding, page, 8, None).unwrap();
            assert_eq!(decoder.read(&mut output, 1024).unwrap(), 0);

            output.pad_nulls(0, 0, 8, &[0]);
            let array = output
                .into_array(Some(Buffer::from(&[0])), &data_type)
                .unwrap();

            assert_eq!(array.len(), 8);
            assert_eq!(array.null_count(), 8);
            assert_eq!(array.logical_null_count(), 8);
        }

        for (encoding, page) in pages {
            let mut output = DictionaryBuffer::<i32, i32>::default();
            decoder.set_data(encoding, page, 8, None).unwrap();
            assert_eq!(decoder.skip_values(1024).unwrap(), 0);

            output.pad_nulls(0, 0, 8, &[0]);
            let array = output
                .into_array(Some(Buffer::from(&[0])), &data_type)
                .unwrap();

            assert_eq!(array.len(), 8);
            assert_eq!(array.null_count(), 8);
            assert_eq!(array.logical_null_count(), 8);
        }
    }
}
