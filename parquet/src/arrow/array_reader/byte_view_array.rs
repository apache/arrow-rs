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

use crate::arrow::array_reader::{read_records, skip_records, ArrayReader};
use crate::arrow::buffer::view_buffer::ViewBuffer;
use crate::arrow::decoder::{DeltaByteArrayDecoder, DictIndexDecoder};
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{ConvertedType, Encoding};
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::data_type::Int32Type;
use crate::encodings::decoding::{Decoder, DeltaBitPackDecoder};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use arrow_array::ArrayRef;
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;
use std::any::Any;

/// Returns an [`ArrayReader`] that decodes the provided byte array column to view types.
pub fn make_byte_view_array_reader(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
    arrow_type: Option<ArrowType>,
) -> Result<Box<dyn ArrayReader>> {
    // Check if Arrow type is specified, else create it from Parquet type
    let data_type = match arrow_type {
        Some(t) => t,
        None => match parquet_to_arrow_field(column_desc.as_ref())?.data_type() {
            ArrowType::Utf8 | ArrowType::Utf8View => ArrowType::Utf8View,
            _ => ArrowType::BinaryView,
        },
    };

    match data_type {
        ArrowType::Utf8View | ArrowType::BinaryView => {
            let reader = GenericRecordReader::new(column_desc);
            Ok(Box::new(ByteViewArrayReader::new(pages, data_type, reader)))
        }
        _ => Err(general_err!(
            "invalid data type for byte array reader - {}",
            data_type
        )),
    }
}

/// An [`ArrayReader`] for variable length byte arrays
struct ByteViewArrayReader {
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: GenericRecordReader<ViewBuffer, ByteViewArrayColumnValueDecoder>,
}

impl ByteViewArrayReader {
    fn new(
        pages: Box<dyn PageIterator>,
        data_type: ArrowType,
        record_reader: GenericRecordReader<ViewBuffer, ByteViewArrayColumnValueDecoder>,
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

impl ArrayReader for ByteViewArrayReader {
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
        self.def_levels_buffer = self.record_reader.consume_def_levels();
        self.rep_levels_buffer = self.record_reader.consume_rep_levels();
        self.record_reader.reset();

        let array: ArrayRef = buffer.into_array(null_buffer, self.data_type.clone());

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

/// A [`ColumnValueDecoder`] for variable length byte arrays
struct ByteViewArrayColumnValueDecoder {
    dict: Option<ViewBuffer>,
    decoder: Option<ByteViewArrayDecoder>,
    validate_utf8: bool,
}

impl ColumnValueDecoder for ByteViewArrayColumnValueDecoder {
    type Buffer = ViewBuffer;

    fn new(desc: &ColumnDescPtr) -> Self {
        let validate_utf8 = desc.converted_type() == ConvertedType::UTF8;
        Self {
            dict: None,
            decoder: None,
            validate_utf8,
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

        let mut buffer = ViewBuffer::default();
        let mut decoder = ByteViewArrayDecoderPlain::new(
            buf,
            num_values as usize,
            Some(num_values as usize),
            self.validate_utf8,
        );
        decoder.read(&mut buffer, usize::MAX)?;
        self.dict = Some(buffer);
        Ok(())
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        self.decoder = Some(ByteViewArrayDecoder::new(
            encoding,
            data,
            num_levels,
            num_values,
            self.validate_utf8,
        )?);
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;

        decoder.read(out, num_values, self.dict.as_ref())
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;

        decoder.skip(num_values, self.dict.as_ref())
    }
}

/// A generic decoder from uncompressed parquet value data to [`ViewBuffer`]
pub enum ByteViewArrayDecoder {
    Plain(ByteViewArrayDecoderPlain),
    Dictionary(ByteViewArrayDecoderDictionary),
    DeltaLength(ByteViewArrayDecoderDeltaLength),
    DeltaByteArray(ByteViewArrayDecoderDelta),
}

impl ByteViewArrayDecoder {
    pub fn new(
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
        validate_utf8: bool,
    ) -> Result<Self> {
        let decoder = match encoding {
            Encoding::PLAIN => ByteViewArrayDecoder::Plain(ByteViewArrayDecoderPlain::new(
                data,
                num_levels,
                num_values,
                validate_utf8,
            )),
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                ByteViewArrayDecoder::Dictionary(ByteViewArrayDecoderDictionary::new(
                    data, num_levels, num_values,
                ))
            }
            Encoding::DELTA_LENGTH_BYTE_ARRAY => ByteViewArrayDecoder::DeltaLength(
                ByteViewArrayDecoderDeltaLength::new(data, validate_utf8)?,
            ),
            Encoding::DELTA_BYTE_ARRAY => ByteViewArrayDecoder::DeltaByteArray(
                ByteViewArrayDecoderDelta::new(data, validate_utf8)?,
            ),
            _ => {
                return Err(general_err!(
                    "unsupported encoding for byte array: {}",
                    encoding
                ))
            }
        };

        Ok(decoder)
    }

    /// Read up to `len` values to `out` with the optional dictionary
    pub fn read(
        &mut self,
        out: &mut ViewBuffer,
        len: usize,
        dict: Option<&ViewBuffer>,
    ) -> Result<usize> {
        match self {
            ByteViewArrayDecoder::Plain(d) => d.read(out, len),
            ByteViewArrayDecoder::Dictionary(d) => {
                let dict =
                    dict.ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.read(out, dict, len)
            }
            ByteViewArrayDecoder::DeltaLength(d) => d.read(out, len),
            ByteViewArrayDecoder::DeltaByteArray(d) => d.read(out, len),
        }
    }

    /// Skip `len` values
    pub fn skip(&mut self, len: usize, dict: Option<&ViewBuffer>) -> Result<usize> {
        match self {
            ByteViewArrayDecoder::Plain(d) => d.skip(len),
            ByteViewArrayDecoder::Dictionary(d) => {
                let dict =
                    dict.ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.skip(dict, len)
            }
            ByteViewArrayDecoder::DeltaLength(d) => d.skip(len),
            ByteViewArrayDecoder::DeltaByteArray(d) => d.skip(len),
        }
    }
}

/// Decoder from [`Encoding::PLAIN`] data to [`ViewBuffer`]
pub struct ByteViewArrayDecoderPlain {
    buf: Bytes,
    /// offset of buf, changed during read or skip.
    offset: usize,
    validate_utf8: bool,

    /// This is a maximum as the null count is not always known, e.g. value data from
    /// a v1 data page
    max_remaining_values: usize,
}

impl ByteViewArrayDecoderPlain {
    pub fn new(
        buf: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
        validate_utf8: bool,
    ) -> Self {
        Self {
            buf,
            validate_utf8,
            offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
        }
    }

    pub fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        let initial_values_length = output.views.len();

        let to_read = len.min(self.max_remaining_values);

        let remaining_bytes = self.buf.len() - self.offset;
        if remaining_bytes == 0 {
            return Ok(0);
        }

        let mut read = 0;

        let buf = self.buf.as_ref();
        output.add_buffer(self.buf.clone());
        while self.offset < self.buf.len() && read != to_read {
            if self.offset + 4 > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte view array".into()));
            }
            let len_bytes: [u8; 4] = buf[self.offset..self.offset + 4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes);

            let start_offset = self.offset + 4;
            let end_offset = start_offset + len as usize;
            if end_offset > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte view array".into()));
            }

            output.try_push_with_offset(start_offset, end_offset)?;

            self.offset = end_offset;
            read += 1;
        }
        self.max_remaining_values -= to_read;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }

        Ok(to_read)
    }

    pub fn skip(&mut self, to_skip: usize) -> Result<usize> {
        let to_skip = to_skip.min(self.max_remaining_values);
        let mut skip = 0;
        let buf = self.buf.as_ref();

        while self.offset < self.buf.len() && skip != to_skip {
            if self.offset + 4 > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }
            let len_bytes: [u8; 4] = buf[self.offset..self.offset + 4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes) as usize;
            skip += 1;
            self.offset = self.offset + 4 + len;
        }
        self.max_remaining_values -= skip;
        Ok(skip)
    }
}

/// Decoder from [`Encoding::DELTA_LENGTH_BYTE_ARRAY`] data to [`ViewBuffer`]
pub struct ByteViewArrayDecoderDeltaLength {
    lengths: Vec<i32>,
    data: Bytes,
    length_offset: usize,
    data_offset: usize,
    validate_utf8: bool,
}

impl ByteViewArrayDecoderDeltaLength {
    fn new(data: Bytes, validate_utf8: bool) -> Result<Self> {
        let mut len_decoder = DeltaBitPackDecoder::<Int32Type>::new();
        len_decoder.set_data(data.clone(), 0)?;
        let values = len_decoder.values_left();

        let mut lengths = vec![0; values];
        len_decoder.get(&mut lengths)?;

        Ok(Self {
            lengths,
            data,
            validate_utf8,
            length_offset: 0,
            data_offset: len_decoder.get_offset(),
        })
    }

    fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        let initial_values_length = output.views.len();

        let to_read = len.min(self.lengths.len() - self.length_offset);

        let src_lengths = &self.lengths[self.length_offset..self.length_offset + to_read];

        let total_bytes: usize = src_lengths.iter().map(|x| *x as usize).sum();

        if self.data_offset + total_bytes > self.data.len() {
            return Err(ParquetError::EOF(
                "Insufficient delta length byte array bytes".to_string(),
            ));
        }

        let mut start_offset = self.data_offset;
        output.add_buffer(self.data.clone());
        for length in src_lengths {
            let end_offset = start_offset + *length as usize;
            output.try_push_with_offset(start_offset, end_offset)?;
            start_offset = end_offset;
        }

        self.data_offset = start_offset;
        self.length_offset += to_read;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }

        Ok(to_read)
    }

    fn skip(&mut self, to_skip: usize) -> Result<usize> {
        let remain_values = self.lengths.len() - self.length_offset;
        let to_skip = remain_values.min(to_skip);

        let src_lengths = &self.lengths[self.length_offset..self.length_offset + to_skip];
        let total_bytes: usize = src_lengths.iter().map(|x| *x as usize).sum();

        self.data_offset += total_bytes;
        self.length_offset += to_skip;
        Ok(to_skip)
    }
}

/// Decoder from [`Encoding::DELTA_BYTE_ARRAY`] to [`ViewBuffer`]
pub struct ByteViewArrayDecoderDelta {
    decoder: DeltaByteArrayDecoder,
    validate_utf8: bool,
}

impl ByteViewArrayDecoderDelta {
    fn new(data: Bytes, validate_utf8: bool) -> Result<Self> {
        Ok(Self {
            decoder: DeltaByteArrayDecoder::new(data)?,
            validate_utf8,
        })
    }

    fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        let initial_values_length = output.views.len();
        let read = self
            .decoder
            .read(len, |bytes| output.try_push(bytes, self.validate_utf8))?;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }

        Ok(read)
    }

    fn skip(&mut self, to_skip: usize) -> Result<usize> {
        self.decoder.skip(to_skip)
    }
}

/// Decoder from [`Encoding::RLE_DICTIONARY`] to [`ViewBuffer`]
pub struct ByteViewArrayDecoderDictionary {
    decoder: DictIndexDecoder,
}

impl ByteViewArrayDecoderDictionary {
    fn new(data: Bytes, num_levels: usize, num_values: Option<usize>) -> Self {
        Self {
            decoder: DictIndexDecoder::new(data, num_levels, num_values),
        }
    }

    fn read(&mut self, output: &mut ViewBuffer, dict: &ViewBuffer, len: usize) -> Result<usize> {
        // All data must be NULL
        if dict.is_empty() {
            return Ok(0);
        }

        self.decoder.read(len, |keys| {
            output.extend_from_dictionary(keys, &dict.views, dict.plain_buffer.as_ref().unwrap())
        })
    }

    fn skip(&mut self, dict: &ViewBuffer, to_skip: usize) -> Result<usize> {
        // All data must be NULL
        if dict.is_empty() {
            return Ok(0);
        }

        self.decoder.skip(to_skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::test_util::{byte_array_all_encodings, utf8_column};
    use crate::arrow::record_reader::buffer::ValuesBuffer;
    use arrow_array::{Array, StringViewArray};
    use arrow_buffer::Buffer;

    #[test]
    fn test_byte_array_decoder() {
        let (pages, encoded_dictionary) = byte_array_all_encodings(vec![
            "hello",
            "world",
            "here comes the snow",
            "large payload over 12 bytes",
        ]);

        let column_desc = utf8_column();
        let mut decoder = ByteViewArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages {
            let mut output = ViewBuffer::default();
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);

            let first = output.views.first().unwrap();
            let len = *first as u32;

            assert_eq!(
                &first.to_le_bytes()[4..4 + len as usize],
                "hello".as_bytes()
            );

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);

            assert_eq!(decoder.read(&mut output, 2).unwrap(), 2);

            assert_eq!(decoder.read(&mut output, 4).unwrap(), 0);

            let valid = [false, false, true, true, false, true, true, false, false];
            let valid_buffer = Buffer::from_iter(valid.iter().cloned());

            output.pad_nulls(0, 4, valid.len(), valid_buffer.as_slice());

            let array = output.into_array(Some(valid_buffer), ArrowType::Utf8View);
            let strings = array.as_any().downcast_ref::<StringViewArray>().unwrap();

            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![
                    None,
                    None,
                    Some("hello"),
                    Some("world"),
                    None,
                    Some("here comes the snow"),
                    Some("large payload over 12 bytes"),
                    None,
                    None,
                ]
            );
        }
    }

    #[test]
    fn test_byte_array_decoder_skip() {
        let (pages, encoded_dictionary) =
            byte_array_all_encodings(vec!["hello", "world", "a", "large payload over 12 bytes"]);

        let column_desc = utf8_column();
        let mut decoder = ByteViewArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages {
            let mut output = ViewBuffer::default();
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);

            let first = output.views.first().unwrap();
            let len = *first as u32;

            assert_eq!(
                &first.to_le_bytes()[4..4 + len as usize],
                "hello".as_bytes()
            );

            assert_eq!(decoder.skip_values(1).unwrap(), 1);
            assert_eq!(decoder.skip_values(1).unwrap(), 1);

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);

            assert_eq!(decoder.read(&mut output, 4).unwrap(), 0);

            let valid = [false, false, true, true, false, false];
            let valid_buffer = Buffer::from_iter(valid.iter().cloned());

            output.pad_nulls(0, 2, valid.len(), valid_buffer.as_slice());
            let array = output.into_array(Some(valid_buffer), ArrowType::Utf8View);
            let strings = array.as_any().downcast_ref::<StringViewArray>().unwrap();

            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![
                    None,
                    None,
                    Some("hello"),
                    Some("large payload over 12 bytes"),
                    None,
                    None,
                ]
            );
        }
    }

    #[test]
    fn test_byte_array_decoder_nulls() {
        let (pages, encoded_dictionary) = byte_array_all_encodings(Vec::<&str>::new());

        let column_desc = utf8_column();
        let mut decoder = ByteViewArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        // test nulls read
        for (encoding, page) in pages.clone() {
            let mut output = ViewBuffer::default();
            decoder.set_data(encoding, page, 4, None).unwrap();
            assert_eq!(decoder.read(&mut output, 1024).unwrap(), 0);
        }

        // test nulls skip
        for (encoding, page) in pages {
            decoder.set_data(encoding, page, 4, None).unwrap();
            assert_eq!(decoder.skip_values(1024).unwrap(), 0);
        }
    }
}
