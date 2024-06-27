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
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{ConvertedType, Encoding};
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use arrow_array::ArrayRef;
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;
use std::any::Any;

/// Returns an [`ArrayReader`] that decodes the provided byte array column to view types.
#[allow(unused)]
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
        ArrowType::BinaryView | ArrowType::Utf8View => {
            let reader = GenericRecordReader::new(column_desc);
            Ok(Box::new(ByteViewArrayReader::new(pages, data_type, reader)))
        }

        _ => Err(general_err!(
            "invalid data type for byte array reader read to view type - {}",
            data_type
        )),
    }
}

/// An [`ArrayReader`] for variable length byte arrays
#[allow(unused)]
struct ByteViewArrayReader {
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: GenericRecordReader<ViewBuffer, ByteViewArrayColumnValueDecoder>,
}

impl ByteViewArrayReader {
    #[allow(unused)]
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

        let array = buffer.into_array(null_buffer, &self.data_type);

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

/// A generic decoder from uncompressed parquet value data to [`OffsetBuffer`]
pub enum ByteViewArrayDecoder {
    Plain(ByteViewArrayDecoderPlain),
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
            Encoding::RLE_DICTIONARY
            | Encoding::PLAIN_DICTIONARY
            | Encoding::DELTA_LENGTH_BYTE_ARRAY
            | Encoding::DELTA_BYTE_ARRAY => unimplemented!("stay tuned!"),
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
        _dict: Option<&ViewBuffer>,
    ) -> Result<usize> {
        match self {
            ByteViewArrayDecoder::Plain(d) => d.read(out, len),
        }
    }

    /// Skip `len` values
    pub fn skip(&mut self, len: usize, _dict: Option<&ViewBuffer>) -> Result<usize> {
        match self {
            ByteViewArrayDecoder::Plain(d) => d.skip(len),
        }
    }
}

/// Decoder from [`Encoding::PLAIN`] data to [`OffsetBuffer`]
pub struct ByteViewArrayDecoderPlain {
    buf: Bytes,
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
            offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
            validate_utf8,
        }
    }

    pub fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        let block_id = output.append_block(self.buf.clone().into());

        let to_read = len.min(self.max_remaining_values);

        let buf = self.buf.as_ref();
        let mut read = 0;
        output.views.reserve(to_read);
        while self.offset < self.buf.len() && read != to_read {
            if self.offset + 4 > self.buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }
            let len_bytes: [u8; 4] = unsafe {
                buf.get_unchecked(self.offset..self.offset + 4)
                    .try_into()
                    .unwrap()
            };
            let len = u32::from_le_bytes(len_bytes);

            let start_offset = self.offset + 4;
            let end_offset = start_offset + len as usize;
            if end_offset > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }

            if self.validate_utf8 {
                check_valid_utf8(unsafe { buf.get_unchecked(start_offset..end_offset) })?;
            }

            unsafe {
                output.append_view_unchecked(block_id, start_offset as u32, len);
            }
            self.offset = end_offset;
            read += 1;
        }

        self.max_remaining_values -= to_read;
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

/// Check that `val` is a valid UTF-8 sequence
pub fn check_valid_utf8(val: &[u8]) -> Result<()> {
    match std::str::from_utf8(val) {
        Ok(_) => Ok(()),
        Err(e) => Err(general_err!("encountered non UTF-8 data: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::StringViewArray;
    use arrow_buffer::Buffer;

    use crate::{
        arrow::{
            array_reader::test_util::{byte_array_all_encodings, utf8_column},
            buffer::view_buffer::ViewBuffer,
            record_reader::buffer::ValuesBuffer,
        },
        basic::Encoding,
        column::reader::decoder::ColumnValueDecoder,
    };

    use super::*;

    #[test]
    fn test_byte_array_string_view_decoder() {
        let (pages, encoded_dictionary) =
            byte_array_all_encodings(vec!["hello", "world", "large payload over 12 bytes", "b"]);

        let column_desc = utf8_column();
        let mut decoder = ByteViewArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages {
            if encoding != Encoding::PLAIN {
                // skip non-plain encodings for now as they are not yet implemented
                continue;
            }
            let mut output = ViewBuffer::default();
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
            assert_eq!(decoder.read(&mut output, 2).unwrap(), 2);
            assert_eq!(decoder.read(&mut output, 4).unwrap(), 0);

            assert_eq!(output.views.len(), 4);
            assert_eq!(output.buffers.len(), 4);

            let valid = [false, false, true, true, false, true, true, false, false];
            let valid_buffer = Buffer::from_iter(valid.iter().cloned());

            output.pad_nulls(0, 4, valid.len(), valid_buffer.as_slice());
            let array = output.into_array(Some(valid_buffer), &ArrowType::Utf8View);
            let strings = array.as_any().downcast_ref::<StringViewArray>().unwrap();

            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![
                    None,
                    None,
                    Some("hello"),
                    Some("world"),
                    None,
                    Some("large payload over 12 bytes"),
                    Some("b"),
                    None,
                    None,
                ]
            );
        }
    }
}
