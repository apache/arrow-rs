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

use crate::arrow::array_reader::offset_buffer::OffsetBuffer;
use crate::arrow::array_reader::{read_records, ArrayReader};
use crate::arrow::record_reader::buffer::ScalarValue;
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{ConvertedType, Encoding};
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::data_type::Int32Type;
use crate::encodings::{
    decoding::{Decoder, DeltaBitPackDecoder},
    rle::RleDecoder,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::memory::ByteBufferPtr;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType as ArrowType;
use std::any::Any;
use std::ops::Range;

/// Returns an [`ArrayReader`] that decodes the provided byte array column
pub fn make_byte_array_reader(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
    arrow_type: Option<ArrowType>,
    null_mask_only: bool,
) -> Result<Box<dyn ArrayReader>> {
    // Check if Arrow type is specified, else create it from Parquet type
    let data_type = match arrow_type {
        Some(t) => t,
        None => parquet_to_arrow_field(column_desc.as_ref())?
            .data_type()
            .clone(),
    };

    match data_type {
        ArrowType::Binary | ArrowType::Utf8 => {
            let reader =
                GenericRecordReader::new_with_options(column_desc, null_mask_only);
            Ok(Box::new(ByteArrayReader::<i32>::new(
                pages, data_type, reader,
            )))
        }
        ArrowType::LargeUtf8 | ArrowType::LargeBinary => {
            let reader =
                GenericRecordReader::new_with_options(column_desc, null_mask_only);
            Ok(Box::new(ByteArrayReader::<i64>::new(
                pages, data_type, reader,
            )))
        }
        _ => Err(general_err!(
            "invalid data type for byte array reader - {}",
            data_type
        )),
    }
}

/// An [`ArrayReader`] for variable length byte arrays
struct ByteArrayReader<I: ScalarValue> {
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
    record_reader: GenericRecordReader<OffsetBuffer<I>, ByteArrayColumnValueDecoder<I>>,
}

impl<I: ScalarValue> ByteArrayReader<I> {
    fn new(
        pages: Box<dyn PageIterator>,
        data_type: ArrowType,
        record_reader: GenericRecordReader<
            OffsetBuffer<I>,
            ByteArrayColumnValueDecoder<I>,
        >,
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

impl<I: OffsetSizeTrait + ScalarValue> ArrayReader for ByteArrayReader<I> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)?;
        let buffer = self.record_reader.consume_record_data()?;
        let null_buffer = self.record_reader.consume_bitmap_buffer()?;
        self.def_levels_buffer = self.record_reader.consume_def_levels()?;
        self.rep_levels_buffer = self.record_reader.consume_rep_levels()?;
        self.record_reader.reset();

        Ok(buffer.into_array(null_buffer, self.data_type.clone()))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// A [`ColumnValueDecoder`] for variable length byte arrays
struct ByteArrayColumnValueDecoder<I: ScalarValue> {
    dict: Option<OffsetBuffer<I>>,
    decoder: Option<ByteArrayDecoder>,
    validate_utf8: bool,
}

impl<I: OffsetSizeTrait + ScalarValue> ColumnValueDecoder
    for ByteArrayColumnValueDecoder<I>
{
    type Slice = OffsetBuffer<I>;

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
        buf: ByteBufferPtr,
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

        let mut buffer = OffsetBuffer::default();
        let mut decoder = ByteArrayDecoderPlain::new(
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
        data: ByteBufferPtr,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        self.decoder = Some(ByteArrayDecoder::new(
            encoding,
            data,
            num_levels,
            num_values,
            self.validate_utf8,
        )?);
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Slice, range: Range<usize>) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;

        decoder.read(out, range.end - range.start, self.dict.as_ref())
    }
}

/// A generic decoder from uncompressed parquet value data to [`OffsetBuffer`]
pub enum ByteArrayDecoder {
    Plain(ByteArrayDecoderPlain),
    Dictionary(ByteArrayDecoderDictionary),
    DeltaLength(ByteArrayDecoderDeltaLength),
    DeltaByteArray(ByteArrayDecoderDelta),
}

impl ByteArrayDecoder {
    pub fn new(
        encoding: Encoding,
        data: ByteBufferPtr,
        num_levels: usize,
        num_values: Option<usize>,
        validate_utf8: bool,
    ) -> Result<Self> {
        let decoder = match encoding {
            Encoding::PLAIN => ByteArrayDecoder::Plain(ByteArrayDecoderPlain::new(
                data,
                num_levels,
                num_values,
                validate_utf8,
            )),
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                ByteArrayDecoder::Dictionary(ByteArrayDecoderDictionary::new(
                    data, num_levels, num_values,
                ))
            }
            Encoding::DELTA_LENGTH_BYTE_ARRAY => ByteArrayDecoder::DeltaLength(
                ByteArrayDecoderDeltaLength::new(data, validate_utf8)?,
            ),
            Encoding::DELTA_BYTE_ARRAY => ByteArrayDecoder::DeltaByteArray(
                ByteArrayDecoderDelta::new(data, validate_utf8)?,
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
    pub fn read<I: OffsetSizeTrait + ScalarValue>(
        &mut self,
        out: &mut OffsetBuffer<I>,
        len: usize,
        dict: Option<&OffsetBuffer<I>>,
    ) -> Result<usize> {
        match self {
            ByteArrayDecoder::Plain(d) => d.read(out, len),
            ByteArrayDecoder::Dictionary(d) => {
                let dict = dict
                    .ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.read(out, dict, len)
            }
            ByteArrayDecoder::DeltaLength(d) => d.read(out, len),
            ByteArrayDecoder::DeltaByteArray(d) => d.read(out, len),
        }
    }
}

/// Decoder from [`Encoding::PLAIN`] data to [`OffsetBuffer`]
pub struct ByteArrayDecoderPlain {
    buf: ByteBufferPtr,
    offset: usize,
    validate_utf8: bool,

    /// This is a maximum as the null count is not always known, e.g. value data from
    /// a v1 data page
    max_remaining_values: usize,
}

impl ByteArrayDecoderPlain {
    pub fn new(
        buf: ByteBufferPtr,
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

    pub fn read<I: OffsetSizeTrait + ScalarValue>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        let initial_values_length = output.values.len();

        let to_read = len.min(self.max_remaining_values);
        output.offsets.reserve(to_read);

        let remaining_bytes = self.buf.len() - self.offset;
        if remaining_bytes == 0 {
            return Ok(0);
        }

        let estimated_bytes = remaining_bytes
            .checked_mul(to_read)
            .map(|x| x / self.max_remaining_values)
            .unwrap_or_default();

        output.values.reserve(estimated_bytes);

        let mut read = 0;

        let buf = self.buf.as_ref();
        while self.offset < self.buf.len() && read != to_read {
            if self.offset + 4 > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }
            let len_bytes: [u8; 4] =
                buf[self.offset..self.offset + 4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes);

            let start_offset = self.offset + 4;
            let end_offset = start_offset + len as usize;
            if end_offset > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }

            output.try_push(&buf[start_offset..end_offset], self.validate_utf8)?;

            self.offset = end_offset;
            read += 1;
        }
        self.max_remaining_values -= to_read;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }
        Ok(to_read)
    }
}

/// Decoder from [`Encoding::DELTA_LENGTH_BYTE_ARRAY`] data to [`OffsetBuffer`]
pub struct ByteArrayDecoderDeltaLength {
    lengths: Vec<i32>,
    data: ByteBufferPtr,
    length_offset: usize,
    data_offset: usize,
    validate_utf8: bool,
}

impl ByteArrayDecoderDeltaLength {
    fn new(data: ByteBufferPtr, validate_utf8: bool) -> Result<Self> {
        let mut len_decoder = DeltaBitPackDecoder::<Int32Type>::new();
        len_decoder.set_data(data.all(), 0)?;
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

    fn read<I: OffsetSizeTrait + ScalarValue>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        let initial_values_length = output.values.len();

        let to_read = len.min(self.lengths.len() - self.length_offset);
        output.offsets.reserve(to_read);

        let src_lengths = &self.lengths[self.length_offset..self.length_offset + to_read];

        let total_bytes: usize = src_lengths.iter().map(|x| *x as usize).sum();
        output.values.reserve(total_bytes);

        if self.data_offset + total_bytes > self.data.len() {
            return Err(ParquetError::EOF(
                "Insufficient delta length byte array bytes".to_string(),
            ));
        }

        let mut start_offset = self.data_offset;
        for length in src_lengths {
            let end_offset = start_offset + *length as usize;
            output.try_push(
                &self.data.as_ref()[start_offset..end_offset],
                self.validate_utf8,
            )?;
            start_offset = end_offset;
        }

        self.data_offset = start_offset;
        self.length_offset += to_read;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }
        Ok(to_read)
    }
}

/// Decoder from [`Encoding::DELTA_BYTE_ARRAY`] to [`OffsetBuffer`]
pub struct ByteArrayDecoderDelta {
    prefix_lengths: Vec<i32>,
    suffix_lengths: Vec<i32>,
    data: ByteBufferPtr,
    length_offset: usize,
    data_offset: usize,
    last_value: Vec<u8>,
    validate_utf8: bool,
}

impl ByteArrayDecoderDelta {
    fn new(data: ByteBufferPtr, validate_utf8: bool) -> Result<Self> {
        let mut prefix = DeltaBitPackDecoder::<Int32Type>::new();
        prefix.set_data(data.all(), 0)?;

        let num_prefix = prefix.values_left();
        let mut prefix_lengths = vec![0; num_prefix];
        assert_eq!(prefix.get(&mut prefix_lengths)?, num_prefix);

        let mut suffix = DeltaBitPackDecoder::<Int32Type>::new();
        suffix.set_data(data.start_from(prefix.get_offset()), 0)?;

        let num_suffix = suffix.values_left();
        let mut suffix_lengths = vec![0; num_suffix];
        assert_eq!(suffix.get(&mut suffix_lengths)?, num_suffix);

        if num_prefix != num_suffix {
            return Err(general_err!(format!(
                "inconsistent DELTA_BYTE_ARRAY lengths, prefixes: {}, suffixes: {}",
                num_prefix, num_suffix
            )));
        }

        Ok(Self {
            prefix_lengths,
            suffix_lengths,
            data,
            length_offset: 0,
            data_offset: prefix.get_offset() + suffix.get_offset(),
            last_value: vec![],
            validate_utf8,
        })
    }

    fn read<I: OffsetSizeTrait + ScalarValue>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        let initial_values_length = output.values.len();
        assert_eq!(self.prefix_lengths.len(), self.suffix_lengths.len());

        let to_read = len.min(self.prefix_lengths.len() - self.length_offset);

        output.offsets.reserve(to_read);

        let length_range = self.length_offset..self.length_offset + to_read;
        let iter = self.prefix_lengths[length_range.clone()]
            .iter()
            .zip(&self.suffix_lengths[length_range]);

        let data = self.data.as_ref();

        for (prefix_length, suffix_length) in iter {
            let prefix_length = *prefix_length as usize;
            let suffix_length = *suffix_length as usize;

            if self.data_offset + suffix_length > self.data.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }

            self.last_value.truncate(prefix_length);
            self.last_value.extend_from_slice(
                &data[self.data_offset..self.data_offset + suffix_length],
            );
            output.try_push(&self.last_value, self.validate_utf8)?;

            self.data_offset += suffix_length;
        }

        self.length_offset += to_read;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }
        Ok(to_read)
    }
}

/// Decoder from [`Encoding::RLE_DICTIONARY`] to [`OffsetBuffer`]
pub struct ByteArrayDecoderDictionary {
    decoder: RleDecoder,

    index_buf: Box<[i32; 1024]>,
    index_buf_len: usize,
    index_offset: usize,

    /// This is a maximum as the null count is not always known, e.g. value data from
    /// a v1 data page
    max_remaining_values: usize,
}

impl ByteArrayDecoderDictionary {
    fn new(data: ByteBufferPtr, num_levels: usize, num_values: Option<usize>) -> Self {
        let bit_width = data[0];
        let mut decoder = RleDecoder::new(bit_width);
        decoder.set_data(data.start_from(1));

        Self {
            decoder,
            index_buf: Box::new([0; 1024]),
            index_buf_len: 0,
            index_offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
        }
    }

    fn read<I: OffsetSizeTrait + ScalarValue>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        dict: &OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        if dict.is_empty() {
            return Ok(0); // All data must be NULL
        }

        let mut values_read = 0;

        while values_read != len && self.max_remaining_values != 0 {
            if self.index_offset == self.index_buf_len {
                let read = self.decoder.get_batch(self.index_buf.as_mut())?;
                if read == 0 {
                    break;
                }
                self.index_buf_len = read;
                self.index_offset = 0;
            }

            let to_read = (len - values_read)
                .min(self.index_buf_len - self.index_offset)
                .min(self.max_remaining_values);

            output.extend_from_dictionary(
                &self.index_buf[self.index_offset..self.index_offset + to_read],
                dict.offsets.as_slice(),
                dict.values.as_slice(),
            )?;

            self.index_offset += to_read;
            values_read += to_read;
            self.max_remaining_values -= to_read;
        }
        Ok(values_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::test_util::{byte_array_all_encodings, utf8_column};
    use crate::arrow::record_reader::buffer::ValuesBuffer;
    use arrow::array::{Array, StringArray};

    #[test]
    fn test_byte_array_decoder() {
        let (pages, encoded_dictionary) =
            byte_array_all_encodings(vec!["hello", "world", "a", "b"]);

        let column_desc = utf8_column();
        let mut decoder = ByteArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages {
            let mut output = OffsetBuffer::<i32>::default();
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 0..1).unwrap(), 1);

            assert_eq!(output.values.as_slice(), "hello".as_bytes());
            assert_eq!(output.offsets.as_slice(), &[0, 5]);

            assert_eq!(decoder.read(&mut output, 1..2).unwrap(), 1);
            assert_eq!(output.values.as_slice(), "helloworld".as_bytes());
            assert_eq!(output.offsets.as_slice(), &[0, 5, 10]);

            assert_eq!(decoder.read(&mut output, 2..4).unwrap(), 2);
            assert_eq!(output.values.as_slice(), "helloworldab".as_bytes());
            assert_eq!(output.offsets.as_slice(), &[0, 5, 10, 11, 12]);

            assert_eq!(decoder.read(&mut output, 4..8).unwrap(), 0);

            let valid = vec![false, false, true, true, false, true, true, false, false];
            let valid_buffer = Buffer::from_iter(valid.iter().cloned());

            output.pad_nulls(0, 4, valid.len(), valid_buffer.as_slice());
            let array = output.into_array(Some(valid_buffer), ArrowType::Utf8);
            let strings = array.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![
                    None,
                    None,
                    Some("hello"),
                    Some("world"),
                    None,
                    Some("a"),
                    Some("b"),
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
        let mut decoder = ByteArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages {
            let mut output = OffsetBuffer::<i32>::default();
            decoder.set_data(encoding, page, 4, None).unwrap();
            assert_eq!(decoder.read(&mut output, 0..1024).unwrap(), 0);
        }
    }
}
