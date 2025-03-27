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
use crate::util::utf8::check_valid_utf8;
use arrow_array::{builder::make_view, ArrayRef};
use arrow_buffer::Buffer;
use arrow_data::ByteView;
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
                let dict = dict
                    .ok_or_else(|| general_err!("dictionary required for dictionary encoding"))?;
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
                let dict = dict
                    .ok_or_else(|| general_err!("dictionary required for dictionary encoding"))?;
                d.skip(dict, len)
            }
            ByteViewArrayDecoder::DeltaLength(d) => d.skip(len),
            ByteViewArrayDecoder::DeltaByteArray(d) => d.skip(len),
        }
    }
}

/// Decoder from [`Encoding::PLAIN`] data to [`ViewBuffer`]
pub struct ByteViewArrayDecoderPlain {
    buf: Buffer,
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
            buf: Buffer::from(buf),
            offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
            validate_utf8,
        }
    }

    pub fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        // avoid creating a new buffer if the last buffer is the same as the current buffer
        // This is especially useful when row-level filtering is applied, where we call lots of small `read` over the same buffer.
        let block_id = {
            if output.buffers.last().is_some_and(|x| x.ptr_eq(&self.buf)) {
                output.buffers.len() as u32 - 1
            } else {
                output.append_block(self.buf.clone())
            }
        };

        let to_read = len.min(self.max_remaining_values);

        let buf: &[u8] = self.buf.as_ref();
        let mut read = 0;
        output.views.reserve(to_read);

        let mut utf8_validation_begin = self.offset;
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
                // It seems you are trying to understand what's going on here, take a breath and be patient.
                // Utf-8 validation is a non-trivial task, here are some background facts:
                // (1) Validating one 2048-byte string is much faster than validating 128 of 16-byte string.
                //     As shown in https://github.com/apache/arrow-rs/pull/6009#issuecomment-2211174229
                //     Potentially because the SIMD operations favor longer strings.
                // (2) Practical strings are short, 99% of strings are smaller than 100 bytes, as shown in paper:
                //     https://www.vldb.org/pvldb/vol17/p148-zeng.pdf, Figure 5f.
                // (3) Parquet plain encoding makes utf-8 validation harder,
                //     because it stores the length of each string right before the string.
                //     This means naive utf-8 validation will be slow, because the validation need to skip the length bytes.
                //     I.e., the validation cannot validate the buffer in one pass, but instead, validate strings chunk by chunk.
                //
                // Given the above observations, the goal is to do batch validation as much as possible.
                // The key idea is that if the length is smaller than 128 (99% of the case), then the length bytes are valid utf-8, as reasoned below:
                // If the length is smaller than 128, its 4-byte encoding are [0, 0, 0, len].
                // Each of the byte is a valid ASCII character, so they are valid utf-8.
                // Since they are all smaller than 128, the won't break a utf-8 code point (won't mess with later bytes).
                //
                // The implementation keeps a water mark `utf8_validation_begin` to track the beginning of the buffer that is not validated.
                // If the length is smaller than 128, then we continue to next string.
                // If the length is larger than 128, then we validate the buffer before the length bytes, and move the water mark to the beginning of next string.
                if len < 128 {
                    // fast path, move to next string.
                    // the len bytes are valid utf8.
                } else {
                    // unfortunately, the len bytes may not be valid utf8, we need to wrap up and validate everything before it.
                    check_valid_utf8(unsafe {
                        buf.get_unchecked(utf8_validation_begin..self.offset)
                    })?;
                    // move the cursor to skip the len bytes.
                    utf8_validation_begin = start_offset;
                }
            }

            unsafe {
                output.append_view_unchecked(block_id, start_offset as u32, len);
            }
            self.offset = end_offset;
            read += 1;
        }

        // validate the last part of the buffer
        if self.validate_utf8 {
            check_valid_utf8(unsafe { buf.get_unchecked(utf8_validation_begin..self.offset) })?;
        }

        self.max_remaining_values -= to_read;
        Ok(to_read)
    }

    pub fn skip(&mut self, to_skip: usize) -> Result<usize> {
        let to_skip = to_skip.min(self.max_remaining_values);
        let mut skip = 0;
        let buf: &[u8] = self.buf.as_ref();

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

pub struct ByteViewArrayDecoderDictionary {
    decoder: DictIndexDecoder,
}

impl ByteViewArrayDecoderDictionary {
    fn new(data: Bytes, num_levels: usize, num_values: Option<usize>) -> Self {
        Self {
            decoder: DictIndexDecoder::new(data, num_levels, num_values),
        }
    }

    /// Reads the next indexes from self.decoder
    /// the indexes are assumed to be indexes into `dict`
    /// the output values are written to output
    ///
    /// Assumptions / Optimization
    /// This function checks if dict.buffers() are the last buffers in `output`, and if so
    /// reuses the dictionary page buffers directly without copying data
    fn read(&mut self, output: &mut ViewBuffer, dict: &ViewBuffer, len: usize) -> Result<usize> {
        if dict.is_empty() || len == 0 {
            return Ok(0);
        }

        // Check if the last few buffer of `output`` are the same as the `dict` buffer
        // This is to avoid creating a new buffers if the same dictionary is used for multiple `read`
        let need_to_create_new_buffer = {
            if output.buffers.len() >= dict.buffers.len() {
                let offset = output.buffers.len() - dict.buffers.len();
                output.buffers[offset..]
                    .iter()
                    .zip(dict.buffers.iter())
                    .any(|(a, b)| !a.ptr_eq(b))
            } else {
                true
            }
        };

        if need_to_create_new_buffer {
            for b in dict.buffers.iter() {
                output.buffers.push(b.clone());
            }
        }

        // Calculate the offset of the dictionary buffers in the output buffers
        // For example if the 2nd buffer in the dictionary is the 5th buffer in the output buffers,
        // then the base_buffer_idx is 5 - 2 = 3
        let base_buffer_idx = output.buffers.len() as u32 - dict.buffers.len() as u32;

        self.decoder.read(len, |keys| {
            for k in keys {
                let view = dict
                    .views
                    .get(*k as usize)
                    .ok_or_else(|| general_err!("invalid key={} for dictionary", *k))?;
                let len = *view as u32;
                if len <= 12 {
                    // directly append the view if it is inlined
                    // Safety: the view is from the dictionary, so it is valid
                    unsafe {
                        output.append_raw_view_unchecked(view);
                    }
                } else {
                    // correct the buffer index and append the view
                    let mut view = ByteView::from(*view);
                    view.buffer_index += base_buffer_idx;
                    // Safety: the view is from the dictionary,
                    // we corrected the index value to point it to output buffer, so it is valid
                    unsafe {
                        output.append_raw_view_unchecked(&view.into());
                    }
                }
            }
            Ok(())
        })
    }

    fn skip(&mut self, dict: &ViewBuffer, to_skip: usize) -> Result<usize> {
        if dict.is_empty() {
            return Ok(0);
        }
        self.decoder.skip(to_skip)
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

        let mut total_bytes = 0;

        for l in lengths.iter() {
            if *l < 0 {
                return Err(ParquetError::General(
                    "negative delta length byte array length".to_string(),
                ));
            }
            total_bytes += *l as usize;
        }

        if total_bytes + len_decoder.get_offset() > data.len() {
            return Err(ParquetError::General(
                "Insufficient delta length byte array bytes".to_string(),
            ));
        }

        Ok(Self {
            lengths,
            data,
            validate_utf8,
            length_offset: 0,
            data_offset: len_decoder.get_offset(),
        })
    }

    fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        let to_read = len.min(self.lengths.len() - self.length_offset);
        output.views.reserve(to_read);

        let src_lengths = &self.lengths[self.length_offset..self.length_offset + to_read];

        // Zero copy convert `bytes::Bytes` into `arrow_buffer::Buffer`
        let bytes = Buffer::from(self.data.clone());
        let block_id = output.append_block(bytes);

        let mut current_offset = self.data_offset;
        let initial_offset = current_offset;
        for length in src_lengths {
            // # Safety
            // The length is from the delta length decoder, so it is valid
            // The start_offset is calculated from the lengths, so it is valid
            // `start_offset + length` is guaranteed to be within the bounds of `data`, as checked in `new`
            unsafe { output.append_view_unchecked(block_id, current_offset as u32, *length as u32) }

            current_offset += *length as usize;
        }

        // Delta length encoding has continuous strings, we can validate utf8 in one go
        if self.validate_utf8 {
            check_valid_utf8(&self.data[initial_offset..current_offset])?;
        }

        self.data_offset = current_offset;
        self.length_offset += to_read;

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

    // Unlike other encodings, we need to copy the data.
    //
    //  DeltaByteArray data is stored using shared prefixes/suffixes,
    // which results in potentially non-contiguous
    // strings, while Arrow encodings require contiguous strings
    //
    // <https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7>

    fn read(&mut self, output: &mut ViewBuffer, len: usize) -> Result<usize> {
        output.views.reserve(len.min(self.decoder.remaining()));

        // array buffer only have long strings
        let mut array_buffer: Vec<u8> = Vec::with_capacity(4096);

        let buffer_id = output.buffers.len() as u32;

        let read = if !self.validate_utf8 {
            self.decoder.read(len, |bytes| {
                let offset = array_buffer.len();
                let view = make_view(bytes, buffer_id, offset as u32);
                if bytes.len() > 12 {
                    // only copy the data to buffer if the string can not be inlined.
                    array_buffer.extend_from_slice(bytes);
                }

                // # Safety
                // The buffer_id is the last buffer in the output buffers
                // The offset is calculated from the buffer, so it is valid
                unsafe {
                    output.append_raw_view_unchecked(&view);
                }
                Ok(())
            })?
        } else {
            // utf8 validation buffer has only short strings. These short
            // strings are inlined into the views but we copy them into a
            // contiguous buffer to accelerate validation.®
            let mut utf8_validation_buffer = Vec::with_capacity(4096);

            let v = self.decoder.read(len, |bytes| {
                let offset = array_buffer.len();
                let view = make_view(bytes, buffer_id, offset as u32);
                if bytes.len() > 12 {
                    // only copy the data to buffer if the string can not be inlined.
                    array_buffer.extend_from_slice(bytes);
                } else {
                    utf8_validation_buffer.extend_from_slice(bytes);
                }

                // # Safety
                // The buffer_id is the last buffer in the output buffers
                // The offset is calculated from the buffer, so it is valid
                // Utf-8 validation is done later
                unsafe {
                    output.append_raw_view_unchecked(&view);
                }
                Ok(())
            })?;
            check_valid_utf8(&array_buffer)?;
            check_valid_utf8(&utf8_validation_buffer)?;
            v
        };

        let actual_block_id = output.append_block(Buffer::from_vec(array_buffer));
        assert_eq!(actual_block_id, buffer_id);
        Ok(read)
    }

    fn skip(&mut self, to_skip: usize) -> Result<usize> {
        self.decoder.skip(to_skip)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::StringViewArray;
    use arrow_buffer::Buffer;

    use crate::{
        arrow::{
            array_reader::test_util::{byte_array_all_encodings, encode_byte_array, utf8_column},
            buffer::view_buffer::ViewBuffer,
            record_reader::buffer::ValuesBuffer,
        },
        basic::Encoding,
        column::reader::decoder::ColumnValueDecoder,
        data_type::ByteArray,
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
            let mut output = ViewBuffer::default();
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
            assert_eq!(decoder.read(&mut output, 2).unwrap(), 2);
            assert_eq!(decoder.read(&mut output, 4).unwrap(), 0);

            assert_eq!(output.views.len(), 4);

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

    #[test]
    fn test_byte_view_array_plain_decoder_reuse_buffer() {
        let byte_array = vec!["hello", "world", "large payload over 12 bytes", "b"];
        let byte_array: Vec<ByteArray> = byte_array.into_iter().map(|x| x.into()).collect();
        let pages = encode_byte_array(Encoding::PLAIN, &byte_array);

        let column_desc = utf8_column();
        let mut decoder = ByteViewArrayColumnValueDecoder::new(&column_desc);

        let mut view_buffer = ViewBuffer::default();
        decoder.set_data(Encoding::PLAIN, pages, 4, None).unwrap();
        decoder.read(&mut view_buffer, 1).unwrap();
        decoder.read(&mut view_buffer, 1).unwrap();
        assert_eq!(view_buffer.buffers.len(), 1);

        decoder.read(&mut view_buffer, 1).unwrap();
        assert_eq!(view_buffer.buffers.len(), 1);
    }
}
