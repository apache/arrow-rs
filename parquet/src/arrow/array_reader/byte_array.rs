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

use crate::arrow::array_reader::{ArrayReader, read_records, skip_records};
use crate::arrow::buffer::bit_util::sign_extend_be;
use crate::arrow::buffer::offset_buffer::OffsetBuffer;
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
use arrow_array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Decimal256Array, OffsetSizeTrait,
};
use arrow_buffer::i256;
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;
use std::any::Any;
use std::sync::Arc;

/// Returns an [`ArrayReader`] that decodes the provided byte array column
pub fn make_byte_array_reader(
    pages: Box<dyn PageIterator>,
    column_desc: ColumnDescPtr,
    arrow_type: Option<ArrowType>,
    batch_size: usize,
    padding_threshold: Option<i16>,
) -> Result<Box<dyn ArrayReader>> {
    // Check if Arrow type is specified, else create it from Parquet type
    let data_type = match arrow_type {
        Some(t) => t,
        None => parquet_to_arrow_field(column_desc.as_ref())?
            .data_type()
            .clone(),
    };

    match data_type {
        ArrowType::Binary
        | ArrowType::Utf8
        | ArrowType::Decimal128(_, _)
        | ArrowType::Decimal256(_, _) => {
            let mut reader = GenericRecordReader::new(column_desc, batch_size);
            if let Some(threshold) = padding_threshold {
                reader.set_padding_threshold(threshold);
            }
            Ok(Box::new(ByteArrayReader::<i32>::new(
                pages, data_type, reader,
            )))
        }
        ArrowType::LargeUtf8 | ArrowType::LargeBinary => {
            let mut reader = GenericRecordReader::new(column_desc, batch_size);
            if let Some(threshold) = padding_threshold {
                reader.set_padding_threshold(threshold);
            }
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
struct ByteArrayReader<I: OffsetSizeTrait> {
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: GenericRecordReader<OffsetBuffer<I>, ByteArrayColumnValueDecoder<I>>,
}

impl<I: OffsetSizeTrait> ByteArrayReader<I> {
    fn new(
        pages: Box<dyn PageIterator>,
        data_type: ArrowType,
        record_reader: GenericRecordReader<OffsetBuffer<I>, ByteArrayColumnValueDecoder<I>>,
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

impl<I: OffsetSizeTrait> ArrayReader for ByteArrayReader<I> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)
    }

    fn stopped_for_capacity(&self) -> bool {
        self.record_reader.stopped_for_capacity()
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let buffer = self.record_reader.consume_record_data();
        let null_buffer = self.record_reader.consume_compact_bitmap();
        self.def_levels_buffer = self.record_reader.consume_def_levels();
        self.rep_levels_buffer = self.record_reader.consume_rep_levels();
        self.record_reader.reset();

        let array: ArrayRef = match self.data_type {
            // Apply conversion to all elements regardless of null slots as the conversions
            // are infallible. This improves performance by avoiding a branch in the inner
            // loop (see docs for `PrimitiveArray::from_unary`).
            ArrowType::Decimal128(p, s) => {
                let array = buffer.into_array(null_buffer, ArrowType::Binary);
                let binary = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                // Null slots will have 0 length, so we need to check for that in the lambda
                // or sign_extend_be will panic.
                let decimal = Decimal128Array::from_unary(binary, |x| match x.len() {
                    0 => i128::default(),
                    _ => i128::from_be_bytes(sign_extend_be(x)),
                })
                .with_precision_and_scale(p, s)?;
                Arc::new(decimal)
            }
            ArrowType::Decimal256(p, s) => {
                let array = buffer.into_array(null_buffer, ArrowType::Binary);
                let binary = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                // Null slots will have 0 length, so we need to check for that in the lambda
                // or sign_extend_be will panic.
                let decimal = Decimal256Array::from_unary(binary, |x| match x.len() {
                    0 => i256::default(),
                    _ => i256::from_be_bytes(sign_extend_be(x)),
                })
                .with_precision_and_scale(p, s)?;
                Arc::new(decimal)
            }
            _ => buffer.into_array(null_buffer, self.data_type.clone()),
        };

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

    fn max_def_level(&self) -> i16 {
        self.record_reader.max_def_level()
    }
}

/// A [`ColumnValueDecoder`] for variable length byte arrays
struct ByteArrayColumnValueDecoder<I: OffsetSizeTrait> {
    dict: Option<OffsetBuffer<I>>,
    decoder: Option<ByteArrayDecoder>,
    validate_utf8: bool,
    /// The length in bytes of the longest value in `dict`
    ///
    /// Lets [`Self::values_capacity`] bound the decoded size of a dictionary
    /// encoded page without decoding any keys
    max_dict_value_len: usize,
}

impl<I: OffsetSizeTrait> ColumnValueDecoder for ByteArrayColumnValueDecoder<I> {
    type Buffer = OffsetBuffer<I>;

    fn new(desc: &ColumnDescPtr) -> Self {
        let validate_utf8 = desc.converted_type() == ConvertedType::UTF8;
        Self {
            dict: None,
            decoder: None,
            validate_utf8,
            max_dict_value_len: 0,
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

        let mut buffer = OffsetBuffer::<I>::with_capacity(0);
        let mut decoder = ByteArrayDecoderPlain::new(
            buf,
            num_values as usize,
            Some(num_values as usize),
            self.validate_utf8,
        );
        decoder.read(&mut buffer, usize::MAX)?;
        self.max_dict_value_len = buffer
            .offsets
            .windows(2)
            .map(|w| w[1].as_usize() - w[0].as_usize())
            .max()
            .unwrap_or(0);
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
        self.decoder = Some(ByteArrayDecoder::new(
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

    fn values_capacity(&self, out: &Self::Buffer) -> Option<usize> {
        // 64 bit offsets cannot overflow for any buffer that fits in memory,
        // and `IS_LARGE` is a constant so this whole method folds to `None`
        // when monomorphised for `i64`
        if I::IS_LARGE {
            return None;
        }
        let headroom = I::MAX_OFFSET.saturating_sub(out.values.len());
        self.decoder
            .as_ref()?
            .values_capacity(headroom, self.max_dict_value_len)
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
        data: Bytes,
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
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => ByteArrayDecoder::Dictionary(
                ByteArrayDecoderDictionary::new(data, num_levels, num_values)?,
            ),
            Encoding::DELTA_LENGTH_BYTE_ARRAY => ByteArrayDecoder::DeltaLength(
                ByteArrayDecoderDeltaLength::new(data, validate_utf8)?,
            ),
            Encoding::DELTA_BYTE_ARRAY => {
                ByteArrayDecoder::DeltaByteArray(ByteArrayDecoderDelta::new(data, validate_utf8)?)
            }
            _ => {
                return Err(general_err!(
                    "unsupported encoding for byte array: {}",
                    encoding
                ));
            }
        };

        Ok(decoder)
    }

    /// Read up to `len` values to `out` with the optional dictionary
    pub fn read<I: OffsetSizeTrait>(
        &mut self,
        out: &mut OffsetBuffer<I>,
        len: usize,
        dict: Option<&OffsetBuffer<I>>,
    ) -> Result<usize> {
        match self {
            ByteArrayDecoder::Plain(d) => d.read(out, len),
            ByteArrayDecoder::Dictionary(d) => {
                let dict =
                    dict.ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.read(out, dict, len)
            }
            ByteArrayDecoder::DeltaLength(d) => d.read(out, len),
            ByteArrayDecoder::DeltaByteArray(d) => d.read(out, len),
        }
    }

    /// Returns an upper bound on the number of values that can be decoded from
    /// the remainder of this page into an [`OffsetBuffer`] with `headroom`
    /// bytes of offset range left, or `None` if the whole remainder is
    /// guaranteed to fit
    ///
    /// `max_dict_value_len` is the length of the longest value in the
    /// dictionary and is only read for dictionary encoded pages.
    ///
    /// The common case is a single comparison against a bound that is already
    /// known, no values are decoded and nothing is scanned.
    pub fn values_capacity(&self, headroom: usize, max_dict_value_len: usize) -> Option<usize> {
        match self {
            ByteArrayDecoder::Plain(d) => d.values_capacity(headroom),
            ByteArrayDecoder::Dictionary(d) => d.values_capacity(headroom, max_dict_value_len),
            ByteArrayDecoder::DeltaLength(d) => d.values_capacity(headroom),
            ByteArrayDecoder::DeltaByteArray(d) => d.values_capacity(headroom),
        }
    }

    /// Skip `len` values
    pub fn skip<I: OffsetSizeTrait>(
        &mut self,
        len: usize,
        dict: Option<&OffsetBuffer<I>>,
    ) -> Result<usize> {
        match self {
            ByteArrayDecoder::Plain(d) => d.skip(len),
            ByteArrayDecoder::Dictionary(d) => {
                let dict =
                    dict.ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.skip(dict, len)
            }
            ByteArrayDecoder::DeltaLength(d) => d.skip(len),
            ByteArrayDecoder::DeltaByteArray(d) => d.skip(len),
        }
    }
}

/// Decoder from [`Encoding::PLAIN`] data to [`OffsetBuffer`]
pub struct ByteArrayDecoderPlain {
    buf: Bytes,
    offset: usize,
    validate_utf8: bool,

    /// This is a maximum as the null count is not always known, e.g. value data from
    /// a v1 data page
    max_remaining_values: usize,
}

impl ByteArrayDecoderPlain {
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

    pub fn read<I: OffsetSizeTrait>(
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
            let len_bytes: [u8; 4] = buf[self.offset..self.offset + 4].try_into().unwrap();
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

    /// See [`ByteArrayDecoder::values_capacity`]
    fn values_capacity(&self, headroom: usize) -> Option<usize> {
        if self.max_remaining_values == 0 {
            return None;
        }
        // The bytes left in the page bound the bytes this decoder can still
        // append, and they include the four byte length prefixes, so this is a
        // strict over-estimate. One comparison, no scan.
        let remaining_bytes = self.buf.len() - self.offset;
        if remaining_bytes <= headroom {
            return None;
        }

        // Only reached when the remainder of the page might not fit: walk the
        // length prefixes to find how many values do.
        let buf = self.buf.as_ref();
        let mut offset = self.offset;
        let mut total = 0usize;
        let mut count = 0usize;
        while count < self.max_remaining_values && offset + 4 <= buf.len() {
            let len_bytes: [u8; 4] = buf[offset..offset + 4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes) as usize;
            if total + len > headroom {
                break;
            }
            total += len;
            offset += 4 + len;
            count += 1;
        }
        Some(count)
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

/// Decoder from [`Encoding::DELTA_LENGTH_BYTE_ARRAY`] data to [`OffsetBuffer`]
pub struct ByteArrayDecoderDeltaLength {
    lengths: Vec<i32>,
    data: Bytes,
    length_offset: usize,
    data_offset: usize,
    validate_utf8: bool,
}

impl ByteArrayDecoderDeltaLength {
    fn new(data: Bytes, validate_utf8: bool) -> Result<Self> {
        let mut len_decoder = DeltaBitPackDecoder::<Int32Type>::new();
        len_decoder.set_data(data.clone(), 0)?;
        let values = len_decoder.values_left();

        let mut lengths = vec![0; values];
        len_decoder.get(&mut lengths)?;

        let mut total_bytes = 0;

        for l in &lengths {
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

    fn read<I: OffsetSizeTrait>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        let initial_values_length = output.values.len();

        let to_read = len.min(self.lengths.len() - self.length_offset);
        let src_lengths = &self.lengths[self.length_offset..self.length_offset + to_read];
        let total_bytes: usize = src_lengths.iter().map(|x| *x as usize).sum();

        // Reserve capacity for both offsets and values upfront
        output.offsets.reserve(to_read);
        output.values.reserve(total_bytes);

        // Delta length data is contiguous — copy all value bytes at once
        let data_end = self.data_offset + total_bytes;
        output
            .values
            .extend_from_slice(&self.data.as_ref()[self.data_offset..data_end]);

        // Compute and extend offsets in batch using extend
        let base_offset = initial_values_length;
        let mut running = base_offset;
        output.offsets.extend(src_lengths.iter().map(|length| {
            running += *length as usize;
            I::from_usize(running).expect("index overflow decoding byte array")
        }));

        self.data_offset = data_end;
        self.length_offset += to_read;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }
        Ok(to_read)
    }

    /// See [`ByteArrayDecoder::values_capacity`]
    fn values_capacity(&self, headroom: usize) -> Option<usize> {
        if self.length_offset >= self.lengths.len() {
            return None;
        }
        // The value bytes left in the page bound what this decoder can still
        // append. One comparison, no scan.
        let remaining_bytes = self.data.len() - self.data_offset;
        if remaining_bytes <= headroom {
            return None;
        }

        let mut total = 0usize;
        let mut count = 0usize;
        for length in &self.lengths[self.length_offset..] {
            let length = (*length).max(0) as usize;
            if total + length > headroom {
                break;
            }
            total += length;
            count += 1;
        }
        Some(count)
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

/// Decoder from [`Encoding::DELTA_BYTE_ARRAY`] to [`OffsetBuffer`]
pub struct ByteArrayDecoderDelta {
    decoder: DeltaByteArrayDecoder,
    validate_utf8: bool,
}

impl ByteArrayDecoderDelta {
    fn new(data: Bytes, validate_utf8: bool) -> Result<Self> {
        Ok(Self {
            decoder: DeltaByteArrayDecoder::new(data)?,
            validate_utf8,
        })
    }

    fn read<I: OffsetSizeTrait>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        let initial_values_length = output.values.len();
        output.offsets.reserve(len.min(self.decoder.remaining()));

        let read = self
            .decoder
            .read(len, |bytes| output.try_push(bytes, self.validate_utf8))?;

        if self.validate_utf8 {
            output.check_valid_utf8(initial_values_length)?;
        }
        Ok(read)
    }

    /// See [`ByteArrayDecoder::values_capacity`]
    fn values_capacity(&self, headroom: usize) -> Option<usize> {
        // A DELTA_BYTE_ARRAY value can be longer than the bytes it occupies in
        // the page because of the shared prefix, so the page length is not a
        // bound. Use the longest value in the page, which the decoder computed
        // when it decoded the length arrays.
        let max_value_len = self.decoder.max_value_len();
        if max_value_len == 0 {
            return None;
        }
        if self.decoder.remaining().saturating_mul(max_value_len) <= headroom {
            return None;
        }
        Some(headroom / max_value_len)
    }

    fn skip(&mut self, to_skip: usize) -> Result<usize> {
        self.decoder.skip(to_skip)
    }
}

/// Decoder from [`Encoding::RLE_DICTIONARY`] to [`OffsetBuffer`]
pub struct ByteArrayDecoderDictionary {
    decoder: DictIndexDecoder,
}

impl ByteArrayDecoderDictionary {
    fn new(data: Bytes, num_levels: usize, num_values: Option<usize>) -> Result<Self> {
        Ok(Self {
            decoder: DictIndexDecoder::new(data, num_levels, num_values)?,
        })
    }

    fn read<I: OffsetSizeTrait>(
        &mut self,
        output: &mut OffsetBuffer<I>,
        dict: &OffsetBuffer<I>,
        len: usize,
    ) -> Result<usize> {
        // All data must be NULL
        if dict.is_empty() {
            return Ok(0);
        }

        // Pre-reserve offsets capacity to avoid per-chunk reallocation
        output.offsets.reserve(len);

        self.decoder.read(len, |keys| {
            output.extend_from_dictionary(keys, dict.offsets.as_slice(), dict.values.as_slice())
        })
    }

    /// See [`ByteArrayDecoder::values_capacity`]
    fn values_capacity(&self, headroom: usize, max_dict_value_len: usize) -> Option<usize> {
        // The keys are not decoded here, so bound the output by the longest
        // value in the dictionary. One multiply and one comparison.
        if max_dict_value_len == 0 {
            return None;
        }
        if self
            .decoder
            .remaining()
            .saturating_mul(max_dict_value_len)
            <= headroom
        {
            return None;
        }
        Some(headroom / max_dict_value_len)
    }

    fn skip<I: OffsetSizeTrait>(
        &mut self,
        dict: &OffsetBuffer<I>,
        to_skip: usize,
    ) -> Result<usize> {
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
    use arrow_array::{Array, StringArray};
    use arrow_buffer::Buffer;

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
            let mut output = OffsetBuffer::<i32>::with_capacity(0);
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);

            assert_eq!(output.values.as_slice(), b"hello");
            assert_eq!(output.offsets.as_slice(), &[0, 5]);

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
            assert_eq!(output.values.as_slice(), b"helloworld");
            assert_eq!(output.offsets.as_slice(), &[0, 5, 10]);

            assert_eq!(decoder.read(&mut output, 2).unwrap(), 2);
            assert_eq!(output.values.as_slice(), b"helloworldab");
            assert_eq!(output.offsets.as_slice(), &[0, 5, 10, 11, 12]);

            assert_eq!(decoder.read(&mut output, 4).unwrap(), 0);

            let valid = [false, false, true, true, false, true, true, false, false];
            let valid_buffer = Buffer::from_iter(valid.iter().copied());

            output
                .pad_nulls(0, 4, valid.len(), valid_buffer.as_slice())
                .unwrap();
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
    fn test_byte_array_decoder_values_capacity() {
        // values are 5, 5, 1 and 1 bytes long
        let (pages, encoded_dictionary) =
            byte_array_all_encodings(vec!["hello", "world", "a", "b"]);

        let column_desc = utf8_column();
        let mut decoder = ByteArrayColumnValueDecoder::<i32>::new(&column_desc);
        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();
        assert_eq!(decoder.max_dict_value_len, 5);

        for (encoding, page) in pages {
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();
            let max_dict_value_len = decoder.max_dict_value_len;
            let inner = decoder.decoder.as_ref().unwrap();

            // Room for everything left in the page, so no cap is reported and
            // the fast path does not scan
            assert_eq!(
                inner.values_capacity(usize::MAX / 2, max_dict_value_len),
                None,
                "{encoding}"
            );

            // Room for the first two values only
            assert_eq!(
                inner.values_capacity(10, max_dict_value_len),
                Some(2),
                "{encoding}"
            );

            // No room at all
            assert_eq!(
                inner.values_capacity(0, max_dict_value_len),
                Some(0),
                "{encoding}"
            );
        }
    }

    #[test]
    fn test_byte_array_decoder_skip() {
        let (pages, encoded_dictionary) =
            byte_array_all_encodings(vec!["hello", "world", "a", "b"]);

        let column_desc = utf8_column();
        let mut decoder = ByteArrayColumnValueDecoder::new(&column_desc);

        decoder
            .set_dict(encoded_dictionary, 4, Encoding::RLE_DICTIONARY, false)
            .unwrap();

        for (encoding, page) in pages {
            let mut output = OffsetBuffer::<i32>::with_capacity(0);
            decoder.set_data(encoding, page, 4, Some(4)).unwrap();

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);

            assert_eq!(output.values.as_slice(), b"hello");
            assert_eq!(output.offsets.as_slice(), &[0, 5]);

            assert_eq!(decoder.skip_values(1).unwrap(), 1);
            assert_eq!(decoder.skip_values(1).unwrap(), 1);

            assert_eq!(decoder.read(&mut output, 1).unwrap(), 1);
            assert_eq!(output.values.as_slice(), b"hellob");
            assert_eq!(output.offsets.as_slice(), &[0, 5, 6]);

            assert_eq!(decoder.read(&mut output, 4).unwrap(), 0);

            let valid = [false, false, true, true, false, false];
            let valid_buffer = Buffer::from_iter(valid.iter().copied());

            output
                .pad_nulls(0, 2, valid.len(), valid_buffer.as_slice())
                .unwrap();
            let array = output.into_array(Some(valid_buffer), ArrowType::Utf8);
            let strings = array.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![None, None, Some("hello"), Some("b"), None, None,]
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

        // test nulls read
        for (encoding, page) in pages.clone() {
            let mut output = OffsetBuffer::<i32>::with_capacity(0);
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
