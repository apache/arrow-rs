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
use crate::arrow::buffer::bit_util::{iter_set_bits_rev, sign_extend_be};
use crate::arrow::decoder::{DeltaByteArrayDecoder, DictIndexDecoder};
use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{Encoding, Type};
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use arrow_array::{
    ArrayRef, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    IntervalDayTimeArray, IntervalYearMonthArray,
};
use arrow_buffer::{i256, Buffer, IntervalDayTime};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{DataType as ArrowType, IntervalUnit};
use bytes::Bytes;
use half::f16;
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// Returns an [`ArrayReader`] that decodes the provided fixed length byte array column
pub fn make_fixed_len_byte_array_reader(
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

    let byte_length = match column_desc.physical_type() {
        Type::FIXED_LEN_BYTE_ARRAY => column_desc.type_length() as usize,
        t => {
            return Err(general_err!(
                "invalid physical type for fixed length byte array reader - {}",
                t
            ))
        }
    };
    match &data_type {
        ArrowType::FixedSizeBinary(_) => {}
        ArrowType::Decimal128(_, _) => {
            if byte_length > 16 {
                return Err(general_err!(
                    "decimal 128 type too large, must be less than 16 bytes, got {}",
                    byte_length
                ));
            }
        }
        ArrowType::Decimal256(_, _) => {
            if byte_length > 32 {
                return Err(general_err!(
                    "decimal 256 type too large, must be less than 32 bytes, got {}",
                    byte_length
                ));
            }
        }
        ArrowType::Interval(_) => {
            if byte_length != 12 {
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval
                return Err(general_err!(
                    "interval type must consist of 12 bytes got {}",
                    byte_length
                ));
            }
        }
        ArrowType::Float16 => {
            if byte_length != 2 {
                return Err(general_err!(
                    "float 16 type must be 2 bytes, got {}",
                    byte_length
                ));
            }
        }
        _ => {
            return Err(general_err!(
                "invalid data type for fixed length byte array reader - {}",
                data_type
            ))
        }
    }

    Ok(Box::new(FixedLenByteArrayReader::new(
        pages,
        column_desc,
        data_type,
        byte_length,
    )))
}

struct FixedLenByteArrayReader {
    data_type: ArrowType,
    byte_length: usize,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: GenericRecordReader<FixedLenByteArrayBuffer, ValueDecoder>,
}

impl FixedLenByteArrayReader {
    fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        data_type: ArrowType,
        byte_length: usize,
    ) -> Self {
        Self {
            data_type,
            byte_length,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            record_reader: GenericRecordReader::new(column_desc),
        }
    }
}

impl ArrayReader for FixedLenByteArrayReader {
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
        let record_data = self.record_reader.consume_record_data();

        let array_data = ArrayDataBuilder::new(ArrowType::FixedSizeBinary(self.byte_length as i32))
            .len(self.record_reader.num_values())
            .add_buffer(Buffer::from_vec(record_data.buffer))
            .null_bit_buffer(self.record_reader.consume_bitmap_buffer());

        let binary = FixedSizeBinaryArray::from(unsafe { array_data.build_unchecked() });

        // TODO: An improvement might be to do this conversion on read
        // Note the conversions below apply to all elements regardless of null slots as the
        // conversion lambdas are all infallible. This improves performance by avoiding a branch in
        // the inner loop (see docs for `PrimitiveArray::from_unary`).
        let array: ArrayRef = match &self.data_type {
            ArrowType::Decimal128(p, s) => {
                let f = |b: &[u8]| i128::from_be_bytes(sign_extend_be(b));
                Arc::new(Decimal128Array::from_unary(&binary, f).with_precision_and_scale(*p, *s)?)
                    as ArrayRef
            }
            ArrowType::Decimal256(p, s) => {
                let f = |b: &[u8]| i256::from_be_bytes(sign_extend_be(b));
                Arc::new(Decimal256Array::from_unary(&binary, f).with_precision_and_scale(*p, *s)?)
                    as ArrayRef
            }
            ArrowType::Interval(unit) => {
                // An interval is stored as 3x 32-bit unsigned integers storing months, days,
                // and milliseconds
                match unit {
                    IntervalUnit::YearMonth => {
                        let f = |b: &[u8]| i32::from_le_bytes(b[0..4].try_into().unwrap());
                        Arc::new(IntervalYearMonthArray::from_unary(&binary, f)) as ArrayRef
                    }
                    IntervalUnit::DayTime => {
                        let f = |b: &[u8]| {
                            IntervalDayTime::new(
                                i32::from_le_bytes(b[4..8].try_into().unwrap()),
                                i32::from_le_bytes(b[8..12].try_into().unwrap()),
                            )
                        };
                        Arc::new(IntervalDayTimeArray::from_unary(&binary, f)) as ArrayRef
                    }
                    IntervalUnit::MonthDayNano => {
                        return Err(nyi_err!("MonthDayNano intervals not supported"));
                    }
                }
            }
            ArrowType::Float16 => {
                let f = |b: &[u8]| f16::from_le_bytes(b[..2].try_into().unwrap());
                Arc::new(Float16Array::from_unary(&binary, f)) as ArrayRef
            }
            _ => Arc::new(binary) as ArrayRef,
        };

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

#[derive(Default)]
struct FixedLenByteArrayBuffer {
    buffer: Vec<u8>,
    /// The length of each element in bytes
    byte_length: Option<usize>,
}

#[inline]
fn move_values<F>(
    buffer: &mut Vec<u8>,
    byte_length: usize,
    values_range: Range<usize>,
    valid_mask: &[u8],
    mut op: F,
) where
    F: FnMut(&mut Vec<u8>, usize, usize, usize),
{
    for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
        debug_assert!(level_pos >= value_pos);
        if level_pos <= value_pos {
            break;
        }

        let level_pos_bytes = level_pos * byte_length;
        let value_pos_bytes = value_pos * byte_length;

        op(buffer, level_pos_bytes, value_pos_bytes, byte_length)
    }
}

impl ValuesBuffer for FixedLenByteArrayBuffer {
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) {
        let byte_length = self.byte_length.unwrap_or_default();

        assert_eq!(self.buffer.len(), (read_offset + values_read) * byte_length);
        self.buffer
            .resize((read_offset + levels_read) * byte_length, 0);

        let values_range = read_offset..read_offset + values_read;
        // Move the bytes from value_pos to level_pos. For values of `byte_length` <= 4,
        // the simple loop is preferred as the compiler can eliminate the loop via unrolling.
        // For `byte_length > 4`, we instead copy from non-overlapping slices. This allows
        // the loop to be vectorized, yielding much better performance.
        const VEC_CUTOFF: usize = 4;
        if byte_length > VEC_CUTOFF {
            let op = |buffer: &mut Vec<u8>, level_pos_bytes, value_pos_bytes, byte_length| {
                let split = buffer.split_at_mut(level_pos_bytes);
                let dst = &mut split.1[..byte_length];
                let src = &split.0[value_pos_bytes..value_pos_bytes + byte_length];
                dst.copy_from_slice(src);
            };
            move_values(&mut self.buffer, byte_length, values_range, valid_mask, op);
        } else {
            let op = |buffer: &mut Vec<u8>, level_pos_bytes, value_pos_bytes, byte_length| {
                for i in 0..byte_length {
                    buffer[level_pos_bytes + i] = buffer[value_pos_bytes + i]
                }
            };
            move_values(&mut self.buffer, byte_length, values_range, valid_mask, op);
        }
    }
}

struct ValueDecoder {
    byte_length: usize,
    dict_page: Option<Bytes>,
    decoder: Option<Decoder>,
}

impl ColumnValueDecoder for ValueDecoder {
    type Buffer = FixedLenByteArrayBuffer;

    fn new(col: &ColumnDescPtr) -> Self {
        Self {
            byte_length: col.type_length() as usize,
            dict_page: None,
            decoder: None,
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
        let expected_len = num_values as usize * self.byte_length;
        if expected_len > buf.len() {
            return Err(general_err!(
                "too few bytes in dictionary page, expected {} got {}",
                expected_len,
                buf.len()
            ));
        }

        self.dict_page = Some(buf);
        Ok(())
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        self.decoder = Some(match encoding {
            Encoding::PLAIN => Decoder::Plain {
                buf: data,
                offset: 0,
            },
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => Decoder::Dict {
                decoder: DictIndexDecoder::new(data, num_levels, num_values),
            },
            Encoding::DELTA_BYTE_ARRAY => Decoder::Delta {
                decoder: DeltaByteArrayDecoder::new(data)?,
            },
            Encoding::BYTE_STREAM_SPLIT => Decoder::ByteStreamSplit {
                buf: data,
                offset: 0,
            },
            _ => {
                return Err(general_err!(
                    "unsupported encoding for fixed length byte array: {}",
                    encoding
                ))
            }
        });
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        match out.byte_length {
            Some(x) => assert_eq!(x, self.byte_length),
            None => out.byte_length = Some(self.byte_length),
        }

        match self.decoder.as_mut().unwrap() {
            Decoder::Plain { offset, buf } => {
                let to_read =
                    (num_values * self.byte_length).min(buf.len() - *offset) / self.byte_length;
                let end_offset = *offset + to_read * self.byte_length;
                out.buffer
                    .extend_from_slice(&buf.as_ref()[*offset..end_offset]);
                *offset = end_offset;
                Ok(to_read)
            }
            Decoder::Dict { decoder } => {
                let dict = self.dict_page.as_ref().unwrap();
                // All data must be NULL
                if dict.is_empty() {
                    return Ok(0);
                }

                decoder.read(num_values, |keys| {
                    out.buffer.reserve(keys.len() * self.byte_length);
                    for key in keys {
                        let offset = *key as usize * self.byte_length;
                        let val = &dict.as_ref()[offset..offset + self.byte_length];
                        out.buffer.extend_from_slice(val);
                    }
                    Ok(())
                })
            }
            Decoder::Delta { decoder } => {
                let to_read = num_values.min(decoder.remaining());
                out.buffer.reserve(to_read * self.byte_length);

                decoder.read(to_read, |slice| {
                    if slice.len() != self.byte_length {
                        return Err(general_err!(
                            "encountered array with incorrect length, got {} expected {}",
                            slice.len(),
                            self.byte_length
                        ));
                    }
                    out.buffer.extend_from_slice(slice);
                    Ok(())
                })
            }
            Decoder::ByteStreamSplit { buf, offset } => {
                // we have n=`byte_length` streams of length `buf.len/byte_length`
                // to read value i, we need the i'th byte from each of the streams
                // so `offset` should be the value offset, not the byte offset
                let total_values = buf.len() / self.byte_length;
                let to_read = num_values.min(total_values - *offset);

                // now read the n streams and reassemble values into the output buffer
                read_byte_stream_split(&mut out.buffer, buf, *offset, to_read, self.byte_length);

                *offset += to_read;
                Ok(to_read)
            }
        }
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        match self.decoder.as_mut().unwrap() {
            Decoder::Plain { offset, buf } => {
                let to_read = num_values.min((buf.len() - *offset) / self.byte_length);
                *offset += to_read * self.byte_length;
                Ok(to_read)
            }
            Decoder::Dict { decoder } => decoder.skip(num_values),
            Decoder::Delta { decoder } => decoder.skip(num_values),
            Decoder::ByteStreamSplit { offset, buf } => {
                let total_values = buf.len() / self.byte_length;
                let to_read = num_values.min(total_values - *offset);
                *offset += to_read;
                Ok(to_read)
            }
        }
    }
}

// `src` is an array laid out like a NxM matrix where N == `data_width` and
// M == total_values_in_src. Each "row" of the matrix is a stream of bytes, with stream `i`
// containing the `ith` byte for each value. Each "column" is a single value.
// This will reassemble `num_values` values by reading columns of the matrix starting at
// `offset`. Values will be appended to `dst`.
fn read_byte_stream_split(
    dst: &mut Vec<u8>,
    src: &mut Bytes,
    offset: usize,
    num_values: usize,
    data_width: usize,
) {
    let stride = src.len() / data_width;
    let idx = dst.len();
    dst.resize(idx + num_values * data_width, 0u8);
    let dst_slc = &mut dst[idx..idx + num_values * data_width];
    for j in 0..data_width {
        let src_slc = &src[offset + j * stride..offset + j * stride + num_values];
        for i in 0..num_values {
            dst_slc[i * data_width + j] = src_slc[i];
        }
    }
}

enum Decoder {
    Plain { buf: Bytes, offset: usize },
    Dict { decoder: DictIndexDecoder },
    Delta { decoder: DeltaByteArrayDecoder },
    ByteStreamSplit { buf: Bytes, offset: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::ParquetRecordBatchReader;
    use crate::arrow::ArrowWriter;
    use arrow::datatypes::Field;
    use arrow::error::Result as ArrowResult;
    use arrow_array::{Array, ListArray};
    use arrow_array::{Decimal256Array, RecordBatch};
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn test_decimal_list() {
        let decimals = Decimal256Array::from_iter_values(
            [1, 2, 3, 4, 5, 6, 7, 8].into_iter().map(i256::from_i128),
        );

        // [[], [1], [2, 3], null, [4], null, [6, 7, 8]]
        let data = ArrayDataBuilder::new(ArrowType::List(Arc::new(Field::new(
            "item",
            decimals.data_type().clone(),
            false,
        ))))
        .len(7)
        .add_buffer(Buffer::from_iter([0_i32, 0, 1, 3, 3, 4, 5, 8]))
        .null_bit_buffer(Some(Buffer::from(&[0b01010111])))
        .child_data(vec![decimals.into_data()])
        .build()
        .unwrap();

        let written =
            RecordBatch::try_from_iter([("list", Arc::new(ListArray::from(data)) as ArrayRef)])
                .unwrap();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
        writer.write(&written).unwrap();
        writer.close().unwrap();

        let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(&written.slice(0, 3), &read[0]);
        assert_eq!(&written.slice(3, 3), &read[1]);
        assert_eq!(&written.slice(6, 1), &read[2]);
    }
}
