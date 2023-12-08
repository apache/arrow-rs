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
use crate::arrow::record_reader::buffer::{BufferQueue, ValuesBuffer};
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{Encoding, Type};
use crate::column::page::PageIterator;
use crate::column::reader::decoder::{ColumnValueDecoder, ValuesBufferSlice};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use arrow_array::{
    ArrayRef, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    IntervalDayTimeArray, IntervalYearMonthArray,
};
use arrow_buffer::{i256, Buffer};
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
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
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
            record_reader: GenericRecordReader::new_with_records(
                column_desc,
                FixedLenByteArrayBuffer {
                    buffer: Default::default(),
                    byte_length,
                },
            ),
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
            .add_buffer(record_data)
            .null_bit_buffer(self.record_reader.consume_bitmap_buffer());

        let binary = FixedSizeBinaryArray::from(unsafe { array_data.build_unchecked() });

        // TODO: An improvement might be to do this conversion on read
        let array: ArrayRef = match &self.data_type {
            ArrowType::Decimal128(p, s) => {
                let decimal = binary
                    .iter()
                    .map(|opt| Some(i128::from_be_bytes(sign_extend_be(opt?))))
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(*p, *s)?;

                Arc::new(decimal)
            }
            ArrowType::Decimal256(p, s) => {
                let decimal = binary
                    .iter()
                    .map(|opt| Some(i256::from_be_bytes(sign_extend_be(opt?))))
                    .collect::<Decimal256Array>()
                    .with_precision_and_scale(*p, *s)?;

                Arc::new(decimal)
            }
            ArrowType::Interval(unit) => {
                // An interval is stored as 3x 32-bit unsigned integers storing months, days,
                // and milliseconds
                match unit {
                    IntervalUnit::YearMonth => Arc::new(
                        binary
                            .iter()
                            .map(|o| o.map(|b| i32::from_le_bytes(b[0..4].try_into().unwrap())))
                            .collect::<IntervalYearMonthArray>(),
                    ) as ArrayRef,
                    IntervalUnit::DayTime => Arc::new(
                        binary
                            .iter()
                            .map(|o| o.map(|b| i64::from_le_bytes(b[4..12].try_into().unwrap())))
                            .collect::<IntervalDayTimeArray>(),
                    ) as ArrayRef,
                    IntervalUnit::MonthDayNano => {
                        return Err(nyi_err!("MonthDayNano intervals not supported"));
                    }
                }
            }
            ArrowType::Float16 => Arc::new(
                binary
                    .iter()
                    .map(|o| o.map(|b| f16::from_le_bytes(b[..2].try_into().unwrap())))
                    .collect::<Float16Array>(),
            ) as ArrayRef,
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
        self.def_levels_buffer.as_ref().map(|buf| buf.typed_data())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_ref().map(|buf| buf.typed_data())
    }
}

struct FixedLenByteArrayBuffer {
    buffer: Vec<u8>,
    /// The length of each element in bytes
    byte_length: usize,
}

impl ValuesBufferSlice for FixedLenByteArrayBuffer {
    fn capacity(&self) -> usize {
        usize::MAX
    }
}

impl BufferQueue for FixedLenByteArrayBuffer {
    type Output = Buffer;
    type Slice = Self;

    fn consume(&mut self) -> Self::Output {
        Buffer::from_vec(self.buffer.consume())
    }

    fn get_output_slice(&mut self, _batch_size: usize) -> &mut Self::Slice {
        self
    }

    fn truncate_buffer(&mut self, len: usize) {
        assert_eq!(self.buffer.len(), len * self.byte_length);
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
        assert_eq!(
            self.buffer.len(),
            (read_offset + values_read) * self.byte_length
        );
        self.buffer
            .resize((read_offset + levels_read) * self.byte_length, 0);

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }

            let level_pos_bytes = level_pos * self.byte_length;
            let value_pos_bytes = value_pos * self.byte_length;

            for i in 0..self.byte_length {
                self.buffer[level_pos_bytes + i] = self.buffer[value_pos_bytes + i]
            }
        }
    }
}

struct ValueDecoder {
    byte_length: usize,
    dict_page: Option<Bytes>,
    decoder: Option<Decoder>,
}

impl ColumnValueDecoder for ValueDecoder {
    type Slice = FixedLenByteArrayBuffer;

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
            _ => {
                return Err(general_err!(
                    "unsupported encoding for fixed length byte array: {}",
                    encoding
                ))
            }
        });
        Ok(())
    }

    fn read(&mut self, out: &mut Self::Slice, range: Range<usize>) -> Result<usize> {
        assert_eq!(self.byte_length, out.byte_length);

        let len = range.end - range.start;
        match self.decoder.as_mut().unwrap() {
            Decoder::Plain { offset, buf } => {
                let to_read = (len * self.byte_length).min(buf.len() - *offset) / self.byte_length;
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

                decoder.read(len, |keys| {
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
                let to_read = len.min(decoder.remaining());
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
        }
    }
}

enum Decoder {
    Plain { buf: Bytes, offset: usize },
    Dict { decoder: DictIndexDecoder },
    Delta { decoder: DeltaByteArrayDecoder },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::ParquetRecordBatchReader;
    use crate::arrow::ArrowWriter;
    use arrow::datatypes::Field;
    use arrow::error::Result as ArrowResult;
    use arrow_array::RecordBatch;
    use arrow_array::{Array, ListArray};
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
