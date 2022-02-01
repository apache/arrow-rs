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

//! Contains writer which writes arrow data into parquet data.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array as arrow_array;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType as ArrowDataType, IntervalUnit, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;

use super::levels::LevelInfo;
use super::schema::{
    add_encoded_arrow_schema_to_metadata, arrow_to_parquet_schema,
    decimal_length_from_precision,
};

use crate::column::writer::ColumnWriter;
use crate::errors::{ParquetError, Result};
use crate::file::properties::WriterProperties;
use crate::{
    data_type::*,
    file::writer::{FileWriter, ParquetWriter, RowGroupWriter, SerializedFileWriter},
};

/// Arrow writer
///
/// Writes Arrow `RecordBatch`es to a Parquet writer, buffering up `RecordBatch` in order
/// to produce row groups with `max_row_group_size` rows. Any remaining rows will be
/// flushed on close, leading the final row group in the output file to potentially
/// contain fewer than `max_row_group_size` rows
pub struct ArrowWriter<W: ParquetWriter> {
    /// Underlying Parquet writer
    writer: SerializedFileWriter<W>,

    /// For each column, maintain an ordered queue of arrays to write
    buffer: Vec<VecDeque<ArrayRef>>,

    /// The total number of rows currently buffered
    buffered_rows: usize,

    /// A copy of the Arrow schema.
    ///
    /// The schema is used to verify that each record batch written has the correct schema
    arrow_schema: SchemaRef,

    /// The length of arrays to write to each row group
    max_row_group_size: usize,
}

impl<W: 'static + ParquetWriter> ArrowWriter<W> {
    /// Try to create a new Arrow writer
    ///
    /// The writer will fail if:
    ///  * a `SerializedFileWriter` cannot be created from the ParquetWriter
    ///  * the Arrow schema contains unsupported datatypes such as Unions
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let schema = arrow_to_parquet_schema(&arrow_schema)?;
        // add serialized arrow schema
        let mut props = props.unwrap_or_else(|| WriterProperties::builder().build());
        add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);

        let max_row_group_size = props.max_row_group_size();

        let file_writer =
            SerializedFileWriter::new(writer, schema.root_schema_ptr(), Arc::new(props))?;

        Ok(Self {
            writer: file_writer,
            buffer: vec![Default::default(); arrow_schema.fields().len()],
            buffered_rows: 0,
            arrow_schema,
            max_row_group_size,
        })
    }

    /// Enqueues the provided `RecordBatch` to be written
    ///
    /// If following this there are more than `max_row_group_size` rows buffered,
    /// this will flush out one or more row groups with `max_row_group_size` rows,
    /// and drop any fully written `RecordBatch`
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // validate batch schema against writer's supplied schema
        if self.arrow_schema != batch.schema() {
            return Err(ParquetError::ArrowError(
                "Record batch schema does not match writer schema".to_string(),
            ));
        }

        for (buffer, column) in self.buffer.iter_mut().zip(batch.columns()) {
            buffer.push_back(column.clone())
        }

        self.buffered_rows += batch.num_rows();

        self.flush_completed()?;

        Ok(())
    }

    /// Flushes buffered data until there are less than `max_row_group_size` rows buffered
    fn flush_completed(&mut self) -> Result<()> {
        while self.buffered_rows >= self.max_row_group_size {
            self.flush_row_group(self.max_row_group_size)?;
        }
        Ok(())
    }

    /// Flushes `num_rows` from the buffer into a new row group
    fn flush_row_group(&mut self, num_rows: usize) -> Result<()> {
        if num_rows == 0 {
            return Ok(());
        }

        assert!(
            num_rows <= self.buffered_rows,
            "cannot flush {} rows only have {}",
            num_rows,
            self.buffered_rows
        );

        assert!(
            num_rows <= self.max_row_group_size,
            "cannot flush {} rows would exceed max row group size of {}",
            num_rows,
            self.max_row_group_size
        );

        let mut row_group_writer = self.writer.next_row_group()?;

        for (col_buffer, field) in self.buffer.iter_mut().zip(self.arrow_schema.fields())
        {
            // Collect the number of arrays to append
            let mut remaining = num_rows;
            let mut arrays = Vec::with_capacity(col_buffer.len());
            while remaining != 0 {
                match col_buffer.pop_front() {
                    Some(next) if next.len() > remaining => {
                        col_buffer
                            .push_front(next.slice(remaining, next.len() - remaining));
                        arrays.push(next.slice(0, remaining));
                        remaining = 0;
                    }
                    Some(next) => {
                        remaining -= next.len();
                        arrays.push(next);
                    }
                    _ => break,
                }
            }

            let mut levels: Vec<_> = arrays
                .iter()
                .map(|array| {
                    let batch_level = LevelInfo::new(0, array.len());
                    let mut levels = batch_level.calculate_array_levels(array, field);
                    // Reverse levels as we pop() them when writing arrays
                    levels.reverse();
                    levels
                })
                .collect();

            write_leaves(row_group_writer.as_mut(), &arrays, &mut levels)?;
        }

        self.writer.close_row_group(row_group_writer)?;
        self.buffered_rows -= num_rows;

        Ok(())
    }

    /// Close and finalize the underlying Parquet writer
    pub fn close(&mut self) -> Result<parquet_format::FileMetaData> {
        self.flush_completed()?;
        self.flush_row_group(self.buffered_rows)?;
        self.writer.close()
    }
}

/// Convenience method to get the next ColumnWriter from the RowGroupWriter
#[inline]
fn get_col_writer(row_group_writer: &mut dyn RowGroupWriter) -> Result<ColumnWriter> {
    let col_writer = row_group_writer
        .next_column()?
        .expect("Unable to get column writer");
    Ok(col_writer)
}

fn write_leaves(
    row_group_writer: &mut dyn RowGroupWriter,
    arrays: &[ArrayRef],
    levels: &mut [Vec<LevelInfo>],
) -> Result<()> {
    assert_eq!(arrays.len(), levels.len());
    assert!(!arrays.is_empty());

    let data_type = arrays.first().unwrap().data_type().clone();
    assert!(arrays.iter().all(|a| a.data_type() == &data_type));

    match &data_type {
        ArrowDataType::Null
        | ArrowDataType::Boolean
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Date32
        | ArrowDataType::Date64
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::LargeBinary
        | ArrowDataType::Binary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::Decimal(_, _)
        | ArrowDataType::FixedSizeBinary(_) => {
            let mut col_writer = get_col_writer(row_group_writer)?;
            for (array, levels) in arrays.iter().zip(levels.iter_mut()) {
                write_leaf(
                    &mut col_writer,
                    array,
                    levels.pop().expect("Levels exhausted"),
                )?;
            }
            row_group_writer.close_column(col_writer)?;
            Ok(())
        }
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
            let arrays: Vec<_> = arrays.iter().map(|array|{
                // write the child list
                let data = array.data();
                arrow_array::make_array(data.child_data()[0].clone())
            }).collect();

            write_leaves(row_group_writer, &arrays, levels)?;
            Ok(())
        }
        ArrowDataType::Struct(fields) => {
            // Groups child arrays by field
            let mut field_arrays = vec![Vec::with_capacity(arrays.len()); fields.len()];

            for array in arrays {
                let struct_array: &arrow_array::StructArray = array
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .expect("Unable to get struct array");

                assert_eq!(struct_array.columns().len(), fields.len());

                for (child_array, field) in field_arrays.iter_mut().zip(struct_array.columns()) {
                    child_array.push(field.clone())
                }
            }

            for field in field_arrays {
                write_leaves(row_group_writer, &field, levels)?;
            }

            Ok(())
        }
        ArrowDataType::Map(_, _) => {
            let mut keys = Vec::with_capacity(arrays.len());
            let mut values = Vec::with_capacity(arrays.len());
            for array in arrays {
                let map_array: &arrow_array::MapArray = array
                    .as_any()
                    .downcast_ref::<arrow_array::MapArray>()
                    .expect("Unable to get map array");
                keys.push(map_array.keys());
                values.push(map_array.values());
            }

            write_leaves(row_group_writer, &keys, levels)?;
            write_leaves(row_group_writer, &values, levels)?;
            Ok(())
        }
        ArrowDataType::Dictionary(_, value_type) => {
            let mut col_writer = get_col_writer(row_group_writer)?;
            for (array, levels) in arrays.iter().zip(levels.iter_mut()) {
                // cast dictionary to a primitive
                let array = arrow::compute::cast(array, value_type)?;
                write_leaf(
                    &mut col_writer,
                    &array,
                    levels.pop().expect("Levels exhausted"),
                )?;
            }
            row_group_writer.close_column(col_writer)?;
            Ok(())
        }
        ArrowDataType::Float16 => Err(ParquetError::ArrowError(
            "Float16 arrays not supported".to_string(),
        )),
        ArrowDataType::FixedSizeList(_, _) | ArrowDataType::Union(_, _) => {
            Err(ParquetError::NYI(
                format!(
                    "Attempting to write an Arrow type {:?} to parquet that is not yet implemented",
                    data_type
                )
            ))
        }
    }
}

fn write_leaf(
    writer: &mut ColumnWriter,
    column: &arrow_array::ArrayRef,
    levels: LevelInfo,
) -> Result<i64> {
    let indices = levels.filter_array_indices();
    // Slice array according to computed offset and length
    let column = column.slice(levels.offset, levels.length);
    let written = match writer {
        ColumnWriter::Int32ColumnWriter(ref mut typed) => {
            let values = match column.data_type() {
                ArrowDataType::Date64 => {
                    // If the column is a Date64, we cast it to a Date32, and then interpret that as Int32
                    let array = if let ArrowDataType::Date64 = column.data_type() {
                        let array =
                            arrow::compute::cast(&column, &ArrowDataType::Date32)?;
                        arrow::compute::cast(&array, &ArrowDataType::Int32)?
                    } else {
                        arrow::compute::cast(&column, &ArrowDataType::Int32)?
                    };
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .expect("Unable to get int32 array");
                    get_numeric_array_slice::<Int32Type, _>(array, &indices)
                }
                ArrowDataType::UInt32 => {
                    // follow C++ implementation and use overflow/reinterpret cast from  u32 to i32 which will map
                    // `(i32::MAX as u32)..u32::MAX` to `i32::MIN..0`
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::UInt32Array>()
                        .expect("Unable to get u32 array");
                    let array = arrow::compute::unary::<_, _, arrow::datatypes::Int32Type>(
                        array,
                        |x| x as i32,
                    );
                    get_numeric_array_slice::<Int32Type, _>(&array, &indices)
                }
                _ => {
                    let array = arrow::compute::cast(&column, &ArrowDataType::Int32)?;
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .expect("Unable to get i32 array");
                    get_numeric_array_slice::<Int32Type, _>(array, &indices)
                }
            };
            typed.write_batch(
                values.as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::BoolColumnWriter(ref mut typed) => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("Unable to get boolean array");
            typed.write_batch(
                get_bool_array_slice(array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::Int64ColumnWriter(ref mut typed) => {
            let values = match column.data_type() {
                ArrowDataType::Int64 => {
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("Unable to get i64 array");
                    get_numeric_array_slice::<Int64Type, _>(array, &indices)
                }
                ArrowDataType::UInt64 => {
                    // follow C++ implementation and use overflow/reinterpret cast from  u64 to i64 which will map
                    // `(i64::MAX as u64)..u64::MAX` to `i64::MIN..0`
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::UInt64Array>()
                        .expect("Unable to get u64 array");
                    let array = arrow::compute::unary::<_, _, arrow::datatypes::Int64Type>(
                        array,
                        |x| x as i64,
                    );
                    get_numeric_array_slice::<Int64Type, _>(&array, &indices)
                }
                _ => {
                    let array = arrow::compute::cast(&column, &ArrowDataType::Int64)?;
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("Unable to get i64 array");
                    get_numeric_array_slice::<Int64Type, _>(array, &indices)
                }
            };
            typed.write_batch(
                values.as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::Int96ColumnWriter(ref mut _typed) => {
            unreachable!("Currently unreachable because data type not supported")
        }
        ColumnWriter::FloatColumnWriter(ref mut typed) => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .expect("Unable to get Float32 array");
            typed.write_batch(
                get_numeric_array_slice::<FloatType, _>(array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::DoubleColumnWriter(ref mut typed) => {
            let array = column
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .expect("Unable to get Float64 array");
            typed.write_batch(
                get_numeric_array_slice::<DoubleType, _>(array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::ByteArrayColumnWriter(ref mut typed) => match column.data_type() {
            ArrowDataType::Binary => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .expect("Unable to get BinaryArray array");
                typed.write_batch(
                    get_binary_array(array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::Utf8 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("Unable to get LargeBinaryArray array");
                typed.write_batch(
                    get_string_array(array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::LargeBinary => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .expect("Unable to get LargeBinaryArray array");
                typed.write_batch(
                    get_large_binary_array(array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::LargeUtf8 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .expect("Unable to get LargeUtf8 array");
                typed.write_batch(
                    get_large_string_array(array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            _ => unreachable!("Currently unreachable because data type not supported"),
        },
        ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) => {
            let bytes = match column.data_type() {
                ArrowDataType::Interval(interval_unit) => match interval_unit {
                    IntervalUnit::YearMonth => {
                        let array = column
                            .as_any()
                            .downcast_ref::<arrow_array::IntervalYearMonthArray>()
                            .unwrap();
                        get_interval_ym_array_slice(array, &indices)
                    }
                    IntervalUnit::DayTime => {
                        let array = column
                            .as_any()
                            .downcast_ref::<arrow_array::IntervalDayTimeArray>()
                            .unwrap();
                        get_interval_dt_array_slice(array, &indices)
                    }
                    _ => {
                        return Err(ParquetError::NYI(
                            format!(
                                "Attempting to write an Arrow interval type {:?} to parquet that is not yet implemented",
                                interval_unit
                            )
                        ));
                    }
                },
                ArrowDataType::FixedSizeBinary(_) => {
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                        .unwrap();
                    get_fsb_array_slice(array, &indices)
                }
                ArrowDataType::Decimal(_, _) => {
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::DecimalArray>()
                        .unwrap();
                    get_decimal_array_slice(array, &indices)
                }
                _ => {
                    return Err(ParquetError::NYI(
                        "Attempting to write an Arrow type that is not yet implemented"
                            .to_string(),
                    ));
                }
            };
            typed.write_batch(
                bytes.as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
    };
    Ok(written as i64)
}

macro_rules! def_get_binary_array_fn {
    ($name:ident, $ty:ty) => {
        fn $name(array: &$ty) -> Vec<ByteArray> {
            let mut byte_array = ByteArray::new();
            let ptr = crate::util::memory::ByteBufferPtr::new(
                unsafe { array.value_data().typed_data::<u8>() }.to_vec(),
            );
            byte_array.set_data(ptr);
            array
                .value_offsets()
                .windows(2)
                .enumerate()
                .filter_map(|(i, offsets)| {
                    if array.is_valid(i) {
                        let start = offsets[0] as usize;
                        let len = offsets[1] as usize - start;
                        Some(byte_array.slice(start, len))
                    } else {
                        None
                    }
                })
                .collect()
        }
    };
}

def_get_binary_array_fn!(get_binary_array, arrow_array::BinaryArray);
def_get_binary_array_fn!(get_string_array, arrow_array::StringArray);
def_get_binary_array_fn!(get_large_binary_array, arrow_array::LargeBinaryArray);
def_get_binary_array_fn!(get_large_string_array, arrow_array::LargeStringArray);

/// Get the underlying numeric array slice, skipping any null values.
/// If there are no null values, it might be quicker to get the slice directly instead of
/// calling this function.
fn get_numeric_array_slice<T, A>(
    array: &arrow_array::PrimitiveArray<A>,
    indices: &[usize],
) -> Vec<T::T>
where
    T: DataType,
    A: arrow::datatypes::ArrowNumericType,
    T::T: From<A::Native>,
{
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        values.push(array.value(*i).into())
    }
    values
}

fn get_bool_array_slice(
    array: &arrow_array::BooleanArray,
    indices: &[usize],
) -> Vec<bool> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        values.push(array.value(*i))
    }
    values
}

/// Returns 12-byte values representing 3 values of months, days and milliseconds (4-bytes each).
/// An Arrow YearMonth interval only stores months, thus only the first 4 bytes are populated.
fn get_interval_ym_array_slice(
    array: &arrow_array::IntervalYearMonthArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let mut value = array.value(*i).to_le_bytes().to_vec();
        let mut suffix = vec![0; 8];
        value.append(&mut suffix);
        values.push(FixedLenByteArray::from(ByteArray::from(value)))
    }
    values
}

/// Returns 12-byte values representing 3 values of months, days and milliseconds (4-bytes each).
/// An Arrow DayTime interval only stores days and millis, thus the first 4 bytes are not populated.
fn get_interval_dt_array_slice(
    array: &arrow_array::IntervalDayTimeArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let mut prefix = vec![0; 4];
        let mut value = array.value(*i).to_le_bytes().to_vec();
        prefix.append(&mut value);
        debug_assert_eq!(prefix.len(), 12);
        values.push(FixedLenByteArray::from(ByteArray::from(prefix)));
    }
    values
}

fn get_decimal_array_slice(
    array: &arrow_array::DecimalArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    let size = decimal_length_from_precision(array.precision());
    for i in indices {
        let as_be_bytes = array.value(*i).to_be_bytes();
        let resized_value = as_be_bytes[(16 - size)..].to_vec();
        values.push(FixedLenByteArray::from(ByteArray::from(resized_value)));
    }
    values
}

fn get_fsb_array_slice(
    array: &arrow_array::FixedSizeBinaryArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let value = array.value(*i).to_vec();
        values.push(FixedLenByteArray::from(ByteArray::from(value)))
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::sync::Arc;

    use arrow::datatypes::ToByteSlice;
    use arrow::datatypes::{DataType, Field, Schema, UInt32Type, UInt8Type};
    use arrow::error::Result as ArrowResult;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow::{array::*, buffer::Buffer};

    use crate::arrow::{ArrowReader, ParquetFileArrowReader};
    use crate::file::metadata::ParquetMetaData;
    use crate::file::{
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
        writer::InMemoryWriteableCursor,
    };

    #[test]
    fn arrow_writer() {
        // define schema
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn roundtrip_bytes() {
        // define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

        // build a record batch
        let expected_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let cursor = InMemoryWriteableCursor::default();

        {
            let mut writer = ArrowWriter::try_new(cursor.clone(), schema, None).unwrap();
            writer.write(&expected_batch).unwrap();
            writer.close().unwrap();
        }

        let buffer = cursor.into_inner().unwrap();

        let cursor = crate::file::serialized_reader::SliceableCursor::new(buffer);
        let reader = SerializedFileReader::new(cursor).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).data().clone();
            let actual_data = actual_batch.column(i).data().clone();

            assert_eq!(expected_data, actual_data);
        }
    }

    #[test]
    fn arrow_writer_non_null() {
        // define schema
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_list() {
        // define schema
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::List(Box::new(Field::new("item", DataType::Int32, false))),
            true,
        )]);

        // create some data
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data().clone())
        .null_bit_buffer(Buffer::from(vec![0b00011011]))
        .build()
        .unwrap();
        let a = ListArray::from(a_list_data);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        assert_eq!(batch.column(0).data().null_count(), 1);

        // This test fails if the max row group size is less than the batch's length
        // see https://github.com/apache/arrow-rs/issues/518
        roundtrip(batch, None);
    }

    #[test]
    fn arrow_writer_list_non_null() {
        // define schema
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::List(Box::new(Field::new("item", DataType::Int32, false))),
            false,
        )]);

        // create some data
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data().clone())
        .build()
        .unwrap();
        let a = ListArray::from(a_list_data);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        // This test fails if the max row group size is less than the batch's length
        // see https://github.com/apache/arrow-rs/issues/518
        assert_eq!(batch.column(0).data().null_count(), 0);

        roundtrip(batch, None);
    }

    #[test]
    fn arrow_writer_binary() {
        let string_field = Field::new("a", DataType::Utf8, false);
        let binary_field = Field::new("b", DataType::Binary, false);
        let schema = Schema::new(vec![string_field, binary_field]);

        let raw_string_values = vec!["foo", "bar", "baz", "quux"];
        let raw_binary_values = vec![
            b"foo".to_vec(),
            b"bar".to_vec(),
            b"baz".to_vec(),
            b"quux".to_vec(),
        ];
        let raw_binary_value_refs = raw_binary_values
            .iter()
            .map(|x| x.as_slice())
            .collect::<Vec<_>>();

        let string_values = StringArray::from(raw_string_values.clone());
        let binary_values = BinaryArray::from(raw_binary_value_refs);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(string_values), Arc::new(binary_values)],
        )
        .unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_decimal() {
        let decimal_field = Field::new("a", DataType::Decimal(5, 2), false);
        let schema = Schema::new(vec![decimal_field]);

        let mut dec_builder = DecimalBuilder::new(4, 5, 2);
        dec_builder.append_value(10_000).unwrap();
        dec_builder.append_value(50_000).unwrap();
        dec_builder.append_value(0).unwrap();
        dec_builder.append_value(-100).unwrap();

        let decimal_values = dec_builder.finish();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(decimal_values)])
                .unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_complex() {
        // define schema
        let struct_field_d = Field::new("d", DataType::Float64, true);
        let struct_field_f = Field::new("f", DataType::Float32, true);
        let struct_field_g = Field::new(
            "g",
            DataType::List(Box::new(Field::new("item", DataType::Int16, true))),
            false,
        );
        let struct_field_h = Field::new(
            "h",
            DataType::List(Box::new(Field::new("item", DataType::Int16, false))),
            true,
        );
        let struct_field_e = Field::new(
            "e",
            DataType::Struct(vec![
                struct_field_f.clone(),
                struct_field_g.clone(),
                struct_field_h.clone(),
            ]),
            false,
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new(
                "c",
                DataType::Struct(vec![struct_field_d.clone(), struct_field_e.clone()]),
                false,
            ),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let d = Float64Array::from(vec![None, None, None, Some(1.0), None]);
        let f = Float32Array::from(vec![Some(0.0), None, Some(333.3), None, Some(5.25)]);

        let g_value = Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
        let g_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets.clone())
            .add_child_data(g_value.data().clone())
            .build()
            .unwrap();
        let g = ListArray::from(g_list_data);
        // The difference between g and h is that h has a null bitmap
        let h_list_data = ArrayData::builder(struct_field_h.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets)
            .add_child_data(g_value.data().clone())
            .null_bit_buffer(Buffer::from(vec![0b00011011]))
            .build()
            .unwrap();
        let h = ListArray::from(h_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f) as ArrayRef),
            (struct_field_g, Arc::new(g) as ArrayRef),
            (struct_field_h, Arc::new(h) as ArrayRef),
        ]);

        let c = StructArray::from(vec![
            (struct_field_d, Arc::new(d) as ArrayRef),
            (struct_field_e, Arc::new(e) as ArrayRef),
        ]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        roundtrip(batch.clone(), Some(SMALL_SIZE / 2));
        roundtrip(batch, Some(SMALL_SIZE / 3));
    }

    #[test]
    fn arrow_writer_complex_mixed() {
        // This test was added while investigating https://github.com/apache/arrow-rs/issues/244.
        // It was subsequently fixed while investigating https://github.com/apache/arrow-rs/issues/245.

        // define schema
        let offset_field = Field::new("offset", DataType::Int32, false);
        let partition_field = Field::new("partition", DataType::Int64, true);
        let topic_field = Field::new("topic", DataType::Utf8, true);
        let schema = Schema::new(vec![Field::new(
            "some_nested_object",
            DataType::Struct(vec![
                offset_field.clone(),
                partition_field.clone(),
                topic_field.clone(),
            ]),
            false,
        )]);

        // create some data
        let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let partition = Int64Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let topic = StringArray::from(vec![Some("A"), None, Some("A"), Some(""), None]);

        let some_nested_object = StructArray::from(vec![
            (offset_field, Arc::new(offset) as ArrayRef),
            (partition_field, Arc::new(partition) as ArrayRef),
            (topic_field, Arc::new(topic) as ArrayRef),
        ]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)])
                .unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_map() {
        // Note: we are using the JSON Arrow reader for brevity
        let json_content = r#"
        {"stocks":{"long": "$AAA", "short": "$BBB"}}
        {"stocks":{"long": null, "long": "$CCC", "short": null}}
        {"stocks":{"hedged": "$YYY", "long": null, "short": "$D"}}
        "#;
        let entries_struct_type = DataType::Struct(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);
        let stocks_field = Field::new(
            "stocks",
            DataType::Map(
                Box::new(Field::new("entries", entries_struct_type, false)),
                false,
            ),
            true,
        );
        let schema = Arc::new(Schema::new(vec![stocks_field]));
        let builder = arrow::json::ReaderBuilder::new()
            .with_schema(schema)
            .with_batch_size(64);
        let mut reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

        let batch = reader.next().unwrap().unwrap();
        roundtrip(batch, None);
    }

    #[test]
    fn arrow_writer_2_level_struct() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Struct(vec![field_c]), true);
        let field_a = Field::new("a", DataType::Struct(vec![field_b.clone()]), true);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from(vec![Some(1), None, Some(3), None, None, Some(6)]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .null_bit_buffer(Buffer::from(vec![0b00100111]))
            .add_child_data(c.data().clone())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .null_bit_buffer(Buffer::from(vec![0b00101111]))
            .add_child_data(b.data().clone())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 1);
        assert_eq!(a.column(0).null_count(), 2);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_2_level_struct_non_null() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, false);
        let field_b = Field::new("b", DataType::Struct(vec![field_c]), false);
        let field_a = Field::new("a", DataType::Struct(vec![field_b.clone()]), false);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .add_child_data(c.data().clone())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .add_child_data(b.data().clone())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 0);
        assert_eq!(a.column(0).null_count(), 0);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_2_level_struct_mixed_null() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, false);
        let field_b = Field::new("b", DataType::Struct(vec![field_c]), true);
        let field_a = Field::new("a", DataType::Struct(vec![field_b.clone()]), false);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .null_bit_buffer(Buffer::from(vec![0b00100111]))
            .add_child_data(c.data().clone())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        // a intentionally has no null buffer, to test that this is handled correctly
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .add_child_data(b.data().clone())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 0);
        assert_eq!(a.column(0).null_count(), 2);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    const SMALL_SIZE: usize = 7;

    fn roundtrip(expected_batch: RecordBatch, max_row_group_size: Option<usize>) -> File {
        let file = tempfile::tempfile().unwrap();

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            expected_batch.schema(),
            max_row_group_size.map(|size| {
                WriterProperties::builder()
                    .set_max_row_group_size(size)
                    .build()
            }),
        )
        .expect("Unable to write file");
        writer.write(&expected_batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).data();
            let actual_data = actual_batch.column(i).data();

            assert_eq!(expected_data, actual_data);
        }

        file
    }

    fn one_column_roundtrip(
        values: ArrayRef,
        nullable: bool,
        max_row_group_size: Option<usize>,
    ) -> File {
        let schema = Schema::new(vec![Field::new(
            "col",
            values.data_type().clone(),
            nullable,
        )]);
        let expected_batch =
            RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

        roundtrip(expected_batch, max_row_group_size)
    }

    fn values_required<A, I>(iter: I)
    where
        A: From<Vec<I::Item>> + Array + 'static,
        I: IntoIterator,
    {
        let raw_values: Vec<_> = iter.into_iter().collect();
        let values = Arc::new(A::from(raw_values));
        one_column_roundtrip(values, false, Some(SMALL_SIZE / 2));
    }

    fn values_optional<A, I>(iter: I)
    where
        A: From<Vec<Option<I::Item>>> + Array + 'static,
        I: IntoIterator,
    {
        let optional_raw_values: Vec<_> = iter
            .into_iter()
            .enumerate()
            .map(|(i, v)| if i % 2 == 0 { None } else { Some(v) })
            .collect();
        let optional_values = Arc::new(A::from(optional_raw_values));
        one_column_roundtrip(optional_values, true, Some(SMALL_SIZE / 2));
    }

    fn required_and_optional<A, I>(iter: I)
    where
        A: From<Vec<I::Item>> + From<Vec<Option<I::Item>>> + Array + 'static,
        I: IntoIterator + Clone,
    {
        values_required::<A, I>(iter.clone());
        values_optional::<A, I>(iter);
    }

    #[test]
    fn all_null_primitive_single_column() {
        let values = Arc::new(Int32Array::from(vec![None; SMALL_SIZE]));
        one_column_roundtrip(values, true, Some(SMALL_SIZE / 2));
    }
    #[test]
    fn null_single_column() {
        let values = Arc::new(NullArray::new(SMALL_SIZE));
        one_column_roundtrip(values, true, Some(SMALL_SIZE / 2));
        // null arrays are always nullable, a test with non-nullable nulls fails
    }

    #[test]
    fn bool_single_column() {
        required_and_optional::<BooleanArray, _>(
            [true, false].iter().cycle().copied().take(SMALL_SIZE),
        );
    }

    #[test]
    fn bool_large_single_column() {
        let values = Arc::new(
            [None, Some(true), Some(false)]
                .iter()
                .cycle()
                .copied()
                .take(200_000)
                .collect::<BooleanArray>(),
        );
        let schema =
            Schema::new(vec![Field::new("col", values.data_type().clone(), true)]);
        let expected_batch =
            RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();
        let file = tempfile::tempfile().unwrap();

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            expected_batch.schema(),
            None,
        )
        .expect("Unable to write file");
        writer.write(&expected_batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn i8_single_column() {
        required_and_optional::<Int8Array, _>(0..SMALL_SIZE as i8);
    }

    #[test]
    fn i16_single_column() {
        required_and_optional::<Int16Array, _>(0..SMALL_SIZE as i16);
    }

    #[test]
    fn i32_single_column() {
        required_and_optional::<Int32Array, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn i64_single_column() {
        required_and_optional::<Int64Array, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    fn u8_single_column() {
        required_and_optional::<UInt8Array, _>(0..SMALL_SIZE as u8);
    }

    #[test]
    fn u16_single_column() {
        required_and_optional::<UInt16Array, _>(0..SMALL_SIZE as u16);
    }

    #[test]
    fn u32_single_column() {
        required_and_optional::<UInt32Array, _>(0..SMALL_SIZE as u32);
    }

    #[test]
    fn u64_single_column() {
        required_and_optional::<UInt64Array, _>(0..SMALL_SIZE as u64);
    }

    #[test]
    fn f32_single_column() {
        required_and_optional::<Float32Array, _>((0..SMALL_SIZE).map(|i| i as f32));
    }

    #[test]
    fn f64_single_column() {
        required_and_optional::<Float64Array, _>((0..SMALL_SIZE).map(|i| i as f64));
    }

    // The timestamp array types don't implement From<Vec<T>> because they need the timezone
    // argument, and they also doesn't support building from a Vec<Option<T>>, so call
    // one_column_roundtrip manually instead of calling required_and_optional for these tests.

    #[test]
    fn timestamp_second_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampSecondArray::from_vec(raw_values, None));

        one_column_roundtrip(values, false, Some(3));
    }

    #[test]
    fn timestamp_millisecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampMillisecondArray::from_vec(raw_values, None));

        one_column_roundtrip(values, false, Some(SMALL_SIZE / 2 + 1));
    }

    #[test]
    fn timestamp_microsecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampMicrosecondArray::from_vec(raw_values, None));

        one_column_roundtrip(values, false, Some(SMALL_SIZE / 2 + 2));
    }

    #[test]
    fn timestamp_nanosecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampNanosecondArray::from_vec(raw_values, None));

        one_column_roundtrip(values, false, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn date32_single_column() {
        required_and_optional::<Date32Array, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn date64_single_column() {
        // Date64 must be a multiple of 86400000, see ARROW-10925
        required_and_optional::<Date64Array, _>(
            (0..(SMALL_SIZE as i64 * 86400000)).step_by(86400000),
        );
    }

    #[test]
    fn time32_second_single_column() {
        required_and_optional::<Time32SecondArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn time32_millisecond_single_column() {
        required_and_optional::<Time32MillisecondArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn time64_microsecond_single_column() {
        required_and_optional::<Time64MicrosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    fn time64_nanosecond_single_column() {
        required_and_optional::<Time64NanosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_second_single_column() {
        required_and_optional::<DurationSecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_millisecond_single_column() {
        required_and_optional::<DurationMillisecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_microsecond_single_column() {
        required_and_optional::<DurationMicrosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_nanosecond_single_column() {
        required_and_optional::<DurationNanosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    fn interval_year_month_single_column() {
        required_and_optional::<IntervalYearMonthArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn interval_day_time_single_column() {
        required_and_optional::<IntervalDayTimeArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(
        expected = "Attempting to write an Arrow interval type MonthDayNano to parquet that is not yet implemented"
    )]
    fn interval_month_day_nano_single_column() {
        required_and_optional::<IntervalMonthDayNanoArray, _>(0..SMALL_SIZE as i128);
    }

    #[test]
    fn binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<BinaryArray, _>(many_vecs_iter);
    }

    #[test]
    fn large_binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // LargeBinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<LargeBinaryArray, _>(many_vecs_iter);
    }

    #[test]
    fn fixed_size_binary_single_column() {
        let mut builder = FixedSizeBinaryBuilder::new(16, 4);
        builder.append_value(b"0123").unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"8910").unwrap();
        builder.append_value(b"1112").unwrap();
        let array = Arc::new(builder.finish());

        one_column_roundtrip(array, true, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<StringArray, _>(raw_strs);
    }

    #[test]
    fn large_string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<LargeStringArray, _>(raw_strs);
    }

    #[test]
    fn list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .null_bit_buffer(Buffer::from(vec![0b00011011]))
        .add_child_data(a_values.data().clone())
        .build()
        .unwrap();

        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);
        let values = Arc::new(a);

        one_column_roundtrip(values, true, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn large_list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0i64, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::LargeList(Box::new(Field::new(
            "large_item",
            DataType::Int32,
            true,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data().clone())
        .null_bit_buffer(Buffer::from(vec![0b00011011]))
        .build()
        .unwrap();

        // I think this setup is incorrect because this should pass
        assert_eq!(a_list_data.null_count(), 1);

        let a = LargeListArray::from(a_list_data);
        let values = Arc::new(a);

        one_column_roundtrip(values, true, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn struct_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let struct_field_a = Field::new("f", DataType::Int32, false);
        let s = StructArray::from(vec![(struct_field_a, Arc::new(a_values) as ArrayRef)]);

        let values = Arc::new(s);
        one_column_roundtrip(values, false, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_string_dictionary() {
        // define schema
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            42,
            true,
        )]));

        // create some data
        let d: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();

        // build a record batch
        let expected_batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();

        roundtrip(expected_batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_primitive_dictionary() {
        // define schema
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
            true,
            42,
            true,
        )]));

        // create some data
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
        builder.append(22345678).unwrap();
        builder.append(12345678).unwrap();
        let d = builder.finish();

        // build a record batch
        let expected_batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();

        roundtrip(expected_batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_string_dictionary_unsigned_index() {
        // define schema
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
            42,
            true,
        )]));

        // create some data
        let d: UInt8DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();

        // build a record batch
        let expected_batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();

        roundtrip(expected_batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn u32_min_max() {
        // check values roundtrip through parquet
        let values = Arc::new(UInt32Array::from_iter_values(vec![
            u32::MIN,
            u32::MIN + 1,
            (i32::MAX as u32) - 1,
            i32::MAX as u32,
            (i32::MAX as u32) + 1,
            u32::MAX - 1,
            u32::MAX,
        ]));
        let file = one_column_roundtrip(values, false, None);

        // check statistics are valid
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        assert_eq!(row_group.num_columns(), 1);
        let column = row_group.column(0);
        let stats = column.statistics().unwrap();
        assert!(stats.has_min_max_set());
        if let Statistics::Int32(stats) = stats {
            assert_eq!(*stats.min() as u32, u32::MIN);
            assert_eq!(*stats.max() as u32, u32::MAX);
        } else {
            panic!("Statistics::Int32 missing")
        }
    }

    #[test]
    fn u64_min_max() {
        // check values roundtrip through parquet
        let values = Arc::new(UInt64Array::from_iter_values(vec![
            u64::MIN,
            u64::MIN + 1,
            (i64::MAX as u64) - 1,
            i64::MAX as u64,
            (i64::MAX as u64) + 1,
            u64::MAX - 1,
            u64::MAX,
        ]));
        let file = one_column_roundtrip(values, false, None);

        // check statistics are valid
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        assert_eq!(row_group.num_columns(), 1);
        let column = row_group.column(0);
        let stats = column.statistics().unwrap();
        assert!(stats.has_min_max_set());
        if let Statistics::Int64(stats) = stats {
            assert_eq!(*stats.min() as u64, u64::MIN);
            assert_eq!(*stats.max() as u64, u64::MAX);
        } else {
            panic!("Statistics::Int64 missing")
        }
    }

    #[test]
    fn statistics_null_counts_only_nulls() {
        // check that null-count statistics for "only NULL"-columns are correct
        let values = Arc::new(UInt64Array::from(vec![None, None]));
        let file = one_column_roundtrip(values, true, None);

        // check statistics are valid
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        assert_eq!(row_group.num_columns(), 1);
        let column = row_group.column(0);
        let stats = column.statistics().unwrap();
        assert_eq!(stats.null_count(), 2);
    }

    #[test]
    fn test_list_of_struct_roundtrip() {
        // define schema
        let int_field = Field::new("a", DataType::Int32, true);
        let int_field2 = Field::new("b", DataType::Int32, true);

        let int_builder = Int32Builder::new(10);
        let int_builder2 = Int32Builder::new(10);

        let struct_builder = StructBuilder::new(
            vec![int_field, int_field2],
            vec![Box::new(int_builder), Box::new(int_builder2)],
        );
        let mut list_builder = ListBuilder::new(struct_builder);

        // Construct the following array
        // [{a: 1, b: 2}], [], null, [null, null], [{a: null, b: 3}], [{a: 2, b: null}]

        // [{a: 1, b: 2}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1)
            .unwrap();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(2)
            .unwrap();
        values.append(true).unwrap();
        list_builder.append(true).unwrap();

        // []
        list_builder.append(true).unwrap();

        // null
        list_builder.append(false).unwrap();

        // [null, null]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null()
            .unwrap();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null()
            .unwrap();
        values.append(false).unwrap();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null()
            .unwrap();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null()
            .unwrap();
        values.append(false).unwrap();
        list_builder.append(true).unwrap();

        // [{a: null, b: 3}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null()
            .unwrap();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(3)
            .unwrap();
        values.append(true).unwrap();
        list_builder.append(true).unwrap();

        // [{a: 2, b: null}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(2)
            .unwrap();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null()
            .unwrap();
        values.append(true).unwrap();
        list_builder.append(true).unwrap();

        let array = Arc::new(list_builder.finish());

        one_column_roundtrip(array, true, Some(10));
    }

    fn row_group_sizes(metadata: &ParquetMetaData) -> Vec<i64> {
        metadata.row_groups().iter().map(|x| x.num_rows()).collect()
    }

    #[test]
    fn test_aggregates_records() {
        let arrays = [
            Int32Array::from((0..100).collect::<Vec<_>>()),
            Int32Array::from((0..50).collect::<Vec<_>>()),
            Int32Array::from((200..500).collect::<Vec<_>>()),
        ];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "int",
            ArrowDataType::Int32,
            false,
        )]));

        let file = tempfile::tempfile().unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();

        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), Some(props))
                .unwrap();

        for array in arrays {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
        }

        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        assert_eq!(&row_group_sizes(reader.metadata()), &[200, 200, 50]);

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let batches = arrow_reader
            .get_record_reader(100)
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(batches.len(), 5);
        assert!(batches.iter().all(|x| x.num_columns() == 1));

        let batch_sizes: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();

        assert_eq!(&batch_sizes, &[100, 100, 100, 100, 50]);

        let values: Vec<_> = batches
            .iter()
            .flat_map(|x| {
                x.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .cloned()
            })
            .collect();

        let expected_values: Vec<_> =
            [0..100, 0..50, 200..500].into_iter().flatten().collect();
        assert_eq!(&values, &expected_values)
    }

    #[test]
    fn complex_aggregate() {
        // Tests aggregating nested data
        let field_a = Field::new("leaf_a", DataType::Int32, false);
        let field_b = Field::new("leaf_b", DataType::Int32, true);
        let struct_a = Field::new(
            "struct_a",
            DataType::Struct(vec![field_a.clone(), field_b.clone()]),
            true,
        );

        let list_a = Field::new("list", DataType::List(Box::new(struct_a)), true);
        let struct_b =
            Field::new("struct_b", DataType::Struct(vec![list_a.clone()]), false);

        let schema = Arc::new(Schema::new(vec![struct_b]));

        // create nested data
        let field_a_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let field_b_array =
            Int32Array::from_iter(vec![Some(1), None, Some(2), None, None, Some(6)]);

        let struct_a_array = StructArray::from(vec![
            (field_a.clone(), Arc::new(field_a_array) as ArrayRef),
            (field_b.clone(), Arc::new(field_b_array) as ArrayRef),
        ]);

        let list_data = ArrayDataBuilder::new(list_a.data_type().clone())
            .len(5)
            .add_buffer(Buffer::from_iter(vec![
                0_i32, 1_i32, 1_i32, 3_i32, 3_i32, 5_i32,
            ]))
            .null_bit_buffer(Buffer::from_iter(vec![true, false, true, false, true]))
            .child_data(vec![struct_a_array.data().clone()])
            .build()
            .unwrap();

        let list_a_array = Arc::new(ListArray::from(list_data)) as ArrayRef;
        let struct_b_array = StructArray::from(vec![(list_a.clone(), list_a_array)]);

        let batch1 = RecordBatch::try_from_iter(vec![(
            "struct_b",
            Arc::new(struct_b_array) as ArrayRef,
        )])
        .unwrap();

        let field_a_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let field_b_array = Int32Array::from_iter(vec![None, None, None, Some(1), None]);

        let struct_a_array = StructArray::from(vec![
            (field_a, Arc::new(field_a_array) as ArrayRef),
            (field_b, Arc::new(field_b_array) as ArrayRef),
        ]);

        let list_data = ArrayDataBuilder::new(list_a.data_type().clone())
            .len(2)
            .add_buffer(Buffer::from_iter(vec![0_i32, 4_i32, 5_i32]))
            .child_data(vec![struct_a_array.data().clone()])
            .build()
            .unwrap();

        let list_a_array = Arc::new(ListArray::from(list_data)) as ArrayRef;
        let struct_b_array = StructArray::from(vec![(list_a, list_a_array)]);

        let batch2 = RecordBatch::try_from_iter(vec![(
            "struct_b",
            Arc::new(struct_b_array) as ArrayRef,
        )])
        .unwrap();

        let batches = &[batch1, batch2];

        // Verify data is as expected

        let expected = r#"
            +-------------------------------------------------------------------------------------------------------------------------------------+
            | struct_b                                                                                                                            |
            +-------------------------------------------------------------------------------------------------------------------------------------+
            | {"list": [{"leaf_a": 1, "leaf_b": 1}]}                                                                                              |
            | {"list": null}                                                                                                                      |
            | {"list": [{"leaf_a": 2, "leaf_b": null}, {"leaf_a": 3, "leaf_b": 2}]}                                                               |
            | {"list": null}                                                                                                                      |
            | {"list": [{"leaf_a": 4, "leaf_b": null}, {"leaf_a": 5, "leaf_b": null}]}                                                            |
            | {"list": [{"leaf_a": 6, "leaf_b": null}, {"leaf_a": 7, "leaf_b": null}, {"leaf_a": 8, "leaf_b": null}, {"leaf_a": 9, "leaf_b": 1}]} |
            | {"list": [{"leaf_a": 10, "leaf_b": null}]}                                                                                          |
            +-------------------------------------------------------------------------------------------------------------------------------------+
        "#.trim().split('\n').map(|x| x.trim()).collect::<Vec<_>>().join("\n");

        let actual = pretty_format_batches(batches).unwrap().to_string();
        assert_eq!(actual, expected);

        // Write data
        let file = tempfile::tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(6)
            .build();

        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(props)).unwrap();

        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        // Read Data
        let reader = SerializedFileReader::new(file).unwrap();

        // Should have written entire first batch and first row of second to the first row group
        // leaving a single row in the second row group
        assert_eq!(&row_group_sizes(reader.metadata()), &[6, 1]);

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let batches = arrow_reader
            .get_record_reader(2)
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(batches.len(), 4);
        let batch_counts: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();
        assert_eq!(&batch_counts, &[2, 2, 2, 1]);

        let actual = pretty_format_batches(&batches).unwrap().to_string();
        assert_eq!(actual, expected);
    }
}
