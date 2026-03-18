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
use crate::arrow::record_reader::RecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::Type as PhysicalType;
use crate::column::page::PageIterator;
use crate::data_type::{DataType, Int96};
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date64Array, Decimal64Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, PrimitiveArray,
    UInt8Array, UInt16Array, builder::PrimitiveDictionaryBuilder, cast::AsArray, downcast_integer,
    types::*,
};
use arrow_array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer, i256};
use arrow_schema::{DataType as ArrowType, TimeUnit};
use std::any::Any;
use std::sync::Arc;

/// Provides conversion from `Vec<T>` to `Buffer`
pub trait IntoBuffer {
    fn into_buffer(self, target_type: &ArrowType) -> Buffer;
}

macro_rules! native_buffer {
    ($($t:ty),*) => {
        $(impl IntoBuffer for Vec<$t> {
            fn into_buffer(self, _target_type: &ArrowType) -> Buffer {
                Buffer::from_vec(self)
            }
        })*
    };
}
native_buffer!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

impl IntoBuffer for Vec<bool> {
    fn into_buffer(self, _target_type: &ArrowType) -> Buffer {
        BooleanBuffer::from_iter(self).into_inner()
    }
}

impl IntoBuffer for Vec<Int96> {
    fn into_buffer(self, target_type: &ArrowType) -> Buffer {
        let mut builder = Vec::with_capacity(self.len());
        match target_type {
            ArrowType::Timestamp(TimeUnit::Second, _) => {
                builder.extend(self.iter().map(|x| x.to_seconds()));
            }
            ArrowType::Timestamp(TimeUnit::Millisecond, _) => {
                builder.extend(self.iter().map(|x| x.to_millis()));
            }
            ArrowType::Timestamp(TimeUnit::Microsecond, _) => {
                builder.extend(self.iter().map(|x| x.to_micros()));
            }
            ArrowType::Timestamp(TimeUnit::Nanosecond, _) => {
                builder.extend(self.iter().map(|x| x.to_nanos()));
            }
            _ => unreachable!("Invalid target_type for Int96."),
        }
        Buffer::from_vec(builder)
    }
}

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct PrimitiveArrayReader<T>
where
    T: DataType,
    T::T: Copy + Default,
    Vec<T::T>: IntoBuffer,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: RecordReader<T>,
}

impl<T> PrimitiveArrayReader<T>
where
    T: DataType,
    T::T: Copy + Default,
    Vec<T::T>: IntoBuffer,
{
    /// Construct primitive array reader.
    pub fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        arrow_type: Option<ArrowType>,
    ) -> Result<Self> {
        // Check if Arrow type is specified, else create it from Parquet type
        let data_type = match arrow_type {
            Some(t) => t,
            None => parquet_to_arrow_field(column_desc.as_ref())?
                .data_type()
                .clone(),
        };

        let record_reader = RecordReader::<T>::new(column_desc);

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            record_reader,
        })
    }
}

/// Implementation of primitive array reader.
impl<T> ArrayReader for PrimitiveArrayReader<T>
where
    T: DataType,
    T::T: Copy + Default,
    Vec<T::T>: IntoBuffer,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type of primitive array.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let target_type = &self.data_type;

        // Convert physical data to equivalent arrow type, and then perform
        // coercion as needed
        let record_data = self
            .record_reader
            .consume_record_data()
            .into_buffer(target_type);

        let len = self.record_reader.num_values();
        let nulls = self
            .record_reader
            .consume_bitmap_buffer()
            .and_then(|b| NullBuffer::from_unsliced_buffer(b, len));

        let array: ArrayRef = match T::get_physical_type() {
            PhysicalType::BOOLEAN => Arc::new(BooleanArray::new(
                BooleanBuffer::new(record_data, 0, len),
                nulls,
            )),
            PhysicalType::INT32 => Arc::new(Int32Array::new(
                ScalarBuffer::new(record_data, 0, len),
                nulls,
            )),
            PhysicalType::INT64 => Arc::new(Int64Array::new(
                ScalarBuffer::new(record_data, 0, len),
                nulls,
            )),
            PhysicalType::FLOAT => Arc::new(Float32Array::new(
                ScalarBuffer::new(record_data, 0, len),
                nulls,
            )),
            PhysicalType::DOUBLE => Arc::new(Float64Array::new(
                ScalarBuffer::new(record_data, 0, len),
                nulls,
            )),
            PhysicalType::INT96 => Arc::new(Int64Array::new(
                ScalarBuffer::new(record_data, 0, len),
                nulls,
            )),
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                unreachable!("PrimitiveArrayReaders don't support complex physical types");
            }
        };

        // Coerce the arrow type to the desired array type
        let array = coerce_array(array, target_type)?;

        // save definition and repetition buffers
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

/// Coerce the parquet physical type array to the target type
///
/// This should match the logic in schema::primitive::apply_hint
fn coerce_array(array: ArrayRef, target_type: &ArrowType) -> Result<ArrayRef> {
    if let ArrowType::Dictionary(key_type, value_type) = target_type {
        let dictionary = pack_dictionary(key_type, array.as_ref())?;
        let any_dictionary = dictionary.as_any_dictionary();

        let coerced_values =
            coerce_array(Arc::clone(any_dictionary.values()), value_type.as_ref())?;

        return Ok(any_dictionary.with_values(coerced_values));
    }

    match array.data_type() {
        ArrowType::Int32 => coerce_i32(array.as_primitive(), target_type),
        ArrowType::Int64 => coerce_i64(array.as_primitive(), target_type),
        ArrowType::Boolean | ArrowType::Float32 | ArrowType::Float64 => Ok(array),
        _ => unreachable!("Cannot coerce array of type {}", array.data_type()),
    }
}

fn coerce_i32(array: &Int32Array, target_type: &ArrowType) -> Result<ArrayRef> {
    Ok(match target_type {
        ArrowType::UInt8 => {
            let array = array.unary(|i| i as u8) as UInt8Array;
            Arc::new(array) as ArrayRef
        }
        ArrowType::Int8 => {
            let array = array.unary(|i| i as i8) as Int8Array;
            Arc::new(array) as ArrayRef
        }
        ArrowType::UInt16 => {
            let array = array.unary(|i| i as u16) as UInt16Array;
            Arc::new(array) as ArrayRef
        }
        ArrowType::Int16 => {
            let array = array.unary(|i| i as i16) as Int16Array;
            Arc::new(array) as ArrayRef
        }
        ArrowType::Int32 => Arc::new(array.clone()),
        // follow C++ implementation and use overflow/reinterpret cast from  i32 to u32 which will map
        // `i32::MIN..0` to `(i32::MAX as u32)..u32::MAX`
        ArrowType::UInt32 => Arc::new(UInt32Array::new(
            array.values().inner().clone().into(),
            array.nulls().cloned(),
        )) as ArrayRef,
        ArrowType::Date32 => Arc::new(array.reinterpret_cast::<Date32Type>()) as _,
        ArrowType::Date64 => {
            let array: Date64Array = array.unary(|x| x as i64 * 86_400_000);
            Arc::new(array) as ArrayRef
        }
        ArrowType::Time32(TimeUnit::Second) => {
            Arc::new(array.reinterpret_cast::<Time32SecondType>()) as ArrayRef
        }
        ArrowType::Time32(TimeUnit::Millisecond) => {
            Arc::new(array.reinterpret_cast::<Time32MillisecondType>()) as ArrayRef
        }
        ArrowType::Timestamp(time_unit, timezone) => match time_unit {
            TimeUnit::Second => {
                let array: TimestampSecondArray = array
                    .unary(|x| x as i64)
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
            TimeUnit::Millisecond => {
                let array: TimestampMillisecondArray = array
                    .unary(|x| x as i64)
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
            TimeUnit::Microsecond => {
                let array: TimestampMicrosecondArray = array
                    .unary(|x| x as i64)
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
            TimeUnit::Nanosecond => {
                let array: TimestampNanosecondArray = array
                    .unary(|x| x as i64)
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
        },
        ArrowType::Decimal32(p, s) => {
            let array = array
                .reinterpret_cast::<Decimal32Type>()
                .with_precision_and_scale(*p, *s)?;
            Arc::new(array) as ArrayRef
        }
        ArrowType::Decimal64(p, s) => {
            let array: Decimal64Array =
                array.unary(|i| i as i64).with_precision_and_scale(*p, *s)?;
            Arc::new(array) as ArrayRef
        }
        ArrowType::Decimal128(p, s) => {
            let array: Decimal128Array = array
                .unary(|i| i as i128)
                .with_precision_and_scale(*p, *s)?;
            Arc::new(array) as ArrayRef
        }
        ArrowType::Decimal256(p, s) => {
            let array: Decimal256Array = array
                .unary(|i| i256::from_i128(i as i128))
                .with_precision_and_scale(*p, *s)?;
            Arc::new(array) as ArrayRef
        }
        _ => unreachable!("Cannot coerce i32 to {target_type}"),
    })
}

fn coerce_i64(array: &Int64Array, target_type: &ArrowType) -> Result<ArrayRef> {
    Ok(match target_type {
        ArrowType::Int64 => Arc::new(array.clone()) as _,
        // follow C++ implementation and use overflow/reinterpret cast from i64 to u64 which will map
        // `i64::MIN..0` to `(i64::MAX as u64)..u64::MAX`
        ArrowType::UInt64 => Arc::new(UInt64Array::new(
            array.values().inner().clone().into(),
            array.nulls().cloned(),
        )) as ArrayRef,
        ArrowType::Date64 => Arc::new(array.reinterpret_cast::<Date64Type>()) as _,
        ArrowType::Time64(TimeUnit::Microsecond) => {
            Arc::new(array.reinterpret_cast::<Time64MicrosecondType>()) as _
        }
        ArrowType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(array.reinterpret_cast::<Time64NanosecondType>()) as _
        }
        ArrowType::Duration(unit) => match unit {
            TimeUnit::Second => Arc::new(array.reinterpret_cast::<DurationSecondType>()) as _,
            TimeUnit::Millisecond => {
                Arc::new(array.reinterpret_cast::<DurationMillisecondType>()) as _
            }
            TimeUnit::Microsecond => {
                Arc::new(array.reinterpret_cast::<DurationMicrosecondType>()) as _
            }
            TimeUnit::Nanosecond => {
                Arc::new(array.reinterpret_cast::<DurationNanosecondType>()) as _
            }
        },
        ArrowType::Timestamp(time_unit, timezone) => match time_unit {
            TimeUnit::Second => {
                let array = array
                    .reinterpret_cast::<TimestampSecondType>()
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
            TimeUnit::Millisecond => {
                let array = array
                    .reinterpret_cast::<TimestampMillisecondType>()
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
            TimeUnit::Microsecond => {
                let array = array
                    .reinterpret_cast::<TimestampMicrosecondType>()
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
            TimeUnit::Nanosecond => {
                let array = array
                    .reinterpret_cast::<TimestampNanosecondType>()
                    .with_timezone_opt(timezone.clone());
                Arc::new(array) as _
            }
        },
        ArrowType::Decimal64(p, s) => {
            let array = array
                .reinterpret_cast::<Decimal64Type>()
                .with_precision_and_scale(*p, *s)?;
            Arc::new(array) as _
        }
        ArrowType::Decimal128(p, s) => {
            let array: Decimal128Array = array
                .unary(|i| i as i128)
                .with_precision_and_scale(*p, *s)?;
            Arc::new(array) as _
        }
        ArrowType::Decimal256(p, s) => {
            let array: Decimal256Array = array
                .unary(|i| i256::from_i128(i as i128))
                .with_precision_and_scale(*p, *s)?;
            Arc::new(array) as _
        }
        _ => unreachable!("Cannot coerce i64 to {target_type}"),
    })
}

macro_rules! pack_dictionary_helper {
    ($t:ty, $values:ident) => {
        match $values.data_type() {
            ArrowType::Int32 => pack_dictionary_impl::<$t, Int32Type>($values.as_primitive()),
            ArrowType::Int64 => pack_dictionary_impl::<$t, Int64Type>($values.as_primitive()),
            ArrowType::Float32 => pack_dictionary_impl::<$t, Float32Type>($values.as_primitive()),
            ArrowType::Float64 => pack_dictionary_impl::<$t, Float64Type>($values.as_primitive()),
            _ => unreachable!("Invalid physical type"),
        }
    };
}

fn pack_dictionary(key: &ArrowType, values: &dyn Array) -> Result<ArrayRef> {
    downcast_integer! {
        key => (pack_dictionary_helper, values),
        _ => unreachable!("Invalid key type"),
    }
}

fn pack_dictionary_impl<K: ArrowDictionaryKeyType, V: ArrowPrimitiveType>(
    values: &PrimitiveArray<V>,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveDictionaryBuilder::<K, V>::with_capacity(1024, values.len());
    builder.extend(values);
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::test_util::EmptyPageIterator;
    use crate::basic::Encoding;
    use crate::column::page::Page;
    use crate::data_type::{Int32Type, Int64Type};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use crate::util::InMemoryPageIterator;
    use crate::util::test_common::rand_gen::make_pages;
    use arrow::datatypes::ArrowPrimitiveType;
    use arrow_array::{Array, Date32Array, PrimitiveArray};

    use arrow::datatypes::DataType::{Date32, Decimal128};
    use rand::distr::uniform::SampleUniform;
    use std::collections::VecDeque;

    #[allow(clippy::too_many_arguments)]
    fn make_column_chunks<T: DataType>(
        column_desc: ColumnDescPtr,
        encoding: Encoding,
        num_levels: usize,
        min_value: T::T,
        max_value: T::T,
        def_levels: &mut Vec<i16>,
        rep_levels: &mut Vec<i16>,
        values: &mut Vec<T::T>,
        page_lists: &mut Vec<Vec<Page>>,
        use_v2: bool,
        num_chunks: usize,
    ) where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        for _i in 0..num_chunks {
            let mut pages = VecDeque::new();
            let mut data = Vec::new();
            let mut page_def_levels = Vec::new();
            let mut page_rep_levels = Vec::new();

            make_pages::<T>(
                column_desc.clone(),
                encoding,
                1,
                num_levels,
                min_value,
                max_value,
                &mut page_def_levels,
                &mut page_rep_levels,
                &mut data,
                &mut pages,
                use_v2,
            );

            def_levels.append(&mut page_def_levels);
            rep_levels.append(&mut page_rep_levels);
            values.append(&mut data);
            page_lists.push(Vec::from(pages));
        }
    }

    #[test]
    fn test_primitive_array_reader_empty_pages() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let mut array_reader = PrimitiveArrayReader::<Int32Type>::new(
            Box::<EmptyPageIterator>::default(),
            schema.column(0),
            None,
        )
        .unwrap();

        // expect no values to be read
        let array = array_reader.next_batch(50).unwrap();
        assert!(array.is_empty());
    }

    #[test]
    fn test_primitive_array_reader_data() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator = InMemoryPageIterator::new(page_lists);

            let mut array_reader =
                PrimitiveArrayReader::<Int32Type>::new(Box::new(page_iterator), column_desc, None)
                    .unwrap();

            // Read first 50 values, which are all from the first column chunk
            let array = array_reader.next_batch(50).unwrap();
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

            assert_eq!(&Int32Array::from(data[0..50].to_vec()), array);

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

            assert_eq!(&Int32Array::from(data[50..150].to_vec()), array);

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

            assert_eq!(&Int32Array::from(data[150..200].to_vec()), array);
        }
    }

    macro_rules! test_primitive_array_reader_one_type {
        (
            $arrow_parquet_type:ty,
            $physical_type:expr,
            $converted_type_str:expr,
            $result_arrow_type:ty,
            $result_arrow_cast_type:ty,
            $result_primitive_type:ty
            $(, $timezone:expr)?
        ) => {{
            let message_type = format!(
                "
            message test_schema {{
              REQUIRED {:?} leaf ({});
          }}
            ",
                $physical_type, $converted_type_str
            );
            let schema = parse_message_type(&message_type)
                .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
                .unwrap();

            let column_desc = schema.column(0);

            // Construct page iterator
            {
                let mut data = Vec::new();
                let mut page_lists = Vec::new();
                make_column_chunks::<$arrow_parquet_type>(
                    column_desc.clone(),
                    Encoding::PLAIN,
                    100,
                    1,
                    200,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &mut data,
                    &mut page_lists,
                    true,
                    2,
                );
                let page_iterator = InMemoryPageIterator::new(page_lists);
                let mut array_reader = PrimitiveArrayReader::<$arrow_parquet_type>::new(
                    Box::new(page_iterator),
                    column_desc.clone(),
                    None,
                )
                .expect("Unable to get array reader");

                let array = array_reader
                    .next_batch(50)
                    .expect("Unable to get batch from reader");

                let result_data_type = <$result_arrow_type>::DATA_TYPE;
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$result_arrow_type>>()
                    .expect(
                        format!(
                            "Unable to downcast {:?} to {:?}",
                            array.data_type(),
                            result_data_type
                        )
                        .as_str(),
                    )
                    $(.clone().with_timezone($timezone))?
                    ;

                // create expected array as primitive, and cast to result type
                let expected = PrimitiveArray::<$result_arrow_cast_type>::from(
                    data[0..50]
                        .iter()
                        .map(|x| *x as $result_primitive_type)
                        .collect::<Vec<$result_primitive_type>>(),
                );
                let expected = Arc::new(expected) as ArrayRef;
                let expected = arrow::compute::cast(&expected, &result_data_type)
                    .expect("Unable to cast expected array");
                assert_eq!(expected.data_type(), &result_data_type);
                let expected = expected
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$result_arrow_type>>()
                    .expect(
                        format!(
                            "Unable to downcast expected {:?} to {:?}",
                            expected.data_type(),
                            result_data_type
                        )
                        .as_str(),
                    )
                    $(.clone().with_timezone($timezone))?
                    ;
                assert_eq!(expected, array);
            }
        }};
    }

    #[test]
    fn test_primitive_array_reader_temporal_types() {
        test_primitive_array_reader_one_type!(
            crate::data_type::Int32Type,
            PhysicalType::INT32,
            "DATE",
            arrow::datatypes::Date32Type,
            arrow::datatypes::Int32Type,
            i32
        );
        test_primitive_array_reader_one_type!(
            crate::data_type::Int32Type,
            PhysicalType::INT32,
            "TIME_MILLIS",
            arrow::datatypes::Time32MillisecondType,
            arrow::datatypes::Int32Type,
            i32
        );
        test_primitive_array_reader_one_type!(
            crate::data_type::Int64Type,
            PhysicalType::INT64,
            "TIME_MICROS",
            arrow::datatypes::Time64MicrosecondType,
            arrow::datatypes::Int64Type,
            i64
        );
        test_primitive_array_reader_one_type!(
            crate::data_type::Int64Type,
            PhysicalType::INT64,
            "TIMESTAMP_MILLIS",
            arrow::datatypes::TimestampMillisecondType,
            arrow::datatypes::Int64Type,
            i64,
            "UTC"
        );
        test_primitive_array_reader_one_type!(
            crate::data_type::Int64Type,
            PhysicalType::INT64,
            "TIMESTAMP_MICROS",
            arrow::datatypes::TimestampMicrosecondType,
            arrow::datatypes::Int64Type,
            i64,
            "UTC"
        );
    }

    #[test]
    fn test_primitive_array_reader_def_and_rep_levels() {
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL INT32 leaf;
            }
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut def_levels,
                &mut rep_levels,
                &mut Vec::new(),
                &mut page_lists,
                true,
                2,
            );

            let page_iterator = InMemoryPageIterator::new(page_lists);

            let mut array_reader =
                PrimitiveArrayReader::<Int32Type>::new(Box::new(page_iterator), column_desc, None)
                    .unwrap();

            let mut accu_len: usize = 0;

            // Read first 50 values, which are all from the first column chunk
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
        }
    }

    #[test]
    fn test_primitive_array_reader_decimal_types() {
        // parquet `INT32` to decimal
        let message_type = "
            message test_schema {
                REQUIRED INT32 decimal1 (DECIMAL(8,2));
        }
        ";
        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);

        // create the array reader
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                -99999999,
                99999999,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator = InMemoryPageIterator::new(page_lists);

            let mut array_reader =
                PrimitiveArrayReader::<Int32Type>::new(Box::new(page_iterator), column_desc, None)
                    .unwrap();

            // read data from the reader
            // the data type is decimal(8,2)
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(array.data_type(), &Decimal128(8, 2));
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let data_decimal_array = data[0..50]
                .iter()
                .copied()
                .map(|v| Some(v as i128))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(8, 2)
                .unwrap();
            assert_eq!(array, &data_decimal_array);

            // not equal with different data type(precision and scale)
            let data_decimal_array = data[0..50]
                .iter()
                .copied()
                .map(|v| Some(v as i128))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(9, 0)
                .unwrap();
            assert_ne!(array, &data_decimal_array)
        }

        // parquet `INT64` to decimal
        let message_type = "
            message test_schema {
                REQUIRED INT64 decimal1 (DECIMAL(18,4));
        }
        ";
        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);

        // create the array reader
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int64Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                -999999999999999999,
                999999999999999999,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator = InMemoryPageIterator::new(page_lists);

            let mut array_reader =
                PrimitiveArrayReader::<Int64Type>::new(Box::new(page_iterator), column_desc, None)
                    .unwrap();

            // read data from the reader
            // the data type is decimal(18,4)
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(array.data_type(), &Decimal128(18, 4));
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let data_decimal_array = data[0..50]
                .iter()
                .copied()
                .map(|v| Some(v as i128))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(18, 4)
                .unwrap();
            assert_eq!(array, &data_decimal_array);

            // not equal with different data type(precision and scale)
            let data_decimal_array = data[0..50]
                .iter()
                .copied()
                .map(|v| Some(v as i128))
                .collect::<Decimal128Array>()
                .with_precision_and_scale(34, 0)
                .unwrap();
            assert_ne!(array, &data_decimal_array)
        }
    }

    #[test]
    fn test_primitive_array_reader_date32_type() {
        // parquet `INT32` to date
        let message_type = "
            message test_schema {
                REQUIRED INT32 date1 (DATE);
        }
        ";
        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);

        // create the array reader
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                -99999999,
                99999999,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator = InMemoryPageIterator::new(page_lists);

            let mut array_reader =
                PrimitiveArrayReader::<Int32Type>::new(Box::new(page_iterator), column_desc, None)
                    .unwrap();

            // read data from the reader
            // the data type is date
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(array.data_type(), &Date32);
            let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let data_date_array = data[0..50]
                .iter()
                .copied()
                .map(Some)
                .collect::<Date32Array>();
            assert_eq!(array, &data_date_array);
        }
    }
}
