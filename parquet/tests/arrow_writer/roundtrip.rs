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

//! Round-trip tests for Arrow data written to Parquet.

use super::roundtrip_helpers::{SMALL_SIZE, required_and_optional};

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DecimalType, Float16Type, Time32MillisecondType, Time64MicrosecondType,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, DictionaryArray, FixedSizeBinaryArray,
    Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    ListArray, PrimitiveArray, RecordBatch, RecordBatchReader, StringArray, StructArray,
    Time32MillisecondArray, Time64MicrosecondArray, UInt8Array, UInt8DictionaryArray, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, Buffer, NullBuffer, i256};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use bytes::Bytes;
use half::f16;
use num_traits::PrimInt;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::Type as PhysicalType;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i8_single_column() {
    required_and_optional::<Int8Array, _>(0..SMALL_SIZE as i8);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i16_single_column() {
    required_and_optional::<Int16Array, _>(0..SMALL_SIZE as i16);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i32_single_column() {
    required_and_optional::<Int32Array, _>(0..SMALL_SIZE as i32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn i64_single_column() {
    required_and_optional::<Int64Array, _>(0..SMALL_SIZE as i64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u8_single_column() {
    required_and_optional::<UInt8Array, _>(0..SMALL_SIZE as u8);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u16_single_column() {
    required_and_optional::<UInt16Array, _>(0..SMALL_SIZE as u16);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u32_single_column() {
    required_and_optional::<UInt32Array, _>(0..SMALL_SIZE as u32);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn u64_single_column() {
    required_and_optional::<UInt64Array, _>(0..SMALL_SIZE as u64);
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn f32_single_column() {
    required_and_optional::<Float32Array, _>((0..SMALL_SIZE).map(|i| i as f32));
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn f64_single_column() {
    required_and_optional::<Float64Array, _>((0..SMALL_SIZE).map(|i| i as f64));
}

#[test]
fn test_unsigned_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("uint32", ArrowDataType::UInt32, true),
        Field::new("uint64", ArrowDataType::UInt64, true),
    ]));

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

    let original = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from_iter_values([
                0,
                i32::MAX as u32,
                u32::MAX,
            ])),
            Arc::new(UInt64Array::from_iter_values([
                0,
                i64::MAX as u64,
                u64::MAX,
            ])),
        ],
    )
    .unwrap();

    writer.write(&original).unwrap();
    writer.close().unwrap();

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024).unwrap();
    let ret = reader.next().unwrap().unwrap();
    assert_eq!(ret, original);

    // Check they can be downcast to the correct type
    ret.column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    ret.column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
}

#[test]
fn test_float16_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("float16", ArrowDataType::Float16, false),
        Field::new("float16-nullable", ArrowDataType::Float16, true),
    ]));

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

    let original = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float16Array::from_iter_values([
                f16::EPSILON,
                f16::MIN,
                f16::MAX,
                f16::NAN,
                f16::INFINITY,
                f16::NEG_INFINITY,
                f16::ONE,
                f16::NEG_ONE,
                f16::ZERO,
                f16::NEG_ZERO,
                f16::E,
                f16::PI,
                f16::FRAC_1_PI,
            ])),
            Arc::new(Float16Array::from(vec![
                None,
                None,
                None,
                Some(f16::NAN),
                Some(f16::INFINITY),
                Some(f16::NEG_INFINITY),
                None,
                None,
                None,
                None,
                None,
                None,
                Some(f16::FRAC_1_PI),
            ])),
        ],
    )?;

    writer.write(&original)?;
    writer.close()?;

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
    let ret = reader.next().unwrap()?;
    assert_eq!(ret, original);

    // Ensure can be downcast to the correct type
    ret.column(0).as_primitive::<Float16Type>();
    ret.column(1).as_primitive::<Float16Type>();

    Ok(())
}

#[test]
fn test_time_utc_roundtrip() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time_millis",
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".to_string(),
            String::new(),
        )])),
        Field::new(
            "time_micros",
            ArrowDataType::Time64(TimeUnit::Microsecond),
            true,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".to_string(),
            String::new(),
        )])),
    ]));

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

    let original = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Time32MillisecondArray::from(vec![
                Some(-1),
                Some(0),
                Some(86_399_000),
                Some(86_400_000),
                Some(86_401_000),
                None,
            ])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(-1),
                Some(0),
                Some(86_399 * 1_000_000),
                Some(86_400 * 1_000_000),
                Some(86_401 * 1_000_000),
                None,
            ])),
        ],
    )?;

    writer.write(&original)?;
    writer.close()?;

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
    let ret = reader.next().unwrap()?;
    assert_eq!(ret, original);

    // Ensure can be downcast to the correct type
    ret.column(0).as_primitive::<Time32MillisecondType>();
    ret.column(1).as_primitive::<Time64MicrosecondType>();

    Ok(())
}

#[test]
fn test_date32_roundtrip() -> Result<()> {
    use arrow_array::Date32Array;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "date32",
        ArrowDataType::Date32,
        false,
    )]));

    let mut buf = Vec::with_capacity(1024);

    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

    let original = RecordBatch::try_new(
        schema,
        vec![Arc::new(Date32Array::from(vec![
            -1_000_000, -100_000, -10_000, -1_000, 0, 1_000, 10_000, 100_000, 1_000_000,
        ]))],
    )?;

    writer.write(&original)?;
    writer.close()?;

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
    let ret = reader.next().unwrap()?;
    assert_eq!(ret, original);

    // Ensure can be downcast to the correct type
    ret.column(0).as_primitive::<Date32Type>();

    Ok(())
}

#[test]
fn test_date64_roundtrip() -> Result<()> {
    use arrow_array::Date64Array;

    let schema = Arc::new(Schema::new(vec![
        Field::new("small-date64", ArrowDataType::Date64, false),
        Field::new("big-date64", ArrowDataType::Date64, false),
        Field::new("invalid-date64", ArrowDataType::Date64, false),
    ]));

    let mut default_buf = Vec::with_capacity(1024);
    let mut coerce_buf = Vec::with_capacity(1024);

    let coerce_props = WriterProperties::builder().set_coerce_types(true).build();

    let mut default_writer = ArrowWriter::try_new(&mut default_buf, schema.clone(), None)?;
    let mut coerce_writer =
        ArrowWriter::try_new(&mut coerce_buf, schema.clone(), Some(coerce_props))?;

    static NUM_MILLISECONDS_IN_DAY: i64 = 1000 * 60 * 60 * 24;

    let original = RecordBatch::try_new(
        schema,
        vec![
            // small-date64
            Arc::new(Date64Array::from(vec![
                -1_000_000 * NUM_MILLISECONDS_IN_DAY,
                -1_000 * NUM_MILLISECONDS_IN_DAY,
                0,
                1_000 * NUM_MILLISECONDS_IN_DAY,
                1_000_000 * NUM_MILLISECONDS_IN_DAY,
            ])),
            // big-date64
            Arc::new(Date64Array::from(vec![
                -10_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                -1_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                0,
                1_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                10_000_000_000 * NUM_MILLISECONDS_IN_DAY,
            ])),
            // invalid-date64
            Arc::new(Date64Array::from(vec![
                -1_000_000 * NUM_MILLISECONDS_IN_DAY + 1,
                -1_000 * NUM_MILLISECONDS_IN_DAY + 1,
                1,
                1_000 * NUM_MILLISECONDS_IN_DAY + 1,
                1_000_000 * NUM_MILLISECONDS_IN_DAY + 1,
            ])),
        ],
    )?;

    default_writer.write(&original)?;
    coerce_writer.write(&original)?;

    default_writer.close()?;
    coerce_writer.close()?;

    let mut default_reader = ParquetRecordBatchReader::try_new(Bytes::from(default_buf), 1024)?;
    let mut coerce_reader = ParquetRecordBatchReader::try_new(Bytes::from(coerce_buf), 1024)?;

    let default_ret = default_reader.next().unwrap()?;
    let coerce_ret = coerce_reader.next().unwrap()?;

    // Roundtrip should be successful when default writer used
    assert_eq!(default_ret, original);

    // Only small-date64 should roundtrip successfully when coerce_types writer is used
    assert_eq!(coerce_ret.column(0), original.column(0));
    assert_ne!(coerce_ret.column(1), original.column(1));
    assert_ne!(coerce_ret.column(2), original.column(2));

    // Ensure both can be downcast to the correct type
    default_ret.column(0).as_primitive::<Date64Type>();
    coerce_ret.column(0).as_primitive::<Date64Type>();

    Ok(())
}

#[test]
fn test_decimal_nullable_struct() {
    let decimals = Decimal256Array::from_iter_values(
        [1, 2, 3, 4, 5, 6, 7, 8].into_iter().map(i256::from_i128),
    );

    let data = ArrayDataBuilder::new(ArrowDataType::Struct(Fields::from(vec![Field::new(
        "decimals",
        decimals.data_type().clone(),
        false,
    )])))
    .len(8)
    .null_bit_buffer(Some(Buffer::from(&[0b11101111])))
    .child_data(vec![decimals.into_data()])
    .build()
    .unwrap();

    let written =
        RecordBatch::try_from_iter([("struct", Arc::new(StructArray::from(data)) as ArrayRef)])
            .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 3), &read[0]);
    assert_eq!(&written.slice(3, 3), &read[1]);
    assert_eq!(&written.slice(6, 2), &read[2]);
}

#[test]
fn test_int32_nullable_struct() {
    let int32 = Int32Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8]);
    let data = ArrayDataBuilder::new(ArrowDataType::Struct(Fields::from(vec![Field::new(
        "int32",
        int32.data_type().clone(),
        false,
    )])))
    .len(8)
    .null_bit_buffer(Some(Buffer::from(&[0b11101111])))
    .child_data(vec![int32.into_data()])
    .build()
    .unwrap();

    let written =
        RecordBatch::try_from_iter([("struct", Arc::new(StructArray::from(data)) as ArrayRef)])
            .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 3), &read[0]);
    assert_eq!(&written.slice(3, 3), &read[1]);
    assert_eq!(&written.slice(6, 2), &read[2]);
}

#[test]
fn test_decimal_list() {
    let decimals = Decimal128Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8]);

    // [[], [1], [2, 3], null, [4], null, [6, 7, 8]]
    let data = ArrayDataBuilder::new(ArrowDataType::List(Arc::new(Field::new_list_field(
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
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 3), &read[0]);
    assert_eq!(&written.slice(3, 3), &read[1]);
    assert_eq!(&written.slice(6, 1), &read[2]);
}

#[test]
fn test_read_dict_fixed_size_binary() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Dictionary(
            Box::new(ArrowDataType::UInt8),
            Box::new(ArrowDataType::FixedSizeBinary(8)),
        ),
        true,
    )]));
    let keys = UInt8Array::from_iter_values(vec![0, 0, 1]);
    let values = FixedSizeBinaryArray::try_from_iter(
        vec![
            (0u8..8u8).collect::<Vec<u8>>(),
            (24u8..32u8).collect::<Vec<u8>>(),
        ]
        .into_iter(),
    )
    .unwrap();
    let arr = UInt8DictionaryArray::new(keys, Arc::new(values));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(&batch, &read[0])
}

#[test]
fn test_read_nullable_structs_with_binary_dict_as_first_child_column() {
    // the `StructArrayReader` will check the definition and repetition levels of the first
    // child column in the struct to determine nullability for the struct. If the first
    // column's is being read by `ByteArrayDictionaryReader` we need to ensure that the
    // nullability is interpreted  correctly from the rep/def level buffers managed by the
    // buffers managed by this array reader.

    let struct_fields = Fields::from(vec![
        Field::new(
            "city",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt8),
                Box::new(ArrowDataType::Utf8),
            ),
            true,
        ),
        Field::new("name", ArrowDataType::Utf8, true),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        ArrowDataType::Struct(struct_fields.clone()),
        true,
    )]));

    let items_arr = StructArray::new(
        struct_fields,
        vec![
            Arc::new(DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0, 1, 1, 0, 2]),
                Arc::new(StringArray::from_iter_values(vec![
                    "quebec",
                    "fredericton",
                    "halifax",
                ])),
            )),
            Arc::new(StringArray::from_iter_values(vec![
                "albert", "terry", "lance", "", "tim",
            ])),
        ],
        Some(NullBuffer::from_iter(vec![true, true, true, false, true])),
    );

    let batch = RecordBatch::try_new(schema, vec![Arc::new(items_arr)]).unwrap();
    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 8)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(&batch, &read[0])
}

#[test]
fn test_arbitrary_decimal() {
    let values = [1, 2, 3, 4, 5, 6, 7, 8];
    let decimals_19_0 = Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(19, 0)
        .unwrap();
    let decimals_12_0 = Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(12, 0)
        .unwrap();
    let decimals_17_10 = Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(17, 10)
        .unwrap();

    let written = RecordBatch::try_from_iter([
        ("decimal_values_19_0", Arc::new(decimals_19_0) as ArrayRef),
        ("decimal_values_12_0", Arc::new(decimals_12_0) as ArrayRef),
        ("decimal_values_17_10", Arc::new(decimals_17_10) as ArrayRef),
    ])
    .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
    writer.write(&written).unwrap();
    writer.close().unwrap();

    let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 8)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(&written.slice(0, 8), &read[0]);
}

fn test_decimal32_roundtrip() {
    let d = |values: Vec<i32>, p: u8| {
        let iter = values.into_iter();
        PrimitiveArray::<Decimal32Type>::from_iter_values(iter)
            .with_precision_and_scale(p, 2)
            .unwrap()
    };

    let d1 = d(vec![1, 2, 3, 4, 5], 9);
    let batch = RecordBatch::try_from_iter([("d1", Arc::new(d1) as ArrayRef)]).unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
    let t1 = builder.parquet_schema().columns()[0].physical_type();
    assert_eq!(t1, PhysicalType::INT32);

    let mut reader = builder.build().unwrap();
    assert_eq!(batch.schema(), reader.schema());

    let out = reader.next().unwrap().unwrap();
    assert_eq!(batch, out);
}

fn test_decimal64_roundtrip() {
    // Precision <= 9 -> INT32
    // Precision <= 18 -> INT64

    let d = |values: Vec<i64>, p: u8| {
        let iter = values.into_iter();
        PrimitiveArray::<Decimal64Type>::from_iter_values(iter)
            .with_precision_and_scale(p, 2)
            .unwrap()
    };

    let d1 = d(vec![1, 2, 3, 4, 5], 9);
    let d2 = d(vec![1, 2, 3, 4, 10.pow(10) - 1], 10);
    let d3 = d(vec![1, 2, 3, 4, 10.pow(18) - 1], 18);

    let batch = RecordBatch::try_from_iter([
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
    ])
    .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
    let t1 = builder.parquet_schema().columns()[0].physical_type();
    assert_eq!(t1, PhysicalType::INT32);
    let t2 = builder.parquet_schema().columns()[1].physical_type();
    assert_eq!(t2, PhysicalType::INT64);
    let t3 = builder.parquet_schema().columns()[2].physical_type();
    assert_eq!(t3, PhysicalType::INT64);

    let mut reader = builder.build().unwrap();
    assert_eq!(batch.schema(), reader.schema());

    let out = reader.next().unwrap().unwrap();
    assert_eq!(batch, out);
}

fn test_decimal_roundtrip<T: DecimalType>() {
    // Precision <= 9 -> INT32
    // Precision <= 18 -> INT64
    // Precision > 18 -> FIXED_LEN_BYTE_ARRAY

    let d = |values: Vec<usize>, p: u8| {
        let iter = values.into_iter().map(T::Native::usize_as);
        PrimitiveArray::<T>::from_iter_values(iter)
            .with_precision_and_scale(p, 2)
            .unwrap()
    };

    let d1 = d(vec![1, 2, 3, 4, 5], 9);
    let d2 = d(vec![1, 2, 3, 4, 10.pow(10) - 1], 10);
    let d3 = d(vec![1, 2, 3, 4, 10.pow(18) - 1], 18);
    let d4 = d(vec![1, 2, 3, 4, 10.pow(19) - 1], 19);

    let batch = RecordBatch::try_from_iter([
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
        ("d4", Arc::new(d4) as ArrayRef),
    ])
    .unwrap();

    let mut buffer = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
    let t1 = builder.parquet_schema().columns()[0].physical_type();
    assert_eq!(t1, PhysicalType::INT32);
    let t2 = builder.parquet_schema().columns()[1].physical_type();
    assert_eq!(t2, PhysicalType::INT64);
    let t3 = builder.parquet_schema().columns()[2].physical_type();
    assert_eq!(t3, PhysicalType::INT64);
    let t4 = builder.parquet_schema().columns()[3].physical_type();
    assert_eq!(t4, PhysicalType::FIXED_LEN_BYTE_ARRAY);

    let mut reader = builder.build().unwrap();
    assert_eq!(batch.schema(), reader.schema());

    let out = reader.next().unwrap().unwrap();
    assert_eq!(batch, out);
}

#[test]
fn test_decimal() {
    test_decimal32_roundtrip();
    test_decimal64_roundtrip();
    test_decimal_roundtrip::<Decimal128Type>();
    test_decimal_roundtrip::<Decimal256Type>();
}
