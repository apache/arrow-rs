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

use arrow_array::builder::{PrimitiveDictionaryBuilder, StringDictionaryBuilder, UnionBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Decimal128Type, Decimal256Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimestampMicrosecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, NullArray, PrimitiveArray, StringArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray,
};
use arrow_buffer::{i256, Buffer, IntervalDayTime, IntervalMonthDayNano};
use arrow_cast::pretty::pretty_format_columns;
use arrow_cast::{can_cast_types, cast};
use arrow_data::ArrayData;
use arrow_schema::{
    ArrowError, DataType, Field, Fields, IntervalUnit, TimeUnit, UnionFields, UnionMode,
};
use half::f16;
use std::sync::Arc;

#[test]
fn test_cast_timestamp_to_string() {
    let a = TimestampMillisecondArray::from(vec![Some(864000000005), Some(1545696000001), None])
        .with_timezone("UTC".to_string());
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Utf8).unwrap();
    let c = b.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(&DataType::Utf8, c.data_type());
    assert_eq!("1997-05-19T00:00:00.005Z", c.value(0));
    assert_eq!("2018-12-25T00:00:00.001Z", c.value(1));
    assert!(c.is_null(2));
}

// See: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for list of valid
// timezones

// Cast Timestamp(_, None) -> Timestamp(_, Some(timezone))
#[test]
fn test_cast_timestamp_with_timezone_daylight_1() {
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
        // This is winter in New York so daylight saving is not in effect
        // UTC offset is -05:00
        Some("2000-01-01T00:00:00.123456789"),
        // This is summer in New York so daylight saving is in effect
        // UTC offset is -04:00
        Some("2010-07-01T00:00:00.123456789"),
        None,
    ]));
    let to_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let timestamp_array = cast(&string_array, &to_type).unwrap();

    let to_type = DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()));
    let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T00:00:00.123456-05:00", result.value(0));
    assert_eq!("2010-07-01T00:00:00.123456-04:00", result.value(1));
    assert!(result.is_null(2));
}

// Cast Timestamp(_, Some(timezone)) -> Timestamp(_, None)
#[test]
fn test_cast_timestamp_with_timezone_daylight_2() {
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
        Some("2000-01-01T07:00:00.123456789"),
        Some("2010-07-01T07:00:00.123456789"),
        None,
    ]));
    let to_type = DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into()));
    let timestamp_array = cast(&string_array, &to_type).unwrap();

    // Check intermediate representation is correct
    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T07:00:00.123-05:00", result.value(0));
    assert_eq!("2010-07-01T07:00:00.123-04:00", result.value(1));
    assert!(result.is_null(2));

    let to_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T12:00:00.123", result.value(0));
    assert_eq!("2010-07-01T11:00:00.123", result.value(1));
    assert!(result.is_null(2));
}

// Cast Timestamp(_, Some(timezone)) -> Timestamp(_, Some(timezone))
#[test]
fn test_cast_timestamp_with_timezone_daylight_3() {
    let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec![
        // Winter in New York, summer in Sydney
        // UTC offset is -05:00 (New York) and +11:00 (Sydney)
        Some("2000-01-01T00:00:00.123456789"),
        // Summer in New York, winter in Sydney
        // UTC offset is -04:00 (New York) and +10:00 (Sydney)
        Some("2010-07-01T00:00:00.123456789"),
        None,
    ]));
    let to_type = DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()));
    let timestamp_array = cast(&string_array, &to_type).unwrap();

    // Check intermediate representation is correct
    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T00:00:00.123456-05:00", result.value(0));
    assert_eq!("2010-07-01T00:00:00.123456-04:00", result.value(1));
    assert!(result.is_null(2));

    let to_type = DataType::Timestamp(TimeUnit::Second, Some("Australia/Sydney".into()));
    let timestamp_array = cast(&timestamp_array, &to_type).unwrap();

    let string_array = cast(&timestamp_array, &DataType::Utf8).unwrap();
    let result = string_array.as_string::<i32>();
    assert_eq!("2000-01-01T16:00:00+11:00", result.value(0));
    assert_eq!("2010-07-01T14:00:00+10:00", result.value(1));
    assert!(result.is_null(2));
}

#[test]
#[cfg_attr(miri, ignore)] // running forever
fn test_can_cast_types() {
    // this function attempts to ensure that can_cast_types stays
    // in sync with cast.  It simply tries all combinations of
    // types and makes sure that if `can_cast_types` returns
    // true, so does `cast`

    let all_types = get_all_types();

    for array in get_arrays_of_all_types() {
        for to_type in &all_types {
            println!("Test casting {:?} --> {:?}", array.data_type(), to_type);
            let cast_result = cast(&array, to_type);
            let reported_cast_ability = can_cast_types(array.data_type(), to_type);

            // check for mismatch
            match (cast_result, reported_cast_ability) {
                (Ok(_), false) => {
                    panic!("Was able to cast array {:?} from {:?} to {:?} but can_cast_types reported false",
                           array, array.data_type(), to_type)
                }
                (Err(e), true) => {
                    panic!("Was not able to cast array {:?} from {:?} to {:?} but can_cast_types reported true. \
                                Error was {:?}",
                           array, array.data_type(), to_type, e)
                }
                // otherwise it was a match
                _ => {}
            };
        }
    }
}

/// Create instances of arrays with varying types for cast tests
fn get_arrays_of_all_types() -> Vec<ArrayRef> {
    let tz_name = "+08:00";
    let binary_data: Vec<&[u8]> = vec![b"foo", b"bar"];
    vec![
        Arc::new(BinaryArray::from(binary_data.clone())),
        Arc::new(LargeBinaryArray::from(binary_data.clone())),
        make_dictionary_primitive::<Int8Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<Int16Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<Int32Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<Int64Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt8Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt16Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt32Type, Int32Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt64Type, Int32Type>(vec![1, 2]),
        make_dictionary_utf8::<Int8Type>(),
        make_dictionary_utf8::<Int16Type>(),
        make_dictionary_utf8::<Int32Type>(),
        make_dictionary_utf8::<Int64Type>(),
        make_dictionary_utf8::<UInt8Type>(),
        make_dictionary_utf8::<UInt16Type>(),
        make_dictionary_utf8::<UInt32Type>(),
        make_dictionary_utf8::<UInt64Type>(),
        Arc::new(make_list_array()),
        Arc::new(make_large_list_array()),
        Arc::new(make_fixed_size_list_array()),
        Arc::new(make_fixed_size_binary_array()),
        Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("b", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
            ),
        ])),
        Arc::new(make_union_array()),
        Arc::new(NullArray::new(10)),
        Arc::new(StringArray::from(vec!["foo", "bar"])),
        Arc::new(LargeStringArray::from(vec!["foo", "bar"])),
        Arc::new(BooleanArray::from(vec![true, false])),
        Arc::new(Int8Array::from(vec![1, 2])),
        Arc::new(Int16Array::from(vec![1, 2])),
        Arc::new(Int32Array::from(vec![1, 2])),
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(UInt8Array::from(vec![1, 2])),
        Arc::new(UInt16Array::from(vec![1, 2])),
        Arc::new(UInt32Array::from(vec![1, 2])),
        Arc::new(UInt64Array::from(vec![1, 2])),
        Arc::new(
            [Some(f16::from_f64(1.0)), Some(f16::from_f64(2.0))]
                .into_iter()
                .collect::<Float16Array>(),
        ),
        Arc::new(Float32Array::from(vec![1.0, 2.0])),
        Arc::new(Float64Array::from(vec![1.0, 2.0])),
        Arc::new(TimestampSecondArray::from(vec![1000, 2000])),
        Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
        Arc::new(TimestampMicrosecondArray::from(vec![1000, 2000])),
        Arc::new(TimestampNanosecondArray::from(vec![1000, 2000])),
        Arc::new(TimestampSecondArray::from(vec![1000, 2000]).with_timezone(tz_name)),
        Arc::new(TimestampMillisecondArray::from(vec![1000, 2000]).with_timezone(tz_name)),
        Arc::new(TimestampMicrosecondArray::from(vec![1000, 2000]).with_timezone(tz_name)),
        Arc::new(TimestampNanosecondArray::from(vec![1000, 2000]).with_timezone(tz_name)),
        Arc::new(Date32Array::from(vec![1000, 2000])),
        Arc::new(Date64Array::from(vec![1000, 2000])),
        Arc::new(Time32SecondArray::from(vec![1000, 2000])),
        Arc::new(Time32MillisecondArray::from(vec![1000, 2000])),
        Arc::new(Time64MicrosecondArray::from(vec![1000, 2000])),
        Arc::new(Time64NanosecondArray::from(vec![1000, 2000])),
        Arc::new(IntervalYearMonthArray::from(vec![1000, 2000])),
        Arc::new(IntervalDayTimeArray::from(vec![
            IntervalDayTime::new(0, 1000),
            IntervalDayTime::new(0, 2000),
        ])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(0, 0, 1000),
            IntervalMonthDayNano::new(0, 0, 1000),
        ])),
        Arc::new(DurationSecondArray::from(vec![1000, 2000])),
        Arc::new(DurationMillisecondArray::from(vec![1000, 2000])),
        Arc::new(DurationMicrosecondArray::from(vec![1000, 2000])),
        Arc::new(DurationNanosecondArray::from(vec![1000, 2000])),
        Arc::new(create_decimal128_array(vec![Some(1), Some(2), Some(3)], 38, 0).unwrap()),
        make_dictionary_primitive::<Int8Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<Int16Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<Int32Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<Int64Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt8Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt16Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt32Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<UInt64Type, Decimal128Type>(vec![1, 2]),
        make_dictionary_primitive::<Int8Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<Int16Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<Int32Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<Int64Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<UInt8Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<UInt16Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<UInt32Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
        make_dictionary_primitive::<UInt64Type, Decimal256Type>(vec![
            i256::from_i128(1),
            i256::from_i128(2),
        ]),
    ]
}

fn make_fixed_size_list_array() -> FixedSizeListArray {
    // Construct a value array
    let value_data = ArrayData::builder(DataType::Int32)
        .len(10)
        .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
        .build()
        .unwrap();

    // Construct a fixed size list array from the above two
    let list_data_type =
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 2);
    let list_data = ArrayData::builder(list_data_type)
        .len(5)
        .add_child_data(value_data)
        .build()
        .unwrap();
    FixedSizeListArray::from(list_data)
}

fn make_fixed_size_binary_array() -> FixedSizeBinaryArray {
    let values: &[u8; 15] = b"hellotherearrow";

    let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
        .len(3)
        .add_buffer(Buffer::from(values))
        .build()
        .unwrap();
    FixedSizeBinaryArray::from(array_data)
}

fn make_list_array() -> ListArray {
    // Construct a value array
    let value_data = ArrayData::builder(DataType::Int32)
        .len(8)
        .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
        .build()
        .unwrap();

    // Construct a buffer for value offsets, for the nested array:
    //  [[0, 1, 2], [3, 4, 5], [6, 7]]
    let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

    // Construct a list array from the above two
    let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
    let list_data = ArrayData::builder(list_data_type)
        .len(3)
        .add_buffer(value_offsets)
        .add_child_data(value_data)
        .build()
        .unwrap();
    ListArray::from(list_data)
}

fn make_large_list_array() -> LargeListArray {
    // Construct a value array
    let value_data = ArrayData::builder(DataType::Int32)
        .len(8)
        .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
        .build()
        .unwrap();

    // Construct a buffer for value offsets, for the nested array:
    //  [[0, 1, 2], [3, 4, 5], [6, 7]]
    let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 8]);

    // Construct a list array from the above two
    let list_data_type =
        DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
    let list_data = ArrayData::builder(list_data_type)
        .len(3)
        .add_buffer(value_offsets)
        .add_child_data(value_data)
        .build()
        .unwrap();
    LargeListArray::from(list_data)
}

fn make_union_array() -> UnionArray {
    let mut builder = UnionBuilder::with_capacity_dense(7);
    builder.append::<Int32Type>("a", 1).unwrap();
    builder.append::<Int64Type>("b", 2).unwrap();
    builder.build().unwrap()
}

/// Creates a dictionary with primitive dictionary values, and keys of type K
/// and values of type V
fn make_dictionary_primitive<K: ArrowDictionaryKeyType, V: ArrowPrimitiveType>(
    values: Vec<V::Native>,
) -> ArrayRef {
    // Pick Int32 arbitrarily for dictionary values
    let mut b: PrimitiveDictionaryBuilder<K, V> = PrimitiveDictionaryBuilder::new();
    values.iter().for_each(|v| {
        b.append(*v).unwrap();
    });
    Arc::new(b.finish())
}

/// Creates a dictionary with utf8 values, and keys of type K
fn make_dictionary_utf8<K: ArrowDictionaryKeyType>() -> ArrayRef {
    // Pick Int32 arbitrarily for dictionary values
    let mut b: StringDictionaryBuilder<K> = StringDictionaryBuilder::new();
    b.append("foo").unwrap();
    b.append("bar").unwrap();
    Arc::new(b.finish())
}

fn create_decimal128_array(
    array: Vec<Option<i128>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, ArrowError> {
    array
        .into_iter()
        .collect::<Decimal128Array>()
        .with_precision_and_scale(precision, scale)
}

// Get a selection of datatypes to try and cast to
fn get_all_types() -> Vec<DataType> {
    use DataType::*;
    let tz_name: Arc<str> = Arc::from("+08:00");

    let mut types = vec![
        Null,
        Boolean,
        Int8,
        Int16,
        Int32,
        UInt64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float16,
        Float32,
        Float64,
        Timestamp(TimeUnit::Second, None),
        Timestamp(TimeUnit::Millisecond, None),
        Timestamp(TimeUnit::Microsecond, None),
        Timestamp(TimeUnit::Nanosecond, None),
        Timestamp(TimeUnit::Second, Some(tz_name.clone())),
        Timestamp(TimeUnit::Millisecond, Some(tz_name.clone())),
        Timestamp(TimeUnit::Microsecond, Some(tz_name.clone())),
        Timestamp(TimeUnit::Nanosecond, Some(tz_name)),
        Date32,
        Date64,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
        Duration(TimeUnit::Second),
        Duration(TimeUnit::Millisecond),
        Duration(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
        Interval(IntervalUnit::YearMonth),
        Interval(IntervalUnit::DayTime),
        Interval(IntervalUnit::MonthDayNano),
        Binary,
        FixedSizeBinary(3),
        LargeBinary,
        Utf8,
        LargeUtf8,
        List(Arc::new(Field::new_list_field(DataType::Int8, true))),
        List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
        FixedSizeList(Arc::new(Field::new_list_field(DataType::Int8, true)), 10),
        FixedSizeList(Arc::new(Field::new_list_field(DataType::Utf8, false)), 10),
        LargeList(Arc::new(Field::new_list_field(DataType::Int8, true))),
        LargeList(Arc::new(Field::new_list_field(DataType::Utf8, false))),
        Struct(Fields::from(vec![
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Utf8, true),
        ])),
        Union(
            UnionFields::new(
                vec![0, 1],
                vec![
                    Field::new("f1", DataType::Int32, false),
                    Field::new("f2", DataType::Utf8, true),
                ],
            ),
            UnionMode::Dense,
        ),
        Decimal128(38, 0),
    ];

    let dictionary_key_types = vec![Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64];

    let mut dictionary_types = dictionary_key_types
        .into_iter()
        .flat_map(|key_type| {
            vec![
                Dictionary(Box::new(key_type.clone()), Box::new(Int32)),
                Dictionary(Box::new(key_type.clone()), Box::new(Utf8)),
                Dictionary(Box::new(key_type.clone()), Box::new(LargeUtf8)),
                Dictionary(Box::new(key_type.clone()), Box::new(Binary)),
                Dictionary(Box::new(key_type.clone()), Box::new(LargeBinary)),
                Dictionary(Box::new(key_type.clone()), Box::new(Decimal128(38, 0))),
                Dictionary(Box::new(key_type), Box::new(Decimal256(76, 0))),
            ]
        })
        .collect::<Vec<_>>();

    types.append(&mut dictionary_types);
    types
}

#[test]
fn test_timestamp_cast_utf8() {
    let array: PrimitiveArray<TimestampMicrosecondType> =
        vec![Some(37800000000), None, Some(86339000000)].into();
    let out = cast(&(Arc::new(array) as ArrayRef), &DataType::Utf8).unwrap();

    let expected = StringArray::from(vec![
        Some("1970-01-01T10:30:00"),
        None,
        Some("1970-01-01T23:58:59"),
    ]);

    assert_eq!(
        out.as_any().downcast_ref::<StringArray>().unwrap(),
        &expected
    );

    let array: PrimitiveArray<TimestampMicrosecondType> =
        vec![Some(37800000000), None, Some(86339000000)].into();
    let array = array.with_timezone("Australia/Sydney".to_string());
    let out = cast(&(Arc::new(array) as ArrayRef), &DataType::Utf8).unwrap();

    let expected = StringArray::from(vec![
        Some("1970-01-01T20:30:00+10:00"),
        None,
        Some("1970-01-02T09:58:59+10:00"),
    ]);

    assert_eq!(
        out.as_any().downcast_ref::<StringArray>().unwrap(),
        &expected
    );
}

fn format_timezone(tz: &str) -> Result<String, ArrowError> {
    let array = Arc::new(TimestampSecondArray::from(vec![Some(11111111), None]).with_timezone(tz));
    Ok(pretty_format_columns("f", &[array])?.to_string())
}

#[test]
fn test_pretty_format_timestamp_second_with_utc_timezone() {
    let table = format_timezone("UTC").unwrap();
    let expected = vec![
        "+----------------------+",
        "| f                    |",
        "+----------------------+",
        "| 1970-05-09T14:25:11Z |",
        "|                      |",
        "+----------------------+",
    ];
    let actual: Vec<&str> = table.lines().collect();
    assert_eq!(expected, actual, "Actual result:\n\n{actual:#?}\n\n");
}

#[test]
fn test_pretty_format_timestamp_second_with_non_utc_timezone() {
    let table = format_timezone("Asia/Taipei").unwrap();

    let expected = vec![
        "+---------------------------+",
        "| f                         |",
        "+---------------------------+",
        "| 1970-05-09T22:25:11+08:00 |",
        "|                           |",
        "+---------------------------+",
    ];
    let actual: Vec<&str> = table.lines().collect();
    assert_eq!(expected, actual, "Actual result:\n\n{actual:#?}\n\n");
}

#[test]
fn test_pretty_format_timestamp_second_with_incorrect_fixed_offset_timezone() {
    let err = format_timezone("08:00").unwrap_err().to_string();
    assert_eq!(
        err,
        "Parser error: Invalid timezone \"08:00\": failed to parse timezone"
    );
}

#[test]
fn test_pretty_format_timestamp_second_with_unknown_timezone() {
    let err = format_timezone("unknown").unwrap_err().to_string();
    assert_eq!(
        err,
        "Parser error: Invalid timezone \"unknown\": failed to parse timezone"
    );
}
