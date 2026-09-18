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

use std::sync::Arc;

use arrow_array::builder::{Int8Builder, Int32Builder, LargeListBuilder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, UInt16Type,
};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int8Array, Int32Array, Int64Array,
    LargeListArray, LargeListViewArray, ListArray, ListViewArray, StringArray, UInt16Array,
    new_empty_array,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::{CastOptions, can_cast_types, cast, cast_with_options};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, TimeUnit};
use half::f16;
#[test]
fn test_cast_i32_to_list_i32() {
    let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
    let b = cast(
        &array,
        &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
    )
    .unwrap();
    assert_eq!(5, b.len());
    let arr = b.as_list::<i32>();
    assert_eq!(&[0, 1, 2, 3, 4, 5], arr.value_offsets());
    assert_eq!(1, arr.value_length(0));
    assert_eq!(1, arr.value_length(1));
    assert_eq!(1, arr.value_length(2));
    assert_eq!(1, arr.value_length(3));
    assert_eq!(1, arr.value_length(4));
    let c = arr.values().as_primitive::<Int32Type>();
    assert_eq!(5, c.value(0));
    assert_eq!(6, c.value(1));
    assert_eq!(7, c.value(2));
    assert_eq!(8, c.value(3));
    assert_eq!(9, c.value(4));
}

#[test]
fn test_cast_i32_to_list_i32_nullable() {
    let array = Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]);
    let b = cast(
        &array,
        &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
    )
    .unwrap();
    assert_eq!(5, b.len());
    assert_eq!(0, b.null_count());
    let arr = b.as_list::<i32>();
    assert_eq!(&[0, 1, 2, 3, 4, 5], arr.value_offsets());
    assert_eq!(1, arr.value_length(0));
    assert_eq!(1, arr.value_length(1));
    assert_eq!(1, arr.value_length(2));
    assert_eq!(1, arr.value_length(3));
    assert_eq!(1, arr.value_length(4));

    let c = arr.values().as_primitive::<Int32Type>();
    assert_eq!(1, c.null_count());
    assert_eq!(5, c.value(0));
    assert!(!c.is_valid(1));
    assert_eq!(7, c.value(2));
    assert_eq!(8, c.value(3));
    assert_eq!(9, c.value(4));
}

#[test]
fn test_cast_i32_to_list_f64_nullable_sliced() {
    let array = Int32Array::from(vec![Some(5), None, Some(7), Some(8), None, Some(10)]);
    let array = array.slice(2, 4);
    let b = cast(
        &array,
        &DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
    )
    .unwrap();
    assert_eq!(4, b.len());
    assert_eq!(0, b.null_count());
    let arr = b.as_list::<i32>();
    assert_eq!(&[0, 1, 2, 3, 4], arr.value_offsets());
    assert_eq!(1, arr.value_length(0));
    assert_eq!(1, arr.value_length(1));
    assert_eq!(1, arr.value_length(2));
    assert_eq!(1, arr.value_length(3));
    let c = arr.values().as_primitive::<Float64Type>();
    assert_eq!(1, c.null_count());
    assert_eq!(7.0, c.value(0));
    assert_eq!(8.0, c.value(1));
    assert!(!c.is_valid(2));
    assert_eq!(10.0, c.value(3));
}

#[test]
fn test_cast_list_i32_to_list_u16() {
    let values = vec![
        Some(vec![Some(0), Some(0), Some(0)]),
        Some(vec![Some(-1), Some(-2), Some(-1)]),
        Some(vec![Some(2), Some(100000000)]),
    ];
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(values);

    let target_type = DataType::List(Arc::new(Field::new("item", DataType::UInt16, true)));
    assert!(can_cast_types(list_array.data_type(), &target_type));
    let cast_array = cast(&list_array, &target_type).unwrap();

    // For the ListArray itself, there are no null values (as there were no nulls when they went in)
    //
    // 3 negative values should get lost when casting to unsigned,
    // 1 value should overflow
    assert_eq!(0, cast_array.null_count());

    // offsets should be the same
    let array = cast_array.as_list::<i32>();
    assert_eq!(list_array.value_offsets(), array.value_offsets());

    assert_eq!(DataType::UInt16, array.value_type());
    assert_eq!(3, array.value_length(0));
    assert_eq!(3, array.value_length(1));
    assert_eq!(2, array.value_length(2));

    // expect 4 nulls: negative numbers and overflow
    let u16arr = array.values().as_primitive::<UInt16Type>();
    assert_eq!(4, u16arr.null_count());

    // expect 4 nulls: negative numbers and overflow
    let expected: UInt16Array = vec![Some(0), Some(0), Some(0), None, None, None, Some(2), None]
        .into_iter()
        .collect();

    assert_eq!(u16arr, &expected);
}

#[test]
fn test_cast_list_i32_to_list_timestamp() {
    // Construct a value array
    let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 8, 100000000]).into_data();

    let value_offsets = Buffer::from_slice_ref([0, 3, 6, 9]);

    // Construct a list array from the above two
    let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
    let list_data = ArrayData::builder(list_data_type)
        .len(3)
        .add_buffer(value_offsets)
        .add_child_data(value_data)
        .build()
        .unwrap();
    let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

    let actual = cast(
        &list_array,
        &DataType::List(Arc::new(Field::new_list_field(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ))),
    )
    .unwrap();

    let expected = cast(
        &cast(
            &list_array,
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
        )
        .unwrap(),
        &DataType::List(Arc::new(Field::new_list_field(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ))),
    )
    .unwrap();

    assert_eq!(&actual, &expected);
}

#[test]
fn test_cast_zero_width_fsl_to_fsl() {
    // size=0 FSL with no nulls: length cannot be inferred from the child buffer
    // (0 bytes / 0 = ambiguous), so the cast must preserve it explicitly.
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let input = FixedSizeListArray::try_new_with_length(
        field,
        0,
        Arc::new(Int32Array::new_null(0)),
        None,
        3,
    )
    .unwrap();
    let to_type =
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int64, true)), 0);
    let result = cast(&(Arc::new(input) as ArrayRef), &to_type).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result.data_type(), &to_type);
}

#[test]
#[cfg_attr(miri, ignore)] // Unsupported inline assembly
fn test_can_cast_fsl_to_fsl() {
    let from_array = Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [Some([Some(1.0), Some(2.0)]), None],
            2,
        ),
    ) as ArrayRef;
    let to_array = Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float16Type, _, _>(
            [
                Some([Some(f16::from_f32(1.0)), Some(f16::from_f32(2.0))]),
                None,
            ],
            2,
        ),
    ) as ArrayRef;

    assert!(can_cast_types(from_array.data_type(), to_array.data_type()));
    let actual = cast(&from_array, to_array.data_type()).unwrap();
    assert_eq!(actual.data_type(), to_array.data_type());

    let invalid_target =
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Binary, true)), 2);
    assert!(!can_cast_types(from_array.data_type(), &invalid_target));

    let invalid_size =
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float16, true)), 5);
    assert!(!can_cast_types(from_array.data_type(), &invalid_size));
}

#[test]
fn test_can_cast_types_fixed_size_list_to_list() {
    // DataType::List
    let array1 = make_fixed_size_list_array();
    assert!(can_cast_types(
        array1.data_type(),
        &DataType::List(Arc::new(Field::new("", DataType::Int32, false)))
    ));

    // DataType::LargeList
    let array2 = make_fixed_size_list_array_for_large_list();
    assert!(can_cast_types(
        array2.data_type(),
        &DataType::LargeList(Arc::new(Field::new("", DataType::Int64, false)))
    ));
}

#[test]
fn test_cast_fixed_size_list_to_list() {
    // Important cases:
    // 1. With/without nulls
    // 2. List/LargeList/ListView/LargeListView
    // 3. With and without inner casts

    let cases = [
        // fixed_size_list<i32, 2> => list<i32>
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [[1, 1].map(Some), [2, 2].map(Some)].map(Some),
                2,
            )) as ArrayRef,
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some([Some(1), Some(1)]),
                Some([Some(2), Some(2)]),
            ])) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => list<i32> (nullable)
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [None, Some([Some(2), Some(2)])],
                2,
            )) as ArrayRef,
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                None,
                Some([Some(2), Some(2)]),
            ])) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => large_list<i64>
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [[1, 1].map(Some), [2, 2].map(Some)].map(Some),
                2,
            )) as ArrayRef,
            Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>([
                Some([Some(1), Some(1)]),
                Some([Some(2), Some(2)]),
            ])) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => large_list<i64> (nullable)
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [None, Some([Some(2), Some(2)])],
                2,
            )) as ArrayRef,
            Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>([
                None,
                Some([Some(2), Some(2)]),
            ])) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => list_view<i32>
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [[1, 1].map(Some), [2, 2].map(Some)].map(Some),
                2,
            )) as ArrayRef,
            Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>([
                Some([Some(1), Some(1)]),
                Some([Some(2), Some(2)]),
            ])) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => list_view<i32> (nullable)
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [None, Some([Some(2), Some(2)])],
                2,
            )) as ArrayRef,
            Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>([
                None,
                Some([Some(2), Some(2)]),
            ])) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => large_list_view<i64>
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [[1, 1].map(Some), [2, 2].map(Some)].map(Some),
                2,
            )) as ArrayRef,
            Arc::new(LargeListViewArray::from_iter_primitive::<Int64Type, _, _>(
                [Some([Some(1), Some(1)]), Some([Some(2), Some(2)])],
            )) as ArrayRef,
        ),
        // fixed_size_list<i32, 2> => large_list_view<i64> (nullable)
        (
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                [None, Some([Some(2), Some(2)])],
                2,
            )) as ArrayRef,
            Arc::new(LargeListViewArray::from_iter_primitive::<Int64Type, _, _>(
                [None, Some([Some(2), Some(2)])],
            )) as ArrayRef,
        ),
    ];

    for (array, expected) in cases {
        assert!(
            can_cast_types(array.data_type(), expected.data_type()),
            "can_cast_types claims we cannot cast {:?} to {:?}",
            array.data_type(),
            expected.data_type()
        );

        let list_array = cast(&array, expected.data_type())
            .unwrap_or_else(|_| panic!("Failed to cast {array:?} to {expected:?}"));
        assert_eq!(
            list_array.as_ref(),
            &expected,
            "Incorrect result from casting {array:?} to {expected:?}",
        );
    }
}

#[test]
fn test_cast_fixed_size_list_to_list_preserves_field_metadata() {
    use std::collections::HashMap;

    let metadata: HashMap<String, String> =
        HashMap::from([("PARQUET:field_id".to_string(), "89".to_string())]);

    let src = Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [[1.0_f32, 2.0].map(Some), [3.0, 4.0].map(Some)].map(Some),
            2,
        ),
    ) as ArrayRef;

    let target_field =
        Arc::new(Field::new("element", DataType::Float32, true).with_metadata(metadata.clone()));

    let target_types = [
        DataType::List(target_field.clone()),
        DataType::LargeList(target_field.clone()),
        DataType::ListView(target_field.clone()),
        DataType::LargeListView(target_field.clone()),
    ];

    for target_type in &target_types {
        let result = cast(&src, target_type).unwrap();
        assert_eq!(
            result.data_type(),
            target_type,
            "Cast to {target_type:?} should preserve field metadata"
        );
    }
}

#[test]
fn test_cast_utf8_to_list() {
    // DataType::List
    let array = Arc::new(StringArray::from(vec!["5"])) as ArrayRef;
    let field = Arc::new(Field::new("", DataType::Int32, false));
    let list_array = cast(&array, &DataType::List(field.clone())).unwrap();
    let actual = list_array.as_list_opt::<i32>().unwrap();
    let expect = ListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(5)])]);
    assert_eq!(&expect.value(0), &actual.value(0));

    // DataType::LargeList
    let list_array = cast(&array, &DataType::LargeList(field.clone())).unwrap();
    let actual = list_array.as_list_opt::<i64>().unwrap();
    let expect = LargeListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(5)])]);
    assert_eq!(&expect.value(0), &actual.value(0));

    // DataType::FixedSizeList
    let list_array = cast(&array, &DataType::FixedSizeList(field.clone(), 1)).unwrap();
    let actual = list_array.as_fixed_size_list_opt().unwrap();
    let expect = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>([Some([Some(5)])], 1);
    assert_eq!(&expect.value(0), &actual.value(0));
}

#[test]
fn test_cast_single_element_fixed_size_list() {
    // FixedSizeList<T>[1] => T
    let from_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(
        [(Some([Some(5)]))],
        1,
    )) as ArrayRef;
    let casted_array = cast(&from_array, &DataType::Int32).unwrap();
    let actual: &Int32Array = casted_array.as_primitive();
    let expected = Int32Array::from(vec![Some(5)]);
    assert_eq!(&expected, actual);

    // FixedSizeList<T>[1] => FixedSizeList<U>[1]
    let from_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(
        [(Some([Some(5)]))],
        1,
    )) as ArrayRef;
    let to_field = Arc::new(Field::new("dummy", DataType::Float32, false));
    let actual = cast(&from_array, &DataType::FixedSizeList(to_field.clone(), 1)).unwrap();
    let expected = Arc::new(FixedSizeListArray::new(
        to_field.clone(),
        1,
        Arc::new(Float32Array::from(vec![Some(5.0)])) as ArrayRef,
        None,
    )) as ArrayRef;
    assert_eq!(*expected, *actual);

    // FixedSizeList<T>[1] => FixedSizeList<FixdSizedList<U>[1]>[1]
    let from_array = Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(
        [(Some([Some(5)]))],
        1,
    )) as ArrayRef;
    let to_field_inner = Arc::new(Field::new_list_field(DataType::Float32, false));
    let to_field = Arc::new(Field::new(
        "dummy",
        DataType::FixedSizeList(to_field_inner.clone(), 1),
        false,
    ));
    let actual = cast(&from_array, &DataType::FixedSizeList(to_field.clone(), 1)).unwrap();
    let expected = Arc::new(FixedSizeListArray::new(
        to_field.clone(),
        1,
        Arc::new(FixedSizeListArray::new(
            to_field_inner.clone(),
            1,
            Arc::new(Float32Array::from(vec![Some(5.0)])) as ArrayRef,
            None,
        )) as ArrayRef,
        None,
    )) as ArrayRef;
    assert_eq!(*expected, *actual);

    // T => FixedSizeList<T>[1] (non-nullable)
    let field = Arc::new(Field::new("dummy", DataType::Float32, false));
    let from_array = Arc::new(Int8Array::from(vec![Some(5)])) as ArrayRef;
    let casted_array = cast(&from_array, &DataType::FixedSizeList(field.clone(), 1)).unwrap();
    let actual = casted_array.as_fixed_size_list();
    let expected = Arc::new(FixedSizeListArray::new(
        field.clone(),
        1,
        Arc::new(Float32Array::from(vec![Some(5.0)])) as ArrayRef,
        None,
    )) as ArrayRef;
    assert_eq!(expected.as_ref(), actual);

    // T => FixedSizeList<T>[1] (nullable)
    let field = Arc::new(Field::new("nullable", DataType::Float32, true));
    let from_array = Arc::new(Int8Array::from(vec![None])) as ArrayRef;
    let casted_array = cast(&from_array, &DataType::FixedSizeList(field.clone(), 1)).unwrap();
    let actual = casted_array.as_fixed_size_list();
    let expected = Arc::new(FixedSizeListArray::new(
        field.clone(),
        1,
        Arc::new(Float32Array::from(vec![None])) as ArrayRef,
        None,
    )) as ArrayRef;
    assert_eq!(expected.as_ref(), actual);
}

#[test]
fn test_cast_list_containers() {
    // large-list to list
    let array = make_large_list_array();
    let list_array = cast(
        &array,
        &DataType::List(Arc::new(Field::new("", DataType::Int32, false))),
    )
    .unwrap();
    let actual = list_array.as_any().downcast_ref::<ListArray>().unwrap();
    let expected = array.as_any().downcast_ref::<LargeListArray>().unwrap();

    assert_eq!(&expected.value(0), &actual.value(0));
    assert_eq!(&expected.value(1), &actual.value(1));
    assert_eq!(&expected.value(2), &actual.value(2));

    // list to large-list
    let array = make_list_array();
    let large_list_array = cast(
        &array,
        &DataType::LargeList(Arc::new(Field::new("", DataType::Int32, false))),
    )
    .unwrap();
    let actual = large_list_array
        .as_any()
        .downcast_ref::<LargeListArray>()
        .unwrap();
    let expected = array.as_any().downcast_ref::<ListArray>().unwrap();

    assert_eq!(&expected.value(0), &actual.value(0));
    assert_eq!(&expected.value(1), &actual.value(1));
    assert_eq!(&expected.value(2), &actual.value(2));
}

#[test]
fn test_cast_list_view() {
    // cast between list view and list view
    let array = make_list_view_array();
    let to = DataType::ListView(Field::new_list_field(DataType::Float32, true).into());
    assert!(can_cast_types(array.data_type(), &to));
    let actual = cast(&array, &to).unwrap();
    let actual = actual.as_list_view::<i32>();

    assert_eq!(
        &Float32Array::from(vec![0.0, 1.0, 2.0]) as &dyn Array,
        actual.value(0).as_ref()
    );
    assert_eq!(
        &Float32Array::from(vec![3.0, 4.0, 5.0]) as &dyn Array,
        actual.value(1).as_ref()
    );
    assert_eq!(
        &Float32Array::from(vec![6.0, 7.0]) as &dyn Array,
        actual.value(2).as_ref()
    );

    // cast between large list view and large list view
    let array = make_large_list_view_array();
    let to = DataType::LargeListView(Field::new_list_field(DataType::Float32, true).into());
    assert!(can_cast_types(array.data_type(), &to));
    let actual = cast(&array, &to).unwrap();
    let actual = actual.as_list_view::<i64>();

    assert_eq!(
        &Float32Array::from(vec![0.0, 1.0, 2.0]) as &dyn Array,
        actual.value(0).as_ref()
    );
    assert_eq!(
        &Float32Array::from(vec![3.0, 4.0, 5.0]) as &dyn Array,
        actual.value(1).as_ref()
    );
    assert_eq!(
        &Float32Array::from(vec![6.0, 7.0]) as &dyn Array,
        actual.value(2).as_ref()
    );
}

#[test]
fn test_non_list_to_list_view() {
    let input = Arc::new(Int32Array::from(vec![Some(0), None, Some(2)])) as ArrayRef;
    let expected_primitive =
        Arc::new(Float32Array::from(vec![Some(0.0), None, Some(2.0)])) as ArrayRef;

    // [[0], [NULL], [2]]
    let expected = ListViewArray::new(
        Field::new_list_field(DataType::Float32, true).into(),
        vec![0, 1, 2].into(),
        vec![1, 1, 1].into(),
        expected_primitive.clone(),
        None,
    );
    assert!(can_cast_types(input.data_type(), expected.data_type()));
    let actual = cast(&input, expected.data_type()).unwrap();
    assert_eq!(actual.as_ref(), &expected);

    // [[0], [NULL], [2]]
    let expected = LargeListViewArray::new(
        Field::new_list_field(DataType::Float32, true).into(),
        vec![0, 1, 2].into(),
        vec![1, 1, 1].into(),
        expected_primitive.clone(),
        None,
    );
    assert!(can_cast_types(input.data_type(), expected.data_type()));
    let actual = cast(&input, expected.data_type()).unwrap();
    assert_eq!(actual.as_ref(), &expected);
}

#[test]
fn test_cast_list_to_zero_size_fsl() {
    let field = Arc::new(Field::new("a", DataType::Null, true));
    let length = 2;
    let expected = Arc::new(
        FixedSizeListArray::try_new_with_length(
            field.clone(),
            0,
            new_empty_array(&DataType::Null),
            None,
            2,
        )
        .unwrap(),
    ) as ArrayRef;

    let list = Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::from_repeated_length(0, length),
        new_empty_array(&DataType::Null),
        None,
    ));
    let fsl = cast(list.as_ref(), expected.data_type()).unwrap();
    assert_eq!(&expected, &fsl);

    let list = Arc::new(ListViewArray::new(
        field.clone(),
        vec![0; length].into(),
        vec![0; length].into(),
        new_empty_array(&DataType::Null),
        None,
    ));
    let fsl = cast(list.as_ref(), expected.data_type()).unwrap();
    assert_eq!(&expected, &fsl);
}

#[test]
fn test_cast_list_to_fsl() {
    // There four noteworthy cases we should handle:
    // 1. No nulls
    // 2. Nulls that are always empty
    // 3. Nulls that have varying lengths
    // 4. Nulls that are correctly sized (same as target list size)

    // Non-null case
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let values = vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5), Some(6)]),
    ];
    let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
    )) as ArrayRef;
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        values, 3,
    )) as ArrayRef;
    let actual = cast(array.as_ref(), &DataType::FixedSizeList(field.clone(), 3)).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    // Null cases
    // Array is [[1, 2, 3], null, [4, 5, 6], null]
    let cases = [
        (
            // Zero-length nulls
            vec![1, 2, 3, 4, 5, 6],
            vec![3, 0, 3, 0],
        ),
        (
            // Varying-length nulls
            vec![1, 2, 3, 0, 0, 4, 5, 6, 0],
            vec![3, 2, 3, 1],
        ),
        (
            // Correctly-sized nulls
            vec![1, 2, 3, 0, 0, 0, 4, 5, 6, 0, 0, 0],
            vec![3, 3, 3, 3],
        ),
        (
            // Mixed nulls
            vec![1, 2, 3, 4, 5, 6, 0, 0, 0],
            vec![3, 0, 3, 3],
        ),
    ];
    let null_buffer = NullBuffer::from(vec![true, false, true, false]);

    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5), Some(6)]),
            None,
        ],
        3,
    )) as ArrayRef;

    for (values, lengths) in &cases {
        let array = Arc::new(ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths(lengths.clone()),
            Arc::new(Int32Array::from(values.clone())),
            Some(null_buffer.clone()),
        )) as ArrayRef;
        let actual = cast(array.as_ref(), &DataType::FixedSizeList(field.clone(), 3)).unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());
    }
}

#[test]
fn test_cast_list_view_to_fsl() {
    // There four noteworthy cases we should handle:
    // 1. No nulls
    // 2. Nulls that are always empty
    // 3. Nulls that have varying lengths
    // 4. Nulls that are correctly sized (same as target list size)

    // Non-null case
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let values = vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5), Some(6)]),
    ];
    let array = Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
    )) as ArrayRef;
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        values, 3,
    )) as ArrayRef;
    let actual = cast(array.as_ref(), &DataType::FixedSizeList(field.clone(), 3)).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    // Null cases
    // Array is [[1, 2, 3], null, [4, 5, 6], null]
    let cases = [
        (
            // Zero-length nulls
            vec![1, 2, 3, 4, 5, 6],
            vec![0, 0, 3, 0],
            vec![3, 0, 3, 0],
        ),
        (
            // Varying-length nulls
            vec![1, 2, 3, 0, 0, 4, 5, 6, 0],
            vec![0, 1, 5, 0],
            vec![3, 2, 3, 1],
        ),
        (
            // Correctly-sized nulls
            vec![1, 2, 3, 0, 0, 0, 4, 5, 6, 0, 0, 0],
            vec![0, 3, 6, 9],
            vec![3, 3, 3, 3],
        ),
        (
            // Mixed nulls
            vec![1, 2, 3, 4, 5, 6, 0, 0, 0],
            vec![0, 0, 3, 6],
            vec![3, 0, 3, 3],
        ),
    ];
    let null_buffer = NullBuffer::from(vec![true, false, true, false]);

    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5), Some(6)]),
            None,
        ],
        3,
    )) as ArrayRef;

    for (values, offsets, lengths) in &cases {
        let array = Arc::new(ListViewArray::new(
            field.clone(),
            offsets.clone().into(),
            lengths.clone().into(),
            Arc::new(Int32Array::from(values.clone())),
            Some(null_buffer.clone()),
        )) as ArrayRef;
        let actual = cast(array.as_ref(), &DataType::FixedSizeList(field.clone(), 3)).unwrap();
        assert_eq!(expected.as_ref(), actual.as_ref());
    }
}

#[test]
fn test_cast_list_to_fsl_safety() {
    let values = vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5)]),
        Some(vec![Some(6), Some(7), Some(8), Some(9)]),
        Some(vec![Some(3), Some(4), Some(5)]),
    ];
    let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
    )) as ArrayRef;

    let res = cast_with_options(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    );
    assert!(res.is_err());
    assert!(
        format!("{res:?}")
            .contains("Cannot cast to FixedSizeList(3): value at index 1 has length 2")
    );

    // When safe=true (default), the cast will fill nulls for lists that are
    // too short and truncate lists that are too long.
    let res = cast(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
    )
    .unwrap();
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None, // Too short -> replaced with null
            None, // Too long -> replaced with null
            Some(vec![Some(3), Some(4), Some(5)]),
        ],
        3,
    )) as ArrayRef;
    assert_eq!(expected.as_ref(), res.as_ref());

    // The safe option is false and the source array contains a null list.
    // issue: https://github.com/apache/arrow-rs/issues/5642
    let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
    ])) as ArrayRef;
    let res = cast_with_options(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .unwrap();
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![Some(vec![Some(1), Some(2), Some(3)]), None],
        3,
    )) as ArrayRef;
    assert_eq!(expected.as_ref(), res.as_ref());
}

#[test]
fn test_cast_list_view_to_fsl_safety() {
    let values = vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5)]),
        Some(vec![Some(6), Some(7), Some(8), Some(9)]),
        Some(vec![Some(3), Some(4), Some(5)]),
    ];
    let array = Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
    )) as ArrayRef;

    let res = cast_with_options(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    );
    assert!(res.is_err());
    assert!(
        format!("{res:?}")
            .contains("Cannot cast to FixedSizeList(3): value at index 1 has length 2")
    );

    // When safe=true (default), the cast will fill nulls for lists that are
    // too short and truncate lists that are too long.
    let res = cast(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
    )
    .unwrap();
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None, // Too short -> replaced with null
            None, // Too long -> replaced with null
            Some(vec![Some(3), Some(4), Some(5)]),
        ],
        3,
    )) as ArrayRef;
    assert_eq!(expected.as_ref(), res.as_ref());

    // The safe option is false and the source array contains a null list.
    // issue: https://github.com/apache/arrow-rs/issues/5642
    let array = Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
    ])) as ArrayRef;
    let res = cast_with_options(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .unwrap();
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![Some(vec![Some(1), Some(2), Some(3)]), None],
        3,
    )) as ArrayRef;
    assert_eq!(expected.as_ref(), res.as_ref());
}

#[test]
fn test_cast_large_list_to_fsl() {
    let values = vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])];
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
        2,
    )) as ArrayRef;
    let target_type =
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 2);

    let array = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
    )) as ArrayRef;
    let actual = cast(array.as_ref(), &target_type).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    let array = Arc::new(LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(
        values.clone(),
    )) as ArrayRef;
    let actual = cast(array.as_ref(), &target_type).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());
}

#[test]
fn test_cast_list_to_fsl_subcast() {
    let array = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(i32::MAX)]),
        ],
    )) as ArrayRef;
    let expected = Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(i32::MAX as i64)]),
        ],
        2,
    )) as ArrayRef;
    let actual = cast(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int64, true)), 2),
    )
    .unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    let res = cast_with_options(
        array.as_ref(),
        &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int16, true)), 2),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    );
    assert!(res.is_err());
    assert!(format!("{res:?}").contains("Can't cast value 2147483647 to type Int16"));
}

#[test]
fn test_cast_list_to_fsl_empty() {
    let inner_field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let target_type = DataType::FixedSizeList(inner_field.clone(), 3);
    let expected = new_empty_array(&target_type);

    // list
    let array = new_empty_array(&DataType::List(inner_field.clone()));
    assert!(can_cast_types(array.data_type(), &target_type));
    let actual = cast(array.as_ref(), &target_type).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    // largelist
    let array = new_empty_array(&DataType::LargeList(inner_field.clone()));
    assert!(can_cast_types(array.data_type(), &target_type));
    let actual = cast(array.as_ref(), &target_type).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    // listview
    let array = new_empty_array(&DataType::ListView(inner_field.clone()));
    assert!(can_cast_types(array.data_type(), &target_type));
    let actual = cast(array.as_ref(), &target_type).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());

    // largelistview
    let array = new_empty_array(&DataType::LargeListView(inner_field.clone()));
    assert!(can_cast_types(array.data_type(), &target_type));
    let actual = cast(array.as_ref(), &target_type).unwrap();
    assert_eq!(expected.as_ref(), actual.as_ref());
}

fn make_list_array() -> ArrayRef {
    // [[0, 1, 2], [3, 4, 5], [6, 7]]
    Arc::new(ListArray::new(
        Field::new_list_field(DataType::Int32, true).into(),
        OffsetBuffer::from_lengths(vec![3, 3, 2]),
        Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
        None,
    ))
}

fn make_large_list_array() -> ArrayRef {
    // [[0, 1, 2], [3, 4, 5], [6, 7]]
    Arc::new(LargeListArray::new(
        Field::new_list_field(DataType::Int32, true).into(),
        OffsetBuffer::from_lengths(vec![3, 3, 2]),
        Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
        None,
    ))
}

fn make_list_view_array() -> ArrayRef {
    // [[0, 1, 2], [3, 4, 5], [6, 7]]
    Arc::new(ListViewArray::new(
        Field::new_list_field(DataType::Int32, true).into(),
        vec![0, 3, 6].into(),
        vec![3, 3, 2].into(),
        Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
        None,
    ))
}

fn make_large_list_view_array() -> ArrayRef {
    // [[0, 1, 2], [3, 4, 5], [6, 7]]
    Arc::new(LargeListViewArray::new(
        Field::new_list_field(DataType::Int32, true).into(),
        vec![0, 3, 6].into(),
        vec![3, 3, 2].into(),
        Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
        None,
    ))
}

fn make_fixed_size_list_array() -> ArrayRef {
    // [[0, 1, 2, 3], [4, 5, 6, 7]]
    Arc::new(FixedSizeListArray::new(
        Field::new_list_field(DataType::Int32, true).into(),
        4,
        Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
        None,
    ))
}

fn make_fixed_size_list_array_for_large_list() -> ArrayRef {
    // [[0, 1, 2, 3], [4, 5, 6, 7]]
    Arc::new(FixedSizeListArray::new(
        Field::new_list_field(DataType::Int64, true).into(),
        4,
        Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7])),
        None,
    ))
}

#[test]
fn test_list_cast_offsets() {
    // test if offset of the array is taken into account during cast
    let array1 = make_list_array().slice(1, 2);
    let array2 = make_list_array();

    let dt = DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
    let out1 = cast(&array1, &dt).unwrap();
    let out2 = cast(&array2, &dt).unwrap();

    assert_eq!(&out1, &out2.slice(1, 2))
}

#[test]
fn test_list_to_string() {
    fn assert_cast(array: &ArrayRef, expected: &[&str]) {
        assert!(can_cast_types(array.data_type(), &DataType::Utf8));
        let out = cast(array, &DataType::Utf8).unwrap();
        let out = out
            .as_string::<i32>()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(out, expected);

        assert!(can_cast_types(array.data_type(), &DataType::LargeUtf8));
        let out = cast(array, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_string::<i64>()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(out, expected);

        assert!(can_cast_types(array.data_type(), &DataType::Utf8View));
        let out = cast(array, &DataType::Utf8View).unwrap();
        let out = out
            .as_string_view()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(out, expected);
    }

    let array = Arc::new(ListArray::new(
        Field::new_list_field(DataType::Utf8, true).into(),
        OffsetBuffer::from_lengths(vec![3, 3, 2]),
        Arc::new(StringArray::from(vec![
            "a", "b", "c", "d", "e", "f", "g", "h",
        ])),
        None,
    )) as ArrayRef;

    assert_cast(&array, &["[a, b, c]", "[d, e, f]", "[g, h]"]);

    let array = make_list_array();
    assert_cast(&array, &["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);

    let array = make_large_list_array();
    assert_cast(&array, &["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);

    let array = make_list_view_array();
    assert_cast(&array, &["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);

    let array = make_large_list_view_array();
    assert_cast(&array, &["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);
}

#[test]
fn test_nested_list() {
    let mut list = ListBuilder::new(Int32Builder::new());
    list.append_value([Some(1), Some(2), Some(3)]);
    list.append_value([Some(4), None, Some(6)]);
    let list = list.finish();

    let to_field = Field::new("nested", list.data_type().clone(), false);
    let to = DataType::List(Arc::new(to_field));
    let out = cast(&list, &to).unwrap();
    let opts = FormatOptions::default().with_null("null");
    let formatted = ArrayFormatter::try_new(out.as_ref(), &opts).unwrap();

    assert_eq!(formatted.value(0).to_string(), "[[1], [2], [3]]");
    assert_eq!(formatted.value(1).to_string(), "[[4], [null], [6]]");
}

#[test]
fn test_nested_list_cast() {
    let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    builder.append_value([Some([Some(1), Some(2), None]), None]);
    builder.append_value([None, Some([]), None]);
    builder.append_null();
    builder.append_value([Some([Some(2), Some(3)])]);
    let start = builder.finish();

    let mut builder = LargeListBuilder::new(LargeListBuilder::new(Int8Builder::new()));
    builder.append_value([Some([Some(1), Some(2), None]), None]);
    builder.append_value([None, Some([]), None]);
    builder.append_null();
    builder.append_value([Some([Some(2), Some(3)])]);
    let expected = builder.finish();

    let actual = cast(&start, expected.data_type()).unwrap();
    assert_eq!(actual.as_ref(), &expected);
}

#[test]
fn test_list_format_options() {
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default().with_null("null"),
    };
    let array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(0), Some(1), Some(2)]),
        Some(vec![Some(0), None, Some(2)]),
    ]);
    let a = cast_with_options(&array, &DataType::Utf8, &options).unwrap();
    let r: Vec<_> = a.as_string::<i32>().iter().flatten().collect();
    assert_eq!(r, &["[0, 1, 2]", "[0, null, 2]"]);
}
fn int32_list_values() -> Vec<Option<Vec<Option<i32>>>> {
    vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5), Some(6)]),
        None,
        Some(vec![Some(7), Some(8), Some(9)]),
        Some(vec![None, Some(10)]),
    ]
}

#[test]
fn test_cast_list_view_to_list() {
    let list_view = ListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();
    let expected_list = ListArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_view_to_large_list() {
    let list_view = ListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i64>();
    let expected_list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_to_list_view() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list.data_type(), &target_type));
    let cast_result = cast(&list, &target_type).unwrap();

    let got_list_view = cast_result.as_list_view::<i32>();
    let expected_list_view =
        ListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got_list_view, &expected_list_view);

    // inner types get cast
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![None, Some(3)]),
    ]);
    let target_type = DataType::ListView(Arc::new(Field::new("item", DataType::Float32, true)));
    assert!(can_cast_types(list.data_type(), &target_type));
    let cast_result = cast(&list, &target_type).unwrap();

    let got_list_view = cast_result.as_list_view::<i32>();
    let expected_list_view = ListViewArray::from_iter_primitive::<Float32Type, _, _>(vec![
        Some(vec![Some(1.0), Some(2.0)]),
        None,
        Some(vec![None, Some(3.0)]),
    ]);
    assert_eq!(got_list_view, &expected_list_view);
}

#[test]
fn test_cast_list_to_large_list_view() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![None, Some(3)]),
    ]);
    let target_type =
        DataType::LargeListView(Arc::new(Field::new("item", DataType::Float32, true)));
    assert!(can_cast_types(list.data_type(), &target_type));
    let cast_result = cast(&list, &target_type).unwrap();

    let got_list_view = cast_result.as_list_view::<i64>();
    let expected_list_view = LargeListViewArray::from_iter_primitive::<Float32Type, _, _>(vec![
        Some(vec![Some(1.0), Some(2.0)]),
        None,
        Some(vec![None, Some(3.0)]),
    ]);
    assert_eq!(got_list_view, &expected_list_view);
}

#[test]
fn test_cast_large_list_view_to_large_list() {
    let list_view = LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i64>();

    let expected_list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_large_list_view_to_list() {
    let list_view = LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();

    let expected_list = ListArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_large_list_to_large_list_view() {
    let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list.data_type(), &target_type));
    let cast_result = cast(&list, &target_type).unwrap();

    let got_list_view = cast_result.as_list_view::<i64>();
    let expected_list_view =
        LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got_list_view, &expected_list_view);

    // inner types get cast
    let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![None, Some(3)]),
    ]);
    let target_type =
        DataType::LargeListView(Arc::new(Field::new("item", DataType::Float32, true)));
    assert!(can_cast_types(list.data_type(), &target_type));
    let cast_result = cast(&list, &target_type).unwrap();

    let got_list_view = cast_result.as_list_view::<i64>();
    let expected_list_view = LargeListViewArray::from_iter_primitive::<Float32Type, _, _>(vec![
        Some(vec![Some(1.0), Some(2.0)]),
        None,
        Some(vec![None, Some(3.0)]),
    ]);
    assert_eq!(got_list_view, &expected_list_view);
}

#[test]
fn test_cast_large_list_to_list_view() {
    let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![None, Some(3)]),
    ]);
    let target_type = DataType::ListView(Arc::new(Field::new("item", DataType::Float32, true)));
    assert!(can_cast_types(list.data_type(), &target_type));
    let cast_result = cast(&list, &target_type).unwrap();

    let got_list_view = cast_result.as_list_view::<i32>();
    let expected_list_view = ListViewArray::from_iter_primitive::<Float32Type, _, _>(vec![
        Some(vec![Some(1.0), Some(2.0)]),
        None,
        Some(vec![None, Some(3.0)]),
    ]);
    assert_eq!(got_list_view, &expected_list_view);
}

#[test]
fn test_cast_list_view_to_list_out_of_order() {
    let list_view = ListViewArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::from(vec![0, 6, 3]),
        ScalarBuffer::from(vec![3, 3, 3]),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9])),
        None,
    );
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();
    let expected_list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(7), Some(8), Some(9)]),
        Some(vec![Some(4), Some(5), Some(6)]),
    ]);
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_view_to_list_overlapping() {
    let list_view = ListViewArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::from(vec![0, 0]),
        ScalarBuffer::from(vec![1, 2]),
        Arc::new(Int32Array::from(vec![1, 2])),
        None,
    );
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();
    let expected_list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(1), Some(2)]),
    ]);
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_view_to_list_empty() {
    let values: Vec<Option<Vec<Option<i32>>>> = vec![];
    let list_view = ListViewArray::from_iter_primitive::<Int32Type, _, _>(values.clone());
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();
    let expected_list = ListArray::from_iter_primitive::<Int32Type, _, _>(values);
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_view_to_list_different_inner_type() {
    let values = int32_list_values();
    let list_view = ListViewArray::from_iter_primitive::<Int32Type, _, _>(values.clone());
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();

    let expected_list =
        ListArray::from_iter_primitive::<Int64Type, _, _>(values.into_iter().map(|list| {
            list.map(|list| {
                list.into_iter()
                    .map(|v| v.map(|v| v as i64))
                    .collect::<Vec<_>>()
            })
        }));
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_view_to_list_out_of_order_with_nulls() {
    let list_view = ListViewArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        ScalarBuffer::from(vec![0, 6, 3]),
        ScalarBuffer::from(vec![3, 3, 3]),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9])),
        Some(NullBuffer::from(vec![false, true, false])),
    );
    let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got_list = cast_result.as_list::<i32>();
    let expected_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::from_lengths([3, 3, 3]),
        Arc::new(Int32Array::from(vec![1, 2, 3, 7, 8, 9, 4, 5, 6])),
        Some(NullBuffer::from(vec![false, true, false])),
    );
    assert_eq!(got_list, &expected_list);
}

#[test]
fn test_cast_list_view_to_large_list_view() {
    let list_view = ListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got = cast_result.as_list_view::<i64>();

    let expected = LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got, &expected);
}

#[test]
fn test_cast_large_list_view_to_list_view() {
    let list_view = LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    let target_type = DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)));
    assert!(can_cast_types(list_view.data_type(), &target_type));
    let cast_result = cast(&list_view, &target_type).unwrap();
    let got = cast_result.as_list_view::<i32>();

    let expected = ListViewArray::from_iter_primitive::<Int32Type, _, _>(int32_list_values());
    assert_eq!(got, &expected);
}
