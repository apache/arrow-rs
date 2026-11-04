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

use arrow_array::builder::{Int8Builder, MapBuilder, MapFieldNames, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{Array, IntervalDayTimeArray, StringArray};
use arrow_buffer::{IntervalDayTime, NullBuffer};
use arrow_cast::{can_cast_types, cast};
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};

#[test]
fn test_cast_map_dont_allow_change_of_order() {
    let string_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut builder = MapBuilder::new(None, string_builder, value_builder);

    builder.keys().append_value("0");
    builder.values().append_value("test_val_1");
    builder.append(true).unwrap();
    builder.keys().append_value("1");
    builder.values().append_value("test_val_2");
    builder.append(true).unwrap();

    // map builder returns unsorted map by default
    let array = builder.finish();

    let new_ordered = true;
    let new_type = DataType::Map(
        Arc::new(Field::new(
            Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
            DataType::Struct(
                vec![
                    Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                    Field::new(Field::MAP_VALUE_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        new_ordered,
    );

    let new_array_result = cast(&array, &new_type);
    assert!(!can_cast_types(array.data_type(), &new_type));
    let Err(ArrowError::CastError(t)) = new_array_result else {
        panic!();
    };
    assert_eq!(
        t,
        r#"Casting from Map("entries": non-null Struct("key": non-null Utf8, "value": Utf8), unsorted) to Map("entries": non-null Struct("key": non-null Utf8, "value": non-null Utf8), sorted) not supported"#
    );
}

#[test]
fn test_cast_map_dont_allow_when_container_cant_cast() {
    let string_builder = StringBuilder::new();
    let value_builder = IntervalDayTimeArray::builder(2);
    let mut builder = MapBuilder::new(None, string_builder, value_builder);

    builder.keys().append_value("0");
    builder.values().append_value(IntervalDayTime::new(1, 1));
    builder.append(true).unwrap();
    builder.keys().append_value("1");
    builder.values().append_value(IntervalDayTime::new(2, 2));
    builder.append(true).unwrap();

    // map builder returns unsorted map by default
    let array = builder.finish();

    let new_ordered = true;
    let new_type = DataType::Map(
        Arc::new(Field::new(
            Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
            DataType::Struct(
                vec![
                    Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                    Field::new(
                        Field::MAP_VALUE_FIELD_DEFAULT_NAME,
                        DataType::Duration(TimeUnit::Second),
                        false,
                    ),
                ]
                .into(),
            ),
            false,
        )),
        new_ordered,
    );

    let new_array_result = cast(&array, &new_type);
    assert!(!can_cast_types(array.data_type(), &new_type));
    let Err(ArrowError::CastError(t)) = new_array_result else {
        panic!();
    };
    assert_eq!(
        t,
        r#"Casting from Map("entries": non-null Struct("key": non-null Utf8, "value": Interval(DayTime)), unsorted) to Map("entries": non-null Struct("key": non-null Utf8, "value": non-null Duration(s)), sorted) not supported"#
    );
}

#[test]
fn test_cast_map_field_names() {
    let string_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut builder = MapBuilder::new(
        Some(MapFieldNames {
            // Explicitly writing the name so it will be apparent from what names to what names are we converting to
            entry: Field::MAP_ENTRIES_FIELD_DEFAULT_NAME.to_string(),
            key: Field::MAP_KEY_FIELD_DEFAULT_NAME.to_string(),
            value: Field::MAP_VALUE_FIELD_DEFAULT_NAME.to_string(),
        }),
        string_builder,
        value_builder,
    );

    builder.keys().append_value("0");
    builder.values().append_value("test_val_1");
    builder.append(true).unwrap();
    builder.keys().append_value("1");
    builder.values().append_value("test_val_2");
    builder.append(true).unwrap();
    builder.append(false).unwrap();

    let array = builder.finish();

    let new_type = DataType::Map(
        Arc::new(Field::new(
            "entries_new",
            DataType::Struct(
                vec![
                    Field::new("key_new", DataType::Utf8, false),
                    Field::new("value_values", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );

    assert_ne!(new_type, array.data_type().clone());

    let new_array = cast(&array, &new_type).unwrap();
    assert_eq!(new_type, new_array.data_type().clone());
    let map_array = new_array.as_map();

    assert_ne!(new_type, array.data_type().clone());
    assert_eq!(new_type, map_array.data_type().clone());

    let key_string = map_array
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(&key_string, &vec!["0", "1"]);

    let values_string_array = cast(map_array.values(), &DataType::Utf8).unwrap();
    let values_string = values_string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(&values_string, &vec!["test_val_1", "test_val_2"]);

    assert_eq!(
        map_array.nulls(),
        Some(&NullBuffer::from(vec![true, true, false]))
    );
}

#[test]
fn test_cast_map_contained_values() {
    let string_builder = StringBuilder::new();
    let value_builder = Int8Builder::new();
    let mut builder = MapBuilder::new(None, string_builder, value_builder);

    builder.keys().append_value("0");
    builder.values().append_value(44);
    builder.append(true).unwrap();
    builder.keys().append_value("1");
    builder.values().append_value(22);
    builder.append(true).unwrap();

    let array = builder.finish();

    let new_type = DataType::Map(
        Arc::new(Field::new(
            Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
            DataType::Struct(
                vec![
                    Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                    Field::new(Field::MAP_VALUE_FIELD_DEFAULT_NAME, DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );

    let new_array = cast(&array, &new_type).unwrap();
    assert_eq!(new_type, new_array.data_type().clone());
    let map_array = new_array.as_map();

    assert_ne!(new_type, array.data_type().clone());
    assert_eq!(new_type, map_array.data_type().clone());

    let key_string = map_array
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(&key_string, &vec!["0", "1"]);

    let values_string_array = cast(map_array.values(), &DataType::Utf8).unwrap();
    let values_string = values_string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(&values_string, &vec!["44", "22"]);
}
