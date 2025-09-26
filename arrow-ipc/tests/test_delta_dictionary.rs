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

use arrow_array::{
    Array, ArrayRef, DictionaryArray, ListArray, RecordBatch, StringArray,
    builder::{ListBuilder, PrimitiveDictionaryBuilder, StringDictionaryBuilder},
};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::{DictionaryHandling, IpcWriteOptions, StreamWriter};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use std::io::Cursor;
use std::sync::Arc;

#[test]
fn test_dictionary_handling_option() {
    // Test that DictionaryHandling can be set
    let _options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);

    // Verify it was set (we can't access private field directly)
    // This test just verifies the API exists
}

#[test]
fn test_nested_dictionary_with_delta() -> Result<(), ArrowError> {
    // Test writing nested dictionaries with delta option
    // Create a simple nested structure for testing

    // Create dictionary arrays
    let mut dict_builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    dict_builder.append_value("hello");
    dict_builder.append_value("world");
    let dict_array = dict_builder.finish();

    // Create a list of dictionaries
    let mut list_builder =
        ListBuilder::new(StringDictionaryBuilder::<arrow_array::types::Int32Type>::new());
    list_builder.values().append_value("item1");
    list_builder.values().append_value("item2");
    list_builder.append(true);
    list_builder.values().append_value("item3");
    list_builder.append(true);
    let list_array = list_builder.finish();

    // Create schema with nested dictionaries
    let schema = Arc::new(Schema::new(vec![
        Field::new("dict", dict_array.data_type().clone(), true),
        Field::new("list_of_dict", list_array.data_type().clone(), true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(dict_array) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )?;

    // Write with delta dictionary handling
    let mut buffer = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    // Read back and verify
    let reader = StreamReader::try_new(Cursor::new(buffer), None)?;
    let read_batches: Result<Vec<_>, _> = reader.collect();
    let read_batches = read_batches?;
    assert_eq!(read_batches.len(), 1);

    let read_batch = &read_batches[0];
    assert_eq!(read_batch.num_columns(), 2);
    assert_eq!(read_batch.num_rows(), 2);
    let dict_array = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();
    let dict_values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(dict_values.len(), 2);
    assert_eq!(dict_values.value(0), "hello");
    assert_eq!(dict_values.value(1), "world");
    let list_array = read_batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let list_dict_array = list_array
        .values()
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();
    let list_values = list_dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(list_values.len(), 3);
    assert_eq!(list_values.value(0), "item1");
    assert_eq!(list_values.value(1), "item2");
    assert_eq!(list_values.value(2), "item3");

    Ok(())
}

#[test]
fn test_complex_nested_dictionaries() -> Result<(), ArrowError> {
    // Test nested structure with dictionaries at multiple levels

    // Create a nested structure: List(Dictionary(List(Dictionary)))

    // Inner dictionary for the nested list
    let _inner_dict_field = Field::new(
        "inner_item",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    );

    // Create a list of dictionaries
    let mut list_builder =
        ListBuilder::new(StringDictionaryBuilder::<arrow_array::types::Int32Type>::new());

    // First list
    list_builder.values().append_value("inner_a");
    list_builder.values().append_value("inner_b");
    list_builder.append(true);

    // Second list
    list_builder.values().append_value("inner_c");
    list_builder.values().append_value("inner_d");
    list_builder.append(true);

    let list_array = list_builder.finish();

    // Create outer dictionary containing the list
    let mut outer_dict_builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    outer_dict_builder.append_value("outer_1");
    outer_dict_builder.append_value("outer_2");
    let outer_dict = outer_dict_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new("outer_dict", outer_dict.data_type().clone(), true),
        Field::new("nested_list", list_array.data_type().clone(), true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(outer_dict) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    )?;

    // Write with delta dictionary handling
    let mut buffer = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    // Verify it writes without error
    assert!(!buffer.is_empty());

    // Read back and verify
    let reader = StreamReader::try_new(Cursor::new(buffer), None)?;
    let read_batches: Result<Vec<_>, _> = reader.collect();
    let read_batches = read_batches?;

    assert_eq!(read_batches.len(), 1);

    let read_batch = &read_batches[0];
    assert_eq!(read_batch.num_columns(), 2);
    assert_eq!(read_batch.num_rows(), 2);
    let outer_dict_array = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();
    let outer_dict_values = outer_dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(outer_dict_values.len(), 2);
    assert_eq!(outer_dict_values.value(0), "outer_1");
    assert_eq!(outer_dict_values.value(1), "outer_2");

    let nested_list_array = read_batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let nested_dict_array = nested_list_array
        .values()
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();
    let nested_dict_values = nested_dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(nested_dict_values.len(), 4);
    assert_eq!(nested_dict_values.value(0), "inner_a");
    assert_eq!(nested_dict_values.value(1), "inner_b");
    assert_eq!(nested_dict_values.value(2), "inner_c");
    assert_eq!(nested_dict_values.value(3), "inner_d");

    Ok(())
}

#[test]
fn test_multiple_dictionary_types() -> Result<(), ArrowError> {
    // Test different dictionary value types in one schema

    // String dictionary
    let mut string_dict_builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    string_dict_builder.append_value("apple");
    string_dict_builder.append_value("banana");
    string_dict_builder.append_value("apple");
    let string_dict = string_dict_builder.finish();

    // Integer dictionary
    let mut int_dict_builder = PrimitiveDictionaryBuilder::<
        arrow_array::types::Int32Type,
        arrow_array::types::Int64Type,
    >::new();
    int_dict_builder.append_value(100);
    int_dict_builder.append_value(200);
    int_dict_builder.append_value(100);
    let int_dict = int_dict_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new("string_dict", string_dict.data_type().clone(), true),
        Field::new("int_dict", int_dict.data_type().clone(), true),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(string_dict) as ArrayRef,
            Arc::new(int_dict) as ArrayRef,
        ],
    )?;

    // Create second batch with extended dictionaries
    let mut string_dict_builder2 = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    string_dict_builder2.append_value("apple");
    string_dict_builder2.append_value("banana");
    string_dict_builder2.append_value("cherry"); // new
    string_dict_builder2.append_value("date"); // new
    let string_dict2 = string_dict_builder2.finish();

    let mut int_dict_builder2 = PrimitiveDictionaryBuilder::<
        arrow_array::types::Int32Type,
        arrow_array::types::Int64Type,
    >::new();
    int_dict_builder2.append_value(100);
    int_dict_builder2.append_value(200);
    int_dict_builder2.append_value(300); // new
    int_dict_builder2.append_value(400); // new
    let int_dict2 = int_dict_builder2.finish();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(string_dict2) as ArrayRef,
            Arc::new(int_dict2) as ArrayRef,
        ],
    )?;

    // Write with delta dictionary handling
    let mut buffer = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)?;
        writer.write(&batch1)?;
        writer.write(&batch2)?;
        writer.finish()?;
    }

    // Read back and verify
    let reader = StreamReader::try_new(Cursor::new(buffer), None)?;
    let read_batches: Result<Vec<_>, _> = reader.collect();
    let read_batches = read_batches?;

    assert_eq!(read_batches.len(), 2);

    // Check string dictionary in second batch
    let read_batch2 = &read_batches[1];
    let string_dict_array = read_batch2
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();

    let string_values = string_dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Should have all 4 string values
    assert_eq!(string_values.len(), 4);
    assert_eq!(string_values.value(0), "apple");
    assert_eq!(string_values.value(1), "banana");
    assert_eq!(string_values.value(2), "cherry");
    assert_eq!(string_values.value(3), "date");

    Ok(())
}

#[test]
fn test_empty_dictionary_delta() -> Result<(), ArrowError> {
    // Test edge case with empty dictionaries

    // First batch with empty dictionary
    let mut builder1 = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    builder1.append_null();
    builder1.append_null();
    let array1 = builder1.finish();

    // Second batch with some values
    let mut builder2 = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    builder2.append_value("first");
    builder2.append_value("second");
    let array2 = builder2.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        array1.data_type().clone(),
        true,
    )]));

    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1) as ArrayRef])?;

    let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array2) as ArrayRef])?;

    // Write with delta dictionary handling
    let mut buffer = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)?;
        writer.write(&batch1)?;
        writer.write(&batch2)?;
        writer.finish()?;
    }

    // Read back and verify
    let reader = StreamReader::try_new(Cursor::new(buffer), None)?;
    let read_batches: Result<Vec<_>, _> = reader.collect();
    let read_batches = read_batches?;

    assert_eq!(read_batches.len(), 2);

    // Second batch should have the dictionary values
    let read_batch2 = &read_batches[1];
    let dict_array = read_batch2
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();

    let dict_values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(dict_values.len(), 2);
    assert_eq!(dict_values.value(0), "first");
    assert_eq!(dict_values.value(1), "second");

    Ok(())
}

#[test]
fn test_delta_with_shared_dictionary_data() -> Result<(), ArrowError> {
    // Test efficient delta detection when dictionaries share underlying data

    // Create initial dictionary
    let mut builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    builder.append_value("alpha");
    builder.append_value("beta");
    let dict1 = builder.finish();

    // Create a dictionary that extends the first one by sharing its data
    // This simulates a common pattern where dictionaries are built incrementally
    let dict1_values = dict1.values();
    let mut builder2 = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    // First, add the existing values
    for i in 0..dict1_values.len() {
        builder2.append_value(
            dict1_values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i),
        );
    }
    // Then add new values
    builder2.append_value("gamma");
    builder2.append_value("delta");
    let dict2 = builder2.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        dict1.data_type().clone(),
        true,
    )]));

    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict1) as ArrayRef])?;

    let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict2) as ArrayRef])?;

    // Write with delta dictionary handling
    let mut buffer = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)?;
        writer.write(&batch1)?;
        writer.write(&batch2)?;
        writer.finish()?;
    }

    // Read back and verify delta was used correctly
    let reader = StreamReader::try_new(Cursor::new(buffer), None)?;
    let read_batches: Result<Vec<_>, _> = reader.collect();
    let read_batches = read_batches?;

    assert_eq!(read_batches.len(), 2);

    // Verify second batch has all values
    let read_batch2 = &read_batches[1];
    let dict_array = read_batch2
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();

    let dict_values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(dict_values.len(), 4);
    assert_eq!(dict_values.value(0), "alpha");
    assert_eq!(dict_values.value(1), "beta");
    assert_eq!(dict_values.value(2), "gamma");
    assert_eq!(dict_values.value(3), "delta");

    Ok(())
}

#[test]
fn test_large_dictionary_delta_performance() -> Result<(), ArrowError> {
    // Test delta dictionary with large dictionaries to ensure efficiency

    // Create a large initial dictionary
    let mut builder1 = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    for i in 0..1000 {
        builder1.append_value(format!("value_{i}"));
    }
    let dict1 = builder1.finish();

    // Create extended dictionary
    let mut builder2 = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    for i in 0..1000 {
        builder2.append_value(format!("value_{i}"));
    }
    // Add just a few new values
    for i in 1000..1005 {
        builder2.append_value(format!("value_{i}"));
    }
    let dict2 = builder2.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        dict1.data_type().clone(),
        true,
    )]));

    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict1) as ArrayRef])?;

    let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict2) as ArrayRef])?;

    // Write with delta dictionary handling
    let mut buffer = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)?;
        writer.write(&batch1)?;
        writer.write(&batch2)?;
        writer.finish()?;
    }

    // The buffer should be relatively small since we only sent 5 new values
    // as delta instead of resending all 1005 values
    let buffer_size = buffer.len();

    // Write without delta for comparison
    let mut buffer_no_delta = Vec::new();
    {
        let options =
            IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Resend);
        let mut writer =
            StreamWriter::try_new_with_options(&mut buffer_no_delta, &schema, options)?;
        writer.write(&batch1)?;
        writer.write(&batch2)?;
        writer.finish()?;
    }

    let buffer_no_delta_size = buffer_no_delta.len();

    // Delta encoding should result in smaller output
    println!("Delta buffer size: {buffer_size}");
    println!("Non-delta buffer size: {buffer_size}");

    // Delta encoding should result in significantly smaller output
    assert!(
        buffer_size < buffer_no_delta_size,
        "Delta buffer ({buffer_size}) should be smaller than non-delta buffer ({buffer_no_delta_size})"
    );

    // The delta should save approximately the size of the second dictionary minus the delta
    // We sent 5 values instead of 1005, saving ~99.5% on the second dictionary
    let savings_ratio = (buffer_no_delta_size - buffer_size) as f64 / buffer_no_delta_size as f64;
    println!("Space savings: {:.1}%", savings_ratio * 100.0);

    // We should save at least 30% (conservative estimate accounting for metadata overhead)
    assert!(
        savings_ratio > 0.30,
        "Delta encoding should provide significant space savings (got {:.1}%)",
        savings_ratio * 100.0
    );

    // Verify correctness
    let reader = StreamReader::try_new(Cursor::new(buffer), None)?;
    let read_batches: Result<Vec<_>, _> = reader.collect();
    let read_batches = read_batches?;

    assert_eq!(read_batches.len(), 2);

    let read_batch2 = &read_batches[1];
    let dict_array = read_batch2
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();

    let dict_values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(dict_values.len(), 1005);
    assert_eq!(dict_values.value(1004), "value_1004");

    Ok(())
}
