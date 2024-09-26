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

//! Utilities for pretty printing [`RecordBatch`]es and [`Array`]s.
//!
//! Note this module is not available unless `feature = "prettyprint"` is enabled.
//!
//! [`RecordBatch`]: arrow_array::RecordBatch
//! [`Array`]: arrow_array::Array

use std::fmt::Display;

use comfy_table::{Cell, Table};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::ArrowError;

use crate::display::{ArrayFormatter, FormatOptions};

/// Create a visual representation of record batches
pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<impl Display, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    pretty_format_batches_with_options(results, &options)
}

/// Create a visual representation of record batches
pub fn pretty_format_batches_with_options(
    results: &[RecordBatch],
    options: &FormatOptions,
) -> Result<impl Display, ArrowError> {
    create_table(results, options)
}

/// Create a visual representation of columns
pub fn pretty_format_columns(
    col_name: &str,
    results: &[ArrayRef],
) -> Result<impl Display, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    pretty_format_columns_with_options(col_name, results, &options)
}

/// Utility function to create a visual representation of columns with options
fn pretty_format_columns_with_options(
    col_name: &str,
    results: &[ArrayRef],
    options: &FormatOptions,
) -> Result<impl Display, ArrowError> {
    create_column(col_name, results, options)
}

/// Prints a visual representation of record batches to stdout
pub fn print_batches(results: &[RecordBatch]) -> Result<(), ArrowError> {
    println!("{}", pretty_format_batches(results)?);
    Ok(())
}

/// Prints a visual representation of a list of column to stdout
pub fn print_columns(col_name: &str, results: &[ArrayRef]) -> Result<(), ArrowError> {
    println!("{}", pretty_format_columns(col_name, results)?);
    Ok(())
}

/// Convert a series of record batches into a table
fn create_table(results: &[RecordBatch], options: &FormatOptions) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for batch in results {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for formatter in &formatters {
                cells.push(Cell::new(formatter.value(row)));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

fn create_column(
    field: &str,
    columns: &[ArrayRef],
    options: &FormatOptions,
) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if columns.is_empty() {
        return Ok(table);
    }

    let header = vec![Cell::new(field)];
    table.set_header(header);

    for col in columns {
        let formatter = ArrayFormatter::try_new(col.as_ref(), options)?;
        for row in 0..col.len() {
            let cells = vec![Cell::new(formatter.value(row))];
            table.add_row(cells);
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;

    use half::f16;

    use arrow_array::builder::*;
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, ScalarBuffer};
    use arrow_schema::*;

    use crate::display::array_value_to_string;

    use super::*;

    #[test]
    fn test_pretty_format_batches() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(array::StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("d"),
                ])),
                Arc::new(array::Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(10),
                    Some(100),
                ])),
            ],
        )
        .unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+---+-----+",
            "| a | b   |",
            "+---+-----+",
            "| a | 1   |",
            "| b |     |",
            "|   | 10  |",
            "| d | 100 |",
            "+---+-----+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_columns() {
        let columns = vec![
            Arc::new(array::StringArray::from(vec![
                Some("a"),
                Some("b"),
                None,
                Some("d"),
            ])) as ArrayRef,
            Arc::new(array::StringArray::from(vec![Some("e"), None, Some("g")])),
        ];

        let table = pretty_format_columns("a", &columns).unwrap().to_string();

        let expected = vec![
            "+---+", "| a |", "+---+", "| a |", "| b |", "|   |", "| d |", "| e |", "|   |",
            "| g |", "+---+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Null, true),
        ]));

        let num_rows = 4;
        let arrays = schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), num_rows))
            .collect();

        // define data (null)
        let batch = RecordBatch::try_new(schema, arrays).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "|   |   |   |",
            "|   |   |   |",
            "|   |   |   |",
            "|   |   |   |",
            "+---+---+---+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table:#?}");
    }

    #[test]
    fn test_pretty_format_dictionary() {
        // define a schema.
        let field = Field::new_dictionary("d1", DataType::Int32, DataType::Utf8, true);
        let schema = Arc::new(Schema::new(vec![field]));

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();

        builder.append_value("one");
        builder.append_null();
        builder.append_value("three");
        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+-------+",
            "| d1    |",
            "+-------+",
            "| one   |",
            "|       |",
            "| three |",
            "+-------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_fixed_size_list() {
        // define a schema.
        let field_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3);
        let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

        let keys_builder = Int32Array::builder(3);
        let mut builder = FixedSizeListBuilder::new(keys_builder, 3);

        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5, 6]);
        builder.append(false);
        builder.values().append_slice(&[7, 8, 9]);
        builder.append(true);

        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let expected = vec![
            "+-----------+",
            "| d1        |",
            "+-----------+",
            "| [1, 2, 3] |",
            "|           |",
            "| [7, 8, 9] |",
            "+-----------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_string_view() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d1",
            DataType::Utf8View,
            true,
        )]));

        // Use a small capacity so we end up with multiple views
        let mut builder = StringViewBuilder::with_capacity(20);
        builder.append_value("hello");
        builder.append_null();
        builder.append_value("longer than 12 bytes");
        builder.append_value("another than 12 bytes");
        builder.append_null();
        builder.append_value("small");

        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let expected = vec![
            "+-----------------------+",
            "| d1                    |",
            "+-----------------------+",
            "| hello                 |",
            "|                       |",
            "| longer than 12 bytes  |",
            "| another than 12 bytes |",
            "|                       |",
            "| small                 |",
            "+-----------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table:#?}");
    }

    #[test]
    fn test_pretty_format_binary_view() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d1",
            DataType::BinaryView,
            true,
        )]));

        // Use a small capacity so we end up with multiple views
        let mut builder = BinaryViewBuilder::with_capacity(20);
        builder.append_value(b"hello");
        builder.append_null();
        builder.append_value(b"longer than 12 bytes");
        builder.append_value(b"another than 12 bytes");
        builder.append_null();
        builder.append_value(b"small");

        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let expected = vec![
            "+--------------------------------------------+",
            "| d1                                         |",
            "+--------------------------------------------+",
            "| 68656c6c6f                                 |",
            "|                                            |",
            "| 6c6f6e676572207468616e203132206279746573   |",
            "| 616e6f74686572207468616e203132206279746573 |",
            "|                                            |",
            "| 736d616c6c                                 |",
            "+--------------------------------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n\n{table:#?}");
    }

    #[test]
    fn test_pretty_format_fixed_size_binary() {
        // define a schema.
        let field_type = DataType::FixedSizeBinary(3);
        let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 3);

        builder.append_value([1, 2, 3]).unwrap();
        builder.append_null();
        builder.append_value([7, 8, 9]).unwrap();

        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let expected = vec![
            "+--------+",
            "| d1     |",
            "+--------+",
            "| 010203 |",
            "|        |",
            "| 070809 |",
            "+--------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    /// Generate an array with type $ARRAYTYPE with a numeric value of
    /// $VALUE, and compare $EXPECTED_RESULT to the output of
    /// formatting that array with `pretty_format_batches`
    macro_rules! check_datetime {
        ($ARRAYTYPE:ident, $VALUE:expr, $EXPECTED_RESULT:expr) => {
            let mut builder = $ARRAYTYPE::builder(10);
            builder.append_value($VALUE);
            builder.append_null();
            let array = builder.finish();

            let schema = Arc::new(Schema::new(vec![Field::new(
                "f",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

            let table = pretty_format_batches(&[batch])
                .expect("formatting batches")
                .to_string();

            let expected = $EXPECTED_RESULT;
            let actual: Vec<&str> = table.lines().collect();

            assert_eq!(expected, actual, "Actual result:\n\n{actual:#?}\n\n");
        };
    }

    fn timestamp_batch<T: ArrowTimestampType>(timezone: &str, value: T::Native) -> RecordBatch {
        let mut builder = PrimitiveBuilder::<T>::with_capacity(10);
        builder.append_value(value);
        builder.append_null();
        let array = builder.finish();
        let array = array.with_timezone(timezone);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_pretty_format_timestamp_second_with_fixed_offset_timezone() {
        let batch = timestamp_batch::<TimestampSecondType>("+08:00", 11111111);
        let table = pretty_format_batches(&[batch]).unwrap().to_string();

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
    fn test_pretty_format_timestamp_second() {
        let expected = vec![
            "+---------------------+",
            "| f                   |",
            "+---------------------+",
            "| 1970-05-09T14:25:11 |",
            "|                     |",
            "+---------------------+",
        ];
        check_datetime!(TimestampSecondArray, 11111111, expected);
    }

    #[test]
    fn test_pretty_format_timestamp_millisecond() {
        let expected = vec![
            "+-------------------------+",
            "| f                       |",
            "+-------------------------+",
            "| 1970-01-01T03:05:11.111 |",
            "|                         |",
            "+-------------------------+",
        ];
        check_datetime!(TimestampMillisecondArray, 11111111, expected);
    }

    #[test]
    fn test_pretty_format_timestamp_microsecond() {
        let expected = vec![
            "+----------------------------+",
            "| f                          |",
            "+----------------------------+",
            "| 1970-01-01T00:00:11.111111 |",
            "|                            |",
            "+----------------------------+",
        ];
        check_datetime!(TimestampMicrosecondArray, 11111111, expected);
    }

    #[test]
    fn test_pretty_format_timestamp_nanosecond() {
        let expected = vec![
            "+-------------------------------+",
            "| f                             |",
            "+-------------------------------+",
            "| 1970-01-01T00:00:00.011111111 |",
            "|                               |",
            "+-------------------------------+",
        ];
        check_datetime!(TimestampNanosecondArray, 11111111, expected);
    }

    #[test]
    fn test_pretty_format_date_32() {
        let expected = vec![
            "+------------+",
            "| f          |",
            "+------------+",
            "| 1973-05-19 |",
            "|            |",
            "+------------+",
        ];
        check_datetime!(Date32Array, 1234, expected);
    }

    #[test]
    fn test_pretty_format_date_64() {
        let expected = vec![
            "+---------------------+",
            "| f                   |",
            "+---------------------+",
            "| 2005-03-18T01:58:20 |",
            "|                     |",
            "+---------------------+",
        ];
        check_datetime!(Date64Array, 1111111100000, expected);
    }

    #[test]
    fn test_pretty_format_time_32_second() {
        let expected = vec![
            "+----------+",
            "| f        |",
            "+----------+",
            "| 00:18:31 |",
            "|          |",
            "+----------+",
        ];
        check_datetime!(Time32SecondArray, 1111, expected);
    }

    #[test]
    fn test_pretty_format_time_32_millisecond() {
        let expected = vec![
            "+--------------+",
            "| f            |",
            "+--------------+",
            "| 03:05:11.111 |",
            "|              |",
            "+--------------+",
        ];
        check_datetime!(Time32MillisecondArray, 11111111, expected);
    }

    #[test]
    fn test_pretty_format_time_64_microsecond() {
        let expected = vec![
            "+-----------------+",
            "| f               |",
            "+-----------------+",
            "| 00:00:11.111111 |",
            "|                 |",
            "+-----------------+",
        ];
        check_datetime!(Time64MicrosecondArray, 11111111, expected);
    }

    #[test]
    fn test_pretty_format_time_64_nanosecond() {
        let expected = vec![
            "+--------------------+",
            "| f                  |",
            "+--------------------+",
            "| 00:00:00.011111111 |",
            "|                    |",
            "+--------------------+",
        ];
        check_datetime!(Time64NanosecondArray, 11111111, expected);
    }

    #[test]
    fn test_int_display() {
        let array = Arc::new(Int32Array::from(vec![6, 3])) as ArrayRef;
        let actual_one = array_value_to_string(&array, 0).unwrap();
        let expected_one = "6";

        let actual_two = array_value_to_string(&array, 1).unwrap();
        let expected_two = "3";
        assert_eq!(actual_one, expected_one);
        assert_eq!(actual_two, expected_two);
    }

    #[test]
    fn test_decimal_display() {
        let precision = 10;
        let scale = 2;

        let array = [Some(101), None, Some(200), Some(3040)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
            .unwrap();

        let dm = Arc::new(array) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            dm.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![dm]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+-------+",
            "| f     |",
            "+-------+",
            "| 1.01  |",
            "|       |",
            "| 2.00  |",
            "| 30.40 |",
            "+-------+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_decimal_display_zero_scale() {
        let precision = 5;
        let scale = 0;

        let array = [Some(101), None, Some(200), Some(3040)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
            .unwrap();

        let dm = Arc::new(array) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            dm.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![dm]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let expected = vec![
            "+------+", "| f    |", "+------+", "| 101  |", "|      |", "| 200  |", "| 3040 |",
            "+------+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_struct() {
        let schema = Schema::new(vec![
            Field::new_struct(
                "c1",
                vec![
                    Field::new("c11", DataType::Int32, true),
                    Field::new_struct(
                        "c12",
                        vec![Field::new("c121", DataType::Utf8, false)],
                        false,
                    ),
                ],
                false,
            ),
            Field::new("c2", DataType::Utf8, false),
        ]);

        let c1 = StructArray::from(vec![
            (
                Arc::new(Field::new("c11", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(5)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new_struct(
                    "c12",
                    vec![Field::new("c121", DataType::Utf8, false)],
                    false,
                )),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("c121", DataType::Utf8, false)),
                    Arc::new(StringArray::from(vec![Some("e"), Some("f"), Some("g")])) as ArrayRef,
                )])) as ArrayRef,
            ),
        ]);
        let c2 = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let expected = vec![
            "+--------------------------+----+",
            "| c1                       | c2 |",
            "+--------------------------+----+",
            "| {c11: 1, c12: {c121: e}} | a  |",
            "| {c11: , c12: {c121: f}}  | b  |",
            "| {c11: 5, c12: {c121: g}} | c  |",
            "+--------------------------+----+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_dense_union() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.2234).unwrap();
        builder.append_null::<Float64Type>("b").unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        let union = builder.build().unwrap();

        let schema = Schema::new(vec![Field::new_union(
            "Teamsters",
            vec![0, 1],
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ],
            UnionMode::Dense,
        )]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(union)]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();
        let expected = vec![
            "+------------+",
            "| Teamsters  |",
            "+------------+",
            "| {a=1}      |",
            "| {b=3.2234} |",
            "| {b=}       |",
            "| {a=}       |",
            "+------------+",
        ];

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_pretty_format_sparse_union() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.2234).unwrap();
        builder.append_null::<Float64Type>("b").unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        let union = builder.build().unwrap();

        let schema = Schema::new(vec![Field::new_union(
            "Teamsters",
            vec![0, 1],
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ],
            UnionMode::Sparse,
        )]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(union)]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();
        let expected = vec![
            "+------------+",
            "| Teamsters  |",
            "+------------+",
            "| {a=1}      |",
            "| {b=3.2234} |",
            "| {b=}       |",
            "| {a=}       |",
            "+------------+",
        ];

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_pretty_format_nested_union() {
        //Inner UnionArray
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("b", 1).unwrap();
        builder.append::<Float64Type>("c", 3.2234).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append_null::<Int32Type>("b").unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        let inner = builder.build().unwrap();

        let inner_field = Field::new_union(
            "European Union",
            vec![0, 1],
            vec![
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Float64, false),
            ],
            UnionMode::Dense,
        );

        // Can't use UnionBuilder with non-primitive types, so manually build outer UnionArray
        let a_array = Int32Array::from(vec![None, None, None, Some(1234), Some(23)]);
        let type_ids = [1, 1, 0, 0, 1].into_iter().collect::<ScalarBuffer<i8>>();

        let children = vec![Arc::new(a_array) as Arc<dyn Array>, Arc::new(inner)];

        let union_fields = [
            (0, Arc::new(Field::new("a", DataType::Int32, true))),
            (1, Arc::new(inner_field.clone())),
        ]
        .into_iter()
        .collect();

        let outer = UnionArray::try_new(union_fields, type_ids, None, children).unwrap();

        let schema = Schema::new(vec![Field::new_union(
            "Teamsters",
            vec![0, 1],
            vec![Field::new("a", DataType::Int32, true), inner_field],
            UnionMode::Sparse,
        )]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(outer)]).unwrap();
        let table = pretty_format_batches(&[batch]).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();
        let expected = vec![
            "+-----------------------------+",
            "| Teamsters                   |",
            "+-----------------------------+",
            "| {European Union={b=1}}      |",
            "| {European Union={c=3.2234}} |",
            "| {a=}                        |",
            "| {a=1234}                    |",
            "| {European Union={c=}}       |",
            "+-----------------------------+",
        ];
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_writing_formatted_batches() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(array::StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("d"),
                ])),
                Arc::new(array::Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(10),
                    Some(100),
                ])),
            ],
        )
        .unwrap();

        let mut buf = String::new();
        write!(&mut buf, "{}", pretty_format_batches(&[batch]).unwrap()).unwrap();

        let s = [
            "+---+-----+",
            "| a | b   |",
            "+---+-----+",
            "| a | 1   |",
            "| b |     |",
            "|   | 10  |",
            "| d | 100 |",
            "+---+-----+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_float16_display() {
        let values = vec![
            Some(f16::from_f32(f32::NAN)),
            Some(f16::from_f32(4.0)),
            Some(f16::from_f32(f32::NEG_INFINITY)),
        ];
        let array = Arc::new(values.into_iter().collect::<Float16Array>()) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f16",
            array.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+------+", "| f16  |", "+------+", "| NaN  |", "| 4    |", "| -inf |", "+------+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_interval_day_time() {
        let arr = Arc::new(arrow_array::IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(-1, -600_000)),
            Some(IntervalDayTime::new(0, -1001)),
            Some(IntervalDayTime::new(0, -1)),
            Some(IntervalDayTime::new(0, 1)),
            Some(IntervalDayTime::new(0, 10)),
            Some(IntervalDayTime::new(0, 100)),
        ]));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "IntervalDayTime",
            arr.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+------------------+",
            "| IntervalDayTime  |",
            "+------------------+",
            "| -1 days -10 mins |",
            "| -1.001 secs      |",
            "| -0.001 secs      |",
            "| 0.001 secs       |",
            "| 0.010 secs       |",
            "| 0.100 secs       |",
            "+------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_pretty_format_interval_month_day_nano_array() {
        let arr = Arc::new(arrow_array::IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(-1, -1, -600_000_000_000)),
            Some(IntervalMonthDayNano::new(0, 0, -1_000_000_001)),
            Some(IntervalMonthDayNano::new(0, 0, -1)),
            Some(IntervalMonthDayNano::new(0, 0, 1)),
            Some(IntervalMonthDayNano::new(0, 0, 10)),
            Some(IntervalMonthDayNano::new(0, 0, 100)),
            Some(IntervalMonthDayNano::new(0, 0, 1_000)),
            Some(IntervalMonthDayNano::new(0, 0, 10_000)),
            Some(IntervalMonthDayNano::new(0, 0, 100_000)),
            Some(IntervalMonthDayNano::new(0, 0, 1_000_000)),
            Some(IntervalMonthDayNano::new(0, 0, 10_000_000)),
            Some(IntervalMonthDayNano::new(0, 0, 100_000_000)),
            Some(IntervalMonthDayNano::new(0, 0, 1_000_000_000)),
        ]));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "IntervalMonthDayNano",
            arr.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let table = pretty_format_batches(&[batch]).unwrap().to_string();

        let expected = vec![
            "+--------------------------+",
            "| IntervalMonthDayNano     |",
            "+--------------------------+",
            "| -1 mons -1 days -10 mins |",
            "| -1.000000001 secs        |",
            "| -0.000000001 secs        |",
            "| 0.000000001 secs         |",
            "| 0.000000010 secs         |",
            "| 0.000000100 secs         |",
            "| 0.000001000 secs         |",
            "| 0.000010000 secs         |",
            "| 0.000100000 secs         |",
            "| 0.001000000 secs         |",
            "| 0.010000000 secs         |",
            "| 0.100000000 secs         |",
            "| 1.000000000 secs         |",
            "+--------------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{table}");
    }

    #[test]
    fn test_format_options() {
        let options = FormatOptions::default().with_null("null");
        let array = Int32Array::from(vec![Some(1), Some(2), None, Some(3), Some(4)]);
        let batch = RecordBatch::try_from_iter([("my_column_name", Arc::new(array) as _)]).unwrap();

        let column = pretty_format_columns_with_options(
            "my_column_name",
            &[batch.column(0).clone()],
            &options,
        )
        .unwrap()
        .to_string();

        let batch = pretty_format_batches_with_options(&[batch], &options)
            .unwrap()
            .to_string();

        let expected = vec![
            "+----------------+",
            "| my_column_name |",
            "+----------------+",
            "| 1              |",
            "| 2              |",
            "| null           |",
            "| 3              |",
            "| 4              |",
            "+----------------+",
        ];

        let actual: Vec<&str> = column.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{column}");

        let actual: Vec<&str> = batch.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{batch}");
    }
}
