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

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, SchemaRef};
use comfy_table::{Cell, Table};
use std::fmt::Display;

use crate::display::{ArrayFormatter, FormatOptions, make_array_formatter};

/// Create a visual representation of [`RecordBatch`]es
///
/// Uses default values for display. See [`pretty_format_batches_with_options`]
/// for more control.
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
/// # use arrow_cast::pretty::pretty_format_batches;
/// # let batch = RecordBatch::try_from_iter(vec![
/// #       ("a", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef),
/// #       ("b", Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, Some("d"), Some("e")]))),
/// # ]).unwrap();
/// // Note, returned object implements `Display`
/// let pretty_table = pretty_format_batches(&[batch]).unwrap();
/// let table_str = format!("Batches:\n{pretty_table}");
/// assert_eq!(table_str,
/// r#"Batches:
/// +---+---+
/// | a | b |
/// +---+---+
/// | 1 | a |
/// | 2 | b |
/// | 3 |   |
/// | 4 | d |
/// | 5 | e |
/// +---+---+"#);
/// ```
pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<impl Display + use<>, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    pretty_format_batches_with_options(results, &options)
}

/// Create a visual representation of [`RecordBatch`]es with a provided schema.
///
/// Useful to display empty batches.
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
/// # use arrow_cast::pretty::pretty_format_batches_with_schema;
/// # use arrow_schema::{DataType, Field, Schema};
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("a", DataType::Int32, false),
///     Field::new("b", DataType::Utf8, true),
/// ]));
/// // Note, returned object implements `Display`
/// let pretty_table = pretty_format_batches_with_schema(schema, &[]).unwrap();
/// let table_str = format!("Batches:\n{pretty_table}");
/// assert_eq!(table_str,
/// r#"Batches:
/// +---+---+
/// | a | b |
/// +---+---+
/// +---+---+"#);
/// ```
pub fn pretty_format_batches_with_schema(
    schema: SchemaRef,
    results: &[RecordBatch],
) -> Result<impl Display + use<>, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    create_table(Some(schema), results, &options)
}

/// Create a visual representation of [`RecordBatch`]es with formatting options.
///
/// # Arguments
/// * `results` - A slice of record batches to display
/// * `options` - [`FormatOptions`] that control the resulting display
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
/// # use arrow_cast::display::FormatOptions;
/// # use arrow_cast::pretty::{pretty_format_batches, pretty_format_batches_with_options};
/// # let batch = RecordBatch::try_from_iter(vec![
/// #       ("a", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
/// #       ("b", Arc::new(StringArray::from(vec![Some("a"), None]))),
/// # ]).unwrap();
/// let options = FormatOptions::new()
///   .with_null("<NULL>");
/// // Note, returned object implements `Display`
/// let pretty_table = pretty_format_batches_with_options(&[batch], &options).unwrap();
/// let table_str = format!("Batches:\n{pretty_table}");
/// assert_eq!(table_str,
/// r#"Batches:
/// +---+--------+
/// | a | b      |
/// +---+--------+
/// | 1 | a      |
/// | 2 | <NULL> |
/// +---+--------+"#);
/// ```
pub fn pretty_format_batches_with_options(
    results: &[RecordBatch],
    options: &FormatOptions,
) -> Result<impl Display + use<>, ArrowError> {
    create_table(None, results, options)
}

/// Create a visual representation of [`ArrayRef`]
///
/// Uses default values for display. See [`pretty_format_columns_with_options`]
///
/// See [`pretty_format_batches`] for an example
pub fn pretty_format_columns(
    col_name: &str,
    results: &[ArrayRef],
) -> Result<impl Display + use<>, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    pretty_format_columns_with_options(col_name, results, &options)
}

/// Create a visual representation of [`ArrayRef`] with formatting options.
///
/// See [`pretty_format_batches_with_options`] for an example
pub fn pretty_format_columns_with_options(
    col_name: &str,
    results: &[ArrayRef],
    options: &FormatOptions,
) -> Result<impl Display + use<>, ArrowError> {
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
fn create_table(
    schema_opt: Option<SchemaRef>,
    results: &[RecordBatch],
    options: &FormatOptions,
) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    let schema_opt = schema_opt.or_else(|| {
        if results.is_empty() {
            None
        } else {
            Some(results[0].schema())
        }
    });

    if let Some(schema) = &schema_opt {
        let mut header = Vec::new();
        for field in schema.fields() {
            if options.types_info() {
                header.push(Cell::new(format!(
                    "{}\n{}",
                    field.name(),
                    field.data_type()
                )))
            } else {
                header.push(Cell::new(field.name()));
            }
        }
        table.set_header(header);
    }

    if results.is_empty() {
        return Ok(table);
    }

    for batch in results {
        let schema = schema_opt.as_ref().unwrap_or(batch.schema_ref());

        // Could be a custom schema that was provided.
        if batch.columns().len() != schema.fields().len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected the same number of columns in a record batch ({}) as the number of fields ({}) in the schema",
                batch.columns().len(),
                schema.fields.len()
            )));
        }

        let formatters = batch
            .columns()
            .iter()
            .zip(schema.fields().iter())
            .map(|(c, field)| make_array_formatter(c, options, Some(field)))
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
        let formatter = match options.formatter_factory() {
            None => ArrayFormatter::try_new(col.as_ref(), options)?,
            Some(formatters) => formatters
                .create_array_formatter(col.as_ref(), options, None)
                .transpose()
                .unwrap_or_else(|| ArrayFormatter::try_new(col.as_ref(), options))?,
        };
        for row in 0..col.len() {
            let cells = vec![Cell::new(formatter.value(row))];
            table.add_row(cells);
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Write;
    use std::sync::Arc;

    use arrow_array::builder::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, ScalarBuffer};
    use arrow_schema::*;
    use half::f16;

    use crate::display::{
        ArrayFormatterFactory, DisplayIndex, DurationFormat, array_value_to_string,
    };

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
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 3);
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
        let options = FormatOptions::default()
            .with_null("null")
            .with_types_info(true);
        let int32_array = Int32Array::from(vec![Some(1), Some(2), None, Some(3), Some(4)]);
        let string_array =
            StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz"), None]);

        let batch = RecordBatch::try_from_iter([
            ("my_int32_name", Arc::new(int32_array) as _),
            ("my_string_name", Arc::new(string_array) as _),
        ])
        .unwrap();

        let column = pretty_format_columns_with_options(
            "my_column_name",
            &[batch.column(0).clone()],
            &options,
        )
        .unwrap()
        .to_string();

        let expected_column = vec![
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
        assert_eq!(expected_column, actual, "Actual result:\n{column}");

        let batch = pretty_format_batches_with_options(&[batch], &options)
            .unwrap()
            .to_string();

        let expected_table = vec![
            "+---------------+----------------+",
            "| my_int32_name | my_string_name |",
            "| Int32         | Utf8           |",
            "+---------------+----------------+",
            "| 1             | foo            |",
            "| 2             | bar            |",
            "| null          | null           |",
            "| 3             | baz            |",
            "| 4             | null           |",
            "+---------------+----------------+",
        ];

        let actual: Vec<&str> = batch.lines().collect();
        assert_eq!(expected_table, actual, "Actual result:\n{batch}");
    }

    #[test]
    fn duration_pretty_and_iso_extremes() {
        // Build [MIN, MAX, 3661, NULL]
        let arr = DurationSecondArray::from(vec![Some(i64::MIN), Some(i64::MAX), Some(3661), None]);
        let array: ArrayRef = Arc::new(arr);

        // Pretty formatting
        let opts = FormatOptions::default().with_null("null");
        let opts = opts.with_duration_format(DurationFormat::Pretty);
        let pretty =
            pretty_format_columns_with_options("pretty", std::slice::from_ref(&array), &opts)
                .unwrap()
                .to_string();

        // Expected output
        let expected_pretty = vec![
            "+------------------------------+",
            "| pretty                       |",
            "+------------------------------+",
            "| <invalid>                    |",
            "| <invalid>                    |",
            "| 0 days 1 hours 1 mins 1 secs |",
            "| null                         |",
            "+------------------------------+",
        ];

        let actual: Vec<&str> = pretty.lines().collect();
        assert_eq!(expected_pretty, actual, "Actual result:\n{pretty}");

        // ISO8601 formatting
        let opts_iso = FormatOptions::default()
            .with_null("null")
            .with_duration_format(DurationFormat::ISO8601);
        let iso = pretty_format_columns_with_options("iso", &[array], &opts_iso)
            .unwrap()
            .to_string();

        // Expected output
        let expected_iso = vec![
            "+-----------+",
            "| iso       |",
            "+-----------+",
            "| <invalid> |",
            "| <invalid> |",
            "| PT3661S   |",
            "| null      |",
            "+-----------+",
        ];

        let actual: Vec<&str> = iso.lines().collect();
        assert_eq!(expected_iso, actual, "Actual result:\n{iso}");
    }

    //
    // Custom Formatting
    //

    /// The factory that will create the [`ArrayFormatter`]s.
    #[derive(Debug)]
    struct TestFormatters {}

    impl ArrayFormatterFactory for TestFormatters {
        fn create_array_formatter<'formatter>(
            &self,
            array: &'formatter dyn Array,
            options: &FormatOptions<'formatter>,
            field: Option<&'formatter Field>,
        ) -> Result<Option<ArrayFormatter<'formatter>>, ArrowError> {
            if field
                .map(|f| f.extension_type_name() == Some("my_money"))
                .unwrap_or(false)
            {
                // We assume that my_money always is an Int32.
                let array = array.as_primitive();
                let display_index = Box::new(MyMoneyFormatter {
                    array,
                    options: options.clone(),
                });
                return Ok(Some(ArrayFormatter::new(display_index, options.safe())));
            }

            if array.data_type() == &DataType::Int32 {
                let array = array.as_primitive();
                let display_index = Box::new(MyInt32Formatter {
                    array,
                    options: options.clone(),
                });
                return Ok(Some(ArrayFormatter::new(display_index, options.safe())));
            }

            Ok(None)
        }
    }

    /// A format that will append a "€" sign to the end of the Int32 values.
    struct MyMoneyFormatter<'a> {
        array: &'a Int32Array,
        options: FormatOptions<'a>,
    }

    impl<'a> DisplayIndex for MyMoneyFormatter<'a> {
        fn write(&self, idx: usize, f: &mut dyn Write) -> crate::display::FormatResult {
            match self.array.is_valid(idx) {
                true => write!(f, "{} €", self.array.value(idx))?,
                false => write!(f, "{}", self.options.null())?,
            }

            Ok(())
        }
    }

    /// The actual formatter
    struct MyInt32Formatter<'a> {
        array: &'a Int32Array,
        options: FormatOptions<'a>,
    }

    impl<'a> DisplayIndex for MyInt32Formatter<'a> {
        fn write(&self, idx: usize, f: &mut dyn Write) -> crate::display::FormatResult {
            match self.array.is_valid(idx) {
                true => write!(f, "{} (32-Bit)", self.array.value(idx))?,
                false => write!(f, "{}", self.options.null())?,
            }

            Ok(())
        }
    }

    #[test]
    fn test_format_batches_with_custom_formatters() {
        // define a schema.
        let options = FormatOptions::new()
            .with_null("<NULL>")
            .with_formatter_factory(Some(&TestFormatters {}));
        let money_metadata = HashMap::from([(
            extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
            "my_money".to_owned(),
        )]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("income", DataType::Int32, true).with_metadata(money_metadata.clone()),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(array::Int32Array::from(vec![
                Some(1),
                None,
                Some(10),
                Some(100),
            ]))],
        )
        .unwrap();

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            pretty_format_batches_with_options(&[batch], &options).unwrap()
        )
        .unwrap();

        let s = [
            "+--------+",
            "| income |",
            "+--------+",
            "| 1 €    |",
            "| <NULL> |",
            "| 10 €   |",
            "| 100 €  |",
            "+--------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_format_batches_with_custom_formatters_multi_nested_list() {
        // define a schema.
        let options = FormatOptions::new()
            .with_null("<NULL>")
            .with_formatter_factory(Some(&TestFormatters {}));
        let money_metadata = HashMap::from([(
            extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
            "my_money".to_owned(),
        )]);
        let nested_field = Arc::new(
            Field::new_list_field(DataType::Int32, true).with_metadata(money_metadata.clone()),
        );

        // Create nested data
        let inner_list = ListBuilder::new(Int32Builder::new()).with_field(nested_field);
        let mut outer_list = FixedSizeListBuilder::new(inner_list, 2);
        outer_list.values().append_value([Some(1)]);
        outer_list.values().append_null();
        outer_list.append(true);
        outer_list.values().append_value([Some(2), Some(8)]);
        outer_list
            .values()
            .append_value([Some(50), Some(25), Some(25)]);
        outer_list.append(true);
        let outer_list = outer_list.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "income",
            outer_list.data_type().clone(),
            true,
        )]));

        // define data.
        let batch = RecordBatch::try_new(schema, vec![Arc::new(outer_list)]).unwrap();

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            pretty_format_batches_with_options(&[batch], &options).unwrap()
        )
        .unwrap();

        let s = [
            "+----------------------------------+",
            "| income                           |",
            "+----------------------------------+",
            "| [[1 €], <NULL>]                  |",
            "| [[2 €, 8 €], [50 €, 25 €, 25 €]] |",
            "+----------------------------------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_format_batches_with_custom_formatters_nested_struct() {
        // define a schema.
        let options = FormatOptions::new()
            .with_null("<NULL>")
            .with_formatter_factory(Some(&TestFormatters {}));
        let money_metadata = HashMap::from([(
            extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
            "my_money".to_owned(),
        )]);
        let fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("income", DataType::Int32, true).with_metadata(money_metadata.clone()),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "income",
            DataType::Struct(fields.clone()),
            true,
        )]));

        // Create nested data
        let mut nested_data = StructBuilder::new(
            fields,
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int32Builder::new()),
            ],
        );
        nested_data
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .extend([Some("Gimli"), Some("Legolas"), Some("Aragorn")]);
        nested_data
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .extend([Some(10), None, Some(30)]);
        nested_data.append(true);
        nested_data.append(true);
        nested_data.append(true);

        // define data.
        let batch = RecordBatch::try_new(schema, vec![Arc::new(nested_data.finish())]).unwrap();

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            pretty_format_batches_with_options(&[batch], &options).unwrap()
        )
        .unwrap();

        let s = [
            "+---------------------------------+",
            "| income                          |",
            "+---------------------------------+",
            "| {name: Gimli, income: 10 €}     |",
            "| {name: Legolas, income: <NULL>} |",
            "| {name: Aragorn, income: 30 €}   |",
            "+---------------------------------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_format_batches_with_custom_formatters_nested_map() {
        // define a schema.
        let options = FormatOptions::new()
            .with_null("<NULL>")
            .with_formatter_factory(Some(&TestFormatters {}));
        let money_metadata = HashMap::from([(
            extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
            "my_money".to_owned(),
        )]);

        let mut array = MapBuilder::<StringBuilder, Int32Builder>::new(
            None,
            StringBuilder::new(),
            Int32Builder::new(),
        )
        .with_values_field(
            Field::new("values", DataType::Int32, true).with_metadata(money_metadata.clone()),
        );
        array
            .keys()
            .extend([Some("Gimli"), Some("Legolas"), Some("Aragorn")]);
        array.values().extend([Some(10), None, Some(30)]);
        array.append(true).unwrap();
        let array = array.finish();

        // define data.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "income",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            pretty_format_batches_with_options(&[batch], &options).unwrap()
        )
        .unwrap();

        let s = [
            "+-----------------------------------------------+",
            "| income                                        |",
            "+-----------------------------------------------+",
            "| {Gimli: 10 €, Legolas: <NULL>, Aragorn: 30 €} |",
            "+-----------------------------------------------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_format_batches_with_custom_formatters_nested_union() {
        // define a schema.
        let options = FormatOptions::new()
            .with_null("<NULL>")
            .with_formatter_factory(Some(&TestFormatters {}));
        let money_metadata = HashMap::from([(
            extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
            "my_money".to_owned(),
        )]);
        let fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("income", DataType::Int32, true).with_metadata(money_metadata.clone())],
        )
        .unwrap();

        // Create nested data and construct it with the correct metadata
        let mut array_builder = UnionBuilder::new_dense();
        array_builder.append::<Int32Type>("income", 1).unwrap();
        let (_, type_ids, offsets, children) = array_builder.build().unwrap().into_parts();
        let array = UnionArray::try_new(fields, type_ids, offsets, children).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "income",
            array.data_type().clone(),
            true,
        )]));

        // define data.
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            pretty_format_batches_with_options(&[batch], &options).unwrap()
        )
        .unwrap();

        let s = [
            "+--------------+",
            "| income       |",
            "+--------------+",
            "| {income=1 €} |",
            "+--------------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_format_batches_with_custom_formatters_custom_schema_overrules_batch_schema() {
        // define a schema.
        let options = FormatOptions::new().with_formatter_factory(Some(&TestFormatters {}));
        let money_metadata = HashMap::from([(
            extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
            "my_money".to_owned(),
        )]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("income", DataType::Int32, true).with_metadata(money_metadata.clone()),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(array::Int32Array::from(vec![
                Some(1),
                None,
                Some(10),
                Some(100),
            ]))],
        )
        .unwrap();

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            create_table(
                // No metadata compared to test_format_batches_with_custom_formatters
                Some(Arc::new(Schema::new(vec![Field::new(
                    "income",
                    DataType::Int32,
                    true
                ),]))),
                &[batch],
                &options,
            )
            .unwrap()
        )
        .unwrap();

        // No € formatting as in test_format_batches_with_custom_formatters
        let s = [
            "+--------------+",
            "| income       |",
            "+--------------+",
            "| 1 (32-Bit)   |",
            "|              |",
            "| 10 (32-Bit)  |",
            "| 100 (32-Bit) |",
            "+--------------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_format_column_with_custom_formatters() {
        // define data.
        let array = Arc::new(array::Int32Array::from(vec![
            Some(1),
            None,
            Some(10),
            Some(100),
        ]));

        let mut buf = String::new();
        write!(
            &mut buf,
            "{}",
            pretty_format_columns_with_options(
                "income",
                &[array],
                &FormatOptions::default().with_formatter_factory(Some(&TestFormatters {}))
            )
            .unwrap()
        )
        .unwrap();

        let s = [
            "+--------------+",
            "| income       |",
            "+--------------+",
            "| 1 (32-Bit)   |",
            "|              |",
            "| 10 (32-Bit)  |",
            "| 100 (32-Bit) |",
            "+--------------+",
        ];
        let expected = s.join("\n");
        assert_eq!(expected, buf);
    }

    #[test]
    fn test_pretty_format_batches_with_schema_with_wrong_number_of_fields() {
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        // define data.
        let batch = RecordBatch::try_new(
            schema_b,
            vec![Arc::new(array::Int32Array::from(vec![
                Some(1),
                None,
                Some(10),
                Some(100),
            ]))],
        )
        .unwrap();

        let error = pretty_format_batches_with_schema(schema_a, &[batch])
            .err()
            .unwrap();
        assert_eq!(
            &error.to_string(),
            "Invalid argument error: Expected the same number of columns in a record batch (1) as the number of fields (2) in the schema"
        );
    }
}
