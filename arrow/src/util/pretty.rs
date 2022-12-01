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

//! Utilities for printing record batches. Note this module is not
//! available unless `feature = "prettyprint"` is enabled.

use crate::{array::ArrayRef, record_batch::RecordBatch};
use comfy_table::{Cell, Table};
use std::fmt::Display;

use crate::error::Result;

use super::display::array_value_to_string;

///! Create a visual representation of record batches
pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<impl Display> {
    create_table(results)
}

///! Create a visual representation of columns
pub fn pretty_format_columns(
    col_name: &str,
    results: &[ArrayRef],
) -> Result<impl Display> {
    create_column(col_name, results)
}

///! Prints a visual representation of record batches to stdout
pub fn print_batches(results: &[RecordBatch]) -> Result<()> {
    println!("{}", create_table(results)?);
    Ok(())
}

///! Prints a visual representation of a list of column to stdout
pub fn print_columns(col_name: &str, results: &[ArrayRef]) -> Result<()> {
    println!("{}", create_column(col_name, results)?);
    Ok(())
}

///! Convert a series of record batches into a table
fn create_table(results: &[RecordBatch]) -> Result<Table> {
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
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(&array_value_to_string(column, row)?));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

fn create_column(field: &str, columns: &[ArrayRef]) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if columns.is_empty() {
        return Ok(table);
    }

    let header = vec![Cell::new(field)];
    table.set_header(header);

    for col in columns {
        for row in 0..col.len() {
            let cells = vec![Cell::new(&array_value_to_string(col, row)?)];
            table.add_row(cells);
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{
            self, new_null_array, Array, Date32Array, Date64Array,
            FixedSizeBinaryBuilder, Float16Array, Int32Array, StringArray,
            StringDictionaryBuilder, StructArray, Time32MillisecondArray,
            Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
            TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray, UnionArray, UnionBuilder,
        },
        buffer::Buffer,
        datatypes::{DataType, Field, Float64Type, Int32Type, Schema, UnionMode},
    };

    use super::*;
    use crate::array::{Decimal128Array, FixedSizeListBuilder};
    use std::fmt::Write;
    use std::sync::Arc;

    use half::f16;

    #[test]
    fn test_pretty_format_batches() -> Result<()> {
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
        )?;

        let table = pretty_format_batches(&[batch])?.to_string();

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

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_columns() -> Result<()> {
        let columns = vec![
            Arc::new(array::StringArray::from(vec![
                Some("a"),
                Some("b"),
                None,
                Some("d"),
            ])) as ArrayRef,
            Arc::new(array::StringArray::from(vec![Some("e"), None, Some("g")])),
        ];

        let table = pretty_format_columns("a", &columns)?.to_string();

        let expected = vec![
            "+---+", "| a |", "+---+", "| a |", "| b |", "|   |", "| d |", "| e |",
            "|   |", "| g |", "+---+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
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

        assert_eq!(expected, actual, "Actual result:\n{:#?}", table);
    }

    #[test]
    fn test_pretty_format_dictionary() -> Result<()> {
        // define a schema.
        let field_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();

        builder.append("one")?;
        builder.append_null();
        builder.append("three")?;
        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array])?;

        let table = pretty_format_batches(&[batch])?.to_string();

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

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_fixed_size_list() -> Result<()> {
        // define a schema.
        let field_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, true)),
            3,
        );
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

        let batch = RecordBatch::try_new(schema, vec![array])?;
        let table = pretty_format_batches(&[batch])?.to_string();
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

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_fixed_size_binary() -> Result<()> {
        // define a schema.
        let field_type = DataType::FixedSizeBinary(3);
        let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 3);

        builder.append_value([1, 2, 3]).unwrap();
        builder.append_null();
        builder.append_value([7, 8, 9]).unwrap();

        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array])?;
        let table = pretty_format_batches(&[batch])?.to_string();
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

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
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

            assert_eq!(expected, actual, "Actual result:\n\n{:#?}\n\n", actual);
        };
    }

    /// Generate an array with type $ARRAYTYPE with a numeric value of
    /// $VALUE, and compare $EXPECTED_RESULT to the output of
    /// formatting that array with `pretty_format_batches`
    macro_rules! check_datetime_with_timezone {
        ($ARRAYTYPE:ident, $VALUE:expr, $TZ_STRING:expr, $EXPECTED_RESULT:expr) => {
            let mut builder = $ARRAYTYPE::builder(10);
            builder.append_value($VALUE);
            builder.append_null();
            let array = builder.finish();
            let array = array.with_timezone($TZ_STRING);

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

            assert_eq!(expected, actual, "Actual result:\n\n{:#?}\n\n", actual);
        };
    }

    #[test]
    #[cfg(features = "chrono-tz")]
    fn test_pretty_format_timestamp_second_with_utc_timezone() {
        let expected = vec![
            "+---------------------------+",
            "| f                         |",
            "+---------------------------+",
            "| 1970-05-09T14:25:11+00:00 |",
            "|                           |",
            "+---------------------------+",
        ];
        check_datetime_with_timezone!(
            TimestampSecondArray,
            11111111,
            "UTC".to_string(),
            expected
        );
    }

    #[test]
    #[cfg(features = "chrono-tz")]
    fn test_pretty_format_timestamp_second_with_non_utc_timezone() {
        let expected = vec![
            "+---------------------------+",
            "| f                         |",
            "+---------------------------+",
            "| 1970-05-09T22:25:11+08:00 |",
            "|                           |",
            "+---------------------------+",
        ];
        check_datetime_with_timezone!(
            TimestampSecondArray,
            11111111,
            "Asia/Taipei".to_string(),
            expected
        );
    }

    #[test]
    fn test_pretty_format_timestamp_second_with_fixed_offset_timezone() {
        let expected = vec![
            "+---------------------------+",
            "| f                         |",
            "+---------------------------+",
            "| 1970-05-09T22:25:11+08:00 |",
            "|                           |",
            "+---------------------------+",
        ];
        check_datetime_with_timezone!(
            TimestampSecondArray,
            11111111,
            "+08:00".to_string(),
            expected
        );
    }

    #[test]
    fn test_pretty_format_timestamp_second_with_incorrect_fixed_offset_timezone() {
        let expected = vec![
            "+-------------------------------------------------+",
            "| f                                               |",
            "+-------------------------------------------------+",
            "| 1970-05-09T14:25:11 (Unknown Time Zone '08:00') |",
            "|                                                 |",
            "+-------------------------------------------------+",
        ];
        check_datetime_with_timezone!(
            TimestampSecondArray,
            11111111,
            "08:00".to_string(),
            expected
        );
    }

    #[test]
    fn test_pretty_format_timestamp_second_with_unknown_timezone() {
        let expected = vec![
            "+---------------------------------------------------+",
            "| f                                                 |",
            "+---------------------------------------------------+",
            "| 1970-05-09T14:25:11 (Unknown Time Zone 'Unknown') |",
            "|                                                   |",
            "+---------------------------------------------------+",
        ];
        check_datetime_with_timezone!(
            TimestampSecondArray,
            11111111,
            "Unknown".to_string(),
            expected
        );
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
            "+------------+",
            "| f          |",
            "+------------+",
            "| 2005-03-18 |",
            "|            |",
            "+------------+",
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
    fn test_int_display() -> Result<()> {
        let array = Arc::new(Int32Array::from(vec![6, 3])) as ArrayRef;
        let actual_one = array_value_to_string(&array, 0).unwrap();
        let expected_one = "6";

        let actual_two = array_value_to_string(&array, 1).unwrap();
        let expected_two = "3";
        assert_eq!(actual_one, expected_one);
        assert_eq!(actual_two, expected_two);
        Ok(())
    }

    #[test]
    fn test_decimal_display() -> Result<()> {
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

        let batch = RecordBatch::try_new(schema, vec![dm])?;

        let table = pretty_format_batches(&[batch])?.to_string();

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
        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_decimal_display_zero_scale() -> Result<()> {
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

        let batch = RecordBatch::try_new(schema, vec![dm])?;

        let table = pretty_format_batches(&[batch])?.to_string();
        let expected = vec![
            "+------+", "| f    |", "+------+", "| 101  |", "|      |", "| 200  |",
            "| 3040 |", "+------+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_struct() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Struct(vec![
                    Field::new("c11", DataType::Int32, true),
                    Field::new(
                        "c12",
                        DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)]),
                        false,
                    ),
                ]),
                false,
            ),
            Field::new("c2", DataType::Utf8, false),
        ]);

        let c1 = StructArray::from(vec![
            (
                Field::new("c11", DataType::Int32, true),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(5)])) as ArrayRef,
            ),
            (
                Field::new(
                    "c12",
                    DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)]),
                    false,
                ),
                Arc::new(StructArray::from(vec![(
                    Field::new("c121", DataType::Utf8, false),
                    Arc::new(StringArray::from(vec![Some("e"), Some("f"), Some("g")]))
                        as ArrayRef,
                )])) as ArrayRef,
            ),
        ]);
        let c2 = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)])
                .unwrap();

        let table = pretty_format_batches(&[batch])?.to_string();
        let expected = vec![
            r#"+-------------------------------------+----+"#,
            r#"| c1                                  | c2 |"#,
            r#"+-------------------------------------+----+"#,
            r#"| {"c11": 1, "c12": {"c121": "e"}}    | a  |"#,
            r#"| {"c11": null, "c12": {"c121": "f"}} | b  |"#,
            r#"| {"c11": 5, "c12": {"c121": "g"}}    | c  |"#,
            r#"+-------------------------------------+----+"#,
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_dense_union() -> Result<()> {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.2234).unwrap();
        builder.append_null::<Float64Type>("b").unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        let union = builder.build().unwrap();

        let schema = Schema::new(vec![Field::new(
            "Teamsters",
            DataType::Union(
                vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Float64, false),
                ],
                vec![0, 1],
                UnionMode::Dense,
            ),
            false,
        )]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(union)]).unwrap();
        let table = pretty_format_batches(&[batch])?.to_string();
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
        Ok(())
    }

    #[test]
    fn test_pretty_format_sparse_union() -> Result<()> {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.2234).unwrap();
        builder.append_null::<Float64Type>("b").unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        let union = builder.build().unwrap();

        let schema = Schema::new(vec![Field::new(
            "Teamsters",
            DataType::Union(
                vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Float64, false),
                ],
                vec![0, 1],
                UnionMode::Sparse,
            ),
            false,
        )]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(union)]).unwrap();
        let table = pretty_format_batches(&[batch])?.to_string();
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
        Ok(())
    }

    #[test]
    fn test_pretty_format_nested_union() -> Result<()> {
        //Inner UnionArray
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("b", 1).unwrap();
        builder.append::<Float64Type>("c", 3.2234).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append_null::<Int32Type>("b").unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        let inner = builder.build().unwrap();

        let inner_field = Field::new(
            "European Union",
            DataType::Union(
                vec![
                    Field::new("b", DataType::Int32, false),
                    Field::new("c", DataType::Float64, false),
                ],
                vec![0, 1],
                UnionMode::Dense,
            ),
            false,
        );

        // Can't use UnionBuilder with non-primitive types, so manually build outer UnionArray
        let a_array = Int32Array::from(vec![None, None, None, Some(1234), Some(23)]);
        let type_ids = Buffer::from_slice_ref([1_i8, 1, 0, 0, 1]);

        let children: Vec<(Field, Arc<dyn Array>)> = vec![
            (Field::new("a", DataType::Int32, true), Arc::new(a_array)),
            (inner_field.clone(), Arc::new(inner)),
        ];

        let outer = UnionArray::try_new(&[0, 1], type_ids, None, children).unwrap();

        let schema = Schema::new(vec![Field::new(
            "Teamsters",
            DataType::Union(
                vec![Field::new("a", DataType::Int32, true), inner_field],
                vec![0, 1],
                UnionMode::Sparse,
            ),
            false,
        )]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(outer)]).unwrap();
        let table = pretty_format_batches(&[batch])?.to_string();
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
        Ok(())
    }

    #[test]
    fn test_writing_formatted_batches() -> Result<()> {
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
        )?;

        let mut buf = String::new();
        write!(&mut buf, "{}", pretty_format_batches(&[batch])?).unwrap();

        let s = vec![
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

        Ok(())
    }

    #[test]
    fn test_float16_display() -> Result<()> {
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

        let batch = RecordBatch::try_new(schema, vec![array])?;

        let table = pretty_format_batches(&[batch])?.to_string();

        let expected = vec![
            "+------+", "| f16  |", "+------+", "| NaN  |", "| 4    |", "| -inf |",
            "+------+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_interval_day_time() -> Result<()> {
        let arr = Arc::new(arrow_array::IntervalDayTimeArray::from(vec![
            Some(1),
            Some(10),
            Some(100),
        ]));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "IntervalDayTime",
            arr.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![arr])?;

        let table = pretty_format_batches(&[batch])?.to_string();

        let expected = vec![
            "+-------------------------------------------------+",
            "| IntervalDayTime                                 |",
            "+-------------------------------------------------+",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.001 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.010 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.100 secs |",
            "+-------------------------------------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_interval_month_day_nano_array() -> Result<()> {
        let arr = Arc::new(arrow_array::IntervalMonthDayNanoArray::from(vec![
            Some(1),
            Some(10),
            Some(100),
            Some(1_000),
            Some(10_000),
            Some(100_000),
            Some(1_000_000),
            Some(10_000_000),
            Some(100_000_000),
            Some(1_000_000_000),
        ]));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "IntervalMonthDayNano",
            arr.data_type().clone(),
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![arr])?;

        let table = pretty_format_batches(&[batch])?.to_string();

        let expected = vec![
            "+-------------------------------------------------------+",
            "| IntervalMonthDayNano                                  |",
            "+-------------------------------------------------------+",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.000000001 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.000000010 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.000000100 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.000001000 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.000010000 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.000100000 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.001000000 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.010000000 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 0.100000000 secs |",
            "| 0 years 0 mons 0 days 0 hours 0 mins 1.000000000 secs |",
            "+-------------------------------------------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }
}
