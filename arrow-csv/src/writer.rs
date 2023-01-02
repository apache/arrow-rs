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

//! CSV Writer
//!
//! This CSV writer allows Arrow data (in record batches) to be written as CSV files.
//! The writer does not support writing `ListArray` and `StructArray`.
//!
//! Example:
//!
//! ```
//! # use arrow_array::*;
//! # use arrow_array::types::*;
//! # use arrow_csv::Writer;
//! # use arrow_schema::*;
//! # use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("c1", DataType::Utf8, false),
//!     Field::new("c2", DataType::Float64, true),
//!     Field::new("c3", DataType::UInt32, false),
//!     Field::new("c4", DataType::Boolean, true),
//! ]);
//! let c1 = StringArray::from(vec![
//!     "Lorem ipsum dolor sit amet",
//!     "consectetur adipiscing elit",
//!     "sed do eiusmod tempor",
//! ]);
//! let c2 = PrimitiveArray::<Float64Type>::from(vec![
//!     Some(123.564532),
//!     None,
//!     Some(-556132.25),
//! ]);
//! let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
//! let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
//!
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema),
//!     vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
//! )
//! .unwrap();
//!
//! let mut output = Vec::with_capacity(1024);
//!
//! let mut writer = Writer::new(&mut output);
//! let batches = vec![&batch, &batch];
//! for batch in batches {
//!     writer.write(batch).unwrap();
//! }
//! ```

use arrow_array::types::*;
use arrow_array::*;
use arrow_cast::display::{
    array_value_to_string, lexical_to_string,
};
use arrow_schema::*;
use std::io::Write;

use crate::map_csv_error;

const DEFAULT_NULL_VALUE: &str = "";

fn write_primitive_value<T>(array: &ArrayRef, i: usize) -> String
where
    T: ArrowPrimitiveType,
    T::Native: lexical_core::ToLexical,
{
    let c = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    lexical_to_string(c.value(i))
}

/// A CSV writer
#[derive(Debug)]
pub struct Writer<W: Write> {
    /// The object to write to
    writer: csv::Writer<W>,
    /// Whether file should be written with headers. Defaults to `true`
    has_headers: bool,
    /// Is the beginning-of-writer
    beginning: bool,
    /// The value to represent null entries
    null_value: String,
}

impl<W: Write> Writer<W> {
    /// Create a new CsvWriter from a writable object, with default options
    pub fn new(writer: W) -> Self {
        let delimiter = b',';
        let mut builder = csv::WriterBuilder::new();
        let writer = builder.delimiter(delimiter).from_writer(writer);
        Writer {
            writer,
            has_headers: true,
            beginning: true,
            null_value: DEFAULT_NULL_VALUE.to_string(),
        }
    }

    /// Convert a record to a string vector
    fn convert(
        &self,
        batch: &[ArrayRef],
        row_index: usize,
        buffer: &mut [String],
    ) -> Result<(), ArrowError> {
        // TODO: it'd be more efficient if we could create `record: Vec<&[u8]>
        for (col_index, item) in buffer.iter_mut().enumerate() {
            let col = &batch[col_index];
            if col.is_null(row_index) {
                // write the configured null value
                *item = self.null_value.clone();
                continue;
            }
            let string = match col.data_type() {
                DataType::Float64 => write_primitive_value::<Float64Type>(col, row_index),
                DataType::Float32 => write_primitive_value::<Float32Type>(col, row_index),
                DataType::Int8 => write_primitive_value::<Int8Type>(col, row_index),
                DataType::Int16 => write_primitive_value::<Int16Type>(col, row_index),
                DataType::Int32 => write_primitive_value::<Int32Type>(col, row_index),
                DataType::Int64 => write_primitive_value::<Int64Type>(col, row_index),
                DataType::UInt8 => write_primitive_value::<UInt8Type>(col, row_index),
                DataType::UInt16 => write_primitive_value::<UInt16Type>(col, row_index),
                DataType::UInt32 => write_primitive_value::<UInt32Type>(col, row_index),
                DataType::UInt64 => write_primitive_value::<UInt64Type>(col, row_index),
                DataType::Boolean => array_value_to_string(col, row_index)?.to_string(),
                DataType::Utf8 => array_value_to_string(col, row_index)?.to_string(),
                DataType::LargeUtf8 => array_value_to_string(col, row_index)?.to_string(),
                DataType::Date32 => array_value_to_string(col, row_index)?.to_string(),
                DataType::Date64 => array_value_to_string(col, row_index)?.to_string(),
                DataType::Time32(TimeUnit::Second) => {
                    array_value_to_string(col, row_index)?.to_string()
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    array_value_to_string(col, row_index)?.to_string()
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    array_value_to_string(col, row_index)?.to_string()
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    array_value_to_string(col, row_index)?.to_string()
                }
                DataType::Timestamp(_, _) => {
                    array_value_to_string(col, row_index)?.to_string()
                }
                DataType::Decimal128(..) => {
                    array_value_to_string(col, row_index)?.to_string()
                }
                t => {
                    // List and Struct arrays not supported by the writer, any
                    // other type needs to be implemented
                    return Err(ArrowError::CsvError(format!(
                        "CSV Writer does not support {:?} data type",
                        t
                    )));
                }
            };
            *item = string;
        }
        Ok(())
    }

    /// Write a vector of record batches to a writable object
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let num_columns = batch.num_columns();
        if self.beginning {
            if self.has_headers {
                let mut headers: Vec<String> = Vec::with_capacity(num_columns);
                batch
                    .schema()
                    .fields()
                    .iter()
                    .for_each(|field| headers.push(field.name().to_string()));
                self.writer
                    .write_record(&headers[..])
                    .map_err(map_csv_error)?;
            }
            self.beginning = false;
        }

        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|array| match array.data_type() {
                DataType::Dictionary(_, value_type) => {
                    arrow_cast::cast(array, value_type)
                        .expect("cannot cast dictionary to underlying values")
                }
                _ => array.clone(),
            })
            .collect();

        let mut buffer = vec!["".to_string(); batch.num_columns()];

        for row_index in 0..batch.num_rows() {
            self.convert(columns.as_slice(), row_index, &mut buffer)?;
            self.writer.write_record(&buffer).map_err(map_csv_error)?;
        }
        self.writer.flush()?;

        Ok(())
    }
}

/// A CSV writer builder
#[derive(Debug)]
pub struct WriterBuilder {
    /// Optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Whether to write column names as file headers. Defaults to `true`
    has_headers: bool,
    /// Optional value to represent null
    null_value: Option<String>,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self {
            has_headers: true,
            delimiter: None,
            null_value: Some(DEFAULT_NULL_VALUE.to_string()),
        }
    }
}

impl WriterBuilder {
    /// Create a new builder for configuring CSV writing options.
    ///
    /// To convert a builder into a writer, call `WriterBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_csv::{Writer, WriterBuilder};
    /// # use std::fs::File;
    ///
    /// fn example() -> Writer<File> {
    ///     let file = File::create("target/out.csv").unwrap();
    ///
    ///     // create a builder that doesn't write headers
    ///     let builder = WriterBuilder::new().has_headers(false);
    ///     let writer = builder.build(file);
    ///
    ///     writer
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to write headers
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set the value to represent null in output
    pub fn with_null(mut self, null_value: String) -> Self {
        self.null_value = Some(null_value);
        self
    }

    /// Create a new `Writer`
    pub fn build<W: Write>(self, writer: W) -> Writer<W> {
        let delimiter = self.delimiter.unwrap_or(b',');
        let mut builder = csv::WriterBuilder::new();
        let writer = builder.delimiter(delimiter).from_writer(writer);
        Writer {
            writer,
            has_headers: self.has_headers,
            beginning: true,
            null_value: self
                .null_value
                .unwrap_or_else(|| DEFAULT_NULL_VALUE.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Seek};
    use std::sync::Arc;

    #[test]
    fn test_write_csv() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
            Field::new(
                "c7",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]);

        let c1 = StringArray::from(vec![
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = PrimitiveArray::<Float64Type>::from(vec![
            Some(123.564532),
            None,
            Some(-556132.25),
        ]);
        let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c5 = TimestampMillisecondArray::from(vec![
            None,
            Some(1555584887378),
            Some(1555555555555),
        ]);
        let c6 = Time32SecondArray::from(vec![1234, 24680, 85563]);
        let c7: DictionaryArray<Int32Type> =
            vec!["cupcakes", "cupcakes", "foo"].into_iter().collect();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
                Arc::new(c5),
                Arc::new(c6),
                Arc::new(c7),
            ],
        )
        .unwrap();

        let mut file = tempfile::tempfile().unwrap();

        let mut writer = Writer::new(&mut file);
        let batches = vec![&batch, &batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }
        drop(writer);

        // check that file was written successfully
        file.rewind().unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        let expected = r#"c1,c2,c3,c4,c5,c6,c7
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34,cupcakes
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378,06:51:20,cupcakes
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555,23:46:03,foo
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34,cupcakes
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378,06:51:20,cupcakes
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555,23:46:03,foo
"#;
        assert_eq!(expected.to_string(), String::from_utf8(buffer).unwrap());
    }

    #[test]
    fn test_write_csv_custom_options() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        ]);

        let c1 = StringArray::from(vec![
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = PrimitiveArray::<Float64Type>::from(vec![
            Some(123.564532),
            None,
            Some(-556132.25),
        ]);
        let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c6 = Time32SecondArray::from(vec![1234, 24680, 85563]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
                Arc::new(c6),
            ],
        )
        .unwrap();

        let mut file = tempfile::tempfile().unwrap();

        let builder = WriterBuilder::new()
            .has_headers(false)
            .with_delimiter(b'|')
            .with_null("NULL".to_string());
        let mut writer = builder.build(&mut file);
        let batches = vec![&batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }
        drop(writer);

        // check that file was written successfully
        file.rewind().unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "Lorem ipsum dolor sit amet|123.564532|3|true|00:20:34\nconsectetur adipiscing elit|NULL|2|false|06:51:20\nsed do eiusmod tempor|-556132.25|1|NULL|23:46:03\n"
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );
    }
}
