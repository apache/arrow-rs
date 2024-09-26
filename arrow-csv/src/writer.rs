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

use arrow_array::*;
use arrow_cast::display::*;
use arrow_schema::*;
use csv::ByteRecord;
use std::io::Write;

use crate::map_csv_error;
const DEFAULT_NULL_VALUE: &str = "";

/// A CSV writer
#[derive(Debug)]
pub struct Writer<W: Write> {
    /// The object to write to
    writer: csv::Writer<W>,
    /// Whether file should be written with headers, defaults to `true`
    has_headers: bool,
    /// The date format for date arrays, defaults to RFC3339
    date_format: Option<String>,
    /// The datetime format for datetime arrays, defaults to RFC3339
    datetime_format: Option<String>,
    /// The timestamp format for timestamp arrays, defaults to RFC3339
    timestamp_format: Option<String>,
    /// The timestamp format for timestamp (with timezone) arrays, defaults to RFC3339
    timestamp_tz_format: Option<String>,
    /// The time format for time arrays, defaults to RFC3339
    time_format: Option<String>,
    /// Is the beginning-of-writer
    beginning: bool,
    /// The value to represent null entries, defaults to [`DEFAULT_NULL_VALUE`]
    null_value: Option<String>,
}

impl<W: Write> Writer<W> {
    /// Create a new CsvWriter from a writable object, with default options
    pub fn new(writer: W) -> Self {
        let delimiter = b',';
        WriterBuilder::new().with_delimiter(delimiter).build(writer)
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

        let options = FormatOptions::default()
            .with_null(self.null_value.as_deref().unwrap_or(DEFAULT_NULL_VALUE))
            .with_date_format(self.date_format.as_deref())
            .with_datetime_format(self.datetime_format.as_deref())
            .with_timestamp_format(self.timestamp_format.as_deref())
            .with_timestamp_tz_format(self.timestamp_tz_format.as_deref())
            .with_time_format(self.time_format.as_deref());

        let converters = batch
            .columns()
            .iter()
            .map(|a| {
                if a.data_type().is_nested() {
                    Err(ArrowError::CsvError(format!(
                        "Nested type {} is not supported in CSV",
                        a.data_type()
                    )))
                } else {
                    ArrayFormatter::try_new(a.as_ref(), &options)
                }
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let mut buffer = String::with_capacity(1024);
        let mut byte_record = ByteRecord::with_capacity(1024, converters.len());

        for row_idx in 0..batch.num_rows() {
            byte_record.clear();
            for (col_idx, converter) in converters.iter().enumerate() {
                buffer.clear();
                converter.value(row_idx).write(&mut buffer).map_err(|e| {
                    ArrowError::CsvError(format!(
                        "Error processing row {}, col {}: {e}",
                        row_idx + 1,
                        col_idx + 1
                    ))
                })?;
                byte_record.push_field(buffer.as_bytes());
            }

            self.writer
                .write_byte_record(&byte_record)
                .map_err(map_csv_error)?;
        }
        self.writer.flush()?;

        Ok(())
    }

    /// Unwraps this `Writer<W>`, returning the underlying writer.
    pub fn into_inner(self) -> W {
        // Safe to call `unwrap` since `write` always flushes the writer.
        self.writer.into_inner().unwrap()
    }
}

impl<W: Write> RecordBatchWriter for Writer<W> {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch)
    }

    fn close(self) -> Result<(), ArrowError> {
        Ok(())
    }
}

/// A CSV writer builder
#[derive(Clone, Debug)]
pub struct WriterBuilder {
    /// Optional column delimiter. Defaults to `b','`
    delimiter: u8,
    /// Whether to write column names as file headers. Defaults to `true`
    has_header: bool,
    /// Optional quote character. Defaults to `b'"'`
    quote: u8,
    /// Optional escape character. Defaults to `b'\\'`
    escape: u8,
    /// Enable double quote escapes. Defaults to `true`
    double_quote: bool,
    /// Optional date format for date arrays
    date_format: Option<String>,
    /// Optional datetime format for datetime arrays
    datetime_format: Option<String>,
    /// Optional timestamp format for timestamp arrays
    timestamp_format: Option<String>,
    /// Optional timestamp format for timestamp with timezone arrays
    timestamp_tz_format: Option<String>,
    /// Optional time format for time arrays
    time_format: Option<String>,
    /// Optional value to represent null
    null_value: Option<String>,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        WriterBuilder {
            delimiter: b',',
            has_header: true,
            quote: b'"',
            escape: b'\\',
            double_quote: true,
            date_format: None,
            datetime_format: None,
            timestamp_format: None,
            timestamp_tz_format: None,
            time_format: None,
            null_value: None,
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
    ///     let builder = WriterBuilder::new().with_header(false);
    ///     let writer = builder.build(file);
    ///
    ///     writer
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to write headers
    #[deprecated(note = "Use Self::with_header")]
    #[doc(hidden)]
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_header = has_headers;
        self
    }

    /// Set whether to write the CSV file with a header
    pub fn with_header(mut self, header: bool) -> Self {
        self.has_header = header;
        self
    }

    /// Returns `true` if this writer is configured to write a header
    pub fn header(&self) -> bool {
        self.has_header
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Get the CSV file's column delimiter as a byte character
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// Set the CSV file's quote character as a byte character
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Get the CSV file's quote character as a byte character
    pub fn quote(&self) -> u8 {
        self.quote
    }

    /// Set the CSV file's escape character as a byte character
    ///
    /// In some variants of CSV, quotes are escaped using a special escape
    /// character like `\` (instead of escaping quotes by doubling them).
    ///
    /// By default, writing these idiosyncratic escapes is disabled, and is
    /// only used when `double_quote` is disabled.
    pub fn with_escape(mut self, escape: u8) -> Self {
        self.escape = escape;
        self
    }

    /// Get the CSV file's escape character as a byte character
    pub fn escape(&self) -> u8 {
        self.escape
    }

    /// Set whether to enable double quote escapes
    ///
    /// When enabled (which is the default), quotes are escaped by doubling
    /// them. e.g., `"` escapes to `""`.
    ///
    /// When disabled, quotes are escaped with the escape character (which
    /// is `\\` by default).
    pub fn with_double_quote(mut self, double_quote: bool) -> Self {
        self.double_quote = double_quote;
        self
    }

    /// Get whether double quote escapes are enabled
    pub fn double_quote(&self) -> bool {
        self.double_quote
    }

    /// Set the CSV file's date format
    pub fn with_date_format(mut self, format: String) -> Self {
        self.date_format = Some(format);
        self
    }

    /// Get the CSV file's date format if set, defaults to RFC3339
    pub fn date_format(&self) -> Option<&str> {
        self.date_format.as_deref()
    }

    /// Set the CSV file's datetime format
    pub fn with_datetime_format(mut self, format: String) -> Self {
        self.datetime_format = Some(format);
        self
    }

    /// Get the CSV file's datetime format if set, defaults to RFC3339
    pub fn datetime_format(&self) -> Option<&str> {
        self.datetime_format.as_deref()
    }

    /// Set the CSV file's time format
    pub fn with_time_format(mut self, format: String) -> Self {
        self.time_format = Some(format);
        self
    }

    /// Get the CSV file's datetime time if set, defaults to RFC3339
    pub fn time_format(&self) -> Option<&str> {
        self.time_format.as_deref()
    }

    /// Set the CSV file's timestamp format
    pub fn with_timestamp_format(mut self, format: String) -> Self {
        self.timestamp_format = Some(format);
        self
    }

    /// Get the CSV file's timestamp format if set, defaults to RFC3339
    pub fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format.as_deref()
    }

    /// Set the CSV file's timestamp tz format
    pub fn with_timestamp_tz_format(mut self, tz_format: String) -> Self {
        self.timestamp_tz_format = Some(tz_format);
        self
    }

    /// Get the CSV file's timestamp tz format if set, defaults to RFC3339
    pub fn timestamp_tz_format(&self) -> Option<&str> {
        self.timestamp_tz_format.as_deref()
    }

    /// Set the value to represent null in output
    pub fn with_null(mut self, null_value: String) -> Self {
        self.null_value = Some(null_value);
        self
    }

    /// Get the value to represent null in output
    pub fn null(&self) -> &str {
        self.null_value.as_deref().unwrap_or(DEFAULT_NULL_VALUE)
    }

    /// Use RFC3339 format for date/time/timestamps (default)
    #[deprecated(note = "Use WriterBuilder::default()")]
    pub fn with_rfc3339(mut self) -> Self {
        self.date_format = None;
        self.datetime_format = None;
        self.time_format = None;
        self.timestamp_format = None;
        self.timestamp_tz_format = None;
        self
    }

    /// Create a new `Writer`
    pub fn build<W: Write>(self, writer: W) -> Writer<W> {
        let mut builder = csv::WriterBuilder::new();
        let writer = builder
            .delimiter(self.delimiter)
            .quote(self.quote)
            .double_quote(self.double_quote)
            .escape(self.escape)
            .from_writer(writer);
        Writer {
            writer,
            beginning: true,
            has_headers: self.has_header,
            date_format: self.date_format,
            datetime_format: self.datetime_format,
            time_format: self.time_format,
            timestamp_format: self.timestamp_format,
            timestamp_tz_format: self.timestamp_tz_format,
            null_value: self.null_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ReaderBuilder;
    use arrow_array::builder::{
        BinaryBuilder, Decimal128Builder, Decimal256Builder, FixedSizeBinaryBuilder,
        LargeBinaryBuilder,
    };
    use arrow_array::types::*;
    use arrow_buffer::i256;
    use core::str;
    use std::io::{Cursor, Read, Seek};
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
            Field::new_dictionary("c7", DataType::Int32, DataType::Utf8, false),
        ]);

        let c1 = StringArray::from(vec![
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 =
            PrimitiveArray::<Float64Type>::from(vec![Some(123.564532), None, Some(-556132.25)]);
        let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c5 =
            TimestampMillisecondArray::from(vec![None, Some(1555584887378), Some(1555555555555)]);
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
        assert_eq!(expected, str::from_utf8(&buffer).unwrap());
    }

    #[test]
    fn test_write_csv_decimal() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Decimal128(38, 6), true),
            Field::new("c2", DataType::Decimal256(76, 6), true),
        ]);

        let mut c1_builder = Decimal128Builder::new().with_data_type(DataType::Decimal128(38, 6));
        c1_builder.extend(vec![Some(-3335724), Some(2179404), None, Some(290472)]);
        let c1 = c1_builder.finish();

        let mut c2_builder = Decimal256Builder::new().with_data_type(DataType::Decimal256(76, 6));
        c2_builder.extend(vec![
            Some(i256::from_i128(-3335724)),
            Some(i256::from_i128(2179404)),
            None,
            Some(i256::from_i128(290472)),
        ]);
        let c2 = c2_builder.finish();

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

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

        let expected = r#"c1,c2
-3.335724,-3.335724
2.179404,2.179404
,
0.290472,0.290472
-3.335724,-3.335724
2.179404,2.179404
,
0.290472,0.290472
"#;
        assert_eq!(expected, str::from_utf8(&buffer).unwrap());
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
            "Lorem ipsum \ndolor sit amet",
            "consectetur \"adipiscing\" elit",
            "sed do eiusmod tempor",
        ]);
        let c2 =
            PrimitiveArray::<Float64Type>::from(vec![Some(123.564532), None, Some(-556132.25)]);
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
            .with_header(false)
            .with_delimiter(b'|')
            .with_quote(b'\'')
            .with_null("NULL".to_string())
            .with_time_format("%r".to_string());
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
            "'Lorem ipsum \ndolor sit amet'|123.564532|3|true|12:20:34 AM\nconsectetur \"adipiscing\" elit|NULL|2|false|06:51:20 AM\nsed do eiusmod tempor|-556132.25|1|NULL|11:46:03 PM\n"
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );

        let mut file = tempfile::tempfile().unwrap();

        let builder = WriterBuilder::new()
            .with_header(true)
            .with_double_quote(false)
            .with_escape(b'$');
        let mut writer = builder.build(&mut file);
        let batches = vec![&batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }
        drop(writer);

        file.rewind().unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "c1,c2,c3,c4,c6\n\"Lorem ipsum \ndolor sit amet\",123.564532,3,true,00:20:34\n\"consectetur $\"adipiscing$\" elit\",,2,false,06:51:20\nsed do eiusmod tempor,-556132.25,1,,23:46:03\n"
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );
    }

    #[test]
    fn test_conversion_consistency() {
        // test if we can serialize and deserialize whilst retaining the same type information/ precision

        let schema = Schema::new(vec![
            Field::new("c1", DataType::Date32, false),
            Field::new("c2", DataType::Date64, false),
            Field::new("c3", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ]);

        let nanoseconds = vec![
            1599566300000000000,
            1599566200000000000,
            1599566100000000000,
        ];
        let c1 = Date32Array::from(vec![3, 2, 1]);
        let c2 = Date64Array::from(vec![3, 2, 1]);
        let c3 = TimestampNanosecondArray::from(nanoseconds.clone());

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3)],
        )
        .unwrap();

        let builder = WriterBuilder::new().with_header(false);

        let mut buf: Cursor<Vec<u8>> = Default::default();
        // drop the writer early to release the borrow.
        {
            let mut writer = builder.build(&mut buf);
            writer.write(&batch).unwrap();
        }
        buf.set_position(0);

        let mut reader = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(3)
            .build_buffered(buf)
            .unwrap();

        let rb = reader.next().unwrap().unwrap();
        let c1 = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        let c2 = rb.column(1).as_any().downcast_ref::<Date64Array>().unwrap();
        let c3 = rb
            .column(2)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        let actual = c1.into_iter().collect::<Vec<_>>();
        let expected = vec![Some(3), Some(2), Some(1)];
        assert_eq!(actual, expected);
        let actual = c2.into_iter().collect::<Vec<_>>();
        let expected = vec![Some(3), Some(2), Some(1)];
        assert_eq!(actual, expected);
        let actual = c3.into_iter().collect::<Vec<_>>();
        let expected = nanoseconds.into_iter().map(Some).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_write_csv_invalid_cast() {
        let schema = Schema::new(vec![
            Field::new("c0", DataType::UInt32, false),
            Field::new("c1", DataType::Date64, false),
        ]);

        let c0 = UInt32Array::from(vec![Some(123), Some(234)]);
        let c1 = Date64Array::from(vec![Some(1926632005177), Some(1926632005177685347)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c0), Arc::new(c1)]).unwrap();

        let mut file = tempfile::tempfile().unwrap();
        let mut writer = Writer::new(&mut file);
        let batches = vec![&batch, &batch];

        for batch in batches {
            let err = writer.write(batch).unwrap_err().to_string();
            assert_eq!(err, "Csv error: Error processing row 2, col 2: Cast error: Failed to convert 1926632005177685347 to temporal for Date64")
        }
        drop(writer);
    }

    #[test]
    fn test_write_csv_using_rfc3339() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                true,
            ),
            Field::new("c2", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("c3", DataType::Date32, false),
            Field::new("c4", DataType::Time32(TimeUnit::Second), false),
        ]);

        let c1 = TimestampMillisecondArray::from(vec![Some(1555584887378), Some(1635577147000)])
            .with_timezone("+00:00".to_string());
        let c2 = TimestampMillisecondArray::from(vec![Some(1555584887378), Some(1635577147000)]);
        let c3 = Date32Array::from(vec![3, 2]);
        let c4 = Time32SecondArray::from(vec![1234, 24680]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
        )
        .unwrap();

        let mut file = tempfile::tempfile().unwrap();

        let builder = WriterBuilder::new();
        let mut writer = builder.build(&mut file);
        let batches = vec![&batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }
        drop(writer);

        file.rewind().unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "c1,c2,c3,c4
2019-04-18T10:54:47.378Z,2019-04-18T10:54:47.378,1970-01-04,00:20:34
2021-10-30T06:59:07Z,2021-10-30T06:59:07,1970-01-03,06:51:20\n",
            String::from_utf8(buffer).unwrap()
        );
    }

    #[test]
    fn test_write_csv_tz_format() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+02:00".into())),
                true,
            ),
            Field::new(
                "c2",
                DataType::Timestamp(TimeUnit::Second, Some("+04:00".into())),
                true,
            ),
        ]);
        let c1 = TimestampMillisecondArray::from(vec![Some(1_000), Some(2_000)])
            .with_timezone("+02:00".to_string());
        let c2 = TimestampSecondArray::from(vec![Some(1_000_000), None])
            .with_timezone("+04:00".to_string());
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let mut file = tempfile::tempfile().unwrap();
        let mut writer = WriterBuilder::new()
            .with_timestamp_tz_format("%M:%H".to_string())
            .build(&mut file);
        writer.write(&batch).unwrap();

        drop(writer);
        file.rewind().unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "c1,c2\n00:02,46:17\n00:02,\n",
            String::from_utf8(buffer).unwrap()
        );
    }

    #[test]
    fn test_write_csv_binary() {
        let fixed_size = 8;
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("c1", DataType::Binary, true),
            Field::new("c2", DataType::FixedSizeBinary(fixed_size), true),
            Field::new("c3", DataType::LargeBinary, true),
        ]));
        let mut c1_builder = BinaryBuilder::new();
        c1_builder.append_value(b"Homer");
        c1_builder.append_value(b"Bart");
        c1_builder.append_null();
        c1_builder.append_value(b"Ned");
        let mut c2_builder = FixedSizeBinaryBuilder::new(fixed_size);
        c2_builder.append_value(b"Simpson ").unwrap();
        c2_builder.append_value(b"Simpson ").unwrap();
        c2_builder.append_null();
        c2_builder.append_value(b"Flanders").unwrap();
        let mut c3_builder = LargeBinaryBuilder::new();
        c3_builder.append_null();
        c3_builder.append_null();
        c3_builder.append_value(b"Comic Book Guy");
        c3_builder.append_null();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(c1_builder.finish()) as ArrayRef,
                Arc::new(c2_builder.finish()) as ArrayRef,
                Arc::new(c3_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let builder = WriterBuilder::new();
        let mut writer = builder.build(&mut buf);
        writer.write(&batch).unwrap();
        drop(writer);
        assert_eq!(
            "\
            c1,c2,c3\n\
            486f6d6572,53696d70736f6e20,\n\
            42617274,53696d70736f6e20,\n\
            ,,436f6d696320426f6f6b20477579\n\
            4e6564,466c616e64657273,\n\
            ",
            String::from_utf8(buf).unwrap()
        );
    }
}
