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

//! # JSON Writer
//!
//! This JSON writer converts Arrow [`RecordBatch`]es into arrays of
//! JSON objects or JSON formatted byte streams.
//!
//! ## Writing JSON formatted byte streams
//!
//! To serialize [`RecordBatch`]es into line-delimited JSON bytes, use
//! [`LineDelimitedWriter`]:
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow_array::{Int32Array, RecordBatch};
//! # use arrow_schema::{DataType, Field, Schema};
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! // Write the record batch out as JSON
//! let buf = Vec::new();
//! let mut writer = arrow_json::LineDelimitedWriter::new(buf);
//! writer.write_batches(&vec![&batch]).unwrap();
//! writer.finish().unwrap();
//!
//! // Get the underlying buffer back,
//! let buf = writer.into_inner();
//! assert_eq!(r#"{"a":1}
//! {"a":2}
//! {"a":3}
//!"#, String::from_utf8(buf).unwrap())
//! ```
//!
//! To serialize [`RecordBatch`]es into a well formed JSON array, use
//! [`ArrayWriter`]:
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow_array::{Int32Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! // Write the record batch out as a JSON array
//! let buf = Vec::new();
//! let mut writer = arrow_json::ArrayWriter::new(buf);
//! writer.write_batches(&vec![&batch]).unwrap();
//! writer.finish().unwrap();
//!
//! // Get the underlying buffer back,
//! let buf = writer.into_inner();
//! assert_eq!(r#"[{"a":1},{"a":2},{"a":3}]"#, String::from_utf8(buf).unwrap())
//! ```
//!
//! [`LineDelimitedWriter`] and [`ArrayWriter`] will omit writing keys with null values.
//! In order to explicitly write null values for keys, configure a custom [`Writer`] by
//! using a [`WriterBuilder`] to construct a [`Writer`].
//!
//! ## Writing to [serde_json] JSON Objects
//!
//! To serialize [`RecordBatch`]es into an array of
//! [JSON](https://docs.serde.rs/serde_json/) objects you can reparse the resulting JSON string.
//! Note that this is less efficient than using the `Writer` API.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow_array::{Int32Array, RecordBatch};
//! # use arrow_schema::{DataType, Field, Schema};
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! // Write the record batch out as json bytes (string)
//! let buf = Vec::new();
//! let mut writer = arrow_json::ArrayWriter::new(buf);
//! writer.write_batches(&vec![&batch]).unwrap();
//! writer.finish().unwrap();
//! let json_data = writer.into_inner();
//!
//! // Parse the string using serde_json
//! use serde_json::{Map, Value};
//! let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice()).unwrap();
//! assert_eq!(
//!     serde_json::Value::Object(json_rows[1].clone()),
//!     serde_json::json!({"a": 2}),
//! );
//! ```
mod encoder;

use std::{fmt::Debug, io::Write};

use arrow_array::*;
use arrow_schema::*;

use encoder::{make_encoder, EncoderOptions};

/// This trait defines how to format a sequence of JSON objects to a
/// byte stream.
pub trait JsonFormat: Debug + Default {
    #[inline]
    /// write any bytes needed at the start of the file to the writer
    fn start_stream<W: Write>(&self, _writer: &mut W) -> Result<(), ArrowError> {
        Ok(())
    }

    #[inline]
    /// write any bytes needed for the start of each row
    fn start_row<W: Write>(&self, _writer: &mut W, _is_first_row: bool) -> Result<(), ArrowError> {
        Ok(())
    }

    #[inline]
    /// write any bytes needed for the end of each row
    fn end_row<W: Write>(&self, _writer: &mut W) -> Result<(), ArrowError> {
        Ok(())
    }

    /// write any bytes needed for the start of each row
    fn end_stream<W: Write>(&self, _writer: &mut W) -> Result<(), ArrowError> {
        Ok(())
    }
}

/// Produces JSON output with one record per line.
///
/// For example:
///
/// ```json
/// {"foo":1}
/// {"bar":1}
///
/// ```
#[derive(Debug, Default)]
pub struct LineDelimited {}

impl JsonFormat for LineDelimited {
    fn end_row<W: Write>(&self, writer: &mut W) -> Result<(), ArrowError> {
        writer.write_all(b"\n")?;
        Ok(())
    }
}

/// Produces JSON output as a single JSON array.
///
/// For example:
///
/// ```json
/// [{"foo":1},{"bar":1}]
/// ```
#[derive(Debug, Default)]
pub struct JsonArray {}

impl JsonFormat for JsonArray {
    fn start_stream<W: Write>(&self, writer: &mut W) -> Result<(), ArrowError> {
        writer.write_all(b"[")?;
        Ok(())
    }

    fn start_row<W: Write>(&self, writer: &mut W, is_first_row: bool) -> Result<(), ArrowError> {
        if !is_first_row {
            writer.write_all(b",")?;
        }
        Ok(())
    }

    fn end_stream<W: Write>(&self, writer: &mut W) -> Result<(), ArrowError> {
        writer.write_all(b"]")?;
        Ok(())
    }
}

/// A JSON writer which serializes [`RecordBatch`]es to newline delimited JSON objects.
pub type LineDelimitedWriter<W> = Writer<W, LineDelimited>;

/// A JSON writer which serializes [`RecordBatch`]es to JSON arrays.
pub type ArrayWriter<W> = Writer<W, JsonArray>;

/// JSON writer builder.
#[derive(Debug, Clone, Default)]
pub struct WriterBuilder(EncoderOptions);

impl WriterBuilder {
    /// Create a new builder for configuring JSON writing options.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_json::{Writer, WriterBuilder};
    /// # use arrow_json::writer::LineDelimited;
    /// # use std::fs::File;
    ///
    /// fn example() -> Writer<File, LineDelimited> {
    ///     let file = File::create("target/out.json").unwrap();
    ///
    ///     // create a builder that keeps keys with null values
    ///     let builder = WriterBuilder::new().with_explicit_nulls(true);
    ///     let writer = builder.build::<_, LineDelimited>(file);
    ///
    ///     writer
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if this writer is configured to keep keys with null values.
    pub fn explicit_nulls(&self) -> bool {
        self.0.explicit_nulls
    }

    /// Set whether to keep keys with null values, or to omit writing them.
    ///
    /// For example, with [`LineDelimited`] format:
    ///
    /// Skip nulls (set to `false`):
    ///
    /// ```json
    /// {"foo":1}
    /// {"foo":1,"bar":2}
    /// {}
    /// ```
    ///
    /// Keep nulls (set to `true`):
    ///
    /// ```json
    /// {"foo":1,"bar":null}
    /// {"foo":1,"bar":2}
    /// {"foo":null,"bar":null}
    /// ```
    ///
    /// Default is to skip nulls (set to `false`).
    pub fn with_explicit_nulls(mut self, explicit_nulls: bool) -> Self {
        self.0.explicit_nulls = explicit_nulls;
        self
    }

    /// Create a new `Writer` with specified `JsonFormat` and builder options.
    pub fn build<W, F>(self, writer: W) -> Writer<W, F>
    where
        W: Write,
        F: JsonFormat,
    {
        Writer {
            writer,
            started: false,
            finished: false,
            format: F::default(),
            options: self.0,
        }
    }
}

/// A JSON writer which serializes [`RecordBatch`]es to a stream of
/// `u8` encoded JSON objects.
///
/// See the module level documentation for detailed usage and examples.
/// The specific format of the stream is controlled by the [`JsonFormat`]
/// type parameter.
///
/// By default the writer will skip writing keys with null values for
/// backward compatibility. See [`WriterBuilder`] on how to customize
/// this behaviour when creating a new writer.
#[derive(Debug)]
pub struct Writer<W, F>
where
    W: Write,
    F: JsonFormat,
{
    /// Underlying writer to use to write bytes
    writer: W,

    /// Has the writer output any records yet?
    started: bool,

    /// Is the writer finished?
    finished: bool,

    /// Determines how the byte stream is formatted
    format: F,

    /// Controls how JSON should be encoded, e.g. whether to write explicit nulls or skip them
    options: EncoderOptions,
}

impl<W, F> Writer<W, F>
where
    W: Write,
    F: JsonFormat,
{
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            started: false,
            finished: false,
            format: F::default(),
            options: EncoderOptions::default(),
        }
    }

    /// Serialize `batch` to JSON output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // BufWriter uses a buffer size of 8KB
        // We therefore double this and flush once we have more than 8KB
        let mut buffer = Vec::with_capacity(16 * 1024);

        let mut is_first_row = !self.started;
        if !self.started {
            self.format.start_stream(&mut buffer)?;
            self.started = true;
        }

        let array = StructArray::from(batch.clone());
        let mut encoder = make_encoder(&array, &self.options)?;

        for idx in 0..batch.num_rows() {
            self.format.start_row(&mut buffer, is_first_row)?;
            is_first_row = false;

            encoder.encode(idx, &mut buffer);
            if buffer.len() > 8 * 1024 {
                self.writer.write_all(&buffer)?;
                buffer.clear();
            }
            self.format.end_row(&mut buffer)?;
        }

        if !buffer.is_empty() {
            self.writer.write_all(&buffer)?;
        }

        Ok(())
    }

    /// Serialize `batches` to JSON output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for b in batches {
            self.write(b)?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced. (e.g. producing the final `']'` if writing
    /// arrays.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        if !self.started {
            self.format.start_stream(&mut self.writer)?;
            self.started = true;
        }
        if !self.finished {
            self.format.end_stream(&mut self.writer)?;
            self.finished = true;
        }

        Ok(())
    }

    /// Unwraps this `Writer<W>`, returning the underlying writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W, F> RecordBatchWriter for Writer<W, F>
where
    W: Write,
    F: JsonFormat,
{
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch)
    }

    fn close(mut self) -> Result<(), ArrowError> {
        self.finish()
    }
}

#[cfg(test)]
mod tests {
    use core::str;
    use std::fs::{read_to_string, File};
    use std::io::{BufReader, Seek};
    use std::sync::Arc;

    use serde_json::{json, Value};

    use arrow_array::builder::*;
    use arrow_array::types::*;
    use arrow_buffer::{i256, Buffer, NullBuffer, OffsetBuffer, ToByteSlice};
    use arrow_data::ArrayData;

    use crate::reader::*;

    use super::*;

    /// Asserts that the NDJSON `input` is semantically identical to `expected`
    fn assert_json_eq(input: &[u8], expected: &str) {
        let expected: Vec<Option<Value>> = expected
            .split('\n')
            .map(|s| (!s.is_empty()).then(|| serde_json::from_str(s).unwrap()))
            .collect();

        let actual: Vec<Option<Value>> = input
            .split(|b| *b == b'\n')
            .map(|s| (!s.is_empty()).then(|| serde_json::from_slice(s).unwrap()))
            .collect();

        assert_eq!(expected, actual);
    }

    #[test]
    fn write_simple_rows() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]);

        let a = Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)]);
        let b = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":1,"c2":"a"}
{"c1":2,"c2":"b"}
{"c1":3,"c2":"c"}
{"c2":"d"}
{"c1":5}
"#,
        );
    }

    #[test]
    fn write_large_utf8() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::LargeUtf8, true),
        ]);

        let a = StringArray::from(vec![Some("a"), None, Some("c"), Some("d"), None]);
        let b = LargeStringArray::from(vec![Some("a"), Some("b"), None, Some("d"), None]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":"a","c2":"a"}
{"c2":"b"}
{"c1":"c"}
{"c1":"d","c2":"d"}
{}
"#,
        );
    }

    #[test]
    fn write_dictionary() {
        let schema = Schema::new(vec![
            Field::new_dictionary("c1", DataType::Int32, DataType::Utf8, true),
            Field::new_dictionary("c2", DataType::Int8, DataType::Utf8, true),
        ]);

        let a: DictionaryArray<Int32Type> = vec![
            Some("cupcakes"),
            Some("foo"),
            Some("foo"),
            None,
            Some("cupcakes"),
        ]
        .into_iter()
        .collect();
        let b: DictionaryArray<Int8Type> =
            vec![Some("sdsd"), Some("sdsd"), None, Some("sd"), Some("sdsd")]
                .into_iter()
                .collect();

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":"cupcakes","c2":"sdsd"}
{"c1":"foo","c2":"sdsd"}
{"c1":"foo"}
{"c2":"sd"}
{"c1":"cupcakes","c2":"sdsd"}
"#,
        );
    }

    #[test]
    fn write_list_of_dictionary() {
        let dict_field = Arc::new(Field::new_dictionary(
            "item",
            DataType::Int32,
            DataType::Utf8,
            true,
        ));
        let schema = Schema::new(vec![Field::new_large_list("l", dict_field.clone(), true)]);

        let dict_array: DictionaryArray<Int32Type> =
            vec![Some("a"), Some("b"), Some("c"), Some("a"), None, Some("c")]
                .into_iter()
                .collect();
        let list_array = LargeListArray::try_new(
            dict_field,
            OffsetBuffer::from_lengths([3_usize, 2, 0, 1]),
            Arc::new(dict_array),
            Some(NullBuffer::from_iter([true, true, false, true])),
        )
        .unwrap();

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_array)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"l":["a","b","c"]}
{"l":["a",null]}
{}
{"l":["c"]}
"#,
        );
    }

    #[test]
    fn write_list_of_dictionary_large_values() {
        let dict_field = Arc::new(Field::new_dictionary(
            "item",
            DataType::Int32,
            DataType::LargeUtf8,
            true,
        ));
        let schema = Schema::new(vec![Field::new_large_list("l", dict_field.clone(), true)]);

        let keys = PrimitiveArray::<Int32Type>::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(0),
            None,
            Some(2),
        ]);
        let values = LargeStringArray::from(vec!["a", "b", "c"]);
        let dict_array = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();

        let list_array = LargeListArray::try_new(
            dict_field,
            OffsetBuffer::from_lengths([3_usize, 2, 0, 1]),
            Arc::new(dict_array),
            Some(NullBuffer::from_iter([true, true, false, true])),
        )
        .unwrap();

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_array)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"l":["a","b","c"]}
{"l":["a",null]}
{}
{"l":["c"]}
"#,
        );
    }

    #[test]
    fn write_timestamps() {
        let ts_string = "2018-11-13T17:11:10.011375885995";
        let ts_nanos = ts_string
            .parse::<chrono::NaiveDateTime>()
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let ts_micros = ts_nanos / 1000;
        let ts_millis = ts_micros / 1000;
        let ts_secs = ts_millis / 1000;

        let arr_nanos = TimestampNanosecondArray::from(vec![Some(ts_nanos), None]);
        let arr_micros = TimestampMicrosecondArray::from(vec![Some(ts_micros), None]);
        let arr_millis = TimestampMillisecondArray::from(vec![Some(ts_millis), None]);
        let arr_secs = TimestampSecondArray::from(vec![Some(ts_secs), None]);
        let arr_names = StringArray::from(vec![Some("a"), Some("b")]);

        let schema = Schema::new(vec![
            Field::new("nanos", arr_nanos.data_type().clone(), true),
            Field::new("micros", arr_micros.data_type().clone(), true),
            Field::new("millis", arr_millis.data_type().clone(), true),
            Field::new("secs", arr_secs.data_type().clone(), true),
            Field::new("name", arr_names.data_type().clone(), true),
        ]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arr_nanos),
                Arc::new(arr_micros),
                Arc::new(arr_millis),
                Arc::new(arr_secs),
                Arc::new(arr_names),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"micros":"2018-11-13T17:11:10.011375","millis":"2018-11-13T17:11:10.011","name":"a","nanos":"2018-11-13T17:11:10.011375885","secs":"2018-11-13T17:11:10"}
{"name":"b"}
"#,
        );
    }

    #[test]
    fn write_timestamps_with_tz() {
        let ts_string = "2018-11-13T17:11:10.011375885995";
        let ts_nanos = ts_string
            .parse::<chrono::NaiveDateTime>()
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let ts_micros = ts_nanos / 1000;
        let ts_millis = ts_micros / 1000;
        let ts_secs = ts_millis / 1000;

        let arr_nanos = TimestampNanosecondArray::from(vec![Some(ts_nanos), None]);
        let arr_micros = TimestampMicrosecondArray::from(vec![Some(ts_micros), None]);
        let arr_millis = TimestampMillisecondArray::from(vec![Some(ts_millis), None]);
        let arr_secs = TimestampSecondArray::from(vec![Some(ts_secs), None]);
        let arr_names = StringArray::from(vec![Some("a"), Some("b")]);

        let tz = "+00:00";

        let arr_nanos = arr_nanos.with_timezone(tz);
        let arr_micros = arr_micros.with_timezone(tz);
        let arr_millis = arr_millis.with_timezone(tz);
        let arr_secs = arr_secs.with_timezone(tz);

        let schema = Schema::new(vec![
            Field::new("nanos", arr_nanos.data_type().clone(), true),
            Field::new("micros", arr_micros.data_type().clone(), true),
            Field::new("millis", arr_millis.data_type().clone(), true),
            Field::new("secs", arr_secs.data_type().clone(), true),
            Field::new("name", arr_names.data_type().clone(), true),
        ]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arr_nanos),
                Arc::new(arr_micros),
                Arc::new(arr_millis),
                Arc::new(arr_secs),
                Arc::new(arr_names),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"micros":"2018-11-13T17:11:10.011375Z","millis":"2018-11-13T17:11:10.011Z","name":"a","nanos":"2018-11-13T17:11:10.011375885Z","secs":"2018-11-13T17:11:10Z"}
{"name":"b"}
"#,
        );
    }

    #[test]
    fn write_dates() {
        let ts_string = "2018-11-13T17:11:10.011375885995";
        let ts_millis = ts_string
            .parse::<chrono::NaiveDateTime>()
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let arr_date32 = Date32Array::from(vec![
            Some(i32::try_from(ts_millis / 1000 / (60 * 60 * 24)).unwrap()),
            None,
        ]);
        let arr_date64 = Date64Array::from(vec![Some(ts_millis), None]);
        let arr_names = StringArray::from(vec![Some("a"), Some("b")]);

        let schema = Schema::new(vec![
            Field::new("date32", arr_date32.data_type().clone(), true),
            Field::new("date64", arr_date64.data_type().clone(), true),
            Field::new("name", arr_names.data_type().clone(), false),
        ]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arr_date32),
                Arc::new(arr_date64),
                Arc::new(arr_names),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"date32":"2018-11-13","date64":"2018-11-13T17:11:10.011","name":"a"}
{"name":"b"}
"#,
        );
    }

    #[test]
    fn write_times() {
        let arr_time32sec = Time32SecondArray::from(vec![Some(120), None]);
        let arr_time32msec = Time32MillisecondArray::from(vec![Some(120), None]);
        let arr_time64usec = Time64MicrosecondArray::from(vec![Some(120), None]);
        let arr_time64nsec = Time64NanosecondArray::from(vec![Some(120), None]);
        let arr_names = StringArray::from(vec![Some("a"), Some("b")]);

        let schema = Schema::new(vec![
            Field::new("time32sec", arr_time32sec.data_type().clone(), true),
            Field::new("time32msec", arr_time32msec.data_type().clone(), true),
            Field::new("time64usec", arr_time64usec.data_type().clone(), true),
            Field::new("time64nsec", arr_time64nsec.data_type().clone(), true),
            Field::new("name", arr_names.data_type().clone(), true),
        ]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arr_time32sec),
                Arc::new(arr_time32msec),
                Arc::new(arr_time64usec),
                Arc::new(arr_time64nsec),
                Arc::new(arr_names),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"time32sec":"00:02:00","time32msec":"00:00:00.120","time64usec":"00:00:00.000120","time64nsec":"00:00:00.000000120","name":"a"}
{"name":"b"}
"#,
        );
    }

    #[test]
    fn write_durations() {
        let arr_durationsec = DurationSecondArray::from(vec![Some(120), None]);
        let arr_durationmsec = DurationMillisecondArray::from(vec![Some(120), None]);
        let arr_durationusec = DurationMicrosecondArray::from(vec![Some(120), None]);
        let arr_durationnsec = DurationNanosecondArray::from(vec![Some(120), None]);
        let arr_names = StringArray::from(vec![Some("a"), Some("b")]);

        let schema = Schema::new(vec![
            Field::new("duration_sec", arr_durationsec.data_type().clone(), true),
            Field::new("duration_msec", arr_durationmsec.data_type().clone(), true),
            Field::new("duration_usec", arr_durationusec.data_type().clone(), true),
            Field::new("duration_nsec", arr_durationnsec.data_type().clone(), true),
            Field::new("name", arr_names.data_type().clone(), true),
        ]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arr_durationsec),
                Arc::new(arr_durationmsec),
                Arc::new(arr_durationusec),
                Arc::new(arr_durationnsec),
                Arc::new(arr_names),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"duration_sec":"PT120S","duration_msec":"PT0.12S","duration_usec":"PT0.00012S","duration_nsec":"PT0.00000012S","name":"a"}
{"name":"b"}
"#,
        );
    }

    #[test]
    fn write_nested_structs() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Struct(Fields::from(vec![
                    Field::new("c11", DataType::Int32, true),
                    Field::new(
                        "c12",
                        DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)].into()),
                        false,
                    ),
                ])),
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
                Arc::new(Field::new(
                    "c12",
                    DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)].into()),
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

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":{"c11":1,"c12":{"c121":"e"}},"c2":"a"}
{"c1":{"c12":{"c121":"f"}},"c2":"b"}
{"c1":{"c11":5,"c12":{"c121":"g"}},"c2":"c"}
"#,
        );
    }

    #[test]
    fn write_struct_with_list_field() {
        let field_c1 = Field::new(
            "c1",
            DataType::List(Arc::new(Field::new("c_list", DataType::Utf8, false))),
            false,
        );
        let field_c2 = Field::new("c2", DataType::Int32, false);
        let schema = Schema::new(vec![field_c1.clone(), field_c2]);

        let a_values = StringArray::from(vec!["a", "a1", "b", "c", "d", "e"]);
        // list column rows: ["a", "a1"], ["b"], ["c"], ["d"], ["e"]
        let a_value_offsets = Buffer::from([0, 2, 3, 4, 5, 6].to_byte_slice());
        let a_list_data = ArrayData::builder(field_c1.data_type().clone())
            .len(5)
            .add_buffer(a_value_offsets)
            .add_child_data(a_values.into_data())
            .null_bit_buffer(Some(Buffer::from([0b00011111])))
            .build()
            .unwrap();
        let a = ListArray::from(a_list_data);

        let b = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":["a","a1"],"c2":1}
{"c1":["b"],"c2":2}
{"c1":["c"],"c2":3}
{"c1":["d"],"c2":4}
{"c1":["e"],"c2":5}
"#,
        );
    }

    #[test]
    fn write_nested_list() {
        let list_inner_type = Field::new(
            "a",
            DataType::List(Arc::new(Field::new("b", DataType::Int32, false))),
            false,
        );
        let field_c1 = Field::new(
            "c1",
            DataType::List(Arc::new(list_inner_type.clone())),
            false,
        );
        let field_c2 = Field::new("c2", DataType::Utf8, true);
        let schema = Schema::new(vec![field_c1.clone(), field_c2]);

        // list column rows: [[1, 2], [3]], [], [[4, 5, 6]]
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);

        let a_value_offsets = Buffer::from([0, 2, 3, 6].to_byte_slice());
        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(list_inner_type.data_type().clone())
            .len(3)
            .add_buffer(a_value_offsets)
            .null_bit_buffer(Some(Buffer::from([0b00000111])))
            .add_child_data(a_values.into_data())
            .build()
            .unwrap();

        let c1_value_offsets = Buffer::from([0, 2, 2, 3].to_byte_slice());
        let c1_list_data = ArrayData::builder(field_c1.data_type().clone())
            .len(3)
            .add_buffer(c1_value_offsets)
            .add_child_data(a_list_data)
            .build()
            .unwrap();

        let c1 = ListArray::from(c1_list_data);
        let c2 = StringArray::from(vec![Some("foo"), Some("bar"), None]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":[[1,2],[3]],"c2":"foo"}
{"c1":[],"c2":"bar"}
{"c1":[[4,5,6]]}
"#,
        );
    }

    #[test]
    fn write_list_of_struct() {
        let field_c1 = Field::new(
            "c1",
            DataType::List(Arc::new(Field::new(
                "s",
                DataType::Struct(Fields::from(vec![
                    Field::new("c11", DataType::Int32, true),
                    Field::new(
                        "c12",
                        DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)].into()),
                        false,
                    ),
                ])),
                false,
            ))),
            true,
        );
        let field_c2 = Field::new("c2", DataType::Int32, false);
        let schema = Schema::new(vec![field_c1.clone(), field_c2]);

        let struct_values = StructArray::from(vec![
            (
                Arc::new(Field::new("c11", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(5)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "c12",
                    DataType::Struct(vec![Field::new("c121", DataType::Utf8, false)].into()),
                    false,
                )),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("c121", DataType::Utf8, false)),
                    Arc::new(StringArray::from(vec![Some("e"), Some("f"), Some("g")])) as ArrayRef,
                )])) as ArrayRef,
            ),
        ]);

        // list column rows (c1):
        // [{"c11": 1, "c12": {"c121": "e"}}, {"c12": {"c121": "f"}}],
        // null,
        // [{"c11": 5, "c12": {"c121": "g"}}]
        let c1_value_offsets = Buffer::from([0, 2, 2, 3].to_byte_slice());
        let c1_list_data = ArrayData::builder(field_c1.data_type().clone())
            .len(3)
            .add_buffer(c1_value_offsets)
            .add_child_data(struct_values.into_data())
            .null_bit_buffer(Some(Buffer::from([0b00000101])))
            .build()
            .unwrap();
        let c1 = ListArray::from(c1_list_data);

        let c2 = Int32Array::from(vec![1, 2, 3]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"c1":[{"c11":1,"c12":{"c121":"e"}},{"c12":{"c121":"f"}}],"c2":1}
{"c2":2}
{"c1":[{"c11":5,"c12":{"c121":"g"}}],"c2":3}
"#,
        );
    }

    fn test_write_for_file(test_file: &str, remove_nulls: bool) {
        let file = File::open(test_file).unwrap();
        let mut reader = BufReader::new(file);
        let (schema, _) = infer_json_schema(&mut reader, None).unwrap();
        reader.rewind().unwrap();

        let builder = ReaderBuilder::new(Arc::new(schema)).with_batch_size(1024);
        let mut reader = builder.build(reader).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut buf = Vec::new();
        {
            if remove_nulls {
                let mut writer = LineDelimitedWriter::new(&mut buf);
                writer.write_batches(&[&batch]).unwrap();
            } else {
                let mut writer = WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, LineDelimited>(&mut buf);
                writer.write_batches(&[&batch]).unwrap();
            }
        }

        let result = str::from_utf8(&buf).unwrap();
        let expected = read_to_string(test_file).unwrap();
        for (r, e) in result.lines().zip(expected.lines()) {
            let mut expected_json = serde_json::from_str::<Value>(e).unwrap();
            if remove_nulls {
                // remove null value from object to make comparison consistent:
                if let Value::Object(obj) = expected_json {
                    expected_json =
                        Value::Object(obj.into_iter().filter(|(_, v)| *v != Value::Null).collect());
                }
            }
            assert_eq!(serde_json::from_str::<Value>(r).unwrap(), expected_json,);
        }
    }

    #[test]
    fn write_basic_rows() {
        test_write_for_file("test/data/basic.json", true);
    }

    #[test]
    fn write_arrays() {
        test_write_for_file("test/data/arrays.json", true);
    }

    #[test]
    fn write_basic_nulls() {
        test_write_for_file("test/data/basic_nulls.json", true);
    }

    #[test]
    fn write_nested_with_nulls() {
        test_write_for_file("test/data/nested_with_nulls.json", false);
    }

    #[test]
    fn json_line_writer_empty() {
        let mut writer = LineDelimitedWriter::new(vec![] as Vec<u8>);
        writer.finish().unwrap();
        assert_eq!(str::from_utf8(&writer.into_inner()).unwrap(), "");
    }

    #[test]
    fn json_array_writer_empty() {
        let mut writer = ArrayWriter::new(vec![] as Vec<u8>);
        writer.finish().unwrap();
        assert_eq!(str::from_utf8(&writer.into_inner()).unwrap(), "[]");
    }

    #[test]
    fn json_line_writer_empty_batch() {
        let mut writer = LineDelimitedWriter::new(vec![] as Vec<u8>);

        let array = Int32Array::from(Vec::<i32>::new());
        let schema = Schema::new(vec![Field::new("c", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(str::from_utf8(&writer.into_inner()).unwrap(), "");
    }

    #[test]
    fn json_array_writer_empty_batch() {
        let mut writer = ArrayWriter::new(vec![] as Vec<u8>);

        let array = Int32Array::from(Vec::<i32>::new());
        let schema = Schema::new(vec![Field::new("c", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(str::from_utf8(&writer.into_inner()).unwrap(), "[]");
    }

    #[test]
    fn json_struct_array_nulls() {
        let inner = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![None]),
            Some(vec![]),
            Some(vec![Some(3), None]), // masked for a
            Some(vec![Some(4), Some(5)]),
            None, // masked for a
            None,
        ]);

        let field = Arc::new(Field::new("list", inner.data_type().clone(), true));
        let array = Arc::new(inner) as ArrayRef;
        let struct_array_a = StructArray::from((
            vec![(field.clone(), array.clone())],
            Buffer::from([0b01010111]),
        ));
        let struct_array_b = StructArray::from(vec![(field, array)]);

        let schema = Schema::new(vec![
            Field::new_struct("a", struct_array_a.fields().clone(), true),
            Field::new_struct("b", struct_array_b.fields().clone(), true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(struct_array_a), Arc::new(struct_array_b)],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"a":{"list":[1,2]},"b":{"list":[1,2]}}
{"a":{"list":[null]},"b":{"list":[null]}}
{"a":{"list":[]},"b":{"list":[]}}
{"b":{"list":[3,null]}}
{"a":{"list":[4,5]},"b":{"list":[4,5]}}
{"b":{}}
{"a":{},"b":{}}
"#,
        );
    }

    #[test]
    fn json_writer_map() {
        let keys_array = super::StringArray::from(vec!["foo", "bar", "baz", "qux", "quux"]);
        let values_array = super::Int64Array::from(vec![10, 20, 30, 40, 50]);

        let keys = Arc::new(Field::new("keys", DataType::Utf8, false));
        let values = Arc::new(Field::new("values", DataType::Int64, false));
        let entry_struct = StructArray::from(vec![
            (keys, Arc::new(keys_array) as ArrayRef),
            (values, Arc::new(values_array) as ArrayRef),
        ]);

        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );

        // [{"foo": 10}, null, {}, {"bar": 20, "baz": 30, "qux": 40}, {"quux": 50}, {}]
        let entry_offsets = Buffer::from([0, 1, 1, 1, 4, 5, 5].to_byte_slice());
        let valid_buffer = Buffer::from([0b00111101]);

        let map_data = ArrayData::builder(map_data_type.clone())
            .len(6)
            .null_bit_buffer(Some(valid_buffer))
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();

        let map = MapArray::from(map_data);

        let map_field = Field::new("map", map_data_type, true);
        let schema = Arc::new(Schema::new(vec![map_field]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(map)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"map":{"foo":10}}
{}
{"map":{}}
{"map":{"bar":20,"baz":30,"qux":40}}
{"map":{"quux":50}}
{"map":{}}
"#,
        );
    }

    #[test]
    fn test_write_single_batch() {
        let test_file = "test/data/basic.json";
        let file = File::open(test_file).unwrap();
        let mut reader = BufReader::new(file);
        let (schema, _) = infer_json_schema(&mut reader, None).unwrap();
        reader.rewind().unwrap();

        let builder = ReaderBuilder::new(Arc::new(schema)).with_batch_size(1024);
        let mut reader = builder.build(reader).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write(&batch).unwrap();
        }

        let result = str::from_utf8(&buf).unwrap();
        let expected = read_to_string(test_file).unwrap();
        for (r, e) in result.lines().zip(expected.lines()) {
            let mut expected_json = serde_json::from_str::<Value>(e).unwrap();
            // remove null value from object to make comparison consistent:
            if let Value::Object(obj) = expected_json {
                expected_json =
                    Value::Object(obj.into_iter().filter(|(_, v)| *v != Value::Null).collect());
            }
            assert_eq!(serde_json::from_str::<Value>(r).unwrap(), expected_json,);
        }
    }

    #[test]
    fn test_write_multi_batches() {
        let test_file = "test/data/basic.json";

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Boolean, true),
            Field::new("d", DataType::Utf8, true),
            Field::new("e", DataType::Utf8, true),
            Field::new("f", DataType::Utf8, true),
            Field::new("g", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("h", DataType::Float16, true),
        ]));

        let mut reader = ReaderBuilder::new(schema.clone())
            .build(BufReader::new(File::open(test_file).unwrap()))
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        // test batches = an empty batch + 2 same batches, finally result should be eq to 2 same batches
        let batches = [&RecordBatch::new_empty(schema), &batch, &batch];

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&batches).unwrap();
        }

        let result = str::from_utf8(&buf).unwrap();
        let expected = read_to_string(test_file).unwrap();
        // result is eq to 2 same batches
        let expected = format!("{expected}\n{expected}");
        for (r, e) in result.lines().zip(expected.lines()) {
            let mut expected_json = serde_json::from_str::<Value>(e).unwrap();
            // remove null value from object to make comparison consistent:
            if let Value::Object(obj) = expected_json {
                expected_json =
                    Value::Object(obj.into_iter().filter(|(_, v)| *v != Value::Null).collect());
            }
            assert_eq!(serde_json::from_str::<Value>(r).unwrap(), expected_json,);
        }
    }

    #[test]
    fn test_writer_explicit_nulls() -> Result<(), ArrowError> {
        fn nested_list() -> (Arc<ListArray>, Arc<Field>) {
            let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![None, None, None]),
                Some(vec![Some(1), Some(2), Some(3)]),
                None,
                Some(vec![None, None, None]),
            ]));
            let field = Arc::new(Field::new("list", array.data_type().clone(), true));
            // [{"list":[null,null,null]},{"list":[1,2,3]},{"list":null},{"list":[null,null,null]}]
            (array, field)
        }

        fn nested_dict() -> (Arc<DictionaryArray<Int32Type>>, Arc<Field>) {
            let array = Arc::new(DictionaryArray::from_iter(vec![
                Some("cupcakes"),
                None,
                Some("bear"),
                Some("kuma"),
            ]));
            let field = Arc::new(Field::new("dict", array.data_type().clone(), true));
            // [{"dict":"cupcakes"},{"dict":null},{"dict":"bear"},{"dict":"kuma"}]
            (array, field)
        }

        fn nested_map() -> (Arc<MapArray>, Arc<Field>) {
            let string_builder = StringBuilder::new();
            let int_builder = Int64Builder::new();
            let mut builder = MapBuilder::new(None, string_builder, int_builder);

            // [{"foo": 10}, null, {}, {"bar": 20, "baz": 30, "qux": 40}]
            builder.keys().append_value("foo");
            builder.values().append_value(10);
            builder.append(true).unwrap();

            builder.append(false).unwrap();

            builder.append(true).unwrap();

            builder.keys().append_value("bar");
            builder.values().append_value(20);
            builder.keys().append_value("baz");
            builder.values().append_value(30);
            builder.keys().append_value("qux");
            builder.values().append_value(40);
            builder.append(true).unwrap();

            let array = Arc::new(builder.finish());
            let field = Arc::new(Field::new("map", array.data_type().clone(), true));
            (array, field)
        }

        fn root_list() -> (Arc<ListArray>, Field) {
            let struct_array = StructArray::from(vec![
                (
                    Arc::new(Field::new("utf8", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, None])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("int32", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![Some(1), None, Some(5), None])) as ArrayRef,
                ),
            ]);

            let field = Field::new_list(
                "list",
                Field::new("struct", struct_array.data_type().clone(), true),
                true,
            );

            // [{"list":[{"int32":1,"utf8":"a"},{"int32":null,"utf8":"b"}]},{"list":null},{"list":[{int32":5,"utf8":null}]},{"list":null}]
            let entry_offsets = Buffer::from([0, 2, 2, 3, 3].to_byte_slice());
            let data = ArrayData::builder(field.data_type().clone())
                .len(4)
                .add_buffer(entry_offsets)
                .add_child_data(struct_array.into_data())
                .null_bit_buffer(Some([0b00000101].into()))
                .build()
                .unwrap();
            let array = Arc::new(ListArray::from(data));
            (array, field)
        }

        let (nested_list_array, nested_list_field) = nested_list();
        let (nested_dict_array, nested_dict_field) = nested_dict();
        let (nested_map_array, nested_map_field) = nested_map();
        let (root_list_array, root_list_field) = root_list();

        let schema = Schema::new(vec![
            Field::new("date", DataType::Date32, true),
            Field::new("null", DataType::Null, true),
            Field::new_struct(
                "struct",
                vec![
                    Arc::new(Field::new("utf8", DataType::Utf8, true)),
                    nested_list_field.clone(),
                    nested_dict_field.clone(),
                    nested_map_field.clone(),
                ],
                true,
            ),
            root_list_field,
        ]);

        let arr_date32 = Date32Array::from(vec![Some(0), None, Some(1), None]);
        let arr_null = NullArray::new(4);
        let arr_struct = StructArray::from(vec![
            // [{"utf8":"a"},{"utf8":null},{"utf8":null},{"utf8":"b"}]
            (
                Arc::new(Field::new("utf8", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("a"), None, None, Some("b")])) as ArrayRef,
            ),
            // [{"list":[null,null,null]},{"list":[1,2,3]},{"list":null},{"list":[null,null,null]}]
            (nested_list_field, nested_list_array as ArrayRef),
            // [{"dict":"cupcakes"},{"dict":null},{"dict":"bear"},{"dict":"kuma"}]
            (nested_dict_field, nested_dict_array as ArrayRef),
            // [{"foo": 10}, null, {}, {"bar": 20, "baz": 30, "qux": 40}]
            (nested_map_field, nested_map_array as ArrayRef),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                // [{"date":"1970-01-01"},{"date":null},{"date":"1970-01-02"},{"date":null}]
                Arc::new(arr_date32),
                // [{"null":null},{"null":null},{"null":null},{"null":null}]
                Arc::new(arr_null),
                Arc::new(arr_struct),
                // [{"list":[{"int32":1,"utf8":"a"},{"int32":null,"utf8":"b"}]},{"list":null},{"list":[{int32":5,"utf8":null}]},{"list":null}]
                root_list_array,
            ],
        )?;

        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut buf);
            writer.write_batches(&[&batch])?;
            writer.finish()?;
        }

        let actual = serde_json::from_slice::<Vec<Value>>(&buf).unwrap();
        let expected = serde_json::from_value::<Vec<Value>>(json!([
          {
            "date": "1970-01-01",
            "list": [
              {
                "int32": 1,
                "utf8": "a"
              },
              {
                "int32": null,
                "utf8": "b"
              }
            ],
            "null": null,
            "struct": {
              "dict": "cupcakes",
              "list": [
                null,
                null,
                null
              ],
              "map": {
                "foo": 10
              },
              "utf8": "a"
            }
          },
          {
            "date": null,
            "list": null,
            "null": null,
            "struct": {
              "dict": null,
              "list": [
                1,
                2,
                3
              ],
              "map": null,
              "utf8": null
            }
          },
          {
            "date": "1970-01-02",
            "list": [
              {
                "int32": 5,
                "utf8": null
              }
            ],
            "null": null,
            "struct": {
              "dict": "bear",
              "list": null,
              "map": {},
              "utf8": null
            }
          },
          {
            "date": null,
            "list": null,
            "null": null,
            "struct": {
              "dict": "kuma",
              "list": [
                null,
                null,
                null
              ],
              "map": {
                "bar": 20,
                "baz": 30,
                "qux": 40
              },
              "utf8": "b"
            }
          }
        ]))
        .unwrap();

        assert_eq!(actual, expected);

        Ok(())
    }

    fn binary_encoding_test<O: OffsetSizeTrait>() {
        // set up schema
        let schema = SchemaRef::new(Schema::new(vec![Field::new(
            "bytes",
            GenericBinaryType::<O>::DATA_TYPE,
            true,
        )]));

        // build record batch:
        let mut builder = GenericByteBuilder::<GenericBinaryType<O>>::new();
        let values = [Some(b"Ned Flanders"), None, Some(b"Troy McClure")];
        for value in values {
            match value {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        let array = Arc::new(builder.finish()) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        // encode and check JSON with explicit nulls:
        {
            let mut buf = Vec::new();
            let json_value: Value = {
                let mut writer = WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, JsonArray>(&mut buf);
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                serde_json::from_slice(&buf).unwrap()
            };

            assert_eq!(
                json!([
                    {
                        "bytes": "4e656420466c616e64657273"
                    },
                    {
                        "bytes": null // the explicit null
                    },
                    {
                        "bytes": "54726f79204d63436c757265"
                    }
                ]),
                json_value,
            );
        }

        // encode and check JSON with no explicit nulls:
        {
            let mut buf = Vec::new();
            let json_value: Value = {
                // explicit nulls are off by default, so we don't need
                // to set that when creating the writer:
                let mut writer = ArrayWriter::new(&mut buf);
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                serde_json::from_slice(&buf).unwrap()
            };

            assert_eq!(
                json!([
                    {
                        "bytes": "4e656420466c616e64657273"
                    },
                    {}, // empty because nulls are omitted
                    {
                        "bytes": "54726f79204d63436c757265"
                    }
                ]),
                json_value
            );
        }
    }

    #[test]
    fn test_writer_binary() {
        // Binary:
        binary_encoding_test::<i32>();
        // LargeBinary:
        binary_encoding_test::<i64>();
    }

    #[test]
    fn test_writer_fixed_size_binary() {
        // set up schema:
        let size = 11;
        let schema = SchemaRef::new(Schema::new(vec![Field::new(
            "bytes",
            DataType::FixedSizeBinary(size),
            true,
        )]));

        // build record batch:
        let mut builder = FixedSizeBinaryBuilder::new(size);
        let values = [Some(b"hello world"), None, Some(b"summer rain")];
        for value in values {
            match value {
                Some(v) => builder.append_value(v).unwrap(),
                None => builder.append_null(),
            }
        }
        let array = Arc::new(builder.finish()) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        // encode and check JSON with explicit nulls:
        {
            let mut buf = Vec::new();
            let json_value: Value = {
                let mut writer = WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, JsonArray>(&mut buf);
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                serde_json::from_slice(&buf).unwrap()
            };

            assert_eq!(
                json!([
                    {
                        "bytes": "68656c6c6f20776f726c64"
                    },
                    {
                        "bytes": null // the explicit null
                    },
                    {
                        "bytes": "73756d6d6572207261696e"
                    }
                ]),
                json_value,
            );
        }
        // encode and check JSON with no explicit nulls:
        {
            let mut buf = Vec::new();
            let json_value: Value = {
                // explicit nulls are off by default, so we don't need
                // to set that when creating the writer:
                let mut writer = ArrayWriter::new(&mut buf);
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                serde_json::from_slice(&buf).unwrap()
            };

            assert_eq!(
                json!([
                    {
                        "bytes": "68656c6c6f20776f726c64"
                    },
                    {}, // empty because nulls are omitted
                    {
                        "bytes": "73756d6d6572207261696e"
                    }
                ]),
                json_value,
            );
        }
    }

    #[test]
    fn test_writer_fixed_size_list() {
        let size = 3;
        let field = FieldRef::new(Field::new("item", DataType::Int32, true));
        let schema = SchemaRef::new(Schema::new(vec![Field::new(
            "list",
            DataType::FixedSizeList(field, size),
            true,
        )]));

        let values_builder = Int32Builder::new();
        let mut list_builder = FixedSizeListBuilder::new(values_builder, size);
        let lists = [
            Some([Some(1), Some(2), None]),
            Some([Some(3), None, Some(4)]),
            Some([None, Some(5), Some(6)]),
            None,
        ];
        for list in lists {
            match list {
                Some(l) => {
                    for value in l {
                        match value {
                            Some(v) => list_builder.values().append_value(v),
                            None => list_builder.values().append_null(),
                        }
                    }
                    list_builder.append(true);
                }
                None => {
                    for _ in 0..size {
                        list_builder.values().append_null();
                    }
                    list_builder.append(false);
                }
            }
        }
        let array = Arc::new(list_builder.finish()) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        //encode and check JSON with explicit nulls:
        {
            let json_value: Value = {
                let mut buf = Vec::new();
                let mut writer = WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, JsonArray>(&mut buf);
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                serde_json::from_slice(&buf).unwrap()
            };
            assert_eq!(
                json!([
                    {"list": [1, 2, null]},
                    {"list": [3, null, 4]},
                    {"list": [null, 5, 6]},
                    {"list": null},
                ]),
                json_value
            );
        }
        // encode and check JSON with no explicit nulls:
        {
            let json_value: Value = {
                let mut buf = Vec::new();
                let mut writer = ArrayWriter::new(&mut buf);
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                serde_json::from_slice(&buf).unwrap()
            };
            assert_eq!(
                json!([
                    {"list": [1, 2, null]},
                    {"list": [3, null, 4]},
                    {"list": [null, 5, 6]},
                    {}, // empty because nulls are omitted
                ]),
                json_value
            );
        }
    }

    #[test]
    fn test_writer_null_dict() {
        let keys = Int32Array::from_iter(vec![Some(0), None, Some(1)]);
        let values = Arc::new(StringArray::from_iter(vec![Some("a"), None]));
        let dict = DictionaryArray::new(keys, values);

        let schema = SchemaRef::new(Schema::new(vec![Field::new(
            "my_dict",
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            true,
        )]));

        let array = Arc::new(dict) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let mut json = Vec::new();
        let write_builder = WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = write_builder.build::<_, JsonArray>(&mut json);
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let json_str = str::from_utf8(&json).unwrap();
        assert_eq!(
            json_str,
            r#"[{"my_dict":"a"},{"my_dict":null},{"my_dict":null}]"#
        )
    }

    #[test]
    fn test_decimal128_encoder() {
        let array = Decimal128Array::from_iter_values([1234, 5678, 9012])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let field = Arc::new(Field::new("decimal", array.data_type().clone(), true));
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"decimal":12.34}
{"decimal":56.78}
{"decimal":90.12}
"#,
        );
    }

    #[test]
    fn test_decimal256_encoder() {
        let array = Decimal256Array::from_iter_values([
            i256::from(123400),
            i256::from(567800),
            i256::from(901200),
        ])
        .with_precision_and_scale(10, 4)
        .unwrap();
        let field = Arc::new(Field::new("decimal", array.data_type().clone(), true));
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"decimal":12.3400}
{"decimal":56.7800}
{"decimal":90.1200}
"#,
        );
    }

    #[test]
    fn test_decimal_encoder_with_nulls() {
        let array = Decimal128Array::from_iter([Some(1234), None, Some(5678)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let field = Arc::new(Field::new("decimal", array.data_type().clone(), true));
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        assert_json_eq(
            &buf,
            r#"{"decimal":12.34}
{}
{"decimal":56.78}
"#,
        );
    }
}
