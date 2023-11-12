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
//! ## Writing JSON Objects
//!
//! To serialize [`RecordBatch`]es into array of
//! [JSON](https://docs.serde.rs/serde_json/) objects, use
//! [`record_batches_to_json_rows`]:
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
//! let json_rows = arrow_json::writer::record_batches_to_json_rows(&[&batch]).unwrap();
//! assert_eq!(
//!     serde_json::Value::Object(json_rows[1].clone()),
//!     serde_json::json!({"a": 2}),
//! );
//! ```
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
//! In order to explicitly write null values for keys, use the alternative
//! [`LineDelimitedWriterWithNulls`] and [`ArrayWriterWithNulls`] versions.

use std::iter;
use std::marker::PhantomData;
use std::{fmt::Debug, io::Write};

use serde_json::map::Map as JsonMap;
use serde_json::Value;

use crate::JsonSerializable;
use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::*;

use arrow_cast::display::{ArrayFormatter, FormatOptions};

/// This trait controls whether null values should be written explicitly
/// for keys in objects, or whether the key should be omitted entirely.
pub trait NullHandler {
    /// How to insert a maybe null into a JSON object
    fn insert_value(row: &mut JsonMap<String, Value>, col_name: &str, value: Option<Value>);

    /// How to handle inserting a [`NullArray`] into a list of JSON objects.
    fn insert_null_array(rows: &mut [JsonMap<String, Value>], col_name: &str);
}

/// Skips writing keys that have null values.
///
/// For example, with [`LineDelimited`] format:
///
/// ```json
/// {"foo":1}
/// {"foo":1,"bar":2}
/// {}
/// ```
pub struct SkipNulls {}

impl NullHandler for SkipNulls {
    #[inline]
    fn insert_value(row: &mut JsonMap<String, Value>, col_name: &str, value: Option<Value>) {
        if let Some(j) = value {
            row.insert(col_name.to_string(), j);
        }
    }

    #[inline]
    fn insert_null_array(_rows: &mut [JsonMap<String, Value>], _col_namee: &str) {}
}

/// Writes keys that have null values.
///
/// For example, with [`LineDelimited`] format:
///
/// ```json
/// {"foo":1,"bar":null}
/// {"foo":1,"bar":2}
/// {"foo":null,"bar":null}
/// ```
pub struct KeepNulls {}

impl NullHandler for KeepNulls {
    #[inline]
    fn insert_value(row: &mut JsonMap<String, Value>, col_name: &str, value: Option<Value>) {
        if let Some(j) = value {
            row.insert(col_name.to_string(), j);
        } else {
            row.insert(col_name.to_string(), Value::Null);
        }
    }

    #[inline]
    fn insert_null_array(rows: &mut [JsonMap<String, Value>], col_name: &str) {
        rows.iter_mut().for_each(|row| {
            row.insert(col_name.to_string(), Value::Null);
        });
    }
}

fn primitive_array_to_json<T>(array: &dyn Array) -> Result<Vec<Value>, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: JsonSerializable,
{
    Ok(array
        .as_primitive::<T>()
        .iter()
        .map(|maybe_value| match maybe_value {
            Some(v) => v.into_json_value().unwrap_or(Value::Null),
            None => Value::Null,
        })
        .collect())
}

fn struct_array_to_jsonmap_array<N: NullHandler>(
    array: &StructArray,
) -> Result<Vec<JsonMap<String, Value>>, ArrowError> {
    let inner_col_names = array.column_names();

    let mut inner_objs = iter::repeat(JsonMap::new())
        .take(array.len())
        .collect::<Vec<JsonMap<String, Value>>>();

    for (j, struct_col) in array.columns().iter().enumerate() {
        set_column_for_json_rows::<N>(&mut inner_objs, struct_col, inner_col_names[j])?
    }
    Ok(inner_objs)
}

/// Converts an arrow [`Array`] into a `Vec` of Serde JSON [`serde_json::Value`]'s
pub fn array_to_json_array(array: &dyn Array) -> Result<Vec<Value>, ArrowError> {
    // For backwards compatibility, default to SkipNulls
    array_to_json_array_internal::<SkipNulls>(array)
}

fn array_to_json_array_internal<N: NullHandler>(
    array: &dyn Array,
) -> Result<Vec<Value>, ArrowError> {
    match array.data_type() {
        DataType::Null => Ok(iter::repeat(Value::Null).take(array.len()).collect()),
        DataType::Boolean => Ok(array
            .as_boolean()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect()),

        DataType::Utf8 => Ok(array
            .as_string::<i32>()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect()),
        DataType::LargeUtf8 => Ok(array
            .as_string::<i64>()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect()),
        DataType::Int8 => primitive_array_to_json::<Int8Type>(array),
        DataType::Int16 => primitive_array_to_json::<Int16Type>(array),
        DataType::Int32 => primitive_array_to_json::<Int32Type>(array),
        DataType::Int64 => primitive_array_to_json::<Int64Type>(array),
        DataType::UInt8 => primitive_array_to_json::<UInt8Type>(array),
        DataType::UInt16 => primitive_array_to_json::<UInt16Type>(array),
        DataType::UInt32 => primitive_array_to_json::<UInt32Type>(array),
        DataType::UInt64 => primitive_array_to_json::<UInt64Type>(array),
        DataType::Float16 => primitive_array_to_json::<Float16Type>(array),
        DataType::Float32 => primitive_array_to_json::<Float32Type>(array),
        DataType::Float64 => primitive_array_to_json::<Float64Type>(array),
        DataType::List(_) => as_list_array(array)
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => Ok(Value::Array(array_to_json_array_internal::<N>(&v)?)),
                None => Ok(Value::Null),
            })
            .collect(),
        DataType::LargeList(_) => as_large_list_array(array)
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => Ok(Value::Array(array_to_json_array_internal::<N>(&v)?)),
                None => Ok(Value::Null),
            })
            .collect(),
        DataType::FixedSizeList(_, _) => as_fixed_size_list_array(array)
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => Ok(Value::Array(array_to_json_array_internal::<N>(&v)?)),
                None => Ok(Value::Null),
            })
            .collect(),
        DataType::Struct(_) => {
            let jsonmaps = struct_array_to_jsonmap_array::<N>(array.as_struct())?;
            Ok(jsonmaps.into_iter().map(Value::Object).collect())
        }
        DataType::Map(_, _) => as_map_array(array)
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => Ok(Value::Array(array_to_json_array_internal::<N>(&v)?)),
                None => Ok(Value::Null),
            })
            .collect(),
        t => Err(ArrowError::JsonError(format!(
            "data type {t:?} not supported"
        ))),
    }
}

macro_rules! set_column_by_array_type {
    ($cast_fn:ident, $col_name:ident, $rows:ident, $array:ident, $null_handler:ty) => {
        let arr = $cast_fn($array);
        $rows
            .iter_mut()
            .zip(arr.iter())
            .for_each(|(row, maybe_value)| {
                <$null_handler>::insert_value(row, $col_name, maybe_value.map(Into::into))
            });
    };
}

fn set_column_by_primitive_type<T, N>(
    rows: &mut [JsonMap<String, Value>],
    array: &ArrayRef,
    col_name: &str,
) where
    T: ArrowPrimitiveType,
    T::Native: JsonSerializable,
    N: NullHandler,
{
    let primitive_arr = array.as_primitive::<T>();

    rows.iter_mut()
        .zip(primitive_arr.iter())
        .for_each(|(row, maybe_value)| {
            N::insert_value(row, col_name, maybe_value.and_then(|v| v.into_json_value()))
        });
}

fn set_column_for_json_rows<N: NullHandler>(
    rows: &mut [JsonMap<String, Value>],
    array: &ArrayRef,
    col_name: &str,
) -> Result<(), ArrowError> {
    match array.data_type() {
        DataType::Int8 => {
            set_column_by_primitive_type::<Int8Type, N>(rows, array, col_name);
        }
        DataType::Int16 => {
            set_column_by_primitive_type::<Int16Type, N>(rows, array, col_name);
        }
        DataType::Int32 => {
            set_column_by_primitive_type::<Int32Type, N>(rows, array, col_name);
        }
        DataType::Int64 => {
            set_column_by_primitive_type::<Int64Type, N>(rows, array, col_name);
        }
        DataType::UInt8 => {
            set_column_by_primitive_type::<UInt8Type, N>(rows, array, col_name);
        }
        DataType::UInt16 => {
            set_column_by_primitive_type::<UInt16Type, N>(rows, array, col_name);
        }
        DataType::UInt32 => {
            set_column_by_primitive_type::<UInt32Type, N>(rows, array, col_name);
        }
        DataType::UInt64 => {
            set_column_by_primitive_type::<UInt64Type, N>(rows, array, col_name);
        }
        DataType::Float16 => {
            set_column_by_primitive_type::<Float16Type, N>(rows, array, col_name);
        }
        DataType::Float32 => {
            set_column_by_primitive_type::<Float32Type, N>(rows, array, col_name);
        }
        DataType::Float64 => {
            set_column_by_primitive_type::<Float64Type, N>(rows, array, col_name);
        }
        DataType::Null => {
            N::insert_null_array(rows, col_name);
        }
        DataType::Boolean => {
            set_column_by_array_type!(as_boolean_array, col_name, rows, array, N);
        }
        DataType::Utf8 => {
            set_column_by_array_type!(as_string_array, col_name, rows, array, N);
        }
        DataType::LargeUtf8 => {
            set_column_by_array_type!(as_largestring_array, col_name, rows, array, N);
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let options = FormatOptions::default();
            let formatter = ArrayFormatter::try_new(array.as_ref(), &options)?;
            let nulls = array.nulls();
            rows.iter_mut().enumerate().for_each(|(idx, row)| {
                let maybe_value = nulls
                    .map(|x| x.is_valid(idx))
                    .unwrap_or(true)
                    .then(|| formatter.value(idx).to_string().into());
                N::insert_value(row, col_name, maybe_value);
            });
        }
        DataType::Struct(_) => {
            let inner_objs = struct_array_to_jsonmap_array::<N>(array.as_struct())?;
            rows.iter_mut().zip(inner_objs).for_each(|(row, obj)| {
                row.insert(col_name.to_string(), Value::Object(obj));
            });
        }
        DataType::List(_) => {
            let listarr = as_list_array(array);
            rows.iter_mut().zip(listarr.iter()).try_for_each(
                |(row, maybe_value)| -> Result<(), ArrowError> {
                    let maybe_value = maybe_value
                        .map(|v| array_to_json_array_internal::<N>(&v).map(Value::Array))
                        .transpose()?;
                    N::insert_value(row, col_name, maybe_value);
                    Ok(())
                },
            )?;
        }
        DataType::LargeList(_) => {
            let listarr = as_large_list_array(array);
            rows.iter_mut().zip(listarr.iter()).try_for_each(
                |(row, maybe_value)| -> Result<(), ArrowError> {
                    let maybe_value = maybe_value
                        .map(|v| array_to_json_array_internal::<N>(&v).map(Value::Array))
                        .transpose()?;
                    N::insert_value(row, col_name, maybe_value);
                    Ok(())
                },
            )?;
        }
        DataType::Dictionary(_, value_type) => {
            let hydrated = arrow_cast::cast::cast(&array, value_type)
                .expect("cannot cast dictionary to underlying values");
            set_column_for_json_rows::<N>(rows, &hydrated, col_name)?;
        }
        DataType::Map(_, _) => {
            let maparr = as_map_array(array);

            let keys = maparr.keys();
            let values = maparr.values();

            // Keys have to be strings to convert to json.
            if !matches!(keys.data_type(), DataType::Utf8) {
                return Err(ArrowError::JsonError(format!(
                    "data type {:?} not supported in nested map for json writer",
                    keys.data_type()
                )));
            }

            let keys = keys.as_string::<i32>();
            let values = array_to_json_array_internal::<N>(values)?;

            let mut kv = keys.iter().zip(values);

            for (i, row) in rows.iter_mut().enumerate() {
                if maparr.is_null(i) {
                    row.insert(col_name.to_string(), serde_json::Value::Null);
                    continue;
                }

                let len = maparr.value_length(i) as usize;
                let mut obj = serde_json::Map::new();

                for (_, (k, v)) in (0..len).zip(&mut kv) {
                    obj.insert(k.expect("keys in a map should be non-null").to_string(), v);
                }

                row.insert(col_name.to_string(), serde_json::Value::Object(obj));
            }
        }
        _ => {
            return Err(ArrowError::JsonError(format!(
                "data type {:?} not supported in nested map for json writer",
                array.data_type()
            )))
        }
    }
    Ok(())
}

/// Converts an arrow [`RecordBatch`] into a `Vec` of Serde JSON
/// [`JsonMap`]s (objects)
pub fn record_batches_to_json_rows(
    batches: &[&RecordBatch],
) -> Result<Vec<JsonMap<String, Value>>, ArrowError> {
    // For backwards compatibility, default to SkipNulls
    record_batches_to_json_rows_internal::<SkipNulls>(batches)
}

fn record_batches_to_json_rows_internal<N: NullHandler>(
    batches: &[&RecordBatch],
) -> Result<Vec<JsonMap<String, Value>>, ArrowError> {
    let mut rows: Vec<JsonMap<String, Value>> = iter::repeat(JsonMap::new())
        .take(batches.iter().map(|b| b.num_rows()).sum())
        .collect();

    if !rows.is_empty() {
        let schema = batches[0].schema();
        let mut base = 0;
        for batch in batches {
            let row_count = batch.num_rows();
            let row_slice = &mut rows[base..base + batch.num_rows()];
            for (j, col) in batch.columns().iter().enumerate() {
                let col_name = schema.field(j).name();
                set_column_for_json_rows::<N>(row_slice, col, col_name)?
            }
            base += row_count;
        }
    }

    Ok(rows)
}

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
///
/// Will skip writing keys with null values.
pub type LineDelimitedWriter<W> = Writer<W, LineDelimited, SkipNulls>;

/// Similar to [`LineDelimitedWriter`] but will keep keys with null values.
pub type LineDelimitedWriterWithNulls<W> = Writer<W, LineDelimited, KeepNulls>;

/// A JSON writer which serializes [`RecordBatch`]es to JSON arrays.
///
/// Will skip writing keys with null values.
pub type ArrayWriter<W> = Writer<W, JsonArray, SkipNulls>;

/// Similar to [`ArrayWriter`] but will keep keys with null values.
pub type ArrayWriterWithNulls<W> = Writer<W, JsonArray, KeepNulls>;

/// A JSON writer which serializes [`RecordBatch`]es to a stream of
/// `u8` encoded JSON objects.
///
/// See the module level documentation for detailed usage and examples.
/// The specific format of the stream is controlled by the [`JsonFormat`]
/// type parameter. The [`NullHandler`] type parameter controls whether
/// nulls should be written explicitly for keys or skipped. Default is
/// [`SkipNulls`] for backward compatibility.
#[derive(Debug)]
pub struct Writer<W, F, N = SkipNulls>
where
    W: Write,
    F: JsonFormat,
    N: NullHandler,
{
    /// Underlying writer to use to write bytes
    writer: W,

    /// Has the writer output any records yet?
    started: bool,

    /// Is the writer finished?
    finished: bool,

    /// Determines how the byte stream is formatted
    format: F,

    /// Determines whether nulls should be written for keys or omitted
    null_handler: PhantomData<N>,
}

impl<W, F, N> Writer<W, F, N>
where
    W: Write,
    F: JsonFormat,
    N: NullHandler,
{
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            started: false,
            finished: false,
            format: F::default(),
            null_handler: Default::default(),
        }
    }

    /// Write a single JSON row to the output writer
    pub fn write_row(&mut self, row: &Value) -> Result<(), ArrowError> {
        let is_first_row = !self.started;
        if !self.started {
            self.format.start_stream(&mut self.writer)?;
            self.started = true;
        }

        self.format.start_row(&mut self.writer, is_first_row)?;
        self.writer.write_all(
            &serde_json::to_vec(row).map_err(|error| ArrowError::JsonError(error.to_string()))?,
        )?;
        self.format.end_row(&mut self.writer)?;
        Ok(())
    }

    /// Convert the `RecordBatch` into JSON rows, and write them to the output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        for row in record_batches_to_json_rows_internal::<N>(&[batch])? {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }

    /// Convert the [`RecordBatch`] into JSON rows, and write them to the output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for row in record_batches_to_json_rows_internal::<N>(batches)? {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced. (e.g. producing the final `']'` if writing
    /// arrays.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        if self.started && !self.finished {
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

impl<W, F, N> RecordBatchWriter for Writer<W, F, N>
where
    W: Write,
    F: JsonFormat,
    N: NullHandler,
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
    use std::fs::{read_to_string, File};
    use std::io::{BufReader, Seek};
    use std::sync::Arc;

    use serde_json::json;

    use arrow_array::builder::{Int32Builder, Int64Builder, MapBuilder, StringBuilder};
    use arrow_buffer::{Buffer, ToByteSlice};
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
    fn write_timestamps() {
        let ts_string = "2018-11-13T17:11:10.011375885995";
        let ts_nanos = ts_string
            .parse::<chrono::NaiveDateTime>()
            .unwrap()
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
            r#"{"duration_sec":"PT120S","duration_msec":"PT0.120S","duration_usec":"PT0.000120S","duration_nsec":"PT0.000000120S","name":"a"}
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
        let a_value_offsets = Buffer::from(&[0, 2, 3, 4, 5, 6].to_byte_slice());
        let a_list_data = ArrayData::builder(field_c1.data_type().clone())
            .len(5)
            .add_buffer(a_value_offsets)
            .add_child_data(a_values.into_data())
            .null_bit_buffer(Some(Buffer::from(vec![0b00011111])))
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

        let a_value_offsets = Buffer::from(&[0, 2, 3, 6].to_byte_slice());
        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(list_inner_type.data_type().clone())
            .len(3)
            .add_buffer(a_value_offsets)
            .null_bit_buffer(Some(Buffer::from(vec![0b00000111])))
            .add_child_data(a_values.into_data())
            .build()
            .unwrap();

        let c1_value_offsets = Buffer::from(&[0, 2, 2, 3].to_byte_slice());
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
        let c1_value_offsets = Buffer::from(&[0, 2, 2, 3].to_byte_slice());
        let c1_list_data = ArrayData::builder(field_c1.data_type().clone())
            .len(3)
            .add_buffer(c1_value_offsets)
            .add_child_data(struct_values.into_data())
            .null_bit_buffer(Some(Buffer::from(vec![0b00000101])))
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
                let mut writer = LineDelimitedWriterWithNulls::new(&mut buf);
                writer.write_batches(&[&batch]).unwrap();
            }
        }

        let result = String::from_utf8(buf).unwrap();
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
    fn json_writer_empty() {
        let mut writer = ArrayWriter::new(vec![] as Vec<u8>);
        writer.finish().unwrap();
        assert_eq!(String::from_utf8(writer.into_inner()).unwrap(), "");
    }

    #[test]
    fn json_writer_one_row() {
        let mut writer = ArrayWriter::new(vec![] as Vec<u8>);
        let v = json!({ "an": "object" });
        writer.write_row(&v).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(writer.into_inner()).unwrap(),
            r#"[{"an":"object"}]"#
        );
    }

    #[test]
    fn json_writer_two_rows() {
        let mut writer = ArrayWriter::new(vec![] as Vec<u8>);
        let v = json!({ "an": "object" });
        writer.write_row(&v).unwrap();
        let v = json!({ "another": "object" });
        writer.write_row(&v).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(writer.into_inner()).unwrap(),
            r#"[{"an":"object"},{"another":"object"}]"#
        );
    }

    #[test]
    #[allow(deprecated)]
    fn json_list_roundtrip() {
        let json_content = r#"
        {"list": [{"ints": 1}]}
        {"list": [{}]}
        {"list": []}
        {"list": null}
        {"list": [{"ints": null}]}
        {"list": [null]}
        "#;
        let ints_struct = DataType::Struct(vec![Field::new("ints", DataType::Int32, true)].into());
        let list_type = DataType::List(Arc::new(Field::new("item", ints_struct, true)));
        let list_field = Field::new("list", list_type, true);
        let schema = Arc::new(Schema::new(vec![list_field]));
        let builder = ReaderBuilder::new(schema).with_batch_size(64);
        let mut reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

        let batch = reader.next().unwrap().unwrap();

        let list_row = batch.column(0).as_list::<i32>();
        let values = list_row.values();
        assert_eq!(values.len(), 4);
        assert_eq!(values.null_count(), 1);

        // write the batch to JSON, and compare output with input
        let mut buf = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer.write_batches(&[&batch]).unwrap();
        }

        // NOTE: The last value should technically be {"list": [null]} but it appears
        // that implementations differ on the treatment of a null struct.
        // It would be more accurate to return a null struct, so this can be done
        // as a follow up.
        assert_json_eq(
            &buf,
            r#"{"list":[{"ints":1}]}
{"list":[{}]}
{"list":[]}
{}
{"list":[{}]}
{"list":[{}]}
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
        let entry_offsets = Buffer::from(&[0, 1, 1, 1, 4, 5, 5].to_byte_slice());
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
{"map":null}
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

        let result = String::from_utf8(buf).unwrap();
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

        let result = String::from_utf8(buf).unwrap();
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
    fn test_array_to_json_array_for_fixed_size_list_array() {
        let expected_json = vec![
            json!([0, 1, 2]),
            json!(null),
            json!([3, null, 5]),
            json!([6, 7, 45]),
        ];

        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7), Some(45)]),
        ];

        let list_array = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(data, 3);
        let list_array = Arc::new(list_array) as ArrayRef;

        assert_eq!(array_to_json_array(&list_array).unwrap(), expected_json);
    }

    #[test]
    fn test_array_to_json_array_for_map_array() {
        let expected_json = serde_json::from_value::<Vec<Value>>(json!([
            [
                {
                    "keys": "joe",
                    "values": 1
                }
            ],
            [
                {
                    "keys": "blogs",
                    "values": 2
                },
                {
                    "keys": "foo",
                    "values": 4
                }
            ],
            [],
            null
        ]))
        .unwrap();

        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);

        let mut builder = MapBuilder::new(None, string_builder, int_builder);

        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();

        let array = builder.finish();

        let map_array = Arc::new(array) as ArrayRef;

        assert_eq!(array_to_json_array(&map_array).unwrap(), expected_json);
    }

    #[test]
    fn test_writer_keep_nulls() -> Result<(), ArrowError> {
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
            let entry_offsets = Buffer::from(&[0, 2, 2, 3, 3].to_byte_slice());
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
            let mut writer = ArrayWriterWithNulls::new(&mut buf);
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
}
