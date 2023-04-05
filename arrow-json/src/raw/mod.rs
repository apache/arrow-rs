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

//! A faster JSON reader that will eventually replace [`Reader`]
//!
//! [`Reader`]: crate::reader::Reader

use crate::raw::boolean_array::BooleanArrayDecoder;
use crate::raw::decimal_array::DecimalArrayDecoder;
use crate::raw::list_array::ListArrayDecoder;
use crate::raw::map_array::MapArrayDecoder;
use crate::raw::primitive_array::PrimitiveArrayDecoder;
use crate::raw::string_array::StringArrayDecoder;
use crate::raw::struct_array::StructArrayDecoder;
use crate::raw::tape::{Tape, TapeDecoder, TapeElement};
use crate::raw::timestamp_array::TimestampArrayDecoder;
use arrow_array::timezone::Tz;
use arrow_array::types::Float32Type;
use arrow_array::types::*;
use arrow_array::{downcast_integer, make_array, RecordBatch, RecordBatchReader};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};
use chrono::Utc;
use serde::Serialize;
use std::io::BufRead;

mod boolean_array;
mod decimal_array;
mod list_array;
mod map_array;
mod primitive_array;
mod serializer;
mod string_array;
mod struct_array;
mod tape;
mod timestamp_array;

/// A builder for [`RawReader`] and [`RawDecoder`]
pub struct RawReaderBuilder {
    batch_size: usize,
    coerce_primitive: bool,

    schema: SchemaRef,
}

impl RawReaderBuilder {
    /// Create a new [`RawReaderBuilder`] with the provided [`SchemaRef`]
    ///
    /// This could be obtained using [`infer_json_schema`] if not known
    ///
    /// Any columns not present in `schema` will be ignored
    ///
    /// [`infer_json_schema`]: crate::reader::infer_json_schema
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batch_size: 1024,
            coerce_primitive: false,
            schema,
        }
    }

    /// Sets the batch size in rows to read
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Sets if the decoder should coerce primitive values (bool and number) into string when the Schema's column is Utf8 or LargeUtf8.
    pub fn coerce_primitive(self, coerce_primitive: bool) -> Self {
        Self {
            coerce_primitive,
            ..self
        }
    }

    /// Create a [`RawReader`] with the provided [`BufRead`]
    pub fn build<R: BufRead>(self, reader: R) -> Result<RawReader<R>, ArrowError> {
        Ok(RawReader {
            reader,
            decoder: self.build_decoder()?,
        })
    }

    /// Create a [`RawDecoder`]
    pub fn build_decoder(self) -> Result<RawDecoder, ArrowError> {
        let decoder = make_decoder(
            DataType::Struct(self.schema.fields.clone()),
            self.coerce_primitive,
            false,
        )?;
        let num_fields = self.schema.all_fields().len();

        Ok(RawDecoder {
            decoder,
            tape_decoder: TapeDecoder::new(self.batch_size, num_fields),
            batch_size: self.batch_size,
            schema: self.schema,
        })
    }
}

/// Reads JSON data with a known schema directly into arrow [`RecordBatch`]
///
/// This is significantly faster than [`Reader`] and eventually intended
/// to replace it ([#3610](https://github.com/apache/arrow-rs/issues/3610))
///
/// Lines consisting solely of ASCII whitespace are ignored
///
/// [`Reader`]: crate::reader::Reader
pub struct RawReader<R> {
    reader: R,
    decoder: RawDecoder,
}

impl<R> std::fmt::Debug for RawReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawReader")
            .field("decoder", &self.decoder)
            .finish()
    }
}

impl<R: BufRead> RawReader<R> {
    /// Reads the next [`RecordBatch`] returning `Ok(None)` if EOF
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let read = buf.len();

            let decoded = self.decoder.decode(buf)?;
            self.reader.consume(decoded);
            if decoded != read {
                break;
            }
        }
        self.decoder.flush()
    }
}

impl<R: BufRead> Iterator for RawReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for RawReader<R> {
    fn schema(&self) -> SchemaRef {
        self.decoder.schema.clone()
    }
}

/// A low-level interface for reading JSON data from a byte stream
///
/// See [`RawReader`] for a higher-level interface for interface with [`BufRead`]
///
/// The push-based interface facilitates integration with sources that yield arbitrarily
/// delimited bytes ranges, such as [`BufRead`], or a chunked byte stream received from
/// object storage
///
/// ```
/// # use std::io::BufRead;
/// # use arrow_array::RecordBatch;
/// # use arrow_json::{RawDecoder, RawReaderBuilder};
/// # use arrow_schema::{ArrowError, SchemaRef};
/// #
/// fn read_from_json<R: BufRead>(
///     mut reader: R,
///     schema: SchemaRef,
/// ) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, ArrowError> {
///     let mut decoder = RawReaderBuilder::new(schema).build_decoder()?;
///     let mut next = move || {
///         loop {
///             // RawDecoder is agnostic that buf doesn't contain whole records
///             let buf = reader.fill_buf()?;
///             if buf.is_empty() {
///                 break; // Input exhausted
///             }
///             let read = buf.len();
///             let decoded = decoder.decode(buf)?;
///
///             // Consume the number of bytes read
///             reader.consume(decoded);
///             if decoded != read {
///                 break; // Read batch size
///             }
///         }
///         decoder.flush()
///     };
///     Ok(std::iter::from_fn(move || next().transpose()))
/// }
/// ```
pub struct RawDecoder {
    tape_decoder: TapeDecoder,
    decoder: Box<dyn ArrayDecoder>,
    batch_size: usize,
    schema: SchemaRef,
}

impl std::fmt::Debug for RawDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawDecoder")
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl RawDecoder {
    /// Read JSON objects from `buf`, returning the number of bytes read
    ///
    /// This method returns once `batch_size` objects have been parsed since the
    /// last call to [`Self::flush`], or `buf` is exhausted. Any remaining bytes
    /// should be included in the next call to [`Self::decode`]
    ///
    /// There is no requirement that `buf` contains a whole number of records, facilitating
    /// integration with arbitrary byte streams, such as that yielded by [`BufRead`]
    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.tape_decoder.decode(buf)
    }

    /// Serialize `rows` to this [`RawDecoder`]
    ///
    /// This provides a simple way to convert [serde]-compatible datastructures into arrow
    /// [`RecordBatch`].
    ///
    /// Custom conversion logic as described in [arrow_array::builder] will likely outperform this,
    /// especially where the schema is known at compile-time, however, this provides a mechanism
    /// to get something up and running quickly
    ///
    /// It can be used with [`serde_json::Value`]
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use serde_json::{Value, json};
    /// # use arrow_array::cast::AsArray;
    /// # use arrow_array::types::Float32Type;
    /// # use arrow_json::RawReaderBuilder;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// let json = vec![json!({"float": 2.3}), json!({"float": 5.7})];
    ///
    /// let schema = Schema::new(vec![Field::new("float", DataType::Float32, true)]);
    /// let mut decoder = RawReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
    ///
    /// decoder.serialize(&json).unwrap();
    /// let batch = decoder.flush().unwrap().unwrap();
    /// assert_eq!(batch.num_rows(), 2);
    /// assert_eq!(batch.num_columns(), 1);
    /// let values = batch.column(0).as_primitive::<Float32Type>().values();
    /// assert_eq!(values, &[2.3, 5.7])
    /// ```
    ///
    /// Or with arbitrary [`Serialize`] types
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_json::RawReaderBuilder;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use serde::Serialize;
    /// # use arrow_array::cast::AsArray;
    /// # use arrow_array::types::{Float32Type, Int32Type};
    /// #
    /// #[derive(Serialize)]
    /// struct MyStruct {
    ///     int32: i32,
    ///     float: f32,
    /// }
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new("int32", DataType::Int32, false),
    ///     Field::new("float", DataType::Float32, false),
    /// ]);
    ///
    /// let rows = vec![
    ///     MyStruct{ int32: 0, float: 3. },
    ///     MyStruct{ int32: 4, float: 67.53 },
    /// ];
    ///
    /// let mut decoder = RawReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
    /// decoder.serialize(&rows).unwrap();
    ///
    /// let batch = decoder.flush().unwrap().unwrap();
    ///
    /// // Expect batch containing two columns
    /// let int32 = batch.column(0).as_primitive::<Int32Type>();
    /// assert_eq!(int32.values(), &[0, 4]);
    ///
    /// let float = batch.column(1).as_primitive::<Float32Type>();
    /// assert_eq!(float.values(), &[3., 67.53]);
    /// ```
    ///
    /// Or even complex nested types
    ///
    /// ```
    /// # use std::collections::BTreeMap;
    /// # use std::sync::Arc;
    /// # use arrow_array::StructArray;
    /// # use arrow_cast::display::{ArrayFormatter, FormatOptions};
    /// # use arrow_json::RawReaderBuilder;
    /// # use arrow_schema::{DataType, Field, Fields, Schema};
    /// # use serde::Serialize;
    /// #
    /// #[derive(Serialize)]
    /// struct MyStruct {
    ///     int32: i32,
    ///     list: Vec<f64>,
    ///     nested: Vec<Option<Nested>>,
    /// }
    ///
    /// impl MyStruct {
    ///     /// Returns the [`Fields`] for [`MyStruct`]
    ///     fn fields() -> Fields {
    ///         let nested = DataType::Struct(Nested::fields());
    ///         Fields::from([
    ///             Arc::new(Field::new("int32", DataType::Int32, false)),
    ///             Arc::new(Field::new_list(
    ///                 "list",
    ///                 Field::new("element", DataType::Float64, false),
    ///                 false,
    ///             )),
    ///             Arc::new(Field::new_list(
    ///                 "nested",
    ///                 Field::new("element", nested, true),
    ///                 true,
    ///             )),
    ///         ])
    ///     }
    /// }
    ///
    /// #[derive(Serialize)]
    /// struct Nested {
    ///     map: BTreeMap<String, Vec<String>>
    /// }
    ///
    /// impl Nested {
    ///     /// Returns the [`Fields`] for [`Nested`]
    ///     fn fields() -> Fields {
    ///         let element = Field::new("element", DataType::Utf8, false);
    ///         Fields::from([
    ///             Arc::new(Field::new_map(
    ///                 "map",
    ///                 "entries",
    ///                 Field::new("key", DataType::Utf8, false),
    ///                 Field::new_list("value", element, false),
    ///                 false, // sorted
    ///                 false, // nullable
    ///             ))
    ///         ])
    ///     }
    /// }
    ///
    /// let data = vec![
    ///     MyStruct {
    ///         int32: 34,
    ///         list: vec![1., 2., 34.],
    ///         nested: vec![
    ///             None,
    ///             Some(Nested {
    ///                 map: vec![
    ///                     ("key1".to_string(), vec!["foo".to_string(), "bar".to_string()]),
    ///                     ("key2".to_string(), vec!["baz".to_string()])
    ///                 ].into_iter().collect()
    ///             })
    ///         ]
    ///     },
    ///     MyStruct {
    ///         int32: 56,
    ///         list: vec![],
    ///         nested: vec![]
    ///     },
    ///     MyStruct {
    ///         int32: 24,
    ///         list: vec![-1., 245.],
    ///         nested: vec![None]
    ///     }
    /// ];
    ///
    /// let schema = Schema::new(MyStruct::fields());
    /// let mut decoder = RawReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
    /// decoder.serialize(&data).unwrap();
    /// let batch = decoder.flush().unwrap().unwrap();
    /// assert_eq!(batch.num_rows(), 3);
    /// assert_eq!(batch.num_columns(), 3);
    ///
    /// // Convert to StructArray to format
    /// let s = StructArray::from(batch);
    /// let options = FormatOptions::default().with_null("null");
    /// let formatter = ArrayFormatter::try_new(&s, &options).unwrap();
    ///
    /// assert_eq!(&formatter.value(0).to_string(), "{int32: 34, list: [1.0, 2.0, 34.0], nested: [null, {map: {key1: [foo, bar], key2: [baz]}}]}");
    /// assert_eq!(&formatter.value(1).to_string(), "{int32: 56, list: [], nested: []}");
    /// assert_eq!(&formatter.value(2).to_string(), "{int32: 24, list: [-1.0, 245.0], nested: [null]}");
    /// ```
    ///
    /// Note: this ignores any batch size setting, and always decodes all rows
    pub fn serialize<S: Serialize>(&mut self, rows: &[S]) -> Result<(), ArrowError> {
        self.tape_decoder.serialize(rows)
    }

    /// Flushes the currently buffered data to a [`RecordBatch`]
    ///
    /// Returns `Ok(None)` if no buffered data
    ///
    /// Note: if called part way through decoding a record, this will return an error
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let tape = self.tape_decoder.finish()?;

        if tape.num_rows() == 0 {
            return Ok(None);
        }

        // First offset is null sentinel
        let mut next_object = 1;
        let pos: Vec<_> = (0..tape.num_rows())
            .map(|_| {
                let end = match tape.get(next_object) {
                    TapeElement::StartObject(end) => end,
                    _ => unreachable!("corrupt tape"),
                };
                std::mem::replace(&mut next_object, end + 1)
            })
            .collect();

        let decoded = self.decoder.decode(&tape, &pos)?;
        self.tape_decoder.clear();

        // Sanity check
        assert!(matches!(decoded.data_type(), DataType::Struct(_)));
        assert_eq!(decoded.null_count(), 0);
        assert_eq!(decoded.len(), pos.len());

        // Clear out buffer
        let columns = decoded
            .child_data()
            .iter()
            .map(|x| make_array(x.clone()))
            .collect();

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(Some(batch))
    }
}

trait ArrayDecoder: Send {
    /// Decode elements from `tape` starting at the indexes contained in `pos`
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError>;
}

macro_rules! primitive_decoder {
    ($t:ty, $data_type:expr) => {
        Ok(Box::new(PrimitiveArrayDecoder::<$t>::new($data_type)))
    };
}

fn make_decoder(
    data_type: DataType,
    coerce_primitive: bool,
    is_nullable: bool,
) -> Result<Box<dyn ArrayDecoder>, ArrowError> {
    downcast_integer! {
        data_type => (primitive_decoder, data_type),
        DataType::Float32 => primitive_decoder!(Float32Type, data_type),
        DataType::Float64 => primitive_decoder!(Float64Type, data_type),
        DataType::Timestamp(TimeUnit::Second, None) => {
            Ok(Box::new(TimestampArrayDecoder::<TimestampSecondType, _>::new(data_type, Utc)))
        },
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Ok(Box::new(TimestampArrayDecoder::<TimestampMillisecondType, _>::new(data_type, Utc)))
        },
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(Box::new(TimestampArrayDecoder::<TimestampMicrosecondType, _>::new(data_type, Utc)))
        },
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Ok(Box::new(TimestampArrayDecoder::<TimestampNanosecondType, _>::new(data_type, Utc)))
        },
        DataType::Timestamp(TimeUnit::Second, Some(ref tz)) => {
            let tz: Tz = tz.parse()?;
            Ok(Box::new(TimestampArrayDecoder::<TimestampSecondType, _>::new(data_type, tz)))
        },
        DataType::Timestamp(TimeUnit::Millisecond, Some(ref tz)) => {
            let tz: Tz = tz.parse()?;
            Ok(Box::new(TimestampArrayDecoder::<TimestampMillisecondType, _>::new(data_type, tz)))
        },
        DataType::Timestamp(TimeUnit::Microsecond, Some(ref tz)) => {
            let tz: Tz = tz.parse()?;
            Ok(Box::new(TimestampArrayDecoder::<TimestampMicrosecondType, _>::new(data_type, tz)))
        },
        DataType::Timestamp(TimeUnit::Nanosecond, Some(ref tz)) => {
            let tz: Tz = tz.parse()?;
            Ok(Box::new(TimestampArrayDecoder::<TimestampNanosecondType, _>::new(data_type, tz)))
        },
        DataType::Date32 => primitive_decoder!(Date32Type, data_type),
        DataType::Date64 => primitive_decoder!(Date64Type, data_type),
        DataType::Time32(TimeUnit::Second) => primitive_decoder!(Time32SecondType, data_type),
        DataType::Time32(TimeUnit::Millisecond) => primitive_decoder!(Time32MillisecondType, data_type),
        DataType::Time64(TimeUnit::Microsecond) => primitive_decoder!(Time64MicrosecondType, data_type),
        DataType::Time64(TimeUnit::Nanosecond) => primitive_decoder!(Time64NanosecondType, data_type),
        DataType::Decimal128(p, s) => Ok(Box::new(DecimalArrayDecoder::<Decimal128Type>::new(p, s))),
        DataType::Decimal256(p, s) => Ok(Box::new(DecimalArrayDecoder::<Decimal256Type>::new(p, s))),
        DataType::Boolean => Ok(Box::<BooleanArrayDecoder>::default()),
        DataType::Utf8 => Ok(Box::new(StringArrayDecoder::<i32>::new(coerce_primitive))),
        DataType::LargeUtf8 => Ok(Box::new(StringArrayDecoder::<i64>::new(coerce_primitive))),
        DataType::List(_) => Ok(Box::new(ListArrayDecoder::<i32>::new(data_type, coerce_primitive, is_nullable)?)),
        DataType::LargeList(_) => Ok(Box::new(ListArrayDecoder::<i64>::new(data_type, coerce_primitive, is_nullable)?)),
        DataType::Struct(_) => Ok(Box::new(StructArrayDecoder::new(data_type, coerce_primitive, is_nullable)?)),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Err(ArrowError::JsonError(format!("{data_type} is not supported by JSON")))
        }
        DataType::Map(_, _) => Ok(Box::new(MapArrayDecoder::new(data_type, coerce_primitive, is_nullable)?)),
        d => Err(ArrowError::NotYetImplemented(format!("Support for {d} in JSON reader")))
    }
}

fn tape_error(d: TapeElement, expected: &str) -> ArrowError {
    ArrowError::JsonError(format!("expected {expected} got {d}"))
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::reader::infer_json_schema;
    use crate::ReaderBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{Array, StructArray};
    use arrow_buffer::ArrowNativeType;
    use arrow_cast::display::{ArrayFormatter, FormatOptions};
    use arrow_schema::{DataType, Field, Schema};
    use std::fs::File;
    use std::io::{BufReader, Cursor, Seek};
    use std::sync::Arc;

    fn do_read(
        buf: &str,
        batch_size: usize,
        coerce_primitive: bool,
        schema: SchemaRef,
    ) -> Vec<RecordBatch> {
        let mut unbuffered = vec![];

        // Test with different batch sizes to test for boundary conditions
        for batch_size in [1, 3, 100, batch_size] {
            unbuffered = RawReaderBuilder::new(schema.clone())
                .with_batch_size(batch_size)
                .coerce_primitive(coerce_primitive)
                .build(Cursor::new(buf.as_bytes()))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            for b in unbuffered.iter().take(unbuffered.len() - 1) {
                assert_eq!(b.num_rows(), batch_size)
            }

            // Test with different buffer sizes to test for boundary conditions
            for b in [1, 3, 5] {
                let buffered = RawReaderBuilder::new(schema.clone())
                    .with_batch_size(batch_size)
                    .coerce_primitive(coerce_primitive)
                    .build(BufReader::with_capacity(b, Cursor::new(buf.as_bytes())))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(unbuffered, buffered);
            }
        }

        unbuffered
    }

    #[test]
    fn test_basic() {
        let buf = r#"
        {"a": 1, "b": 2, "c": true, "d": 1}
        {"a": 2E0, "b": 4, "c": false, "d": 2, "e": 254}

        {"b": 6, "a": 2.0, "d": 45}
        {"b": "5", "a": 2}
        {"b": 4e0}
        {"b": 7, "a": null}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Boolean, true),
            Field::new("d", DataType::Date32, true),
            Field::new("e", DataType::Date64, true),
        ]));

        let batches = do_read(buf, 1024, false, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_primitive::<Int64Type>();
        assert_eq!(col1.null_count(), 2);
        assert_eq!(col1.values(), &[1, 2, 2, 2, 0, 0]);
        assert!(col1.is_null(4));
        assert!(col1.is_null(5));

        let col2 = batches[0].column(1).as_primitive::<Int32Type>();
        assert_eq!(col2.null_count(), 0);
        assert_eq!(col2.values(), &[2, 4, 6, 5, 4, 7]);

        let col3 = batches[0].column(2).as_boolean();
        assert_eq!(col3.null_count(), 4);
        assert!(col3.value(0));
        assert!(!col3.is_null(0));
        assert!(!col3.value(1));
        assert!(!col3.is_null(1));

        let col4 = batches[0].column(3).as_primitive::<Date32Type>();
        assert_eq!(col4.null_count(), 3);
        assert!(col4.is_null(3));
        assert_eq!(col4.values(), &[1, 2, 45, 0, 0, 0]);

        let col5 = batches[0].column(4).as_primitive::<Date64Type>();
        assert_eq!(col5.null_count(), 5);
        assert!(col5.is_null(0));
        assert!(col5.is_null(2));
        assert!(col5.is_null(3));
        assert_eq!(col5.values(), &[0, 254, 0, 0, 0, 0]);
    }

    #[test]
    fn test_string() {
        let buf = r#"
        {"a": "1", "b": "2"}
        {"a": "hello", "b": "shoo"}
        {"b": "\t😁foo", "a": "\nfoobar\ud83d\ude00\u0061\u0073\u0066\u0067\u00FF"}
        
        {"b": null}
        {"b": "", "a": null}

        "#;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::LargeUtf8, true),
        ]));

        let batches = do_read(buf, 1024, false, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_string::<i32>();
        assert_eq!(col1.null_count(), 2);
        assert_eq!(col1.value(0), "1");
        assert_eq!(col1.value(1), "hello");
        assert_eq!(col1.value(2), "\nfoobar😀asfgÿ");
        assert!(col1.is_null(3));
        assert!(col1.is_null(4));

        let col2 = batches[0].column(1).as_string::<i64>();
        assert_eq!(col2.null_count(), 1);
        assert_eq!(col2.value(0), "2");
        assert_eq!(col2.value(1), "shoo");
        assert_eq!(col2.value(2), "\t😁foo");
        assert!(col2.is_null(3));
        assert_eq!(col2.value(4), "");
    }

    #[test]
    fn test_complex() {
        let buf = r#"
           {"list": [], "nested": {"a": 1, "b": 2}, "nested_list": {"list2": [{"c": 3}, {"c": 4}]}}
           {"list": [5, 6], "nested": {"a": 7}, "nested_list": {"list2": []}}
           {"list": null, "nested": {"a": null}}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new_list("list", Field::new("element", DataType::Int32, false), true),
            Field::new_struct(
                "nested",
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Int32, true),
                ],
                true,
            ),
            Field::new_struct(
                "nested_list",
                vec![Field::new_list(
                    "list2",
                    Field::new_struct(
                        "element",
                        vec![Field::new("c", DataType::Int32, false)],
                        false,
                    ),
                    true,
                )],
                true,
            ),
        ]));

        let batches = do_read(buf, 1024, false, schema);
        assert_eq!(batches.len(), 1);

        let list = batches[0].column(0).as_list::<i32>();
        assert_eq!(list.len(), 3);
        assert_eq!(list.value_offsets(), &[0, 0, 2, 2]);
        assert_eq!(list.null_count(), 1);
        assert!(list.is_null(2));
        let list_values = list.values().as_primitive::<Int32Type>();
        assert_eq!(list_values.values(), &[5, 6]);

        let nested = batches[0].column(1).as_struct();
        let a = nested.column(0).as_primitive::<Int32Type>();
        assert_eq!(list.null_count(), 1);
        assert_eq!(a.values(), &[1, 7, 0]);
        assert!(list.is_null(2));

        let b = nested.column(1).as_primitive::<Int32Type>();
        assert_eq!(b.null_count(), 2);
        assert_eq!(b.len(), 3);
        assert_eq!(b.value(0), 2);
        assert!(b.is_null(1));
        assert!(b.is_null(2));

        let nested_list = batches[0].column(2).as_struct();
        assert_eq!(nested_list.len(), 3);
        assert_eq!(nested_list.null_count(), 1);
        assert!(nested_list.is_null(2));

        let list2 = nested_list.column(0).as_list::<i32>();
        assert_eq!(list2.len(), 3);
        assert_eq!(list2.null_count(), 1);
        assert_eq!(list2.value_offsets(), &[0, 2, 2, 2]);
        assert!(list2.is_null(2));

        let list2_values = list2.values().as_struct();

        let c = list2_values.column(0).as_primitive::<Int32Type>();
        assert_eq!(c.values(), &[3, 4]);
    }

    #[test]
    fn test_projection() {
        let buf = r#"
           {"list": [], "nested": {"a": 1, "b": 2}, "nested_list": {"list2": [{"c": 3, "d": 5}, {"c": 4}]}}
           {"list": [5, 6], "nested": {"a": 7}, "nested_list": {"list2": []}}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new_struct(
                "nested",
                vec![Field::new("a", DataType::Int32, false)],
                true,
            ),
            Field::new_struct(
                "nested_list",
                vec![Field::new_list(
                    "list2",
                    Field::new_struct(
                        "element",
                        vec![Field::new("d", DataType::Int32, true)],
                        false,
                    ),
                    true,
                )],
                true,
            ),
        ]));

        let batches = do_read(buf, 1024, false, schema);
        assert_eq!(batches.len(), 1);

        let nested = batches[0].column(0).as_struct();
        assert_eq!(nested.num_columns(), 1);
        let a = nested.column(0).as_primitive::<Int32Type>();
        assert_eq!(a.null_count(), 0);
        assert_eq!(a.values(), &[1, 7]);

        let nested_list = batches[0].column(1).as_struct();
        assert_eq!(nested_list.num_columns(), 1);
        assert_eq!(nested_list.null_count(), 0);

        let list2 = nested_list.column(0).as_list::<i32>();
        assert_eq!(list2.value_offsets(), &[0, 2, 2]);
        assert_eq!(list2.null_count(), 0);

        let child = list2.values().as_struct();
        assert_eq!(child.num_columns(), 1);
        assert_eq!(child.len(), 2);
        assert_eq!(child.null_count(), 0);

        let c = child.column(0).as_primitive::<Int32Type>();
        assert_eq!(c.values(), &[5, 0]);
        assert_eq!(c.null_count(), 1);
        assert!(c.is_null(1));
    }

    #[test]
    fn test_map() {
        let buf = r#"
           {"map": {"a": ["foo", null]}}
           {"map": {"a": [null], "b": []}}
           {"map": {"c": null, "a": ["baz"]}}
        "#;
        let map = Field::new_map(
            "map",
            "entries",
            Field::new("key", DataType::Utf8, false),
            Field::new_list("value", Field::new("element", DataType::Utf8, true), true),
            false,
            true,
        );

        let schema = Arc::new(Schema::new(vec![map]));

        let batches = do_read(buf, 1024, false, schema);
        assert_eq!(batches.len(), 1);

        let map = batches[0].column(0).as_map();
        let map_keys = map.keys().as_string::<i32>();
        let map_values = map.values().as_list::<i32>();
        assert_eq!(map.value_offsets(), &[0, 1, 3, 5]);

        let k: Vec<_> = map_keys.iter().map(|x| x.unwrap()).collect();
        assert_eq!(&k, &["a", "a", "b", "c", "a"]);

        let list_values = map_values.values().as_string::<i32>();
        let lv: Vec<_> = list_values.iter().collect();
        assert_eq!(&lv, &[Some("foo"), None, None, Some("baz")]);
        assert_eq!(map_values.value_offsets(), &[0, 2, 3, 3, 3, 4]);
        assert_eq!(map_values.null_count(), 1);
        assert!(map_values.is_null(3));

        let options = FormatOptions::default().with_null("null");
        let formatter = ArrayFormatter::try_new(map, &options).unwrap();
        assert_eq!(formatter.value(0).to_string(), "{a: [foo, null]}");
        assert_eq!(formatter.value(1).to_string(), "{a: [null], b: []}");
        assert_eq!(formatter.value(2).to_string(), "{c: null, a: [baz]}");
    }

    #[test]
    fn integration_test() {
        let files = [
            "test/data/basic.json",
            "test/data/basic_nulls.json",
            "test/data/list_string_dict_nested_nulls.json",
        ];

        for file in files {
            let mut f = BufReader::new(File::open(file).unwrap());
            let schema = Arc::new(infer_json_schema(&mut f, None).unwrap());

            f.rewind().unwrap();
            let a = ReaderBuilder::new()
                .with_schema(schema.clone())
                .build(&mut f)
                .unwrap();
            let a_result = a.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

            f.rewind().unwrap();
            let b = RawReaderBuilder::new(schema).build(f).unwrap();
            let b_result = b.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

            assert_eq!(a_result, b_result);
        }
    }

    #[test]
    fn test_not_coercing_primitive_into_string_without_flag() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        let buf = r#"{"a": 1}"#;
        let result = RawReaderBuilder::new(schema.clone())
            .with_batch_size(1024)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .read();

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Json error: expected string got number".to_string()
        );

        let buf = r#"{"a": true}"#;
        let result = RawReaderBuilder::new(schema)
            .with_batch_size(1024)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .read();

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Json error: expected string got true".to_string()
        );
    }

    #[test]
    fn test_coercing_primitive_into_string() {
        let buf = r#"
        {"a": 1, "b": 2, "c": true}
        {"a": 2E0, "b": 4, "c": false}

        {"b": 6, "a": 2.0}
        {"b": "5", "a": 2}
        {"b": 4e0}
        {"b": 7, "a": null}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]));

        let batches = do_read(buf, 1024, true, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_string::<i32>();
        assert_eq!(col1.null_count(), 2);
        assert_eq!(col1.value(0), "1");
        assert_eq!(col1.value(1), "2E0");
        assert_eq!(col1.value(2), "2.0");
        assert_eq!(col1.value(3), "2");
        assert!(col1.is_null(4));
        assert!(col1.is_null(5));

        let col2 = batches[0].column(1).as_string::<i32>();
        assert_eq!(col2.null_count(), 0);
        assert_eq!(col2.value(0), "2");
        assert_eq!(col2.value(1), "4");
        assert_eq!(col2.value(2), "6");
        assert_eq!(col2.value(3), "5");
        assert_eq!(col2.value(4), "4e0");
        assert_eq!(col2.value(5), "7");

        let col3 = batches[0].column(2).as_string::<i32>();
        assert_eq!(col3.null_count(), 4);
        assert_eq!(col3.value(0), "true");
        assert_eq!(col3.value(1), "false");
        assert!(col3.is_null(2));
        assert!(col3.is_null(3));
        assert!(col3.is_null(4));
        assert!(col3.is_null(5));
    }

    fn test_decimal<T: DecimalType>(data_type: DataType) {
        let buf = r#"
        {"a": 1, "b": 2, "c": 38.30}
        {"a": 2, "b": 4, "c": 123.456}

        {"b": 1337, "a": "2.0452"}
        {"b": "5", "a": "11034.2"}
        {"b": 40}
        {"b": 1234, "a": null}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", data_type.clone(), true),
            Field::new("b", data_type.clone(), true),
            Field::new("c", data_type, true),
        ]));

        let batches = do_read(buf, 1024, true, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_primitive::<T>();
        assert_eq!(col1.null_count(), 2);
        assert!(col1.is_null(4));
        assert!(col1.is_null(5));
        assert_eq!(
            col1.values(),
            &[100, 200, 204, 1103420, 0, 0].map(T::Native::usize_as)
        );

        let col2 = batches[0].column(1).as_primitive::<T>();
        assert_eq!(col2.null_count(), 0);
        assert_eq!(
            col2.values(),
            &[200, 400, 133700, 500, 4000, 123400].map(T::Native::usize_as)
        );

        let col3 = batches[0].column(2).as_primitive::<T>();
        assert_eq!(col3.null_count(), 4);
        assert!(!col3.is_null(0));
        assert!(!col3.is_null(1));
        assert!(col3.is_null(2));
        assert!(col3.is_null(3));
        assert!(col3.is_null(4));
        assert!(col3.is_null(5));
        assert_eq!(
            col3.values(),
            &[3830, 12345, 0, 0, 0, 0].map(T::Native::usize_as)
        );
    }

    #[test]
    fn test_decimals() {
        test_decimal::<Decimal128Type>(DataType::Decimal128(10, 2));
        test_decimal::<Decimal256Type>(DataType::Decimal256(10, 2));
    }

    fn test_timestamp<T: ArrowTimestampType>() {
        let buf = r#"
        {"a": 1, "b": "2020-09-08T13:42:29.190855+00:00", "c": 38.30, "d": "1997-01-31T09:26:56.123"}
        {"a": 2, "b": "2020-09-08T13:42:29.190855Z", "c": 123.456, "d": 123.456}

        {"b": 1337, "b": "2020-09-08T13:42:29Z", "c": "1997-01-31T09:26:56.123", "d": "1997-01-31T09:26:56.123Z"}
        {"b": 40, "c": "2020-09-08T13:42:29.190855+00:00", "d": "1997-01-31 09:26:56.123-05:00"}
        {"b": 1234, "a": null, "c": "1997-01-31 09:26:56.123Z", "d": "1997-01-31 092656"}
        {"c": "1997-01-31T14:26:56.123-05:00", "d": "1997-01-31"}
        "#;

        let with_timezone = DataType::Timestamp(T::UNIT, Some("+08:00".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", T::DATA_TYPE, true),
            Field::new("b", T::DATA_TYPE, true),
            Field::new("c", T::DATA_TYPE, true),
            Field::new("d", with_timezone, true),
        ]));

        let batches = do_read(buf, 1024, true, schema);
        assert_eq!(batches.len(), 1);

        let unit_in_nanos: i64 = match T::UNIT {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        };

        let col1 = batches[0].column(0).as_primitive::<T>();
        assert_eq!(col1.null_count(), 4);
        assert!(col1.is_null(2));
        assert!(col1.is_null(3));
        assert!(col1.is_null(4));
        assert!(col1.is_null(5));
        assert_eq!(col1.values(), &[1, 2, 0, 0, 0, 0].map(T::Native::usize_as));

        let col2 = batches[0].column(1).as_primitive::<T>();
        assert_eq!(col2.null_count(), 1);
        assert!(col2.is_null(5));
        assert_eq!(
            col2.values(),
            &[
                1599572549190855000 / unit_in_nanos,
                1599572549190855000 / unit_in_nanos,
                1599572549000000000 / unit_in_nanos,
                40,
                1234,
                0
            ]
        );

        let col3 = batches[0].column(2).as_primitive::<T>();
        assert_eq!(col3.null_count(), 0);
        assert_eq!(
            col3.values(),
            &[
                38,
                123,
                854702816123000000 / unit_in_nanos,
                1599572549190855000 / unit_in_nanos,
                854702816123000000 / unit_in_nanos,
                854738816123000000 / unit_in_nanos
            ]
        );

        let col4 = batches[0].column(3).as_primitive::<T>();

        assert_eq!(col4.null_count(), 0);
        assert_eq!(
            col4.values(),
            &[
                854674016123000000 / unit_in_nanos,
                123,
                854702816123000000 / unit_in_nanos,
                854720816123000000 / unit_in_nanos,
                854674016000000000 / unit_in_nanos,
                854640000000000000 / unit_in_nanos
            ]
        );
    }

    #[test]
    fn test_timestamps() {
        test_timestamp::<TimestampSecondType>();
        test_timestamp::<TimestampMillisecondType>();
        test_timestamp::<TimestampMicrosecondType>();
        test_timestamp::<TimestampNanosecondType>();
    }

    fn test_time<T: ArrowTemporalType>() {
        let buf = r#"
        {"a": 1, "b": "09:26:56.123 AM", "c": 38.30}
        {"a": 2, "b": "23:59:59", "c": 123.456}

        {"b": 1337, "b": "6:00 pm", "c": "09:26:56.123"}
        {"b": 40, "c": "13:42:29.190855"}
        {"b": 1234, "a": null, "c": "09:26:56.123"}
        {"c": "14:26:56.123"}
        "#;

        let unit = match T::DATA_TYPE {
            DataType::Time32(unit) | DataType::Time64(unit) => unit,
            _ => unreachable!(),
        };

        let unit_in_nanos = match unit {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", T::DATA_TYPE, true),
            Field::new("b", T::DATA_TYPE, true),
            Field::new("c", T::DATA_TYPE, true),
        ]));

        let batches = do_read(buf, 1024, true, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_primitive::<T>();
        assert_eq!(col1.null_count(), 4);
        assert!(col1.is_null(2));
        assert!(col1.is_null(3));
        assert!(col1.is_null(4));
        assert!(col1.is_null(5));
        assert_eq!(col1.values(), &[1, 2, 0, 0, 0, 0].map(T::Native::usize_as));

        let col2 = batches[0].column(1).as_primitive::<T>();
        assert_eq!(col2.null_count(), 1);
        assert!(col2.is_null(5));
        assert_eq!(
            col2.values(),
            &[
                34016123000000 / unit_in_nanos,
                86399000000000 / unit_in_nanos,
                64800000000000 / unit_in_nanos,
                40,
                1234,
                0
            ]
            .map(T::Native::usize_as)
        );

        let col3 = batches[0].column(2).as_primitive::<T>();
        assert_eq!(col3.null_count(), 0);
        assert_eq!(
            col3.values(),
            &[
                38,
                123,
                34016123000000 / unit_in_nanos,
                49349190855000 / unit_in_nanos,
                34016123000000 / unit_in_nanos,
                52016123000000 / unit_in_nanos
            ]
            .map(T::Native::usize_as)
        );
    }

    #[test]
    fn test_times() {
        test_time::<Time32MillisecondType>();
        test_time::<Time32SecondType>();
        test_time::<Time64MicrosecondType>();
        test_time::<Time64NanosecondType>();
    }

    #[test]
    fn test_delta_checkpoint() {
        let json = "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}";
        let schema = Arc::new(Schema::new(vec![
            Field::new_struct(
                "protocol",
                vec![
                    Field::new("minReaderVersion", DataType::Int32, true),
                    Field::new("minWriterVersion", DataType::Int32, true),
                ],
                true,
            ),
            Field::new_struct(
                "add",
                vec![Field::new_map(
                    "partitionValues",
                    "key_value",
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                    false,
                    false,
                )],
                true,
            ),
        ]));

        let batches = do_read(json, 1024, true, schema);
        assert_eq!(batches.len(), 1);

        let s: StructArray = batches.into_iter().next().unwrap().into();
        let opts = FormatOptions::default().with_null("null");
        let formatter = ArrayFormatter::try_new(&s, &opts).unwrap();
        assert_eq!(
            formatter.value(0).to_string(),
            "{protocol: {minReaderVersion: 1, minWriterVersion: 2}, add: null}"
        );
    }

    #[test]
    fn struct_nullability() {
        let do_test = |child: DataType| {
            // Test correctly enforced nullability
            let non_null = r#"{"foo": {}}"#;
            let schema = Arc::new(Schema::new(vec![Field::new_struct(
                "foo",
                vec![Field::new("bar", child, false)],
                true,
            )]));
            let mut reader = RawReaderBuilder::new(schema.clone())
                .build(Cursor::new(non_null.as_bytes()))
                .unwrap();
            assert!(reader.next().unwrap().is_err()); // Should error as not nullable

            let null = r#"{"foo": {bar: null}}"#;
            let mut reader = RawReaderBuilder::new(schema.clone())
                .build(Cursor::new(null.as_bytes()))
                .unwrap();
            assert!(reader.next().unwrap().is_err()); // Should error as not nullable

            // Test nulls in nullable parent can mask nulls in non-nullable child
            let null = r#"{"foo": null}"#;
            let mut reader = RawReaderBuilder::new(schema)
                .build(Cursor::new(null.as_bytes()))
                .unwrap();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.num_columns(), 1);
            let foo = batch.column(0).as_struct();
            assert_eq!(foo.len(), 1);
            assert!(foo.is_null(0));
            assert_eq!(foo.num_columns(), 1);

            let bar = foo.column(0);
            assert_eq!(bar.len(), 1);
            // Non-nullable child can still contain null as masked by parent
            assert!(bar.is_null(0));
        };

        do_test(DataType::Boolean);
        do_test(DataType::Int32);
        do_test(DataType::Utf8);
        do_test(DataType::Decimal128(2, 1));
        do_test(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("+00:00".into()),
        ));
    }
}
