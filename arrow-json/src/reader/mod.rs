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

//! JSON reader
//!
//! This JSON reader allows JSON records to be read into the Arrow memory
//! model. Records are loaded in batches and are then converted from the record-oriented
//! representation to the columnar arrow data model.
//!
//! The reader ignores whitespace between JSON values, including `\n` and `\r`, allowing
//! parsing of sequences of one or more arbitrarily formatted JSON values, including
//! but not limited to newline-delimited JSON.
//!
//! # Basic Usage
//!
//! [`Reader`] can be used directly with synchronous data sources, such as [`std::fs::File`]
//!
//! ```
//! # use arrow_schema::*;
//! # use std::fs::File;
//! # use std::io::BufReader;
//! # use std::sync::Arc;
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("a", DataType::Float64, false),
//!     Field::new("b", DataType::Float64, false),
//!     Field::new("c", DataType::Boolean, true),
//! ]));
//!
//! let file = File::open("test/data/basic.json").unwrap();
//!
//! let mut json = arrow_json::ReaderBuilder::new(schema).build(BufReader::new(file)).unwrap();
//! let batch = json.next().unwrap().unwrap();
//! ```
//!
//! # Async Usage
//!
//! The lower-level [`Decoder`] can be integrated with various forms of async data streams,
//! and is designed to be agnostic to the various different kinds of async IO primitives found
//! within the Rust ecosystem.
//!
//! For example, see below for how it can be used with an arbitrary `Stream` of `Bytes`
//!
//! ```
//! # use std::task::{Poll, ready};
//! # use bytes::{Buf, Bytes};
//! # use arrow_schema::ArrowError;
//! # use futures::stream::{Stream, StreamExt};
//! # use arrow_array::RecordBatch;
//! # use arrow_json::reader::Decoder;
//! #
//! fn decode_stream<S: Stream<Item = Bytes> + Unpin>(
//!     mut decoder: Decoder,
//!     mut input: S,
//! ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
//!     let mut buffered = Bytes::new();
//!     futures::stream::poll_fn(move |cx| {
//!         loop {
//!             if buffered.is_empty() {
//!                 buffered = match ready!(input.poll_next_unpin(cx)) {
//!                     Some(b) => b,
//!                     None => break,
//!                 };
//!             }
//!             let decoded = match decoder.decode(buffered.as_ref()) {
//!                 Ok(decoded) => decoded,
//!                 Err(e) => return Poll::Ready(Some(Err(e))),
//!             };
//!             let read = buffered.len();
//!             buffered.advance(decoded);
//!             if decoded != read {
//!                 break
//!             }
//!         }
//!
//!         Poll::Ready(decoder.flush().transpose())
//!     })
//! }
//!
//! ```
//!
//! In a similar vein, it can also be used with tokio-based IO primitives
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow_schema::{DataType, Field, Schema};
//! # use std::pin::Pin;
//! # use std::task::{Poll, ready};
//! # use futures::{Stream, TryStreamExt};
//! # use tokio::io::AsyncBufRead;
//! # use arrow_array::RecordBatch;
//! # use arrow_json::reader::Decoder;
//! # use arrow_schema::ArrowError;
//! fn decode_stream<R: AsyncBufRead + Unpin>(
//!     mut decoder: Decoder,
//!     mut reader: R,
//! ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
//!     futures::stream::poll_fn(move |cx| {
//!         loop {
//!             let b = match ready!(Pin::new(&mut reader).poll_fill_buf(cx)) {
//!                 Ok(b) if b.is_empty() => break,
//!                 Ok(b) => b,
//!                 Err(e) => return Poll::Ready(Some(Err(e.into()))),
//!             };
//!             let read = b.len();
//!             let decoded = match decoder.decode(b) {
//!                 Ok(decoded) => decoded,
//!                 Err(e) => return Poll::Ready(Some(Err(e))),
//!             };
//!             Pin::new(&mut reader).consume(decoded);
//!             if decoded != read {
//!                 break;
//!             }
//!         }
//!
//!         Poll::Ready(decoder.flush().transpose())
//!     })
//! }
//! ```
//!

use crate::StructMode;
use std::io::BufRead;
use std::sync::Arc;

use chrono::Utc;
use serde::Serialize;

use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::{downcast_integer, make_array, RecordBatch, RecordBatchReader, StructArray};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, FieldRef, Schema, SchemaRef, TimeUnit};
pub use schema::*;

use crate::reader::boolean_array::BooleanArrayDecoder;
use crate::reader::decimal_array::DecimalArrayDecoder;
use crate::reader::list_array::ListArrayDecoder;
use crate::reader::map_array::MapArrayDecoder;
use crate::reader::null_array::NullArrayDecoder;
use crate::reader::primitive_array::PrimitiveArrayDecoder;
use crate::reader::string_array::StringArrayDecoder;
use crate::reader::struct_array::StructArrayDecoder;
use crate::reader::tape::{Tape, TapeDecoder};
use crate::reader::timestamp_array::TimestampArrayDecoder;

mod boolean_array;
mod decimal_array;
mod list_array;
mod map_array;
mod null_array;
mod primitive_array;
mod schema;
mod serializer;
mod string_array;
mod struct_array;
mod tape;
mod timestamp_array;

/// A builder for [`Reader`] and [`Decoder`]
pub struct ReaderBuilder {
    batch_size: usize,
    coerce_primitive: bool,
    strict_mode: bool,
    is_field: bool,
    struct_mode: StructMode,

    schema: SchemaRef,
}

impl ReaderBuilder {
    /// Create a new [`ReaderBuilder`] with the provided [`SchemaRef`]
    ///
    /// This could be obtained using [`infer_json_schema`] if not known
    ///
    /// Any columns not present in `schema` will be ignored, unless `strict_mode` is set to true.
    /// In this case, an error is returned when a column is missing from `schema`.
    ///
    /// [`infer_json_schema`]: crate::reader::infer_json_schema
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batch_size: 1024,
            coerce_primitive: false,
            strict_mode: false,
            is_field: false,
            struct_mode: Default::default(),
            schema,
        }
    }

    /// Create a new [`ReaderBuilder`] that will parse JSON values of `field.data_type()`
    ///
    /// Unlike [`ReaderBuilder::new`] this does not require the root of the JSON data
    /// to be an object, i.e. `{..}`, allowing for parsing of any valid JSON value(s)
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::cast::AsArray;
    /// # use arrow_array::types::Int32Type;
    /// # use arrow_json::ReaderBuilder;
    /// # use arrow_schema::{DataType, Field};
    /// // Root of JSON schema is a numeric type
    /// let data = "1\n2\n3\n";
    /// let field = Arc::new(Field::new("int", DataType::Int32, true));
    /// let mut reader = ReaderBuilder::new_with_field(field.clone()).build(data.as_bytes()).unwrap();
    /// let b = reader.next().unwrap().unwrap();
    /// let values = b.column(0).as_primitive::<Int32Type>().values();
    /// assert_eq!(values, &[1, 2, 3]);
    ///
    /// // Root of JSON schema is a list type
    /// let data = "[1, 2, 3, 4, 5, 6, 7]\n[1, 2, 3]";
    /// let field = Field::new_list("int", field.clone(), true);
    /// let mut reader = ReaderBuilder::new_with_field(field).build(data.as_bytes()).unwrap();
    /// let b = reader.next().unwrap().unwrap();
    /// let list = b.column(0).as_list::<i32>();
    ///
    /// assert_eq!(list.offsets().as_ref(), &[0, 7, 10]);
    /// let list_values = list.values().as_primitive::<Int32Type>();
    /// assert_eq!(list_values.values(), &[1, 2, 3, 4, 5, 6, 7, 1, 2, 3]);
    /// ```
    pub fn new_with_field(field: impl Into<FieldRef>) -> Self {
        Self {
            batch_size: 1024,
            coerce_primitive: false,
            strict_mode: false,
            is_field: true,
            struct_mode: Default::default(),
            schema: Arc::new(Schema::new([field.into()])),
        }
    }

    /// Sets the batch size in rows to read
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Sets if the decoder should coerce primitive values (bool and number) into string
    /// when the Schema's column is Utf8 or LargeUtf8.
    pub fn with_coerce_primitive(self, coerce_primitive: bool) -> Self {
        Self {
            coerce_primitive,
            ..self
        }
    }

    /// Sets if the decoder should return an error if it encounters a column not
    /// present in `schema`. If `struct_mode` is `ListOnly` the value of
    /// `strict_mode` is effectively `true`. It is required for all fields of
    /// the struct to be in the list: without field names, there is no way to
    /// determine which field is missing.
    pub fn with_strict_mode(self, strict_mode: bool) -> Self {
        Self {
            strict_mode,
            ..self
        }
    }

    /// Set the [`StructMode`] for the reader, which determines whether structs
    /// can be decoded from JSON as objects or lists. For more details refer to
    /// the enum documentation. Default is to use `ObjectOnly`.
    pub fn with_struct_mode(self, struct_mode: StructMode) -> Self {
        Self {
            struct_mode,
            ..self
        }
    }

    /// Create a [`Reader`] with the provided [`BufRead`]
    pub fn build<R: BufRead>(self, reader: R) -> Result<Reader<R>, ArrowError> {
        Ok(Reader {
            reader,
            decoder: self.build_decoder()?,
        })
    }

    /// Create a [`Decoder`]
    pub fn build_decoder(self) -> Result<Decoder, ArrowError> {
        let (data_type, nullable) = match self.is_field {
            false => (DataType::Struct(self.schema.fields.clone()), false),
            true => {
                let field = &self.schema.fields[0];
                (field.data_type().clone(), field.is_nullable())
            }
        };

        let decoder = make_decoder(
            data_type,
            self.coerce_primitive,
            self.strict_mode,
            nullable,
            self.struct_mode,
        )?;

        let num_fields = self.schema.flattened_fields().len();

        Ok(Decoder {
            decoder,
            is_field: self.is_field,
            tape_decoder: TapeDecoder::new(self.batch_size, num_fields),
            batch_size: self.batch_size,
            schema: self.schema,
        })
    }
}

/// Reads JSON data with a known schema directly into arrow [`RecordBatch`]
///
/// Lines consisting solely of ASCII whitespace are ignored
pub struct Reader<R> {
    reader: R,
    decoder: Decoder,
}

impl<R> std::fmt::Debug for Reader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("decoder", &self.decoder)
            .finish()
    }
}

impl<R: BufRead> Reader<R> {
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

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for Reader<R> {
    fn schema(&self) -> SchemaRef {
        self.decoder.schema.clone()
    }
}

/// A low-level interface for reading JSON data from a byte stream
///
/// See [`Reader`] for a higher-level interface for interface with [`BufRead`]
///
/// The push-based interface facilitates integration with sources that yield arbitrarily
/// delimited bytes ranges, such as [`BufRead`], or a chunked byte stream received from
/// object storage
///
/// ```
/// # use std::io::BufRead;
/// # use arrow_array::RecordBatch;
/// # use arrow_json::reader::{Decoder, ReaderBuilder};
/// # use arrow_schema::{ArrowError, SchemaRef};
/// #
/// fn read_from_json<R: BufRead>(
///     mut reader: R,
///     schema: SchemaRef,
/// ) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, ArrowError> {
///     let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
///     let mut next = move || {
///         loop {
///             // Decoder is agnostic that buf doesn't contain whole records
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
pub struct Decoder {
    tape_decoder: TapeDecoder,
    decoder: Box<dyn ArrayDecoder>,
    batch_size: usize,
    is_field: bool,
    schema: SchemaRef,
}

impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder")
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl Decoder {
    /// Read JSON objects from `buf`, returning the number of bytes read
    ///
    /// This method returns once `batch_size` objects have been parsed since the
    /// last call to [`Self::flush`], or `buf` is exhausted. Any remaining bytes
    /// should be included in the next call to [`Self::decode`]
    ///
    /// There is no requirement that `buf` contains a whole number of records, facilitating
    /// integration with arbitrary byte streams, such as those yielded by [`BufRead`]
    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.tape_decoder.decode(buf)
    }

    /// Serialize `rows` to this [`Decoder`]
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
    /// # use arrow_json::ReaderBuilder;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// let json = vec![json!({"float": 2.3}), json!({"float": 5.7})];
    ///
    /// let schema = Schema::new(vec![Field::new("float", DataType::Float32, true)]);
    /// let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
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
    /// # use arrow_json::ReaderBuilder;
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
    /// let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
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
    /// # use arrow_json::ReaderBuilder;
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
    /// let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
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

    /// True if the decoder is currently part way through decoding a record.
    pub fn has_partial_record(&self) -> bool {
        self.tape_decoder.has_partial_row()
    }

    /// The number of unflushed records, including the partially decoded record (if any).
    pub fn len(&self) -> usize {
        self.tape_decoder.num_buffered_rows()
    }

    /// True if there are no records to flush, i.e. [`Self::len`] is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Flushes the currently buffered data to a [`RecordBatch`]
    ///
    /// Returns `Ok(None)` if no buffered data, i.e. [`Self::is_empty`] is true.
    ///
    /// Note: This will return an error if called part way through decoding a record,
    /// i.e. [`Self::has_partial_record`] is true.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let tape = self.tape_decoder.finish()?;

        if tape.num_rows() == 0 {
            return Ok(None);
        }

        // First offset is null sentinel
        let mut next_object = 1;
        let pos: Vec<_> = (0..tape.num_rows())
            .map(|_| {
                let next = tape.next(next_object, "row").unwrap();
                std::mem::replace(&mut next_object, next)
            })
            .collect();

        let decoded = self.decoder.decode(&tape, &pos)?;
        self.tape_decoder.clear();

        let batch = match self.is_field {
            true => RecordBatch::try_new(self.schema.clone(), vec![make_array(decoded)])?,
            false => {
                RecordBatch::from(StructArray::from(decoded)).with_schema(self.schema.clone())?
            }
        };

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
    strict_mode: bool,
    is_nullable: bool,
    struct_mode: StructMode,
) -> Result<Box<dyn ArrayDecoder>, ArrowError> {
    downcast_integer! {
        data_type => (primitive_decoder, data_type),
        DataType::Null => Ok(Box::<NullArrayDecoder>::default()),
        DataType::Float16 => primitive_decoder!(Float16Type, data_type),
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
        DataType::Duration(TimeUnit::Nanosecond) => primitive_decoder!(DurationNanosecondType, data_type),
        DataType::Duration(TimeUnit::Microsecond) => primitive_decoder!(DurationMicrosecondType, data_type),
        DataType::Duration(TimeUnit::Millisecond) => primitive_decoder!(DurationMillisecondType, data_type),
        DataType::Duration(TimeUnit::Second) => primitive_decoder!(DurationSecondType, data_type),
        DataType::Decimal128(p, s) => Ok(Box::new(DecimalArrayDecoder::<Decimal128Type>::new(p, s))),
        DataType::Decimal256(p, s) => Ok(Box::new(DecimalArrayDecoder::<Decimal256Type>::new(p, s))),
        DataType::Boolean => Ok(Box::<BooleanArrayDecoder>::default()),
        DataType::Utf8 => Ok(Box::new(StringArrayDecoder::<i32>::new(coerce_primitive))),
        DataType::LargeUtf8 => Ok(Box::new(StringArrayDecoder::<i64>::new(coerce_primitive))),
        DataType::List(_) => Ok(Box::new(ListArrayDecoder::<i32>::new(data_type, coerce_primitive, strict_mode, is_nullable, struct_mode)?)),
        DataType::LargeList(_) => Ok(Box::new(ListArrayDecoder::<i64>::new(data_type, coerce_primitive, strict_mode, is_nullable, struct_mode)?)),
        DataType::Struct(_) => Ok(Box::new(StructArrayDecoder::new(data_type, coerce_primitive, strict_mode, is_nullable, struct_mode)?)),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Err(ArrowError::JsonError(format!("{data_type} is not supported by JSON")))
        }
        DataType::Map(_, _) => Ok(Box::new(MapArrayDecoder::new(data_type, coerce_primitive, strict_mode, is_nullable, struct_mode)?)),
        d => Err(ArrowError::NotYetImplemented(format!("Support for {d} in JSON reader")))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::fs::File;
    use std::io::{BufReader, Cursor, Seek};

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, BooleanArray, Float64Array, ListArray, StringArray};
    use arrow_buffer::{ArrowNativeType, Buffer};
    use arrow_cast::display::{ArrayFormatter, FormatOptions};
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{Field, Fields};

    use super::*;

    fn do_read(
        buf: &str,
        batch_size: usize,
        coerce_primitive: bool,
        strict_mode: bool,
        schema: SchemaRef,
    ) -> Vec<RecordBatch> {
        let mut unbuffered = vec![];

        // Test with different batch sizes to test for boundary conditions
        for batch_size in [1, 3, 100, batch_size] {
            unbuffered = ReaderBuilder::new(schema.clone())
                .with_batch_size(batch_size)
                .with_coerce_primitive(coerce_primitive)
                .build(Cursor::new(buf.as_bytes()))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            for b in unbuffered.iter().take(unbuffered.len() - 1) {
                assert_eq!(b.num_rows(), batch_size)
            }

            // Test with different buffer sizes to test for boundary conditions
            for b in [1, 3, 5] {
                let buffered = ReaderBuilder::new(schema.clone())
                    .with_batch_size(batch_size)
                    .with_coerce_primitive(coerce_primitive)
                    .with_strict_mode(strict_mode)
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

        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder().unwrap();
        assert!(decoder.is_empty());
        assert_eq!(decoder.len(), 0);
        assert!(!decoder.has_partial_record());
        assert_eq!(decoder.decode(buf.as_bytes()).unwrap(), 221);
        assert!(!decoder.is_empty());
        assert_eq!(decoder.len(), 6);
        assert!(!decoder.has_partial_record());
        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 6);
        assert!(decoder.is_empty());
        assert_eq!(decoder.len(), 0);
        assert!(!decoder.has_partial_record());

        let batches = do_read(buf, 1024, false, false, schema);
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

        let batches = do_read(buf, 1024, false, false, schema);
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

        let batches = do_read(buf, 1024, false, false, schema);
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

        let batches = do_read(buf, 1024, false, false, schema);
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

        let batches = do_read(buf, 1024, false, false, schema);
        assert_eq!(batches.len(), 1);

        let map = batches[0].column(0).as_map();
        let map_keys = map.keys().as_string::<i32>();
        let map_values = map.values().as_list::<i32>();
        assert_eq!(map.value_offsets(), &[0, 1, 3, 5]);

        let k: Vec<_> = map_keys.iter().flatten().collect();
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
    fn test_not_coercing_primitive_into_string_without_flag() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        let buf = r#"{"a": 1}"#;
        let err = ReaderBuilder::new(schema.clone())
            .with_batch_size(1024)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .read()
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Json error: whilst decoding field 'a': expected string got 1"
        );

        let buf = r#"{"a": true}"#;
        let err = ReaderBuilder::new(schema)
            .with_batch_size(1024)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .read()
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Json error: whilst decoding field 'a': expected string got true"
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

        let batches = do_read(buf, 1024, true, false, schema);
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

        let batches = do_read(buf, 1024, true, false, schema);
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

        let batches = do_read(buf, 1024, true, false, schema);
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

        let batches = do_read(buf, 1024, true, false, schema);
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

    fn test_duration<T: ArrowTemporalType>() {
        let buf = r#"
        {"a": 1, "b": "2"}
        {"a": 3, "b": null}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", T::DATA_TYPE, true),
            Field::new("b", T::DATA_TYPE, true),
        ]));

        let batches = do_read(buf, 1024, true, false, schema);
        assert_eq!(batches.len(), 1);

        let col_a = batches[0].column_by_name("a").unwrap().as_primitive::<T>();
        assert_eq!(col_a.null_count(), 0);
        assert_eq!(col_a.values(), &[1, 3].map(T::Native::usize_as));

        let col2 = batches[0].column_by_name("b").unwrap().as_primitive::<T>();
        assert_eq!(col2.null_count(), 1);
        assert_eq!(col2.values(), &[2, 0].map(T::Native::usize_as));
    }

    #[test]
    fn test_durations() {
        test_duration::<DurationNanosecondType>();
        test_duration::<DurationMicrosecondType>();
        test_duration::<DurationMillisecondType>();
        test_duration::<DurationSecondType>();
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

        let batches = do_read(json, 1024, true, false, schema);
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
            let mut reader = ReaderBuilder::new(schema.clone())
                .build(Cursor::new(non_null.as_bytes()))
                .unwrap();
            assert!(reader.next().unwrap().is_err()); // Should error as not nullable

            let null = r#"{"foo": {bar: null}}"#;
            let mut reader = ReaderBuilder::new(schema.clone())
                .build(Cursor::new(null.as_bytes()))
                .unwrap();
            assert!(reader.next().unwrap().is_err()); // Should error as not nullable

            // Test nulls in nullable parent can mask nulls in non-nullable child
            let null = r#"{"foo": null}"#;
            let mut reader = ReaderBuilder::new(schema)
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

    #[test]
    fn test_truncation() {
        let buf = r#"
        {"i64": 9223372036854775807, "u64": 18446744073709551615 }
        {"i64": "9223372036854775807", "u64": "18446744073709551615" }
        {"i64": -9223372036854775808, "u64": 0 }
        {"i64": "-9223372036854775808", "u64": 0 }
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("i64", DataType::Int64, true),
            Field::new("u64", DataType::UInt64, true),
        ]));

        let batches = do_read(buf, 1024, true, false, schema);
        assert_eq!(batches.len(), 1);

        let i64 = batches[0].column(0).as_primitive::<Int64Type>();
        assert_eq!(i64.values(), &[i64::MAX, i64::MAX, i64::MIN, i64::MIN]);

        let u64 = batches[0].column(1).as_primitive::<UInt64Type>();
        assert_eq!(u64.values(), &[u64::MAX, u64::MAX, u64::MIN, u64::MIN]);
    }

    #[test]
    fn test_timestamp_truncation() {
        let buf = r#"
        {"time": 9223372036854775807 }
        {"time": -9223372036854775808 }
        {"time": 9e5 }
        "#;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        let batches = do_read(buf, 1024, true, false, schema);
        assert_eq!(batches.len(), 1);

        let i64 = batches[0]
            .column(0)
            .as_primitive::<TimestampNanosecondType>();
        assert_eq!(i64.values(), &[i64::MAX, i64::MIN, 900000]);
    }

    #[test]
    fn test_strict_mode_no_missing_columns_in_schema() {
        let buf = r#"
        {"a": 1, "b": "2", "c": true}
        {"a": 2E0, "b": "4", "c": false}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int16, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Boolean, false),
        ]));

        let batches = do_read(buf, 1024, true, true, schema);
        assert_eq!(batches.len(), 1);

        let buf = r#"
        {"a": 1, "b": "2", "c": {"a": true, "b": 1}}
        {"a": 2E0, "b": "4", "c": {"a": false, "b": 2}}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int16, false),
            Field::new("b", DataType::Utf8, false),
            Field::new_struct(
                "c",
                vec![
                    Field::new("a", DataType::Boolean, false),
                    Field::new("b", DataType::Int16, false),
                ],
                false,
            ),
        ]));

        let batches = do_read(buf, 1024, true, true, schema);
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn test_strict_mode_missing_columns_in_schema() {
        let buf = r#"
        {"a": 1, "b": "2", "c": true}
        {"a": 2E0, "b": "4", "c": false}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int16, true),
            Field::new("c", DataType::Boolean, true),
        ]));

        let err = ReaderBuilder::new(schema)
            .with_batch_size(1024)
            .with_strict_mode(true)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .read()
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Json error: column 'b' missing from schema"
        );

        let buf = r#"
        {"a": 1, "b": "2", "c": {"a": true, "b": 1}}
        {"a": 2E0, "b": "4", "c": {"a": false, "b": 2}}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int16, false),
            Field::new("b", DataType::Utf8, false),
            Field::new_struct("c", vec![Field::new("a", DataType::Boolean, false)], false),
        ]));

        let err = ReaderBuilder::new(schema)
            .with_batch_size(1024)
            .with_strict_mode(true)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .read()
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Json error: whilst decoding field 'c': column 'b' missing from schema"
        );
    }

    fn read_file(path: &str, schema: Option<Schema>) -> Reader<BufReader<File>> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let schema = schema.unwrap_or_else(|| {
            let (schema, _) = infer_json_schema(&mut reader, None).unwrap();
            reader.rewind().unwrap();
            schema
        });
        let builder = ReaderBuilder::new(Arc::new(schema)).with_batch_size(64);
        builder.build(reader).unwrap()
    }

    #[test]
    fn test_json_basic() {
        let mut reader = read_file("test/data/basic.json", None);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(8, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(1, b.0);
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(2, c.0);
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(3, d.0);
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch.column(a.0).as_primitive::<Int64Type>();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        let bb = batch.column(b.0).as_primitive::<Float64Type>();
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-3.5, bb.value(1));
        let cc = batch.column(c.0).as_boolean();
        assert!(!cc.value(0));
        assert!(cc.value(10));
        let dd = batch.column(d.0).as_string::<i32>();
        assert_eq!("4", dd.value(0));
        assert_eq!("text", dd.value(8));
    }

    #[test]
    fn test_json_empty_projection() {
        let mut reader = read_file("test/data/basic.json", Some(Schema::empty()));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(0, batch.num_columns());
        assert_eq!(12, batch.num_rows());
    }

    #[test]
    fn test_json_basic_with_nulls() {
        let mut reader = read_file("test/data/basic_nulls.json", None);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch.column(a.0).as_primitive::<Int64Type>();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(11));
        let bb = batch.column(b.0).as_primitive::<Float64Type>();
        assert!(bb.is_valid(0));
        assert!(!bb.is_valid(2));
        assert!(!bb.is_valid(11));
        let cc = batch.column(c.0).as_boolean();
        assert!(cc.is_valid(0));
        assert!(!cc.is_valid(4));
        assert!(!cc.is_valid(11));
        let dd = batch.column(d.0).as_string::<i32>();
        assert!(!dd.is_valid(0));
        assert!(dd.is_valid(1));
        assert!(!dd.is_valid(4));
        assert!(!dd.is_valid(11));
    }

    #[test]
    fn test_json_basic_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::Utf8, false),
        ]);

        let mut reader = read_file("test/data/basic.json", Some(schema.clone()));
        let reader_schema = reader.schema();
        assert_eq!(reader_schema.as_ref(), &schema);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float32, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch.column(a.0).as_primitive::<Int64Type>();
        assert_eq!(1, aa.value(0));
        assert_eq!(100000000000000, aa.value(11));
        let bb = batch.column(b.0).as_primitive::<Float32Type>();
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-3.5, bb.value(1));
    }

    #[test]
    fn test_json_basic_schema_projection() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Boolean, false),
        ]);

        let mut reader = read_file("test/data/basic.json", Some(schema.clone()));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.schema().fields().len());
        assert_eq!(12, batch.num_rows());

        assert_eq!(batch.schema().as_ref(), &schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int64, a.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(1, c.0);
        assert_eq!(&DataType::Boolean, c.1.data_type());
    }

    #[test]
    fn test_json_arrays() {
        let mut reader = read_file("test/data/arrays.json", None);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(
            &DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
            b.1.data_type()
        );
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(
            &DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true))),
            c.1.data_type()
        );
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch.column(a.0).as_primitive::<Int64Type>();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        assert_eq!(1627668684594000000, aa.value(2));
        let bb = batch.column(b.0).as_list::<i32>();
        let bb = bb.values().as_primitive::<Float64Type>();
        assert_eq!(9, bb.len());
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-6.1, bb.value(5));
        assert!(!bb.is_valid(7));

        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let cc = cc.values().as_boolean();
        assert_eq!(6, cc.len());
        assert!(!cc.value(0));
        assert!(!cc.value(4));
        assert!(!cc.is_valid(5));
    }

    #[test]
    fn test_empty_json_arrays() {
        let json_content = r#"
            {"items": []}
            {"items": null}
            {}
            "#;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(FieldRef::new(Field::new_list_field(DataType::Null, true))),
            true,
        )]));

        let batches = do_read(json_content, 1024, false, false, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_list::<i32>();
        assert_eq!(col1.null_count(), 2);
        assert!(col1.value(0).is_empty());
        assert_eq!(col1.value(0).data_type(), &DataType::Null);
        assert!(col1.is_null(1));
        assert!(col1.is_null(2));
    }

    #[test]
    fn test_nested_empty_json_arrays() {
        let json_content = r#"
            {"items": [[],[]]}
            {"items": [[null, null],[null]]}
            "#;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(FieldRef::new(Field::new_list_field(
                DataType::List(FieldRef::new(Field::new_list_field(DataType::Null, true))),
                true,
            ))),
            true,
        )]));

        let batches = do_read(json_content, 1024, false, false, schema);
        assert_eq!(batches.len(), 1);

        let col1 = batches[0].column(0).as_list::<i32>();
        assert_eq!(col1.null_count(), 0);
        assert_eq!(col1.value(0).len(), 2);
        assert!(col1.value(0).as_list::<i32>().value(0).is_empty());
        assert!(col1.value(0).as_list::<i32>().value(1).is_empty());

        assert_eq!(col1.value(1).len(), 2);
        assert_eq!(col1.value(1).as_list::<i32>().value(0).len(), 2);
        assert_eq!(col1.value(1).as_list::<i32>().value(1).len(), 1);
    }

    #[test]
    fn test_nested_list_json_arrays() {
        let c_field = Field::new_struct("c", vec![Field::new("d", DataType::Utf8, true)], true);
        let a_struct_field = Field::new_struct(
            "a",
            vec![Field::new("b", DataType::Boolean, true), c_field.clone()],
            true,
        );
        let a_field = Field::new("a", DataType::List(Arc::new(a_struct_field.clone())), true);
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new(schema).with_batch_size(64);
        let json_content = r#"
        {"a": [{"b": true, "c": {"d": "a_text"}}, {"b": false, "c": {"d": "b_text"}}]}
        {"a": [{"b": false, "c": null}]}
        {"a": [{"b": true, "c": {"d": "c_text"}}, {"b": null, "c": {"d": "d_text"}}, {"b": true, "c": {"d": null}}]}
        {"a": null}
        {"a": []}
        {"a": [null]}
        "#;
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();

        // build expected output
        let d = StringArray::from(vec![
            Some("a_text"),
            Some("b_text"),
            None,
            Some("c_text"),
            Some("d_text"),
            None,
            None,
        ]);
        let c = ArrayDataBuilder::new(c_field.data_type().clone())
            .len(7)
            .add_child_data(d.to_data())
            .null_bit_buffer(Some(Buffer::from([0b00111011])))
            .build()
            .unwrap();
        let b = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
            Some(true),
            None,
        ]);
        let a = ArrayDataBuilder::new(a_struct_field.data_type().clone())
            .len(7)
            .add_child_data(b.to_data())
            .add_child_data(c.clone())
            .null_bit_buffer(Some(Buffer::from([0b00111111])))
            .build()
            .unwrap();
        let a_list = ArrayDataBuilder::new(a_field.data_type().clone())
            .len(6)
            .add_buffer(Buffer::from_slice_ref([0i32, 2, 3, 6, 6, 6, 7]))
            .add_child_data(a)
            .null_bit_buffer(Some(Buffer::from([0b00110111])))
            .build()
            .unwrap();
        let expected = make_array(a_list);

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert_eq!(read.len(), 6);
        // compare the arrays the long way around, to better detect differences
        let read: &ListArray = read.as_list::<i32>();
        let expected = expected.as_list::<i32>();
        assert_eq!(read.value_offsets(), &[0, 2, 3, 6, 6, 6, 7]);
        // compare list null buffers
        assert_eq!(read.nulls(), expected.nulls());
        // build struct from list
        let struct_array = read.values().as_struct();
        let expected_struct_array = expected.values().as_struct();

        assert_eq!(7, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert_eq!(7, expected_struct_array.len());
        assert_eq!(1, expected_struct_array.null_count());
        // test struct's nulls
        assert_eq!(struct_array.nulls(), expected_struct_array.nulls());
        // test struct's fields
        let read_b = struct_array.column(0);
        assert_eq!(read_b.as_ref(), &b);
        let read_c = struct_array.column(1);
        assert_eq!(read_c.to_data(), c);
        let read_c = read_c.as_struct();
        let read_d = read_c.column(0);
        assert_eq!(read_d.as_ref(), &d);

        assert_eq!(read, expected);
    }

    #[test]
    fn test_skip_empty_lines() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let builder = ReaderBuilder::new(Arc::new(schema)).with_batch_size(64);
        let json_content = "
        {\"a\": 1}
        {\"a\": 2}
        {\"a\": 3}";
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let c = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, c.1.data_type());
    }

    #[test]
    fn test_with_multiple_batches() {
        let file = File::open("test/data/basic_nulls.json").unwrap();
        let mut reader = BufReader::new(file);
        let (schema, _) = infer_json_schema(&mut reader, None).unwrap();
        reader.rewind().unwrap();

        let builder = ReaderBuilder::new(Arc::new(schema)).with_batch_size(5);
        let mut reader = builder.build(reader).unwrap();

        let mut num_records = Vec::new();
        while let Some(rb) = reader.next().transpose().unwrap() {
            num_records.push(rb.num_rows());
        }

        assert_eq!(vec![5, 5, 2], num_records);
    }

    #[test]
    fn test_timestamp_from_json_seconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]);

        let mut reader = read_file("test/data/basic_nulls.json", Some(schema));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Second, None),
            a.1.data_type()
        );

        let aa = batch.column(a.0).as_primitive::<TimestampSecondType>();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_timestamp_from_json_milliseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);

        let mut reader = read_file("test/data/basic_nulls.json", Some(schema));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            a.1.data_type()
        );

        let aa = batch.column(a.0).as_primitive::<TimestampMillisecondType>();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_date_from_json_milliseconds() {
        let schema = Schema::new(vec![Field::new("a", DataType::Date64, true)]);

        let mut reader = read_file("test/data/basic_nulls.json", Some(schema));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Date64, a.1.data_type());

        let aa = batch.column(a.0).as_primitive::<Date64Type>();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_time_from_json_nanoseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]);

        let mut reader = read_file("test/data/basic_nulls.json", Some(schema));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Time64(TimeUnit::Nanosecond), a.1.data_type());

        let aa = batch.column(a.0).as_primitive::<Time64NanosecondType>();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_json_iterator() {
        let file = File::open("test/data/basic.json").unwrap();
        let mut reader = BufReader::new(file);
        let (schema, _) = infer_json_schema(&mut reader, None).unwrap();
        reader.rewind().unwrap();

        let builder = ReaderBuilder::new(Arc::new(schema)).with_batch_size(5);
        let reader = builder.build(reader).unwrap();
        let schema = reader.schema();
        let (col_a_index, _) = schema.column_with_name("a").unwrap();

        let mut sum_num_rows = 0;
        let mut num_batches = 0;
        let mut sum_a = 0;
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(8, batch.num_columns());
            sum_num_rows += batch.num_rows();
            num_batches += 1;
            let batch_schema = batch.schema();
            assert_eq!(schema, batch_schema);
            let a_array = batch.column(col_a_index).as_primitive::<Int64Type>();
            sum_a += (0..a_array.len()).map(|i| a_array.value(i)).sum::<i64>();
        }
        assert_eq!(12, sum_num_rows);
        assert_eq!(3, num_batches);
        assert_eq!(100000000000011, sum_a);
    }

    #[test]
    fn test_decoder_error() {
        let schema = Arc::new(Schema::new(vec![Field::new_struct(
            "a",
            vec![Field::new("child", DataType::Int32, false)],
            true,
        )]));

        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder().unwrap();
        let _ = decoder.decode(r#"{"a": { "child":"#.as_bytes()).unwrap();
        assert!(decoder.tape_decoder.has_partial_row());
        assert_eq!(decoder.tape_decoder.num_buffered_rows(), 1);
        let _ = decoder.flush().unwrap_err();
        assert!(decoder.tape_decoder.has_partial_row());
        assert_eq!(decoder.tape_decoder.num_buffered_rows(), 1);

        let parse_err = |s: &str| {
            ReaderBuilder::new(schema.clone())
                .build(Cursor::new(s.as_bytes()))
                .unwrap()
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
        };

        let err = parse_err(r#"{"a": 123}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': expected { got 123"
        );

        let err = parse_err(r#"{"a": ["bar"]}"#);
        assert_eq!(
            err,
            r#"Json error: whilst decoding field 'a': expected { got ["bar"]"#
        );

        let err = parse_err(r#"{"a": []}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': expected { got []"
        );

        let err = parse_err(r#"{"a": [{"child": 234}]}"#);
        assert_eq!(
            err,
            r#"Json error: whilst decoding field 'a': expected { got [{"child": 234}]"#
        );

        let err = parse_err(r#"{"a": [{"child": {"foo": [{"foo": ["bar"]}]}}]}"#);
        assert_eq!(
            err,
            r#"Json error: whilst decoding field 'a': expected { got [{"child": {"foo": [{"foo": ["bar"]}]}}]"#
        );

        let err = parse_err(r#"{"a": true}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': expected { got true"
        );

        let err = parse_err(r#"{"a": false}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': expected { got false"
        );

        let err = parse_err(r#"{"a": "foo"}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': expected { got \"foo\""
        );

        let err = parse_err(r#"{"a": {"child": false}}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': whilst decoding field 'child': expected primitive got false"
        );

        let err = parse_err(r#"{"a": {"child": []}}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': whilst decoding field 'child': expected primitive got []"
        );

        let err = parse_err(r#"{"a": {"child": [123]}}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': whilst decoding field 'child': expected primitive got [123]"
        );

        let err = parse_err(r#"{"a": {"child": [123, 3465346]}}"#);
        assert_eq!(
            err,
            "Json error: whilst decoding field 'a': whilst decoding field 'child': expected primitive got [123, 3465346]"
        );
    }

    #[test]
    fn test_serialize_timestamp() {
        let json = vec![
            json!({"timestamp": 1681319393}),
            json!({"timestamp": "1970-01-01T00:00:00+02:00"}),
        ];
        let schema = Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]);
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .build_decoder()
            .unwrap();
        decoder.serialize(&json).unwrap();
        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        let values = batch.column(0).as_primitive::<TimestampSecondType>();
        assert_eq!(values.values(), &[1681319393, -7200]);
    }

    #[test]
    fn test_serialize_decimal() {
        let json = vec![
            json!({"decimal": 1.234}),
            json!({"decimal": "1.234"}),
            json!({"decimal": 1234}),
            json!({"decimal": "1234"}),
        ];
        let schema = Schema::new(vec![Field::new(
            "decimal",
            DataType::Decimal128(10, 3),
            true,
        )]);
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .build_decoder()
            .unwrap();
        decoder.serialize(&json).unwrap();
        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 1);
        let values = batch.column(0).as_primitive::<Decimal128Type>();
        assert_eq!(values.values(), &[1234, 1234, 1234000, 1234000]);
    }

    #[test]
    fn test_serde_field() {
        let field = Field::new("int", DataType::Int32, true);
        let mut decoder = ReaderBuilder::new_with_field(field)
            .build_decoder()
            .unwrap();
        decoder.serialize(&[1_i32, 2, 3, 4]).unwrap();
        let b = decoder.flush().unwrap().unwrap();
        let values = b.column(0).as_primitive::<Int32Type>().values();
        assert_eq!(values, &[1, 2, 3, 4]);
    }

    #[test]
    fn test_serde_large_numbers() {
        let field = Field::new("int", DataType::Int64, true);
        let mut decoder = ReaderBuilder::new_with_field(field)
            .build_decoder()
            .unwrap();

        decoder.serialize(&[1699148028689_u64, 2, 3, 4]).unwrap();
        let b = decoder.flush().unwrap().unwrap();
        let values = b.column(0).as_primitive::<Int64Type>().values();
        assert_eq!(values, &[1699148028689, 2, 3, 4]);

        let field = Field::new(
            "int",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        );
        let mut decoder = ReaderBuilder::new_with_field(field)
            .build_decoder()
            .unwrap();

        decoder.serialize(&[1699148028689_u64, 2, 3, 4]).unwrap();
        let b = decoder.flush().unwrap().unwrap();
        let values = b
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .values();
        assert_eq!(values, &[1699148028689, 2, 3, 4]);
    }

    #[test]
    fn test_coercing_primitive_into_string_decoder() {
        let buf = &format!(
            r#"[{{"a": 1, "b": "A", "c": "T"}}, {{"a": 2, "b": "BB", "c": "F"}}, {{"a": {}, "b": 123, "c": false}}, {{"a": {}, "b": 789, "c": true}}]"#,
            (i32::MAX as i64 + 10),
            i64::MAX - 10
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]);
        let json_array: Vec<serde_json::Value> = serde_json::from_str(buf).unwrap();
        let schema_ref = Arc::new(schema);

        // read record batches
        let reader = ReaderBuilder::new(schema_ref.clone()).with_coerce_primitive(true);
        let mut decoder = reader.build_decoder().unwrap();
        decoder.serialize(json_array.as_slice()).unwrap();
        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(
            batch,
            RecordBatch::try_new(
                schema_ref,
                vec![
                    Arc::new(Float64Array::from(vec![
                        1.0,
                        2.0,
                        (i32::MAX as i64 + 10) as f64,
                        (i64::MAX - 10) as f64
                    ])),
                    Arc::new(StringArray::from(vec!["A", "BB", "123", "789"])),
                    Arc::new(StringArray::from(vec!["T", "F", "false", "true"])),
                ]
            )
            .unwrap()
        );
    }

    // Parse the given `row` in `struct_mode` as a type given by fields.
    //
    // If as_struct == true, wrap the fields in a Struct field with name "r".
    // If as_struct == false, wrap the fields in a Schema.
    fn _parse_structs(
        row: &str,
        struct_mode: StructMode,
        fields: Fields,
        as_struct: bool,
    ) -> Result<RecordBatch, ArrowError> {
        let builder = if as_struct {
            ReaderBuilder::new_with_field(Field::new("r", DataType::Struct(fields), true))
        } else {
            ReaderBuilder::new(Arc::new(Schema::new(fields)))
        };
        builder
            .with_struct_mode(struct_mode)
            .build(Cursor::new(row.as_bytes()))
            .unwrap()
            .next()
            .unwrap()
    }

    #[test]
    fn test_struct_decoding_list_length() {
        use arrow_array::array;

        let row = "[1, 2]";

        let mut fields = vec![Field::new("a", DataType::Int32, true)];
        let too_few_fields = Fields::from(fields.clone());
        fields.push(Field::new("b", DataType::Int32, true));
        let correct_fields = Fields::from(fields.clone());
        fields.push(Field::new("c", DataType::Int32, true));
        let too_many_fields = Fields::from(fields.clone());

        let parse = |fields: Fields, as_struct: bool| {
            _parse_structs(row, StructMode::ListOnly, fields, as_struct)
        };

        let expected_row = StructArray::new(
            correct_fields.clone(),
            vec![
                Arc::new(array::Int32Array::from(vec![1])),
                Arc::new(array::Int32Array::from(vec![2])),
            ],
            None,
        );
        let row_field = Field::new("r", DataType::Struct(correct_fields.clone()), true);

        assert_eq!(
            parse(too_few_fields.clone(), true).unwrap_err().to_string(),
            "Json error: found extra columns for 1 fields".to_string()
        );
        assert_eq!(
            parse(too_few_fields, false).unwrap_err().to_string(),
            "Json error: found extra columns for 1 fields".to_string()
        );
        assert_eq!(
            parse(correct_fields.clone(), true).unwrap(),
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![row_field])),
                vec![Arc::new(expected_row.clone())]
            )
            .unwrap()
        );
        assert_eq!(
            parse(correct_fields, false).unwrap(),
            RecordBatch::from(expected_row)
        );
        assert_eq!(
            parse(too_many_fields.clone(), true)
                .unwrap_err()
                .to_string(),
            "Json error: found 2 columns for 3 fields".to_string()
        );
        assert_eq!(
            parse(too_many_fields, false).unwrap_err().to_string(),
            "Json error: found 2 columns for 3 fields".to_string()
        );
    }

    #[test]
    fn test_struct_decoding() {
        use arrow_array::builder;

        let nested_object_json = r#"{"a": {"b": [1, 2], "c": {"d": 3}}}"#;
        let nested_list_json = r#"[[[1, 2], {"d": 3}]]"#;
        let nested_mixed_json = r#"{"a": [[1, 2], {"d": 3}]}"#;

        let struct_fields = Fields::from(vec![
            Field::new("b", DataType::new_list(DataType::Int32, true), true),
            Field::new_map(
                "c",
                "entries",
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int32, true),
                false,
                false,
            ),
        ]);

        let list_array =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), Some(2)])]);

        let map_array = {
            let mut map_builder = builder::MapBuilder::new(
                None,
                builder::StringBuilder::new(),
                builder::Int32Builder::new(),
            );
            map_builder.keys().append_value("d");
            map_builder.values().append_value(3);
            map_builder.append(true).unwrap();
            map_builder.finish()
        };

        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(list_array), Arc::new(map_array)],
            None,
        );

        let fields = Fields::from(vec![Field::new("a", DataType::Struct(struct_fields), true)]);
        let schema = Arc::new(Schema::new(fields.clone()));
        let expected = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();

        let parse = |row: &str, struct_mode: StructMode| {
            _parse_structs(row, struct_mode, fields.clone(), false)
        };

        assert_eq!(
            parse(nested_object_json, StructMode::ObjectOnly).unwrap(),
            expected
        );
        assert_eq!(
            parse(nested_list_json, StructMode::ObjectOnly)
                .unwrap_err()
                .to_string(),
            "Json error: expected { got [[[1, 2], {\"d\": 3}]]".to_owned()
        );
        assert_eq!(
            parse(nested_mixed_json, StructMode::ObjectOnly)
                .unwrap_err()
                .to_string(),
            "Json error: whilst decoding field 'a': expected { got [[1, 2], {\"d\": 3}]".to_owned()
        );

        assert_eq!(
            parse(nested_list_json, StructMode::ListOnly).unwrap(),
            expected
        );
        assert_eq!(
            parse(nested_object_json, StructMode::ListOnly)
                .unwrap_err()
                .to_string(),
            "Json error: expected [ got {\"a\": {\"b\": [1, 2]\"c\": {\"d\": 3}}}".to_owned()
        );
        assert_eq!(
            parse(nested_mixed_json, StructMode::ListOnly)
                .unwrap_err()
                .to_string(),
            "Json error: expected [ got {\"a\": [[1, 2], {\"d\": 3}]}".to_owned()
        );
    }

    // Test cases:
    // [] -> RecordBatch row with no entries.  Schema = [('a', Int32)] -> Error
    // [] -> RecordBatch row with no entries. Schema = [('r', [('a', Int32)])] -> Error
    // [] -> StructArray row with no entries. Fields [('a', Int32')] -> Error
    // [[]] -> RecordBatch row with empty struct entry. Schema = [('r', [('a', Int32)])] -> Error
    #[test]
    fn test_struct_decoding_empty_list() {
        let int_field = Field::new("a", DataType::Int32, true);
        let struct_field = Field::new(
            "r",
            DataType::Struct(Fields::from(vec![int_field.clone()])),
            true,
        );

        let parse = |row: &str, as_struct: bool, field: Field| {
            _parse_structs(
                row,
                StructMode::ListOnly,
                Fields::from(vec![field]),
                as_struct,
            )
        };

        // Missing fields
        assert_eq!(
            parse("[]", true, struct_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: found 0 columns for 1 fields".to_owned()
        );
        assert_eq!(
            parse("[]", false, int_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: found 0 columns for 1 fields".to_owned()
        );
        assert_eq!(
            parse("[]", false, struct_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: found 0 columns for 1 fields".to_owned()
        );
        assert_eq!(
            parse("[[]]", false, struct_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: whilst decoding field 'r': found 0 columns for 1 fields".to_owned()
        );
    }

    #[test]
    fn test_decode_list_struct_with_wrong_types() {
        let int_field = Field::new("a", DataType::Int32, true);
        let struct_field = Field::new(
            "r",
            DataType::Struct(Fields::from(vec![int_field.clone()])),
            true,
        );

        let parse = |row: &str, as_struct: bool, field: Field| {
            _parse_structs(
                row,
                StructMode::ListOnly,
                Fields::from(vec![field]),
                as_struct,
            )
        };

        // Wrong values
        assert_eq!(
            parse(r#"[["a"]]"#, false, struct_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: whilst decoding field 'r': whilst decoding field 'a': failed to parse \"a\" as Int32".to_owned()
        );
        assert_eq!(
            parse(r#"[["a"]]"#, true, struct_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: whilst decoding field 'r': whilst decoding field 'a': failed to parse \"a\" as Int32".to_owned()
        );
        assert_eq!(
            parse(r#"["a"]"#, true, int_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: whilst decoding field 'a': failed to parse \"a\" as Int32".to_owned()
        );
        assert_eq!(
            parse(r#"["a"]"#, false, int_field.clone())
                .unwrap_err()
                .to_string(),
            "Json error: whilst decoding field 'a': failed to parse \"a\" as Int32".to_owned()
        );
    }
}
