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

//! CSV Reader
//!
//! # Basic Usage
//!
//! This CSV reader allows CSV files to be read into the Arrow memory model. Records are
//! loaded in batches and are then converted from row-based data to columnar data.
//!
//! Example:
//!
//! ```
//! # use arrow_schema::*;
//! # use arrow_csv::{Reader, ReaderBuilder};
//! # use std::fs::File;
//! # use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("city", DataType::Utf8, false),
//!     Field::new("lat", DataType::Float64, false),
//!     Field::new("lng", DataType::Float64, false),
//! ]);
//!
//! let file = File::open("test/data/uk_cities.csv").unwrap();
//!
//! let mut csv = ReaderBuilder::new(Arc::new(schema)).build(file).unwrap();
//! let batch = csv.next().unwrap().unwrap();
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
//! # use arrow_csv::reader::Decoder;
//! #
//! fn decode_stream<S: Stream<Item = Bytes> + Unpin>(
//!     mut decoder: Decoder,
//!     mut input: S,
//! ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
//!     let mut buffered = Bytes::new();
//!     futures::stream::poll_fn(move |cx| {
//!         loop {
//!             if buffered.is_empty() {
//!                 if let Some(b) = ready!(input.poll_next_unpin(cx)) {
//!                     buffered = b;
//!                 }
//!                 // Note: don't break on `None` as the decoder needs
//!                 // to be called with an empty array to delimit the
//!                 // final record
//!             }
//!             let decoded = match decoder.decode(buffered.as_ref()) {
//!                 Ok(0) => break,
//!                 Ok(decoded) => decoded,
//!                 Err(e) => return Poll::Ready(Some(Err(e))),
//!             };
//!             buffered.advance(decoded);
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
//! # use std::pin::Pin;
//! # use std::task::{Poll, ready};
//! # use futures::Stream;
//! # use tokio::io::AsyncBufRead;
//! # use arrow_array::RecordBatch;
//! # use arrow_csv::reader::Decoder;
//! # use arrow_schema::ArrowError;
//! fn decode_stream<R: AsyncBufRead + Unpin>(
//!     mut decoder: Decoder,
//!     mut reader: R,
//! ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
//!     futures::stream::poll_fn(move |cx| {
//!         loop {
//!             let b = match ready!(Pin::new(&mut reader).poll_fill_buf(cx)) {
//!                 Ok(b) => b,
//!                 Err(e) => return Poll::Ready(Some(Err(e.into()))),
//!             };
//!             let decoded = match decoder.decode(b) {
//!                 // Note: the decoder needs to be called with an empty
//!                 // array to delimit the final record
//!                 Ok(0) => break,
//!                 Ok(decoded) => decoded,
//!                 Err(e) => return Poll::Ready(Some(Err(e))),
//!             };
//!             Pin::new(&mut reader).consume(decoded);
//!         }
//!
//!         Poll::Ready(decoder.flush().transpose())
//!     })
//! }
//! ```
//!

mod records;

use arrow_array::builder::{NullBuilder, PrimitiveBuilder};
use arrow_array::types::*;
use arrow_array::*;
use arrow_cast::parse::{parse_decimal, string_to_datetime, Parser};
use arrow_schema::*;
use chrono::{TimeZone, Utc};
use csv::StringRecord;
use lazy_static::lazy_static;
use regex::{Regex, RegexSet};
use std::fmt::{self, Debug};
use std::fs::File;
use std::io::{BufRead, BufReader as StdBufReader, Read};
use std::sync::Arc;

use crate::map_csv_error;
use crate::reader::records::{RecordDecoder, StringRecords};
use arrow_array::timezone::Tz;

lazy_static! {
    /// Order should match [`InferredDataType`]
    static ref REGEX_SET: RegexSet = RegexSet::new([
        r"(?i)^(true)$|^(false)$(?-i)", //BOOLEAN
        r"^-?(\d+)$", //INTEGER
        r"^-?((\d*\.\d+|\d+\.\d*)([eE][-+]?\d+)?|\d+([eE][-+]?\d+))$", //DECIMAL
        r"^\d{4}-\d\d-\d\d$", //DATE32
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:[^\d\.].*)?$", //Timestamp(Second)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,3}(?:[^\d].*)?$", //Timestamp(Millisecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,6}(?:[^\d].*)?$", //Timestamp(Microsecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,9}(?:[^\d].*)?$", //Timestamp(Nanosecond)
    ]).unwrap();
}

/// A wrapper over `Option<Regex>` to check if the value is `NULL`.
#[derive(Debug, Clone, Default)]
struct NullRegex(Option<Regex>);

impl NullRegex {
    /// Returns true if the value should be considered as `NULL` according to
    /// the provided regular expression.
    #[inline]
    fn is_null(&self, s: &str) -> bool {
        match &self.0 {
            Some(r) => r.is_match(s),
            None => s.is_empty(),
        }
    }
}

#[derive(Default, Copy, Clone)]
struct InferredDataType {
    /// Packed booleans indicating type
    ///
    /// 0 - Boolean
    /// 1 - Integer
    /// 2 - Float64
    /// 3 - Date32
    /// 4 - Timestamp(Second)
    /// 5 - Timestamp(Millisecond)
    /// 6 - Timestamp(Microsecond)
    /// 7 - Timestamp(Nanosecond)
    /// 8 - Utf8
    packed: u16,
}

impl InferredDataType {
    /// Returns the inferred data type
    fn get(&self) -> DataType {
        match self.packed {
            0 => DataType::Null,
            1 => DataType::Boolean,
            2 => DataType::Int64,
            4 | 6 => DataType::Float64, // Promote Int64 to Float64
            b if b != 0 && (b & !0b11111000) == 0 => match b.leading_zeros() {
                // Promote to highest precision temporal type
                8 => DataType::Timestamp(TimeUnit::Nanosecond, None),
                9 => DataType::Timestamp(TimeUnit::Microsecond, None),
                10 => DataType::Timestamp(TimeUnit::Millisecond, None),
                11 => DataType::Timestamp(TimeUnit::Second, None),
                12 => DataType::Date32,
                _ => unreachable!(),
            },
            _ => DataType::Utf8,
        }
    }

    /// Updates the [`InferredDataType`] with the given string
    fn update(&mut self, string: &str) {
        self.packed |= if string.starts_with('"') {
            1 << 8 // Utf8
        } else if let Some(m) = REGEX_SET.matches(string).into_iter().next() {
            if m == 1 && string.len() >= 19 && string.parse::<i64>().is_err() {
                // if overflow i64, fallback to utf8
                1 << 8
            } else {
                1 << m
            }
        } else if string == "NaN" || string == "nan" || string == "inf" || string == "-inf" {
            1 << 2 // Float64
        } else {
            1 << 8 // Utf8
        }
    }
}

/// The format specification for the CSV file
#[derive(Debug, Clone, Default)]
pub struct Format {
    header: bool,
    delimiter: Option<u8>,
    escape: Option<u8>,
    quote: Option<u8>,
    terminator: Option<u8>,
    comment: Option<u8>,
    null_regex: NullRegex,
    truncated_rows: bool,
}

impl Format {
    /// Specify whether the CSV file has a header, defaults to `false`
    ///
    /// When `true`, the first row of the CSV file is treated as a header row
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.header = has_header;
        self
    }

    /// Specify a custom delimiter character, defaults to comma `','`
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Specify an escape character, defaults to `None`
    pub fn with_escape(mut self, escape: u8) -> Self {
        self.escape = Some(escape);
        self
    }

    /// Specify a custom quote character, defaults to double quote `'"'`
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = Some(quote);
        self
    }

    /// Specify a custom terminator character, defaults to CRLF
    pub fn with_terminator(mut self, terminator: u8) -> Self {
        self.terminator = Some(terminator);
        self
    }

    /// Specify a comment character, defaults to `None`
    ///
    /// Lines starting with this character will be ignored
    pub fn with_comment(mut self, comment: u8) -> Self {
        self.comment = Some(comment);
        self
    }

    /// Provide a regex to match null values, defaults to `^$`
    pub fn with_null_regex(mut self, null_regex: Regex) -> Self {
        self.null_regex = NullRegex(Some(null_regex));
        self
    }

    /// Whether to allow truncated rows when parsing.
    ///
    /// By default this is set to `false` and will error if the CSV rows have different lengths.
    /// When set to true then it will allow records with less than the expected number of columns
    /// and fill the missing columns with nulls. If the record's schema is not nullable, then it
    /// will still return an error.
    pub fn with_truncated_rows(mut self, allow: bool) -> Self {
        self.truncated_rows = allow;
        self
    }

    /// Infer schema of CSV records from the provided `reader`
    ///
    /// If `max_records` is `None`, all records will be read, otherwise up to `max_records`
    /// records are read to infer the schema
    ///
    /// Returns inferred schema and number of records read
    pub fn infer_schema<R: Read>(
        &self,
        reader: R,
        max_records: Option<usize>,
    ) -> Result<(Schema, usize), ArrowError> {
        let mut csv_reader = self.build_reader(reader);

        // get or create header names
        // when has_header is false, creates default column names with column_ prefix
        let headers: Vec<String> = if self.header {
            let headers = &csv_reader.headers().map_err(map_csv_error)?.clone();
            headers.iter().map(|s| s.to_string()).collect()
        } else {
            let first_record_count = &csv_reader.headers().map_err(map_csv_error)?.len();
            (0..*first_record_count)
                .map(|i| format!("column_{}", i + 1))
                .collect()
        };

        let header_length = headers.len();
        // keep track of inferred field types
        let mut column_types: Vec<InferredDataType> = vec![Default::default(); header_length];

        let mut records_count = 0;

        let mut record = StringRecord::new();
        let max_records = max_records.unwrap_or(usize::MAX);
        while records_count < max_records {
            if !csv_reader.read_record(&mut record).map_err(map_csv_error)? {
                break;
            }
            records_count += 1;

            // Note since we may be looking at a sample of the data, we make the safe assumption that
            // they could be nullable
            for (i, column_type) in column_types.iter_mut().enumerate().take(header_length) {
                if let Some(string) = record.get(i) {
                    if !self.null_regex.is_null(string) {
                        column_type.update(string)
                    }
                }
            }
        }

        // build schema from inference results
        let fields: Fields = column_types
            .iter()
            .zip(&headers)
            .map(|(inferred, field_name)| Field::new(field_name, inferred.get(), true))
            .collect();

        Ok((Schema::new(fields), records_count))
    }

    /// Build a [`csv::Reader`] for this [`Format`]
    fn build_reader<R: Read>(&self, reader: R) -> csv::Reader<R> {
        let mut builder = csv::ReaderBuilder::new();
        builder.has_headers(self.header);
        builder.flexible(self.truncated_rows);

        if let Some(c) = self.delimiter {
            builder.delimiter(c);
        }
        builder.escape(self.escape);
        if let Some(c) = self.quote {
            builder.quote(c);
        }
        if let Some(t) = self.terminator {
            builder.terminator(csv::Terminator::Any(t));
        }
        if let Some(comment) = self.comment {
            builder.comment(Some(comment));
        }
        builder.from_reader(reader)
    }

    /// Build a [`csv_core::Reader`] for this [`Format`]
    fn build_parser(&self) -> csv_core::Reader {
        let mut builder = csv_core::ReaderBuilder::new();
        builder.escape(self.escape);
        builder.comment(self.comment);

        if let Some(c) = self.delimiter {
            builder.delimiter(c);
        }
        if let Some(c) = self.quote {
            builder.quote(c);
        }
        if let Some(t) = self.terminator {
            builder.terminator(csv_core::Terminator::Any(t));
        }
        builder.build()
    }
}

/// Infer schema from a list of CSV files by reading through first n records
/// with `max_read_records` controlling the maximum number of records to read.
///
/// Files will be read in the given order until n records have been reached.
///
/// If `max_read_records` is not set, all files will be read fully to infer the schema.
pub fn infer_schema_from_files(
    files: &[String],
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<Schema, ArrowError> {
    let mut schemas = vec![];
    let mut records_to_read = max_read_records.unwrap_or(usize::MAX);
    let format = Format {
        delimiter: Some(delimiter),
        header: has_header,
        ..Default::default()
    };

    for fname in files.iter() {
        let f = File::open(fname)?;
        let (schema, records_read) = format.infer_schema(f, Some(records_to_read))?;
        if records_read == 0 {
            continue;
        }
        schemas.push(schema.clone());
        records_to_read -= records_read;
        if records_to_read == 0 {
            break;
        }
    }

    Schema::try_merge(schemas)
}

// optional bounds of the reader, of the form (min line, max line).
type Bounds = Option<(usize, usize)>;

/// CSV file reader using [`std::io::BufReader`]
pub type Reader<R> = BufReader<StdBufReader<R>>;

/// CSV file reader
pub struct BufReader<R> {
    /// File reader
    reader: R,

    /// The decoder
    decoder: Decoder,
}

impl<R> fmt::Debug for BufReader<R>
where
    R: BufRead,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("decoder", &self.decoder)
            .finish()
    }
}

impl<R: Read> Reader<R> {
    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.decoder.projection {
            Some(projection) => {
                let fields = self.decoder.schema.fields();
                let projected = projection.iter().map(|i| fields[*i].clone());
                Arc::new(Schema::new(projected.collect::<Fields>()))
            }
            None => self.decoder.schema.clone(),
        }
    }
}

impl<R: BufRead> BufReader<R> {
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            let buf = self.reader.fill_buf()?;
            let decoded = self.decoder.decode(buf)?;
            self.reader.consume(decoded);
            // Yield if decoded no bytes or the decoder is full
            //
            // The capacity check avoids looping around and potentially
            // blocking reading data in fill_buf that isn't needed
            // to flush the next batch
            if decoded == 0 || self.decoder.capacity() == 0 {
                break;
            }
        }

        self.decoder.flush()
    }
}

impl<R: BufRead> Iterator for BufReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for BufReader<R> {
    fn schema(&self) -> SchemaRef {
        self.decoder.schema.clone()
    }
}

/// A push-based interface for decoding CSV data from an arbitrary byte stream
///
/// See [`Reader`] for a higher-level interface for interface with [`Read`]
///
/// The push-based interface facilitates integration with sources that yield arbitrarily
/// delimited bytes ranges, such as [`BufRead`], or a chunked byte stream received from
/// object storage
///
/// ```
/// # use std::io::BufRead;
/// # use arrow_array::RecordBatch;
/// # use arrow_csv::ReaderBuilder;
/// # use arrow_schema::{ArrowError, SchemaRef};
/// #
/// fn read_from_csv<R: BufRead>(
///     mut reader: R,
///     schema: SchemaRef,
///     batch_size: usize,
/// ) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, ArrowError> {
///     let mut decoder = ReaderBuilder::new(schema)
///         .with_batch_size(batch_size)
///         .build_decoder();
///
///     let mut next = move || {
///         loop {
///             let buf = reader.fill_buf()?;
///             let decoded = decoder.decode(buf)?;
///             if decoded == 0 {
///                 break;
///             }
///
///             // Consume the number of bytes read
///             reader.consume(decoded);
///         }
///         decoder.flush()
///     };
///     Ok(std::iter::from_fn(move || next().transpose()))
/// }
/// ```
#[derive(Debug)]
pub struct Decoder {
    /// Explicit schema for the CSV file
    schema: SchemaRef,

    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,

    /// Number of records per batch
    batch_size: usize,

    /// Rows to skip
    to_skip: usize,

    /// Current line number
    line_number: usize,

    /// End line number
    end: usize,

    /// A decoder for [`StringRecords`]
    record_decoder: RecordDecoder,

    /// Check if the string matches this pattern for `NULL`.
    null_regex: NullRegex,
}

impl Decoder {
    /// Decode records from `buf` returning the number of bytes read
    ///
    /// This method returns once `batch_size` objects have been parsed since the
    /// last call to [`Self::flush`], or `buf` is exhausted. Any remaining bytes
    /// should be included in the next call to [`Self::decode`]
    ///
    /// There is no requirement that `buf` contains a whole number of records, facilitating
    /// integration with arbitrary byte streams, such as that yielded by [`BufRead`] or
    /// network sources such as object storage
    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        if self.to_skip != 0 {
            // Skip in units of `to_read` to avoid over-allocating buffers
            let to_skip = self.to_skip.min(self.batch_size);
            let (skipped, bytes) = self.record_decoder.decode(buf, to_skip)?;
            self.to_skip -= skipped;
            self.record_decoder.clear();
            return Ok(bytes);
        }

        let to_read = self.batch_size.min(self.end - self.line_number) - self.record_decoder.len();
        let (_, bytes) = self.record_decoder.decode(buf, to_read)?;
        Ok(bytes)
    }

    /// Flushes the currently buffered data to a [`RecordBatch`]
    ///
    /// This should only be called after [`Self::decode`] has returned `Ok(0)`,
    /// otherwise may return an error if part way through decoding a record
    ///
    /// Returns `Ok(None)` if no buffered data
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.record_decoder.is_empty() {
            return Ok(None);
        }

        let rows = self.record_decoder.flush()?;
        let batch = parse(
            &rows,
            self.schema.fields(),
            Some(self.schema.metadata.clone()),
            self.projection.as_ref(),
            self.line_number,
            &self.null_regex,
        )?;
        self.line_number += rows.len();
        Ok(Some(batch))
    }

    /// Returns the number of records that can be read before requiring a call to [`Self::flush`]
    pub fn capacity(&self) -> usize {
        self.batch_size - self.record_decoder.len()
    }
}

/// Parses a slice of [`StringRecords`] into a [RecordBatch]
fn parse(
    rows: &StringRecords<'_>,
    fields: &Fields,
    metadata: Option<std::collections::HashMap<String, String>>,
    projection: Option<&Vec<usize>>,
    line_number: usize,
    null_regex: &NullRegex,
) -> Result<RecordBatch, ArrowError> {
    let projection: Vec<usize> = match projection {
        Some(v) => v.clone(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };

    let arrays: Result<Vec<ArrayRef>, _> = projection
        .iter()
        .map(|i| {
            let i = *i;
            let field = &fields[i];
            match field.data_type() {
                DataType::Boolean => build_boolean_array(line_number, rows, i, null_regex),
                DataType::Decimal128(precision, scale) => build_decimal_array::<Decimal128Type>(
                    line_number,
                    rows,
                    i,
                    *precision,
                    *scale,
                    null_regex,
                ),
                DataType::Decimal256(precision, scale) => build_decimal_array::<Decimal256Type>(
                    line_number,
                    rows,
                    i,
                    *precision,
                    *scale,
                    null_regex,
                ),
                DataType::Int8 => {
                    build_primitive_array::<Int8Type>(line_number, rows, i, null_regex)
                }
                DataType::Int16 => {
                    build_primitive_array::<Int16Type>(line_number, rows, i, null_regex)
                }
                DataType::Int32 => {
                    build_primitive_array::<Int32Type>(line_number, rows, i, null_regex)
                }
                DataType::Int64 => {
                    build_primitive_array::<Int64Type>(line_number, rows, i, null_regex)
                }
                DataType::UInt8 => {
                    build_primitive_array::<UInt8Type>(line_number, rows, i, null_regex)
                }
                DataType::UInt16 => {
                    build_primitive_array::<UInt16Type>(line_number, rows, i, null_regex)
                }
                DataType::UInt32 => {
                    build_primitive_array::<UInt32Type>(line_number, rows, i, null_regex)
                }
                DataType::UInt64 => {
                    build_primitive_array::<UInt64Type>(line_number, rows, i, null_regex)
                }
                DataType::Float32 => {
                    build_primitive_array::<Float32Type>(line_number, rows, i, null_regex)
                }
                DataType::Float64 => {
                    build_primitive_array::<Float64Type>(line_number, rows, i, null_regex)
                }
                DataType::Date32 => {
                    build_primitive_array::<Date32Type>(line_number, rows, i, null_regex)
                }
                DataType::Date64 => {
                    build_primitive_array::<Date64Type>(line_number, rows, i, null_regex)
                }
                DataType::Time32(TimeUnit::Second) => {
                    build_primitive_array::<Time32SecondType>(line_number, rows, i, null_regex)
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    build_primitive_array::<Time32MillisecondType>(line_number, rows, i, null_regex)
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    build_primitive_array::<Time64MicrosecondType>(line_number, rows, i, null_regex)
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    build_primitive_array::<Time64NanosecondType>(line_number, rows, i, null_regex)
                }
                DataType::Timestamp(TimeUnit::Second, tz) => {
                    build_timestamp_array::<TimestampSecondType>(
                        line_number,
                        rows,
                        i,
                        tz.as_deref(),
                        null_regex,
                    )
                }
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    build_timestamp_array::<TimestampMillisecondType>(
                        line_number,
                        rows,
                        i,
                        tz.as_deref(),
                        null_regex,
                    )
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    build_timestamp_array::<TimestampMicrosecondType>(
                        line_number,
                        rows,
                        i,
                        tz.as_deref(),
                        null_regex,
                    )
                }
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    build_timestamp_array::<TimestampNanosecondType>(
                        line_number,
                        rows,
                        i,
                        tz.as_deref(),
                        null_regex,
                    )
                }
                DataType::Null => Ok(Arc::new({
                    let mut builder = NullBuilder::new();
                    builder.append_nulls(rows.len());
                    builder.finish()
                }) as ArrayRef),
                DataType::Utf8 => Ok(Arc::new(
                    rows.iter()
                        .map(|row| {
                            let s = row.get(i);
                            (!null_regex.is_null(s)).then_some(s)
                        })
                        .collect::<StringArray>(),
                ) as ArrayRef),
                DataType::Utf8View => Ok(Arc::new(
                    rows.iter()
                        .map(|row| {
                            let s = row.get(i);
                            (!null_regex.is_null(s)).then_some(s)
                        })
                        .collect::<StringViewArray>(),
                ) as ArrayRef),
                DataType::Dictionary(key_type, value_type)
                    if value_type.as_ref() == &DataType::Utf8 =>
                {
                    match key_type.as_ref() {
                        DataType::Int8 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<Int8Type>>(),
                        ) as ArrayRef),
                        DataType::Int16 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<Int16Type>>(),
                        ) as ArrayRef),
                        DataType::Int32 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef),
                        DataType::Int64 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<Int64Type>>(),
                        ) as ArrayRef),
                        DataType::UInt8 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<UInt8Type>>(),
                        ) as ArrayRef),
                        DataType::UInt16 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<UInt16Type>>(),
                        ) as ArrayRef),
                        DataType::UInt32 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<UInt32Type>>(),
                        ) as ArrayRef),
                        DataType::UInt64 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| {
                                    let s = row.get(i);
                                    (!null_regex.is_null(s)).then_some(s)
                                })
                                .collect::<DictionaryArray<UInt64Type>>(),
                        ) as ArrayRef),
                        _ => Err(ArrowError::ParseError(format!(
                            "Unsupported dictionary key type {key_type:?}"
                        ))),
                    }
                }
                other => Err(ArrowError::ParseError(format!(
                    "Unsupported data type {other:?}"
                ))),
            }
        })
        .collect();

    let projected_fields: Fields = projection.iter().map(|i| fields[*i].clone()).collect();

    let projected_schema = Arc::new(match metadata {
        None => Schema::new(projected_fields),
        Some(metadata) => Schema::new_with_metadata(projected_fields, metadata),
    });

    arrays.and_then(|arr| {
        RecordBatch::try_new_with_options(
            projected_schema,
            arr,
            &RecordBatchOptions::new()
                .with_match_field_names(true)
                .with_row_count(Some(rows.len())),
        )
    })
}

fn parse_bool(string: &str) -> Option<bool> {
    if string.eq_ignore_ascii_case("false") {
        Some(false)
    } else if string.eq_ignore_ascii_case("true") {
        Some(true)
    } else {
        None
    }
}

// parse the column string to an Arrow Array
fn build_decimal_array<T: DecimalType>(
    _line_number: usize,
    rows: &StringRecords<'_>,
    col_idx: usize,
    precision: u8,
    scale: i8,
    null_regex: &NullRegex,
) -> Result<ArrayRef, ArrowError> {
    let mut decimal_builder = PrimitiveBuilder::<T>::with_capacity(rows.len());
    for row in rows.iter() {
        let s = row.get(col_idx);
        if null_regex.is_null(s) {
            // append null
            decimal_builder.append_null();
        } else {
            let decimal_value: Result<T::Native, _> = parse_decimal::<T>(s, precision, scale);
            match decimal_value {
                Ok(v) => {
                    decimal_builder.append_value(v);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }
    Ok(Arc::new(
        decimal_builder
            .finish()
            .with_precision_and_scale(precision, scale)?,
    ))
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_primitive_array<T: ArrowPrimitiveType + Parser>(
    line_number: usize,
    rows: &StringRecords<'_>,
    col_idx: usize,
    null_regex: &NullRegex,
) -> Result<ArrayRef, ArrowError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            let s = row.get(col_idx);
            if null_regex.is_null(s) {
                return Ok(None);
            }

            match T::parse(s) {
                Some(e) => Ok(Some(e)),
                None => Err(ArrowError::ParseError(format!(
                    // TODO: we should surface the underlying error here.
                    "Error while parsing value {} for column {} at line {}",
                    s,
                    col_idx,
                    line_number + row_index
                ))),
            }
        })
        .collect::<Result<PrimitiveArray<T>, ArrowError>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

fn build_timestamp_array<T: ArrowTimestampType>(
    line_number: usize,
    rows: &StringRecords<'_>,
    col_idx: usize,
    timezone: Option<&str>,
    null_regex: &NullRegex,
) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(match timezone {
        Some(timezone) => {
            let tz: Tz = timezone.parse()?;
            build_timestamp_array_impl::<T, _>(line_number, rows, col_idx, &tz, null_regex)?
                .with_timezone(timezone)
        }
        None => build_timestamp_array_impl::<T, _>(line_number, rows, col_idx, &Utc, null_regex)?,
    }))
}

fn build_timestamp_array_impl<T: ArrowTimestampType, Tz: TimeZone>(
    line_number: usize,
    rows: &StringRecords<'_>,
    col_idx: usize,
    timezone: &Tz,
    null_regex: &NullRegex,
) -> Result<PrimitiveArray<T>, ArrowError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            let s = row.get(col_idx);
            if null_regex.is_null(s) {
                return Ok(None);
            }

            let date = string_to_datetime(timezone, s)
                .and_then(|date| match T::UNIT {
                    TimeUnit::Second => Ok(date.timestamp()),
                    TimeUnit::Millisecond => Ok(date.timestamp_millis()),
                    TimeUnit::Microsecond => Ok(date.timestamp_micros()),
                    TimeUnit::Nanosecond => date.timestamp_nanos_opt().ok_or_else(|| {
                        ArrowError::ParseError(format!(
                            "{} would overflow 64-bit signed nanoseconds",
                            date.to_rfc3339(),
                        ))
                    }),
                })
                .map_err(|e| {
                    ArrowError::ParseError(format!(
                        "Error parsing column {col_idx} at line {}: {}",
                        line_number + row_index,
                        e
                    ))
                })?;
            Ok(Some(date))
        })
        .collect()
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_boolean_array(
    line_number: usize,
    rows: &StringRecords<'_>,
    col_idx: usize,
    null_regex: &NullRegex,
) -> Result<ArrayRef, ArrowError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            let s = row.get(col_idx);
            if null_regex.is_null(s) {
                return Ok(None);
            }
            let parsed = parse_bool(s);
            match parsed {
                Some(e) => Ok(Some(e)),
                None => Err(ArrowError::ParseError(format!(
                    // TODO: we should surface the underlying error here.
                    "Error while parsing value {} for column {} at line {}",
                    s,
                    col_idx,
                    line_number + row_index
                ))),
            }
        })
        .collect::<Result<BooleanArray, _>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

/// CSV file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Schema of the CSV file
    schema: SchemaRef,
    /// Format of the CSV file
    format: Format,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// The bounds over which to scan the reader. `None` starts from 0 and runs until EOF.
    bounds: Bounds,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
}

impl ReaderBuilder {
    /// Create a new builder for configuring CSV parsing options.
    ///
    /// To convert a builder into a reader, call `ReaderBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_csv::{Reader, ReaderBuilder};
    /// # use std::fs::File;
    /// # use std::io::Seek;
    /// # use std::sync::Arc;
    /// # use arrow_csv::reader::Format;
    /// #
    /// let mut file = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    /// // Infer the schema with the first 100 records
    /// let (schema, _) = Format::default().infer_schema(&mut file, Some(100)).unwrap();
    /// file.rewind().unwrap();
    ///
    /// // create a builder
    /// ReaderBuilder::new(Arc::new(schema)).build(file).unwrap();
    /// ```
    pub fn new(schema: SchemaRef) -> ReaderBuilder {
        Self {
            schema,
            format: Format::default(),
            batch_size: 1024,
            bounds: None,
            projection: None,
        }
    }

    /// Set whether the CSV file has a header
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.format.header = has_header;
        self
    }

    /// Overrides the [Format] of this [ReaderBuilder]
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.format.delimiter = Some(delimiter);
        self
    }

    /// Set the given character as the CSV file's escape character
    pub fn with_escape(mut self, escape: u8) -> Self {
        self.format.escape = Some(escape);
        self
    }

    /// Set the given character as the CSV file's quote character, by default it is double quote
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.format.quote = Some(quote);
        self
    }

    /// Provide a custom terminator character, defaults to CRLF
    pub fn with_terminator(mut self, terminator: u8) -> Self {
        self.format.terminator = Some(terminator);
        self
    }

    /// Provide a comment character, lines starting with this character will be ignored
    pub fn with_comment(mut self, comment: u8) -> Self {
        self.format.comment = Some(comment);
        self
    }

    /// Provide a regex to match null values, defaults to `^$`
    pub fn with_null_regex(mut self, null_regex: Regex) -> Self {
        self.format.null_regex = NullRegex(Some(null_regex));
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the bounds over which to scan the reader.
    /// `start` and `end` are line numbers.
    pub fn with_bounds(mut self, start: usize, end: usize) -> Self {
        self.bounds = Some((start, end));
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Whether to allow truncated rows when parsing.
    ///
    /// By default this is set to `false` and will error if the CSV rows have different lengths.
    /// When set to true then it will allow records with less than the expected number of columns
    /// and fill the missing columns with nulls. If the record's schema is not nullable, then it
    /// will still return an error.
    pub fn with_truncated_rows(mut self, allow: bool) -> Self {
        self.format.truncated_rows = allow;
        self
    }

    /// Create a new `Reader` from a non-buffered reader
    ///
    /// If `R: BufRead` consider using [`Self::build_buffered`] to avoid unnecessary additional
    /// buffering, as internally this method wraps `reader` in [`std::io::BufReader`]
    pub fn build<R: Read>(self, reader: R) -> Result<Reader<R>, ArrowError> {
        self.build_buffered(StdBufReader::new(reader))
    }

    /// Create a new `BufReader` from a buffered reader
    pub fn build_buffered<R: BufRead>(self, reader: R) -> Result<BufReader<R>, ArrowError> {
        Ok(BufReader {
            reader,
            decoder: self.build_decoder(),
        })
    }

    /// Builds a decoder that can be used to decode CSV from an arbitrary byte stream
    pub fn build_decoder(self) -> Decoder {
        let delimiter = self.format.build_parser();
        let record_decoder = RecordDecoder::new(
            delimiter,
            self.schema.fields().len(),
            self.format.truncated_rows,
        );

        let header = self.format.header as usize;

        let (start, end) = match self.bounds {
            Some((start, end)) => (start + header, end + header),
            None => (header, usize::MAX),
        };

        Decoder {
            schema: self.schema,
            to_skip: start,
            record_decoder,
            line_number: start,
            end,
            projection: self.projection,
            batch_size: self.batch_size,
            null_regex: self.format.null_regex,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Cursor, Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    use arrow_array::cast::AsArray;

    #[test]
    fn test_csv() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]));

        let file = File::open("test/data/uk_cities.csv").unwrap();
        let mut csv = ReaderBuilder::new(schema.clone()).build(file).unwrap();
        assert_eq!(schema, csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch.column(1).as_primitive::<Float64Type>();
        assert_eq!(57.653484, lat.value(0));

        // access data from a string array (ListArray<u8>)
        let city = batch.column(0).as_string::<i32>();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }

    #[test]
    fn test_csv_schema_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("foo".to_owned(), "bar".to_owned());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("lat", DataType::Float64, false),
                Field::new("lng", DataType::Float64, false),
            ],
            metadata.clone(),
        ));

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = ReaderBuilder::new(schema.clone()).build(file).unwrap();
        assert_eq!(schema, csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        assert_eq!(&metadata, batch.schema().metadata());
    }

    #[test]
    fn test_csv_reader_with_decimal() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Decimal128(38, 6), false),
            Field::new("lng", DataType::Decimal256(76, 6), false),
        ]));

        let file = File::open("test/data/decimal_test.csv").unwrap();

        let mut csv = ReaderBuilder::new(schema).build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();
        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert_eq!("57.653484", lat.value_as_string(0));
        assert_eq!("53.002666", lat.value_as_string(1));
        assert_eq!("52.412811", lat.value_as_string(2));
        assert_eq!("51.481583", lat.value_as_string(3));
        assert_eq!("12.123456", lat.value_as_string(4));
        assert_eq!("50.760000", lat.value_as_string(5));
        assert_eq!("0.123000", lat.value_as_string(6));
        assert_eq!("123.000000", lat.value_as_string(7));
        assert_eq!("123.000000", lat.value_as_string(8));
        assert_eq!("-50.760000", lat.value_as_string(9));

        let lng = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        assert_eq!("-3.335724", lng.value_as_string(0));
        assert_eq!("-2.179404", lng.value_as_string(1));
        assert_eq!("-1.778197", lng.value_as_string(2));
        assert_eq!("-3.179090", lng.value_as_string(3));
        assert_eq!("-3.179090", lng.value_as_string(4));
        assert_eq!("0.290472", lng.value_as_string(5));
        assert_eq!("0.290472", lng.value_as_string(6));
        assert_eq!("0.290472", lng.value_as_string(7));
        assert_eq!("0.290472", lng.value_as_string(8));
        assert_eq!("0.290472", lng.value_as_string(9));
    }

    #[test]
    fn test_csv_from_buf_reader() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file_with_headers = File::open("test/data/uk_cities_with_headers.csv").unwrap();
        let file_without_headers = File::open("test/data/uk_cities.csv").unwrap();
        let both_files = file_with_headers
            .chain(Cursor::new("\n".to_string()))
            .chain(file_without_headers);
        let mut csv = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(both_files)
            .unwrap();
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(74, batch.num_rows());
        assert_eq!(3, batch.num_columns());
    }

    #[test]
    fn test_csv_with_schema_inference() {
        let mut file = File::open("test/data/uk_cities_with_headers.csv").unwrap();

        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(&mut file, None)
            .unwrap();

        file.rewind().unwrap();
        let builder = ReaderBuilder::new(Arc::new(schema)).with_header(true);

        let mut csv = builder.build(file).unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("lat", DataType::Float64, true),
            Field::new("lng", DataType::Float64, true),
        ]);
        assert_eq!(Arc::new(expected_schema), csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(57.653484, lat.value(0));

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }

    #[test]
    fn test_csv_with_schema_inference_no_headers() {
        let mut file = File::open("test/data/uk_cities.csv").unwrap();

        let (schema, _) = Format::default().infer_schema(&mut file, None).unwrap();
        file.rewind().unwrap();

        let mut csv = ReaderBuilder::new(Arc::new(schema)).build(file).unwrap();

        // csv field names should be 'column_{number}'
        let schema = csv.schema();
        assert_eq!("column_1", schema.field(0).name());
        assert_eq!("column_2", schema.field(1).name());
        assert_eq!("column_3", schema.field(2).name());
        let batch = csv.next().unwrap().unwrap();
        let batch_schema = batch.schema();

        assert_eq!(schema, batch_schema);
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(57.653484, lat.value(0));

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }

    #[test]
    fn test_csv_builder_with_bounds() {
        let mut file = File::open("test/data/uk_cities.csv").unwrap();

        // Set the bounds to the lines 0, 1 and 2.
        let (schema, _) = Format::default().infer_schema(&mut file, None).unwrap();
        file.rewind().unwrap();
        let mut csv = ReaderBuilder::new(Arc::new(schema))
            .with_bounds(0, 2)
            .build(file)
            .unwrap();
        let batch = csv.next().unwrap().unwrap();

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // The value on line 0 is within the bounds
        assert_eq!("Elgin, Scotland, the UK", city.value(0));

        // The value on line 13 is outside of the bounds. Therefore
        // the call to .value() will panic.
        let result = std::panic::catch_unwind(|| city.value(13));
        assert!(result.is_err());
    }

    #[test]
    fn test_csv_with_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]));

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = ReaderBuilder::new(schema)
            .with_projection(vec![0, 1])
            .build(file)
            .unwrap();

        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
        ]));
        assert_eq!(projected_schema, csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(projected_schema, batch.schema());
        assert_eq!(37, batch.num_rows());
        assert_eq!(2, batch.num_columns());
    }

    #[test]
    fn test_csv_with_dictionary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new_dictionary("city", DataType::Int32, DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]));

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = ReaderBuilder::new(schema)
            .with_projection(vec![0, 1])
            .build(file)
            .unwrap();

        let projected_schema = Arc::new(Schema::new(vec![
            Field::new_dictionary("city", DataType::Int32, DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
        ]));
        assert_eq!(projected_schema, csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(projected_schema, batch.schema());
        assert_eq!(37, batch.num_rows());
        assert_eq!(2, batch.num_columns());

        let strings = arrow_cast::cast(batch.column(0), &DataType::Utf8).unwrap();
        let strings = strings.as_string::<i32>();

        assert_eq!(strings.value(0), "Elgin, Scotland, the UK");
        assert_eq!(strings.value(4), "Eastbourne, East Sussex, UK");
        assert_eq!(strings.value(29), "Uckfield, East Sussex, UK");
    }

    #[test]
    fn test_csv_with_nullable_dictionary() {
        let offset_type = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        for data_type in offset_type {
            let file = File::open("test/data/dictionary_nullable_test.csv").unwrap();
            let dictionary_type =
                DataType::Dictionary(Box::new(data_type), Box::new(DataType::Utf8));
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", dictionary_type.clone(), true),
            ]));

            let mut csv = ReaderBuilder::new(schema)
                .build(file.try_clone().unwrap())
                .unwrap();

            let batch = csv.next().unwrap().unwrap();
            assert_eq!(3, batch.num_rows());
            assert_eq!(2, batch.num_columns());

            let names = arrow_cast::cast(batch.column(1), &dictionary_type).unwrap();
            assert!(!names.is_null(2));
            assert!(names.is_null(1));
        }
    }
    #[test]
    fn test_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, true),
            Field::new("c_string", DataType::Utf8, true),
            Field::new("c_bool", DataType::Boolean, false),
        ]));

        let file = File::open("test/data/null_test.csv").unwrap();

        let mut csv = ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)
            .unwrap();

        let batch = csv.next().unwrap().unwrap();

        assert!(!batch.column(1).is_null(0));
        assert!(!batch.column(1).is_null(1));
        assert!(batch.column(1).is_null(2));
        assert!(!batch.column(1).is_null(3));
        assert!(!batch.column(1).is_null(4));
    }

    #[test]
    fn test_init_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_int", DataType::UInt64, true),
            Field::new("c_float", DataType::Float32, true),
            Field::new("c_string", DataType::Utf8, true),
            Field::new("c_bool", DataType::Boolean, true),
            Field::new("c_null", DataType::Null, true),
        ]));
        let file = File::open("test/data/init_null_test.csv").unwrap();

        let mut csv = ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)
            .unwrap();

        let batch = csv.next().unwrap().unwrap();

        assert!(batch.column(1).is_null(0));
        assert!(!batch.column(1).is_null(1));
        assert!(batch.column(1).is_null(2));
        assert!(!batch.column(1).is_null(3));
        assert!(!batch.column(1).is_null(4));
    }

    #[test]
    fn test_init_nulls_with_inference() {
        let format = Format::default().with_header(true).with_delimiter(b',');

        let mut file = File::open("test/data/init_null_test.csv").unwrap();
        let (schema, _) = format.infer_schema(&mut file, None).unwrap();
        file.rewind().unwrap();

        let expected_schema = Schema::new(vec![
            Field::new("c_int", DataType::Int64, true),
            Field::new("c_float", DataType::Float64, true),
            Field::new("c_string", DataType::Utf8, true),
            Field::new("c_bool", DataType::Boolean, true),
            Field::new("c_null", DataType::Null, true),
        ]);
        assert_eq!(schema, expected_schema);

        let mut csv = ReaderBuilder::new(Arc::new(schema))
            .with_format(format)
            .build(file)
            .unwrap();

        let batch = csv.next().unwrap().unwrap();

        assert!(batch.column(1).is_null(0));
        assert!(!batch.column(1).is_null(1));
        assert!(batch.column(1).is_null(2));
        assert!(!batch.column(1).is_null(3));
        assert!(!batch.column(1).is_null(4));
    }

    #[test]
    fn test_custom_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_int", DataType::UInt64, true),
            Field::new("c_float", DataType::Float32, true),
            Field::new("c_string", DataType::Utf8, true),
            Field::new("c_bool", DataType::Boolean, true),
        ]));

        let file = File::open("test/data/custom_null_test.csv").unwrap();

        let null_regex = Regex::new("^nil$").unwrap();

        let mut csv = ReaderBuilder::new(schema)
            .with_header(true)
            .with_null_regex(null_regex)
            .build(file)
            .unwrap();

        let batch = csv.next().unwrap().unwrap();

        // "nil"s should be NULL
        assert!(batch.column(0).is_null(1));
        assert!(batch.column(1).is_null(2));
        assert!(batch.column(3).is_null(4));
        assert!(batch.column(2).is_null(3));
        assert!(!batch.column(2).is_null(4));
    }

    #[test]
    fn test_nulls_with_inference() {
        let mut file = File::open("test/data/various_types.csv").unwrap();
        let format = Format::default().with_header(true).with_delimiter(b'|');

        let (schema, _) = format.infer_schema(&mut file, None).unwrap();
        file.rewind().unwrap();

        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_format(format)
            .with_batch_size(512)
            .with_projection(vec![0, 1, 2, 3, 4, 5]);

        let mut csv = builder.build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();

        assert_eq!(10, batch.num_rows());
        assert_eq!(6, batch.num_columns());

        let schema = batch.schema();

        assert_eq!(&DataType::Int64, schema.field(0).data_type());
        assert_eq!(&DataType::Float64, schema.field(1).data_type());
        assert_eq!(&DataType::Float64, schema.field(2).data_type());
        assert_eq!(&DataType::Boolean, schema.field(3).data_type());
        assert_eq!(&DataType::Date32, schema.field(4).data_type());
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Second, None),
            schema.field(5).data_type()
        );

        let names: Vec<&str> = schema.fields().iter().map(|x| x.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "c_int",
                "c_float",
                "c_string",
                "c_bool",
                "c_date",
                "c_datetime"
            ]
        );

        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
        assert!(schema.field(3).is_nullable());
        assert!(schema.field(4).is_nullable());
        assert!(schema.field(5).is_nullable());

        assert!(!batch.column(1).is_null(0));
        assert!(!batch.column(1).is_null(1));
        assert!(batch.column(1).is_null(2));
        assert!(!batch.column(1).is_null(3));
        assert!(!batch.column(1).is_null(4));
    }

    #[test]
    fn test_custom_nulls_with_inference() {
        let mut file = File::open("test/data/custom_null_test.csv").unwrap();

        let null_regex = Regex::new("^nil$").unwrap();

        let format = Format::default()
            .with_header(true)
            .with_null_regex(null_regex);

        let (schema, _) = format.infer_schema(&mut file, None).unwrap();
        file.rewind().unwrap();

        let expected_schema = Schema::new(vec![
            Field::new("c_int", DataType::Int64, true),
            Field::new("c_float", DataType::Float64, true),
            Field::new("c_string", DataType::Utf8, true),
            Field::new("c_bool", DataType::Boolean, true),
        ]);

        assert_eq!(schema, expected_schema);

        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_format(format)
            .with_batch_size(512)
            .with_projection(vec![0, 1, 2, 3]);

        let mut csv = builder.build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();

        assert_eq!(5, batch.num_rows());
        assert_eq!(4, batch.num_columns());

        assert_eq!(batch.schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_scientific_notation_with_inference() {
        let mut file = File::open("test/data/scientific_notation_test.csv").unwrap();
        let format = Format::default().with_header(false).with_delimiter(b',');

        let (schema, _) = format.infer_schema(&mut file, None).unwrap();
        file.rewind().unwrap();

        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_format(format)
            .with_batch_size(512)
            .with_projection(vec![0, 1]);

        let mut csv = builder.build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();

        let schema = batch.schema();

        assert_eq!(&DataType::Float64, schema.field(0).data_type());
    }

    #[test]
    fn test_parse_invalid_csv() {
        let file = File::open("test/data/various_types_invalid.csv").unwrap();

        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, false),
            Field::new("c_string", DataType::Utf8, false),
            Field::new("c_bool", DataType::Boolean, false),
        ]);

        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .with_delimiter(b'|')
            .with_batch_size(512)
            .with_projection(vec![0, 1, 2, 3]);

        let mut csv = builder.build(file).unwrap();
        match csv.next() {
            Some(e) => match e {
                Err(e) => assert_eq!(
                    "ParseError(\"Error while parsing value 4.x4 for column 1 at line 4\")",
                    format!("{e:?}")
                ),
                Ok(_) => panic!("should have failed"),
            },
            None => panic!("should have failed"),
        }
    }

    /// Infer the data type of a record
    fn infer_field_schema(string: &str) -> DataType {
        let mut v = InferredDataType::default();
        v.update(string);
        v.get()
    }

    #[test]
    fn test_infer_field_schema() {
        assert_eq!(infer_field_schema("A"), DataType::Utf8);
        assert_eq!(infer_field_schema("\"123\""), DataType::Utf8);
        assert_eq!(infer_field_schema("10"), DataType::Int64);
        assert_eq!(infer_field_schema("10.2"), DataType::Float64);
        assert_eq!(infer_field_schema(".2"), DataType::Float64);
        assert_eq!(infer_field_schema("2."), DataType::Float64);
        assert_eq!(infer_field_schema("NaN"), DataType::Float64);
        assert_eq!(infer_field_schema("nan"), DataType::Float64);
        assert_eq!(infer_field_schema("inf"), DataType::Float64);
        assert_eq!(infer_field_schema("-inf"), DataType::Float64);
        assert_eq!(infer_field_schema("true"), DataType::Boolean);
        assert_eq!(infer_field_schema("trUe"), DataType::Boolean);
        assert_eq!(infer_field_schema("false"), DataType::Boolean);
        assert_eq!(infer_field_schema("2020-11-08"), DataType::Date32);
        assert_eq!(
            infer_field_schema("2020-11-08T14:20:01"),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            infer_field_schema("2020-11-08 14:20:01"),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            infer_field_schema("2020-11-08 14:20:01"),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(infer_field_schema("-5.13"), DataType::Float64);
        assert_eq!(infer_field_schema("0.1300"), DataType::Float64);
        assert_eq!(
            infer_field_schema("2021-12-19 13:12:30.921"),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            infer_field_schema("2021-12-19T13:12:30.123456789"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(infer_field_schema("–9223372036854775809"), DataType::Utf8);
        assert_eq!(infer_field_schema("9223372036854775808"), DataType::Utf8);
    }

    #[test]
    fn parse_date32() {
        assert_eq!(Date32Type::parse("1970-01-01").unwrap(), 0);
        assert_eq!(Date32Type::parse("2020-03-15").unwrap(), 18336);
        assert_eq!(Date32Type::parse("1945-05-08").unwrap(), -9004);
    }

    #[test]
    fn parse_time() {
        assert_eq!(
            Time64NanosecondType::parse("12:10:01.123456789 AM"),
            Some(601_123_456_789)
        );
        assert_eq!(
            Time64MicrosecondType::parse("12:10:01.123456 am"),
            Some(601_123_456)
        );
        assert_eq!(
            Time32MillisecondType::parse("2:10:01.12 PM"),
            Some(51_001_120)
        );
        assert_eq!(Time32SecondType::parse("2:10:01 pm"), Some(51_001));
    }

    #[test]
    fn parse_date64() {
        assert_eq!(Date64Type::parse("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(
            Date64Type::parse("2018-11-13T17:11:10").unwrap(),
            1542129070000
        );
        assert_eq!(
            Date64Type::parse("2018-11-13T17:11:10.011").unwrap(),
            1542129070011
        );
        assert_eq!(
            Date64Type::parse("1900-02-28T12:34:56").unwrap(),
            -2203932304000
        );
        assert_eq!(
            Date64Type::parse_formatted("1900-02-28 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap(),
            -2203932304000
        );
        assert_eq!(
            Date64Type::parse_formatted("1900-02-28 12:34:56+0030", "%Y-%m-%d %H:%M:%S%z").unwrap(),
            -2203932304000 - (30 * 60 * 1000)
        );
    }

    fn test_parse_timestamp_impl<T: ArrowTimestampType>(
        timezone: Option<Arc<str>>,
        expected: &[i64],
    ) {
        let csv = [
            "1970-01-01T00:00:00",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00+02:00",
        ]
        .join("\n");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "field",
            DataType::Timestamp(T::UNIT, timezone.clone()),
            true,
        )]));

        let mut decoder = ReaderBuilder::new(schema).build_decoder();

        let decoded = decoder.decode(csv.as_bytes()).unwrap();
        assert_eq!(decoded, csv.len());
        decoder.decode(&[]).unwrap();

        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
        let col = batch.column(0).as_primitive::<T>();
        assert_eq!(col.values(), expected);
        assert_eq!(col.data_type(), &DataType::Timestamp(T::UNIT, timezone));
    }

    #[test]
    fn test_parse_timestamp() {
        test_parse_timestamp_impl::<TimestampNanosecondType>(None, &[0, 0, -7_200_000_000_000]);
        test_parse_timestamp_impl::<TimestampNanosecondType>(
            Some("+00:00".into()),
            &[0, 0, -7_200_000_000_000],
        );
        test_parse_timestamp_impl::<TimestampNanosecondType>(
            Some("-05:00".into()),
            &[18_000_000_000_000, 0, -7_200_000_000_000],
        );
        test_parse_timestamp_impl::<TimestampMicrosecondType>(
            Some("-03".into()),
            &[10_800_000_000, 0, -7_200_000_000],
        );
        test_parse_timestamp_impl::<TimestampMillisecondType>(
            Some("-03".into()),
            &[10_800_000, 0, -7_200_000],
        );
        test_parse_timestamp_impl::<TimestampSecondType>(Some("-03".into()), &[10_800, 0, -7_200]);
    }

    #[test]
    fn test_infer_schema_from_multiple_files() {
        let mut csv1 = NamedTempFile::new().unwrap();
        let mut csv2 = NamedTempFile::new().unwrap();
        let csv3 = NamedTempFile::new().unwrap(); // empty csv file should be skipped
        let mut csv4 = NamedTempFile::new().unwrap();
        writeln!(csv1, "c1,c2,c3").unwrap();
        writeln!(csv1, "1,\"foo\",0.5").unwrap();
        writeln!(csv1, "3,\"bar\",1").unwrap();
        writeln!(csv1, "3,\"bar\",2e-06").unwrap();
        // reading csv2 will set c2 to optional
        writeln!(csv2, "c1,c2,c3,c4").unwrap();
        writeln!(csv2, "10,,3.14,true").unwrap();
        // reading csv4 will set c3 to optional
        writeln!(csv4, "c1,c2,c3").unwrap();
        writeln!(csv4, "10,\"foo\",").unwrap();

        let schema = infer_schema_from_files(
            &[
                csv3.path().to_str().unwrap().to_string(),
                csv1.path().to_str().unwrap().to_string(),
                csv2.path().to_str().unwrap().to_string(),
                csv4.path().to_str().unwrap().to_string(),
            ],
            b',',
            Some(4), // only csv1 and csv2 should be read
            true,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
        assert!(schema.field(3).is_nullable());

        assert_eq!(&DataType::Int64, schema.field(0).data_type());
        assert_eq!(&DataType::Utf8, schema.field(1).data_type());
        assert_eq!(&DataType::Float64, schema.field(2).data_type());
        assert_eq!(&DataType::Boolean, schema.field(3).data_type());
    }

    #[test]
    fn test_bounded() {
        let schema = Schema::new(vec![Field::new("int", DataType::UInt32, false)]);
        let data = [
            vec!["0"],
            vec!["1"],
            vec!["2"],
            vec!["3"],
            vec!["4"],
            vec!["5"],
            vec!["6"],
        ];

        let data = data
            .iter()
            .map(|x| x.join(","))
            .collect::<Vec<_>>()
            .join("\n");
        let data = data.as_bytes();

        let reader = std::io::Cursor::new(data);

        let mut csv = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(2)
            .with_projection(vec![0])
            .with_bounds(2, 6)
            .build_buffered(reader)
            .unwrap();

        let batch = csv.next().unwrap().unwrap();
        let a = batch.column(0);
        let a = a.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(a, &UInt32Array::from(vec![2, 3]));

        let batch = csv.next().unwrap().unwrap();
        let a = batch.column(0);
        let a = a.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(a, &UInt32Array::from(vec![4, 5]));

        assert!(csv.next().is_none());
    }

    #[test]
    fn test_empty_projection() {
        let schema = Schema::new(vec![Field::new("int", DataType::UInt32, false)]);
        let data = [vec!["0"], vec!["1"]];

        let data = data
            .iter()
            .map(|x| x.join(","))
            .collect::<Vec<_>>()
            .join("\n");

        let mut csv = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(2)
            .with_projection(vec![])
            .build_buffered(Cursor::new(data.as_bytes()))
            .unwrap();

        let batch = csv.next().unwrap().unwrap();
        assert_eq!(batch.columns().len(), 0);
        assert_eq!(batch.num_rows(), 2);

        assert!(csv.next().is_none());
    }

    #[test]
    fn test_parsing_bool() {
        // Encode the expected behavior of boolean parsing
        assert_eq!(Some(true), parse_bool("true"));
        assert_eq!(Some(true), parse_bool("tRUe"));
        assert_eq!(Some(true), parse_bool("True"));
        assert_eq!(Some(true), parse_bool("TRUE"));
        assert_eq!(None, parse_bool("t"));
        assert_eq!(None, parse_bool("T"));
        assert_eq!(None, parse_bool(""));

        assert_eq!(Some(false), parse_bool("false"));
        assert_eq!(Some(false), parse_bool("fALse"));
        assert_eq!(Some(false), parse_bool("False"));
        assert_eq!(Some(false), parse_bool("FALSE"));
        assert_eq!(None, parse_bool("f"));
        assert_eq!(None, parse_bool("F"));
        assert_eq!(None, parse_bool(""));
    }

    #[test]
    fn test_parsing_float() {
        assert_eq!(Some(12.34), Float64Type::parse("12.34"));
        assert_eq!(Some(-12.34), Float64Type::parse("-12.34"));
        assert_eq!(Some(12.0), Float64Type::parse("12"));
        assert_eq!(Some(0.0), Float64Type::parse("0"));
        assert_eq!(Some(2.0), Float64Type::parse("2."));
        assert_eq!(Some(0.2), Float64Type::parse(".2"));
        assert!(Float64Type::parse("nan").unwrap().is_nan());
        assert!(Float64Type::parse("NaN").unwrap().is_nan());
        assert!(Float64Type::parse("inf").unwrap().is_infinite());
        assert!(Float64Type::parse("inf").unwrap().is_sign_positive());
        assert!(Float64Type::parse("-inf").unwrap().is_infinite());
        assert!(Float64Type::parse("-inf").unwrap().is_sign_negative());
        assert_eq!(None, Float64Type::parse(""));
        assert_eq!(None, Float64Type::parse("dd"));
        assert_eq!(None, Float64Type::parse("12.34.56"));
    }

    #[test]
    fn test_non_std_quote() {
        let schema = Schema::new(vec![
            Field::new("text1", DataType::Utf8, false),
            Field::new("text2", DataType::Utf8, false),
        ]);
        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_header(false)
            .with_quote(b'~'); // default is ", change to ~

        let mut csv_text = Vec::new();
        let mut csv_writer = std::io::Cursor::new(&mut csv_text);
        for index in 0..10 {
            let text1 = format!("id{index:}");
            let text2 = format!("value{index:}");
            csv_writer
                .write_fmt(format_args!("~{text1}~,~{text2}~\r\n"))
                .unwrap();
        }
        let mut csv_reader = std::io::Cursor::new(&csv_text);
        let mut reader = builder.build(&mut csv_reader).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let col0 = batch.column(0);
        assert_eq!(col0.len(), 10);
        let col0_arr = col0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col0_arr.value(0), "id0");
        let col1 = batch.column(1);
        assert_eq!(col1.len(), 10);
        let col1_arr = col1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col1_arr.value(5), "value5");
    }

    #[test]
    fn test_non_std_escape() {
        let schema = Schema::new(vec![
            Field::new("text1", DataType::Utf8, false),
            Field::new("text2", DataType::Utf8, false),
        ]);
        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_header(false)
            .with_escape(b'\\'); // default is None, change to \

        let mut csv_text = Vec::new();
        let mut csv_writer = std::io::Cursor::new(&mut csv_text);
        for index in 0..10 {
            let text1 = format!("id{index:}");
            let text2 = format!("value\\\"{index:}");
            csv_writer
                .write_fmt(format_args!("\"{text1}\",\"{text2}\"\r\n"))
                .unwrap();
        }
        let mut csv_reader = std::io::Cursor::new(&csv_text);
        let mut reader = builder.build(&mut csv_reader).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let col0 = batch.column(0);
        assert_eq!(col0.len(), 10);
        let col0_arr = col0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col0_arr.value(0), "id0");
        let col1 = batch.column(1);
        assert_eq!(col1.len(), 10);
        let col1_arr = col1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col1_arr.value(5), "value\"5");
    }

    #[test]
    fn test_non_std_terminator() {
        let schema = Schema::new(vec![
            Field::new("text1", DataType::Utf8, false),
            Field::new("text2", DataType::Utf8, false),
        ]);
        let builder = ReaderBuilder::new(Arc::new(schema))
            .with_header(false)
            .with_terminator(b'\n'); // default is CRLF, change to LF

        let mut csv_text = Vec::new();
        let mut csv_writer = std::io::Cursor::new(&mut csv_text);
        for index in 0..10 {
            let text1 = format!("id{index:}");
            let text2 = format!("value{index:}");
            csv_writer
                .write_fmt(format_args!("\"{text1}\",\"{text2}\"\n"))
                .unwrap();
        }
        let mut csv_reader = std::io::Cursor::new(&csv_text);
        let mut reader = builder.build(&mut csv_reader).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let col0 = batch.column(0);
        assert_eq!(col0.len(), 10);
        let col0_arr = col0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col0_arr.value(0), "id0");
        let col1 = batch.column(1);
        assert_eq!(col1.len(), 10);
        let col1_arr = col1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col1_arr.value(5), "value5");
    }

    #[test]
    fn test_header_bounds() {
        let csv = "a,b\na,b\na,b\na,b\na,b\n";
        let tests = [
            (None, false, 5),
            (None, true, 4),
            (Some((0, 4)), false, 4),
            (Some((1, 4)), false, 3),
            (Some((0, 4)), true, 4),
            (Some((1, 4)), true, 3),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("a", DataType::Utf8, false),
        ]));

        for (idx, (bounds, has_header, expected)) in tests.into_iter().enumerate() {
            let mut reader = ReaderBuilder::new(schema.clone()).with_header(has_header);
            if let Some((start, end)) = bounds {
                reader = reader.with_bounds(start, end);
            }
            let b = reader
                .build_buffered(Cursor::new(csv.as_bytes()))
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(b.num_rows(), expected, "{idx}");
        }
    }

    #[test]
    fn test_null_boolean() {
        let csv = "true,false\nFalse,True\n,True\nFalse,";
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("a", DataType::Boolean, true),
        ]));

        let b = ReaderBuilder::new(schema)
            .build_buffered(Cursor::new(csv.as_bytes()))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(b.num_rows(), 4);
        assert_eq!(b.num_columns(), 2);

        let c = b.column(0).as_boolean();
        assert_eq!(c.null_count(), 1);
        assert!(c.value(0));
        assert!(!c.value(1));
        assert!(c.is_null(2));
        assert!(!c.value(3));

        let c = b.column(1).as_boolean();
        assert_eq!(c.null_count(), 1);
        assert!(!c.value(0));
        assert!(c.value(1));
        assert!(c.value(2));
        assert!(c.is_null(3));
    }

    #[test]
    fn test_truncated_rows() {
        let data = "a,b,c\n1,2,3\n4,5\n\n6,7,8";
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));

        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_truncated_rows(true)
            .build(Cursor::new(data))
            .unwrap();

        let batches = reader.collect::<Result<Vec<_>, _>>();
        assert!(batches.is_ok());
        let batch = batches.unwrap().into_iter().next().unwrap();
        // Empty rows are skipped by the underlying csv parser
        assert_eq!(batch.num_rows(), 3);

        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_truncated_rows(false)
            .build(Cursor::new(data))
            .unwrap();

        let batches = reader.collect::<Result<Vec<_>, _>>();
        assert!(match batches {
            Err(ArrowError::CsvError(e)) => e.to_string().contains("incorrect number of fields"),
            _ => false,
        });
    }

    #[test]
    fn test_truncated_rows_csv() {
        let file = File::open("test/data/truncated_rows.csv").unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("Name", DataType::Utf8, true),
            Field::new("Age", DataType::UInt32, true),
            Field::new("Occupation", DataType::Utf8, true),
            Field::new("DOB", DataType::Date32, true),
        ]));
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_batch_size(24)
            .with_truncated_rows(true);
        let csv = reader.build(file).unwrap();
        let batches = csv.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 6);
        assert_eq!(batch.num_columns(), 4);
        let name = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let age = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let occupation = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dob = batch
            .column(3)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        assert_eq!(name.value(0), "A1");
        assert_eq!(name.value(1), "B2");
        assert!(name.is_null(2));
        assert_eq!(name.value(3), "C3");
        assert_eq!(name.value(4), "D4");
        assert_eq!(name.value(5), "E5");

        assert_eq!(age.value(0), 34);
        assert_eq!(age.value(1), 29);
        assert!(age.is_null(2));
        assert_eq!(age.value(3), 45);
        assert!(age.is_null(4));
        assert_eq!(age.value(5), 31);

        assert_eq!(occupation.value(0), "Engineer");
        assert_eq!(occupation.value(1), "Doctor");
        assert!(occupation.is_null(2));
        assert_eq!(occupation.value(3), "Artist");
        assert!(occupation.is_null(4));
        assert!(occupation.is_null(5));

        assert_eq!(dob.value(0), 5675);
        assert!(dob.is_null(1));
        assert!(dob.is_null(2));
        assert_eq!(dob.value(3), -1858);
        assert!(dob.is_null(4));
        assert!(dob.is_null(5));
    }

    #[test]
    fn test_truncated_rows_not_nullable_error() {
        let data = "a,b,c\n1,2,3\n4,5";
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_truncated_rows(true)
            .build(Cursor::new(data))
            .unwrap();

        let batches = reader.collect::<Result<Vec<_>, _>>();
        assert!(match batches {
            Err(ArrowError::InvalidArgumentError(e)) =>
                e.to_string().contains("contains null values"),
            _ => false,
        });
    }

    #[test]
    fn test_buffered() {
        let tests = [
            ("test/data/uk_cities.csv", false, 37),
            ("test/data/various_types.csv", true, 10),
            ("test/data/decimal_test.csv", false, 10),
        ];

        for (path, has_header, expected_rows) in tests {
            let (schema, _) = Format::default()
                .infer_schema(File::open(path).unwrap(), None)
                .unwrap();
            let schema = Arc::new(schema);

            for batch_size in [1, 4] {
                for capacity in [1, 3, 7, 100] {
                    let reader = ReaderBuilder::new(schema.clone())
                        .with_batch_size(batch_size)
                        .with_header(has_header)
                        .build(File::open(path).unwrap())
                        .unwrap();

                    let expected = reader.collect::<Result<Vec<_>, _>>().unwrap();

                    assert_eq!(
                        expected.iter().map(|x| x.num_rows()).sum::<usize>(),
                        expected_rows
                    );

                    let buffered =
                        std::io::BufReader::with_capacity(capacity, File::open(path).unwrap());

                    let reader = ReaderBuilder::new(schema.clone())
                        .with_batch_size(batch_size)
                        .with_header(has_header)
                        .build_buffered(buffered)
                        .unwrap();

                    let actual = reader.collect::<Result<Vec<_>, _>>().unwrap();
                    assert_eq!(expected, actual)
                }
            }
        }
    }

    fn err_test(csv: &[u8], expected: &str) {
        fn err_test_with_schema(csv: &[u8], expected: &str, schema: Arc<Schema>) {
            let buffer = std::io::BufReader::with_capacity(2, Cursor::new(csv));
            let b = ReaderBuilder::new(schema)
                .with_batch_size(2)
                .build_buffered(buffer)
                .unwrap();
            let err = b.collect::<Result<Vec<_>, _>>().unwrap_err().to_string();
            assert_eq!(err, expected)
        }

        let schema_utf8 = Arc::new(Schema::new(vec![
            Field::new("text1", DataType::Utf8, true),
            Field::new("text2", DataType::Utf8, true),
        ]));
        err_test_with_schema(csv, expected, schema_utf8);

        let schema_utf8view = Arc::new(Schema::new(vec![
            Field::new("text1", DataType::Utf8View, true),
            Field::new("text2", DataType::Utf8View, true),
        ]));
        err_test_with_schema(csv, expected, schema_utf8view);
    }

    #[test]
    fn test_invalid_utf8() {
        err_test(
            b"sdf,dsfg\ndfd,hgh\xFFue\n,sds\nFalhghse,",
            "Csv error: Encountered invalid UTF-8 data for line 2 and field 2",
        );

        err_test(
            b"sdf,dsfg\ndksdk,jf\nd\xFFfd,hghue\n,sds\nFalhghse,",
            "Csv error: Encountered invalid UTF-8 data for line 3 and field 1",
        );

        err_test(
            b"sdf,dsfg\ndksdk,jf\ndsdsfd,hghue\n,sds\nFalhghse,\xFF",
            "Csv error: Encountered invalid UTF-8 data for line 5 and field 2",
        );

        err_test(
            b"\xFFsdf,dsfg\ndksdk,jf\ndsdsfd,hghue\n,sds\nFalhghse,\xFF",
            "Csv error: Encountered invalid UTF-8 data for line 1 and field 1",
        );
    }

    struct InstrumentedRead<R> {
        r: R,
        fill_count: usize,
        fill_sizes: Vec<usize>,
    }

    impl<R> InstrumentedRead<R> {
        fn new(r: R) -> Self {
            Self {
                r,
                fill_count: 0,
                fill_sizes: vec![],
            }
        }
    }

    impl<R: Seek> Seek for InstrumentedRead<R> {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.r.seek(pos)
        }
    }

    impl<R: BufRead> Read for InstrumentedRead<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.r.read(buf)
        }
    }

    impl<R: BufRead> BufRead for InstrumentedRead<R> {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            self.fill_count += 1;
            let buf = self.r.fill_buf()?;
            self.fill_sizes.push(buf.len());
            Ok(buf)
        }

        fn consume(&mut self, amt: usize) {
            self.r.consume(amt)
        }
    }

    #[test]
    fn test_io() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let csv = "foo,bar\nbaz,foo\na,b\nc,d";
        let mut read = InstrumentedRead::new(Cursor::new(csv.as_bytes()));
        let reader = ReaderBuilder::new(schema)
            .with_batch_size(3)
            .build_buffered(&mut read)
            .unwrap();

        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[1].num_rows(), 1);

        // Expect 4 calls to fill_buf
        // 1. Read first 3 rows
        // 2. Read final row
        // 3. Delimit and flush final row
        // 4. Iterator finished
        assert_eq!(&read.fill_sizes, &[23, 3, 0, 0]);
        assert_eq!(read.fill_count, 4);
    }

    #[test]
    fn test_inference() {
        let cases: &[(&[&str], DataType)] = &[
            (&[], DataType::Null),
            (&["false", "12"], DataType::Utf8),
            (&["12", "cupcakes"], DataType::Utf8),
            (&["12", "12.4"], DataType::Float64),
            (&["14050", "24332"], DataType::Int64),
            (&["14050.0", "true"], DataType::Utf8),
            (&["14050", "2020-03-19 00:00:00"], DataType::Utf8),
            (&["14050", "2340.0", "2020-03-19 00:00:00"], DataType::Utf8),
            (
                &["2020-03-19 02:00:00", "2020-03-19 00:00:00"],
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (&["2020-03-19", "2020-03-20"], DataType::Date32),
            (
                &["2020-03-19", "2020-03-19 02:00:00", "2020-03-19 00:00:00"],
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                &[
                    "2020-03-19",
                    "2020-03-19 02:00:00",
                    "2020-03-19 00:00:00.000",
                ],
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (
                &[
                    "2020-03-19",
                    "2020-03-19 02:00:00",
                    "2020-03-19 00:00:00.000000",
                ],
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                &["2020-03-19 02:00:00+02:00", "2020-03-19 02:00:00Z"],
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                &[
                    "2020-03-19",
                    "2020-03-19 02:00:00+02:00",
                    "2020-03-19 02:00:00Z",
                    "2020-03-19 02:00:00.12Z",
                ],
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (
                &[
                    "2020-03-19",
                    "2020-03-19 02:00:00.000000000",
                    "2020-03-19 00:00:00.000000",
                ],
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
        ];

        for (values, expected) in cases {
            let mut t = InferredDataType::default();
            for v in *values {
                t.update(v)
            }
            assert_eq!(&t.get(), expected, "{values:?}")
        }
    }

    #[test]
    fn test_record_length_mismatch() {
        let csv = "\
        a,b,c\n\
        1,2,3\n\
        4,5\n\
        6,7,8";
        let mut read = Cursor::new(csv.as_bytes());
        let result = Format::default()
            .with_header(true)
            .infer_schema(&mut read, None);
        assert!(result.is_err());
        // Include line number in the error message to help locate and fix the issue
        assert_eq!(result.err().unwrap().to_string(), "Csv error: Encountered unequal lengths between records on CSV file. Expected 2 records, found 3 records at line 3");
    }

    #[test]
    fn test_comment() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
        ]);

        let csv = "# comment1 \n1,2\n#comment2\n11,22";
        let mut read = Cursor::new(csv.as_bytes());
        let reader = ReaderBuilder::new(Arc::new(schema))
            .with_comment(b'#')
            .build(&mut read)
            .unwrap();

        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        let b = batches.first().unwrap();
        assert_eq!(b.num_columns(), 2);
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &vec![1, 11]
        );
        assert_eq!(
            b.column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &vec![2, 22]
        );
    }

    #[test]
    fn test_parse_string_view_single_column() {
        let csv = ["foo", "something_cannot_be_inlined", "foobar"].join("\n");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::Utf8View,
            true,
        )]));

        let mut decoder = ReaderBuilder::new(schema).build_decoder();

        let decoded = decoder.decode(csv.as_bytes()).unwrap();
        assert_eq!(decoded, csv.len());
        decoder.decode(&[]).unwrap();

        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
        let col = batch.column(0).as_string_view();
        assert_eq!(col.data_type(), &DataType::Utf8View);
        assert_eq!(col.value(0), "foo");
        assert_eq!(col.value(1), "something_cannot_be_inlined");
        assert_eq!(col.value(2), "foobar");
    }

    #[test]
    fn test_parse_string_view_multi_column() {
        let csv = ["foo,", ",something_cannot_be_inlined", "foobarfoobar,bar"].join("\n");
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8View, true),
            Field::new("c2", DataType::Utf8View, true),
        ]));

        let mut decoder = ReaderBuilder::new(schema).build_decoder();

        let decoded = decoder.decode(csv.as_bytes()).unwrap();
        assert_eq!(decoded, csv.len());
        decoder.decode(&[]).unwrap();

        let batch = decoder.flush().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        let c1 = batch.column(0).as_string_view();
        let c2 = batch.column(1).as_string_view();
        assert_eq!(c1.data_type(), &DataType::Utf8View);
        assert_eq!(c2.data_type(), &DataType::Utf8View);

        assert!(!c1.is_null(0));
        assert!(c1.is_null(1));
        assert!(!c1.is_null(2));
        assert_eq!(c1.value(0), "foo");
        assert_eq!(c1.value(2), "foobarfoobar");

        assert!(c2.is_null(0));
        assert!(!c2.is_null(1));
        assert!(!c2.is_null(2));
        assert_eq!(c2.value(1), "something_cannot_be_inlined");
        assert_eq!(c2.value(2), "bar");
    }
}
