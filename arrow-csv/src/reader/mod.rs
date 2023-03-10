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
//! This CSV reader allows CSV files to be read into the Arrow memory model. Records are
//! loaded in batches and are then converted from row-based data to columnar data.
//!
//! Example:
//!
//! ```
//! # use arrow_schema::*;
//! # use arrow_csv::Reader;
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
//! let mut csv = Reader::new(file, Arc::new(schema), false, None, 1024, None, None, None);
//! let batch = csv.next().unwrap().unwrap();
//! ```

mod records;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::*;
use arrow_array::*;
use arrow_cast::parse::{parse_decimal, Parser};
use arrow_schema::*;
use lazy_static::lazy_static;
use regex::{Regex, RegexSet};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader as StdBufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::map_csv_error;
use crate::reader::records::{RecordDecoder, StringRecords};
use csv::StringRecord;

lazy_static! {
    /// Order should match [`InferredDataType`]
    static ref REGEX_SET: RegexSet = RegexSet::new([
        r"(?i)^(true)$|^(false)$(?-i)", //BOOLEAN
        r"^-?(\d+)$", //INTEGER
        r"^-?((\d*\.\d+|\d+\.\d*)([eE]-?\d+)?|\d+([eE]-?\d+))$", //DECIMAL
        r"^\d{4}-\d\d-\d\d$", //DATE32
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d$", //Timestamp(Second)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d.\d{1,3}$", //Timestamp(Millisecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d.\d{1,6}$", //Timestamp(Microsecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d.\d{1,9}$", //Timestamp(Nanosecond)
    ]).unwrap();
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
    fn update(&mut self, string: &str, datetime_re: Option<&Regex>) {
        self.packed |= if string.starts_with('"') {
            1 << 8 // Utf8
        } else if let Some(m) = REGEX_SET.matches(string).into_iter().next() {
            1 << m
        } else {
            match datetime_re {
                // Timestamp(Nanosecond)
                Some(d) if d.is_match(string) => 1 << 7,
                _ => 1 << 8, // Utf8
            }
        }
    }
}

/// This is a collection of options for csv reader when the builder pattern cannot be used
/// and the parameters need to be passed around
#[derive(Debug, Default, Clone)]
struct ReaderOptions {
    has_header: bool,
    delimiter: Option<u8>,
    escape: Option<u8>,
    quote: Option<u8>,
    terminator: Option<u8>,
    max_read_records: Option<usize>,
    datetime_re: Option<Regex>,
}

/// Infer the schema of a CSV file by reading through the first n records of the file,
/// with `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its schema.
///
/// Return inferred schema and number of records used for inference. This function does not change
/// reader cursor offset.
///
/// The inferred schema will always have each field set as nullable.
pub fn infer_file_schema<R: Read + Seek>(
    reader: R,
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<(Schema, usize), ArrowError> {
    let roptions = ReaderOptions {
        delimiter: Some(delimiter),
        max_read_records,
        has_header,
        ..Default::default()
    };

    infer_file_schema_with_csv_options(reader, roptions)
}

fn infer_file_schema_with_csv_options<R: Read + Seek>(
    mut reader: R,
    roptions: ReaderOptions,
) -> Result<(Schema, usize), ArrowError> {
    let saved_offset = reader.stream_position()?;

    let (schema, records_count) =
        infer_reader_schema_with_csv_options(&mut reader, roptions)?;
    // return the reader seek back to the start
    reader.seek(SeekFrom::Start(saved_offset))?;

    Ok((schema, records_count))
}

/// Infer schema of CSV records provided by struct that implements `Read` trait.
///
/// `max_read_records` controlling the maximum number of records to read. If `max_read_records` is
/// not set, all records are read to infer the schema.
///
/// Return infered schema and number of records used for inference.
pub fn infer_reader_schema<R: Read>(
    reader: R,
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<(Schema, usize), ArrowError> {
    let roptions = ReaderOptions {
        delimiter: Some(delimiter),
        max_read_records,
        has_header,
        ..Default::default()
    };
    infer_reader_schema_with_csv_options(reader, roptions)
}

/// Creates a `csv::Reader`
fn build_csv_reader<R: Read>(
    reader: R,
    has_header: bool,
    delimiter: Option<u8>,
    escape: Option<u8>,
    quote: Option<u8>,
    terminator: Option<u8>,
) -> csv::Reader<R> {
    let mut reader_builder = csv::ReaderBuilder::new();
    reader_builder.has_headers(has_header);

    if let Some(c) = delimiter {
        reader_builder.delimiter(c);
    }
    reader_builder.escape(escape);
    if let Some(c) = quote {
        reader_builder.quote(c);
    }
    if let Some(t) = terminator {
        reader_builder.terminator(csv::Terminator::Any(t));
    }
    reader_builder.from_reader(reader)
}

fn infer_reader_schema_with_csv_options<R: Read>(
    reader: R,
    roptions: ReaderOptions,
) -> Result<(Schema, usize), ArrowError> {
    let mut csv_reader = build_csv_reader(
        reader,
        roptions.has_header,
        roptions.delimiter,
        roptions.escape,
        roptions.quote,
        roptions.terminator,
    );

    // get or create header names
    // when has_header is false, creates default column names with column_ prefix
    let headers: Vec<String> = if roptions.has_header {
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
    let max_records = roptions.max_read_records.unwrap_or(usize::MAX);
    while records_count < max_records {
        if !csv_reader.read_record(&mut record).map_err(map_csv_error)? {
            break;
        }
        records_count += 1;

        // Note since we may be looking at a sample of the data, we make the safe assumption that
        // they could be nullable
        for (i, column_type) in column_types.iter_mut().enumerate().take(header_length) {
            if let Some(string) = record.get(i) {
                if !string.is_empty() {
                    column_type.update(string, roptions.datetime_re.as_ref())
                }
            }
        }
    }

    // build schema from inference results
    let fields = column_types
        .iter()
        .zip(&headers)
        .map(|(inferred, field_name)| Field::new(field_name, inferred.get(), true))
        .collect();

    Ok((Schema::new(fields), records_count))
}

/// Infer schema from a list of CSV files by reading through first n records
/// with `max_read_records` controlling the maximum number of records to read.
///
/// Files will be read in the given order untill n records have been reached.
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

    for fname in files.iter() {
        let (schema, records_read) = infer_file_schema(
            &mut File::open(fname)?,
            delimiter,
            Some(records_to_read),
            has_header,
        )?;
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
    /// Create a new CsvReader from any value that implements the `Read` trait.
    ///
    /// If reading a `File` or an input that supports `std::io::Read` and `std::io::Seek`;
    /// you can customise the Reader, such as to enable schema inference, use
    /// `ReaderBuilder`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reader: R,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
        datetime_format: Option<String>,
    ) -> Self {
        let mut builder = ReaderBuilder::new()
            .has_header(has_header)
            .with_batch_size(batch_size)
            .with_schema(schema);

        if let Some(delimiter) = delimiter {
            builder = builder.with_delimiter(delimiter);
        }
        if let Some((start, end)) = bounds {
            builder = builder.with_bounds(start, end);
        }
        if let Some(projection) = projection {
            builder = builder.with_projection(projection)
        }
        if let Some(format) = datetime_format {
            builder = builder.with_datetime_format(format)
        }

        Self {
            decoder: builder.build_decoder(),
            reader: StdBufReader::new(reader),
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.decoder.projection {
            Some(projection) => {
                let fields = self.decoder.schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.decoder.schema.clone(),
        }
    }

    /// Create a new CsvReader from a Reader
    ///
    /// This constructor allows you more flexibility in what records are processed by the
    /// csv reader.
    #[allow(clippy::too_many_arguments)]
    #[deprecated(note = "Use Reader::new or ReaderBuilder")]
    pub fn from_reader(
        reader: R,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
        datetime_format: Option<String>,
    ) -> Self {
        Self::new(
            reader,
            schema,
            has_header,
            delimiter,
            batch_size,
            bounds,
            projection,
            datetime_format,
        )
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
///     let mut decoder = ReaderBuilder::new()
///         .with_schema(schema)
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

    /// datetime format used to parse datetime values, (format understood by chrono)
    ///
    /// For format refer to [chrono docs](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html)
    datetime_format: Option<String>,
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

        let to_read =
            self.batch_size.min(self.end - self.line_number) - self.record_decoder.len();
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
            self.datetime_format.as_deref(),
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
    fields: &[Field],
    metadata: Option<std::collections::HashMap<String, String>>,
    projection: Option<&Vec<usize>>,
    line_number: usize,
    datetime_format: Option<&str>,
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
                DataType::Boolean => build_boolean_array(line_number, rows, i),
                DataType::Decimal128(precision, scale) => {
                    build_decimal_array::<Decimal128Type>(
                        line_number,
                        rows,
                        i,
                        *precision,
                        *scale,
                    )
                }
                DataType::Decimal256(precision, scale) => {
                    build_decimal_array::<Decimal256Type>(
                        line_number,
                        rows,
                        i,
                        *precision,
                        *scale,
                    )
                }
                DataType::Int8 => {
                    build_primitive_array::<Int8Type>(line_number, rows, i, None)
                }
                DataType::Int16 => {
                    build_primitive_array::<Int16Type>(line_number, rows, i, None)
                }
                DataType::Int32 => {
                    build_primitive_array::<Int32Type>(line_number, rows, i, None)
                }
                DataType::Int64 => {
                    build_primitive_array::<Int64Type>(line_number, rows, i, None)
                }
                DataType::UInt8 => {
                    build_primitive_array::<UInt8Type>(line_number, rows, i, None)
                }
                DataType::UInt16 => {
                    build_primitive_array::<UInt16Type>(line_number, rows, i, None)
                }
                DataType::UInt32 => {
                    build_primitive_array::<UInt32Type>(line_number, rows, i, None)
                }
                DataType::UInt64 => {
                    build_primitive_array::<UInt64Type>(line_number, rows, i, None)
                }
                DataType::Float32 => {
                    build_primitive_array::<Float32Type>(line_number, rows, i, None)
                }
                DataType::Float64 => {
                    build_primitive_array::<Float64Type>(line_number, rows, i, None)
                }
                DataType::Date32 => {
                    build_primitive_array::<Date32Type>(line_number, rows, i, None)
                }
                DataType::Date64 => build_primitive_array::<Date64Type>(
                    line_number,
                    rows,
                    i,
                    datetime_format,
                ),
                DataType::Time32(TimeUnit::Second) => {
                    build_primitive_array::<Time32SecondType>(line_number, rows, i, None)
                }
                DataType::Time32(TimeUnit::Millisecond) => build_primitive_array::<
                    Time32MillisecondType,
                >(
                    line_number, rows, i, None
                ),
                DataType::Time64(TimeUnit::Microsecond) => build_primitive_array::<
                    Time64MicrosecondType,
                >(
                    line_number, rows, i, None
                ),
                DataType::Time64(TimeUnit::Nanosecond) => build_primitive_array::<
                    Time64NanosecondType,
                >(
                    line_number, rows, i, None
                ),
                DataType::Timestamp(TimeUnit::Second, _) => build_primitive_array::<
                    TimestampSecondType,
                >(
                    line_number, rows, i, None
                ),
                DataType::Timestamp(TimeUnit::Millisecond, None) => {
                    build_primitive_array::<TimestampMillisecondType>(
                        line_number,
                        rows,
                        i,
                        None,
                    )
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    build_primitive_array::<TimestampMicrosecondType>(
                        line_number,
                        rows,
                        i,
                        None,
                    )
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    build_primitive_array::<TimestampNanosecondType>(
                        line_number,
                        rows,
                        i,
                        None,
                    )
                }
                DataType::Utf8 => Ok(Arc::new(
                    rows.iter()
                        .map(|row| Some(row.get(i)))
                        .collect::<StringArray>(),
                ) as ArrayRef),
                DataType::Dictionary(key_type, value_type)
                    if value_type.as_ref() == &DataType::Utf8 =>
                {
                    match key_type.as_ref() {
                        DataType::Int8 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<Int8Type>>(),
                        ) as ArrayRef),
                        DataType::Int16 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<Int16Type>>(),
                        ) as ArrayRef),
                        DataType::Int32 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef),
                        DataType::Int64 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<Int64Type>>(),
                        ) as ArrayRef),
                        DataType::UInt8 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<UInt8Type>>(),
                        ) as ArrayRef),
                        DataType::UInt16 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<UInt16Type>>(),
                        ) as ArrayRef),
                        DataType::UInt32 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
                                .collect::<DictionaryArray<UInt32Type>>(),
                        ) as ArrayRef),
                        DataType::UInt64 => Ok(Arc::new(
                            rows.iter()
                                .map(|row| row.get(i))
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

    let projected_fields: Vec<Field> =
        projection.iter().map(|i| fields[*i].clone()).collect();

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
fn parse_item<T: Parser>(string: &str) -> Option<T::Native> {
    T::parse(string)
}

fn parse_formatted<T: Parser>(string: &str, format: &str) -> Option<T::Native> {
    T::parse_formatted(string, format)
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
) -> Result<ArrayRef, ArrowError> {
    let mut decimal_builder = PrimitiveBuilder::<T>::with_capacity(rows.len());
    for row in rows.iter() {
        let s = row.get(col_idx);
        if s.is_empty() {
            // append null
            decimal_builder.append_null();
        } else {
            let decimal_value: Result<T::Native, _> =
                parse_decimal::<T>(s, precision, scale);
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
    format: Option<&str>,
) -> Result<ArrayRef, ArrowError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            let s = row.get(col_idx);
            if s.is_empty() {
                return Ok(None);
            }

            let parsed = match format {
                Some(format) => parse_formatted::<T>(s, format),
                _ => parse_item::<T>(s),
            };
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
        .collect::<Result<PrimitiveArray<T>, ArrowError>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_boolean_array(
    line_number: usize,
    rows: &StringRecords<'_>,
    col_idx: usize,
) -> Result<ArrayRef, ArrowError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            let s = row.get(col_idx);
            if s.is_empty() {
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
    /// Optional schema for the CSV file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the CSV structure.
    schema: Option<SchemaRef>,
    /// Whether the file has headers or not
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// An optional escape character. Defaults None
    escape: Option<u8>,
    /// An optional quote character. Defaults b'\"'
    quote: Option<u8>,
    /// An optional record terminator. Defaults CRLF
    terminator: Option<u8>,
    /// Optional maximum number of records to read during schema inference
    ///
    /// If a number is not provided, all the records are read.
    max_records: Option<usize>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// The bounds over which to scan the reader. `None` starts from 0 and runs until EOF.
    bounds: Bounds,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
    /// DateTime format to be used while trying to infer datetime format
    datetime_re: Option<Regex>,
    /// DateTime format to be used while parsing datetime format
    datetime_format: Option<String>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            has_header: false,
            delimiter: None,
            escape: None,
            quote: None,
            terminator: None,
            max_records: None,
            batch_size: 1024,
            bounds: None,
            projection: None,
            datetime_re: None,
            datetime_format: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring CSV parsing options.
    ///
    /// To convert a builder into a reader, call `ReaderBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// use arrow_csv::{Reader, ReaderBuilder};
    /// use std::fs::File;
    ///
    /// fn example() -> Reader<File> {
    ///     let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> ReaderBuilder {
        ReaderBuilder::default()
    }

    /// Set the CSV file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set whether the CSV file has headers
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set the datetime regex used to parse the string to Date64Type
    /// this regex is used while infering schema
    pub fn with_datetime_re(mut self, datetime_re: Regex) -> Self {
        self.datetime_re = Some(datetime_re);
        self
    }

    /// Set the datetime fromat used to parse the string to Date64Type
    /// this fromat is used while when the schema wants to parse Date64Type.
    ///
    /// For format refer to [chrono docs](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html)
    ///
    pub fn with_datetime_format(mut self, datetime_format: String) -> Self {
        self.datetime_format = Some(datetime_format);
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    pub fn with_escape(mut self, escape: u8) -> Self {
        self.escape = Some(escape);
        self
    }

    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = Some(quote);
        self
    }

    pub fn with_terminator(mut self, terminator: u8) -> Self {
        self.terminator = Some(terminator);
        self
    }

    /// Set the CSV reader to infer the schema of the file
    pub fn infer_schema(mut self, max_records: Option<usize>) -> Self {
        // remove any schema that is set
        self.schema = None;
        self.max_records = max_records;
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

    /// Create a new `Reader` from a non-buffered reader
    ///
    /// If `R: BufRead` consider using [`Self::build_buffered`] to avoid unnecessary additional
    /// buffering, as internally this method wraps `reader` in [`std::io::BufReader`]
    pub fn build<R: Read + Seek>(self, reader: R) -> Result<Reader<R>, ArrowError> {
        self.build_buffered(StdBufReader::new(reader))
    }

    /// Create a new `BufReader` from a buffered reader
    pub fn build_buffered<R: BufRead + Seek>(
        mut self,
        mut reader: R,
    ) -> Result<BufReader<R>, ArrowError> {
        // check if schema should be inferred
        if self.schema.is_none() {
            let delimiter = self.delimiter.unwrap_or(b',');
            let roptions = ReaderOptions {
                delimiter: Some(delimiter),
                max_read_records: self.max_records,
                has_header: self.has_header,
                escape: self.escape,
                quote: self.quote,
                terminator: self.terminator,
                datetime_re: self.datetime_re.take(),
            };
            let (inferred_schema, _) =
                infer_file_schema_with_csv_options(&mut reader, roptions)?;
            self.schema = Some(Arc::new(inferred_schema))
        }

        Ok(BufReader {
            reader,
            decoder: self.build_decoder(),
        })
    }

    /// Builds a decoder that can be used to decode CSV from an arbitrary byte stream
    ///
    /// # Panics
    ///
    /// This method panics if no schema provided
    pub fn build_decoder(self) -> Decoder {
        let schema = self.schema.expect("schema should be provided");
        let mut reader_builder = csv_core::ReaderBuilder::new();
        reader_builder.escape(self.escape);

        if let Some(c) = self.delimiter {
            reader_builder.delimiter(c);
        }
        if let Some(c) = self.quote {
            reader_builder.quote(c);
        }
        if let Some(t) = self.terminator {
            reader_builder.terminator(csv_core::Terminator::Any(t));
        }
        let delimiter = reader_builder.build();
        let record_decoder = RecordDecoder::new(delimiter, schema.fields().len());

        let header = self.has_header as usize;

        let (start, end) = match self.bounds {
            Some((start, end)) => (start + header, end + header),
            None => (header, usize::MAX),
        };

        Decoder {
            schema,
            to_skip: start,
            record_decoder,
            line_number: start,
            end,
            projection: self.projection,
            datetime_format: self.datetime_format,
            batch_size: self.batch_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    use arrow_array::cast::as_boolean_array;
    use chrono::prelude::*;

    #[test]
    fn test_csv() {
        for format in [None, Some("%Y-%m-%dT%H:%M:%S%.f%:z".to_string())] {
            let schema = Schema::new(vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("lat", DataType::Float64, false),
                Field::new("lng", DataType::Float64, false),
            ]);

            let file = File::open("test/data/uk_cities.csv").unwrap();
            let mut csv = Reader::new(
                file,
                Arc::new(schema.clone()),
                false,
                None,
                1024,
                None,
                None,
                format,
            );
            assert_eq!(Arc::new(schema), csv.schema());
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
    }

    #[test]
    fn test_csv_schema_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("foo".to_owned(), "bar".to_owned());
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("lat", DataType::Float64, false),
                Field::new("lng", DataType::Float64, false),
            ],
            metadata.clone(),
        );

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = Reader::new(
            file,
            Arc::new(schema.clone()),
            false,
            None,
            1024,
            None,
            None,
            None,
        );
        assert_eq!(Arc::new(schema), csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        assert_eq!(&metadata, batch.schema().metadata());
    }

    #[test]
    fn test_csv_reader_with_decimal() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Decimal128(38, 6), false),
            Field::new("lng", DataType::Decimal256(76, 6), false),
        ]);

        let file = File::open("test/data/decimal_test.csv").unwrap();

        let mut csv =
            Reader::new(file, Arc::new(schema), false, None, 1024, None, None, None);
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

        let file_with_headers =
            File::open("test/data/uk_cities_with_headers.csv").unwrap();
        let file_without_headers = File::open("test/data/uk_cities.csv").unwrap();
        let both_files = file_with_headers
            .chain(Cursor::new("\n".to_string()))
            .chain(file_without_headers);
        let mut csv = Reader::new(
            both_files,
            Arc::new(schema),
            true,
            None,
            1024,
            None,
            None,
            None,
        );
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(74, batch.num_rows());
        assert_eq!(3, batch.num_columns());
    }

    #[test]
    fn test_csv_with_schema_inference() {
        let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();

        let builder = ReaderBuilder::new().has_header(true).infer_schema(None);

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
        let file = File::open("test/data/uk_cities.csv").unwrap();

        let builder = ReaderBuilder::new().infer_schema(None);

        let mut csv = builder.build(file).unwrap();

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
        let file = File::open("test/data/uk_cities.csv").unwrap();

        // Set the bounds to the lines 0, 1 and 2.
        let mut csv = ReaderBuilder::new().with_bounds(0, 2).build(file).unwrap();
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
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = Reader::new(
            file,
            Arc::new(schema),
            false,
            None,
            1024,
            None,
            Some(vec![0, 1]),
            None,
        );
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
        let schema = Schema::new(vec![
            Field::new(
                "city",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = Reader::new(
            file,
            Arc::new(schema),
            false,
            None,
            1024,
            None,
            Some(vec![0, 1]),
            None,
        );
        let projected_schema = Arc::new(Schema::new(vec![
            Field::new(
                "city",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("lat", DataType::Float64, false),
        ]));
        assert_eq!(projected_schema, csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(projected_schema, batch.schema());
        assert_eq!(37, batch.num_rows());
        assert_eq!(2, batch.num_columns());

        let strings = arrow_cast::cast(batch.column(0), &DataType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(strings.value(0), "Elgin, Scotland, the UK");
        assert_eq!(strings.value(4), "Eastbourne, East Sussex, UK");
        assert_eq!(strings.value(29), "Uckfield, East Sussex, UK");
    }

    #[test]
    fn test_nulls() {
        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, true),
            Field::new("c_string", DataType::Utf8, false),
            Field::new("c_bool", DataType::Boolean, false),
        ]);

        let file = File::open("test/data/null_test.csv").unwrap();

        let mut csv =
            Reader::new(file, Arc::new(schema), true, None, 1024, None, None, None);
        let batch = csv.next().unwrap().unwrap();

        assert!(!batch.column(1).is_null(0));
        assert!(!batch.column(1).is_null(1));
        assert!(batch.column(1).is_null(2));
        assert!(!batch.column(1).is_null(3));
        assert!(!batch.column(1).is_null(4));
    }

    #[test]
    fn test_nulls_with_inference() {
        let file = File::open("test/data/various_types.csv").unwrap();

        let builder = ReaderBuilder::new()
            .infer_schema(None)
            .has_header(true)
            .with_delimiter(b'|')
            .with_batch_size(512)
            .with_projection(vec![0, 1, 2, 3, 4, 5]);

        let mut csv = builder.build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();

        assert_eq!(7, batch.num_rows());
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

        let names: Vec<&str> =
            schema.fields().iter().map(|x| x.name().as_str()).collect();
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
    fn test_parse_invalid_csv() {
        let file = File::open("test/data/various_types_invalid.csv").unwrap();

        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, false),
            Field::new("c_string", DataType::Utf8, false),
            Field::new("c_bool", DataType::Boolean, false),
        ]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .has_header(true)
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
    fn infer_field_schema(string: &str, datetime_re: Option<Regex>) -> DataType {
        let mut v = InferredDataType::default();
        v.update(string, datetime_re.as_ref());
        v.get()
    }

    #[test]
    fn test_infer_field_schema() {
        assert_eq!(infer_field_schema("A", None), DataType::Utf8);
        assert_eq!(infer_field_schema("\"123\"", None), DataType::Utf8);
        assert_eq!(infer_field_schema("10", None), DataType::Int64);
        assert_eq!(infer_field_schema("10.2", None), DataType::Float64);
        assert_eq!(infer_field_schema(".2", None), DataType::Float64);
        assert_eq!(infer_field_schema("2.", None), DataType::Float64);
        assert_eq!(infer_field_schema("true", None), DataType::Boolean);
        assert_eq!(infer_field_schema("trUe", None), DataType::Boolean);
        assert_eq!(infer_field_schema("false", None), DataType::Boolean);
        assert_eq!(infer_field_schema("2020-11-08", None), DataType::Date32);
        assert_eq!(
            infer_field_schema("2020-11-08T14:20:01", None),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            infer_field_schema("2020-11-08 14:20:01", None),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        let reg = Regex::new(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$").ok();
        assert_eq!(
            infer_field_schema("2020-11-08 14:20:01", reg),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(infer_field_schema("-5.13", None), DataType::Float64);
        assert_eq!(infer_field_schema("0.1300", None), DataType::Float64);
        assert_eq!(
            infer_field_schema("2021-12-19 13:12:30.921", None),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            infer_field_schema("2021-12-19T13:12:30.123456789", None),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn parse_date32() {
        assert_eq!(parse_item::<Date32Type>("1970-01-01").unwrap(), 0);
        assert_eq!(parse_item::<Date32Type>("2020-03-15").unwrap(), 18336);
        assert_eq!(parse_item::<Date32Type>("1945-05-08").unwrap(), -9004);
    }

    #[test]
    fn parse_time() {
        assert_eq!(
            parse_item::<Time64NanosecondType>("12:10:01.123456789 AM"),
            Some(601_123_456_789)
        );
        assert_eq!(
            parse_item::<Time64MicrosecondType>("12:10:01.123456 am"),
            Some(601_123_456)
        );
        assert_eq!(
            parse_item::<Time32MillisecondType>("2:10:01.12 PM"),
            Some(51_001_120)
        );
        assert_eq!(parse_item::<Time32SecondType>("2:10:01 pm"), Some(51_001));
    }

    #[test]
    fn parse_date64() {
        assert_eq!(parse_item::<Date64Type>("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(
            parse_item::<Date64Type>("2018-11-13T17:11:10").unwrap(),
            1542129070000
        );
        assert_eq!(
            parse_item::<Date64Type>("2018-11-13T17:11:10.011").unwrap(),
            1542129070011
        );
        assert_eq!(
            parse_item::<Date64Type>("1900-02-28T12:34:56").unwrap(),
            -2203932304000
        );
        assert_eq!(
            parse_formatted::<Date64Type>("1900-02-28 12:34:56", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
            -2203932304000
        );
        assert_eq!(
            parse_formatted::<Date64Type>(
                "1900-02-28 12:34:56+0030",
                "%Y-%m-%d %H:%M:%S%z"
            )
            .unwrap(),
            -2203932304000 - (30 * 60 * 1000)
        );
    }

    #[test]
    fn test_parse_timestamp_microseconds() {
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("1970-01-01T00:00:00Z").unwrap(),
            0
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2018, 11, 13).unwrap(),
            NaiveTime::from_hms_nano_opt(17, 11, 10, 0).unwrap(),
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("2018-11-13T17:11:10").unwrap(),
            naive_datetime.timestamp_nanos() / 1000
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("2018-11-13 17:11:10").unwrap(),
            naive_datetime.timestamp_nanos() / 1000
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2018, 11, 13).unwrap(),
            NaiveTime::from_hms_nano_opt(17, 11, 10, 11000000).unwrap(),
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("2018-11-13T17:11:10.011").unwrap(),
            naive_datetime.timestamp_nanos() / 1000
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1900, 2, 28).unwrap(),
            NaiveTime::from_hms_nano_opt(12, 34, 56, 0).unwrap(),
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("1900-02-28T12:34:56").unwrap(),
            naive_datetime.timestamp_nanos() / 1000
        );
    }

    #[test]
    fn test_parse_timestamp_nanoseconds() {
        assert_eq!(
            parse_item::<TimestampNanosecondType>("1970-01-01T00:00:00Z").unwrap(),
            0
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2018, 11, 13).unwrap(),
            NaiveTime::from_hms_nano_opt(17, 11, 10, 0).unwrap(),
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("2018-11-13T17:11:10").unwrap(),
            naive_datetime.timestamp_nanos()
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("2018-11-13 17:11:10").unwrap(),
            naive_datetime.timestamp_nanos()
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2018, 11, 13).unwrap(),
            NaiveTime::from_hms_nano_opt(17, 11, 10, 11000000).unwrap(),
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("2018-11-13T17:11:10.011").unwrap(),
            naive_datetime.timestamp_nanos()
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1900, 2, 28).unwrap(),
            NaiveTime::from_hms_nano_opt(12, 34, 56, 0).unwrap(),
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("1900-02-28T12:34:56").unwrap(),
            naive_datetime.timestamp_nanos()
        );
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
        let data = vec![
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

        let mut csv = Reader::new(
            reader,
            Arc::new(schema),
            false,
            None,
            2,
            // starting at row 2 and up to row 6.
            Some((2, 6)),
            Some(vec![0]),
            None,
        );

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
        let data = vec![vec!["0"], vec!["1"]];

        let data = data
            .iter()
            .map(|x| x.join(","))
            .collect::<Vec<_>>()
            .join("\n");
        let data = data.as_bytes();

        let reader = std::io::Cursor::new(data);

        let mut csv = Reader::new(
            reader,
            Arc::new(schema),
            false,
            None,
            2,
            None,
            Some(vec![]),
            None,
        );

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
        assert_eq!(Some(12.34), parse_item::<Float64Type>("12.34"));
        assert_eq!(Some(-12.34), parse_item::<Float64Type>("-12.34"));
        assert_eq!(Some(12.0), parse_item::<Float64Type>("12"));
        assert_eq!(Some(0.0), parse_item::<Float64Type>("0"));
        assert_eq!(Some(2.0), parse_item::<Float64Type>("2."));
        assert_eq!(Some(0.2), parse_item::<Float64Type>(".2"));
        assert!(parse_item::<Float64Type>("nan").unwrap().is_nan());
        assert!(parse_item::<Float64Type>("NaN").unwrap().is_nan());
        assert!(parse_item::<Float64Type>("inf").unwrap().is_infinite());
        assert!(parse_item::<Float64Type>("inf").unwrap().is_sign_positive());
        assert!(parse_item::<Float64Type>("-inf").unwrap().is_infinite());
        assert!(parse_item::<Float64Type>("-inf")
            .unwrap()
            .is_sign_negative());
        assert_eq!(None, parse_item::<Float64Type>(""));
        assert_eq!(None, parse_item::<Float64Type>("dd"));
        assert_eq!(None, parse_item::<Float64Type>("12.34.56"));
    }

    #[test]
    fn test_non_std_quote() {
        let schema = Schema::new(vec![
            Field::new("text1", DataType::Utf8, false),
            Field::new("text2", DataType::Utf8, false),
        ]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .has_header(false)
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
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .has_header(false)
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
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .has_header(false)
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

        for (idx, (bounds, has_header, expected)) in tests.into_iter().enumerate() {
            let mut reader = ReaderBuilder::new().has_header(has_header);
            if let Some((start, end)) = bounds {
                reader = reader.with_bounds(start, end);
            }
            let b = reader
                .build(Cursor::new(csv.as_bytes()))
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
        let b = ReaderBuilder::new()
            .build_buffered(Cursor::new(csv.as_bytes()))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(b.num_rows(), 4);
        assert_eq!(b.num_columns(), 2);

        let c = as_boolean_array(b.column(0));
        assert_eq!(c.null_count(), 1);
        assert!(c.value(0));
        assert!(!c.value(1));
        assert!(c.is_null(2));
        assert!(!c.value(3));

        let c = as_boolean_array(b.column(1));
        assert_eq!(c.null_count(), 1);
        assert!(!c.value(0));
        assert!(c.value(1));
        assert!(c.value(2));
        assert!(c.is_null(3));
    }

    #[test]
    fn test_buffered() {
        let tests = [
            ("test/data/uk_cities.csv", false, 37),
            ("test/data/various_types.csv", true, 7),
            ("test/data/decimal_test.csv", false, 10),
        ];

        for (path, has_header, expected_rows) in tests {
            for batch_size in [1, 4] {
                for capacity in [1, 3, 7, 100] {
                    let reader = ReaderBuilder::new()
                        .with_batch_size(batch_size)
                        .has_header(has_header)
                        .build(File::open(path).unwrap())
                        .unwrap();

                    let expected = reader.collect::<Result<Vec<_>, _>>().unwrap();

                    assert_eq!(
                        expected.iter().map(|x| x.num_rows()).sum::<usize>(),
                        expected_rows
                    );

                    let buffered = std::io::BufReader::with_capacity(
                        capacity,
                        File::open(path).unwrap(),
                    );

                    let reader = ReaderBuilder::new()
                        .with_batch_size(batch_size)
                        .has_header(has_header)
                        .build_buffered(buffered)
                        .unwrap();

                    let actual = reader.collect::<Result<Vec<_>, _>>().unwrap();
                    assert_eq!(expected, actual)
                }
            }
        }
    }

    fn err_test(csv: &[u8], expected: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text1", DataType::Utf8, false),
            Field::new("text2", DataType::Utf8, false),
        ]));
        let buffer = std::io::BufReader::with_capacity(2, Cursor::new(csv));
        let b = ReaderBuilder::new()
            .with_schema(schema)
            .with_batch_size(2)
            .build_buffered(buffer)
            .unwrap();
        let err = b.collect::<Result<Vec<_>, _>>().unwrap_err().to_string();
        assert_eq!(err, expected)
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
        let reader = ReaderBuilder::new()
            .with_schema(schema)
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
            (&[], DataType::Utf8),
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
                t.update(v, None)
            }
            assert_eq!(&t.get(), expected, "{values:?}")
        }
    }
}
