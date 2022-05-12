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
//! use arrow::csv;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use std::fs::File;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("city", DataType::Utf8, false),
//!     Field::new("lat", DataType::Float64, false),
//!     Field::new("lng", DataType::Float64, false),
//! ]);
//!
//! let file = File::open("test/data/uk_cities.csv").unwrap();
//!
//! let mut csv = csv::Reader::new(file, Arc::new(schema), false, None, 1024, None, None, None);
//! let batch = csv.next().unwrap().unwrap();
//! ```

use core::cmp::min;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::array::{
    ArrayRef, BooleanArray, DecimalBuilder, DictionaryArray, PrimitiveArray, StringArray,
};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;
use crate::util::reader_parser::Parser;

use csv_crate::{ByteRecord, StringRecord};
use std::ops::Neg;

lazy_static! {
    static ref PARSE_DECIMAL_RE: Regex =
        Regex::new(r"^-?(\d+\.?\d*|\d*\.?\d+)$").unwrap();
    static ref DECIMAL_RE: Regex =
        Regex::new(r"^-?((\d*\.\d+|\d+\.\d*)([eE]-?\d+)?|\d+([eE]-?\d+))$").unwrap();
    static ref INTEGER_RE: Regex = Regex::new(r"^-?(\d+)$").unwrap();
    static ref BOOLEAN_RE: Regex = RegexBuilder::new(r"^(true)$|^(false)$")
        .case_insensitive(true)
        .build()
        .unwrap();
    static ref DATE_RE: Regex = Regex::new(r"^\d{4}-\d\d-\d\d$").unwrap();
    static ref DATETIME_RE: Regex =
        Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$").unwrap();
}

/// Infer the data type of a record
fn infer_field_schema(string: &str, datetime_re: Option<Regex>) -> DataType {
    let datetime_re = datetime_re.unwrap_or_else(|| DATETIME_RE.clone());
    // when quoting is enabled in the reader, these quotes aren't escaped, we default to
    // Utf8 for them
    if string.starts_with('"') {
        return DataType::Utf8;
    }
    // match regex in a particular order
    if BOOLEAN_RE.is_match(string) {
        DataType::Boolean
    } else if DECIMAL_RE.is_match(string) {
        DataType::Float64
    } else if INTEGER_RE.is_match(string) {
        DataType::Int64
    } else if datetime_re.is_match(string) {
        DataType::Date64
    } else if DATE_RE.is_match(string) {
        DataType::Date32
    } else {
        DataType::Utf8
    }
}

/// This is a collection of options for csv reader when the builder pattern cannot be used
/// and the parameters need to be passed around
#[derive(Debug, Default, Clone)]
pub struct ReaderOptions {
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
pub fn infer_file_schema<R: Read + Seek>(
    reader: &mut R,
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<(Schema, usize)> {
    let roptions = ReaderOptions {
        delimiter: Some(delimiter),
        max_read_records,
        has_header,
        ..Default::default()
    };

    infer_file_schema_with_csv_options(reader, roptions)
}

fn infer_file_schema_with_csv_options<R: Read + Seek>(
    reader: &mut R,
    roptoins: ReaderOptions,
) -> Result<(Schema, usize)> {
    let saved_offset = reader.seek(SeekFrom::Current(0))?;

    let (schema, records_count) = infer_reader_schema_with_csv_options(reader, roptoins)?;
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
    reader: &mut R,
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<(Schema, usize)> {
    let roptions = ReaderOptions {
        delimiter: Some(delimiter),
        max_read_records,
        has_header,
        ..Default::default()
    };
    infer_reader_schema_with_csv_options(reader, roptions)
}

fn infer_reader_schema_with_csv_options<R: Read>(
    reader: &mut R,
    roptions: ReaderOptions,
) -> Result<(Schema, usize)> {
    let mut csv_reader = Reader::build_csv_reader(
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
        let headers = &csv_reader.headers()?.clone();
        headers.iter().map(|s| s.to_string()).collect()
    } else {
        let first_record_count = &csv_reader.headers()?.len();
        (0..*first_record_count)
            .map(|i| format!("column_{}", i + 1))
            .collect()
    };

    let header_length = headers.len();
    // keep track of inferred field types
    let mut column_types: Vec<HashSet<DataType>> = vec![HashSet::new(); header_length];
    // keep track of columns with nulls
    let mut nulls: Vec<bool> = vec![false; header_length];

    let mut records_count = 0;
    let mut fields = vec![];

    let mut record = StringRecord::new();
    let max_records = roptions.max_read_records.unwrap_or(usize::MAX);
    while records_count < max_records {
        if !csv_reader.read_record(&mut record)? {
            break;
        }
        records_count += 1;

        for i in 0..header_length {
            if let Some(string) = record.get(i) {
                if string.is_empty() {
                    nulls[i] = true;
                } else {
                    column_types[i]
                        .insert(infer_field_schema(string, roptions.datetime_re.clone()));
                }
            }
        }
    }

    // build schema from inference results
    for i in 0..header_length {
        let possibilities = &column_types[i];
        let has_nulls = nulls[i];
        let field_name = &headers[i];

        // determine data type based on possible types
        // if there are incompatible types, use DataType::Utf8
        match possibilities.len() {
            1 => {
                for dtype in possibilities.iter() {
                    fields.push(Field::new(field_name, dtype.clone(), has_nulls));
                }
            }
            2 => {
                if possibilities.contains(&DataType::Int64)
                    && possibilities.contains(&DataType::Float64)
                {
                    // we have an integer and double, fall down to double
                    fields.push(Field::new(field_name, DataType::Float64, has_nulls));
                } else {
                    // default to Utf8 for conflicting datatypes (e.g bool and int)
                    fields.push(Field::new(field_name, DataType::Utf8, has_nulls));
                }
            }
            _ => fields.push(Field::new(field_name, DataType::Utf8, has_nulls)),
        }
    }

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
) -> Result<Schema> {
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

/// CSV file reader
pub struct Reader<R: Read> {
    /// Explicit schema for the CSV file
    schema: SchemaRef,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
    /// File reader
    reader: csv_crate::Reader<R>,
    /// Current line number
    line_number: usize,
    /// Maximum number of rows to read
    end: usize,
    /// Number of records per batch
    batch_size: usize,
    /// Vector that can hold the `StringRecord`s of the batches
    batch_records: Vec<StringRecord>,
    /// datetime format used to parse datetime values, (format understood by chrono)
    ///
    /// For format refer to [chrono docs](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html)
    datetime_format: Option<String>,
}

impl<R> fmt::Debug for Reader<R>
where
    R: Read,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("line_number", &self.line_number)
            .field("datetime_format", &self.datetime_format)
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
        Self::from_reader(
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

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }

    /// Create a new CsvReader from a Reader
    ///
    /// This constructor allows you more flexibility in what records are processed by the
    /// csv reader.
    #[allow(clippy::too_many_arguments)]
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
        let csv_reader =
            Self::build_csv_reader(reader, has_header, delimiter, None, None, None);
        Self::from_csv_reader(
            csv_reader,
            schema,
            has_header,
            batch_size,
            bounds,
            projection,
            datetime_format,
        )
    }

    fn build_csv_reader(
        reader: R,
        has_header: bool,
        delimiter: Option<u8>,
        escape: Option<u8>,
        quote: Option<u8>,
        terminator: Option<u8>,
    ) -> csv_crate::Reader<R> {
        let mut reader_builder = csv_crate::ReaderBuilder::new();
        reader_builder.has_headers(has_header);

        if let Some(c) = delimiter {
            reader_builder.delimiter(c);
        }
        reader_builder.escape(escape);
        if let Some(c) = quote {
            reader_builder.quote(c);
        }
        if let Some(t) = terminator {
            reader_builder.terminator(csv_crate::Terminator::Any(t));
        }
        reader_builder.from_reader(reader)
    }

    fn from_csv_reader(
        mut csv_reader: csv_crate::Reader<R>,
        schema: SchemaRef,
        has_header: bool,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
        datetime_format: Option<String>,
    ) -> Self {
        let (start, end) = match bounds {
            None => (0, usize::MAX),
            Some((start, end)) => (start, end),
        };

        // First we will skip `start` rows
        // note that this skips by iteration. This is because in general it is not possible
        // to seek in CSV. However, skipping still saves the burden of creating arrow arrays,
        // which is a slow operation that scales with the number of columns

        let mut record = ByteRecord::new();
        // Skip first start items
        for _ in 0..start {
            let res = csv_reader.read_byte_record(&mut record);
            if !res.unwrap_or(false) {
                break;
            }
        }

        // Initialize batch_records with StringRecords so they
        // can be reused across batches
        let mut batch_records = Vec::with_capacity(batch_size);
        batch_records.resize_with(batch_size, Default::default);

        Self {
            schema,
            projection,
            reader: csv_reader,
            line_number: if has_header { start + 1 } else { start },
            batch_size,
            end,
            batch_records,
            datetime_format,
        }
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.end - self.line_number;

        let mut read_records = 0;
        for i in 0..min(self.batch_size, remaining) {
            match self.reader.read_record(&mut self.batch_records[i]) {
                Ok(true) => {
                    read_records += 1;
                }
                Ok(false) => break,
                Err(e) => {
                    return Some(Err(ArrowError::ParseError(format!(
                        "Error parsing line {}: {:?}",
                        self.line_number + i,
                        e
                    ))));
                }
            }
        }

        // return early if no data was loaded
        if read_records == 0 {
            return None;
        }

        let format: Option<&str> = match self.datetime_format {
            Some(ref format) => Some(format.as_ref()),
            _ => None,
        };

        // parse the batches into a RecordBatch
        let result = parse(
            &self.batch_records[..read_records],
            self.schema.fields(),
            Some(self.schema.metadata.clone()),
            self.projection.as_ref(),
            self.line_number,
            format,
        );

        self.line_number += read_records;

        Some(result)
    }
}

/// parses a slice of [csv_crate::StringRecord] into a
/// [RecordBatch](crate::record_batch::RecordBatch).
fn parse(
    rows: &[StringRecord],
    fields: &[Field],
    metadata: Option<std::collections::HashMap<String, String>>,
    projection: Option<&Vec<usize>>,
    line_number: usize,
    datetime_format: Option<&str>,
) -> Result<RecordBatch> {
    let projection: Vec<usize> = match projection {
        Some(v) => v.clone(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };

    let arrays: Result<Vec<ArrayRef>> = projection
        .iter()
        .map(|i| {
            let i = *i;
            let field = &fields[i];
            match field.data_type() {
                DataType::Boolean => build_boolean_array(line_number, rows, i),
                DataType::Decimal(precision, scale) => {
                    build_decimal_array(line_number, rows, i, *precision, *scale)
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
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    build_primitive_array::<TimestampMicrosecondType>(
                        line_number,
                        rows,
                        i,
                        None,
                    )
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    build_primitive_array::<TimestampNanosecondType>(
                        line_number,
                        rows,
                        i,
                        None,
                    )
                }
                DataType::Utf8 => Ok(Arc::new(
                    rows.iter().map(|row| row.get(i)).collect::<StringArray>(),
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
                            "Unsupported dictionary key type {:?}",
                            key_type
                        ))),
                    }
                }
                other => Err(ArrowError::ParseError(format!(
                    "Unsupported data type {:?}",
                    other
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

    arrays.and_then(|arr| RecordBatch::try_new(projected_schema, arr))
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
fn build_decimal_array(
    _line_number: usize,
    rows: &[StringRecord],
    col_idx: usize,
    precision: usize,
    scale: usize,
) -> Result<ArrayRef> {
    let mut decimal_builder = DecimalBuilder::new(rows.len(), precision, scale);
    for row in rows {
        let col_s = row.get(col_idx);
        match col_s {
            None => {
                // No data for this row
                decimal_builder.append_null()?;
            }
            Some(s) => {
                if s.is_empty() {
                    // append null
                    decimal_builder.append_null()?;
                } else {
                    let decimal_value: Result<i128> =
                        parse_decimal_with_parameter(s, precision, scale);
                    match decimal_value {
                        Ok(v) => {
                            decimal_builder.append_value(v)?;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }
    Ok(Arc::new(decimal_builder.finish()))
}

// Parse the string format decimal value to i128 format and checking the precision and scale.
// The result i128 value can't be out of bounds.
fn parse_decimal_with_parameter(s: &str, precision: usize, scale: usize) -> Result<i128> {
    if PARSE_DECIMAL_RE.is_match(s) {
        let mut offset = s.len();
        let len = s.len();
        let mut base = 1;

        // handle the value after the '.' and meet the scale
        let delimiter_position = s.find('.');
        match delimiter_position {
            None => {
                // there is no '.'
                base = 10_i128.pow(scale as u32);
            }
            Some(mid) => {
                // there is the '.'
                if len - mid >= scale + 1 {
                    // If the string value is "123.12345" and the scale is 2, we should just remain '.12' and drop the '345' value.
                    offset -= len - mid - 1 - scale;
                } else {
                    // If the string value is "123.12" and the scale is 4, we should append '00' to the tail.
                    base = 10_i128.pow((scale + 1 + mid - len) as u32);
                }
            }
        };

        // each byte is digit、'-' or '.'
        let bytes = s.as_bytes();
        let mut negative = false;
        let mut result: i128 = 0;

        bytes[0..offset].iter().rev().for_each(|&byte| match byte {
            b'-' => {
                negative = true;
            }
            b'0'..=b'9' => {
                result += i128::from(byte - b'0') * base;
                base *= 10;
            }
            // because of the PARSE_DECIMAL_RE, bytes just contains digit、'-' and '.'.
            _ => {}
        });

        if negative {
            result = result.neg();
        }
        validate_decimal_precision(result, precision)
            .map_err(|e| ArrowError::ParseError(format!("parse decimal overflow: {}", e)))
    } else {
        Err(ArrowError::ParseError(format!(
            "can't parse the string value {} to decimal",
            s
        )))
    }
}

// Parse the string format decimal value to i128 format without checking the precision and scale.
// Like "125.12" to 12512_i128.
#[cfg(test)]
fn parse_decimal(s: &str) -> Result<i128> {
    if PARSE_DECIMAL_RE.is_match(s) {
        let mut offset = s.len();
        // each byte is digit、'-' or '.'
        let bytes = s.as_bytes();
        let mut negative = false;
        let mut result: i128 = 0;
        let mut base = 1;
        while offset > 0 {
            match bytes[offset - 1] {
                b'-' => {
                    negative = true;
                }
                b'.' => {
                    // do nothing
                }
                b'0'..=b'9' => {
                    result += i128::from(bytes[offset - 1] - b'0') * base;
                    base *= 10;
                }
                _ => {
                    return Err(ArrowError::ParseError(format!(
                        "can't match byte {}",
                        bytes[offset - 1]
                    )));
                }
            }
            offset -= 1;
        }
        if negative {
            Ok(result.neg())
        } else {
            Ok(result)
        }
    } else {
        Err(ArrowError::ParseError(format!(
            "can't parse the string value {} to decimal",
            s
        )))
    }
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_primitive_array<T: ArrowPrimitiveType + Parser>(
    line_number: usize,
    rows: &[StringRecord],
    col_idx: usize,
    format: Option<&str>,
) -> Result<ArrayRef> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            match row.get(col_idx) {
                Some(s) => {
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
                }
                None => Ok(None),
            }
        })
        .collect::<Result<PrimitiveArray<T>>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_boolean_array(
    line_number: usize,
    rows: &[StringRecord],
    col_idx: usize,
) -> Result<ArrayRef> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            match row.get(col_idx) {
                Some(s) => {
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
                }
                None => Ok(None),
            }
        })
        .collect::<Result<BooleanArray>>()
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
    /// extern crate arrow;
    ///
    /// use arrow::csv;
    /// use std::fs::File;
    ///
    /// fn example() -> csv::Reader<File> {
    ///     let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = csv::ReaderBuilder::new().infer_schema(Some(100));
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

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R: Read + Seek>(self, mut reader: R) -> Result<Reader<R>> {
        // check if schema should be inferred
        let delimiter = self.delimiter.unwrap_or(b',');
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                let roptions = ReaderOptions {
                    delimiter: Some(delimiter),
                    max_read_records: self.max_records,
                    has_header: self.has_header,
                    escape: self.escape,
                    quote: self.quote,
                    terminator: self.terminator,
                    datetime_re: self.datetime_re,
                };
                let (inferred_schema, _) =
                    infer_file_schema_with_csv_options(&mut reader, roptions)?;

                Arc::new(inferred_schema)
            }
        };
        let csv_reader = Reader::build_csv_reader(
            reader,
            self.has_header,
            self.delimiter,
            self.escape,
            self.quote,
            self.terminator,
        );
        Ok(Reader::from_csv_reader(
            csv_reader,
            schema,
            self.has_header,
            self.batch_size,
            self.bounds,
            self.projection.clone(),
            self.datetime_format,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    use crate::array::*;
    use crate::compute::cast;
    use crate::datatypes::Field;
    use chrono::{prelude::*, LocalResult};

    #[test]
    fn test_csv() {
        let _: Vec<()> = vec![None, Some("%Y-%m-%dT%H:%M:%S%.f%:z".to_string())]
            .into_iter()
            .map(|format| {
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
            })
            .collect();
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
            Field::new("lat", DataType::Decimal(26, 6), false),
            Field::new("lng", DataType::Decimal(26, 6), false),
        ]);

        let file = File::open("test/data/decimal_test.csv").unwrap();

        let mut csv =
            Reader::new(file, Arc::new(schema), false, None, 1024, None, None, None);
        let batch = csv.next().unwrap().unwrap();
        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<DecimalArray>()
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
        let mut csv = Reader::from_reader(
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
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
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

        let strings = cast(batch.column(0), &DataType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(strings.value(0), "Elgin, Scotland, the UK");
        assert_eq!(strings.value(4), "Eastbourne, East Sussex, UK");
        assert_eq!(strings.value(29), "Uckfield, East Sussex, UK");
    }

    #[test]
    fn test_nulls() {
        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, false),
            Field::new("c_string", DataType::Utf8, false),
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
        assert_eq!(&DataType::Date64, schema.field(5).data_type());

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

        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(schema.field(2).is_nullable());
        assert!(!schema.field(3).is_nullable());
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
                    format!("{:?}", e)
                ),
                Ok(_) => panic!("should have failed"),
            },
            None => panic!("should have failed"),
        }
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
        assert_eq!(infer_field_schema("false", None), DataType::Boolean);
        assert_eq!(infer_field_schema("2020-11-08", None), DataType::Date32);
        assert_eq!(
            infer_field_schema("2020-11-08T14:20:01", None),
            DataType::Date64
        );
        // to be inferred as a date64 this needs a custom datetime_re
        assert_eq!(
            infer_field_schema("2020-11-08 14:20:01", None),
            DataType::Utf8
        );
        let reg = Regex::new(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$").ok();
        assert_eq!(
            infer_field_schema("2020-11-08 14:20:01", reg),
            DataType::Date64
        );
        assert_eq!(infer_field_schema("-5.13", None), DataType::Float64);
        assert_eq!(infer_field_schema("0.1300", None), DataType::Float64);
    }

    #[test]
    fn parse_date32() {
        assert_eq!(parse_item::<Date32Type>("1970-01-01").unwrap(), 0);
        assert_eq!(parse_item::<Date32Type>("2020-03-15").unwrap(), 18336);
        assert_eq!(parse_item::<Date32Type>("1945-05-08").unwrap(), -9004);
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
    fn test_parse_decimal() {
        let tests = [
            ("123.00", 12300i128),
            ("123.123", 123123i128),
            ("0.0123", 123i128),
            ("0.12300", 12300i128),
            ("-5.123", -5123i128),
            ("-45.432432", -45432432i128),
        ];
        for (s, i) in tests {
            let result = parse_decimal(s);
            assert_eq!(i, result.unwrap());
        }
    }

    #[test]
    fn test_parse_decimal_with_parameter() {
        let tests = [
            ("123.123", 123123i128),
            ("123.1234", 123123i128),
            ("123.1", 123100i128),
            ("123", 123000i128),
            ("-123.123", -123123i128),
            ("-123.1234", -123123i128),
            ("-123.1", -123100i128),
            ("-123", -123000i128),
            ("0.0000123", 0i128),
            ("12.", 12000i128),
            ("-12.", -12000i128),
            ("00.1", 100i128),
            ("-00.1", -100i128),
            ("12345678912345678.1234", 12345678912345678123i128),
            ("-12345678912345678.1234", -12345678912345678123i128),
            ("99999999999999999.999", 99999999999999999999i128),
            ("-99999999999999999.999", -99999999999999999999i128),
            (".123", 123i128),
            ("-.123", -123i128),
            ("123.", 123000i128),
            ("-123.", -123000i128),
        ];
        for (s, i) in tests {
            let result = parse_decimal_with_parameter(s, 20, 3);
            assert_eq!(i, result.unwrap())
        }
        let can_not_parse_tests = ["123,123", ".", "123.123.123"];
        for s in can_not_parse_tests {
            let result = parse_decimal_with_parameter(s, 20, 3);
            assert_eq!(
                format!(
                    "Parser error: can't parse the string value {} to decimal",
                    s
                ),
                result.unwrap_err().to_string()
            );
        }
        let overflow_parse_tests = ["12345678", "12345678.9", "99999999.99"];
        for s in overflow_parse_tests {
            let result = parse_decimal_with_parameter(s, 10, 3);
            let expected = "Parser error: parse decimal overflow";
            let actual = result.unwrap_err().to_string();

            assert!(
                actual.contains(&expected),
                "actual: '{}', expected: '{}'",
                actual,
                expected
            );
        }
    }

    /// Interprets a naive_datetime (with no explicit timezone offset)
    /// using the local timezone and returns the timestamp in UTC (0
    /// offset)
    fn naive_datetime_to_timestamp(naive_datetime: &NaiveDateTime) -> i64 {
        // Note: Use chrono APIs that are different than
        // naive_datetime_to_timestamp to compute the utc offset to
        // try and double check the logic
        let utc_offset_secs = match Local.offset_from_local_datetime(naive_datetime) {
            LocalResult::Single(local_offset) => {
                local_offset.fix().local_minus_utc() as i64
            }
            _ => panic!(
                "Unexpected failure converting {} to local datetime",
                naive_datetime
            ),
        };
        let utc_offset_nanos = utc_offset_secs * 1_000_000_000;
        naive_datetime.timestamp_nanos() - utc_offset_nanos
    }

    #[test]
    fn test_parse_timestamp_microseconds() {
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("1970-01-01T00:00:00Z").unwrap(),
            0
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2018, 11, 13),
            NaiveTime::from_hms_nano(17, 11, 10, 0),
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("2018-11-13T17:11:10").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime) / 1000
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("2018-11-13 17:11:10").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime) / 1000
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2018, 11, 13),
            NaiveTime::from_hms_nano(17, 11, 10, 11000000),
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("2018-11-13T17:11:10.011").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime) / 1000
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(1900, 2, 28),
            NaiveTime::from_hms_nano(12, 34, 56, 0),
        );
        assert_eq!(
            parse_item::<TimestampMicrosecondType>("1900-02-28T12:34:56").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime) / 1000
        );
    }

    #[test]
    fn test_parse_timestamp_nanoseconds() {
        assert_eq!(
            parse_item::<TimestampNanosecondType>("1970-01-01T00:00:00Z").unwrap(),
            0
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2018, 11, 13),
            NaiveTime::from_hms_nano(17, 11, 10, 0),
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("2018-11-13T17:11:10").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime)
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("2018-11-13 17:11:10").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime)
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2018, 11, 13),
            NaiveTime::from_hms_nano(17, 11, 10, 11000000),
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("2018-11-13T17:11:10.011").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime)
        );
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(1900, 2, 28),
            NaiveTime::from_hms_nano(12, 34, 56, 0),
        );
        assert_eq!(
            parse_item::<TimestampNanosecondType>("1900-02-28T12:34:56").unwrap(),
            naive_datetime_to_timestamp(&naive_datetime)
        );
    }

    #[test]
    fn test_infer_schema_from_multiple_files() -> Result<()> {
        let mut csv1 = NamedTempFile::new()?;
        let mut csv2 = NamedTempFile::new()?;
        let csv3 = NamedTempFile::new()?; // empty csv file should be skipped
        let mut csv4 = NamedTempFile::new()?;
        writeln!(csv1, "c1,c2,c3")?;
        writeln!(csv1, "1,\"foo\",0.5")?;
        writeln!(csv1, "3,\"bar\",1")?;
        writeln!(csv1, "3,\"bar\",2e-06")?;
        // reading csv2 will set c2 to optional
        writeln!(csv2, "c1,c2,c3,c4")?;
        writeln!(csv2, "10,,3.14,true")?;
        // reading csv4 will set c3 to optional
        writeln!(csv4, "c1,c2,c3")?;
        writeln!(csv4, "10,\"foo\",")?;

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
        )?;

        assert_eq!(schema.fields().len(), 4);
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert!(!schema.field(2).is_nullable());
        assert!(!schema.field(3).is_nullable());

        assert_eq!(&DataType::Int64, schema.field(0).data_type());
        assert_eq!(&DataType::Utf8, schema.field(1).data_type());
        assert_eq!(&DataType::Float64, schema.field(2).data_type());
        assert_eq!(&DataType::Boolean, schema.field(3).data_type());

        Ok(())
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
            let text1 = format!("id{:}", index);
            let text2 = format!("value{:}", index);
            csv_writer
                .write_fmt(format_args!("~{}~,~{}~\r\n", text1, text2))
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
            let text1 = format!("id{:}", index);
            let text2 = format!("value\\\"{:}", index);
            csv_writer
                .write_fmt(format_args!("\"{}\",\"{}\"\r\n", text1, text2))
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
            let text1 = format!("id{:}", index);
            let text2 = format!("value{:}", index);
            csv_writer
                .write_fmt(format_args!("\"{}\",\"{}\"\n", text1, text2))
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
}
