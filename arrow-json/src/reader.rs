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

//! # JSON Reader
//!
//! This JSON reader allows JSON line-delimited files to be read into the Arrow memory
//! model. Records are loaded in batches and are then converted from row-based data to
//! columnar data.
//!
//! Example:
//!
//! ```
//! # use arrow_schema::*;
//! # use std::fs::File;
//! # use std::io::BufReader;
//! # use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("a", DataType::Float64, false),
//!     Field::new("b", DataType::Float64, false),
//!     Field::new("c", DataType::Float64, true),
//! ]);
//!
//! let file = File::open("test/data/basic.json").unwrap();
//!
//! let mut json = arrow_json::Reader::new(
//!    BufReader::new(file),
//!    Arc::new(schema),
//!    arrow_json::reader::DecoderOptions::new(),
//! );
//!
//! let batch = json.next().unwrap().unwrap();
//! ```

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use serde_json::json;
use serde_json::{map::Map as JsonMap, Value};

use arrow_array::builder::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{bit_util, Buffer, MutableBuffer};
use arrow_cast::parse::Parser;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::*;

#[derive(Debug, Clone)]
enum InferredType {
    Scalar(HashSet<DataType>),
    Array(Box<InferredType>),
    Object(HashMap<String, InferredType>),
    Any,
}

impl InferredType {
    fn merge(&mut self, other: InferredType) -> Result<(), ArrowError> {
        match (self, other) {
            (InferredType::Array(s), InferredType::Array(o)) => {
                s.merge(*o)?;
            }
            (InferredType::Scalar(self_hs), InferredType::Scalar(other_hs)) => {
                other_hs.into_iter().for_each(|v| {
                    self_hs.insert(v);
                });
            }
            (InferredType::Object(self_map), InferredType::Object(other_map)) => {
                for (k, v) in other_map {
                    self_map.entry(k).or_insert(InferredType::Any).merge(v)?;
                }
            }
            (s @ InferredType::Any, v) => {
                *s = v;
            }
            (_, InferredType::Any) => {}
            // convert a scalar type to a single-item scalar array type.
            (
                InferredType::Array(self_inner_type),
                other_scalar @ InferredType::Scalar(_),
            ) => {
                self_inner_type.merge(other_scalar)?;
            }
            (s @ InferredType::Scalar(_), InferredType::Array(mut other_inner_type)) => {
                other_inner_type.merge(s.clone())?;
                *s = InferredType::Array(other_inner_type);
            }
            // incompatible types
            (s, o) => {
                return Err(ArrowError::JsonError(format!(
                    "Incompatible type found during schema inference: {:?} v.s. {:?}",
                    s, o,
                )));
            }
        }

        Ok(())
    }
}

/// Coerce data type during inference
///
/// * `Int64` and `Float64` should be `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * All other types are coerced to `Utf8`
fn coerce_data_type(dt: Vec<&DataType>) -> DataType {
    let mut dt_iter = dt.into_iter().cloned();
    let dt_init = dt_iter.next().unwrap_or(DataType::Utf8);

    dt_iter.fold(dt_init, |l, r| match (l, r) {
        (DataType::Boolean, DataType::Boolean) => DataType::Boolean,
        (DataType::Int64, DataType::Int64) => DataType::Int64,
        (DataType::Float64, DataType::Float64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Int64, DataType::Float64) => DataType::Float64,
        (DataType::List(l), DataType::List(r)) => DataType::List(Box::new(Field::new(
            "item",
            coerce_data_type(vec![l.data_type(), r.data_type()]),
            true,
        ))),
        // coerce scalar and scalar array into scalar array
        (DataType::List(e), not_list) | (not_list, DataType::List(e)) => {
            DataType::List(Box::new(Field::new(
                "item",
                coerce_data_type(vec![e.data_type(), &not_list]),
                true,
            )))
        }
        _ => DataType::Utf8,
    })
}

fn generate_datatype(t: &InferredType) -> Result<DataType, ArrowError> {
    Ok(match t {
        InferredType::Scalar(hs) => coerce_data_type(hs.iter().collect()),
        InferredType::Object(spec) => DataType::Struct(generate_fields(spec)?),
        InferredType::Array(ele_type) => DataType::List(Box::new(Field::new(
            "item",
            generate_datatype(ele_type)?,
            true,
        ))),
        InferredType::Any => DataType::Null,
    })
}

fn generate_fields(
    spec: &HashMap<String, InferredType>,
) -> Result<Vec<Field>, ArrowError> {
    spec.iter()
        .map(|(k, types)| Ok(Field::new(k, generate_datatype(types)?, true)))
        .collect()
}

/// Generate schema from JSON field names and inferred data types
fn generate_schema(spec: HashMap<String, InferredType>) -> Result<Schema, ArrowError> {
    Ok(Schema::new(generate_fields(&spec)?))
}

/// JSON file reader that produces a serde_json::Value iterator from a Read trait
///
/// # Example
///
/// ```
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow_json::reader::ValueIter;
///
/// let mut reader =
///     BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
/// let mut value_reader = ValueIter::new(&mut reader, None);
/// for value in value_reader {
///     println!("JSON value: {}", value.unwrap());
/// }
/// ```
#[derive(Debug)]
pub struct ValueIter<'a, R: Read> {
    reader: &'a mut BufReader<R>,
    max_read_records: Option<usize>,
    record_count: usize,
    // reuse line buffer to avoid allocation on each record
    line_buf: String,
}

impl<'a, R: Read> ValueIter<'a, R> {
    /// Creates a new `ValueIter`
    pub fn new(reader: &'a mut BufReader<R>, max_read_records: Option<usize>) -> Self {
        Self {
            reader,
            max_read_records,
            record_count: 0,
            line_buf: String::new(),
        }
    }
}

impl<'a, R: Read> Iterator for ValueIter<'a, R> {
    type Item = Result<Value, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(max) = self.max_read_records {
            if self.record_count >= max {
                return None;
            }
        }

        loop {
            self.line_buf.truncate(0);
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => {
                    // read_line returns 0 when stream reached EOF
                    return None;
                }
                Err(e) => {
                    return Some(Err(ArrowError::JsonError(format!(
                        "Failed to read JSON record: {}",
                        e
                    ))));
                }
                _ => {
                    let trimmed_s = self.line_buf.trim();
                    if trimmed_s.is_empty() {
                        // ignore empty lines
                        continue;
                    }

                    self.record_count += 1;
                    return Some(serde_json::from_str(trimmed_s).map_err(|e| {
                        ArrowError::JsonError(format!("Not valid JSON: {}", e))
                    }));
                }
            }
        }
    }
}

/// Infer the fields of a JSON file by reading the first n records of the file, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// Contrary to [`infer_json_schema`], this function will seek back to the start of the `reader`.
/// That way, the `reader` can be used immediately afterwards to create a [`Reader`].
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow_json::reader::infer_json_schema_from_seekable;
///
/// let file = File::open("test/data/mixed_arrays.json").unwrap();
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(file);
/// let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();
/// // file's cursor's offset automatically set at 0
/// ```
pub fn infer_json_schema_from_seekable<R: Read + Seek>(
    reader: &mut BufReader<R>,
    max_read_records: Option<usize>,
) -> Result<Schema, ArrowError> {
    let schema = infer_json_schema(reader, max_read_records);
    // return the reader seek back to the start
    reader.seek(SeekFrom::Start(0))?;

    schema
}

/// Infer the fields of a JSON file by reading the first n records of the buffer, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// This function will not seek back to the start of the `reader`. The user has to manage the
/// original file's cursor. This function is useful when the `reader`'s cursor is not available
/// (does not implement [`Seek`]), such is the case for compressed streams decoders.
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::{BufReader, SeekFrom, Seek};
/// use flate2::read::GzDecoder;
/// use arrow_json::reader::infer_json_schema;
///
/// let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
///
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(GzDecoder::new(&file));
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// // cursor's offset at end of file
///
/// // seek back to start so that the original file is usable again
/// file.seek(SeekFrom::Start(0)).unwrap();
/// ```
pub fn infer_json_schema<R: Read>(
    reader: &mut BufReader<R>,
    max_read_records: Option<usize>,
) -> Result<Schema, ArrowError> {
    infer_json_schema_from_iterator(ValueIter::new(reader, max_read_records))
}

fn set_object_scalar_field_type(
    field_types: &mut HashMap<String, InferredType>,
    key: &str,
    ftype: DataType,
) -> Result<(), ArrowError> {
    if !field_types.contains_key(key) {
        field_types.insert(key.to_string(), InferredType::Scalar(HashSet::new()));
    }

    match field_types.get_mut(key).unwrap() {
        InferredType::Scalar(hs) => {
            hs.insert(ftype);
            Ok(())
        }
        // in case of column contains both scalar type and scalar array type, we convert type of
        // this column to scalar array.
        scalar_array @ InferredType::Array(_) => {
            let mut hs = HashSet::new();
            hs.insert(ftype);
            scalar_array.merge(InferredType::Scalar(hs))?;
            Ok(())
        }
        t => Err(ArrowError::JsonError(format!(
            "Expected scalar or scalar array JSON type, found: {:?}",
            t,
        ))),
    }
}

fn infer_scalar_array_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    let mut hs = HashSet::new();

    for v in array {
        match v {
            Value::Null => {}
            Value::Number(n) => {
                if n.is_i64() {
                    hs.insert(DataType::Int64);
                } else {
                    hs.insert(DataType::Float64);
                }
            }
            Value::Bool(_) => {
                hs.insert(DataType::Boolean);
            }
            Value::String(_) => {
                hs.insert(DataType::Utf8);
            }
            Value::Array(_) | Value::Object(_) => {
                return Err(ArrowError::JsonError(format!(
                    "Expected scalar value for scalar array, got: {:?}",
                    v
                )));
            }
        }
    }

    Ok(InferredType::Scalar(hs))
}

fn infer_nested_array_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    let mut inner_ele_type = InferredType::Any;

    for v in array {
        match v {
            Value::Array(inner_array) => {
                inner_ele_type.merge(infer_array_element_type(inner_array)?)?;
            }
            x => {
                return Err(ArrowError::JsonError(format!(
                    "Got non array element in nested array: {:?}",
                    x
                )));
            }
        }
    }

    Ok(InferredType::Array(Box::new(inner_ele_type)))
}

fn infer_struct_array_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    let mut field_types = HashMap::new();

    for v in array {
        match v {
            Value::Object(map) => {
                collect_field_types_from_object(&mut field_types, map)?;
            }
            _ => {
                return Err(ArrowError::JsonError(format!(
                    "Expected struct value for struct array, got: {:?}",
                    v
                )));
            }
        }
    }

    Ok(InferredType::Object(field_types))
}

fn infer_array_element_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    match array.iter().take(1).next() {
        None => Ok(InferredType::Any), // empty array, return any type that can be updated later
        Some(a) => match a {
            Value::Array(_) => infer_nested_array_type(array),
            Value::Object(_) => infer_struct_array_type(array),
            _ => infer_scalar_array_type(array),
        },
    }
}

fn collect_field_types_from_object(
    field_types: &mut HashMap<String, InferredType>,
    map: &JsonMap<String, Value>,
) -> Result<(), ArrowError> {
    for (k, v) in map {
        match v {
            Value::Array(array) => {
                let ele_type = infer_array_element_type(array)?;

                if !field_types.contains_key(k) {
                    match ele_type {
                        InferredType::Scalar(_) => {
                            field_types.insert(
                                k.to_string(),
                                InferredType::Array(Box::new(InferredType::Scalar(
                                    HashSet::new(),
                                ))),
                            );
                        }
                        InferredType::Object(_) => {
                            field_types.insert(
                                k.to_string(),
                                InferredType::Array(Box::new(InferredType::Object(
                                    HashMap::new(),
                                ))),
                            );
                        }
                        InferredType::Any | InferredType::Array(_) => {
                            // set inner type to any for nested array as well
                            // so it can be updated properly from subsequent type merges
                            field_types.insert(
                                k.to_string(),
                                InferredType::Array(Box::new(InferredType::Any)),
                            );
                        }
                    }
                }

                match field_types.get_mut(k).unwrap() {
                    InferredType::Array(inner_type) => {
                        inner_type.merge(ele_type)?;
                    }
                    // in case of column contains both scalar type and scalar array type, we
                    // convert type of this column to scalar array.
                    field_type @ InferredType::Scalar(_) => {
                        field_type.merge(ele_type)?;
                        *field_type = InferredType::Array(Box::new(field_type.clone()));
                    }
                    t => {
                        return Err(ArrowError::JsonError(format!(
                            "Expected array json type, found: {:?}",
                            t,
                        )));
                    }
                }
            }
            Value::Bool(_) => {
                set_object_scalar_field_type(field_types, k, DataType::Boolean)?;
            }
            Value::Null => {
                // do nothing, we treat json as nullable by default when
                // inferring
            }
            Value::Number(n) => {
                if n.is_f64() {
                    set_object_scalar_field_type(field_types, k, DataType::Float64)?;
                } else {
                    // default to i64
                    set_object_scalar_field_type(field_types, k, DataType::Int64)?;
                }
            }
            Value::String(_) => {
                set_object_scalar_field_type(field_types, k, DataType::Utf8)?;
            }
            Value::Object(inner_map) => {
                if !field_types.contains_key(k) {
                    field_types
                        .insert(k.to_string(), InferredType::Object(HashMap::new()));
                }
                match field_types.get_mut(k).unwrap() {
                    InferredType::Object(inner_field_types) => {
                        collect_field_types_from_object(inner_field_types, inner_map)?;
                    }
                    t => {
                        return Err(ArrowError::JsonError(format!(
                            "Expected object json type, found: {:?}",
                            t,
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Infer the fields of a JSON file by reading all items from the JSON Value Iterator.
///
/// The following type coercion logic is implemented:
/// * `Int64` and `Float64` are converted to `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * All other cases are coerced to `Utf8` (String)
///
/// Note that the above coercion logic is different from what Spark has, where it would default to
/// String type in case of List and Scalar values appeared in the same field.
///
/// The reason we diverge here is because we don't have utilities to deal with JSON data once it's
/// interpreted as Strings. We should match Spark's behavior once we added more JSON parsing
/// kernels in the future.
pub fn infer_json_schema_from_iterator<I>(value_iter: I) -> Result<Schema, ArrowError>
where
    I: Iterator<Item = Result<Value, ArrowError>>,
{
    let mut field_types: HashMap<String, InferredType> = HashMap::new();

    for record in value_iter {
        match record? {
            Value::Object(map) => {
                collect_field_types_from_object(&mut field_types, &map)?;
            }
            value => {
                return Err(ArrowError::JsonError(format!(
                    "Expected JSON record to be an object, found {:?}",
                    value
                )));
            }
        };
    }

    generate_schema(field_types)
}

/// JSON values to Arrow record batch decoder.
///
/// A [`Decoder`] decodes arbitrary streams of [`serde_json::Value`]s and
/// converts them to [`RecordBatch`]es. To decode JSON formatted files,
/// see [`Reader`].
///
/// # Examples
/// ```
/// use arrow_json::reader::{Decoder, DecoderOptions, ValueIter, infer_json_schema};
/// use std::fs::File;
/// use std::io::{BufReader, Seek, SeekFrom};
/// use std::sync::Arc;
///
/// let mut reader =
///     BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// let options = DecoderOptions::new()
///     .with_batch_size(1024);
/// let decoder = Decoder::new(Arc::new(inferred_schema), options);
///
/// // seek back to start so that the original file is usable again
/// reader.seek(SeekFrom::Start(0)).unwrap();
/// let mut value_reader = ValueIter::new(&mut reader, None);
/// let batch = decoder.next_batch(&mut value_reader).unwrap().unwrap();
/// assert_eq!(4, batch.num_rows());
/// assert_eq!(4, batch.num_columns());
/// ```
#[derive(Debug)]
pub struct Decoder {
    /// Explicit schema for the JSON file
    schema: SchemaRef,
    /// This is a collection of options for json decoder
    options: DecoderOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Options for JSON decoding
pub struct DecoderOptions {
    /// Batch size (number of records to load each time), defaults to 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (case-sensitive names)
    projection: Option<Vec<String>>,
    /// optional HashMap of column name to its format string
    format_strings: Option<HashMap<String, String>>,
}

impl Default for DecoderOptions {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            projection: None,
            format_strings: None,
        }
    }
}

impl DecoderOptions {
    /// Creates a new `DecoderOptions`
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the decoder's format Strings param
    pub fn with_format_strings(
        mut self,
        format_strings: HashMap<String, String>,
    ) -> Self {
        self.format_strings = Some(format_strings);
        self
    }
}

impl Decoder {
    /// Create a new JSON decoder from some value that implements an
    /// iterator over [`serde_json::Value`]s (aka implements the
    /// `Iterator<Item=Result<Value>>` trait).
    pub fn new(schema: SchemaRef, options: DecoderOptions) -> Self {
        Self { schema, options }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.options.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> = fields
                    .iter()
                    .filter_map(|field| {
                        if projection.contains(field.name()) {
                            Some(field.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }

    /// Read the next batch of [`serde_json::Value`] records from the
    /// interator into a [`RecordBatch`].
    ///
    /// Returns `None` if the input iterator is exhausted.
    pub fn next_batch<I>(
        &self,
        value_iter: &mut I,
    ) -> Result<Option<RecordBatch>, ArrowError>
    where
        I: Iterator<Item = Result<Value, ArrowError>>,
    {
        let batch_size = self.options.batch_size;
        let mut rows: Vec<Value> = Vec::with_capacity(batch_size);

        for value in value_iter.by_ref().take(batch_size) {
            let v = value?;
            match v {
                Value::Object(_) => rows.push(v),
                _ => {
                    return Err(ArrowError::JsonError(format!(
                        "Row needs to be of type object, got: {:?}",
                        v
                    )));
                }
            }
        }
        if rows.is_empty() {
            // reached end of file
            return Ok(None);
        }

        let rows = &rows[..];

        let arrays =
            self.build_struct_array(rows, self.schema.fields(), &self.options.projection);

        let projected_fields = if let Some(projection) = self.options.projection.as_ref()
        {
            projection
                .iter()
                .filter_map(|name| self.schema.column_with_name(name))
                .map(|(_, field)| field.clone())
                .collect()
        } else {
            self.schema.fields().to_vec()
        };

        let projected_schema = Arc::new(Schema::new(projected_fields));

        arrays.and_then(|arr| {
            RecordBatch::try_new_with_options(
                projected_schema,
                arr,
                &RecordBatchOptions::new()
                    .with_match_field_names(true)
                    .with_row_count(Some(rows.len())),
            )
            .map(Some)
        })
    }

    fn build_wrapped_list_array(
        &self,
        rows: &[Value],
        col_name: &str,
        key_type: &DataType,
    ) -> Result<ArrayRef, ArrowError> {
        match *key_type {
            DataType::Int8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int8Type>(&dtype, col_name, rows)
            }
            DataType::Int16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int16Type>(&dtype, col_name, rows)
            }
            DataType::Int32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int32Type>(&dtype, col_name, rows)
            }
            DataType::Int64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int64Type>(&dtype, col_name, rows)
            }
            DataType::UInt8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt8Type>(&dtype, col_name, rows)
            }
            DataType::UInt16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt16Type>(&dtype, col_name, rows)
            }
            DataType::UInt32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt32Type>(&dtype, col_name, rows)
            }
            DataType::UInt64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt64Type>(&dtype, col_name, rows)
            }
            ref e => Err(ArrowError::JsonError(format!(
                "Data type is currently not supported for dictionaries in list : {:?}",
                e
            ))),
        }
    }

    #[inline(always)]
    fn list_array_string_array_builder<DT>(
        &self,
        data_type: &DataType,
        col_name: &str,
        rows: &[Value],
    ) -> Result<ArrayRef, ArrowError>
    where
        DT: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Utf8 => {
                let values_builder =
                    StringBuilder::with_capacity(rows.len(), rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            DataType::Dictionary(_, _) => {
                let values_builder =
                    self.build_string_dictionary_builder::<DT>(rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            e => {
                return Err(ArrowError::JsonError(format!(
                    "Nested list data builder type is not supported: {:?}",
                    e
                )))
            }
        };

        for row in rows {
            if let Some(value) = row.get(col_name) {
                // value can be an array or a scalar
                let vals: Vec<Option<String>> = if let Value::String(v) = value {
                    vec![Some(v.to_string())]
                } else if let Value::Array(n) = value {
                    n.iter()
                        .map(|v: &Value| {
                            if v.is_string() {
                                Some(v.as_str().unwrap().to_string())
                            } else if v.is_array() || v.is_object() || v.is_null() {
                                // implicitly drop nested values
                                // TODO support deep-nesting
                                None
                            } else {
                                Some(v.to_string())
                            }
                        })
                        .collect()
                } else if let Value::Null = value {
                    vec![None]
                } else if !value.is_object() {
                    vec![Some(value.to_string())]
                } else {
                    return Err(ArrowError::JsonError(
                        "Only scalars are currently supported in JSON arrays".to_string(),
                    ));
                };

                // TODO: ARROW-10335: APIs of dictionary arrays and others are different. Unify
                // them.
                match data_type {
                    DataType::Utf8 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .ok_or_else(||ArrowError::JsonError(
                                "Cast failed for ListBuilder<StringBuilder> during nested data parsing".to_string(),
                            ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append_value(&v);
                            } else {
                                builder.values().append_null();
                            };
                        }

                        // Append to the list
                        builder.append(true);
                    }
                    DataType::Dictionary(_, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<ListBuilder<StringDictionaryBuilder<DT>>>().ok_or_else(||ArrowError::JsonError(
                            "Cast failed for ListBuilder<StringDictionaryBuilder> during nested data parsing".to_string(),
                        ))?;
                        for val in vals {
                            if let Some(v) = val {
                                let _ = builder.values().append(&v);
                            } else {
                                builder.values().append_null();
                            };
                        }

                        // Append to the list
                        builder.append(true);
                    }
                    e => {
                        return Err(ArrowError::JsonError(format!(
                            "Nested list data builder type is not supported: {:?}",
                            e
                        )))
                    }
                }
            }
        }

        Ok(builder.finish() as ArrayRef)
    }

    #[inline(always)]
    fn build_string_dictionary_builder<T>(
        &self,
        row_len: usize,
    ) -> StringDictionaryBuilder<T>
    where
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        StringDictionaryBuilder::with_capacity(row_len, row_len, row_len * 5)
    }

    #[inline(always)]
    fn build_string_dictionary_array(
        &self,
        rows: &[Value],
        col_name: &str,
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<ArrayRef, ArrowError> {
        if let DataType::Utf8 = *value_type {
            match *key_type {
                DataType::Int8 => self.build_dictionary_array::<Int8Type>(rows, col_name),
                DataType::Int16 => {
                    self.build_dictionary_array::<Int16Type>(rows, col_name)
                }
                DataType::Int32 => {
                    self.build_dictionary_array::<Int32Type>(rows, col_name)
                }
                DataType::Int64 => {
                    self.build_dictionary_array::<Int64Type>(rows, col_name)
                }
                DataType::UInt8 => {
                    self.build_dictionary_array::<UInt8Type>(rows, col_name)
                }
                DataType::UInt16 => {
                    self.build_dictionary_array::<UInt16Type>(rows, col_name)
                }
                DataType::UInt32 => {
                    self.build_dictionary_array::<UInt32Type>(rows, col_name)
                }
                DataType::UInt64 => {
                    self.build_dictionary_array::<UInt64Type>(rows, col_name)
                }
                _ => Err(ArrowError::JsonError(
                    "unsupported dictionary key type".to_string(),
                )),
            }
        } else {
            Err(ArrowError::JsonError(
                "dictionary types other than UTF-8 not yet supported".to_string(),
            ))
        }
    }

    fn build_boolean_array(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef, ArrowError> {
        let mut builder = BooleanBuilder::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = row.get(col_name) {
                if let Some(boolean) = value.as_bool() {
                    builder.append_value(boolean);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_primitive_array<T: ArrowPrimitiveType + Parser>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef, ArrowError>
    where
        T: ArrowPrimitiveType,
        T::Native: num::NumCast,
    {
        let format_string = self
            .options
            .format_strings
            .as_ref()
            .and_then(|fmts| fmts.get(col_name));
        Ok(Arc::new(
            rows.iter()
                .map(|row| {
                    row.get(col_name).and_then(|value| {
                        if value.is_i64() {
                            value.as_i64().and_then(num::cast::cast)
                        } else if value.is_u64() {
                            value.as_u64().and_then(num::cast::cast)
                        } else if value.is_string() {
                            match format_string {
                                Some(fmt) => {
                                    T::parse_formatted(value.as_str().unwrap(), fmt)
                                }
                                None => T::parse(value.as_str().unwrap()),
                            }
                        } else {
                            value.as_f64().and_then(num::cast::cast)
                        }
                    })
                })
                .collect::<PrimitiveArray<T>>(),
        ))
    }

    /// Build a nested GenericListArray from a list of unnested `Value`s
    fn build_nested_list_array<OffsetSize: OffsetSizeTrait>(
        &self,
        rows: &[Value],
        list_field: &Field,
    ) -> Result<ArrayRef, ArrowError> {
        // build list offsets
        let mut cur_offset = OffsetSize::zero();
        let list_len = rows.len();
        let num_list_bytes = bit_util::ceil(list_len, 8);
        let mut offsets = Vec::with_capacity(list_len + 1);
        let mut list_nulls = MutableBuffer::from_len_zeroed(num_list_bytes);
        let list_nulls = list_nulls.as_slice_mut();
        offsets.push(cur_offset);
        rows.iter().enumerate().for_each(|(i, v)| {
            if let Value::Array(a) = v {
                cur_offset += OffsetSize::from_usize(a.len()).unwrap();
                bit_util::set_bit(list_nulls, i);
            } else if let Value::Null = v {
                // value is null, not incremented
            } else {
                cur_offset += OffsetSize::one();
            }
            offsets.push(cur_offset);
        });
        let valid_len = cur_offset.to_usize().unwrap();
        let array_data = match list_field.data_type() {
            DataType::Null => NullArray::new(valid_len).into_data(),
            DataType::Boolean => {
                let num_bytes = bit_util::ceil(valid_len, 8);
                let mut bool_values = MutableBuffer::from_len_zeroed(num_bytes);
                let mut bool_nulls =
                    MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                let mut curr_index = 0;
                rows.iter().for_each(|v| {
                    if let Value::Array(vs) = v {
                        vs.iter().for_each(|value| {
                            if let Value::Bool(child) = value {
                                // if valid boolean, append value
                                if *child {
                                    bit_util::set_bit(
                                        bool_values.as_slice_mut(),
                                        curr_index,
                                    );
                                }
                            } else {
                                // null slot
                                bit_util::unset_bit(
                                    bool_nulls.as_slice_mut(),
                                    curr_index,
                                );
                            }
                            curr_index += 1;
                        });
                    }
                });
                unsafe {
                    ArrayData::builder(list_field.data_type().clone())
                        .len(valid_len)
                        .add_buffer(bool_values.into())
                        .null_bit_buffer(Some(bool_nulls.into()))
                        .build_unchecked()
                }
            }
            DataType::Int8 => self.read_primitive_list_values::<Int8Type>(rows),
            DataType::Int16 => self.read_primitive_list_values::<Int16Type>(rows),
            DataType::Int32 => self.read_primitive_list_values::<Int32Type>(rows),
            DataType::Int64 => self.read_primitive_list_values::<Int64Type>(rows),
            DataType::UInt8 => self.read_primitive_list_values::<UInt8Type>(rows),
            DataType::UInt16 => self.read_primitive_list_values::<UInt16Type>(rows),
            DataType::UInt32 => self.read_primitive_list_values::<UInt32Type>(rows),
            DataType::UInt64 => self.read_primitive_list_values::<UInt64Type>(rows),
            DataType::Float16 => {
                return Err(ArrowError::JsonError("Float16 not supported".to_string()))
            }
            DataType::Float32 => self.read_primitive_list_values::<Float32Type>(rows),
            DataType::Float64 => self.read_primitive_list_values::<Float64Type>(rows),
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => {
                return Err(ArrowError::JsonError(
                    "Temporal types are not yet supported, see ARROW-4803".to_string(),
                ))
            }
            DataType::Utf8 => flatten_json_string_values(rows)
                .into_iter()
                .collect::<StringArray>()
                .data()
                .clone(),
            DataType::LargeUtf8 => flatten_json_string_values(rows)
                .into_iter()
                .collect::<LargeStringArray>()
                .data()
                .clone(),
            DataType::List(field) => {
                let child = self
                    .build_nested_list_array::<i32>(&flatten_json_values(rows), field)?;
                child.into_data()
            }
            DataType::LargeList(field) => {
                let child = self
                    .build_nested_list_array::<i64>(&flatten_json_values(rows), field)?;
                child.into_data()
            }
            DataType::Struct(fields) => {
                // extract list values, with non-lists converted to Value::Null
                let array_item_count = cur_offset.to_usize().unwrap();
                let num_bytes = bit_util::ceil(array_item_count, 8);
                let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                let mut struct_index = 0;
                let rows: Vec<Value> = rows
                    .iter()
                    .flat_map(|row| match row {
                        Value::Array(values) if !values.is_empty() => {
                            values.iter().for_each(|value| {
                                if !value.is_null() {
                                    bit_util::set_bit(
                                        null_buffer.as_slice_mut(),
                                        struct_index,
                                    );
                                }
                                struct_index += 1;
                            });
                            values.clone()
                        }
                        _ => {
                            vec![]
                        }
                    })
                    .collect();
                let arrays =
                    self.build_struct_array(rows.as_slice(), fields.as_slice(), &None)?;
                let data_type = DataType::Struct(fields.clone());
                let buf = null_buffer.into();
                unsafe {
                    ArrayDataBuilder::new(data_type)
                        .len(rows.len())
                        .null_bit_buffer(Some(buf))
                        .child_data(arrays.into_iter().map(|a| a.into_data()).collect())
                        .build_unchecked()
                }
            }
            datatype => {
                return Err(ArrowError::JsonError(format!(
                    "Nested list of {:?} not supported",
                    datatype
                )));
            }
        };
        // build list
        let list_data = ArrayData::builder(DataType::List(Box::new(list_field.clone())))
            .len(list_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(array_data)
            .null_bit_buffer(Some(list_nulls.into()));
        let list_data = unsafe { list_data.build_unchecked() };
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
    }

    /// Builds the child values of a `StructArray`, falling short of constructing the StructArray.
    /// The function does not construct the StructArray as some callers would want the child arrays.
    ///
    /// *Note*: The function is recursive, and will read nested structs.
    ///
    /// If `projection` is &None, then all values are returned. The first level of projection
    /// occurs at the `RecordBatch` level. No further projection currently occurs, but would be
    /// useful if plucking values from a struct, e.g. getting `a.b.c.e` from `a.b.c.{d, e}`.
    fn build_struct_array(
        &self,
        rows: &[Value],
        struct_fields: &[Field],
        projection: &Option<Vec<String>>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        let arrays: Result<Vec<ArrayRef>, ArrowError> = struct_fields
            .iter()
            .filter(|field| {
                projection
                    .as_ref()
                    .map(|p| p.contains(field.name()))
                    .unwrap_or(true)
            })
            .map(|field| {
                match field.data_type() {
                    DataType::Null => {
                        Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef)
                    }
                    DataType::Boolean => self.build_boolean_array(rows, field.name()),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, field.name())
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, field.name())
                    }
                    DataType::Int64 => {
                        self.build_primitive_array::<Int64Type>(rows, field.name())
                    }
                    DataType::Int32 => {
                        self.build_primitive_array::<Int32Type>(rows, field.name())
                    }
                    DataType::Int16 => {
                        self.build_primitive_array::<Int16Type>(rows, field.name())
                    }
                    DataType::Int8 => {
                        self.build_primitive_array::<Int8Type>(rows, field.name())
                    }
                    DataType::UInt64 => {
                        self.build_primitive_array::<UInt64Type>(rows, field.name())
                    }
                    DataType::UInt32 => {
                        self.build_primitive_array::<UInt32Type>(rows, field.name())
                    }
                    DataType::UInt16 => {
                        self.build_primitive_array::<UInt16Type>(rows, field.name())
                    }
                    DataType::UInt8 => {
                        self.build_primitive_array::<UInt8Type>(rows, field.name())
                    }
                    // TODO: this is incomplete
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<TimestampSecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<TimestampMicrosecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<TimestampMillisecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<TimestampNanosecondType>(
                                rows,
                                field.name(),
                            ),
                    },
                    DataType::Date64 => {
                        self.build_primitive_array::<Date64Type>(rows, field.name())
                    }
                    DataType::Date32 => {
                        self.build_primitive_array::<Date32Type>(rows, field.name())
                    }
                    DataType::Time64(unit) => match unit {
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<Time64MicrosecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<Time64NanosecondType>(
                                rows,
                                field.name(),
                            ),
                        t => Err(ArrowError::JsonError(format!(
                            "TimeUnit {:?} not supported with Time64",
                            t
                        ))),
                    },
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<Time32SecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<Time32MillisecondType>(
                                rows,
                                field.name(),
                            ),
                        t => Err(ArrowError::JsonError(format!(
                            "TimeUnit {:?} not supported with Time32",
                            t
                        ))),
                    },
                    DataType::Utf8 => Ok(Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = row.get(field.name());
                                maybe_value.and_then(|value| value.as_str())
                            })
                            .collect::<StringArray>(),
                    ) as ArrayRef),
                    DataType::Binary => Ok(Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = row.get(field.name());
                                maybe_value.and_then(|value| value.as_str())
                            })
                            .collect::<BinaryArray>(),
                    ) as ArrayRef),
                    DataType::List(ref list_field) => {
                        match list_field.data_type() {
                            DataType::Dictionary(ref key_ty, _) => {
                                self.build_wrapped_list_array(rows, field.name(), key_ty)
                            }
                            _ => {
                                // extract rows by name
                                let extracted_rows = rows
                                    .iter()
                                    .map(|row| {
                                        row.get(field.name())
                                            .cloned()
                                            .unwrap_or(Value::Null)
                                    })
                                    .collect::<Vec<Value>>();
                                self.build_nested_list_array::<i32>(
                                    extracted_rows.as_slice(),
                                    list_field,
                                )
                            }
                        }
                    }
                    DataType::Dictionary(ref key_ty, ref val_ty) => self
                        .build_string_dictionary_array(
                            rows,
                            field.name(),
                            key_ty,
                            val_ty,
                        ),
                    DataType::Struct(fields) => {
                        let len = rows.len();
                        let num_bytes = bit_util::ceil(len, 8);
                        let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                        let struct_rows = rows
                            .iter()
                            .enumerate()
                            .map(|(i, row)| {
                                (i, row.as_object().and_then(|v| v.get(field.name())))
                            })
                            .map(|(i, v)| match v {
                                // we want the field as an object, if it's not, we treat as null
                                Some(Value::Object(value)) => {
                                    bit_util::set_bit(null_buffer.as_slice_mut(), i);
                                    Value::Object(value.clone())
                                }
                                _ => Value::Object(Default::default()),
                            })
                            .collect::<Vec<Value>>();
                        let arrays =
                            self.build_struct_array(&struct_rows, fields, &None)?;
                        // construct a struct array's data in order to set null buffer
                        let data_type = DataType::Struct(fields.clone());
                        let data = ArrayDataBuilder::new(data_type)
                            .len(len)
                            .null_bit_buffer(Some(null_buffer.into()))
                            .child_data(
                                arrays.into_iter().map(|a| a.into_data()).collect(),
                            );
                        let data = unsafe { data.build_unchecked() };
                        Ok(make_array(data))
                    }
                    DataType::Map(map_field, _) => self.build_map_array(
                        rows,
                        field.name(),
                        field.data_type(),
                        map_field,
                    ),
                    _ => Err(ArrowError::JsonError(format!(
                        "{:?} type is not supported",
                        field.data_type()
                    ))),
                }
            })
            .collect();
        arrays
    }

    fn build_map_array(
        &self,
        rows: &[Value],
        field_name: &str,
        map_type: &DataType,
        struct_field: &Field,
    ) -> Result<ArrayRef, ArrowError> {
        // A map has the format {"key": "value"} where key is most commonly a string,
        // but could be a string, number or boolean (🤷🏾‍♂️) (e.g. {1: "value"}).
        // A map is also represented as a flattened contiguous array, with the number
        // of key-value pairs being separated by a list offset.
        // If row 1 has 2 key-value pairs, and row 2 has 3, the offsets would be
        // [0, 2, 5].
        //
        // Thus we try to read a map by iterating through the keys and values

        let (key_field, value_field) =
            if let DataType::Struct(fields) = struct_field.data_type() {
                if fields.len() != 2 {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "DataType::Map expects a struct with 2 fields, found {} fields",
                        fields.len()
                    )));
                }
                (&fields[0], &fields[1])
            } else {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "JSON map array builder expects a DataType::Map, found {:?}",
                    struct_field.data_type()
                )));
            };
        let value_map_iter = rows.iter().map(|value| {
            value
                .get(field_name)
                .and_then(|v| v.as_object().map(|map| (map, map.len() as i32)))
        });
        let rows_len = rows.len();
        let mut list_offsets = Vec::with_capacity(rows_len + 1);
        list_offsets.push(0i32);
        let mut last_offset = 0;
        let num_bytes = bit_util::ceil(rows_len, 8);
        let mut list_bitmap = MutableBuffer::from_len_zeroed(num_bytes);
        let null_data = list_bitmap.as_slice_mut();

        let struct_rows = value_map_iter
            .enumerate()
            .filter_map(|(i, v)| match v {
                Some((map, len)) => {
                    list_offsets.push(last_offset + len);
                    last_offset += len;
                    bit_util::set_bit(null_data, i);
                    Some(map.iter().map(|(k, v)| {
                        json!({
                            key_field.name(): k,
                            value_field.name(): v
                        })
                    }))
                }
                None => {
                    list_offsets.push(last_offset);
                    None
                }
            })
            .flatten()
            .collect::<Vec<Value>>();

        let struct_children = self.build_struct_array(
            struct_rows.as_slice(),
            &[key_field.clone(), value_field.clone()],
            &None,
        )?;

        unsafe {
            Ok(make_array(ArrayData::new_unchecked(
                map_type.clone(),
                rows_len,
                None,
                Some(list_bitmap.into()),
                0,
                vec![Buffer::from_slice_ref(&list_offsets)],
                vec![ArrayData::new_unchecked(
                    struct_field.data_type().clone(),
                    struct_children[0].len(),
                    None,
                    None,
                    0,
                    vec![],
                    struct_children
                        .into_iter()
                        .map(|array| array.into_data())
                        .collect(),
                )],
            )))
        }
    }

    #[inline(always)]
    fn build_dictionary_array<T>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef, ArrowError>
    where
        T::Native: num::NumCast,
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: StringDictionaryBuilder<T> =
            self.build_string_dictionary_builder(rows.len());
        for row in rows {
            if let Some(value) = row.get(col_name) {
                if let Some(str_v) = value.as_str() {
                    builder.append(str_v).map(drop)?
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    /// Read the primitive list's values into ArrayData
    fn read_primitive_list_values<T>(&self, rows: &[Value]) -> ArrayData
    where
        T: ArrowPrimitiveType,
        T::Native: num::NumCast,
    {
        let values = rows
            .iter()
            .flat_map(|row| {
                // read values from list
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(|value| {
                            let v: Option<T::Native> =
                                value.as_f64().and_then(num::cast::cast);
                            v
                        })
                        .collect::<Vec<Option<T::Native>>>()
                } else if let Value::Number(value) = row {
                    // handle the scalar number case
                    let v: Option<T::Native> = value.as_f64().and_then(num::cast::cast);
                    v.map(|v| vec![Some(v)]).unwrap_or_default()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Option<T::Native>>>();
        let array = values.iter().collect::<PrimitiveArray<T>>();
        array.into_data()
    }
}

/// Reads a JSON value as a string, regardless of its type.
/// This is useful if the expected datatype is a string, in which case we preserve
/// all the values regardless of they type.
///
/// Applying `value.to_string()` unfortunately results in an escaped string, which
/// is not what we want.
#[inline(always)]
fn json_value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(string) => Some(string.clone()),
        _ => Some(value.to_string()),
    }
}

/// Flattens a list of JSON values, by flattening lists, and treating all other values as
/// single-value lists.
/// This is used to read into nested lists (list of list, list of struct) and non-dictionary lists.
#[inline]
fn flatten_json_values(values: &[Value]) -> Vec<Value> {
    values
        .iter()
        .flat_map(|row| {
            if let Value::Array(values) = row {
                values.clone()
            } else if let Value::Null = row {
                vec![Value::Null]
            } else {
                // we interpret a scalar as a single-value list to minimise data loss
                vec![row.clone()]
            }
        })
        .collect()
}

/// Flattens a list into string values, dropping Value::Null in the process.
/// This is useful for interpreting any JSON array as string, dropping nulls.
/// See `json_value_as_string`.
#[inline]
fn flatten_json_string_values(values: &[Value]) -> Vec<Option<String>> {
    values
        .iter()
        .flat_map(|row| {
            if let Value::Array(values) = row {
                values
                    .iter()
                    .map(json_value_as_string)
                    .collect::<Vec<Option<_>>>()
            } else if let Value::Null = row {
                vec![]
            } else {
                vec![json_value_as_string(row)]
            }
        })
        .collect::<Vec<Option<_>>>()
}
/// JSON file reader
#[derive(Debug)]
pub struct Reader<R: Read> {
    reader: BufReader<R>,
    /// JSON value decoder
    decoder: Decoder,
}

impl<R: Read> Reader<R> {
    /// Create a new JSON Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn new(reader: R, schema: SchemaRef, options: DecoderOptions) -> Self {
        Self::from_buf_reader(BufReader::new(reader), schema, options)
    }

    /// Create a new JSON Reader from a `BufReader<R: Read>`
    ///
    /// To customize the schema, such as to enable schema inference, use `ReaderBuilder`
    pub fn from_buf_reader(
        reader: BufReader<R>,
        schema: SchemaRef,
        options: DecoderOptions,
    ) -> Self {
        Self {
            reader,
            decoder: Decoder::new(schema, options),
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Read the next batch of records
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.decoder
            .next_batch(&mut ValueIter::new(&mut self.reader, None))
    }
}

/// JSON file reader builder
#[derive(Debug, Default)]
pub struct ReaderBuilder {
    /// Optional schema for the JSON file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the JSON structure.
    schema: Option<SchemaRef>,
    /// Optional maximum number of records to read during schema inference
    ///
    /// If a number is not provided, all the records are read.
    max_records: Option<usize>,
    /// Options for json decoder
    options: DecoderOptions,
}

impl ReaderBuilder {
    /// Create a new builder for configuring JSON parsing options.
    ///
    /// To convert a builder into a reader, call `Reader::from_builder`
    ///
    /// # Example
    ///
    /// ```
    /// # use std::fs::File;
    ///
    /// fn example() -> arrow_json::Reader<File> {
    ///     let file = File::open("test/data/basic.json").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = arrow_json::ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build::<File>(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the JSON reader to infer the schema of the file
    pub fn infer_schema(mut self, max_records: Option<usize>) -> Self {
        // remove any schema that is set
        self.schema = None;
        self.max_records = max_records;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.options = self.options.with_batch_size(batch_size);
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.options = self.options.with_projection(projection);
        self
    }

    /// Set the decoder's format Strings param
    pub fn with_format_strings(
        mut self,
        format_strings: HashMap<String, String>,
    ) -> Self {
        self.options = self.options.with_format_strings(format_strings);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R>(self, source: R) -> Result<Reader<R>, ArrowError>
    where
        R: Read + Seek,
    {
        let mut buf_reader = BufReader::new(source);

        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => Arc::new(infer_json_schema_from_seekable(
                &mut buf_reader,
                self.max_records,
            )?),
        };

        Ok(Reader::from_buf_reader(buf_reader, schema, self.options))
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::ToByteSlice;
    use arrow_schema::DataType::{Dictionary, List};
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::Cursor;

    #[test]
    fn test_json_basic() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(5, batch.num_columns());
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

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-3.5, bb.value(1));
        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!cc.value(0));
        assert!(cc.value(10));
        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("4", dd.value(0));
        assert_eq!("text", dd.value(8));
    }

    #[test]
    fn test_json_empty_projection() {
        let builder = ReaderBuilder::new()
            .infer_schema(None)
            .with_batch_size(64)
            .with_projection(vec![]);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(0, batch.num_columns());
        assert_eq!(12, batch.num_rows());
    }

    #[test]
    fn test_json_basic_with_nulls() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
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

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(11));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(bb.is_valid(0));
        assert!(!bb.is_valid(2));
        assert!(!bb.is_valid(11));
        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(cc.is_valid(0));
        assert!(!cc.is_valid(4));
        assert!(!cc.is_valid(11));
        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!dd.is_valid(0));
        assert!(dd.is_valid(1));
        assert!(!dd.is_valid(4));
        assert!(!dd.is_valid(11));
    }

    #[test]
    fn test_json_basic_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::Utf8, false),
        ]);

        let mut reader: Reader<File> = Reader::new(
            File::open("test/data/basic.json").unwrap(),
            Arc::new(schema.clone()),
            DecoderOptions::new(),
        );
        let reader_schema = reader.schema();
        assert_eq!(reader_schema, Arc::new(schema));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int32, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float32, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        // test that a 64bit value is returned as null due to overflowing
        assert!(!aa.is_valid(11));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-3.5, bb.value(1));
    }

    #[test]
    fn test_json_format_strings_for_date() {
        let schema = Arc::new(Schema::new(vec![Field::new("e", DataType::Date32, true)]));
        let e = schema.column_with_name("e").unwrap();
        assert_eq!(&DataType::Date32, e.1.data_type());
        let mut fmts = HashMap::new();
        let date_format = "%Y-%m-%d".to_string();
        fmts.insert("e".to_string(), date_format.clone());

        let mut reader: Reader<File> = Reader::new(
            File::open("test/data/basic.json").unwrap(),
            schema.clone(),
            DecoderOptions::new().with_format_strings(fmts),
        );
        let reader_schema = reader.schema();
        assert_eq!(reader_schema, schema);
        let batch = reader.next().unwrap().unwrap();

        let ee = batch
            .column(e.0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let dt = Date32Type::parse_formatted("1970-1-2", &date_format).unwrap();
        assert_eq!(dt, ee.value(0));
        let dt = Date32Type::parse_formatted("1969-12-31", &date_format).unwrap();
        assert_eq!(dt, ee.value(1));
        assert!(!ee.is_valid(2));
    }

    #[test]
    fn test_json_basic_schema_projection() {
        // We test implicit and explicit projection:
        // Implicit: omitting fields from a schema
        // Explicit: supplying a vec of fields to take
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
        ]);

        let mut reader: Reader<File> = Reader::new(
            File::open("test/data/basic.json").unwrap(),
            Arc::new(schema),
            DecoderOptions::new().with_projection(vec!["a".to_string(), "c".to_string()]),
        );
        let reader_schema = reader.schema();
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("c", DataType::Boolean, false),
        ]));
        assert_eq!(reader_schema, expected_schema);

        let batch = reader.next().unwrap().unwrap();

        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.schema().fields().len());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();
        assert_eq!(reader_schema, schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int32, a.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(1, c.0);
        assert_eq!(&DataType::Boolean, c.1.data_type());
    }

    #[test]
    fn test_json_arrays() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/arrays.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            b.1.data_type()
        );
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            c.1.data_type()
        );
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        assert_eq!(1627668684594000000, aa.value(2));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bb = bb.values();
        let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(9, bb.len());
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-6.1, bb.value(5));
        assert!(!bb.is_valid(7));

        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let cc = cc.values();
        let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(6, cc.len());
        assert!(!cc.value(0));
        assert!(!cc.value(4));
        assert!(!cc.is_valid(5));
    }

    #[test]
    fn test_invalid_json_infer_schema() {
        let re =
            infer_json_schema_from_seekable(&mut BufReader::new(Cursor::new(b"}")), None);
        assert_eq!(
            re.err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_invalid_json_read_record() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]),
            true,
        )]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let mut reader = builder.build(Cursor::new(b"}")).unwrap();
        assert_eq!(
            reader.next().err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_coersion_scalar_and_list() {
        use arrow_schema::DataType::*;

        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(vec![
                &Float64,
                &List(Box::new(Field::new("item", Float64, true)))
            ])
        );
        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(vec![
                &Float64,
                &List(Box::new(Field::new("item", Int64, true)))
            ])
        );
        assert_eq!(
            List(Box::new(Field::new("item", Int64, true))),
            coerce_data_type(vec![
                &Int64,
                &List(Box::new(Field::new("item", Int64, true)))
            ])
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            List(Box::new(Field::new("item", Utf8, true))),
            coerce_data_type(vec![
                &Boolean,
                &List(Box::new(Field::new("item", Float64, true)))
            ])
        );
    }

    #[test]
    fn test_mixed_json_arrays() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/mixed_arrays.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let schema = infer_json_schema(&mut reader, None).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let reader = BufReader::new(GzDecoder::new(&file));
        let options = DecoderOptions::new().with_batch_size(64);
        let mut reader = Reader::from_buf_reader(reader, Arc::new(schema), options);
        let batch_gz = reader.next().unwrap().unwrap();

        for batch in vec![batch, batch_gz] {
            assert_eq!(4, batch.num_columns());
            assert_eq!(4, batch.num_rows());

            let schema = batch.schema();

            let a = schema.column_with_name("a").unwrap();
            assert_eq!(&DataType::Int64, a.1.data_type());
            let b = schema.column_with_name("b").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                b.1.data_type()
            );
            let c = schema.column_with_name("c").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                c.1.data_type()
            );
            let d = schema.column_with_name("d").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                d.1.data_type()
            );

            let bb = batch
                .column(b.0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let bb = bb.values();
            let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(10, bb.len());
            assert_eq!(4.0, bb.value(9));

            let cc = batch
                .column(c.0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            // test that the list offsets are correct
            assert_eq!(
                cc.data().buffers()[0],
                Buffer::from_slice_ref([0i32, 2, 2, 4, 5])
            );
            let cc = cc.values();
            let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
            let cc_expected = BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
            ]);
            assert_eq!(cc.data_ref(), cc_expected.data_ref());

            let dd: &ListArray = batch
                .column(d.0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            // test that the list offsets are correct
            assert_eq!(
                dd.data().buffers()[0],
                Buffer::from_slice_ref([0i32, 1, 1, 2, 6])
            );
            let dd = dd.values();
            let dd = dd.as_any().downcast_ref::<StringArray>().unwrap();
            // values are 6 because a `d: null` is treated as a null slot
            // and a list's null slot can be omitted from the child (i.e. same offset)
            assert_eq!(6, dd.len());
            assert_eq!("text", dd.value(1));
            assert_eq!("1", dd.value(2));
            assert_eq!("false", dd.value(3));
            assert_eq!("array", dd.value(4));
            assert_eq!("2.4", dd.value(5));
        }
    }

    #[test]
    fn test_nested_struct_json_arrays() {
        let c_field = Field::new(
            "c",
            DataType::Struct(vec![Field::new("d", DataType::Utf8, true)]),
            true,
        );
        let a_field = Field::new(
            "a",
            DataType::Struct(vec![
                Field::new("b", DataType::Boolean, true),
                c_field.clone(),
            ]),
            true,
        );
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/nested_structs.json").unwrap())
            .unwrap();

        // build expected output
        let d = StringArray::from(vec![Some("text"), None, Some("text"), None]);
        let c = ArrayDataBuilder::new(c_field.data_type().clone())
            .len(4)
            .add_child_data(d.into_data())
            .null_bit_buffer(Some(Buffer::from(vec![0b00000101])))
            .build()
            .unwrap();
        let b = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
        let a = ArrayDataBuilder::new(a_field.data_type().clone())
            .len(4)
            .add_child_data(b.into_data())
            .add_child_data(c)
            .null_bit_buffer(Some(Buffer::from(vec![0b00000111])))
            .build()
            .unwrap();
        let expected = make_array(a);

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert!(
            expected.data_ref() == read.data_ref(),
            "{:?} != {:?}",
            expected.data(),
            read.data(),
        );
    }

    #[test]
    fn test_nested_list_json_arrays() {
        let c_field = Field::new(
            "c",
            DataType::Struct(vec![Field::new("d", DataType::Utf8, true)]),
            true,
        );
        let a_struct_field = Field::new(
            "a",
            DataType::Struct(vec![
                Field::new("b", DataType::Boolean, true),
                c_field.clone(),
            ]),
            true,
        );
        let a_field =
            Field::new("a", DataType::List(Box::new(a_struct_field.clone())), true);
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
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
            .add_child_data(d.data().clone())
            .null_bit_buffer(Some(Buffer::from(vec![0b00111011])))
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
            .add_child_data(b.data().clone())
            .add_child_data(c.clone())
            .null_bit_buffer(Some(Buffer::from(vec![0b00111111])))
            .build()
            .unwrap();
        let a_list = ArrayDataBuilder::new(a_field.data_type().clone())
            .len(6)
            .add_buffer(Buffer::from_slice_ref([0i32, 2, 3, 6, 6, 6, 7]))
            .add_child_data(a)
            .null_bit_buffer(Some(Buffer::from(vec![0b00110111])))
            .build()
            .unwrap();
        let expected = make_array(a_list);

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert_eq!(read.len(), 6);
        // compare the arrays the long way around, to better detect differences
        let read: &ListArray = read.as_any().downcast_ref::<ListArray>().unwrap();
        let expected = expected.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            read.data().buffers()[0],
            Buffer::from_slice_ref([0i32, 2, 3, 6, 6, 6, 7])
        );
        // compare list null buffers
        assert_eq!(read.data().null_buffer(), expected.data().null_buffer());
        // build struct from list
        let struct_values = read.values();
        let struct_array: &StructArray = struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let expected_struct_values = expected.values();
        let expected_struct_array = expected_struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_eq!(7, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert_eq!(7, expected_struct_array.len());
        assert_eq!(1, expected_struct_array.null_count());
        // test struct's nulls
        assert_eq!(
            struct_array.data().null_buffer(),
            expected_struct_array.data().null_buffer()
        );
        // test struct's fields
        let read_b = struct_array.column(0);
        assert_eq!(b.data_ref(), read_b.data_ref());
        let read_c = struct_array.column(1);
        assert_eq!(&c, read_c.data_ref());
        let read_c: &StructArray = read_c.as_any().downcast_ref::<StructArray>().unwrap();
        let read_d = read_c.column(0);
        assert_eq!(d.data_ref(), read_d.data_ref());

        assert_eq!(read.data_ref(), expected.data_ref());
    }

    #[test]
    fn test_map_json_arrays() {
        let account_field = Field::new("account", DataType::UInt16, false);
        let value_list_type =
            DataType::List(Box::new(Field::new("item", DataType::Utf8, false)));
        let entries_struct_type = DataType::Struct(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", value_list_type.clone(), true),
        ]);
        let stocks_field = Field::new(
            "stocks",
            DataType::Map(
                Box::new(Field::new("entries", entries_struct_type.clone(), false)),
                false,
            ),
            true,
        );
        let schema = Arc::new(Schema::new(vec![account_field, stocks_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        // Note: account 456 has 'long' twice, to show that the JSON reader will overwrite
        // existing keys. This thus guarantees unique keys for the map
        let json_content = r#"
        {"account": 123, "stocks":{"long": ["$AAA", "$BBB"], "short": ["$CCC", "$D"]}}
        {"account": 456, "stocks":{"long": null, "long": ["$AAA", "$CCC", "$D"], "short": null}}
        {"account": 789, "stocks":{"hedged": ["$YYY"], "long": null, "short": ["$D"]}}
        "#;
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();

        // build expected output
        let expected_accounts = UInt16Array::from(vec![123, 456, 789]);

        let expected_keys = StringArray::from(vec![
            "long", "short", "long", "short", "hedged", "long", "short",
        ])
        .data()
        .clone();
        let expected_value_array_data = StringArray::from(vec![
            "$AAA", "$BBB", "$CCC", "$D", "$AAA", "$CCC", "$D", "$YYY", "$D",
        ])
        .data()
        .clone();
        // Create the list that holds ["$_", "$_"]
        let expected_values = ArrayDataBuilder::new(value_list_type)
            .len(7)
            .add_buffer(Buffer::from(
                vec![0i32, 2, 4, 7, 7, 8, 8, 9].to_byte_slice(),
            ))
            .add_child_data(expected_value_array_data)
            .null_bit_buffer(Some(Buffer::from(vec![0b01010111])))
            .build()
            .unwrap();
        let expected_stocks_entries_data = ArrayDataBuilder::new(entries_struct_type)
            .len(7)
            .add_child_data(expected_keys)
            .add_child_data(expected_values)
            .build()
            .unwrap();
        let expected_stocks_data =
            ArrayDataBuilder::new(stocks_field.data_type().clone())
                .len(3)
                .add_buffer(Buffer::from(vec![0i32, 2, 4, 7].to_byte_slice()))
                .add_child_data(expected_stocks_entries_data)
                .build()
                .unwrap();

        let expected_stocks = make_array(expected_stocks_data);

        // compare with result from json reader
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        let col1 = batch.column(0);
        assert_eq!(col1.data(), expected_accounts.data());
        // Compare the map
        let col2 = batch.column(1);
        assert_eq!(col2.data(), expected_stocks.data());
    }

    #[test]
    fn test_dictionary_from_json_basic_with_nulls() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            d.1.data_type()
        );

        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int16Type>>()
            .unwrap();
        assert!(!dd.is_valid(0));
        assert!(dd.is_valid(1));
        assert!(dd.is_valid(2));
        assert!(!dd.is_valid(11));

        assert_eq!(
            dd.keys(),
            &Int16Array::from(vec![
                None,
                Some(0),
                Some(1),
                Some(0),
                None,
                None,
                Some(0),
                None,
                Some(1),
                Some(0),
                Some(0),
                None
            ])
        );
    }

    #[test]
    fn test_dictionary_from_json_int8() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_int32() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_int64() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_skip_empty_lines() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
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
    fn test_row_type_validation() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let json_content = "
        [1, \"hello\"]
        \"world\"";
        let re = builder.build(Cursor::new(json_content));
        assert_eq!(
            re.err().unwrap().to_string(),
            r#"Json error: Expected JSON record to be an object, found Array [Number(1), String("hello")]"#,
        );
    }

    #[test]
    fn test_list_of_string_dictionary_from_json() {
        let schema = Schema::new(vec![Field::new(
            "events",
            List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true,
            ))),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/list_string_dict_nested.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let events = schema.column_with_name("events").unwrap();
        assert_eq!(
            &List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true
            ))),
            events.1.data_type()
        );

        let evs_list = batch
            .column(events.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let evs_list = evs_list.values();
        let evs_list = evs_list
            .as_any()
            .downcast_ref::<DictionaryArray<UInt64Type>>()
            .unwrap();
        assert_eq!(6, evs_list.len());
        assert!(evs_list.is_valid(1));
        assert_eq!(DataType::Utf8, evs_list.value_type());

        // dict from the events list
        let dict_el = evs_list.values();
        let dict_el = dict_el.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(3, dict_el.len());
        assert_eq!("Elect Leader", dict_el.value(0));
        assert_eq!("Do Ballot", dict_el.value(1));
        assert_eq!("Send Data", dict_el.value(2));
    }

    #[test]
    fn test_list_of_string_dictionary_from_json_with_nulls() {
        let schema = Schema::new(vec![Field::new(
            "events",
            List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true,
            ))),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(
                File::open("test/data/list_string_dict_nested_nulls.json").unwrap(),
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let events = schema.column_with_name("events").unwrap();
        assert_eq!(
            &List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true
            ))),
            events.1.data_type()
        );

        let evs_list = batch
            .column(events.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let evs_list = evs_list.values();
        let evs_list = evs_list
            .as_any()
            .downcast_ref::<DictionaryArray<UInt64Type>>()
            .unwrap();
        assert_eq!(8, evs_list.len());
        assert!(evs_list.is_valid(1));
        assert_eq!(DataType::Utf8, evs_list.value_type());

        // dict from the events list
        let dict_el = evs_list.values();
        let dict_el = dict_el.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, evs_list.null_count());
        assert_eq!(3, dict_el.len());
        assert_eq!("Elect Leader", dict_el.value(0));
        assert_eq!("Do Ballot", dict_el.value(1));
        assert_eq!("Send Data", dict_el.value(2));
    }

    #[test]
    fn test_dictionary_from_json_uint8() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_uint32() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_uint64() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_with_multiple_batches() {
        let builder = ReaderBuilder::new()
            .infer_schema(Some(4))
            .with_batch_size(5);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();

        let mut num_records = Vec::new();
        while let Some(rb) = reader.next().unwrap() {
            num_records.push(rb.num_rows());
        }

        assert_eq!(vec![5, 5, 2], num_records);
    }

    #[test]
    fn test_json_infer_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new(
                "b",
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new(
                "c",
                DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                true,
            ),
            Field::new(
                "d",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);

        let mut reader =
            BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
        let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, schema);

        let file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let inferred_schema = infer_json_schema(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, schema);
    }

    #[test]
    fn test_json_infer_schema_nested_structs() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Struct(vec![
                    Field::new("a", DataType::Boolean, true),
                    Field::new(
                        "b",
                        DataType::Struct(vec![Field::new("c", DataType::Utf8, true)]),
                        true,
                    ),
                ]),
                true,
            ),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Utf8, true),
        ]);

        let inferred_schema = infer_json_schema_from_iterator(
            vec![
                Ok(serde_json::json!({"c1": {"a": true, "b": {"c": "text"}}, "c2": 1})),
                Ok(serde_json::json!({"c1": {"a": false, "b": null}, "c2": 0})),
                Ok(serde_json::json!({"c1": {"a": true, "b": {"c": "text"}}, "c3": "ok"})),
            ]
            .into_iter(),
        )
        .unwrap();

        assert_eq!(inferred_schema, schema);
    }

    #[test]
    fn test_json_infer_schema_struct_in_list() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(vec![
                        Field::new("a", DataType::Utf8, true),
                        Field::new("b", DataType::Int64, true),
                        Field::new("c", DataType::Boolean, true),
                    ]),
                    true,
                ))),
                true,
            ),
            Field::new("c2", DataType::Float64, true),
            Field::new(
                "c3",
                // empty json array's inner types are inferred as null
                DataType::List(Box::new(Field::new("item", DataType::Null, true))),
                true,
            ),
        ]);

        let inferred_schema = infer_json_schema_from_iterator(
            vec![
                Ok(serde_json::json!({
                    "c1": [{"a": "foo", "b": 100}], "c2": 1, "c3": [],
                })),
                Ok(serde_json::json!({
                    "c1": [{"a": "bar", "b": 2}, {"a": "foo", "c": true}], "c2": 0, "c3": [],
                })),
                Ok(serde_json::json!({"c1": [], "c2": 0.5, "c3": []})),
            ]
            .into_iter(),
        )
        .unwrap();

        assert_eq!(inferred_schema, schema);
    }

    #[test]
    fn test_json_infer_schema_nested_list() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
            Field::new("c2", DataType::Float64, true),
        ]);

        let inferred_schema = infer_json_schema_from_iterator(
            vec![
                Ok(serde_json::json!({
                    "c1": [],
                    "c2": 12,
                })),
                Ok(serde_json::json!({
                    "c1": [["a", "b"], ["c"]],
                })),
                Ok(serde_json::json!({
                    "c1": [["foo"]],
                    "c2": 0.11,
                })),
            ]
            .into_iter(),
        )
        .unwrap();

        assert_eq!(inferred_schema, schema);
    }

    #[test]
    fn test_timestamp_from_json_seconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
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

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
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

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
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

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
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

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Date64, a.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap();
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

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Time64(TimeUnit::Nanosecond), a.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();
        assert!(aa.is_valid(0));
        assert!(!aa.is_valid(1));
        assert!(!aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_time_from_string() {
        parse_string_column::<Time64NanosecondType>(4);
        parse_string_column::<Time64MicrosecondType>(4);
        parse_string_column::<Time32MillisecondType>(4);
        parse_string_column::<Time32SecondType>(4);
    }

    fn parse_string_column<T>(value: T::Native)
    where
        T: ArrowPrimitiveType,
    {
        let schema = Schema::new(vec![Field::new("d", T::DATA_TYPE, true)]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        let dd = batch
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();
        assert_eq!(value, dd.value(1));
        assert!(!dd.is_valid(2));
    }

    #[test]
    fn test_json_read_nested_list() {
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ))),
            true,
        )]);

        let decoder = Decoder::new(Arc::new(schema), DecoderOptions::new());
        let batch = decoder
            .next_batch(
                &mut vec![
                    Ok(serde_json::json!({
                        "c1": [],
                    })),
                    Ok(serde_json::json!({
                        "c1": [["a", "b"], ["c"], ["e", "f"], ["g"], ["h"], ["i"], ["j"], ["k"]],
                    })),
                    Ok(serde_json::json!({
                        "c1": [["foo"], ["bar"]],
                    })),
                ]
                .into_iter(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_json_read_list_of_structs() {
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::List(Box::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("a", DataType::Int64, true)]),
                true,
            ))),
            true,
        )]);

        let decoder = Decoder::new(Arc::new(schema), DecoderOptions::new());
        let batch = decoder
            .next_batch(
                // NOTE: total struct element count needs to be greater than
                // bit_util::ceil(array_count, 8) to test validity bit buffer length calculation
                // logic
                &mut vec![
                    Ok(serde_json::json!({
                        "c1": [{"a": 1}],
                    })),
                    Ok(serde_json::json!({
                        "c1": [{"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}, {"a": 6}, {"a": 7}],
                    })),
                    Ok(serde_json::json!({
                        "c1": [{"a": 10}, {"a": 11}],
                    })),
                ]
                .into_iter(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_json_read_binary_structs() {
        let schema = Schema::new(vec![Field::new("c1", DataType::Binary, true)]);
        let decoder = Decoder::new(Arc::new(schema), DecoderOptions::new());
        let batch = decoder
            .next_batch(
                &mut vec![
                    Ok(serde_json::json!({
                        "c1": "₁₂₃",
                    })),
                    Ok(serde_json::json!({
                        "c1": "foo",
                    })),
                ]
                .into_iter(),
            )
            .unwrap()
            .unwrap();
        let data = batch.columns().iter().collect::<Vec<_>>();

        let schema = Schema::new(vec![Field::new("c1", DataType::Binary, true)]);
        let binary_values = BinaryArray::from(vec!["₁₂₃".as_bytes(), "foo".as_bytes()]);
        let expected_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(binary_values)])
                .unwrap();
        let expected_data = expected_batch.columns().iter().collect::<Vec<_>>();

        assert_eq!(data, expected_data);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_json_iterator() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(5);
        let reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic.json").unwrap())
            .unwrap();
        let schema = reader.schema();
        let (col_a_index, _) = schema.column_with_name("a").unwrap();

        let mut sum_num_rows = 0;
        let mut num_batches = 0;
        let mut sum_a = 0;
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(5, batch.num_columns());
            sum_num_rows += batch.num_rows();
            num_batches += 1;
            let batch_schema = batch.schema();
            assert_eq!(schema, batch_schema);
            let a_array = batch
                .column(col_a_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            sum_a += (0..a_array.len()).map(|i| a_array.value(i)).sum::<i64>();
        }
        assert_eq!(12, sum_num_rows);
        assert_eq!(3, num_batches);
        assert_eq!(100000000000011, sum_a);
    }

    #[test]
    fn test_options_clone() {
        // ensure options have appropriate derivation
        let options = DecoderOptions::new().with_batch_size(64);
        let cloned = options.clone();
        assert_eq!(options, cloned);
    }
}
