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

use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use serde_json::Value;
use std::borrow::Borrow;
use std::io::{BufRead, Seek};
use std::sync::Arc;

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
            (InferredType::Array(self_inner_type), other_scalar @ InferredType::Scalar(_)) => {
                self_inner_type.merge(other_scalar)?;
            }
            (s @ InferredType::Scalar(_), InferredType::Array(mut other_inner_type)) => {
                other_inner_type.merge(s.clone())?;
                *s = InferredType::Array(other_inner_type);
            }
            // incompatible types
            (s, o) => {
                return Err(ArrowError::JsonError(format!(
                    "Incompatible type found during schema inference: {s:?} v.s. {o:?}",
                )));
            }
        }

        Ok(())
    }

    fn is_none_or_any(ty: Option<&Self>) -> bool {
        matches!(ty, Some(Self::Any) | None)
    }
}

/// Shorthand for building list data type of `ty`
fn list_type_of(ty: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", ty, true)))
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
        (DataType::Null, o) | (o, DataType::Null) => o,
        (DataType::Boolean, DataType::Boolean) => DataType::Boolean,
        (DataType::Int64, DataType::Int64) => DataType::Int64,
        (DataType::Float64, DataType::Float64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Int64, DataType::Float64) => DataType::Float64,
        (DataType::List(l), DataType::List(r)) => {
            list_type_of(coerce_data_type(vec![l.data_type(), r.data_type()]))
        }
        // coerce scalar and scalar array into scalar array
        (DataType::List(e), not_list) | (not_list, DataType::List(e)) => {
            list_type_of(coerce_data_type(vec![e.data_type(), &not_list]))
        }
        _ => DataType::Utf8,
    })
}

fn generate_datatype(t: &InferredType) -> Result<DataType, ArrowError> {
    Ok(match t {
        InferredType::Scalar(hs) => coerce_data_type(hs.iter().collect()),
        InferredType::Object(spec) => DataType::Struct(generate_fields(spec)?),
        InferredType::Array(ele_type) => list_type_of(generate_datatype(ele_type)?),
        InferredType::Any => DataType::Null,
    })
}

fn generate_fields(spec: &HashMap<String, InferredType>) -> Result<Fields, ArrowError> {
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
pub struct ValueIter<R: BufRead> {
    reader: R,
    max_read_records: Option<usize>,
    record_count: usize,
    // reuse line buffer to avoid allocation on each record
    line_buf: String,
}

impl<R: BufRead> ValueIter<R> {
    /// Creates a new `ValueIter`
    pub fn new(reader: R, max_read_records: Option<usize>) -> Self {
        Self {
            reader,
            max_read_records,
            record_count: 0,
            line_buf: String::new(),
        }
    }
}

impl<R: BufRead> Iterator for ValueIter<R> {
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
                        "Failed to read JSON record: {e}"
                    ))));
                }
                _ => {
                    let trimmed_s = self.line_buf.trim();
                    if trimmed_s.is_empty() {
                        // ignore empty lines
                        continue;
                    }

                    self.record_count += 1;
                    return Some(
                        serde_json::from_str(trimmed_s)
                            .map_err(|e| ArrowError::JsonError(format!("Not valid JSON: {e}"))),
                    );
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
/// Returns inferred schema and number of records read.
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
///
/// [`Reader`]: super::Reader
pub fn infer_json_schema_from_seekable<R: BufRead + Seek>(
    mut reader: R,
    max_read_records: Option<usize>,
) -> Result<(Schema, usize), ArrowError> {
    let schema = infer_json_schema(&mut reader, max_read_records);
    // return the reader seek back to the start
    reader.rewind()?;

    schema
}

/// Infer the fields of a JSON file by reading the first n records of the buffer, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// Returns inferred schema and number of records read.
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
pub fn infer_json_schema<R: BufRead>(
    reader: R,
    max_read_records: Option<usize>,
) -> Result<(Schema, usize), ArrowError> {
    let mut values = ValueIter::new(reader, max_read_records);
    let schema = infer_json_schema_from_iterator(&mut values)?;
    Ok((schema, values.record_count))
}

fn set_object_scalar_field_type(
    field_types: &mut HashMap<String, InferredType>,
    key: &str,
    ftype: DataType,
) -> Result<(), ArrowError> {
    if InferredType::is_none_or_any(field_types.get(key)) {
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
            "Expected scalar or scalar array JSON type, found: {t:?}",
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
                    "Expected scalar value for scalar array, got: {v:?}"
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
                    "Got non array element in nested array: {x:?}"
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
                    "Expected struct value for struct array, got: {v:?}"
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
    map: &serde_json::map::Map<String, Value>,
) -> Result<(), ArrowError> {
    for (k, v) in map {
        match v {
            Value::Array(array) => {
                let ele_type = infer_array_element_type(array)?;

                if InferredType::is_none_or_any(field_types.get(k)) {
                    match ele_type {
                        InferredType::Scalar(_) => {
                            field_types.insert(
                                k.to_string(),
                                InferredType::Array(Box::new(InferredType::Scalar(HashSet::new()))),
                            );
                        }
                        InferredType::Object(_) => {
                            field_types.insert(
                                k.to_string(),
                                InferredType::Array(Box::new(InferredType::Object(HashMap::new()))),
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
                            "Expected array json type, found: {t:?}",
                        )));
                    }
                }
            }
            Value::Bool(_) => {
                set_object_scalar_field_type(field_types, k, DataType::Boolean)?;
            }
            Value::Null => {
                // we treat json as nullable by default when inferring, so just
                // mark existence of a field if it wasn't known before
                if !field_types.contains_key(k) {
                    field_types.insert(k.to_string(), InferredType::Any);
                }
            }
            Value::Number(n) => {
                if n.is_i64() {
                    set_object_scalar_field_type(field_types, k, DataType::Int64)?;
                } else {
                    set_object_scalar_field_type(field_types, k, DataType::Float64)?;
                }
            }
            Value::String(_) => {
                set_object_scalar_field_type(field_types, k, DataType::Utf8)?;
            }
            Value::Object(inner_map) => {
                if let InferredType::Any = field_types.get(k).unwrap_or(&InferredType::Any) {
                    field_types.insert(k.to_string(), InferredType::Object(HashMap::new()));
                }
                match field_types.get_mut(k).unwrap() {
                    InferredType::Object(inner_field_types) => {
                        collect_field_types_from_object(inner_field_types, inner_map)?;
                    }
                    t => {
                        return Err(ArrowError::JsonError(format!(
                            "Expected object json type, found: {t:?}",
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
pub fn infer_json_schema_from_iterator<I, V>(value_iter: I) -> Result<Schema, ArrowError>
where
    I: Iterator<Item = Result<V, ArrowError>>,
    V: Borrow<Value>,
{
    let mut field_types: HashMap<String, InferredType> = HashMap::new();

    for record in value_iter {
        match record?.borrow() {
            Value::Object(map) => {
                collect_field_types_from_object(&mut field_types, map)?;
            }
            value => {
                return Err(ArrowError::JsonError(format!(
                    "Expected JSON record to be an object, found {value:?}"
                )));
            }
        };
    }

    generate_schema(field_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::{BufReader, Cursor};

    #[test]
    fn test_json_infer_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", list_type_of(DataType::Float64), true),
            Field::new("c", list_type_of(DataType::Boolean), true),
            Field::new("d", list_type_of(DataType::Utf8), true),
        ]);

        let mut reader = BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
        let (inferred_schema, n_rows) = infer_json_schema_from_seekable(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, schema);
        assert_eq!(n_rows, 4);

        let file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let (inferred_schema, n_rows) = infer_json_schema(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, schema);
        assert_eq!(n_rows, 4);
    }

    #[test]
    fn test_row_limit() {
        let mut reader = BufReader::new(File::open("test/data/basic.json").unwrap());

        let (_, n_rows) = infer_json_schema_from_seekable(&mut reader, None).unwrap();
        assert_eq!(n_rows, 12);

        let (_, n_rows) = infer_json_schema_from_seekable(&mut reader, Some(5)).unwrap();
        assert_eq!(n_rows, 5);
    }

    #[test]
    fn test_json_infer_schema_nested_structs() {
        let schema = Schema::new(vec![
            Field::new(
                "c1",
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Boolean, true),
                    Field::new(
                        "b",
                        DataType::Struct(vec![Field::new("c", DataType::Utf8, true)].into()),
                        true,
                    ),
                ])),
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
                list_type_of(DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Utf8, true),
                    Field::new("b", DataType::Int64, true),
                    Field::new("c", DataType::Boolean, true),
                ]))),
                true,
            ),
            Field::new("c2", DataType::Float64, true),
            Field::new(
                "c3",
                // empty json array's inner types are inferred as null
                list_type_of(DataType::Null),
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
            Field::new("c1", list_type_of(list_type_of(DataType::Utf8)), true),
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
    fn test_infer_json_schema_bigger_than_i64_max() {
        let bigger_than_i64_max = (i64::MAX as i128) + 1;
        let smaller_than_i64_min = (i64::MIN as i128) - 1;
        let json = format!(
            "{{ \"bigger_than_i64_max\": {}, \"smaller_than_i64_min\": {} }}",
            bigger_than_i64_max, smaller_than_i64_min
        );
        let mut buf_reader = BufReader::new(json.as_bytes());
        let (inferred_schema, _) = infer_json_schema(&mut buf_reader, Some(1)).unwrap();
        let fields = inferred_schema.fields();

        let (_, big_field) = fields.find("bigger_than_i64_max").unwrap();
        assert_eq!(big_field.data_type(), &DataType::Float64);
        let (_, small_field) = fields.find("smaller_than_i64_min").unwrap();
        assert_eq!(small_field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_coercion_scalar_and_list() {
        assert_eq!(
            list_type_of(DataType::Float64),
            coerce_data_type(vec![&DataType::Float64, &list_type_of(DataType::Float64)])
        );
        assert_eq!(
            list_type_of(DataType::Float64),
            coerce_data_type(vec![&DataType::Float64, &list_type_of(DataType::Int64)])
        );
        assert_eq!(
            list_type_of(DataType::Int64),
            coerce_data_type(vec![&DataType::Int64, &list_type_of(DataType::Int64)])
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            list_type_of(DataType::Utf8),
            coerce_data_type(vec![&DataType::Boolean, &list_type_of(DataType::Float64)])
        );
    }

    #[test]
    fn test_invalid_json_infer_schema() {
        let re = infer_json_schema_from_seekable(Cursor::new(b"}"), None);
        assert_eq!(
            re.err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_null_field_inferred_as_null() {
        let data = r#"
            {"in":1,    "ni":null, "ns":null, "sn":"4",  "n":null, "an":[],   "na": null, "nas":null}
            {"in":null, "ni":2,    "ns":"3",  "sn":null, "n":null, "an":null, "na": [],   "nas":["8"]}
            {"in":1,    "ni":null, "ns":null, "sn":"4",  "n":null, "an":[],   "na": null, "nas":[]}
        "#;
        let (inferred_schema, _) =
            infer_json_schema_from_seekable(Cursor::new(data), None).expect("infer");
        let schema = Schema::new(vec![
            Field::new("an", list_type_of(DataType::Null), true),
            Field::new("in", DataType::Int64, true),
            Field::new("n", DataType::Null, true),
            Field::new("na", list_type_of(DataType::Null), true),
            Field::new("nas", list_type_of(DataType::Utf8), true),
            Field::new("ni", DataType::Int64, true),
            Field::new("ns", DataType::Utf8, true),
            Field::new("sn", DataType::Utf8, true),
        ]);
        assert_eq!(inferred_schema, schema);
    }

    #[test]
    fn test_infer_from_null_then_object() {
        let data = r#"
            {"obj":null}
            {"obj":{"foo":1}}
        "#;
        let (inferred_schema, _) =
            infer_json_schema_from_seekable(Cursor::new(data), None).expect("infer");
        let schema = Schema::new(vec![Field::new(
            "obj",
            DataType::Struct(
                [Field::new("foo", DataType::Int64, true)]
                    .into_iter()
                    .collect(),
            ),
            true,
        )]);
        assert_eq!(inferred_schema, schema);
    }
}
