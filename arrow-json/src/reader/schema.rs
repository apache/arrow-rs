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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{BufRead, Seek};
use std::usize;

use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
use bumpalo::Bump;
use indexmap::IndexMap;
use serde_json::Value;

use super::tape::{Tape, TapeDecoder, TapeElement};

type Result<T> = std::result::Result<T, ArrowError>;

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
/// let file = File::open("test/data/arrays.json").unwrap();
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
) -> Result<(Schema, usize)> {
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
///
/// Note that JSON is not able to represent all Arrow data types exactly. So the inferred schema
/// might be different from the schema of the original data that was encoded as JSON. For example,
/// JSON does not have different integer types, so all integers are inferred as `Int64`. Another
/// example is binary data, which is encoded as a [Base16] string in JSON and therefore inferred
/// as String type by this function.
///
/// [Base16]: https://en.wikipedia.org/wiki/Base16#Base16
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::{BufReader, SeekFrom, Seek};
/// use flate2::read::GzDecoder;
/// use arrow_json::reader::infer_json_schema;
///
/// let mut file = File::open("test/data/arrays.json.gz").unwrap();
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
    mut reader: R,
    max_read_records: Option<usize>,
) -> Result<(Schema, usize)> {
    let arena = Bump::new();
    let mut decoder = SchemaDecoder::new(max_read_records, &arena);

    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        let read = buf.len();

        let decoded = decoder.decode(buf)?;
        reader.consume(read);
        if decoded != read {
            break;
        }
    }

    decoder.finish()
}

/// Infer the fields of a JSON file by reading all items from the JSON Value Iterator.
///
/// The following type coercion logic is implemented:
/// * `Int64` and `Float64` are converted to `Float64`
/// * Incompatible scalars are coerced to `Utf8` (String)
///
/// Note that the above coercion logic is different from what Spark has, where it would default to
/// String type in case of List and Scalar values appeared in the same field.
///
/// The reason we diverge here is because we don't have utilities to deal with JSON data once it's
/// interpreted as Strings. We should match Spark's behavior once we added more JSON parsing
/// kernels in the future.
pub fn infer_json_schema_from_iterator<I, V>(value_iter: I) -> Result<Schema>
where
    I: Iterator<Item = Result<V>>,
    V: Borrow<Value>,
{
    let arena = &Bump::new();

    value_iter
        .into_iter()
        .map(|record| infer_json_type(record?.borrow(), arena))
        .try_fold(*EMPTY_OBJECT_TY, |a, b| a.union(b?, arena))?
        .into_schema()
}

/// Infer the schema of a JSON file by reading all items from the JSON Value Iterator.
///
/// This differs from `infer_json_schema_from_iterator` in that it returns the inferred
/// schema as a `DataType` rather than a `Schema`, permitting the top-level items to be
/// values other than objects (such as arrays, or even scalar values).
pub fn infer_single_field_json_schema_from_iterator<I, V>(value_iter: I) -> Result<DataType>
where
    I: Iterator<Item = Result<V>>,
    V: Borrow<Value>,
{
    let arena = &Bump::new();

    let datatype = value_iter
        .into_iter()
        .map(|record| infer_json_type(record?.borrow(), arena))
        .try_fold(*NEVER_TY, |a, b| a.union(b?, arena))?
        .into_datatype();

    Ok(datatype)
}

struct SchemaDecoder<'a> {
    decoder: TapeDecoder,
    max_read_records: Option<usize>,
    record_count: usize,
    schema: &'a InferredTy<'a>,
    arena: &'a Bump,
}

impl<'a> SchemaDecoder<'a> {
    pub fn new(max_read_records: Option<usize>, arena: &'a Bump) -> Self {
        Self {
            decoder: TapeDecoder::new(1024, 8),
            max_read_records,
            record_count: 0,
            schema: NEVER_TY,
            arena,
        }
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<usize> {
        let read = self.decoder.decode(buf)?;
        if read != buf.len() {
            self.infer_batch()?;
        }
        Ok(read)
    }

    pub fn finish(mut self) -> Result<(Schema, usize)> {
        self.infer_batch()?;
        Ok((self.schema.into_schema()?, self.record_count))
    }

    fn infer_batch(&mut self) -> Result<()> {
        let tape = self.decoder.finish()?;

        let remaining_records = self
            .max_read_records
            .map_or(usize::MAX, |max| max - self.record_count);

        for idx in iter_elements(&tape, 0, tape.len()).take(remaining_records) {
            self.schema = infer_type(&tape, idx, self.schema, self.arena)?;
            self.record_count += 1;
        }

        self.decoder.clear();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum InferredTy<'a> {
    Never,
    Scalar(ScalarTy),
    Array(&'a InferredTy<'a>),
    Object(InferredFields<'a>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScalarTy {
    Bool,
    Int64,
    Float64,
    String,
    // NOTE: Null isn't needed because it's absorbed into Never
}

type InferredFields<'a> = &'a [(&'a str, &'a InferredTy<'a>)];

static NEVER_TY: &InferredTy<'static> = &InferredTy::Never;
static BOOL_TY: &InferredTy<'static> = &InferredTy::Scalar(ScalarTy::Bool);
static INT64_TY: &InferredTy<'static> = &InferredTy::Scalar(ScalarTy::Int64);
static FLOAT64_TY: &InferredTy<'static> = &InferredTy::Scalar(ScalarTy::Float64);
static STRING_TY: &InferredTy<'static> = &InferredTy::Scalar(ScalarTy::String);
static ARRAY_OF_NEVER_TY: &InferredTy<'static> = &InferredTy::Array(NEVER_TY);
static EMPTY_OBJECT_TY: &InferredTy<'static> = &InferredTy::Object(&[]);

fn infer_type<'a>(
    tape: &Tape,
    idx: u32,
    expect: &'a InferredTy<'a>,
    arena: &'a Bump,
) -> Result<&'a InferredTy<'a>> {
    let make_err = |got| {
        let expected = match expect {
            InferredTy::Never => unreachable!(),
            InferredTy::Scalar(_) => "a scalar value",
            InferredTy::Array(_) => "an array",
            InferredTy::Object(_) => "an object",
        };
        let msg = format!("Expected {expected}, found {got}");
        ArrowError::JsonError(msg)
    };

    let infer_scalar = |scalar: ScalarTy, got: &'static str| {
        Ok(match expect {
            InferredTy::Never => match scalar {
                ScalarTy::Bool => BOOL_TY,
                ScalarTy::Int64 => INT64_TY,
                ScalarTy::Float64 => FLOAT64_TY,
                ScalarTy::String => STRING_TY,
            },
            InferredTy::Scalar(expect) => match (expect, &scalar) {
                (ScalarTy::Bool, ScalarTy::Bool) => BOOL_TY,
                (ScalarTy::Int64, ScalarTy::Int64) => INT64_TY,
                // Mixed numbers coerce to f64
                (ScalarTy::Int64 | ScalarTy::Float64, ScalarTy::Int64 | ScalarTy::Float64) => {
                    FLOAT64_TY
                }
                // Any other combination coerces to string
                _ => STRING_TY,
            },
            _ => Err(make_err(got))?,
        })
    };

    match tape.get(idx) {
        TapeElement::Null => Ok(expect),
        TapeElement::False => infer_scalar(ScalarTy::Bool, "a boolean"),
        TapeElement::True => infer_scalar(ScalarTy::Bool, "a boolean"),
        TapeElement::I64(_) | TapeElement::I32(_) => infer_scalar(ScalarTy::Int64, "a number"),
        TapeElement::F64(_) | TapeElement::F32(_) => infer_scalar(ScalarTy::Float64, "a number"),
        TapeElement::Number(s) => {
            if tape.get_string(s).parse::<i64>().is_ok() {
                infer_scalar(ScalarTy::Int64, "a number")
            } else {
                infer_scalar(ScalarTy::Float64, "a number")
            }
        }
        TapeElement::String(_) => infer_scalar(ScalarTy::String, "a string"),
        TapeElement::StartList(end) => {
            let (expect, expect_elem) = match *expect {
                InferredTy::Never => (ARRAY_OF_NEVER_TY, NEVER_TY),
                InferredTy::Array(inner) => (expect, inner),
                _ => Err(make_err("an array"))?,
            };

            let elem = iter_elements(tape, idx, end).try_fold(expect_elem, |expect, idx| {
                infer_type(tape, idx, expect, arena)
            })?;

            Ok(if std::ptr::eq(elem, expect_elem) {
                expect
            } else {
                arena.alloc(InferredTy::Array(elem))
            })
        }
        TapeElement::EndList(_) => unreachable!(),
        TapeElement::StartObject(end) => {
            let (expect, expect_fields) = match *expect {
                InferredTy::Never => (EMPTY_OBJECT_TY, &[] as &[_]),
                InferredTy::Object(fields) => (expect, fields),
                _ => Err(make_err("an object"))?,
            };

            let mut num_fields = expect_fields.len();
            let mut substs = HashMap::<usize, (&'a str, &'a InferredTy<'a>)>::new();

            for (key, idx) in iter_fields(tape, idx, end) {
                let existing_field = expect_fields
                    .iter()
                    .copied()
                    .enumerate()
                    .find(|(_, (existing_key, _))| *existing_key == key);

                match existing_field {
                    Some((field_idx, (key, expect_ty))) => {
                        let ty = infer_type(tape, idx, expect_ty, arena)?;
                        if !std::ptr::eq(ty, expect_ty) {
                            substs.insert(field_idx, (key, ty));
                        }
                    }
                    None => {
                        let field_idx = num_fields;
                        num_fields += 1;
                        let key = arena.alloc_str(key);
                        let ty = infer_type(tape, idx, NEVER_TY, arena)?;
                        substs.insert(field_idx, (key, ty));
                    }
                };
            }

            Ok(if substs.is_empty() {
                expect
            } else {
                let fields =
                    arena.alloc_slice_fill_with(num_fields, |idx| match substs.get(&idx) {
                        Some(subst) => *subst,
                        None => expect_fields[idx],
                    });
                arena.alloc(InferredTy::Object(fields))
            })
        }
        TapeElement::EndObject(_) => unreachable!(),
    }
}

fn iter_elements(tape: &Tape, start: u32, end: u32) -> impl Iterator<Item = u32> {
    let mut idx = start + 1;
    std::iter::from_fn(move || {
        (idx < end).then(|| {
            let elem_idx = idx;
            idx = tape.next(idx, "??").unwrap();
            elem_idx
        })
    })
}

fn iter_fields<'a>(tape: &'a Tape, start: u32, end: u32) -> impl Iterator<Item = (&'a str, u32)> {
    let mut iter = iter_elements(tape, start, end);
    std::iter::from_fn(move || {
        let key_idx = iter.next()?;
        let key = match tape.get(key_idx) {
            TapeElement::String(s) => tape.get_string(s),
            _ => unreachable!(),
        };
        let field_idx = iter.next().unwrap();
        Some((key, field_idx))
    })
}

impl<'a> InferredTy<'a> {
    // fn make_array(elem: Self, arena: &'a Bump) -> Self {
    //     Self::Array(arena.alloc(elem))
    // }

    // fn make_object<I>(fields: I, arena: &'a Bump) -> Self
    // where
    //     I: IntoIterator<Item = (&'a str, &'a InferredTy<'a>)>,
    //     I::IntoIter: ExactSizeIterator,
    // {
    //     let fields = arena.alloc_slice_fill_iter(fields);
    //     Self::Object(fields)
    // }

    // fn union(self, other: Self, arena: &'a Bump) -> Result<Self> {
    //     Ok(match (self, other) {
    //         (ty, Self::Never) | (Self::Never, ty) => ty,

    //         (Self::Scalar(left), Self::Scalar(right)) => Self::Scalar(ScalarTy::union(left, right)),

    //         (Self::Array(left), Self::Array(right)) => {
    //             let elem = left.union(*right, arena)?;
    //             Self::make_array(elem, arena)
    //         }

    //         (Self::Object(left), Self::Object(right)) => Self::union_objects(left, right, arena)?,

    //         _ => Err(ArrowError::JsonError(format!(
    //             "Incompatible type found during schema inference: {self:?} vs {other:?}"
    //         )))?,
    //     })
    // }

    // fn union_objects(
    //     left: InferredFields<'a>,
    //     right: InferredFields<'a>,
    //     arena: &'a Bump,
    // ) -> Result<Self> {
    //     let mut fields = IndexMap::new();

    //     for (key, ty) in left.iter().copied() {
    //         fields.insert(key, ty);
    //     }

    //     for (key, ty) in right.iter().copied() {
    //         use indexmap::map::Entry;
    //         match fields.entry(key) {
    //             Entry::Occupied(mut entry) => {
    //                 let merged = &*arena.alloc(entry.get().union(*ty, arena)?);
    //                 entry.insert(merged);
    //             }
    //             Entry::Vacant(entry) => {
    //                 entry.insert(ty);
    //             }
    //         }
    //     }

    //     Ok(Self::make_object(fields, arena))
    // }

    fn into_datatype(self) -> DataType {
        match self {
            Self::Never => DataType::Null,
            Self::Scalar(s) => s.into_datatype(),
            Self::Array(elem) => DataType::List(elem.into_list_field().into()),
            Self::Object(fields) => {
                DataType::Struct(fields.iter().map(|(key, ty)| ty.into_field(*key)).collect())
            }
        }
    }

    fn into_field(self, name: impl Into<String>) -> Field {
        Field::new(name, self.into_datatype(), true)
    }

    fn into_list_field(self) -> Field {
        Field::new_list_field(self.into_datatype(), true)
    }

    fn into_schema(self) -> Result<Schema> {
        let Self::Object(fields) = self else {
            Err(ArrowError::JsonError(format!(
                "Expected JSON object, found {self:?}",
            )))?
        };

        let fields = fields
            .iter()
            .map(|(key, ty)| ty.into_field(*key))
            .collect::<Fields>();

        Ok(Schema::new(fields))
    }
}

impl ScalarTy {
    fn union(left: Self, right: Self) -> Self {
        match (left, right) {
            _ if left == right => left,
            (Self::Int64, Self::Float64) => Self::Float64,
            (Self::Float64, Self::Int64) => Self::Float64,
            _ => Self::String,
        }
    }

    fn into_datatype(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::String => DataType::Utf8,
        }
    }
}

fn infer_json_type<'a>(value: &Value, arena: &'a Bump) -> Result<InferredTy<'a>> {
    Ok(match value {
        Value::Null => InferredTy::Never,
        Value::Bool(_) => InferredTy::Scalar(ScalarTy::Bool),
        Value::Number(n) => {
            if n.is_i64() {
                InferredTy::Scalar(ScalarTy::Int64)
            } else {
                InferredTy::Scalar(ScalarTy::Float64)
            }
        }
        Value::String(_) => InferredTy::Scalar(ScalarTy::String),
        Value::Array(elements) => {
            let elem_ty = elements
                .iter()
                .map(|value| infer_json_type(value, arena))
                .try_fold(InferredTy::Never, |a, b| a.union(b?, arena))?;
            InferredTy::make_array(elem_ty, arena)
        }
        Value::Object(fields) => {
            let fields = fields
                .iter()
                .map(|(key, value)| {
                    let key = &*arena.alloc_str(key);
                    let value = &*arena.alloc(infer_json_type(value, arena)?);
                    Ok((key, value))
                })
                .collect::<Result<Vec<_>>>()?;
            InferredTy::make_object(fields, arena)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::{BufReader, Cursor};
    use std::sync::Arc;

    #[test]
    fn test_json_infer_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", list_type_of(DataType::Float64), true),
            Field::new("c", list_type_of(DataType::Boolean), true),
            Field::new("d", DataType::Utf8, true),
        ]);

        let mut reader = BufReader::new(File::open("test/data/arrays.json").unwrap());
        let (inferred_schema, n_rows) = infer_json_schema_from_seekable(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, schema);
        assert_eq!(n_rows, 3);

        let file = File::open("test/data/arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let (inferred_schema, n_rows) = infer_json_schema(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, schema);
        assert_eq!(n_rows, 3);
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
            "{{ \"bigger_than_i64_max\": {bigger_than_i64_max}, \"smaller_than_i64_min\": {smaller_than_i64_min} }}",
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
    fn test_invalid_json_infer_schema() {
        let re = infer_json_schema_from_seekable(Cursor::new(b"}"), None);
        assert_eq!(
            re.err().unwrap().to_string(),
            "Json error: Encountered unexpected '}' whilst parsing value",
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
            Field::new("in", DataType::Int64, true),
            Field::new("ni", DataType::Int64, true),
            Field::new("ns", DataType::Utf8, true),
            Field::new("sn", DataType::Utf8, true),
            Field::new("n", DataType::Null, true),
            Field::new("an", list_type_of(DataType::Null), true),
            Field::new("na", list_type_of(DataType::Null), true),
            Field::new("nas", list_type_of(DataType::Utf8), true),
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

    /// Shorthand for building list data type of `ty`
    fn list_type_of(ty: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(ty, true)))
    }
}
