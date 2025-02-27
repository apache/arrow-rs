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

//! Transfer data between the Arrow memory format and JSON line-delimited records.
//!
//! See the module level documentation for the
//! [`reader`] and [`writer`] for usage examples.
//!
//! # Binary Data
//!
//! As per [RFC7159] JSON cannot encode arbitrary binary data. A common approach to workaround
//! this is to use a [binary-to-text encoding] scheme, such as base64, to encode the
//! input data and then decode it on output.
//!
//! ```
//! # use std::io::Cursor;
//! # use std::sync::Arc;
//! # use arrow_array::{BinaryArray, RecordBatch, StringArray};
//! # use arrow_array::cast::AsArray;
//! # use arrow_cast::base64::{b64_decode, b64_encode, BASE64_STANDARD};
//! # use arrow_json::{LineDelimitedWriter, ReaderBuilder};
//! #
//! // The data we want to write
//! let input = BinaryArray::from(vec![b"\xDE\x00\xFF".as_ref()]);
//!
//! // Base64 encode it to a string
//! let encoded: StringArray = b64_encode(&BASE64_STANDARD, &input);
//!
//! // Write the StringArray to JSON
//! let batch = RecordBatch::try_from_iter([("col", Arc::new(encoded) as _)]).unwrap();
//! let mut buf = Vec::with_capacity(1024);
//! let mut writer = LineDelimitedWriter::new(&mut buf);
//! writer.write(&batch).unwrap();
//! writer.finish().unwrap();
//!
//! // Read the JSON data
//! let cursor = Cursor::new(buf);
//! let mut reader = ReaderBuilder::new(batch.schema()).build(cursor).unwrap();
//! let batch = reader.next().unwrap().unwrap();
//!
//! // Reverse the base64 encoding
//! let col: BinaryArray = batch.column(0).as_string::<i32>().clone().into();
//! let output = b64_decode(&BASE64_STANDARD, &col).unwrap();
//!
//! assert_eq!(input, output);
//! ```
//!
//! [RFC7159]: https://datatracker.ietf.org/doc/html/rfc7159#section-8.1
//! [binary-to-text encoding]: https://en.wikipedia.org/wiki/Binary-to-text_encoding
//!

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

pub mod reader;
pub mod writer;

pub use self::reader::{Reader, ReaderBuilder};
pub use self::writer::{ArrayWriter, LineDelimitedWriter, Writer, WriterBuilder};
use half::f16;
use serde_json::{Number, Value};

/// Specifies what is considered valid JSON when reading or writing
/// RecordBatches or StructArrays.
///
/// This enum controls which form(s) the Reader will accept and which form the
/// Writer will produce. For example, if the RecordBatch Schema is
/// `[("a", Int32), ("r", Struct([("b", Boolean), ("c", Utf8)]))]`
/// then a Reader with [`StructMode::ObjectOnly`] would read rows of the form
/// `{"a": 1, "r": {"b": true, "c": "cat"}}` while with ['StructMode::ListOnly']
/// would read rows of the form `[1, [true, "cat"]]`. A Writer would produce
/// rows formatted similarly.
///
/// The list encoding is more compact if the schema is known, and is used by
/// tools such as [Presto] and [Trino].
///
/// When reading objects, the order of the key does not matter. When reading
/// lists, the entries must be the same number and in the same order as the
/// struct fields. Map columns are not affected by this option.
///
/// [Presto]: https://prestodb.io/docs/current/develop/client-protocol.html#important-queryresults-attributes
/// [Trino]: https://trino.io/docs/current/develop/client-protocol.html#important-queryresults-attributes
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum StructMode {
    #[default]
    /// Encode/decode structs as objects (e.g., {"a": 1, "b": "c"})
    ObjectOnly,
    /// Encode/decode structs as lists (e.g., [1, "c"])
    ListOnly,
}

/// Trait declaring any type that is serializable to JSON. This includes all primitive types (bool, i32, etc.).
pub trait JsonSerializable: 'static {
    /// Converts self into json value if its possible
    fn into_json_value(self) -> Option<Value>;
}

macro_rules! json_serializable {
    ($t:ty) => {
        impl JsonSerializable for $t {
            fn into_json_value(self) -> Option<Value> {
                Some(self.into())
            }
        }
    };
}

json_serializable!(bool);
json_serializable!(u8);
json_serializable!(u16);
json_serializable!(u32);
json_serializable!(u64);
json_serializable!(i8);
json_serializable!(i16);
json_serializable!(i32);
json_serializable!(i64);

impl JsonSerializable for i128 {
    fn into_json_value(self) -> Option<Value> {
        // Serialize as string to avoid issues with arbitrary_precision serde_json feature
        // - https://github.com/serde-rs/json/issues/559
        // - https://github.com/serde-rs/json/issues/845
        // - https://github.com/serde-rs/json/issues/846
        Some(self.to_string().into())
    }
}

impl JsonSerializable for f16 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(f64::from(self) * 1000.0) / 1000.0).map(Value::Number)
    }
}

impl JsonSerializable for f32 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(self as f64 * 1000.0) / 1000.0).map(Value::Number)
    }
}

impl JsonSerializable for f64 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(self).map(Value::Number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::Value::{Bool, Number as VNumber, String as VString};

    #[test]
    fn test_arrow_native_type_to_json() {
        assert_eq!(Some(Bool(true)), true.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i64.into_json_value());
        assert_eq!(Some(VString("1".to_string())), 1i128.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u64.into_json_value());
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01.into_json_value()
        );
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01f64.into_json_value()
        );
        assert_eq!(None, f32::NAN.into_json_value());
    }

    #[test]
    fn test_json_roundtrip_structs() {
        use crate::writer::LineDelimited;
        use arrow_schema::DataType;
        use arrow_schema::Field;
        use arrow_schema::Fields;
        use arrow_schema::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
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
        ]));

        {
            let object_input = r#"{"c1":{"c11":1,"c12":{"c121":"e"}},"c2":"a"}
{"c1":{"c12":{"c121":"f"}},"c2":"b"}
{"c1":{"c11":5,"c12":{"c121":"g"}},"c2":"c"}
"#
            .as_bytes();
            let object_reader = ReaderBuilder::new(schema.clone())
                .with_struct_mode(StructMode::ObjectOnly)
                .build(object_input)
                .unwrap();

            let mut object_output: Vec<u8> = Vec::new();
            let mut object_writer = WriterBuilder::new()
                .with_struct_mode(StructMode::ObjectOnly)
                .build::<_, LineDelimited>(&mut object_output);
            for batch_res in object_reader {
                object_writer.write(&batch_res.unwrap()).unwrap();
            }
            assert_eq!(object_input, &object_output);
        }

        {
            let list_input = r#"[[1,["e"]],"a"]
[[null,["f"]],"b"]
[[5,["g"]],"c"]
"#
            .as_bytes();
            let list_reader = ReaderBuilder::new(schema.clone())
                .with_struct_mode(StructMode::ListOnly)
                .build(list_input)
                .unwrap();

            let mut list_output: Vec<u8> = Vec::new();
            let mut list_writer = WriterBuilder::new()
                .with_struct_mode(StructMode::ListOnly)
                .build::<_, LineDelimited>(&mut list_output);
            for batch_res in list_reader {
                list_writer.write(&batch_res.unwrap()).unwrap();
            }
            assert_eq!(list_input, &list_output);
        }
    }
}
