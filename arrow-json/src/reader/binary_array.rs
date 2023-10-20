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

use arrow_array::builder::GenericBinaryBuilder;
use arrow_array::{Array, GenericBinaryArray, OffsetSizeTrait};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use std::borrow::Cow;
use std::marker::PhantomData;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;

/// A [`BinaryArrayDecoder`] is responsible for converting json to the desired binary format
pub trait BinaryArrayDecoder: Send {
    /// Decodes the reference into the desired format or returns it unchanged or None for null
    fn decode_string<'a>(&self, buffer: &'a str) -> Option<Cow<'a, str>>;

    /// Returns the anticipated length of the string after conversion or None for null
    fn get_string_length(&self, buffer: &str) -> Option<usize>;

    /// Allows overriding the behavior for null records
    fn null_string_replacement(&self) -> Option<String> {
        None
    }
}

pub struct BinaryArrayDecoderWrapper<O: OffsetSizeTrait> {
    decoder: Box<dyn BinaryArrayDecoder>,
    phantom: PhantomData<O>,
    size: Option<usize>,
}

impl<O: OffsetSizeTrait> BinaryArrayDecoderWrapper<O> {
    pub fn new(decoder: Box<dyn BinaryArrayDecoder>, size: Option<usize>) -> Self {
        Self {
            decoder,
            phantom: Default::default(),
            size,
        }
    }
}

impl<O: OffsetSizeTrait> ArrayDecoder for BinaryArrayDecoderWrapper<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let (null_replacement, replace_null, replacement_length) = self
            .decoder
            .null_string_replacement()
            .map(|s| {
                let len = s.len();
                (s, true, len)
            })
            .unwrap_or((String::new(), false, 0));
        let data_capacity = match self.size {
            Some(data_capacity) => data_capacity,
            None => {
                let mut data_capacity = 0;
                for p in pos {
                    match tape.get(*p) {
                        TapeElement::String(idx) => {
                            if let Some(size) =
                                self.decoder.get_string_length(tape.get_string(idx))
                            {
                                data_capacity += size;
                            }
                        }
                        TapeElement::Null if replace_null => {
                            data_capacity += replacement_length
                        }
                        TapeElement::Null if !replace_null => {}
                        _ => return Err(tape.error(*p, "binary")),
                    }
                }

                data_capacity
            }
        };

        if O::from_usize(data_capacity).is_none() {
            return Err(ArrowError::JsonError(format!(
                "offset overflow decoding {}",
                GenericBinaryArray::<O>::DATA_TYPE
            )));
        }

        let empty: [u8; 0] = [];
        let mut builder =
            GenericBinaryBuilder::<O>::with_capacity(pos.len(), data_capacity);
        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    match self.decoder.decode_string(tape.get_string(idx)) {
                        Some(value) => builder.append_value(value.as_bytes()),
                        None => builder.append_null(),
                    }
                }
                TapeElement::Null if !replace_null => builder.append_value(&empty),
                TapeElement::Null if replace_null => {
                    builder.append_value(&null_replacement)
                }
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use arrow_array::{
        cast::AsArray, types::GenericBinaryType, OffsetSizeTrait, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};

    use crate::reader::{
        binary_array::BinaryArrayDecoderWrapper, make_decoder, tape::TapeDecoder,
        ArrayDecoder, BinaryArrayDecoder, BinaryDecoderProvider,
    };
    const CONTENT: &str = r#"
"first"
"second"
"third"
    "#;
    const FIXED_CONTENT: &str = r#"
"first "
"second"
"third "
        "#;
    const NULL_CONTENT: &str = r#"
{"id": 1, "data": "this"}
{"id": 2}
{"id": 3, "data": "is"}
{"id": 4}
{"id": 5, "data": "a" }
{"id": 6}
{"id": 7, "data": "dream"}
"#;
    struct TestBinaryDecoder {
        length: Option<usize>,
    }

    impl TestBinaryDecoder {
        fn new(length: Option<usize>) -> Self {
            Self { length }
        }
    }

    impl BinaryArrayDecoder for TestBinaryDecoder {
        fn decode_string<'a>(
            &self,
            buffer: &'a str,
        ) -> Option<std::borrow::Cow<'a, str>> {
            if let Some(length) = self.length {
                if buffer.len() != length {
                    Some(Cow::Owned("x".repeat(length)))
                } else {
                    Some(std::borrow::Cow::Borrowed(buffer))
                }
            } else {
                Some(Cow::Borrowed(buffer))
            }
        }

        fn get_string_length(&self, buffer: &str) -> Option<usize> {
            Some(buffer.len())
        }
    }

    struct NullOverrideDecoder {}

    impl BinaryArrayDecoder for NullOverrideDecoder {
        fn decode_string<'a>(
            &self,
            buffer: &'a str,
        ) -> Option<std::borrow::Cow<'a, str>> {
            Some(Cow::from(buffer))
        }

        fn get_string_length(&self, buffer: &str) -> Option<usize> {
            Some(buffer.len())
        }

        fn null_string_replacement(&self) -> Option<String> {
            Some("test".into())
        }
    }

    #[test]
    fn test_decode_binary_success() {
        let inner = Box::new(TestBinaryDecoder::new(None));
        let decoder = BinaryArrayDecoderWrapper::<i32>::new(inner, None);
        test_decode_success(decoder, CONTENT);
    }

    #[test]
    fn test_decode_fixed_binary_success() {
        let inner = Box::new(TestBinaryDecoder::new(Some(6)));
        let decoder = BinaryArrayDecoderWrapper::<i32>::new(inner, None);
        test_decode_success(decoder, FIXED_CONTENT);
    }

    #[test]
    fn test_decode_large_binary_success() {
        let inner = Box::new(TestBinaryDecoder::new(None));
        let decoder = BinaryArrayDecoderWrapper::<i64>::new(inner, None);
        test_decode_success(decoder, CONTENT);
    }

    #[test]
    fn test_decode_with_null_override() {
        let mut tape_decoder = TapeDecoder::new(16, 1);
        let _ = tape_decoder.decode(NULL_CONTENT.as_bytes()).unwrap();
        let tape = tape_decoder.finish().unwrap();
        let mut next_object = 1;
        let pos: Vec<_> = (0..tape.num_rows())
            .map(|_| {
                let next = tape.next(next_object, "row").unwrap();
                std::mem::replace(&mut next_object, next)
            })
            .collect();
        let provider: BinaryDecoderProvider = |_, _| Box::new(NullOverrideDecoder {});
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", arrow_schema::DataType::Binary, false),
        ]);
        let s = DataType::Struct(fields.clone());
        let mut decoder = make_decoder(s, false, true, false, Some(&provider)).unwrap();
        let data = decoder.decode(&tape, &pos).expect("this should decode");
        let schema = Arc::new(Schema::new(fields.clone()));
        let batch = RecordBatch::from(arrow_array::StructArray::from(data))
            .with_schema(schema.clone())
            .unwrap();
        let binary_values = batch
            .column_by_name("data")
            .unwrap()
            .as_bytes::<GenericBinaryType<i32>>();
        assert_eq!(7, binary_values.iter().count());
        let expected_values = &["this", "test", "is", "test", "a", "test", "dream"];
        for i in 0..6 {
            let actual = binary_values.value(i);
            assert_eq!(expected_values[i].as_bytes(), actual);
        }
    }

    fn test_decode_success<O: OffsetSizeTrait>(
        mut decoder: BinaryArrayDecoderWrapper<O>,
        input: &str,
    ) {
        let mut tape_decoder = TapeDecoder::new(16, 1);
        let _ = tape_decoder.decode(input.as_bytes()).unwrap();
        let tape = tape_decoder.finish().unwrap();
        let mut next_object = 1;
        let pos: Vec<_> = (0..tape.num_rows())
            .map(|_| {
                let next = tape.next(next_object, "row").unwrap();
                std::mem::replace(&mut next_object, next)
            })
            .collect();
        decoder.decode(&tape, &pos).expect("this should decode");
    }
}
