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

//! Parquet schema parser.
//! Provides methods to parse and validate string message type into Parquet
//! [`Type`](crate::schema::types::Type).
//!
//! # Example
//!
//! ```rust
//! use parquet::schema::parser::parse_message_type;
//!
//! let message_type = "
//!   message spark_schema {
//!     OPTIONAL BYTE_ARRAY a (UTF8);
//!     REQUIRED INT32 b;
//!     REQUIRED DOUBLE c;
//!     REQUIRED BOOLEAN d;
//!     OPTIONAL group e (LIST) {
//!       REPEATED group list {
//!         REQUIRED INT32 element;
//!       }
//!     }
//!   }
//! ";
//!
//! let schema = parse_message_type(message_type).expect("Expected valid schema");
//! println!("{:?}", schema);
//! ```

use std::sync::Arc;

use crate::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::{Type, TypePtr};

/// Parses message type as string into a Parquet [`Type`](crate::schema::types::Type)
/// which, for example, could be used to extract individual columns. Returns Parquet
/// general error when parsing or validation fails.
pub fn parse_message_type(message_type: &str) -> Result<Type> {
    let mut parser = Parser {
        tokenizer: &mut Tokenizer::from_str(message_type),
    };
    parser.parse_message_type()
}

/// Tokenizer to split message type string into tokens that are separated using characters
/// defined in `is_schema_delim` method. Tokenizer also preserves delimiters as tokens.
/// Tokenizer provides Iterator interface to process tokens; it also allows to step back
/// to reprocess previous tokens.
struct Tokenizer<'a> {
    // List of all tokens for a string
    tokens: Vec<&'a str>,
    // Current index of vector
    index: usize,
}

impl<'a> Tokenizer<'a> {
    // Create tokenizer from message type string
    pub fn from_str(string: &'a str) -> Self {
        let vec = string
            .split_whitespace()
            .flat_map(Self::split_token)
            .collect();
        Tokenizer {
            tokens: vec,
            index: 0,
        }
    }

    // List of all special characters in schema
    fn is_schema_delim(c: char) -> bool {
        c == ';' || c == '{' || c == '}' || c == '(' || c == ')' || c == '=' || c == ','
    }

    /// Splits string into tokens; input string can already be token or can contain
    /// delimiters, e.g. required" -> Vec("required") and
    /// "(UTF8);" -> Vec("(", "UTF8", ")", ";")
    fn split_token(string: &str) -> Vec<&str> {
        let mut buffer: Vec<&str> = Vec::new();
        let mut tail = string;
        while let Some(index) = tail.find(Self::is_schema_delim) {
            let (h, t) = tail.split_at(index);
            if !h.is_empty() {
                buffer.push(h);
            }
            buffer.push(&t[0..1]);
            tail = &t[1..];
        }
        if !tail.is_empty() {
            buffer.push(tail);
        }
        buffer
    }

    // Move pointer to a previous element
    fn backtrack(&mut self) {
        self.index -= 1;
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.index < self.tokens.len() {
            self.index += 1;
            Some(self.tokens[self.index - 1])
        } else {
            None
        }
    }
}

/// Internal Schema parser.
/// Traverses message type using tokenizer and parses each group/primitive type
/// recursively.
struct Parser<'a> {
    tokenizer: &'a mut Tokenizer<'a>,
}

// Utility function to assert token on validity.
fn assert_token(token: Option<&str>, expected: &str) -> Result<()> {
    match token {
        Some(value) if value == expected => Ok(()),
        Some(other) => Err(general_err!(
            "Expected '{}', found token '{}'",
            expected,
            other
        )),
        None => Err(general_err!(
            "Expected '{}', but no token found (None)",
            expected
        )),
    }
}

// Utility function to parse i32 or return general error.
#[inline]
fn parse_i32(
    value: Option<&str>,
    not_found_msg: &str,
    parse_fail_msg: &str,
) -> Result<i32> {
    value
        .ok_or_else(|| general_err!(not_found_msg))
        .and_then(|v| v.parse::<i32>().map_err(|_| general_err!(parse_fail_msg)))
}

// Utility function to parse boolean or return general error.
#[inline]
fn parse_bool(
    value: Option<&str>,
    not_found_msg: &str,
    parse_fail_msg: &str,
) -> Result<bool> {
    value
        .ok_or_else(|| general_err!(not_found_msg))
        .and_then(|v| {
            v.to_lowercase()
                .parse::<bool>()
                .map_err(|_| general_err!(parse_fail_msg))
        })
}

// Utility function to parse TimeUnit or return general error.
fn parse_timeunit(
    value: Option<&str>,
    not_found_msg: &str,
    parse_fail_msg: &str,
) -> Result<TimeUnit> {
    value
        .ok_or_else(|| general_err!(not_found_msg))
        .and_then(|v| match v.to_uppercase().as_str() {
            "MILLIS" => Ok(TimeUnit::MILLIS(Default::default())),
            "MICROS" => Ok(TimeUnit::MICROS(Default::default())),
            "NANOS" => Ok(TimeUnit::NANOS(Default::default())),
            _ => Err(general_err!(parse_fail_msg)),
        })
}

impl<'a> Parser<'a> {
    // Entry function to parse message type, uses internal tokenizer.
    fn parse_message_type(&mut self) -> Result<Type> {
        // Check that message type starts with "message".
        match self.tokenizer.next() {
            Some("message") => {
                let name = self
                    .tokenizer
                    .next()
                    .ok_or_else(|| general_err!("Expected name, found None"))?;
                let mut fields = self.parse_child_types()?;
                Type::group_type_builder(name)
                    .with_fields(&mut fields)
                    .build()
            }
            _ => Err(general_err!("Message type does not start with 'message'")),
        }
    }

    // Parses child types for a current group type.
    // This is only invoked on root and group types.
    fn parse_child_types(&mut self) -> Result<Vec<TypePtr>> {
        assert_token(self.tokenizer.next(), "{")?;
        let mut vec = Vec::new();
        while let Some(value) = self.tokenizer.next() {
            if value == "}" {
                break;
            } else {
                self.tokenizer.backtrack();
                vec.push(Arc::new(self.add_type()?));
            }
        }
        Ok(vec)
    }

    fn add_type(&mut self) -> Result<Type> {
        // Parse repetition
        let repetition = self
            .tokenizer
            .next()
            .ok_or_else(|| general_err!("Expected repetition, found None"))
            .and_then(|v| v.to_uppercase().parse::<Repetition>())?;

        match self.tokenizer.next() {
            Some(group) if group.to_uppercase() == "GROUP" => {
                self.add_group_type(Some(repetition))
            }
            Some(type_string) => {
                let physical_type = type_string.to_uppercase().parse::<PhysicalType>()?;
                self.add_primitive_type(repetition, physical_type)
            }
            None => Err(general_err!("Invalid type, could not extract next token")),
        }
    }

    fn add_group_type(&mut self, repetition: Option<Repetition>) -> Result<Type> {
        // Parse name of the group type
        let name = self
            .tokenizer
            .next()
            .ok_or_else(|| general_err!("Expected name, found None"))?;

        // Parse logical or converted type if exists
        let (logical_type, converted_type) = if let Some("(") = self.tokenizer.next() {
            let tpe = self
                .tokenizer
                .next()
                .ok_or_else(|| general_err!("Expected converted type, found None"))
                .and_then(|v| {
                    // Try logical type first
                    let upper = v.to_uppercase();
                    let logical = upper.parse::<LogicalType>();
                    match logical {
                        Ok(logical) => Ok((
                            Some(logical.clone()),
                            ConvertedType::from(Some(logical)),
                        )),
                        Err(_) => Ok((None, upper.parse::<ConvertedType>()?)),
                    }
                })?;
            assert_token(self.tokenizer.next(), ")")?;
            tpe
        } else {
            self.tokenizer.backtrack();
            (None, ConvertedType::NONE)
        };

        // Parse optional id
        let id = if let Some("=") = self.tokenizer.next() {
            self.tokenizer.next().and_then(|v| v.parse::<i32>().ok())
        } else {
            self.tokenizer.backtrack();
            None
        };

        let mut fields = self.parse_child_types()?;
        let mut builder = Type::group_type_builder(name)
            .with_logical_type(logical_type)
            .with_converted_type(converted_type)
            .with_fields(&mut fields);
        if let Some(rep) = repetition {
            builder = builder.with_repetition(rep);
        }
        if let Some(id) = id {
            builder = builder.with_id(id);
        }
        builder.build()
    }

    fn add_primitive_type(
        &mut self,
        repetition: Repetition,
        physical_type: PhysicalType,
    ) -> Result<Type> {
        // Read type length if the type is FIXED_LEN_BYTE_ARRAY.
        let mut length: i32 = -1;
        if physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY {
            assert_token(self.tokenizer.next(), "(")?;
            length = parse_i32(
                self.tokenizer.next(),
                "Expected length for FIXED_LEN_BYTE_ARRAY, found None",
                "Failed to parse length for FIXED_LEN_BYTE_ARRAY",
            )?;
            assert_token(self.tokenizer.next(), ")")?;
        }

        // Parse name of the primitive type
        let name = self
            .tokenizer
            .next()
            .ok_or_else(|| general_err!("Expected name, found None"))?;

        // Parse converted type
        let (logical_type, converted_type, precision, scale) = if let Some("(") =
            self.tokenizer.next()
        {
            let (mut logical, mut converted) = self
                .tokenizer
                .next()
                .ok_or_else(|| {
                    general_err!("Expected logical or converted type, found None")
                })
                .and_then(|v| {
                    let upper = v.to_uppercase();
                    let logical = upper.parse::<LogicalType>();
                    match logical {
                        Ok(logical) => Ok((
                            Some(logical.clone()),
                            ConvertedType::from(Some(logical)),
                        )),
                        Err(_) => Ok((None, upper.parse::<ConvertedType>()?)),
                    }
                })?;

            // Parse precision and scale for decimals
            let mut precision: i32 = -1;
            let mut scale: i32 = -1;

            // Parse the concrete logical type
            if let Some(tpe) = &logical {
                match tpe {
                    LogicalType::Decimal { .. } => {
                        if let Some("(") = self.tokenizer.next() {
                            precision = parse_i32(
                                self.tokenizer.next(),
                                "Expected precision, found None",
                                "Failed to parse precision for DECIMAL type",
                            )?;
                            if let Some(",") = self.tokenizer.next() {
                                scale = parse_i32(
                                    self.tokenizer.next(),
                                    "Expected scale, found None",
                                    "Failed to parse scale for DECIMAL type",
                                )?;
                                assert_token(self.tokenizer.next(), ")")?;
                            } else {
                                scale = 0
                            }
                            logical = Some(LogicalType::Decimal { scale, precision });
                            converted = ConvertedType::from(logical.clone());
                        }
                    }
                    LogicalType::Time { .. } => {
                        if let Some("(") = self.tokenizer.next() {
                            let unit = parse_timeunit(
                                self.tokenizer.next(),
                                "Invalid timeunit found",
                                "Failed to parse timeunit for TIME type",
                            )?;
                            if let Some(",") = self.tokenizer.next() {
                                let is_adjusted_to_u_t_c = parse_bool(
                                    self.tokenizer.next(),
                                    "Invalid boolean found",
                                    "Failed to parse timezone info for TIME type",
                                )?;
                                assert_token(self.tokenizer.next(), ")")?;
                                logical = Some(LogicalType::Time {
                                    is_adjusted_to_u_t_c,
                                    unit,
                                });
                                converted = ConvertedType::from(logical.clone());
                            } else {
                                // Invalid token for unit
                                self.tokenizer.backtrack();
                            }
                        }
                    }
                    LogicalType::Timestamp { .. } => {
                        if let Some("(") = self.tokenizer.next() {
                            let unit = parse_timeunit(
                                self.tokenizer.next(),
                                "Invalid timeunit found",
                                "Failed to parse timeunit for TIMESTAMP type",
                            )?;
                            if let Some(",") = self.tokenizer.next() {
                                let is_adjusted_to_u_t_c = parse_bool(
                                    self.tokenizer.next(),
                                    "Invalid boolean found",
                                    "Failed to parse timezone info for TIMESTAMP type",
                                )?;
                                assert_token(self.tokenizer.next(), ")")?;
                                logical = Some(LogicalType::Timestamp {
                                    is_adjusted_to_u_t_c,
                                    unit,
                                });
                                converted = ConvertedType::from(logical.clone());
                            } else {
                                // Invalid token for unit
                                self.tokenizer.backtrack();
                            }
                        }
                    }
                    LogicalType::Integer { .. } => {
                        if let Some("(") = self.tokenizer.next() {
                            let bit_width = parse_i32(
                                self.tokenizer.next(),
                                "Invalid bit_width found",
                                "Failed to parse bit_width for INTEGER type",
                            )? as i8;
                            match physical_type {
                                PhysicalType::INT32 => {
                                    match bit_width {
                                        8 | 16 | 32 => {}
                                        _ => {
                                            return Err(general_err!("Incorrect bit width {} for INT32", bit_width))
                                        }
                                    }
                                }
                                PhysicalType::INT64 => {
                                    if bit_width != 64 {
                                        return Err(general_err!("Incorrect bit width {} for INT64", bit_width))
                                    }
                                }
                                _ => {
                                    return Err(general_err!("Logical type Integer cannot be used with physical type {}", physical_type))
                                }
                            }
                            if let Some(",") = self.tokenizer.next() {
                                let is_signed = parse_bool(
                                    self.tokenizer.next(),
                                    "Invalid boolean found",
                                    "Failed to parse is_signed for INTEGER type",
                                )?;
                                assert_token(self.tokenizer.next(), ")")?;
                                logical = Some(LogicalType::Integer {
                                    bit_width,
                                    is_signed,
                                });
                                converted = ConvertedType::from(logical.clone());
                            } else {
                                // Invalid token for unit
                                self.tokenizer.backtrack();
                            }
                        }
                    }
                    _ => {}
                }
            } else if converted == ConvertedType::DECIMAL {
                if let Some("(") = self.tokenizer.next() {
                    // Parse precision
                    precision = parse_i32(
                        self.tokenizer.next(),
                        "Expected precision, found None",
                        "Failed to parse precision for DECIMAL type",
                    )?;

                    // Parse scale
                    scale = if let Some(",") = self.tokenizer.next() {
                        parse_i32(
                            self.tokenizer.next(),
                            "Expected scale, found None",
                            "Failed to parse scale for DECIMAL type",
                        )?
                    } else {
                        // Scale is not provided, set it to 0.
                        self.tokenizer.backtrack();
                        0
                    };

                    assert_token(self.tokenizer.next(), ")")?;
                } else {
                    self.tokenizer.backtrack();
                }
            }

            assert_token(self.tokenizer.next(), ")")?;
            (logical, converted, precision, scale)
        } else {
            self.tokenizer.backtrack();
            (None, ConvertedType::NONE, -1, -1)
        };

        // Parse optional id
        let id = if let Some("=") = self.tokenizer.next() {
            self.tokenizer.next().and_then(|v| v.parse::<i32>().ok())
        } else {
            self.tokenizer.backtrack();
            None
        };
        assert_token(self.tokenizer.next(), ";")?;

        let mut builder = Type::primitive_type_builder(name, physical_type)
            .with_repetition(repetition)
            .with_logical_type(logical_type)
            .with_converted_type(converted_type)
            .with_length(length)
            .with_precision(precision)
            .with_scale(scale);
        if let Some(id) = id {
            builder = builder.with_id(id);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_empty_string() {
        assert_eq!(Tokenizer::from_str("").next(), None);
    }

    #[test]
    fn test_tokenize_delimiters() {
        let mut iter = Tokenizer::from_str(",;{}()=");
        assert_eq!(iter.next(), Some(","));
        assert_eq!(iter.next(), Some(";"));
        assert_eq!(iter.next(), Some("{"));
        assert_eq!(iter.next(), Some("}"));
        assert_eq!(iter.next(), Some("("));
        assert_eq!(iter.next(), Some(")"));
        assert_eq!(iter.next(), Some("="));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_tokenize_delimiters_with_whitespaces() {
        let mut iter = Tokenizer::from_str(" , ; { } ( ) = ");
        assert_eq!(iter.next(), Some(","));
        assert_eq!(iter.next(), Some(";"));
        assert_eq!(iter.next(), Some("{"));
        assert_eq!(iter.next(), Some("}"));
        assert_eq!(iter.next(), Some("("));
        assert_eq!(iter.next(), Some(")"));
        assert_eq!(iter.next(), Some("="));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_tokenize_words() {
        let mut iter = Tokenizer::from_str("abc def ghi jkl mno");
        assert_eq!(iter.next(), Some("abc"));
        assert_eq!(iter.next(), Some("def"));
        assert_eq!(iter.next(), Some("ghi"));
        assert_eq!(iter.next(), Some("jkl"));
        assert_eq!(iter.next(), Some("mno"));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_tokenize_backtrack() {
        let mut iter = Tokenizer::from_str("abc;");
        assert_eq!(iter.next(), Some("abc"));
        assert_eq!(iter.next(), Some(";"));
        iter.backtrack();
        assert_eq!(iter.next(), Some(";"));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_tokenize_message_type() {
        let schema = "
    message schema {
      required int32 a;
      optional binary c (UTF8);
      required group d {
        required int32 a;
        optional binary c (UTF8);
      }
      required group e (LIST) {
        repeated group list {
          required int32 element;
        }
      }
    }
    ";
        let iter = Tokenizer::from_str(schema);
        let mut res = Vec::new();
        for token in iter {
            res.push(token);
        }
        assert_eq!(
            res,
            vec![
                "message", "schema", "{", "required", "int32", "a", ";", "optional",
                "binary", "c", "(", "UTF8", ")", ";", "required", "group", "d", "{",
                "required", "int32", "a", ";", "optional", "binary", "c", "(", "UTF8",
                ")", ";", "}", "required", "group", "e", "(", "LIST", ")", "{",
                "repeated", "group", "list", "{", "required", "int32", "element", ";",
                "}", "}", "}"
            ]
        );
    }

    #[test]
    fn test_assert_token() {
        assert!(assert_token(Some("a"), "a").is_ok());
        assert!(assert_token(Some("a"), "b").is_err());
        assert!(assert_token(None, "b").is_err());
    }

    #[test]
    fn test_parse_message_type_invalid() {
        let mut iter = Tokenizer::from_str("test");
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet error: Message type does not start with 'message'"
        );
    }

    #[test]
    fn test_parse_message_type_no_name() {
        let mut iter = Tokenizer::from_str("message");
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Parquet error: Expected name, found None"
        );
    }

    #[test]
    fn test_parse_message_type_fixed_byte_array() {
        let schema = "
    message schema {
      REQUIRED FIXED_LEN_BYTE_ARRAY col;
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());

        let schema = "
    message schema {
      REQUIRED FIXED_LEN_BYTE_ARRAY(16) col;
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_message_type_integer() {
        // Invalid integer syntax
        let schema = "
    message root {
      optional int64 f1 (INTEGER());
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert_eq!(
            result,
            Err(general_err!("Failed to parse bit_width for INTEGER type"))
        );

        // Invalid integer syntax, needs both bit-width and UTC sign
        let schema = "
    message root {
      optional int64 f1 (INTEGER(32,));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert_eq!(
            result,
            Err(general_err!("Incorrect bit width 32 for INT64"))
        );

        // Invalid integer because of non-numeric bit width
        let schema = "
    message root {
      optional int32 f1 (INTEGER(eight,true));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert_eq!(
            result,
            Err(general_err!("Failed to parse bit_width for INTEGER type"))
        );

        // Valid types
        let schema = "
    message root {
      optional int32 f1 (INTEGER(8,false));
      optional int32 f2 (INTEGER(8,true));
      optional int32 f3 (INTEGER(16,false));
      optional int32 f4 (INTEGER(16,true));
      optional int32 f5 (INTEGER(32,false));
      optional int32 f6 (INTEGER(32,true));
      optional int64 f7 (INTEGER(64,false));
      optional int64 f7 (INTEGER(64,true));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_message_type_temporal() {
        // Invalid timestamp syntax
        let schema = "
    message root {
      optional int64 f1 (TIMESTAMP();
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert_eq!(
            result,
            Err(general_err!("Failed to parse timeunit for TIMESTAMP type"))
        );

        // Invalid timestamp syntax, needs both unit and UTC adjustment
        let schema = "
    message root {
      optional int64 f1 (TIMESTAMP(MILLIS,));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert_eq!(
            result,
            Err(general_err!(
                "Failed to parse timezone info for TIMESTAMP type"
            ))
        );

        // Invalid timestamp because of unknown unit
        let schema = "
    message root {
      optional int64 f1 (TIMESTAMP(YOCTOS,));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert_eq!(
            result,
            Err(general_err!("Failed to parse timeunit for TIMESTAMP type"))
        );

        // Valid types
        let schema = "
    message root {
      optional int32 f1 (DATE);
      optional int32 f2 (TIME(MILLIS,true));
      optional int64 f3 (TIME(MICROS,false));
      optional int64 f4 (TIME(NANOS,true));
      optional int64 f5 (TIMESTAMP(MILLIS,true));
      optional int64 f6 (TIMESTAMP(MICROS,true));
      optional int64 f7 (TIMESTAMP(NANOS,false));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_message_type_decimal() {
        // It is okay for decimal to omit precision and scale with right syntax.
        // Here we test wrong syntax of decimal type

        // Invalid decimal syntax
        let schema = "
    message root {
      optional int32 f1 (DECIMAL();
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());

        // Invalid decimal, need precision and scale
        let schema = "
    message root {
      optional int32 f1 (DECIMAL());
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());

        // Invalid decimal because of `,` - has precision, needs scale
        let schema = "
    message root {
      optional int32 f1 (DECIMAL(8,));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());

        // Invalid decimal because, we always require either precision or scale to be
        // specified as part of converted type
        let schema = "
    message root {
      optional int32 f3 (DECIMAL);
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_err());

        // Valid decimal (precision, scale)
        let schema = "
    message root {
      optional int32 f1 (DECIMAL(8, 3));
      optional int32 f2 (DECIMAL(8));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let result = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type();
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_message_type_compare_1() {
        let schema = "
    message root {
      optional fixed_len_byte_array(5) f1 (DECIMAL(9, 3));
      optional fixed_len_byte_array (16) f2 (DECIMAL (38, 18));
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let message = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type()
        .unwrap();

        let expected = Type::group_type_builder("root")
            .with_fields(&mut vec![
                Arc::new(
                    Type::primitive_type_builder(
                        "f1",
                        PhysicalType::FIXED_LEN_BYTE_ARRAY,
                    )
                    .with_logical_type(Some(LogicalType::Decimal {
                        precision: 9,
                        scale: 3,
                    }))
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_length(5)
                    .with_precision(9)
                    .with_scale(3)
                    .build()
                    .unwrap(),
                ),
                Arc::new(
                    Type::primitive_type_builder(
                        "f2",
                        PhysicalType::FIXED_LEN_BYTE_ARRAY,
                    )
                    .with_logical_type(Some(LogicalType::Decimal {
                        precision: 38,
                        scale: 18,
                    }))
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_length(16)
                    .with_precision(38)
                    .with_scale(18)
                    .build()
                    .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        assert_eq!(message, expected);
    }

    #[test]
    fn test_parse_message_type_compare_2() {
        let schema = "
    message root {
      required group a0 {
        optional group a1 (LIST) {
          repeated binary a2 (UTF8);
        }

        optional group b1 (LIST) {
          repeated group b2 {
            optional int32 b3;
            optional double b4;
          }
        }
      }
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let message = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type()
        .unwrap();

        let expected = Type::group_type_builder("root")
            .with_fields(&mut vec![Arc::new(
                Type::group_type_builder("a0")
                    .with_repetition(Repetition::REQUIRED)
                    .with_fields(&mut vec![
                        Arc::new(
                            Type::group_type_builder("a1")
                                .with_repetition(Repetition::OPTIONAL)
                                .with_logical_type(Some(LogicalType::List))
                                .with_converted_type(ConvertedType::LIST)
                                .with_fields(&mut vec![Arc::new(
                                    Type::primitive_type_builder(
                                        "a2",
                                        PhysicalType::BYTE_ARRAY,
                                    )
                                    .with_repetition(Repetition::REPEATED)
                                    .with_converted_type(ConvertedType::UTF8)
                                    .build()
                                    .unwrap(),
                                )])
                                .build()
                                .unwrap(),
                        ),
                        Arc::new(
                            Type::group_type_builder("b1")
                                .with_repetition(Repetition::OPTIONAL)
                                .with_logical_type(Some(LogicalType::List))
                                .with_converted_type(ConvertedType::LIST)
                                .with_fields(&mut vec![Arc::new(
                                    Type::group_type_builder("b2")
                                        .with_repetition(Repetition::REPEATED)
                                        .with_fields(&mut vec![
                                            Arc::new(
                                                Type::primitive_type_builder(
                                                    "b3",
                                                    PhysicalType::INT32,
                                                )
                                                .build()
                                                .unwrap(),
                                            ),
                                            Arc::new(
                                                Type::primitive_type_builder(
                                                    "b4",
                                                    PhysicalType::DOUBLE,
                                                )
                                                .build()
                                                .unwrap(),
                                            ),
                                        ])
                                        .build()
                                        .unwrap(),
                                )])
                                .build()
                                .unwrap(),
                        ),
                    ])
                    .build()
                    .unwrap(),
            )])
            .build()
            .unwrap();

        assert_eq!(message, expected);
    }

    #[test]
    fn test_parse_message_type_compare_3() {
        let schema = "
    message root {
      required int32 _1 (INT_8);
      required int32 _2 (INT_16);
      required float _3;
      required double _4;
      optional int32 _5 (DATE);
      optional binary _6 (UTF8);
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let message = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type()
        .unwrap();

        let mut fields = vec![
            Arc::new(
                Type::primitive_type_builder("_1", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_8)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_2", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_16)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_3", PhysicalType::FLOAT)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_4", PhysicalType::DOUBLE)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_5", PhysicalType::INT32)
                    .with_logical_type(Some(LogicalType::Date))
                    .with_converted_type(ConvertedType::DATE)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_6", PhysicalType::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::UTF8)
                    .build()
                    .unwrap(),
            ),
        ];

        let expected = Type::group_type_builder("root")
            .with_fields(&mut fields)
            .build()
            .unwrap();
        assert_eq!(message, expected);
    }

    #[test]
    fn test_parse_message_type_compare_4() {
        let schema = "
    message root {
      required int32 _1 (INTEGER(8,true));
      required int32 _2 (INTEGER(16,false));
      required float _3;
      required double _4;
      optional int32 _5 (DATE);
      optional int32 _6 (TIME(MILLIS,false));
      optional int64 _7 (TIME(MICROS,true));
      optional int64 _8 (TIMESTAMP(MILLIS,true));
      optional int64 _9 (TIMESTAMP(NANOS,false));
      optional binary _10 (STRING);
    }
    ";
        let mut iter = Tokenizer::from_str(schema);
        let message = Parser {
            tokenizer: &mut iter,
        }
        .parse_message_type()
        .unwrap();

        let mut fields = vec![
            Arc::new(
                Type::primitive_type_builder("_1", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    }))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_2", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::Integer {
                        bit_width: 16,
                        is_signed: false,
                    }))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_3", PhysicalType::FLOAT)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_4", PhysicalType::DOUBLE)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_5", PhysicalType::INT32)
                    .with_logical_type(Some(LogicalType::Date))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_6", PhysicalType::INT32)
                    .with_logical_type(Some(LogicalType::Time {
                        unit: TimeUnit::MILLIS(Default::default()),
                        is_adjusted_to_u_t_c: false,
                    }))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_7", PhysicalType::INT64)
                    .with_logical_type(Some(LogicalType::Time {
                        unit: TimeUnit::MICROS(Default::default()),
                        is_adjusted_to_u_t_c: true,
                    }))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_8", PhysicalType::INT64)
                    .with_logical_type(Some(LogicalType::Timestamp {
                        unit: TimeUnit::MILLIS(Default::default()),
                        is_adjusted_to_u_t_c: true,
                    }))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_9", PhysicalType::INT64)
                    .with_logical_type(Some(LogicalType::Timestamp {
                        unit: TimeUnit::NANOS(Default::default()),
                        is_adjusted_to_u_t_c: false,
                    }))
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("_10", PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::String))
                    .build()
                    .unwrap(),
            ),
        ];

        let expected = Type::group_type_builder("root")
            .with_fields(&mut fields)
            .build()
            .unwrap();
        assert_eq!(message, expected);
    }
}
