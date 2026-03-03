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

use std::{fmt::Display, iter::Peekable, str::Chars, sync::Arc};

use crate::{ArrowError, DataType, Field, Fields, IntervalUnit, TimeUnit, UnionFields, UnionMode};

/// Parses a DataType from a string representation
///
/// For example, the string "Int32" would be parsed into [`DataType::Int32`]
pub(crate) fn parse_data_type(val: &str) -> ArrowResult<DataType> {
    Parser::new(val).parse()
}

type ArrowResult<T> = Result<T, ArrowError>;

fn make_error(val: &str, msg: &str) -> ArrowError {
    let msg = format!(
        "Unsupported type '{val}'. Must be a supported arrow type name such as 'Int32' or 'Timestamp(ns)'. Error {msg}"
    );
    ArrowError::ParseError(msg)
}

fn make_error_expected(val: &str, expected: &Token, actual: &Token) -> ArrowError {
    make_error(val, &format!("Expected '{expected}', got '{actual}'"))
}

/// Implementation of `parse_data_type`, modeled after <https://github.com/sqlparser-rs/sqlparser-rs>
#[derive(Debug)]
struct Parser<'a> {
    val: &'a str,
    tokenizer: Peekable<Tokenizer<'a>>,
}

impl<'a> Parser<'a> {
    fn new(val: &'a str) -> Self {
        Self {
            val,
            tokenizer: Tokenizer::new(val).peekable(),
        }
    }

    fn parse(mut self) -> ArrowResult<DataType> {
        let data_type = self.parse_next_type()?;
        // ensure that there is no trailing content
        if self.tokenizer.next().is_some() {
            Err(make_error(
                self.val,
                &format!("checking trailing content after parsing '{data_type}'"),
            ))
        } else {
            Ok(data_type)
        }
    }

    /// parses the next full DataType
    fn parse_next_type(&mut self) -> ArrowResult<DataType> {
        match self.next_token()? {
            Token::SimpleType(data_type) => Ok(data_type),
            Token::Timestamp => self.parse_timestamp(),
            Token::Time32 => self.parse_time32(),
            Token::Time64 => self.parse_time64(),
            Token::Duration => self.parse_duration(),
            Token::Interval => self.parse_interval(),
            Token::FixedSizeBinary => self.parse_fixed_size_binary(),
            Token::Decimal32 => self.parse_decimal_32(),
            Token::Decimal64 => self.parse_decimal_64(),
            Token::Decimal128 => self.parse_decimal_128(),
            Token::Decimal256 => self.parse_decimal_256(),
            Token::Dictionary => self.parse_dictionary(),
            Token::List => self.parse_list(),
            Token::ListView => self.parse_list_view(),
            Token::LargeList => self.parse_large_list(),
            Token::LargeListView => self.parse_large_list_view(),
            Token::FixedSizeList => self.parse_fixed_size_list(),
            Token::Struct => self.parse_struct(),
            Token::Union => self.parse_union(),
            Token::Map => self.parse_map(),
            Token::RunEndEncoded => self.parse_run_end_encoded(),
            tok => Err(make_error(
                self.val,
                &format!("finding next type, got unexpected '{tok}'"),
            )),
        }
    }

    /// parses Field, this is the inversion of `format_field` in `datatype_display.rs`.
    /// E.g: "a": non-null Int64
    ///
    /// TODO: support metadata: `"a": non-null Int64 metadata: {"foo": "value"}`
    fn parse_field(&mut self) -> ArrowResult<Field> {
        let name = self.parse_double_quoted_string("Field")?;
        self.expect_token(Token::Colon)?;
        let nullable = self.parse_opt_nullable();
        let data_type = self.parse_next_type()?;
        Ok(Field::new(name, data_type, nullable))
    }

    /// Parses field inside a list. Use `Field::LIST_FIELD_DEFAULT_NAME`
    /// if no field name is specified.
    /// E.g: `non-null Int64, field: 'foo'` or `non-null Int64`
    ///
    /// TODO: support metadata: `non-ull Int64, metadata: {"foo2": "value"}`
    fn parse_list_field(&mut self, context: &str) -> ArrowResult<Field> {
        let nullable = self.parse_opt_nullable();
        let data_type = self.parse_next_type()?;

        // the field name (if exists) must be after a comma
        let field_name = if self
            .tokenizer
            .next_if(|next| matches!(next, Ok(Token::Comma)))
            .is_none()
        {
            Field::LIST_FIELD_DEFAULT_NAME.into()
        } else {
            // expects: `field: 'field_name'`.
            self.expect_token(Token::Field)?;
            self.expect_token(Token::Colon)?;
            self.parse_single_quoted_string(context)?
        };

        Ok(Field::new(field_name, data_type, nullable))
    }

    /// Parses the List type (called after `List` has been consumed)
    /// E.g: List(non-null Int64, field: 'foo')
    fn parse_list(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let field = self.parse_list_field("List")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::List(Arc::new(field)))
    }

    /// Parses the ListView type (called after `ListView` has been consumed)
    /// E.g: ListView(non-null Int64, field: 'foo')
    fn parse_list_view(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let field = self.parse_list_field("ListView")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::ListView(Arc::new(field)))
    }

    /// Parses the LargeList type (called after `LargeList` has been consumed)
    /// E.g: LargeList(non-null Int64, field: 'foo')
    fn parse_large_list(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let field = self.parse_list_field("LargeList")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::LargeList(Arc::new(field)))
    }

    /// Parses the LargeListView type (called after `LargeListView` has been consumed)
    /// E.g: LargeListView(non-null Int64, field: 'foo')
    fn parse_large_list_view(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let field = self.parse_list_field("LargeListView")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::LargeListView(Arc::new(field)))
    }

    /// Parses the FixedSizeList type (called after `FixedSizeList` has been consumed)
    ///
    /// Examples:
    /// * `FixedSizeList(5 x non-null Int64, field: 'foo')`
    /// * `FixedSizeList(4, Int64)`
    ///
    fn parse_fixed_size_list(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let length = self.parse_i32("FixedSizeList")?;
        match self.next_token()? {
            // `FixedSizeList(5 x non-null Int64, field: 'foo')` format
            Token::X => {
                let field = self.parse_list_field("FixedSizeList")?;
                self.expect_token(Token::RParen)?;
                Ok(DataType::FixedSizeList(Arc::new(field), length))
            }
            // `FixedSizeList(4, Int64)` format
            Token::Comma => {
                let data_type = self.parse_next_type()?;
                self.expect_token(Token::RParen)?;
                Ok(DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(data_type, true)),
                    length,
                ))
            }
            tok => Err(make_error(
                self.val,
                &format!("Expected 'x' or ',' after length for FixedSizeList, got '{tok}'"),
            )),
        }
    }

    /// Parses the next timeunit
    fn parse_time_unit(&mut self, context: &str) -> ArrowResult<TimeUnit> {
        match self.next_token()? {
            Token::TimeUnit(time_unit) => Ok(time_unit),
            tok => Err(make_error(
                self.val,
                &format!("finding TimeUnit for {context}, got {tok}"),
            )),
        }
    }

    /// Parses the next double quoted string
    fn parse_double_quoted_string(&mut self, context: &str) -> ArrowResult<String> {
        let token = self.next_token()?;
        if let Token::DoubleQuotedString(string) = token {
            Ok(string)
        } else {
            Err(make_error(
                self.val,
                &format!("expected double quoted string for {context}, got '{token}'"),
            ))
        }
    }

    /// Parses the next single quoted string
    fn parse_single_quoted_string(&mut self, context: &str) -> ArrowResult<String> {
        let token = self.next_token()?;
        if let Token::SingleQuotedString(string) = token {
            Ok(string)
        } else {
            Err(make_error(
                self.val,
                &format!("expected single quoted string for {context}, got '{token}'"),
            ))
        }
    }

    /// Parses the next integer value
    fn parse_i64(&mut self, context: &str) -> ArrowResult<i64> {
        match self.next_token()? {
            Token::Integer(v) => Ok(v),
            tok => Err(make_error(
                self.val,
                &format!("finding i64 for {context}, got '{tok}'"),
            )),
        }
    }

    /// Parses the next i32 integer value
    fn parse_i32(&mut self, context: &str) -> ArrowResult<i32> {
        let length = self.parse_i64(context)?;
        length.try_into().map_err(|e| {
            make_error(
                self.val,
                &format!("converting {length} into i32 for {context}: {e}"),
            )
        })
    }

    /// Parses the next i8 integer value
    fn parse_i8(&mut self, context: &str) -> ArrowResult<i8> {
        let length = self.parse_i64(context)?;
        length.try_into().map_err(|e| {
            make_error(
                self.val,
                &format!("converting {length} into i8 for {context}: {e}"),
            )
        })
    }

    /// Parses the next u8 integer value
    fn parse_u8(&mut self, context: &str) -> ArrowResult<u8> {
        let length = self.parse_i64(context)?;
        length.try_into().map_err(|e| {
            make_error(
                self.val,
                &format!("converting {length} into u8 for {context}: {e}"),
            )
        })
    }

    /// Parses the next timestamp (called after `Timestamp` has been consumed)
    fn parse_timestamp(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Timestamp")?;

        let timezone;
        match self.next_token()? {
            Token::Comma => {
                match self.next_token()? {
                    // Support old style `Timestamp(Nanosecond, None)`
                    Token::None => {
                        timezone = None;
                    }
                    // Support old style `Timestamp(Nanosecond, Some("Timezone"))`
                    Token::Some => {
                        self.expect_token(Token::LParen)?;
                        timezone = Some(self.parse_double_quoted_string("Timezone")?);
                        self.expect_token(Token::RParen)?;
                    }
                    Token::DoubleQuotedString(tz) => {
                        // Support new style `Timestamp(Nanosecond, "Timezone")`
                        timezone = Some(tz);
                    }
                    tok => {
                        return Err(make_error(
                            self.val,
                            &format!("Expected None, Some, or a timezone string, got {tok:?}"),
                        ));
                    }
                };
                self.expect_token(Token::RParen)?;
            }
            // No timezone (e.g `Timestamp(ns)`)
            Token::RParen => {
                timezone = None;
            }
            next_token => {
                return Err(make_error(
                    self.val,
                    &format!("Expected comma followed by a timezone, or an ), got {next_token:?}"),
                ));
            }
        }
        Ok(DataType::Timestamp(time_unit, timezone.map(Into::into)))
    }

    /// Parses the next Time32 (called after `Time32` has been consumed)
    fn parse_time32(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Time32")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Time32(time_unit))
    }

    /// Parses the next Time64 (called after `Time64` has been consumed)
    fn parse_time64(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Time64")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Time64(time_unit))
    }

    /// Parses the next Duration (called after `Duration` has been consumed)
    fn parse_duration(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Duration")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Duration(time_unit))
    }

    /// Parses the next Interval (called after `Interval` has been consumed)
    fn parse_interval(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let interval_unit = match self.next_token()? {
            Token::IntervalUnit(interval_unit) => interval_unit,
            tok => {
                return Err(make_error(
                    self.val,
                    &format!("finding IntervalUnit for Interval, got {tok}"),
                ));
            }
        };
        self.expect_token(Token::RParen)?;
        Ok(DataType::Interval(interval_unit))
    }

    /// Parses the next FixedSizeBinary (called after `FixedSizeBinary` has been consumed)
    fn parse_fixed_size_binary(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let length = self.parse_i32("FixedSizeBinary")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::FixedSizeBinary(length))
    }

    /// Parses the next Decimal32 (called after `Decimal32` has been consumed)
    fn parse_decimal_32(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let precision = self.parse_u8("Decimal32")?;
        self.expect_token(Token::Comma)?;
        let scale = self.parse_i8("Decimal32")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Decimal32(precision, scale))
    }

    /// Parses the next Decimal64 (called after `Decimal64` has been consumed)
    fn parse_decimal_64(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let precision = self.parse_u8("Decimal64")?;
        self.expect_token(Token::Comma)?;
        let scale = self.parse_i8("Decimal64")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Decimal64(precision, scale))
    }

    /// Parses the next Decimal128 (called after `Decimal128` has been consumed)
    fn parse_decimal_128(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let precision = self.parse_u8("Decimal128")?;
        self.expect_token(Token::Comma)?;
        let scale = self.parse_i8("Decimal128")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Decimal128(precision, scale))
    }

    /// Parses the next Decimal256 (called after `Decimal256` has been consumed)
    fn parse_decimal_256(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let precision = self.parse_u8("Decimal256")?;
        self.expect_token(Token::Comma)?;
        let scale = self.parse_i8("Decimal256")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Decimal256(precision, scale))
    }

    /// Parses the next Dictionary (called after `Dictionary` has been consumed)
    fn parse_dictionary(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let key_type = self.parse_next_type()?;
        self.expect_token(Token::Comma)?;
        let value_type = self.parse_next_type()?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Dictionary(
            Box::new(key_type),
            Box::new(value_type),
        ))
    }

    /// Parses the next Struct (called after `Struct` has been consumed)
    fn parse_struct(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let mut fields = Vec::new();
        loop {
            if self
                .tokenizer
                .next_if(|next| matches!(next, Ok(Token::RParen)))
                .is_some()
            {
                break;
            }

            let field = self.parse_field()?;
            fields.push(Arc::new(field));
            match self.next_token()? {
                Token::Comma => continue,
                Token::RParen => break,
                tok => {
                    return Err(make_error(
                        self.val,
                        &format!(
                            "Unexpected token while parsing Struct fields. Expected ',' or ')', but got '{tok}'"
                        ),
                    ));
                }
            }
        }
        Ok(DataType::Struct(Fields::from(fields)))
    }

    /// Parses the next Union (called after `Union` has been consumed)
    /// E.g: Union(Sparse, 0: ("a": Int32), 1: ("b": non-null Utf8))
    fn parse_union(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let union_mode = self.parse_union_mode()?;
        let mut type_ids = vec![];
        let mut fields = vec![];
        loop {
            if self
                .tokenizer
                .next_if(|next| matches!(next, Ok(Token::RParen)))
                .is_some()
            {
                break;
            }
            self.expect_token(Token::Comma)?;
            let (type_id, field) = self.parse_union_field()?;
            type_ids.push(type_id);
            fields.push(field);
        }
        Ok(DataType::Union(
            UnionFields::try_new(type_ids, fields)?,
            union_mode,
        ))
    }

    /// Parses the next UnionMode
    fn parse_union_mode(&mut self) -> ArrowResult<UnionMode> {
        match self.next_token()? {
            Token::UnionMode(union_mode) => Ok(union_mode),
            tok => Err(make_error(
                self.val,
                &format!("finding UnionMode for Union, got {tok}"),
            )),
        }
    }

    /// Parses the next UnionField
    /// 0: ("a": non-null Int32)
    fn parse_union_field(&mut self) -> ArrowResult<(i8, Field)> {
        let type_id = self.parse_i8("UnionField")?;
        self.expect_token(Token::Colon)?;
        self.expect_token(Token::LParen)?;
        let field = self.parse_field()?;
        self.expect_token(Token::RParen)?;
        Ok((type_id, field))
    }

    /// Parses the next Map (called after `Map` has been consumed)
    /// E.g: Map("entries": Struct("key": Utf8, "value": non-null Int32), sorted)
    fn parse_map(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let field = self.parse_field()?;
        self.expect_token(Token::Comma)?;
        let sorted = self.parse_map_sorted()?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Map(Arc::new(field), sorted))
    }

    /// Parses map's sorted
    fn parse_map_sorted(&mut self) -> ArrowResult<bool> {
        match self.next_token()? {
            Token::MapSorted(sorted) => Ok(sorted),
            tok => Err(make_error(
                self.val,
                &format!("Expected sorted or unsorted for a map; got {tok:?}"),
            )),
        }
    }

    /// Parses the next RunEndEncoded (called after `RunEndEncoded` has been consumed)
    /// E.g: RunEndEncoded("run_ends": UInt32, "values": nonnull Int32)
    fn parse_run_end_encoded(&mut self) -> ArrowResult<DataType> {
        self.expect_token(Token::LParen)?;
        let run_ends = self.parse_field()?;
        self.expect_token(Token::Comma)?;
        let values = self.parse_field()?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::RunEndEncoded(
            Arc::new(run_ends),
            Arc::new(values),
        ))
    }

    /// consume the next token and return `false` if the field is `nonnull`.
    fn parse_opt_nullable(&mut self) -> bool {
        let tok = self
            .tokenizer
            .next_if(|next| matches!(next, Ok(Token::NonNull | Token::Nullable)));
        !matches!(tok, Some(Ok(Token::NonNull)))
    }

    /// return the next token, or an error if there are none left
    fn next_token(&mut self) -> ArrowResult<Token> {
        match self.tokenizer.next() {
            None => Err(make_error(self.val, "finding next token")),
            Some(token) => token,
        }
    }

    /// consume the next token, returning OK(()) if it matches tok, and Err if not
    fn expect_token(&mut self, tok: Token) -> ArrowResult<()> {
        let next_token = self.next_token()?;
        if next_token == tok {
            Ok(())
        } else {
            Err(make_error_expected(self.val, &tok, &next_token))
        }
    }
}

/// returns true if this character is a separator
fn is_separator(c: char) -> bool {
    c == '(' || c == ')' || c == ',' || c == ':' || c == ' '
}

enum QuoteType {
    Double,
    Single,
}

#[derive(Debug)]
/// Splits a strings like Dictionary(Int32, Int64) into tokens suitable for parsing
///
/// For example the string "Timestamp(ns)" would be parsed into:
///
/// * Token::Timestamp
/// * Token::Lparen
/// * Token::IntervalUnit(IntervalUnit::Nanosecond)
/// * Token::Rparen,
struct Tokenizer<'a> {
    val: &'a str,
    chars: Peekable<Chars<'a>>,
    // temporary buffer for parsing words
    word: String,
}

impl<'a> Tokenizer<'a> {
    fn new(val: &'a str) -> Self {
        Self {
            val,
            chars: val.chars().peekable(),
            word: String::new(),
        }
    }

    /// returns the next char, without consuming it
    fn peek_next_char(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }

    /// returns the next char, and consuming it
    fn next_char(&mut self) -> Option<char> {
        self.chars.next()
    }

    /// parse the characters in val starting at pos, until the next
    /// `,`, `(`, or `)` or end of line
    fn parse_word(&mut self) -> ArrowResult<Token> {
        // reset temp space
        self.word.clear();
        loop {
            match self.peek_next_char() {
                None => break,
                Some(c) if is_separator(c) => break,
                Some(c) => {
                    self.next_char();
                    self.word.push(c);
                }
            }
        }

        if let Some(c) = self.word.chars().next() {
            // if it started with a number, try parsing it as an integer
            if c == '-' || c.is_numeric() {
                let val: i64 = self.word.parse().map_err(|e| {
                    make_error(self.val, &format!("parsing {} as integer: {e}", self.word))
                })?;
                return Ok(Token::Integer(val));
            }
        }

        // figure out what the word was
        let token = match self.word.as_str() {
            "Null" => Token::SimpleType(DataType::Null),
            "Boolean" => Token::SimpleType(DataType::Boolean),

            "Int8" => Token::SimpleType(DataType::Int8),
            "Int16" => Token::SimpleType(DataType::Int16),
            "Int32" => Token::SimpleType(DataType::Int32),
            "Int64" => Token::SimpleType(DataType::Int64),

            "UInt8" => Token::SimpleType(DataType::UInt8),
            "UInt16" => Token::SimpleType(DataType::UInt16),
            "UInt32" => Token::SimpleType(DataType::UInt32),
            "UInt64" => Token::SimpleType(DataType::UInt64),

            "Utf8" => Token::SimpleType(DataType::Utf8),
            "LargeUtf8" => Token::SimpleType(DataType::LargeUtf8),
            "Utf8View" => Token::SimpleType(DataType::Utf8View),
            "Binary" => Token::SimpleType(DataType::Binary),
            "BinaryView" => Token::SimpleType(DataType::BinaryView),
            "LargeBinary" => Token::SimpleType(DataType::LargeBinary),

            "Float16" => Token::SimpleType(DataType::Float16),
            "Float32" => Token::SimpleType(DataType::Float32),
            "Float64" => Token::SimpleType(DataType::Float64),

            "Date32" => Token::SimpleType(DataType::Date32),
            "Date64" => Token::SimpleType(DataType::Date64),

            "List" => Token::List,
            "ListView" => Token::ListView,
            "LargeList" => Token::LargeList,
            "LargeListView" => Token::LargeListView,
            "FixedSizeList" => Token::FixedSizeList,

            "s" | "Second" => Token::TimeUnit(TimeUnit::Second),
            "ms" | "Millisecond" => Token::TimeUnit(TimeUnit::Millisecond),
            "µs" | "us" | "Microsecond" => Token::TimeUnit(TimeUnit::Microsecond),
            "ns" | "Nanosecond" => Token::TimeUnit(TimeUnit::Nanosecond),

            "Timestamp" => Token::Timestamp,
            "Time32" => Token::Time32,
            "Time64" => Token::Time64,
            "Duration" => Token::Duration,
            "Interval" => Token::Interval,
            "Dictionary" => Token::Dictionary,

            "FixedSizeBinary" => Token::FixedSizeBinary,

            "Decimal32" => Token::Decimal32,
            "Decimal64" => Token::Decimal64,
            "Decimal128" => Token::Decimal128,
            "Decimal256" => Token::Decimal256,

            "YearMonth" => Token::IntervalUnit(IntervalUnit::YearMonth),
            "DayTime" => Token::IntervalUnit(IntervalUnit::DayTime),
            "MonthDayNano" => Token::IntervalUnit(IntervalUnit::MonthDayNano),

            "Some" => Token::Some,
            "None" => Token::None,

            "non-null" => Token::NonNull,
            "nullable" => Token::Nullable,
            "field" => Token::Field,
            "x" => Token::X,

            "Struct" => Token::Struct,

            "Union" => Token::Union,
            "Sparse" => Token::UnionMode(UnionMode::Sparse),
            "Dense" => Token::UnionMode(UnionMode::Dense),

            "Map" => Token::Map,
            "sorted" => Token::MapSorted(true),
            "unsorted" => Token::MapSorted(false),

            "RunEndEncoded" => Token::RunEndEncoded,

            token => {
                return Err(make_error(self.val, &format!("unknown token: {token}")));
            }
        };
        Ok(token)
    }

    /// Parses e.g. `"foo bar"`, `'foo bar'`
    fn parse_quoted_string(&mut self, quote_type: QuoteType) -> ArrowResult<Token> {
        let quote = match quote_type {
            QuoteType::Double => '\"',
            QuoteType::Single => '\'',
        };

        if self.next_char() != Some(quote) {
            return Err(make_error(self.val, "Expected \""));
        }

        // reset temp space
        self.word.clear();

        let mut is_escaped = false;

        loop {
            match self.next_char() {
                None => {
                    return Err(ArrowError::ParseError(format!(
                        "Unterminated string at: \"{}",
                        self.word
                    )));
                }
                Some(c) => match c {
                    '\\' => {
                        is_escaped = true;
                        self.word.push(c);
                    }
                    c if c == quote => {
                        if is_escaped {
                            self.word.push(c);
                            is_escaped = false;
                        } else {
                            break;
                        }
                    }
                    c => {
                        self.word.push(c);
                    }
                },
            }
        }

        let val: String = self.word.parse().map_err(|err| {
            ArrowError::ParseError(format!("Failed to parse string: \"{}\": {err}", self.word))
        })?;

        if val.is_empty() {
            // Using empty strings as field names is just asking for trouble
            return Err(make_error(self.val, "empty strings aren't allowed"));
        }

        match quote_type {
            QuoteType::Double => Ok(Token::DoubleQuotedString(val)),
            QuoteType::Single => Ok(Token::SingleQuotedString(val)),
        }
    }
}

impl Iterator for Tokenizer<'_> {
    type Item = ArrowResult<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.peek_next_char()? {
                ' ' => {
                    // skip whitespace
                    self.next_char();
                    continue;
                }
                '"' => {
                    return Some(self.parse_quoted_string(QuoteType::Double));
                }
                '\'' => {
                    return Some(self.parse_quoted_string(QuoteType::Single));
                }
                '(' => {
                    self.next_char();
                    return Some(Ok(Token::LParen));
                }
                ')' => {
                    self.next_char();
                    return Some(Ok(Token::RParen));
                }
                ',' => {
                    self.next_char();
                    return Some(Ok(Token::Comma));
                }
                ':' => {
                    self.next_char();
                    return Some(Ok(Token::Colon));
                }
                _ => return Some(self.parse_word()),
            }
        }
    }
}

/// Grammar is
///
#[derive(Debug, PartialEq)]
enum Token {
    // Null, or Int32
    SimpleType(DataType),
    Timestamp,
    Time32,
    Time64,
    Duration,
    Interval,
    FixedSizeBinary,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Dictionary,
    TimeUnit(TimeUnit),
    IntervalUnit(IntervalUnit),
    LParen,
    RParen,
    Comma,
    Colon,
    Some,
    None,
    Integer(i64),
    DoubleQuotedString(String),
    SingleQuotedString(String),
    List,
    ListView,
    LargeList,
    LargeListView,
    FixedSizeList,
    Struct,
    Union,
    UnionMode(UnionMode),
    Map,
    MapSorted(bool),
    RunEndEncoded,
    NonNull,
    Nullable,
    Field,
    X,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::SimpleType(t) => write!(f, "{t}"),
            Token::List => write!(f, "List"),
            Token::ListView => write!(f, "ListView"),
            Token::LargeList => write!(f, "LargeList"),
            Token::LargeListView => write!(f, "LargeListView"),
            Token::FixedSizeList => write!(f, "FixedSizeList"),
            Token::Timestamp => write!(f, "Timestamp"),
            Token::Time32 => write!(f, "Time32"),
            Token::Time64 => write!(f, "Time64"),
            Token::Duration => write!(f, "Duration"),
            Token::Interval => write!(f, "Interval"),
            Token::TimeUnit(u) => write!(f, "TimeUnit({u:?})"),
            Token::IntervalUnit(u) => write!(f, "IntervalUnit({u:?})"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Some => write!(f, "Some"),
            Token::None => write!(f, "None"),
            Token::FixedSizeBinary => write!(f, "FixedSizeBinary"),
            Token::Decimal32 => write!(f, "Decimal32"),
            Token::Decimal64 => write!(f, "Decimal64"),
            Token::Decimal128 => write!(f, "Decimal128"),
            Token::Decimal256 => write!(f, "Decimal256"),
            Token::Dictionary => write!(f, "Dictionary"),
            Token::Integer(v) => write!(f, "Integer({v})"),
            Token::DoubleQuotedString(s) => write!(f, "DoubleQuotedString({s})"),
            Token::SingleQuotedString(s) => write!(f, "SingleQuotedString({s})"),
            Token::Struct => write!(f, "Struct"),
            Token::Union => write!(f, "Union"),
            Token::UnionMode(m) => write!(f, "{m:?}"),
            Token::Map => write!(f, "Map"),
            Token::MapSorted(sorted) => {
                write!(f, "{}", if *sorted { "sorted" } else { "unsorted" })
            }
            Token::RunEndEncoded => write!(f, "RunEndEncoded"),
            Token::NonNull => write!(f, "non-null"),
            Token::Nullable => write!(f, "nullable"),
            Token::Field => write!(f, "field"),
            Token::X => write!(f, "x"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_data_type() {
        // this ensures types can be parsed correctly from their string representations
        for dt in list_datatypes() {
            round_trip(dt)
        }
    }

    /// Ensure we converting data_type to a string, and then parse it as a type
    /// verifying it is the same
    fn round_trip(data_type: DataType) {
        let data_type_string = data_type.to_string();
        println!("Input '{data_type_string}' ({data_type:?})");
        let parsed_type = parse_data_type(&data_type_string).unwrap();
        assert_eq!(
            data_type, parsed_type,
            "Mismatch parsing {data_type_string}"
        );
    }

    fn list_datatypes() -> Vec<DataType> {
        vec![
            // ---------
            // Non Nested types
            // ---------
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            // we can't cover all possible timezones, here we only test utc and +08:00
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+08:00".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+08:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("+08:00".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+08:00".into())),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Decimal32(7, 8),
            DataType::Decimal64(6, 9),
            DataType::Decimal128(7, 12),
            DataType::Decimal256(6, 13),
            // ---------
            // Nested types
            // ---------
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::FixedSizeBinary(23)),
            ),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(
                    // nested dictionaries are probably a bad idea but they are possible
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                ),
            ),
            DataType::Struct(Fields::from(vec![
                Field::new("f1", DataType::Int64, true),
                Field::new("f2", DataType::Float64, true),
                Field::new(
                    "f3",
                    DataType::Timestamp(TimeUnit::Second, Some("+08:00".into())),
                    true,
                ),
                Field::new(
                    "f4",
                    DataType::Dictionary(
                        Box::new(DataType::Int8),
                        Box::new(DataType::FixedSizeBinary(23)),
                    ),
                    true,
                ),
            ])),
            DataType::Struct(Fields::from(vec![
                Field::new("Int64", DataType::Int64, true),
                Field::new("Float64", DataType::Float64, true),
            ])),
            DataType::Struct(Fields::from(vec![
                Field::new("f1", DataType::Int64, true),
                Field::new(
                    "nested_struct",
                    DataType::Struct(Fields::from(vec![Field::new("n1", DataType::Int64, true)])),
                    true,
                ),
            ])),
            DataType::Struct(Fields::from(vec![Field::new("f1", DataType::Int64, true)])),
            DataType::Struct(Fields::empty()),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false))),
            DataType::List(Arc::new(Field::new("Int64", DataType::Int64, true))),
            DataType::List(Arc::new(Field::new("Int64", DataType::Int64, false))),
            DataType::List(Arc::new(Field::new(
                "nested_list",
                DataType::List(Arc::new(Field::new("Int64", DataType::Int64, true))),
                true,
            ))),
            DataType::ListView(Arc::new(Field::new_list_field(DataType::Int64, true))),
            DataType::ListView(Arc::new(Field::new_list_field(DataType::Int64, false))),
            DataType::ListView(Arc::new(Field::new("Int64", DataType::Int64, true))),
            DataType::ListView(Arc::new(Field::new("Int64", DataType::Int64, false))),
            DataType::ListView(Arc::new(Field::new(
                "nested_list_view",
                DataType::ListView(Arc::new(Field::new("Int64", DataType::Int64, true))),
                true,
            ))),
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, false))),
            DataType::LargeList(Arc::new(Field::new("Int64", DataType::Int64, true))),
            DataType::LargeList(Arc::new(Field::new("Int64", DataType::Int64, false))),
            DataType::LargeList(Arc::new(Field::new(
                "nested_large_list",
                DataType::LargeList(Arc::new(Field::new("Int64", DataType::Int64, true))),
                true,
            ))),
            DataType::LargeListView(Arc::new(Field::new_list_field(DataType::Int64, true))),
            DataType::LargeListView(Arc::new(Field::new_list_field(DataType::Int64, false))),
            DataType::LargeListView(Arc::new(Field::new("Int64", DataType::Int64, true))),
            DataType::LargeListView(Arc::new(Field::new("Int64", DataType::Int64, false))),
            DataType::LargeListView(Arc::new(Field::new(
                "nested_large_list_view",
                DataType::LargeListView(Arc::new(Field::new("Int64", DataType::Int64, true))),
                true,
            ))),
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int64, true)), 2),
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int64, false)), 2),
            DataType::FixedSizeList(Arc::new(Field::new("Int64", DataType::Int64, true)), 2),
            DataType::FixedSizeList(Arc::new(Field::new("Int64", DataType::Int64, false)), 2),
            DataType::FixedSizeList(
                Arc::new(Field::new(
                    "nested_fixed_size_list",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("Int64", DataType::Int64, true)),
                        2,
                    ),
                    true,
                )),
                2,
            ),
            DataType::Union(
                UnionFields::from_fields(vec![
                    Field::new("Int32", DataType::Int32, false),
                    Field::new("Utf8", DataType::Utf8, true),
                ]),
                UnionMode::Sparse,
            ),
            DataType::Union(
                UnionFields::from_fields(vec![
                    Field::new("Int32", DataType::Int32, false),
                    Field::new("Utf8", DataType::Utf8, true),
                ]),
                UnionMode::Dense,
            ),
            DataType::Union(
                UnionFields::from_fields(vec![
                    Field::new_union(
                        "nested_union",
                        vec![0, 1],
                        vec![
                            Field::new("Int32", DataType::Int32, false),
                            Field::new("Utf8", DataType::Utf8, true),
                        ],
                        UnionMode::Dense,
                    ),
                    Field::new("Utf8", DataType::Utf8, true),
                ]),
                UnionMode::Sparse,
            ),
            DataType::Union(
                UnionFields::from_fields(vec![Field::new("Int32", DataType::Int32, false)]),
                UnionMode::Dense,
            ),
            DataType::Union(
                UnionFields::try_new(Vec::<i8>::new(), Vec::<Field>::new()).unwrap(),
                UnionMode::Sparse,
            ),
            DataType::Map(Arc::new(Field::new("Int64", DataType::Int64, true)), true),
            DataType::Map(Arc::new(Field::new("Int64", DataType::Int64, true)), false),
            DataType::Map(
                Arc::new(Field::new_map(
                    "nested_map",
                    "entries",
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                    false,
                    true,
                )),
                true,
            ),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::UInt32, false)),
                Arc::new(Field::new("values", DataType::Int32, true)),
            ),
            DataType::RunEndEncoded(
                Arc::new(Field::new(
                    "nested_run_end_encoded",
                    DataType::RunEndEncoded(
                        Arc::new(Field::new("run_ends", DataType::UInt32, false)),
                        Arc::new(Field::new("values", DataType::Int32, true)),
                    ),
                    true,
                )),
                Arc::new(Field::new("values", DataType::Int32, true)),
            ),
        ]
    }

    #[test]
    fn test_parse_data_type_whitespace_tolerance() {
        // (string to parse, expected DataType)
        let cases = [
            ("Int8", DataType::Int8),
            (
                "Timestamp        (ns)",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "Timestamp        (ns)  ",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "          Timestamp        (ns               )",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "Timestamp        (ns               )  ",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
        ];

        for (data_type_string, expected_data_type) in cases {
            let parsed_data_type = parse_data_type(data_type_string).unwrap();
            assert_eq!(
                parsed_data_type, expected_data_type,
                "Parsing '{data_type_string}', expecting '{expected_data_type}'"
            );
        }
    }

    /// Ensure that old style types can still be parsed
    #[test]
    fn test_parse_data_type_backwards_compatibility() {
        use DataType::*;
        use IntervalUnit::*;
        use TimeUnit::*;
        // List below created with:
        for t in list_datatypes() {
            println!(r#"("{t}", {t:?}),"#);
        }
        // (string to parse, expected DataType)
        let cases = [
            ("Timestamp(Nanosecond, None)", Timestamp(Nanosecond, None)),
            ("Timestamp(Microsecond, None)", Timestamp(Microsecond, None)),
            ("Timestamp(Millisecond, None)", Timestamp(Millisecond, None)),
            ("Timestamp(Second, None)", Timestamp(Second, None)),
            ("Timestamp(Nanosecond, None)", Timestamp(Nanosecond, None)),
            // Timezones
            (
                r#"Timestamp(Nanosecond, Some("+00:00"))"#,
                Timestamp(Nanosecond, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(Microsecond, Some("+00:00"))"#,
                Timestamp(Microsecond, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(Millisecond, Some("+00:00"))"#,
                Timestamp(Millisecond, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(Second, Some("+00:00"))"#,
                Timestamp(Second, Some("+00:00".into())),
            ),
            ("Null", Null),
            ("Boolean", Boolean),
            ("Int8", Int8),
            ("Int16", Int16),
            ("Int32", Int32),
            ("Int64", Int64),
            ("UInt8", UInt8),
            ("UInt16", UInt16),
            ("UInt32", UInt32),
            ("UInt64", UInt64),
            ("Float16", Float16),
            ("Float32", Float32),
            ("Float64", Float64),
            ("Timestamp(s)", Timestamp(Second, None)),
            ("Timestamp(ms)", Timestamp(Millisecond, None)),
            ("Timestamp(µs)", Timestamp(Microsecond, None)),
            ("Timestamp(ns)", Timestamp(Nanosecond, None)),
            (
                r#"Timestamp(ns, "+00:00")"#,
                Timestamp(Nanosecond, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(µs, "+00:00")"#,
                Timestamp(Microsecond, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(ms, "+00:00")"#,
                Timestamp(Millisecond, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(s, "+00:00")"#,
                Timestamp(Second, Some("+00:00".into())),
            ),
            (
                r#"Timestamp(ns, "+08:00")"#,
                Timestamp(Nanosecond, Some("+08:00".into())),
            ),
            (
                r#"Timestamp(µs, "+08:00")"#,
                Timestamp(Microsecond, Some("+08:00".into())),
            ),
            (
                r#"Timestamp(ms, "+08:00")"#,
                Timestamp(Millisecond, Some("+08:00".into())),
            ),
            (
                r#"Timestamp(s, "+08:00")"#,
                Timestamp(Second, Some("+08:00".into())),
            ),
            ("Date32", Date32),
            ("Date64", Date64),
            ("Time32(s)", Time32(Second)),
            ("Time32(ms)", Time32(Millisecond)),
            ("Time32(µs)", Time32(Microsecond)),
            ("Time32(ns)", Time32(Nanosecond)),
            ("Time64(s)", Time64(Second)),
            ("Time64(ms)", Time64(Millisecond)),
            ("Time64(µs)", Time64(Microsecond)),
            ("Time64(ns)", Time64(Nanosecond)),
            ("Duration(s)", Duration(Second)),
            ("Duration(ms)", Duration(Millisecond)),
            ("Duration(µs)", Duration(Microsecond)),
            ("Duration(ns)", Duration(Nanosecond)),
            ("Interval(YearMonth)", Interval(YearMonth)),
            ("Interval(DayTime)", Interval(DayTime)),
            ("Interval(MonthDayNano)", Interval(MonthDayNano)),
            ("Binary", Binary),
            ("BinaryView", BinaryView),
            ("FixedSizeBinary(0)", FixedSizeBinary(0)),
            ("FixedSizeBinary(1234)", FixedSizeBinary(1234)),
            ("FixedSizeBinary(-432)", FixedSizeBinary(-432)),
            ("LargeBinary", LargeBinary),
            ("Utf8", Utf8),
            ("Utf8View", Utf8View),
            ("LargeUtf8", LargeUtf8),
            ("Decimal32(7, 8)", Decimal32(7, 8)),
            ("Decimal64(6, 9)", Decimal64(6, 9)),
            ("Decimal128(7, 12)", Decimal128(7, 12)),
            ("Decimal256(6, 13)", Decimal256(6, 13)),
            (
                "Dictionary(Int32, Utf8)",
                Dictionary(Box::new(Int32), Box::new(Utf8)),
            ),
            (
                "Dictionary(Int8, Utf8)",
                Dictionary(Box::new(Int8), Box::new(Utf8)),
            ),
            (
                "Dictionary(Int8, Timestamp(ns))",
                Dictionary(Box::new(Int8), Box::new(Timestamp(Nanosecond, None))),
            ),
            (
                "Dictionary(Int8, FixedSizeBinary(23))",
                Dictionary(Box::new(Int8), Box::new(FixedSizeBinary(23))),
            ),
            (
                "Dictionary(Int8, Dictionary(Int8, Utf8))",
                Dictionary(
                    Box::new(Int8),
                    Box::new(Dictionary(Box::new(Int8), Box::new(Utf8))),
                ),
            ),
            (
                r#"Struct("f1": nullable Int64, "f2": nullable Float64, "f3": nullable Timestamp(s, "+08:00"), "f4": nullable Dictionary(Int8, FixedSizeBinary(23)))"#,
                Struct(Fields::from(vec![
                    Field::new("f1", Int64, true),
                    Field::new("f2", Float64, true),
                    Field::new("f3", Timestamp(Second, Some("+08:00".into())), true),
                    Field::new(
                        "f4",
                        Dictionary(Box::new(Int8), Box::new(FixedSizeBinary(23))),
                        true,
                    ),
                ])),
            ),
            (
                r#"Struct("Int64": nullable Int64, "Float64": nullable Float64)"#,
                Struct(Fields::from(vec![
                    Field::new("Int64", Int64, true),
                    Field::new("Float64", Float64, true),
                ])),
            ),
            (
                r#"Struct("f1": nullable Int64, "nested_struct": nullable Struct("n1": nullable Int64))"#,
                Struct(Fields::from(vec![
                    Field::new("f1", Int64, true),
                    Field::new(
                        "nested_struct",
                        Struct(Fields::from(vec![Field::new("n1", Int64, true)])),
                        true,
                    ),
                ])),
            ),
            (r#"Struct()"#, Struct(Fields::empty())),
            (
                "FixedSizeList(4, Int64)",
                FixedSizeList(Arc::new(Field::new_list_field(Int64, true)), 4),
            ),
            (
                "List(Int64)",
                List(Arc::new(Field::new_list_field(Int64, true))),
            ),
            (
                "LargeList(Int64)",
                LargeList(Arc::new(Field::new_list_field(Int64, true))),
            ),
        ];

        for (data_type_string, expected_data_type) in cases {
            let parsed_data_type = parse_data_type(data_type_string).unwrap();
            assert_eq!(
                parsed_data_type, expected_data_type,
                "Parsing '{data_type_string}', expecting '{expected_data_type}'"
            );
        }
    }

    #[test]
    fn parse_data_type_errors() {
        // (string to parse, expected error message)
        let cases = [
            ("", "Unsupported type ''"),
            ("", "Error finding next token"),
            ("null", "Unsupported type 'null'"),
            ("Nu", "Unsupported type 'Nu'"),
            (r#"Timestamp(ns, +00:00)"#, "Error unknown token: +00"),
            (
                r#"Timestamp(ns, "+00:00)"#,
                r#"Unterminated string at: "+00:00)"#,
            ),
            (r#"Timestamp(ns, "")"#, r#"empty strings aren't allowed"#),
            (
                r#"Timestamp(ns, "+00:00"")"#,
                r#"Parser error: Unterminated string at: ")"#,
            ),
            ("Timestamp(ns, ", "Error finding next token"),
            (
                "Float32 Float32",
                "trailing content after parsing 'Float32'",
            ),
            ("Int32, ", "trailing content after parsing 'Int32'"),
            ("Int32(3), ", "trailing content after parsing 'Int32'"),
            (
                "FixedSizeBinary(Int32), ",
                "Error finding i64 for FixedSizeBinary, got 'Int32'",
            ),
            (
                "FixedSizeBinary(3.0), ",
                "Error parsing 3.0 as integer: invalid digit found in string",
            ),
            // too large for i32
            (
                "FixedSizeBinary(4000000000), ",
                "Error converting 4000000000 into i32 for FixedSizeBinary: out of range integral type conversion attempted",
            ),
            // can't have negative precision
            (
                "Decimal32(-3, 5)",
                "Error converting -3 into u8 for Decimal32: out of range integral type conversion attempted",
            ),
            (
                "Decimal64(-3, 5)",
                "Error converting -3 into u8 for Decimal64: out of range integral type conversion attempted",
            ),
            (
                "Decimal128(-3, 5)",
                "Error converting -3 into u8 for Decimal128: out of range integral type conversion attempted",
            ),
            (
                "Decimal256(-3, 5)",
                "Error converting -3 into u8 for Decimal256: out of range integral type conversion attempted",
            ),
            (
                "Decimal32(3, 500)",
                "Error converting 500 into i8 for Decimal32: out of range integral type conversion attempted",
            ),
            (
                "Decimal64(3, 500)",
                "Error converting 500 into i8 for Decimal64: out of range integral type conversion attempted",
            ),
            (
                "Decimal128(3, 500)",
                "Error converting 500 into i8 for Decimal128: out of range integral type conversion attempted",
            ),
            (
                "Decimal256(3, 500)",
                "Error converting 500 into i8 for Decimal256: out of range integral type conversion attempted",
            ),
            ("Struct(f1 Int64)", "Error unknown token: f1"),
            ("Struct(\"f1\" Int64)", "Expected ':'"),
            (
                "Struct(\"f1\": )",
                "Error finding next type, got unexpected ')'",
            ),
        ];

        for (data_type_string, expected_message) in cases {
            println!("Parsing '{data_type_string}', expecting '{expected_message}'");
            match parse_data_type(data_type_string) {
                Ok(d) => panic!("Expected error while parsing '{data_type_string}', but got '{d}'"),
                Err(e) => {
                    let message = e.to_string();
                    assert!(
                        message.contains(expected_message),
                        "\n\ndid not find expected in actual.\n\nexpected: {expected_message}\nactual: {message}\n"
                    );

                    if !message.contains("Unterminated string") {
                        // errors should also contain a help message
                        assert!(message.contains("Must be a supported arrow type name such as 'Int32' or 'Timestamp(ns)'"), "message: {message}");
                    }
                }
            }
        }
    }

    #[test]
    fn parse_error_type() {
        let err = parse_data_type("foobar").unwrap_err();
        assert!(matches!(err, ArrowError::ParseError(_)));
        assert_eq!(
            err.to_string(),
            "Parser error: Unsupported type 'foobar'. Must be a supported arrow type name such as 'Int32' or 'Timestamp(ns)'. Error unknown token: foobar"
        );
    }
}
