// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Parquet schema definitions and methods to print and parse schema.
//!
//! * [`SchemaDescriptor`] describes the data types of the columns stored in a file
//! * [`ColumnDescriptor`]: Describes the schema of a single (leaf) column.
//! * [`ColumnPath`]: Represents the location of a column in the schema (e.g. a nested field)
//!
//! Parquet distinguishes
//! between "logical" and "physical" data types.
//! For instance, strings (logical type) are stored as byte arrays (physical type).
//! Likewise, temporal types like dates, times, timestamps, etc. (logical type)
//! are stored as integers (physical type).
//!
//! [`SchemaDescriptor`]: types::SchemaDescriptor
//! [`ColumnDescriptor`]: types::ColumnDescriptor
//! [`ColumnPath`]: types::ColumnPath
//!
//! # Example
//!
//! ```rust
//! use parquet::{
//!     basic::{ConvertedType, Repetition, Type as PhysicalType},
//!     schema::{parser, printer, types::Type},
//! };
//! use std::sync::Arc;
//!
//! // Create the following schema:
//! //
//! // message schema {
//! //   OPTIONAL BYTE_ARRAY a (UTF8);
//! //   REQUIRED INT32 b;
//! // }
//!
//! let field_a = Type::primitive_type_builder("a", PhysicalType::BYTE_ARRAY)
//!     .with_converted_type(ConvertedType::UTF8)
//!     .with_repetition(Repetition::OPTIONAL)
//!     .build()
//!     .unwrap();
//!
//! let field_b = Type::primitive_type_builder("b", PhysicalType::INT32)
//!     .with_repetition(Repetition::REQUIRED)
//!     .build()
//!     .unwrap();
//!
//! let schema = Type::group_type_builder("schema")
//!     .with_fields(vec![Arc::new(field_a), Arc::new(field_b)])
//!     .build()
//!     .unwrap();
//!
//! let mut buf = Vec::new();
//!
//! // Print schema into buffer
//! printer::print_schema(&mut buf, &schema);
//!
//! // Parse schema from the string
//! let string_schema = String::from_utf8(buf).unwrap();
//! let parsed_schema = parser::parse_message_type(&string_schema).unwrap();
//!
//! assert_eq!(schema, parsed_schema);
//! ```

pub mod parser;
pub mod printer;
pub mod types;
pub mod visitor;
