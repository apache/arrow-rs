// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! String Writer
//! This string writer encapsulates `std::string::String` and
//! implements `std::io::Write` trait, which makes String as a
//! writable object like File.
//!
//! Example:
//!
//! ```
//! #[cfg(feature = "csv")]
//! {
//! use arrow::array::*;
//! use arrow::csv;
//! use arrow::datatypes::*;
//! use arrow::record_batch::RecordBatch;
//! use arrow::util::string_writer::StringWriter;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("c1", DataType::Utf8, false),
//!     Field::new("c2", DataType::Float64, true),
//!     Field::new("c3", DataType::UInt32, false),
//!     Field::new("c3", DataType::Boolean, true),
//! ]);
//! let c1 = StringArray::from(vec![
//!     "Lorem ipsum dolor sit amet",
//!     "consectetur adipiscing elit",
//!     "sed do eiusmod tempor",
//! ]);
//! let c2 = PrimitiveArray::<Float64Type>::from(vec![
//!     Some(123.564532),
//!     None,
//!     Some(-556132.25),
//! ]);
//! let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
//! let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
//!
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema),
//!     vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
//! )
//! .unwrap();
//!
//! let sw = StringWriter::new();
//! let mut writer = csv::Writer::new(sw);
//! writer.write(&batch).unwrap();
//! }
//! ```

use core::str;
use std::fmt::Formatter;
use std::io::{Error, ErrorKind, Result, Write};

/// A writer that allows writing to a `String`
/// like an `std::io::Write` object.
#[derive(Debug, Default)]
pub struct StringWriter {
    data: String,
}

impl StringWriter {
    /// Create a new `StringWriter`
    pub fn new() -> Self {
        Self::default()
    }
}

impl std::fmt::Display for StringWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl Write for StringWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let string = match str::from_utf8(buf) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(ErrorKind::InvalidData, e));
            }
        };
        self.data.push_str(string);
        Ok(string.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
