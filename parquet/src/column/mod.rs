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

//! Low level column reader and writer APIs.
//!
//! This API is designed for reading and writing column values, definition and repetition
//! levels directly.
//!
//! # Example of writing and reading data
//!
//! Data has the following format:
//! ```text
//! +---------------+
//! |         values|
//! +---------------+
//! |[1, 2]         |
//! |[3, null, null]|
//! +---------------+
//! ```
//!
//! The example uses column writer and reader APIs to write raw values, definition and
//! repetition levels and read them to verify write/read correctness.
//!
//! ```rust,no_run
//! # use std::{fs, path::Path, sync::Arc};
//! #
//! # use parquet::{
//! #    column::{reader::ColumnReader, writer::ColumnWriter},
//! #    data_type::Int32Type,
//! #    file::{
//! #        properties::WriterProperties,
//! #        reader::{FileReader, SerializedFileReader},
//! #        writer::SerializedFileWriter,
//! #    },
//! #    schema::parser::parse_message_type,
//! # };
//!
//! let path = Path::new("/path/to/column_sample.parquet");
//!
//! // Writing data using column writer API.
//!
//! let message_type = "
//!   message schema {
//!     optional group values (LIST) {
//!       repeated group list {
//!         optional INT32 element;
//!       }
//!     }
//!   }
//! ";
//! let schema = Arc::new(parse_message_type(message_type).unwrap());
//! let file = fs::File::create(path).unwrap();
//! let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
//!
//! let mut row_group_writer = writer.next_row_group().unwrap();
//! while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
//!     col_writer
//!         .typed::<Int32Type>()
//!         .write_batch(&[1, 2, 3], Some(&[3, 3, 3, 2, 2]), Some(&[0, 1, 0, 1, 1]))
//!         .unwrap();
//!     col_writer.close().unwrap();
//! }
//! row_group_writer.close().unwrap();
//!
//! writer.close().unwrap();
//!
//! // Reading data using column reader API.
//!
//! let file = fs::File::open(path).unwrap();
//! let reader = SerializedFileReader::new(file).unwrap();
//! let metadata = reader.metadata();
//!
//! let mut values = vec![];
//! let mut def_levels = vec![];
//! let mut rep_levels = vec![];
//!
//! for i in 0..metadata.num_row_groups() {
//!     let row_group_reader = reader.get_row_group(i).unwrap();
//!     let row_group_metadata = metadata.row_group(i);
//!
//!     for j in 0..row_group_metadata.num_columns() {
//!         let mut column_reader = row_group_reader.get_column_reader(j).unwrap();
//!         match column_reader {
//!             // You can also use `get_typed_column_reader` method to extract typed reader.
//!             ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
//!                 let (records, values, levels) = typed_reader.read_records(
//!                     8, // maximum records to read
//!                     Some(&mut def_levels),
//!                     Some(&mut rep_levels),
//!                     &mut values,
//!                 ).unwrap();
//!                 assert_eq!(records, 2);
//!                 assert_eq!(levels, 5);
//!                 assert_eq!(values, 3);
//!             }
//!             _ => {}
//!         }
//!     }
//! }
//!
//! assert_eq!(values, vec![1, 2, 3]);
//! assert_eq!(def_levels, vec![3, 3, 3, 2, 2]);
//! assert_eq!(rep_levels, vec![0, 1, 0, 1, 1]);
//! ```

pub mod page;
pub mod reader;
pub mod writer;
