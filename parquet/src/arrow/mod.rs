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

//! [Apache Arrow](http://arrow.apache.org/) is a cross-language development platform for
//! in-memory data.
//!
//! This mod provides API for converting between arrow and parquet.
//!
//! # Example of reading parquet file into arrow record batch
//!
//! ```rust, no_run
//! use arrow::record_batch::RecordBatchReader;
//! use parquet::file::reader::SerializedFileReader;
//! use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
//! use std::sync::Arc;
//! use std::fs::File;
//!
//! let file = File::open("parquet.file").unwrap();
//! let file_reader = SerializedFileReader::new(file).unwrap();
//! let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
//!
//! println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());
//! println!("Arrow schema after projection is: {}",
//!    arrow_reader.get_schema_by_columns(vec![2, 4, 6], true).unwrap());
//!
//! let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();
//!
//! for maybe_record_batch in record_batch_reader {
//!    let record_batch = maybe_record_batch.unwrap();
//!    if record_batch.num_rows() > 0 {
//!        println!("Read {} records.", record_batch.num_rows());
//!    } else {
//!        println!("End of file!");
//!    }
//!}
//! ```

pub mod array_reader;
pub mod arrow_array_reader;
pub mod arrow_reader;
pub mod arrow_writer;
pub mod converter;
pub(in crate::arrow) mod levels;
pub(in crate::arrow) mod record_reader;
pub mod schema;

pub use self::arrow_reader::ArrowReader;
pub use self::arrow_reader::ParquetFileArrowReader;
pub use self::arrow_writer::ArrowWriter;
pub use self::schema::{
    arrow_to_parquet_schema, parquet_to_arrow_schema, parquet_to_arrow_schema_by_columns,
    parquet_to_arrow_schema_by_root_columns,
};

/// Schema metadata key used to store serialized Arrow IPC schema
pub const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";
