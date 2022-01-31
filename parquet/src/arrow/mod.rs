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
//!# Example of writing Arrow record batch to Parquet file
//!
//!```rust
//! use arrow::array::Int32Array;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::record_batch::RecordBatch;
//! use parquet::arrow::arrow_writer::ArrowWriter;
//! use parquet::file::properties::WriterProperties;
//! use std::fs::File;
//! use std::sync::Arc;
//! let ids = Int32Array::from(vec![1, 2, 3, 4]);
//! let vals = Int32Array::from(vec![5, 6, 7, 8]);
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("val", DataType::Int32, false),
//! ]));
//!
//! let file = File::create("data.parquet").unwrap();
//!
//! let batch =
//!     RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(vals)]).unwrap();
//! let batches = vec![batch];
//!
//! // Default writer properties
//! let props = WriterProperties::builder().build();
//!
//! let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
//!
//! for batch in batches {
//!     writer.write(&batch).expect("Writing batch");
//! }
//! writer.close().unwrap();
//! ```
//!
//! `WriterProperties` can be used to set Parquet file options
//! ```rust
//! use parquet::file::properties::WriterProperties;
//! use parquet::basic::{ Compression, Encoding };
//! use parquet::file::properties::WriterVersion;
//!
//! // File compression
//! let props = WriterProperties::builder()
//!     .set_compression(Compression::SNAPPY)
//!     .build();
//! ```
//!
//! # Example of reading parquet file into arrow record batch
//!
//! ```rust
//! use arrow::record_batch::RecordBatchReader;
//! use parquet::file::reader::SerializedFileReader;
//! use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
//! use std::sync::Arc;
//! use std::fs::File;
//!
//! # use arrow::array::Int32Array;
//! # use arrow::datatypes::{DataType, Field, Schema};
//! # use arrow::record_batch::RecordBatch;
//! # use parquet::arrow::arrow_writer::ArrowWriter;
//! # let ids = Int32Array::from(vec![1, 2, 3, 4]);
//! # let schema = Arc::new(Schema::new(vec![
//! #    Field::new("id", DataType::Int32, false),
//! # ]));
//! #
//! # // Write to a memory buffer (can also write to a File)
//! # let file = File::create("data.parquet").unwrap();
//! #
//! # let batch =
//! #    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
//! # let batches = vec![batch];
//! #
//! # let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
//! #
//! # for batch in batches {
//! #     writer.write(&batch).expect("Writing batch");
//! # }
//! # writer.close().unwrap();
//!
//! let file = File::open("data.parquet").unwrap();
//! let file_reader = SerializedFileReader::new(file).unwrap();
//! let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
//!
//! println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());
//! println!("Arrow schema after projection is: {}",
//!    arrow_reader.get_schema_by_columns(vec![0], true).unwrap());
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

experimental_mod!(array_reader);
pub mod arrow_reader;
pub mod arrow_writer;
mod bit_util;
experimental_mod!(converter);
pub(in crate::arrow) mod levels;
pub(in crate::arrow) mod record_reader;
experimental_mod!(schema);

pub use self::arrow_reader::ArrowReader;
pub use self::arrow_reader::ParquetFileArrowReader;
pub use self::arrow_writer::ArrowWriter;

pub use self::schema::{
    arrow_to_parquet_schema, parquet_to_arrow_schema, parquet_to_arrow_schema_by_columns,
    parquet_to_arrow_schema_by_root_columns,
};

/// Schema metadata key used to store serialized Arrow IPC schema
pub const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";
