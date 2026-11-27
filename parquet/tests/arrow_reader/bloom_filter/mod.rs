// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[cfg(feature = "async")]
mod r#async;
mod push;
mod sync;

use bytes::Bytes;
#[cfg(feature = "async")]
use futures::future::{BoxFuture, ready};
#[cfg(feature = "async")]
use parquet::arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader};
use parquet::basic::Type as PhysicalType;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::reader::{ChunkReader, Length};
use parquet::schema::{
    parser::parse_message_type,
    types::{ColumnDescriptor, ColumnPath, SchemaDescriptor, Type},
};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct RecordingReader {
    responses: Mutex<VecDeque<Result<Bytes>>>,
    requests: Mutex<Vec<Range<u64>>>,
}

impl RecordingReader {
    fn new(responses: impl IntoIterator<Item = Bytes>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(Ok).collect()),
            requests: Mutex::default(),
        }
    }

    fn requests(&self) -> Vec<Range<u64>> {
        self.requests.lock().unwrap().clone()
    }

    fn fail_at(&self, index: usize) {
        self.responses.lock().unwrap().insert(
            index,
            Err(ParquetError::General("injected read failure".to_owned())),
        );
    }

    fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.requests.lock().unwrap().push(range);
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected read")
    }
}

impl Length for RecordingReader {
    fn len(&self) -> u64 {
        panic!("Bloom reads must not load metadata")
    }
}

impl ChunkReader for RecordingReader {
    type T = std::io::Empty;

    fn get_read(&self, _: u64) -> Result<Self::T> {
        panic!("Bloom reads must use byte ranges")
    }

    fn get_bytes(&self, offset: u64, length: usize) -> Result<Bytes> {
        self.read(offset..offset + length as u64)
    }
}

#[cfg(feature = "async")]
impl AsyncFileReader for RecordingReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(ready(self.read(range)))
    }

    fn get_metadata<'a>(
        &'a mut self,
        _: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        panic!("Bloom reads must not load metadata")
    }
}

fn column(offset: Option<i64>, length: Option<i32>) -> ColumnChunkMetaData {
    let descriptor = Arc::new(ColumnDescriptor::new(
        Arc::new(
            Type::primitive_type_builder("value", PhysicalType::BYTE_ARRAY)
                .build()
                .unwrap(),
        ),
        0,
        0,
        ColumnPath::new(vec!["value".to_owned()]),
    ));
    ColumnChunkMetaData::builder(descriptor)
        .set_bloom_filter_offset(offset)
        .set_bloom_filter_length(length)
        .build()
        .unwrap()
}

fn metadata(offset: Option<i64>, length: Option<i32>) -> ParquetMetaData {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(
        parse_message_type("message schema { REQUIRED BYTE_ARRAY value; }").unwrap(),
    )));
    let row_group = RowGroupMetaData::builder(schema.clone())
        .set_column_metadata(vec![column(offset, length)])
        .build()
        .unwrap();
    ParquetMetaData::new(
        FileMetaData::new(1, 0, None, None, schema, None),
        vec![row_group],
    )
}

fn encode(num_bytes: i32, bitset_length: usize) -> Bytes {
    // Compact-Thrift zigzag varints for the sizes used by these malformed fixtures.
    let encoded_size: &[u8] = match num_bytes {
        -1 => &[0x01],
        0 => &[0x00],
        32 => &[0x40],
        33 => &[0x42],
        64 => &[0x80, 0x01],
        _ => panic!("unsupported fixture size"),
    };
    let mut bytes = vec![0x15]; // Field 1: i32 numBytes.
    bytes.extend_from_slice(encoded_size);
    // Fields 2–4: BLOCK, XXHASH, UNCOMPRESSED (each a union containing an empty struct),
    // followed by the header's STOP byte.
    bytes.extend_from_slice(&[0x1c, 0x1c, 0, 0, 0x1c, 0x1c, 0, 0, 0x1c, 0x1c, 0, 0, 0]);
    bytes.resize(bytes.len() + bitset_length, 0);
    bytes.into()
}
