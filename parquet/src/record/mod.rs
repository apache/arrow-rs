// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Contains record-based API for reading Parquet files.

mod api;
pub mod reader;
mod record_reader;
mod record_writer;
mod triplet;

pub use self::{
    api::{
        Field, List, ListAccessor, Map, MapAccessor, Row, RowAccessor, RowColumnIter, RowFormatter,
    },
    record_reader::RecordReader,
    record_writer::RecordWriter,
};
