// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::schema::types::TypePtr;

use super::super::errors::ParquetError;
use super::super::file::writer::SerializedRowGroupWriter;

/// Trait describing how to write a record (the implementer) to a row group writer.
///
/// [`parquet_derive`] crate provides a derive macro [`ParquetRecordWriter`] for this trait
/// for unnested structs.
///
/// The type parameter `T` is used to work around the rust orphan rule
/// when implementing on types such as `&[T]`.
///
/// [`parquet_derive`]: https://crates.io/crates/parquet_derive
/// [`ParquetRecordWriter`]: https://docs.rs/parquet_derive/53.0.0/parquet_derive/derive.ParquetRecordWriter.html
pub trait RecordWriter<T> {
    /// Writes from `self` into `row_group_writer`.
    fn write_to_row_group<W: std::io::Write + Send>(
        &self,
        row_group_writer: &mut SerializedRowGroupWriter<W>,
    ) -> Result<(), ParquetError>;

    /// Generated schema used by `row_group_writer`
    fn schema(&self) -> Result<TypePtr, ParquetError>;
}
