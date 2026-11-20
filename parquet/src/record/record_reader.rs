// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::super::errors::ParquetError;
use super::super::file::reader::RowGroupReader;

/// Read up to `num_records` records from `row_group_reader` into `self`.
///
/// The type parameter `T` is used to work around the rust orphan rule
/// when implementing on types such as `Vec<T>`.
pub trait RecordReader<T> {
    /// Read up to `num_records` records from `row_group_reader` into `self`.
    fn read_from_row_group(
        &mut self,
        row_group_reader: &mut dyn RowGroupReader,
        num_records: usize,
    ) -> Result<(), ParquetError>;
}
