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
