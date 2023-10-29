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

use crate::schema::types::TypePtr;

use super::super::errors::ParquetError;
use super::super::file::writer::SerializedRowGroupWriter;

/// `write_to_row_group` writes from `self` into `row_group_writer`
/// `schema` builds the schema used by `row_group_writer`
/// The type parameter `T` is used to work around the rust orphan rule
/// when implementing on types such as `&[T]`.
pub trait RecordWriter<T> {
    fn write_to_row_group<W: std::io::Write + Send>(
        &self,
        row_group_writer: &mut SerializedRowGroupWriter<W>,
    ) -> Result<(), ParquetError>;

    /// Generated schema
    fn schema(&self) -> Result<TypePtr, ParquetError>;
}
