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

use crate::arrow::array_reader::ArrayReader;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::RowGroupMetaData;
use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use std::any::Any;
use std::sync::Arc;

pub(crate) struct RowNumberReader {
    buffered_row_numbers: Vec<i64>,
    remaining_row_numbers: std::iter::Flatten<std::vec::IntoIter<std::ops::Range<i64>>>,
}

impl RowNumberReader {
    pub(crate) fn try_new<'a>(
        row_groups: impl Iterator<Item = &'a RowGroupMetaData>,
    ) -> Result<Self> {
        let ranges = row_groups
            .map(|rg| {
                let first_row_number = rg.first_row_index().ok_or(ParquetError::General(
                    "Row group missing row number".to_string(),
                ))?;
                Ok(first_row_number..first_row_number + rg.num_rows())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            buffered_row_numbers: Vec::new(),
            remaining_row_numbers: ranges.into_iter().flatten(),
        })
    }
}

impl ArrayReader for RowNumberReader {
    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let starting_len = self.buffered_row_numbers.len();
        self.buffered_row_numbers
            .extend((&mut self.remaining_row_numbers).take(batch_size));
        Ok(self.buffered_row_numbers.len() - starting_len)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        // TODO: Use advance_by when it stabilizes to improve performance
        Ok((&mut self.remaining_row_numbers).take(num_records).count())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &DataType::Int64
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from_iter(
            self.buffered_row_numbers.drain(..),
        )))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}
