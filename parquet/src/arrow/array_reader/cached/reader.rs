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
use arrow_array::{new_empty_array, Array, ArrayRef, BooleanArray};
use arrow_schema::DataType;
use std::any::Any;
use std::collections::VecDeque;

pub(crate) struct CachedArrayReader {
    /// The cached arrays. These should already be broken down into the correct batch_size chunks
    cached_arrays: VecDeque<ArrayRef>,
    data_type: DataType,
    // /// The filter that was applied to the cached array (that has already been applied)
    //filter: BooleanArray,
    /// The length of the currently "in progress" array
    current_length: usize,
}

impl CachedArrayReader {
    pub(crate) fn new(cached_arrays: Vec<ArrayRef>, _filters: &[BooleanArray]) -> Self {
        //let input: Vec<&dyn Array> = filters.iter().map(|b| b as &dyn Array).collect::<Vec<_>>();
        //let filter = concat(&input).unwrap().as_boolean().clone();
        let data_type = cached_arrays
            .first()
            .expect("had at least one array")
            .data_type()
            .clone();
        Self {
            cached_arrays: VecDeque::from(cached_arrays),
            data_type,
            current_length: 0,
        }
    }
}

impl ArrayReader for CachedArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> crate::errors::Result<usize> {
        // since the entire array is cached, reads always succeed
        self.current_length += batch_size;
        Ok(batch_size)
    }

    // Produce the "in progress" batch
    fn consume_batch(&mut self) -> crate::errors::Result<ArrayRef> {
        if self.current_length == 0 {
            return Ok(new_empty_array(&self.data_type));
        }

        let next_array = self.cached_arrays.pop_front().ok_or_else(|| {
            crate::errors::ParquetError::General(
                "Internal error: no more cached arrays".to_string(),
            )
        })?;

        // the next batch is the next array in the queue
        assert_eq!(self.current_length, next_array.len());
        self.current_length = 0;
        Ok(next_array)
    }

    fn skip_records(&mut self, num_records: usize) -> crate::errors::Result<usize> {
        // todo!()
        // it would be good to verify the pattern of read/consume matches
        // the boolean array
        Ok(num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None // TODO this is likely not right for structured types
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None // TODO this is likely not right for structured types
    }
}
