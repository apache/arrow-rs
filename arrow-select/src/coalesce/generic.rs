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

use super::InProgressArray;
use crate::concat::concat;
use arrow_array::{Array, ArrayRef};
use arrow_schema::ArrowError;

/// Fallback implementation for [`InProgressArray`]
///
/// Internally, buffers arrays and calls [`concat`]
///
/// [`concat`]: crate::concat::concat
#[derive(Debug)]
pub(crate) struct GenericInProgressArray {
    /// The buffered arrays
    buffered_arrays: Vec<ArrayRef>,
}

impl GenericInProgressArray {
    /// Create a new `GenericInProgressArray`
    pub(crate) fn new() -> Self {
        Self {
            buffered_arrays: vec![],
        }
    }
}
impl InProgressArray for GenericInProgressArray {
    fn push_array(&mut self, array: ArrayRef) {
        self.buffered_arrays.push(array);
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        // Concatenate all buffered arrays into a single array, which uses 2x
        // peak memory
        let array = concat(
            &self
                .buffered_arrays
                .iter()
                .map(|array| array as &dyn Array)
                .collect::<Vec<_>>(),
        )?;
        self.buffered_arrays.clear();
        Ok(array)
    }
}
