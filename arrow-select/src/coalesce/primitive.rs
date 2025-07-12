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

use crate::coalesce::InProgressArray;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use std::fmt::Debug;
use std::sync::Arc;

/// InProgressArray for [`PrimitiveArray`]
#[derive(Debug)]
pub(crate) struct InProgressPrimitiveArray<T: ArrowPrimitiveType> {
    /// Data type of the array
    data_type: DataType,
    /// The current source, if any
    source: Option<ArrayRef>,
    /// the target batch size (and thus size for views allocation)
    batch_size: usize,
    /// In progress nulls
    nulls: NullBufferBuilder,
    /// The currently in progress array
    current: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> InProgressPrimitiveArray<T> {
    /// Create a new `InProgressPrimitiveArray`
    pub(crate) fn new(batch_size: usize, data_type: DataType) -> Self {
        Self {
            data_type,
            batch_size,
            source: None,
            nulls: NullBufferBuilder::new(batch_size),
            current: vec![],
        }
    }

    /// Allocate space for output values if necessary.
    ///
    /// This is done on write (when we know it is necessary) rather than
    /// eagerly to avoid allocations that are not used.
    fn ensure_capacity(&mut self) {
        self.current.reserve(self.batch_size);
    }
}

impl<T: ArrowPrimitiveType + Debug> InProgressArray for InProgressPrimitiveArray<T> {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.ensure_capacity();

        let s = self
            .source
            .as_ref()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Internal Error: InProgressPrimitiveArray: source not set".to_string(),
                )
            })?
            .as_primitive::<T>();

        // add nulls if necessary
        if let Some(nulls) = s.nulls().as_ref() {
            let nulls = nulls.slice(offset, len);
            self.nulls.append_buffer(&nulls);
        } else {
            self.nulls.append_n_non_nulls(len);
        };

        // Copy the values
        self.current
            .extend_from_slice(&s.values()[offset..offset + len]);

        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        // take and reset the current values and nulls
        let values = std::mem::take(&mut self.current);
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);

        let array = PrimitiveArray::<T>::try_new(ScalarBuffer::from(values), nulls)?
            // preserve timezone / precision+scale if applicable
            .with_data_type(self.data_type.clone());
        Ok(Arc::new(array))
    }
}
