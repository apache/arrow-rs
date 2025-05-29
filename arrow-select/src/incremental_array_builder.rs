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

//! Incrementally builds Arrow Arrays from a stream of values

use crate::concat::ArrayBuilderExtAppend;
use crate::filter::{ArrayBuilderExtFilter, FilterPredicate};
use arrow_array::builder::{ArrayBuilder, GenericByteViewBuilder, PrimitiveBuilder};
use arrow_array::types::ByteViewType;
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType};
use arrow_schema::ArrowError;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// This builder is used to incrementally build Arrow Arrays
///
/// The difference between this and a regular `ArrayBuilder` is that this
/// builder includes a `filter` method that can quickly copy append subsets of
/// existing arrays to the incremental array.
///
/// This is the same operation as applying `filter` then `concat` kernels
/// but is more efficient as it avoids the intermediate array creation and
/// does not need extra buffering.
///
/// This is useful for scenarios where you want to build an output array
/// from the results of filtering or taking elements from existing arrays,
/// a common operation in data processing pipelines and query engines.
///
/// (TODO diagram)
///
/// TODO: add comments
///
/// TODO note this can also be extended to support `take` operations as well
///
/// TODO make this an extension trait for `ArrayBuilder`s
pub trait IncrementalArrayBuilder:
    ArrayBuilder + ArrayBuilderExtFilter + ArrayBuilderExtAppend
{
}

/// Generic incremental array builder for any Arrow Array type
///
/// Uses `concat` / `filter` to build the final output array. This is less
/// efficient than using a specific builder but works for any Arrow Array type
/// until we have implemented specific builders all types.
#[derive(Debug, Default)]
pub struct GenericIncrementalArrayBuilder {
    /// In progress arrays being built
    arrays: Vec<ArrayRef>,
}

impl GenericIncrementalArrayBuilder {
    /// Create a new generic incremental array builder
    pub fn new() -> Self {
        Default::default()
    }
}

impl ArrayBuilder for GenericIncrementalArrayBuilder {
    fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    fn len(&self) -> usize {
        self.arrays.iter().map(|a| a.len()).sum()
    }

    /// Build the final output array by concatenating all appended arrays, and
    /// resetting the builder state.
    fn finish(&mut self) -> ArrayRef {
        let output_array = self.finish_cloned();
        self.arrays.clear(); // Reset the builder state
        output_array
    }

    fn finish_cloned(&self) -> ArrayRef {
        // must conform to concat signature
        let concat_input: Vec<&dyn Array> = self.arrays.iter().map(|a| a.as_ref()).collect();
        crate::concat::concat(&concat_input).expect("concat should not fail")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl ArrayBuilderExtFilter for GenericIncrementalArrayBuilder {
    fn append_filtered(
        &mut self,
        array: &dyn Array,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        println!(
            "[GenericIncrementalArrayBuilder] append_filtered {} called",
            array.data_type()
        );
        // Filter the array using the filter mask
        let filtered_array = crate::filter::filter_array(array, filter)?;
        println!(
            "[GenericIncrementalArrayBuilder] filtered_array_type {} ",
            filtered_array.data_type()
        );
        self.arrays.push(filtered_array);
        Ok(())
    }
}

impl ArrayBuilderExtAppend for GenericIncrementalArrayBuilder {
    fn append_array(&mut self, array: &ArrayRef) -> Result<(), ArrowError> {
        println!(
            "[GenericIncrementalArrayBuilder] append_ {} called",
            array.data_type()
        );
        self.arrays.push(Arc::clone(array));
        Ok(())
    }
}

impl IncrementalArrayBuilder for GenericIncrementalArrayBuilder {}

impl<T: ArrowPrimitiveType> IncrementalArrayBuilder for PrimitiveBuilder<T> {}

impl<T: ByteViewType> IncrementalArrayBuilder for GenericByteViewBuilder<T> {}
