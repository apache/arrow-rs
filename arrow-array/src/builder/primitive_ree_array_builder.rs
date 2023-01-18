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

use crate::{types::ArrowRunEndIndexType, ArrowPrimitiveType, RunEndEncodedArray};

use super::PrimitiveBuilder;

use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;

/// Array builder for [`RunEndEncodedArray`] that encodes primitive values.
///
/// # Example:
///
/// ```
///
/// # use arrow_array::builder::PrimitiveREEArrayBuilder;
/// # use arrow_array::types::{UInt32Type, Int16Type};
/// # use arrow_array::{Array, UInt32Array, Int16Array};
///
/// let mut builder =
/// PrimitiveREEArrayBuilder::<Int16Type, UInt32Type>::new();
/// builder.append_value(1234).unwrap();
/// builder.append_value(1234).unwrap();
/// builder.append_value(1234).unwrap();
/// builder.append_null().unwrap();
/// builder.append_value(5678).unwrap();
/// builder.append_value(5678).unwrap();
/// let array = builder.finish();
///
/// assert_eq!(
///     array.run_ends(),
///     &Int16Array::from(vec![Some(3), Some(4), Some(6)])
/// );
///
/// let av = array.values();
///
/// assert!(!av.is_null(0));
/// assert!(av.is_null(1));
/// assert!(!av.is_null(2));
///
/// // Values are polymorphic and so require a downcast.
/// let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();
///
/// assert_eq!(ava, &UInt32Array::from(vec![Some(1234), None, Some(5678)]));
/// ```
#[derive(Debug)]
pub struct PrimitiveREEArrayBuilder<R, V>
where
    R: ArrowRunEndIndexType,
    V: ArrowPrimitiveType,
{
    run_ends_builder: PrimitiveBuilder<R>,
    values_builder: PrimitiveBuilder<V>,
    current_value: Option<V::Native>,
    current_run_end_index: usize,
}

impl<R, V> Default for PrimitiveREEArrayBuilder<R, V>
where
    R: ArrowRunEndIndexType,
    V: ArrowPrimitiveType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R, V> PrimitiveREEArrayBuilder<R, V>
where
    R: ArrowRunEndIndexType,
    V: ArrowPrimitiveType,
{
    /// Creates a new `PrimitiveREEArrayBuilder`
    pub fn new() -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::new(),
            values_builder: PrimitiveBuilder::new(),
            current_value: None,
            current_run_end_index: 0,
        }
    }

    /// Creates a new `PrimitiveREEArrayBuilder` with the provided capacity
    ///
    /// `capacity`: the expected number of run-end encoded values.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            run_ends_builder: PrimitiveBuilder::with_capacity(capacity),
            values_builder: PrimitiveBuilder::with_capacity(capacity),
            current_value: None,
            current_run_end_index: 0,
        }
    }
}

impl<R, V> PrimitiveREEArrayBuilder<R, V>
where
    R: ArrowRunEndIndexType,
    V: ArrowPrimitiveType,
{
    /// Appends Option<V> to the logical array encoded by the RunEndEncodedArray.
    pub fn append_option(&mut self, value: Option<V::Native>) -> Result<(), ArrowError> {
        if self.current_run_end_index == 0 {
            self.current_run_end_index = 1;
            self.current_value = value;
            return Ok(());
        }
        if self.current_value != value {
            self.append_run_end()?;
            self.current_value = value;
        }

        self.current_run_end_index = self
            .current_run_end_index
            .checked_add(1)
            .ok_or(ArrowError::RunEndIndexOverflowError)?;

        Ok(())
    }
    /// Appends value to the logical array encoded by the run-ends array.
    pub fn append_value(&mut self, value: V::Native) -> Result<(), ArrowError> {
        self.append_option(Some(value))
    }
    /// Appends null to the logical array encoded by the run-ends array.
    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        self.append_option(None)
    }
    /// Creates the RunEndEncodedArray and resets the builder.
    /// Panics if RunEndEncodedArray cannot be built.
    pub fn finish(&mut self) -> RunEndEncodedArray<R> {
        //write the last run end to the array.
        self.append_run_end().unwrap();

        //reset the run index to zero.
        self.current_value = None;
        self.current_run_end_index = 0;

        //build the run encoded array by adding run_ends and values array as its children.
        let run_ends_array = self.run_ends_builder.finish();
        let values_array = self.values_builder.finish();
        RunEndEncodedArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }
    /// Creates the RunEndEncodedArray and without resetting the builder.
    /// Panics if RunEndEncodedArray cannot be built.
    pub fn finish_cloned(&mut self) -> RunEndEncodedArray<R> {
        //write the last run end to the array.
        self.append_run_end().unwrap();

        //build the run encoded array by adding run_ends and values array as its children.
        let run_ends_array = self.run_ends_builder.finish_cloned();
        let values_array = self.values_builder.finish_cloned();
        RunEndEncodedArray::<R>::try_new(&run_ends_array, &values_array).unwrap()
    }

    //Appends the current run to the array
    fn append_run_end(&mut self) -> Result<(), ArrowError> {
        let run_end_index = R::Native::from_usize(self.current_run_end_index)
            .ok_or_else(|| {
                ArrowError::ParseError(format!(
                    "Cannot convert the value {} from `usize` to native form of arrow datatype {}",
                    self.current_run_end_index,
                    R::DATA_TYPE
                ))
            })?;
        self.run_ends_builder.append_value(run_end_index);
        self.values_builder.append_option(self.current_value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::PrimitiveREEArrayBuilder;
    use crate::types::{Int16Type, UInt32Type};
    use crate::{Int16Array, UInt32Array};
    #[test]
    fn test_primitive_ree_array_builder() {
        let mut builder = PrimitiveREEArrayBuilder::<Int16Type, UInt32Type>::new();
        builder.append_value(1234).unwrap();
        builder.append_value(1234).unwrap();
        builder.append_value(1234).unwrap();
        builder.append_null().unwrap();
        builder.append_value(5678).unwrap();
        builder.append_value(5678).unwrap();
        let array = builder.finish();

        assert_eq!(
            array.run_ends(),
            &Int16Array::from(vec![Some(3), Some(4), Some(6)])
        );

        let av = array.values();

        assert!(!av.is_null(0));
        assert!(av.is_null(1));
        assert!(!av.is_null(2));

        // Values are polymorphic and so require a downcast.
        let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();

        assert_eq!(ava, &UInt32Array::from(vec![Some(1234), None, Some(5678)]));
    }
}
