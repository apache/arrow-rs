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

use arrow_array::{Array, ArrayRef, Int32Array, Float64Array, StringArray, BooleanArray, PrimitiveArray};
use arrow_array::cast::{AsArray, as_primitive_array};
use arrow_schema::DataType;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, Float32Type};
use std::fmt::Debug;
use std::sync::Arc;
use std::cmp::Ordering;

/// Operator for column value predicates
#[derive(Debug, Clone, PartialEq)]
pub enum PushdownOp {
    /// Equal to
    Eq,
    /// Not equal to
    NotEq,
    /// Less than
    Lt,
    /// Less than or equal to
    LtEq,
    /// Greater than
    Gt, 
    // Greater than or equal to
    GtEq,
    /// Value in a set
    In,
    /// Value between two bounds (inclusive)
    Between,
}

/// A value used in predicate pushdown conditions
#[derive(Debug, Clone)]
pub enum PushdownValue {
    /// A scalar value
    Scalar(Arc<dyn Array>),
    /// A range for between operations
    Range(Arc<dyn Array>, Arc<dyn Array>),
    /// Multiple values for IN operations
    Set(Arc<dyn Array>),
}

/// A predicate that can be pushed down to the Parquet reader
/// to skip data that doesn't match the condition
#[derive(Debug, Clone)]
pub struct PredicatePushdown {
    /// Column name
    column: String,
    /// Predicate operator
    op: PushdownOp,
    /// Value(s) used in the predicate
    value: PushdownValue,
}

impl PredicatePushdown {
    /// Create a new predicate pushdown for a column with the given operator and value
    pub fn new(column: impl Into<String>, op: PushdownOp, value: PushdownValue) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }

    /// Create a predicate for column == value
    pub fn eq<A: Into<Arc<dyn Array>>>(column: impl Into<String>, value: A) -> Self {
        Self::new(
            column,
            PushdownOp::Eq,
            PushdownValue::Scalar(value.into()),
        )
    }

    /// Create a predicate for column != value
    pub fn not_eq<A: Into<Arc<dyn Array>>>(column: impl Into<String>, value: A) -> Self {
        Self::new(
            column,
            PushdownOp::NotEq,
            PushdownValue::Scalar(value.into()),
        )
    }

    /// Create a predicate for column < value
    pub fn lt<A: Into<Arc<dyn Array>>>(column: impl Into<String>, value: A) -> Self {
        Self::new(
            column,
            PushdownOp::Lt,
            PushdownValue::Scalar(value.into()),
        )
    }

    /// Create a predicate for column <= value
    pub fn lteq<A: Into<Arc<dyn Array>>>(column: impl Into<String>, value: A) -> Self {
        Self::new(
            column,
            PushdownOp::LtEq,
            PushdownValue::Scalar(value.into()),
        )
    }

    /// Create a predicate for column > value
    pub fn gt<A: Into<Arc<dyn Array>>>(column: impl Into<String>, value: A) -> Self {
        Self::new(
            column,
            PushdownOp::Gt,
            PushdownValue::Scalar(value.into()),
        )
    }

    /// Create a predicate for column >= value
    pub fn gteq<A: Into<Arc<dyn Array>>>(column: impl Into<String>, value: A) -> Self {
        Self::new(
            column,
            PushdownOp::GtEq,
            PushdownValue::Scalar(value.into()),
        )
    }

    /// Create a predicate for column IN (values)
    pub fn in_list<A: Into<Arc<dyn Array>>>(column: impl Into<String>, values: A) -> Self {
        Self::new(
            column,
            PushdownOp::In,
            PushdownValue::Set(values.into()),
        )
    }

    /// Create a predicate for column BETWEEN min AND max (inclusive)
    pub fn between<A: Into<Arc<dyn Array>>, B: Into<Arc<dyn Array>>>(
        column: impl Into<String>,
        min: A,
        max: B,
    ) -> Self {
        Self::new(
            column,
            PushdownOp::Between,
            PushdownValue::Range(min.into(), max.into()),
        )
    }

    /// Get the column name for this predicate
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Get the operator for this predicate
    pub fn op(&self) -> &PushdownOp {
        &self.op
    }

    /// Get the value for this predicate
    pub fn value(&self) -> &PushdownValue {
        &self.value
    }
    

    /// Determines if a chunk can possibly contain rows that match our predicate
    /// Returns true if the chunk might contain matching rows, false if it definitely doesn't
    pub(crate) fn can_use_chunk(&self, min: &dyn Array, max: &dyn Array) -> bool {
        match (&self.op, &self.value) {
            (PushdownOp::Gt, PushdownValue::Scalar(pred_value)) => {
                // For column > value: can use if max_value > pred_value
                compare_max_gt_scalar(max, pred_value.as_ref())
            },
            _ => true, // Default to using the chunk if we can't determine
        }
    }
}


/// Checks if max value in array > scalar value
/// Returns true if the chunk could contain values that satisfy column > scalar
fn compare_max_gt_scalar(max_array: &dyn Array, scalar: &dyn Array) -> bool {
    if scalar.len() != 1 {
        return true; // Not a scalar value, be safe and use the chunk
    }

    match max_array.data_type() {
        DataType::Int32 => {
            let max_array = as_primitive_array::<Int32Type>(max_array);
            let scalar = as_primitive_array::<Int32Type>(scalar);
            if scalar.is_null(0) || max_array.is_null(0) {
                return true; // Nulls involved, be safe and use the chunk
            }
            
            let scalar_value = scalar.value(0);
            let max_value = max_array.value(0);
            
            // If max_value > scalar_value, then the chunk could have values > scalar_value
            max_value > scalar_value
        },
        DataType::Int64 => {
            let max_array = as_primitive_array::<Int64Type>(max_array);
            let scalar = as_primitive_array::<Int64Type>(scalar);
            if scalar.is_null(0) || max_array.is_null(0) {
                return true;
            }
            
            let scalar_value = scalar.value(0);
            let max_value = max_array.value(0);
            
            max_value > scalar_value
        },
        DataType::Float32 => {
            let max_array = as_primitive_array::<Float32Type>(max_array);
            let scalar = as_primitive_array::<Float32Type>(scalar);
            if scalar.is_null(0) || max_array.is_null(0) {
                return true;
            }
            
            let scalar_value = scalar.value(0);
            
            // Handle NaN values - be safe if NaN present
            if scalar_value.is_nan() || max_array.iter().any(|x| x.map_or(false, |v| v.is_nan())) {
                return true;
            }
            
            // Find max non-NaN value
            let max_value = max_array.iter()
                .flatten()
                .filter(|&x| !x.is_nan())
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                
            match max_value {
                Some(max_value) => max_value > scalar_value,
                None => false, // No valid values, can't match predicate
            }
        },
        // Additional data types would follow the same pattern
        // ...
        _ => true, // Default to using the chunk for unsupported types
    }
}

/// Convenience methods for creating common predicate types
pub mod push {
    use super::*;
    
    /// Create an int32 scalar array from a single value
    pub fn int32(value: i32) -> ArrayRef {
        Arc::new(Int32Array::from(vec![value])) as ArrayRef
    }
    
}

/// Container for multiple predicate pushdowns
#[derive(Debug, Default, Clone)]
pub struct PredicatePushdowns {
    /// The predicates to push down
    predicates: Vec<PredicatePushdown>,
}

impl PredicatePushdowns {
    /// Create a new empty set of predicates
    pub fn new() -> Self {
        Self {
            predicates: Vec::new(),
        }
    }

    /// Add a predicate to the set
    pub fn add(&mut self, predicate: PredicatePushdown) {
        self.predicates.push(predicate);
    }

    /// Create from a single predicate
    pub fn from_predicate(predicate: PredicatePushdown) -> Self {
        Self {
            predicates: vec![predicate],
        }
    }

    /// Get the predicates in this set
    pub fn predicates(&self) -> &[PredicatePushdown] {
        &self.predicates
    }
} 
