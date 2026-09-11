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

use std::sync::Arc;

use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, Datum, PrimitiveArray, RunArray, Scalar, downcast_run_array, make_array,
};
use arrow_buffer::ArrowNativeType;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType};

/// Values and (logical end, physical value index) pairs for an operand.
/// Primitive arrays have one run per row.
struct Operand<'a> {
    values: &'a dyn Array,
    runs: Box<dyn Iterator<Item = (usize, usize)> + 'a>,
}

fn next_run(runs: &mut dyn Iterator<Item = (usize, usize)>) -> Result<(usize, usize), ArrowError> {
    runs.next().ok_or_else(|| {
        ArrowError::ComputeError(
            "Run-end encoded operand has no run for a non-empty logical array".to_owned(),
        )
    })
}

impl<'a> Operand<'a> {
    fn new(array: &'a dyn Array) -> Self {
        let len = array.len();
        downcast_run_array!(array => {
            let values = array.values().as_ref();
            let start = array.get_start_physical_index();
            let count = if array.is_empty() {
                0
            } else {
                array.get_end_physical_index() - start + 1
            };
            let offset = array.offset();
            let runs = Box::new(array.run_ends().values().iter().enumerate().skip(start).take(count)
                .map(move |(index, end)| ((end.as_usize() - offset).min(len), index)));
            Self { values, runs }
        }, _ => {
            let runs = Box::new((0..len).map(|index| (index + 1, index)));
            Self { values: array, runs }
        })
    }
}

fn values_slice(array: &dyn Array) -> ArrayRef {
    downcast_run_array!(array => array.values_slice(), _ => array.slice(0, array.len()))
}

fn run_end_type(array: &dyn Array) -> Option<&DataType> {
    match array.data_type() {
        DataType::RunEndEncoded(ends, _) => Some(ends.data_type()),
        _ => None,
    }
}

fn encode<R: RunEndIndexType>(
    ends: Vec<usize>,
    values: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    let ends = ends
        .into_iter()
        .map(|end| {
            R::Native::from_usize(end).ok_or_else(|| {
                ArrowError::ComputeError(format!("Run end {end} exceeds {}", R::DATA_TYPE))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ends = PrimitiveArray::<R>::new(ends.into(), None);
    Ok(Arc::new(RunArray::<R>::try_new(&ends, values)?))
}

fn finish(
    ends: Vec<usize>,
    values: ArrayRef,
    data_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    match data_type {
        DataType::Int16 => encode::<Int16Type>(ends, values.as_ref()),
        DataType::Int32 => encode::<Int32Type>(ends, values.as_ref()),
        DataType::Int64 => encode::<Int64Type>(ends, values.as_ref()),
        _ => unreachable!("run ends must be Int16, Int32 or Int64"),
    }
}

pub(super) fn unary(
    array: &dyn Array,
    op: impl Fn(&dyn Array) -> Result<ArrayRef, ArrowError>,
) -> Result<ArrayRef, ArrowError> {
    downcast_run_array!(array => {
        let operand = Operand::new(array);
        let ends = operand.runs.map(|(end, _)| end).collect();
        let values = op(array.values_slice().as_ref())?;
        finish(ends, values, array.run_ends_field().data_type())
    }, _ => unreachable!("expected a run array"))
}

pub(super) fn binary(
    left: &dyn Array,
    left_scalar: bool,
    right: &dyn Array,
    right_scalar: bool,
    op: impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<ArrayRef, ArrowError> {
    let len = match (left_scalar, right_scalar) {
        (true, false) => right.len(),
        (false, true) => left.len(),
        _ if left.len() == right.len() => left.len(),
        _ => {
            return Err(ArrowError::ComputeError(
                "Cannot perform arithmetic operation on arrays of different length".to_owned(),
            ));
        }
    };
    let left_type = run_end_type(left);
    let right_type = run_end_type(right);
    if (left_type.is_none() && !left_scalar) || (right_type.is_none() && !right_scalar) {
        return Err(ArrowError::InvalidArgumentError(
            "Run-end encoded arithmetic requires the other operand to be a scalar or run-end encoded array".to_owned(),
        ));
    }
    let result_type = match (left_type, right_type) {
        (Some(DataType::Int64), _) | (_, Some(DataType::Int64)) => DataType::Int64,
        (Some(DataType::Int32), _) | (_, Some(DataType::Int32)) => DataType::Int32,
        _ => DataType::Int16,
    };
    // A scalar does not split runs: evaluate the visible physical values directly,
    // without gathering or expanding the scalar to one value per run.
    if left_scalar || right_scalar {
        let left_values = values_slice(left);
        let right_values = values_slice(right);
        let values = match (left_scalar, right_scalar) {
            (true, true) => op(&Scalar::new(left_values), &Scalar::new(right_values))?,
            (true, false) => op(&Scalar::new(left_values), &right_values)?,
            (false, true) => op(&left_values, &Scalar::new(right_values))?,
            _ => unreachable!(),
        };
        let array = if left_scalar { right } else { left };
        let ends = Operand::new(array).runs.map(|(end, _)| end).collect();
        return finish(ends, values, &result_type);
    }

    let left_ends: Vec<_> = Operand::new(left).runs.map(|(end, _)| end).collect();
    let right_ends: Vec<_> = Operand::new(right).runs.map(|(end, _)| end).collect();
    if left_ends == right_ends {
        let values = op(&values_slice(left), &values_slice(right))?;
        return finish(left_ends, values, &result_type);
    }
    let mut left = Operand::new(left);
    let mut right = Operand::new(right);

    // Validate value types before gathering.
    op(&left.values.slice(0, 0), &right.values.slice(0, 0))?;

    let left_data = left.values.to_data();
    let right_data = right.values.to_data();
    let capacity = left_ends.len() + right_ends.len();
    let mut left_values = MutableArrayData::try_new(vec![&left_data], false, capacity)?;
    let mut right_values = MutableArrayData::try_new(vec![&right_data], false, capacity)?;
    let mut ends = Vec::with_capacity(capacity);
    let mut l = next_run(left.runs.as_mut())?;
    let mut r = next_run(right.runs.as_mut())?;
    loop {
        let end = l.0.min(r.0);
        left_values.try_extend(0, l.1, l.1 + 1)?;
        right_values.try_extend(0, r.1, r.1 + 1)?;
        ends.push(end);
        if end == len {
            break;
        }
        if l.0 == end {
            l = next_run(left.runs.as_mut())?;
        }
        if r.0 == end {
            r = next_run(right.runs.as_mut())?;
        }
    }
    let values = op(
        &make_array(left_values.freeze()),
        &make_array(right_values.freeze()),
    )?;
    finish(ends, values, &result_type)
}
