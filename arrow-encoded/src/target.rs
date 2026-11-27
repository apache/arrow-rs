// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::{Array, ArrayRef};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, Field};

use crate::logically_equal;

/// Check that `target` can hold `null_count` nulls of logical type `data_type`.
pub fn validate_target(
    data_type: &DataType,
    null_count: usize,
    target: &Field,
) -> Result<(), ArrowError> {
    if !logically_equal(data_type, target.data_type()) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "cannot decode {data_type} to {}: different logical types",
            target.data_type()
        )));
    }
    if null_count > 0 && !target.is_nullable() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "cannot decode {null_count} nulls to non-nullable field {}",
            target.name()
        )));
    }
    Ok(())
}

/// Return `array` in the layout of `target`, casting only when it differs.
///
/// With no target, `array` is returned as is.
pub fn cast_to_target(array: ArrayRef, target: Option<&Field>) -> Result<ArrayRef, ArrowError> {
    let Some(target) = target else {
        return Ok(array);
    };
    validate_target(array.data_type(), array.logical_null_count(), target)?;
    if array.data_type() == target.data_type() {
        return Ok(array);
    }
    // A cast between logically equal types never loses values, so any
    // failure is an error rather than a null.
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    cast_with_options(&array, target.data_type(), &options)
}
