// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Arrow selection kernels
//!
//! # Platform Support
//!
//! Only little-endian platforms are officially supported and tested in CI.
//! Big-endian platforms are not tested in CI and may not work correctly.
//! Fixes for big-endian platforms are welcome and handled on a best-effort basis,
//! but compatibility is not guaranteed.

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod coalesce;
pub mod concat;
pub mod dictionary;
pub mod filter;
pub mod interleave;
pub mod merge;
pub mod nullif;
pub mod take;
pub mod union_extract;
pub mod window;
pub mod zip;

#[cfg(test)]
mod test_util {
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, Int16Array, Int32Array, Int64Array, StringArray, make_array,
    };
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field};

    pub(crate) fn run_arrays() -> Vec<ArrayRef> {
        let mut arrays = Vec::new();
        for ends in [
            Int16Array::from(vec![2, 4]).to_data(),
            Int32Array::from(vec![2, 4]).to_data(),
            Int64Array::from(vec![2, 4]).to_data(),
        ] {
            for nullable in [false, true] {
                let values =
                    StringArray::from(vec![Some("a"), if nullable { None } else { Some("b") }]);
                let values_field = Field::new("strings", DataType::Utf8, nullable)
                    .with_metadata([("source".to_string(), "test".to_string())]);
                let data_type = DataType::RunEndEncoded(
                    Arc::new(Field::new("ends", ends.data_type().clone(), false)),
                    Arc::new(values_field),
                );
                let data = ArrayData::builder(data_type)
                    .len(4)
                    .child_data(vec![ends.clone(), values.to_data()])
                    .build()
                    .unwrap();
                let array = make_array(data);
                arrays.extend([Arc::clone(&array), array.slice(1, 3), array.slice(4, 0)]);
            }
        }
        arrays
    }
}
