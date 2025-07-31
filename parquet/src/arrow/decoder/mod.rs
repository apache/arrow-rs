// This file contains both Apache Software Foundation (ASF) licensed code as
// well as Synnada, Inc. extensions. Changes that constitute Synnada, Inc.
// extensions are available in the SYNNADA-CONTRIBUTIONS.txt file. Synnada, Inc.
// claims copyright only for Synnada, Inc. extensions. The license notice
// applicable to non-Synnada sections of the file is given below.
// --
//
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

//! Specialized decoders optimised for decoding to arrow format

mod delta_byte_array;
mod dictionary_index;

pub use delta_byte_array::DeltaByteArrayDecoder;
pub use dictionary_index::DictIndexDecoder;

// THESE IMPORTS ARE ARAS ONLY
use arrow_data::UnsafeFlag;

// THIS ENUM IS ARAS ONLY
///
/// Default value for invalid UTF-8 strings.
///
/// This enum is used to specify the default value for invalid UTF-8 strings
/// when decoding column values. It can be set to a specific default string,
/// a null value, or no default value at all.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum DefaultValueForInvalidUtf8 {
    /// Default value for invalid UTF-8 strings.
    Default(String),
    /// Default value for invalid UTF-8 strings, which is a null value.
    Null,
    #[default]
    /// No default value for invalid UTF-8 strings.
    /// This means that the decoder will return an error if it encounters invalid UTF-8.
    /// This is the default behavior.
    None,
}

/// THIS STRUCT IS ARAS ONLY
///
/// Options for column value decoding behavior.
///
/// Contains settings that control how column values are decoded, such as
/// whether to validate decoded values.
///
/// Setting `skip_validation` to true may improve performance but could
/// result in incorrect data if the input is malformed.
#[derive(Debug, Default, Clone)]
pub struct ColumnValueDecoderOptions {
    /// These two are kept separately to lower the cost of maintaining the upstream diff because UnsafeFlag is upstream feature.
    ///
    /// Skip validation of the values read from the column.
    pub skip_validation: UnsafeFlag,
    /// Default value of non-valid UTF-8 strings.
    pub default_value: DefaultValueForInvalidUtf8,
}

impl ColumnValueDecoderOptions {
    /// Create a new `ColumnValueDecoderOptions` with the given `skip_validation` flag.
    pub fn new(skip_validation: UnsafeFlag, default_value: DefaultValueForInvalidUtf8) -> Self {
        if skip_validation.get() && default_value != DefaultValueForInvalidUtf8::None {
            // If skip_validation is set, we should not set a default value
            // for invalid UTF-8 strings. This is because the decoder will
            // not validate the values
            panic!("Invalid setting");
        }

        Self {
            skip_validation,
            default_value,
        }
    }
}
