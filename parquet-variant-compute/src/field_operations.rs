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

//! Field extraction and removal operations for variant objects
//! 
//! NOTE: Most functionality in this module has been superseded by the high-level
//! Variant API (variant.as_object(), variant.get_object_field(), etc.).
//! For new code, prefer using the high-level API over these low-level operations.

// This module is mostly empty now - the manual field operations have been
// replaced by high-level Variant API usage. See variant_array.rs for examples
// of how field removal is now implemented using VariantBuilder.

/// Field operations for variant objects
pub struct FieldOperations;

// Note: This struct is kept for backwards compatibility but most methods
// have been removed in favor of high-level Variant API usage.
impl FieldOperations {
    // All manual field manipulation methods have been removed.
    // Use the high-level Variant API instead:
    // - variant.get_object_field(name) instead of extract_field_bytes()
    // - variant.get_list_element(index) instead of array element extraction
    // - variant.get_path(&path) instead of get_path_bytes()
    // - VariantBuilder::new_object() instead of manual object reconstruction
}

#[cfg(test)]
mod tests {
    // Tests have been removed since the functions they tested are no longer needed.
    // Field operations are now tested as part of variant_array.rs tests.
}


