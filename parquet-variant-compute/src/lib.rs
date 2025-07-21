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

//! Parquet variant compute functions

pub mod from_json;
pub mod to_json;
pub mod variant_array;
pub mod variant_array_builder;
pub mod variant_get;

pub use from_json::batch_json_string_to_variant;
pub use to_json::batch_variant_to_json_string;
pub use variant_array::VariantArray;
pub use variant_array_builder::VariantArrayBuilder;
pub use variant_get::{variant_get, GetOptions};
