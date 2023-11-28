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

//! Defines the logical data types of Arrow arrays.
//!
//! The most important things you might be looking for are:
//!  * [`Schema`] to describe a schema.
//!  * [`Field`] to describe one field within a schema.
//!  * [`DataType`] to describe the type of a field.

pub use arrow_array::types::*;
pub use arrow_array::{ArrowNativeTypeOp, ArrowNumericType, ArrowPrimitiveType};
pub use arrow_buffer::{i256, ArrowNativeType, ToByteSlice};
pub use arrow_data::decimal::*;
pub use arrow_schema::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaBuilder, SchemaRef, TimeUnit,
    UnionFields, UnionMode,
};
