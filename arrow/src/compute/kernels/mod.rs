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

//! Computation kernels on Arrow Arrays

pub mod aggregate;
pub mod arithmetic;
pub mod arity;
pub mod bitwise;
pub mod boolean;
pub mod cast;
pub mod cast_utils;
pub mod comparison;
pub mod concat;
pub mod concat_elements;
pub mod filter;
pub mod length;
pub mod limit;
pub mod partition;
pub mod regexp;
pub mod sort;
pub mod substring;
pub mod take;
pub mod temporal;
pub mod window;
pub mod zip;
