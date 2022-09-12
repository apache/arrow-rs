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

//! Defines `ArrowSchemaError` for representing failures in arrow schema

use std::error::Error;

#[derive(Debug)]
pub enum ArrowSchemaError {
    Parse(String),
    Merge(String),
    Field(String),
}

impl std::fmt::Display for ArrowSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowSchemaError::Parse(message) => {
                write!(f, "Error parsing schema: {}", message)
            }
            ArrowSchemaError::Merge(message) => {
                write!(f, "Error merging schema: {}", message)
            }
            ArrowSchemaError::Field(message) => {
                write!(f, "Error indexing field: {}", message)
            }
        }
    }
}

impl Error for ArrowSchemaError {}
