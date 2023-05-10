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

use crate::{Error, Result};
use std::fmt::{Debug, Display, Formatter};

/// Provides deferred parsing of a value
///
/// This allows builders to defer fallibility to build
#[derive(Debug, Clone)]
pub enum ConfigValue<T> {
    Parsed(T),
    Deferred(String),
}

impl<T: Display> Display for ConfigValue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parsed(v) => write!(f, "{v}"),
            Self::Deferred(v) => write!(f, "{v}"),
        }
    }
}

impl<T> From<T> for ConfigValue<T> {
    fn from(value: T) -> Self {
        Self::Parsed(value)
    }
}

impl<T: Parse + Clone> ConfigValue<T> {
    pub fn parse(&mut self, v: impl Into<String>) {
        *self = Self::Deferred(v.into())
    }

    pub fn get(&self) -> Result<T> {
        match self {
            Self::Parsed(v) => Ok(v.clone()),
            Self::Deferred(v) => T::parse(v),
        }
    }
}

impl<T: Default> Default for ConfigValue<T> {
    fn default() -> Self {
        Self::Parsed(T::default())
    }
}

/// A value that can be stored in [`ConfigValue`]
pub trait Parse: Sized {
    fn parse(v: &str) -> Result<Self>;
}

impl Parse for bool {
    fn parse(v: &str) -> Result<Self> {
        let lower = v.to_ascii_lowercase();
        match lower.as_str() {
            "1" | "true" | "on" | "yes" | "y" => Ok(true),
            "0" | "false" | "off" | "no" | "n" => Ok(false),
            _ => Err(Error::Generic {
                store: "Config",
                source: format!("failed to parse \"{v}\" as boolean").into(),
            }),
        }
    }
}
