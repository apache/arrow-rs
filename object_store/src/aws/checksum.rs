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

use crate::config::Parse;
use ring::digest::{self, digest as ring_digest};
use std::str::FromStr;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enum representing checksum algorithm supported by S3.
pub enum Checksum {
    /// SHA-256 algorithm.
    SHA256,
}

impl Checksum {
    pub(super) fn digest(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Self::SHA256 => ring_digest(&digest::SHA256, bytes).as_ref().to_owned(),
        }
    }

    pub(super) fn header_name(&self) -> &'static str {
        match self {
            Self::SHA256 => "x-amz-checksum-sha256",
        }
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::SHA256 => write!(f, "sha256"),
        }
    }
}

impl FromStr for Checksum {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sha256" => Ok(Self::SHA256),
            _ => Err(()),
        }
    }
}

impl TryFrom<&String> for Checksum {
    type Error = ();

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl Parse for Checksum {
    fn parse(v: &str) -> crate::Result<Self> {
        v.parse().map_err(|_| crate::Error::Generic {
            store: "Config",
            source: format!("\"{v}\" is not a valid checksum algorithm").into(),
        })
    }
}
