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

//! The list response format used by GCP and AWS

use crate::path::Path;
use crate::{ListResult, ObjectMeta, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

impl TryFrom<ListResponse> for ListResult {
    type Error = crate::Error;

    fn try_from(value: ListResponse) -> Result<Self> {
        let common_prefixes = value
            .common_prefixes
            .into_iter()
            .map(|x| Ok(Path::parse(x.prefix)?))
            .collect::<Result<_>>()?;

        let objects = value
            .contents
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_>>()?;

        Ok(Self {
            common_prefixes,
            objects,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<String>,
}

impl TryFrom<ListContents> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: ListContents) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.key)?,
            last_modified: value.last_modified,
            size: value.size,
            e_tag: value.e_tag,
        })
    }
}
