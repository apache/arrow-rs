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

use std::sync::Arc;

use arrow_buffer::Buffer;

/// A cheaply cloneable, owned slice of [`Buffer`]
///
/// Similar to `Arc<Vec<Buffer>>` or `Arc<[Buffer]>`
#[derive(Clone, Debug)]
pub struct ViewBuffers(pub(crate) Arc<[Buffer]>);

impl FromIterator<Buffer> for ViewBuffers {
    fn from_iter<T: IntoIterator<Item = Buffer>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Buffer>> for ViewBuffers {
    fn from(value: Vec<Buffer>) -> Self {
        Self(value.into())
    }
}

impl From<&[Buffer]> for ViewBuffers {
    fn from(value: &[Buffer]) -> Self {
        Self(value.into())
    }
}
