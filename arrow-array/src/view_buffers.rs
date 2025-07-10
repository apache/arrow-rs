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

use std::{ops::Deref, sync::Arc};

use arrow_buffer::Buffer;

/// A cheaply cloneable, owned slice of [`Buffer`]
///
/// Similar to `Arc<Vec<Buffer>>` or `Arc<[Buffer]>`
#[derive(Clone, Debug)]
pub struct ViewBuffers(Arc<Vec<Buffer>>);

impl ViewBuffers {
    /// Return a mutable reference to the underlying buffers, copying the buffers if necessary.
    pub fn make_mut(&mut self) -> &mut Vec<Buffer> {
        // If the underlying Arc is unique, we can mutate it in place
        Arc::make_mut(&mut self.0)
    }

    /// Convertes this ViewBuffers into a Vec<Buffer>, cloning the underlying buffers if
    /// they are shared.
    pub fn unwrap_or_clone(self) -> Vec<Buffer> {
        Arc::unwrap_or_clone(self.0)
    }
}

impl FromIterator<Buffer> for ViewBuffers {
    fn from_iter<T: IntoIterator<Item = Buffer>>(iter: T) -> Self {
        let v: Vec<_> = iter.into_iter().collect();
        Self(v.into())
    }
}

impl From<Vec<Buffer>> for ViewBuffers {
    fn from(value: Vec<Buffer>) -> Self {
        Self(value.into())
    }
}

impl Deref for ViewBuffers {
    type Target = [Buffer];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
