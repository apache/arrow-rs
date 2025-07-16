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
use std::{borrow::Cow, ops::Deref};

/// Represents a qualified path to a potential subfield or index of a variant value.
#[derive(Debug, Clone)]
pub struct VariantPath<'a>(Vec<VariantPathElement<'a>>);

impl<'a> VariantPath<'a> {
    pub fn new(path: Vec<VariantPathElement<'a>>) -> Self {
        Self(path)
    }

    pub fn path(&self) -> &Vec<VariantPathElement> {
        &self.0
    }
}

impl<'a> From<Vec<VariantPathElement<'a>>> for VariantPath<'a> {
    fn from(value: Vec<VariantPathElement<'a>>) -> Self {
        Self::new(value)
    }
}

impl<'a> Deref for VariantPath<'a> {
    type Target = [VariantPathElement<'a>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Element of a path
#[derive(Debug, Clone)]
pub enum VariantPathElement<'a> {
    /// Access field with name `name`
    Field { name: Cow<'a, str> },
    /// Access the list element at `index`
    Index { index: usize },
}

impl<'a> VariantPathElement<'a> {
    pub fn field(name: Cow<'a, str>) -> VariantPathElement<'a> {
        VariantPathElement::Field { name }
    }

    pub fn index(index: usize) -> VariantPathElement<'a> {
        VariantPathElement::Index { index }
    }
}
