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

use crate::{Field, FieldRef};
use std::ops::Deref;
use std::sync::Arc;

/// A cheaply cloneable, owned slice of [`FieldRef`]
///
/// Similar to `Arc<Vec<FieldPtr>>` or `Arc<[FieldPtr]>`
///
/// Can be constructed in a number of ways
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, Fields};
/// // Can be constructed from Vec<Field>
/// Fields::from(vec![Field::new("a", DataType::Boolean, false)]);
/// // Can be constructed from Vec<FieldRef>
/// Fields::from(vec![Arc::new(Field::new("a", DataType::Boolean, false))]);
/// // Can be constructed from an iterator of Field
/// std::iter::once(Field::new("a", DataType::Boolean, false)).collect::<Fields>();
/// // Can be constructed from an iterator of FieldRef
/// std::iter::once(Arc::new(Field::new("a", DataType::Boolean, false))).collect::<Fields>();
/// ```
///
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Fields(Arc<[FieldRef]>);

impl std::fmt::Debug for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl Fields {
    /// Returns a new empty [`Fields`]
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }

    /// Return size of this instance in bytes.
    pub fn size(&self) -> usize {
        self.iter().map(|field| field.size()).sum()
    }

    /// Searches for a field by name, returning it along with its index if found
    pub fn find(&self, name: &str) -> Option<(usize, &FieldRef)> {
        self.0.iter().enumerate().find(|(_, b)| b.name() == name)
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Field>> for Fields {
    fn from(value: Vec<Field>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(value: Vec<FieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<&[FieldRef]> for Fields {
    fn from(value: &[FieldRef]) -> Self {
        Self(value.into())
    }
}

impl<const N: usize> From<[FieldRef; N]> for Fields {
    fn from(value: [FieldRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for Fields {
    type Target = [FieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = &'a FieldRef;
    type IntoIter = std::slice::Iter<'a, FieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

// Manually implement to avoid needing serde rc feature
#[cfg(feature = "serde")]
impl serde::Serialize for Fields {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for e in self.iter() {
            seq.serialize_element(e.as_ref())?;
        }
        seq.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Fields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Vec::<Field>::deserialize(deserializer)?.into())
    }
}
