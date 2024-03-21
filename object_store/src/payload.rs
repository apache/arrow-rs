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

use crate::Error;
use bytes::Bytes;
use std::sync::Arc;

/// A cheaply cloneable, ordered collection of [`Bytes`]
#[derive(Debug, Clone)]
pub struct PutPayload(Arc<[Bytes]>);

impl Default for PutPayload {
    fn default() -> Self {
        Self(Arc::new([]))
    }
}

impl PutPayload {
    /// Create a new empty [`PutPayload`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a [`PutPayload`] from a static slice
    pub fn from_static(s: &'static [u8]) -> Self {
        s.into()
    }

    /// Creates a [`PutPayload`] from a [`Bytes`]
    pub fn from_bytes(s: Bytes) -> Self {
        s.into()
    }

    #[cfg(feature = "cloud")]
    pub(crate) fn body(&self) -> reqwest::Body {
        reqwest::Body::wrap_stream(futures::stream::iter(
            self.clone().into_iter().map(Ok::<_, Error>),
        ))
    }

    /// Returns the total length of the [`Bytes`] in this payload
    pub fn content_length(&self) -> usize {
        self.0.iter().map(|b| b.len()).sum()
    }

    /// Returns an iterator over the [`Bytes`] in this payload
    pub fn iter(&self) -> PutPayloadIter<'_> {
        PutPayloadIter(self.0.iter())
    }
}

impl<'a> IntoIterator for &'a PutPayload {
    type Item = &'a Bytes;
    type IntoIter = PutPayloadIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for PutPayload {
    type Item = Bytes;
    type IntoIter = PutPayloadIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        PutPayloadIntoIter {
            payload: self,
            idx: 0,
        }
    }
}

/// An iterator over [`PutPayload`]
#[derive(Debug)]
pub struct PutPayloadIter<'a>(std::slice::Iter<'a, Bytes>);

impl<'a> Iterator for PutPayloadIter<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// An owning iterator of [`PutPayload`]
#[derive(Debug)]
pub struct PutPayloadIntoIter {
    payload: PutPayload,
    idx: usize,
}

impl Iterator for PutPayloadIntoIter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let p = self.payload.0.get(self.idx)?.clone();
        self.idx += 1;
        Some(p)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = self.payload.0.len() - self.idx;
        (l, Some(l))
    }
}

impl From<Bytes> for PutPayload {
    fn from(value: Bytes) -> Self {
        Self(Arc::new([value]))
    }
}

impl From<Vec<u8>> for PutPayload {
    fn from(value: Vec<u8>) -> Self {
        Self(Arc::new([value.into()]))
    }
}

impl From<&'static str> for PutPayload {
    fn from(value: &'static str) -> Self {
        Bytes::from(value).into()
    }
}

impl From<&'static [u8]> for PutPayload {
    fn from(value: &'static [u8]) -> Self {
        Bytes::from(value).into()
    }
}

impl From<String> for PutPayload {
    fn from(value: String) -> Self {
        Bytes::from(value).into()
    }
}

impl FromIterator<u8> for PutPayload {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        // TODO Use PutPayloadMut to avoid bump allocating
        Bytes::from_iter(iter).into()
    }
}

impl FromIterator<Bytes> for PutPayload {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<PutPayload> for Bytes {
    fn from(value: PutPayload) -> Self {
        match value.0.len() {
            0 => Self::new(),
            1 => value.0[0].clone(),
            _ => {
                let mut buf = Vec::with_capacity(value.content_length());
                value.iter().for_each(|x| buf.extend_from_slice(x));
                buf.into()
            }
        }
    }
}
