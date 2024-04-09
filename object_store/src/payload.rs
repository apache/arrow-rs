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
            self.clone().into_iter().map(Ok::<_, crate::Error>),
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

impl AsRef<[Bytes]> for PutPayload {
    fn as_ref(&self) -> &[Bytes] {
        self.0.as_ref()
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

/// A builder for [`PutPayload`] that allocates memory in chunks
#[derive(Debug)]
pub struct PutPayloadMut {
    len: usize,
    completed: Vec<Bytes>,
    in_progress: Vec<u8>,
    min_alloc: usize,
}

impl Default for PutPayloadMut {
    fn default() -> Self {
        Self {
            len: 0,
            completed: vec![],
            in_progress: vec![],

            min_alloc: 8 * 1024,
        }
    }
}

impl PutPayloadMut {
    /// Create a new [`PutPayloadMut`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the minimum allocation size
    ///
    /// Defaults to 8KB
    pub fn with_minimum_allocation_size(self, min_alloc: usize) -> Self {
        Self { min_alloc, ..self }
    }

    /// Write bytes into this [`PutPayloadMut`]
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        let remaining = self.in_progress.capacity() - self.in_progress.len();
        let to_copy = remaining.min(slice.len());

        self.in_progress.extend_from_slice(&slice[..to_copy]);
        if self.in_progress.capacity() == self.in_progress.len() {
            let new_cap = self.min_alloc.max(slice.len() - to_copy);
            let completed = std::mem::replace(&mut self.in_progress, Vec::with_capacity(new_cap));
            if !completed.is_empty() {
                self.completed.push(completed.into())
            }
            self.in_progress.extend_from_slice(&slice[to_copy..])
        }
        self.len += slice.len();
    }

    /// Append a [`Bytes`] to this [`PutPayloadMut`]
    pub fn push(&mut self, bytes: Bytes) {
        if !self.in_progress.is_empty() {
            let completed = std::mem::take(&mut self.in_progress);
            self.completed.push(completed.into())
        }
        self.completed.push(bytes)
    }

    /// Returns `true` if this [`PutPayloadMut`] contains no bytes
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the total length of the [`Bytes`] in this payload
    #[inline]
    pub fn content_length(&self) -> usize {
        self.len
    }

    /// Convert into [`PutPayload`]
    pub fn freeze(mut self) -> PutPayload {
        if !self.in_progress.is_empty() {
            let completed = std::mem::take(&mut self.in_progress).into();
            self.completed.push(completed);
        }
        PutPayload(self.completed.into())
    }
}

impl From<PutPayloadMut> for PutPayload {
    fn from(value: PutPayloadMut) -> Self {
        value.freeze()
    }
}

#[cfg(test)]
mod test {
    use crate::PutPayloadMut;

    #[test]
    fn test_put_payload() {
        let mut chunk = PutPayloadMut::new().with_minimum_allocation_size(23);
        chunk.extend_from_slice(&[1; 16]);
        chunk.extend_from_slice(&[2; 32]);
        chunk.extend_from_slice(&[2; 5]);
        chunk.extend_from_slice(&[2; 21]);
        chunk.extend_from_slice(&[2; 40]);
        let payload = chunk.freeze();
        assert_eq!(payload.content_length(), 114);

        let chunks = payload.as_ref();
        assert_eq!(chunks.len(), 5);

        assert_eq!(chunks[0].len(), 23);
        assert_eq!(chunks[1].len(), 25); // 32 - (23 - 16)
        assert_eq!(chunks[2].len(), 23);
        assert_eq!(chunks[3].len(), 23);
        assert_eq!(chunks[4].len(), 20);
    }
}
