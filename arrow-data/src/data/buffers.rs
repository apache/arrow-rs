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

use arrow_buffer::Buffer;
use std::iter::Chain;
use std::ops::Index;

/// A collection of [`Buffer`]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub struct Buffers<'a>([Option<&'a Buffer>; 2]);

impl<'a> Buffers<'a> {
    /// Temporary will be removed once ArrayData does not store `Vec<Buffer>` directly (#3769)
    pub(crate) fn from_slice(a: &'a [Buffer]) -> Self {
        match a.len() {
            0 => Self([None, None]),
            1 => Self([Some(&a[0]), None]),
            _ => Self([Some(&a[0]), Some(&a[1])]),
        }
    }

    /// Returns the number of [`Buffer`] in this collection
    #[inline]
    pub fn len(&self) -> usize {
        self.0[0].is_some() as usize + self.0[1].is_some() as usize
    }

    /// Returns `true` if this collection is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0[0].is_none() && self.0[1].is_none()
    }

    #[inline]
    pub fn iter(&self) -> IntoIter<'a> {
        self.into_iter()
    }

    /// Converts this [`Buffers`] to a `Vec<Buffer>`
    #[inline]
    pub fn to_vec(&self) -> Vec<Buffer> {
        self.iter().cloned().collect()
    }
}

impl<'a> Index<usize> for Buffers<'a> {
    type Output = &'a Buffer;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.0[index].as_ref().unwrap()
    }
}

impl<'a> IntoIterator for Buffers<'a> {
    type Item = &'a Buffer;
    type IntoIter = IntoIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.0[0].into_iter().chain(self.0[1].into_iter()))
    }
}

type OptionIter<'a> = std::option::IntoIter<&'a Buffer>;

/// [`Iterator`] for [`Buffers`]
pub struct IntoIter<'a>(Chain<OptionIter<'a>, OptionIter<'a>>);

impl<'a> Iterator for IntoIter<'a> {
    type Item = &'a Buffer;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
