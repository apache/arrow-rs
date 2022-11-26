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

use crate::bit_iterator::BitSliceIterator;
use arrow_buffer::Buffer;

/// An iterator of `(usize, usize)` each representing an interval
/// `[start, end)` whose slots of a bitmap [Buffer] are true. Each
/// interval corresponds to a contiguous region of memory to be
/// "taken" from an array to be filtered.
///
/// ## Notes:
///
/// Only performant for filters that copy across long contiguous runs
#[derive(Debug)]
pub struct SlicesIterator<'a>(BitSliceIterator<'a>);

impl<'a> SlicesIterator<'a> {
    pub fn new_from_buffer(values: &'a Buffer, offset: usize, len: usize) -> Self {
        Self(BitSliceIterator::new(values, offset, len))
    }
}

impl<'a> Iterator for SlicesIterator<'a> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
