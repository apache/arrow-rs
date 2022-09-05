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

use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use std::cmp::Ordering;
use std::ops::Index;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Interned(u32);

/// A byte array interner that generates normalized keys that are sorted with respect
/// to the interned values, e.g. `inter(a) < intern(b) => a < b`
#[derive(Debug, Default)]
pub struct OrderPreservingInterner {
    keys: InternBuffer,
    values: InternBuffer,
    bucket: Box<Bucket>,

    hasher: ahash::RandomState,
    lookup: HashMap<Interned, (), ()>,
}

impl OrderPreservingInterner {
    /// Interns an iterator of values returning a list of [`Interned`] which can be
    /// used with [`Self::normalized_key`] to retrieve the normalized keys with a
    /// lifetime not tied to the mutable borrow passed to this method
    pub fn intern<I, V>(&mut self, input: I) -> Vec<Interned>
    where
        I: IntoIterator<Item = V>,
        V: AsRef<[u8]>,
    {
        let iter = input.into_iter();
        let capacity = iter.size_hint().0;
        let mut out = Vec::with_capacity(capacity);
        let mut to_intern: Vec<(usize, u64, V)> = Vec::with_capacity(capacity);
        let mut to_intern_len = 0;

        for (idx, value) in iter.enumerate() {
            let v = value.as_ref();
            let hash = self.hasher.hash_one(v);
            let entry = self
                .lookup
                .raw_entry_mut()
                .from_hash(hash, |a| &self.values[*a] == v);

            match entry {
                RawEntryMut::Occupied(o) => out.push(*o.key()),
                RawEntryMut::Vacant(_) => {
                    // Push placeholder
                    out.push(Interned(0));
                    to_intern_len += v.len();
                    to_intern.push((idx, hash, value));
                }
            };
        }

        to_intern.sort_unstable_by(|(_, _, a), (_, _, b)| a.as_ref().cmp(b.as_ref()));

        self.keys.offsets.reserve(to_intern.len());
        self.keys.values.reserve(to_intern.len()); // Approximation
        self.values.offsets.reserve(to_intern.len());
        self.values.values.reserve(to_intern_len);

        for (idx, hash, value) in to_intern {
            let val = value.as_ref();

            let entry = self
                .lookup
                .raw_entry_mut()
                .from_hash(hash, |a| &self.values[*a] == val);

            match entry {
                RawEntryMut::Occupied(o) => {
                    out[idx] = *o.key();
                }
                RawEntryMut::Vacant(v) => {
                    let val = value.as_ref();
                    self.bucket
                        .insert(&mut self.values, val, &mut self.keys.values);
                    self.keys.values.push(0);
                    let interned = self.keys.append();

                    let hasher = &mut self.hasher;
                    let values = &self.values;
                    v.insert_with_hasher(hash, interned, (), |key| {
                        hasher.hash_one(&values[*key])
                    });
                    out[idx] = interned;
                }
            }
        }

        out
    }

    /// Returns a null-terminated byte array that can be compared against other normalized_key
    /// returned by this instance, to establish ordering of the interned values
    pub fn normalized_key(&self, key: Interned) -> &[u8] {
        &self.keys[key]
    }
}

/// A buffer of `[u8]` indexed by `[Interned]`
#[derive(Debug)]
struct InternBuffer {
    values: Vec<u8>,
    offsets: Vec<usize>,
}

impl Default for InternBuffer {
    fn default() -> Self {
        Self {
            values: Default::default(),
            offsets: vec![0],
        }
    }
}

impl InternBuffer {
    /// Insert `data` returning the corresponding [`Interned`]
    fn insert(&mut self, data: &[u8]) -> Interned {
        self.values.extend_from_slice(data);
        self.append()
    }

    /// Appends the next value based on data written to `self.values`
    /// returning the corresponding [`Interned`]
    fn append(&mut self) -> Interned {
        let idx = self.offsets.len() - 1;
        let key = Interned(idx.try_into().unwrap());
        self.offsets.push(self.values.len());
        key
    }
}

impl Index<Interned> for InternBuffer {
    type Output = [u8];

    fn index(&self, key: Interned) -> &Self::Output {
        let index = key.0 as usize;
        let end = self.offsets[index + 1];
        let start = self.offsets[index];
        unsafe { self.values.get_unchecked(start..end) }
    }
}

/// A slot corresponds to a single byte-value in the generated normalized key
///
/// It may contain a value, if not the first slot, and may contain a child [`Bucket`] representing
/// the next byte in the generated normalized key
#[derive(Debug, Default, Clone)]
struct Slot {
    value: Option<Interned>,
    /// Child values smaller than `self.value` if any
    child: Option<Box<Bucket>>,
}

/// Each bucket corresponds to a single byte in the normalized key
#[derive(Debug, Clone)]
struct Bucket {
    slots: Box<[Slot]>,
}

impl Default for Bucket {
    fn default() -> Self {
        let slots = (0..255).map(|_| Slot::default()).collect::<Vec<_>>().into();
        Self { slots }
    }
}

impl Bucket {
    /// Perform a skewed binary search to find the first slot that is empty or less
    ///
    /// Returns `Ok(idx)` if an exact match is found, otherwise returns `Err(idx)`
    /// containing the slot index to insert at
    fn insert_pos(&self, values_buf: &InternBuffer, data: &[u8]) -> Result<usize, usize> {
        let mut size = self.slots.len() - 1;
        let mut left = 0;
        let mut right = size;
        while left < right {
            // Skew binary search to leave gaps of at most 3 elements
            let mid = left + (size / 2).min(3);

            let slot = &self.slots[mid];
            let val = match slot.value {
                Some(val) => val,
                None => return Err(mid),
            };

            let cmp = values_buf[val].cmp(data);
            if cmp == Ordering::Less {
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                right = mid;
            } else {
                return Ok(mid);
            }

            size = right - left;
        }
        Err(left)
    }

    /// Insert `data` into this bucket or one of its children, appending the
    /// normalized key to `out` as it is constructed
    ///
    /// # Panics
    ///
    /// Panics if the value already exists
    fn insert(&mut self, values_buf: &mut InternBuffer, data: &[u8], out: &mut Vec<u8>) {
        match self.insert_pos(values_buf, data) {
            Ok(_) => unreachable!("value already exists"),
            Err(idx) => {
                let slot = &mut self.slots[idx];
                // Must always spill final slot so can always create a value less than
                if idx != 254 && slot.value.is_none() {
                    out.push(idx as u8 + 2);
                    slot.value = Some(values_buf.insert(data))
                } else {
                    out.push(idx as u8 + 1);
                    slot.child
                        .get_or_insert_with(Default::default)
                        .insert(values_buf, data, out);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    // Clippy isn't smart enough to understand dropping mutability
    #[allow(clippy::needless_collect)]
    fn test_intern_values(values: &[u64]) {
        let mut interner = OrderPreservingInterner::default();

        // Intern a single value at a time to check ordering
        let interned: Vec<_> = values
            .iter()
            .flat_map(|v| interner.intern([&v.to_be_bytes()]))
            .collect();

        let interned: Vec<_> = interned
            .into_iter()
            .map(|x| interner.normalized_key(x))
            .collect();

        for (i, a) in interned.iter().enumerate() {
            for (j, b) in interned.iter().enumerate() {
                let interned_cmp = a.cmp(b);
                let values_cmp = values[i].cmp(&values[j]);
                assert_eq!(
                    interned_cmp, values_cmp,
                    "({:?} vs {:?}) vs ({} vs {})",
                    a, b, values[i], values[j]
                )
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_interner() {
        test_intern_values(&[8, 6, 5, 7]);

        let mut values: Vec<_> = (0_u64..2000).collect();
        test_intern_values(&values);

        let mut rng = thread_rng();
        values.shuffle(&mut rng);
        test_intern_values(&values);
    }

    #[test]
    fn test_intern_duplicates() {
        // Unsorted with duplicates
        let values = vec![0_u8, 1, 8, 4, 1, 0];
        let mut interner = OrderPreservingInterner::default();

        let interned = interner.intern(values.iter().map(std::slice::from_ref));

        assert_eq!(interned[0], interned[5]);
        assert_eq!(interned[1], interned[4]);
        assert!(
            interner.normalized_key(interned[0]) < interner.normalized_key(interned[1])
        );
        assert!(
            interner.normalized_key(interned[1]) < interner.normalized_key(interned[2])
        );
        assert!(
            interner.normalized_key(interned[1]) < interner.normalized_key(interned[3])
        );
        assert!(
            interner.normalized_key(interned[3]) < interner.normalized_key(interned[2])
        );
    }
}
