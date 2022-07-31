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

use crate::data_type::AsBytes;
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use std::hash::Hash;

/// Storage trait for [`Interner`]
pub trait Storage {
    type Key: Copy;

    type Value: AsBytes + PartialEq + ?Sized;

    /// Gets an element by its key
    fn get(&self, idx: Self::Key) -> &Self::Value;

    /// Adds a new element, returning the key
    fn push(&mut self, value: &Self::Value) -> Self::Key;
}

/// A generic value interner supporting various different [`Storage`]
pub struct Interner<S: Storage> {
    state: ahash::RandomState,

    /// Used to provide a lookup from value to unique value
    ///
    /// Note: `S::Key`'s hash implementation is not used, instead the raw entry
    /// API is used to store keys w.r.t the hash of the strings themselves
    ///
    dedup: HashMap<S::Key, (), ()>,

    storage: S,
}

impl<S: Storage> Interner<S> {
    /// Create a new `Interner` with the provided storage
    pub fn new(storage: S) -> Self {
        Self {
            state: Default::default(),
            dedup: Default::default(),
            storage,
        }
    }

    /// Intern the value, returning the interned key, and if this was a new value
    pub fn intern(&mut self, value: &S::Value) -> S::Key {
        let hash = compute_hash(&self.state, value);

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |index| value == self.storage.get(*index));

        match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let key = self.storage.push(value);

                *entry
                    .insert_with_hasher(hash, key, (), |key| {
                        compute_hash(&self.state, self.storage.get(*key))
                    })
                    .0
            }
        }
    }

    /// Returns the storage for this interner
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

fn compute_hash<T: AsBytes + ?Sized>(state: &ahash::RandomState, value: &T) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = state.build_hasher();
    value.as_bytes().hash(&mut hasher);
    hasher.finish()
}
