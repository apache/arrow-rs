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

const DEFAULT_DEDUP_CAPACITY: usize = 4096;

/// Storage trait for [`Interner`]
pub trait Storage {
    type Key: Copy;

    type Value: AsBytes + PartialEq + ?Sized;

    /// Gets an element by its key
    fn get(&self, idx: Self::Key) -> &Self::Value;

    /// Adds a new element, returning the key
    fn push(&mut self, value: &Self::Value) -> Self::Key;

    /// Return an estimate of the memory used in this storage, in bytes
    #[allow(dead_code)] // not used in parquet_derive, so is dead there
    fn estimated_memory_size(&self) -> usize;
}

/// A generic value interner supporting various different [`Storage`]
#[derive(Debug, Default)]
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
            dedup: HashMap::with_capacity_and_hasher(DEFAULT_DEDUP_CAPACITY, ()),
            storage,
        }
    }

    /// Intern the value, returning the interned key, and if this was a new value
    pub fn intern(&mut self, value: &S::Value) -> S::Key {
        let hash = self.state.hash_one(value.as_bytes());

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
                        self.state.hash_one(self.storage.get(*key).as_bytes())
                    })
                    .0
            }
        }
    }

    /// Return estimate of the memory used, in bytes
    #[allow(dead_code)] // not used in parquet_derive, so is dead there
    pub fn estimated_memory_size(&self) -> usize {
        self.storage.estimated_memory_size() +
            // estimate size of dedup hashmap as just th size of the keys
            self.dedup.capacity() + std::mem::size_of::<S::Key>()
    }

    /// Returns the storage for this interner
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Unwraps the inner storage
    #[cfg(feature = "arrow")]
    pub fn into_inner(self) -> S {
        self.storage
    }
}
