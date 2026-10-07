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
use hashbrown::HashTable;

const DEFAULT_DEDUP_CAPACITY: usize = 4096;

/// Storage trait for [`Interner`]
pub trait Storage {
    type Key: Copy;

    type Value: AsBytes + ?Sized;

    /// Gets an element by its key
    fn get(&self, idx: Self::Key) -> &Self::Value;

    /// Adds a new element, returning the key
    fn push(&mut self, value: &Self::Value) -> Self::Key;

    /// Return an estimate of the memory used in this storage, in bytes
    fn estimated_memory_size(&self) -> usize;
}

/// A generic value interner supporting various different [`Storage`]
///
/// Each entry of the hash table caches the hash of its value, so growing the
/// table never hashes stored values again, and a probe only compares the bytes
/// of values whose full hash matches.
#[derive(Debug, Default)]
pub struct Interner<S: Storage> {
    state: ahash::RandomState,

    /// Used to provide a lookup from value to unique value, with its hash
    dedup: HashTable<(S::Key, u64)>,

    storage: S,
}

impl<S: Storage> Interner<S> {
    /// Create a new `Interner` with the provided storage
    pub fn new(storage: S) -> Self {
        Self {
            state: Default::default(),
            dedup: HashTable::with_capacity(DEFAULT_DEDUP_CAPACITY),
            storage,
        }
    }

    /// Intern each of `values`, appending their keys to `keys`
    pub fn intern_batch(&mut self, values: &[S::Value], keys: &mut Vec<S::Key>)
    where
        S::Value: Sized,
    {
        keys.reserve(values.len());
        for value in values {
            let hash = hash_bytes(&self.state, value.as_bytes());
            keys.push(self.intern_hashed(value, hash));
        }
    }

    #[inline]
    fn intern_hashed(&mut self, value: &S::Value, hash: u64) -> S::Key {
        let existing = self.dedup.find(hash, |(key, key_hash)| {
            // Compare bytes rather than directly comparing values so NaNs can be interned
            *key_hash == hash && value.as_bytes() == self.storage.get(*key).as_bytes()
        });
        match existing {
            Some((key, _)) => *key,
            None => {
                let key = self.storage.push(value);
                if self.dedup.len() == self.dedup.capacity() {
                    // Grow 4x rather than hashbrown's 2x: high cardinality
                    // columns keep growing until the dictionary falls back,
                    // and every growth moves every entry to cold memory
                    self.dedup
                        .reserve(self.dedup.len() * 3, |(_, key_hash)| *key_hash);
                }
                self.dedup
                    .insert_unique(hash, (key, hash), |(_, key_hash)| *key_hash);
                key
            }
        }
    }

    /// Return estimate of the memory used, in bytes
    pub fn estimated_memory_size(&self) -> usize {
        self.storage.estimated_memory_size() + self.dedup.allocation_size()
    }

    /// Returns the storage for this interner
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

/// Hashes the bytes of a value
///
/// Fixed width values, whose length is known once inlined, are hashed as a
/// single integer, which is far cheaper than hashing a byte slice.
#[inline(always)]
fn hash_bytes(state: &ahash::RandomState, bytes: &[u8]) -> u64 {
    match bytes.len() {
        4 => state.hash_one(u32::from_ne_bytes(bytes.try_into().unwrap())),
        8 => state.hash_one(u64::from_ne_bytes(bytes.try_into().unwrap())),
        _ => state.hash_one(bytes),
    }
}
