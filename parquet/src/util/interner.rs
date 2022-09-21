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
use hashbrown::raw::RawTable;

const DEFAULT_DEDUP_CAPACITY: usize = 4096;

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
#[derive(Default)]
pub struct Interner<S: Storage> {
    state: ahash::RandomState,

    /// Used to provide a lookup from `S::Value` to `S::Key`
    dedup: RawTable<S::Key>,

    storage: S,
}

impl<S: Storage + std::fmt::Debug> std::fmt::Debug for Interner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Interner").field(&self.storage).finish()
    }
}

impl<S: Storage> Interner<S> {
    /// Create a new `Interner` with the provided storage
    pub fn new(storage: S) -> Self {
        Self {
            state: Default::default(),
            dedup: RawTable::with_capacity(DEFAULT_DEDUP_CAPACITY),
            storage,
        }
    }

    /// Intern the value, returning the interned key, and if this was a new value
    pub fn intern(&mut self, value: &S::Value) -> S::Key {
        let hash = self.state.hash_one(value.as_bytes());

        let maybe_key = self
            .dedup
            .get(hash, |index| value == self.storage.get(*index));

        match maybe_key {
            Some(key) => *key,
            None => {
                let key = self.storage.push(value);
                self.dedup.insert(hash, key, |key| {
                    self.state.hash_one(self.storage.get(*key).as_bytes())
                });
                key
            }
        }
    }

    /// Returns the storage for this interner
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Unwraps the inner storage
    pub fn into_inner(self) -> S {
        self.storage
    }
}
