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
#[derive(Debug, Default)]
pub struct Interner<S: Storage> {
    state: ahash::RandomState,

    /// Used to provide a lookup from value to unique value
    dedup: HashTable<S::Key>,

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

    /// Intern the value, returning the interned key, and if this was a new value
    #[inline(always)]
    pub fn intern(&mut self, value: &S::Value) -> S::Key {
        let hash = self.state.hash_one(value.as_bytes());

        *self
            .dedup
            .entry(
                hash,
                // Compare bytes rather than directly comparing values so NaNs can be interned
                |index| value.as_bytes() == self.storage.get(*index).as_bytes(),
                |key| self.state.hash_one(self.storage.get(*key).as_bytes()),
            )
            .or_insert_with(|| self.storage.push(value))
            .get()
    }

    /// Like [`Self::intern`], but keyed by the value's raw bytes — the owned
    /// `S::Value` is built (via `make`) **only on a dedup miss**. Lets callers
    /// whose owned value is expensive to construct (e.g. a heap-backed
    /// `FixedLenByteArray`) pay that cost once per *unique* value instead of once
    /// per occurrence; on a hit nothing is allocated, only the bytes are hashed
    /// and compared.
    #[inline(always)]
    pub fn intern_bytes(&mut self, bytes: &[u8], make: impl FnOnce() -> S::Value) -> S::Key
    where
        S::Value: Sized,
    {
        let hash = self.state.hash_one(bytes);

        *self
            .dedup
            .entry(
                hash,
                |index| bytes == self.storage.get(*index).as_bytes(),
                |key| self.state.hash_one(self.storage.get(*key).as_bytes()),
            )
            .or_insert_with(|| self.storage.push(&make()))
            .get()
    }

    /// Return estimate of the memory used, in bytes
    pub fn estimated_memory_size(&self) -> usize {
        self.storage.estimated_memory_size() + self.dedup.allocation_size()
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::data_type::ByteArray;

    #[derive(Default)]
    struct VecStorage(Vec<ByteArray>);

    impl Storage for VecStorage {
        type Key = usize;
        type Value = ByteArray;

        fn get(&self, idx: Self::Key) -> &Self::Value {
            &self.0[idx]
        }

        fn push(&mut self, value: &Self::Value) -> Self::Key {
            let key = self.0.len();
            self.0.push(value.clone());
            key
        }

        fn estimated_memory_size(&self) -> usize {
            self.0.iter().map(|value| value.as_bytes().len()).sum()
        }
    }

    #[test]
    fn intern_bytes_constructs_values_only_on_miss() {
        let calls = Cell::new(0);
        let make = |bytes: &[u8]| {
            calls.set(calls.get() + 1);
            ByteArray::from(bytes.to_vec())
        };
        let mut interner = Interner::new(VecStorage::default());

        let first = interner.intern_bytes(b"same", || make(b"same"));
        let duplicate = interner.intern_bytes(b"same", || make(b"same"));
        let other = interner.intern_bytes(b"other", || make(b"other"));

        assert_eq!(first, duplicate);
        assert_ne!(first, other);
        assert_eq!(calls.get(), 2);
        assert_eq!(interner.storage().0.len(), 2);
    }
}
