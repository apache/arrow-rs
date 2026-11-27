// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::data_type::AsBytes;
use hashbrown::HashTable;
use std::fmt::Debug;

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
            // Start empty: a column that gets no value never needs the table. The
            // dictionary encoder reserves the full table before its first value
            // (`reserve_if_empty`), as growing from empty is slower.
            dedup: HashTable::new(),
            storage,
        }
    }

    /// Reserves `DEFAULT_DEDUP_CAPACITY` entries if the table has none yet.
    pub(crate) fn reserve_if_empty(&mut self) {
        if self.dedup.capacity() == 0 {
            self.dedup.reserve(DEFAULT_DEDUP_CAPACITY, |key| {
                self.state.hash_one(self.storage.get(*key).as_bytes())
            });
        }
    }

    /// Intern the value, returning the interned key, and if this was a new value
    ///
    /// The dictionary encoder calls this once per value, so it and the hash probe
    /// of `HashTable::entry` must be inlined into the caller's loop. Without
    /// `#[inline]` (and hashbrown's `inline-more` feature) each has a single copy,
    /// and whether it is inlined depends on how rustc splits the crate into
    /// codegen units, so unrelated changes can turn it into a call per value.
    #[inline]
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

    /// Return estimate of the memory used, in bytes
    pub fn estimated_memory_size(&self) -> usize {
        self.storage.estimated_memory_size() + self.dedup.allocation_size()
    }

    /// Returns the storage for this interner
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct VecStorage(Vec<i64>);

    impl Storage for VecStorage {
        type Key = u64;
        type Value = i64;

        fn get(&self, idx: Self::Key) -> &Self::Value {
            &self.0[idx as usize]
        }

        fn push(&mut self, value: &Self::Value) -> Self::Key {
            self.0.push(*value);
            (self.0.len() - 1) as u64
        }

        fn estimated_memory_size(&self) -> usize {
            self.0.capacity() * std::mem::size_of::<i64>()
        }
    }

    #[test]
    fn keys_do_not_depend_on_dedup_capacity() {
        // 5,000 distinct values cross several resizes of a table grown from empty.
        let values: Vec<i64> = (0..20_000).map(|i| (i * 7919) % 5_000).collect();

        let mut grown = Interner::new(VecStorage::default());
        let mut reserved = Interner::new(VecStorage::default());
        reserved.reserve_if_empty();
        let grown_keys: Vec<_> = values.iter().map(|v| grown.intern(v)).collect();
        let reserved_keys: Vec<_> = values.iter().map(|v| reserved.intern(v)).collect();

        assert_eq!(grown_keys, reserved_keys);
        assert_eq!(grown.storage().0, reserved.storage().0);
        assert_eq!(grown.storage().0.len(), 5_000);
    }
}
