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

    /// Whether every value is the same width, at most 8 bytes, so that its bytes can
    /// be kept in the interner's table in place of a hash and compared there.
    const INLINE_VALUES: bool = false;
}

/// A generic value interner supporting various different [`Storage`]
#[derive(Debug, Default)]
pub struct Interner<S: Storage> {
    state: ahash::RandomState,

    /// Used to provide a lookup from value to unique value. Next to each key it keeps
    /// the value's bytes when [`Storage::INLINE_VALUES`], so lookups and growth never
    /// read `storage`, and the value's hash otherwise, so growth does not re-hash.
    dedup: HashTable<(u64, S::Key)>,

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
    ///
    /// The dictionary encoder calls this once per value, so it and the hash probe
    /// of `HashTable::entry` must be inlined into the caller's loop. Without
    /// `#[inline]` (and hashbrown's `inline-more` feature) each has a single copy,
    /// and whether it is inlined depends on how rustc splits the crate into
    /// codegen units, so unrelated changes can turn it into a call per value.
    #[inline]
    pub fn intern(&mut self, value: &S::Value) -> S::Key {
        let bytes = value.as_bytes();
        if S::INLINE_VALUES {
            let mut padded = [0; 8];
            padded[..bytes.len()].copy_from_slice(bytes);
            let inline = u64::from_le_bytes(padded);
            let state = &self.state;
            let hash = state.hash_one(inline);
            return self
                .dedup
                .entry(
                    hash,
                    // Comparing bytes rather than values lets NaNs be interned
                    |&(entry, _)| entry == inline,
                    |&(entry, _)| state.hash_one(entry),
                )
                .or_insert_with(|| (inline, self.storage.push(value)))
                .get()
                .1;
        }

        let hash = self.state.hash_one(bytes);
        self.dedup
            .entry(
                hash,
                // Compare bytes rather than directly comparing values so NaNs can be interned
                |&(entry_hash, index)| {
                    entry_hash == hash && bytes == self.storage.get(index).as_bytes()
                },
                |&(entry_hash, _)| entry_hash,
            )
            .or_insert_with(|| (hash, self.storage.push(value)))
            .get()
            .1
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
