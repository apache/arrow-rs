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

/// The bits of a fixed width value of 4 or 8 bytes, for [`FixedWidthInterner`]
pub trait Bits: Copy + Eq + std::fmt::Debug {
    /// Reads the bits from the bytes of a value of this width
    fn from_bytes(bytes: &[u8]) -> Self;

    fn to_u64(self) -> u64;
}

impl Bits for u32 {
    #[inline(always)]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_ne_bytes(bytes.try_into().unwrap())
    }

    #[inline(always)]
    fn to_u64(self) -> u64 {
        self.into()
    }
}

impl Bits for u64 {
    #[inline(always)]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_ne_bytes(bytes.try_into().unwrap())
    }

    #[inline(always)]
    fn to_u64(self) -> u64 {
        self
    }
}

/// Returns the hash of the bits of a fixed width value, as Arrow C++ hashes
/// integers (`ScalarHelper::ComputeHash`, from apache/arrow#3005)
///
/// Multiplying by the prime, one of xxHash's chosen for its bit dispersion,
/// mixes the low bits into the high bits; byte swapping, a single instruction,
/// then moves the mixed high bits to the low bits, from which the hash table
/// takes a value's bucket.
#[inline(always)]
fn hash_bits<B: Bits>(bits: B) -> u64 {
    const PRIME: u64 = 11400714785074694791;
    bits.to_u64().wrapping_mul(PRIME).swap_bytes()
}

/// An entry of [`FixedWidthInterner`]'s hash table
#[derive(Debug, Clone, Copy)]
struct Slot<B> {
    /// The bits of the value
    bits: B,
    /// Index of the value in the dictionary
    key: u64,
}

/// An [`Interner`] for values of 4 or 8 bytes, such as integers and floats
///
/// Hashing such a value as an integer is cheaper than storing and comparing a
/// hash, so unlike [`Interner`] an entry holds no hash but the value's bits
/// themselves: a lookup compares them directly, without reading the
/// dictionary's values, and growing the table hashes the bits again.
///
/// Values are compared by their bits, so NaNs are interned like any other
/// value.
#[derive(Debug)]
pub struct FixedWidthInterner<S: Storage<Key = u64>, B: Bits> {
    dedup: HashTable<Slot<B>>,

    storage: S,
}

impl<S: Storage<Key = u64>, B: Bits> FixedWidthInterner<S, B> {
    /// Create a new `FixedWidthInterner` with the provided storage
    pub fn new(storage: S) -> Self {
        Self {
            dedup: HashTable::new(),
            storage,
        }
    }

    /// Intern each of `values`, appending their keys to `keys`
    pub fn intern_batch(&mut self, values: &[S::Value], keys: &mut Vec<u64>)
    where
        S::Value: Sized,
    {
        let start = keys.len();
        keys.resize(start + values.len(), 0);
        for (value, key) in values.iter().zip(&mut keys[start..]) {
            *key = self.intern(value, B::from_bytes(value.as_bytes()));
        }
    }

    /// Returns the key of `value` with `bits`, inserting it if absent
    #[inline]
    fn intern(&mut self, value: &S::Value, bits: B) -> u64 {
        let hash = hash_bits(bits);
        match self.dedup.find(hash, |slot| slot.bits == bits) {
            Some(slot) => slot.key,
            None => self.insert(value, bits, hash),
        }
    }

    /// Appends `value`, absent from the dictionary, to it
    ///
    /// Out of line, so the lookup loop needs no registers for it
    #[cold]
    #[inline(never)]
    fn insert(&mut self, value: &S::Value, bits: B, hash: u64) -> u64 {
        let key = self.storage.push(value);
        self.dedup
            .insert_unique(hash, Slot { bits, key }, |slot| hash_bits(slot.bits));
        key
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
    use rand::prelude::*;
    use std::collections::HashMap;

    /// Stores values in a `Vec`, like the dictionary encoder
    #[derive(Debug)]
    struct VecStorage<T>(Vec<T>);

    impl<T: AsBytes + Clone> Storage for VecStorage<T> {
        type Key = u64;
        type Value = T;

        fn get(&self, idx: u64) -> &T {
            &self.0[idx as usize]
        }

        fn push(&mut self, value: &T) -> u64 {
            self.0.push(value.clone());
            self.0.len() as u64 - 1
        }

        fn estimated_memory_size(&self) -> usize {
            self.0.capacity() * std::mem::size_of::<T>()
        }
    }

    /// Interns `values` in batches, checking each key against a `HashMap` of
    /// the values' bits, and the dictionary against the first appearances
    fn check<T, B>(values: &[T], bits: impl Fn(&T) -> B)
    where
        T: AsBytes + Clone + std::fmt::Debug,
        B: Bits + std::hash::Hash,
    {
        let mut interner = FixedWidthInterner::<VecStorage<T>, B>::new(VecStorage(vec![]));
        let mut expected_keys = HashMap::new();
        let mut expected_values = vec![];
        let mut keys = vec![];
        for batch in values.chunks(100) {
            keys.clear();
            interner.intern_batch(batch, &mut keys);
            for (value, key) in batch.iter().zip(&keys) {
                let expected = *expected_keys.entry(bits(value)).or_insert_with(|| {
                    expected_values.push(value.clone());
                    expected_values.len() as u64 - 1
                });
                assert_eq!(*key, expected, "{value:?}");
            }
        }
        let dictionary = &interner.storage().0;
        assert_eq!(dictionary.len(), expected_values.len());
        for (a, b) in dictionary.iter().zip(&expected_values) {
            assert_eq!(bits(a), bits(b));
        }
    }

    #[test]
    fn test_fixed_width_interner() {
        let mut rng = StdRng::seed_from_u64(42);

        // Few distinct values, many repeats, and enough distinct ones to grow
        // the table several times
        let mut i32s: Vec<i32> = (0..20_000).map(|_| rng.random_range(-50..50)).collect();
        i32s.extend((0..20_000).map(|_| rng.random::<i32>()));
        i32s.extend([i32::MIN, 0, i32::MAX, -1]);
        i32s.shuffle(&mut rng);
        check(&i32s, |v| *v as u32);

        let mut i64s: Vec<i64> = (0..20_000).map(|_| rng.random_range(-50..50)).collect();
        i64s.extend((0..20_000).map(|_| rng.random::<i64>()));
        i64s.extend([i64::MIN, 0, i64::MAX, -1]);
        i64s.shuffle(&mut rng);
        check(&i64s, |v| *v as u64);

        // Floats are interned by their bits: different NaNs and zeros differ
        let mut f32s: Vec<f32> = (0..5_000)
            .map(|_| rng.random_range(-10..10) as f32)
            .collect();
        f32s.extend([
            f32::NAN,
            -f32::NAN,
            f32::from_bits(0x7fc0_0001),
            0.0,
            -0.0,
            f32::NAN,
        ]);
        f32s.shuffle(&mut rng);
        check(&f32s, |v| v.to_bits());

        let mut f64s: Vec<f64> = (0..5_000).map(|_| rng.random::<f64>()).collect();
        f64s.extend([
            f64::NAN,
            -f64::NAN,
            0.0,
            -0.0,
            f64::from_bits(0x7ff8_0000_0000_0001),
            f64::NAN,
        ]);
        f64s.shuffle(&mut rng);
        check(&f64s, |v| v.to_bits());
    }
}
