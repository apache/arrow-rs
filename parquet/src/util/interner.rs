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
    /// Marks an empty slot. A value with these bits is interned apart from the
    /// table, so the table needs no other marker of an empty slot.
    const EMPTY: Self;

    /// Reads the bits from the bytes of a value of this width
    fn from_bytes(bytes: &[u8]) -> Self;

    fn to_u64(self) -> u64;
}

impl Bits for u32 {
    const EMPTY: Self = 0x9E37_79B9;

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
    const EMPTY: Self = 0x9E37_79B9_7F4A_7C15;

    #[inline(always)]
    fn from_bytes(bytes: &[u8]) -> Self {
        Self::from_ne_bytes(bytes.try_into().unwrap())
    }

    #[inline(always)]
    fn to_u64(self) -> u64 {
        self
    }
}

/// A slot of [`FixedWidthInterner`]'s hash table
#[derive(Debug, Clone, Copy)]
struct Slot<B> {
    /// [`Bits::EMPTY`], or the bits of the value in this slot
    bits: B,
    /// Index of the value in the dictionary
    key: u32,
}

/// Initial number of slots of [`FixedWidthInterner`]
const MIN_SLOTS: usize = 16;

/// An [`Interner`] for values of 4 or 8 bytes, such as integers and floats
///
/// Hashing such a value is a single multiply, so unlike [`Interner`] the
/// table stores no hash: each slot holds the value's bits themselves, which
/// a lookup compares directly, without reading the dictionary's values.
///
/// * The slot of a value is the top bits of its bits times a random odd
///   multiplier: one multiply and a shift
/// * Open addressing with linear probing, at most half full. A lookup looks
///   in the value's slot and the next with no branch between them, as a
///   branch on which one holds the value mispredicts; anything else is left
///   to an out of line probe.
/// * One more, always empty, slot after the power of two, so the slot after
///   any value's own is in bounds without wrapping
/// * Values are compared by their bits, so NaNs are interned like any other
///   value
#[derive(Debug)]
pub struct FixedWidthInterner<S: Storage<Key = u64>, B: Bits> {
    /// Odd multiplier from a value's bits to its slot
    multiplier: u64,

    /// Hash table: a power of two number of slots, then one more that is
    /// always empty; or none before first use
    slots: Vec<Slot<B>>,

    /// The slot of a value is its bits times `multiplier` shifted right by this
    shift: u32,

    /// Key of the value whose bits are [`Bits::EMPTY`], if interned
    empty_bits_key: Option<u32>,

    /// Number of values in the table
    len: usize,

    storage: S,
}

impl<S: Storage<Key = u64>, B: Bits> FixedWidthInterner<S, B> {
    /// Create a new `FixedWidthInterner` with the provided storage
    pub fn new(storage: S) -> Self {
        Self {
            multiplier: ahash::RandomState::new().hash_one(0u64) | 1,
            slots: vec![],
            shift: u64::BITS,
            empty_bits_key: None,
            len: 0,
            storage,
        }
    }

    /// Intern each of `values`, appending their keys to `keys`
    pub fn intern_batch(&mut self, values: &[S::Value], keys: &mut Vec<u64>)
    where
        S::Value: Sized,
    {
        if self.slots.is_empty() {
            self.grow();
        }
        let start = keys.len();
        keys.resize(start + values.len(), 0);
        for (value, key) in values.iter().zip(&mut keys[start..]) {
            *key = self.intern(value, B::from_bytes(value.as_bytes())).into();
        }
    }

    /// Returns the key of `value` with `bits`, inserting it if absent
    #[inline(always)]
    fn intern(&mut self, value: &S::Value, bits: B) -> u32 {
        if bits == B::EMPTY {
            return self.intern_empty_bits(value);
        }
        let pos = self.position(bits);
        // SAFETY: `pos` is less than the power of two number of slots, which
        // the always empty slot follows
        let (first, second) = unsafe {
            let slots = &self.slots;
            (*slots.get_unchecked(pos), *slots.get_unchecked(pos + 1))
        };
        let in_first = first.bits == bits;
        let found = in_first | (second.bits == bits);
        let key = std::hint::select_unpredictable(in_first, first.key, second.key);
        if found {
            return key;
        }
        self.probe(value, bits, pos)
    }

    #[inline(always)]
    fn position(&self, bits: B) -> usize {
        (bits.to_u64().wrapping_mul(self.multiplier) >> self.shift) as usize
    }

    /// Returns the power of two number of slots, less one
    fn mask(&self) -> usize {
        self.slots.len() - 2
    }

    /// [`Self::intern`] for a value in neither its slot nor the next, or absent
    #[cold]
    #[inline(never)]
    fn probe(&mut self, value: &S::Value, bits: B, mut pos: usize) -> u32 {
        let mask = self.mask();
        loop {
            let slot = self.slots[pos];
            if slot.bits == bits {
                return slot.key;
            }
            if slot.bits == B::EMPTY {
                return self.insert(value, bits, pos);
            }
            pos = (pos + 1) & mask;
        }
    }

    /// [`Self::intern`] for the value whose bits mark an empty slot
    #[cold]
    #[inline(never)]
    fn intern_empty_bits(&mut self, value: &S::Value) -> u32 {
        match self.empty_bits_key {
            Some(key) => key,
            None => {
                let key = self.push(value);
                self.empty_bits_key = Some(key);
                key
            }
        }
    }

    fn push(&mut self, value: &S::Value) -> u32 {
        u32::try_from(self.storage.push(value)).expect("too many dictionary values")
    }

    /// Appends `value` to the dictionary, in the empty slot at `pos`
    fn insert(&mut self, value: &S::Value, bits: B, pos: usize) -> u32 {
        let key = self.push(value);
        self.len += 1;
        let slot = Slot { bits, key };
        // At most half full, so probe sequences stay short
        if self.len * 2 > self.mask() + 1 {
            self.grow();
            self.place(slot);
        } else {
            self.slots[pos] = slot;
        }
        key
    }

    /// Doubles the number of slots, moving every value to its new slot
    #[cold]
    fn grow(&mut self) {
        let len = (self.slots.len().saturating_sub(1) * 2).max(MIN_SLOTS);
        let empty = Slot {
            bits: B::EMPTY,
            key: 0,
        };
        let old = std::mem::replace(&mut self.slots, vec![empty; len + 1]);
        self.shift = u64::BITS - len.trailing_zeros();
        for slot in old.into_iter().filter(|slot| slot.bits != B::EMPTY) {
            self.place(slot);
        }
    }

    /// Puts `slot` in the first empty slot from its position
    fn place(&mut self, slot: Slot<B>) {
        let mask = self.mask();
        let mut pos = self.position(slot.bits);
        while self.slots[pos].bits != B::EMPTY {
            pos = (pos + 1) & mask;
        }
        self.slots[pos] = slot;
    }

    /// Return estimate of the memory used, in bytes
    pub fn estimated_memory_size(&self) -> usize {
        self.storage.estimated_memory_size()
            + self.slots.capacity() * std::mem::size_of::<Slot<B>>()
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
        // the table several times; with the bits that mark an empty slot
        let mut i32s: Vec<i32> = (0..20_000).map(|_| rng.random_range(-50..50)).collect();
        i32s.extend((0..20_000).map(|_| rng.random::<i32>()));
        i32s.extend([u32::EMPTY as i32, 0, u32::EMPTY as i32, -1]);
        i32s.shuffle(&mut rng);
        check(&i32s, |v| *v as u32);

        let mut i64s: Vec<i64> = (0..20_000).map(|_| rng.random_range(-50..50)).collect();
        i64s.extend((0..20_000).map(|_| rng.random::<i64>()));
        i64s.extend([u64::EMPTY as i64, 0, u64::EMPTY as i64, i64::MIN]);
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
            f64::from_bits(u64::EMPTY),
            f64::NAN,
        ]);
        f64s.shuffle(&mut rng);
        check(&f64s, |v| v.to_bits());
    }
}
