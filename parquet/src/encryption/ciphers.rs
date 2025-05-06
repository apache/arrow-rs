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

use crate::errors::ParquetError;
use crate::errors::ParquetError::General;
use crate::errors::Result;
use ring::aead::{Aad, LessSafeKey, NonceSequence, UnboundKey, AES_128_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use std::fmt::Debug;

const RIGHT_TWELVE: u128 = 0x0000_0000_ffff_ffff_ffff_ffff_ffff_ffff;
pub(crate) const NONCE_LEN: usize = 12;
pub(crate) const TAG_LEN: usize = 16;
pub(crate) const SIZE_LEN: usize = 4;

pub(crate) trait BlockDecryptor: Debug + Send + Sync {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    fn compute_plaintext_tag(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub(crate) struct RingGcmBlockDecryptor {
    key: LessSafeKey,
}

impl RingGcmBlockDecryptor {
    pub(crate) fn new(key_bytes: &[u8]) -> Result<Self> {
        // todo support other key sizes
        let key = UnboundKey::new(&AES_128_GCM, key_bytes)
            .map_err(|_| General("Failed to create AES key".to_string()))?;

        Ok(Self {
            key: LessSafeKey::new(key),
        })
    }
}

impl BlockDecryptor for RingGcmBlockDecryptor {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(length_and_ciphertext.len() - SIZE_LEN - NONCE_LEN);
        result.extend_from_slice(&length_and_ciphertext[SIZE_LEN + NONCE_LEN..]);

        let nonce = ring::aead::Nonce::try_assume_unique_for_key(
            &length_and_ciphertext[SIZE_LEN..SIZE_LEN + NONCE_LEN],
        )?;

        self.key.open_in_place(nonce, Aad::from(aad), &mut result)?;

        // Truncate result to remove the tag
        result.resize(result.len() - TAG_LEN, 0u8);
        Ok(result)
    }

    fn compute_plaintext_tag(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut plaintext = plaintext.to_vec();
        let nonce = &plaintext[plaintext.len() - NONCE_LEN - TAG_LEN..plaintext.len() - TAG_LEN];
        let nonce = ring::aead::Nonce::try_assume_unique_for_key(nonce)?;
        let plaintext_end = plaintext.len() - NONCE_LEN - TAG_LEN;
        let tag = self.key.seal_in_place_separate_tag(
            nonce,
            Aad::from(aad),
            &mut plaintext[..plaintext_end],
        )?;
        Ok(tag.as_ref().to_vec())
    }
}

pub(crate) trait BlockEncryptor: Debug + Send + Sync {
    fn encrypt(&mut self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
struct CounterNonce {
    start: u128,
    counter: u128,
}

impl CounterNonce {
    pub fn new(rng: &SystemRandom) -> Result<Self> {
        let mut buf = [0; 16];
        rng.fill(&mut buf)?;

        // Since this is a random seed value, endianness doesn't matter at all,
        // and we can use whatever is platform-native.
        let start = u128::from_ne_bytes(buf) & RIGHT_TWELVE;
        let counter = start.wrapping_add(1);

        Ok(Self { start, counter })
    }

    /// One accessor for the nonce bytes to avoid potentially flipping endianness
    #[inline]
    pub fn get_bytes(&self) -> [u8; NONCE_LEN] {
        self.counter.to_le_bytes()[0..NONCE_LEN].try_into().unwrap()
    }
}

impl NonceSequence for CounterNonce {
    fn advance(&mut self) -> Result<ring::aead::Nonce, ring::error::Unspecified> {
        // If we've wrapped around, we've exhausted this nonce sequence
        if (self.counter & RIGHT_TWELVE) == (self.start & RIGHT_TWELVE) {
            Err(ring::error::Unspecified)
        } else {
            // Otherwise, just advance and return the new value
            let buf: [u8; NONCE_LEN] = self.get_bytes();
            self.counter = self.counter.wrapping_add(1);
            Ok(ring::aead::Nonce::assume_unique_for_key(buf))
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RingGcmBlockEncryptor {
    key: LessSafeKey,
    nonce_sequence: CounterNonce,
}

impl RingGcmBlockEncryptor {
    /// Create a new `RingGcmBlockEncryptor` with a given key and random nonce.
    /// The nonce will advance appropriately with each block encryption and
    /// return an error if it wraps around.
    pub(crate) fn new(key_bytes: &[u8]) -> Result<Self> {
        let rng = SystemRandom::new();

        // todo support other key sizes
        let key = UnboundKey::new(&AES_128_GCM, key_bytes)
            .map_err(|e| general_err!("Error creating AES key: {}", e))?;
        let nonce = CounterNonce::new(&rng)?;

        Ok(Self {
            key: LessSafeKey::new(key),
            nonce_sequence: nonce,
        })
    }
}

impl BlockEncryptor for RingGcmBlockEncryptor {
    fn encrypt(&mut self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        // Create encrypted buffer.
        // Format is: [ciphertext size, nonce, ciphertext, authentication tag]
        let ciphertext_length: u32 = (NONCE_LEN + plaintext.len() + TAG_LEN)
            .try_into()
            .map_err(|err| General(format!("Plaintext data too long. {:?}", err)))?;
        // Not checking for overflow here because we've already checked for it with ciphertext_length
        let mut ciphertext = Vec::with_capacity(SIZE_LEN + ciphertext_length as usize);
        ciphertext.extend((ciphertext_length).to_le_bytes());

        let nonce = self.nonce_sequence.advance()?;
        ciphertext.extend(nonce.as_ref());
        ciphertext.extend(plaintext);

        let tag = self.key.seal_in_place_separate_tag(
            nonce,
            Aad::from(aad),
            &mut ciphertext[SIZE_LEN + NONCE_LEN..],
        )?;

        ciphertext.extend(tag.as_ref());

        debug_assert_eq!(SIZE_LEN + ciphertext_length as usize, ciphertext.len());

        Ok(ciphertext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let key = [0u8; 16];
        let mut encryptor = RingGcmBlockEncryptor::new(&key).unwrap();
        let decryptor = RingGcmBlockDecryptor::new(&key).unwrap();

        let plaintext = b"hello, world!";
        let aad = b"some aad";

        let ciphertext = encryptor.encrypt(plaintext, aad).unwrap();
        let decrypted = decryptor.decrypt(&ciphertext, aad).unwrap();

        assert_eq!(plaintext, decrypted.as_slice());
    }
}
