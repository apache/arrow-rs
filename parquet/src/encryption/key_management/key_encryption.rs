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

//! Encryption and decryption of data encryption keys (DEKs) with key encryption keys (KEKs)

use crate::errors::{ParquetError, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use ring::aead::{Aad, LessSafeKey, UnboundKey, AES_128_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};

/// Encrypt a DEK with a KEK using AES-GCM
pub fn encrypt_encryption_key(dek: Vec<u8>, kek_id: &str, kek_bytes: &Vec<u8>) -> Result<String> {
    let algorithm = &AES_128_GCM;
    let kek = UnboundKey::new(algorithm, kek_bytes).map_err(|e| {
        general_err!(
            "Error creating AES key from key encryption key bytes: {}",
            e
        )
    })?;
    let kek = LessSafeKey::new(kek);

    let rng = SystemRandom::new();
    let mut nonce = [0u8; NONCE_LEN];
    rng.fill(&mut nonce)?;
    let nonce = ring::aead::Nonce::assume_unique_for_key(nonce);

    let aad = BASE64_STANDARD
        .decode(kek_id)
        .map_err(|e| general_err!("Could not base64 decode key encryption key id: {}", e))?;

    let mut ciphertext = Vec::with_capacity(NONCE_LEN + dek.len() + algorithm.tag_len());
    ciphertext.extend_from_slice(nonce.as_ref());
    ciphertext.extend_from_slice(&dek);
    let tag =
        kek.seal_in_place_separate_tag(nonce, Aad::from(aad), &mut ciphertext[NONCE_LEN..])?;
    ciphertext.extend_from_slice(tag.as_ref());
    let encoded = BASE64_STANDARD.encode(&ciphertext);
    Ok(encoded)
}

/// Decrypt a DEK that has been encrypted with a KEK using AES-GCM
pub fn decrypt_encryption_key(
    wrapped_key: &str,
    kek_id: &str,
    kek_bytes: &Vec<u8>,
) -> Result<Vec<u8>> {
    let encrypted_key = BASE64_STANDARD
        .decode(wrapped_key)
        .map_err(|e| general_err!("Could not base64 decode encrypted key: {}", e))?;

    let algorithm = &AES_128_GCM;
    let kek = UnboundKey::new(algorithm, kek_bytes).map_err(|e| {
        general_err!(
            "Error creating AES key from key encryption key bytes: {}",
            e
        )
    })?;
    let kek = LessSafeKey::new(kek);

    let nonce = ring::aead::Nonce::try_assume_unique_for_key(&encrypted_key[..12])?;
    let aad = BASE64_STANDARD
        .decode(kek_id)
        .map_err(|e| general_err!("Could not base64 decode key encryption key id: {}", e))?;

    let mut plaintext = Vec::with_capacity(encrypted_key.len() - NONCE_LEN);
    plaintext.extend_from_slice(&encrypted_key[NONCE_LEN..]);

    kek.open_in_place(nonce, Aad::from(aad), &mut plaintext)?;
    plaintext.resize(plaintext.len() - algorithm.tag_len(), 0u8);

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_encryption_round_trip() {
        let dek_bytes = "1234567890123450".as_bytes().to_vec();
        let kek_bytes = "1234567890123452".as_bytes().to_vec();
        let kek_id = "kek1";

        let encrypted_key = encrypt_encryption_key(dek_bytes.clone(), kek_id, &kek_bytes).unwrap();
        let decrypted_dek = decrypt_encryption_key(&encrypted_key, "kek1", &kek_bytes).unwrap();

        assert_eq!(dek_bytes, decrypted_dek);
    }
}
