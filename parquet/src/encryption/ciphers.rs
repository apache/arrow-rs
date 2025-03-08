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

use crate::errors::ParquetError::General;
use crate::errors::Result;
use ring::aead::{Aad, LessSafeKey, UnboundKey, AES_128_GCM};
use std::fmt::Debug;

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const SIZE_LEN: usize = 4;

pub trait BlockDecryptor: Debug + Send + Sync {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;
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
}
