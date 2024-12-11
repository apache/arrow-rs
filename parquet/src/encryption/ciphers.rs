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

//! Encryption implementation specific to Parquet, as described
//! in the [spec](https://github.com/apache/parquet-format/blob/master/Encryption.md).

use ring::aead::{Aad, LessSafeKey, NonceSequence, UnboundKey, AES_128_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use crate::errors::{ParquetError, Result};

pub trait BlockEncryptor {
    fn encrypt(&mut self, plaintext: &[u8], aad: &[u8]) -> Vec<u8>;
}

pub trait BlockDecryptor {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Vec<u8>;
}

const RIGHT_TWELVE: u128 = 0x0000_0000_ffff_ffff_ffff_ffff_ffff_ffff;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const SIZE_LEN: usize = 4;

struct CounterNonce {
    start: u128,
    counter: u128,
}

impl CounterNonce {
    pub fn new(rng: &SystemRandom) -> Self {
        let mut buf = [0; 16];
        rng.fill(&mut buf).unwrap();

        // Since this is a random seed value, endianess doesn't matter at all,
        // and we can use whatever is platform-native.
        let start = u128::from_ne_bytes(buf) & RIGHT_TWELVE;
        let counter = start.wrapping_add(1);

        Self { start, counter }
    }

    /// One accessor for the nonce bytes to avoid potentially flipping endianess
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

pub(crate) struct RingGcmBlockEncryptor {
    key: LessSafeKey,
    nonce_sequence: CounterNonce,
}

impl RingGcmBlockEncryptor {
    // todo TBD: some KMS systems produce data keys, need to be able to pass them to Encryptor.
    // todo TBD: for other KMSs, we will create data keys inside arrow-rs, making sure to use SystemRandom
    /// Create a new `RingGcmBlockEncryptor` with a given key and random nonce.
    /// The nonce will advance appropriately with each block encryption and
    /// return an error if it wraps around.
    pub(crate) fn new(key_bytes: &[u8]) -> Self {
        let rng = SystemRandom::new();

        // todo support other key sizes
        let key = UnboundKey::new(&AES_128_GCM, key_bytes.as_ref()).unwrap();
        let nonce = CounterNonce::new(&rng);

        Self {
            key: LessSafeKey::new(key),
            nonce_sequence: nonce,
        }
    }
}

impl BlockEncryptor for RingGcmBlockEncryptor {
    fn encrypt(&mut self, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
        let nonce = self.nonce_sequence.advance().unwrap();
        let ciphertext_len = plaintext.len() + NONCE_LEN + TAG_LEN;
        // todo TBD: add first 4 bytes with the length, per https://github.com/apache/parquet-format/blob/master/Encryption.md#51-encrypted-module-serialization
        let mut result = Vec::with_capacity(SIZE_LEN + ciphertext_len);
        result.extend_from_slice((ciphertext_len as i32).to_le_bytes().as_ref());
        result.extend_from_slice(nonce.as_ref());
        result.extend_from_slice(plaintext);

        let tag = self
            .key
            .seal_in_place_separate_tag(nonce, Aad::from(aad), &mut result[SIZE_LEN + NONCE_LEN..])
            .unwrap();
        result.extend_from_slice(tag.as_ref());

        result
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RingGcmBlockDecryptor {
    key: LessSafeKey,
}

impl RingGcmBlockDecryptor {
    pub(crate) fn new(key_bytes: &[u8]) -> Self {
        // todo support other key sizes
        let key = UnboundKey::new(&AES_128_GCM, key_bytes).unwrap();

        Self {
            key: LessSafeKey::new(key),
        }
    }
}

impl BlockDecryptor for RingGcmBlockDecryptor {
    fn decrypt(&self, length_and_ciphertext: &[u8], aad: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(
            length_and_ciphertext.len() - SIZE_LEN - NONCE_LEN - TAG_LEN,
        );
        result.extend_from_slice(&length_and_ciphertext[SIZE_LEN + NONCE_LEN..]);

        let nonce = ring::aead::Nonce::try_assume_unique_for_key(
            &length_and_ciphertext[SIZE_LEN..SIZE_LEN + NONCE_LEN],
        )
        .unwrap();

        self.key
            .open_in_place(nonce, Aad::from(aad), &mut result)
            .unwrap();

        result
    }
}

pub(crate) enum ModuleType {
    Footer = 0,
    ColumnMetaData = 1,
    DataPage = 2,
    DictionaryPage = 3,
    DataPageHeader = 4,
    DictionaryPageHeader = 5,
    ColumnIndex = 6,
    OffsetIndex = 7,
    BloomFilterHeader = 8,
    BloomFilterBitset = 9,
}

pub fn create_footer_aad(file_aad: &[u8]) -> Result<Vec<u8>> {
    create_module_aad(file_aad, ModuleType::Footer, -1, -1, -1)
}

fn create_module_aad(file_aad: &[u8], module_type: ModuleType, row_group_ordinal: i32,
                     column_ordinal: i32, page_ordinal: i32) -> Result<Vec<u8>> {

    let module_buf = [module_type as u8];

    if module_buf[0] == (ModuleType::Footer as u8) {
        let mut aad = Vec::with_capacity(file_aad.len() + 1);
        aad.extend_from_slice(file_aad);
        aad.extend_from_slice(module_buf.as_ref());
        return Ok(aad)
    }

    if row_group_ordinal < 0 {
        return Err(general_err!("Wrong row group ordinal: {}", row_group_ordinal));
    }
    if row_group_ordinal > u16::MAX as i32 {
        return Err(general_err!("Encrypted parquet files can't have more than {} row groups: {}",
            u16::MAX, row_group_ordinal));
    }

    if column_ordinal < 0 {
        return Err(general_err!("Wrong column ordinal: {}", column_ordinal));
    }
    if column_ordinal > u16::MAX as i32 {
        return Err(general_err!("Encrypted parquet files can't have more than {} columns: {}",
            u16::MAX, column_ordinal));
    }

    if module_buf[0] != (ModuleType::DataPageHeader as u8) &&
        module_buf[0] != (ModuleType::DataPage as u8) {
        let mut aad = Vec::with_capacity(file_aad.len() + 5);
        aad.extend_from_slice(file_aad);
        aad.extend_from_slice(module_buf.as_ref());
        aad.extend_from_slice((row_group_ordinal as u16).to_le_bytes().as_ref());
        aad.extend_from_slice((column_ordinal as u16).to_le_bytes().as_ref());
        return Ok(aad)
    }

    if page_ordinal < 0 {
        return Err(general_err!("Wrong column ordinal: {}", page_ordinal));
    }
    if page_ordinal > u16::MAX as i32 {
        return Err(general_err!("Encrypted parquet files can't have more than {} pages in a chunk: {}",
            u16::MAX, page_ordinal));
    }

    let mut aad = Vec::with_capacity(file_aad.len() + 7);
    aad.extend_from_slice(file_aad);
    aad.extend_from_slice(module_buf.as_ref());
    aad.extend_from_slice((row_group_ordinal as u16).to_le_bytes().as_ref());
    aad.extend_from_slice((column_ordinal as u16).to_le_bytes().as_ref());
    aad.extend_from_slice((page_ordinal as u16).to_le_bytes().as_ref());
    Ok(aad)
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileDecryptionProperties {
    footer_key: Option<Vec<u8>>
}

impl FileDecryptionProperties {
    pub fn builder() -> DecryptionPropertiesBuilder {
        DecryptionPropertiesBuilder::with_defaults()
    }
}

pub struct DecryptionPropertiesBuilder {
    footer_key: Option<Vec<u8>>
}

impl DecryptionPropertiesBuilder {
    pub fn with_defaults() -> Self {
        Self {
            footer_key: None
        }
    }

    pub fn build(self) -> FileDecryptionProperties {
        FileDecryptionProperties {
            footer_key: self.footer_key
        }
    }

    // todo decr: doc comment
    pub fn with_footer_key(mut self, value: Vec<u8>) -> Self {
        self.footer_key = Some(value);
        self
    }
}

#[derive(Debug, Clone)]
pub struct FileDecryptor {
    decryption_properties: FileDecryptionProperties,
    // todo decr: change to BlockDecryptor
    footer_decryptor: RingGcmBlockDecryptor
}

impl PartialEq for FileDecryptor {
    fn eq(&self, other: &Self) -> bool {
        self.decryption_properties == other.decryption_properties
    }
}

impl FileDecryptor {
    pub(crate) fn new(decryption_properties: &FileDecryptionProperties) -> Self {
        Self {
            // todo decr: if no key available yet (not set in properties, will be retrieved from metadata)
            footer_decryptor: RingGcmBlockDecryptor::new(decryption_properties.footer_key.clone().unwrap().as_ref()),
            decryption_properties: decryption_properties.clone()
        }
    }

    // todo decr: change to BlockDecryptor
    pub(crate) fn get_footer_decryptor(self) -> RingGcmBlockDecryptor {
        self.footer_decryptor
    }
}

pub struct CryptoContext {
    row_group_ordinal: i32,
    column_ordinal: i32,
    metadata_decryptor: FileDecryptor,
    data_decryptor: FileDecryptor,
    file_decryption_properties: FileDecryptionProperties,
    aad: Vec<u8>,
}

impl CryptoContext {
    pub fn data_decryptor(self) -> FileDecryptor { self.data_decryptor }
    pub fn file_decryption_properties(&self) -> &FileDecryptionProperties { &self.file_decryption_properties }
}
