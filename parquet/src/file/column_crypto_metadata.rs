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

//! Column chunk encryption metadata

use crate::errors::Result;
use crate::format::{
    ColumnCryptoMetaData as TColumnCryptoMetaData,
    EncryptionWithColumnKey as TEncryptionWithColumnKey,
    EncryptionWithFooterKey as TEncryptionWithFooterKey,
};

/// ColumnCryptoMetadata for a column chunk
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnCryptoMetaData {
    /// The column is encrypted with the footer key
    EncryptionWithFooterKey,
    /// The column is encrypted with a column-specific key
    EncryptionWithColumnKey(EncryptionWithColumnKey),
}

/// Encryption metadata for a column chunk encrypted with a column-specific key
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptionWithColumnKey {
    /// Path to the column in the Parquet schema
    pub path_in_schema: Vec<String>,
    /// Metadata required to retrieve the column encryption key
    pub key_metadata: Option<Vec<u8>>,
}

/// Converts Thrift definition into `ColumnCryptoMetadata`.
pub fn try_from_thrift(
    thrift_column_crypto_metadata: &TColumnCryptoMetaData,
) -> Result<ColumnCryptoMetaData> {
    let crypto_metadata = match thrift_column_crypto_metadata {
        TColumnCryptoMetaData::ENCRYPTIONWITHFOOTERKEY(_) => {
            ColumnCryptoMetaData::EncryptionWithFooterKey
        }
        TColumnCryptoMetaData::ENCRYPTIONWITHCOLUMNKEY(encryption_with_column_key) => {
            ColumnCryptoMetaData::EncryptionWithColumnKey(EncryptionWithColumnKey {
                path_in_schema: encryption_with_column_key.path_in_schema.clone(),
                key_metadata: encryption_with_column_key.key_metadata.clone(),
            })
        }
    };
    Ok(crypto_metadata)
}

/// Converts `ColumnCryptoMetadata` into Thrift definition.
pub fn to_thrift(column_crypto_metadata: &ColumnCryptoMetaData) -> TColumnCryptoMetaData {
    match column_crypto_metadata {
        ColumnCryptoMetaData::EncryptionWithFooterKey => {
            TColumnCryptoMetaData::ENCRYPTIONWITHFOOTERKEY(TEncryptionWithFooterKey {})
        }
        ColumnCryptoMetaData::EncryptionWithColumnKey(encryption_with_column_key) => {
            TColumnCryptoMetaData::ENCRYPTIONWITHCOLUMNKEY(TEncryptionWithColumnKey {
                path_in_schema: encryption_with_column_key.path_in_schema.clone(),
                key_metadata: encryption_with_column_key.key_metadata.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_with_footer_key_from_thrift() {
        let metadata = ColumnCryptoMetaData::EncryptionWithFooterKey;

        assert_eq!(try_from_thrift(&to_thrift(&metadata)).unwrap(), metadata);
    }

    #[test]
    fn test_encryption_with_column_key_from_thrift() {
        let metadata = ColumnCryptoMetaData::EncryptionWithColumnKey(EncryptionWithColumnKey {
            path_in_schema: vec!["abc".to_owned(), "def".to_owned()],
            key_metadata: Some(vec![0, 1, 2, 3, 4, 5]),
        });

        assert_eq!(try_from_thrift(&to_thrift(&metadata)).unwrap(), metadata);
    }
}
