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

use std::io::Write;

use crate::errors::{ParquetError, Result};
use crate::file::metadata::HeapSize;
use crate::format::{
    ColumnCryptoMetaData as TColumnCryptoMetaData,
    EncryptionWithColumnKey as TEncryptionWithColumnKey,
    EncryptionWithFooterKey as TEncryptionWithFooterKey,
};
use crate::parquet_thrift::{
    read_thrift_vec, ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol,
    ThriftCompactOutputProtocol, WriteThrift, WriteThriftField,
};
use crate::{thrift_struct, thrift_union};

// define this and ColumnCryptoMetadata here so they're only defined when
// the encryption feature is enabled

thrift_struct!(
/// Encryption metadata for a column chunk encrypted with a column-specific key
pub struct EncryptionWithColumnKey {
  /// Path to the column in the Parquet schema
  1: required list<string> path_in_schema

  /// Path to the column in the Parquet schema
  2: optional binary key_metadata
}
);

impl HeapSize for EncryptionWithColumnKey {
    fn heap_size(&self) -> usize {
        self.path_in_schema.heap_size() + self.key_metadata.heap_size()
    }
}

thrift_union!(
/// ColumnCryptoMetadata for a column chunk
union ColumnCryptoMetaData {
  1: ENCRYPTION_WITH_FOOTER_KEY
  2: (EncryptionWithColumnKey) ENCRYPTION_WITH_COLUMN_KEY
}
);

impl HeapSize for ColumnCryptoMetaData {
    fn heap_size(&self) -> usize {
        match self {
            Self::ENCRYPTION_WITH_FOOTER_KEY => 0,
            Self::ENCRYPTION_WITH_COLUMN_KEY(path) => path.heap_size(),
        }
    }
}

/// Converts Thrift definition into `ColumnCryptoMetadata`.
pub fn try_from_thrift(
    thrift_column_crypto_metadata: &TColumnCryptoMetaData,
) -> Result<ColumnCryptoMetaData> {
    let crypto_metadata = match thrift_column_crypto_metadata {
        TColumnCryptoMetaData::ENCRYPTIONWITHFOOTERKEY(_) => {
            ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY
        }
        TColumnCryptoMetaData::ENCRYPTIONWITHCOLUMNKEY(encryption_with_column_key) => {
            ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(EncryptionWithColumnKey {
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
        ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY => {
            TColumnCryptoMetaData::ENCRYPTIONWITHFOOTERKEY(TEncryptionWithFooterKey {})
        }
        ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(encryption_with_column_key) => {
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
    use crate::parquet_thrift::tests::test_roundtrip;

    #[test]
    fn test_encryption_with_footer_key_from_thrift() {
        let metadata = ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY;

        assert_eq!(try_from_thrift(&to_thrift(&metadata)).unwrap(), metadata);
    }

    #[test]
    fn test_encryption_with_column_key_from_thrift() {
        let metadata = ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(EncryptionWithColumnKey {
            path_in_schema: vec!["abc".to_owned(), "def".to_owned()],
            key_metadata: Some(vec![0, 1, 2, 3, 4, 5]),
        });

        assert_eq!(try_from_thrift(&to_thrift(&metadata)).unwrap(), metadata);
    }

    #[test]
    fn test_column_crypto_roundtrip() {
        test_roundtrip(ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY);

        let path_in_schema = vec!["foo".to_owned(), "bar".to_owned(), "really".to_owned()];
        let key_metadata = vec![1u8; 32];
        test_roundtrip(ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(
            EncryptionWithColumnKey {
                path_in_schema: path_in_schema.clone(),
                key_metadata: None,
            },
        ));
        test_roundtrip(ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(
            EncryptionWithColumnKey {
                path_in_schema,
                key_metadata: Some(key_metadata),
            },
        ));
    }
}
