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

//! Encryption support for Thrift serialization

use crate::{
    encryption::decrypt::{FileDecryptionProperties, FileDecryptor},
    errors::{ParquetError, Result},
    file::{
        column_crypto_metadata::ColumnCryptoMetaData,
        metadata::{
            HeapSize, ParquetMetaData, ParquetMetaDataOptions, RowGroupMetaData,
            thrift::{parquet_metadata_from_bytes, read_column_metadata, validate_column_metadata},
        },
    },
    parquet_thrift::{
        ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol,
        ThriftCompactOutputProtocol, ThriftSliceInputProtocol, WriteThrift, WriteThriftField,
    },
    thrift_struct, thrift_union,
};
use std::io::Write;
use std::sync::Arc;

thrift_struct!(
pub(crate) struct AesGcmV1 {
  /// AAD prefix
  1: optional binary aad_prefix

  /// Unique file identifier part of AAD suffix
  2: optional binary aad_file_unique

  /// In files encrypted with AAD prefix without storing it,
  /// readers must supply the prefix
  3: optional bool supply_aad_prefix
}
);

impl HeapSize for AesGcmV1 {
    fn heap_size(&self) -> usize {
        self.aad_prefix.heap_size()
            + self.aad_file_unique.heap_size()
            + self.supply_aad_prefix.heap_size()
    }
}

thrift_struct!(
pub(crate) struct AesGcmCtrV1 {
  /// AAD prefix
  1: optional binary aad_prefix

  /// Unique file identifier part of AAD suffix
  2: optional binary aad_file_unique

  /// In files encrypted with AAD prefix without storing it,
  /// readers must supply the prefix
  3: optional bool supply_aad_prefix
}
);

impl HeapSize for AesGcmCtrV1 {
    fn heap_size(&self) -> usize {
        self.aad_prefix.heap_size()
            + self.aad_file_unique.heap_size()
            + self.supply_aad_prefix.heap_size()
    }
}

thrift_union!(
union EncryptionAlgorithm {
  1: (AesGcmV1) AES_GCM_V1
  2: (AesGcmCtrV1) AES_GCM_CTR_V1
}
);

impl HeapSize for EncryptionAlgorithm {
    fn heap_size(&self) -> usize {
        match self {
            Self::AES_GCM_V1(gcm) => gcm.heap_size(),
            Self::AES_GCM_CTR_V1(gcm_ctr) => gcm_ctr.heap_size(),
        }
    }
}

thrift_struct!(
/// Crypto metadata for files with encrypted footer
pub(crate) struct FileCryptoMetaData<'a> {
  /// Encryption algorithm. This field is only used for files
  /// with encrypted footer. Files with plaintext footer store algorithm id
  /// inside footer (FileMetaData structure).
  1: required EncryptionAlgorithm encryption_algorithm

  /// Retrieval metadata of key used for encryption of footer,
  /// and (possibly) columns.
  2: optional binary<'a> key_metadata
}
);

fn row_group_from_encrypted_thrift(
    mut rg: RowGroupMetaData,
    decryptor: Option<&FileDecryptor>,
    options: Option<&ParquetMetaDataOptions>,
) -> Result<RowGroupMetaData> {
    let schema_descr = rg.schema_descr;

    if schema_descr.num_columns() != rg.columns.len() {
        return Err(general_err!(
            "Column count mismatch. Schema has {} columns while Row Group has {}",
            schema_descr.num_columns(),
            rg.columns.len()
        ));
    }
    let total_byte_size = rg.total_byte_size;
    let num_rows = rg.num_rows;
    let mut columns = vec![];

    for (i, (mut c, d)) in rg
        .columns
        .drain(0..)
        .zip(schema_descr.columns())
        .enumerate()
    {
        // Read encrypted metadata if it's present and we have a decryptor.
        if let (true, Some(decryptor)) = (c.encrypted_column_metadata.is_some(), decryptor) {
            let column_decryptor = match c.crypto_metadata() {
                None => {
                    return Err(general_err!(
                        "No crypto_metadata is set for column '{}', which has encrypted metadata",
                        d.path().string()
                    ));
                }
                Some(ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(crypto_metadata)) => {
                    let column_name = crypto_metadata.path_in_schema.join(".");
                    // Try to get the decryptor - if it fails, we don't have the key
                    match decryptor.get_column_metadata_decryptor(
                        column_name.as_str(),
                        crypto_metadata.key_metadata.as_deref(),
                    ) {
                        Ok(dec) => dec,
                        Err(_) => {
                            // We don't have the key for this column, so we can't decrypt its metadata.
                            columns.push(c);
                            continue;
                        }
                    }
                }
                Some(ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY) => {
                    decryptor.get_footer_decryptor()?
                }
            };

            let column_aad = crate::encryption::modules::create_module_aad(
                decryptor.file_aad(),
                crate::encryption::modules::ModuleType::ColumnMetaData,
                rg.ordinal.unwrap() as usize,
                i,
                None,
            )?;

            // Take the encrypted column metadata as it is no longer needed.
            let encrypted_column_metadata = c.encrypted_column_metadata.take();
            let buf = encrypted_column_metadata.unwrap();
            let decrypted_cc_buf = column_decryptor
                .decrypt(&buf, column_aad.as_ref())
                .map_err(|_| {
                    general_err!(
                        "Unable to decrypt column '{}', perhaps the column key is wrong?",
                        d.path().string()
                    )
                })?;

            // parse decrypted buffer and then replace fields in 'c'
            let mut prot = ThriftSliceInputProtocol::new(&decrypted_cc_buf);
            let mask = read_column_metadata(&mut prot, &mut c, i, options)?;
            validate_column_metadata(mask)?;

            columns.push(c);
        } else {
            columns.push(c);
        }
    }

    let sorting_columns = rg.sorting_columns;
    let file_offset = rg.file_offset;
    let ordinal = rg.ordinal;

    Ok(RowGroupMetaData {
        columns,
        num_rows,
        sorting_columns,
        total_byte_size,
        schema_descr,
        file_offset,
        ordinal,
    })
}

/// Decodes [`ParquetMetaData`] from the provided bytes, handling metadata that may be encrypted.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thrift compact binary protocol, as specified
/// by the [Parquet Spec]. Buffer can be encrypted with AES GCM or AES CTR
/// ciphers as specfied in the [Parquet Encryption Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
/// [Parquet Encryption Spec]: https://parquet.apache.org/docs/file-format/data-pages/encryption/
pub(crate) fn parquet_metadata_with_encryption(
    file_decryption_properties: Option<&Arc<FileDecryptionProperties>>,
    encrypted_footer: bool,
    buf: &[u8],
    options: Option<&ParquetMetaDataOptions>,
) -> Result<ParquetMetaData> {
    use crate::file::metadata::ParquetMetaDataBuilder;

    let mut buf = buf;
    let mut file_decryptor = None;
    let decrypted_fmd_buf;

    if encrypted_footer {
        let mut prot = ThriftSliceInputProtocol::new(buf);
        if let Some(file_decryption_properties) = file_decryption_properties {
            let t_file_crypto_metadata: FileCryptoMetaData =
                FileCryptoMetaData::read_thrift(&mut prot)
                    .map_err(|e| general_err!("Could not parse crypto metadata: {}", e))?;
            let supply_aad_prefix = match &t_file_crypto_metadata.encryption_algorithm {
                EncryptionAlgorithm::AES_GCM_V1(algo) => algo.supply_aad_prefix,
                _ => Some(false),
            }
            .unwrap_or(false);
            if supply_aad_prefix && file_decryption_properties.aad_prefix().is_none() {
                return Err(general_err!(
                    "Parquet file was encrypted with an AAD prefix that is not stored in the file, \
                        but no AAD prefix was provided in the file decryption properties"
                ));
            }
            let decryptor = get_file_decryptor(
                t_file_crypto_metadata.encryption_algorithm,
                t_file_crypto_metadata.key_metadata,
                file_decryption_properties,
            )?;
            let footer_decryptor = decryptor.get_footer_decryptor();
            let aad_footer = crate::encryption::modules::create_footer_aad(decryptor.file_aad())?;

            decrypted_fmd_buf = footer_decryptor?
                .decrypt(prot.as_slice().as_ref(), aad_footer.as_ref())
                .map_err(|_| {
                    general_err!(
                        "Provided footer key and AAD were unable to decrypt parquet footer"
                    )
                })?;

            buf = &decrypted_fmd_buf;
            file_decryptor = Some(decryptor);
        } else {
            return Err(general_err!(
                "Parquet file has an encrypted footer but decryption properties were not provided"
            ));
        }
    }

    let parquet_meta = parquet_metadata_from_bytes(buf, options)
        .map_err(|e| general_err!("Could not parse metadata: {}", e))?;

    let ParquetMetaData {
        mut file_metadata,
        row_groups,
        column_index: _,
        offset_index: _,
        file_decryptor: _,
    } = parquet_meta;

    // Take the encryption algorithm and footer signing key metadata as they are no longer
    // needed after this.
    if let (Some(algo), Some(file_decryption_properties)) = (
        file_metadata.encryption_algorithm.take(),
        file_decryption_properties,
    ) {
        let footer_signing_key_metadata = file_metadata.footer_signing_key_metadata.take();

        // File has a plaintext footer but encryption algorithm is set
        let file_decryptor_value = get_file_decryptor(
            *algo,
            footer_signing_key_metadata.as_deref(),
            file_decryption_properties,
        )?;
        if file_decryption_properties.check_plaintext_footer_integrity() && !encrypted_footer {
            file_decryptor_value.verify_plaintext_footer_signature(buf)?;
        }
        file_decryptor = Some(file_decryptor_value);
    }

    // decrypt column chunk info
    let row_groups = row_groups
        .into_iter()
        .map(|rg| row_group_from_encrypted_thrift(rg, file_decryptor.as_ref(), options))
        .collect::<Result<Vec<_>>>()?;

    let metadata = ParquetMetaDataBuilder::new(file_metadata)
        .set_row_groups(row_groups)
        .set_file_decryptor(file_decryptor)
        .build();

    Ok(metadata)
}

fn get_file_decryptor(
    encryption_algorithm: EncryptionAlgorithm,
    footer_key_metadata: Option<&[u8]>,
    file_decryption_properties: &Arc<FileDecryptionProperties>,
) -> Result<FileDecryptor> {
    match encryption_algorithm {
        EncryptionAlgorithm::AES_GCM_V1(algo) => {
            let aad_file_unique = algo
                .aad_file_unique
                .ok_or_else(|| general_err!("AAD unique file identifier is not set"))?;
            let aad_prefix = if let Some(aad_prefix) = file_decryption_properties.aad_prefix() {
                aad_prefix.clone()
            } else {
                algo.aad_prefix.map(|v| v.to_vec()).unwrap_or_default()
            };
            let aad_file_unique = aad_file_unique.to_vec();

            FileDecryptor::new(
                file_decryption_properties,
                footer_key_metadata,
                aad_file_unique,
                aad_prefix,
            )
        }
        EncryptionAlgorithm::AES_GCM_CTR_V1(_) => Err(nyi_err!(
            "The AES_GCM_CTR_V1 encryption algorithm is not yet supported"
        )),
    }
}
