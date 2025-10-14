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

// a collection of generated structs used to parse thrift metadata

use std::io::Write;

use crate::{
    basic::{Compression, EncodingMask},
    encryption::decrypt::{FileDecryptionProperties, FileDecryptor},
    errors::{ParquetError, Result},
    file::{
        column_crypto_metadata::ColumnCryptoMetaData,
        metadata::{
            HeapSize, LevelHistogram, PageEncodingStats, ParquetMetaData, RowGroupMetaData,
            thrift_gen::{
                GeospatialStatistics, SizeStatistics, Statistics, convert_geo_stats, convert_stats,
                parquet_metadata_from_bytes,
            },
        },
    },
    parquet_thrift::{
        ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol,
        ThriftCompactOutputProtocol, ThriftSliceInputProtocol, WriteThrift, WriteThriftField,
        read_thrift_vec,
    },
    thrift_struct, thrift_union,
};

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
                    decryptor.get_column_metadata_decryptor(
                        column_name.as_str(),
                        crypto_metadata.key_metadata.as_deref(),
                    )?
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
            let col_meta = read_column_metadata(decrypted_cc_buf.as_slice())?;

            let (
                unencoded_byte_array_data_bytes,
                repetition_level_histogram,
                definition_level_histogram,
            ) = if let Some(size_stats) = col_meta.size_statistics {
                (
                    size_stats.unencoded_byte_array_data_bytes,
                    size_stats.repetition_level_histogram,
                    size_stats.definition_level_histogram,
                )
            } else {
                (None, None, None)
            };

            let repetition_level_histogram = repetition_level_histogram.map(LevelHistogram::from);
            let definition_level_histogram = definition_level_histogram.map(LevelHistogram::from);

            c.encodings = col_meta.encodings;
            c.compression = col_meta.codec;
            c.num_values = col_meta.num_values;
            c.total_uncompressed_size = col_meta.total_uncompressed_size;
            c.total_compressed_size = col_meta.total_compressed_size;
            c.data_page_offset = col_meta.data_page_offset;
            c.index_page_offset = col_meta.index_page_offset;
            c.dictionary_page_offset = col_meta.dictionary_page_offset;
            c.statistics = convert_stats(d.physical_type(), col_meta.statistics)?;
            c.encoding_stats = col_meta.encoding_stats;
            c.bloom_filter_offset = col_meta.bloom_filter_offset;
            c.bloom_filter_length = col_meta.bloom_filter_length;
            c.unencoded_byte_array_data_bytes = unencoded_byte_array_data_bytes;
            c.repetition_level_histogram = repetition_level_histogram;
            c.definition_level_histogram = definition_level_histogram;
            c.geo_statistics = convert_geo_stats(col_meta.geospatial_statistics);

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
    file_decryption_properties: Option<&FileDecryptionProperties>,
    encrypted_footer: bool,
    buf: &[u8],
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

    let parquet_meta = parquet_metadata_from_bytes(buf)
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
        .map(|rg| row_group_from_encrypted_thrift(rg, file_decryptor.as_ref()))
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
    file_decryption_properties: &FileDecryptionProperties,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColumnMetaData<'a> {
    encodings: EncodingMask,
    codec: Compression,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    data_page_offset: i64,
    index_page_offset: Option<i64>,
    dictionary_page_offset: Option<i64>,
    statistics: Option<Statistics<'a>>,
    encoding_stats: Option<Vec<PageEncodingStats>>,
    bloom_filter_offset: Option<i64>,
    bloom_filter_length: Option<i32>,
    size_statistics: Option<SizeStatistics>,
    geospatial_statistics: Option<GeospatialStatistics>,
}

fn read_column_metadata<'a>(buf: &'a [u8]) -> Result<ColumnMetaData<'a>> {
    let mut prot = ThriftSliceInputProtocol::new(buf);

    let mut encodings: Option<EncodingMask> = None;
    let mut codec: Option<Compression> = None;
    let mut num_values: Option<i64> = None;
    let mut total_uncompressed_size: Option<i64> = None;
    let mut total_compressed_size: Option<i64> = None;
    let mut data_page_offset: Option<i64> = None;
    let mut index_page_offset: Option<i64> = None;
    let mut dictionary_page_offset: Option<i64> = None;
    let mut statistics: Option<Statistics> = None;
    let mut encoding_stats: Option<Vec<PageEncodingStats>> = None;
    let mut bloom_filter_offset: Option<i64> = None;
    let mut bloom_filter_length: Option<i32> = None;
    let mut size_statistics: Option<SizeStatistics> = None;
    let mut geospatial_statistics: Option<GeospatialStatistics> = None;

    // `ColumnMetaData`. Read inline for performance sake.
    // struct ColumnMetaData {
    //   1: required Type type
    //   2: required list<Encoding> encodings
    //   3: required list<string> path_in_schema
    //   4: required CompressionCodec codec
    //   5: required i64 num_values
    //   6: required i64 total_uncompressed_size
    //   7: required i64 total_compressed_size
    //   8: optional list<KeyValue> key_value_metadata
    //   9: required i64 data_page_offset
    //   10: optional i64 index_page_offset
    //   11: optional i64 dictionary_page_offset
    //   12: optional Statistics statistics;
    //   13: optional list<PageEncodingStats> encoding_stats;
    //   14: optional i64 bloom_filter_offset;
    //   15: optional i32 bloom_filter_length;
    //   16: optional SizeStatistics size_statistics;
    //   17: optional GeospatialStatistics geospatial_statistics;
    // }
    let mut last_field_id = 0i16;
    loop {
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        match field_ident.id {
            // 1: type is never used, we can use the column descriptor
            2 => {
                let val = EncodingMask::read_thrift(&mut prot)?;
                encodings = Some(val);
            }
            // 3: path_in_schema is redundant
            4 => {
                codec = Some(Compression::read_thrift(&mut prot)?);
            }
            5 => {
                num_values = Some(i64::read_thrift(&mut prot)?);
            }
            6 => {
                total_uncompressed_size = Some(i64::read_thrift(&mut prot)?);
            }
            7 => {
                total_compressed_size = Some(i64::read_thrift(&mut prot)?);
            }
            // 8: we don't expose this key value
            9 => {
                data_page_offset = Some(i64::read_thrift(&mut prot)?);
            }
            10 => {
                index_page_offset = Some(i64::read_thrift(&mut prot)?);
            }
            11 => {
                dictionary_page_offset = Some(i64::read_thrift(&mut prot)?);
            }
            12 => {
                statistics = Some(Statistics::read_thrift(&mut prot)?);
            }
            13 => {
                let val =
                    read_thrift_vec::<PageEncodingStats, ThriftSliceInputProtocol>(&mut prot)?;
                encoding_stats = Some(val);
            }
            14 => {
                bloom_filter_offset = Some(i64::read_thrift(&mut prot)?);
            }
            15 => {
                bloom_filter_length = Some(i32::read_thrift(&mut prot)?);
            }
            16 => {
                let val = SizeStatistics::read_thrift(&mut prot)?;
                size_statistics = Some(val);
            }
            17 => {
                let val = GeospatialStatistics::read_thrift(&mut prot)?;
                geospatial_statistics = Some(val);
            }
            _ => {
                prot.skip(field_ident.field_type)?;
            }
        };
        last_field_id = field_ident.id;
    }

    let Some(encodings) = encodings else {
        return Err(ParquetError::General(
            "Required field encodings is missing".to_owned(),
        ));
    };
    let Some(codec) = codec else {
        return Err(ParquetError::General(
            "Required field codec is missing".to_owned(),
        ));
    };
    let Some(num_values) = num_values else {
        return Err(ParquetError::General(
            "Required field num_values is missing".to_owned(),
        ));
    };
    let Some(total_uncompressed_size) = total_uncompressed_size else {
        return Err(ParquetError::General(
            "Required field total_uncompressed_size is missing".to_owned(),
        ));
    };
    let Some(total_compressed_size) = total_compressed_size else {
        return Err(ParquetError::General(
            "Required field total_compressed_size is missing".to_owned(),
        ));
    };
    let Some(data_page_offset) = data_page_offset else {
        return Err(ParquetError::General(
            "Required field data_page_offset is missing".to_owned(),
        ));
    };

    Ok(ColumnMetaData {
        encodings,
        num_values,
        codec,
        total_uncompressed_size,
        total_compressed_size,
        data_page_offset,
        index_page_offset,
        dictionary_page_offset,
        statistics,
        encoding_stats,
        bloom_filter_offset,
        bloom_filter_length,
        size_statistics,
        geospatial_statistics,
    })
}
