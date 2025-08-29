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
use std::sync::Arc;

use crate::{
    basic::{
        ColumnOrder, Compression, ConvertedType, Encoding, LogicalType, PageType, Repetition, Type,
    },
    data_type::{ByteArray, FixedLenByteArray, Int96},
    errors::{ParquetError, Result},
    file::{
        metadata::{
            ColumnChunkMetaData, KeyValue, LevelHistogram, ParquetMetaData, RowGroupMetaData,
            SortingColumn,
        },
        page_encoding_stats::PageEncodingStats,
        statistics::ValueStatistics,
    },
    parquet_thrift::{
        read_thrift_vec, ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol,
        ThriftCompactOutputProtocol, WriteThrift, WriteThriftField,
    },
    schema::types::{parquet_schema_from_array, ColumnDescriptor, SchemaDescriptor},
    thrift_struct, thrift_struct_write_impl, thrift_union,
    util::bit_util::FromBytes,
};
#[cfg(feature = "encryption")]
use crate::{
    encryption::decrypt::{FileDecryptionProperties, FileDecryptor},
    file::column_crypto_metadata::ColumnCryptoMetaData,
    parquet_thrift::ThriftSliceInputProtocol,
    schema::types::SchemaDescPtr,
};

// this needs to be visible to the schema conversion code
thrift_struct!(
pub(crate) struct SchemaElement<'a> {
  /** Data type for this field. Not set if the current element is a non-leaf node */
  1: optional Type type_;
  2: optional i32 type_length;
  3: optional Repetition repetition_type;
  4: required string<'a> name;
  5: optional i32 num_children;
  6: optional ConvertedType converted_type;
  7: optional i32 scale
  8: optional i32 precision
  9: optional i32 field_id;
  10: optional LogicalType logical_type
}
);

thrift_struct!(
pub(crate) struct AesGcmV1<'a> {
  /// AAD prefix
  1: optional binary<'a> aad_prefix

  /// Unique file identifier part of AAD suffix
  2: optional binary<'a> aad_file_unique

  /// In files encrypted with AAD prefix without storing it,
  /// readers must supply the prefix
  3: optional bool supply_aad_prefix
}
);

thrift_struct!(
pub(crate) struct AesGcmCtrV1<'a> {
  /// AAD prefix
  1: optional binary<'a> aad_prefix

  /// Unique file identifier part of AAD suffix
  2: optional binary<'a> aad_file_unique

  /// In files encrypted with AAD prefix without storing it,
  /// readers must supply the prefix
  3: optional bool supply_aad_prefix
}
);

thrift_union!(
union EncryptionAlgorithm<'a> {
  1: (AesGcmV1<'a>) AES_GCM_V1
  2: (AesGcmCtrV1<'a>) AES_GCM_CTR_V1
}
);

#[cfg(feature = "encryption")]
thrift_struct!(
/// Crypto metadata for files with encrypted footer
pub(crate) struct FileCryptoMetaData<'a> {
  /// Encryption algorithm. This field is only used for files
  /// with encrypted footer. Files with plaintext footer store algorithm id
  /// inside footer (FileMetaData structure).
  1: required EncryptionAlgorithm<'a> encryption_algorithm

  /** Retrieval metadata of key used for encryption of footer,
   *  and (possibly) columns **/
  2: optional binary<'a> key_metadata
}
);

// the following are only used internally so are private
thrift_struct!(
struct FileMetaData<'a> {
  /** Version of this file **/
  1: required i32 version
  2: required list<'a><SchemaElement> schema;
  3: required i64 num_rows
  4: required list<'a><RowGroup> row_groups
  5: optional list<KeyValue> key_value_metadata
  6: optional string created_by
  7: optional list<ColumnOrder> column_orders;
  8: optional EncryptionAlgorithm<'a> encryption_algorithm
  9: optional binary<'a> footer_signing_key_metadata
}
);

thrift_struct!(
struct RowGroup<'a> {
  1: required list<'a><ColumnChunk> columns
  2: required i64 total_byte_size
  3: required i64 num_rows
  4: optional list<SortingColumn> sorting_columns
  5: optional i64 file_offset
  // we don't expose total_compressed_size so skip
  //6: optional i64 total_compressed_size
  7: optional i16 ordinal
}
);

#[cfg(feature = "encryption")]
thrift_struct!(
struct ColumnChunk<'a> {
  1: optional string<'a> file_path
  2: required i64 file_offset = 0
  3: optional ColumnMetaData<'a> meta_data
  4: optional i64 offset_index_offset
  5: optional i32 offset_index_length
  6: optional i64 column_index_offset
  7: optional i32 column_index_length
  8: optional ColumnCryptoMetaData crypto_metadata
  9: optional binary<'a> encrypted_column_metadata
}
);
#[cfg(not(feature = "encryption"))]
thrift_struct!(
struct ColumnChunk<'a> {
  1: optional string file_path
  2: required i64 file_offset = 0
  3: optional ColumnMetaData<'a> meta_data
  4: optional i64 offset_index_offset
  5: optional i32 offset_index_length
  6: optional i64 column_index_offset
  7: optional i32 column_index_length
}
);

type CompressionCodec = Compression;
thrift_struct!(
struct ColumnMetaData<'a> {
  1: required Type type_
  2: required list<Encoding> encodings
  // we don't expose path_in_schema so skip
  //3: required list<string> path_in_schema
  4: required CompressionCodec codec
  5: required i64 num_values
  6: required i64 total_uncompressed_size
  7: required i64 total_compressed_size
  // we don't expose key_value_metadata so skip
  //8: optional list<KeyValue> key_value_metadata
  9: required i64 data_page_offset
  10: optional i64 index_page_offset
  11: optional i64 dictionary_page_offset
  12: optional Statistics<'a> statistics
  13: optional list<PageEncodingStats> encoding_stats;
  14: optional i64 bloom_filter_offset;
  15: optional i32 bloom_filter_length;
  16: optional SizeStatistics size_statistics;
  17: optional GeospatialStatistics geospatial_statistics;
}
);

thrift_struct!(
struct BoundingBox {
  1: required double xmin;
  2: required double xmax;
  3: required double ymin;
  4: required double ymax;
  5: optional double zmin;
  6: optional double zmax;
  7: optional double mmin;
  8: optional double mmax;
}
);

thrift_struct!(
struct GeospatialStatistics {
  /** A bounding box of geospatial instances */
  1: optional BoundingBox bbox;
  /** Geospatial type codes of all instances, or an empty list if not known */
  2: optional list<i32> geospatial_types;
}
);

thrift_struct!(
struct SizeStatistics {
   1: optional i64 unencoded_byte_array_data_bytes;
   2: optional list<i64> repetition_level_histogram;
   3: optional list<i64> definition_level_histogram;
}
);

thrift_struct!(
pub(crate) struct Statistics<'a> {
   1: optional binary<'a> max;
   2: optional binary<'a> min;
   3: optional i64 null_count;
   4: optional i64 distinct_count;
   5: optional binary<'a> max_value;
   6: optional binary<'a> min_value;
   7: optional bool is_max_value_exact;
   8: optional bool is_min_value_exact;
}
);

// convert collection of thrift RowGroups into RowGroupMetaData
fn convert_row_groups(
    mut row_groups: Vec<RowGroup>,
    schema_descr: Arc<SchemaDescriptor>,
) -> Result<Vec<RowGroupMetaData>> {
    let mut res: Vec<RowGroupMetaData> = Vec::with_capacity(row_groups.len());
    for rg in row_groups.drain(0..) {
        res.push(convert_row_group(rg, schema_descr.clone())?);
    }

    Ok(res)
}

fn convert_row_group(
    row_group: RowGroup,
    schema_descr: Arc<SchemaDescriptor>,
) -> Result<RowGroupMetaData> {
    let num_rows = row_group.num_rows;
    let sorting_columns = row_group.sorting_columns;
    let total_byte_size = row_group.total_byte_size;
    let file_offset = row_group.file_offset;
    let ordinal = row_group.ordinal;

    let columns = convert_columns(row_group.columns, schema_descr.clone())?;

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

fn convert_columns(
    mut columns: Vec<ColumnChunk>,
    schema_descr: Arc<SchemaDescriptor>,
) -> Result<Vec<ColumnChunkMetaData>> {
    let mut res: Vec<ColumnChunkMetaData> = Vec::with_capacity(columns.len());
    for (c, d) in columns.drain(0..).zip(schema_descr.columns()) {
        res.push(convert_column(c, d.clone())?);
    }

    Ok(res)
}

fn convert_column(
    column: ColumnChunk,
    column_descr: Arc<ColumnDescriptor>,
) -> Result<ColumnChunkMetaData> {
    if column.meta_data.is_none() {
        return Err(general_err!("Expected to have column metadata"));
    }
    let col_metadata = column.meta_data.unwrap();
    let column_type = col_metadata.type_;
    let encodings = col_metadata.encodings;
    let compression = col_metadata.codec;
    let file_path = column.file_path.map(|v| v.to_owned());
    let file_offset = column.file_offset;
    let num_values = col_metadata.num_values;
    let total_compressed_size = col_metadata.total_compressed_size;
    let total_uncompressed_size = col_metadata.total_uncompressed_size;
    let data_page_offset = col_metadata.data_page_offset;
    let index_page_offset = col_metadata.index_page_offset;
    let dictionary_page_offset = col_metadata.dictionary_page_offset;
    let statistics = convert_stats(column_type, col_metadata.statistics)?;
    let encoding_stats = col_metadata.encoding_stats;
    let bloom_filter_offset = col_metadata.bloom_filter_offset;
    let bloom_filter_length = col_metadata.bloom_filter_length;
    let offset_index_offset = column.offset_index_offset;
    let offset_index_length = column.offset_index_length;
    let column_index_offset = column.column_index_offset;
    let column_index_length = column.column_index_length;
    let (unencoded_byte_array_data_bytes, repetition_level_histogram, definition_level_histogram) =
        if let Some(size_stats) = col_metadata.size_statistics {
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

    // FIXME: need column crypto

    let result = ColumnChunkMetaData {
        column_descr,
        encodings,
        file_path,
        file_offset,
        num_values,
        compression,
        total_compressed_size,
        total_uncompressed_size,
        data_page_offset,
        index_page_offset,
        dictionary_page_offset,
        statistics,
        encoding_stats,
        bloom_filter_offset,
        bloom_filter_length,
        offset_index_offset,
        offset_index_length,
        column_index_offset,
        column_index_length,
        unencoded_byte_array_data_bytes,
        repetition_level_histogram,
        definition_level_histogram,
        #[cfg(feature = "encryption")]
        column_crypto_metadata: column.crypto_metadata,
    };
    Ok(result)
}

pub(crate) fn convert_stats(
    physical_type: Type,
    thrift_stats: Option<Statistics>,
) -> Result<Option<crate::file::statistics::Statistics>> {
    use crate::file::statistics::Statistics as FStatistics;
    Ok(match thrift_stats {
        Some(stats) => {
            // Number of nulls recorded, when it is not available, we just mark it as 0.
            // TODO this should be `None` if there is no information about NULLS.
            // see https://github.com/apache/arrow-rs/pull/6216/files
            let null_count = stats.null_count.unwrap_or(0);

            if null_count < 0 {
                return Err(ParquetError::General(format!(
                    "Statistics null count is negative {null_count}",
                )));
            }

            // Generic null count.
            let null_count = Some(null_count as u64);
            // Generic distinct count (count of distinct values occurring)
            let distinct_count = stats.distinct_count.map(|value| value as u64);
            // Whether or not statistics use deprecated min/max fields.
            let old_format = stats.min_value.is_none() && stats.max_value.is_none();
            // Generic min value as bytes.
            let min = if old_format {
                stats.min
            } else {
                stats.min_value
            };
            // Generic max value as bytes.
            let max = if old_format {
                stats.max
            } else {
                stats.max_value
            };

            fn check_len(min: &Option<&[u8]>, max: &Option<&[u8]>, len: usize) -> Result<()> {
                if let Some(min) = min {
                    if min.len() < len {
                        return Err(ParquetError::General(
                            "Insufficient bytes to parse min statistic".to_string(),
                        ));
                    }
                }
                if let Some(max) = max {
                    if max.len() < len {
                        return Err(ParquetError::General(
                            "Insufficient bytes to parse max statistic".to_string(),
                        ));
                    }
                }
                Ok(())
            }

            match physical_type {
                Type::BOOLEAN => check_len(&min, &max, 1),
                Type::INT32 | Type::FLOAT => check_len(&min, &max, 4),
                Type::INT64 | Type::DOUBLE => check_len(&min, &max, 8),
                Type::INT96 => check_len(&min, &max, 12),
                _ => Ok(()),
            }?;

            // Values are encoded using PLAIN encoding definition, except that
            // variable-length byte arrays do not include a length prefix.
            //
            // Instead of using actual decoder, we manually convert values.
            let res = match physical_type {
                Type::BOOLEAN => FStatistics::boolean(
                    min.map(|data| data[0] != 0),
                    max.map(|data| data[0] != 0),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT32 => FStatistics::int32(
                    min.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT64 => FStatistics::int64(
                    min.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT96 => {
                    // INT96 statistics may not be correct, because comparison is signed
                    let min = if let Some(data) = min {
                        assert_eq!(data.len(), 12);
                        Some(Int96::try_from_le_slice(data)?)
                    } else {
                        None
                    };
                    let max = if let Some(data) = max {
                        assert_eq!(data.len(), 12);
                        Some(Int96::try_from_le_slice(data)?)
                    } else {
                        None
                    };
                    FStatistics::int96(min, max, distinct_count, null_count, old_format)
                }
                Type::FLOAT => FStatistics::float(
                    min.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::DOUBLE => FStatistics::double(
                    min.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::BYTE_ARRAY => FStatistics::ByteArray(
                    ValueStatistics::new(
                        min.map(ByteArray::from),
                        max.map(ByteArray::from),
                        distinct_count,
                        null_count,
                        old_format,
                    )
                    .with_max_is_exact(stats.is_max_value_exact.unwrap_or(false))
                    .with_min_is_exact(stats.is_min_value_exact.unwrap_or(false)),
                ),
                Type::FIXED_LEN_BYTE_ARRAY => FStatistics::FixedLenByteArray(
                    ValueStatistics::new(
                        min.map(ByteArray::from).map(FixedLenByteArray::from),
                        max.map(ByteArray::from).map(FixedLenByteArray::from),
                        distinct_count,
                        null_count,
                        old_format,
                    )
                    .with_max_is_exact(stats.is_max_value_exact.unwrap_or(false))
                    .with_min_is_exact(stats.is_min_value_exact.unwrap_or(false)),
                ),
            };

            Some(res)
        }
        None => None,
    })
}

#[cfg(feature = "encryption")]
fn row_group_from_encrypted_thrift(
    mut rg: RowGroup,
    schema_descr: SchemaDescPtr,
    decryptor: Option<&FileDecryptor>,
) -> Result<RowGroupMetaData> {
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
            let column_decryptor = match c.crypto_metadata.as_ref() {
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

            let buf = c.encrypted_column_metadata.unwrap();
            let decrypted_cc_buf =
                column_decryptor
                    .decrypt(buf, column_aad.as_ref())
                    .map_err(|_| {
                        general_err!(
                            "Unable to decrypt column '{}', perhaps the column key is wrong?",
                            d.path().string()
                        )
                    })?;

            let mut prot = ThriftSliceInputProtocol::new(decrypted_cc_buf.as_slice());
            let col_meta = ColumnMetaData::read_thrift(&mut prot)?;
            c.meta_data = Some(col_meta);
            columns.push(convert_column(c, d.clone())?);
        } else {
            columns.push(convert_column(c, d.clone())?);
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

#[cfg(feature = "encryption")]
pub(crate) fn parquet_metadata_with_encryption(
    file_decryption_properties: Option<&FileDecryptionProperties>,
    encrypted_footer: bool,
    buf: &[u8],
) -> Result<ParquetMetaData> {
    let mut prot = ThriftSliceInputProtocol::new(buf);
    let mut file_decryptor = None;
    let decrypted_fmd_buf;

    if encrypted_footer {
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
            prot = ThriftSliceInputProtocol::new(decrypted_fmd_buf.as_ref());

            file_decryptor = Some(decryptor);
        } else {
            return Err(general_err!(
                "Parquet file has an encrypted footer but decryption properties were not provided"
            ));
        }
    }

    let file_meta = super::thrift_gen::FileMetaData::read_thrift(&mut prot)
        .map_err(|e| general_err!("Could not parse metadata: {}", e))?;

    let version = file_meta.version;
    let num_rows = file_meta.num_rows;
    let created_by = file_meta.created_by.map(|c| c.to_owned());
    let key_value_metadata = file_meta.key_value_metadata;

    let val = parquet_schema_from_array(file_meta.schema)?;
    let schema_descr = Arc::new(SchemaDescriptor::new(val));

    if let (Some(algo), Some(file_decryption_properties)) =
        (file_meta.encryption_algorithm, file_decryption_properties)
    {
        // File has a plaintext footer but encryption algorithm is set
        let file_decryptor_value = get_file_decryptor(
            algo,
            file_meta.footer_signing_key_metadata,
            file_decryption_properties,
        )?;
        if file_decryption_properties.check_plaintext_footer_integrity() && !encrypted_footer {
            file_decryptor_value.verify_plaintext_footer_signature(buf)?;
        }
        file_decryptor = Some(file_decryptor_value);
    }

    // decrypt column chunk info
    let mut row_groups = Vec::with_capacity(file_meta.row_groups.len());
    for rg in file_meta.row_groups {
        let r = row_group_from_encrypted_thrift(rg, schema_descr.clone(), file_decryptor.as_ref())?;
        row_groups.push(r);
    }

    // need to map read column orders to actual values based on the schema
    if file_meta
        .column_orders
        .as_ref()
        .is_some_and(|cos| cos.len() != schema_descr.num_columns())
    {
        return Err(general_err!("Column order length mismatch"));
    }

    let column_orders = file_meta.column_orders.map(|cos| {
        let mut res = Vec::with_capacity(cos.len());
        for (i, column) in schema_descr.columns().iter().enumerate() {
            match cos[i] {
                ColumnOrder::TYPE_DEFINED_ORDER(_) => {
                    let sort_order = ColumnOrder::get_sort_order(
                        column.logical_type(),
                        column.converted_type(),
                        column.physical_type(),
                    );
                    res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                }
                _ => res.push(cos[i]),
            }
        }
        res
    });

    let fmd = crate::file::metadata::FileMetaData::new(
        version,
        num_rows,
        created_by,
        key_value_metadata,
        schema_descr,
        column_orders,
    );
    let mut metadata = ParquetMetaData::new(fmd, row_groups);

    metadata.with_file_decryptor(file_decryptor);

    Ok(metadata)
}

#[cfg(feature = "encryption")]
pub(super) fn get_file_decryptor(
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

/// Create ParquetMetaData from thrift input. Note that this only decodes the file metadata in
/// the Parquet footer. Page indexes will need to be added later.
impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for ParquetMetaData {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        let file_meta = super::thrift_gen::FileMetaData::read_thrift(prot)?;

        let version = file_meta.version;
        let num_rows = file_meta.num_rows;
        let row_groups = file_meta.row_groups;
        let created_by = file_meta.created_by.map(|c| c.to_owned());
        let key_value_metadata = file_meta.key_value_metadata;

        let val = parquet_schema_from_array(file_meta.schema)?;
        let schema_descr = Arc::new(SchemaDescriptor::new(val));

        // need schema_descr to get final RowGroupMetaData
        let row_groups = convert_row_groups(row_groups, schema_descr.clone())?;

        // need to map read column orders to actual values based on the schema
        if file_meta
            .column_orders
            .as_ref()
            .is_some_and(|cos| cos.len() != schema_descr.num_columns())
        {
            return Err(general_err!("Column order length mismatch"));
        }

        let column_orders = file_meta.column_orders.map(|cos| {
            let mut res = Vec::with_capacity(cos.len());
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match cos[i] {
                    ColumnOrder::TYPE_DEFINED_ORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                    _ => res.push(cos[i]),
                }
            }
            res
        });

        let fmd = crate::file::metadata::FileMetaData::new(
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
        );

        Ok(ParquetMetaData::new(fmd, row_groups))
    }
}

// page header stuff. this is partially hand coded so we can avoid parsing the page statistics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DataPageHeader<'a> {
    pub(crate) num_values: i32,
    pub(crate) encoding: Encoding,
    pub(crate) definition_level_encoding: Encoding,
    pub(crate) repetition_level_encoding: Encoding,
    // this will only be used on write
    pub(crate) statistics: Option<Statistics<'a>>,
}

thrift_struct_write_impl!(
struct DataPageHeader<'a> {
  1: required i32 num_values
  2: required Encoding encoding
  3: required Encoding definition_level_encoding;
  4: required Encoding repetition_level_encoding;
  5: optional Statistics<'a> statistics;
}
);

// read data page header but skip statistics
impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for DataPageHeader<'a> {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        let mut num_values: Option<i32> = None;
        let mut encoding: Option<Encoding> = None;
        let mut definition_level_encoding: Option<Encoding> = None;
        let mut repetition_level_encoding: Option<Encoding> = None;

        let mut last_field_id = 0i16;
        loop {
            let field_ident = prot.read_field_begin(last_field_id)?;
            if field_ident.field_type == FieldType::Stop {
                break;
            }
            match field_ident.id {
                1 => num_values = Some(prot.read_i32()?),
                2 => encoding = Some(Encoding::read_thrift(prot)?),
                3 => definition_level_encoding = Some(Encoding::read_thrift(prot)?),
                4 => repetition_level_encoding = Some(Encoding::read_thrift(prot)?),
                _ => {
                    prot.skip(field_ident.field_type)?;
                }
            };
            last_field_id = field_ident.id;
        }

        let num_values = num_values.expect("Required field num_values is missing");
        let encoding = encoding.expect("Required field encoding is missing");
        let definition_level_encoding =
            definition_level_encoding.expect("Required field definition_level_encoding is missing");
        let repetition_level_encoding =
            repetition_level_encoding.expect("Required field repetition_level_encoding is missing");

        Ok(Self {
            num_values,
            encoding,
            definition_level_encoding,
            repetition_level_encoding,
            statistics: None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DataPageHeaderV2<'a> {
    pub(crate) num_values: i32,
    pub(crate) num_nulls: i32,
    pub(crate) num_rows: i32,
    pub(crate) encoding: Encoding,
    pub(crate) definition_levels_byte_length: i32,
    pub(crate) repetition_levels_byte_length: i32,
    pub(crate) is_compressed: Option<bool>,
    // this will only be used on write
    pub(crate) statistics: Option<Statistics<'a>>,
}

thrift_struct_write_impl!(
struct DataPageHeaderV2<'a> {
  1: required i32 num_values
  2: required i32 num_nulls
  3: required i32 num_rows
  4: required Encoding encoding
  5: required i32 definition_levels_byte_length;
  6: required i32 repetition_levels_byte_length;
  7: optional bool is_compressed = true;
  8: optional Statistics<'a> statistics;
}
);

// read data page header but skip statistics
impl<'a, R: ThriftCompactInputProtocol<'a>> ReadThrift<'a, R> for DataPageHeaderV2<'a> {
    fn read_thrift(prot: &mut R) -> Result<Self> {
        let mut num_values: Option<i32> = None;
        let mut num_nulls: Option<i32> = None;
        let mut num_rows: Option<i32> = None;
        let mut encoding: Option<Encoding> = None;
        let mut definition_levels_byte_length: Option<i32> = None;
        let mut repetition_levels_byte_length: Option<i32> = None;
        let mut is_compressed: Option<bool> = Some(true);

        let mut last_field_id = 0i16;
        loop {
            let field_ident = prot.read_field_begin(last_field_id)?;
            if field_ident.field_type == FieldType::Stop {
                break;
            }
            match field_ident.id {
                1 => num_values = Some(prot.read_i32()?),
                2 => num_nulls = Some(prot.read_i32()?),
                3 => num_rows = Some(prot.read_i32()?),
                4 => encoding = Some(Encoding::read_thrift(prot)?),
                5 => definition_levels_byte_length = Some(prot.read_i32()?),
                6 => repetition_levels_byte_length = Some(prot.read_i32()?),
                7 => is_compressed = field_ident.bool_val,
                _ => {
                    prot.skip(field_ident.field_type)?;
                }
            };
            last_field_id = field_ident.id;
        }

        let num_values = num_values.expect("Required field num_values is missing");
        let num_nulls = num_nulls.expect("Required field num_nulls is missing");
        let num_rows = num_rows.expect("Required field num_rows is missing");
        let encoding = encoding.expect("Required field encoding is missing");
        let definition_levels_byte_length = definition_levels_byte_length
            .expect("Required field definition_levels_byte_length is missing");
        let repetition_levels_byte_length = repetition_levels_byte_length
            .expect("Required field repetition_levels_byte_length is missing");

        Ok(Self {
            num_values,
            num_nulls,
            num_rows,
            encoding,
            definition_levels_byte_length,
            repetition_levels_byte_length,
            is_compressed,
            statistics: None,
        })
    }
}

thrift_struct!(
    pub(crate) struct IndexPageHeader {}
);

thrift_struct!(
pub(crate) struct DictionaryPageHeader {
  /// Number of values in the dictionary
  1: required i32 num_values;

  /// Encoding using this dictionary page
  2: required Encoding encoding

  /// If true, the entries in the dictionary are sorted in ascending order
  3: optional bool is_sorted;
}
);

thrift_struct!(
#[allow(dead_code)]
pub(crate) struct PageHeader<'a> {
  /// the type of the page: indicates which of the *_header fields is set
  1: required PageType type_

  /// Uncompressed page size in bytes (not including this header)
  2: required i32 uncompressed_page_size

  /// Compressed (and potentially encrypted) page size in bytes, not including this header
  3: required i32 compressed_page_size

  /// The 32-bit CRC checksum for the page, to be be calculated as follows:
  4: optional i32 crc

  // Headers for page specific data.  One only will be set.
  5: optional DataPageHeader<'a> data_page_header;
  6: optional IndexPageHeader index_page_header;
  7: optional DictionaryPageHeader dictionary_page_header;
  8: optional DataPageHeaderV2<'a> data_page_header_v2;
}
);


#[cfg(test)]
mod tests {
    use crate::file::metadata::thrift_gen::BoundingBox;
    use crate::parquet_thrift::tests::test_roundtrip;

    #[test]
    fn test_bounding_box_roundtrip() {
        test_roundtrip(BoundingBox {
            xmin: 0.1.into(),
            xmax: 10.3.into(),
            ymin: 0.001.into(),
            ymax: 128.5.into(),
            zmin: None,
            zmax: None,
            mmin: None,
            mmax: None,
        });

        test_roundtrip(BoundingBox {
            xmin: 0.1.into(),
            xmax: 10.3.into(),
            ymin: 0.001.into(),
            ymax: 128.5.into(),
            zmin: Some(11.0.into()),
            zmax: Some(1300.0.into()),
            mmin: None,
            mmax: None,
        });

        test_roundtrip(BoundingBox {
            xmin: 0.1.into(),
            xmax: 10.3.into(),
            ymin: 0.001.into(),
            ymax: 128.5.into(),
            zmin: Some(11.0.into()),
            zmax: Some(1300.0.into()),
            mmin: Some(3.7.into()),
            mmax: Some(42.0.into()),
        });
    }
}