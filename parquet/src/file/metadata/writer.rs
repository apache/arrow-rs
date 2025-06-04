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

#[cfg(feature = "encryption")]
use crate::encryption::{
    encrypt::{
        encrypt_object, encrypt_object_to_vec, write_signed_plaintext_object, FileEncryptor,
    },
    modules::{create_footer_aad, create_module_aad, ModuleType},
};
#[cfg(feature = "encryption")]
use crate::errors::ParquetError;
use crate::errors::Result;
use crate::file::metadata::{KeyValue, ParquetMetaData};
use crate::file::page_index::index::Index;
use crate::file::writer::{get_file_magic, TrackedWrite};
use crate::format::EncryptionAlgorithm;
#[cfg(feature = "encryption")]
use crate::format::{AesGcmV1, ColumnCryptoMetaData};
use crate::format::{ColumnChunk, ColumnIndex, FileMetaData, OffsetIndex, RowGroup};
use crate::schema::types;
use crate::schema::types::{SchemaDescPtr, SchemaDescriptor, TypePtr};
use crate::thrift::TSerializable;
use std::io::Write;
use std::sync::Arc;
use thrift::protocol::TCompactOutputProtocol;

/// Writes `crate::file::metadata` structures to a thrift encoded byte stream
///
/// See [`ParquetMetaDataWriter`] for background and example.
pub(crate) struct ThriftMetadataWriter<'a, W: Write> {
    buf: &'a mut TrackedWrite<W>,
    schema: &'a TypePtr,
    schema_descr: &'a SchemaDescPtr,
    row_groups: Vec<RowGroup>,
    column_indexes: Option<&'a [Vec<Option<ColumnIndex>>]>,
    offset_indexes: Option<&'a [Vec<Option<OffsetIndex>>]>,
    key_value_metadata: Option<Vec<KeyValue>>,
    created_by: Option<String>,
    object_writer: MetadataObjectWriter,
    writer_version: i32,
}

impl<'a, W: Write> ThriftMetadataWriter<'a, W> {
    /// Serialize all the offset indexes to `self.buf`,
    ///
    /// Note: also updates the `ColumnChunk::offset_index_offset` and
    /// `ColumnChunk::offset_index_length` to reflect the position and length
    /// of the serialized offset indexes.
    fn write_offset_indexes(&mut self, offset_indexes: &[Vec<Option<OffsetIndex>>]) -> Result<()> {
        // iter row group
        // iter each column
        // write offset index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(offset_index) = &offset_indexes[row_group_idx][column_idx] {
                    let start_offset = self.buf.bytes_written();
                    self.object_writer.write_offset_index(
                        offset_index,
                        column_metadata,
                        row_group_idx,
                        column_idx,
                        &mut self.buf,
                    )?;
                    let end_offset = self.buf.bytes_written();
                    // set offset and index for offset index
                    column_metadata.offset_index_offset = Some(start_offset as i64);
                    column_metadata.offset_index_length = Some((end_offset - start_offset) as i32);
                }
            }
        }
        Ok(())
    }

    /// Serialize all the column indexes to the `self.buf`
    ///
    /// Note: also updates the `ColumnChunk::column_index_offset` and
    /// `ColumnChunk::column_index_length` to reflect the position and length
    /// of the serialized column indexes.
    fn write_column_indexes(&mut self, column_indexes: &[Vec<Option<ColumnIndex>>]) -> Result<()> {
        // iter row group
        // iter each column
        // write column index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(column_index) = &column_indexes[row_group_idx][column_idx] {
                    let start_offset = self.buf.bytes_written();
                    self.object_writer.write_column_index(
                        column_index,
                        column_metadata,
                        row_group_idx,
                        column_idx,
                        &mut self.buf,
                    )?;
                    let end_offset = self.buf.bytes_written();
                    // set offset and index for offset index
                    column_metadata.column_index_offset = Some(start_offset as i64);
                    column_metadata.column_index_length = Some((end_offset - start_offset) as i32);
                }
            }
        }
        Ok(())
    }

    /// Assembles and writes the final metadata to self.buf
    pub fn finish(mut self) -> Result<crate::format::FileMetaData> {
        let num_rows = self.row_groups.iter().map(|x| x.num_rows).sum();

        // Write column indexes and offset indexes
        if let Some(column_indexes) = self.column_indexes {
            self.write_column_indexes(column_indexes)?;
        }
        if let Some(offset_indexes) = self.offset_indexes {
            self.write_offset_indexes(offset_indexes)?;
        }

        // We only include ColumnOrder for leaf nodes.
        // Currently only supported ColumnOrder is TypeDefinedOrder so we set this
        // for all leaf nodes.
        // Even if the column has an undefined sort order, such as INTERVAL, this
        // is still technically the defined TYPEORDER so it should still be set.
        let column_orders = (0..self.schema_descr.num_columns())
            .map(|_| crate::format::ColumnOrder::TYPEORDER(crate::format::TypeDefinedOrder {}))
            .collect();
        // This field is optional, perhaps in cases where no min/max fields are set
        // in any Statistics or ColumnIndex object in the whole file.
        // But for simplicity we always set this field.
        let column_orders = Some(column_orders);

        let (row_groups, unencrypted_row_groups) = self
            .object_writer
            .apply_row_group_encryption(self.row_groups)?;

        let mut file_metadata = FileMetaData {
            num_rows,
            row_groups,
            key_value_metadata: self.key_value_metadata.clone(),
            version: self.writer_version,
            schema: types::to_thrift(self.schema.as_ref())?,
            created_by: self.created_by.clone(),
            column_orders,
            encryption_algorithm: self.object_writer.get_footer_encryption_algorithm(),
            footer_signing_key_metadata: None,
        };

        // Write file metadata
        let start_pos = self.buf.bytes_written();
        self.object_writer
            .write_file_metadata(&file_metadata, &mut self.buf)?;
        let end_pos = self.buf.bytes_written();

        // Write footer
        let metadata_len = (end_pos - start_pos) as u32;

        self.buf.write_all(&metadata_len.to_le_bytes())?;
        self.buf.write_all(self.object_writer.get_file_magic())?;

        if let Some(row_groups) = unencrypted_row_groups {
            // If row group metadata was encrypted, we replace the encrypted row groups with
            // unencrypted metadata before it is returned to users. This allows the metadata
            // to be usable for retrieving the row group statistics for example, without users
            // needing to decrypt the metadata.
            file_metadata.row_groups = row_groups;
        }

        Ok(file_metadata)
    }

    pub fn new(
        buf: &'a mut TrackedWrite<W>,
        schema: &'a TypePtr,
        schema_descr: &'a SchemaDescPtr,
        row_groups: Vec<RowGroup>,
        created_by: Option<String>,
        writer_version: i32,
    ) -> Self {
        Self {
            buf,
            schema,
            schema_descr,
            row_groups,
            column_indexes: None,
            offset_indexes: None,
            key_value_metadata: None,
            created_by,
            object_writer: Default::default(),
            writer_version,
        }
    }

    pub fn with_column_indexes(mut self, column_indexes: &'a [Vec<Option<ColumnIndex>>]) -> Self {
        self.column_indexes = Some(column_indexes);
        self
    }

    pub fn with_offset_indexes(mut self, offset_indexes: &'a [Vec<Option<OffsetIndex>>]) -> Self {
        self.offset_indexes = Some(offset_indexes);
        self
    }

    pub fn with_key_value_metadata(mut self, key_value_metadata: Vec<KeyValue>) -> Self {
        self.key_value_metadata = Some(key_value_metadata);
        self
    }

    #[cfg(feature = "encryption")]
    pub fn with_file_encryptor(mut self, file_encryptor: Option<Arc<FileEncryptor>>) -> Self {
        self.object_writer = self.object_writer.with_file_encryptor(file_encryptor);
        self
    }
}

/// Writes [`ParquetMetaData`] to a byte stream
///
/// This structure handles the details of writing the various parts of Parquet
/// metadata into a byte stream. It is used to write the metadata into a parquet
/// file and can also write metadata into other locations (such as a store of
/// bytes).
///
/// # Discussion
///
/// The process of writing Parquet metadata is tricky because the
/// metadata is not stored as a single inline thrift structure. It can have
/// several "out of band" structures such as the [`OffsetIndex`] and
/// BloomFilters stored in separate structures whose locations are stored as
/// offsets from the beginning of the file.
///
/// Note: this writer does not directly write BloomFilters. In order to write
/// BloomFilters, write the bloom filters into the buffer before creating the
/// metadata writer. Then set the corresponding `bloom_filter_offset` and
/// `bloom_filter_length` on [`ColumnChunkMetaData`] passed to this writer.
///
/// # Output Format
///
/// The format of the metadata is as follows:
///
/// 1. Optional [`ColumnIndex`] (thrift encoded)
/// 2. Optional [`OffsetIndex`] (thrift encoded)
/// 3. [`FileMetaData`] (thrift encoded)
/// 4. Length of encoded `FileMetaData` (4 bytes, little endian)
/// 5. Parquet Magic Bytes (4 bytes)
///
/// [`FileMetaData`]: crate::format::FileMetaData
/// [`ColumnChunkMetaData`]: crate::file::metadata::ColumnChunkMetaData
///
/// ```text
/// ┌──────────────────────┐
/// │                      │
/// │         ...          │
/// │                      │
/// │┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ │
/// │     ColumnIndex     ◀│─ ─ ─
/// ││    (Optional)     │ │     │
/// │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
/// │┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ │     │ FileMetadata
/// │     OffsetIndex      │       contains embedded
/// ││    (Optional)     │◀┼ ─   │ offsets to
/// │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │  │    ColumnIndex and
/// │╔═══════════════════╗ │     │ OffsetIndex
/// │║                   ║ │  │
/// │║                   ║ ┼ ─   │
/// │║   FileMetadata    ║ │
/// │║                   ║ ┼ ─ ─ ┘
/// │║                   ║ │
/// │╚═══════════════════╝ │
/// │┌───────────────────┐ │
/// ││  metadata length  │ │ length of FileMetadata  (only)
/// │└───────────────────┘ │
/// │┌───────────────────┐ │
/// ││      'PAR1'       │ │ Parquet Magic Bytes
/// │└───────────────────┘ │
/// └──────────────────────┘
///      Output Buffer
/// ```
///
/// # Example
/// ```no_run
/// # use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataWriter};
/// # fn get_metadata() -> ParquetMetaData { unimplemented!(); }
/// // write parquet metadata to an in-memory buffer
/// let mut buffer = vec![];
/// let metadata: ParquetMetaData = get_metadata();
/// let writer = ParquetMetaDataWriter::new(&mut buffer, &metadata);
/// // write the metadata to the buffer
/// writer.finish().unwrap();
/// assert!(!buffer.is_empty());
/// ```
pub struct ParquetMetaDataWriter<'a, W: Write> {
    buf: TrackedWrite<W>,
    metadata: &'a ParquetMetaData,
}

impl<'a, W: Write> ParquetMetaDataWriter<'a, W> {
    /// Create a new `ParquetMetaDataWriter` to write to `buf`
    ///
    /// Note any embedded offsets in the metadata will be written assuming the
    /// metadata is at the start of the buffer. If the metadata is being written
    /// to a location other than the start of the buffer, see [`Self::new_with_tracked`]
    ///
    /// See example on the struct level documentation
    pub fn new(buf: W, metadata: &'a ParquetMetaData) -> Self {
        Self::new_with_tracked(TrackedWrite::new(buf), metadata)
    }

    /// Create a new ParquetMetaDataWriter to write to `buf`
    ///
    /// This method is used when the metadata is being written to a location other
    /// than the start of the buffer.
    ///
    /// See example on the struct level documentation
    pub fn new_with_tracked(buf: TrackedWrite<W>, metadata: &'a ParquetMetaData) -> Self {
        Self { buf, metadata }
    }

    /// Write the metadata to the buffer
    pub fn finish(mut self) -> Result<()> {
        let file_metadata = self.metadata.file_metadata();

        let schema = Arc::new(file_metadata.schema().clone());
        let schema_descr = Arc::new(SchemaDescriptor::new(schema.clone()));
        let created_by = file_metadata.created_by().map(str::to_string);

        let row_groups = self
            .metadata
            .row_groups()
            .iter()
            .map(|rg| rg.to_thrift())
            .collect::<Vec<_>>();

        let key_value_metadata = file_metadata.key_value_metadata().cloned();

        let column_indexes = self.convert_column_indexes();
        let offset_indexes = self.convert_offset_index();

        let mut encoder = ThriftMetadataWriter::new(
            &mut self.buf,
            &schema,
            &schema_descr,
            row_groups,
            created_by,
            file_metadata.version(),
        );
        encoder = encoder.with_column_indexes(&column_indexes);
        encoder = encoder.with_offset_indexes(&offset_indexes);
        if let Some(key_value_metadata) = key_value_metadata {
            encoder = encoder.with_key_value_metadata(key_value_metadata);
        }
        encoder.finish()?;

        Ok(())
    }

    fn convert_column_indexes(&self) -> Vec<Vec<Option<ColumnIndex>>> {
        if let Some(row_group_column_indexes) = self.metadata.column_index() {
            (0..self.metadata.row_groups().len())
                .map(|rg_idx| {
                    let column_indexes = &row_group_column_indexes[rg_idx];
                    column_indexes
                        .iter()
                        .map(|column_index| match column_index {
                            Index::NONE => None,
                            Index::BOOLEAN(column_index) => Some(column_index.to_thrift()),
                            Index::BYTE_ARRAY(column_index) => Some(column_index.to_thrift()),
                            Index::DOUBLE(column_index) => Some(column_index.to_thrift()),
                            Index::FIXED_LEN_BYTE_ARRAY(column_index) => {
                                Some(column_index.to_thrift())
                            }
                            Index::FLOAT(column_index) => Some(column_index.to_thrift()),
                            Index::INT32(column_index) => Some(column_index.to_thrift()),
                            Index::INT64(column_index) => Some(column_index.to_thrift()),
                            Index::INT96(column_index) => Some(column_index.to_thrift()),
                        })
                        .collect()
                })
                .collect()
        } else {
            // make a None for each row group, for each column
            self.metadata
                .row_groups()
                .iter()
                .map(|rg| std::iter::repeat(None).take(rg.columns().len()).collect())
                .collect()
        }
    }

    fn convert_offset_index(&self) -> Vec<Vec<Option<OffsetIndex>>> {
        if let Some(row_group_offset_indexes) = self.metadata.offset_index() {
            (0..self.metadata.row_groups().len())
                .map(|rg_idx| {
                    let offset_indexes = &row_group_offset_indexes[rg_idx];
                    offset_indexes
                        .iter()
                        .map(|offset_index| Some(offset_index.to_thrift()))
                        .collect()
                })
                .collect()
        } else {
            // make a None for each row group, for each column
            self.metadata
                .row_groups()
                .iter()
                .map(|rg| std::iter::repeat(None).take(rg.columns().len()).collect())
                .collect()
        }
    }
}

#[derive(Debug, Default)]
struct MetadataObjectWriter {
    #[cfg(feature = "encryption")]
    file_encryptor: Option<Arc<FileEncryptor>>,
}

impl MetadataObjectWriter {
    #[inline]
    fn write_object(object: &impl TSerializable, sink: impl Write) -> Result<()> {
        let mut protocol = TCompactOutputProtocol::new(sink);
        object.write_to_out_protocol(&mut protocol)?;
        Ok(())
    }
}

/// Implementations of [`MetadataObjectWriter`] methods for when encryption is disabled
#[cfg(not(feature = "encryption"))]
impl MetadataObjectWriter {
    /// Write [`FileMetaData`] in Thrift format
    fn write_file_metadata(&self, file_metadata: &FileMetaData, sink: impl Write) -> Result<()> {
        Self::write_object(file_metadata, sink)
    }

    /// Write a column [`OffsetIndex`] in Thrift format
    fn write_offset_index(
        &self,
        offset_index: &OffsetIndex,
        _column_chunk: &ColumnChunk,
        _row_group_idx: usize,
        _column_idx: usize,
        sink: impl Write,
    ) -> Result<()> {
        Self::write_object(offset_index, sink)
    }

    /// Write a column [`ColumnIndex`] in Thrift format
    fn write_column_index(
        &self,
        column_index: &ColumnIndex,
        _column_chunk: &ColumnChunk,
        _row_group_idx: usize,
        _column_idx: usize,
        sink: impl Write,
    ) -> Result<()> {
        Self::write_object(column_index, sink)
    }

    /// No-op implementation of row-group metadata encryption
    fn apply_row_group_encryption(
        &self,
        row_groups: Vec<RowGroup>,
    ) -> Result<(Vec<RowGroup>, Option<Vec<RowGroup>>)> {
        Ok((row_groups, None))
    }

    /// Get the "magic" bytes identifying the file type
    pub fn get_file_magic(&self) -> &[u8; 4] {
        get_file_magic()
    }

    fn get_footer_encryption_algorithm(&self) -> Option<EncryptionAlgorithm> {
        None
    }
}

/// Implementations of [`MetadataObjectWriter`] methods that rely on encryption being enabled
#[cfg(feature = "encryption")]
impl MetadataObjectWriter {
    /// Set the file encryptor to use
    fn with_file_encryptor(mut self, encryptor: Option<Arc<FileEncryptor>>) -> Self {
        self.file_encryptor = encryptor;
        self
    }

    /// Write [`FileMetaData`] in Thrift format, possibly encrypting it if required
    fn write_file_metadata(
        &self,
        file_metadata: &FileMetaData,
        mut sink: impl Write,
    ) -> Result<()> {
        match self.file_encryptor.as_ref() {
            Some(file_encryptor) if file_encryptor.properties().encrypt_footer() => {
                // First write FileCryptoMetadata
                let crypto_metadata = Self::file_crypto_metadata(file_encryptor)?;
                let mut protocol = TCompactOutputProtocol::new(&mut sink);
                crypto_metadata.write_to_out_protocol(&mut protocol)?;

                // Then write encrypted footer
                let aad = create_footer_aad(file_encryptor.file_aad())?;
                let mut encryptor = file_encryptor.get_footer_encryptor()?;
                encrypt_object(file_metadata, &mut encryptor, &mut sink, &aad)
            }
            Some(file_encryptor) if file_metadata.encryption_algorithm.is_some() => {
                let aad = create_footer_aad(file_encryptor.file_aad())?;
                let mut encryptor = file_encryptor.get_footer_encryptor()?;
                write_signed_plaintext_object(file_metadata, &mut encryptor, &mut sink, &aad)
            }
            _ => Self::write_object(file_metadata, &mut sink),
        }
    }

    /// Write a column [`OffsetIndex`] in Thrift format, possibly encrypting it if required
    fn write_offset_index(
        &self,
        offset_index: &OffsetIndex,
        column_chunk: &ColumnChunk,
        row_group_idx: usize,
        column_idx: usize,
        sink: impl Write,
    ) -> Result<()> {
        match &self.file_encryptor {
            Some(file_encryptor) => Self::write_object_with_encryption(
                offset_index,
                sink,
                file_encryptor,
                column_chunk,
                ModuleType::OffsetIndex,
                row_group_idx,
                column_idx,
            ),
            None => Self::write_object(offset_index, sink),
        }
    }

    /// Write a column [`ColumnIndex`] in Thrift format, possibly encrypting it if required
    fn write_column_index(
        &self,
        column_index: &ColumnIndex,
        column_chunk: &ColumnChunk,
        row_group_idx: usize,
        column_idx: usize,
        sink: impl Write,
    ) -> Result<()> {
        match &self.file_encryptor {
            Some(file_encryptor) => Self::write_object_with_encryption(
                column_index,
                sink,
                file_encryptor,
                column_chunk,
                ModuleType::ColumnIndex,
                row_group_idx,
                column_idx,
            ),
            None => Self::write_object(column_index, sink),
        }
    }

    /// If encryption is enabled and configured, encrypt row group metadata.
    /// Returns a tuple of the row group metadata to write,
    /// and possibly unencrypted metadata to be returned to clients if data was encrypted.
    fn apply_row_group_encryption(
        &self,
        row_groups: Vec<RowGroup>,
    ) -> Result<(Vec<RowGroup>, Option<Vec<RowGroup>>)> {
        match &self.file_encryptor {
            Some(file_encryptor) => {
                let unencrypted_row_groups = row_groups.clone();
                let encrypted_row_groups = Self::encrypt_row_groups(row_groups, file_encryptor)?;
                Ok((encrypted_row_groups, Some(unencrypted_row_groups)))
            }
            None => Ok((row_groups, None)),
        }
    }

    /// Get the "magic" bytes identifying the file type
    fn get_file_magic(&self) -> &[u8; 4] {
        get_file_magic(
            self.file_encryptor
                .as_ref()
                .map(|encryptor| encryptor.properties()),
        )
    }

    fn write_object_with_encryption(
        object: &impl TSerializable,
        mut sink: impl Write,
        file_encryptor: &FileEncryptor,
        column_metadata: &ColumnChunk,
        module_type: ModuleType,
        row_group_index: usize,
        column_index: usize,
    ) -> Result<()> {
        let column_path_vec = &column_metadata
            .meta_data
            .as_ref()
            .ok_or_else(|| {
                general_err!(
                    "Column metadata not set for column {} when encrypting object",
                    column_index
                )
            })?
            .path_in_schema;

        let joined_column_path;
        let column_path = if column_path_vec.len() == 1 {
            &column_path_vec[0]
        } else {
            joined_column_path = column_path_vec.join(".");
            &joined_column_path
        };

        if file_encryptor.is_column_encrypted(column_path) {
            let aad = create_module_aad(
                file_encryptor.file_aad(),
                module_type,
                row_group_index,
                column_index,
                None,
            )?;
            let mut encryptor = file_encryptor.get_column_encryptor(column_path)?;
            encrypt_object(object, &mut encryptor, &mut sink, &aad)
        } else {
            Self::write_object(object, sink)
        }
    }

    fn get_footer_encryption_algorithm(&self) -> Option<EncryptionAlgorithm> {
        if let Some(file_encryptor) = &self.file_encryptor {
            return Some(Self::encryption_algorithm_from_encryptor(file_encryptor));
        }
        None
    }

    fn encryption_algorithm_from_encryptor(file_encryptor: &FileEncryptor) -> EncryptionAlgorithm {
        let supply_aad_prefix = file_encryptor
            .properties()
            .aad_prefix()
            .map(|_| !file_encryptor.properties().store_aad_prefix());
        let aad_prefix = if file_encryptor.properties().store_aad_prefix() {
            file_encryptor.properties().aad_prefix().cloned()
        } else {
            None
        };
        EncryptionAlgorithm::AESGCMV1(AesGcmV1 {
            aad_prefix,
            aad_file_unique: Some(file_encryptor.aad_file_unique().clone()),
            supply_aad_prefix,
        })
    }

    fn file_crypto_metadata(
        file_encryptor: &FileEncryptor,
    ) -> Result<crate::format::FileCryptoMetaData> {
        let properties = file_encryptor.properties();
        Ok(crate::format::FileCryptoMetaData {
            encryption_algorithm: Self::encryption_algorithm_from_encryptor(file_encryptor),
            key_metadata: properties.footer_key_metadata().cloned(),
        })
    }

    fn encrypt_row_groups(
        row_groups: Vec<RowGroup>,
        file_encryptor: &Arc<FileEncryptor>,
    ) -> Result<Vec<RowGroup>> {
        row_groups
            .into_iter()
            .enumerate()
            .map(|(rg_idx, mut rg)| {
                let cols: Result<Vec<ColumnChunk>> = rg
                    .columns
                    .into_iter()
                    .enumerate()
                    .map(|(col_idx, c)| {
                        Self::encrypt_column_chunk(c, file_encryptor, rg_idx, col_idx)
                    })
                    .collect();
                rg.columns = cols?;
                Ok(rg)
            })
            .collect()
    }

    /// Apply column encryption to column chunk metadata
    fn encrypt_column_chunk(
        mut column_chunk: ColumnChunk,
        file_encryptor: &Arc<FileEncryptor>,
        row_group_index: usize,
        column_index: usize,
    ) -> Result<ColumnChunk> {
        // Column crypto metadata should have already been set when the column was created.
        // Here we apply the encryption by encrypting the column metadata if required.
        match column_chunk.crypto_metadata.as_ref() {
            None => {}
            Some(ColumnCryptoMetaData::ENCRYPTIONWITHFOOTERKEY(_)) => {
                // When uniform encryption is used the footer is already encrypted,
                // so the column chunk does not need additional encryption.
            }
            Some(ColumnCryptoMetaData::ENCRYPTIONWITHCOLUMNKEY(col_key)) => {
                let column_path = col_key.path_in_schema.join(".");
                let mut column_encryptor = file_encryptor.get_column_encryptor(&column_path)?;
                let meta_data = column_chunk
                    .meta_data
                    .take()
                    .ok_or_else(|| general_err!("Column metadata not set for encryption"))?;
                let aad = create_module_aad(
                    file_encryptor.file_aad(),
                    ModuleType::ColumnMetaData,
                    row_group_index,
                    column_index,
                    None,
                )?;
                let ciphertext = encrypt_object_to_vec(&meta_data, &mut column_encryptor, &aad)?;

                column_chunk.encrypted_column_metadata = Some(ciphertext);
                debug_assert!(column_chunk.meta_data.is_none());
            }
        }

        Ok(column_chunk)
    }
}
