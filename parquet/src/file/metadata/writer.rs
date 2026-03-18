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

use crate::file::metadata::thrift::FileMeta;
use crate::file::metadata::{
    ColumnChunkMetaData, ParquetColumnIndex, ParquetOffsetIndex, RowGroupMetaData,
};
use crate::schema::types::{SchemaDescPtr, SchemaDescriptor};
use crate::{
    basic::ColumnOrder,
    file::metadata::{FileMetaData, ParquetMetaDataBuilder},
};
#[cfg(feature = "encryption")]
use crate::{
    encryption::{
        encrypt::{FileEncryptor, encrypt_thrift_object, write_signed_plaintext_thrift_object},
        modules::{ModuleType, create_footer_aad, create_module_aad},
    },
    file::column_crypto_metadata::ColumnCryptoMetaData,
    file::metadata::thrift::encryption::{AesGcmV1, EncryptionAlgorithm, FileCryptoMetaData},
};
use crate::{errors::Result, file::page_index::column_index::ColumnIndexMetaData};

use crate::{
    file::writer::{TrackedWrite, get_file_magic},
    parquet_thrift::WriteThrift,
};
use crate::{
    file::{
        metadata::{KeyValue, ParquetMetaData},
        page_index::offset_index::OffsetIndexMetaData,
    },
    parquet_thrift::ThriftCompactOutputProtocol,
};
use std::io::Write;
use std::sync::Arc;

/// Writes `crate::file::metadata` structures to a thrift encoded byte stream
///
/// See [`ParquetMetaDataWriter`] for background and example.
pub(crate) struct ThriftMetadataWriter<'a, W: Write> {
    buf: &'a mut TrackedWrite<W>,
    schema_descr: &'a SchemaDescPtr,
    row_groups: Vec<RowGroupMetaData>,
    column_indexes: Option<Vec<Vec<Option<ColumnIndexMetaData>>>>,
    offset_indexes: Option<Vec<Vec<Option<OffsetIndexMetaData>>>>,
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
    fn write_offset_indexes(
        &mut self,
        offset_indexes: &[Vec<Option<OffsetIndexMetaData>>],
    ) -> Result<()> {
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
    fn write_column_indexes(
        &mut self,
        column_indexes: &[Vec<Option<ColumnIndexMetaData>>],
    ) -> Result<()> {
        // iter row group
        // iter each column
        // write column index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(column_index) = &column_indexes[row_group_idx][column_idx] {
                    let start_offset = self.buf.bytes_written();
                    // only update column_metadata if the write succeeds
                    if self.object_writer.write_column_index(
                        column_index,
                        column_metadata,
                        row_group_idx,
                        column_idx,
                        &mut self.buf,
                    )? {
                        let end_offset = self.buf.bytes_written();
                        // set offset and index for offset index
                        column_metadata.column_index_offset = Some(start_offset as i64);
                        column_metadata.column_index_length =
                            Some((end_offset - start_offset) as i32);
                    }
                }
            }
        }
        Ok(())
    }

    /// Serialize the column indexes and transform to `Option<ParquetColumnIndex>`
    fn finalize_column_indexes(&mut self) -> Result<Option<ParquetColumnIndex>> {
        let column_indexes = std::mem::take(&mut self.column_indexes);

        // Write column indexes to file
        if let Some(column_indexes) = column_indexes.as_ref() {
            self.write_column_indexes(column_indexes)?;
        }

        // check to see if the index is `None` for every row group and column chunk
        let all_none = column_indexes
            .as_ref()
            .is_some_and(|ci| ci.iter().all(|cii| cii.iter().all(|idx| idx.is_none())));

        // transform from Option<Vec<Vec<Option<ColumnIndexMetaData>>>> to
        // Option<Vec<Vec<ColumnIndexMetaData>>>
        let column_indexes: Option<ParquetColumnIndex> = if all_none {
            None
        } else {
            column_indexes.map(|ovvi| {
                ovvi.into_iter()
                    .map(|vi| {
                        vi.into_iter()
                            .map(|ci| ci.unwrap_or(ColumnIndexMetaData::NONE))
                            .collect()
                    })
                    .collect()
            })
        };

        Ok(column_indexes)
    }

    /// Serialize the offset indexes and transform to `Option<ParquetOffsetIndex>`
    fn finalize_offset_indexes(&mut self) -> Result<Option<ParquetOffsetIndex>> {
        let offset_indexes = std::mem::take(&mut self.offset_indexes);

        // Write offset indexes to file
        if let Some(offset_indexes) = offset_indexes.as_ref() {
            self.write_offset_indexes(offset_indexes)?;
        }

        // check to see if the index is `None` for every row group and column chunk
        let all_none = offset_indexes
            .as_ref()
            .is_some_and(|oi| oi.iter().all(|oii| oii.iter().all(|idx| idx.is_none())));

        let offset_indexes: Option<ParquetOffsetIndex> = if all_none {
            None
        } else {
            // FIXME(ets): this will panic if there's a missing index.
            offset_indexes.map(|ovvi| {
                ovvi.into_iter()
                    .map(|vi| vi.into_iter().map(|oi| oi.unwrap()).collect())
                    .collect()
            })
        };

        Ok(offset_indexes)
    }

    /// Assembles and writes the final metadata to self.buf
    pub fn finish(mut self) -> Result<ParquetMetaData> {
        let num_rows = self.row_groups.iter().map(|x| x.num_rows).sum();

        // serialize page indexes and transform to the proper form for use in ParquetMetaData
        let column_indexes = self.finalize_column_indexes()?;
        let offset_indexes = self.finalize_offset_indexes()?;

        // We only include ColumnOrder for leaf nodes.
        // Currently only supported ColumnOrder is TypeDefinedOrder so we set this
        // for all leaf nodes.
        // Even if the column has an undefined sort order, such as INTERVAL, this
        // is still technically the defined TYPEORDER so it should still be set.
        let column_orders = self
            .schema_descr
            .columns()
            .iter()
            .map(|col| {
                let sort_order = ColumnOrder::sort_order_for_type(
                    col.logical_type_ref(),
                    col.converted_type(),
                    col.physical_type(),
                );
                ColumnOrder::TYPE_DEFINED_ORDER(sort_order)
            })
            .collect();

        // This field is optional, perhaps in cases where no min/max fields are set
        // in any Statistics or ColumnIndex object in the whole file.
        // But for simplicity we always set this field.
        let column_orders = Some(column_orders);

        let (row_groups, unencrypted_row_groups) = self
            .object_writer
            .apply_row_group_encryption(self.row_groups)?;

        #[cfg(feature = "encryption")]
        let (encryption_algorithm, footer_signing_key_metadata) =
            self.object_writer.get_plaintext_footer_crypto_metadata();
        #[cfg(feature = "encryption")]
        let file_metadata = FileMetaData::new(
            self.writer_version,
            num_rows,
            self.created_by,
            self.key_value_metadata,
            self.schema_descr.clone(),
            column_orders,
        )
        .with_encryption_algorithm(encryption_algorithm)
        .with_footer_signing_key_metadata(footer_signing_key_metadata);

        #[cfg(not(feature = "encryption"))]
        let file_metadata = FileMetaData::new(
            self.writer_version,
            num_rows,
            self.created_by,
            self.key_value_metadata,
            self.schema_descr.clone(),
            column_orders,
        );

        let file_meta = FileMeta {
            file_metadata: &file_metadata,
            row_groups: &row_groups,
        };

        // Write file metadata
        let start_pos = self.buf.bytes_written();
        self.object_writer
            .write_file_metadata(&file_meta, &mut self.buf)?;
        let end_pos = self.buf.bytes_written();

        // Write footer
        let metadata_len = (end_pos - start_pos) as u32;

        self.buf.write_all(&metadata_len.to_le_bytes())?;
        self.buf.write_all(self.object_writer.get_file_magic())?;

        // If row group metadata was encrypted, we replace the encrypted row groups with
        // unencrypted metadata before it is returned to users. This allows the metadata
        // to be usable for retrieving the row group statistics for example, without users
        // needing to decrypt the metadata.
        let builder = ParquetMetaDataBuilder::new(file_metadata)
            .set_column_index(column_indexes)
            .set_offset_index(offset_indexes);

        Ok(match unencrypted_row_groups {
            Some(rg) => builder.set_row_groups(rg).build(),
            None => builder.set_row_groups(row_groups).build(),
        })
    }

    pub fn new(
        buf: &'a mut TrackedWrite<W>,
        schema_descr: &'a SchemaDescPtr,
        row_groups: Vec<RowGroupMetaData>,
        created_by: Option<String>,
        writer_version: i32,
    ) -> Self {
        Self {
            buf,
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

    pub fn with_column_indexes(
        mut self,
        column_indexes: Vec<Vec<Option<ColumnIndexMetaData>>>,
    ) -> Self {
        self.column_indexes = Some(column_indexes);
        self
    }

    pub fn with_offset_indexes(
        mut self,
        offset_indexes: Vec<Vec<Option<OffsetIndexMetaData>>>,
    ) -> Self {
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
/// [`FileMetaData`]: https://github.com/apache/parquet-format/tree/master?tab=readme-ov-file#metadata
/// [`ColumnChunkMetaData`]: crate::file::metadata::ColumnChunkMetaData
/// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
/// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
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

        let row_groups = self.metadata.row_groups.clone();

        let key_value_metadata = file_metadata.key_value_metadata().cloned();

        let column_indexes = self.convert_column_indexes();
        let offset_indexes = self.convert_offset_index();

        let mut encoder = ThriftMetadataWriter::new(
            &mut self.buf,
            &schema_descr,
            row_groups,
            created_by,
            file_metadata.version(),
        );

        if let Some(column_indexes) = column_indexes {
            encoder = encoder.with_column_indexes(column_indexes);
        }

        if let Some(offset_indexes) = offset_indexes {
            encoder = encoder.with_offset_indexes(offset_indexes);
        }

        if let Some(key_value_metadata) = key_value_metadata {
            encoder = encoder.with_key_value_metadata(key_value_metadata);
        }
        encoder.finish()?;

        Ok(())
    }

    fn convert_column_indexes(&self) -> Option<Vec<Vec<Option<ColumnIndexMetaData>>>> {
        // TODO(ets): we're converting from ParquetColumnIndex to vec<vec<option>>,
        // but then converting back to ParquetColumnIndex in the end. need to unify this.
        self.metadata
            .column_index()
            .map(|row_group_column_indexes| {
                (0..self.metadata.row_groups().len())
                    .map(|rg_idx| {
                        let column_indexes = &row_group_column_indexes[rg_idx];
                        column_indexes
                            .iter()
                            .map(|column_index| Some(column_index.clone()))
                            .collect()
                    })
                    .collect()
            })
    }

    fn convert_offset_index(&self) -> Option<Vec<Vec<Option<OffsetIndexMetaData>>>> {
        self.metadata
            .offset_index()
            .map(|row_group_offset_indexes| {
                (0..self.metadata.row_groups().len())
                    .map(|rg_idx| {
                        let offset_indexes = &row_group_offset_indexes[rg_idx];
                        offset_indexes
                            .iter()
                            .map(|offset_index| Some(offset_index.clone()))
                            .collect()
                    })
                    .collect()
            })
    }
}

#[derive(Debug, Default)]
struct MetadataObjectWriter {
    #[cfg(feature = "encryption")]
    file_encryptor: Option<Arc<FileEncryptor>>,
}

impl MetadataObjectWriter {
    #[inline]
    fn write_thrift_object(object: &impl WriteThrift, sink: impl Write) -> Result<()> {
        let mut protocol = ThriftCompactOutputProtocol::new(sink);
        object.write_thrift(&mut protocol)?;
        Ok(())
    }
}

/// Implementations of [`MetadataObjectWriter`] methods for when encryption is disabled
#[cfg(not(feature = "encryption"))]
impl MetadataObjectWriter {
    /// Write [`FileMetaData`] in Thrift format
    ///
    /// [`FileMetaData`]: https://github.com/apache/parquet-format/tree/master?tab=readme-ov-file#metadata
    fn write_file_metadata(&self, file_metadata: &FileMeta, sink: impl Write) -> Result<()> {
        Self::write_thrift_object(file_metadata, sink)
    }

    /// Write a column [`OffsetIndex`] in Thrift format
    ///
    /// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    fn write_offset_index(
        &self,
        offset_index: &OffsetIndexMetaData,
        _column_chunk: &ColumnChunkMetaData,
        _row_group_idx: usize,
        _column_idx: usize,
        sink: impl Write,
    ) -> Result<()> {
        Self::write_thrift_object(offset_index, sink)
    }

    /// Write a column [`ColumnIndex`] in Thrift format
    ///
    /// If `column_index` is [`ColumnIndexMetaData::NONE`] the index will not be written and
    /// this will return `false`. Returns `true` otherwise.
    ///
    /// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    fn write_column_index(
        &self,
        column_index: &ColumnIndexMetaData,
        _column_chunk: &ColumnChunkMetaData,
        _row_group_idx: usize,
        _column_idx: usize,
        sink: impl Write,
    ) -> Result<bool> {
        match column_index {
            // Missing indexes may also have the placeholder ColumnIndexMetaData::NONE
            ColumnIndexMetaData::NONE => Ok(false),
            _ => {
                Self::write_thrift_object(column_index, sink)?;
                Ok(true)
            }
        }
    }

    /// No-op implementation of row-group metadata encryption
    fn apply_row_group_encryption(
        &self,
        row_groups: Vec<RowGroupMetaData>,
    ) -> Result<(Vec<RowGroupMetaData>, Option<Vec<RowGroupMetaData>>)> {
        Ok((row_groups, None))
    }

    /// Get the "magic" bytes identifying the file type
    pub fn get_file_magic(&self) -> &[u8; 4] {
        get_file_magic()
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
    ///
    /// [`FileMetaData`]: https://github.com/apache/parquet-format/tree/master?tab=readme-ov-file#metadata
    fn write_file_metadata(&self, file_metadata: &FileMeta, mut sink: impl Write) -> Result<()> {
        match self.file_encryptor.as_ref() {
            Some(file_encryptor) if file_encryptor.properties().encrypt_footer() => {
                // First write FileCryptoMetadata
                let crypto_metadata = Self::file_crypto_metadata(file_encryptor)?;
                let mut protocol = ThriftCompactOutputProtocol::new(&mut sink);
                crypto_metadata.write_thrift(&mut protocol)?;

                // Then write encrypted footer
                let aad = create_footer_aad(file_encryptor.file_aad())?;
                let mut encryptor = file_encryptor.get_footer_encryptor()?;
                encrypt_thrift_object(file_metadata, &mut encryptor, &mut sink, &aad)
            }
            Some(file_encryptor) if file_metadata.file_metadata.encryption_algorithm.is_some() => {
                let aad = create_footer_aad(file_encryptor.file_aad())?;
                let mut encryptor = file_encryptor.get_footer_encryptor()?;
                write_signed_plaintext_thrift_object(file_metadata, &mut encryptor, &mut sink, &aad)
            }
            _ => Self::write_thrift_object(file_metadata, &mut sink),
        }
    }

    /// Write a column [`OffsetIndex`] in Thrift format, possibly encrypting it if required
    ///
    /// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    fn write_offset_index(
        &self,
        offset_index: &OffsetIndexMetaData,
        column_chunk: &ColumnChunkMetaData,
        row_group_idx: usize,
        column_idx: usize,
        sink: impl Write,
    ) -> Result<()> {
        match &self.file_encryptor {
            Some(file_encryptor) => Self::write_thrift_object_with_encryption(
                offset_index,
                sink,
                file_encryptor,
                column_chunk,
                ModuleType::OffsetIndex,
                row_group_idx,
                column_idx,
            ),
            None => Self::write_thrift_object(offset_index, sink),
        }
    }

    /// Write a column [`ColumnIndex`] in Thrift format, possibly encrypting it if required
    ///
    /// If `column_index` is [`ColumnIndexMetaData::NONE`] the index will not be written and
    /// this will return `false`. Returns `true` otherwise.
    ///
    /// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    fn write_column_index(
        &self,
        column_index: &ColumnIndexMetaData,
        column_chunk: &ColumnChunkMetaData,
        row_group_idx: usize,
        column_idx: usize,
        sink: impl Write,
    ) -> Result<bool> {
        match column_index {
            // Missing indexes may also have the placeholder ColumnIndexMetaData::NONE
            ColumnIndexMetaData::NONE => Ok(false),
            _ => {
                match &self.file_encryptor {
                    Some(file_encryptor) => Self::write_thrift_object_with_encryption(
                        column_index,
                        sink,
                        file_encryptor,
                        column_chunk,
                        ModuleType::ColumnIndex,
                        row_group_idx,
                        column_idx,
                    )?,
                    None => Self::write_thrift_object(column_index, sink)?,
                }
                Ok(true)
            }
        }
    }

    /// If encryption is enabled and configured, encrypt row group metadata.
    /// Returns a tuple of the row group metadata to write,
    /// and possibly unencrypted metadata to be returned to clients if data was encrypted.
    fn apply_row_group_encryption(
        &self,
        row_groups: Vec<RowGroupMetaData>,
    ) -> Result<(Vec<RowGroupMetaData>, Option<Vec<RowGroupMetaData>>)> {
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

    fn write_thrift_object_with_encryption(
        object: &impl WriteThrift,
        mut sink: impl Write,
        file_encryptor: &FileEncryptor,
        column_metadata: &ColumnChunkMetaData,
        module_type: ModuleType,
        row_group_index: usize,
        column_index: usize,
    ) -> Result<()> {
        let column_path_vec = column_metadata.column_path().as_ref();

        let joined_column_path;
        let column_path = if column_path_vec.len() == 1 {
            &column_path_vec[0]
        } else {
            joined_column_path = column_path_vec.join(".");
            &joined_column_path
        };

        if file_encryptor.is_column_encrypted(column_path) {
            use crate::encryption::encrypt::encrypt_thrift_object;

            let aad = create_module_aad(
                file_encryptor.file_aad(),
                module_type,
                row_group_index,
                column_index,
                None,
            )?;
            let mut encryptor = file_encryptor.get_column_encryptor(column_path)?;
            encrypt_thrift_object(object, &mut encryptor, &mut sink, &aad)
        } else {
            Self::write_thrift_object(object, sink)
        }
    }

    fn get_plaintext_footer_crypto_metadata(
        &self,
    ) -> (Option<EncryptionAlgorithm>, Option<Vec<u8>>) {
        // Only plaintext footers may contain encryption algorithm and footer key metadata.
        if let Some(file_encryptor) = self.file_encryptor.as_ref() {
            let encryption_properties = file_encryptor.properties();
            if !encryption_properties.encrypt_footer() {
                return (
                    Some(Self::encryption_algorithm_from_encryptor(file_encryptor)),
                    encryption_properties.footer_key_metadata().cloned(),
                );
            }
        }
        (None, None)
    }

    fn encryption_algorithm_from_encryptor(file_encryptor: &FileEncryptor) -> EncryptionAlgorithm {
        let supply_aad_prefix = file_encryptor
            .properties()
            .aad_prefix()
            .map(|_| !file_encryptor.properties().store_aad_prefix());
        let aad_prefix = if file_encryptor.properties().store_aad_prefix() {
            file_encryptor.properties().aad_prefix()
        } else {
            None
        };
        EncryptionAlgorithm::AES_GCM_V1(AesGcmV1 {
            aad_prefix: aad_prefix.cloned(),
            aad_file_unique: Some(file_encryptor.aad_file_unique().clone()),
            supply_aad_prefix,
        })
    }

    fn file_crypto_metadata(file_encryptor: &'_ FileEncryptor) -> Result<FileCryptoMetaData<'_>> {
        let properties = file_encryptor.properties();
        Ok(FileCryptoMetaData {
            encryption_algorithm: Self::encryption_algorithm_from_encryptor(file_encryptor),
            key_metadata: properties.footer_key_metadata().map(|v| v.as_slice()),
        })
    }

    fn encrypt_row_groups(
        row_groups: Vec<RowGroupMetaData>,
        file_encryptor: &Arc<FileEncryptor>,
    ) -> Result<Vec<RowGroupMetaData>> {
        row_groups
            .into_iter()
            .enumerate()
            .map(|(rg_idx, mut rg)| {
                let cols: Result<Vec<ColumnChunkMetaData>> = rg
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
        mut column_chunk: ColumnChunkMetaData,
        file_encryptor: &Arc<FileEncryptor>,
        row_group_index: usize,
        column_index: usize,
    ) -> Result<ColumnChunkMetaData> {
        // Column crypto metadata should have already been set when the column was created.
        // Here we apply the encryption by encrypting the column metadata if required.
        let encryptor = match column_chunk.column_crypto_metadata.as_deref() {
            None => None,
            Some(ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY) => {
                let is_footer_encrypted = file_encryptor.properties().encrypt_footer();

                // When uniform encryption is used the footer is already encrypted,
                // so the column chunk does not need additional encryption.
                // Except if we're in plaintext footer mode, then we need to encrypt
                // the column metadata here.
                if !is_footer_encrypted {
                    Some(file_encryptor.get_footer_encryptor()?)
                } else {
                    None
                }
            }
            Some(ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(col_key)) => {
                let column_path = col_key.path_in_schema.join(".");
                Some(file_encryptor.get_column_encryptor(&column_path)?)
            }
        };

        if let Some(mut encryptor) = encryptor {
            use crate::file::metadata::thrift::serialize_column_meta_data;

            let aad = create_module_aad(
                file_encryptor.file_aad(),
                ModuleType::ColumnMetaData,
                row_group_index,
                column_index,
                None,
            )?;
            // create temp ColumnMetaData that we can encrypt
            let mut buffer: Vec<u8> = vec![];
            {
                let mut prot = ThriftCompactOutputProtocol::new(&mut buffer);
                serialize_column_meta_data(&column_chunk, &mut prot)?;
            }
            let ciphertext = encryptor.encrypt(&buffer, &aad)?;
            column_chunk.encrypted_column_metadata = Some(ciphertext);
            // Track whether the footer is plaintext, which affects how we serialize
            // the column metadata (we need to write stripped metadata for backward compatibility)
            column_chunk.plaintext_footer_mode = !file_encryptor.properties().encrypt_footer();
        }

        Ok(column_chunk)
    }
}
