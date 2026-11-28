// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::errors::{ParquetError, Result};
use crate::file::metadata::extensions::ThriftStruct;
#[cfg(any(feature = "experimental", test))]
use crate::file::metadata::extensions::{
    self as extensions, HookState, MetadataExtensionHook, MetadataExtensionSelection,
};
use crate::file::metadata::extensions::{HookHandle, SiteInfo};
use crate::file::metadata::page_index::PageIndexProvider;
use crate::file::metadata::thrift::FileMeta;
use crate::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use crate::file::page_index::column_index::ColumnIndexMetaData;
#[cfg(feature = "encryption")]
use crate::parquet_thrift::ThriftCompactOutputProtocol;
use crate::schema::types::{SchemaDescPtr, SchemaDescriptor};
use crate::{
    basic::ColumnOrder,
    file::metadata::{FileMetaData, ParquetMetaDataBuilder},
};
#[cfg(feature = "encryption")]
use crate::{
    encryption::{
        encrypt::{FileEncryptor, encrypt_thrift, write_signed_plaintext_thrift},
        modules::{ModuleType, create_footer_aad, create_module_aad},
    },
    file::column_crypto_metadata::ColumnCryptoMetaData,
    file::metadata::thrift::encryption::{AesGcmV1, EncryptionAlgorithm, FileCryptoMetaData},
};

use crate::file::{
    metadata::{KeyValue, ParquetMetaData},
    page_index::offset_index::OffsetIndexMetaData,
};
use crate::{
    file::writer::{TrackedWrite, get_file_magic},
    parquet_thrift::WriteThrift,
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
    page_index: Option<Arc<dyn PageIndexProvider>>,
    key_value_metadata: Option<Vec<KeyValue>>,
    created_by: Option<String>,
    object_writer: MetadataObjectWriter,
    writer_version: i32,
    write_path_in_schema: bool,
    preserve_page_index_locations: bool,
}

impl<'a, W: Write> ThriftMetadataWriter<'a, W> {
    /// Serialize all the offset indexes to `self.buf`,
    ///
    /// Note: also updates the `ColumnChunk::offset_index_offset` and
    /// `ColumnChunk::offset_index_length` to reflect the position and length
    /// of the serialized offset indexes.
    fn write_offset_indexes(&mut self, page_index: &dyn PageIndexProvider) -> Result<()> {
        // iter row group
        // iter each column
        // write offset index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(offset_index) = page_index.offset_index(row_group_idx, column_idx) {
                    let start_offset = self.buf.bytes_written();
                    self.object_writer.write_offset_index(
                        offset_index,
                        column_metadata,
                        row_group_idx,
                        column_idx,
                        &mut *self.buf,
                    )?;
                    let end_offset = self.buf.bytes_written();
                    let length = i32::try_from(end_offset - start_offset)
                        .map_err(|_| general_err!("Offset index length exceeds i32::MAX bytes"))?;
                    // set offset and index for offset index
                    column_metadata.offset_index_offset = Some(start_offset as i64);
                    column_metadata.offset_index_length = Some(length);
                } else if !self.preserve_page_index_locations {
                    column_metadata.offset_index_offset = None;
                    column_metadata.offset_index_length = None;
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
    fn write_column_indexes(&mut self, page_index: &dyn PageIndexProvider) -> Result<()> {
        // iter row group
        // iter each column
        // write column index to the file
        for (row_group_idx, row_group) in self.row_groups.iter_mut().enumerate() {
            for (column_idx, column_metadata) in row_group.columns.iter_mut().enumerate() {
                if let Some(column_index) = page_index.column_index(row_group_idx, column_idx) {
                    let start_offset = self.buf.bytes_written();
                    // only update column_metadata if the write succeeds
                    if self.object_writer.write_column_index(
                        column_index,
                        column_metadata,
                        row_group_idx,
                        column_idx,
                        &mut *self.buf,
                    )? {
                        let end_offset = self.buf.bytes_written();
                        let length = i32::try_from(end_offset - start_offset).map_err(|_| {
                            general_err!("Column index length exceeds i32::MAX bytes")
                        })?;
                        // set offset and length for column index
                        column_metadata.column_index_offset = Some(start_offset as i64);
                        column_metadata.column_index_length = Some(length);
                    }
                } else if !self.preserve_page_index_locations {
                    column_metadata.column_index_offset = None;
                    column_metadata.column_index_length = None;
                }
            }
        }
        Ok(())
    }

    fn clear_page_indexes(&mut self) {
        for row_group in &mut self.row_groups {
            for column_metadata in &mut row_group.columns {
                column_metadata.column_index_offset = None;
                column_metadata.column_index_length = None;
                column_metadata.offset_index_offset = None;
                column_metadata.offset_index_length = None;
            }
        }
    }

    /// Assembles and writes the final metadata to self.buf
    pub(crate) fn finish(mut self) -> Result<ParquetMetaData> {
        let num_rows = self.row_groups.iter().map(|x| x.num_rows).sum();

        // serialize page indexes and update index locations
        let page_index = self.page_index.take();
        if let Some(pi) = page_index.as_deref() {
            self.write_column_indexes(pi)?;
            self.write_offset_indexes(pi)?;
        } else if !self.preserve_page_index_locations {
            // no page indexes at all, then clear index locations
            self.clear_page_indexes();
        }

        // We only include ColumnOrder for leaf nodes.
        let column_orders = self
            .schema_descr
            .columns()
            .iter()
            .map(|col| {
                ColumnOrder::column_order_for_type(
                    col.logical_type_ref(),
                    col.converted_type(),
                    col.physical_type(),
                )
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
            write_path_in_schema: self.write_path_in_schema,
        };

        // Write file metadata
        let start_pos = self.buf.bytes_written();
        self.object_writer
            .write_file_metadata(&file_meta, &mut *self.buf)?;
        let end_pos = self.buf.bytes_written();

        // Write footer
        let metadata_len = u32::try_from(end_pos - start_pos)
            .map_err(|_| general_err!("File metadata length exceeds u32::MAX bytes"))?;

        self.buf.write_all(&metadata_len.to_le_bytes())?;
        self.buf.write_all(self.object_writer.get_file_magic())?;

        // Note: we simply pass the original reference counted page index provider to the
        // builder. Either it came from ParquetMetaDataWriter where it was cloned, and this
        // reference will go away when ParquetMetaDataWriter::finish drops the newly created
        // metadata, or it was passed an owned version from SerializedFileWriter which then
        // takes it back via the returned metadata.
        let builder = ParquetMetaDataBuilder::new(file_metadata).set_page_index(page_index);

        // If row group metadata was encrypted, we replace the encrypted row groups with
        // unencrypted metadata before it is returned to users. This allows the metadata
        // to be usable for retrieving the row group statistics for example, without users
        // needing to decrypt the metadata.
        Ok(match unencrypted_row_groups {
            Some(rg) => builder.set_row_groups(rg).build(),
            None => builder.set_row_groups(row_groups).build(),
        })
    }

    pub(crate) fn new(
        buf: &'a mut TrackedWrite<W>,
        schema_descr: &'a SchemaDescPtr,
        row_groups: Vec<RowGroupMetaData>,
        created_by: Option<String>,
        writer_version: i32,
        write_path_in_schema: bool,
        preserve_page_index_locations: bool,
    ) -> Self {
        Self {
            buf,
            schema_descr,
            row_groups,
            page_index: None,
            key_value_metadata: None,
            created_by,
            object_writer: Default::default(),
            writer_version,
            write_path_in_schema,
            preserve_page_index_locations,
        }
    }

    pub(crate) fn with_extensions(mut self, hook: Option<HookHandle>) -> Self {
        self.object_writer.metadata_extension_hook = hook;
        self
    }

    pub(crate) fn with_page_index(mut self, page_index: Arc<dyn PageIndexProvider>) -> Self {
        self.page_index = Some(page_index);
        self
    }

    pub(crate) fn with_key_value_metadata(mut self, key_value_metadata: Vec<KeyValue>) -> Self {
        self.key_value_metadata = Some(key_value_metadata);
        self
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn with_file_encryptor(
        mut self,
        file_encryptor: Option<Arc<FileEncryptor>>,
    ) -> Self {
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
/// The writer serializes the page index of any [`PageIndexProvider`] attached
/// to the input [`ParquetMetaData`]. By default, if the provider has no index
/// for a column chunk, the offset and length of that index in the [`ColumnChunkMetaData`]
/// are preserved in the output metadata. This is done so one can store the footer
/// metadata externally but keep the page indexes in the original file. If this
/// behavior is not desired, then [`Self::with_preserve_page_index_locations`] should
/// be set to `false`.
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
/// [`PageIndexProvider`]: crate::file::metadata::page_index::PageIndexProvider
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
    metadata_extension_hook: Option<HookHandle>,
    buf: TrackedWrite<W>,
    metadata: &'a ParquetMetaData,
    write_path_in_schema: bool,
    preserve_page_index_locations: bool,
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
        Self {
            buf,
            metadata,
            metadata_extension_hook: None,
            write_path_in_schema: true,
            preserve_page_index_locations: true,
        }
    }

    /// Install one hook and its complete target list for the footer and page indexes.
    /// Existing data pages are not rewritten. Empty lists or a second installation fail.
    #[cfg(any(feature = "experimental", test))]
    #[doc(hidden)]
    pub fn with_metadata_extension_hook<H: MetadataExtensionHook + 'static>(
        mut self,
        targets: impl IntoIterator<Item = MetadataExtensionSelection>,
        hook: H,
    ) -> Result<Self> {
        extensions::install(&mut self.metadata_extension_hook, targets, |targets| {
            Arc::new(HookState::new(targets, hook))
        })?;
        Ok(self)
    }

    /// Set whether or not to write the `path_in_schema` field in the Thrift `ColumnMetaData`
    /// struct.
    pub fn with_write_path_in_schema(self, val: bool) -> Self {
        Self {
            write_path_in_schema: val,
            ..self
        }
    }

    /// Set whether or not to preserve the page index location metadata in the Thrift
    /// `ColumnMetaData` (defaults to `true`).
    ///
    /// Because this struct is often used to externalize the footer metadata, it is
    /// usually desirable to preserve this location information, even when the
    /// page indexes are not duplicated (for instance if the provided `ParquetMetaData`
    /// returns no `PageIndexProvider`). As such, this defaults to `true`.
    ///
    /// Set this to `false` to reset the location metadata if no page indexes are
    /// present in the provided [`ParquetMetaData`].
    pub fn with_preserve_page_index_locations(self, val: bool) -> Self {
        Self {
            preserve_page_index_locations: val,
            ..self
        }
    }

    /// Write the metadata to the buffer
    pub fn finish(mut self) -> Result<()> {
        let file_metadata = self.metadata.file_metadata();

        let schema = Arc::new(file_metadata.schema().clone());
        let schema_descr = Arc::new(SchemaDescriptor::new(schema.clone()));
        let created_by = file_metadata.created_by().map(str::to_string);

        // need to clone because ThriftMetadataWriter wants to update page
        // index pointers. we throw this clone away upon calling encoder.finish()
        let row_groups = self.metadata.row_groups.as_ref().to_vec();

        let key_value_metadata = file_metadata.key_value_metadata().cloned();

        let mut encoder = ThriftMetadataWriter::new(
            &mut self.buf,
            &schema_descr,
            row_groups,
            created_by,
            file_metadata.version(),
            self.write_path_in_schema,
            self.preserve_page_index_locations,
        )
        .with_extensions(self.metadata_extension_hook);

        if let Some(page_index_arc) = self.metadata.page_index.clone() {
            encoder = encoder.with_page_index(page_index_arc);
        }

        if let Some(key_value_metadata) = key_value_metadata {
            encoder = encoder.with_key_value_metadata(key_value_metadata);
        }
        encoder.finish()?;

        Ok(())
    }
}

#[derive(Debug, Default)]
struct MetadataObjectWriter {
    metadata_extension_hook: Option<HookHandle>,
    #[cfg(feature = "encryption")]
    file_encryptor: Option<Arc<FileEncryptor>>,
}

impl MetadataObjectWriter {
    fn write_thrift_object(
        &self,
        object: &impl WriteThrift,
        sink: impl Write,
        root: impl FnOnce() -> SiteInfo,
    ) -> Result<()> {
        crate::file::metadata::extensions::write_thrift(
            object,
            sink,
            self.metadata_extension_hook.as_ref(),
            root,
        )
    }

    #[cfg(feature = "encryption")]
    fn with_file_encryptor(mut self, encryptor: Option<Arc<FileEncryptor>>) -> Self {
        self.file_encryptor = encryptor;
        self
    }

    fn write_file_metadata(&self, file_metadata: &FileMeta, sink: impl Write) -> Result<()> {
        let root = || SiteInfo::new(ThriftStruct::FileMetaData);
        #[cfg(feature = "encryption")]
        {
            let mut sink = sink;
            if let Some(file_encryptor) = &self.file_encryptor {
                if file_encryptor.properties().encrypt_footer() {
                    let crypto = Self::file_crypto_metadata(file_encryptor)?;
                    self.write_thrift_object(&crypto, &mut sink, || {
                        SiteInfo::new(ThriftStruct::FileCryptoMetaData)
                    })?;
                    let aad = create_footer_aad(file_encryptor.file_aad())?;
                    let mut encryptor = file_encryptor.get_footer_encryptor()?;
                    return encrypt_thrift(
                        |buffer| self.write_thrift_object(file_metadata, buffer, root),
                        &mut encryptor,
                        &mut sink,
                        &aad,
                    );
                }
                if file_metadata.file_metadata.encryption_algorithm.is_some() {
                    let aad = create_footer_aad(file_encryptor.file_aad())?;
                    let mut encryptor = file_encryptor.get_footer_encryptor()?;
                    return write_signed_plaintext_thrift(
                        |buffer| self.write_thrift_object(file_metadata, buffer, root),
                        &mut encryptor,
                        &mut sink,
                        &aad,
                    );
                }
            }
            self.write_thrift_object(file_metadata, sink, root)
        }
        #[cfg(not(feature = "encryption"))]
        self.write_thrift_object(file_metadata, sink, root)
    }

    fn write_offset_index(
        &self,
        index: &OffsetIndexMetaData,
        column: &ColumnChunkMetaData,
        row_group: usize,
        column_index: usize,
        sink: impl Write,
    ) -> Result<()> {
        self.write_index(
            index,
            column,
            ThriftStruct::OffsetIndex,
            (row_group, column_index),
            sink,
        )
    }

    fn write_column_index(
        &self,
        index: &ColumnIndexMetaData,
        column: &ColumnChunkMetaData,
        row_group: usize,
        column_index: usize,
        sink: impl Write,
    ) -> Result<bool> {
        self.write_index(
            index,
            column,
            ThriftStruct::ColumnIndex,
            (row_group, column_index),
            sink,
        )?;
        Ok(true)
    }

    fn write_index(
        &self,
        object: &impl WriteThrift,
        column: &ColumnChunkMetaData,
        kind: ThriftStruct,
        position: (usize, usize),
        sink: impl Write,
    ) -> Result<()> {
        #[cfg(feature = "encryption")]
        if let Some(encryptor) = &self.file_encryptor {
            let module = match kind {
                ThriftStruct::OffsetIndex => ModuleType::OffsetIndex,
                ThriftStruct::ColumnIndex => ModuleType::ColumnIndex,
                _ => unreachable!("only page indexes use this helper"),
            };
            return self.write_thrift_object_with_encryption(
                object, sink, encryptor, column, module, position,
            );
        }
        #[cfg(not(feature = "encryption"))]
        let _ = column;
        self.write_thrift_object(object, sink, || {
            SiteInfo::column(kind, Some(position.0), Some(position.1))
        })
    }

    fn apply_row_group_encryption(
        &self,
        row_groups: Vec<RowGroupMetaData>,
    ) -> Result<(Vec<RowGroupMetaData>, Option<Vec<RowGroupMetaData>>)> {
        #[cfg(feature = "encryption")]
        if let Some(encryptor) = &self.file_encryptor {
            let unencrypted = row_groups.clone();
            return Ok((
                self.encrypt_row_groups(row_groups, encryptor)?,
                Some(unencrypted),
            ));
        }
        Ok((row_groups, None))
    }

    fn get_file_magic(&self) -> &[u8; 4] {
        #[cfg(feature = "encryption")]
        {
            get_file_magic(self.file_encryptor.as_ref().map(|e| e.properties()))
        }
        #[cfg(not(feature = "encryption"))]
        {
            get_file_magic()
        }
    }

    #[cfg(feature = "encryption")]
    fn write_thrift_object_with_encryption(
        &self,
        object: &impl WriteThrift,
        mut sink: impl Write,
        file_encryptor: &FileEncryptor,
        column_metadata: &ColumnChunkMetaData,
        module_type: ModuleType,
        (row_group_index, column_index): (usize, usize),
    ) -> Result<()> {
        let column_path_vec = column_metadata.column_path().as_ref();

        let joined_column_path;
        let column_path = if column_path_vec.len() == 1 {
            &column_path_vec[0]
        } else {
            joined_column_path = column_path_vec.join(".");
            &joined_column_path
        };

        let encrypted = file_encryptor.is_column_encrypted(column_path);
        let kind = match module_type {
            ModuleType::OffsetIndex => ThriftStruct::OffsetIndex,
            ModuleType::ColumnIndex => ThriftStruct::ColumnIndex,
            _ => unreachable!("Only indexes use this helper"),
        };
        let root = || SiteInfo::column(kind, Some(row_group_index), Some(column_index));
        if encrypted {
            let aad = create_module_aad(
                file_encryptor.file_aad(),
                module_type,
                row_group_index,
                column_index,
                None,
            )?;
            let mut encryptor = file_encryptor.get_column_encryptor(column_path)?;
            encrypt_thrift(
                |buffer| self.write_thrift_object(object, buffer, root),
                &mut encryptor,
                &mut sink,
                &aad,
            )
        } else {
            self.write_thrift_object(object, sink, root)
        }
    }

    #[cfg(feature = "encryption")]
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

    #[cfg(feature = "encryption")]
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

    #[cfg(feature = "encryption")]
    fn file_crypto_metadata(file_encryptor: &'_ FileEncryptor) -> Result<FileCryptoMetaData<'_>> {
        let properties = file_encryptor.properties();
        Ok(FileCryptoMetaData {
            encryption_algorithm: Self::encryption_algorithm_from_encryptor(file_encryptor),
            key_metadata: properties.footer_key_metadata().map(|v| v.as_slice()),
        })
    }

    #[cfg(feature = "encryption")]
    fn encrypt_row_groups(
        &self,
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
                        self.encrypt_column_chunk(c, file_encryptor, rg_idx, col_idx)
                    })
                    .collect();
                rg.columns = cols?;
                Ok(rg)
            })
            .collect()
    }

    /// Apply column encryption to column chunk metadata
    #[cfg(feature = "encryption")]
    fn encrypt_column_chunk(
        &self,
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
                struct ColumnMeta<'a>(&'a ColumnChunkMetaData);
                impl WriteThrift for ColumnMeta<'_> {
                    const ELEMENT_TYPE: crate::parquet_thrift::ElementType =
                        crate::parquet_thrift::ElementType::Struct;
                    fn write_thrift<W: Write, const TRACK: bool>(
                        &self,
                        writer: &mut ThriftCompactOutputProtocol<W, TRACK>,
                    ) -> Result<()> {
                        serialize_column_meta_data(self.0, writer, true)
                    }
                }
                let metadata = ColumnMeta(&column_chunk);
                self.write_thrift_object(&metadata, &mut buffer, || {
                    SiteInfo::column(
                        ThriftStruct::ColumnMetaData,
                        Some(row_group_index),
                        Some(column_index),
                    )
                })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::metadata::extensions::{MetadataSite, ThriftPathSegment, ThriftStruct};
    use crate::file::metadata::page_index::PageIndexBuilder;
    use crate::file::metadata::thrift_fields::{file_meta_data, offset_index};
    use crate::file::page_index::offset_index::PageLocation;
    use crate::schema::parser::parse_message_type;

    // Reuse one small buffer, discarding the output rather than allocating GiBs.
    struct RepeatedPayload(Vec<u8>);

    impl MetadataExtensionHook for RepeatedPayload {
        fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
            use ThriftPathSegment::{Element, Field};
            let coordinates = crate::file::metadata::extensions::test_util::coordinates(site)?;
            Ok(match (site.root(), coordinates.as_slice()) {
                (ThriftStruct::OffsetIndex, [Field(offset_index::PAGE_LOCATIONS), Element(_)])
                | (
                    ThriftStruct::FileMetaData,
                    [Field(file_meta_data::KEY_VALUE_METADATA), Element(_)],
                ) => Some(self.0.as_slice()),
                _ => None,
            })
        }
    }

    fn metadata(key_value_metadata: Option<Vec<KeyValue>>) -> ParquetMetaData {
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            parse_message_type("message m { REQUIRED INT32 id; }").unwrap(),
        )));
        let column = ColumnChunkMetaData::builder(schema.column(0))
            .build()
            .unwrap();
        let row_group = RowGroupMetaData::builder(schema.clone())
            .set_column_metadata(vec![column])
            .build()
            .unwrap();
        ParquetMetaDataBuilder::new(FileMetaData::new(
            1,
            0,
            None,
            key_value_metadata,
            schema,
            None,
        ))
        .set_row_groups(vec![row_group])
        .build()
    }

    #[cfg(feature = "experimental")]
    #[test]
    fn test_metadata_extension_footer_suffix() {
        use crate::file::metadata::extensions::{
            MetadataExtensionSelection, UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE,
        };
        let metadata = metadata(None);
        let mut bytes = Vec::new();
        ParquetMetaDataWriter::new(&mut bytes, &metadata)
            .with_metadata_extension_hook(
                [MetadataExtensionSelection::roots([ThriftStruct::FileMetaData]).root_only()],
                |site: &mut MetadataSite<'_>| -> Result<Option<&'static [u8]>> {
                    assert!(site.at_root());
                    Ok(Some(b"footer"))
                },
            )
            .unwrap()
            .finish()
            .unwrap();
        let end = bytes.len() - UNSIGNED_FOOTER_EXTENSION_SUFFIX_SIZE;
        assert_eq!(&bytes[end - 6..end], b"footer");
    }

    #[test]
    fn test_metadata_extension_offset_index_length_overflow() {
        for page_count in [2047, 2048] {
            let mut page_index = PageIndexBuilder::new(1, 1);
            page_index
                .try_put_offset_index(
                    OffsetIndexMetaData {
                        page_locations: (0..page_count)
                            .map(|i| PageLocation {
                                offset: i,
                                compressed_page_size: 1,
                                first_row_index: i,
                            })
                            .collect(),
                        unencoded_byte_array_data_bytes: None,
                    },
                    0,
                    0,
                )
                .unwrap();
            let metadata = metadata(None)
                .into_builder()
                .set_page_index(Some(Arc::new(page_index.build())))
                .build();
            let mut sink = TrackedWrite::new(std::io::sink());
            let result = ParquetMetaDataWriter::new(&mut sink, &metadata)
                .with_metadata_extension_hook(
                    [Default::default()],
                    RepeatedPayload(vec![0; 1024 * 1024]),
                )
                .unwrap()
                .finish();
            if page_count == 2047 {
                result.unwrap();
                assert!(sink.bytes_written() < i32::MAX as usize);
            } else {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "Parquet error: Offset index length exceeds i32::MAX bytes"
                );
                assert!(sink.bytes_written() > i32::MAX as usize);
            }
        }
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_metadata_extension_footer_length_overflow() {
        for entry_count in [4095, 4096] {
            let metadata = metadata(Some(
                (0..entry_count)
                    .map(|i| KeyValue::new(i.to_string(), None))
                    .collect(),
            ));
            let mut sink = TrackedWrite::new(std::io::sink());
            let result = ParquetMetaDataWriter::new(&mut sink, &metadata)
                .with_metadata_extension_hook(
                    [Default::default()],
                    RepeatedPayload(vec![0; 1024 * 1024]),
                )
                .unwrap()
                .finish();
            if entry_count == 4095 {
                result.unwrap();
                assert!(sink.bytes_written() < u32::MAX as usize);
            } else {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "Parquet error: File metadata length exceeds u32::MAX bytes"
                );
                assert!(sink.bytes_written() > u32::MAX as usize);
            }
        }
    }
}
