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

//! [`PageStoreWriter`] — writes Arrow data to a content-addressed page store.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, SchemaRef};

use super::{MANIFEST_KEY, PageRef, PageStoreManifest};
use crate::arrow::ArrowSchemaConverter;
use crate::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnChunkData, ArrowColumnWriterImpl, ArrowRowGroupWriter,
    SharedColumnChunk,
};
use crate::column::chunker::ContentDefinedChunker;
use crate::column::page::{CompressedPage, PageWriteSpec, PageWriter};
use crate::column::writer::{GenericColumnWriter, get_column_writer};
use crate::errors::Result;
use crate::file::metadata::{
    FileMetaData, KeyValue, ParquetMetaData, ParquetMetaDataBuilder, ParquetMetaDataWriter,
    RowGroupMetaData,
};
use crate::file::page_index::column_index::ColumnIndexMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::properties::{
    CdcOptions, EnabledStatistics, WriterProperties, WriterPropertiesPtr,
};
use crate::parquet_thrift::{ThriftCompactOutputProtocol, WriteThrift};
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr, SchemaDescriptor};

// ---------------------------------------------------------------------------
// ContentAddressedPageWriter — internal PageWriter impl
// ---------------------------------------------------------------------------

/// A [`PageWriter`] that writes each page to a content-addressed store directory.
struct ContentAddressedPageWriter {
    buffer: SharedColumnChunk,
    store_dir: PathBuf,
    page_refs: Arc<Mutex<Vec<PageRef>>>,
    row_group: usize,
    column: usize,
    page_count: usize,
}

impl ContentAddressedPageWriter {
    fn new(
        store_dir: PathBuf,
        page_refs: Arc<Mutex<Vec<PageRef>>>,
        row_group: usize,
        column: usize,
    ) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(ArrowColumnChunkData::default())),
            store_dir,
            page_refs,
            row_group,
            column,
            page_count: 0,
        }
    }
}

impl PageWriter for ContentAddressedPageWriter {
    fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
        let page_header = page.to_thrift_header()?;
        let mut header_bytes = Vec::with_capacity(256);
        {
            let mut protocol = ThriftCompactOutputProtocol::new(&mut header_bytes);
            page_header.write_thrift(&mut protocol)?;
        }
        let header = Bytes::from(header_bytes);

        let data = page.compressed_page().buffer().clone();
        let compressed_size = data.len() + header.len();

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        hasher.update(&data);
        let hash = hasher.finalize();
        let hash_hex = hash.to_hex().to_string();

        let page_path = self.store_dir.join(format!("{hash_hex}.page"));
        if !page_path.exists() {
            let mut file = fs::File::create(&page_path)?;
            file.write_all(&header)?;
            file.write_all(&data)?;
        }

        let mut buf = self.buffer.try_lock().unwrap();
        let offset = buf.length as u64;
        buf.length += compressed_size;
        buf.data.push(header.clone());
        buf.data.push(data);

        let is_dict = page.page_type() == crate::basic::PageType::DICTIONARY_PAGE;
        self.page_refs.lock().unwrap().push(PageRef {
            row_group: self.row_group,
            column: self.column,
            page_index: self.page_count,
            offset: offset as i64,
            size: compressed_size as i32,
            hash: hash_hex,
            is_dict,
        });
        self.page_count += 1;

        let mut spec = PageWriteSpec::new();
        spec.page_type = page.page_type();
        spec.num_values = page.num_values();
        spec.uncompressed_size = page.uncompressed_size() + header.len();
        spec.offset = offset;
        spec.compressed_size = compressed_size;
        spec.bytes_written = compressed_size as u64;
        Ok(spec)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Column writer factory
// ---------------------------------------------------------------------------

fn create_column_writers(
    schema: &SchemaDescriptor,
    arrow_schema: &SchemaRef,
    props: &WriterPropertiesPtr,
    store_dir: &Path,
    page_refs: &Arc<Mutex<Vec<PageRef>>>,
    row_group: usize,
) -> Result<Vec<crate::arrow::arrow_writer::ArrowColumnWriter>> {
    let mut writers = Vec::new();
    let mut leaves = schema.columns().iter();
    let mut col_idx = 0usize;
    for field in &arrow_schema.fields {
        create_writers_for_type(
            field.data_type(),
            props,
            &mut leaves,
            store_dir,
            page_refs,
            row_group,
            &mut col_idx,
            &mut writers,
        )?;
    }
    Ok(writers)
}

fn make_column_writer(
    desc: &ColumnDescPtr,
    props: &WriterPropertiesPtr,
    store_dir: &Path,
    page_refs: &Arc<Mutex<Vec<PageRef>>>,
    row_group: usize,
    col_idx: usize,
    use_byte_array: bool,
) -> Result<crate::arrow::arrow_writer::ArrowColumnWriter> {
    let pw = Box::new(ContentAddressedPageWriter::new(
        store_dir.to_path_buf(),
        page_refs.clone(),
        row_group,
        col_idx,
    ));
    let chunk: SharedColumnChunk = pw.buffer.clone();

    let writer = if use_byte_array {
        ArrowColumnWriterImpl::ByteArray(GenericColumnWriter::new(desc.clone(), props.clone(), pw))
    } else {
        ArrowColumnWriterImpl::Column(get_column_writer(desc.clone(), props.clone(), pw))
    };

    Ok(crate::arrow::arrow_writer::ArrowColumnWriter { chunk, writer })
}

#[allow(clippy::too_many_arguments)]
fn create_writers_for_type(
    data_type: &ArrowDataType,
    props: &WriterPropertiesPtr,
    leaves: &mut std::slice::Iter<'_, ColumnDescPtr>,
    store_dir: &Path,
    page_refs: &Arc<Mutex<Vec<PageRef>>>,
    row_group: usize,
    col_idx: &mut usize,
    out: &mut Vec<crate::arrow::arrow_writer::ArrowColumnWriter>,
) -> Result<()> {
    let col = |idx: &mut usize, leaves: &mut std::slice::Iter<'_, ColumnDescPtr>| {
        let desc = leaves.next().unwrap();
        let i = *idx;
        *idx += 1;
        make_column_writer(desc, props, store_dir, page_refs, row_group, i, false)
    };

    let bytes = |idx: &mut usize, leaves: &mut std::slice::Iter<'_, ColumnDescPtr>| {
        let desc = leaves.next().unwrap();
        let i = *idx;
        *idx += 1;
        make_column_writer(desc, props, store_dir, page_refs, row_group, i, true)
    };

    match data_type {
        _ if data_type.is_primitive() => out.push(col(col_idx, leaves)?),
        ArrowDataType::FixedSizeBinary(_) | ArrowDataType::Boolean | ArrowDataType::Null => {
            out.push(col(col_idx, leaves)?)
        }
        ArrowDataType::LargeBinary
        | ArrowDataType::Binary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View => out.push(bytes(col_idx, leaves)?),
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _)
        | ArrowDataType::ListView(f)
        | ArrowDataType::LargeListView(f) => {
            create_writers_for_type(
                f.data_type(),
                props,
                leaves,
                store_dir,
                page_refs,
                row_group,
                col_idx,
                out,
            )?;
        }
        ArrowDataType::Struct(fields) => {
            for field in fields {
                create_writers_for_type(
                    field.data_type(),
                    props,
                    leaves,
                    store_dir,
                    page_refs,
                    row_group,
                    col_idx,
                    out,
                )?;
            }
        }
        ArrowDataType::Map(f, _) => match f.data_type() {
            ArrowDataType::Struct(f) => {
                create_writers_for_type(
                    f[0].data_type(),
                    props,
                    leaves,
                    store_dir,
                    page_refs,
                    row_group,
                    col_idx,
                    out,
                )?;
                create_writers_for_type(
                    f[1].data_type(),
                    props,
                    leaves,
                    store_dir,
                    page_refs,
                    row_group,
                    col_idx,
                    out,
                )?;
            }
            _ => unreachable!("invalid map type"),
        },
        ArrowDataType::Dictionary(_, value_type) => match value_type.as_ref() {
            ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::Utf8View
            | ArrowDataType::BinaryView
            | ArrowDataType::FixedSizeBinary(_) => out.push(bytes(col_idx, leaves)?),
            _ => out.push(col(col_idx, leaves)?),
        },
        _ => {
            return Err(crate::errors::ParquetError::NYI(format!(
                "PageStoreWriter: unsupported Arrow type {data_type}"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// PageStoreWriter
// ---------------------------------------------------------------------------

/// Writes Arrow [`RecordBatch`]es to a content-addressed page store.
///
/// Each data page is written as a separate file named by its BLAKE3 hash
/// under `store_dir`. The metadata-only Parquet file is written to an
/// explicit path on [`Self::finish`], containing the schema, row group
/// metadata, and a manifest mapping page locations to their hashes.
///
/// A single `store_dir` can hold pages belonging to many Parquet files;
/// identical pages across files are automatically deduplicated.
///
/// # Example
/// ```no_run
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array, RecordBatch};
/// # use parquet::arrow::page_store::PageStoreWriter;
/// # use parquet::file::properties::WriterProperties;
/// let batch = RecordBatch::try_from_iter(vec![
///     ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
/// ]).unwrap();
///
/// let store = std::env::temp_dir().join("pages");
/// let mut writer = PageStoreWriter::try_new(&store, batch.schema(), None).unwrap();
/// writer.write(&batch).unwrap();
/// writer.finish(store.join("table_a.parquet")).unwrap();
/// ```
pub struct PageStoreWriter {
    store_dir: PathBuf,
    schema: SchemaDescPtr,
    arrow_schema: SchemaRef,
    props: WriterPropertiesPtr,
    page_refs: Arc<Mutex<Vec<PageRef>>>,
    row_groups: Vec<RowGroupMetaData>,
    column_indexes: Vec<Vec<ColumnIndexMetaData>>,
    offset_indexes: Vec<Vec<OffsetIndexMetaData>>,
    in_progress: Option<ArrowRowGroupWriter>,
    cdc_chunkers: Option<Vec<ContentDefinedChunker>>,
    row_group_index: usize,
    total_rows: i64,
    next_page_offset: i64,
}

impl PageStoreWriter {
    /// Create a new `PageStoreWriter`.
    ///
    /// Creates `store_dir` if it does not exist.
    pub fn try_new(
        store_dir: impl Into<PathBuf>,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let store_dir = store_dir.into();
        fs::create_dir_all(&store_dir)?;

        let props = props.unwrap_or_else(|| {
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_content_defined_chunking(Some(CdcOptions::default()))
                .build()
        });

        let cdc_default = CdcOptions::default();
        let cdc_opts = props.content_defined_chunking().or(Some(&cdc_default));

        let schema = {
            let converter = ArrowSchemaConverter::new().with_coerce_types(props.coerce_types());
            converter.convert(&arrow_schema)?
        };

        let schema_descr = Arc::new(SchemaDescriptor::new(schema.root_schema_ptr()));

        let cdc_chunkers = cdc_opts
            .map(|opts| {
                schema_descr
                    .columns()
                    .iter()
                    .map(|desc| ContentDefinedChunker::new(desc, opts))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let props_ptr = Arc::new(props);

        Ok(Self {
            store_dir,
            schema: schema_descr,
            arrow_schema,
            props: props_ptr,
            page_refs: Arc::new(Mutex::new(Vec::new())),
            row_groups: Vec::new(),
            column_indexes: Vec::new(),
            offset_indexes: Vec::new(),
            in_progress: None,
            cdc_chunkers,
            row_group_index: 0,
            total_rows: 0,
            next_page_offset: 0,
        })
    }

    /// Write a [`RecordBatch`] to the page store.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.in_progress.is_none() {
            let writers = create_column_writers(
                &self.schema,
                &self.arrow_schema,
                &self.props,
                &self.store_dir,
                &self.page_refs,
                self.row_group_index,
            )?;
            self.in_progress = Some(ArrowRowGroupWriter::new(writers, &self.arrow_schema));
        }

        let in_progress = self.in_progress.as_mut().unwrap();
        match self.cdc_chunkers.as_mut() {
            Some(chunkers) => in_progress.write_with_chunkers(batch, chunkers)?,
            None => in_progress.write(batch)?,
        }
        Ok(())
    }

    /// Flush the current row group.
    pub fn flush(&mut self) -> Result<()> {
        let in_progress = match self.in_progress.take() {
            Some(ip) => ip,
            None => return Ok(()),
        };

        let buffered_rows = in_progress.buffered_rows;
        let chunks: Vec<ArrowColumnChunk> = in_progress.close()?;

        let mut column_metadata = Vec::with_capacity(chunks.len());
        let mut col_indexes: Vec<ColumnIndexMetaData> = Vec::with_capacity(chunks.len());
        let mut off_indexes: Vec<OffsetIndexMetaData> = Vec::with_capacity(chunks.len());
        let mut total_byte_size = 0i64;

        let mut cumulative_offset: i64 = self.next_page_offset;

        for (col_idx, chunk) in chunks.into_iter().enumerate() {
            let mut close = chunk.close;
            total_byte_size += close.metadata.uncompressed_size();

            let src_dict_offset = close.metadata.dictionary_page_offset();
            let src_data_offset = close.metadata.data_page_offset();
            let src_start = src_dict_offset.unwrap_or(src_data_offset);
            let delta = cumulative_offset - src_start;

            let mut col_builder = close.metadata.into_builder();
            col_builder = col_builder.set_data_page_offset(src_data_offset + delta);
            if let Some(dict_off) = src_dict_offset {
                col_builder = col_builder.set_dictionary_page_offset(Some(dict_off + delta));
            }
            close.metadata = col_builder.build()?;

            if let Some(ref mut oi) = close.offset_index {
                for loc in &mut oi.page_locations {
                    loc.offset += delta;
                }
            }

            {
                let mut page_refs = self.page_refs.lock().unwrap();
                for pr in page_refs.iter_mut() {
                    if pr.row_group == self.row_group_index && pr.column == col_idx {
                        pr.offset += delta;
                    }
                }
            }

            cumulative_offset += close.metadata.compressed_size();

            column_metadata.push(close.metadata);
            col_indexes.push(close.column_index.unwrap_or(ColumnIndexMetaData::NONE));
            if let Some(oi) = close.offset_index {
                off_indexes.push(oi);
            } else {
                off_indexes.push(OffsetIndexMetaData {
                    page_locations: vec![],
                    unencoded_byte_array_data_bytes: None,
                });
            }
        }

        self.next_page_offset = cumulative_offset;

        let row_group = RowGroupMetaData::builder(self.schema.clone())
            .set_column_metadata(column_metadata)
            .set_total_byte_size(total_byte_size)
            .set_num_rows(buffered_rows as i64)
            .set_ordinal(self.row_group_index as i16)
            .build()?;

        self.total_rows += buffered_rows as i64;
        self.row_groups.push(row_group);
        self.column_indexes.push(col_indexes);
        self.offset_indexes.push(off_indexes);
        self.row_group_index += 1;
        Ok(())
    }

    /// Flush remaining data and write the metadata-only Parquet file to `path`.
    pub fn finish(mut self, path: impl AsRef<Path>) -> Result<ParquetMetaData> {
        self.flush()?;

        let page_refs = self.page_refs.lock().unwrap().clone();
        let manifest = PageStoreManifest { pages: page_refs };
        let manifest_json = serde_json::to_string(&manifest)
            .map_err(|e| crate::errors::ParquetError::General(e.to_string()))?;

        let file_metadata = FileMetaData::new(
            2,
            self.total_rows,
            Some("parquet-rs page_store".to_string()),
            Some(vec![KeyValue::new(MANIFEST_KEY.to_string(), manifest_json)]),
            self.schema.clone(),
            None,
        );

        let mut builder = ParquetMetaDataBuilder::new(file_metadata);
        for rg in self.row_groups {
            builder = builder.add_row_group(rg);
        }
        builder = builder.set_column_index(Some(self.column_indexes));
        builder = builder.set_offset_index(Some(self.offset_indexes));
        let metadata = builder.build();

        let file = fs::File::create(path.as_ref())?;
        ParquetMetaDataWriter::new(file, &metadata).finish()?;

        Ok(metadata)
    }
}
