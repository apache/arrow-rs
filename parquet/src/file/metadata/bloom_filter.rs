// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Sparse decoded Bloom filters keyed by row-group and column coordinates.

use crate::DecodeResult;
use crate::bloom_filter::{Sbbf, SbbfPushDecoder};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::column_chunk::{ColumnChunkEntries, ColumnChunkKey};
use crate::file::metadata::{
    ColumnChunkMask, ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData,
};
use crate::file::reader::ChunkReader;
use bytes::Bytes;
use std::ops::Range;

/// Sparse decoded Bloom filters keyed by row group and physical leaf column.
///
/// Load a set using [`ParquetMetaDataReader::read_bloom_filters`], which also
/// provides an example, or its asynchronous counterpart when enabled.
/// Use [`Self::get`] with original file coordinates to look up a filter; selection
/// does not renumber row groups or columns.
///
/// [`ParquetMetaDataReader::read_bloom_filters`]: crate::file::metadata::ParquetMetaDataReader::read_bloom_filters
#[derive(Debug, Clone)]
pub struct SbbfSet {
    entries: ColumnChunkEntries<Sbbf>,
}

impl SbbfSet {
    /// Looks up a decoded filter by row group and physical leaf column.
    ///
    /// Returns `Ok(None)` for unselected chunks or chunks without a Bloom filter
    /// offset, and an error for out-of-bounds coordinates. To check whether the file contains
    /// a filter for the chunk, consult [`ColumnChunkMetaData::bloom_filter_offset`].
    /// A `None` offset indicates a missing filter.
    pub fn get(&self, row_group_idx: usize, column_idx: usize) -> Result<Option<&Sbbf>> {
        self.entries.get(row_group_idx, column_idx)
    }
}

pub(crate) struct SbbfBatch {
    num_row_groups: usize,
    num_columns: usize,
    entries: Vec<(ColumnChunkKey, Option<Sbbf>)>,
    decoders: Vec<(usize, Option<SbbfPushDecoder>)>,
}

impl SbbfBatch {
    pub(crate) fn try_new(metadata: &ParquetMetaData, mask: &ColumnChunkMask) -> Result<Self> {
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.file_metadata().schema_descr().num_columns();
        let columns = mask
            .column_chunk_indices(num_row_groups, num_columns)
            .map(|(row, column)| Ok((row, column, metadata.column_chunk(row, column)?)));
        Self::try_from_columns(num_row_groups, num_columns, columns)
    }

    pub(crate) fn try_new_row_group(
        metadata: &RowGroupMetaData,
        column_mask: &ColumnChunkMask,
    ) -> Result<Self> {
        let num_columns = metadata.num_columns();
        let columns = column_mask
            .column_indices(num_columns)
            .map(|column| Ok((0, column, metadata.column(column))));
        Self::try_from_columns(1, num_columns, columns)
    }

    fn try_from_columns<'a>(
        num_row_groups: usize,
        num_columns: usize,
        columns: impl IntoIterator<Item = Result<(usize, usize, &'a ColumnChunkMetaData)>>,
    ) -> Result<Self> {
        let mut entries = Vec::new();
        let mut decoders = Vec::new();
        for column in columns {
            let (row, column, metadata) = column?;
            if let Some(decoder) = SbbfPushDecoder::try_new(metadata)? {
                decoders.push((entries.len(), Some(decoder)));
                entries.push((ColumnChunkKey::try_new(row, column)?, None));
            }
        }
        Ok(Self {
            num_row_groups,
            num_columns,
            entries,
            decoders,
        })
    }

    pub(crate) fn read<R: ChunkReader>(mut self, reader: &R) -> Result<SbbfSet> {
        loop {
            let (owners, ranges) = self.requests()?;
            if ranges.is_empty() {
                return self.finish();
            }
            let buffers = ranges
                .iter()
                .map(|range| {
                    reader.get_bytes(range.start, usize::try_from(range.end - range.start)?)
                })
                .collect::<Result<Vec<_>>>()?;
            self.push_responses(owners, ranges, buffers)?;
        }
    }

    pub(super) fn requests(&mut self) -> Result<(Vec<usize>, Vec<Range<u64>>)> {
        let mut owners = Vec::new();
        let mut ranges = Vec::new();
        for (index, (entry, decoder)) in self.decoders.iter_mut().enumerate() {
            let Some(active) = decoder.as_mut() else {
                continue;
            };
            match active.try_decode()? {
                DecodeResult::NeedsData(requests) => {
                    for range in requests {
                        owners.push(index);
                        ranges.push(range);
                    }
                }
                DecodeResult::Data(filter) => {
                    self.entries[*entry].1 = Some(filter);
                    *decoder = None;
                }
                DecodeResult::Finished => {
                    return Err(ParquetError::General(
                        "Bloom filter decoder finished without returning a filter".into(),
                    ));
                }
            }
        }
        Ok((owners, ranges))
    }

    pub(super) fn push_responses(
        &mut self,
        owners: Vec<usize>,
        ranges: Vec<Range<u64>>,
        buffers: Vec<Bytes>,
    ) -> Result<()> {
        if ranges.len() != buffers.len() {
            return Err(ParquetError::General(format!(
                "Bloom filter response count mismatch: expected {}, got {}",
                ranges.len(),
                buffers.len()
            )));
        }
        // Validate the entire response before dispatching any bytes to decoders.
        for (range, buffer) in ranges.iter().zip(&buffers) {
            if u64::try_from(buffer.len())? != range.end - range.start {
                return Err(ParquetError::General(format!(
                    "Bloom filter response length mismatch for range {range:?}: got {}",
                    buffer.len()
                )));
            }
        }
        for ((owner, range), buffer) in owners.into_iter().zip(ranges).zip(buffers) {
            self.decoders[owner]
                .1
                .as_mut()
                .ok_or_else(|| ParquetError::General("Bloom filter decoder is not active".into()))?
                .push_range(range, buffer)?;
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<SbbfSet> {
        let entries = self
            .entries
            .into_iter()
            .map(|(key, filter)| {
                let filter = filter.ok_or_else(|| {
                    ParquetError::General(
                        "Bloom filter batch finished without decoding a filter".into(),
                    )
                })?;
                Ok((key, filter))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(SbbfSet {
            entries: ColumnChunkEntries::try_from_sorted_entries(
                self.num_row_groups,
                self.num_columns,
                entries,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use std::sync::Arc;

    #[test]
    fn row_group_storage_is_sparse_and_local() {
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "message schema { REQUIRED INT32 a; REQUIRED INT32 b; REQUIRED INT32 c; }",
            )
            .unwrap(),
        )));
        let mut filter = Sbbf::new_with_num_of_bytes(32);
        filter.insert(&42_i32);
        let mut bytes = Vec::new();
        filter.write(&mut bytes).unwrap();
        let columns = (0..3)
            .map(|column| {
                let builder = ColumnChunkMetaData::builder(schema.column(column));
                if column == 1 {
                    builder
                        .set_bloom_filter_offset(Some(0))
                        .set_bloom_filter_length(Some(bytes.len() as i32))
                        .build()
                        .unwrap()
                } else {
                    builder.build().unwrap()
                }
            })
            .collect();
        let metadata = RowGroupMetaData::builder(schema)
            .set_column_metadata(columns)
            .set_ordinal(7)
            .build()
            .unwrap();
        let bytes = Bytes::from(bytes);
        let filters = SbbfBatch::try_new_row_group(&metadata, &ColumnChunkMask::all())
            .unwrap()
            .read(&bytes)
            .unwrap();
        assert_eq!(filters.entries.iter().count(), 1);
        assert!(filters.get(0, 1).unwrap().unwrap().check(&42_i32));
        assert!(filters.get(0, 0).unwrap().is_none());
        assert!(filters.get(0, 2).unwrap().is_none());
        assert!(filters.get(7, 1).is_err());

        // No filter bytes are needed when loading is disabled or only absent columns are selected.
        for mask in [ColumnChunkMask::none(), ColumnChunkMask::columns([0, 2])] {
            let filters = SbbfBatch::try_new_row_group(&metadata, &mask)
                .unwrap()
                .read(&Bytes::new())
                .unwrap();
            assert_eq!(filters.entries.iter().count(), 0);
            assert!(filters.get(0, 1).unwrap().is_none());
            assert!(filters.get(0, 3).is_err());
        }
    }

    #[test]
    fn batch_finish_rejects_undecoded_filter() {
        let batch = SbbfBatch {
            num_row_groups: 1,
            num_columns: 1,
            entries: vec![(ColumnChunkKey::try_new(0, 0).unwrap(), None)],
            decoders: vec![],
        };
        assert!(batch.finish().is_err());
    }
}
