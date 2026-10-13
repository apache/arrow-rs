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

//! Sparse storage keyed by row-group and column coordinates.

use crate::errors::{ParquetError, Result};
use crate::file::metadata::memory::HeapSize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ColumnChunkKey(u64);

impl ColumnChunkKey {
    pub(crate) fn try_new(row_group_idx: usize, column_idx: usize) -> Result<Self> {
        let row_group_idx = u32::try_from(row_group_idx).map_err(|_| {
            ParquetError::General(format!(
                "row group index exceeds packed coordinate limit: {row_group_idx}"
            ))
        })?;
        let column_idx = u32::try_from(column_idx).map_err(|_| {
            ParquetError::General(format!(
                "column index exceeds packed coordinate limit: {column_idx}"
            ))
        })?;
        Ok(Self(
            (u64::from(row_group_idx) << u32::BITS) | u64::from(column_idx),
        ))
    }

    pub(crate) fn coordinates(self) -> (usize, usize) {
        ((self.0 >> u32::BITS) as usize, (self.0 as u32) as usize)
    }
}

impl HeapSize for ColumnChunkKey {
    fn heap_size(&self) -> usize {
        0
    }
}

fn validate_dimensions(num_row_groups: usize, num_columns: usize) -> Result<()> {
    // Parquet metadata collection lengths are represented by signed 32-bit Thrift values.
    i32::try_from(num_row_groups).map_err(|_| {
        ParquetError::General(format!(
            "row group count exceeds i32::MAX: {num_row_groups}"
        ))
    })?;
    i32::try_from(num_columns).map_err(|_| {
        ParquetError::General(format!("column count exceeds i32::MAX: {num_columns}"))
    })?;
    Ok(())
}

#[derive(Debug, PartialEq)]
pub(crate) struct ColumnChunkMap<T> {
    num_row_groups: usize,
    num_columns: usize,
    entries: HashMap<ColumnChunkKey, Arc<T>>,
}

impl<T> Clone for ColumnChunkMap<T> {
    fn clone(&self) -> Self {
        Self {
            num_row_groups: self.num_row_groups,
            num_columns: self.num_columns,
            entries: self.entries.clone(),
        }
    }
}

impl<T> ColumnChunkMap<T> {
    pub(crate) fn try_new(num_row_groups: usize, num_columns: usize) -> Result<Self> {
        validate_dimensions(num_row_groups, num_columns)?;
        Ok(Self {
            num_row_groups,
            num_columns,
            entries: HashMap::new(),
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn key(&self, row_group_idx: usize, column_idx: usize) -> Result<ColumnChunkKey> {
        if row_group_idx >= self.num_row_groups || column_idx >= self.num_columns {
            return Err(ParquetError::General(format!(
                "column chunk coordinate out of bounds: row group {row_group_idx}, column \
                 {column_idx}; dimensions are {} by {}",
                self.num_row_groups, self.num_columns
            )));
        }
        ColumnChunkKey::try_new(row_group_idx, column_idx)
    }

    /// Inserts or replaces an entry.
    pub(crate) fn try_insert(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
        value: Arc<T>,
    ) -> Result<Option<Arc<T>>> {
        let key = self.key(row_group_idx, column_idx)?;
        if !self.entries.contains_key(&key) {
            self.entries.try_reserve(1).map_err(|err| {
                ParquetError::General(format!(
                    "failed to allocate column chunk map entry at row group {row_group_idx}, \
                     column {column_idx}: {err}"
                ))
            })?;
        }
        Ok(self.entries.insert(key, value))
    }

    pub(crate) fn try_freeze(self) -> Result<ColumnChunkEntries<T>> {
        validate_dimensions(self.num_row_groups, self.num_columns)?;
        let num_entries = self.entries.len();
        let mut entries = Vec::new();
        entries.try_reserve_exact(num_entries).map_err(|err| {
            ParquetError::General(format!(
                "failed to allocate storage for {num_entries} column chunk entries: {err}"
            ))
        })?;
        for entry in self.entries {
            entries.push(entry);
        }
        entries.sort_unstable_by_key(|(key, _)| *key);
        Ok(ColumnChunkEntries {
            num_row_groups: self.num_row_groups,
            num_columns: self.num_columns,
            entries: entries.into_boxed_slice(),
        })
    }
}

impl<T: HeapSize> HeapSize for ColumnChunkMap<T> {
    fn heap_size(&self) -> usize {
        self.entries.heap_size()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct ColumnChunkEntries<T> {
    num_row_groups: usize,
    num_columns: usize,
    entries: Box<[(ColumnChunkKey, Arc<T>)]>,
}

impl<T> Clone for ColumnChunkEntries<T> {
    fn clone(&self) -> Self {
        Self {
            num_row_groups: self.num_row_groups,
            num_columns: self.num_columns,
            entries: self.entries.clone(),
        }
    }
}

impl<T> ColumnChunkEntries<T> {
    pub(crate) fn try_from_rows(rows: Vec<Vec<Option<T>>>) -> Result<Self> {
        let num_row_groups = rows.len();
        let num_columns = rows.iter().map(Vec::len).max().unwrap_or_default();
        validate_dimensions(num_row_groups, num_columns)?;
        let num_entries = rows
            .iter()
            .map(|columns| columns.iter().filter(|value| value.is_some()).count())
            .try_fold(0_usize, |total, count| total.checked_add(count))
            .ok_or_else(|| {
                ParquetError::General("column chunk entry count exceeds usize::MAX".to_string())
            })?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(num_entries).map_err(|err| {
            ParquetError::General(format!(
                "failed to allocate storage for {num_entries} column chunk entries: {err}"
            ))
        })?;
        for (row_group_idx, columns) in rows.into_iter().enumerate() {
            for (column_idx, value) in columns.into_iter().enumerate() {
                if let Some(value) = value {
                    entries.push((
                        ColumnChunkKey::try_new(row_group_idx, column_idx)?,
                        Arc::new(value),
                    ));
                }
            }
        }
        Ok(Self {
            num_row_groups,
            num_columns,
            entries: entries.into_boxed_slice(),
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn key(&self, row_group_idx: usize, column_idx: usize) -> Result<ColumnChunkKey> {
        if row_group_idx >= self.num_row_groups || column_idx >= self.num_columns {
            return Err(ParquetError::General(format!(
                "column chunk coordinate out of bounds: row group {row_group_idx}, column \
                 {column_idx}; dimensions are {} by {}",
                self.num_row_groups, self.num_columns
            )));
        }
        ColumnChunkKey::try_new(row_group_idx, column_idx)
    }

    pub(crate) fn get(&self, row_group_idx: usize, column_idx: usize) -> Result<Option<&T>> {
        let key = self.key(row_group_idx, column_idx)?;
        let Ok(entry_idx) = self
            .entries
            .binary_search_by_key(&key, |(entry_key, _)| *entry_key)
        else {
            return Ok(None);
        };
        let Some((_, value)) = self.entries.get(entry_idx) else {
            return Err(ParquetError::General(format!(
                "column chunk binary search returned invalid entry index {entry_idx}"
            )));
        };
        Ok(Some(value.as_ref()))
    }

    #[cfg_attr(not(test), expect(unused))]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (ColumnChunkKey, &T)> {
        self.entries
            .iter()
            .map(|(key, value)| (*key, value.as_ref()))
    }

    pub(crate) fn into_iter(self) -> impl Iterator<Item = (ColumnChunkKey, Arc<T>)> {
        self.entries.into_vec().into_iter()
    }

    pub(crate) fn try_into_map(self) -> Result<ColumnChunkMap<T>> {
        let mut map = ColumnChunkMap::try_new(self.num_row_groups, self.num_columns)?;
        for (key, value) in self.entries {
            let (row_group_idx, column_idx) = key.coordinates();
            map.try_insert(row_group_idx, column_idx, value)?;
        }
        Ok(map)
    }
}

impl<T: HeapSize> HeapSize for ColumnChunkEntries<T> {
    fn heap_size(&self) -> usize {
        let entries_size = std::mem::size_of_val(self.entries.as_ref());
        let mut values_size = 0;
        for (idx, (_, value)) in self.entries.iter().enumerate() {
            if self.entries[..idx]
                .iter()
                .any(|(_, previous)| Arc::ptr_eq(previous, value))
            {
                continue;
            }
            values_size += value.heap_size();
        }
        entries_size + values_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_storage_is_sorted_and_validates_coordinates() {
        let mut map = ColumnChunkMap::try_new(2, 3).unwrap();
        map.try_insert(1, 2, Arc::new(12)).unwrap();
        map.try_insert(0, 1, Arc::new(1)).unwrap();
        let entries = map.try_freeze().unwrap();

        let coordinates = entries
            .iter()
            .map(|(key, _)| key.coordinates())
            .collect::<Vec<_>>();
        assert_eq!(coordinates, vec![(0, 1), (1, 2)]);
        assert_eq!(entries.get(0, 1).unwrap(), Some(&1));
        assert_eq!(entries.get(0, 2).unwrap(), None);
        assert!(entries.get(2, 0).is_err());
        assert!(entries.get(0, 3).is_err());
    }

    #[test]
    fn zero_and_invalid_dimensions() {
        assert!(ColumnChunkMap::<()>::try_new(0, 0).is_ok());
        assert!(ColumnChunkMap::<()>::try_new(i32::MAX as usize + 1, 1).is_err());
        assert!(ColumnChunkMap::<()>::try_new(1, i32::MAX as usize + 1).is_err());
    }

    #[test]
    fn freeze_and_thaw_preserve_entries() {
        let mut map = ColumnChunkMap::try_new(2, 2).unwrap();
        map.try_insert(1, 0, Arc::new(3)).unwrap();
        let entries = map.try_freeze().unwrap();
        let thawed = entries.clone().try_into_map().unwrap();
        assert_eq!(thawed.try_freeze().unwrap(), entries);
    }
}
