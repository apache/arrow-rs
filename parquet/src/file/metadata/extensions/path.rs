// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Bounded ancestry shared by tracked writers and non-rewindable input.

use super::ThriftPathSegment;
use crate::errors::{ParquetError, Result};

// Known Parquet metadata reaches six frames (footer, schema element, logical
// type, timestamp/time, time unit, empty unit) and at most eight coordinates.
// User schemas are flattened. These are serializer bounds, not wire limits;
// readers add the unknown-value skip budget. New codec shapes must update them.
pub(super) const MAX_THRIFT_FRAMES: usize = 6;
pub(super) const MAX_THRIFT_PATH_SEGMENTS: usize = 8;

// Keep ordinary metadata paths small; allocate only for deeper unknown values.
// All slots are initialized, and the caller separately enforces its wire budget.
enum Slots<T, const INLINE: usize> {
    Inline([T; INLINE]),
    Spill(Vec<T>),
}

impl<T: Copy, const I: usize> Slots<T, I> {
    fn new(value: T) -> Self {
        Self::Inline([value; I])
    }

    #[inline]
    fn as_slice(&self) -> &[T] {
        match self {
            Self::Inline(values) => values,
            Self::Spill(values) => values,
        }
    }

    #[inline]
    fn set(&mut self, index: usize, value: T) {
        if let Self::Inline(values) = self
            && index < I
        {
            values[index] = value;
        } else {
            self.set_spilled(index, value);
        }
    }

    #[cold]
    #[inline(never)]
    fn set_spilled(&mut self, index: usize, value: T) {
        if let Self::Inline(values) = self {
            *self = Self::Spill(values.to_vec());
        }
        if let Self::Spill(values) = self {
            if index >= values.len() {
                values.resize(index + 1, value);
            }
            values[index] = value;
        }
    }
}

pub(crate) struct ThriftPath<const FRAMES: usize, const SEGMENTS: usize> {
    path: Slots<ThriftPathSegment, MAX_THRIFT_PATH_SEGMENTS>,
    path_len: usize,
    parents: Slots<usize, MAX_THRIFT_FRAMES>,
    depth: usize,
    #[cfg(test)]
    pub(crate) max_depth: usize,
}

impl<const F: usize, const P: usize> Default for ThriftPath<F, P> {
    fn default() -> Self {
        Self {
            path: Slots::new(ThriftPathSegment::Element(0)),
            path_len: 0,
            parents: Slots::new(0),
            depth: 0,
            #[cfg(test)]
            max_depth: 0,
        }
    }
}

impl<const F: usize, const P: usize> ThriftPath<F, P> {
    #[inline]
    pub(crate) fn path(&self) -> &[ThriftPathSegment] {
        &self.path.as_slice()[..self.path_len]
    }

    pub(crate) fn reset(&mut self) {
        self.path_len = 0;
        self.depth = 0;
        #[cfg(test)]
        {
            self.max_depth = 0;
        }
    }

    /// Enter a struct (including a union envelope), returning its frame index.
    #[inline]
    pub(crate) fn begin(&mut self) -> Result<usize> {
        if self.depth >= F {
            return Err(general_err!(
                "Thrift metadata exceeds extension frame depth"
            ));
        }
        self.parents.set(self.depth, self.path_len);
        let frame = self.depth;
        self.depth += 1;
        #[cfg(test)]
        {
            self.max_depth = self.max_depth.max(self.depth);
        }
        Ok(frame)
    }

    /// Leave a struct, restoring its path and returning its frame index.
    #[inline]
    pub(crate) fn end(&mut self) -> Result<usize> {
        self.depth = self
            .depth
            .checked_sub(1)
            .ok_or_else(|| general_err!("Unbalanced extension struct"))?;
        self.path_len = self.parents.as_slice()[self.depth];
        Ok(self.depth)
    }

    #[inline]
    pub(crate) fn field(&mut self, id: i16) -> Result<()> {
        self.path_len = self.struct_path()?.len();
        self.push(ThriftPathSegment::Field(id))?;
        Ok(())
    }

    /// Add a collection coordinate, returning the previous path length.
    #[inline]
    pub(crate) fn push(&mut self, segment: ThriftPathSegment) -> Result<usize> {
        if self.path_len >= P {
            return Err(general_err!("Thrift metadata exceeds extension path depth"));
        }
        self.path.set(self.path_len, segment);
        let len = self.path_len;
        self.path_len += 1;
        Ok(len)
    }

    #[inline]
    pub(crate) fn truncate(&mut self, len: usize) {
        self.path_len = self.path_len.min(len);
    }

    /// Path of the current struct, excluding its current field.
    #[inline]
    pub(crate) fn struct_path(&self) -> Result<&[ThriftPathSegment]> {
        let parent = self
            .depth
            .checked_sub(1)
            .ok_or_else(|| general_err!("Missing extension struct scope"))?;
        Ok(&self.path.as_slice()[..self.parents.as_slice()[parent]])
    }

    #[cfg(any(debug_assertions, test))]
    pub(crate) fn finish(&self) -> Result<()> {
        if self.depth != 0 || self.path_len != 0 {
            return Err(general_err!("Unbalanced extension struct"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ThriftPathSegment::{Element, Field};

    #[test]
    fn nested_paths_restore_parent_coordinates() {
        let mut path = ThriftPath::<3, 5>::default();
        assert_eq!(path.begin().unwrap(), 0);
        path.field(4).unwrap();
        let before_element = path.push(Element(2)).unwrap();
        assert_eq!(path.begin().unwrap(), 1);
        path.field(7).unwrap();
        assert_eq!(path.struct_path().unwrap(), [Field(4), Element(2)]);
        assert_eq!(path.path(), [Field(4), Element(2), Field(7)]);
        path.end().unwrap();
        path.truncate(before_element);
        path.field(8).unwrap();
        assert_eq!(path.path(), [Field(8)]);
        path.end().unwrap();
        path.finish().unwrap();
    }

    #[test]
    fn bounded_nested_paths_match_reference() {
        for depth in 1..=6 {
            for mask in 0..(1 << (depth - 1)) {
                let mut path = ThriftPath::<6, 8>::default();
                let mut parents = vec![vec![]];
                let mut expected = Vec::new();
                path.begin().unwrap();
                let mut overflow = false;
                for level in 1..depth {
                    expected.push(Field(level as i16));
                    let mut result = path.field(level as i16);
                    if result.is_ok() && mask & (1 << (level - 1)) != 0 {
                        expected.push(Element(level));
                        result = path.push(Element(level)).map(|_| ());
                    }
                    if result.is_err() {
                        assert!(expected.len() > 8);
                        overflow = true;
                        break;
                    }
                    path.begin().unwrap();
                    parents.push(expected.clone());
                }
                if !overflow {
                    if expected.len() < 8 {
                        path.field(99).unwrap();
                    }
                    for expected in parents.into_iter().rev() {
                        path.end().unwrap();
                        assert_eq!(path.path(), expected);
                    }
                    path.finish().unwrap();
                }
                path.reset();
                path.begin().unwrap();
                path.end().unwrap();
                path.finish().unwrap();
            }
        }
    }

    #[test]
    fn bounds_and_balance_are_checked() {
        let mut path = ThriftPath::<1, 1>::default();
        assert!(path.end().is_err());
        assert!(path.field(1).is_err());
        path.begin().unwrap();
        assert!(path.begin().is_err());
        path.field(1).unwrap();
        assert!(path.push(Element(0)).is_err());
        assert!(path.finish().is_err());
        path.end().unwrap();
        path.finish().unwrap();
        path.push(Element(0)).unwrap();
        assert!(path.finish().is_err());
    }
}
