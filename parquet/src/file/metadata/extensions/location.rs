// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! On-demand structural discovery using the shared compact-protocol primitives.
//! Scanning advances within a scope and restarts when the scope changes. It never
//! invokes callbacks or inspects input beyond the current event.

use super::{DecoderScope, ThriftPathSegment};
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{
    ElementType, FieldType, ThriftCompactInputProtocol, ThriftSliceInputProtocol,
};

#[derive(Clone, Copy)]
enum Frame {
    Struct {
        last: i16,
        restore: usize,
    },
    Sequence {
        kind: FieldType,
        index: usize,
        size: usize,
        restore: usize,
    },
    Map {
        key: FieldType,
        value: FieldType,
        index: usize,
        size: usize,
        is_value: bool,
        restore: usize,
    },
}

pub(super) struct PathLocator {
    scope: DecoderScope,
    offset: usize,
    target: Option<usize>,
    failed: bool,
    frames: Vec<Frame>,
    path: Vec<ThriftPathSegment>,
}

impl PathLocator {
    pub(super) fn new(scope: DecoderScope) -> Self {
        let prefix = scope.prefix;
        Self {
            scope,
            offset: 0,
            target: None,
            failed: false,
            frames: vec![Frame::Struct {
                last: 0,
                restore: prefix.wire_len(),
            }],
            path: (0..prefix.wire_len())
                .filter_map(|i| prefix.coordinate(i))
                .collect(),
        }
    }

    /// Reuse allocation when entering a different known decoder scope. A failed
    /// cursor remains failed for the entire root, even if the listener ignored it.
    fn reset(&mut self, scope: DecoderScope) {
        let prefix = scope.prefix;
        self.scope = scope;
        self.offset = 0;
        self.target = None;
        self.frames.clear();
        self.frames.push(Frame::Struct {
            last: 0,
            restore: prefix.wire_len(),
        });
        self.path.clear();
        self.path
            .extend((0..prefix.wire_len()).filter_map(|i| prefix.coordinate(i)));
    }

    /// Change the replay anchor only on a location query, not on every decoded
    /// object. Returning to a parent scope does not clear a prior failure.
    pub(super) fn locate_scoped(
        &mut self,
        input: &[u8],
        end: usize,
        scope: DecoderScope,
    ) -> Result<&[ThriftPathSegment]> {
        if self.scope.start != scope.start || self.scope.prefix != scope.prefix {
            self.reset(scope);
        }
        let bytes = input
            .get(scope.start..)
            .ok_or_else(|| general_err!("Invalid metadata scope start"))?;
        let target = end
            .checked_sub(scope.start)
            .ok_or_else(|| general_err!("Invalid metadata scope end"))?;
        self.locate(bytes, target)
    }

    fn locate(&mut self, bytes: &[u8], target: usize) -> Result<&[ThriftPathSegment]> {
        if self.failed {
            return Err(general_err!("Extension location cursor previously failed"));
        }
        if self.target == Some(target) {
            return Ok(&self.path);
        }
        self.failed = true;
        if target <= self.offset || target > bytes.len() {
            return Err(general_err!("Invalid extension location offset"));
        }

        // Limit the cursor to already-consumed input. Context resolution must
        // not inspect unauthenticated future data or report a later parse error.
        let mut input = ThriftSliceInputProtocol::new(&bytes[self.offset..target]);
        while let Some(frame) = self.frames.pop() {
            let restore = self.path.len();
            let (kind, segment, extension) = match frame {
                Frame::Struct { last, restore } => {
                    let field = input.read_field_begin(last)?;
                    if field.field_type == FieldType::Stop {
                        self.path.truncate(restore);
                        continue;
                    }
                    self.frames.push(Frame::Struct {
                        last: field.id,
                        restore,
                    });
                    (
                        field.field_type,
                        ThriftPathSegment::Field(field.id),
                        field.id == i16::MAX,
                    )
                }
                Frame::Sequence {
                    kind,
                    index,
                    size,
                    restore,
                } => {
                    if index == size {
                        self.path.truncate(restore);
                        continue;
                    }
                    self.frames.push(Frame::Sequence {
                        kind,
                        index: index + 1,
                        size,
                        restore,
                    });
                    (kind, ThriftPathSegment::Element(index), false)
                }
                Frame::Map {
                    key,
                    value,
                    index,
                    size,
                    is_value,
                    restore,
                } => {
                    if index == size {
                        self.path.truncate(restore);
                        continue;
                    }
                    self.frames.push(Frame::Map {
                        key,
                        value,
                        index: index + usize::from(is_value),
                        size,
                        is_value: !is_value,
                        restore,
                    });
                    if is_value {
                        (value, ThriftPathSegment::MapValue(index), false)
                    } else {
                        (key, ThriftPathSegment::MapKey(index), false)
                    }
                }
            };
            match kind {
                FieldType::Struct | FieldType::List | FieldType::Set | FieldType::Map => {
                    if self.path.len() == super::reader::MAX_READ_PATH_SEGMENTS {
                        return Err(general_err!("Extension location exceeds structural limits"));
                    }
                    self.path.push(segment);
                    let frame = match kind {
                        FieldType::Struct => Frame::Struct { last: 0, restore },
                        FieldType::List | FieldType::Set => {
                            let list = input.read_list_begin()?;
                            Frame::Sequence {
                                kind: collection_kind(list.element_type),
                                index: 0,
                                size: list.size as usize,
                                restore,
                            }
                        }
                        FieldType::Map => {
                            let size = i32::try_from(input.read_vlq()?)
                                .map_err(|_| general_err!("Invalid map size"))?
                                as usize;
                            let (key, value) = if size == 0 {
                                (FieldType::Byte, FieldType::Byte)
                            } else {
                                let kinds = input.read_byte()?;
                                (
                                    collection_kind(ElementType::try_from(kinds >> 4)?),
                                    collection_kind(ElementType::try_from(kinds & 15)?),
                                )
                            };
                            Frame::Map {
                                key,
                                value,
                                index: 0,
                                size,
                                is_value: false,
                                restore,
                            }
                        }
                        _ => unreachable!(),
                    };
                    self.frames.push(frame);
                }
                other => input.skip(other)?,
            }
            if extension && kind == FieldType::Binary && input.as_slice().is_empty() {
                self.offset = target;
                self.target = Some(target);
                self.failed = false;
                return Ok(&self.path);
            }
        }
        Err(general_err!("Extension location is not a binary field"))
    }
}

fn collection_kind(kind: ElementType) -> FieldType {
    // Collection booleans have a byte, unlike inline boolean struct fields.
    if kind == ElementType::Bool {
        FieldType::Byte
    } else {
        kind.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::metadata::extensions::FooterScope;

    #[test]
    fn scoped_cursor_reuses_locations_without_inspecting_future_bytes() {
        // Reserved binary field containing "x", followed by invalid future input.
        let bytes = [8, 254, 255, 3, 1, b'x', 255];
        let scope = DecoderScope {
            prefix: FooterScope::Schema(7),
            start: 0,
        };
        let mut cursor = PathLocator::new(scope);
        assert_eq!(cursor.scope.prefix, scope.prefix);
        assert_eq!(cursor.target, None);
        let expected: Vec<_> = (0..scope.prefix.wire_len())
            .filter_map(|i| scope.prefix.coordinate(i))
            .collect();
        assert_eq!(cursor.locate_scoped(&bytes, 6, scope).unwrap(), expected);
        let cached = cursor.path.as_ptr();
        assert_eq!(cursor.locate_scoped(&bytes, 6, scope).unwrap(), expected);
        assert_eq!(cursor.path.as_ptr(), cached);
        let parent = DecoderScope::default();
        assert!(cursor.locate_scoped(&bytes, 6, parent).unwrap().is_empty());
        // An explicit later lookup now fails. Its error must invalidate even an
        // earlier cached empty path, rather than advertise false exhaustion.
        assert!(cursor.locate_scoped(&bytes, 7, parent).is_err());
        assert!(cursor.locate_scoped(&bytes, 6, parent).is_err());
    }

    #[test]
    fn changing_scope_does_not_clear_a_failed_cursor() {
        let scope = DecoderScope::default();
        let mut cursor = PathLocator::new(scope);
        assert!(cursor.locate_scoped(&[255], 1, scope).is_err());
        let next = DecoderScope {
            prefix: FooterScope::RowGroup(3),
            start: 0,
        };
        let error = cursor
            .locate_scoped(&[8, 254, 255, 3, 0], 5, next)
            .unwrap_err();
        assert!(error.to_string().contains("previously failed"));
        assert!(cursor.failed);
    }
}
