// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Support for reading [`ColumnIndexMetaData`] and [`OffsetIndexMetaData`] from parquet metadata.

use crate::basic::{BoundaryOrder, Type};
use crate::data_type::Int96;
use crate::errors::{ParquetError, Result};
#[cfg(all(test, feature = "experimental"))]
use crate::file::metadata::extensions::ThriftStruct;
use crate::file::metadata::extensions::{ObserverHandle, SiteInfo};
use crate::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftCompactOutputProtocol,
    ThriftSliceInputProtocol, WriteThrift, WriteThriftField, read_thrift_vec,
};
use crate::thrift_struct;
use std::io::Write;

/// Decode a Thrift [`OffsetIndex`] from the provided bytes.
///
/// The passed in bytes contain a serialized Thrift `OffsetIndex` struct as
/// read from a Parquet file.
///
/// Returns an [`OffsetIndexMetaData`] containing page location information.
///
/// # Example
///
/// ```
/// # use parquet::file::reader::{FileReader, SerializedFileReader};
/// # use parquet::file::page_index::index_reader::decode_offset_index;
/// # use std::fs::File;
/// # use std::io::{Read, Seek};
/// # use parquet::errors::Result;
/// #
/// # fn read_offset_index() -> Result<()> {
/// // Open the Parquet file
/// let mut file = File::open("data.parquet")?;
/// let reader = SerializedFileReader::new(file.try_clone()?)?;
/// let metadata = reader.metadata();
///
/// // Select a row group and column to read
/// let row_group_idx = 0;
/// let column_idx = 0;
///
/// // Get the column chunk metadata
/// let row_group = metadata.row_group(row_group_idx);
/// let column_chunk = row_group.column(column_idx);
///
/// // Get the offset index byte range from the column metadata
/// if let Some(range) = column_chunk.offset_index_range() {
///     // Read the offset index bytes from the file
///     let mut buffer = vec![0u8; (range.end - range.start) as usize];
///     file.seek(std::io::SeekFrom::Start(range.start))?;
///     file.read_exact(&mut buffer)?;
///
///     // Decode the offset index
///     let offset_index = decode_offset_index(&buffer)?;
///
///     // Access page location information
///     for (i, page_location) in offset_index.page_locations().iter().enumerate() {
///         println!("Page {}: offset={}, size={}, first_row={}",
///             i,
///             page_location.offset,
///             page_location.compressed_page_size,
///             page_location.first_row_index
///         );
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/e94a5d090b324a0c0ee1adbb8ea6b099852dc3cc/src/main/thrift/parquet.thrift#L1253-L1273
pub fn decode_offset_index(data: &[u8]) -> Result<OffsetIndexMetaData, ParquetError> {
    read_offset_index(&mut ThriftSliceInputProtocol::new(data))
}

pub(crate) fn read_offset_index(
    prot: &mut ThriftSliceInputProtocol<'_>,
) -> Result<OffsetIndexMetaData> {
    let data = prot.as_slice();
    match OffsetIndexMetaData::try_from_fast(prot) {
        Ok((offset_index, _)) => Ok(offset_index),
        Err(_) => {
            *prot = ThriftSliceInputProtocol::new(data);
            OffsetIndexMetaData::read_thrift(prot)
        }
    }
}

/// Try only the strict, callback-free decoder before entering observed decoding.
/// Success requires canonical locations and either STOP or one final root
/// extension. Root payloads are delivered only after the probe succeeds. Other
/// unknown fields/layouts fall back before any event is sent.
pub(crate) fn read_offset_index_with_listener(
    data: &[u8],
    listener: &ObserverHandle,
    root: SiteInfo,
) -> Result<OffsetIndexMetaData> {
    if let Ok((index, extension)) =
        OffsetIndexMetaData::try_from_fast(&mut ThriftSliceInputProtocol::new(data))
    {
        if let Some(payload) = extension {
            listener.observe_root(&root, payload)?;
        }
        return Ok(index);
    }
    crate::file::metadata::extensions::with_read_extensions!(
        ThriftSliceInputProtocol::new(data),
        Some(listener),
        root,
        with_observer,
        |mut prot| OffsetIndexMetaData::read_thrift(&mut prot)
    )
}

use crate::file::metadata::thrift_fields::column_index as fields;

// private struct only used for decoding then discarded
thrift_struct!(@root

pub(super) struct ThriftColumnIndex<'a> {
  fields::NULL_PAGES: required list<bool> null_pages
  fields::MIN_VALUES: required list<'a><binary> min_values
  fields::MAX_VALUES: required list<'a><binary> max_values
  fields::BOUNDARY_ORDER: required BoundaryOrder boundary_order
  fields::NULL_COUNTS: optional list<i64> null_counts
  fields::REPETITION_LEVEL_HISTOGRAMS: optional list<i64> repetition_level_histograms;
  fields::DEFINITION_LEVEL_HISTOGRAMS: optional list<i64> definition_level_histograms;
  fields::NAN_COUNTS: optional list<i64> nan_counts
}
);

/// Decode a Thrift [`ColumnIndex`] from the provided bytes.
///
/// The passed in bytes contain a serialized Thrift `OffsetIndex` struct as
/// read from a Parquet file. The `column_type` can be obtained via
/// [`ColumnChunkMetaData::column_type`].
///
/// Returns a [`ColumnIndexMetaData`] containing per-page statistics.
///
/// # Example
///
/// ```
/// # use parquet::file::reader::{FileReader, SerializedFileReader};
/// # use parquet::file::page_index::index_reader::decode_column_index;
/// # use std::fs::File;
/// # use std::io::{Read, Seek};
/// # use parquet::errors::Result;
/// #
/// # fn read_column_index() -> Result<()> {
/// // Open the Parquet file
/// let mut file = File::open("data.parquet")?;
/// let reader = SerializedFileReader::new(file.try_clone()?)?;
/// let metadata = reader.metadata();
///
/// // Select a row group and column to read
/// let row_group_idx = 0;
/// let column_idx = 0;
///
/// // Get the column chunk metadata
/// let row_group = metadata.row_group(row_group_idx);
/// let column_chunk = row_group.column(column_idx);
///
/// // Get the column index byte range from the column metadata
/// if let Some(range) = column_chunk.column_index_range() {
///     // Get the column type for proper deserialization
///     let column_type = column_chunk.column_type();
///
///     // Read the column index bytes from the file
///     let mut buffer = vec![0u8; (range.end - range.start) as usize];
///     file.seek(std::io::SeekFrom::Start(range.start))?;
///     file.read_exact(&mut buffer)?;
///
///     // Decode the column index
///     let column_index = decode_column_index(&buffer, column_type)?;
///
///     // Access per-page statistics (example for INT32 column)
///     use parquet::file::page_index::column_index::ColumnIndexMetaData;
///     match column_index {
///         ColumnIndexMetaData::INT32(index) => {
///             for (i, (min, max)) in index.min_values().iter()
///                 .zip(index.max_values().iter())
///                 .enumerate() {
///                 println!("Page {}: min={}, max={}", i, min, max);
///             }
///         }
///         _ => println!("Column is not INT32 type"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`ColumnChunkMetaData::column_type`]: crate::file::metadata::ColumnChunkMetaData::column_type
/// [`ColumnIndex`]: https://github.com/apache/parquet-format/blob/e94a5d090b324a0c0ee1adbb8ea6b099852dc3cc/src/main/thrift/parquet.thrift#L1275-1373
pub fn decode_column_index(
    data: &[u8],
    column_type: Type,
) -> Result<ColumnIndexMetaData, ParquetError> {
    read_column_index(&mut ThriftSliceInputProtocol::new(data), column_type)
}

pub(crate) fn read_column_index<S: crate::parquet_thrift::InputTracking>(
    prot: &mut ThriftSliceInputProtocol<'_, S>,
    column_type: Type,
) -> Result<ColumnIndexMetaData> {
    let index = ThriftColumnIndex::read_thrift(prot)?;

    let index = match column_type {
        Type::BOOLEAN => {
            ColumnIndexMetaData::BOOLEAN(PrimitiveColumnIndex::<bool>::try_from_thrift(index)?)
        }
        Type::INT32 => {
            ColumnIndexMetaData::INT32(PrimitiveColumnIndex::<i32>::try_from_thrift(index)?)
        }
        Type::INT64 => {
            ColumnIndexMetaData::INT64(PrimitiveColumnIndex::<i64>::try_from_thrift(index)?)
        }
        Type::INT96 => {
            ColumnIndexMetaData::INT96(PrimitiveColumnIndex::<Int96>::try_from_thrift(index)?)
        }
        Type::FLOAT => {
            ColumnIndexMetaData::FLOAT(PrimitiveColumnIndex::<f32>::try_from_thrift(index)?)
        }
        Type::DOUBLE => {
            ColumnIndexMetaData::DOUBLE(PrimitiveColumnIndex::<f64>::try_from_thrift(index)?)
        }
        Type::BYTE_ARRAY => {
            ColumnIndexMetaData::BYTE_ARRAY(ByteArrayColumnIndex::try_from_thrift(index)?)
        }
        Type::FIXED_LEN_BYTE_ARRAY => {
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(ByteArrayColumnIndex::try_from_thrift(index)?)
        }
    };

    Ok(index)
}

#[cfg(all(test, feature = "experimental"))]
mod extension_tests {
    use super::*;
    use crate::file::metadata::extensions::{MetadataSite, ThriftPathSegment};
    use std::sync::{Arc, Mutex};

    const ROOT: SiteInfo = SiteInfo::column(ThriftStruct::OffsetIndex, Some(2), Some(3));
    // One canonical PageLocation: offset=1, size=2, first_row=3.
    const PLAIN: &[u8] = &[0x19, 0x1c, 0x16, 2, 0x15, 4, 0x16, 6, 0, 0];
    const EXT: &[u8] = &[8, 0xfe, 0xff, 3, 1, 42];

    #[test]
    fn strict_index_probe_never_delivers_callbacks() {
        use ThriftPathSegment::{Element, Field};
        let mut root = PLAIN[..PLAIN.len() - 1].to_vec();
        root.extend(EXT);
        root.push(0);
        let mut probe = ThriftSliceInputProtocol::new(&root);
        let (_, payload) = OffsetIndexMetaData::try_from_fast(&mut probe).unwrap();
        assert_eq!(payload, Some(&[42][..]));
        assert!(probe.as_slice().is_empty());
        let mut child = PLAIN[..PLAIN.len() - 2].to_vec();
        child.extend(EXT);
        child.extend([0, 0]);
        let mut unknown = PLAIN[..PLAIN.len() - 1].to_vec();
        unknown.push(0x2c);
        unknown.extend(EXT);
        unknown.extend([0, 0]); // field 3: unknown struct
        let mut scalar = PLAIN[..PLAIN.len() - 1].to_vec();
        scalar.extend([0x25, 2]);
        scalar.extend(EXT);
        scalar.push(0); // field 3: unknown i32
        let mut optional = PLAIN[..PLAIN.len() - 1].to_vec();
        optional.extend([0x19, 0x16, 18, 0]); // field 2: list<i64> [9]
        let mut duplicate = root[..root.len() - 1].to_vec();
        duplicate.extend(EXT);
        duplicate.push(0);
        let mut after = root[..root.len() - 1].to_vec();
        after.extend([0x05, 6, 2, 0]);
        let mut early = EXT.to_vec();
        early.extend([0x09, 2]);
        early.extend(&PLAIN[1..]);
        let mut optional_extension = optional[..optional.len() - 1].to_vec();
        optional_extension.extend(EXT);
        optional_extension.push(0);
        for (bytes, paths) in [
            (PLAIN.to_vec(), vec![]),
            (optional, vec![]),
            (root, vec![vec![]]),
            (child, vec![vec![Field(1), Element(0)]]),
            (unknown, vec![vec![Field(3)]]),
            (scalar, vec![vec![]]),
            (duplicate, vec![vec![], vec![]]),
            (after, vec![vec![]]),
            (early, vec![vec![]]),
            (optional_extension, vec![vec![]]),
        ] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let events = seen.clone();
            let listener =
                ObserverHandle::listener(move |event: &mut MetadataSite<'_>, payload: &[u8]| {
                    assert_eq!(
                        crate::file::metadata::extensions::test_util::entry(event)?,
                        ROOT
                    );
                    assert_eq!(payload, [42]);
                    events.lock().unwrap().push(
                        crate::file::metadata::extensions::test_util::coordinates(event)?,
                    );
                    Ok(())
                });
            let decoded = read_offset_index_with_listener(&bytes, &listener, ROOT).unwrap();
            let plain = decode_offset_index(&bytes).unwrap();
            assert_eq!(decoded, plain);
            assert_eq!(*seen.lock().unwrap(), paths);
        }
    }

    #[test]
    fn index_probe_bounds_untrusted_list_size_before_allocating() {
        let bytes = [0x19, 0xfc, 0xff, 0xff, 0xff, 0xff, 0x07, 0];
        let listener = ObserverHandle::listener(|_: &mut MetadataSite<'_>, _payload: &[u8]| {
            panic!("No extension was decoded")
        });
        assert!(decode_offset_index(&bytes).is_err());
        assert!(read_offset_index_with_listener(&bytes, &listener, ROOT).is_err());
    }

    #[test]
    fn index_fallback_preserves_callback_errors_and_later_parse_errors() {
        let calls = Arc::new(Mutex::new(0));
        let observed = calls.clone();
        let listener =
            ObserverHandle::listener(move |_: &mut MetadataSite<'_>, _payload: &[u8]| {
                *observed.lock().unwrap() += 1;
                Err(ParquetError::NYI("extension sentinel".into()))
            });
        let mut bytes = PLAIN[..PLAIN.len() - 1].to_vec();
        bytes.extend(EXT);
        bytes.push(0);
        let error = read_offset_index_with_listener(&bytes, &listener, ROOT).unwrap_err();
        assert!(matches!(error, ParquetError::NYI(message) if message == "extension sentinel"));
        assert_eq!(*calls.lock().unwrap(), 1);

        let calls = Arc::new(Mutex::new(0));
        let observed = calls.clone();
        let listener =
            ObserverHandle::listener(move |_: &mut MetadataSite<'_>, _payload: &[u8]| {
                *observed.lock().unwrap() += 1;
                Ok(())
            });
        bytes.pop(); // Error after the root extension, not an authentication event.
        assert!(read_offset_index_with_listener(&bytes, &listener, ROOT).is_err());
        assert_eq!(*calls.lock().unwrap(), 1);
        assert!(read_offset_index_with_listener(&PLAIN[..3], &listener, ROOT).is_err());
        assert_eq!(*calls.lock().unwrap(), 1);
    }
}
