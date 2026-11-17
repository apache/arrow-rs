// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Sample-based dictionary selection, adapted from Comet's `DictionaryChooser` by Andy Grove.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{
    Array, OffsetSizeTrait, RecordBatch, downcast_dictionary_array, downcast_run_array,
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{DataType, Schema};

use super::{
    FixedLenByteArrayVisitor, PrimitiveVisitor, visit_fixed_len_byte_array, visit_i32, visit_i64,
};
use crate::arrow::ArrowSchemaConverter;
use crate::basic::Type as PhysicalType;
use crate::data_type::AsBytes;
use crate::errors::{ParquetError, Result};
use crate::file::properties::{WriterProperties, WriterVersion};

/// Controls the first-page approximation used by [`choose_dictionary_encoding`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DictionaryEncodingOptions {
    /// The first top-level row at which page size and row limits are checked.
    ///
    /// Defaults to parquet-java's `parquet.page.size.row.check.min`, 100. Must be positive.
    pub min_row_count_for_page_size_check: usize,
}

impl Default for DictionaryEncodingOptions {
    fn default() -> Self {
        Self {
            min_row_count_for_page_size_check: 100,
        }
    }
}

/// Disables dictionaries whose sampled first-page encoded size is not smaller than plain size.
///
/// A dictionary is kept only when `index bytes + dictionary bytes < plain bytes`, counting
/// sizes as parquet-java does. Index bytes include the one-byte bit width. This opt-in helper
/// only disables per-column dictionaries; writer defaults and other properties are unchanged.
///
/// Exceeding the dictionary-size limit also disables the dictionary. Each leaf samples from
/// the start of `sample` across batch boundaries until that limit is exceeded, the first page
/// ends, the row-group row limit is reached, or input ends.
/// Page limits count top-level rows, including null and empty lists; pages end between rows.
/// Starting at the first check, the page ends at its row limit or when raw value bytes reach
/// the page-size limit less parquet-java's 10% tolerance. Each leaf inspects at most
/// `max(column page-row limit, first check)` top-level rows, capped by the row-group row limit.
///
/// Supply the beginning of the row group to evaluate. Row-group byte limits are not simulated;
/// the caller must exclude rows belonging to later row groups. An incomplete sample is judged
/// as if the chunk ended there, which can reject a dictionary useful over the full row group.
/// The caller owns buffering and decides whether to reuse the properties for later row groups.
///
/// # Compatibility
///
/// This approximates parquet-java's first-page size comparison. It checks every row after the
/// first check and excludes repetition/definition-level bytes, so page boundaries can differ
/// in either direction. Floating-point keys use raw bits: signed zeroes and distinct NaN
/// payloads are distinct; parquet-java may merge NaNs with different payloads into one entry.
///
/// For nonempty Parquet v1 samples, fixed-length byte-array dictionaries are disabled to match
/// parquet-java, even though the Rust writer supports them. Boolean columns are unchanged.
/// INT96 is outside this Arrow-schema interface because [`super::ArrowWriter`] cannot write it.
/// Physical types and paths use [`ArrowSchemaConverter`] and `props.coerce_types()`, not a
/// custom schema supplied through [`super::ArrowWriterOptions::with_parquet_schema`].
///
/// Options and all batch fields are validated first. Empty samples, including lists of empty
/// batches, return the original properties unchanged.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{ArrayRef, Int64Array, RecordBatch};
/// use parquet::arrow::arrow_writer::{choose_dictionary_encoding, DictionaryEncodingOptions};
/// use parquet::file::properties::WriterProperties;
/// use parquet::schema::types::ColumnPath;
///
/// let sample = RecordBatch::try_from_iter([
///     ("id", Arc::new(Int64Array::from_iter_values(0..1000)) as ArrayRef),
/// ])?;
/// let props = choose_dictionary_encoding(
///     &sample.schema(), WriterProperties::default(), &[sample.clone()],
///     &DictionaryEncodingOptions::default(),
/// )?;
/// assert!(!props.dictionary_enabled(&ColumnPath::from("id")));
/// // Pass props to ArrowWriter::try_new before writing the sampled rows and remaining data.
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns an error for a zero first-check interval, mismatched batch fields, or a schema/value
/// conversion unsupported by the Arrow writer.
pub fn choose_dictionary_encoding(
    schema: &Schema,
    props: WriterProperties,
    sample: &[RecordBatch],
    options: &DictionaryEncodingOptions,
) -> Result<WriterProperties> {
    let first_check = options.min_row_count_for_page_size_check;
    if first_check == 0 {
        return Err(ParquetError::General(
            "The minimum page-size-check row count must be positive".into(),
        ));
    }
    for batch in sample {
        if batch.schema_ref().fields() != schema.fields() {
            return Err(ParquetError::General(
                "Dictionary sample fields do not match the Arrow schema".into(),
            ));
        }
    }
    if !sample.iter().any(|batch| batch.num_rows() != 0) {
        return Ok(props);
    }
    let mut leaves = Vec::new();
    for (column, field) in schema.fields().iter().enumerate() {
        collect_leaves(
            field.data_type(),
            &mut Leaf {
                column,
                steps: Vec::new(),
            },
            &mut leaves,
        )?;
    }
    let parquet = ArrowSchemaConverter::new()
        .with_coerce_types(props.coerce_types())
        .convert(schema)?;
    if leaves.len() != parquet.num_columns() {
        return Err(ParquetError::General(
            "Arrow and Parquet leaf columns do not match".into(),
        ));
    }
    let mut disabled = Vec::new();
    for (column, leaf) in parquet.columns().iter().zip(leaves) {
        let path = column.path();
        let kind = column.physical_type();
        if !props.dictionary_enabled(path) || kind == PhysicalType::BOOLEAN {
            continue;
        }
        if kind == PhysicalType::FIXED_LEN_BYTE_ARRAY
            && props.writer_version() == WriterVersion::PARQUET_1_0
        {
            disabled.push(path.clone());
            continue;
        }
        let page_rows = props
            .column_data_page_row_count_limit(path)
            .max(first_check)
            .min(props.max_row_group_row_count().unwrap_or(usize::MAX));
        let page_size = props.column_data_page_size_limit(path);
        let tolerance = (page_size as f32 * 0.1) as usize;
        let mut page = FirstPage::new(
            kind,
            props.column_dictionary_page_size_limit(path) as u64,
            page_size.saturating_sub(tolerance) as u64,
            first_check,
        );
        for batch in sample {
            if page.is_closed() || page.rows == page_rows {
                break;
            }
            let rows = batch.num_rows().min(page_rows - page.rows);
            let top: Entries<'_> = Box::new((0..rows).map(|row| (row, row)));
            let (array, entries) = leaf.entries(batch, top);
            page.extend(array, entries, rows, column.type_length().max(0) as usize)?;
        }
        if !page.keeps_dictionary() {
            disabled.push(path.clone());
        }
    }
    Ok(disabled
        .into_iter()
        .fold(props.into_builder(), |builder, path| {
            builder.set_column_dictionary_enabled(path, false)
        })
        .build())
}

struct Leaf {
    column: usize,
    steps: Vec<Step>,
}

#[derive(Clone, Copy)]
enum Step {
    Field(usize),
    Element,
    Key,
    Value,
    Dictionary,
    Run,
}

type Entries<'a> = Box<dyn Iterator<Item = (usize, usize)> + 'a>;

fn collect_leaves(data_type: &DataType, leaf: &mut Leaf, leaves: &mut Vec<Leaf>) -> Result<()> {
    let mut descend = |step, data_type: &DataType| {
        leaf.steps.push(step);
        let result = collect_leaves(data_type, leaf, leaves);
        leaf.steps.pop();
        result
    };
    match data_type {
        DataType::Struct(fields) => {
            for (index, field) in fields.iter().enumerate() {
                descend(Step::Field(index), field.data_type())?;
            }
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => descend(Step::Element, field.data_type())?,
        DataType::Map(field, _) => {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(ParquetError::General("Map entries must be a struct".into()));
            };
            if fields.len() != 2 {
                return Err(ParquetError::General(
                    "Map entries must contain a key and value".into(),
                ));
            }
            descend(Step::Key, fields[0].data_type())?;
            descend(Step::Value, fields[1].data_type())?;
        }
        DataType::Dictionary(_, value) => descend(Step::Dictionary, value)?,
        DataType::RunEndEncoded(_, value) => descend(Step::Run, value.data_type())?,
        DataType::Union(_, _) => {
            return Err(ParquetError::NYI(
                "Dictionary sampling of union arrays".into(),
            ));
        }
        _ => leaves.push(Leaf {
            column: leaf.column,
            steps: leaf.steps.clone(),
        }),
    }
    Ok(())
}

impl Leaf {
    fn entries<'a>(
        &self,
        batch: &'a RecordBatch,
        mut entries: Entries<'a>,
    ) -> (&'a dyn Array, Entries<'a>) {
        let mut array = batch.column(self.column).as_ref();
        for step in &self.steps {
            entries = present(array, entries);
            (array, entries) = match (step, array.data_type()) {
                (Step::Field(index), DataType::Struct(_)) => {
                    (array.as_struct().column(*index).as_ref(), entries)
                }
                (Step::Element, DataType::List(_)) => {
                    let list = array.as_list::<i32>();
                    (
                        list.values().as_ref(),
                        children(entries, list.value_offsets()),
                    )
                }
                (Step::Element, DataType::LargeList(_)) => {
                    let list = array.as_list::<i64>();
                    (
                        list.values().as_ref(),
                        children(entries, list.value_offsets()),
                    )
                }
                (Step::Element, DataType::FixedSizeList(_, size)) => {
                    let list = array.as_fixed_size_list();
                    let size = *size as usize;
                    let entries: Entries<'a> = Box::new(entries.flat_map(move |(row, index)| {
                        let start = list.value_offset_at(index);
                        (start..start + size).map(move |child| (row, child))
                    }));
                    (list.values().as_ref(), entries)
                }
                (Step::Element, DataType::ListView(_)) => {
                    let list = array.as_list_view::<i32>();
                    (
                        list.values().as_ref(),
                        view_children(entries, list.value_offsets(), list.value_sizes()),
                    )
                }
                (Step::Element, DataType::LargeListView(_)) => {
                    let list = array.as_list_view::<i64>();
                    (
                        list.values().as_ref(),
                        view_children(entries, list.value_offsets(), list.value_sizes()),
                    )
                }
                (Step::Key, DataType::Map(_, _)) => {
                    let map = array.as_map();
                    (map.keys().as_ref(), children(entries, map.value_offsets()))
                }
                (Step::Value, DataType::Map(_, _)) => {
                    let map = array.as_map();
                    (
                        map.values().as_ref(),
                        children(entries, map.value_offsets()),
                    )
                }
                (Step::Dictionary, DataType::Dictionary(_, _)) => {
                    dictionary_entries(array, entries)
                }
                (Step::Run, DataType::RunEndEncoded(_, _)) => run_entries(array, entries),
                _ => unreachable!("sample fields were validated against the schema"),
            };
        }
        (array, present(array, entries))
    }
}

fn present<'a>(array: &'a dyn Array, entries: Entries<'a>) -> Entries<'a> {
    if array.data_type() == &DataType::Null {
        return Box::new(std::iter::empty());
    }
    match array.nulls() {
        Some(nulls) => Box::new(entries.filter(move |&(_, index)| nulls.is_valid(index))),
        None => entries,
    }
}

fn children<'a, O: OffsetSizeTrait>(entries: Entries<'a>, offsets: &'a [O]) -> Entries<'a> {
    Box::new(entries.flat_map(move |(row, index)| {
        (offsets[index].as_usize()..offsets[index + 1].as_usize()).map(move |child| (row, child))
    }))
}

fn view_children<'a, O: OffsetSizeTrait>(
    entries: Entries<'a>,
    offsets: &'a [O],
    sizes: &'a [O],
) -> Entries<'a> {
    Box::new(entries.flat_map(move |(row, index)| {
        let start = offsets[index].as_usize();
        (start..start + sizes[index].as_usize()).map(move |child| (row, child))
    }))
}

fn dictionary_entries<'a>(
    array: &'a dyn Array,
    entries: Entries<'a>,
) -> (&'a dyn Array, Entries<'a>) {
    downcast_dictionary_array!(array => {
        let keys = array.keys();
        let entries: Entries<'a> = Box::new(entries.map(move |(row, index)| (row, keys.value(index).as_usize())));
        (array.values().as_ref(), entries)
    }, _ => unreachable!("dictionary schema has a dictionary array"))
}

fn run_entries<'a>(array: &'a dyn Array, entries: Entries<'a>) -> (&'a dyn Array, Entries<'a>) {
    downcast_run_array!(array => {
        let entries: Entries<'a> = Box::new(entries.map(move |(row, index)| (row, array.get_physical_index(index))));
        (array.values().as_ref(), entries)
    }, _ => unreachable!("run-end schema has a run array"))
}

enum DictionaryKeys {
    Four(HashMap<[u8; 4], u32, ahash::RandomState>),
    Eight(HashMap<[u8; 8], u32, ahash::RandomState>),
    Bytes(HashMap<Box<[u8]>, u32, ahash::RandomState>),
}

impl DictionaryKeys {
    fn new(kind: PhysicalType) -> Self {
        match kind {
            PhysicalType::INT32 | PhysicalType::FLOAT => Self::Four(HashMap::default()),
            PhysicalType::INT64 | PhysicalType::DOUBLE => Self::Eight(HashMap::default()),
            _ => Self::Bytes(HashMap::default()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Four(keys) => keys.len(),
            Self::Eight(keys) => keys.len(),
            Self::Bytes(keys) => keys.len(),
        }
    }

    fn get(&self, value: &[u8]) -> Option<&u32> {
        match self {
            Self::Four(keys) => {
                keys.get::<[u8; 4]>(value.try_into().expect("INT32 and FLOAT have four bytes"))
            }
            Self::Eight(keys) => {
                keys.get::<[u8; 8]>(value.try_into().expect("INT64 and DOUBLE have eight bytes"))
            }
            Self::Bytes(keys) => keys.get(value),
        }
    }

    fn insert(&mut self, value: &[u8], id: u32) {
        match self {
            Self::Four(keys) => {
                keys.insert(
                    value.try_into().expect("INT32 and FLOAT have four bytes"),
                    id,
                );
            }
            Self::Eight(keys) => {
                keys.insert(
                    value.try_into().expect("INT64 and DOUBLE have eight bytes"),
                    id,
                );
            }
            Self::Bytes(keys) => {
                keys.insert(value.into(), id);
            }
        }
    }
}

struct FirstPage {
    kind: PhysicalType,
    dictionary_limit: u64,
    page_bytes: u64,
    first_check: usize,
    dictionary: DictionaryKeys,
    previous: Option<u32>,
    ids: HybridRuns,
    plain_bytes: u64,
    dictionary_bytes: u64,
    rows: usize,
    full: bool,
    overflowed: bool,
}

impl FirstPage {
    fn new(kind: PhysicalType, dictionary_limit: u64, page_bytes: u64, first_check: usize) -> Self {
        Self {
            kind,
            dictionary_limit,
            page_bytes,
            first_check,
            dictionary: DictionaryKeys::new(kind),
            previous: None,
            ids: HybridRuns::default(),
            plain_bytes: 0,
            dictionary_bytes: 0,
            rows: 0,
            full: false,
            overflowed: false,
        }
    }

    fn is_closed(&self) -> bool {
        self.full || self.overflowed
    }

    fn extend(
        &mut self,
        array: &dyn Array,
        entries: Entries<'_>,
        rows: usize,
        type_length: usize,
    ) -> Result<()> {
        if array.data_type() == &DataType::Null {
            self.consume(entries, rows, |_, _| unreachable!());
            return Ok(());
        }
        let sampler = SampleValues {
            page: self,
            entries,
            rows,
            type_length,
        };
        match sampler.page.kind {
            PhysicalType::INT32 => visit_i32(array, sampler),
            PhysicalType::INT64 => visit_i64(array, sampler),
            PhysicalType::FLOAT => sampler.slice(array.as_primitive::<Float32Type>().values()),
            PhysicalType::DOUBLE => sampler.slice(array.as_primitive::<Float64Type>().values()),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => visit_fixed_len_byte_array(array, sampler),
            PhysicalType::BYTE_ARRAY => {
                let value = byte_values(array)?;
                sampler
                    .page
                    .consume(sampler.entries, rows, |page, index| page.add(value(index)));
                Ok(())
            }
            PhysicalType::BOOLEAN | PhysicalType::INT96 => {
                unreachable!("not a candidate Arrow leaf")
            }
        }
    }

    fn consume(
        &mut self,
        entries: Entries<'_>,
        rows: usize,
        mut add: impl FnMut(&mut Self, usize),
    ) {
        let mut entries = entries.peekable();
        for row in 0..rows {
            while let Some((_, index)) = entries.next_if(|&(entry_row, _)| entry_row == row) {
                add(self, index);
                if self.overflowed {
                    return;
                }
            }
            self.rows += 1;
            if self.rows >= self.first_check && self.plain_bytes >= self.page_bytes {
                self.full = true;
                return;
            }
        }
    }

    fn add(&mut self, value: &[u8]) {
        let length = value.len() as u64;
        let raw_size = match self.kind {
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => 4 + length,
            _ => length,
        };
        self.plain_bytes += raw_size;
        let id = match self.dictionary.get(value) {
            Some(id) => *id,
            None => {
                let dictionary_size = if self.kind == PhysicalType::BYTE_ARRAY {
                    4 + length
                } else {
                    length
                };
                if self.dictionary_bytes + dictionary_size > self.dictionary_limit
                    || self.dictionary.len() >= (i32::MAX - 1) as usize
                {
                    self.overflowed = true;
                    return;
                }
                let id = self.dictionary.len() as u32;
                self.dictionary.insert(value, id);
                self.dictionary_bytes += dictionary_size;
                id
            }
        };
        self.ids.write(self.previous == Some(id));
        self.previous = Some(id);
    }

    fn keeps_dictionary(&self) -> bool {
        if self.overflowed {
            return false;
        }
        let max_id = self.dictionary.len().saturating_sub(1) as u32;
        let bit_width = u32::BITS - max_id.leading_zeros();
        1 + self.ids.len(bit_width) + self.dictionary_bytes < self.plain_bytes
    }
}

struct SampleValues<'a> {
    page: &'a mut FirstPage,
    entries: Entries<'a>,
    rows: usize,
    type_length: usize,
}

impl<T: Copy + AsBytes> PrimitiveVisitor<T> for SampleValues<'_> {
    type Output = ();

    fn slice(self, values: &[T]) -> Result<()> {
        self.page.consume(self.entries, self.rows, |page, index| {
            page.add(values[index].as_bytes())
        });
        Ok(())
    }

    fn map<S: Copy>(self, values: &[S], convert: impl Fn(S) -> T) -> Result<()> {
        self.page.consume(self.entries, self.rows, |page, index| {
            page.add(convert(values[index]).as_bytes())
        });
        Ok(())
    }
}

impl FixedLenByteArrayVisitor for SampleValues<'_> {
    type Output = ();

    fn decimal_length(&self, _precision: u8, _native_length: usize) -> usize {
        self.type_length
    }

    fn values(self, length: usize, fill: impl Fn(usize, &mut [u8])) -> Result<()> {
        let mut value = vec![0; length];
        self.page.consume(self.entries, self.rows, |page, index| {
            fill(index, &mut value);
            page.add(&value);
        });
        Ok(())
    }
}

type ValueBytes<'a> = Box<dyn Fn(usize) -> &'a [u8] + 'a>;

fn byte_values(array: &dyn Array) -> Result<ValueBytes<'_>> {
    Ok(match array.data_type() {
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            Box::new(move |i| array.value(i).as_bytes())
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            Box::new(move |i| array.value(i).as_bytes())
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            Box::new(move |i| array.value(i).as_bytes())
        }
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            Box::new(move |i| array.value(i))
        }
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            Box::new(move |i| array.value(i))
        }
        DataType::BinaryView => {
            let array = array.as_binary_view();
            Box::new(move |i| array.value(i))
        }
        other => {
            return Err(ParquetError::General(format!(
                "Cannot sample {other} as BYTE_ARRAY"
            )));
        }
    })
}

/// `RunLengthBitPackingHybridEncoder`'s run-splitting state machine, counting what it would write
/// instead of writing it. A value repeated 8 or more times from the start of a group of 8 becomes
/// an RLE run; everything else is bit-packed 8 values at a time, up to 63 groups per run header.
///
/// Where the encoder cuts runs depends only on whether each id repeats the one before it, so that
/// is all this is told. The bit width, which the dictionary's final size decides, only comes in
/// when the length is read off.
#[derive(Clone, Default)]
struct HybridRuns {
    repeat_count: u64,
    buffered: u32,
    /// Groups in the bit-packed run that is open.
    groups: u32,
    bit_packed_run_open: bool,
    /// One header byte each.
    bit_packed_runs: u64,
    /// `bit_width` bytes each: eight values of `bit_width` bits.
    bit_packed_groups: u64,
    /// The value, padded to whole bytes, each.
    rle_runs: u64,
    /// The varint headers of the RLE runs, which hold the run lengths.
    rle_header_bytes: u64,
}

impl HybridRuns {
    /// `writeInt` for an id that does or does not repeat the one before it. The encoder starts from
    /// a previous value of 0, which is always the first id, and either way a first value starts a
    /// run of one.
    fn write(&mut self, repeat: bool) {
        if repeat {
            self.repeat_count += 1;
            if self.repeat_count >= 8 {
                return;
            }
        } else {
            if self.repeat_count >= 8 {
                self.write_rle_run();
            }
            self.repeat_count = 1;
        }
        self.buffered += 1;
        if self.buffered == 8 {
            self.write_bit_packed_group();
        }
    }

    fn write_bit_packed_group(&mut self) {
        if self.groups >= 63 {
            self.end_bit_packed_run();
        }
        if !self.bit_packed_run_open {
            self.bit_packed_runs += 1;
            self.bit_packed_run_open = true;
        }
        self.bit_packed_groups += 1;
        self.buffered = 0;
        self.repeat_count = 0;
        self.groups += 1;
    }

    fn end_bit_packed_run(&mut self) {
        self.bit_packed_run_open = false;
        self.groups = 0;
    }

    fn write_rle_run(&mut self) {
        self.end_bit_packed_run();
        self.rle_runs += 1;
        self.rle_header_bytes += varint_len(self.repeat_count << 1);
        self.repeat_count = 0;
        self.buffered = 0;
    }

    /// The bytes `toBytes()` would return at `bit_width`, including its flush of what is still
    /// buffered, whose last group is padded to 8 values.
    fn len(&self, bit_width: u32) -> u64 {
        let mut finished = self.clone();
        if finished.repeat_count >= 8 {
            finished.write_rle_run();
        } else if finished.buffered > 0 {
            finished.write_bit_packed_group();
        }
        let bit_width = bit_width as u64;
        finished.bit_packed_runs
            + finished.bit_packed_groups * bit_width
            + finished.rle_header_bytes
            + finished.rle_runs * bit_width.div_ceil(8)
    }
}

fn varint_len(value: u64) -> u64 {
    (u64::BITS - value.leading_zeros()).div_ceil(7).max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::builder::{
        Int32Builder, Int64Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };
    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int32Array, Int64Array, StringArray, StructArray,
    };
    use arrow::datatypes::{Field, Fields};
    use std::sync::Arc;
    /// splitmix64, the generator the parquet-mr ground truth below was recorded with.
    fn mix(x: u64) -> u64 {
        let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    const CARDINALITIES: [u64; 14] = [
        1000, 2780, 5000, 12000, 13000, 14000, 14500, 15000, 15500, 16000, 17000, 20000, 30000,
        38000,
    ];
    const CYCLE_UNIQUE: [u64; 9] = [60, 65, 68, 69, 70, 71, 72, 75, 80];
    const WIDE_CARDINALITIES: [u64; 4] = [3000, 5000, 7000, 9000];

    /// Rows `rows` of a table built to sit on both sides of parquet-mr's cut-off: random columns
    /// of rising cardinality in each physical type, runs, cycles that mix repeats with unique
    /// values, nulls, wide strings whose first page the page size ends, and nested leaves.
    fn corpus(rows: std::ops::Range<u64>) -> RecordBatch {
        let ints = |f: &dyn Fn(u64) -> i32| -> ArrayRef {
            Arc::new(Int32Array::from_iter_values(rows.clone().map(f)))
        };
        let longs = |f: &dyn Fn(u64) -> i64| -> ArrayRef {
            Arc::new(Int64Array::from_iter_values(rows.clone().map(f)))
        };
        let strings = |f: &dyn Fn(u64) -> String| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(rows.clone().map(f)))
        };
        let mut columns: Vec<(String, ArrayRef)> = vec![
            ("unique_long".into(), longs(&|i| i as i64)),
            ("mod10_int".into(), ints(&|i| (i % 10) as i32)),
        ];
        for c in CARDINALITIES {
            columns.push((format!("rand_int_{c}"), ints(&|i| (mix(i) % c) as i32)));
        }
        for c in CARDINALITIES {
            columns.push((
                format!("rand_long_{c}"),
                longs(&|i| (mix(i + 7) % c) as i64),
            ));
        }
        for c in CARDINALITIES {
            columns.push((
                format!("rand_str_{c}"),
                strings(&|i| format!("s{}", mix(i + 13) % c)),
            ));
        }
        columns.push((
            "unique_str".into(),
            strings(&|i| format!("{:016x}", mix(i))),
        ));
        columns.push((
            "all_null".into(),
            Arc::new(Int32Array::from_iter(rows.clone().map(|_| None::<i32>))),
        ));
        columns.push((
            "half_null_unique".into(),
            Arc::new(Int64Array::from_iter(
                rows.clone().map(|i| (i % 2 == 0).then_some(i as i64)),
            )),
        ));
        columns.push(("runs_of_3".into(), longs(&|i| (i / 3) as i64)));
        columns.push(("runs_of_10".into(), ints(&|i| (i / 10) as i32)));
        for u in CYCLE_UNIQUE {
            columns.push((
                format!("cycle_unique_{u}"),
                ints(&|i| if i % 100 < u { (1 + i) as i32 } else { 0 }),
            ));
        }
        columns.push((
            "unique_double".into(),
            Arc::new(Float64Array::from_iter_values(
                rows.clone().map(|i| f64::from_bits(mix(i) >> 2)),
            )),
        ));
        columns.push((
            "low_float".into(),
            Arc::new(Float32Array::from_iter_values(
                rows.clone().map(|i| (i % 100) as f32),
            )),
        ));
        columns.push((
            "wide_unique_str".into(),
            strings(&|i| format!("{:016x}", mix(i + 99)).repeat(12)),
        ));
        columns.push((
            "wide_low_str".into(),
            strings(&|i| format!("{}{}", "x".repeat(200), i % 20)),
        ));
        for c in WIDE_CARDINALITIES {
            columns.push((
                format!("wide_rand_str_{c}"),
                strings(&|i| format!("{}{}", "y".repeat(120), mix(i + 31) % c)),
            ));
        }
        columns.push((
            "bool".into(),
            Arc::new(BooleanArray::from_iter(
                rows.clone().map(|i| Some(i % 3 == 0)),
            )),
        ));
        columns.push((
            "dec_low".into(),
            Arc::new(
                Decimal128Array::from_iter_values(rows.clone().map(|i| (i % 1000) as i128))
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ));
        columns.push((
            "date_low".into(),
            Arc::new(Date32Array::from_iter_values(
                rows.clone().map(|i| 18000 + (i % 365) as i32),
            )),
        ));
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int32, true),
        ]);
        columns.push((
            "st".into(),
            Arc::new(StructArray::new(
                struct_fields,
                vec![longs(&|i| mix(i + 5) as i64), ints(&|i| (i % 7) as i32)],
                None,
            )),
        ));
        let element = |data_type| Arc::new(Field::new("element", data_type, true));
        let mut list_low =
            ListBuilder::new(Int32Builder::new()).with_field(element(DataType::Int32));
        let mut list_unique =
            ListBuilder::new(Int64Builder::new()).with_field(element(DataType::Int64));
        let mut map = MapBuilder::new(
            Some(MapFieldNames {
                entry: "key_value".into(),
                key: "key".into(),
                value: "value".into(),
            }),
            StringBuilder::new(),
            Int64Builder::new(),
        );
        for i in rows.clone() {
            list_low.values().append_value((i % 5) as i32);
            list_low.values().append_value((i % 3) as i32);
            list_low.values().append_null();
            list_low.append(true);
            for k in 0..3 {
                list_unique.values().append_value((3 * i + k) as i64);
            }
            list_unique.append(true);
            map.keys().append_value(format!("k{}", i % 4));
            map.values().append_value(mix(i + 11) as i64);
            map.keys().append_value(format!("z{}", i % 2));
            map.values().append_value((i % 9) as i64);
            map.append(true).unwrap();
        }
        columns.push(("list_low".into(), Arc::new(list_low.finish())));
        columns.push(("list_unique".into(), Arc::new(list_unique.finish())));
        columns.push(("m".into(), Arc::new(map.finish())));
        RecordBatch::try_from_iter(columns).unwrap()
    }

    /// `rows` rows of [`corpus`] in batches of `batch_rows`.
    fn corpus_batches(rows: u64, batch_rows: u64) -> Vec<RecordBatch> {
        (0..rows)
            .step_by(batch_rows as usize)
            .map(|start| corpus(start..(start + batch_rows).min(rows)))
            .collect()
    }

    fn properties(page_rows: usize, page_size: usize, dictionary_limit: usize) -> WriterProperties {
        WriterProperties::builder()
            .set_data_page_row_count_limit(page_rows)
            .set_data_page_size_limit(page_size)
            .set_dictionary_page_size_limit(dictionary_limit)
            .set_max_row_group_bytes(Some(128 * 1024 * 1024))
            .set_max_row_group_row_count(None)
            .build()
    }

    /// The leaf columns that keep dictionaries, excluding BOOLEAN leaves the helper never changes.
    fn dictionary_columns(properties: WriterProperties, sample: &[RecordBatch]) -> Vec<String> {
        let schema = sample[0].schema();
        let chosen = choose_dictionary_encoding(
            &schema,
            properties,
            sample,
            &DictionaryEncodingOptions::default(),
        )
        .unwrap();
        ArrowSchemaConverter::new()
            .convert(&schema)
            .unwrap()
            .columns()
            .iter()
            .filter(|descr| {
                descr.physical_type() != PhysicalType::BOOLEAN
                    && chosen.dictionary_enabled(descr.path())
            })
            .map(|descr| descr.path().string())
            .collect()
    }

    // The expected columns below are parquet-mr's own answers. They were recorded by writing the
    // same rows through iceberg-java's `Parquet.write` with `GenericParquetWriter`, with the same
    // `write.parquet.page-row-limit`, `page-size-bytes` and `dict-size-bytes`, and listing the
    // column chunks of the first row group whose encoding stats include a dictionary page. Iceberg
    // 1.5.2, 1.8.1, 1.10.0 and 1.11.0 (parquet-mr 1.13.1, 1.15.0, 1.16.0 and 1.17.1) all agree.

    #[test]
    fn matches_parquet_mr_at_iceberg_defaults() {
        let sample = corpus_batches(24_576, 8192);
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "rand_int_2780",
                "rand_int_5000",
                "rand_int_12000",
                "rand_int_13000",
                "rand_int_14000",
                "rand_int_14500",
                "rand_int_15000",
                "rand_long_1000",
                "rand_long_2780",
                "rand_long_5000",
                "rand_long_12000",
                "rand_long_13000",
                "rand_long_14000",
                "rand_long_14500",
                "rand_long_15000",
                "rand_long_15500",
                "rand_long_16000",
                "rand_long_17000",
                "rand_long_20000",
                "rand_long_30000",
                "rand_str_1000",
                "rand_str_2780",
                "rand_str_5000",
                "rand_str_12000",
                "rand_str_13000",
                "rand_str_14000",
                "rand_str_14500",
                "rand_str_15000",
                "rand_str_15500",
                "rand_str_16000",
                "rand_str_17000",
                "rand_str_20000",
                "rand_str_30000",
                "rand_str_38000",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "low_float",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "dec_low",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// Pages that end at 16 KiB, before the 2000-row limit, for all but the narrowest columns: the
    /// settings of `CometIcebergNativeSuite`'s page skipping test.
    #[test]
    fn matches_parquet_mr_when_the_page_size_ends_the_first_page() {
        let sample = corpus_batches(8192, 8192);
        assert_eq!(
            dictionary_columns(properties(2000, 16 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "rand_long_1000",
                "rand_long_2780",
                "rand_str_1000",
                "rand_str_2780",
                "rand_str_5000",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "cycle_unique_68",
                "cycle_unique_69",
                "cycle_unique_70",
                "cycle_unique_71",
                "cycle_unique_72",
                "low_float",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "dec_low",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// parquet-mr does not check page sizes before row 100, so a lower row limit still gives
    /// 100-row first pages.
    #[test]
    fn matches_parquet_mr_when_the_row_limit_is_below_the_first_size_check() {
        let sample = corpus_batches(1000, 1000);
        assert_eq!(
            dictionary_columns(properties(50, 1024 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "cycle_unique_68",
                "cycle_unique_69",
                "cycle_unique_70",
                "cycle_unique_71",
                "cycle_unique_72",
                "cycle_unique_75",
                "cycle_unique_80",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// A partition with fewer rows than a page is judged on all of them, as parquet-mr judges a
    /// column chunk that ends within its first page.
    #[test]
    fn matches_parquet_mr_when_the_rows_end_within_the_first_page() {
        let sample = corpus_batches(5000, 2048);
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "rand_int_2780",
                "rand_long_1000",
                "rand_long_2780",
                "rand_long_5000",
                "rand_str_1000",
                "rand_str_2780",
                "rand_str_5000",
                "rand_str_13000",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "cycle_unique_68",
                "cycle_unique_69",
                "cycle_unique_70",
                "cycle_unique_71",
                "low_float",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "dec_low",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// A dictionary that outgrows `write.parquet.dict-size-bytes` on the first page is dropped,
    /// however much it would have saved.
    #[test]
    fn matches_parquet_mr_when_the_dictionary_outgrows_its_limit() {
        let sample = corpus_batches(24_576, 8192);
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 4096), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "low_float",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
            ]
        );
    }

    /// Where a partition's rows were cut into batches does not change its choice.
    #[test]
    fn batch_boundaries_do_not_change_the_choice() {
        let properties = || properties(20_000, 1024 * 1024, 2 * 1024 * 1024);
        let whole = dictionary_columns(properties(), &corpus_batches(20_000, 20_000));
        for batch_rows in [777, 1000, 8192] {
            assert_eq!(
                dictionary_columns(properties(), &corpus_batches(20_000, batch_rows)),
                whole,
                "batches of {batch_rows} rows"
            );
        }
    }

    #[test]
    fn hybrid_len_matches_parquet_mr() {
        fn sequence(len: usize, value: impl Fn(usize) -> u32) -> Vec<u32> {
            (0..len).map(value).collect()
        }
        // What `RunLengthBitPackingHybridEncoder.toBytes()` returned for the same values, at the
        // bit width of their maximum.
        let cases: Vec<(&str, Vec<u32>, u64)> = vec![
            ("empty", vec![], 0),
            ("one zero", vec![0], 1),
            ("20000 zeros", vec![0; 20_000], 3),
            ("ascending", sequence(20_000, |i| i as u32), 37_540),
            ("seven ones", vec![1; 7], 2),
            ("eight ones", vec![1; 8], 2),
            ("nine ones", vec![1; 9], 2),
            ("runs of 3", sequence(20_000, |i| (i / 3) as u32), 32_540),
            ("runs of 8", sequence(20_000, |i| (i / 8) as u32), 7_500),
            ("runs of 10", sequence(20_000, |i| (i / 10) as u32), 6_000),
            (
                "runs of 11 starting mid-group",
                sequence(20_000, |i| ((i + 3) / 11) as u32),
                5_466,
            ),
            (
                "random under 5000",
                sequence(20_000, |i| (mix(i as u64) % 5000) as u32),
                32_540,
            ),
            (
                "cycles of 70 unique and 30 repeats",
                sequence(20_000, |i| if i % 100 < 70 { 1 + i as u32 } else { 0 }),
                27_800,
            ),
            (
                "runs of 9 broken by one value",
                sequence(1000, |i| if i % 10 < 9 { 5 } else { i as u32 }),
                1_100,
            ),
            (
                "scattered repeats",
                sequence(5000, |i| {
                    if mix(i as u64).is_multiple_of(3) {
                        i as u32
                    } else {
                        7
                    }
                }),
                7_794,
            ),
            (
                "504 ascending, 63 groups in one run",
                sequence(504, |i| i as u32),
                568,
            ),
            ("505 ascending", sequence(505, |i| i as u32), 578),
            (
                "512 ascending, 64 groups in two runs",
                sequence(512, |i| i as u32),
                578,
            ),
            ("513 ascending", sequence(513, |i| i as u32), 652),
            ("600 ascending", sequence(600, |i| i as u32), 752),
        ];
        for (name, values, expected) in cases {
            let mut runs = HybridRuns::default();
            for (i, value) in values.iter().enumerate() {
                runs.write(i > 0 && values[i - 1] == *value);
            }
            let max = values.iter().copied().max().unwrap_or(0);
            assert_eq!(
                runs.len(u32::BITS - max.leading_zeros()),
                expected,
                "{name}"
            );
        }
    }

    /// The dictionary has to come out strictly smaller. Here the ids (a bit-width byte and one
    /// bit-packed group, 5 bytes) plus the dictionary (7 values, 35 bytes) exactly match the plain
    /// page (8 values, 40 bytes), and parquet-mr writes the column plain.
    #[test]
    fn a_dictionary_that_only_breaks_even_is_dropped() {
        let values = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "a"]);
        let batch = RecordBatch::try_from_iter([("s", Arc::new(values) as ArrayRef)]).unwrap();
        assert!(dictionary_columns(properties(20_000, 1024 * 1024, 1024), &[batch]).is_empty());
    }

    fn choose(batch: &RecordBatch, props: WriterProperties) -> WriterProperties {
        choose_dictionary_encoding(
            &batch.schema(),
            props,
            std::slice::from_ref(batch),
            &DictionaryEncodingOptions::default(),
        )
        .unwrap()
    }

    fn enabled(props: &WriterProperties, name: &str) -> bool {
        props.dictionary_enabled(&crate::schema::types::ColumnPath::from(name))
    }

    #[test]
    fn validates_options_and_all_schemas_before_empty_sample() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let options = DictionaryEncodingOptions::default();
        let props = WriterProperties::default();
        let bad_options = DictionaryEncodingOptions {
            min_row_count_for_page_size_check: 0,
        };
        assert!(choose_dictionary_encoding(&schema, props.clone(), &[], &bad_options).is_err());
        let wrong = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "wrong",
            DataType::Int64,
            true,
        )])));
        assert!(choose_dictionary_encoding(&schema, props.clone(), &[wrong], &options).is_err());
        assert!(enabled(
            &choose_dictionary_encoding(&schema, props, &[], &options).unwrap(),
            "a"
        ));
        let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
        assert!(enabled(
            &choose_dictionary_encoding(&schema, WriterProperties::default(), &[empty], &options)
                .unwrap(),
            "a"
        ));

        let fixed = Schema::new(vec![Field::new(
            "fixed",
            DataType::FixedSizeBinary(4),
            false,
        )]);
        let empty = RecordBatch::new_empty(Arc::new(fixed.clone()));
        for sample in [vec![], vec![empty]] {
            assert!(enabled(
                &choose_dictionary_encoding(&fixed, WriterProperties::default(), &sample, &options)
                    .unwrap(),
                "fixed"
            ));
        }
    }

    #[test]
    fn preserves_properties_and_respects_column_limits() {
        use crate::basic::{Compression, Encoding};
        use crate::schema::types::ColumnPath;
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values((0..1000).map(|i| i % 4)));
        let batch = RecordBatch::try_from_iter([
            ("small", values.clone()),
            ("large", values.clone()),
            ("disabled", values),
        ])
        .unwrap();
        let small = ColumnPath::from("small");
        let large = ColumnPath::from("large");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_column_encoding(large.clone(), Encoding::PLAIN)
            .set_column_dictionary_page_size_limit(small.clone(), 8)
            .set_column_dictionary_page_size_limit(large.clone(), 1024)
            .set_column_dictionary_enabled(ColumnPath::from("disabled"), false)
            .set_max_row_group_row_count(Some(321))
            .set_created_by("dictionary sampling test".into())
            .build();
        let chosen = choose(&batch, props.clone());
        assert!(!enabled(&chosen, "small"));
        assert!(enabled(&chosen, "large"));
        assert!(!enabled(&chosen, "disabled"));
        assert_eq!(chosen.compression(&large), props.compression(&large));
        assert_eq!(chosen.encoding(&large), props.encoding(&large));
        assert_eq!(chosen.created_by(), props.created_by());
        assert_eq!(
            chosen.max_row_group_row_count(),
            props.max_row_group_row_count()
        );
        assert_eq!(chosen.column_dictionary_page_size_limit(&small), 8);
    }

    #[test]
    fn first_check_and_page_limits_use_top_level_rows() {
        use crate::schema::types::ColumnPath;
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..100).chain(std::iter::repeat_n(7, 1000)),
        ));
        let batch = RecordBatch::try_from_iter([
            ("row_limited", values.clone()),
            ("byte_limited", values.clone()),
            ("long", values),
        ])
        .unwrap();
        let props = WriterProperties::builder()
            .set_column_data_page_row_count_limit(ColumnPath::from("row_limited"), 50)
            .set_column_data_page_size_limit(ColumnPath::from("byte_limited"), 1)
            .build();
        let chosen = choose(&batch, props.clone());
        assert!(!enabled(&chosen, "row_limited"));
        assert!(!enabled(&chosen, "byte_limited"));
        assert!(enabled(&chosen, "long"));
        let options = DictionaryEncodingOptions {
            min_row_count_for_page_size_check: 200,
        };
        let chosen = choose_dictionary_encoding(
            &batch.schema(),
            props,
            std::slice::from_ref(&batch),
            &options,
        )
        .unwrap();
        assert!(enabled(&chosen, "row_limited"));
        assert!(enabled(&chosen, "byte_limited"));

        let first = batch.slice(0, 100);
        assert!(!enabled(
            &choose(&first, WriterProperties::default()),
            "long"
        ));
        let page = WriterProperties::builder()
            .set_data_page_row_count_limit(100)
            .build();
        let extra = choose(&batch, page);
        assert!(!enabled(&extra, "long"));
    }

    #[test]
    fn row_group_row_limit_ends_the_sampled_chunk() {
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Int64Array::from_iter_values(
                (0..100).chain(std::iter::repeat_n(7, 1000)),
            )) as ArrayRef,
        )])
        .unwrap();
        let limited = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .build();
        assert!(!enabled(&choose(&batch, limited), "v"));
        assert!(enabled(&choose(&batch, WriterProperties::default()), "v"));
    }

    #[test]
    fn first_check_boundary_is_inclusive() {
        let values = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "a", "a"]);
        let batch = RecordBatch::try_from_iter([("s", Arc::new(values) as ArrayRef)]).unwrap();
        let props = WriterProperties::builder()
            .set_data_page_size_limit(1)
            .build();
        let options = DictionaryEncodingOptions {
            min_row_count_for_page_size_check: 8,
        };
        let chosen = choose_dictionary_encoding(
            &batch.schema(),
            props,
            std::slice::from_ref(&batch),
            &options,
        )
        .unwrap();
        assert!(!enabled(&chosen, "s"));
    }

    #[test]
    fn page_size_boundary_uses_inclusive_tolerance() {
        let values = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "a", "a"]);
        let batch = RecordBatch::try_from_iter([("s", Arc::new(values) as ArrayRef)]).unwrap();
        let props = WriterProperties::builder()
            .set_data_page_size_limit(44)
            .build();
        let options = DictionaryEncodingOptions {
            min_row_count_for_page_size_check: 8,
        };
        let chosen = choose_dictionary_encoding(
            &batch.schema(),
            props,
            std::slice::from_ref(&batch),
            &options,
        )
        .unwrap();
        assert!(!enabled(&chosen, "s"));
    }

    #[test]
    fn fixed_length_binary_java_v1_and_v2_accounting() {
        use arrow_array::{FixedSizeBinaryArray, Float16Array, IntervalYearMonthArray};
        let fixed: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter([b"a", b"b", b"c", b"d"].into_iter()).unwrap(),
        );
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from(vec![1, 2, 3, 4])
                .with_precision_and_scale(20, 0)
                .unwrap(),
        );
        let interval: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![1, 2, 3, 4]));
        let half: ArrayRef = Arc::new(Float16Array::from_iter_values(
            (1..=4).map(|value| half::f16::from_f32(value as f32)),
        ));
        let batch = RecordBatch::try_from_iter([
            ("fixed", fixed),
            ("decimal", decimal),
            ("interval", interval),
            ("half", half),
        ])
        .unwrap();
        let v1 = choose(&batch, WriterProperties::default());
        for name in ["fixed", "decimal", "interval", "half"] {
            assert!(!enabled(&v1, name), "{name}");
        }
        let v2 = choose(
            &batch,
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .build(),
        );
        // Java counts an extra four bytes per raw binary value, but not per dictionary entry.
        for name in ["fixed", "decimal", "interval", "half"] {
            assert!(enabled(&v2, name), "{name}");
        }
    }

    #[test]
    fn uses_writer_physical_conversions() {
        use arrow_array::{Date64Array, Decimal256Array, Int8Array};
        use arrow_buffer::i256;
        let small: ArrayRef = Arc::new(Int8Array::from_iter_values(
            (0..256).map(|i| (i % 128) as i8),
        ));
        let small = RecordBatch::try_from_iter([("small", small)]).unwrap();
        assert!(enabled(
            &choose(&small, WriterProperties::default()),
            "small"
        ));
        let dates: ArrayRef = Arc::new(Date64Array::from_iter_values(0..1000));
        let decimals =
            Decimal256Array::from_iter_values((0..1000).map(|i| i256::from_i128(i % 900)))
                .with_precision_and_scale(9, 0)
                .unwrap();
        decimals.validate_decimal_precision(9).unwrap();
        let decimals: ArrayRef = Arc::new(decimals);
        let batch = RecordBatch::try_from_iter([("date", dates), ("decimal", decimals)]).unwrap();
        let ordinary = choose(&batch, WriterProperties::default());
        assert!(!enabled(&ordinary, "date"));
        // 900 distinct values in 1000 rows lose with INT32 sizes but win with 32-byte sizes.
        assert!(!enabled(&ordinary, "decimal"));
        let coerced = choose(
            &batch,
            WriterProperties::builder().set_coerce_types(true).build(),
        );
        assert!(enabled(&coerced, "date"));
    }

    #[test]
    fn float_keys_use_raw_bits_including_mixed_nan_payloads() {
        let choose_f32 = |values| {
            let batch = RecordBatch::try_from_iter([(
                "v",
                Arc::new(Float32Array::from(values)) as ArrayRef,
            )])
            .unwrap();
            enabled(&choose(&batch, WriterProperties::default()), "v")
        };
        let choose_f64 = |values| {
            let batch = RecordBatch::try_from_iter([(
                "v",
                Arc::new(Float64Array::from(values)) as ArrayRef,
            )])
            .unwrap();
            enabled(&choose(&batch, WriterProperties::default()), "v")
        };
        assert!(choose_f32(vec![0.0, 0.0]));
        assert!(!choose_f32(vec![0.0, -0.0]));
        assert!(choose_f64(vec![0.0, 0.0]));
        assert!(!choose_f64(vec![0.0, -0.0]));
        assert!(choose_f32(vec![f32::from_bits(0x7fc00001); 4]));
        assert!(!choose_f32(
            (1..=4).map(|i| f32::from_bits(0x7fc00000 + i)).collect()
        ));
        assert!(choose_f64(vec![f64::from_bits(0x7ff8000000000001); 4]));
        assert!(!choose_f64(
            (1..=4)
                .map(|i| f64::from_bits(0x7ff8000000000000 + i))
                .collect()
        ));
    }

    #[test]
    fn dictionary_inputs_follow_keys_and_value_nulls() {
        use arrow_array::types::Int8Type;
        use arrow_array::{DictionaryArray, Int8Array};
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("same"),
            Some("unreferenced"),
            None,
        ]));
        let keys = Int8Array::from(vec![Some(0), Some(0), Some(2), None, Some(0), Some(0)]);
        let dictionary: ArrayRef =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
        let batch = RecordBatch::try_from_iter([("v", dictionary)]).unwrap();
        let chosen = choose(
            &batch,
            WriterProperties::builder()
                .set_dictionary_page_size_limit(8)
                .build(),
        );
        assert!(enabled(&chosen, "v"));
        let empty: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![None, None]),
                Arc::new(StringArray::from(Vec::<String>::new())),
            )
            .unwrap(),
        );
        assert!(!enabled(
            &choose(
                &RecordBatch::try_from_iter([("v", empty)]).unwrap(),
                WriterProperties::default()
            ),
            "v"
        ));
    }

    #[test]
    fn nested_parent_boundaries_and_slices_do_not_expand_hidden_values() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(999);
        builder.append(true);
        for _ in 0..1000 {
            builder.values().append_value(7);
        }
        builder.append(true);
        builder.append(true);
        for i in 0..1000 {
            builder.values().append_value(i);
        }
        builder.append(false);
        for i in 0..1000 {
            builder.values().append_value(i);
        }
        builder.append(true);
        let list: ArrayRef = Arc::new(builder.finish().slice(1, 4));
        let batch = RecordBatch::try_from_iter([("list", list)]).unwrap();
        let options = DictionaryEncodingOptions {
            min_row_count_for_page_size_check: 1,
        };
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(3)
            .set_dictionary_page_size_limit(16)
            .build();
        let chosen = choose_dictionary_encoding(
            &batch.schema(),
            props.clone(),
            std::slice::from_ref(&batch),
            &options,
        )
        .unwrap();
        assert!(enabled(&chosen, "list.list.item"));
        let pieces = vec![batch.slice(0, 1), batch.slice(1, 1), batch.slice(2, 2)];
        let split = choose_dictionary_encoding(&batch.schema(), props, &pieces, &options).unwrap();
        assert!(enabled(&split, "list.list.item"));
        let byte_limited = WriterProperties::builder()
            .set_data_page_size_limit(1)
            .set_dictionary_page_size_limit(16)
            .build();
        let chosen =
            choose_dictionary_encoding(&batch.schema(), byte_limited, &[batch], &options).unwrap();
        assert!(enabled(&chosen, "list.list.item"));
    }

    #[test]
    fn struct_parent_nulls_hide_child_values() {
        use arrow_buffer::NullBuffer;
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..1000).chain(std::iter::repeat_n(7, 1000)),
        ));
        let fields = Fields::from(vec![Field::new("value", DataType::Int64, false)]);
        let array: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![values],
            Some(NullBuffer::from_iter((0..2000).map(|i| i >= 1000))),
        ));
        let batch = RecordBatch::try_from_iter([("parent", array)]).unwrap();
        let chosen = choose(
            &batch,
            WriterProperties::builder()
                .set_dictionary_page_size_limit(16)
                .build(),
        );
        assert!(enabled(&chosen, "parent.value"));
    }

    #[test]
    fn oversized_values_are_rejected_before_dictionary_allocation() {
        let mut page = FirstPage::new(PhysicalType::BYTE_ARRAY, 32, 1024, 100);
        page.add(&[b'x'; 4096]);
        assert!(!page.keeps_dictionary());
        assert_eq!(page.dictionary.len(), 0);
    }

    #[test]
    fn fixed_width_keys_keep_every_byte_and_first_observation_ids() {
        for (kind, width) in [
            (PhysicalType::INT32, 4),
            (PhysicalType::FLOAT, 4),
            (PhysicalType::INT64, 8),
            (PhysicalType::DOUBLE, 8),
        ] {
            let mut page = FirstPage::new(kind, 2 * width as u64, 1024, 100);
            let first = vec![0; width];
            let mut last_byte = first.clone();
            last_byte[width - 1] = 1;
            page.add(&last_byte);
            page.add(&first);
            assert_eq!(page.dictionary.len(), 2, "{kind:?}");
            assert_eq!(page.dictionary.get(&last_byte), Some(&0), "{kind:?}");
            assert_eq!(page.dictionary.get(&first), Some(&1), "{kind:?}");
            page.add(&last_byte);
            assert_eq!(page.previous, Some(0), "{kind:?}");
            assert_eq!(page.dictionary_bytes, 2 * width as u64, "{kind:?}");
            assert_eq!(page.plain_bytes, 3 * width as u64, "{kind:?}");
            assert!(!page.overflowed, "{kind:?}");
            let mut new_value = first.clone();
            new_value[0] = 1;
            page.add(&new_value);
            assert!(page.overflowed, "{kind:?}");
            assert!(!page.keeps_dictionary(), "{kind:?}");
            assert_eq!(page.dictionary.len(), 2, "{kind:?}");
            assert_eq!(page.dictionary_bytes, 2 * width as u64, "{kind:?}");
            assert_eq!(page.previous, Some(0), "{kind:?}");
            assert_eq!(page.dictionary.get(&new_value), None, "{kind:?}");
        }
    }

    #[test]
    fn null_samples_disable_dictionary() {
        let batch = RecordBatch::try_from_iter([(
            "null",
            Arc::new(arrow_array::NullArray::new(1_000_000)) as ArrayRef,
        )])
        .unwrap();
        assert!(!enabled(
            &choose(&batch, WriterProperties::default()),
            "null"
        ));
    }

    #[test]
    fn list_layouts_and_encoded_arrays_preserve_logical_rows() {
        use arrow_array::DictionaryArray;
        use arrow_array::Int8Array;
        use arrow_array::types::Int8Type;
        use arrow_array::types::Int16Type;
        use arrow_array::{
            FixedSizeListArray, Int16Array, LargeListArray, LargeListViewArray, ListArray,
            ListViewArray, RunArray,
        };

        let make_rows = || {
            vec![
                Some(vec![Some(99)]),
                Some(vec![Some(7); 100]),
                Some(vec![]),
                None,
            ]
        };
        let list =
            ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(make_rows());
        let large =
            LargeListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(make_rows());
        let view = ListViewArray::from(list.clone());
        let large_view = LargeListViewArray::from(large.clone());
        let fixed = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            100,
            Arc::new(Int32Array::from_iter_values(
                (0..100).chain(std::iter::repeat_n(7, 300)),
            )),
            None,
        );
        for array in [
            Arc::new(list) as ArrayRef,
            Arc::new(large),
            Arc::new(view),
            Arc::new(large_view),
            Arc::new(fixed),
        ] {
            let batch = RecordBatch::try_from_iter([("v", array.slice(1, 3))]).unwrap();
            let chosen = choose(
                &batch,
                WriterProperties::builder()
                    .set_dictionary_page_size_limit(8)
                    .build(),
            );
            assert!(enabled(&chosen, "v.list.item"), "{}", batch.schema());
        }
        let runs = RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 200]),
            &StringArray::from(vec!["unused", "same"]),
        )
        .unwrap();
        let batch =
            RecordBatch::try_from_iter([("v", Arc::new(runs.slice(1, 199)) as ArrayRef)]).unwrap();
        assert!(enabled(
            &choose(
                &batch,
                WriterProperties::builder()
                    .set_dictionary_page_size_limit(8)
                    .build()
            ),
            "v"
        ));

        let values = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(7); 10]),
            Some((0..100).map(Some).collect::<Vec<_>>()),
        ]);
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(0)]),
            Arc::new(values),
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter([("v", Arc::new(dictionary) as ArrayRef)]).unwrap();
        assert!(enabled(
            &choose(
                &batch,
                WriterProperties::builder()
                    .set_dictionary_page_size_limit(8)
                    .build()
            ),
            "v.list.item"
        ));
    }

    #[test]
    fn chosen_properties_control_file_pages_and_roundtrip() {
        use crate::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
        use bytes::Bytes;
        let batch = RecordBatch::try_from_iter([
            (
                "unique",
                Arc::new(Int64Array::from_iter_values(0..512)) as ArrayRef,
            ),
            (
                "repeated",
                Arc::new(StringArray::from_iter_values((0..512).map(|_| "same"))) as ArrayRef,
            ),
            ("nulls", Arc::new(Int32Array::new_null(512)) as ArrayRef),
        ])
        .unwrap();
        let chosen = choose(&batch, WriterProperties::default());
        let mut output = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(chosen)).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();
        let columns = metadata.row_group(0).columns();
        assert!(columns[0].dictionary_page_offset().is_none());
        assert!(columns[1].dictionary_page_offset().is_some());
        assert!(columns[2].dictionary_page_offset().is_none());
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output))
            .unwrap()
            .with_batch_size(1024)
            .build()
            .unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert!(reader.next().is_none());
    }
}
