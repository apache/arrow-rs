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

//! Fixed-length byte-array physical storage and Arrow writer bridge.

use super::*;

/// Already-downcast storage for Arrow types represented by fixed-length byte arrays.
#[derive(Clone, Copy)]
pub(super) enum FixedLenByteArrayStorage<'a> {
    Fixed(&'a arrow_array::FixedSizeBinaryArray),
    YearMonth(&'a [i32]),
    DayTime(&'a [arrow_buffer::IntervalDayTime]),
    Decimal32 {
        values: &'a [i32],
        width: usize,
    },
    Decimal64 {
        values: &'a [i64],
        width: usize,
    },
    Decimal128 {
        values: &'a [i128],
        width: usize,
    },
    Decimal256 {
        values: &'a [arrow_buffer::i256],
        width: usize,
    },
    Float16(&'a [f16]),
}

impl FixedLenByteArrayStorage<'_> {
    fn len(self) -> usize {
        match self {
            Self::Fixed(values) => values.len(),
            Self::YearMonth(values) => values.len(),
            Self::DayTime(values) => values.len(),
            Self::Decimal32 { values, .. } => values.len(),
            Self::Decimal64 { values, .. } => values.len(),
            Self::Decimal128 { values, .. } => values.len(),
            Self::Decimal256 { values, .. } => values.len(),
            Self::Float16(values) => values.len(),
        }
    }

    fn width(self) -> usize {
        match self {
            Self::Fixed(values) => values.value_size(),
            Self::YearMonth(_) | Self::DayTime(_) => 12,
            Self::Decimal32 { width, .. }
            | Self::Decimal64 { width, .. }
            | Self::Decimal128 { width, .. }
            | Self::Decimal256 { width, .. } => width,
            Self::Float16(_) => 2,
        }
    }

    fn write_at(self, index: usize, dest: &mut [u8]) {
        match self {
            Self::Fixed(values) => dest.copy_from_slice(values.value(index)),
            Self::YearMonth(values) => {
                dest[..4].copy_from_slice(&values[index].to_le_bytes());
                dest[4..].fill(0);
            }
            Self::DayTime(values) => {
                let value = values[index];
                dest[..4].fill(0);
                dest[4..8].copy_from_slice(&value.days.to_le_bytes());
                dest[8..12].copy_from_slice(&value.milliseconds.to_le_bytes());
            }
            Self::Decimal32 { values, width } => {
                dest.copy_from_slice(&values[index].to_be_bytes()[(4 - width)..])
            }
            Self::Decimal64 { values, width } => {
                dest.copy_from_slice(&values[index].to_be_bytes()[(8 - width)..])
            }
            Self::Decimal128 { values, width } => {
                dest.copy_from_slice(&values[index].to_be_bytes()[(16 - width)..])
            }
            Self::Decimal256 { values, width } => {
                dest.copy_from_slice(&values[index].to_be_bytes()[(32 - width)..])
            }
            Self::Float16(values) => dest.copy_from_slice(&values[index].to_le_bytes()),
        }
    }
}

struct PhysicalFixedLenByteArraySource<'a>(
    FixedLenByteArrayStorage<'a>,
    PhysicalValueSelection<'a>,
);

impl FixedLenByteArraySource for PhysicalFixedLenByteArraySource<'_> {
    fn len(&self) -> usize {
        self.1.len()
    }

    fn validate_width(&self, expected_width: usize) -> Result<()> {
        let width = self.0.width();
        if self.len() != 0 && width != expected_width {
            return Err(general_err!(
                "Mismatched FixedLenByteArray sizes: {} != {}",
                width,
                expected_width
            ));
        }
        Ok(())
    }

    fn is_grouped(&self) -> bool {
        self.1.is_grouped()
    }

    fn write_to(self, sink: &mut FixedLenByteArraySink<'_>) -> Result<()> {
        let Self(storage, selection) = self;
        if selection.should_cache_dictionary(storage.len())
            && storage.width() <= FIXED_LEN_BYTE_ARRAY_MAX_WIDTH
            && sink.try_consume_physical_source(
                storage.len(),
                selection,
                storage.width(),
                selection.is_grouped(),
                move |index, dest| storage.write_at(index, dest),
            )?
        {
            return Ok(());
        }

        if let FixedLenByteArrayStorage::Fixed(array) = storage
            && !selection.is_grouped()
        {
            let bytes = array.value_data();
            let width = array.value_size();
            let mut push = |start: usize, len: usize| {
                let byte_start = start * width;
                sink.push_batch(FixedLenByteArrayBatch::Packed(
                    PackedFixedLenByteArrayBatch::new(
                        &bytes[byte_start..byte_start + len * width],
                        width,
                        len,
                    ),
                ))
            };
            if selection.try_for_each_borrowable_range(|range| push(range.start, range.len()))? {
                return Ok(());
            }
        }

        if let FixedLenByteArrayStorage::Fixed(array) = storage {
            if !selection.is_grouped() {
                let bytes = array.value_data();
                let width = array.value_size();
                return sink.push_selected(map_values(selection, move |index| {
                    &bytes[index * width..(index + 1) * width]
                }));
            }
            let values = map_values(selection, move |index| array.value(index));
            return gather_run_groups_tiled::<FIXED_LEN_BYTE_ARRAY_BATCH_VALUES, _, _>(
                values,
                |values, counts| {
                    sink.push_batch(FixedLenByteArrayBatch::RunGroups(RunBatch {
                        values,
                        counts,
                    }))
                },
            );
        }

        write_computed_fixed_len_values(selection, sink, storage.width(), move |index, dest| {
            storage.write_at(index, dest)
        })
    }
}

impl<'a> ArrowPhysicalBridge<'a> for FixedLenByteArrayStorage<'a> {
    type ColumnEncoder = TypedColumnChunkEncoder<FixedLenByteArrayType>;

    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self> {
        Ok(match column.data_type() {
            ArrowDataType::FixedSizeBinary(_) => Self::Fixed(column.as_fixed_size_binary()),
            ArrowDataType::Interval(IntervalUnit::YearMonth) => {
                Self::YearMonth(primitive_values::<IntervalYearMonthType>(column))
            }
            ArrowDataType::Interval(IntervalUnit::DayTime) => {
                Self::DayTime(primitive_values::<IntervalDayTimeType>(column))
            }
            ArrowDataType::Decimal32(_, _) => {
                let array = column.as_primitive::<Decimal32Type>();
                Self::Decimal32 {
                    values: array.values().as_ref(),
                    width: decimal_length_from_precision(array.precision()),
                }
            }
            ArrowDataType::Decimal64(_, _) => {
                let array = column.as_primitive::<Decimal64Type>();
                Self::Decimal64 {
                    values: array.values().as_ref(),
                    width: decimal_length_from_precision(array.precision()),
                }
            }
            ArrowDataType::Decimal128(_, _) => {
                let array = column.as_primitive::<Decimal128Type>();
                Self::Decimal128 {
                    values: array.values().as_ref(),
                    width: decimal_length_from_precision(array.precision()),
                }
            }
            ArrowDataType::Decimal256(_, _) => {
                let array = column.as_primitive::<Decimal256Type>();
                Self::Decimal256 {
                    values: array.values().as_ref(),
                    width: decimal_length_from_precision(array.precision()),
                }
            }
            ArrowDataType::Float16 => Self::Float16(primitive_values::<Float16Type>(column)),
            ArrowDataType::Interval(interval_unit) => {
                return Err(ParquetError::NYI(format!(
                    "Attempting to write an Arrow interval type {interval_unit:?} to parquet that is not yet implemented"
                )));
            }
            _ => {
                return Err(ParquetError::NYI(
                    "Attempting to write an Arrow type to FixedLenByteArray that is not yet implemented"
                        .to_string(),
                ));
            }
        })
    }

    fn write_values(
        self,
        encoder: &mut Self::ColumnEncoder,
        selection: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        encoder.write_fixed_len_byte_array_source(PhysicalFixedLenByteArraySource(self, selection))
    }
}

/// Emit computed values (decimals, float16, interval): `write_at(i, dest)`
/// writes value `i`'s `width` bytes straight into a packed tile slot.
#[inline]
fn write_computed_fixed_len_values<F>(
    selection: PhysicalValueSelection<'_>,
    sink: &mut FixedLenByteArraySink<'_>,
    width: usize,
    write_at: F,
) -> Result<()>
where
    F: Fn(usize, &mut [u8]) + Copy,
{
    if selection.is_grouped() {
        let mut buf = [0u8; FIXED_LEN_BYTE_ARRAY_MAX_WIDTH];
        return selection.try_for_each_index_group(|index, count| {
            write_at(index, &mut buf[..width]);
            sink.push_batch(FixedLenByteArrayBatch::RunGroups(RunBatch {
                values: &[&buf[..width]],
                counts: &[count],
            }))
        });
    }

    let mut packer = FixedLenByteArrayBatchPacker::new(sink, width);
    selection.try_for_each_index(|index| packer.push(|dest| write_at(index, dest)))?;
    packer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Decimal128Array, FixedSizeBinaryArray, Float16Array, IntervalYearMonthArray,
    };
    use std::sync::Arc;

    #[test]
    fn native_fixed_width_validation_uses_emitted_width() {
        let arrays: [(ArrayRef, usize); 4] = [
            (
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some([1, 2, 3])].into_iter(),
                        3,
                    )
                    .unwrap(),
                ),
                3,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![123])
                        .with_precision_and_scale(5, 0)
                        .unwrap(),
                ),
                3,
            ),
            (Arc::new(IntervalYearMonthArray::from(vec![1])), 12),
            (Arc::new(Float16Array::from(vec![f16::from_f32(1.0)])), 2),
        ];
        for (array, width) in arrays {
            let storage = FixedLenByteArrayStorage::bind(array.as_ref()).unwrap();
            for selection in [
                ValueSelectionRef::Dense { offset: 0, len: 1 },
                ValueSelectionRef::Sparse(&[0]),
            ] {
                let source = PhysicalFixedLenByteArraySource(
                    storage,
                    PhysicalValueSelection::identity(selection),
                );
                source.validate_width(width).unwrap();
                for wrong_width in [width - 1, width + 1] {
                    let error = source.validate_width(wrong_width).unwrap_err();
                    assert!(
                        error.to_string().contains(&format!(
                            "Mismatched FixedLenByteArray sizes: {width} != {wrong_width}"
                        )),
                        "{error}"
                    );
                }
            }
            // Empty selections emit no bytes, matching the legacy slice source.
            PhysicalFixedLenByteArraySource(
                storage,
                PhysicalValueSelection::identity(ValueSelectionRef::Empty),
            )
            .validate_width(width + 1)
            .unwrap();
        }
    }
}
