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

//! Utilities to generate random arrays and batches

use std::sync::Arc;

use rand::distributions::uniform::SampleRange;
use rand::{distributions::uniform::SampleUniform, Rng};

use crate::array::*;
use crate::error::{ArrowError, Result};
use crate::{
    buffer::{Buffer, MutableBuffer},
    datatypes::*,
};

use super::{bench_util::*, bit_util, test_util::seedable_rng};

/// Create a random [RecordBatch] from a schema
pub fn create_random_batch(
    schema: SchemaRef,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| create_random_array(field, size, null_density, true_density))
        .collect::<Result<Vec<ArrayRef>>>()?;

    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_match_field_names(false),
    )
}

/// Create a random [ArrayRef] from a [DataType] with a length,
/// null density and true density (for [BooleanArray]).
pub fn create_random_array(
    field: &Field,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<ArrayRef> {
    // Override null density with 0.0 if the array is non-nullable
    // and a primitive type in case a nested field is nullable
    let primitive_null_density = match field.is_nullable() {
        true => null_density,
        false => 0.0,
    };
    use DataType::*;
    Ok(match field.data_type() {
        Null => Arc::new(NullArray::new(size)) as ArrayRef,
        Boolean => Arc::new(create_boolean_array(
            size,
            primitive_null_density,
            true_density,
        )),
        Int8 => Arc::new(create_primitive_array::<Int8Type>(
            size,
            primitive_null_density,
        )),
        Int16 => Arc::new(create_primitive_array::<Int16Type>(
            size,
            primitive_null_density,
        )),
        Int32 => Arc::new(create_primitive_array::<Int32Type>(
            size,
            primitive_null_density,
        )),
        Int64 => Arc::new(create_primitive_array::<Int64Type>(
            size,
            primitive_null_density,
        )),
        UInt8 => Arc::new(create_primitive_array::<UInt8Type>(
            size,
            primitive_null_density,
        )),
        UInt16 => Arc::new(create_primitive_array::<UInt16Type>(
            size,
            primitive_null_density,
        )),
        UInt32 => Arc::new(create_primitive_array::<UInt32Type>(
            size,
            primitive_null_density,
        )),
        UInt64 => Arc::new(create_primitive_array::<UInt64Type>(
            size,
            primitive_null_density,
        )),
        Float16 => {
            return Err(ArrowError::NotYetImplemented(
                "Float16 is not implemented".to_string(),
            ))
        }
        Float32 => Arc::new(create_primitive_array::<Float32Type>(
            size,
            primitive_null_density,
        )),
        Float64 => Arc::new(create_primitive_array::<Float64Type>(
            size,
            primitive_null_density,
        )),
        Timestamp(unit, tz) => match unit {
            TimeUnit::Second => Arc::new(
                create_random_temporal_array::<TimestampSecondType>(size, primitive_null_density)
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Millisecond => Arc::new(
                create_random_temporal_array::<TimestampMillisecondType>(
                    size,
                    primitive_null_density,
                )
                .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Microsecond => Arc::new(
                create_random_temporal_array::<TimestampMicrosecondType>(
                    size,
                    primitive_null_density,
                )
                .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Nanosecond => Arc::new(
                create_random_temporal_array::<TimestampNanosecondType>(
                    size,
                    primitive_null_density,
                )
                .with_timezone_opt(tz.clone()),
            ),
        },
        Date32 => Arc::new(create_random_temporal_array::<Date32Type>(
            size,
            primitive_null_density,
        )),
        Date64 => Arc::new(create_random_temporal_array::<Date64Type>(
            size,
            primitive_null_density,
        )),
        Time32(unit) => match unit {
            TimeUnit::Second => Arc::new(create_random_temporal_array::<Time32SecondType>(
                size,
                primitive_null_density,
            )) as ArrayRef,
            TimeUnit::Millisecond => Arc::new(
                create_random_temporal_array::<Time32MillisecondType>(size, primitive_null_density),
            ),
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported unit {unit:?} for Time32"
                )))
            }
        },
        Time64(unit) => match unit {
            TimeUnit::Microsecond => Arc::new(
                create_random_temporal_array::<Time64MicrosecondType>(size, primitive_null_density),
            ) as ArrayRef,
            TimeUnit::Nanosecond => Arc::new(create_random_temporal_array::<Time64NanosecondType>(
                size,
                primitive_null_density,
            )),
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported unit {unit:?} for Time64"
                )))
            }
        },
        Utf8 => Arc::new(create_string_array::<i32>(size, primitive_null_density)),
        LargeUtf8 => Arc::new(create_string_array::<i64>(size, primitive_null_density)),
        Utf8View => Arc::new(create_string_view_array_with_len(
            size,
            primitive_null_density,
            4,
            false,
        )),
        Binary => Arc::new(create_binary_array::<i32>(size, primitive_null_density)),
        LargeBinary => Arc::new(create_binary_array::<i64>(size, primitive_null_density)),
        FixedSizeBinary(len) => Arc::new(create_fsb_array(
            size,
            primitive_null_density,
            *len as usize,
        )),
        BinaryView => Arc::new(
            create_string_view_array_with_len(size, primitive_null_density, 4, false)
                .to_binary_view(),
        ),
        List(_) => create_random_list_array(field, size, null_density, true_density)?,
        LargeList(_) => create_random_list_array(field, size, null_density, true_density)?,
        Struct(_) => create_random_struct_array(field, size, null_density, true_density)?,
        d @ Dictionary(_, value_type) if crate::compute::can_cast_types(value_type, d) => {
            let f = Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            );
            let v = create_random_array(&f, size, null_density, true_density)?;
            crate::compute::cast(&v, d)?
        }
        Map(_, _) => create_random_map_array(field, size, null_density, true_density)?,
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Generating random arrays not yet implemented for {other:?}"
            )))
        }
    })
}

#[inline]
fn create_random_list_array(
    field: &Field,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<ArrayRef> {
    // Override null density with 0.0 if the array is non-nullable
    let list_null_density = match field.is_nullable() {
        true => null_density,
        false => 0.0,
    };
    let list_field;
    let (offsets, child_len) = match field.data_type() {
        DataType::List(f) => {
            let (offsets, child_len) = create_random_offsets::<i32>(size, 0, 5);
            list_field = f;
            (Buffer::from(offsets.to_byte_slice()), child_len as usize)
        }
        DataType::LargeList(f) => {
            let (offsets, child_len) = create_random_offsets::<i64>(size, 0, 5);
            list_field = f;
            (Buffer::from(offsets.to_byte_slice()), child_len as usize)
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot create list array for field {field:?}"
            )))
        }
    };

    // Create list's child data
    let child_array = create_random_array(list_field, child_len, null_density, true_density)?;
    let child_data = child_array.to_data();
    // Create list's null buffers, if it is nullable
    let null_buffer = match field.is_nullable() {
        true => Some(create_random_null_buffer(size, list_null_density)),
        false => None,
    };
    let list_data = unsafe {
        ArrayData::new_unchecked(
            field.data_type().clone(),
            size,
            None,
            null_buffer,
            0,
            vec![offsets],
            vec![child_data],
        )
    };
    Ok(make_array(list_data))
}

#[inline]
fn create_random_struct_array(
    field: &Field,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<ArrayRef> {
    let struct_fields = match field.data_type() {
        DataType::Struct(fields) => fields,
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot create struct array for field {field:?}"
            )))
        }
    };

    let child_arrays = struct_fields
        .iter()
        .map(|struct_field| create_random_array(struct_field, size, null_density, true_density))
        .collect::<Result<Vec<_>>>()?;

    let null_buffer = match field.is_nullable() {
        true => {
            let nulls = arrow_buffer::BooleanBuffer::new(
                create_random_null_buffer(size, null_density),
                0,
                size,
            );
            Some(nulls.into())
        }
        false => None,
    };

    Ok(Arc::new(StructArray::try_new(
        struct_fields.clone(),
        child_arrays,
        null_buffer,
    )?))
}

#[inline]
fn create_random_map_array(
    field: &Field,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<ArrayRef> {
    // Override null density with 0.0 if the array is non-nullable
    let map_null_density = match field.is_nullable() {
        true => null_density,
        false => 0.0,
    };

    let entries_field = match field.data_type() {
        DataType::Map(f, _) => f,
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot create map array for field {field:?}"
            )))
        }
    };

    let (offsets, child_len) = create_random_offsets::<i32>(size, 0, 5);
    let offsets = Buffer::from(offsets.to_byte_slice());

    let entries = create_random_array(
        entries_field,
        child_len as usize,
        null_density,
        true_density,
    )?
    .to_data();

    let null_buffer = match field.is_nullable() {
        true => Some(create_random_null_buffer(size, map_null_density)),
        false => None,
    };

    let map_data = unsafe {
        ArrayData::new_unchecked(
            field.data_type().clone(),
            size,
            None,
            null_buffer,
            0,
            vec![offsets],
            vec![entries],
        )
    };
    Ok(make_array(map_data))
}

/// Generate random offsets for list arrays
fn create_random_offsets<T: OffsetSizeTrait + SampleUniform>(
    size: usize,
    min: T,
    max: T,
) -> (Vec<T>, T) {
    let rng = &mut seedable_rng();

    let mut current_offset = T::zero();

    let mut offsets = Vec::with_capacity(size + 1);
    offsets.push(current_offset);

    (0..size).for_each(|_| {
        current_offset += rng.gen_range(min..max);
        offsets.push(current_offset);
    });

    (offsets, current_offset)
}

fn create_random_null_buffer(size: usize, null_density: f32) -> Buffer {
    let mut rng = seedable_rng();
    let mut mut_buf = MutableBuffer::new_null(size);
    {
        let mut_slice = mut_buf.as_slice_mut();
        (0..size).for_each(|i| {
            if rng.gen::<f32>() >= null_density {
                bit_util::set_bit(mut_slice, i)
            }
        })
    };
    mut_buf.into()
}

/// Useful for testing. The range of values are not likely to be representative of the
/// actual bounds.
pub trait RandomTemporalValue: ArrowTemporalType {
    /// Returns the range of values for `impl`'d type
    fn value_range() -> impl SampleRange<Self::Native>;

    /// Generate a random value within the range of the type
    fn gen_range<R: Rng>(rng: &mut R) -> Self::Native
    where
        Self::Native: SampleUniform,
    {
        rng.gen_range(Self::value_range())
    }

    /// Generate a random value of the type
    fn random<R: Rng>(rng: &mut R) -> Self::Native
    where
        Self::Native: SampleUniform,
    {
        Self::gen_range(rng)
    }
}

impl RandomTemporalValue for TimestampSecondType {
    /// Range of values for a timestamp in seconds. The range begins at the start
    /// of the unix epoch and continues for 100 years.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..60 * 60 * 24 * 365 * 100
    }
}

impl RandomTemporalValue for TimestampMillisecondType {
    /// Range of values for a timestamp in milliseconds. The range begins at the start
    /// of the unix epoch and continues for 100 years.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 60 * 60 * 24 * 365 * 100
    }
}

impl RandomTemporalValue for TimestampMicrosecondType {
    /// Range of values for a timestamp in microseconds. The range begins at the start
    /// of the unix epoch and continues for 100 years.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 1_000 * 60 * 60 * 24 * 365 * 100
    }
}

impl RandomTemporalValue for TimestampNanosecondType {
    /// Range of values for a timestamp in nanoseconds. The range begins at the start
    /// of the unix epoch and continues for 100 years.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 1_000 * 1_000 * 60 * 60 * 24 * 365 * 100
    }
}

impl RandomTemporalValue for Date32Type {
    /// Range of values representing the elapsed time since UNIX epoch in days. The
    /// range begins at the start of the unix epoch and continues for 100 years.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..365 * 100
    }
}

impl RandomTemporalValue for Date64Type {
    /// Range of values  representing the elapsed time since UNIX epoch in milliseconds.
    /// The range begins at the start of the unix epoch and continues for 100 years.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 60 * 60 * 24 * 365 * 100
    }
}

impl RandomTemporalValue for Time32SecondType {
    /// Range of values representing the elapsed time since midnight in seconds. The
    /// range is from 0 to 24 hours.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..60 * 60 * 24
    }
}

impl RandomTemporalValue for Time32MillisecondType {
    /// Range of values representing the elapsed time since midnight in milliseconds. The
    /// range is from 0 to 24 hours.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 60 * 60 * 24
    }
}

impl RandomTemporalValue for Time64MicrosecondType {
    /// Range of values representing the elapsed time since midnight in microseconds. The
    /// range is from 0 to 24 hours.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 1_000 * 60 * 60 * 24
    }
}

impl RandomTemporalValue for Time64NanosecondType {
    /// Range of values representing the elapsed time since midnight in nanoseconds. The
    /// range is from 0 to 24 hours.
    fn value_range() -> impl SampleRange<Self::Native> {
        0..1_000 * 1_000 * 1_000 * 60 * 60 * 24
    }
}

fn create_random_temporal_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: RandomTemporalValue,
    <T as ArrowPrimitiveType>::Native: SampleUniform,
{
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(T::random(&mut rng))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_batch() {
        let size = 32;
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "timestamp_without_timezone",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "timestamp_with_timezone",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ];
        let schema = Schema::new(fields);
        let schema_ref = Arc::new(schema);
        let batch = create_random_batch(schema_ref.clone(), size, 0.35, 0.7).unwrap();

        assert_eq!(batch.schema(), schema_ref);
        assert_eq!(batch.num_columns(), schema_ref.fields().len());
        for array in batch.columns() {
            assert_eq!(array.len(), size);
        }
    }

    #[test]
    fn test_create_batch_non_null() {
        let size = 32;
        let fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new_list_field(DataType::LargeUtf8, false))),
                false,
            ),
            Field::new("a", DataType::Int32, false),
        ];
        let schema = Schema::new(fields);
        let schema_ref = Arc::new(schema);
        let batch = create_random_batch(schema_ref.clone(), size, 0.35, 0.7).unwrap();

        assert_eq!(batch.schema(), schema_ref);
        assert_eq!(batch.num_columns(), schema_ref.fields().len());
        for array in batch.columns() {
            assert_eq!(array.null_count(), 0);
            assert_eq!(array.logical_null_count(), 0);
        }
        // Test that the list's child values are non-null
        let b_array = batch.column(1);
        let list_array = b_array.as_list::<i32>();
        let child_array = list_array.values();
        assert_eq!(child_array.null_count(), 0);
        // There should be more values than the list, to show that it's a list
        assert!(child_array.len() > list_array.len());
    }

    #[test]
    fn test_create_struct_array() {
        let size = 32;
        let struct_fields = Fields::from(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new(
                "c",
                DataType::LargeList(Arc::new(Field::new_list_field(
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::FixedSizeBinary(6),
                        true,
                    ))),
                    false,
                ))),
                true,
            ),
            Field::new(
                "d",
                DataType::Struct(Fields::from(vec![
                    Field::new("d_x", DataType::Int32, true),
                    Field::new("d_y", DataType::Float32, false),
                    Field::new("d_z", DataType::Binary, true),
                ])),
                true,
            ),
        ]);
        let field = Field::new("struct", DataType::Struct(struct_fields), true);
        let array = create_random_array(&field, size, 0.2, 0.5).unwrap();

        assert_eq!(array.len(), 32);
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.columns().len(), 3);

        // Test that the nested list makes sense,
        // i.e. its children's values are more than the parent, to show repetition
        let col_c = struct_array.column_by_name("c").unwrap();
        let col_c = col_c.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(col_c.len(), size);
        let col_c_list = col_c.values().as_list::<i32>();
        assert!(col_c_list.len() > size);
        // Its values should be FixedSizeBinary(6)
        let fsb = col_c_list.values();
        assert_eq!(fsb.data_type(), &DataType::FixedSizeBinary(6));
        assert!(fsb.len() > col_c_list.len());

        // Test nested struct
        let col_d = struct_array.column_by_name("d").unwrap();
        let col_d = col_d.as_any().downcast_ref::<StructArray>().unwrap();
        let col_d_y = col_d.column_by_name("d_y").unwrap();
        assert_eq!(col_d_y.data_type(), &DataType::Float32);
        assert_eq!(col_d_y.null_count(), 0);
    }

    #[test]
    fn test_create_list_array_nested_nullability() {
        let list_field = Field::new_list(
            "not_null_list",
            Field::new_list_field(DataType::Boolean, true),
            false,
        );

        let list_array = create_random_array(&list_field, 100, 0.95, 0.5).unwrap();

        assert_eq!(list_array.null_count(), 0);
        assert!(list_array.as_list::<i32>().values().null_count() > 0);
    }

    #[test]
    fn test_create_struct_array_nested_nullability() {
        let struct_child_fields = vec![
            Field::new("null_int", DataType::Int32, true),
            Field::new("int", DataType::Int32, false),
        ];
        let struct_field = Field::new_struct("not_null_struct", struct_child_fields, false);

        let struct_array = create_random_array(&struct_field, 100, 0.95, 0.5).unwrap();

        assert_eq!(struct_array.null_count(), 0);
        assert!(
            struct_array
                .as_struct()
                .column_by_name("null_int")
                .unwrap()
                .null_count()
                > 0
        );
        assert_eq!(
            struct_array
                .as_struct()
                .column_by_name("int")
                .unwrap()
                .null_count(),
            0
        );
    }

    #[test]
    fn test_create_list_array_nested_struct_nullability() {
        let struct_child_fields = vec![
            Field::new("null_int", DataType::Int32, true),
            Field::new("int", DataType::Int32, false),
        ];
        let list_item_field =
            Field::new_list_field(DataType::Struct(struct_child_fields.into()), true);
        let list_field = Field::new_list("not_null_list", list_item_field, false);

        let list_array = create_random_array(&list_field, 100, 0.95, 0.5).unwrap();

        assert_eq!(list_array.null_count(), 0);
        assert!(list_array.as_list::<i32>().values().null_count() > 0);
        assert!(
            list_array
                .as_list::<i32>()
                .values()
                .as_struct()
                .column_by_name("null_int")
                .unwrap()
                .null_count()
                > 0
        );
        assert_eq!(
            list_array
                .as_list::<i32>()
                .values()
                .as_struct()
                .column_by_name("int")
                .unwrap()
                .null_count(),
            0
        );
    }

    #[test]
    fn test_create_map_array() {
        let map_field = Field::new_map(
            "map",
            "entries",
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            false,
            false,
        );
        let array = create_random_array(&map_field, 100, 0.8, 0.5).unwrap();

        assert_eq!(array.len(), 100);
        // Map field is not null
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.logical_null_count(), 0);
        // Maps have multiple values like a list, so internal arrays are longer
        assert!(array.as_map().keys().len() > array.len());
        assert!(array.as_map().values().len() > array.len());
        // Keys are not nullable
        assert_eq!(array.as_map().keys().null_count(), 0);
        // Values are nullable
        assert!(array.as_map().values().null_count() > 0);

        assert_eq!(array.as_map().keys().data_type(), &DataType::Utf8);
        assert_eq!(array.as_map().values().data_type(), &DataType::Utf8);
    }
}
