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

use std::collections::HashMap;

use crate::array::ArrayDataBuilder;
use crate::array::Int32BufferBuilder;
use crate::array::Int8BufferBuilder;
use crate::array::UnionArray;
use crate::buffer::MutableBuffer;

use crate::datatypes::ArrowPrimitiveType;
use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::datatypes::IntervalMonthDayNanoType;
use crate::datatypes::IntervalUnit;
use crate::datatypes::{Float32Type, Float64Type};
use crate::datatypes::{Int16Type, Int32Type, Int64Type, Int8Type};
use crate::datatypes::{UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use crate::error::{ArrowError, Result};

use super::{BooleanBufferBuilder, BufferBuilder};

use super::buffer_builder::builder_to_mutable_buffer;
use super::buffer_builder::mutable_buffer_to_builder;
use crate::array::make_array;

/// `FieldData` is a helper struct to track the state of the fields in the `UnionBuilder`.
#[derive(Debug)]
struct FieldData {
    /// The type id for this field
    type_id: i8,
    /// The Arrow data type represented in the `values_buffer`, which is untyped
    data_type: DataType,
    /// A buffer containing the values for this field in raw bytes
    values_buffer: Option<MutableBuffer>,
    ///  The number of array slots represented by the buffer
    slots: usize,
    /// A builder for the null bitmap
    bitmap_builder: BooleanBufferBuilder,
}

impl FieldData {
    /// Creates a new `FieldData`.
    fn new(type_id: i8, data_type: DataType) -> Self {
        Self {
            type_id,
            data_type,
            values_buffer: Some(MutableBuffer::new(1)),
            slots: 0,
            bitmap_builder: BooleanBufferBuilder::new(1),
        }
    }

    /// Appends a single value to this `FieldData`'s `values_buffer`.
    #[allow(clippy::unnecessary_wraps)]
    fn append_to_values_buffer<T: ArrowPrimitiveType>(
        &mut self,
        v: T::Native,
    ) -> Result<()> {
        let values_buffer = self
            .values_buffer
            .take()
            .expect("Values buffer was never created");
        let mut builder: BufferBuilder<T::Native> =
            mutable_buffer_to_builder(values_buffer, self.slots);
        builder.append(v);
        let mutable_buffer = builder_to_mutable_buffer(builder);
        self.values_buffer = Some(mutable_buffer);

        self.slots += 1;
        self.bitmap_builder.append(true);
        Ok(())
    }

    /// Appends a null to this `FieldData`.
    #[allow(clippy::unnecessary_wraps)]
    fn append_null<T: ArrowPrimitiveType>(&mut self) -> Result<()> {
        let values_buffer = self
            .values_buffer
            .take()
            .expect("Values buffer was never created");

        let mut builder: BufferBuilder<T::Native> =
            mutable_buffer_to_builder(values_buffer, self.slots);

        builder.advance(1);
        let mutable_buffer = builder_to_mutable_buffer(builder);
        self.values_buffer = Some(mutable_buffer);
        self.slots += 1;
        self.bitmap_builder.append(false);
        Ok(())
    }

    /// Appends a null to this `FieldData` when the type is not known at compile time.
    ///
    /// As the main `append` method of `UnionBuilder` is generic, we need a way to append null
    /// slots to the fields that are not being appended to in the case of sparse unions.  This
    /// method solves this problem by appending dynamically based on `DataType`.
    ///
    /// Note, this method does **not** update the length of the `UnionArray` (this is done by the
    /// main append operation) and assumes that it is called from a method that is generic over `T`
    /// where `T` satisfies the bound `ArrowPrimitiveType`.
    fn append_null_dynamic(&mut self) -> Result<()> {
        match self.data_type {
            DataType::Null => unimplemented!(),
            DataType::Int8 => self.append_null::<Int8Type>()?,
            DataType::Int16 => self.append_null::<Int16Type>()?,
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => {
                self.append_null::<Int32Type>()?
            }
            DataType::Int64
            | DataType::Timestamp(_, _)
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Interval(IntervalUnit::DayTime)
            | DataType::Duration(_) => self.append_null::<Int64Type>()?,
            DataType::Interval(IntervalUnit::MonthDayNano) => self.append_null::<IntervalMonthDayNanoType>()?,
            DataType::UInt8 => self.append_null::<UInt8Type>()?,
            DataType::UInt16 => self.append_null::<UInt16Type>()?,
            DataType::UInt32 => self.append_null::<UInt32Type>()?,
            DataType::UInt64 => self.append_null::<UInt64Type>()?,
            DataType::Float32 => self.append_null::<Float32Type>()?,
            DataType::Float64 => self.append_null::<Float64Type>()?,
            _ => unreachable!("All cases of types that satisfy the trait bounds over T are covered above."),
        };
        Ok(())
    }
}

/// Builder type for creating a new `UnionArray`.
///
/// Example: **Dense Memory Layout**
///
/// ```
/// use arrow::array::UnionBuilder;
/// use arrow::datatypes::{Float64Type, Int32Type};
///
/// let mut builder = UnionBuilder::new_dense(3);
/// builder.append::<Int32Type>("a", 1).unwrap();
/// builder.append::<Float64Type>("b", 3.0).unwrap();
/// builder.append::<Int32Type>("a", 4).unwrap();
/// let union = builder.build().unwrap();
///
/// assert_eq!(union.type_id(0), 0_i8);
/// assert_eq!(union.type_id(1), 1_i8);
/// assert_eq!(union.type_id(2), 0_i8);
///
/// assert_eq!(union.value_offset(0), 0_i32);
/// assert_eq!(union.value_offset(1), 0_i32);
/// assert_eq!(union.value_offset(2), 1_i32);
/// ```
///
/// Example: **Sparse Memory Layout**
/// ```
/// use arrow::array::UnionBuilder;
/// use arrow::datatypes::{Float64Type, Int32Type};
///
/// let mut builder = UnionBuilder::new_sparse(3);
/// builder.append::<Int32Type>("a", 1).unwrap();
/// builder.append::<Float64Type>("b", 3.0).unwrap();
/// builder.append::<Int32Type>("a", 4).unwrap();
/// let union = builder.build().unwrap();
///
/// assert_eq!(union.type_id(0), 0_i8);
/// assert_eq!(union.type_id(1), 1_i8);
/// assert_eq!(union.type_id(2), 0_i8);
///
/// assert_eq!(union.value_offset(0), 0_i32);
/// assert_eq!(union.value_offset(1), 1_i32);
/// assert_eq!(union.value_offset(2), 2_i32);
/// ```
#[derive(Debug)]
pub struct UnionBuilder {
    /// The current number of slots in the array
    len: usize,
    /// Maps field names to `FieldData` instances which track the builders for that field
    fields: HashMap<String, FieldData>,
    /// Builder to keep track of type ids
    type_id_builder: Int8BufferBuilder,
    /// Builder to keep track of offsets (`None` for sparse unions)
    value_offset_builder: Option<Int32BufferBuilder>,
}

impl UnionBuilder {
    /// Creates a new dense array builder.
    pub fn new_dense(capacity: usize) -> Self {
        Self {
            len: 0,
            fields: HashMap::default(),
            type_id_builder: Int8BufferBuilder::new(capacity),
            value_offset_builder: Some(Int32BufferBuilder::new(capacity)),
        }
    }

    /// Creates a new sparse array builder.
    pub fn new_sparse(capacity: usize) -> Self {
        Self {
            len: 0,
            fields: HashMap::default(),
            type_id_builder: Int8BufferBuilder::new(capacity),
            value_offset_builder: None,
        }
    }

    /// Appends a null to this builder, encoding the null in the array
    /// of the `type_name` child / field.
    ///
    /// Since `UnionArray` encodes nulls as an entry in its children
    /// (it doesn't have a validity bitmap itself), and where the null
    /// is part of the final array, appending a NULL requires
    /// specifying which field (child) to use.
    #[inline]
    pub fn append_null<T: ArrowPrimitiveType>(&mut self, type_name: &str) -> Result<()> {
        self.append_option::<T>(type_name, None)
    }

    /// Appends a value to this builder.
    #[inline]
    pub fn append<T: ArrowPrimitiveType>(
        &mut self,
        type_name: &str,
        v: T::Native,
    ) -> Result<()> {
        self.append_option::<T>(type_name, Some(v))
    }

    fn append_option<T: ArrowPrimitiveType>(
        &mut self,
        type_name: &str,
        v: Option<T::Native>,
    ) -> Result<()> {
        let type_name = type_name.to_string();

        let mut field_data = match self.fields.remove(&type_name) {
            Some(data) => {
                if data.data_type != T::DATA_TYPE {
                    return Err(ArrowError::InvalidArgumentError(format!("Attempt to write col \"{}\" with type {} doesn't match existing type {}", type_name, T::DATA_TYPE, data.data_type)));
                }
                data
            }
            None => match self.value_offset_builder {
                Some(_) => FieldData::new(self.fields.len() as i8, T::DATA_TYPE),
                None => {
                    let mut fd = FieldData::new(self.fields.len() as i8, T::DATA_TYPE);
                    for _ in 0..self.len {
                        fd.append_null::<T>()?;
                    }
                    fd
                }
            },
        };
        self.type_id_builder.append(field_data.type_id);

        match &mut self.value_offset_builder {
            // Dense Union
            Some(offset_builder) => {
                offset_builder.append(field_data.slots as i32);
            }
            // Sparse Union
            None => {
                for (_, fd) in self.fields.iter_mut() {
                    // Append to all bar the FieldData currently being appended to
                    fd.append_null_dynamic()?;
                }
            }
        }

        match v {
            Some(v) => field_data.append_to_values_buffer::<T>(v)?,
            None => field_data.append_null::<T>()?,
        }

        self.fields.insert(type_name, field_data);
        self.len += 1;
        Ok(())
    }

    /// Builds this builder creating a new `UnionArray`.
    pub fn build(mut self) -> Result<UnionArray> {
        let type_id_buffer = self.type_id_builder.finish();
        let value_offsets_buffer = self.value_offset_builder.map(|mut b| b.finish());
        let mut children = Vec::new();
        for (
            name,
            FieldData {
                type_id,
                data_type,
                values_buffer,
                slots,
                mut bitmap_builder,
            },
        ) in self.fields.into_iter()
        {
            let buffer = values_buffer
                .expect("The `values_buffer` should only ever be None inside the `append` method.")
                .into();
            let arr_data_builder = ArrayDataBuilder::new(data_type.clone())
                .add_buffer(buffer)
                .len(slots)
                .null_bit_buffer(Some(bitmap_builder.finish()));

            let arr_data_ref = unsafe { arr_data_builder.build_unchecked() };
            let array_ref = make_array(arr_data_ref);
            children.push((type_id, (Field::new(&name, data_type, false), array_ref)))
        }

        children.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .expect("This will never be None as type ids are always i8 values.")
        });
        let children: Vec<_> = children.into_iter().map(|(_, b)| b).collect();

        let type_ids: Vec<i8> = (0_i8..children.len() as i8).collect();

        UnionArray::try_new(&type_ids, type_id_buffer, value_offsets_buffer, children)
    }
}

#[cfg(test)]
mod tests {}
