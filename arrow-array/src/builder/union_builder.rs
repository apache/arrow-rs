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

use crate::builder::buffer_builder::{Int32BufferBuilder, Int8BufferBuilder};
use crate::builder::BufferBuilder;
use crate::{make_array, ArrowPrimitiveType, UnionArray};
use arrow_buffer::NullBufferBuilder;
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, Field};
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

/// `FieldData` is a helper struct to track the state of the fields in the `UnionBuilder`.
#[derive(Debug)]
struct FieldData {
    /// The type id for this field
    type_id: i8,
    /// The Arrow data type represented in the `values_buffer`, which is untyped
    data_type: DataType,
    /// A buffer containing the values for this field in raw bytes
    values_buffer: Box<dyn FieldDataValues>,
    ///  The number of array slots represented by the buffer
    slots: usize,
    /// A builder for the null bitmap
    null_buffer_builder: NullBufferBuilder,
}

/// A type-erased [`BufferBuilder`] used by [`FieldData`]
trait FieldDataValues: std::fmt::Debug {
    fn as_mut_any(&mut self) -> &mut dyn Any;

    fn append_null(&mut self);

    fn finish(&mut self) -> Buffer;
}

impl<T: ArrowNativeType> FieldDataValues for BufferBuilder<T> {
    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn append_null(&mut self) {
        self.advance(1)
    }

    fn finish(&mut self) -> Buffer {
        self.finish()
    }
}

impl FieldData {
    /// Creates a new `FieldData`.
    fn new<T: ArrowPrimitiveType>(type_id: i8, data_type: DataType, capacity: usize) -> Self {
        Self {
            type_id,
            data_type,
            slots: 0,
            values_buffer: Box::new(BufferBuilder::<T::Native>::new(capacity)),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    /// Appends a single value to this `FieldData`'s `values_buffer`.
    fn append_value<T: ArrowPrimitiveType>(&mut self, v: T::Native) {
        self.values_buffer
            .as_mut_any()
            .downcast_mut::<BufferBuilder<T::Native>>()
            .expect("Tried to append unexpected type")
            .append(v);

        self.null_buffer_builder.append(true);
        self.slots += 1;
    }

    /// Appends a null to this `FieldData`.
    fn append_null(&mut self) {
        self.values_buffer.append_null();
        self.null_buffer_builder.append(false);
        self.slots += 1;
    }
}

/// Builder for [`UnionArray`]
///
/// Example: **Dense Memory Layout**
///
/// ```
/// # use arrow_array::builder::UnionBuilder;
/// # use arrow_array::types::{Float64Type, Int32Type};
///
/// let mut builder = UnionBuilder::new_dense();
/// builder.append::<Int32Type>("a", 1).unwrap();
/// builder.append::<Float64Type>("b", 3.0).unwrap();
/// builder.append::<Int32Type>("a", 4).unwrap();
/// let union = builder.build().unwrap();
///
/// assert_eq!(union.type_id(0), 0);
/// assert_eq!(union.type_id(1), 1);
/// assert_eq!(union.type_id(2), 0);
///
/// assert_eq!(union.value_offset(0), 0);
/// assert_eq!(union.value_offset(1), 0);
/// assert_eq!(union.value_offset(2), 1);
/// ```
///
/// Example: **Sparse Memory Layout**
/// ```
/// # use arrow_array::builder::UnionBuilder;
/// # use arrow_array::types::{Float64Type, Int32Type};
///
/// let mut builder = UnionBuilder::new_sparse();
/// builder.append::<Int32Type>("a", 1).unwrap();
/// builder.append::<Float64Type>("b", 3.0).unwrap();
/// builder.append::<Int32Type>("a", 4).unwrap();
/// let union = builder.build().unwrap();
///
/// assert_eq!(union.type_id(0), 0);
/// assert_eq!(union.type_id(1), 1);
/// assert_eq!(union.type_id(2), 0);
///
/// assert_eq!(union.value_offset(0), 0);
/// assert_eq!(union.value_offset(1), 1);
/// assert_eq!(union.value_offset(2), 2);
/// ```
#[derive(Debug)]
pub struct UnionBuilder {
    /// The current number of slots in the array
    len: usize,
    /// Maps field names to `FieldData` instances which track the builders for that field
    fields: BTreeMap<String, FieldData>,
    /// Builder to keep track of type ids
    type_id_builder: Int8BufferBuilder,
    /// Builder to keep track of offsets (`None` for sparse unions)
    value_offset_builder: Option<Int32BufferBuilder>,
    initial_capacity: usize,
}

impl UnionBuilder {
    /// Creates a new dense array builder.
    pub fn new_dense() -> Self {
        Self::with_capacity_dense(1024)
    }

    /// Creates a new sparse array builder.
    pub fn new_sparse() -> Self {
        Self::with_capacity_sparse(1024)
    }

    /// Creates a new dense array builder with capacity.
    pub fn with_capacity_dense(capacity: usize) -> Self {
        Self {
            len: 0,
            fields: Default::default(),
            type_id_builder: Int8BufferBuilder::new(capacity),
            value_offset_builder: Some(Int32BufferBuilder::new(capacity)),
            initial_capacity: capacity,
        }
    }

    /// Creates a new sparse array builder  with capacity.
    pub fn with_capacity_sparse(capacity: usize) -> Self {
        Self {
            len: 0,
            fields: Default::default(),
            type_id_builder: Int8BufferBuilder::new(capacity),
            value_offset_builder: None,
            initial_capacity: capacity,
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
    pub fn append_null<T: ArrowPrimitiveType>(
        &mut self,
        type_name: &str,
    ) -> Result<(), ArrowError> {
        self.append_option::<T>(type_name, None)
    }

    /// Appends a value to this builder.
    #[inline]
    pub fn append<T: ArrowPrimitiveType>(
        &mut self,
        type_name: &str,
        v: T::Native,
    ) -> Result<(), ArrowError> {
        self.append_option::<T>(type_name, Some(v))
    }

    fn append_option<T: ArrowPrimitiveType>(
        &mut self,
        type_name: &str,
        v: Option<T::Native>,
    ) -> Result<(), ArrowError> {
        let type_name = type_name.to_string();

        let mut field_data = match self.fields.remove(&type_name) {
            Some(data) => {
                if data.data_type != T::DATA_TYPE {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Attempt to write col \"{}\" with type {} doesn't match existing type {}",
                        type_name,
                        T::DATA_TYPE,
                        data.data_type
                    )));
                }
                data
            }
            None => match self.value_offset_builder {
                Some(_) => FieldData::new::<T>(
                    self.fields.len() as i8,
                    T::DATA_TYPE,
                    self.initial_capacity,
                ),
                // In the case of a sparse union, we should pass the maximum of the currently length and the capacity.
                None => {
                    let mut fd = FieldData::new::<T>(
                        self.fields.len() as i8,
                        T::DATA_TYPE,
                        self.len.max(self.initial_capacity),
                    );
                    for _ in 0..self.len {
                        fd.append_null();
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
                    fd.append_null();
                }
            }
        }

        match v {
            Some(v) => field_data.append_value::<T>(v),
            None => field_data.append_null(),
        }

        self.fields.insert(type_name, field_data);
        self.len += 1;
        Ok(())
    }

    /// Builds this builder creating a new `UnionArray`.
    pub fn build(self) -> Result<UnionArray, ArrowError> {
        let mut children = Vec::with_capacity(self.fields.len());
        let union_fields = self
            .fields
            .into_iter()
            .map(
                |(
                    name,
                    FieldData {
                        type_id,
                        data_type,
                        mut values_buffer,
                        slots,
                        mut null_buffer_builder,
                    },
                )| {
                    let array_ref = make_array(unsafe {
                        ArrayDataBuilder::new(data_type.clone())
                            .add_buffer(values_buffer.finish())
                            .len(slots)
                            .nulls(null_buffer_builder.finish())
                            .build_unchecked()
                    });
                    children.push(array_ref);
                    (type_id, Arc::new(Field::new(name, data_type, false)))
                },
            )
            .collect();
        UnionArray::try_new(
            union_fields,
            self.type_id_builder.into(),
            self.value_offset_builder.map(Into::into),
            children,
        )
    }
}
