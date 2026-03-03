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

use crate::builder::buffer_builder::{Int8BufferBuilder, Int32BufferBuilder};
use crate::builder::{ArrayBuilder, BufferBuilder};
use crate::{ArrayRef, ArrowPrimitiveType, UnionArray, make_array};
use arrow_buffer::NullBufferBuilder;
use arrow_buffer::{ArrowNativeType, Buffer, ScalarBuffer};
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
trait FieldDataValues: std::fmt::Debug + Send + Sync {
    fn as_mut_any(&mut self) -> &mut dyn Any;

    fn append_null(&mut self);

    fn finish(&mut self) -> Buffer;

    fn finish_cloned(&self) -> Buffer;
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

    fn finish_cloned(&self) -> Buffer {
        Buffer::from_slice_ref(self.as_slice())
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
#[derive(Debug, Default)]
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

    /// Builds this builder creating a new `UnionArray` without consuming the builder.
    ///
    /// This is used for the `finish_cloned` implementation in `ArrayBuilder`.
    fn build_cloned(&self) -> Result<UnionArray, ArrowError> {
        let mut children = Vec::with_capacity(self.fields.len());
        let union_fields: Vec<_> = self
            .fields
            .iter()
            .map(|(name, field_data)| {
                let FieldData {
                    type_id,
                    data_type,
                    values_buffer,
                    slots,
                    null_buffer_builder,
                } = field_data;

                let array_ref = make_array(unsafe {
                    ArrayDataBuilder::new(data_type.clone())
                        .add_buffer(values_buffer.finish_cloned())
                        .len(*slots)
                        .nulls(null_buffer_builder.finish_cloned())
                        .build_unchecked()
                });
                children.push(array_ref);
                (
                    *type_id,
                    Arc::new(Field::new(name.clone(), data_type.clone(), false)),
                )
            })
            .collect();
        UnionArray::try_new(
            union_fields.into_iter().collect(),
            ScalarBuffer::from(self.type_id_builder.as_slice().to_vec()),
            self.value_offset_builder
                .as_ref()
                .map(|builder| ScalarBuffer::from(builder.as_slice().to_vec())),
            children,
        )
    }
}

impl ArrayBuilder for UnionBuilder {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.len
    }

    /// Builds the array
    fn finish(&mut self) -> ArrayRef {
        // Even simpler - just move the builder using mem::take and replace with default
        let builder = std::mem::take(self);

        // Since UnionBuilder controls all invariants, this should never fail
        Arc::new(builder.build().unwrap())
    }

    /// Builds the array without resetting the underlying builder
    fn finish_cloned(&self) -> ArrayRef {
        // We construct the UnionArray carefully to ensure try_new cannot fail.
        // Since UnionBuilder controls all the invariants, this should never panic.
        Arc::new(self.build_cloned().unwrap_or_else(|err| {
            panic!("UnionBuilder::build_cloned failed unexpectedly: {}", err)
        }))
    }

    /// Returns the builder as a non-mutable `Any` reference
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Array;
    use crate::cast::AsArray;
    use crate::types::{Float64Type, Int32Type};

    #[test]
    fn test_union_builder_array_builder_trait() {
        // Test that UnionBuilder implements ArrayBuilder trait
        let mut builder = UnionBuilder::new_dense();

        // Add some data
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("b", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();

        assert_eq!(builder.len(), 3);

        // Test finish_cloned (non-destructive)
        let array1 = builder.finish_cloned();
        assert_eq!(array1.len(), 3);

        // Verify values in cloned array
        let union1 = array1.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union1.type_ids(), &[0, 1, 0]);
        assert_eq!(union1.offsets().unwrap().as_ref(), &[0, 0, 1]);
        let int_array1 = union1.child(0).as_primitive::<Int32Type>();
        let float_array1 = union1.child(1).as_primitive::<Float64Type>();
        assert_eq!(int_array1.value(0), 1);
        assert_eq!(int_array1.value(1), 4);
        assert_eq!(float_array1.value(0), 3.0);

        // Builder should still be usable after finish_cloned
        builder.append::<Float64Type>("b", 5.0).unwrap();
        assert_eq!(builder.len(), 4);

        // Test finish (destructive)
        let array2 = builder.finish();
        assert_eq!(array2.len(), 4);

        // Verify values in final array
        let union2 = array2.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union2.type_ids(), &[0, 1, 0, 1]);
        assert_eq!(union2.offsets().unwrap().as_ref(), &[0, 0, 1, 1]);
        let int_array2 = union2.child(0).as_primitive::<Int32Type>();
        let float_array2 = union2.child(1).as_primitive::<Float64Type>();
        assert_eq!(int_array2.value(0), 1);
        assert_eq!(int_array2.value(1), 4);
        assert_eq!(float_array2.value(0), 3.0);
        assert_eq!(float_array2.value(1), 5.0);
    }

    #[test]
    fn test_union_builder_type_erased() {
        // Test type-erased usage with Box<dyn ArrayBuilder>
        let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![Box::new(UnionBuilder::new_sparse())];

        // Downcast and use
        let union_builder = builders[0]
            .as_any_mut()
            .downcast_mut::<UnionBuilder>()
            .unwrap();
        union_builder.append::<Int32Type>("x", 10).unwrap();
        union_builder.append::<Float64Type>("y", 20.0).unwrap();

        assert_eq!(builders[0].len(), 2);

        let result = builders
            .into_iter()
            .map(|mut b| b.finish())
            .collect::<Vec<_>>();
        assert_eq!(result[0].len(), 2);

        // Verify sparse union values
        let union = result[0].as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids(), &[0, 1]);
        assert!(union.offsets().is_none()); // Sparse union has no offsets
        let int_array = union.child(0).as_primitive::<Int32Type>();
        let float_array = union.child(1).as_primitive::<Float64Type>();
        assert_eq!(int_array.value(0), 10);
        assert!(int_array.is_null(1)); // Null in sparse layout
        assert!(float_array.is_null(0)); // Null in sparse layout
        assert_eq!(float_array.value(1), 20.0);
    }
}
