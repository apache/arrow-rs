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

use crate::builder::null_buffer_builder::NullBufferBuilder;
use crate::builder::*;
use crate::{Array, ArrayRef, StructArray};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use std::any::Any;
use std::sync::Arc;

/// Array builder for Struct types.
///
/// Note that callers should make sure that methods of all the child field builders are
/// properly called to maintain the consistency of the data structure.
pub struct StructBuilder {
    fields: Vec<Field>,
    field_builders: Vec<Box<dyn ArrayBuilder>>,
    null_buffer_builder: NullBufferBuilder,
}

impl std::fmt::Debug for StructBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructBuilder")
            .field("fields", &self.fields)
            .field("bitmap_builder", &self.null_buffer_builder)
            .field("len", &self.len())
            .finish()
    }
}

impl ArrayBuilder for StructBuilder {
    /// Returns the number of array slots in the builder.
    ///
    /// Note that this always return the first child field builder's length, and it is
    /// the caller's responsibility to maintain the consistency that all the child field
    /// builder should have the equal number of elements.
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Builds the array.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Returns a builder with capacity `capacity` that corresponds to the datatype `DataType`
/// This function is useful to construct arrays from an arbitrary vectors with known/expected
/// schema.
pub fn make_builder(datatype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    use crate::builder::*;
    match datatype {
        DataType::Null => unimplemented!(),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 1024)),
        DataType::FixedSizeBinary(len) => {
            Box::new(FixedSizeBinaryBuilder::with_capacity(capacity, *len))
        }
        DataType::Decimal128(_precision, _scale) => {
            Box::new(Decimal128Builder::with_capacity(capacity))
        }
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 1024)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Date64 => Box::new(Date64Builder::with_capacity(capacity)),
        DataType::Time32(TimeUnit::Second) => {
            Box::new(Time32SecondBuilder::with_capacity(capacity))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Box::new(Time32MillisecondBuilder::with_capacity(capacity))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Box::new(Time64MicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Box::new(Time64NanosecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Box::new(TimestampSecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(IntervalYearMonthBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(IntervalDayTimeBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Box::new(IntervalMonthDayNanoBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Second) => {
            Box::new(DurationSecondBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Box::new(DurationMillisecondBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Box::new(DurationMicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Box::new(DurationNanosecondBuilder::with_capacity(capacity))
        }
        DataType::Struct(fields) => {
            Box::new(StructBuilder::from_fields(fields.clone(), capacity))
        }
        t => panic!("Data type {:?} is not currently supported", t),
    }
}

impl StructBuilder {
    /// Creates a new `StructBuilder`
    pub fn new(fields: Vec<Field>, field_builders: Vec<Box<dyn ArrayBuilder>>) -> Self {
        Self {
            fields,
            field_builders,
            null_buffer_builder: NullBufferBuilder::new(0),
        }
    }

    /// Creates a new `StructBuilder` from vector of [`Field`] with `capacity`
    pub fn from_fields(fields: Vec<Field>, capacity: usize) -> Self {
        let mut builders = Vec::with_capacity(fields.len());
        for field in &fields {
            builders.push(make_builder(field.data_type(), capacity));
        }
        Self::new(fields, builders)
    }

    /// Returns a mutable reference to the child field builder at index `i`.
    /// Result will be `None` if the input type `T` provided doesn't match the actual
    /// field builder's type.
    pub fn field_builder<T: ArrayBuilder>(&mut self, i: usize) -> Option<&mut T> {
        self.field_builders[i].as_any_mut().downcast_mut::<T>()
    }

    /// Returns the number of fields for the struct this builder is building.
    pub fn num_fields(&self) -> usize {
        self.field_builders.len()
    }

    /// Appends an element (either null or non-null) to the struct. The actual elements
    /// should be appended for each child sub-array in a consistent way.
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);
    }

    /// Appends a null element to the struct.
    #[inline]
    pub fn append_null(&mut self) {
        self.append(false)
    }

    /// Builds the `StructArray` and reset this builder.
    pub fn finish(&mut self) -> StructArray {
        self.validate_content();

        let mut child_data = Vec::with_capacity(self.field_builders.len());
        for f in &mut self.field_builders {
            let arr = f.finish();
            child_data.push(arr.data().clone());
        }
        let length = self.len();
        let null_bit_buffer = self.null_buffer_builder.finish();

        let builder = ArrayData::builder(DataType::Struct(self.fields.clone()))
            .len(length)
            .child_data(child_data)
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        StructArray::from(array_data)
    }

    /// Builds the `StructArray` without resetting the builder.
    pub fn finish_cloned(&self) -> StructArray {
        self.validate_content();

        let mut child_data = Vec::with_capacity(self.field_builders.len());
        for f in &self.field_builders {
            let arr = f.finish_cloned();
            child_data.push(arr.data().clone());
        }
        let length = self.len();
        let null_bit_buffer = self
            .null_buffer_builder
            .as_slice()
            .map(Buffer::from_slice_ref);

        let builder = ArrayData::builder(DataType::Struct(self.fields.clone()))
            .len(length)
            .child_data(child_data)
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { builder.build_unchecked() };
        StructArray::from(array_data)
    }

    /// Constructs and validates contents in the builder to ensure that
    /// - fields and field_builders are of equal length
    /// - the number of items in individual field_builders are equal to self.len()
    fn validate_content(&self) {
        if self.fields.len() != self.field_builders.len() {
            panic!("Number of fields is not equal to the number of field_builders.");
        }
        if !self.field_builders.iter().all(|x| x.len() == self.len()) {
            panic!("StructBuilder and field_builders are of unequal lengths.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::Buffer;
    use arrow_data::Bitmap;

    use crate::array::Array;

    #[test]
    fn test_struct_array_builder() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Utf8, false));
        field_builders.push(Box::new(string_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert_eq!(2, builder.num_fields());

        let string_builder = builder
            .field_builder::<StringBuilder>(0)
            .expect("builder at field 0 should be string builder");
        string_builder.append_value("joe");
        string_builder.append_null();
        string_builder.append_null();
        string_builder.append_value("mark");

        let int_builder = builder
            .field_builder::<Int32Builder>(1)
            .expect("builder at field 1 should be int builder");
        int_builder.append_value(1);
        int_builder.append_value(2);
        int_builder.append_null();
        int_builder.append_value(4);

        builder.append(true);
        builder.append(true);
        builder.append_null();
        builder.append(true);

        let arr = builder.finish();

        let struct_data = arr.data();
        assert_eq!(4, struct_data.len());
        assert_eq!(1, struct_data.null_count());
        assert_eq!(
            Some(&Bitmap::from(Buffer::from(&[11_u8]))),
            struct_data.null_bitmap()
        );

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(&[9_u8])))
            .add_buffer(Buffer::from_slice_ref([0, 3, 3, 3, 7]))
            .add_buffer(Buffer::from_slice_ref(b"joemark"))
            .build()
            .unwrap();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_bit_buffer(Some(Buffer::from_slice_ref([11_u8])))
            .add_buffer(Buffer::from_slice_ref([1, 2, 0, 4]))
            .build()
            .unwrap();

        assert_eq!(expected_string_data, *arr.column(0).data());
        assert_eq!(expected_int_data, *arr.column(1).data());
    }

    #[test]
    fn test_struct_array_builder_finish() {
        let int_builder = Int32Builder::new();
        let bool_builder = BooleanBuilder::new();

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Boolean, false));
        field_builders.push(Box::new(bool_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[
                false, true, false, true, false, true, false, true, false, true,
            ]);

        // Append slot values - all are valid.
        for _ in 0..10 {
            builder.append(true);
        }

        assert_eq!(10, builder.len());

        let arr = builder.finish();

        assert_eq!(10, arr.len());
        assert_eq!(0, builder.len());

        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[1, 3, 5, 7, 9]);
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[false, true, false, true, false]);

        // Append slot values - all are valid.
        for _ in 0..5 {
            builder.append(true);
        }

        assert_eq!(5, builder.len());

        let arr = builder.finish();

        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_struct_array_builder_finish_cloned() {
        let int_builder = Int32Builder::new();
        let bool_builder = BooleanBuilder::new();

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Boolean, false));
        field_builders.push(Box::new(bool_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[
                false, true, false, true, false, true, false, true, false, true,
            ]);

        // Append slot values - all are valid.
        for _ in 0..10 {
            builder.append(true);
        }

        assert_eq!(10, builder.len());

        let mut arr = builder.finish_cloned();

        assert_eq!(10, arr.len());
        assert_eq!(10, builder.len());

        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[1, 3, 5, 7, 9]);
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[false, true, false, true, false]);

        // Append slot values - all are valid.
        for _ in 0..5 {
            builder.append(true);
        }

        assert_eq!(15, builder.len());

        arr = builder.finish();

        assert_eq!(15, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_struct_array_builder_from_schema() {
        let mut fields = vec![
            Field::new("f1", DataType::Float32, false),
            Field::new("f2", DataType::Utf8, false),
        ];
        let sub_fields = vec![
            Field::new("g1", DataType::Int32, false),
            Field::new("g2", DataType::Boolean, false),
        ];
        let struct_type = DataType::Struct(sub_fields);
        fields.push(Field::new("f3", struct_type, false));

        let mut builder = StructBuilder::from_fields(fields, 5);
        assert_eq!(3, builder.num_fields());
        assert!(builder.field_builder::<Float32Builder>(0).is_some());
        assert!(builder.field_builder::<StringBuilder>(1).is_some());
        assert!(builder.field_builder::<StructBuilder>(2).is_some());
    }

    #[test]
    #[should_panic(
        expected = "Data type List(Field { name: \"item\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }) is not currently supported"
    )]
    fn test_struct_array_builder_from_schema_unsupported_type() {
        let mut fields = vec![Field::new("f1", DataType::Int16, false)];
        let list_type =
            DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
        fields.push(Field::new("f2", list_type, false));

        let _ = StructBuilder::from_fields(fields, 5);
    }

    #[test]
    fn test_struct_array_builder_field_builder_type_mismatch() {
        let int_builder = Int32Builder::with_capacity(10);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert!(builder.field_builder::<BinaryBuilder>(0).is_none());
    }

    #[test]
    #[should_panic(expected = "StructBuilder and field_builders are of unequal lengths.")]
    fn test_struct_array_builder_unequal_field_builders_lengths() {
        let mut int_builder = Int32Builder::with_capacity(10);
        let mut bool_builder = BooleanBuilder::new();

        int_builder.append_value(1);
        int_builder.append_value(2);
        bool_builder.append_value(true);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Boolean, false));
        field_builders.push(Box::new(bool_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        builder.append(true);
        builder.append(true);
        builder.finish();
    }

    #[test]
    #[should_panic(
        expected = "Number of fields is not equal to the number of field_builders."
    )]
    fn test_struct_array_builder_unequal_field_field_builders() {
        let int_builder = Int32Builder::with_capacity(10);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Boolean, false));

        let mut builder = StructBuilder::new(fields, field_builders);
        builder.finish();
    }
}
