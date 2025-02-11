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

use crate::StructArray;
use crate::{
    builder::*,
    types::{Int16Type, Int32Type, Int64Type, Int8Type},
};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{DataType, Fields, IntervalUnit, SchemaBuilder, TimeUnit};
use std::sync::Arc;

/// Builder for [`StructArray`]
///
/// Note that callers should make sure that methods of all the child field builders are
/// properly called to maintain the consistency of the data structure.
///
///
/// Handling arrays with complex layouts, such as `List<Struct<List<Struct>>>`, in Rust can be challenging due to its strong typing system.
/// To construct a collection builder ([`ListBuilder`], [`LargeListBuilder`], or [`MapBuilder`]) using [`make_builder`], multiple calls are required. This complexity arises from the recursive approach utilized by [`StructBuilder::from_fields`].
///
/// Initially, [`StructBuilder::from_fields`] invokes [`make_builder`], which returns a `Box<dyn ArrayBuilder>`. To obtain the specific collection builder, one must first use [`StructBuilder::field_builder`] to get a `Collection<[Box<dyn ArrayBuilder>]>`. Subsequently, the `values()` result from this operation can be downcast to the desired builder type.
///
/// For example, when working with [`ListBuilder`], you would first call [`StructBuilder::field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>`] and then downcast the [`Box<dyn ArrayBuilder>`] to the specific [`StructBuilder`] you need.
///
/// For a practical example see the code below:
///
/// ```rust
///    use arrow_array::builder::{ArrayBuilder, ListBuilder, StringBuilder, StructBuilder};
///    use arrow_schema::{DataType, Field, Fields};
///    use std::sync::Arc;
///
///    // This is an example column that has a List<Struct<List<Struct>>> layout
///    let mut example_col = ListBuilder::new(StructBuilder::from_fields(
///        vec![Field::new(
///            "value_list",
///            DataType::List(Arc::new(Field::new_list_field(
///                DataType::Struct(Fields::from(vec![
///                    Field::new("key", DataType::Utf8, true),
///                    Field::new("value", DataType::Utf8, true),
///                ])), //In this example we are trying to get to this builder and insert key/value pairs
///                true,
///            ))),
///            true,
///        )],
///        0,
///    ));
///
///   // We can obtain the StructBuilder without issues, because example_col was created with StructBuilder
///   let col_struct_builder: &mut StructBuilder = example_col.values();
///
///   // We can't obtain the ListBuilder<StructBuilder> with the expected generic types, because under the hood
///   // the StructBuilder was returned as a Box<dyn ArrayBuilder> and passed as such to the ListBuilder constructor
///   
///   // This panics in runtime, even though we know that the builder is a ListBuilder<StructBuilder>.
///   // let sb = col_struct_builder
///   //     .field_builder::<ListBuilder<StructBuilder>>(0)
///   //     .as_mut()
///   //     .unwrap();
///
///   //To keep in line with Rust's strong typing, we fetch a ListBuilder<Box<dyn ArrayBuilder>> from the column StructBuilder first...
///   let mut list_builder_option =
///       col_struct_builder.field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(0);
///
///   let list_builder = list_builder_option.as_mut().unwrap();
///
///   // ... and then downcast the key/value pair values to a StructBuilder
///   let struct_builder = list_builder
///       .values()
///       .as_any_mut()
///       .downcast_mut::<StructBuilder>()
///       .unwrap();
///
///   // We can now append values to the StructBuilder
///   let key_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
///   key_builder.append_value("my key");
///
///   let value_builder = struct_builder.field_builder::<StringBuilder>(1).unwrap();
///   value_builder.append_value("my value");
///
///   struct_builder.append(true);
///   list_builder.append(true);
///   col_struct_builder.append(true);
///   example_col.append(true);
///
///   let array = example_col.finish();
///
///   println!("My array: {:?}", array);
/// ```
///
pub struct StructBuilder {
    fields: Fields,
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

/// Returns a builder with capacity for `capacity` elements of datatype
/// `DataType`.
///
/// This function is useful to construct arrays from an arbitrary vectors with
/// known/expected schema.
///
/// See comments on [StructBuilder] for retrieving collection builders built by
/// make_builder.
pub fn make_builder(datatype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    use crate::builder::*;
    match datatype {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float16 => Box::new(Float16Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 1024)),
        DataType::LargeBinary => Box::new(LargeBinaryBuilder::with_capacity(capacity, 1024)),
        DataType::FixedSizeBinary(len) => {
            Box::new(FixedSizeBinaryBuilder::with_capacity(capacity, *len))
        }
        DataType::Decimal128(p, s) => Box::new(
            Decimal128Builder::with_capacity(capacity).with_data_type(DataType::Decimal128(*p, *s)),
        ),
        DataType::Decimal256(p, s) => Box::new(
            Decimal256Builder::with_capacity(capacity).with_data_type(DataType::Decimal256(*p, *s)),
        ),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 1024)),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, 1024)),
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
        DataType::Timestamp(TimeUnit::Second, tz) => Box::new(
            TimestampSecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Second, tz.clone())),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => Box::new(
            TimestampMillisecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, tz.clone())),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Box::new(
            TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Box::new(
            TimestampNanosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())),
        ),
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
        DataType::List(field) => {
            let builder = make_builder(field.data_type(), capacity);
            Box::new(ListBuilder::with_capacity(builder, capacity).with_field(field.clone()))
        }
        DataType::LargeList(field) => {
            let builder = make_builder(field.data_type(), capacity);
            Box::new(LargeListBuilder::with_capacity(builder, capacity).with_field(field.clone()))
        }
        DataType::FixedSizeList(field, size) => {
            let size = *size;
            let values_builder_capacity = {
                let size: usize = size.try_into().unwrap();
                capacity * size
            };
            let builder = make_builder(field.data_type(), values_builder_capacity);
            Box::new(
                FixedSizeListBuilder::with_capacity(builder, size, capacity)
                    .with_field(field.clone()),
            )
        }
        DataType::ListView(field) => {
            let builder = make_builder(field.data_type(), capacity);
            Box::new(ListViewBuilder::with_capacity(builder, capacity).with_field(field.clone()))
        }
        DataType::LargeListView(field) => {
            let builder = make_builder(field.data_type(), capacity);
            Box::new(
                LargeListViewBuilder::with_capacity(builder, capacity).with_field(field.clone()),
            )
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(fields) => {
                let map_field_names = MapFieldNames {
                    key: fields[0].name().clone(),
                    value: fields[1].name().clone(),
                    entry: field.name().clone(),
                };
                let key_builder = make_builder(fields[0].data_type(), capacity);
                let value_builder = make_builder(fields[1].data_type(), capacity);
                Box::new(
                    MapBuilder::with_capacity(
                        Some(map_field_names),
                        key_builder,
                        value_builder,
                        capacity,
                    )
                    .with_keys_field(fields[0].clone())
                    .with_values_field(fields[1].clone()),
                )
            }
            t => panic!("The field of Map data type {t:?} should have a child Struct field"),
        },
        DataType::Struct(fields) => Box::new(StructBuilder::from_fields(fields.clone(), capacity)),
        t @ DataType::Dictionary(key_type, value_type) => {
            macro_rules! dict_builder {
                ($key_type:ty) => {
                    match &**value_type {
                        DataType::Utf8 => {
                            let dict_builder: StringDictionaryBuilder<$key_type> =
                                StringDictionaryBuilder::with_capacity(capacity, 256, 1024);
                            Box::new(dict_builder)
                        }
                        DataType::LargeUtf8 => {
                            let dict_builder: LargeStringDictionaryBuilder<$key_type> =
                                LargeStringDictionaryBuilder::with_capacity(capacity, 256, 1024);
                            Box::new(dict_builder)
                        }
                        DataType::Binary => {
                            let dict_builder: BinaryDictionaryBuilder<$key_type> =
                                BinaryDictionaryBuilder::with_capacity(capacity, 256, 1024);
                            Box::new(dict_builder)
                        }
                        DataType::LargeBinary => {
                            let dict_builder: LargeBinaryDictionaryBuilder<$key_type> =
                                LargeBinaryDictionaryBuilder::with_capacity(capacity, 256, 1024);
                            Box::new(dict_builder)
                        }
                        t => panic!("Dictionary value type {t:?} is not currently supported"),
                    }
                };
            }
            match &**key_type {
                DataType::Int8 => dict_builder!(Int8Type),
                DataType::Int16 => dict_builder!(Int16Type),
                DataType::Int32 => dict_builder!(Int32Type),
                DataType::Int64 => dict_builder!(Int64Type),
                _ => {
                    panic!("Data type {t:?} with key type {key_type:?} is not currently supported")
                }
            }
        }
        t => panic!("Data type {t:?} is not currently supported"),
    }
}

impl StructBuilder {
    /// Creates a new `StructBuilder`
    pub fn new(fields: impl Into<Fields>, field_builders: Vec<Box<dyn ArrayBuilder>>) -> Self {
        Self {
            field_builders,
            fields: fields.into(),
            null_buffer_builder: NullBufferBuilder::new(0),
        }
    }

    /// Creates a new `StructBuilder` from [`Fields`] and `capacity`
    pub fn from_fields(fields: impl Into<Fields>, capacity: usize) -> Self {
        let fields = fields.into();
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
        if self.fields.is_empty() {
            return StructArray::new_empty_fields(self.len(), self.null_buffer_builder.finish());
        }

        let arrays = self.field_builders.iter_mut().map(|f| f.finish()).collect();
        let nulls = self.null_buffer_builder.finish();
        StructArray::new(self.fields.clone(), arrays, nulls)
    }

    /// Builds the `StructArray` without resetting the builder.
    pub fn finish_cloned(&self) -> StructArray {
        self.validate_content();

        if self.fields.is_empty() {
            return StructArray::new_empty_fields(
                self.len(),
                self.null_buffer_builder.finish_cloned(),
            );
        }

        let arrays = self
            .field_builders
            .iter()
            .map(|f| f.finish_cloned())
            .collect();

        let nulls = self.null_buffer_builder.finish_cloned();

        StructArray::new(self.fields.clone(), arrays, nulls)
    }

    /// Constructs and validates contents in the builder to ensure that
    /// - fields and field_builders are of equal length
    /// - the number of items in individual field_builders are equal to self.len()
    fn validate_content(&self) {
        if self.fields.len() != self.field_builders.len() {
            panic!("Number of fields is not equal to the number of field_builders.");
        }
        self.field_builders.iter().enumerate().for_each(|(idx, x)| {
            if x.len() != self.len() {
                let builder = SchemaBuilder::from(&self.fields);
                let schema = builder.finish();

                panic!("{}", format!(
                    "StructBuilder ({:?}) and field_builder with index {} ({:?}) are of unequal lengths: ({} != {}).",
                    schema,
                    idx,
                    self.fields[idx].data_type(),
                    self.len(),
                    x.len()
                ));
            }
        });
    }

    /// Returns the current null buffer as a slice
    pub fn validity_slice(&self) -> Option<&[u8]> {
        self.null_buffer_builder.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::any::type_name;

    use super::*;
    use arrow_buffer::Buffer;
    use arrow_data::ArrayData;
    use arrow_schema::Field;

    use crate::{array::Array, types::ArrowDictionaryKeyType};

    #[test]
    fn test_struct_array_builder() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();

        let fields = vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ];
        let field_builders = vec![
            Box::new(string_builder) as Box<dyn ArrayBuilder>,
            Box::new(int_builder) as Box<dyn ArrayBuilder>,
        ];

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

        let struct_data = builder.finish().into_data();

        assert_eq!(4, struct_data.len());
        assert_eq!(1, struct_data.null_count());
        assert_eq!(&[11_u8], struct_data.nulls().unwrap().validity());

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

        assert_eq!(expected_string_data, struct_data.child_data()[0]);
        assert_eq!(expected_int_data, struct_data.child_data()[1]);
    }

    #[test]
    fn test_struct_array_builder_finish() {
        let int_builder = Int32Builder::new();
        let bool_builder = BooleanBuilder::new();

        let fields = vec![
            Field::new("f1", DataType::Int32, false),
            Field::new("f2", DataType::Boolean, false),
        ];
        let field_builders = vec![
            Box::new(int_builder) as Box<dyn ArrayBuilder>,
            Box::new(bool_builder) as Box<dyn ArrayBuilder>,
        ];

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
    fn test_build_fixed_size_list() {
        const LIST_LENGTH: i32 = 4;
        let fixed_size_list_dtype =
            DataType::new_fixed_size_list(DataType::Int32, LIST_LENGTH, false);
        let mut builder = make_builder(&fixed_size_list_dtype, 10);
        let builder = builder
            .as_any_mut()
            .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>();
        match builder {
            Some(builder) => {
                assert_eq!(builder.value_length(), LIST_LENGTH);
                assert!(builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .is_some());
            }
            None => panic!("expected FixedSizeListBuilder, got a different builder type"),
        }
    }

    #[test]
    fn test_struct_array_builder_finish_cloned() {
        let int_builder = Int32Builder::new();
        let bool_builder = BooleanBuilder::new();

        let fields = vec![
            Field::new("f1", DataType::Int32, false),
            Field::new("f2", DataType::Boolean, false),
        ];
        let field_builders = vec![
            Box::new(int_builder) as Box<dyn ArrayBuilder>,
            Box::new(bool_builder) as Box<dyn ArrayBuilder>,
        ];

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
        let struct_type = DataType::Struct(sub_fields.into());
        fields.push(Field::new("f3", struct_type, false));

        let mut builder = StructBuilder::from_fields(fields, 5);
        assert_eq!(3, builder.num_fields());
        assert!(builder.field_builder::<Float32Builder>(0).is_some());
        assert!(builder.field_builder::<StringBuilder>(1).is_some());
        assert!(builder.field_builder::<StructBuilder>(2).is_some());
    }

    #[test]
    fn test_datatype_properties() {
        let fields = Fields::from(vec![
            Field::new("f1", DataType::Decimal128(1, 2), false),
            Field::new(
                "f2",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                false,
            ),
        ]);
        let mut builder = StructBuilder::from_fields(fields.clone(), 1);
        builder
            .field_builder::<Decimal128Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .field_builder::<TimestampMillisecondBuilder>(1)
            .unwrap()
            .append_value(1);
        builder.append(true);
        let array = builder.finish();

        assert_eq!(array.data_type(), &DataType::Struct(fields.clone()));
        assert_eq!(array.column(0).data_type(), fields[0].data_type());
        assert_eq!(array.column(1).data_type(), fields[1].data_type());
    }

    #[test]
    fn test_struct_array_builder_from_dictionary_type_int8_key() {
        test_struct_array_builder_from_dictionary_type_inner::<Int8Type>(DataType::Int8);
    }

    #[test]
    fn test_struct_array_builder_from_dictionary_type_int16_key() {
        test_struct_array_builder_from_dictionary_type_inner::<Int16Type>(DataType::Int16);
    }

    #[test]
    fn test_struct_array_builder_from_dictionary_type_int32_key() {
        test_struct_array_builder_from_dictionary_type_inner::<Int32Type>(DataType::Int32);
    }

    #[test]
    fn test_struct_array_builder_from_dictionary_type_int64_key() {
        test_struct_array_builder_from_dictionary_type_inner::<Int64Type>(DataType::Int64);
    }

    fn test_struct_array_builder_from_dictionary_type_inner<K: ArrowDictionaryKeyType>(
        key_type: DataType,
    ) {
        let dict_field = Field::new(
            "f1",
            DataType::Dictionary(Box::new(key_type), Box::new(DataType::Utf8)),
            false,
        );
        let fields = vec![dict_field.clone()];
        let expected_dtype = DataType::Struct(fields.into());
        let cloned_dict_field = dict_field.clone();
        let expected_child_dtype = dict_field.data_type();
        let mut struct_builder = StructBuilder::from_fields(vec![cloned_dict_field], 5);
        let Some(dict_builder) = struct_builder.field_builder::<StringDictionaryBuilder<K>>(0)
        else {
            panic!(
                "Builder should be StringDictionaryBuilder<{}>",
                type_name::<K>()
            )
        };
        dict_builder.append_value("dict string");
        struct_builder.append(true);
        let array = struct_builder.finish();

        assert_eq!(array.data_type(), &expected_dtype);
        assert_eq!(array.column(0).data_type(), expected_child_dtype);
        assert_eq!(array.column(0).len(), 1);
    }

    #[test]
    #[should_panic(
        expected = "Data type Dictionary(UInt64, Utf8) with key type UInt64 is not currently supported"
    )]
    fn test_struct_array_builder_from_schema_unsupported_type() {
        let fields = vec![
            Field::new("f1", DataType::UInt64, false),
            Field::new(
                "f2",
                DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                false,
            ),
        ];

        let _ = StructBuilder::from_fields(fields, 5);
    }

    #[test]
    #[should_panic(expected = "Dictionary value type Int32 is not currently supported")]
    fn test_struct_array_builder_from_dict_with_unsupported_value_type() {
        let fields = vec![Field::new(
            "f1",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            false,
        )];

        let _ = StructBuilder::from_fields(fields, 5);
    }

    #[test]
    fn test_struct_array_builder_field_builder_type_mismatch() {
        let int_builder = Int32Builder::with_capacity(10);

        let fields = vec![Field::new("f1", DataType::Int32, false)];
        let field_builders = vec![Box::new(int_builder) as Box<dyn ArrayBuilder>];

        let mut builder = StructBuilder::new(fields, field_builders);
        assert!(builder.field_builder::<BinaryBuilder>(0).is_none());
    }

    #[test]
    #[should_panic(
        expected = "StructBuilder (Schema { fields: [Field { name: \"f1\", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: \"f2\", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} }) and field_builder with index 1 (Boolean) are of unequal lengths: (2 != 1)."
    )]
    fn test_struct_array_builder_unequal_field_builders_lengths() {
        let mut int_builder = Int32Builder::with_capacity(10);
        let mut bool_builder = BooleanBuilder::new();

        int_builder.append_value(1);
        int_builder.append_value(2);
        bool_builder.append_value(true);

        let fields = vec![
            Field::new("f1", DataType::Int32, false),
            Field::new("f2", DataType::Boolean, false),
        ];
        let field_builders = vec![
            Box::new(int_builder) as Box<dyn ArrayBuilder>,
            Box::new(bool_builder) as Box<dyn ArrayBuilder>,
        ];

        let mut builder = StructBuilder::new(fields, field_builders);
        builder.append(true);
        builder.append(true);
        builder.finish();
    }

    #[test]
    #[should_panic(expected = "Number of fields is not equal to the number of field_builders.")]
    fn test_struct_array_builder_unequal_field_field_builders() {
        let int_builder = Int32Builder::with_capacity(10);

        let fields = vec![
            Field::new("f1", DataType::Int32, false),
            Field::new("f2", DataType::Boolean, false),
        ];
        let field_builders = vec![Box::new(int_builder) as Box<dyn ArrayBuilder>];

        let mut builder = StructBuilder::new(fields, field_builders);
        builder.finish();
    }

    #[test]
    #[should_panic(
        expected = "Incorrect datatype for StructArray field \\\"timestamp\\\", expected Timestamp(Nanosecond, Some(\\\"UTC\\\")) got Timestamp(Nanosecond, None)"
    )]
    fn test_struct_array_mismatch_builder() {
        let fields = vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned().into())),
            false,
        )];

        let field_builders: Vec<Box<dyn ArrayBuilder>> =
            vec![Box::new(TimestampNanosecondBuilder::new())];

        let mut sa = StructBuilder::new(fields, field_builders);
        sa.finish();
    }

    #[test]
    fn test_empty() {
        let mut builder = StructBuilder::new(Fields::empty(), vec![]);
        builder.append(true);
        builder.append(false);

        let a1 = builder.finish_cloned();
        let a2 = builder.finish();
        assert_eq!(a1, a2);
        assert_eq!(a1.len(), 2);
        assert_eq!(a1.null_count(), 1);
        assert!(a1.is_valid(0));
        assert!(a1.is_null(1));
    }
}
