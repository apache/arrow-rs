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

use std::ops::Deref;
use std::sync::Arc;

use crate::{ArrowError, DataType, Field, FieldRef};

/// A cheaply cloneable, owned slice of [`FieldRef`]
///
/// Similar to `Arc<Vec<FieldRef>>` or `Arc<[FieldRef]>`
///
/// Can be constructed in a number of ways
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, Fields, SchemaBuilder};
/// // Can be constructed from Vec<Field>
/// Fields::from(vec![Field::new("a", DataType::Boolean, false)]);
/// // Can be constructed from Vec<FieldRef>
/// Fields::from(vec![Arc::new(Field::new("a", DataType::Boolean, false))]);
/// // Can be constructed from an iterator of Field
/// std::iter::once(Field::new("a", DataType::Boolean, false)).collect::<Fields>();
/// // Can be constructed from an iterator of FieldRef
/// std::iter::once(Arc::new(Field::new("a", DataType::Boolean, false))).collect::<Fields>();
/// ```
///
/// See [`SchemaBuilder`] for mutating or updating [`Fields`]
///
/// ```
/// # use arrow_schema::{DataType, Field, SchemaBuilder};
/// let mut builder = SchemaBuilder::new();
/// builder.push(Field::new("a", DataType::Boolean, false));
/// builder.push(Field::new("b", DataType::Boolean, false));
/// let fields = builder.finish().fields;
///
/// let mut builder = SchemaBuilder::from(&fields);
/// builder.remove(0);
/// let new = builder.finish().fields;
/// ```
///
/// [`SchemaBuilder`]: crate::SchemaBuilder
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Fields(Arc<[FieldRef]>);

impl std::fmt::Debug for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl Fields {
    /// Returns a new empty [`Fields`]
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }

    /// Return size of this instance in bytes.
    pub fn size(&self) -> usize {
        self.iter()
            .map(|field| field.size() + std::mem::size_of::<FieldRef>())
            .sum()
    }

    /// Searches for a field by name, returning it along with its index if found
    pub fn find(&self, name: &str) -> Option<(usize, &FieldRef)> {
        self.0.iter().enumerate().find(|(_, b)| b.name() == name)
    }

    /// Check to see if `self` is a superset of `other`
    ///
    /// In particular returns true if both have the same number of fields, and [`Field::contains`]
    /// for each field across self and other
    ///
    /// In other words, any record that conforms to `other` should also conform to `self`
    pub fn contains(&self, other: &Fields) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(a, b)| Arc::ptr_eq(a, b) || a.contains(b))
    }

    /// Returns a copy of this [`Fields`] containing only those [`FieldRef`] passing a predicate
    ///
    /// Performs a depth-first scan of [`Fields`] invoking `filter` for each [`FieldRef`]
    /// containing no child [`FieldRef`], a leaf field, along with a count of the number
    /// of such leaves encountered so far. Only [`FieldRef`] for which `filter`
    /// returned `true` will be included in the result.
    ///
    /// This can therefore be used to select a subset of fields from nested types
    /// such as [`DataType::Struct`] or [`DataType::List`].
    ///
    /// ```
    /// # use arrow_schema::{DataType, Field, Fields};
    /// let fields = Fields::from(vec![
    ///     Field::new("a", DataType::Int32, true), // Leaf 0
    ///     Field::new("b", DataType::Struct(Fields::from(vec![
    ///         Field::new("c", DataType::Float32, false), // Leaf 1
    ///         Field::new("d", DataType::Float64, false), // Leaf 2
    ///         Field::new("e", DataType::Struct(Fields::from(vec![
    ///             Field::new("f", DataType::Int32, false),   // Leaf 3
    ///             Field::new("g", DataType::Float16, false), // Leaf 4
    ///         ])), true),
    ///     ])), false)
    /// ]);
    /// let filtered = fields.filter_leaves(|idx, _| [0, 2, 3, 4].contains(&idx));
    /// let expected = Fields::from(vec![
    ///     Field::new("a", DataType::Int32, true),
    ///     Field::new("b", DataType::Struct(Fields::from(vec![
    ///         Field::new("d", DataType::Float64, false),
    ///         Field::new("e", DataType::Struct(Fields::from(vec![
    ///             Field::new("f", DataType::Int32, false),
    ///             Field::new("g", DataType::Float16, false),
    ///         ])), true),
    ///     ])), false)
    /// ]);
    /// assert_eq!(filtered, expected);
    /// ```
    pub fn filter_leaves<F: FnMut(usize, &FieldRef) -> bool>(&self, mut filter: F) -> Self {
        self.try_filter_leaves(|idx, field| Ok(filter(idx, field)))
            .unwrap()
    }

    /// Returns a copy of this [`Fields`] containing only those [`FieldRef`] passing a predicate
    /// or an error if the predicate fails.
    ///
    /// See [`Fields::filter_leaves`] for more information.
    pub fn try_filter_leaves<F: FnMut(usize, &FieldRef) -> Result<bool, ArrowError>>(
        &self,
        mut filter: F,
    ) -> Result<Self, ArrowError> {
        fn filter_field<F: FnMut(&FieldRef) -> Result<bool, ArrowError>>(
            f: &FieldRef,
            filter: &mut F,
        ) -> Result<Option<FieldRef>, ArrowError> {
            use DataType::*;

            let v = match f.data_type() {
                Dictionary(_, v) => v.as_ref(),       // Key must be integer
                RunEndEncoded(_, v) => v.data_type(), // Run-ends must be integer
                d => d,
            };
            let d = match v {
                List(child) => {
                    let fields = filter_field(child, filter)?;
                    if let Some(fields) = fields {
                        List(fields)
                    } else {
                        return Ok(None);
                    }
                }
                LargeList(child) => {
                    let fields = filter_field(child, filter)?;
                    if let Some(fields) = fields {
                        LargeList(fields)
                    } else {
                        return Ok(None);
                    }
                }
                Map(child, ordered) => {
                    let fields = filter_field(child, filter)?;
                    if let Some(fields) = fields {
                        Map(fields, *ordered)
                    } else {
                        return Ok(None);
                    }
                }
                FixedSizeList(child, size) => {
                    let fields = filter_field(child, filter)?;
                    if let Some(fields) = fields {
                        FixedSizeList(fields, *size)
                    } else {
                        return Ok(None);
                    }
                }
                Struct(fields) => {
                    let filtered: Result<Vec<_>, _> =
                        fields.iter().map(|f| filter_field(f, filter)).collect();
                    let filtered: Fields = filtered?
                        .iter()
                        .filter_map(|f| f.as_ref().cloned())
                        .collect();

                    if filtered.is_empty() {
                        return Ok(None);
                    }

                    Struct(filtered)
                }
                Union(fields, mode) => {
                    let filtered: Result<Vec<_>, _> = fields
                        .iter()
                        .map(|(id, f)| filter_field(f, filter).map(|f| f.map(|f| (id, f))))
                        .collect();
                    let filtered: UnionFields = filtered?
                        .iter()
                        .filter_map(|f| f.as_ref().cloned())
                        .collect();

                    if filtered.is_empty() {
                        return Ok(None);
                    }

                    Union(filtered, *mode)
                }
                _ => {
                    let filtered = filter(f)?;
                    return Ok(filtered.then(|| f.clone()));
                }
            };
            let d = match f.data_type() {
                Dictionary(k, _) => Dictionary(k.clone(), Box::new(d)),
                RunEndEncoded(v, f) => {
                    RunEndEncoded(v.clone(), Arc::new(f.as_ref().clone().with_data_type(d)))
                }
                _ => d,
            };
            Ok(Some(Arc::new(f.as_ref().clone().with_data_type(d))))
        }

        let mut leaf_idx = 0;
        let mut filter = |f: &FieldRef| {
            let t = filter(leaf_idx, f)?;
            leaf_idx += 1;
            Ok(t)
        };

        let filtered: Result<Vec<_>, _> = self
            .0
            .iter()
            .map(|f| filter_field(f, &mut filter))
            .collect();
        let filtered = filtered?
            .iter()
            .filter_map(|f| f.as_ref().cloned())
            .collect();
        Ok(filtered)
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Field>> for Fields {
    fn from(value: Vec<Field>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(value: Vec<FieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<&[FieldRef]> for Fields {
    fn from(value: &[FieldRef]) -> Self {
        Self(value.into())
    }
}

impl<const N: usize> From<[FieldRef; N]> for Fields {
    fn from(value: [FieldRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for Fields {
    type Target = [FieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = &'a FieldRef;
    type IntoIter = std::slice::Iter<'a, FieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A cheaply cloneable, owned collection of [`FieldRef`] and their corresponding type ids
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct UnionFields(Arc<[(i8, FieldRef)]>);

impl std::fmt::Debug for UnionFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

/// Allows direct indexing into [`UnionFields`] to access fields by position.
///
/// # Panics
///
/// Panics if the index is out of bounds. Note that [`UnionFields`] supports
/// a maximum of 128 fields, as type IDs are represented as `i8` values.
///
/// For a non-panicking alternative, use [`UnionFields::get`].
impl std::ops::Index<usize> for UnionFields {
    type Output = (i8, FieldRef);

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl UnionFields {
    /// Create a new [`UnionFields`] with no fields
    pub fn empty() -> Self {
        Self(Arc::from([]))
    }

    /// Create a new [`UnionFields`] from a [`Fields`] and array of type_ids
    ///
    /// See <https://arrow.apache.org/docs/format/Columnar.html#union-layout>
    ///
    /// # Errors
    ///
    /// This function returns an error if:
    /// - Any type_id appears more than once (duplicate type ids)
    /// - The type_ids are duplicated
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    /// // Create a new UnionFields with type id mapping
    /// // 1 -> DataType::UInt8
    /// // 3 -> DataType::Utf8
    /// let result = UnionFields::try_new(
    ///     vec![1, 3],
    ///     vec![
    ///         Field::new("field1", DataType::UInt8, false),
    ///         Field::new("field3", DataType::Utf8, false),
    ///     ],
    /// );
    /// assert!(result.is_ok());
    ///
    /// // This will fail due to duplicate type ids
    /// let result = UnionFields::try_new(
    ///     vec![1, 1],
    ///     vec![
    ///         Field::new("field1", DataType::UInt8, false),
    ///         Field::new("field2", DataType::Utf8, false),
    ///     ],
    /// );
    /// assert!(result.is_err());
    /// ```
    pub fn try_new<F, T>(type_ids: T, fields: F) -> Result<Self, ArrowError>
    where
        F: IntoIterator,
        F::Item: Into<FieldRef>,
        T: IntoIterator<Item = i8>,
    {
        let mut type_ids_iter = type_ids.into_iter();
        let mut fields_iter = fields.into_iter().map(Into::into);

        let mut seen_type_ids = 0u128;

        let mut out = Vec::new();

        loop {
            match (type_ids_iter.next(), fields_iter.next()) {
                (None, None) => return Ok(Self(out.into())),
                (Some(type_id), Some(field)) => {
                    // check type id is non-negative
                    if type_id < 0 {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "type ids must be non-negative: {type_id}"
                        )));
                    }

                    // check type id uniqueness
                    let mask = 1_u128 << type_id;
                    if (seen_type_ids & mask) != 0 {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "duplicate type id: {type_id}"
                        )));
                    }

                    seen_type_ids |= mask;

                    out.push((type_id, field));
                }
                (None, Some(_)) => {
                    return Err(ArrowError::InvalidArgumentError(
                        "fields iterator has more elements than type_ids iterator".to_string(),
                    ));
                }
                (Some(_), None) => {
                    return Err(ArrowError::InvalidArgumentError(
                        "type_ids iterator has more elements than fields iterator".to_string(),
                    ));
                }
            }
        }
    }

    /// Create a new [`UnionFields`] from a collection of fields with automatically
    /// assigned type IDs starting from 0.
    ///
    /// The type IDs are assigned in increasing order: 0, 1, 2, 3, etc.
    ///
    /// See <https://arrow.apache.org/docs/format/Columnar.html#union-layout>
    ///
    /// # Panics
    ///
    /// Panics if the number of fields exceeds 127 (the maximum value for i8 type IDs).
    ///
    /// If you want to avoid panics, use [`UnionFields::try_from_fields`] instead, which
    /// returns a `Result`.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    /// // Create a new UnionFields with automatic type id assignment
    /// // 0 -> DataType::UInt8
    /// // 1 -> DataType::Utf8
    /// let union_fields = UnionFields::from_fields(vec![
    ///     Field::new("field1", DataType::UInt8, false),
    ///     Field::new("field2", DataType::Utf8, false),
    /// ]);
    /// assert_eq!(union_fields.len(), 2);
    /// ```
    pub fn from_fields<F>(fields: F) -> Self
    where
        F: IntoIterator,
        F::Item: Into<FieldRef>,
    {
        fields
            .into_iter()
            .enumerate()
            .map(|(i, field)| {
                let id = i8::try_from(i).expect("UnionFields cannot contain more than 128 fields");

                (id, field.into())
            })
            .collect()
    }

    /// Create a new [`UnionFields`] from a collection of fields with automatically
    /// assigned type IDs starting from 0.
    ///
    /// The type IDs are assigned in increasing order: 0, 1, 2, 3, etc.
    ///
    /// This is the non-panicking version of [`UnionFields::from_fields`].
    ///
    /// See <https://arrow.apache.org/docs/format/Columnar.html#union-layout>
    ///
    /// # Errors
    ///
    /// Returns an error if the number of fields exceeds 127 (the maximum value for i8 type IDs).
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    /// // Create a new UnionFields with automatic type id assignment
    /// // 0 -> DataType::UInt8
    /// // 1 -> DataType::Utf8
    /// let result = UnionFields::try_from_fields(vec![
    ///     Field::new("field1", DataType::UInt8, false),
    ///     Field::new("field2", DataType::Utf8, false),
    /// ]);
    /// assert!(result.is_ok());
    /// assert_eq!(result.unwrap().len(), 2);
    ///
    /// // This will fail with too many fields
    /// let many_fields: Vec<_> = (0..200)
    ///     .map(|i| Field::new(format!("field{}", i), DataType::Int32, false))
    ///     .collect();
    /// let result = UnionFields::try_from_fields(many_fields);
    /// assert!(result.is_err());
    /// ```
    pub fn try_from_fields<F>(fields: F) -> Result<Self, ArrowError>
    where
        F: IntoIterator,
        F::Item: Into<FieldRef>,
    {
        let mut out = Vec::with_capacity(i8::MAX as usize + 1);

        for (i, field) in fields.into_iter().enumerate() {
            let id = i8::try_from(i).map_err(|_| {
                ArrowError::InvalidArgumentError(
                    "UnionFields cannot contain more than 128 fields".into(),
                )
            })?;

            out.push((id, field.into()));
        }

        Ok(Self(out.into()))
    }

    /// Create a new [`UnionFields`] from a [`Fields`] and array of type_ids
    ///
    /// See <https://arrow.apache.org/docs/format/Columnar.html#union-layout>
    ///
    /// # Deprecated
    ///
    /// Use [`UnionFields::try_new`] instead. This method panics on invalid input,
    /// while `try_new` returns a `Result`.
    ///
    /// # Panics
    ///
    /// Panics if any type_id appears more than once (duplicate type ids).
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    /// // Create a new UnionFields with type id mapping
    /// // 1 -> DataType::UInt8
    /// // 3 -> DataType::Utf8
    /// UnionFields::try_new(
    ///     vec![1, 3],
    ///     vec![
    ///         Field::new("field1", DataType::UInt8, false),
    ///         Field::new("field3", DataType::Utf8, false),
    ///     ],
    /// );
    /// ```
    #[deprecated(since = "57.0.0", note = "Use `try_new` instead")]
    pub fn new<F, T>(type_ids: T, fields: F) -> Self
    where
        F: IntoIterator,
        F::Item: Into<FieldRef>,
        T: IntoIterator<Item = i8>,
    {
        let fields = fields.into_iter().map(Into::into);
        let mut set = 0_u128;
        type_ids
            .into_iter()
            .inspect(|&idx| {
                let mask = 1_u128 << idx;
                if (set & mask) != 0 {
                    panic!("duplicate type id: {idx}");
                } else {
                    set |= mask;
                }
            })
            .zip(fields)
            .collect()
    }

    /// Return size of this instance in bytes.
    pub fn size(&self) -> usize {
        self.iter()
            .map(|(_, field)| field.size() + std::mem::size_of::<(i8, FieldRef)>())
            .sum()
    }

    /// Returns the number of fields in this [`UnionFields`]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if this is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over the fields and type ids in this [`UnionFields`]
    pub fn iter(&self) -> impl Iterator<Item = (i8, &FieldRef)> + '_ {
        self.0.iter().map(|(id, f)| (*id, f))
    }

    /// Returns a reference to the field at the given index, or `None` if out of bounds.
    ///
    /// This is a safe alternative to direct indexing via `[]`.
    ///
    /// # Example
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    ///
    /// let fields = UnionFields::new(
    ///     vec![1, 3],
    ///     vec![
    ///         Field::new("field1", DataType::UInt8, false),
    ///         Field::new("field3", DataType::Utf8, false),
    ///     ],
    /// );
    ///
    /// assert!(fields.get(0).is_some());
    /// assert!(fields.get(1).is_some());
    /// assert!(fields.get(2).is_none());
    /// ```
    pub fn get(&self, index: usize) -> Option<&(i8, FieldRef)> {
        self.0.get(index)
    }

    /// Searches for a field by its type id, returning the type id and field reference if found.
    /// Returns `None` if no field with the given type id exists.
    pub fn find_by_type_id(&self, type_id: i8) -> Option<(i8, &FieldRef)> {
        self.iter().find(|&(i, _)| i == type_id)
    }

    /// Searches for a field by value equality, returning its type id and reference if found.
    /// Returns `None` if no matching field exists in this [`UnionFields`].
    pub fn find_by_field(&self, field: &Field) -> Option<(i8, &FieldRef)> {
        self.iter().find(|&(_, f)| f.as_ref() == field)
    }

    /// Merge this field into self if it is compatible.
    ///
    /// See [`Field::try_merge`]
    pub(crate) fn try_merge(&mut self, other: &Self) -> Result<(), ArrowError> {
        // TODO: This currently may produce duplicate type IDs (#3982)
        let mut output: Vec<_> = self.iter().map(|(id, f)| (id, f.clone())).collect();
        for (field_type_id, from_field) in other.iter() {
            let mut is_new_field = true;
            for (self_type_id, self_field) in output.iter_mut() {
                if from_field == self_field {
                    // If the nested fields in two unions are the same, they must have same
                    // type id.
                    if *self_type_id != field_type_id {
                        return Err(ArrowError::SchemaError(format!(
                            "Fail to merge schema field '{}' because the self_type_id = {} does not equal field_type_id = {}",
                            self_field.name(),
                            self_type_id,
                            field_type_id
                        )));
                    }

                    is_new_field = false;
                    break;
                }
            }

            if is_new_field {
                output.push((field_type_id, from_field.clone()))
            }
        }
        *self = output.into_iter().collect();
        Ok(())
    }
}

impl FromIterator<(i8, FieldRef)> for UnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, FieldRef)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnionMode;

    #[test]
    fn test_filter() {
        let floats = Fields::from(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float32, false),
        ]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("floats", DataType::Struct(floats.clone()), true),
            Field::new("b", DataType::Int16, true),
            Field::new(
                "c",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new(
                "d",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Struct(floats.clone())),
                ),
                false,
            ),
            Field::new_list(
                "e",
                Field::new("floats", DataType::Struct(floats.clone()), true),
                true,
            ),
            Field::new_fixed_size_list(
                "f",
                Field::new_list_field(DataType::Int32, false),
                3,
                false,
            ),
            Field::new_map(
                "g",
                "entries",
                Field::new("keys", DataType::LargeUtf8, false),
                Field::new("values", DataType::Int32, true),
                false,
                false,
            ),
            Field::new(
                "h",
                DataType::Union(
                    UnionFields::try_new(
                        vec![1, 3],
                        vec![
                            Field::new("field1", DataType::UInt8, false),
                            Field::new("field3", DataType::Utf8, false),
                        ],
                    )
                    .unwrap(),
                    UnionMode::Dense,
                ),
                true,
            ),
            Field::new(
                "i",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Struct(floats.clone()), true)),
                ),
                false,
            ),
        ]);

        let floats_a = DataType::Struct(vec![floats[0].clone()].into());

        let r = fields.filter_leaves(|idx, _| idx == 0 || idx == 1);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], fields[0]);
        assert_eq!(r[1].data_type(), &floats_a);

        let r = fields.filter_leaves(|_, f| f.name() == "a");
        assert_eq!(r.len(), 5);
        assert_eq!(r[0], fields[0]);
        assert_eq!(r[1].data_type(), &floats_a);
        assert_eq!(
            r[2].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(floats_a.clone()))
        );
        assert_eq!(
            r[3].as_ref(),
            &Field::new_list("e", Field::new("floats", floats_a.clone(), true), true)
        );
        assert_eq!(
            r[4].as_ref(),
            &Field::new(
                "i",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", floats_a.clone(), true)),
                ),
                false,
            )
        );

        let r = fields.filter_leaves(|_, f| f.name() == "floats");
        assert_eq!(r.len(), 0);

        let r = fields.filter_leaves(|idx, _| idx == 9);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], fields[6]);

        let r = fields.filter_leaves(|idx, _| idx == 10 || idx == 11);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], fields[7]);

        let union = DataType::Union(
            UnionFields::try_new(vec![1], vec![Field::new("field1", DataType::UInt8, false)])
                .unwrap(),
            UnionMode::Dense,
        );

        let r = fields.filter_leaves(|idx, _| idx == 12);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].data_type(), &union);

        let r = fields.filter_leaves(|idx, _| idx == 14 || idx == 15);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], fields[9]);

        // Propagate error
        let r = fields.try_filter_leaves(|_, _| Err(ArrowError::SchemaError("error".to_string())));
        assert!(r.is_err());
    }

    #[test]
    fn test_union_fields_try_new_valid() {
        let res = UnionFields::try_new(
            vec![1, 6, 7],
            vec![
                Field::new("f1", DataType::UInt8, false),
                Field::new("f6", DataType::Utf8, false),
                Field::new("f7", DataType::Int32, true),
            ],
        );
        assert!(res.is_ok());
        let union_fields = res.unwrap();
        assert_eq!(union_fields.len(), 3);
        assert_eq!(
            union_fields.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            vec![1, 6, 7]
        );
    }

    #[test]
    fn test_union_fields_try_new_empty() {
        let res = UnionFields::try_new(Vec::<i8>::new(), Vec::<Field>::new());
        assert!(res.is_ok());
        assert!(res.unwrap().is_empty());
    }

    #[test]
    fn test_union_fields_try_new_duplicate_type_id() {
        let res = UnionFields::try_new(
            vec![1, 1],
            vec![
                Field::new("f1", DataType::UInt8, false),
                Field::new("f2", DataType::Utf8, false),
            ],
        );
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("duplicate type id: 1")
        );
    }

    #[test]
    fn test_union_fields_try_new_duplicate_field() {
        let field = Field::new("field", DataType::UInt8, false);
        let res = UnionFields::try_new(vec![1, 2], vec![field.clone(), field]);
        assert!(res.is_ok());
    }

    #[test]
    fn test_union_fields_try_new_more_type_ids() {
        let res = UnionFields::try_new(
            vec![1, 2, 3],
            vec![
                Field::new("f1", DataType::UInt8, false),
                Field::new("f2", DataType::Utf8, false),
            ],
        );
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("type_ids iterator has more elements")
        );
    }

    #[test]
    fn test_union_fields_try_new_more_fields() {
        let res = UnionFields::try_new(
            vec![1, 2],
            vec![
                Field::new("f1", DataType::UInt8, false),
                Field::new("f2", DataType::Utf8, false),
                Field::new("f3", DataType::Int32, true),
            ],
        );
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("fields iterator has more elements")
        );
    }

    #[test]
    fn test_union_fields_try_new_negative_type_ids() {
        let res = UnionFields::try_new(
            vec![-128, -1, 0, 127],
            vec![
                Field::new("field_min", DataType::UInt8, false),
                Field::new("field_neg", DataType::Utf8, false),
                Field::new("field_zero", DataType::Int32, true),
                Field::new("field_max", DataType::Boolean, false),
            ],
        );
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("type ids must be non-negative")
        )
    }

    #[test]
    fn test_union_fields_try_new_complex_types() {
        let res = UnionFields::try_new(
            vec![0, 1, 2],
            vec![
                Field::new(
                    "struct_field",
                    DataType::Struct(Fields::from(vec![
                        Field::new("a", DataType::Int32, false),
                        Field::new("b", DataType::Utf8, true),
                    ])),
                    false,
                ),
                Field::new_list(
                    "list_field",
                    Field::new("item", DataType::Float64, true),
                    true,
                ),
                Field::new(
                    "dict_field",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
            ],
        );
        assert!(res.is_ok());
        assert_eq!(res.unwrap().len(), 3);
    }

    #[test]
    fn test_union_fields_try_new_single_field() {
        let res = UnionFields::try_new(
            vec![42],
            vec![Field::new("only_field", DataType::Int64, false)],
        );
        assert!(res.is_ok());
        let union_fields = res.unwrap();
        assert_eq!(union_fields.len(), 1);
        assert_eq!(union_fields.iter().next().unwrap().0, 42);
    }

    #[test]
    fn test_union_fields_try_from_fields_empty() {
        let res = UnionFields::try_from_fields(Vec::<Field>::new());
        assert!(res.is_ok());
        assert!(res.unwrap().is_empty());
    }

    #[test]
    fn test_union_fields_try_from_fields_single() {
        let res = UnionFields::try_from_fields(vec![Field::new("only", DataType::Int64, false)]);
        assert!(res.is_ok());
        let union_fields = res.unwrap();
        assert_eq!(union_fields.len(), 1);
        assert_eq!(union_fields.iter().next().unwrap().0, 0);
    }

    #[test]
    fn test_union_fields_try_from_fields_too_many() {
        let many_fields: Vec<_> = (0..200)
            .map(|i| Field::new(format!("field{}", i), DataType::Int32, false))
            .collect();
        let res = UnionFields::try_from_fields(many_fields);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("UnionFields cannot contain more than 128 fields")
        );
    }

    #[test]
    fn test_union_fields_try_from_fields_max_valid() {
        let fields: Vec<_> = (0..=i8::MAX)
            .map(|i| Field::new(format!("field{}", i), DataType::Int32, false))
            .collect();
        let res = UnionFields::try_from_fields(fields);
        assert!(res.is_ok());
        let union_fields = res.unwrap();
        assert_eq!(union_fields.len(), 128);
        assert_eq!(union_fields.iter().map(|(id, _)| id).min().unwrap(), 0);
        assert_eq!(union_fields.iter().map(|(id, _)| id).max().unwrap(), 127);
    }

    #[test]
    fn test_union_fields_try_from_fields_over_max() {
        // 129 fields should fail
        let fields: Vec<_> = (0..129)
            .map(|i| Field::new(format!("field{}", i), DataType::Int32, false))
            .collect();
        let res = UnionFields::try_from_fields(fields);
        assert!(res.is_err());
    }

    #[test]
    fn test_union_fields_try_from_fields_complex_types() {
        let res = UnionFields::try_from_fields(vec![
            Field::new(
                "struct_field",
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Utf8, true),
                ])),
                false,
            ),
            Field::new_list(
                "list_field",
                Field::new("item", DataType::Float64, true),
                true,
            ),
            Field::new(
                "dict_field",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().len(), 3);
    }
}
