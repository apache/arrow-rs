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

use crate::{ArrowError, DataType, Field, FieldRef, SchemaBuilder};

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
        fn filter_field<F: FnMut(&FieldRef) -> bool>(
            f: &FieldRef,
            filter: &mut F,
        ) -> Option<FieldRef> {
            use DataType::*;

            let v = match f.data_type() {
                Dictionary(_, v) => v.as_ref(),       // Key must be integer
                RunEndEncoded(_, v) => v.data_type(), // Run-ends must be integer
                d => d,
            };
            let d = match v {
                List(child) => List(filter_field(child, filter)?),
                LargeList(child) => LargeList(filter_field(child, filter)?),
                Map(child, ordered) => Map(filter_field(child, filter)?, *ordered),
                FixedSizeList(child, size) => FixedSizeList(filter_field(child, filter)?, *size),
                Struct(fields) => {
                    let filtered: Fields = fields
                        .iter()
                        .filter_map(|f| filter_field(f, filter))
                        .collect();

                    if filtered.is_empty() {
                        return None;
                    }

                    Struct(filtered)
                }
                Union(fields, mode) => {
                    let filtered: UnionFields = fields
                        .iter()
                        .filter_map(|(id, f)| Some((id, filter_field(f, filter)?)))
                        .collect();

                    if filtered.is_empty() {
                        return None;
                    }

                    Union(filtered, *mode)
                }
                _ => return filter(f).then(|| f.clone()),
            };
            let d = match f.data_type() {
                Dictionary(k, _) => Dictionary(k.clone(), Box::new(d)),
                RunEndEncoded(v, f) => {
                    RunEndEncoded(v.clone(), Arc::new(f.as_ref().clone().with_data_type(d)))
                }
                _ => d,
            };
            Some(Arc::new(f.as_ref().clone().with_data_type(d)))
        }

        let mut leaf_idx = 0;
        let mut filter = |f: &FieldRef| {
            let t = filter(leaf_idx, f);
            leaf_idx += 1;
            t
        };

        self.0
            .iter()
            .filter_map(|f| filter_field(f, &mut filter))
            .collect()
    }

    /// Remove a field by index and return it.
    ///
    /// # Panic
    ///
    /// Panics if `index` is out of bounds.
    ///
    /// # Example
    /// ```
    /// use arrow_schema::{DataType, Field, Fields};
    /// let mut fields = Fields::from(vec![
    ///   Field::new("a", DataType::Boolean, false),
    ///   Field::new("b", DataType::Int8, false),
    ///   Field::new("c", DataType::Utf8, false),
    /// ]);
    /// assert_eq!(fields.len(), 3);
    /// assert_eq!(fields.remove(1), Field::new("b", DataType::Int8, false).into());
    /// assert_eq!(fields.len(), 2);
    /// ```
    #[deprecated(note = "Use SchemaBuilder::remove")]
    #[doc(hidden)]
    pub fn remove(&mut self, index: usize) -> FieldRef {
        let mut builder = SchemaBuilder::from(Fields::from(&*self.0));
        let field = builder.remove(index);
        *self = builder.finish().fields;
        field
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

impl UnionFields {
    /// Create a new [`UnionFields`] with no fields
    pub fn empty() -> Self {
        Self(Arc::from([]))
    }

    /// Create a new [`UnionFields`] from a [`Fields`] and array of type_ids
    ///
    /// See <https://arrow.apache.org/docs/format/Columnar.html#union-layout>
    ///
    /// ```
    /// use arrow_schema::{DataType, Field, UnionFields};
    /// // Create a new UnionFields with type id mapping
    /// // 1 -> DataType::UInt8
    /// // 3 -> DataType::Utf8
    /// UnionFields::new(
    ///     vec![1, 3],
    ///     vec![
    ///         Field::new("field1", DataType::UInt8, false),
    ///         Field::new("field3", DataType::Utf8, false),
    ///     ],
    /// );
    /// ```
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
            .map(|idx| {
                let mask = 1_u128 << idx;
                if (set & mask) != 0 {
                    panic!("duplicate type id: {}", idx);
                } else {
                    set |= mask;
                }
                idx
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
    ///
    /// Note: the iteration order is not guaranteed
    pub fn iter(&self) -> impl Iterator<Item = (i8, &FieldRef)> + '_ {
        self.0.iter().map(|(id, f)| (*id, f))
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
                        return Err(ArrowError::SchemaError(
                            format!("Fail to merge schema field '{}' because the self_type_id = {} does not equal field_type_id = {}",
                                    self_field.name(), self_type_id, field_type_id)
                        ));
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
        // TODO: Should this validate type IDs are unique (#3982)
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
            Field::new(
                "f",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3),
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
                    UnionFields::new(
                        vec![1, 3],
                        vec![
                            Field::new("field1", DataType::UInt8, false),
                            Field::new("field3", DataType::Utf8, false),
                        ],
                    ),
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
            UnionFields::new(vec![1], vec![Field::new("field1", DataType::UInt8, false)]),
            UnionMode::Dense,
        );

        let r = fields.filter_leaves(|idx, _| idx == 12);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].data_type(), &union);

        let r = fields.filter_leaves(|idx, _| idx == 14 || idx == 15);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], fields[9]);
    }
}
