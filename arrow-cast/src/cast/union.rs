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

//! Cast support for union arrays.

use crate::cast::can_cast_types;
use crate::cast_with_options;
use arrow_array::{Array, ArrayRef, UnionArray};
use arrow_schema::{ArrowError, DataType, FieldRef, UnionFields};
use arrow_select::union_extract::union_extract_by_id;

use super::CastOptions;

// this is used during child array selection to prefer a "close" type over a distant cast
// for example: when targeting Utf8View, a Utf8 child is preferred over Int32 despite both being castable
fn same_type_family(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    matches!(
        (a, b),
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
            | (
                Binary | LargeBinary | BinaryView,
                Binary | LargeBinary | BinaryView
            )
            | (Int8 | Int16 | Int32 | Int64, Int8 | Int16 | Int32 | Int64)
            | (
                UInt8 | UInt16 | UInt32 | UInt64,
                UInt8 | UInt16 | UInt32 | UInt64
            )
            | (Float16 | Float32 | Float64, Float16 | Float32 | Float64)
    )
}

/// Selects the best-matching child array from a [`UnionArray`] for a given target type
///
/// The goal is to find the source field whose type is closest to the target,
/// so that the subsequent cast is as lossless as possible. The heuristic uses
/// three passes with decreasing specificity:
///
/// 1. **Exact match**: field type equals the target type.
/// 2. **Same type family**: field and target belong to the same logical family
///    (e.g. `Utf8` and `Utf8View` are both strings). This avoids a greedy
///    cross-family cast in pass 3 (e.g. picking `Int32` over `Utf8` when the
///    target is `Utf8View`, since `can_cast_types(Int32, Utf8View)` is true)
/// 3. **Castable**:`can_cast_types` reports the field can be cast to the target
///    Nested target types are skipped here because union extraction introduces
///    nulls, which can conflict with non-nullable inner fields
///
/// Each pass greedily picks the first matching field by type_id order
pub(crate) fn resolve_child_array<'a>(
    fields: &'a UnionFields,
    target_type: &DataType,
) -> Option<(i8, &'a FieldRef)> {
    fields
        .iter()
        .find(|(_, f)| f.data_type() == target_type)
        .or_else(|| {
            fields
                .iter()
                .find(|(_, f)| same_type_family(f.data_type(), target_type))
        })
        .or_else(|| {
            // skip nested types in pass 3 because union extraction introduces nulls,
            // and casting nullable arrays to nested types like List/Struct/Map can fail
            // when inner fields are non-nullable.
            if target_type.is_nested() {
                return None;
            }
            fields
                .iter()
                .find(|(_, f)| can_cast_types(f.data_type(), target_type))
        })
}

/// Extracts the best-matching child array from a [`UnionArray`] for a given target type,
/// and casts it to that type.
///
/// Rows where a different child array is active become NULL.
/// If no child array matches, returns an error.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_schema::{DataType, Field, UnionFields};
/// # use arrow_array::{UnionArray, StringArray, Int32Array, Array};
/// # use arrow_cast::cast::union_extract_by_type;
/// # use arrow_cast::CastOptions;
/// let fields = UnionFields::try_new(
///     [0, 1],
///     [
///         Field::new("int", DataType::Int32, true),
///         Field::new("str", DataType::Utf8, true),
///     ],
/// ).unwrap();
///
/// let union = UnionArray::try_new(
///     fields,
///     vec![0, 1, 0].into(),
///     None,
///     vec![
///         Arc::new(Int32Array::from(vec![Some(42), None, Some(99)])),
///         Arc::new(StringArray::from(vec![None, Some("hello"), None])),
///     ],
/// )
/// .unwrap();
///
/// // extract the Utf8 child array and cast to Utf8View
/// let result = union_extract_by_type(&union, &DataType::Utf8View, &CastOptions::default()).unwrap();
/// assert_eq!(result.data_type(), &DataType::Utf8View);
/// assert!(result.is_null(0));   // Int32 row -> NULL
/// assert!(!result.is_null(1));  // Utf8 row -> "hello"
/// assert!(result.is_null(2));   // Int32 row -> NULL
/// ```
pub fn union_extract_by_type(
    union_array: &UnionArray,
    target_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let fields = match union_array.data_type() {
        DataType::Union(fields, _) => fields,
        _ => unreachable!("union_extract_by_type called on non-union array"),
    };

    let Some((type_id, _)) = resolve_child_array(fields, target_type) else {
        return Err(ArrowError::CastError(format!(
            "cannot cast Union with fields {} to {}",
            fields
                .iter()
                .map(|(_, f)| f.data_type().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            target_type
        )));
    };

    let extracted = union_extract_by_id(union_array, type_id)?;

    if extracted.data_type() == target_type {
        return Ok(extracted);
    }

    cast_with_options(&extracted, target_type, cast_options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast;
    use arrow_array::*;
    use arrow_schema::{Field, UnionFields, UnionMode};
    use std::sync::Arc;

    fn int_str_fields() -> UnionFields {
        UnionFields::try_new(
            [0, 1],
            [
                Field::new("int", DataType::Int32, true),
                Field::new("str", DataType::Utf8, true),
            ],
        )
        .unwrap()
    }

    fn int_str_union_type(mode: UnionMode) -> DataType {
        DataType::Union(int_str_fields(), mode)
    }

    // pass 1: exact type match.
    // Union(Int32, Utf8) targeting Utf8. The Utf8 child matches exactly.
    // Int32 rows become NULL. tested for both sparse and dense.
    #[test]
    fn test_exact_type_match() {
        let target = DataType::Utf8;

        // sparse
        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        let sparse = UnionArray::try_new(
            int_str_fields(),
            vec![1_i8, 0, 1].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(42), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
            ],
        )
        .unwrap();

        let result = cast::cast(&sparse, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hello");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "world");

        // dense
        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Dense),
            &target
        ));

        let dense = UnionArray::try_new(
            int_str_fields(),
            vec![1_i8, 0, 1].into(),
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )
        .unwrap();

        let result = cast::cast(&dense, &target).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hello");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "world");
    }

    // pass 2: same type family match.
    // Union(Int32, Utf8) targeting Utf8View. No exact match, but Utf8 and Utf8View
    // are in the same family. picks the Utf8 child array and casts to Utf8View.
    // this is the bug that motivated this work: without pass 2, pass 3 would
    // greedily pick Int32 (since can_cast_types(Int32, Utf8View) is true).
    #[test]
    fn test_same_family_utf8_to_utf8view() {
        let target = DataType::Utf8View;

        // sparse
        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        let sparse = UnionArray::try_new(
            int_str_fields(),
            vec![1_i8, 0, 1, 1].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(42), None, None])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("agent_alpha"),
                    None,
                    Some("agent_beta"),
                    None,
                ])),
            ],
        )
        .unwrap();

        let result = cast::cast(&sparse, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let arr = result.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(arr.value(0), "agent_alpha");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "agent_beta");
        assert!(arr.is_null(3));

        // dense
        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Dense),
            &target
        ));

        let dense = UnionArray::try_new(
            int_str_fields(),
            vec![1_i8, 0, 1].into(),
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])),
            ],
        )
        .unwrap();

        let result = cast::cast(&dense, &target).unwrap();
        let arr = result.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(arr.value(0), "alpha");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "beta");
    }

    // pass 3: one-directional cast across type families.
    // Union(Int32, Utf8) targeting Boolean — no exact match, no family match.
    // pass 3 picks Int32 (first child array where can_cast_types is true) and
    // casts to Boolean (0 → false, nonzero → true). Utf8 rows become NULL.
    #[test]
    fn test_one_directional_cast() {
        let target = DataType::Boolean;

        // sparse
        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        let sparse = UnionArray::try_new(
            int_str_fields(),
            vec![0_i8, 1, 0].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(42), None, Some(0)])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("hello"), None])),
            ],
        )
        .unwrap();

        let result = cast::cast(&sparse, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(arr.is_null(1));
        assert!(!arr.value(2));

        // dense
        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Dense),
            &target
        ));

        let dense = UnionArray::try_new(
            int_str_fields(),
            vec![0_i8, 1, 0].into(),
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![Some(42), Some(0)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("hello")])),
            ],
        )
        .unwrap();

        let result = cast::cast(&dense, &target).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(arr.is_null(1));
        assert!(!arr.value(2));
    }

    // duplicate field names: ensure we resolve by type_id, not field name.
    // Union has two children both named "val" — Int32 (type_id 0) and Utf8 (type_id 1).
    // Casting to Utf8 should select the Utf8 child (type_id 1), not the Int32 child (type_id 0).
    #[test]
    fn test_duplicate_field_names() {
        let fields = UnionFields::try_new(
            [0, 1],
            [
                Field::new("val", DataType::Int32, true),
                Field::new("val", DataType::Utf8, true),
            ],
        )
        .unwrap();

        let target = DataType::Utf8;

        let sparse = UnionArray::try_new(
            fields.clone(),
            vec![0_i8, 1, 0, 1].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(42), None, Some(99), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    None,
                    Some("hello"),
                    None,
                    Some("world"),
                ])),
            ],
        )
        .unwrap();

        let result = cast::cast(&sparse, &target).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), "hello");
        assert!(arr.is_null(2));
        assert_eq!(arr.value(3), "world");

        let dense = UnionArray::try_new(
            fields,
            vec![0_i8, 1, 1].into(),
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )
        .unwrap();

        let result = cast::cast(&dense, &target).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), "hello");
        assert_eq!(arr.value(2), "world");
    }

    // no matching child array, all three passes fail.
    // Union(Int32, Utf8) targeting Struct({x: Int32}). neither Int32 nor Utf8
    // can be cast to a Struct, so both can_cast_types and cast return errors.
    #[test]
    fn test_no_match_errors() {
        let target = DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into());

        assert!(!can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        let union = UnionArray::try_new(
            int_str_fields(),
            vec![0_i8, 1].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(42), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("hello")])),
            ],
        )
        .unwrap();

        assert!(cast::cast(&union, &target).is_err());
    }

    // priority: exact match (pass 1) wins over family match (pass 2).
    // Union(Utf8, Utf8View) targeting Utf8View. Both child arrays are in the string
    // family, but Utf8View is an exact match. pass 1 should pick it, not Utf8.
    #[test]
    fn test_exact_match_preferred_over_family() {
        let fields = UnionFields::try_new(
            [0, 1],
            [
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8View, true),
            ],
        )
        .unwrap();
        let target = DataType::Utf8View;

        assert!(can_cast_types(
            &DataType::Union(fields.clone(), UnionMode::Sparse),
            &target,
        ));

        // [Utf8("from_a"), Utf8View("from_b"), Utf8("also_a")]
        let union = UnionArray::try_new(
            fields,
            vec![0_i8, 1, 0].into(),
            None,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("from_a"),
                    None,
                    Some("also_a"),
                ])) as ArrayRef,
                Arc::new(StringViewArray::from(vec![None, Some("from_b"), None])),
            ],
        )
        .unwrap();

        let result = cast::cast(&union, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        let arr = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        // pass 1 picks child "b" (Utf8View), so child "a" rows become NULL
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), "from_b");
        assert!(arr.is_null(2));
    }

    // null values within the selected child array stay null.
    // this is distinct from "wrong child array -> NULL": here the correct child array
    // is active but its value is null.
    #[test]
    fn test_null_in_selected_child_array() {
        let target = DataType::Utf8;

        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        // ["hello", NULL(str), "world"]
        // all rows are the Utf8 child array, but row 1 has a null value
        let union = UnionArray::try_new(
            int_str_fields(),
            vec![1_i8, 1, 1].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
            ],
        )
        .unwrap();

        let result = cast::cast(&union, &target).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hello");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "world");
    }

    // empty union array returns a zero-length result of the target type.
    #[test]
    fn test_empty_union() {
        let target = DataType::Utf8View;

        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        let union = UnionArray::try_new(
            int_str_fields(),
            Vec::<i8>::new().into(),
            None,
            vec![
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())) as ArrayRef,
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .unwrap();

        let result = cast::cast(&union, &target).unwrap();
        assert_eq!(result.data_type(), &target);
        assert_eq!(result.len(), 0);
    }
}
