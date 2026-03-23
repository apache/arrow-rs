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
use arrow_array::{Array, ArrayRef, UnionArray, new_null_array};
use arrow_schema::{ArrowError, DataType, FieldRef, UnionFields};
use arrow_select::union_extract::union_extract;

use super::CastOptions;

// this is used during variant selection to prefer a "close" type over a distant cast
// for example: when targeting Utf8View, a Utf8 variant is preferred over Int32 despite both being castable
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

// variant selection heuristic — 3 passes with decreasing specificity:
//
// first pass: field type == target type
// second pass: field and target are in the same equivalence class
//              (e.g., Utf8 and Utf8View are both strings)
// third pass: field can be cast to target
//      note: this is the most permissive and may lose information
//      also, the matching logic is greedy so it will pick the first 'castable' variant
//
// each pass picks the first matching variant by type_id order.
pub(crate) fn resolve_variant<'a>(
    fields: &'a UnionFields,
    target_type: &DataType,
) -> Option<&'a FieldRef> {
    fields
        .iter()
        .find(|(_, f)| f.data_type() == target_type)
        .or_else(|| {
            fields
                .iter()
                .find(|(_, f)| same_type_family(f.data_type(), target_type))
        })
        .or_else(|| {
            fields
                .iter()
                .find(|(_, f)| can_cast_types(f.data_type(), target_type))
        })
        .map(|(_, f)| f)
}

/// Extracts the best-matching variant from a union array for a given target type,
/// and casts it to that type.
///
/// Rows where a different variant is active become NULL.
/// If no variant matches, returns a null array.
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
/// // extract the Utf8 variant and cast to Utf8View
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

    let Some(field) = resolve_variant(fields, target_type) else {
        return Ok(new_null_array(target_type, union_array.len()));
    };

    let extracted = union_extract(union_array, field.name())?;

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
    // Union(Int32, Utf8) targeting Utf8 — the Utf8 variant matches exactly.
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
    // Union(Int32, Utf8) targeting Utf8View — no exact match, but Utf8 and Utf8View
    // are in the same family. picks the Utf8 variant and casts to Utf8View.
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
    // pass 3 picks Int32 (first variant where can_cast_types is true) and
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

    // no matching variant — all three passes fail.
    // Union(Int32, Utf8) targeting Struct({x: Int32}). neither Int32 nor Utf8
    // can be cast to a Struct, so can_cast_types returns false and
    // union_extract_by_type returns an all-null array.
    #[test]
    fn test_no_match_returns_nulls() {
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

        let result = union_extract_by_type(&union, &target, &CastOptions::default()).unwrap();
        assert_eq!(result.data_type(), &target);
        assert_eq!(result.null_count(), 2);
    }

    // priority: exact match (pass 1) wins over family match (pass 2).
    // Union(Utf8, Utf8View) targeting Utf8View — both variants are in the string
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

        // pass 1 picks variant "b" (Utf8View), so variant "a" rows become NULL
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), "from_b");
        assert!(arr.is_null(2));
    }

    // null values within the selected variant stay null.
    // this is distinct from "wrong variant → NULL": here the correct variant
    // is active but its value is null.
    #[test]
    fn test_null_in_selected_variant() {
        let target = DataType::Utf8;

        assert!(can_cast_types(
            &int_str_union_type(UnionMode::Sparse),
            &target
        ));

        // ["hello", NULL(str), "world"]
        // all rows are the Utf8 variant, but row 1 has a null value
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
