// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_schema::{DataType, Field};

/// Returns true if `a` and `b` hold the same logical values and differ at most
/// in physical layout.
///
/// Dictionary and run-end encoded types compare as their value type. String,
/// binary and variable-size list types compare equal within their family,
/// whatever their offset width or view layout. Decimals compare by precision
/// and scale, whatever their width. Nested types compare their children the
/// same way, by field name, nullability and logical type. Every other type
/// compares with [`DataType::equals_datatype`].
pub fn logically_equal(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        (Dictionary(_, a), b) | (b, Dictionary(_, a)) => logically_equal(a, b),
        (RunEndEncoded(_, a), b) | (b, RunEndEncoded(_, a)) => logically_equal(a.data_type(), b),
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View) => true,
        (Binary | LargeBinary | BinaryView, Binary | LargeBinary | BinaryView) => true,
        (
            Decimal32(pa, sa) | Decimal64(pa, sa) | Decimal128(pa, sa) | Decimal256(pa, sa),
            Decimal32(pb, sb) | Decimal64(pb, sb) | Decimal128(pb, sb) | Decimal256(pb, sb),
        ) => pa == pb && sa == sb,
        (
            List(a) | LargeList(a) | ListView(a) | LargeListView(a),
            List(b) | LargeList(b) | ListView(b) | LargeListView(b),
        ) => fields_logically_equal(a, b, false),
        (FixedSizeList(a, na), FixedSizeList(b, nb)) => {
            na == nb && fields_logically_equal(a, b, false)
        }
        (Struct(a), Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(a, b)| fields_logically_equal(a, b, true))
        }
        (Map(a, sorted_a), Map(b, sorted_b)) => {
            sorted_a == sorted_b && fields_logically_equal(a, b, false)
        }
        _ => a.equals_datatype(b),
    }
}

fn fields_logically_equal(a: &Field, b: &Field, compare_names: bool) -> bool {
    (!compare_names || a.name() == b.name())
        && a.is_nullable() == b.is_nullable()
        && logically_equal(a.data_type(), b.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Fields;
    use std::sync::Arc;

    fn dict(value: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(value))
    }

    fn ree(value: DataType) -> DataType {
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", value, true)),
        )
    }

    fn list(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(item, true)))
    }

    fn large_list_view(item: DataType) -> DataType {
        DataType::LargeListView(Arc::new(Field::new("element", item, true)))
    }

    fn strukt(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn equal_across_physical_layouts() {
        use DataType::*;
        let pairs = [
            (Utf8, Utf8View),
            (LargeUtf8, dict(Utf8View)),
            (Binary, LargeBinary),
            (Float64, ree(Float64)),
            (dict(Int32), ree(Int32)),
            (Decimal32(9, 2), Decimal256(9, 2)),
            (list(Utf8), large_list_view(dict(Utf8View))),
            (
                strukt(vec![("a", Utf8), ("b", Decimal128(5, 1))]),
                strukt(vec![("a", dict(LargeUtf8)), ("b", Decimal64(5, 1))]),
            ),
        ];
        for (a, b) in pairs {
            assert!(logically_equal(&a, &b), "{a} vs {b}");
            assert!(logically_equal(&b, &a), "{b} vs {a}");
        }
    }

    #[test]
    fn different_logical_types() {
        use DataType::*;
        let pairs = [
            (Utf8, Binary),
            (Float32, Float64),
            (Int32, dict(Int64)),
            (Decimal128(9, 2), Decimal128(9, 3)),
            (Decimal128(9, 2), Decimal128(10, 2)),
            (list(Utf8), list(Binary)),
            (
                list(Utf8),
                FixedSizeList(Arc::new(Field::new_list_field(Utf8, true)), 2),
            ),
            (strukt(vec![("a", Utf8)]), strukt(vec![("b", Utf8)])),
            (
                Timestamp(arrow_schema::TimeUnit::Second, None),
                Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            ),
        ];
        for (a, b) in pairs {
            assert!(!logically_equal(&a, &b), "{a} vs {b}");
            assert!(!logically_equal(&b, &a), "{b} vs {a}");
        }
    }
}
