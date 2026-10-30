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

use arrow_array::{Array, Int32Array, LargeStringArray, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;

#[test]
fn test_cast_to_strings() {
    let a = Int32Array::from(vec![1, 2, 3]);
    let out = cast(&a, &DataType::Utf8).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
    let out = cast(&a, &DataType::LargeUtf8).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
}

#[test]
fn test_str_to_str_casts() {
    for data in [
        vec![Some("foo"), Some("bar"), Some("ham")],
        vec![Some("foo"), None, Some("bar")],
    ] {
        let a = LargeStringArray::from(data.clone());
        let to = cast(&a, &DataType::Utf8).unwrap();
        let expect = a
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        let out = to
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(expect, out);

        let a = StringArray::from(data);
        let to = cast(&a, &DataType::LargeUtf8).unwrap();
        let expect = a
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        let out = to
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(expect, out);
    }
}

#[test]
fn test_utf8_cast_offsets() {
    // test if offset of the array is taken into account during cast
    let str_array = StringArray::from(vec!["a", "b", "c"]);
    let str_array = str_array.slice(1, 2);

    let out = cast(&str_array, &DataType::LargeUtf8).unwrap();

    let large_str_array = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
    let strs = large_str_array.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(strs, &["b", "c"])
}
