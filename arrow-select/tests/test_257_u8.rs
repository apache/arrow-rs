use arrow_array::{DictionaryArray, Int32Array, UInt8Array};
use arrow_array::types::UInt8Type;
use arrow_select::concat::concat;
use std::sync::Arc;

fn build_dict(start: u32, count: u32) -> DictionaryArray<UInt8Type> {
    let values: Vec<i32> = (start..start + count).map(|v| v as i32).collect();
    let values_array = Int32Array::from(values);
    let keys = UInt8Array::from_iter_values(0..count as u8);
    DictionaryArray::try_new(keys, Arc::new(values_array)).unwrap()
}

#[test]
fn test_257_values_u8_should_fail() {
    let a = build_dict(0, 128);
    let b = build_dict(128, 129); // 257 distinct values

    let result = concat(&[&a, &b]);

    assert!(result.is_err(), "257 distinct values must fail for u8");
}
