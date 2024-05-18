use std::sync::Arc;

use arrow::array::{Array, ArrayAccessor, ArrayIter, ArrayRef, Int32Array, StringArray, StructArray, TypedStructArray};


extern crate arrow_array_derive;

#[derive(Clone, Copy, Debug, arrow_array_derive::Struct)]
struct NestedStructArray<'a> {
    inner: TypedStructArray<'a, SimpleStructArray<'a>>
}

#[derive(Clone, Copy, Debug, arrow_array_derive::Struct)]
struct SimpleStructArray<'a> {
    text: &'a StringArray,
    number: &'a Int32Array,
}

fn main() {

    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("joe"),
        None,
        None,
        Some("mark"),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

    let inner_arr: ArrayRef = Arc::new(StructArray::try_from(vec![("text", strings.clone()), ("number", ints.clone())]).unwrap());

    let arr = StructArray::try_from(vec![("inner", inner_arr.clone())]).unwrap();

    let typed = <TypedStructArray<NestedStructArray>>::try_from(&arr as &dyn Array).unwrap();

    let iter = ArrayIter::new(typed);

    for item in iter.flatten() {

        item.is_valid();
        item.is_null();

        item.is_inner_null();
        item.is_inner_valid();

        let nested = item.inner();
        let nested_opt = item.inner_opt();

        let text: &str = nested.text();
        let text: Option<&str> = nested.text_opt();

        let number: i32 = nested.number();
        let number: Option<i32> = nested.number_opt();

    }

}
