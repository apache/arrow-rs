use arrow::array::{BooleanArray, StringArray};
use arrow::compute::{eq_bool, ilike_utf8_scalar};

fn main() {
    let left = StringArray::from(vec!["arrow", "parrow", "arrows", "arr"]);
    let right = "ArroW";
    let res = ilike_utf8_scalar(&left, right).unwrap();
    assert!(res.value(0));
}
