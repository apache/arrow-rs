use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
/// The tests in this file ensure a `Schema` can be manipulated
/// outside of the arrow crate

#[test]
fn schema_destructure() {
    let meta = HashMap::from([("foo".to_string(), "baz".to_string())]);

    let field = Field::new("c1", DataType::Utf8, false);
    let schema = Schema::new(vec![field]).with_metadata(meta);

    // Destructuring a Schema allows rewriting metadata
    // without copying
    //
    // Model this usecase below:

    let Schema {
        fields,
        mut metadata,
    } = schema;

    metadata.insert("foo".to_string(), "bar".to_string());

    let new_schema = Schema::new(fields).with_metadata(metadata);

    assert_eq!(new_schema.metadata.get("foo").unwrap(), "bar");
}
