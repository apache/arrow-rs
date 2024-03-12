use arrow_array::builder::{ArrayBuilder, ListBuilder, StringBuilder, StructBuilder};
use arrow_schema::{DataType, Field, Fields};
use std::sync::Arc;

fn main() {
    // This is an example column that has a List<Struct<List<Struct>>> layout
    let mut example_col = ListBuilder::new(StructBuilder::from_fields(example_schema_fields(), 0));

    // We can obtain the StructBuilder without issues, because example_col was created with StructBuilder
    let col_struct_builder: &mut StructBuilder = example_col.values();

    // We can't obtain the ListBuilder<StructBuilder> without issues, because under the hood the StructBuilder was created as a Box<dyn ArrayBuilder>
    // let mut sb: &mut ListBuilder<StructBuilder> = struct_builder.field_builder(1).unwrap();  //This breaks!

    //We fetch the ListBuilder<Box<dyn ArrayBuilder>> from the StructBuilder first...
    let mut list_builder_option =
        col_struct_builder.field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(0);

    let list_builder = list_builder_option.as_mut().unwrap();

    // ... and then downcast it to a ListBuilder<StructBuilder>
    let struct_builder = list_builder
        .values()
        .as_any_mut()
        .downcast_mut::<StructBuilder>()
        .unwrap();

    // We can now append values to the StructBuilder
    let key_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
    key_builder.append_value("my key");

    let value_builder = struct_builder.field_builder::<StringBuilder>(1).unwrap();
    value_builder.append_value("my value");

    struct_builder.append(true);
    list_builder.append(true);
    col_struct_builder.append(true);
    example_col.append(true);

    let array = example_col.finish();

    println!("My array: {:?}", array);
}

pub fn example_schema_fields() -> Vec<Field> {
    vec![Field::new(
        "value_list",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(key_value_schema_fields())), //In the above example we are trying to get to this builder and insert key/value pairs
            true,
        ))),
        true,
    )]
}
pub fn key_value_schema_fields() -> Vec<Field> {
    vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]
}
