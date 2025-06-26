use crate::variant_buffer_manager::VariantBufferManager;
use crate::{AppendVariantHelper, ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use arrow_schema::ArrowError;
use serde_json::{Map, Value};

/// Eventually, internal writes should also be performed using VariantBufferManager instead of
/// ValueBuffer and MetadataBuffer so the caller has control of the memory.
/// Returns a pair <value_size, metadata_size>
pub fn json_to_variant(
    json: &str,
    variant_buffer_manager: &mut impl VariantBufferManager,
) -> Result<(usize, usize), ArrowError> {
    let mut builder = VariantBuilder::new();
    let json: Value = serde_json::from_str(json)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("JSON format error: {}", e)))?;

    build_json(&json, &mut builder)?;
    let (metadata, value) = builder.finish();
    let value_size = value.len();
    let metadata_size = metadata.len();

    // Write to caller's buffers - Remove this when the library internally writes to the caller's
    // buffers anyway
    let caller_metadata_buffer =
        variant_buffer_manager.ensure_size_and_borrow_metadata_buffer(metadata_size)?;
    caller_metadata_buffer[..metadata_size].copy_from_slice(metadata.as_slice());
    let caller_value_buffer =
        variant_buffer_manager.ensure_size_and_borrow_value_buffer(value_size)?;
    caller_value_buffer[..value_size].copy_from_slice(value.as_slice());
    Ok((metadata_size, value_size))
}

fn build_json(json: &Value, builder: &mut VariantBuilder) -> Result<(), ArrowError> {
    append_json(json, builder)?;
    Ok(())
}

fn append_json(json: &Value, builder: &mut impl AppendVariantHelper) -> Result<(), ArrowError> {
    match json {
        Value::Null => builder.append_value_helper(Variant::Null),
        Value::Bool(b) => builder.append_value_helper(*b),
        Value::Number(n) => builder.append_value_helper(Variant::try_from(n)?),
        Value::String(s) => builder.append_value_helper(s.as_str()),
        Value::Array(arr) => {
            let mut list_builder = builder.new_list_helper();
            build_list(arr, &mut list_builder)?;
            list_builder.finish();
        }
        Value::Object(obj) => {
            let mut obj_builder = builder.new_object_helper();
            build_object(obj, &mut obj_builder)?;
            obj_builder.finish();
        }
    };
    Ok(())
}

fn build_list(arr: &[Value], builder: &mut ListBuilder) -> Result<(), ArrowError> {
    for val in arr {
        append_json(val, builder)?;
    }
    Ok(())
}

fn build_object<'a, 'b>(
    obj: &'b Map<String, Value>,
    builder: &mut ObjectBuilder<'a, 'b>,
) -> Result<(), ArrowError> {

    for (key, value) in obj.iter() {
        match value {
            Value::Null => builder.insert(key, Variant::Null),
            Value::Bool(b) => builder.insert(key, *b),
            Value::Number(n) => builder.insert(key, Variant::try_from(n)?),
            Value::String(s) => builder.insert(key, s.as_str()),
            Value::Array(arr) => {
                let mut list_builder = builder.new_list(key);
                build_list(arr, &mut list_builder)?;
                list_builder.finish()
            }
            Value::Object(obj) => {
                let mut obj_builder = builder.new_object(key);
                build_object(obj, &mut obj_builder)?;
                obj_builder.finish();
            }
        }
    }
    Ok(())
}
