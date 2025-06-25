use crate::variant_buffer_manager::VariantBufferManager;
use crate::{ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use arrow_schema::ArrowError;
use serde_json::{Map, Value};

/// Eventually, internal writes should also be performed using VariantBufferManager instead of
/// ValueBuffer and MetadataBuffer so the caller has control of the memory.
/// Returns a pair <value_size, metadata_size>
pub fn json_to_variant<'a, T: VariantBufferManager>(
    json: &str,
    variant_buffer_manager: &'a mut T,
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
    variant_buffer_manager.ensure_metadata_buffer_size(metadata_size)?;
    variant_buffer_manager.ensure_value_buffer_size(value_size)?;

    let caller_metadata_buffer = variant_buffer_manager.borrow_metadata_buffer();
    caller_metadata_buffer[..metadata_size].copy_from_slice(metadata.as_slice());
    let caller_value_buffer = variant_buffer_manager.borrow_value_buffer();
    caller_value_buffer[..value_size].copy_from_slice(value.as_slice());
    Ok((metadata_size, value_size))
}

fn build_json(json: &Value, builder: &mut VariantBuilder) -> Result<(), ArrowError> {
    match json {
        Value::Null => builder.append_value(Variant::Null),
        Value::Bool(b) => builder.append_value(*b),
        Value::Number(n) => {
            let v: Variant = n.try_into()?;
            builder.append_value(v)
        }
        Value::String(s) => builder.append_value(s.as_str()),
        Value::Array(arr) => {
            let mut list_builder = builder.new_list();
            build_list(arr, &mut list_builder)?;
            list_builder.finish();
        }
        Value::Object(obj) => {
            let mut obj_builder = builder.new_object();
            build_object(obj, &mut obj_builder)?;
            obj_builder.finish();
        }
    };
    Ok(())
}

fn build_list(arr: &Vec<Value>, builder: &mut ListBuilder) -> Result<(), ArrowError> {
    for val in arr {
        match val {
            Value::Null => builder.append_value(Variant::Null),
            Value::Bool(b) => builder.append_value(*b),
            Value::Number(n) => {
                let v: Variant = n.try_into()?;
                builder.append_value(v)
            }
            Value::String(s) => builder.append_value(s.as_str()),
            Value::Array(arr) => {
                let mut list_builder = builder.new_list();
                build_list(arr, &mut list_builder)?;
                list_builder.finish()
            }
            Value::Object(obj) => {
                let mut obj_builder = builder.new_object();
                build_object(obj, &mut obj_builder)?;
                obj_builder.finish();
            }
        }
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
            Value::Number(n) => {
                let v: Variant = n.try_into()?;
                builder.insert(key, v)
            }
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
