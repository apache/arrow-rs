pub use crate::variant::{VariantDecimal4, VariantDecimal8};
use crate::variant_buffer_manager::VariantBufferManager;
use crate::{AppendVariantHelper, ListBuilder, ObjectBuilder, Variant, VariantBuilder};
use arrow_schema::ArrowError;
use rust_decimal::prelude::*;
use serde_json::{Map, Number, Value};

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

fn variant_from_number<'a, 'b>(n: &Number) -> Result<Variant<'a, 'b>, ArrowError> {
    if let Some(i) = n.as_i64() {
        // Find minimum Integer width to fit
        if i as i8 as i64 == i {
            Ok((i as i8).into())
        } else if i as i16 as i64 == i {
            Ok((i as i16).into())
        } else if i as i32 as i64 == i {
            Ok((i as i32).into())
        } else {
            Ok(i.into())
        }
    } else {
        // Try decimal
        // TODO: Replace with custom decimal parsing as the rust_decimal library only supports
        // a max unscaled value of 2^96.
        match Decimal::from_str_exact(n.as_str()) {
            Ok(dec) => {
                let unscaled: i128 = dec.mantissa();
                let scale = dec.scale() as u8;
                if unscaled.abs() <= VariantDecimal4::MAX_UNSCALED_VALUE as i128
                    && scale <= VariantDecimal4::MAX_PRECISION as u8
                {
                    (unscaled as i32, scale).try_into()
                } else if unscaled.abs() <= VariantDecimal8::MAX_UNSCALED_VALUE as i128
                    && scale <= VariantDecimal8::MAX_PRECISION as u8
                {
                    (unscaled as i64, scale).try_into()
                } else {
                    (unscaled, scale).try_into()
                }
            }
            Err(_) => {
                // Try double
                match n.as_f64() {
                    Some(f) => return Ok(f.into()),
                    None => Err(ArrowError::InvalidArgumentError(format!(
                        "Failed to parse {} as number",
                        n.as_str()
                    ))),
                }?
            }
        }
    }
}

fn append_json(json: &Value, builder: &mut impl AppendVariantHelper) -> Result<(), ArrowError> {
    match json {
        Value::Null => builder.append_value(Variant::Null),
        Value::Bool(b) => builder.append_value(*b),
        Value::Number(n) => {
            builder.append_value(variant_from_number(n)?);
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

fn build_list(arr: &[Value], builder: &mut ListBuilder) -> Result<(), ArrowError> {
    arr.iter().try_fold((), |_, val| append_json(val, builder))
}

fn build_object<'a, 'b>(
    obj: &'b Map<String, Value>,
    builder: &mut ObjectBuilder<'a, 'b>,
) -> Result<(), ArrowError> {
    obj.iter().try_fold((), |_, (key, value)| {
        let mut field_builder = ObjectFieldBuilder { key, builder };
        append_json(value, &mut field_builder)
    })
}

struct ObjectFieldBuilder<'a, 'b, 'c> {
    key: &'a str,
    builder: &'b mut ObjectBuilder<'c, 'a>,
}

impl AppendVariantHelper for ObjectFieldBuilder<'_, '_, '_> {
    fn append_value<'m, 'd, T: Into<Variant<'m, 'd>>>(&mut self, value: T) {
        self.builder.insert(self.key, value);
    }

    fn new_list(&mut self) -> ListBuilder {
        self.builder.new_list(self.key)
    }

    fn new_object(&mut self) -> ObjectBuilder {
        self.builder.new_object(self.key)
    }
}
