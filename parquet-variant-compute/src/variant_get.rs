use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BinaryArray, PrimitiveBuilder, StructArray},
    compute::CastOptions,
    datatypes::UInt64Type,
    error::Result,
};
use arrow_schema::{ArrowError, DataType, Field};
use parquet_variant::Variant;

use crate::utils::variant_from_struct_array;

/// Returns an array with the specified path extracted from the variant values.
pub fn variant_get(input: &ArrayRef, options: GetOptions) -> Result<ArrayRef> {
    let (struct_array, metadata_array, value_array) = variant_from_struct_array(input)?;

    // TODO: can we use OffsetBuffer and NullBuffer here instead?
    //       I couldn't find a way to set individual indices here, so went with vecs.
    let mut offsets = vec![0; struct_array.len()];
    let mut nulls = if let Some(struct_nulls) = struct_array.nulls() {
        struct_nulls.iter().collect()
    } else {
        vec![true; struct_array.len()]
    };

    for path in options.path.0.iter().take(options.path.0.len() - 1) {
        match path {
            VariantPathElement::Field { name } => {
                go_to_object_field(
                    struct_array,
                    metadata_array,
                    value_array,
                    name,
                    &mut offsets,
                    &mut nulls,
                )?;
            }
            VariantPathElement::Index { offset } => {
                go_to_array_index(
                    struct_array,
                    metadata_array,
                    value_array,
                    *offset,
                    &mut offsets,
                    &mut nulls,
                )?;
            }
        }
    }

    let as_type = options.as_type.ok_or_else(|| {
        ArrowError::NotYetImplemented(
            "getting variant from variant is not implemented yet".to_owned(),
        )
    })?;
    match as_type.data_type() {
        DataType::UInt64 => {
            get_top_level_as_u64(struct_array, metadata_array, value_array, &offsets, &nulls)
        }
        other_type => Err(ArrowError::NotYetImplemented(format!(
            "getting variant as {} is not yet implemented",
            other_type
        ))),
    }
}

fn get_top_level_as_u64(
    struct_array: &StructArray,
    metadata_array: &BinaryArray,
    value_array: &BinaryArray,
    offsets: &[i32],
    nulls: &[bool],
) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<UInt64Type>::with_capacity(struct_array.len());
    for i in 0..struct_array.len() {
        if !nulls[i] {
            builder.append_null();
            continue;
        }
        let metadata = metadata_array.value(i);
        let value = value_array.value(i);
        let value = value.get(offsets[i] as usize..value.len()).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Got invalid offset {} for variant access",
                offsets[i]
            ))
        })?;
        let variant = Variant::new(metadata, value);

        match variant {
            // TODO: narrowing?
            Variant::Int64(i) => builder.append_value(i as u64),
            Variant::Null => builder.append_null(),
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn go_to_object_field(
    struct_array: &StructArray,
    metadata_array: &BinaryArray,
    value_array: &BinaryArray,
    name: &str,
    offsets: &mut [i32],
    nulls: &mut [bool],
) -> Result<()> {
    for i in 0..struct_array.len() {
        if !nulls[i] {
            continue;
        }
        let metadata = metadata_array.value(i);
        let value = value_array.value(i);
        let variant = Variant::new(metadata, value);

        let Variant::Object(variant) = variant else {
            nulls[i] = false;
            continue;
        };

        let offset = variant.field_offset(name).unwrap_or(Ok(0))?;
        offsets[i] += offset as i32;
    }

    Ok(())
}

fn go_to_array_index(
    struct_array: &StructArray,
    metadata_array: &BinaryArray,
    value_array: &BinaryArray,
    index: usize,
    offsets: &mut [i32],
    nulls: &mut [bool],
) -> Result<()> {
    for i in 0..struct_array.len() {
        if !nulls[i] {
            continue;
        }
        let metadata = metadata_array.value(i);
        let value = value_array.value(i);
        let variant = Variant::new(metadata, value);

        let Variant::List(variant) = variant else {
            nulls[i] = false;
            continue;
        };

        if index >= variant.len() {
            nulls[i] = false;
            continue;
        }

        let offset = variant.get_offset(index)?;
        offsets[i] += offset as i32;
    }

    Ok(())
}

/// Controls the action of the variant_get kernel
///
/// If `as_type` is specified `cast_options` controls what to do if the
///
pub struct GetOptions<'a> {
    /// What path to extract
    path: VariantPath,
    /// if `as_type` is None, the returned array will itself be a StructArray with Variant values
    ///
    /// if `as_type` is `Some(type)` the field is returned as the specified type if possible. To specify returning
    /// a Variant, pass a Field with variant type in the metadata.
    as_type: Option<Field>,
    /// Controls the casting behavior (e.g. error vs substituting null on cast error)
    cast_options: CastOptions<'a>,
}

/// Represents a qualified path to a potential subfield of an element
pub struct VariantPath(Vec<VariantPathElement>);

/// Element of a path
enum VariantPathElement {
    /// Access field with name `name`
    Field { name: String },
    /// Access the list element offset
    Index { offset: usize },
}
