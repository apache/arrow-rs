use crate::decoder::{VariantBasicType, VariantPrimitiveType};
use crate::Variant;
use std::collections::HashMap;

const BASIC_TYPE_BITS: u8 = 2;
const MAX_SHORT_STRING_SIZE: usize = 0x3F;

pub fn primitive_header(primitive_type: VariantPrimitiveType) -> u8 {
    (primitive_type as u8) << 2 | VariantBasicType::Primitive as u8
}

pub fn short_string_header(len: usize) -> u8 {
    (len as u8) << 2 | VariantBasicType::ShortString as u8
}

pub fn array_header(large: bool, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << BASIC_TYPE_BITS)
        | VariantBasicType::Array as u8
}

pub fn object_header(large: bool, id_size: u8, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 4))
        | ((id_size - 1) << (BASIC_TYPE_BITS + 2))
        | ((offset_size - 1) << BASIC_TYPE_BITS)
        | VariantBasicType::Object as u8
}

fn int_size(v: usize) -> u8 {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x10000..=0xFFFFFF => 3,
        _ => 4,
    }
}

/// Write little-endian integer to buffer
fn write_offset(buf: &mut [u8], value: usize, nbytes: u8) {
    for i in 0..nbytes {
        buf[i as usize] = ((value >> (i * 8)) & 0xFF) as u8;
    }
}

/// Helper to make room for header by moving data
fn make_room_for_header(buffer: &mut Vec<u8>, start_pos: usize, header_size: usize) {
    let current_len = buffer.len();
    buffer.resize(current_len + header_size, 0);

    let src_start = start_pos;
    let src_end = current_len;
    let dst_start = start_pos + header_size;

    buffer.copy_within(src_start..src_end, dst_start);
}

/// Builder for [`Variant`] values
///
/// # Example: create a Primitive Int8
/// ```
/// # use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
/// let mut builder = VariantBuilder::new();
/// builder.append_value(Variant::Int8(42));
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let metadata = VariantMetadata::try_new(&metadata).unwrap();
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// assert_eq!(variant, Variant::Int8(42));
/// ```
///
/// # Example: Create an Object
/// This example shows how to create an object with two fields:
/// ```json
/// {
///  "first_name": "Jiaying",
///  "last_name": "Li"
/// }
/// ```
///
/// ```
/// # use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
/// let mut builder = VariantBuilder::new();
/// // Create an object builder that will write fields to the object
/// let mut object_builder = builder.new_object();
/// object_builder.append_value("first_name", "Jiaying");
/// object_builder.append_value("last_name", "Li");
/// object_builder.finish();
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let metadata = VariantMetadata::try_new(&metadata).unwrap();
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let Variant::Object(variant_object) = variant else {
///   panic!("unexpected variant type")
/// };
/// /* TODO: uncomment this, but now VariantObject:field is not implemented
/// assert_eq!(
///   variant_object.field("first_name").unwrap(),
///   Variant::String("Jiaying")
/// );
/// assert_eq!(
///   variant_object.field("last_name").unwrap(),
///   Variant::String("Li")
/// );
/// */
/// ```
///
/// # Example: Create an Array
///
/// This example shows how to create an array of integers: `[1, 2, 3]`.
/// (this test actually fails at the moment)
/// ```
///  # use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
///  let mut builder = VariantBuilder::new();
///  // Create an array builder that will write elements to the array
///  let mut array_builder = builder.new_array();
///  array_builder.append_value(1i8);
///  array_builder.append_value(2i8);
///  array_builder.append_value(3i8);
/// // Finish the builder to get the metadata and value
/// let (metadata, value) = builder.finish();
/// // use the Variant API to verify the result
/// let metadata = VariantMetadata::try_new(&metadata).unwrap();
/// let variant = Variant::try_new(&metadata, &value).unwrap();
/// let Variant::Object(variant_object) = variant else {
///   panic!("unexpected variant type")
/// };
/// // TODO: VERIFY THE RESULT this, but now VariantObject:field is not implemented
/// ```
///
/// # Example: Array of objects
///
/// THis example shows how to create an array of objects:
/// ```json
/// [
///  {
///   "first_name": "Jiaying",
///  "last_name": "Li"
/// },
///   {
///    "first_name": "Malthe",
///    "last_name": "Karbo"
/// }
/// ```
///
/// TODO
///
pub struct VariantBuilder {
    buffer: Vec<u8>,
    dict: HashMap<String, u32>,
    dict_keys: Vec<String>,
}

impl VariantBuilder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            dict: HashMap::new(),
            dict_keys: Vec::new(),
        }
    }

    fn append_null(&mut self) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Null));
    }

    fn append_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.buffer.push(primitive_header(primitive_type));
    }

    fn append_int8(&mut self, value: i8) {
        self.buffer
            .push(primitive_header(VariantPrimitiveType::Int8));
        self.buffer.push(value as u8);
    }

    fn append_string(&mut self, value: &str) {
        if value.len() <= MAX_SHORT_STRING_SIZE {
            self.buffer.push(short_string_header(value.len()));
            self.buffer.extend_from_slice(value.as_bytes());
        } else {
            self.buffer
                .push(primitive_header(VariantPrimitiveType::String));
            self.buffer
                .extend_from_slice(&(value.len() as u32).to_le_bytes());
            self.buffer.extend_from_slice(value.as_bytes());
        }
    }

    /// Add key to dictionary, return its ID
    fn add_key(&mut self, key: &str) -> u32 {
        use std::collections::hash_map::Entry;
        match self.dict.entry(key.to_string()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = self.dict_keys.len() as u32;
                entry.insert(id);
                self.dict_keys.push(key.to_string());
                id
            }
        }
    }

    fn offset(&self) -> usize {
        self.buffer.len()
    }

    /// Create an [`ArrayBuilder`] for creating [`Variant::Array`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_array(&mut self) -> ArrayBuilder {
        ArrayBuilder::new(self)
    }

    /// Create an [`ObjectBuilder`] for creating [`Variant::Object`] values.
    ///
    /// See the examples on [`VariantBuilder`] for usage.
    pub fn new_object(&mut self) -> ObjectBuilder {
        ObjectBuilder::new(self)
    }

    pub fn finish(self) -> (Vec<u8>, Vec<u8>) {
        let nkeys = self.dict_keys.len();

        // Calculate metadata size
        let total_dict_size: usize = self.dict_keys.iter().map(|k| k.len()).sum();

        // Determine appropriate offset size based on the larger of dict size or total string size
        let max_offset = std::cmp::max(total_dict_size, nkeys);
        let offset_size = int_size(max_offset);

        let offset_start = 1 + offset_size as usize;
        let string_start = offset_start + (nkeys + 1) * offset_size as usize;
        let metadata_size = string_start + total_dict_size;

        // Pre-allocate exact size to avoid reallocations
        let mut metadata = Vec::with_capacity(metadata_size);
        metadata.resize(metadata_size, 0);

        // Write header: version=1, not sorted, with calculated offset_size
        metadata[0] = 0x01 | ((offset_size - 1) << 6);

        // Write dictionary size
        write_offset(&mut metadata[1..], nkeys, offset_size);

        // Write offsets and string data
        let mut cur_offset = 0;
        for (i, key) in self.dict_keys.iter().enumerate() {
            write_offset(
                &mut metadata[offset_start + i * offset_size as usize..],
                cur_offset,
                offset_size,
            );
            let start = string_start + cur_offset;
            metadata[start..start + key.len()].copy_from_slice(key.as_bytes());
            cur_offset += key.len();
        }
        // Write final offset
        write_offset(
            &mut metadata[offset_start + nkeys * offset_size as usize..],
            cur_offset,
            offset_size,
        );

        (metadata, self.buffer)
    }

    pub fn append_value<T: Into<Variant<'static, 'static>>>(&mut self, value: T) {
        let variant = value.into();
        match variant {
            Variant::Null => self.append_null(),
            Variant::BooleanTrue => self.append_bool(true),
            Variant::BooleanFalse => self.append_bool(false),
            Variant::Int8(v) => self.append_int8(v),
            Variant::String(s) | Variant::ShortString(s) => self.append_string(s),
            // TODO: Add types for the rest of primitives
            Variant::Object(_) | Variant::Array(_) => {
                unreachable!("Object and Array variants cannot be created through Into<Variant>")
            }
        }
    }
}

impl Default for VariantBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A builder for creating [`Variant::Array`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ArrayBuilder<'a> {
    parent: &'a mut VariantBuilder,
    start_pos: usize,
    offsets: Vec<usize>,
}

impl<'a> ArrayBuilder<'a> {
    fn new(parent: &'a mut VariantBuilder) -> Self {
        let start_pos = parent.offset();
        Self {
            parent,
            start_pos,
            offsets: vec![0],
        }
    }

    pub fn append_value<T: Into<Variant<'static, 'static>>>(&mut self, value: T) {
        self.parent.append_value(value);
        let element_end = self.parent.offset() - self.start_pos;
        self.offsets.push(element_end);
    }

    pub fn finish(self) {
        let data_size = self.parent.offset() - self.start_pos;
        let num_elements = self.offsets.len() - 1;
        let is_large = num_elements > u8::MAX as usize;
        let size_bytes = if is_large { 4 } else { 1 };
        let offset_size = int_size(data_size);
        let header_size = 1 + size_bytes + (num_elements + 1) * offset_size as usize;

        make_room_for_header(&mut self.parent.buffer, self.start_pos, header_size);

        // Write header
        let mut pos = self.start_pos;
        self.parent.buffer[pos] = array_header(is_large, offset_size);
        pos += 1;

        if is_large {
            self.parent.buffer[pos..pos + 4].copy_from_slice(&(num_elements as u32).to_le_bytes());
            pos += 4;
        } else {
            self.parent.buffer[pos] = num_elements as u8;
            pos += 1;
        }

        // Write offsets
        for offset in &self.offsets {
            write_offset(
                &mut self.parent.buffer[pos..pos + offset_size as usize],
                *offset,
                offset_size,
            );
            pos += offset_size as usize;
        }
    }
}

/// A builder for creating [`Variant::Object`] values.
///
/// See the examples on [`VariantBuilder`] for usage.
pub struct ObjectBuilder<'a> {
    parent: &'a mut VariantBuilder,
    start_pos: usize,
    fields: Vec<(u32, usize)>, // (field_id, offset)
}

impl<'a> ObjectBuilder<'a> {
    fn new(parent: &'a mut VariantBuilder) -> Self {
        let start_pos = parent.offset();
        Self {
            parent,
            start_pos,
            fields: Vec::new(),
        }
    }

    /// Add a field with key and value to the object
    pub fn append_value<T: Into<Variant<'static, 'static>>>(&mut self, key: &str, value: T) {
        let id = self.parent.add_key(key);
        let field_start = self.parent.offset() - self.start_pos;
        self.parent.append_value(value);
        self.fields.push((id, field_start));
    }

    /// Finalize object with sorted fields
    pub fn finish(mut self) {
        // Sort fields by key name
        self.fields.sort_by(|a, b| {
            let key_a = &self.parent.dict_keys[a.0 as usize];
            let key_b = &self.parent.dict_keys[b.0 as usize];
            key_a.cmp(key_b)
        });

        let data_size = self.parent.offset() - self.start_pos;
        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;
        let size_bytes = if is_large { 4 } else { 1 };

        let max_id = self.fields.iter().map(|&(id, _)| id).max().unwrap_or(0);
        let id_size = int_size(max_id as usize);
        let offset_size = int_size(data_size);

        let header_size = 1
            + size_bytes
            + num_fields * id_size as usize
            + (num_fields + 1) * offset_size as usize;

        make_room_for_header(&mut self.parent.buffer, self.start_pos, header_size);

        // Write header
        let mut pos = self.start_pos;
        self.parent.buffer[pos] = object_header(is_large, id_size, offset_size);
        pos += 1;

        if is_large {
            self.parent.buffer[pos..pos + 4].copy_from_slice(&(num_fields as u32).to_le_bytes());
            pos += 4;
        } else {
            self.parent.buffer[pos] = num_fields as u8;
            pos += 1;
        }

        // Write field IDs (sorted order)
        for &(id, _) in &self.fields {
            write_offset(
                &mut self.parent.buffer[pos..pos + id_size as usize],
                id as usize,
                id_size,
            );
            pos += id_size as usize;
        }

        // Write field offsets
        for &(_, offset) in &self.fields {
            write_offset(
                &mut self.parent.buffer[pos..pos + offset_size as usize],
                offset,
                offset_size,
            );
            pos += offset_size as usize;
        }
        write_offset(
            &mut self.parent.buffer[pos..pos + offset_size as usize],
            data_size,
            offset_size,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_usage() {
        let mut builder = VariantBuilder::new();

        builder.append_value(());
        builder.append_value(true);
        builder.append_value(42i8);
        builder.append_value("hello");

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_array() {
        let mut builder = VariantBuilder::new();

        {
            let mut array = builder.new_array();
            array.append_value(1i8);
            array.append_value(2i8);
            array.append_value("test");
            array.finish();
        }

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_object() {
        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("name", "John");
            obj.append_value("age", 42i8);
            obj.finish();
        }

        let (metadata, value) = builder.finish();
        assert!(!metadata.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_object_field_ordering() {
        let mut builder = VariantBuilder::new();

        {
            let mut obj = builder.new_object();
            obj.append_value("zebra", "stripes"); // ID = 0
            obj.append_value("apple", "red"); // ID = 1
            obj.append_value("banana", "yellow"); // ID = 2
            obj.finish();
        }

        let (_, value) = builder.finish();

        let header = value[0];
        assert_eq!(header & 0x03, VariantBasicType::Object as u8);

        let field_count = value[1] as usize;
        assert_eq!(field_count, 3);

        // Get field IDs from the object header
        let field_ids: Vec<u8> = value[2..5].to_vec();

        // apple(1), banana(2), zebra(0)
        assert_eq!(field_ids, vec![1, 2, 0]);
    }
}
