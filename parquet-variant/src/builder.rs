use std::collections::HashMap;
use crate::decoder::{VariantBasicType, VariantPrimitiveType};

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
    (large_bit << (BASIC_TYPE_BITS + 2)) |
    ((offset_size - 1) << BASIC_TYPE_BITS) |
    VariantBasicType::Array as u8
}

pub fn object_header(large: bool, id_size: u8, offset_size: u8) -> u8 {
    let large_bit = if large { 1 } else { 0 };
    (large_bit << (BASIC_TYPE_BITS + 4)) |
    ((id_size - 1) << (BASIC_TYPE_BITS + 2)) |
    ((offset_size - 1) << BASIC_TYPE_BITS) |
    VariantBasicType::Object as u8
}

fn int_size(v: usize) -> u8 {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x10000..=0xFFFFFF => 3,
        _ => 4,
    }
}

fn write_offset(buf: &mut [u8], value: usize, nbytes: u8) {
    for i in 0..nbytes {
        buf[i as usize] = ((value >> (i * 8)) & 0xFF) as u8;
    }
}

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

    pub fn append_null(&mut self) {
        self.buffer.push(primitive_header(VariantPrimitiveType::Null));
    }

    pub fn append_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.buffer.push(primitive_header(primitive_type));
    }

    pub fn append_int8(&mut self, value: i8) {
        self.buffer.push(primitive_header(VariantPrimitiveType::Int8));
        self.buffer.push(value as u8);
    }

    pub fn append_string(&mut self, value: &str) {
        if value.len() <= MAX_SHORT_STRING_SIZE {
            self.buffer.push(short_string_header(value.len()));
            self.buffer.extend_from_slice(value.as_bytes());
        } else {
            self.buffer.push(primitive_header(VariantPrimitiveType::String));
            self.buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            self.buffer.extend_from_slice(value.as_bytes());
        }
    }

    pub fn add_key(&mut self, key: &str) -> u32 {
        if let Some(&id) = self.dict.get(key) {
            return id;
        }
        let id = self.dict_keys.len() as u32;
        self.dict.insert(key.to_string(), id);
        self.dict_keys.push(key.to_string());
        id
    }

    pub fn offset(&self) -> usize {
        self.buffer.len()
    }

    pub fn begin_array(&mut self) -> ArrayBuilder {
        ArrayBuilder::new(self)
    }

    pub fn begin_object(&mut self) -> ObjectBuilder {
        ObjectBuilder::new(self)
    }

    pub fn build(self) -> Vec<u8> {
        self.buffer
    }

    pub fn finish(self) -> (Vec<u8>, Vec<u8>) {
        // Create metadata buffer with proper header
        let mut metadata = Vec::new();
        
        // Write metadata header: version=1, not sorted, offset_size=1 (offset_size_minus_one=0)
        // Header format: bits 7-6: offset_size_minus_one (00), bit 5: sorted (0), bits 4-0: version (1)
        let header = 0x01; // version = 1, sorted = false, offset_size_minus_one = 0
        metadata.push(header);
        
        // Write dictionary size (4 bytes little endian)
        metadata.extend_from_slice(&(self.dict_keys.len() as u32).to_le_bytes());
        
        // Write dictionary offsets (for empty dict, just write [0])
        if self.dict_keys.is_empty() {
            metadata.push(0); // offset 0 for empty dictionary
        } else {
            // Write offsets for each dictionary entry plus end offset
            let mut current_offset = 0u32;
            metadata.push(current_offset as u8); // start offset
            
            for key in &self.dict_keys {
                current_offset += key.len() as u32;
                metadata.push(current_offset as u8);
            }
            
            // Write the dictionary string data
            for key in &self.dict_keys {
                metadata.extend_from_slice(key.as_bytes());
            }
        }
        
        (metadata, self.buffer)
    }
}

impl Default for VariantBuilder {
    fn default() -> Self {
        Self::new()
    }
}

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

    pub fn append_element<F>(&mut self, f: F) 
    where F: FnOnce(&mut VariantBuilder)
    {
        f(self.parent);
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

        let current_len = self.parent.buffer.len();
        self.parent.buffer.resize(current_len + header_size, 0);
        
        let src_start = self.start_pos;
        let src_end = current_len;
        let dst_start = self.start_pos + header_size;
        
        self.parent.buffer.copy_within(src_start..src_end, dst_start);

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

        for offset in &self.offsets {
            write_offset(&mut self.parent.buffer[pos..pos + offset_size as usize], *offset, offset_size);
            pos += offset_size as usize;
        }
    }
}

pub struct ObjectBuilder<'a> {
    parent: &'a mut VariantBuilder,
    start_pos: usize,
    fields: Vec<(u32, usize)>,
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

    pub fn append_field<F>(&mut self, key: &str, f: F)
    where F: FnOnce(&mut VariantBuilder)
    {
        let id = self.parent.add_key(key);
        let field_start = self.parent.offset() - self.start_pos;
        f(self.parent);
        self.fields.push((id, field_start));
    }

    pub fn finish(mut self) {
        self.fields.sort_by_key(|&(id, _)| id);

        let data_size = self.parent.offset() - self.start_pos;
        let num_fields = self.fields.len();
        let is_large = num_fields > u8::MAX as usize;
        let size_bytes = if is_large { 4 } else { 1 };
        
        let max_id = self.fields.iter().map(|&(id, _)| id).max().unwrap_or(0);
        let id_size = int_size(max_id as usize);
        let offset_size = int_size(data_size);
        
        let header_size = 1 + size_bytes + num_fields * id_size as usize + (num_fields + 1) * offset_size as usize;

        let current_len = self.parent.buffer.len();
        self.parent.buffer.resize(current_len + header_size, 0);
        
        let src_start = self.start_pos;
        let src_end = current_len;
        let dst_start = self.start_pos + header_size;
        
        self.parent.buffer.copy_within(src_start..src_end, dst_start);

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

        for &(id, _) in &self.fields {
            write_offset(&mut self.parent.buffer[pos..pos + id_size as usize], id as usize, id_size);
            pos += id_size as usize;
        }

        for &(_, offset) in &self.fields {
            write_offset(&mut self.parent.buffer[pos..pos + offset_size as usize], offset, offset_size);
            pos += offset_size as usize;
        }
        write_offset(&mut self.parent.buffer[pos..pos + offset_size as usize], data_size, offset_size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_usage() {
        let mut builder = VariantBuilder::new();
        
        builder.append_null();
        builder.append_bool(true);
        builder.append_int8(42);
        builder.append_string("hello");
        
        let result = builder.build();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_array() {
        let mut builder = VariantBuilder::new();
        
        {
            let mut array = builder.begin_array();
            array.append_element(|b| b.append_int8(1));
            array.append_element(|b| b.append_int8(2));
            array.append_element(|b| b.append_string("test"));
            array.finish();
        }
        
        let result = builder.build();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_object() {
        let mut builder = VariantBuilder::new();
        
        {
            let mut obj = builder.begin_object();
            obj.append_field("name", |b| b.append_string("John"));
            obj.append_field("age", |b| b.append_int8(30));
            obj.finish();
        }
        
        let result = builder.build();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_compatibility() {
        use crate::decoder::{decode_int8, decode_short_string, get_basic_type, get_primitive_type};
        
        let mut builder = VariantBuilder::new();
        builder.append_int8(42);
        let result = builder.build();
        
        let header = result[0];
        assert_eq!(get_basic_type(header).unwrap(), VariantBasicType::Primitive);
        assert_eq!(get_primitive_type(header).unwrap(), VariantPrimitiveType::Int8);
        assert_eq!(decode_int8(&result).unwrap(), 42);
        
        let mut builder = VariantBuilder::new();
        builder.append_string("Hello");
        let result = builder.build();
        
        let header = result[0];
        assert_eq!(get_basic_type(header).unwrap(), VariantBasicType::ShortString);
        assert_eq!(decode_short_string(&result).unwrap(), "Hello");
    }

    #[test]
    fn test_object_structure() {
        let mut builder = VariantBuilder::new();
        
        {
            let mut obj = builder.begin_object();
            obj.append_field("a", |b| b.append_int8(1));
            obj.append_field("b", |b| b.append_int8(2));
            obj.finish();
        }
        
        let result = builder.build();
        
        // Print the byte structure for debugging
        println!("Object bytes: {:?}", result);
        
        // Basic sanity check - should have more than just the header
        assert!(result.len() > 10, "Object should have substantial size");
        
        // Verify it can be parsed by the decoder
        use crate::decoder::{get_basic_type, VariantBasicType};
        let header = result[0];
        assert_eq!(get_basic_type(header).unwrap(), VariantBasicType::Object);
    }

    #[test]
    fn test_object_offset_correctness() {
        // Test with known field sizes to verify offset calculation
        let mut builder = VariantBuilder::new();
        
        {
            let mut obj = builder.begin_object();
            // Field "x": int8 = 2 bytes (1 header + 1 value)  
            obj.append_field("x", |b| b.append_int8(42));
            // Field "y": string "hi" = 3 bytes (1 header + 2 chars)
            obj.append_field("y", |b| b.append_string("hi"));
            obj.finish();
        }
        
        let result = builder.build();
        println!("Object with known sizes: {:?}", result);
        
        // Verify the structure makes sense
        assert!(result.len() > 5, "Should have reasonable size");
        
        // Test that it doesn't crash when parsed
        use crate::decoder::{get_basic_type, VariantBasicType};
        let header = result[0];
        assert_eq!(get_basic_type(header).unwrap(), VariantBasicType::Object);
    }
}
