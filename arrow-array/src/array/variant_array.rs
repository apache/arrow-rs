// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::array::print_long_array;
use crate::builder::{ArrayBuilder, BinaryBuilder};
use crate::{Array, ArrayRef};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};

#[cfg(feature = "canonical_extension_types")]
use arrow_schema::extension::Variant;
use std::sync::Arc;
use std::any::Any;

/// An array of Variant values.
///
/// The Variant extension type stores data as two binary values: metadata and value.
/// This array stores each Variant as a concatenated binary value (metadata + value).
///
/// # Example
///
/// ```
/// use arrow_array::VariantArray;
/// use arrow_schema::extension::Variant;
/// use arrow_array::Array; // Import the Array trait
///
/// // Create metadata and value for each variant
/// let metadata = vec![
///     0x01,  // header: version=1, sorted=0, offset_size=1
///     0x01,  // dictionary_size = 1
///     0x00,  // offset 0
///     0x03,  // offset 3
///     b'k', b'e', b'y'  // dictionary bytes
/// ];
/// let variant_type = Variant::new(metadata.clone(), vec![]);
/// 
/// // Create variants with different values
/// let variants = vec![
///     Variant::new(metadata.clone(), b"null".to_vec()),
///     Variant::new(metadata.clone(), b"true".to_vec()),
///     Variant::new(metadata.clone(), b"{\"a\": 1}".to_vec()),
/// ];
/// 
/// // Create a VariantArray
/// let variant_array = VariantArray::from_variants(variant_type, variants.clone()).expect("Failed to create VariantArray");
///
/// // Access variants from the array
/// assert_eq!(variant_array.len(), 3);
/// let retrieved = variant_array.value(0).expect("Failed to get value");
/// assert_eq!(retrieved.metadata(), &metadata);
/// assert_eq!(retrieved.value(), b"null");
/// ```
#[cfg(feature = "canonical_extension_types")]
pub mod variant_array_module {
    use super::*;

    /// An array of Variant values.
    ///
    /// The Variant extension type stores data as two binary values: metadata and value.
    /// This array stores each Variant as a concatenated binary value (metadata + value).
    ///
    /// # Example
    ///
    /// ```
    /// use arrow_array::VariantArray;
    /// use arrow_schema::extension::Variant;
    /// use arrow_array::Array; // Import the Array trait
    ///
    /// // Create metadata and value for each variant
    /// let metadata = vec![
    ///     0x01,  // header: version=1, sorted=0, offset_size=1
    ///     0x01,  // dictionary_size = 1
    ///     0x00,  // offset 0
    ///     0x03,  // offset 3
    ///     b'k', b'e', b'y'  // dictionary bytes
    /// ];
    /// let variant_type = Variant::new(metadata.clone(), vec![]);
    /// 
    /// // Create variants with different values
    /// let variants = vec![
    ///     Variant::new(metadata.clone(), b"null".to_vec()),
    ///     Variant::new(metadata.clone(), b"true".to_vec()),
    ///     Variant::new(metadata.clone(), b"{\"a\": 1}".to_vec()),
    /// ];
    /// 
    /// // Create a VariantArray
    /// let variant_array = VariantArray::from_variants(variant_type, variants.clone()).expect("Failed to create VariantArray");
    ///
    /// // Access variants from the array
    /// assert_eq!(variant_array.len(), 3);
    /// let retrieved = variant_array.value(0).expect("Failed to get value");
    /// assert_eq!(retrieved.metadata(), &metadata);
    /// assert_eq!(retrieved.value(), b"null");
    /// ```
    #[derive(Clone, Debug)]
    pub struct VariantArray {
        data_type: DataType,           // DataType::Binary with extension metadata
        value_data: Buffer,            // Binary data containing serialized variants
        offsets: OffsetBuffer<i32>,    // Offsets into value_data
        nulls: Option<NullBuffer>,     // Null bitmap
        len: usize,                    // Length of the array
        variant_type: Variant,         // The extension type information
    }

    impl VariantArray {
        /// Create a new VariantArray from component parts
        ///
        /// # Panics
        ///
        /// Panics if:
        /// * `offsets.len() != len + 1`
        /// * `nulls` is present and `nulls.len() != len`
        pub fn new(
            variant_type: Variant,
            value_data: Buffer,
            offsets: OffsetBuffer<i32>,
            nulls: Option<NullBuffer>,
            len: usize,
        ) -> Self {
            assert_eq!(offsets.len(), len + 1, "VariantArray offsets length must be len + 1");
            
            if let Some(n) = &nulls {
                assert_eq!(n.len(), len, "VariantArray nulls length must match array length");
            }
            
            Self {
                data_type: DataType::Binary,
                value_data,
                offsets,
                nulls,
                len,
                variant_type,
            }
        }

        /// Create a new VariantArray from raw array data
        pub fn from_data(data: ArrayData, variant_type: Variant) -> Result<Self, ArrowError> {
            if !matches!(data.data_type(), DataType::Binary | DataType::LargeBinary) {
                return Err(ArrowError::InvalidArgumentError(
                    "VariantArray can only be created from Binary or LargeBinary data".to_string()
                ));
            }
            
            let len = data.len();
            let nulls = data.nulls().cloned();
            
            let buffers = data.buffers();
            if buffers.len() != 2 {
                return Err(ArrowError::InvalidArgumentError(
                    "VariantArray data must contain exactly 2 buffers".to_string()
                ));
            }
            
            // Convert Buffer to ScalarBuffer<i32> for OffsetBuffer
            let scalar_buffer = ScalarBuffer::<i32>::new(buffers[0].clone(), 0, len + 1);
            let offsets = OffsetBuffer::new(scalar_buffer);
            let value_data = buffers[1].clone();
            
            Ok(Self {
                data_type: DataType::Binary,
                value_data,
                offsets,
                nulls,
                len,
                variant_type,
            })
        }

        /// Create a new VariantArray from a collection of Variant objects.
        pub fn from_variants(variant_type: Variant, variants: Vec<Variant>) -> Result<Self, ArrowError> {
            // Use BinaryBuilder as a helper to create the underlying storage
            let mut builder = BinaryBuilder::new();
            
            for variant in &variants {
                let mut data = Vec::new();
                data.extend_from_slice(variant.metadata());
                data.extend_from_slice(variant.value());
                builder.append_value(&data);
            }
            
            let binary_array = builder.finish();
            let binary_data = binary_array.to_data();
            
            // Extract the component parts
            let len = binary_data.len();
            let nulls = binary_data.nulls().cloned();
            let buffers = binary_data.buffers();
            
            // Convert Buffer to ScalarBuffer<i32> for OffsetBuffer
            let scalar_buffer = ScalarBuffer::<i32>::new(buffers[0].clone(), 0, len + 1);
            let offsets = OffsetBuffer::new(scalar_buffer);
            let value_data = buffers[1].clone();
            
            Ok(Self {
                data_type: DataType::Binary,
                value_data,
                offsets,
                nulls,
                len,
                variant_type,
            })
        }

        /// Return the serialized binary data for an element at the given index
        fn value_bytes(&self, i: usize) -> Result<&[u8], ArrowError> {
            if i >= self.len {
                return Err(ArrowError::InvalidArgumentError("VariantArray index out of bounds".to_string()));
            }
            let start = *self.offsets.get(i).ok_or_else(|| ArrowError::InvalidArgumentError("Index out of bounds".to_string()))? as usize;
            let end = *self.offsets.get(i + 1).ok_or_else(|| ArrowError::InvalidArgumentError("Index out of bounds".to_string()))? as usize;
            Ok(&self.value_data.as_slice()[start..end])
        }

        /// Calculate the length of variant metadata from serialized data
        fn get_metadata_length(serialized: &[u8]) -> Result<usize, ArrowError> {
            if serialized.is_empty() {
                return Err(ArrowError::InvalidArgumentError("Empty variant data".to_string()));
            }
            
            // Parse header
            let header = serialized[0];
            let version = header & 0x0F;
            let offset_size_minus_one = (header >> 6) & 0x03;
            let offset_size = (offset_size_minus_one + 1) as usize;
            
            if version != 1 {
                return Err(ArrowError::InvalidArgumentError(format!("Invalid variant version: {}", version)));
            }
            
            if serialized.len() < 1 + offset_size {
                return Err(ArrowError::InvalidArgumentError("Variant data too short for dictionary size".to_string()));
            }
            
            // Read dictionary_size
            let mut dictionary_size = 0u32;
            for i in 0..offset_size {
                dictionary_size |= (serialized[1 + i] as u32) << (8 * i);
            }
            
            // Calculate metadata structure size
            let offset_list_size = offset_size * (dictionary_size as usize + 1);
            let metadata_header_size = 1 + offset_size + offset_list_size;
            
            if serialized.len() < metadata_header_size {
                return Err(ArrowError::InvalidArgumentError("Variant data too short for offsets".to_string()));
            }
            
            // Get bytes length from last offset
            let last_offset_pos = 1 + offset_size + offset_list_size - offset_size;
            let mut bytes_length = 0u32;
            for i in 0..offset_size {
                bytes_length |= (serialized[last_offset_pos + i] as u32) << (8 * i);
            }
            
            // Calculate total metadata length
            let metadata_len = metadata_header_size + bytes_length as usize;
            
            if serialized.len() < metadata_len {
                return Err(ArrowError::InvalidArgumentError("Variant metadata exceeds available data".to_string()));
            }
            
            Ok(metadata_len)
        }

        /// Return the Variant at the specified position.
        pub fn value(&self, i: usize) -> Result<Variant, ArrowError> {
            let serialized = self.value_bytes(i)?;
            let metadata_len = Self::get_metadata_length(serialized)?;
            
            // Split metadata and value
            let metadata = &serialized[0..metadata_len];
            let value = &serialized[metadata_len..];
            
            Ok(Variant::new(metadata.to_vec(), value.to_vec()))
        }

        /// Return the Variant type for this array
        pub fn variant_type(&self) -> &Variant {
            &self.variant_type
        }

        /// Create a field with the Variant extension type metadata
        pub fn to_field(&self, name: &str) -> Field {
            Field::new(name, DataType::Binary, self.nulls.is_some())
                .with_extension_type(self.variant_type.clone())
        }
    }

    impl Array for VariantArray {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn to_data(&self) -> ArrayData {
            let mut builder = ArrayDataBuilder::new(self.data_type.clone())
                .len(self.len)
                .add_buffer(self.offsets.clone().into_inner().into())
                .add_buffer(self.value_data.clone());

            if let Some(nulls) = &self.nulls {
                builder = builder.nulls(Some(nulls.clone()));
            }

            unsafe { builder.build_unchecked() }
        }
        
        fn into_data(self) -> ArrayData {
            self.to_data()
        }

        fn data_type(&self) -> &DataType {
            &self.data_type
        }

        fn slice(&self, offset: usize, length: usize) -> ArrayRef {
            assert!(offset + length <= self.len);
            
            let offsets = self.offsets.slice(offset, length + 1);
            
            let nulls = self.nulls.as_ref().map(|n| n.slice(offset, length));
            
            Arc::new(Self {
                data_type: self.data_type.clone(),
                value_data: self.value_data.clone(),
                offsets,
                nulls,
                len: length,
                variant_type: self.variant_type.clone(),
            }) as ArrayRef
        }

        fn len(&self) -> usize {
            self.len
        }

        fn is_empty(&self) -> bool {
            self.len == 0
        }

        fn offset(&self) -> usize {
            0
        }

        fn nulls(&self) -> Option<&NullBuffer> {
            self.nulls.as_ref()
        }

        fn get_buffer_memory_size(&self) -> usize {
            let mut size = 0;
            size += self.value_data.capacity();
            size += self.offsets.inner().as_ref().len() * std::mem::size_of::<i32>();
            if let Some(n) = &self.nulls {
                size += n.buffer().capacity();
            }
            size
        }

        fn get_array_memory_size(&self) -> usize {
            self.get_buffer_memory_size() + std::mem::size_of::<Self>()
        }
    }

    /// A builder for creating a [`VariantArray`]
    pub struct VariantBuilder {
        binary_builder: BinaryBuilder,
        variant_type: Variant,
    }

    impl VariantBuilder {
        /// Create a new builder with the given variant type
        pub fn new(variant_type: Variant) -> Self {
            Self {
                binary_builder: BinaryBuilder::new(),
                variant_type,
            }
        }

        /// Append a Variant value to the builder
        pub fn append_value(&mut self, variant: &Variant) {
            let mut data = Vec::new();
            data.extend_from_slice(variant.metadata());
            data.extend_from_slice(variant.value());
            self.binary_builder.append_value(&data);
        }

        /// Append a null value to the builder
        pub fn append_null(&mut self) {
            self.binary_builder.append_null();
        }

        /// Complete building the array and return the result
        pub fn finish(mut self) -> Result<VariantArray, ArrowError> {
            let binary_array = self.binary_builder.finish();
            let binary_data = binary_array.to_data();
            
            // Extract the component parts
            let len = binary_data.len();
            let nulls = binary_data.nulls().cloned();
            let buffers = binary_data.buffers();
            
            // Convert Buffer to ScalarBuffer<i32> for OffsetBuffer
            let scalar_buffer = ScalarBuffer::<i32>::new(buffers[0].clone(), 0, len + 1);
            let offsets = OffsetBuffer::new(scalar_buffer);
            let value_data = buffers[1].clone();
            
            Ok(VariantArray {
                data_type: DataType::Binary,
                value_data,
                offsets,
                nulls,
                len,
                variant_type: self.variant_type,
            })
        }

        /// Return the current capacity of the builder
        pub fn capacity(&self) -> usize {
            self.binary_builder.len()
        }

        /// Return the number of elements in the builder
        pub fn len(&self) -> usize {
            self.binary_builder.len()
        }

        /// Return whether the builder is empty
        pub fn is_empty(&self) -> bool {
            self.binary_builder.is_empty()
        }
    }

    // Display implementation for prettier debug output
    impl std::fmt::Display for VariantArray {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            writeln!(f, "VariantArray")?;
            writeln!(f, "-- variant_type: {:?}", self.variant_type)?;
            writeln!(f, "[")?;
            print_long_array(self, f, |array, index, f| {
                match array.as_any().downcast_ref::<VariantArray>().unwrap().value(index) {
                    Ok(variant) => write!(f, "{:?}", variant),
                    Err(_) => write!(f, "Error retrieving variant"),
                }
            })?;
            writeln!(f, "]")
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // Helper function to create valid metadata for tests
        fn create_test_metadata() -> Vec<u8> {
            vec![
                0x01,  // header: version=1, sorted=0, offset_size=1
                0x01,  // dictionary_size = 1
                0x00,  // offset 0
                0x03,  // offset 3
                b'k', b'e', b'y'  // dictionary bytes
            ]
        }

        #[test]
        fn test_variant_array_from_variants() {
            let metadata = create_test_metadata();
            let variant_type = Variant::new(metadata.clone(), vec![]);
            
            let variants = vec![
                Variant::new(metadata.clone(), b"value1".to_vec()),
                Variant::new(metadata.clone(), b"value2".to_vec()),
                Variant::new(metadata.clone(), b"value3".to_vec()),
            ];
            
            let array = VariantArray::from_variants(variant_type, variants.clone())
                .expect("Failed to create VariantArray");
            
            assert_eq!(array.len(), 3);
            
            for i in 0..3 {
                let variant = array.value(i).expect("Failed to get value");
                assert_eq!(variant.metadata(), &metadata);
                assert_eq!(variant.value(), variants[i].value());
            }
        }

        #[test]
        fn test_variant_builder() {
            let metadata = create_test_metadata();
            let variant_type = Variant::new(metadata.clone(), vec![]);
            
            let variants = vec![
                Variant::new(metadata.clone(), b"value1".to_vec()),
                Variant::new(metadata.clone(), b"value2".to_vec()),
                Variant::new(metadata.clone(), b"value3".to_vec()),
            ];
            
            let mut builder = VariantBuilder::new(variant_type);
            
            for variant in &variants {
                builder.append_value(variant);
            }
            
            builder.append_null();
            
            let array = builder.finish().expect("Failed to finish VariantBuilder");
            
            assert_eq!(array.len(), 4);
            assert_eq!(array.null_count(), 1);
            
            for i in 0..3 {
                assert!(!array.is_null(i));
                let variant = array.value(i).expect("Failed to get value");
                assert_eq!(variant.metadata(), &metadata);
                assert_eq!(variant.value(), variants[i].value());
            }
            
            assert!(array.is_null(3));
        }

        #[test]
        fn test_variant_array_slice() {
            let metadata = create_test_metadata();
            let variant_type = Variant::new(metadata.clone(), vec![]);
            
            let variants = vec![
                Variant::new(metadata.clone(), b"value1".to_vec()),
                Variant::new(metadata.clone(), b"value2".to_vec()),
                Variant::new(metadata.clone(), b"value3".to_vec()),
                Variant::new(metadata.clone(), b"value4".to_vec()),
            ];
            
            let array = VariantArray::from_variants(variant_type, variants.clone())
                .expect("Failed to create VariantArray");
            
            let sliced = array.slice(1, 2);
            let sliced = sliced.as_any().downcast_ref::<VariantArray>().unwrap();
            
            assert_eq!(sliced.len(), 2);
            
            for i in 0..2 {
                let variant = sliced.value(i).expect("Failed to get value");
                assert_eq!(variant.metadata(), &metadata);
                assert_eq!(variant.value(), variants[i + 1].value());
            }
        }

        #[test]
        fn test_from_binary_data() {
            let metadata = create_test_metadata();
            let variant_type = Variant::new(metadata.clone(), vec![]);
            
            let mut builder = BinaryBuilder::new();
            
            // Manually add serialized variants
            for i in 1..4 {
                let variant = Variant::new(metadata.clone(), format!("value{}", i).into_bytes());
                let mut data = Vec::new();
                data.extend_from_slice(variant.metadata());
                data.extend_from_slice(variant.value());
                builder.append_value(&data);
            }
            
            let binary_array = builder.finish();
            let binary_data = binary_array.to_data();
            let variant_array = VariantArray::from_data(binary_data, variant_type)
                .expect("Failed to create VariantArray");
            
            assert_eq!(variant_array.len(), 3);
            
            for i in 0..3 {
                let variant = variant_array.value(i).expect("Failed to get value");
                assert_eq!(variant.metadata(), &metadata);
                assert_eq!(
                    std::str::from_utf8(variant.value()).unwrap(), 
                    format!("value{}", i+1)
                );
            }
        }
        #[test]
        fn test_get_metadata_length() {
            // Create metadata following the spec:
            // - header: version=1, sorted=0, offset_size=2 bytes (offset_size_minus_one=1)
            // - dictionary_size: 2 strings
            // - dictionary strings: "key1", "key2"
            let mut data = vec![
                0x41,  // header: 0100 0001b (version=1, sorted=0, offset_size_minus_one=1)
                0x02, 0x00,  // dictionary_size = 2 (2 bytes, little-endian)
                // offsets (3 offsets, 2 bytes each)
                0x00, 0x00,  // offset for "key1" start
                0x04, 0x00,  // offset for "key2" start
                0x08, 0x00,  // total bytes length
                // dictionary string bytes
                b'k', b'e', b'y', b'1',  // first string
                b'k', b'e', b'y', b'2'   // second string
            ];
            // Add some value data after metadata
            data.extend_from_slice(b"value data");

            // Total metadata length should be:
            // 1 (header) + 2 (dictionary_size) + 6 (offsets) + 8 (string bytes) = 17
            assert_eq!(VariantArray::get_metadata_length(&data).unwrap(), 17);

            // Test error cases
            assert!(VariantArray::get_metadata_length(&[]).is_err());  // Empty
            assert!(VariantArray::get_metadata_length(&[0x42]).is_err());  // Wrong version
            assert!(VariantArray::get_metadata_length(&[0x41, 0x02]).is_err());  // Too short
        }
    }
}

// Re-export the types from the module when the feature is enabled
#[cfg(feature = "canonical_extension_types")]
pub use variant_array_module::*;