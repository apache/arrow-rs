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

//! Contains all supported encoders for Parquet.

use std::{cmp, marker::PhantomData};

use crate::basic::*;
#[cfg(feature = "arrow")]
use crate::column::value_selection::{
    PhysicalValueSelection, PhysicalValueSpan, ValueSelectionRef,
};
#[cfg(any(test, feature = "test_common", feature = "experimental"))]
use crate::column::writer::encoder::EncodingFamilyFor;
use crate::data_type::private::{ParquetValueType, PlainEncoderValue};
use crate::data_type::*;
use crate::encodings::rle::RleEncoder;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::{BitWriter, num_required_bits};

#[cfg(feature = "arrow")]
use crate::util::bit_util::get_bit;
pub(crate) use alp_family::AlpValue;
#[cfg(feature = "arrow")]
use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;

use byte_stream_split_encoder::{ByteStreamSplitEncoder, VariableWidthByteStreamSplitEncoder};
use bytes::Bytes;
pub use dict_encoder::DictEncoder;
pub(crate) use dict_encoder::{DictionaryStorage, DictionaryValue};

mod alp_encoder;
mod alp_family;
mod boolean;
mod byte_stream_split_encoder;
#[cfg(feature = "experimental")]
mod compatibility;
mod dict_encoder;
mod fixed_len_byte_array;
mod numeric;

pub(crate) use boolean::{BoolBatch, BoolEncoder};
#[cfg(feature = "experimental")]
#[expect(deprecated, reason = "Preserve the experimental concrete encoder API")]
pub use compatibility::{DeltaByteArrayEncoder, DeltaLengthByteArrayEncoder, PlainEncoder};
pub(crate) use fixed_len_byte_array::{FixedLenByteArrayEncoder, PackedFixedLenByteArrayBatch};
pub use numeric::DeltaBitPackEncoder;

// ----------------------------------------------------------------------
// Encoders

/// An Parquet encoder for the data type `T`.
///
/// Currently this allocates internal buffers for the encoded values. After done putting
/// values, caller should call `flush_buffer()` to get an immutable buffer pointer.
pub trait Encoder<T: DataType>: Send {
    /// Encodes data from `values`.
    fn put(&mut self, values: &[T::T]) -> Result<()>;

    /// Encodes data from `values`, which contains spaces for null values, that is
    /// identified by `valid_bits`.
    ///
    /// Returns the number of non-null values encoded.
    #[cfg(test)]
    fn put_spaced(&mut self, values: &[T::T], valid_bits: &[u8]) -> Result<usize> {
        let num_values = values.len();
        let mut buffer = Vec::with_capacity(num_values);
        // TODO: this is pretty inefficient. Revisit in future.
        for (i, item) in values.iter().enumerate().take(num_values) {
            if crate::util::bit_util::get_bit(valid_bits, i) {
                buffer.push(item.clone());
            }
        }
        self.put(&buffer[..])?;
        Ok(buffer.len())
    }

    /// Returns the encoding type of this encoder.
    fn encoding(&self) -> Encoding;

    /// Returns an estimate of the encoded data, in bytes.
    /// Method call must be O(1).
    fn estimated_data_encoded_size(&self) -> usize;

    /// Returns an estimate of the memory use of this encoder, in bytes
    fn estimated_memory_size(&self) -> usize;

    /// Flushes the underlying byte buffer that's being processed by this encoder, and
    /// return the immutable copy of it. This will also reset the internal state.
    fn flush_buffer(&mut self) -> Result<Bytes>;
}

/// Gets an encoder for the particular data type `T` and `encoding`.
#[cfg(any(test, feature = "test_common", feature = "experimental"))]
pub fn get_encoder<T: DataType>(
    encoding: Encoding,
    descr: &ColumnDescPtr,
) -> Result<Box<dyn Encoder<T>>> {
    let encoder = <EncodingFamilyFor<T> as EncodingFamily<T>>::try_new(false, encoding, descr)?;
    Ok(Box::new(encoder))
}

mod encoding_family_private {
    use super::*;

    /// Marker for physical types handled by the generic PLAIN encoder.
    pub trait PlainEncoderType: DataType<T: PlainEncoderValue> {}

    impl<T: DataType> PlainEncoderType for T where T::T: PlainEncoderValue {}

    /// Dictionary lifecycle and page sizing for one physical type's encoding family.
    pub trait EncodingFamily<T: DataType>: Sized {
        /// Build the initial encoder: the dictionary when supported (eagerly validating
        /// the fallback encoding so an unsupported one fails fast at construction),
        /// otherwise the configured fallback encoding.
        fn try_new(
            dict_supported: bool,
            fallback_encoding: Encoding,
            descr: &ColumnDescPtr,
        ) -> Result<Self>;
        fn is_dictionary(&self) -> bool {
            false
        }
        /// Visit dictionary values directly from their physical storage, when active.
        fn visit_dictionary_values(&self, _visit: impl FnMut(&[u8])) {}
        #[cfg(feature = "arrow")]
        fn start_arrow_source(&mut self) {}
        /// If dictionary-encoding, serialize the dictionary page as `(buf, num_values,
        /// is_sorted)` and transition in place to the fallback encoding (dictionary
        /// fallback); otherwise `None`.
        fn take_dict_page(
            &mut self,
            _fallback_encoding: Encoding,
            _descr: &ColumnDescPtr,
        ) -> Result<Option<(Bytes, usize, bool)>> {
            Ok(None)
        }
        fn flush_data_page(&mut self) -> Result<(Bytes, Encoding)>;
        fn dict_page_size(&self) -> Option<usize> {
            None
        }
        fn data_page_size(&self) -> usize;
        fn memory_size(&self) -> usize;
    }
}

pub(crate) use encoding_family_private::{EncodingFamily, PlainEncoderType};

/// Builds a flat encoding-family enum and forwards [`Encoder`] to its valid variants.
macro_rules! encoding_family_enum {
    (
        $name:ident $(<$generic:ident : $bound:path>)?,
        $ty:ty,
        [$($dict_variant:ident($dict_encoder:ty))?],
        {$($variant:ident($encoder:ty)),+ $(,)?}
    ) => {
        pub enum $name $(<$generic: $bound>)? {
            $($dict_variant($dict_encoder),)?
            $($variant($encoder)),+
        }

        impl<D $(, $generic)?> Encoder<D> for $name $(<$generic>)?
        where
            D: DataType<T = <$ty as DataType>::T>,
            $($generic: $bound,)?
        {
            fn put(&mut self, values: &[D::T]) -> Result<()> {
                match self {
                    $(Self::$dict_variant(_) => unreachable!("dictionary variant is not routed through Encoder"),)?
                    $(Self::$variant(e) => e.put(values)),+
                }
            }

            fn encoding(&self) -> Encoding {
                match self {
                    $(Self::$dict_variant(_) => unreachable!("dictionary variant is not routed through Encoder"),)?
                    $(Self::$variant(e) => e.encoding()),+
                }
            }

            fn estimated_data_encoded_size(&self) -> usize {
                match self {
                    $(Self::$dict_variant(_) => unreachable!("dictionary variant is not routed through Encoder"),)?
                    $(Self::$variant(e) => e.estimated_data_encoded_size()),+
                }
            }

            fn estimated_memory_size(&self) -> usize {
                match self {
                    $(Self::$dict_variant(_) => unreachable!("dictionary variant is not routed through Encoder"),)?
                    $(Self::$variant(e) => e.estimated_memory_size()),+
                }
            }

            fn flush_buffer(&mut self) -> Result<Bytes> {
                match self {
                    $(Self::$dict_variant(_) => unreachable!("dictionary variant is not routed through Encoder"),)?
                    $(Self::$variant(e) => e.flush_buffer()),+
                }
            }
        }
    };
}

/// Adds the shared dictionary lifecycle to an encoding-family enum.
macro_rules! impl_dictionary_encoding_family {
    ($name:ident $(<$generic:ident : $bound:path>)?, $ty:ty) => {
        impl<D $(, $generic)?> EncodingFamily<D> for $name $(<$generic>)?
        where
            D: DataType<T = <$ty as DataType>::T>,
            $($generic: $bound,)?
        {
            fn try_new(
                dict_supported: bool,
                fallback_encoding: Encoding,
                descr: &ColumnDescPtr,
            ) -> Result<Self> {
                if dict_supported {
                    // Eagerly validate the fallback encoding (fail fast on an
                    // unsupported one) and initialize the dictionary.
                    Self::from_encoding(fallback_encoding, descr)?;
                    Ok(Self::Dictionary(DictEncoder::new(descr.clone())))
                } else {
                    Self::from_encoding(fallback_encoding, descr)
                }
            }

            fn is_dictionary(&self) -> bool {
                matches!(self, Self::Dictionary(_))
            }

            fn visit_dictionary_values(&self, visit: impl FnMut(&[u8])) {
                if let Self::Dictionary(dict) = self {
                    dict.visit_uniques(visit);
                }
            }

            #[cfg(feature = "arrow")]
            fn start_arrow_source(&mut self) {
                if let Self::Dictionary(dict) = self {
                    dict.start_arrow_source()

                }
            }

            fn take_dict_page(
                &mut self,
                fallback_encoding: Encoding,
                descr: &ColumnDescPtr,
            ) -> Result<Option<(Bytes, usize, bool)>> {
                if !<Self as EncodingFamily<D>>::is_dictionary(self) {
                    return Ok(None);
                }
                // Abandon the dictionary by building the fallback encoder,
                // swapping it in, and consuming the extracted dictionary into
                // its page.
                let fallback = Self::from_encoding(fallback_encoding, descr)?;
                let Self::Dictionary(dict) = std::mem::replace(self, fallback) else {
                    unreachable!("is_dictionary checked above");
                };
                let num_values = dict.num_entries();
                let is_sorted = dict.is_sorted();
                let buf = dict.into_dict_page()?;
                Ok(Some((buf, num_values, is_sorted)))
            }

            fn flush_data_page(&mut self) -> Result<(Bytes, Encoding)> {
                match self {
                    Self::Dictionary(dict) => Ok((dict.write_indices()?, Encoding::RLE_DICTIONARY)),
                    other => Ok((
                        <Self as Encoder<$ty>>::flush_buffer(other)?,
                        <Self as Encoder<$ty>>::encoding(other),
                    )),
                }
            }

            fn dict_page_size(&self) -> Option<usize> {
                match self {
                    Self::Dictionary(dict) => Some(dict.dict_encoded_size()),
                    _ => None,
                }
            }

            fn data_page_size(&self) -> usize {
                match self {
                    Self::Dictionary(dict) => {
                        <DictEncoder<$ty> as Encoder<$ty>>::estimated_data_encoded_size(dict)
                    }
                    other => <Self as Encoder<$ty>>::estimated_data_encoded_size(other),
                }
            }

            fn memory_size(&self) -> usize {
                match self {
                    Self::Dictionary(dict) => {
                        <DictEncoder<$ty> as Encoder<$ty>>::estimated_memory_size(dict)
                    }
                    other => <Self as Encoder<$ty>>::estimated_memory_size(other),
                }
            }
        }
    };
}

mod byte_array;
pub(crate) use byte_array::{
    ByteArrayDeltaEncoder, ByteArrayDeltaLengthEncoder, ByteArrayEncodingFamily,
    ByteArrayPlainEncoder,
};

/// Defines a flat encoding-family enum with dictionary lifecycle handling.
macro_rules! dictionary_encoding_family {
    (
        $name:ident $(<$generic:ident : $bound:path>)?,
        $ty:ty,
        {$($variant:ident($encoder:ty)),+ $(,)?}
    ) => {
        encoding_family_enum!(
            $name $(<$generic: $bound>)?,
            $ty,
            [Dictionary(DictEncoder<$ty>)],
            {$($variant($encoder)),+}
        );
        impl_dictionary_encoding_family!($name $(<$generic: $bound>)?, $ty);
    };
}

/// Formats a rejection from an encoding-family constructor. Supported combinations
/// are determined only by the constructors, not by a separate validation table.
fn unsupported_column_encoding(encoding: Encoding, physical_type: Type) -> ParquetError {
    match encoding {
        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
            general_err!("Cannot initialize this encoding through this function")
        }
        #[expect(
            deprecated,
            reason = "BIT_PACKED is unsupported for every physical type"
        )]
        Encoding::BIT_PACKED => nyi_err!("Encoding {} is not supported", encoding),
        Encoding::ALP => general_err!(
            "Encoding {} only supports FLOAT and DOUBLE, got {}",
            encoding,
            physical_type
        ),
        _ => nyi_err!(
            "Encoding {} is not supported for physical type {:?}",
            encoding,
            physical_type
        ),
    }
}

mod encoding_families {
    use super::PlainEncoderImpl as PlainEncoder;
    use super::*;

    encoding_family_enum!(
        BoolEncodingFamily,
        BoolType,
        [],
        {
            Plain(PlainEncoder<BoolType>),
            Rle(RleValueEncoder<BoolType>),
        }
    );

    dictionary_encoding_family!(
        FixedLenByteArrayEncodingFamily,
        FixedLenByteArrayType,
        {
            Plain(PlainEncoder<FixedLenByteArrayType>),
            DeltaByteArray(ByteArrayDeltaEncoder),
            ByteStreamSplit(VariableWidthByteStreamSplitEncoder<FixedLenByteArrayType>),
        }
    );

    dictionary_encoding_family!(
        NumericEncodingFamily<T: PlainEncoderType>,
        T,
        {
            Plain(PlainEncoder<T>),
            DeltaBinaryPacked(Box<DeltaBitPackEncoder<T>>),
            ByteStreamSplit(ByteStreamSplitEncoder<T>),
            Alp(Box<<T::T as AlpValue>::Encoder<T>>),
        }
    );
}

pub(crate) use encoding_families::{
    BoolEncodingFamily, FixedLenByteArrayEncodingFamily, NumericEncodingFamily,
};

// ----------------------------------------------------------------------
// Plain encoding

mod plain {
    use super::*;

    /// Plain encoding for boolean, numeric, and fixed-length byte-array values.
    /// Values are encoded back to back.
    /// The plain encoding is used whenever a more efficient encoding can not be used.
    /// It stores the data in the following format:
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as IEEE little-endian.
    /// - DOUBLE - 8 bytes per value, stored as IEEE little-endian.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    pub struct PlainEncoderImpl<T: DataType> {
        pub(super) buffer: Vec<u8>,
        pub(super) bit_writer: BitWriter,
        _phantom: PhantomData<T>,
    }

    impl<T: DataType> Default for PlainEncoderImpl<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T: DataType> PlainEncoderImpl<T> {
        /// Creates new plain encoder.
        pub fn new() -> Self {
            Self {
                buffer: vec![],
                bit_writer: BitWriter::new(256),
                _phantom: PhantomData,
            }
        }
    }

    impl<T: DataType> Encoder<T> for PlainEncoderImpl<T>
    where
        T::T: PlainEncoderValue,
    {
        // Performance Note:
        // As far as can be seen these functions are rarely called and as such we can hint to the
        // compiler that they dont need to be folded into hot locations in the final output.
        #[cold]
        fn encoding(&self) -> Encoding {
            Encoding::PLAIN
        }

        fn estimated_data_encoded_size(&self) -> usize {
            self.buffer.len() + self.bit_writer.bytes_written()
        }

        #[inline]
        fn flush_buffer(&mut self) -> Result<Bytes> {
            self.buffer
                .extend_from_slice(self.bit_writer.flush_buffer());
            self.bit_writer.clear();
            Ok(std::mem::take(&mut self.buffer).into())
        }

        #[inline]
        fn put(&mut self, values: &[T::T]) -> Result<()> {
            <T::T as PlainEncoderValue>::encode(values, &mut self.buffer, &mut self.bit_writer)?;
            Ok(())
        }

        /// Return the estimated memory size of this encoder.
        fn estimated_memory_size(&self) -> usize {
            self.buffer.capacity() * std::mem::size_of::<u8>()
                + self.bit_writer.estimated_memory_size()
        }
    }
}

pub(crate) use plain::PlainEncoderImpl;

// ----------------------------------------------------------------------
// RLE encoding

const DEFAULT_RLE_BUFFER_LEN: usize = 1024;

/// RLE/Bit-Packing hybrid encoding for values.
/// Currently is used only for data pages v2 and supports boolean types.
pub struct RleValueEncoder<T: DataType> {
    // Buffer with raw values that we collect,
    // when flushing buffer they are encoded using RLE encoder
    encoder: Option<RleEncoder>,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for RleValueEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> RleValueEncoder<T> {
    /// Creates new rle value encoder.
    pub fn new() -> Self {
        Self {
            encoder: None,
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for RleValueEncoder<T> {
    #[inline]
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        ensure_phys_ty!(Type::BOOLEAN, "RleValueEncoder only supports BoolType");

        let rle_encoder = self.encoder.get_or_insert_with(|| {
            let mut buffer = Vec::with_capacity(DEFAULT_RLE_BUFFER_LEN);
            // Reserve space for length
            buffer.extend_from_slice(&[0; 4]);
            RleEncoder::new_from_buf(1, buffer)
        });

        let mut buf = [0_u64; 64];
        for chunk in values.chunks(buf.len()) {
            let buf = &mut buf[..chunk.len()];
            for (b, value) in buf.iter_mut().zip(chunk) {
                *b = value.as_u64()?;
            }
            rle_encoder.put_batch(buf);
        }
        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::RLE
    }

    #[inline]
    fn estimated_data_encoded_size(&self) -> usize {
        match self.encoder {
            Some(ref enc) => enc.len(),
            None => 0,
        }
    }

    #[inline]
    fn flush_buffer(&mut self) -> Result<Bytes> {
        ensure_phys_ty!(Type::BOOLEAN, "RleValueEncoder only supports BoolType");
        let rle_encoder = self
            .encoder
            .take()
            .expect("RLE value encoder is not initialized");

        // Flush all encoder buffers and raw values
        let mut buf = rle_encoder.consume();
        assert!(buf.len() >= 4, "should have had padding inserted");

        // Note that buf does not have any offset, all data is encoded bytes
        let len = (buf.len() - 4) as i32;
        buf[..4].copy_from_slice(&len.to_le_bytes());

        Ok(buf.into())
    }

    /// return the estimated memory size of this encoder.
    fn estimated_memory_size(&self) -> usize {
        self.encoder
            .as_ref()
            .map_or(0, |enc| enc.estimated_memory_size())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "arrow")]
    use super::PlainEncoderImpl as PlainEncoder;
    #[cfg(feature = "arrow")]
    use super::boolean::BoolBatchSelection;
    use super::*;
    #[cfg(feature = "arrow")]
    use crate::column::value_selection::DictionaryKeys;

    use std::sync::Arc;

    use crate::encodings::decoding::{Decoder, DictDecoder, PlainDecoder, get_decoder};
    use crate::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type as SchemaType};
    use crate::util::bit_util;
    use crate::util::test_common::rand_gen::{RandGen, random_bytes};

    const TEST_SET_SIZE: usize = 1024;

    #[test]
    fn test_get_encoders() {
        // supported encodings
        create_and_check_encoder::<Int32Type>(0, Encoding::PLAIN, None);
        create_and_check_encoder::<Int32Type>(0, Encoding::DELTA_BINARY_PACKED, None);
        create_and_check_encoder::<BoolType>(0, Encoding::RLE, None);

        // error when initializing
        create_and_check_encoder::<Int32Type>(
            0,
            Encoding::RLE_DICTIONARY,
            Some(general_err!(
                "Cannot initialize this encoding through this function"
            )),
        );
        create_and_check_encoder::<Int32Type>(
            0,
            Encoding::PLAIN_DICTIONARY,
            Some(general_err!(
                "Cannot initialize this encoding through this function"
            )),
        );
        create_and_check_encoder::<Int32Type>(
            0,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Some(unsupported_column_encoding(
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
                Type::INT32,
            )),
        );
        create_and_check_encoder::<Int32Type>(
            0,
            Encoding::DELTA_BYTE_ARRAY,
            Some(unsupported_column_encoding(
                Encoding::DELTA_BYTE_ARRAY,
                Type::INT32,
            )),
        );
        create_and_check_encoder::<Int32Type>(
            0,
            Encoding::ALP,
            Some(general_err!(
                "Encoding ALP only supports FLOAT and DOUBLE, got INT32"
            )),
        );

        // unsupported
        #[expect(deprecated)]
        create_and_check_encoder::<Int32Type>(
            0,
            Encoding::BIT_PACKED,
            Some(nyi_err!("Encoding BIT_PACKED is not supported")),
        );
    }

    #[test]
    fn column_construction_validates_encoding_fallback_matrix() {
        use crate::file::properties::{WriterProperties, WriterVersion};
        use crate::file::writer::SerializedFileWriter;
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::sync::Arc;

        fn check_encoder<T: DataType>(encoding: Encoding, supported: bool) {
            let error = if supported {
                None
            } else {
                Some(match encoding {
                    Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                        general_err!("Cannot initialize this encoding through this function")
                    }
                    #[expect(deprecated, reason = "Verify the globally unsupported encoding")]
                    Encoding::BIT_PACKED => nyi_err!("Encoding BIT_PACKED is not supported"),
                    Encoding::ALP => general_err!(
                        "Encoding ALP only supports FLOAT and DOUBLE, got {}",
                        T::get_physical_type()
                    ),
                    _ => nyi_err!(
                        "Encoding {} is not supported for physical type {:?}",
                        encoding,
                        T::get_physical_type()
                    ),
                })
            };
            create_and_check_encoder::<T>(4, encoding, error);
        }

        let encodings = [
            Encoding::PLAIN,
            Encoding::RLE,
            Encoding::DELTA_BINARY_PACKED,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY,
            Encoding::BYTE_STREAM_SPLIT,
            Encoding::ALP,
        ];
        for physical in [
            Type::BOOLEAN,
            Type::INT32,
            Type::INT64,
            Type::INT96,
            Type::FLOAT,
            Type::DOUBLE,
            Type::BYTE_ARRAY,
            Type::FIXED_LEN_BYTE_ARRAY,
        ] {
            let valid: &[Encoding] = match physical {
                Type::BOOLEAN => &[Encoding::PLAIN, Encoding::RLE],
                Type::INT32 | Type::INT64 => &[
                    Encoding::PLAIN,
                    Encoding::DELTA_BINARY_PACKED,
                    Encoding::BYTE_STREAM_SPLIT,
                ],
                Type::INT96 => &[Encoding::PLAIN],
                Type::FLOAT | Type::DOUBLE => {
                    &[Encoding::PLAIN, Encoding::BYTE_STREAM_SPLIT, Encoding::ALP]
                }
                Type::BYTE_ARRAY => &[
                    Encoding::PLAIN,
                    Encoding::DELTA_LENGTH_BYTE_ARRAY,
                    Encoding::DELTA_BYTE_ARRAY,
                ],
                Type::FIXED_LEN_BYTE_ARRAY => &[
                    Encoding::PLAIN,
                    Encoding::DELTA_BYTE_ARRAY,
                    Encoding::BYTE_STREAM_SPLIT,
                ],
            };
            // Check the factory's returned errors directly, independently of the
            // infallible column writer constructor that unwraps those errors.
            // Also preserve globally unsupported and wrong-entry-point errors
            // across every family, including the byte-array implementation.
            #[expect(deprecated, reason = "Verify the globally unsupported encoding")]
            for encoding in encodings.into_iter().chain([
                Encoding::BIT_PACKED,
                Encoding::RLE_DICTIONARY,
                Encoding::PLAIN_DICTIONARY,
            ]) {
                let supported = valid.contains(&encoding);
                match physical {
                    Type::BOOLEAN => check_encoder::<BoolType>(encoding, supported),
                    Type::INT32 => check_encoder::<Int32Type>(encoding, supported),
                    Type::INT64 => check_encoder::<Int64Type>(encoding, supported),
                    Type::INT96 => check_encoder::<Int96Type>(encoding, supported),
                    Type::FLOAT => check_encoder::<FloatType>(encoding, supported),
                    Type::DOUBLE => check_encoder::<DoubleType>(encoding, supported),
                    Type::BYTE_ARRAY => check_encoder::<ByteArrayType>(encoding, supported),
                    Type::FIXED_LEN_BYTE_ARRAY => {
                        check_encoder::<FixedLenByteArrayType>(encoding, supported)
                    }
                }
            }
            for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
                for dictionary in [false, true] {
                    for encoding in encodings {
                        let field = Arc::new(
                            SchemaType::primitive_type_builder("a", physical)
                                .with_length(4)
                                .build()
                                .unwrap(),
                        );
                        let schema = Arc::new(
                            SchemaType::group_type_builder("schema")
                                .with_fields(vec![field])
                                .build()
                                .unwrap(),
                        );
                        let props = Arc::new(
                            WriterProperties::builder()
                                .set_writer_version(version)
                                .set_dictionary_enabled(dictionary)
                                .set_encoding(encoding)
                                .build(),
                        );
                        let created = catch_unwind(AssertUnwindSafe(|| {
                            let mut writer =
                                SerializedFileWriter::new(Vec::new(), schema, props).unwrap();
                            let mut group = writer.next_row_group().unwrap();
                            group.next_column().unwrap().unwrap();
                        }));
                        assert_eq!(
                            created.is_ok(),
                            valid.contains(&encoding),
                            "{physical:?} {version:?} dictionary={dictionary} {encoding:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_bool() {
        BoolType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        BoolType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        BoolType::test(Encoding::RLE, TEST_SET_SIZE, -1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_i32() {
        Int32Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        Int32Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        Int32Type::test(Encoding::DELTA_BINARY_PACKED, TEST_SET_SIZE, -1);
        Int32Type::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, -1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_i64() {
        Int64Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        Int64Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        Int64Type::test(Encoding::DELTA_BINARY_PACKED, TEST_SET_SIZE, -1);
        Int64Type::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, -1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_i96() {
        Int96Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        Int96Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_float() {
        FloatType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        FloatType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        FloatType::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, -1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_double() {
        DoubleType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        DoubleType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        DoubleType::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, -1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_byte_array() {
        ByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        ByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        ByteArrayType::test(Encoding::DELTA_LENGTH_BYTE_ARRAY, TEST_SET_SIZE, -1);
        ByteArrayType::test(Encoding::DELTA_BYTE_ARRAY, TEST_SET_SIZE, -1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_fixed_len_byte_array() {
        FixedLenByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, 100);
        FixedLenByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, 100);
        FixedLenByteArrayType::test(Encoding::DELTA_BYTE_ARRAY, TEST_SET_SIZE, 100);
        for width in [2, 3, 4, 5, 6, 7, 8, 100] {
            FixedLenByteArrayType::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, width);
        }
    }

    #[test]
    fn test_dict_encoded_size() {
        fn run_test<T: DataType>(type_length: i32, values: &[T::T], expected_size: usize) {
            let mut encoder = create_test_dict_encoder::<T>(type_length);
            assert_eq!(encoder.dict_encoded_size(), 0);
            encoder.put(values).unwrap();
            assert_eq!(encoder.dict_encoded_size(), expected_size);
            // We do not reset encoded size of the dictionary keys after flush_buffer
            encoder.flush_buffer().unwrap();
            assert_eq!(encoder.dict_encoded_size(), expected_size);
        }

        // Only 2 variations of values 1 byte each
        run_test::<BoolType>(-1, &[true, false, true, false, true], 2);
        run_test::<Int32Type>(-1, &[1i32, 2i32, 3i32, 4i32, 5i32], 20);
        run_test::<Int64Type>(-1, &[1i64, 2i64, 3i64, 4i64, 5i64], 40);
        run_test::<FloatType>(-1, &[1f32, 2f32, 3f32, 4f32, 5f32], 20);
        run_test::<DoubleType>(-1, &[1f64, 2f64, 3f64, 4f64, 5f64], 40);
        // Int96: len + reference
        run_test::<Int96Type>(
            -1,
            &[Int96::from(vec![1, 2, 3]), Int96::from(vec![2, 3, 4])],
            24,
        );
        run_test::<ByteArrayType>(-1, &[ByteArray::from("abcd"), ByteArray::from("efj")], 15);
        run_test::<FixedLenByteArrayType>(
            2,
            &[ByteArray::from("ab").into(), ByteArray::from("bc").into()],
            4,
        );
    }

    #[test]
    fn test_estimated_data_encoded_size() {
        fn run_test<T: DataType>(
            encoding: Encoding,
            type_length: i32,
            values: &[T::T],
            initial_size: usize,
            max_size: usize,
            flush_size: usize,
        ) {
            let mut encoder = match encoding {
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                    Box::new(create_test_dict_encoder::<T>(type_length))
                }
                _ => create_test_encoder::<T>(type_length, encoding),
            };
            assert_eq!(encoder.estimated_data_encoded_size(), initial_size);

            encoder.put(values).unwrap();
            assert_eq!(encoder.estimated_data_encoded_size(), max_size);

            encoder.flush_buffer().unwrap();
            assert_eq!(encoder.estimated_data_encoded_size(), flush_size);
        }

        // PLAIN
        run_test::<Int32Type>(Encoding::PLAIN, -1, &[123; 1024], 0, 4096, 0);

        // DICTIONARY
        // NOTE: The final size is almost the same because the dictionary entries are
        // preserved after encoded values have been written.
        run_test::<Int32Type>(Encoding::RLE_DICTIONARY, -1, &[123, 1024], 1, 3, 1);

        // DELTA_BINARY_PACKED
        run_test::<Int32Type>(Encoding::DELTA_BINARY_PACKED, -1, &[123; 1024], 0, 35, 0);

        // RLE
        let mut values = vec![];
        values.extend_from_slice(&[true; 16]);
        values.extend_from_slice(&[false; 16]);
        run_test::<BoolType>(Encoding::RLE, -1, &values, 0, 6, 0);

        // DELTA_LENGTH_BYTE_ARRAY
        run_test::<ByteArrayType>(
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            -1,
            &[ByteArray::from("ab"), ByteArray::from("abc")],
            0,
            5, // only value bytes, length encoder is not flushed yet
            0,
        );

        // DELTA_BYTE_ARRAY
        run_test::<ByteArrayType>(
            Encoding::DELTA_BYTE_ARRAY,
            -1,
            &[ByteArray::from("ab"), ByteArray::from("abc")],
            0,
            3, // only suffix bytes, length encoder is not flushed yet
            0,
        );

        // BYTE_STREAM_SPLIT
        run_test::<FloatType>(Encoding::BYTE_STREAM_SPLIT, -1, &[0.1, 0.2], 0, 8, 0);
    }

    #[test]
    fn test_byte_stream_split_example_f32() {
        // Test data from https://github.com/apache/parquet-format/blob/2a481fe1aad64ff770e21734533bb7ef5a057dac/Encodings.md#byte-stream-split-byte_stream_split--9
        let mut encoder = create_test_encoder::<FloatType>(0, Encoding::BYTE_STREAM_SPLIT);
        let mut decoder = create_test_decoder::<FloatType>(0, Encoding::BYTE_STREAM_SPLIT);

        let input = vec![
            f32::from_le_bytes([0xAA, 0xBB, 0xCC, 0xDD]),
            f32::from_le_bytes([0x00, 0x11, 0x22, 0x33]),
            f32::from_le_bytes([0xA3, 0xB4, 0xC5, 0xD6]),
        ];

        encoder.put(&input).unwrap();
        let encoded = encoder.flush_buffer().unwrap();

        assert_eq!(
            encoded,
            Bytes::from(vec![
                0xAA_u8, 0x00, 0xA3, 0xBB, 0x11, 0xB4, 0xCC, 0x22, 0xC5, 0xDD, 0x33, 0xD6
            ])
        );

        let mut decoded = vec![0.0; input.len()];
        decoder.set_data(encoded, input.len()).unwrap();
        decoder.get(&mut decoded).unwrap();

        assert_eq!(decoded, input);
    }

    // See: https://github.com/sunchao/parquet-rs/issues/47
    #[test]
    fn test_issue_47() {
        let mut encoder = create_test_encoder::<ByteArrayType>(0, Encoding::DELTA_BYTE_ARRAY);
        let mut decoder = create_test_decoder::<ByteArrayType>(0, Encoding::DELTA_BYTE_ARRAY);

        let input = vec![
            ByteArray::from("aa"),
            ByteArray::from("aaa"),
            ByteArray::from("aa"),
            ByteArray::from("aaa"),
        ];

        let mut output = vec![ByteArray::default(); input.len()];

        let mut result = put_and_get(&mut encoder, &mut decoder, &input[..2], &mut output[..2]);
        assert!(
            result.is_ok(),
            "first put_and_get() failed with: {}",
            result.unwrap_err()
        );
        result = put_and_get(&mut encoder, &mut decoder, &input[2..], &mut output[2..]);
        assert!(
            result.is_ok(),
            "second put_and_get() failed with: {}",
            result.unwrap_err()
        );
        assert_eq!(output, input);
    }

    trait EncodingTester<T: DataType> {
        fn test(enc: Encoding, total: usize, type_length: i32) {
            let result = match enc {
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                    Self::test_dict_internal(total, type_length)
                }
                enc => Self::test_internal(enc, total, type_length),
            };

            assert!(
                result.is_ok(),
                "Expected result to be OK but got err:\n {}",
                result.unwrap_err()
            );
        }

        fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()>;

        fn test_dict_internal(total: usize, type_length: i32) -> Result<()>;
    }

    impl<T: DataType + RandGen<T>> EncodingTester<T> for T {
        fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()> {
            let mut encoder = create_test_encoder::<T>(type_length, enc);
            let mut decoder = create_test_decoder::<T>(type_length, enc);
            let mut values = <T as RandGen<T>>::gen_vec(type_length, total);
            let mut result_data = vec![T::T::default(); total];

            // Test put/get spaced.
            let num_bytes = bit_util::ceil(total as i64, 8);
            let valid_bits = random_bytes(num_bytes as usize);
            let values_written = encoder.put_spaced(&values[..], &valid_bits[..])?;
            let data = encoder.flush_buffer()?;
            decoder.set_data(data, values_written)?;
            let _ = decoder.get_spaced(
                &mut result_data[..],
                values.len() - values_written,
                &valid_bits[..],
            )?;

            // Check equality
            for i in 0..total {
                if bit_util::get_bit(&valid_bits[..], i) {
                    assert_eq!(result_data[i], values[i]);
                } else {
                    assert_eq!(result_data[i], T::T::default());
                }
            }

            let mut actual_total = put_and_get(
                &mut encoder,
                &mut decoder,
                &values[..],
                &mut result_data[..],
            )?;
            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            // Encode more data after flush and test with decoder

            values = <T as RandGen<T>>::gen_vec(type_length, total);
            actual_total = put_and_get(
                &mut encoder,
                &mut decoder,
                &values[..],
                &mut result_data[..],
            )?;
            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            Ok(())
        }

        fn test_dict_internal(total: usize, type_length: i32) -> Result<()> {
            let mut encoder = create_test_dict_encoder::<T>(type_length);
            let mut values = <T as RandGen<T>>::gen_vec(type_length, total);
            encoder.put(&values[..])?;

            let mut data = encoder.flush_buffer()?;
            let mut decoder = create_test_dict_decoder::<T>();
            let mut dict_decoder = PlainDecoder::<T>::new(type_length);
            dict_decoder.set_data(encoder.write_dict()?, encoder.num_entries())?;
            decoder.set_dict(Box::new(dict_decoder))?;
            let mut result_data = vec![T::T::default(); total];
            decoder.set_data(data, total)?;
            let mut actual_total = decoder.get(&mut result_data)?;

            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            // Encode more data after flush and test with decoder

            values = <T as RandGen<T>>::gen_vec(type_length, total);
            encoder.put(&values[..])?;
            data = encoder.flush_buffer()?;

            let mut dict_decoder = PlainDecoder::<T>::new(type_length);
            dict_decoder.set_data(encoder.write_dict()?, encoder.num_entries())?;
            decoder.set_dict(Box::new(dict_decoder))?;
            decoder.set_data(data, total)?;
            actual_total = decoder.get(&mut result_data)?;

            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            Ok(())
        }
    }

    fn put_and_get<T: DataType>(
        encoder: &mut Box<dyn Encoder<T>>,
        decoder: &mut Box<dyn Decoder<T>>,
        input: &[T::T],
        output: &mut [T::T],
    ) -> Result<usize> {
        encoder.put(input)?;
        let data = encoder.flush_buffer()?;
        decoder.set_data(data, input.len())?;
        decoder.get(output)
    }

    fn create_and_check_encoder<T: DataType>(
        type_length: i32,
        encoding: Encoding,
        err: Option<ParquetError>,
    ) {
        let desc = create_test_col_desc_ptr(type_length, T::get_physical_type());
        let encoder = get_encoder::<T>(encoding, &desc);
        match err {
            Some(parquet_error) => {
                assert_eq!(
                    encoder.err().unwrap().to_string(),
                    parquet_error.to_string()
                )
            }
            None => assert_eq!(encoder.unwrap().encoding(), encoding),
        }
    }

    // Creates test column descriptor.
    fn create_test_col_desc_ptr(type_len: i32, t: Type) -> ColumnDescPtr {
        let ty = SchemaType::primitive_type_builder("t", t)
            .with_length(type_len)
            .build()
            .unwrap();
        Arc::new(ColumnDescriptor::new(
            Arc::new(ty),
            0,
            0,
            ColumnPath::new(vec![]),
        ))
    }

    fn create_test_encoder<T: DataType>(type_len: i32, enc: Encoding) -> Box<dyn Encoder<T>> {
        let desc = create_test_col_desc_ptr(type_len, T::get_physical_type());
        get_encoder(enc, &desc).unwrap()
    }

    fn create_test_decoder<T: DataType>(type_len: i32, enc: Encoding) -> Box<dyn Decoder<T>> {
        let desc = create_test_col_desc_ptr(type_len, T::get_physical_type());
        get_decoder(desc, enc).unwrap()
    }

    fn create_test_dict_encoder<T: DataType>(type_len: i32) -> DictEncoder<T> {
        let desc = create_test_col_desc_ptr(type_len, T::get_physical_type());
        DictEncoder::<T>::new(desc)
    }

    fn create_test_dict_decoder<T: DataType>() -> DictDecoder<T> {
        DictDecoder::<T>::new()
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn physical_boolean_identity_descriptors() {
        let bits = [0b1010_1101, 0b0111_0010, 0b1100_1001];
        let selection =
            PhysicalValueSelection::identity(ValueSelectionRef::Dense { offset: 3, len: 14 });
        let packed = BoolBatch::new_physical(&bits, 2, selection);

        assert!(matches!(packed.selection, BoolBatchSelection::Dense { .. }));
        assert_eq!(
            packed.true_count(),
            (3..17)
                .filter(|&index| bit_util::get_bit(&bits, 2 + index))
                .count()
        );

        let indices = [0, 2, 5, 9];
        let selection = PhysicalValueSelection::identity(ValueSelectionRef::Sparse(&indices));
        let packed = BoolBatch::new_physical(&bits, 2, selection);
        assert!(matches!(
            packed.selection,
            BoolBatchSelection::Sparse { .. }
        ));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn physical_boolean_dictionary_streams_sliced_keys_directly() {
        let bit_offset = 5;
        let bits = [0b1011_0101, 0b0101_1010];
        let mut keys = Vec::with_capacity(160);
        for row in 0..160 {
            keys.push((row % 6) as i16);
        }
        let selection = PhysicalValueSelection::dictionary(
            ValueSelectionRef::Dense {
                offset: 0,
                len: keys.len(),
            },
            DictionaryKeys::I16(&keys),
        )
        .slice(7, 137);
        let packed = BoolBatch::new_physical(&bits, bit_offset, selection);
        assert!(matches!(
            packed.selection,
            BoolBatchSelection::Physical { scalar: true, .. }
        ));

        let expected = (7..144)
            .map(|row| bit_util::get_bit(&bits, bit_offset + keys[row] as usize))
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        packed.for_each(|value| actual.push(value));
        assert_eq!(actual, expected);
        assert_eq!(
            packed.true_count(),
            expected.iter().filter(|&&value| value).count()
        );

        let mut encoder = PlainEncoder::<BoolType>::new();
        encoder.put_bool_batch(packed).unwrap();
        let encoded = encoder.flush_buffer().unwrap();
        assert_eq!(encoded.len(), expected.len().div_ceil(8));
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(bit_util::get_bit(&encoded, index), expected);
        }
    }
}
