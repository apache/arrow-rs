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
use std::io::Write;
use std::sync::Arc;

use crate::StructMode;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, FieldRef};
use half::f16;
use lexical_core::FormattedSize;
use serde::Serializer;

/// Configuration options for the JSON encoder.
#[derive(Debug, Clone, Default)]
pub struct EncoderOptions {
    /// Whether to include nulls in the output or elide them.
    explicit_nulls: bool,
    /// Whether to encode structs as JSON objects or JSON arrays of their values.
    struct_mode: StructMode,
    /// An optional hook for customizing encoding behavior.
    encoder_factory: Option<Arc<dyn EncoderFactory>>,
}

impl EncoderOptions {
    /// Set whether to include nulls in the output or elide them.
    pub fn with_explicit_nulls(mut self, explicit_nulls: bool) -> Self {
        self.explicit_nulls = explicit_nulls;
        self
    }

    /// Set whether to encode structs as JSON objects or JSON arrays of their values.
    pub fn with_struct_mode(mut self, struct_mode: StructMode) -> Self {
        self.struct_mode = struct_mode;
        self
    }

    /// Set an optional hook for customizing encoding behavior.
    pub fn with_encoder_factory(mut self, encoder_factory: Arc<dyn EncoderFactory>) -> Self {
        self.encoder_factory = Some(encoder_factory);
        self
    }

    /// Get whether to include nulls in the output or elide them.
    pub fn explicit_nulls(&self) -> bool {
        self.explicit_nulls
    }

    /// Get whether to encode structs as JSON objects or JSON arrays of their values.
    pub fn struct_mode(&self) -> StructMode {
        self.struct_mode
    }

    /// Get the optional hook for customizing encoding behavior.
    pub fn encoder_factory(&self) -> Option<&Arc<dyn EncoderFactory>> {
        self.encoder_factory.as_ref()
    }
}

/// A trait to create custom encoders for specific data types.
///
/// This allows overriding the default encoders for specific data types,
/// or adding new encoders for custom data types.
///
/// # Examples
///
/// ```
/// use std::io::Write;
/// use arrow_array::{ArrayAccessor, Array, BinaryArray, Float64Array, RecordBatch};
/// use arrow_array::cast::AsArray;
/// use arrow_schema::{DataType, Field, Schema, FieldRef};
/// use arrow_json::{writer::{WriterBuilder, JsonArray, NullableEncoder}, StructMode};
/// use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
/// use arrow_schema::ArrowError;
/// use std::sync::Arc;
/// use serde_json::json;
/// use serde_json::Value;
///
/// struct IntArrayBinaryEncoder<B> {
///     array: B,
/// }
///
/// impl<'a, B> Encoder for IntArrayBinaryEncoder<B>
/// where
///     B: ArrayAccessor<Item = &'a [u8]>,
/// {
///     fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
///         out.push(b'[');
///         let child = self.array.value(idx);
///         for (idx, byte) in child.iter().enumerate() {
///             write!(out, "{byte}").unwrap();
///             if idx < child.len() - 1 {
///                 out.push(b',');
///             }
///         }
///         out.push(b']');
///     }
/// }
///
/// #[derive(Debug)]
/// struct IntArayBinaryEncoderFactory;
///
/// impl EncoderFactory for IntArayBinaryEncoderFactory {
///     fn make_default_encoder<'a>(
///         &self,
///         _field: &'a FieldRef,
///         array: &'a dyn Array,
///         _options: &'a EncoderOptions,
///     ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
///         match array.data_type() {
///             DataType::Binary => {
///                 let array = array.as_binary::<i32>();
///                 let encoder = IntArrayBinaryEncoder { array };
///                 let array_encoder = Box::new(encoder) as Box<dyn Encoder + 'a>;
///                 let nulls = array.nulls().cloned();
///                 Ok(Some(NullableEncoder::new(array_encoder, nulls)))
///             }
///             _ => Ok(None),
///         }
///     }
/// }
///
/// let binary_array = BinaryArray::from_iter([Some(b"a".as_slice()), None, Some(b"b".as_slice())]);
/// let float_array = Float64Array::from(vec![Some(1.0), Some(2.3), None]);
/// let fields = vec![
///     Field::new("bytes", DataType::Binary, true),
///     Field::new("float", DataType::Float64, true),
/// ];
/// let batch = RecordBatch::try_new(
///     Arc::new(Schema::new(fields)),
///     vec![
///         Arc::new(binary_array) as Arc<dyn Array>,
///         Arc::new(float_array) as Arc<dyn Array>,
///     ],
/// )
/// .unwrap();
///
/// let json_value: Value = {
///     let mut buf = Vec::new();
///     let mut writer = WriterBuilder::new()
///         .with_encoder_factory(Arc::new(IntArayBinaryEncoderFactory))
///         .build::<_, JsonArray>(&mut buf);
///     writer.write_batches(&[&batch]).unwrap();
///     writer.finish().unwrap();
///     serde_json::from_slice(&buf).unwrap()
/// };
///
/// let expected = json!([
///     {"bytes": [97], "float": 1.0},
///     {"float": 2.3},
///     {"bytes": [98]},
/// ]);
///
/// assert_eq!(json_value, expected);
/// ```
pub trait EncoderFactory: std::fmt::Debug + Send + Sync {
    /// Make an encoder that overrides the default encoder for a specific field and array or provides an encoder for a custom data type.
    /// This can be used to override how e.g. binary data is encoded so that it is an encoded string or an array of integers.
    ///
    /// Note that the type of the field may not match the type of the array: for dictionary arrays unless the top-level dictionary is handled this
    /// will be called again for the keys and values of the dictionary, at which point the field type will still be the outer dictionary type but the
    /// array will have a different type.
    /// For example, `field`` might have the type `Dictionary(i32, Utf8)` but `array` will be `Utf8`.
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        _array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        Ok(None)
    }
}

/// An encoder + a null buffer.
/// This is packaged together into a wrapper struct to minimize dynamic dispatch for null checks.
pub struct NullableEncoder<'a> {
    encoder: Box<dyn Encoder + 'a>,
    nulls: Option<NullBuffer>,
}

impl<'a> NullableEncoder<'a> {
    /// Create a new encoder with a null buffer.
    pub fn new(encoder: Box<dyn Encoder + 'a>, nulls: Option<NullBuffer>) -> Self {
        Self { encoder, nulls }
    }

    /// Encode the value at index `idx` to `out`.
    pub fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.encoder.encode(idx, out)
    }

    /// Returns whether the value at index `idx` is null.
    pub fn is_null(&self, idx: usize) -> bool {
        self.nulls.as_ref().is_some_and(|nulls| nulls.is_null(idx))
    }

    /// Returns whether the encoder has any nulls.
    pub fn has_nulls(&self) -> bool {
        match self.nulls {
            Some(ref nulls) => nulls.null_count() > 0,
            None => false,
        }
    }
}

impl Encoder for NullableEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.encoder.encode(idx, out)
    }
}

/// A trait to format array values as JSON values
///
/// Nullability is handled by the caller to allow encoding nulls implicitly, i.e. `{}` instead of `{"a": null}`
pub trait Encoder {
    /// Encode the non-null value at index `idx` to `out`.
    ///
    /// The behaviour is unspecified if `idx` corresponds to a null index.
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>);
}

/// Creates an encoder for the given array and field.
///
/// This first calls the EncoderFactory if one is provided, and then falls back to the default encoders.
pub fn make_encoder<'a>(
    field: &'a FieldRef,
    array: &'a dyn Array,
    options: &'a EncoderOptions,
) -> Result<NullableEncoder<'a>, ArrowError> {
    macro_rules! primitive_helper {
        ($t:ty) => {{
            let array = array.as_primitive::<$t>();
            let nulls = array.nulls().cloned();
            NullableEncoder::new(Box::new(PrimitiveEncoder::new(array)), nulls)
        }};
    }

    if let Some(factory) = options.encoder_factory() {
        if let Some(encoder) = factory.make_default_encoder(field, array, options)? {
            return Ok(encoder);
        }
    }

    let nulls = array.nulls().cloned();
    let encoder = downcast_integer! {
        array.data_type() => (primitive_helper),
        DataType::Float16 => primitive_helper!(Float16Type),
        DataType::Float32 => primitive_helper!(Float32Type),
        DataType::Float64 => primitive_helper!(Float64Type),
        DataType::Boolean => {
            let array = array.as_boolean();
            NullableEncoder::new(Box::new(BooleanEncoder(array)), array.nulls().cloned())
        }
        DataType::Null => NullableEncoder::new(Box::new(NullEncoder), array.logical_nulls()),
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            NullableEncoder::new(Box::new(StringEncoder(array)), array.nulls().cloned())
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            NullableEncoder::new(Box::new(StringEncoder(array)), array.nulls().cloned())
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            NullableEncoder::new(Box::new(StringViewEncoder(array)), array.nulls().cloned())
        }
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            NullableEncoder::new(Box::new(ListEncoder::try_new(field, array, options)?), array.nulls().cloned())
        }
        DataType::LargeList(_) => {
            let array = array.as_list::<i64>();
            NullableEncoder::new(Box::new(ListEncoder::try_new(field, array, options)?), array.nulls().cloned())
        }
        DataType::FixedSizeList(_, _) => {
            let array = array.as_fixed_size_list();
            NullableEncoder::new(Box::new(FixedSizeListEncoder::try_new(field, array, options)?), array.nulls().cloned())
        }

        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            array => {
                NullableEncoder::new(Box::new(DictionaryEncoder::try_new(field, array, options)?), array.nulls().cloned())
            },
            _ => unreachable!()
        }

        DataType::Map(_, _) => {
            let array = array.as_map();
            NullableEncoder::new(Box::new(MapEncoder::try_new(field, array, options)?), array.nulls().cloned())
        }

        DataType::FixedSizeBinary(_) => {
            let array = array.as_fixed_size_binary();
            NullableEncoder::new(Box::new(BinaryEncoder::new(array)) as _, array.nulls().cloned())
        }

        DataType::Binary => {
            let array: &BinaryArray = array.as_binary();
            NullableEncoder::new(Box::new(BinaryEncoder::new(array)), array.nulls().cloned())
        }

        DataType::LargeBinary => {
            let array: &LargeBinaryArray = array.as_binary();
            NullableEncoder::new(Box::new(BinaryEncoder::new(array)), array.nulls().cloned())
        }

        DataType::Struct(fields) => {
            let array = array.as_struct();
            let encoders = fields.iter().zip(array.columns()).map(|(field, array)| {
                let encoder = make_encoder(field, array, options)?;
                Ok(FieldEncoder{
                    field: field.clone(),
                    encoder,
                })
            }).collect::<Result<Vec<_>, ArrowError>>()?;

            let encoder = StructArrayEncoder{
                encoders,
                explicit_nulls: options.explicit_nulls(),
                struct_mode: options.struct_mode(),
            };
            let nulls = array.nulls().cloned();
            NullableEncoder::new(Box::new(encoder) as Box<dyn Encoder + 'a>, nulls)
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            let options = FormatOptions::new().with_display_error(true);
            let formatter = JsonArrayFormatter::new(ArrayFormatter::try_new(array, &options)?);
            NullableEncoder::new(Box::new(RawArrayFormatter(formatter)) as Box<dyn Encoder + 'a>, nulls)
        }
        d => match d.is_temporal() {
            true => {
                // Note: the implementation of Encoder for ArrayFormatter assumes it does not produce
                // characters that would need to be escaped within a JSON string, e.g. `'"'`.
                // If support for user-provided format specifications is added, this assumption
                // may need to be revisited
                let options = FormatOptions::new().with_display_error(true);
                let formatter = ArrayFormatter::try_new(array, &options)?;
                let formatter = JsonArrayFormatter::new(formatter);
                NullableEncoder::new(Box::new(formatter) as Box<dyn Encoder + 'a>, nulls)
            }
            false => return Err(ArrowError::JsonError(format!(
                "Unsupported data type for JSON encoding: {:?}",
                d
            )))
        }
    };

    Ok(encoder)
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    let mut serializer = serde_json::Serializer::new(out);
    serializer.serialize_str(s).unwrap();
}

struct FieldEncoder<'a> {
    field: FieldRef,
    encoder: NullableEncoder<'a>,
}

impl FieldEncoder<'_> {
    fn is_null(&self, idx: usize) -> bool {
        self.encoder.is_null(idx)
    }
}

struct StructArrayEncoder<'a> {
    encoders: Vec<FieldEncoder<'a>>,
    explicit_nulls: bool,
    struct_mode: StructMode,
}

impl Encoder for StructArrayEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        match self.struct_mode {
            StructMode::ObjectOnly => out.push(b'{'),
            StructMode::ListOnly => out.push(b'['),
        }
        let mut is_first = true;
        // Nulls can only be dropped in explicit mode
        let drop_nulls = (self.struct_mode == StructMode::ObjectOnly) && !self.explicit_nulls;

        for field_encoder in self.encoders.iter_mut() {
            let is_null = field_encoder.is_null(idx);
            if is_null && drop_nulls {
                continue;
            }

            if !is_first {
                out.push(b',');
            }
            is_first = false;

            if self.struct_mode == StructMode::ObjectOnly {
                encode_string(field_encoder.field.name(), out);
                out.push(b':');
            }

            if is_null {
                out.extend_from_slice(b"null");
            } else {
                field_encoder.encoder.encode(idx, out);
            }
        }
        match self.struct_mode {
            StructMode::ObjectOnly => out.push(b'}'),
            StructMode::ListOnly => out.push(b']'),
        }
    }
}

trait PrimitiveEncode: ArrowNativeType {
    type Buffer;

    // Workaround https://github.com/rust-lang/rust/issues/61415
    fn init_buffer() -> Self::Buffer;

    /// Encode the primitive value as bytes, returning a reference to that slice.
    ///
    /// `buf` is temporary space that may be used
    fn encode(self, buf: &mut Self::Buffer) -> &[u8];
}

macro_rules! integer_encode {
    ($($t:ty),*) => {
        $(
            impl PrimitiveEncode for $t {
                type Buffer = [u8; Self::FORMATTED_SIZE];

                fn init_buffer() -> Self::Buffer {
                    [0; Self::FORMATTED_SIZE]
                }

                fn encode(self, buf: &mut Self::Buffer) -> &[u8] {
                    lexical_core::write(self, buf)
                }
            }
        )*
    };
}
integer_encode!(i8, i16, i32, i64, u8, u16, u32, u64);

macro_rules! float_encode {
    ($($t:ty),*) => {
        $(
            impl PrimitiveEncode for $t {
                type Buffer = [u8; Self::FORMATTED_SIZE];

                fn init_buffer() -> Self::Buffer {
                    [0; Self::FORMATTED_SIZE]
                }

                fn encode(self, buf: &mut Self::Buffer) -> &[u8] {
                    if self.is_infinite() || self.is_nan() {
                        b"null"
                    } else {
                        lexical_core::write(self, buf)
                    }
                }
            }
        )*
    };
}
float_encode!(f32, f64);

impl PrimitiveEncode for f16 {
    type Buffer = <f32 as PrimitiveEncode>::Buffer;

    fn init_buffer() -> Self::Buffer {
        f32::init_buffer()
    }

    fn encode(self, buf: &mut Self::Buffer) -> &[u8] {
        self.to_f32().encode(buf)
    }
}

struct PrimitiveEncoder<N: PrimitiveEncode> {
    values: ScalarBuffer<N>,
    buffer: N::Buffer,
}

impl<N: PrimitiveEncode> PrimitiveEncoder<N> {
    fn new<P: ArrowPrimitiveType<Native = N>>(array: &PrimitiveArray<P>) -> Self {
        Self {
            values: array.values().clone(),
            buffer: N::init_buffer(),
        }
    }
}

impl<N: PrimitiveEncode> Encoder for PrimitiveEncoder<N> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(self.values[idx].encode(&mut self.buffer));
    }
}

struct BooleanEncoder<'a>(&'a BooleanArray);

impl Encoder for BooleanEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        match self.0.value(idx) {
            true => out.extend_from_slice(b"true"),
            false => out.extend_from_slice(b"false"),
        }
    }
}

struct StringEncoder<'a, O: OffsetSizeTrait>(&'a GenericStringArray<O>);

impl<O: OffsetSizeTrait> Encoder for StringEncoder<'_, O> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        encode_string(self.0.value(idx), out);
    }
}

struct StringViewEncoder<'a>(&'a StringViewArray);

impl Encoder for StringViewEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        encode_string(self.0.value(idx), out);
    }
}

struct ListEncoder<'a, O: OffsetSizeTrait> {
    offsets: OffsetBuffer<O>,
    encoder: NullableEncoder<'a>,
}

impl<'a, O: OffsetSizeTrait> ListEncoder<'a, O> {
    fn try_new(
        field: &'a FieldRef,
        array: &'a GenericListArray<O>,
        options: &'a EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let encoder = make_encoder(field, array.values().as_ref(), options)?;
        Ok(Self {
            offsets: array.offsets().clone(),
            encoder,
        })
    }
}

impl<O: OffsetSizeTrait> Encoder for ListEncoder<'_, O> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let end = self.offsets[idx + 1].as_usize();
        let start = self.offsets[idx].as_usize();
        out.push(b'[');

        if self.encoder.has_nulls() {
            for idx in start..end {
                if idx != start {
                    out.push(b',')
                }
                if self.encoder.is_null(idx) {
                    out.extend_from_slice(b"null");
                } else {
                    self.encoder.encode(idx, out);
                }
            }
        } else {
            for idx in start..end {
                if idx != start {
                    out.push(b',')
                }
                self.encoder.encode(idx, out);
            }
        }
        out.push(b']');
    }
}

struct FixedSizeListEncoder<'a> {
    value_length: usize,
    encoder: NullableEncoder<'a>,
}

impl<'a> FixedSizeListEncoder<'a> {
    fn try_new(
        field: &'a FieldRef,
        array: &'a FixedSizeListArray,
        options: &'a EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let encoder = make_encoder(field, array.values().as_ref(), options)?;
        Ok(Self {
            encoder,
            value_length: array.value_length().as_usize(),
        })
    }
}

impl Encoder for FixedSizeListEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let start = idx * self.value_length;
        let end = start + self.value_length;
        out.push(b'[');
        if self.encoder.has_nulls() {
            for idx in start..end {
                if idx != start {
                    out.push(b',')
                }
                if self.encoder.is_null(idx) {
                    out.extend_from_slice(b"null");
                } else {
                    self.encoder.encode(idx, out);
                }
            }
        } else {
            for idx in start..end {
                if idx != start {
                    out.push(b',')
                }
                self.encoder.encode(idx, out);
            }
        }
        out.push(b']');
    }
}

struct DictionaryEncoder<'a, K: ArrowDictionaryKeyType> {
    keys: ScalarBuffer<K::Native>,
    encoder: NullableEncoder<'a>,
}

impl<'a, K: ArrowDictionaryKeyType> DictionaryEncoder<'a, K> {
    fn try_new(
        field: &'a FieldRef,
        array: &'a DictionaryArray<K>,
        options: &'a EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let encoder = make_encoder(field, array.values().as_ref(), options)?;

        Ok(Self {
            keys: array.keys().values().clone(),
            encoder,
        })
    }
}

impl<K: ArrowDictionaryKeyType> Encoder for DictionaryEncoder<'_, K> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.encoder.encode(self.keys[idx].as_usize(), out)
    }
}

/// A newtype wrapper around [`ArrayFormatter`] to keep our usage of it private and not implement `Encoder` for the public type
struct JsonArrayFormatter<'a> {
    formatter: ArrayFormatter<'a>,
}

impl<'a> JsonArrayFormatter<'a> {
    fn new(formatter: ArrayFormatter<'a>) -> Self {
        Self { formatter }
    }
}

impl Encoder for JsonArrayFormatter<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        // Should be infallible
        // Note: We are making an assumption that the formatter does not produce characters that require escaping
        let _ = write!(out, "{}", self.formatter.value(idx));
        out.push(b'"')
    }
}

/// A newtype wrapper around [`JsonArrayFormatter`] that skips surrounding the value with `"`
struct RawArrayFormatter<'a>(JsonArrayFormatter<'a>);

impl Encoder for RawArrayFormatter<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let _ = write!(out, "{}", self.0.formatter.value(idx));
    }
}

struct NullEncoder;

impl Encoder for NullEncoder {
    fn encode(&mut self, _idx: usize, _out: &mut Vec<u8>) {
        unreachable!()
    }
}

struct MapEncoder<'a> {
    offsets: OffsetBuffer<i32>,
    keys: NullableEncoder<'a>,
    values: NullableEncoder<'a>,
    explicit_nulls: bool,
}

impl<'a> MapEncoder<'a> {
    fn try_new(
        field: &'a FieldRef,
        array: &'a MapArray,
        options: &'a EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let values = array.values();
        let keys = array.keys();

        if !matches!(keys.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(ArrowError::JsonError(format!(
                "Only UTF8 keys supported by JSON MapArray Writer: got {:?}",
                keys.data_type()
            )));
        }

        let keys = make_encoder(field, keys, options)?;
        let values = make_encoder(field, values, options)?;

        // We sanity check nulls as these are currently not enforced by MapArray (#1697)
        if keys.has_nulls() {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in MapArray keys".to_string(),
            ));
        }

        if array.entries().nulls().is_some_and(|x| x.null_count() != 0) {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in MapArray entries".to_string(),
            ));
        }

        Ok(Self {
            offsets: array.offsets().clone(),
            keys,
            values,
            explicit_nulls: options.explicit_nulls(),
        })
    }
}

impl Encoder for MapEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let end = self.offsets[idx + 1].as_usize();
        let start = self.offsets[idx].as_usize();

        let mut is_first = true;

        out.push(b'{');

        for idx in start..end {
            let is_null = self.values.is_null(idx);
            if is_null && !self.explicit_nulls {
                continue;
            }

            if !is_first {
                out.push(b',');
            }
            is_first = false;

            self.keys.encode(idx, out);
            out.push(b':');

            if is_null {
                out.extend_from_slice(b"null");
            } else {
                self.values.encode(idx, out);
            }
        }
        out.push(b'}');
    }
}

/// New-type wrapper for encoding the binary types in arrow: `Binary`, `LargeBinary`
/// and `FixedSizeBinary` as hex strings in JSON.
struct BinaryEncoder<B>(B);

impl<'a, B> BinaryEncoder<B>
where
    B: ArrayAccessor<Item = &'a [u8]>,
{
    fn new(array: B) -> Self {
        Self(array)
    }
}

impl<'a, B> Encoder for BinaryEncoder<B>
where
    B: ArrayAccessor<Item = &'a [u8]>,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        for byte in self.0.value(idx) {
            // this write is infallible
            write!(out, "{byte:02x}").unwrap();
        }
        out.push(b'"');
    }
}
