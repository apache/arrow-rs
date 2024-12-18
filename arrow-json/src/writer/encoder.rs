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

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, FieldRef};
use half::f16;
use lexical_core::FormattedSize;
use serde::Serializer;
use std::io::Write;

#[derive(Debug, Clone, Default)]
pub struct EncoderOptions {
    pub explicit_nulls: bool,
}

/// A trait to format array values as JSON values
///
/// Nullability is handled by the caller to allow encoding nulls implicitly, i.e. `{}` instead of `{"a": null}`
pub trait Encoder {
    /// Encode the non-null value at index `idx` to `out`
    ///
    /// The behaviour is unspecified if `idx` corresponds to a null index
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>);
}

pub fn make_encoder<'a>(
    array: &'a dyn Array,
    options: &EncoderOptions,
) -> Result<Box<dyn Encoder + 'a>, ArrowError> {
    let (encoder, nulls) = make_encoder_impl(array, options)?;
    assert!(nulls.is_none(), "root cannot be nullable");
    Ok(encoder)
}

fn make_encoder_impl<'a>(
    array: &'a dyn Array,
    options: &EncoderOptions,
) -> Result<(Box<dyn Encoder + 'a>, Option<NullBuffer>), ArrowError> {
    macro_rules! primitive_helper {
        ($t:ty) => {{
            let array = array.as_primitive::<$t>();
            let nulls = array.nulls().cloned();
            (Box::new(PrimitiveEncoder::new(array)) as _, nulls)
        }};
    }

    Ok(downcast_integer! {
        array.data_type() => (primitive_helper),
        DataType::Float16 => primitive_helper!(Float16Type),
        DataType::Float32 => primitive_helper!(Float32Type),
        DataType::Float64 => primitive_helper!(Float64Type),
        DataType::Boolean => {
            let array = array.as_boolean();
            (Box::new(BooleanEncoder(array)), array.nulls().cloned())
        }
        DataType::Null => (Box::new(NullEncoder), array.logical_nulls()),
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            (Box::new(StringEncoder(array)) as _, array.nulls().cloned())
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            (Box::new(StringEncoder(array)) as _, array.nulls().cloned())
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            (Box::new(StringViewEncoder(array)) as _, array.nulls().cloned())
        }
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            (Box::new(ListEncoder::try_new(array, options)?) as _, array.nulls().cloned())
        }
        DataType::LargeList(_) => {
            let array = array.as_list::<i64>();
            (Box::new(ListEncoder::try_new(array, options)?) as _, array.nulls().cloned())
        }
        DataType::FixedSizeList(_, _) => {
            let array = array.as_fixed_size_list();
            (Box::new(FixedSizeListEncoder::try_new(array, options)?) as _, array.nulls().cloned())
        }

        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            array => (Box::new(DictionaryEncoder::try_new(array, options)?) as _,  array.logical_nulls()),
            _ => unreachable!()
        }

        DataType::Map(_, _) => {
            let array = array.as_map();
            (Box::new(MapEncoder::try_new(array, options)?) as _,  array.nulls().cloned())
        }

        DataType::FixedSizeBinary(_) => {
            let array = array.as_fixed_size_binary();
            (Box::new(BinaryEncoder::new(array)) as _, array.nulls().cloned())
        }

        DataType::Binary => {
            let array: &BinaryArray = array.as_binary();
            (Box::new(BinaryEncoder::new(array)) as _, array.nulls().cloned())
        }

        DataType::LargeBinary => {
            let array: &LargeBinaryArray = array.as_binary();
            (Box::new(BinaryEncoder::new(array)) as _, array.nulls().cloned())
        }

        DataType::Struct(fields) => {
            let array = array.as_struct();
            let encoders = fields.iter().zip(array.columns()).map(|(field, array)| {
                let (encoder, nulls) = make_encoder_impl(array, options)?;
                Ok(FieldEncoder{
                    field: field.clone(),
                    encoder, nulls
                })
            }).collect::<Result<Vec<_>, ArrowError>>()?;

            let encoder = StructArrayEncoder{
                encoders,
                explicit_nulls: options.explicit_nulls,
            };
            (Box::new(encoder) as _, array.nulls().cloned())
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            let options = FormatOptions::new().with_display_error(true);
            let formatter = ArrayFormatter::try_new(array, &options)?;
            (Box::new(RawArrayFormatter(formatter)) as _, array.nulls().cloned())
        }
        d => match d.is_temporal() {
            true => {
                // Note: the implementation of Encoder for ArrayFormatter assumes it does not produce
                // characters that would need to be escaped within a JSON string, e.g. `'"'`.
                // If support for user-provided format specifications is added, this assumption
                // may need to be revisited
                let options = FormatOptions::new().with_display_error(true);
                let formatter = ArrayFormatter::try_new(array, &options)?;
                (Box::new(formatter) as _, array.nulls().cloned())
            }
            false => return Err(ArrowError::InvalidArgumentError(format!("JSON Writer does not support data type: {d}"))),
        }
    })
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    let mut serializer = serde_json::Serializer::new(out);
    serializer.serialize_str(s).unwrap();
}

struct FieldEncoder<'a> {
    field: FieldRef,
    encoder: Box<dyn Encoder + 'a>,
    nulls: Option<NullBuffer>,
}

struct StructArrayEncoder<'a> {
    encoders: Vec<FieldEncoder<'a>>,
    explicit_nulls: bool,
}

/// This API is only stable since 1.70 so can't use it when current MSRV is lower
#[inline(always)]
fn is_some_and<T>(opt: Option<T>, f: impl FnOnce(T) -> bool) -> bool {
    match opt {
        None => false,
        Some(x) => f(x),
    }
}

impl Encoder for StructArrayEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'{');
        let mut is_first = true;
        for field_encoder in &mut self.encoders {
            let is_null = is_some_and(field_encoder.nulls.as_ref(), |n| n.is_null(idx));
            if is_null && !self.explicit_nulls {
                continue;
            }

            if !is_first {
                out.push(b',');
            }
            is_first = false;

            encode_string(field_encoder.field.name(), out);
            out.push(b':');

            match is_null {
                true => out.extend_from_slice(b"null"),
                false => field_encoder.encoder.encode(idx, out),
            }
        }
        out.push(b'}');
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
    nulls: Option<NullBuffer>,
    encoder: Box<dyn Encoder + 'a>,
}

impl<'a, O: OffsetSizeTrait> ListEncoder<'a, O> {
    fn try_new(
        array: &'a GenericListArray<O>,
        options: &EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let (encoder, nulls) = make_encoder_impl(array.values().as_ref(), options)?;
        Ok(Self {
            offsets: array.offsets().clone(),
            encoder,
            nulls,
        })
    }
}

impl<O: OffsetSizeTrait> Encoder for ListEncoder<'_, O> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let end = self.offsets[idx + 1].as_usize();
        let start = self.offsets[idx].as_usize();
        out.push(b'[');
        match self.nulls.as_ref() {
            Some(n) => (start..end).for_each(|idx| {
                if idx != start {
                    out.push(b',')
                }
                match n.is_null(idx) {
                    true => out.extend_from_slice(b"null"),
                    false => self.encoder.encode(idx, out),
                }
            }),
            None => (start..end).for_each(|idx| {
                if idx != start {
                    out.push(b',')
                }
                self.encoder.encode(idx, out);
            }),
        }
        out.push(b']');
    }
}

struct FixedSizeListEncoder<'a> {
    value_length: usize,
    nulls: Option<NullBuffer>,
    encoder: Box<dyn Encoder + 'a>,
}

impl<'a> FixedSizeListEncoder<'a> {
    fn try_new(
        array: &'a FixedSizeListArray,
        options: &EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let (encoder, nulls) = make_encoder_impl(array.values().as_ref(), options)?;
        Ok(Self {
            encoder,
            nulls,
            value_length: array.value_length().as_usize(),
        })
    }
}

impl Encoder for FixedSizeListEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let start = idx * self.value_length;
        let end = start + self.value_length;
        out.push(b'[');
        match self.nulls.as_ref() {
            Some(n) => (start..end).for_each(|idx| {
                if idx != start {
                    out.push(b',');
                }
                if n.is_null(idx) {
                    out.extend_from_slice(b"null");
                } else {
                    self.encoder.encode(idx, out);
                }
            }),
            None => (start..end).for_each(|idx| {
                if idx != start {
                    out.push(b',');
                }
                self.encoder.encode(idx, out);
            }),
        }
        out.push(b']');
    }
}

struct DictionaryEncoder<'a, K: ArrowDictionaryKeyType> {
    keys: ScalarBuffer<K::Native>,
    encoder: Box<dyn Encoder + 'a>,
}

impl<'a, K: ArrowDictionaryKeyType> DictionaryEncoder<'a, K> {
    fn try_new(
        array: &'a DictionaryArray<K>,
        options: &EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let (encoder, _) = make_encoder_impl(array.values().as_ref(), options)?;

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

impl Encoder for ArrayFormatter<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        // Should be infallible
        // Note: We are making an assumption that the formatter does not produce characters that require escaping
        let _ = write!(out, "{}", self.value(idx));
        out.push(b'"')
    }
}

/// A newtype wrapper around [`ArrayFormatter`] that skips surrounding the value with `"`
struct RawArrayFormatter<'a>(ArrayFormatter<'a>);

impl Encoder for RawArrayFormatter<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let _ = write!(out, "{}", self.0.value(idx));
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
    keys: Box<dyn Encoder + 'a>,
    values: Box<dyn Encoder + 'a>,
    value_nulls: Option<NullBuffer>,
    explicit_nulls: bool,
}

impl<'a> MapEncoder<'a> {
    fn try_new(array: &'a MapArray, options: &EncoderOptions) -> Result<Self, ArrowError> {
        let values = array.values();
        let keys = array.keys();

        if !matches!(keys.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(ArrowError::JsonError(format!(
                "Only UTF8 keys supported by JSON MapArray Writer: got {:?}",
                keys.data_type()
            )));
        }

        let (keys, key_nulls) = make_encoder_impl(keys, options)?;
        let (values, value_nulls) = make_encoder_impl(values, options)?;

        // We sanity check nulls as these are currently not enforced by MapArray (#1697)
        if is_some_and(key_nulls, |x| x.null_count() != 0) {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in MapArray keys".to_string(),
            ));
        }

        if is_some_and(array.entries().nulls(), |x| x.null_count() != 0) {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in MapArray entries".to_string(),
            ));
        }

        Ok(Self {
            offsets: array.offsets().clone(),
            keys,
            values,
            value_nulls,
            explicit_nulls: options.explicit_nulls,
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
            let is_null = is_some_and(self.value_nulls.as_ref(), |n| n.is_null(idx));
            if is_null && !self.explicit_nulls {
                continue;
            }

            if !is_first {
                out.push(b',');
            }
            is_first = false;

            self.keys.encode(idx, out);
            out.push(b':');

            match is_null {
                true => out.extend_from_slice(b"null"),
                false => self.values.encode(idx, out),
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
