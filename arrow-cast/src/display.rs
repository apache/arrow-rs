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

//! Functions for printing array values, as strings, for debugging
//! purposes. See the `pretty` crate for additional functions for
//! record batch pretty printing.

use std::fmt::{Debug, Display, Formatter, Write};
use std::ops::Range;

use arrow_array::cast::*;
use arrow_array::temporal_conversions::*;
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::*;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};

/// Options for formatting arrays
#[derive(Debug, Clone)]
pub struct FormatOptions<'a> {
    safe: bool,
    null: &'a str,
}

impl<'a> Default for FormatOptions<'a> {
    fn default() -> Self {
        Self {
            safe: true,
            null: "",
        }
    }
}

impl<'a> FormatOptions<'a> {
    /// If set to `true` any formatting errors will be written to the output
    /// instead of being converted into a [`std::fmt::Error`]
    pub fn with_display_error(mut self, safe: bool) -> Self {
        self.safe = safe;
        self
    }

    /// Overrides the string used to represent a null
    ///
    /// Defaults to `""`
    pub fn with_null(self, null: &'a str) -> Self {
        Self { null, ..self }
    }
}

/// Implements [`Display`] for a specific array value
pub struct ValueFormatter<'a> {
    idx: usize,
    formatter: &'a ArrayFormatter<'a>,
}

impl<'a> ValueFormatter<'a> {
    /// Writes this value to the provided `String`
    ///
    /// Note: this ignores [`FormatOptions::with_display_error`]
    pub fn write(&self, s: &mut String) -> Result<(), ArrowError> {
        match self.formatter.format.write(self.idx, s) {
            Ok(_) => Ok(()),
            Err(FormatError::Arrow(e)) => Err(e),
            Err(FormatError::Format(_)) => {
                unreachable!("formatting to string is infallible")
            }
        }
    }

    /// Fallibly converts this to a string
    pub fn try_to_string(&self) -> Result<String, ArrowError> {
        let mut s = String::new();
        self.write(&mut s)?;
        Ok(s)
    }
}

impl<'a> Display for ValueFormatter<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.formatter.format.write(self.idx, f) {
            Ok(()) => Ok(()),
            Err(FormatError::Arrow(e)) if self.formatter.safe => {
                write!(f, "ERROR: {}", e)
            }
            Err(_) => Err(std::fmt::Error),
        }
    }
}

/// A string formatter for an [`Array`]
pub struct ArrayFormatter<'a> {
    format: Box<dyn DisplayIndex + 'a>,
    safe: bool,
}

impl<'a> ArrayFormatter<'a> {
    /// Returns an [`ArrayFormatter`] that can be used to format `array`
    ///
    /// This returns an error if an array of the given data type cannot be formatted
    pub fn try_new(
        array: &'a dyn Array,
        options: &FormatOptions<'a>,
    ) -> Result<Self, ArrowError> {
        Ok(Self {
            format: make_formatter(array, options)?,
            safe: options.safe,
        })
    }

    /// Returns a [`ValueFormatter`] that implements [`Display`] for
    /// the value of the array at `idx`
    pub fn value(&self, idx: usize) -> ValueFormatter<'_> {
        ValueFormatter {
            formatter: self,
            idx,
        }
    }
}

fn make_formatter<'a>(
    array: &'a dyn Array,
    options: &FormatOptions<'a>,
) -> Result<Box<dyn DisplayIndex + 'a>, ArrowError> {
    downcast_primitive_array! {
        array => array_format(array, options),
        DataType::Null => array_format(as_null_array(array), options),
        DataType::Boolean => array_format(as_boolean_array(array), options),
        DataType::Utf8 => array_format(as_string_array(array), options),
        DataType::LargeUtf8 => array_format(as_largestring_array(array), options),
        DataType::Binary => array_format(as_generic_binary_array::<i32>(array), options),
        DataType::LargeBinary => array_format(as_generic_binary_array::<i64>(array), options),
        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            array => array_format(array, options),
            _ => unreachable!()
        }
        DataType::List(_) => array_format(as_generic_list_array::<i32>(array), options),
        DataType::LargeList(_) => array_format(as_generic_list_array::<i64>(array), options),
        DataType::FixedSizeList(_, _) => {
            let a = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            array_format(a, options)
        }
        DataType::Struct(_) => array_format(as_struct_array(array), options),
        DataType::Map(_, _) => array_format(as_map_array(array), options),
        DataType::Union(_, _, _) => array_format(as_union_array(array), options),
        d => Err(ArrowError::NotYetImplemented(format!("formatting {d} is not yet supported"))),
    }
}

/// Either an [`ArrowError`] or [`std::fmt::Error`]
enum FormatError {
    Format(std::fmt::Error),
    Arrow(ArrowError),
}

type FormatResult = Result<(), FormatError>;

impl From<std::fmt::Error> for FormatError {
    fn from(value: std::fmt::Error) -> Self {
        Self::Format(value)
    }
}

impl From<ArrowError> for FormatError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value)
    }
}

/// [`Display`] but accepting an index
trait DisplayIndex {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult;
}

/// [`DisplayIndex`] with additional state
trait DisplayIndexState<'a> {
    type State;

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError>;

    fn write(&self, state: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult;
}

impl<'a, T: DisplayIndex> DisplayIndexState<'a> for T {
    type State = ();

    fn prepare(&self, _options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        Ok(())
    }

    fn write(&self, _: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        DisplayIndex::write(self, idx, f)
    }
}

struct ArrayFormat<'a, F: DisplayIndexState<'a>> {
    state: F::State,
    array: F,
}

fn array_format<'a, F>(
    array: F,
    options: &FormatOptions<'a>,
) -> Result<Box<dyn DisplayIndex + 'a>, ArrowError>
where
    F: DisplayIndexState<'a> + Array + 'a,
{
    let state = array.prepare(options)?;
    Ok(Box::new(ArrayFormat { state, array }))
}

impl<'a, F: DisplayIndexState<'a> + Array> DisplayIndex for ArrayFormat<'a, F> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.array.is_null(idx) {
            return Ok(());
        }
        DisplayIndexState::write(&self.array, &self.state, idx, f)
    }
}

impl<'a> DisplayIndex for &'a BooleanArray {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", self.value(idx))?;
        Ok(())
    }
}

impl<'a> DisplayIndex for &'a NullArray {
    fn write(&self, _idx: usize, _f: &mut dyn Write) -> FormatResult {
        Ok(())
    }
}

macro_rules! primitive_display {
    ($($t:ty),+) => {
        $(impl<'a> DisplayIndex for &'a PrimitiveArray<$t>
        {
            fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
                write!(f, "{}", self.value(idx))?;
                Ok(())
            }
        })+
    };
}

primitive_display!(Int8Type, Int16Type, Int32Type, Int64Type);
primitive_display!(UInt8Type, UInt16Type, UInt32Type, UInt64Type);
primitive_display!(Float16Type, Float32Type, Float64Type);

macro_rules! decimal_display {
    ($($t:ty),+) => {
        $(impl<'a> DisplayIndexState<'a> for &'a PrimitiveArray<$t> {
            type State = (u8, i8);

            fn prepare(&self, _options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
                Ok((self.precision(), self.scale()))
            }

            fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
                write!(f, "{}", <$t>::format_decimal(self.values()[idx], s.0, s.1))?;
                Ok(())
            }
        })+
    };
}

decimal_display!(Decimal128Type, Decimal256Type);

macro_rules! timestamp_display {
    ($($t:ty),+) => {
        $(impl<'a> DisplayIndexState<'a> for &'a PrimitiveArray<$t> {
            type State = Option<Tz>;

            fn prepare(&self, _options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
                match self.data_type() {
                    DataType::Timestamp(_, tz) => tz.as_ref().map(|x| x.parse()).transpose(),
                    _ => unreachable!(),
                }
            }

            fn write(&self, tz: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
                let value = self.value(idx);
                let naive = as_datetime::<$t>(value).ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "Failed to convert {} to datetime for {}",
                        value,
                        self.data_type()
                    ))
                })?;

                match tz {
                    Some(tz) => {
                        let date = Utc.from_utc_datetime(&naive).with_timezone(tz);
                        write!(f, "{}", format_rfc3339(date))?;
                    }
                    None => write!(f, "{naive:?}")?,
                }
                Ok(())
            }
        })+
    };
}

timestamp_display!(
    TimestampSecondType,
    TimestampMillisecondType,
    TimestampMicrosecondType,
    TimestampNanosecondType
);

macro_rules! temporal_display {
    ($convert:ident, $t:ty) => {
        impl<'a> DisplayIndex for &'a PrimitiveArray<$t> {
            fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
                let value = self.value(idx);
                let naive = $convert(value as _).ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "Failed to convert {} to temporal for {}",
                        value,
                        self.data_type()
                    ))
                })?;

                write!(f, "{naive}")?;
                Ok(())
            }
        }
    };
}

impl<'a, O: OffsetSizeTrait> DisplayIndex for &'a GenericStringArray<O> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", self.value(idx))?;
        Ok(())
    }
}

impl<'a, O: OffsetSizeTrait> DisplayIndex for &'a GenericBinaryArray<O> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        let v = self.value(idx);
        for byte in v {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl<'a, K: ArrowDictionaryKeyType> DisplayIndexState<'a> for &'a DictionaryArray<K> {
    type State = Box<dyn DisplayIndex + 'a>;

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        make_formatter(self.values().as_ref(), options)
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let value_idx = self.keys().values()[idx].as_usize();
        s.as_ref().write(value_idx, f)
    }
}

fn write_list(
    f: &mut dyn Write,
    mut range: Range<usize>,
    values: &dyn DisplayIndex,
) -> FormatResult {
    f.write_char('[')?;
    if let Some(idx) = range.next() {
        values.write(idx, f)?;
    }
    for idx in range {
        write!(f, ", ")?;
        values.write(idx, f)?;
    }
    f.write_char(']')?;
    Ok(())
}

impl<'a, O: OffsetSizeTrait> DisplayIndexState<'a> for &'a GenericListArray<O> {
    type State = Box<dyn DisplayIndex + 'a>;

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        make_formatter(self.values().as_ref(), options)
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let offsets = self.value_offsets();
        let end = offsets[idx + 1].as_usize();
        let start = offsets[idx].as_usize();
        write_list(f, start..end, s.as_ref())
    }
}

impl<'a> DisplayIndexState<'a> for &'a FixedSizeListArray {
    type State = (usize, Box<dyn DisplayIndex + 'a>);

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        let values = make_formatter(self.values().as_ref(), options)?;
        let length = self.value_length();
        Ok((length as usize, values))
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let start = idx * s.0;
        let end = start + s.0;
        write_list(f, start..end, s.1.as_ref())
    }
}

/// Pairs a boxed [`DisplayIndex`] with its field name
type FieldDisplay<'a> = (&'a str, Box<dyn DisplayIndex + 'a>);

impl<'a> DisplayIndexState<'a> for &'a StructArray {
    type State = Vec<FieldDisplay<'a>>;

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        let fields = match (*self).data_type() {
            DataType::Struct(f) => f,
            _ => unreachable!(),
        };

        self.columns()
            .iter()
            .zip(fields)
            .map(|(a, f)| {
                let format = make_formatter(a.as_ref(), options)?;
                Ok((f.name().as_str(), format))
            })
            .collect()
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let mut iter = s.iter();
        if let Some((name, display)) = iter.next() {
            write!(f, "{name}: ")?;
            display.as_ref().write(idx, f)?;
        }
        for (name, display) in iter {
            write!(f, ", {name}: ")?;
            display.as_ref().write(idx, f)?;
        }
        Ok(())
    }
}

impl<'a> DisplayIndexState<'a> for &'a MapArray {
    type State = (Box<dyn DisplayIndex + 'a>, Box<dyn DisplayIndex + 'a>);

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        let keys = make_formatter(self.keys().as_ref(), &options)?;
        let values = make_formatter(self.values().as_ref(), options)?;
        Ok((keys, values))
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let offsets = self.value_offsets();
        let end = offsets[idx + 1].as_usize();
        let start = offsets[idx].as_usize();
        let mut iter = start..end;

        f.write_char('{')?;
        if let Some(idx) = iter.next() {
            s.0.write(idx, f)?;
            write!(f, ": ")?;
            s.1.write(idx, f)?;
        }

        for idx in iter {
            write!(f, ", ")?;
            s.0.write(idx, f)?;
            write!(f, ": ")?;
            s.1.write(idx, f)?;
        }

        f.write_char('}')?;
        Ok(())
    }
}

impl<'a> DisplayIndexState<'a> for &'a UnionArray {
    type State = (
        Vec<Option<(&'a str, Box<dyn DisplayIndex + 'a>)>>,
        UnionMode,
    );

    fn prepare(&self, options: &FormatOptions<'a>) -> Result<Self::State, ArrowError> {
        let (fields, type_ids, mode) = match (*self).data_type() {
            DataType::Union(fields, type_ids, mode) => (fields, type_ids, mode),
            _ => unreachable!(),
        };

        let max_id = type_ids.iter().copied().max().unwrap_or_default() as usize;
        let mut out: Vec<Option<FieldDisplay>> = (0..max_id + 1).map(|_| None).collect();
        for (i, field) in type_ids.iter().zip(fields) {
            let formatter = make_formatter(self.child(*i).as_ref(), options)?;
            out[*i as usize] = Some((field.name().as_str(), formatter))
        }
        Ok((out, *mode))
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let idx = match s.1 {
            UnionMode::Dense => self.value_offset(idx) as usize,
            UnionMode::Sparse => idx,
        };
        let id = self.type_id(idx);
        let (name, field) = s.0[id as usize].as_ref().unwrap();

        write!(f, "{{{name}=")?;
        field.write(idx, f)
    }
}

#[inline]
fn date32_to_date(value: i32) -> Option<NaiveDate> {
    Some(date32_to_datetime(value)?.date())
}

#[inline]
fn date64_to_date(value: i64) -> Option<NaiveDate> {
    Some(date64_to_datetime(value)?.date())
}

temporal_display!(date32_to_date, Date32Type);
temporal_display!(date64_to_date, Date64Type);
temporal_display!(time32s_to_time, Time32SecondType);
temporal_display!(time32ms_to_time, Time32MillisecondType);
temporal_display!(time64us_to_time, Time64MicrosecondType);
temporal_display!(time64ns_to_time, Time64NanosecondType);

macro_rules! duration_display {
    ($convert:ident, $t:ty) => {
        impl<'a> DisplayIndex for &'a PrimitiveArray<$t> {
            fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
                write!(f, "{}", $convert(self.value(idx)))?;
                Ok(())
            }
        }
    };
}

duration_display!(duration_s_to_duration, DurationSecondType);
duration_display!(duration_ms_to_duration, DurationMillisecondType);
duration_display!(duration_us_to_duration, DurationMicrosecondType);
duration_display!(duration_ns_to_duration, DurationNanosecondType);

impl<'a> DisplayIndex for &'a PrimitiveArray<IntervalYearMonthType> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        let interval = self.value(idx) as f64;
        let years = (interval / 12_f64).floor();
        let month = interval - (years * 12_f64);

        write!(
            f,
            "{} years {} mons 0 days 0 hours 0 mins 0.00 secs",
            years, month,
        )?;
        Ok(())
    }
}

impl<'a> DisplayIndex for &'a PrimitiveArray<IntervalDayTimeType> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        let value: u64 = self.value(idx) as u64;

        let days_parts: i32 = ((value & 0xFFFFFFFF00000000) >> 32) as i32;
        let milliseconds_part: i32 = (value & 0xFFFFFFFF) as i32;

        let secs = milliseconds_part / 1_000;
        let mins = secs / 60;
        let hours = mins / 60;

        let secs = secs - (mins * 60);
        let mins = mins - (hours * 60);

        let milliseconds = milliseconds_part % 1_000;

        let secs_sign = if secs < 0 || milliseconds < 0 {
            "-"
        } else {
            ""
        };

        write!(
            f,
            "0 years 0 mons {} days {} hours {} mins {}{}.{:03} secs",
            days_parts,
            hours,
            mins,
            secs_sign,
            secs.abs(),
            milliseconds.abs(),
        )?;
        Ok(())
    }
}

impl<'a> DisplayIndex for &'a PrimitiveArray<IntervalMonthDayNanoType> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        let value: u128 = self.value(idx) as u128;

        let months_part: i32 =
            ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
        let days_part: i32 = ((value & 0xFFFFFFFF0000000000000000) >> 64) as i32;
        let nanoseconds_part: i64 = (value & 0xFFFFFFFFFFFFFFFF) as i64;

        let secs = nanoseconds_part / 1_000_000_000;
        let mins = secs / 60;
        let hours = mins / 60;

        let secs = secs - (mins * 60);
        let mins = mins - (hours * 60);

        let nanoseconds = nanoseconds_part % 1_000_000_000;

        let secs_sign = if secs < 0 || nanoseconds < 0 { "-" } else { "" };

        write!(
            f,
            "0 years {} mons {} days {} hours {} mins {}{}.{:09} secs",
            months_part,
            days_part,
            hours,
            mins,
            secs_sign,
            secs.abs(),
            nanoseconds.abs(),
        )?;
        Ok(())
    }
}

fn format_rfc3339(date: DateTime<Tz>) -> impl Display {
    use chrono::format::*;

    const ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Literal("-"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Literal("-"),
        Item::Numeric(Numeric::Day, Pad::Zero),
        Item::Literal("T"),
        Item::Numeric(Numeric::Hour, Pad::Zero),
        Item::Literal(":"),
        Item::Numeric(Numeric::Minute, Pad::Zero),
        Item::Literal(":"),
        Item::Numeric(Numeric::Second, Pad::Zero),
        Item::Fixed(Fixed::Nanosecond),
        Item::Fixed(Fixed::TimezoneOffsetColonZ),
    ];
    date.format_with_items(ITEMS.iter())
}

/// Get the value at the given row in an array as a String.
///
/// Note this function is quite inefficient and is unlikely to be
/// suitable for converting large arrays or record batches.
pub fn array_value_to_string(
    column: &ArrayRef,
    row: usize,
) -> Result<String, ArrowError> {
    let options = FormatOptions::default().with_display_error(true);
    let formatter = ArrayFormatter::try_new(column.as_ref(), &options)?;
    Ok(formatter.value(row).to_string())
}

/// Converts numeric type to a `String`
pub fn lexical_to_string<N: lexical_core::ToLexical>(n: N) -> String {
    let mut buf = Vec::<u8>::with_capacity(N::FORMATTED_SIZE_DECIMAL);
    unsafe {
        // JUSTIFICATION
        //  Benefit
        //      Allows using the faster serializer lexical core and convert to string
        //  Soundness
        //      Length of buf is set as written length afterwards. lexical_core
        //      creates a valid string, so doesn't need to be checked.
        let slice = std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity());
        let len = lexical_core::write(n, slice).len();
        buf.set_len(len);
        String::from_utf8_unchecked(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_map_arry_to_string() {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let values_data = UInt32Array::from(vec![0u32, 10, 20, 30, 40, 50, 60, 70]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[a, b, c], [d, e, f], [g, h]]
        let entry_offsets = [0, 3, 6, 8];

        let map_array = MapArray::new_from_strings(
            keys.clone().into_iter(),
            &values_data,
            &entry_offsets,
        )
        .unwrap();
        let param = Arc::new(map_array) as ArrayRef;
        assert_eq!(
            "{d: 30, e: 40, f: 50}",
            array_value_to_string(&param, 1).unwrap()
        );
    }

    #[test]
    fn test_array_value_to_string_duration() {
        let ns_array =
            Arc::new(DurationNanosecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(
            array_value_to_string(&ns_array, 0).unwrap(),
            "PT0.000000001S"
        );
        assert_eq!(array_value_to_string(&ns_array, 1).unwrap(), "");

        let us_array =
            Arc::new(DurationMicrosecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(array_value_to_string(&us_array, 0).unwrap(), "PT0.000001S");
        assert_eq!(array_value_to_string(&us_array, 1).unwrap(), "");

        let ms_array =
            Arc::new(DurationMillisecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(array_value_to_string(&ms_array, 0).unwrap(), "PT0.001S");
        assert_eq!(array_value_to_string(&ms_array, 1).unwrap(), "");

        let s_array =
            Arc::new(DurationSecondArray::from(vec![Some(1), None])) as ArrayRef;
        assert_eq!(array_value_to_string(&s_array, 0).unwrap(), "PT1S");
        assert_eq!(array_value_to_string(&s_array, 1).unwrap(), "");
    }
}
