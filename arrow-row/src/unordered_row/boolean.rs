use arrow_array::BooleanArray;
use arrow_buffer::{bit_util, BooleanBuffer, MutableBuffer, NullBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{DataType};
use super::fixed::{FixedLengthEncoding, split_off};

pub(super) const FIXED_SIZE: usize = 1;

// Inline always to make sure the other dedicated functions will have optimized away the valid
#[inline(always)]
fn encode_bool_and_null(mut value: bool, valid: bool) -> u8 {
  // if valid is false, set value to false
  // if valid is true take the value
  value = value & valid;

  let value_bit = value as u8;
  let valid_bit = valid as u8;

  // Doing shift on the valid bit and not on the value bit, so in case when there is no nulls we can avoid the shift and it will be optimized away
  valid_bit << 1 | value_bit
}

fn encode_bool_and_nullable_with_no_nulls(value: bool) -> u8 {
  encode_bool_and_null(value, true)
}

fn decode_null_and_bool(encoded: u8) -> (bool, bool) {
  // we know if the value is valid if it is not 0
  // as for invalid we set also the value bit to 0
  let is_valid = encoded != 0;
  let value = encoded & 1 == 1;

  (is_valid, value)
}

/// Boolean values are encoded as
///
/// - 1 byte `0` if null or `1` if valid
/// - bytes of [`crate::unordered_row::fixed::FixedLengthEncoding`]
pub fn encode_boolean(
  data: &mut [u8],
  offsets: &mut [usize],
  values: &BooleanBuffer,
  nulls: &NullBuffer,
) {
  for (idx, (value, is_valid)) in values.iter().zip(nulls.iter()).enumerate() {
    let offset = &mut offsets[idx + 1];
    data[*offset] = encode_bool_and_null(value, is_valid);
    *offset += 1;
  }
}

/// Encoding for non-nullable boolean arrays.
/// Iterates directly over `values`, and skips NULLs-checking.
pub fn encode_boolean_not_null(
  data: &mut [u8],
  offsets: &mut [usize],
  values: &BooleanBuffer,
) {
  for (idx, value) in values.iter().enumerate() {
    let offset = &mut offsets[idx + 1];
    data[*offset] = encode_bool_and_nullable_with_no_nulls(value);
    *offset += 1;
  }
}

/// Decodes a `BooleanArray` from rows
pub fn decode_bool(rows: &mut [&[u8]]) -> BooleanArray {
  let len = rows.len();

  let mut null_count = 0;
  let mut nulls = MutableBuffer::new(bit_util::ceil(len, 64) * 8);
  let mut values = MutableBuffer::new(bit_util::ceil(len, 64) * 8);

  let chunks = len / 64;
  let remainder = len % 64;
  for chunk in 0..chunks {
    let mut null_packed = 0;
    let mut values_packed = 0;

    for bit_idx in 0..64 {
      let i = split_off(&mut rows[bit_idx + chunk * 64], 1);
      let (is_valid, value) = decode_null_and_bool(i[0]);
      null_count += !is_valid as usize;
      null_packed |= (is_valid as u64) << bit_idx;
      values_packed |= (value as u64) << bit_idx;
    }

    nulls.push(null_packed);
    values.push(values_packed);
  }

  if remainder != 0 {
    let mut null_packed = 0;
    let mut values_packed = 0;

    for bit_idx in 0..remainder {
      let i = split_off(&mut rows[bit_idx + chunks * 64], 1);
      let (is_valid, value) = decode_null_and_bool(i[0]);
      null_count += !is_valid as usize;
      null_packed |= (is_valid as u64) << bit_idx;
      values_packed |= (value as u64) << bit_idx;
    }

    nulls.push(null_packed);
    values.push(values_packed);
  }

  let builder = ArrayDataBuilder::new(DataType::Boolean)
    .len(rows.len())
    .null_count(null_count)
    .add_buffer(values.into())
    .null_bit_buffer(Some(nulls.into()));

  // SAFETY:
  // Buffers are the correct length
  unsafe { BooleanArray::from(builder.build_unchecked()) }
}
