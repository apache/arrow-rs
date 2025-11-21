# ArrayData + Bitmap Layout Contract

This document specifies the invariants for `ArrayData`, offsets, and bitmap buffers (validity and condition bitmaps) in Arrow-rs. These rules are derived from the actual implementation and must be treated as ground truth for bitwise and null-handling operations like `nullif`.

## 1. Basics

- **Bit Numbering**: Bits are numbered starting from 0 as the least significant bit (LSB) of byte 0, then bit 1 as the next LSB, and so on, across bytes in little-endian order.
- **Validity Bitmap Invariant**:
  > For an `ArrayData` with `len = L`, `offset = O` (in elements), and validity bitmap buffer `B`:
  > Logical index `i` in `[0, L)` is valid iff `get_bit(B, O + i) == true`.
- **Null Count**: If `null_count` is set, it MUST equal the count of `false` bits in the validity bitmap over the logical range `[0, L)`.

## 2. Slicing

- **Slice Operation**: `array.slice(offset', len')` produces a new `ArrayData` with:
  - `len = len'`,
  - `offset = O + offset'`,
  - Shared underlying buffers (no copying).
- **Sliced Validity Invariant**:
  > For the sliced `ArrayData` with `len = L'`, `offset = O'`, and the same validity bitmap buffer `B`:
  > Logical index `i` in `[0, L')` is valid iff `get_bit(B, O' + i) == true`.

## 3. Constructing New ArrayData with a Fresh Validity Buffer

- **Case 1: Offset = 0 and New Validity Bitmap**:
  - Bit `i` in the validity bitmap `B'` corresponds directly to logical index `i` for `i` in `[0, L)`.
  - `null_count` MUST equal the number of `false` bits in `B'` over `[0, L)`, or be left unset for Arrow to compute.
- **Case 2: Reusing Existing Null Buffer with Offset**:
  - When reusing a `NullBuffer` from an existing array, the `offset` in the new `ArrayData` adjusts the logical-to-physical bit mapping as per the basic invariant.
  - Ensure the reused buffer's bits align with the new `len` and `offset`; `null_count` must reflect the new logical range.

## 4. `nullif`-Specific Contract

- **Condition Bitmap**: A condition bitmap `C` indicates elements to nullify: logical index `i` should be nulled if `C[i] == true`.
- **Validity Computation Rule**:
  > Given left validity `V` and condition `C` (both in logical index space):
  > `result_valid(i) = V(i) & !C(i)`.
- **Buffer Mapping**:
  - For left array with `offset_left`, bit index for `V(i)` is `offset_left + i`.
  - For condition array with `offset_cond`, bit index for `C(i)` is `offset_cond + i`.
  - The result validity bitmap should be constructed with `offset = 0` and bits computed as above.

## 5. Do/Don't Guidelines

- **DO**: Centralize bitmap math in helpers that take `(buffer, bit_offset, len)` and return a new bitmap with `offset = 0`.
- **DO**: Ensure `null_count` matches the validity bitmap or leave it unset for automatic computation.
- **DON'T**: Manually slice buffers and add offsets (avoid double-compensation); use the slicing semantics instead.

## 6. Nullif Implementation Contract

For any ArrayData:

len = data.len()            // logical elements
offset = data.offset()      // logical starting index into buffers

Validity bitmap (if present) is a Buffer B.

Invariant:
  Logical index i in [0, len) is valid iff get_bit(B, offset + i) == true.

For the result of nullif:

We will build a fresh ArrayData with offset = 0.

For that result:
  Logical index i is valid iff get_bit(result_validity, i) == true.
  Values buffer is laid out so element 0 is first result value, etc.

For nullif semantics:

Let V(i) = left is valid at i
    C(i) = condition "nullify at i" is true (depends on left, right, type)

Then:
  result_valid(i) = V(i) & !C(i)
  result_value(i) = left_value(i)    // when result_valid(i) == true