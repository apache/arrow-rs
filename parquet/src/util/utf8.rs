//! [`check_valid_utf8`] validation function
use crate::errors::{ParquetError, Result};

/// Check that `val` is a valid UTF-8 sequence.
///
/// If the `simdutf8` feature is enabled, this function will use
/// SIMD-accelerated validation from the [`simdutf8`] crate. Otherwise, it will use
/// [`std::str::from_utf8`].
///
/// # Errors
///
/// Returns `Err::General` with a message compatible with [`std::str::from_utf8`] on failure.
///
/// # Example
/// ```
/// use parquet::utf8::check_valid_utf8;
/// assert!(check_valid_utf8(b"hello").is_ok());
/// assert!(check_valid_utf8(b"hello \xF0\x9F\x98\x8E").is_ok());
/// // invalid UTF-8
/// assert!(check_valid_utf8(b"hello \xF0\x9F\x98").is_err());
/// ```
///
/// [`simdutf8`]: https://crates.io/crates/simdutf8
#[inline(always)]
pub fn check_valid_utf8(val: &[u8]) -> Result<()> {
    #[cfg(feature = "simdutf8")]
    match simdutf8::basic::from_utf8(val) {
        Ok(_) => Ok(()),
        Err(_) => {
            // Use simdutf8::compat to return details about the decoding error
            let e = simdutf8::compat::from_utf8(val).unwrap_err();
            Err(general_err!("encountered non UTF-8 data: {}", e))
        }
    }
    #[cfg(not(feature = "simdutf8"))]
    match std::str::from_utf8(val) {
        Ok(_) => Ok(()),
        Err(e) => Err(general_err!("encountered non UTF-8 data: {}", e)),
    }
}
