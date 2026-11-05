mod alp;
pub mod decoding;
pub mod encoding;
pub mod levels;

// Keep this module declaration explicit so rustfmt discovers its source file.
// See the comment in the crate root.
#[cfg(feature = "experimental")]
#[doc(hidden)]
pub mod rle;
#[cfg(not(feature = "experimental"))]
pub(crate) mod rle;
