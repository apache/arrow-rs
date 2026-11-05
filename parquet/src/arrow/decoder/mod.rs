//! Specialized decoders optimised for decoding to arrow format

mod delta_byte_array;
mod dictionary_index;

pub use delta_byte_array::DeltaByteArrayDecoder;
pub use dictionary_index::DictIndexDecoder;
