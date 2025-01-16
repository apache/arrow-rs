#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Enum representing checksum algorithms that may be used to verify object integrity.
pub enum ChecksumAlgorithm {
    /// MD5 algorithm.
    MD5,
}
