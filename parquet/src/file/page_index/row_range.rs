

/// An page range
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Range {
    /// Its start
    pub start: usize,
    /// Its length
    pub length: usize,
}

impl Range {
    /// Create a new page range
    pub fn new(start: usize, length: usize) -> Self {
        Self { start, length }
    }
}
