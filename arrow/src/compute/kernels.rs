//! Computation kernels on Arrow Arrays

pub use arrow_arith::{aggregate, arithmetic, arity, bitwise, boolean, numeric, temporal};
pub use arrow_cast::cast;
pub use arrow_cast::parse as cast_utils;
pub use arrow_ord::{cmp, partition, rank, sort};
pub use arrow_select::{
    coalesce, concat, filter, interleave, merge, nullif, take, union_extract, window, zip,
};
pub use arrow_string::{concat_elements, length, regexp, substring};

/// Comparison kernels for `Array`s.
pub mod comparison {
    pub use arrow_ord::comparison::*;
    pub use arrow_string::like::*;
    pub use arrow_string::regexp::{regexp_is_match, regexp_is_match_scalar};
}
