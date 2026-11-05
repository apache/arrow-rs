//! Statically typed implementations of Arrow Arrays
//!
//! **See [arrow_array] for examples and usage instructions**

// --------------------- Array & ArrayData ---------------------
pub use arrow_array::builder::*;
pub use arrow_array::cast::*;
pub use arrow_array::iterator::*;
pub use arrow_array::*;
pub use arrow_data::{
    ArrayData, ArrayDataBuilder, ArrayDataRef, BufferSpec, ByteView, DataTypeLayout,
    MAX_INLINE_VIEW_LEN, layout,
};

pub use arrow_data::transform::{Capacities, MutableArrayData};

// --------------------- Array's values comparison ---------------------
pub use arrow_ord::ord::{DynComparator, make_comparator};
