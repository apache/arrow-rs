use crate::ArrowPrimitiveType;

/// A subtype of primitive type that represents numeric values.
pub trait ArrowNumericType: ArrowPrimitiveType {}

impl<T: ArrowPrimitiveType> ArrowNumericType for T {}
