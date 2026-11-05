//! Types of shared memory region

mod offset;
pub use offset::*;
mod immutable;
pub use immutable::*;
mod mutable;
pub use mutable::*;
mod ops;
pub use ops::*;
mod scalar;
pub use scalar::*;
mod boolean;
pub use boolean::*;
mod null;
pub use null::*;
mod run;
pub use run::*;
