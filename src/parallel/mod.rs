//! Synchronous execution of independent arithmetic work.
//!
//! With `rayon`, independent subtasks use the active Rayon pool. Otherwise,
//! subtasks execute sequentially on the calling thread.

mod executor;
#[cfg(feature = "rayon")]
mod topology;

pub use executor::{DefaultExecutor, ParallelExecutor, SequentialExecutor};
#[cfg(feature = "rayon")]
pub use executor::{FixedParallelismExecutor, RayonExecutor};
#[cfg(feature = "rayon")]
pub use topology::narrow_default_pool;

#[cfg(test)]
mod tests;
