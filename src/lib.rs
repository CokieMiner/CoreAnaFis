//! Native limb types and the parallel execution engine of multiprecision
//! arithmetic crates such as `ntt_anafis`.
//!
//! The crate is `no_std` and allocates nothing. The `rayon` feature links `std`
//! and Rayon.
//!
//! # Limbs
//!
//! [`Limb`] is the native unsigned word ([`usize`]); multiprecision operands
//! store limbs least-significant first. [`DoubleLimb`] holds a full product of
//! two limbs.
//!
//! # Execution
//!
//! A [`ParallelExecutor`] reports a planning budget, joins two independent
//! closures, and runs a closure where its joins need no transfer between threads.
//! [`DefaultExecutor::with_resolved`] resolves the built-in backend once per
//! complete operation: sequential execution by default, or the active Rayon pool
//! with the `rayon` feature.
//!
//! Outside Rayon workers, the first resolution attempts to initialize Rayon's
//! global pool at the physical-core count when that count is below the
//! available parallelism. `RAYON_NUM_THREADS`, an existing global pool, or an
//! unknown topology preserves Rayon's configuration. Linux topology comes from
//! sysfs sibling lists.
//!
//! ```
//! use core_anafis::{DefaultExecutor, ParallelExecutor};
//!
//! let (left, right) = DefaultExecutor::with_resolved(|executor| {
//!     executor.join(|| 2 + 3, || 4 * 5)
//! });
//! assert_eq!((left, right), (5, 20));
//! ```

#![cfg_attr(not(feature = "rayon"), no_std)]

#[cfg(test)]
extern crate alloc;

mod limb;
mod parallel;

pub use limb::{DoubleLimb, LIMB_BITS, LIMB_BYTES, Limb};
pub use parallel::{DefaultExecutor, ParallelExecutor, SequentialExecutor};
#[cfg(feature = "rayon")]
pub use parallel::{FixedParallelismExecutor, RayonExecutor};
