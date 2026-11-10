//! Native limb types and the parallel execution engine of multiprecision
//! arithmetic crates such as `ntt_anafis`.
//!
//! The crate is `no_std` and allocates nothing by default. The `rayon` feature
//! links `std` and enables Rayon-backed parallelism.
//!
//! # Limbs
//!
//! [`Limb`] is the native unsigned register word ([`usize`]); multiprecision
//! operands store limbs least-significant first. [`DoubleLimb`] holds full
//! two-limb products, carries, and division intermediates. Constant bit and
//! byte widths are exposed as [`LIMB_BITS`] and [`LIMB_BYTES`].
//!
//! # Execution
//!
//! An [`Executor`] reports a planning budget ([`Executor::budget`]), joins two
//! independent closures ([`Executor::join`]), and optimizes multi-phase pool
//! execution ([`Executor::on_workers`]).
//!
//! - [`Sequential`] runs work synchronously on the calling thread.
//! - [`Rayon`] executes work across the active Rayon thread pool (requires `rayon`).
//! - [`Budgeted`] applies a fixed budget over any underlying executor.
//! - [`Budgeted::resolve`] provides the default executor for an operation:
//!   [`Sequential`] without Rayon, or the active/global [`Rayon`] pool when enabled.
//!
//! Outside Rayon workers, the first resolution initializes Rayon's global pool
//! at the detected physical-core count to avoid hyper-threading overhead, unless
//! overridden by `RAYON_NUM_THREADS` or an existing pool.
//!
//! # Feature Flags
//!
//! - **`rayon`**: Enables the [`Rayon`] executor and links `std`.
//!
//! # Examples
//!
//! ```
//! use core_anafis::{Budgeted, Executor};
//!
//! let (left, right) = Budgeted::resolve(|executor| {
//!     executor.join(|| 2 + 3, || 4 * 5)
//! });
//! assert_eq!((left, right), (5, 20));
//! ```

#![cfg_attr(not(feature = "rayon"), no_std)]

#[cfg(test)]
extern crate alloc;

mod limb;
mod parallel;

#[cfg(test)]
mod tests;

pub use limb::{DoubleLimb, LIMB_BITS, LIMB_BYTES, Limb};
#[cfg(feature = "rayon")]
pub use parallel::Rayon;
pub use parallel::{Budgeted, Executor, Sequential};
