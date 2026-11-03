//! Native limb widths.
//!
//! `Limb` is `usize`, so limb arithmetic uses native registers on every
//! target. `DoubleLimb` has twice its width and holds limb products,
//! carries, and division intermediates.

use core::mem::size_of;

/// Native unsigned word storing one multiprecision limb.
pub type Limb = usize;

/// Unsigned integer with twice the width of [`Limb`].
#[cfg(target_pointer_width = "64")]
pub type DoubleLimb = u128;
/// Unsigned integer with twice the width of [`Limb`].
#[cfg(target_pointer_width = "32")]
pub type DoubleLimb = u64;
/// Unsigned integer with twice the width of [`Limb`].
#[cfg(target_pointer_width = "16")]
pub type DoubleLimb = u32;

/// Number of bits in a limb.
#[expect(
    clippy::as_conversions,
    reason = "Limb::BITS is at most 64 and fits usize on every pointer width"
)]
pub const LIMB_BITS: usize = Limb::BITS as usize;

/// Number of bytes in a limb.
pub const LIMB_BYTES: usize = size_of::<Limb>();
