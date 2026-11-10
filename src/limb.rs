//! Native limb widths and word types.

use core::mem::size_of;

/// Native unsigned word storing one multiprecision limb.
///
/// Multi-limb numbers are stored as slices `&[Limb]` in least-significant-limb
/// first (little-endian) order.
///
/// # Examples
///
/// ```
/// use core_anafis::{Limb, LIMB_BYTES};
///
/// let x: Limb = 42;
/// assert_eq!(core::mem::size_of_val(&x), LIMB_BYTES);
/// ```
pub type Limb = usize;

/// Unsigned integer with twice the width of [`Limb`].
///
/// Holds the widened product of two limbs, division intermediates, or
/// accumulated sums with carry.
///
/// # Examples
///
/// ```
/// use core_anafis::{DoubleLimb, Limb};
///
/// let a: Limb = 1_000_000;
/// let b: Limb = 2_000_000;
/// let product: DoubleLimb = (a as DoubleLimb) * (b as DoubleLimb);
/// assert_eq!(product, 2_000_000_000_000);
/// ```
#[cfg(target_pointer_width = "64")]
pub type DoubleLimb = u128;
/// Unsigned integer with twice the width of [`Limb`].
#[cfg(target_pointer_width = "32")]
pub type DoubleLimb = u64;
/// Unsigned integer with twice the width of [`Limb`].
#[cfg(target_pointer_width = "16")]
pub type DoubleLimb = u32;

/// Number of bits in a [`Limb`].
///
/// # Examples
///
/// ```
/// use core_anafis::LIMB_BITS;
///
/// assert_eq!(LIMB_BITS, usize::BITS as usize);
/// ```
#[expect(
    clippy::as_conversions,
    reason = "Limb::BITS is at most 64 and fits usize on every pointer width"
)]
pub const LIMB_BITS: usize = Limb::BITS as usize;

/// Number of bytes in a [`Limb`].
///
/// # Examples
///
/// ```
/// use core_anafis::{Limb, LIMB_BYTES};
///
/// assert_eq!(LIMB_BYTES, core::mem::size_of::<Limb>());
/// ```
pub const LIMB_BYTES: usize = size_of::<Limb>();
