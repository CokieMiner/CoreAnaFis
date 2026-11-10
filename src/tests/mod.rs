//! Executor contracts, panic propagation, global-pool lifecycle, and topology discovery.

mod executor;
#[cfg(feature = "rayon")]
mod topology;
