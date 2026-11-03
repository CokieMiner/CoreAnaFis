//! Executor contracts, global-pool initialization, and topology discovery.

mod contracts;
#[cfg(feature = "rayon")]
mod initialization;
mod panics;
#[cfg(all(feature = "rayon", target_os = "linux"))]
mod topology;
#[cfg(feature = "rayon")]
mod workers;
