//! Host concurrency topology and the default worker-pool width.
//!
//! Outside Rayon workers, the default executor attempts global-pool
//! initialization once. It selects the physical-core count when that count is
//! below available parallelism. `RAYON_NUM_THREADS`, an existing global pool,
//! or unknown topology preserves Rayon's pool configuration.
//!
//! Linux topology comes from sysfs sibling lists. Other platforms report
//! unknown topology. Incomplete scans also report unknown topology.

use core::num::NonZeroUsize;
use std::{env::var_os, thread::available_parallelism};
#[cfg(target_os = "linux")]
use std::{
    fs::{read_dir, read_to_string},
    path::Path,
};

use rayon::ThreadPoolBuilder;

/// Attempts global-pool initialization at a smaller physical-core width.
///
/// The default executor calls this once per process, outside Rayon workers.
/// Explicit environment configuration, an existing pool, or unknown topology
/// prevents narrowing.
pub fn narrow_default_pool() {
    if var_os("RAYON_NUM_THREADS").is_some() {
        return;
    }
    let Ok(available) = available_parallelism() else {
        return;
    };
    #[cfg(target_os = "linux")]
    let detected = detect_physical_parallelism(Path::new("/sys/devices/system/cpu"));
    #[cfg(not(target_os = "linux"))]
    let detected: Option<NonZeroUsize> = None;
    let Some(physical) = detected else {
        return;
    };
    if physical >= available {
        return;
    }
    // Failed initialization leaves an existing pool unchanged.
    let _initialized = ThreadPoolBuilder::new()
        .num_threads(physical.get())
        .build_global()
        .is_ok();
}

/// Counts physical cores in a sysfs CPU directory.
///
/// Unreadable entries, missing sibling lists, invalid primary CPU indices, or
/// an unrepresentable core count report unknown topology.
#[cfg(target_os = "linux")]
pub fn detect_physical_parallelism(root: &Path) -> Option<NonZeroUsize> {
    let entries = read_dir(root).ok()?;
    let mut cores = 0_usize;
    for entry_result in entries {
        let entry = entry_result.ok()?;
        let file_name = entry.file_name();
        let Some(digits) = file_name.to_str().and_then(|text| text.strip_prefix("cpu")) else {
            continue;
        };
        if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
            continue;
        }
        // CPU indices outside the native width make the scan incomplete.
        let index = digits.parse::<usize>().ok()?;
        let siblings = entry.path().join("topology/thread_siblings_list");
        // A partial count cannot establish the process-global worker budget.
        let list = read_to_string(siblings).ok()?;
        // Sysfs lists sibling indices in ascending order. Only the first
        // sibling contributes to the physical-core count.
        let primary = list
            .split(',')
            .next()
            .and_then(|first| first.split('-').next())
            .and_then(|first| first.trim().parse::<usize>().ok())?;
        if primary == index {
            cores = cores.checked_add(1)?;
        }
    }
    NonZeroUsize::new(cores)
}
