//! Synchronous execution adapters and host concurrency topology.
//!
//! Adapters report a planning budget and join independent closures before
//! returning their results. The default backend uses the active Rayon pool
//! when `rayon` is enabled and sequential execution otherwise.

use core::num::NonZeroUsize;
#[cfg(all(feature = "rayon", target_os = "linux"))]
use core::ops::RangeInclusive;
#[cfg(feature = "rayon")]
use std::{env::var_os, sync::OnceLock, thread::available_parallelism};
#[cfg(all(feature = "rayon", target_os = "linux"))]
use std::{
    fs::{read_dir, read_to_string},
    path::Path,
};

#[cfg(feature = "rayon")]
use rayon::{ThreadPoolBuilder, current_num_threads, current_thread_index, join};

/// Schedules independent synchronous arithmetic work.
///
/// Implementations provide sequential ([`Sequential`]) or Rayon ([`Rayon`])
/// execution with the same join interface.
///
/// # Examples
///
/// ```
/// use core_anafis::{Budgeted, Executor};
///
/// fn parallel_sum<E: Executor>(executor: &E, slice: &[u32]) -> u32 {
///     if slice.len() <= 2 {
///         return slice.iter().sum();
///     }
///     let (left, right) = slice.split_at(slice.len() / 2);
///     let (a, b) = executor.join(|| parallel_sum(executor, left), || parallel_sum(executor, right));
///     a + b
/// }
///
/// let data = [1, 2, 3, 4, 5, 6, 7, 8];
/// let sum = Budgeted::resolve(|executor| parallel_sum(executor, &data));
/// assert_eq!(sum, 36);
/// ```
pub trait Executor: Sync {
    /// Returns the planning budget, the most tasks that scratch is sized for.
    ///
    /// Algorithms partition this budget when subdividing work and scratch. The
    /// executor reserves no worker thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use core_anafis::{Executor, Sequential};
    ///
    /// assert_eq!(Sequential.budget().get(), 1);
    /// ```
    fn budget(&self) -> NonZeroUsize;

    /// Executes two independent closures and returns their results.
    ///
    /// # Examples
    ///
    /// ```
    /// use core_anafis::{Executor, Sequential};
    ///
    /// let (left, right) = Sequential.join(|| 2 + 2, || 3 * 3);
    /// assert_eq!((left, right), (4, 9));
    /// ```
    ///
    /// # Panics
    ///
    /// Propagates a closure panic after all started work has stopped. A
    /// sequential executor does not start `right` if `left` panics.
    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send;

    /// Runs `op` where its joins need no transfer between threads, and returns
    /// its result.
    ///
    /// A join from outside a Rayon pool wakes a worker and blocks the caller.
    /// An operation with several parallel phases enters the pool once through
    /// this method instead of once per phase. A sequential executor runs `op` in
    /// place.
    ///
    /// # Examples
    ///
    /// ```
    /// use core_anafis::{Executor, Sequential};
    ///
    /// let value = Sequential.on_workers(|| 100);
    /// assert_eq!(value, 100);
    /// ```
    ///
    /// # Panics
    ///
    /// Propagates a panic of `op`.
    #[inline]
    fn on_workers<R, F>(&self, op: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        op()
    }
}

/// Executes closures sequentially on the calling thread.
///
/// # Examples
///
/// ```
/// use core_anafis::{Executor, Sequential};
///
/// let (a, b) = Sequential.join(|| 1, || 2);
/// assert_eq!((a, b), (1, 2));
/// assert_eq!(Sequential.budget().get(), 1);
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Sequential;

/// Executes closures in the active Rayon pool.
///
/// Requires the `rayon` feature flag.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "rayon")]
/// # {
/// use core_anafis::{Executor, Rayon};
///
/// let (a, b) = Rayon.join(|| 10, || 20);
/// assert_eq!((a, b), (10, 20));
/// # }
/// ```
#[cfg(feature = "rayon")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Rayon;

/// Reports a fixed planning budget over a borrowed executor.
///
/// A budget of one executes joins sequentially. Larger budgets forward joins
/// to the underlying executor without reserving or creating workers.
///
/// # Examples
///
/// ```
/// use core::num::NonZeroUsize;
/// use core_anafis::{Budgeted, Executor, Sequential};
///
/// let budget = NonZeroUsize::new(4).expect("positive test budget");
/// let budgeted = Budgeted::new(&Sequential, budget);
/// assert_eq!(budgeted.budget().get(), 4);
/// ```
#[derive(Debug)]
pub struct Budgeted<'executor, E: ?Sized> {
    executor: &'executor E,
    budget: NonZeroUsize,
}

// A shared reference and a budget copy for every executor; derived impls would
// require `E: Clone`.
impl<E: ?Sized> Clone for Budgeted<'_, E> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<E: ?Sized> Copy for Budgeted<'_, E> {}

#[cfg(feature = "rayon")]
impl Budgeted<'static, Rayon> {
    /// Runs `action` with the built-in executor, resolved once for a complete
    /// arithmetic operation.
    ///
    /// The executor is the active Rayon pool, and its budget is the pool's width.
    /// Outside Rayon workers, the first call starts the global pool at the
    /// physical-core count unless it is configured or running already.
    ///
    /// # Examples
    ///
    /// ```
    /// use core_anafis::{Budgeted, Executor};
    ///
    /// let answer = Budgeted::resolve(|executor| executor.join(|| 21, || 21));
    /// assert_eq!(answer, (21, 21));
    /// ```
    pub fn resolve<R>(action: impl FnOnce(&Self) -> R) -> R {
        static GLOBAL_BUDGET: OnceLock<NonZeroUsize> = OnceLock::new();
        // Calls inside Rayon workers use the active pool without initializing
        // the global pool policy.
        let budget = if current_thread_index().is_some() {
            Rayon.budget()
        } else {
            // The global pool's width is immutable after initialization.
            *GLOBAL_BUDGET.get_or_init(|| {
                narrow_default_pool();
                Rayon.budget()
            })
        };
        action(&Self::new(&Rayon, budget))
    }
}

#[cfg(not(feature = "rayon"))]
impl Budgeted<'static, Sequential> {
    /// Runs `action` with the built-in executor, resolved once for a complete
    /// arithmetic operation.
    ///
    /// Without Rayon, the executor is [`Sequential`] itself, whose budget of one is
    /// a constant, so callers compile no parallel path.
    ///
    /// # Examples
    ///
    /// ```
    /// use core_anafis::{Budgeted, Executor};
    ///
    /// let answer = Budgeted::resolve(|executor| executor.join(|| 21, || 21));
    /// assert_eq!(answer, (21, 21));
    /// ```
    pub fn resolve<R>(action: impl FnOnce(&Sequential) -> R) -> R {
        action(&Sequential)
    }
}

impl<'executor, E: Executor + ?Sized> Budgeted<'executor, E> {
    /// Borrows `executor` with the budget used to size prepared scratch.
    ///
    /// # Examples
    ///
    /// ```
    /// use core::num::NonZeroUsize;
    /// use core_anafis::{Budgeted, Sequential};
    ///
    /// let budget = NonZeroUsize::new(2).expect("positive test budget");
    /// let budgeted = Budgeted::new(&Sequential, budget);
    /// ```
    #[must_use]
    pub const fn new(executor: &'executor E, budget: NonZeroUsize) -> Self {
        Self { executor, budget }
    }
}

impl Executor for Sequential {
    #[inline]
    fn budget(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    #[inline]
    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        (left(), right())
    }
}

#[cfg(feature = "rayon")]
impl Executor for Rayon {
    /// Returns the active pool width.
    #[inline]
    fn budget(&self) -> NonZeroUsize {
        NonZeroUsize::new(current_num_threads()).unwrap_or(NonZeroUsize::MIN)
    }

    #[inline]
    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        join(left, right)
    }

    /// Runs `op` on a worker of the active pool, or in place on a worker already.
    #[inline]
    fn on_workers<R, F>(&self, op: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        if current_thread_index().is_some() {
            op()
        } else {
            join(op, || ()).0
        }
    }
}

impl<E: Executor + ?Sized> Executor for Budgeted<'_, E> {
    #[inline]
    fn budget(&self) -> NonZeroUsize {
        self.budget
    }

    #[inline]
    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        if self.budget.get() == 1 {
            (left(), right())
        } else {
            self.executor.join(left, right)
        }
    }

    #[inline]
    fn on_workers<R, F>(&self, op: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        if self.budget.get() == 1 {
            op()
        } else {
            self.executor.on_workers(op)
        }
    }
}

/// Attempts global-pool initialization at a smaller physical-core width.
///
/// The default executor calls this once per process, outside Rayon workers.
/// Explicit environment configuration, an existing pool, or unknown topology
/// prevents narrowing.
#[cfg(feature = "rayon")]
fn narrow_default_pool() {
    if var_os("RAYON_NUM_THREADS").is_some() {
        return;
    }
    let Ok(available) = available_parallelism() else {
        return;
    };
    #[cfg(target_os = "linux")]
    let detected = read_to_string("/proc/self/status").ok().and_then(|status| {
        let allowed = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))?;
        detect_physical_parallelism(Path::new("/sys/devices/system/cpu"), allowed)
    });
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

/// Counts the physical cores of the allowed CPUs in a sysfs CPU directory.
///
/// `allowed` is a CPU list such as `0-3,8`. A core counts once, at its first
/// allowed hardware thread. Unreadable entries, missing or malformed lists, or an
/// unrepresentable core count report unknown topology.
#[cfg(all(feature = "rayon", target_os = "linux"))]
pub fn detect_physical_parallelism(root: &Path, allowed: &str) -> Option<NonZeroUsize> {
    let permitted = cpu_list(allowed)?;
    let permits = |cpu: &usize| permitted.iter().any(|range| range.contains(cpu));
    let mut cores = 0_usize;
    for entry_result in read_dir(root).ok()? {
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
        if !permits(&index) {
            continue;
        }
        // A partial count cannot establish the process-global worker budget.
        let siblings = read_to_string(entry.path().join("topology/thread_siblings_list")).ok()?;
        // Sysfs lists siblings in ascending order, `index` among them, so the
        // search ends at or before it.
        if cpu_list(&siblings)?.into_iter().flatten().find(permits) == Some(index) {
            cores = cores.checked_add(1)?;
        }
    }
    NonZeroUsize::new(cores)
}

/// Returns the ranges of a CPU list such as `0-3,8`, or `None` when malformed.
#[cfg(all(feature = "rayon", target_os = "linux"))]
fn cpu_list(list: &str) -> Option<Vec<RangeInclusive<usize>>> {
    list.trim()
        .split(',')
        .map(|range| {
            let (low, high) = range.split_once('-').unwrap_or((range, range));
            Some(low.parse().ok()?..=high.parse().ok()?)
        })
        .collect()
}
