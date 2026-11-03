//! Synchronous execution adapters for independent arithmetic work.
//!
//! Adapters report a planning budget and join independent closures before
//! returning their results. The default backend uses the active Rayon pool
//! when `rayon` is enabled and sequential execution otherwise.

use core::num::NonZeroUsize;
#[cfg(feature = "rayon")]
use std::sync::OnceLock;

#[cfg(feature = "rayon")]
use rayon::{current_num_threads, current_thread_index, join};

#[cfg(feature = "rayon")]
use super::narrow_default_pool;

/// Schedules independent synchronous arithmetic work.
///
/// Implementations provide sequential or Rayon execution with the same join
/// interface.
pub trait ParallelExecutor: Sync {
    /// Returns the maximum concurrency degree used to size scratch buffers.
    ///
    /// The value is a nonzero planning budget. Algorithms partition this
    /// budget when subdividing scratch; the adapter does not reserve worker
    /// threads.
    fn parallelism(&self) -> NonZeroUsize;

    /// Executes two independent closures and returns their results.
    ///
    /// # Panics
    ///
    /// Propagates a closure panic after all started work has stopped. A
    /// sequential adapter does not start `right` if `left` panics.
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
    /// this method instead of once per phase. A sequential adapter runs `op` in
    /// place.
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

/// Resolves the default execution backend for a complete arithmetic operation.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct DefaultExecutor;

/// Executes closures sequentially on the calling thread.
#[derive(Clone, Copy, Debug, Default)]
pub struct SequentialExecutor;

/// Executes closures in the active Rayon pool.
#[cfg(feature = "rayon")]
#[derive(Clone, Copy, Debug, Default)]
pub struct RayonExecutor;

/// Reports a fixed planning budget over a borrowed executor.
///
/// A budget of one executes joins sequentially. Larger budgets forward joins
/// to the underlying executor without reserving or creating workers.
#[cfg(feature = "rayon")]
#[derive(Clone, Copy, Debug)]
pub struct FixedParallelismExecutor<'executor, E: ?Sized> {
    executor: &'executor E,
    parallelism: NonZeroUsize,
}

impl DefaultExecutor {
    /// Resolves the built-in backend once for a complete arithmetic operation.
    ///
    /// The budget is the width of the active pool. Outside Rayon workers, the
    /// first call starts the global pool at the physical-core count unless it is
    /// configured or running already.
    #[cfg(feature = "rayon")]
    pub fn with_resolved<R>(
        action: impl FnOnce(&FixedParallelismExecutor<'static, RayonExecutor>) -> R,
    ) -> R {
        static GLOBAL_PARALLELISM: OnceLock<NonZeroUsize> = OnceLock::new();
        // Calls inside Rayon workers use the active pool without initializing
        // the global pool policy.
        let parallelism = if current_thread_index().is_some() {
            RayonExecutor.parallelism()
        } else {
            // The global pool's width is immutable after initialization.
            *GLOBAL_PARALLELISM.get_or_init(|| {
                narrow_default_pool();
                RayonExecutor.parallelism()
            })
        };
        action(&FixedParallelismExecutor::new(&RayonExecutor, parallelism))
    }

    /// Resolves the sequential backend when Rayon is not compiled in.
    #[cfg(not(feature = "rayon"))]
    pub fn with_resolved<R>(action: impl FnOnce(&SequentialExecutor) -> R) -> R {
        action(&SequentialExecutor)
    }
}

#[cfg(feature = "rayon")]
impl<'executor, E: ParallelExecutor + ?Sized> FixedParallelismExecutor<'executor, E> {
    /// Borrows `executor` with the budget used to size prepared scratch.
    #[must_use]
    pub const fn new(executor: &'executor E, parallelism: NonZeroUsize) -> Self {
        Self {
            executor,
            parallelism,
        }
    }
}

impl ParallelExecutor for SequentialExecutor {
    #[inline]
    fn parallelism(&self) -> NonZeroUsize {
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
impl ParallelExecutor for RayonExecutor {
    /// Returns the active pool width used to size scratch buffers.
    #[inline]
    fn parallelism(&self) -> NonZeroUsize {
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

#[cfg(feature = "rayon")]
impl<E: ParallelExecutor + ?Sized> ParallelExecutor for FixedParallelismExecutor<'_, E> {
    #[inline]
    fn parallelism(&self) -> NonZeroUsize {
        self.parallelism
    }

    #[inline]
    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        if self.parallelism.get() == 1 {
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
        if self.parallelism.get() == 1 {
            op()
        } else {
            self.executor.on_workers(op)
        }
    }
}
