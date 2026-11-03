//! Rayon pool widths, fixed budgets, nested joins, and panic propagation.

use core::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

use alloc::vec::Vec;

use proptest::{
    arbitrary::any,
    collection::vec,
    prop_assert_eq,
    test_runner::{Config, TestRunner},
};
use rayon::{ThreadPoolBuilder, current_thread_index};

use crate::parallel::{DefaultExecutor, FixedParallelismExecutor, ParallelExecutor, RayonExecutor};

use super::{contracts::check_joins, panics::check_panics};

#[derive(Debug, Default)]
struct RecordingExecutor {
    joins: AtomicUsize,
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Rayon worker startup violates Miri Stacked Borrows in crossbeam-epoch"
)]
fn on_workers_enters_the_pool_once_and_runs_budgets_of_one_in_place() {
    let budget = NonZeroUsize::new(2).expect("positive test budget");
    assert!(RayonExecutor.on_workers(|| current_thread_index().is_some()));
    assert!(
        FixedParallelismExecutor::new(&RayonExecutor, budget)
            .on_workers(|| current_thread_index().is_some())
    );
    let pool = ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("private executor test pool");
    pool.install(|| {
        let entered = current_thread_index();
        assert_eq!(
            RayonExecutor.on_workers(current_thread_index),
            entered,
            "a worker runs the closure in place"
        );
    });
    assert!(
        FixedParallelismExecutor::new(&RayonExecutor, NonZeroUsize::MIN)
            .on_workers(|| current_thread_index().is_none()),
        "a budget of one stays on the calling thread"
    );
    let recording = RecordingExecutor::default();
    assert_eq!(
        FixedParallelismExecutor::new(&recording, budget).on_workers(|| 7),
        7
    );
    assert_eq!(recording.joins.load(Ordering::Relaxed), 0);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Rayon worker startup violates Miri Stacked Borrows in crossbeam-epoch"
)]
fn worker_pools_preserve_join_contracts_and_fixed_budgets() {
    let pools: Vec<_> = [1, 2, 4]
        .into_iter()
        .map(|width| {
            ThreadPoolBuilder::new()
                .num_threads(width)
                .build()
                .expect("private executor test pool")
        })
        .collect();

    let strategy = (vec(any::<u8>(), 0..=64), 0_usize..=64);
    let mut runner = TestRunner::new(Config {
        source_file: Some(file!()),
        ..Config::default()
    });
    runner
        .run(&strategy, |(values, split_seed)| {
            let split = split_seed.min(values.len());
            for pool in &pools {
                pool.install(|| {
                    prop_assert_eq!(
                        RayonExecutor.parallelism().get(),
                        pool.current_num_threads()
                    );
                    check_joins(&RayonExecutor, &values, split)?;
                    DefaultExecutor::with_resolved(|executor| {
                        prop_assert_eq!(executor.parallelism().get(), pool.current_num_threads());
                        check_joins(executor, &values, split)
                    })?;

                    for width in [1, 2, usize::MAX] {
                        let budget = NonZeroUsize::new(width).expect("positive test budget");
                        let recording = RecordingExecutor::default();
                        let fixed = FixedParallelismExecutor::new(&recording, budget);
                        prop_assert_eq!(fixed.parallelism(), budget);
                        check_joins(&fixed, &values, split)?;
                        prop_assert_eq!(
                            recording.joins.load(Ordering::Relaxed),
                            if width == 1 { 0 } else { 3 },
                            "one outer and two nested joins forward only for budgets above one"
                        );
                    }
                    Ok(())
                })?;
            }
            Ok(())
        })
        .expect("Rayon join contracts hold for generated inputs and worker budgets");

    for pool in &pools {
        pool.install(|| {
            check_panics(&RayonExecutor, false);
            DefaultExecutor::with_resolved(|executor| {
                check_panics(executor, pool.current_num_threads() == 1);
            });
            for width in [1, 2, usize::MAX] {
                let budget = NonZeroUsize::new(width).expect("positive test budget");
                let fixed = FixedParallelismExecutor::new(&RayonExecutor, budget);
                check_panics(&fixed, width == 1);
            }
        });
    }
}

impl ParallelExecutor for RecordingExecutor {
    fn parallelism(&self) -> NonZeroUsize {
        RayonExecutor.parallelism()
    }

    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let _previous = self.joins.fetch_add(1, Ordering::Relaxed);
        RayonExecutor.join(left, right)
    }
}
