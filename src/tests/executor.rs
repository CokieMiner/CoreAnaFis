//! Join contracts, panic propagation, worker pools, and budget enforcement.

extern crate std;

#[cfg(feature = "rayon")]
use core::sync::atomic::{AtomicUsize, Ordering};
use core::{num::NonZeroUsize, panic::AssertUnwindSafe};
use std::panic::{catch_unwind, resume_unwind};

use alloc::boxed::Box;
#[cfg(feature = "rayon")]
use alloc::vec::Vec;

use proptest::{
    arbitrary::any,
    collection::vec,
    prop_assert_eq,
    test_runner::{Config, TestCaseResult, TestRunner},
};
#[cfg(feature = "rayon")]
use rayon::{ThreadPoolBuilder, current_thread_index};

#[cfg(feature = "rayon")]
use crate::Rayon;
use crate::{Budgeted, Executor, Sequential};

#[cfg(feature = "rayon")]
#[derive(Debug, Default)]
struct RecordingExecutor {
    joins: AtomicUsize,
}

#[test]
fn joins_preserve_borrowed_data_and_resolved_budgets() {
    let strategy = (vec(any::<u8>(), 0..=64), 0_usize..=64);
    let mut runner = TestRunner::new(Config {
        source_file: Some(file!()),
        ..Config::default()
    });
    runner
        .run(&strategy, |(values, split_seed)| {
            let split = split_seed.min(values.len());
            prop_assert_eq!(Sequential.budget().get(), 1);
            check_joins(&Sequential, &values, split)?;
            prop_assert_eq!(Sequential.on_workers(|| values.len()), values.len());

            for width in [1, 2, usize::MAX] {
                let budget = NonZeroUsize::new(width).expect("positive test budget");
                let budgeted = Budgeted::new(&Sequential, budget);
                prop_assert_eq!(budgeted.budget(), budget);
                check_joins(&budgeted, &values, split)?;
                prop_assert_eq!(budgeted.on_workers(|| values.len()), values.len());
            }

            #[cfg(not(feature = "rayon"))]
            Budgeted::resolve(|executor| {
                prop_assert_eq!(executor.budget().get(), 1);
                check_joins(executor, &values, split)
            })?;

            Ok(())
        })
        .expect("executor contracts hold for generated inputs");
}

#[test]
fn join_panics_complete_started_work_and_preserve_sequential_order() {
    check_panics(&Sequential, true);

    #[cfg(not(feature = "rayon"))]
    Budgeted::resolve(|executor| check_panics(executor, true));
}

#[test]
#[cfg(feature = "rayon")]
#[cfg_attr(
    miri,
    ignore = "Rayon worker startup violates Miri Stacked Borrows in crossbeam-epoch"
)]
fn on_workers_enters_the_pool_once_and_runs_budgets_of_one_in_place() {
    let budget = NonZeroUsize::new(2).expect("positive test budget");
    assert!(Rayon.on_workers(|| current_thread_index().is_some()));
    assert!(Budgeted::new(&Rayon, budget).on_workers(|| current_thread_index().is_some()));
    let pool = ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("private executor test pool");
    pool.install(|| {
        let entered = current_thread_index();
        assert_eq!(
            Rayon.on_workers(current_thread_index),
            entered,
            "a worker runs the closure in place"
        );
    });
    assert!(
        Budgeted::new(&Rayon, NonZeroUsize::MIN).on_workers(|| current_thread_index().is_none()),
        "a budget of one stays on the calling thread"
    );
    let recording = RecordingExecutor::default();
    assert_eq!(Budgeted::new(&recording, budget).on_workers(|| 7), 7);
    assert_eq!(recording.joins.load(Ordering::Relaxed), 0);
}

#[test]
#[cfg(feature = "rayon")]
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
                    prop_assert_eq!(Rayon.budget().get(), pool.current_num_threads());
                    check_joins(&Rayon, &values, split)?;
                    Budgeted::resolve(|executor| {
                        prop_assert_eq!(executor.budget().get(), pool.current_num_threads());
                        check_joins(executor, &values, split)
                    })?;

                    for width in [1, 2, usize::MAX] {
                        let budget = NonZeroUsize::new(width).expect("positive test budget");
                        let recording = RecordingExecutor::default();
                        let fixed = Budgeted::new(&recording, budget);
                        let copied = fixed;
                        prop_assert_eq!(copied.budget(), budget);
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
            check_panics(&Rayon, false);
            Budgeted::resolve(|executor| {
                check_panics(executor, pool.current_num_threads() == 1);
            });
            for width in [1, 2, usize::MAX] {
                let budget = NonZeroUsize::new(width).expect("positive test budget");
                let fixed = Budgeted::new(&Rayon, budget);
                check_panics(&fixed, width == 1);
            }
        });
    }
}

fn check_joins<E: Executor>(executor: &E, input: &[u8], split: usize) -> TestCaseResult {
    let mut values = input.to_vec();
    let mut expected = input.to_vec();
    for (index, value) in expected.iter_mut().enumerate() {
        *value = if index < split {
            value.reverse_bits()
        } else {
            value.rotate_left(1)
        };
    }

    let (left, right) = values.split_at_mut(split);
    let ((left_first, left_span, left_length), (right_last, right_span, right_length)) = executor
        .join(
            || {
                let length = left.len();
                let (prefix, suffix) = left.split_at_mut(length.div_ceil(2));
                let (prefix_len, suffix_len) = executor.join(
                    || {
                        for value in prefix.iter_mut() {
                            *value = value.reverse_bits();
                        }
                        prefix.len()
                    },
                    || {
                        for value in suffix.iter_mut() {
                            *value = value.reverse_bits();
                        }
                        suffix.len()
                    },
                );
                (left.first(), prefix_len.checked_add(suffix_len), length)
            },
            || {
                let length = right.len();
                let (prefix, suffix) = right.split_at_mut(length.div_ceil(2));
                let (prefix_len, suffix_len) = executor.join(
                    || {
                        for value in prefix.iter_mut() {
                            *value = value.rotate_left(1);
                        }
                        prefix.len()
                    },
                    || {
                        for value in suffix.iter_mut() {
                            *value = value.rotate_left(1);
                        }
                        suffix.len()
                    },
                );
                (right.last(), prefix_len.checked_add(suffix_len), length)
            },
        );

    prop_assert_eq!(left_span, Some(left_length), "left partition lengths");
    prop_assert_eq!(right_span, Some(right_length), "right partition lengths");
    prop_assert_eq!(
        left_first.copied(),
        expected.first().filter(|_| split > 0).copied()
    );
    prop_assert_eq!(
        right_last.copied(),
        expected.last().filter(|_| split < expected.len()).copied()
    );
    prop_assert_eq!(values, expected);
    Ok(())
}

fn check_panics<E: Executor>(executor: &E, sequential: bool) {
    for panic_left in [true, false] {
        let mut left_completed = false;
        let mut right_completed = false;
        let result = catch_unwind(AssertUnwindSafe(|| {
            executor.join(
                || {
                    if panic_left {
                        resume_unwind(Box::new("left join branch"));
                    }
                    left_completed = true;
                },
                || {
                    if !panic_left {
                        resume_unwind(Box::new("right join branch"));
                    }
                    right_completed = true;
                },
            )
        }));

        let payload = result.expect_err("a branch panic must propagate to the join caller");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&if panic_left {
                "left join branch"
            } else {
                "right join branch"
            }),
            "the join preserves the branch panic payload"
        );
        assert_eq!(
            left_completed, !panic_left,
            "left branch completion before unwinding"
        );
        assert_eq!(
            right_completed,
            panic_left && !sequential,
            "a sequential left panic skips the right branch; Rayon completes it"
        );
    }
}

#[cfg(feature = "rayon")]
impl Executor for RecordingExecutor {
    fn budget(&self) -> NonZeroUsize {
        Rayon.budget()
    }

    fn join<A, B, RA, RB>(&self, left: A, right: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let _previous = self.joins.fetch_add(1, Ordering::Relaxed);
        Rayon.join(left, right)
    }
}
