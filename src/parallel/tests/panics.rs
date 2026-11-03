//! Join unwinding and completion of borrowed work.

extern crate std;

use core::panic::AssertUnwindSafe;
use std::panic::{catch_unwind, resume_unwind};

use alloc::boxed::Box;

#[cfg(not(feature = "rayon"))]
use crate::parallel::DefaultExecutor;
use crate::parallel::{ParallelExecutor, SequentialExecutor};

#[test]
fn join_panics_complete_started_work_and_preserve_sequential_order() {
    check_panics(&SequentialExecutor, true);

    #[cfg(not(feature = "rayon"))]
    DefaultExecutor::with_resolved(|executor| check_panics(executor, true));
}

pub fn check_panics<E: ParallelExecutor>(executor: &E, sequential: bool) {
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
