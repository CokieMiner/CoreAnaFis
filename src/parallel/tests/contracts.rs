//! Nested joins, borrowed results, and worker-budget resolution.

use proptest::{
    arbitrary::any,
    collection::vec,
    prop_assert_eq,
    test_runner::{Config, TestCaseResult, TestRunner},
};

#[cfg(not(feature = "rayon"))]
use crate::parallel::DefaultExecutor;
use crate::parallel::{ParallelExecutor, SequentialExecutor};

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
            prop_assert_eq!(SequentialExecutor.parallelism().get(), 1);
            check_joins(&SequentialExecutor, &values, split)?;
            prop_assert_eq!(SequentialExecutor.on_workers(|| values.len()), values.len());

            #[cfg(not(feature = "rayon"))]
            DefaultExecutor::with_resolved(|executor| {
                prop_assert_eq!(executor.parallelism().get(), 1);
                check_joins(executor, &values, split)
            })?;

            Ok(())
        })
        .expect("executor contracts hold for generated inputs");
}

pub fn check_joins<E: ParallelExecutor>(
    executor: &E,
    input: &[u8],
    split: usize,
) -> TestCaseResult {
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
