//! Process-isolated checks for Rayon's global-pool lifecycle.

#[cfg(not(target_os = "linux"))]
use core::num::NonZeroUsize;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::{
    env::{current_exe, var_os},
    process::Command,
    thread::available_parallelism,
};

use alloc::string::String;

use rayon::{ThreadPoolBuilder, current_num_threads};

#[cfg(target_os = "linux")]
use crate::parallel::topology::detect_physical_parallelism;
use crate::parallel::{DefaultExecutor, ParallelExecutor};

const SCENARIO_ENV: &str = "CORE_ANAFIS_PARALLEL_TEST_SCENARIO";
const TEST_PATH: &str =
    "parallel::tests::initialization::global_pool_lifecycle_respects_caller_configuration";
const SCENARIOS: &[&str] = &[
    "automatic",
    "explicit_environment",
    "existing_global",
    "custom_pool_first",
];

#[test]
#[cfg_attr(
    miri,
    ignore = "Global-pool lifecycle requires subprocess isolation and native host queries"
)]
fn global_pool_lifecycle_respects_caller_configuration() {
    let Some(scenario) = var_os(SCENARIO_ENV) else {
        for scenario in SCENARIOS {
            let mut command = Command::new(current_exe().expect("test executable path"));
            let _configured = command
                .args(["--exact", TEST_PATH, "--test-threads=1"])
                .env(SCENARIO_ENV, scenario)
                .env_remove("RAYON_NUM_THREADS");
            if *scenario == "explicit_environment" {
                let _configured_width = command.env("RAYON_NUM_THREADS", "3");
            }
            let output = command.output().expect("isolated pool-policy test process");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "scenario {scenario}: {stdout}\n{stderr}"
            );
            assert!(
                stdout.contains("1 passed; 0 failed"),
                "scenario {scenario} must execute its exact test: {stdout}"
            );
        }
        return;
    };

    execute_global_pool_scenario(scenario.to_str().expect("known ASCII test scenario"));
}

fn execute_global_pool_scenario(scenario: &str) {
    match scenario {
        "automatic" => {
            let available = available_parallelism().expect("native available parallelism");
            #[cfg(target_os = "linux")]
            let detected = detect_physical_parallelism(Path::new("/sys/devices/system/cpu"));
            #[cfg(not(target_os = "linux"))]
            let detected: Option<NonZeroUsize> = None;
            let expected = detected.map_or(available, |physical| available.min(physical));
            for _resolution in 0..2 {
                DefaultExecutor::with_resolved(|executor| {
                    assert_eq!(executor.parallelism(), expected, "automatic pool width");
                    assert_eq!(
                        executor.join(current_num_threads, current_num_threads),
                        (expected.get(), expected.get()),
                        "joins use the resolved global pool"
                    );
                });
            }
        }
        "explicit_environment" | "existing_global" => {
            if scenario == "existing_global" {
                ThreadPoolBuilder::new()
                    .num_threads(3)
                    .build_global()
                    .expect("caller initializes the global pool");
            }
            DefaultExecutor::with_resolved(|executor| {
                assert_eq!(
                    executor.parallelism().get(),
                    3,
                    "caller-selected global width"
                );
                assert_eq!(
                    executor.join(current_num_threads, current_num_threads),
                    (3, 3),
                    "caller-selected width survives resolution"
                );
            });
        }
        "custom_pool_first" => {
            let custom = ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .expect("custom pool");
            custom.install(|| {
                DefaultExecutor::with_resolved(|executor| {
                    assert_eq!(executor.parallelism().get(), 3, "active custom pool width");
                });
            });
            ThreadPoolBuilder::new()
                .num_threads(2)
                .build_global()
                .expect("custom-pool first use must preserve caller global initialization");
            DefaultExecutor::with_resolved(|executor| {
                assert_eq!(executor.parallelism().get(), 2, "caller global width");
            });
            let single = ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .expect("single-worker pool");
            single.install(|| {
                DefaultExecutor::with_resolved(|executor| {
                    assert_eq!(
                        executor.parallelism().get(),
                        1,
                        "custom pool bypasses the global cache"
                    );
                });
            });
            DefaultExecutor::with_resolved(|executor| {
                assert_eq!(
                    executor.parallelism().get(),
                    2,
                    "global cache survives custom-pool use"
                );
            });
        }
        #[expect(
            clippy::panic,
            reason = "An unknown subprocess scenario is a test harness error"
        )]
        unexpected => panic!("unknown isolated test scenario: {unexpected}"),
    }
}
