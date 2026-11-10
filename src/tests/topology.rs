//! Global-pool lifecycle isolation and host topology sysfs discovery.

use core::num::NonZeroUsize;
#[cfg(target_os = "linux")]
use std::{
    env::temp_dir,
    fs::{create_dir_all, read_to_string, remove_dir_all, remove_file, write},
    path::{Path, PathBuf},
    process::id,
    time::{SystemTime, UNIX_EPOCH},
};
use std::{
    env::{current_exe, var_os},
    process::Command,
    thread::available_parallelism,
};

use alloc::string::String;
#[cfg(target_os = "linux")]
use alloc::{format, vec::Vec};

use rayon::{ThreadPoolBuilder, current_num_threads};

#[cfg(target_os = "linux")]
use proptest::{
    prop_assert_eq,
    test_runner::{Config, TestRunner},
};

#[cfg(target_os = "linux")]
use crate::parallel::detect_physical_parallelism;
use crate::{Budgeted, Executor};

const SCENARIO_ENV: &str = "CORE_ANAFIS_PARALLEL_TEST_SCENARIO";
const TEST_PATH: &str = "tests::topology::global_pool_lifecycle_respects_caller_configuration";
const SCENARIOS: &[&str] = &[
    "automatic",
    "explicit_environment",
    "existing_global",
    "custom_pool_first",
];

#[cfg(target_os = "linux")]
const ALL_CPUS: &str = "0-1023\n";

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct CpuDirectory(PathBuf);

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
            let detected = {
                let status = read_to_string("/proc/self/status").expect("process status");
                let allowed = status
                    .lines()
                    .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
                    .expect("allowed CPU list");
                detect_physical_parallelism(Path::new("/sys/devices/system/cpu"), allowed)
            };
            #[cfg(not(target_os = "linux"))]
            let detected: Option<NonZeroUsize> = None;
            let expected = detected.map_or(available, |physical| available.min(physical));
            for _resolution in 0..2 {
                Budgeted::resolve(|executor| {
                    assert_eq!(executor.budget(), expected, "automatic pool width");
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
            Budgeted::resolve(|executor| {
                assert_eq!(executor.budget().get(), 3, "caller-selected global width");
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
                Budgeted::resolve(|executor| {
                    assert_eq!(executor.budget().get(), 3, "active custom pool width");
                });
            });
            ThreadPoolBuilder::new()
                .num_threads(2)
                .build_global()
                .expect("custom-pool first use must preserve caller global initialization");
            Budgeted::resolve(|executor| {
                assert_eq!(executor.budget().get(), 2, "caller global width");
            });
            let single = ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .expect("single-worker pool");
            single.install(|| {
                Budgeted::resolve(|executor| {
                    assert_eq!(
                        executor.budget().get(),
                        1,
                        "custom pool bypasses the global cache"
                    );
                });
            });
            Budgeted::resolve(|executor| {
                assert_eq!(
                    executor.budget().get(),
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

#[test]
#[cfg(target_os = "linux")]
#[cfg_attr(miri, ignore = "Sysfs discovery requires native filesystem access")]
fn discovery_counts_complete_topologies_and_rejects_partial_scans() {
    let strategy = (0_usize..=8, 1_usize..=4, 0_u8..3);
    let mut runner = TestRunner::new(Config {
        source_file: Some(file!()),
        ..Config::default()
    });
    runner
        .run(&strategy, |(cores, siblings, style)| {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("native timestamp")
                .as_nanos();
            let root = temp_dir().join(format!("core_anafis_cpu_topology_{}_{nonce}", id()));
            create_dir_all(&root).expect("create the unique fixture directory");
            let fixture = CpuDirectory(root);
            create_dir_all(fixture.0.join("cpufreq")).expect("non-CPU sysfs entry");
            create_dir_all(fixture.0.join("cpu_non_numeric")).expect("non-numeric sysfs entry");

            for core in 0..cores {
                let first = core.checked_mul(siblings).expect("small fixture CPU index");
                let end = first
                    .checked_add(siblings)
                    .expect("small fixture CPU range");
                let last_index = end.checked_sub(1).expect("nonempty sibling group");
                let indices: Vec<_> = (first..end).map(|index| format!("{index}")).collect();
                let list = match style {
                    0 => format!("{}\n", indices.join(",")),
                    1 => format!("{first}-{last_index}\n"),
                    _ if siblings == 1 => format!(" {first}\n"),
                    _ => {
                        let next = first.checked_add(1).expect("small fixture CPU index");
                        format!(" {first},{next}-{last_index}\n")
                    }
                };
                for index in first..end {
                    let topology = fixture.0.join(format!("cpu{index}/topology"));
                    create_dir_all(&topology).expect("fixture CPU topology directory");
                    write(topology.join("thread_siblings_list"), &list)
                        .expect("fixture sibling list");
                }
            }

            prop_assert_eq!(
                detect_physical_parallelism(&fixture.0, ALL_CPUS),
                NonZeroUsize::new(cores)
            );
            prop_assert_eq!(
                detect_physical_parallelism(&fixture.0.join("absent"), ALL_CPUS),
                None
            );

            // A core counts once, at its first allowed hardware thread.
            let last_threads: Vec<_> = (1..=cores)
                .map(|core| {
                    let last = core
                        .checked_mul(siblings)
                        .and_then(|end| end.checked_sub(1))
                        .expect("small fixture CPU index");
                    format!("{last}")
                })
                .collect();
            prop_assert_eq!(
                detect_physical_parallelism(&fixture.0, &last_threads.join(",")),
                NonZeroUsize::new(cores)
            );
            let first_core = format!(
                "0-{}",
                siblings.checked_sub(1).expect("nonempty sibling group")
            );
            prop_assert_eq!(
                detect_physical_parallelism(&fixture.0, &first_core),
                NonZeroUsize::new(cores.min(1))
            );
            prop_assert_eq!(detect_physical_parallelism(&fixture.0, "4096"), None);
            prop_assert_eq!(detect_physical_parallelism(&fixture.0, "0-x"), None);

            if cores > 0 {
                let first_list = fixture.0.join("cpu0/topology/thread_siblings_list");
                let original = read_to_string(&first_list).expect("original fixture sibling list");
                for invalid in ["", "\n", "-1", "cpu0"] {
                    write(&first_list, invalid).expect("invalid primary-index fixture");
                    prop_assert_eq!(detect_physical_parallelism(&fixture.0, ALL_CPUS), None);
                }
                write(&first_list, format!("{}0\n", usize::MAX))
                    .expect("unrepresentable primary-index fixture");
                prop_assert_eq!(detect_physical_parallelism(&fixture.0, ALL_CPUS), None);
                write(&first_list, &original).expect("restore the complete sibling list");
                prop_assert_eq!(
                    detect_physical_parallelism(&fixture.0, ALL_CPUS),
                    NonZeroUsize::new(cores)
                );
                remove_file(&first_list).expect("remove one sibling list");
                prop_assert_eq!(detect_physical_parallelism(&fixture.0, ALL_CPUS), None);
                write(&first_list, original).expect("restore the removed sibling list");
                prop_assert_eq!(
                    detect_physical_parallelism(&fixture.0, ALL_CPUS),
                    NonZeroUsize::new(cores)
                );
            }

            let oversized_cpu = fixture.0.join(format!("cpu{}0", usize::MAX));
            create_dir_all(oversized_cpu).expect("unrepresentable CPU-index fixture");
            prop_assert_eq!(detect_physical_parallelism(&fixture.0, ALL_CPUS), None);
            Ok(())
        })
        .expect("topology discovery respects complete and incomplete CPU descriptions");
}

#[cfg(target_os = "linux")]
impl Drop for CpuDirectory {
    fn drop(&mut self) {
        let _cleanup = remove_dir_all(&self.0);
    }
}
