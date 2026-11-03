//! Generated sysfs layouts and conservative topology failure handling.

use core::num::NonZeroUsize;
use std::{
    env::temp_dir,
    fs::{create_dir_all, read_to_string, remove_dir_all, remove_file, write},
    path::PathBuf,
    process::id,
    time::{SystemTime, UNIX_EPOCH},
};

use alloc::{format, vec::Vec};

use proptest::{
    prop_assert_eq,
    test_runner::{Config, TestRunner},
};

use crate::parallel::topology::detect_physical_parallelism;

#[derive(Debug)]
struct CpuDirectory(PathBuf);

#[test]
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
                detect_physical_parallelism(&fixture.0),
                NonZeroUsize::new(cores)
            );
            prop_assert_eq!(detect_physical_parallelism(&fixture.0.join("absent")), None);

            if cores > 0 {
                let first_list = fixture.0.join("cpu0/topology/thread_siblings_list");
                let original = read_to_string(&first_list).expect("original fixture sibling list");
                for invalid in ["", "\n", "-1", "cpu0"] {
                    write(&first_list, invalid).expect("invalid primary-index fixture");
                    prop_assert_eq!(detect_physical_parallelism(&fixture.0), None);
                }
                write(&first_list, format!("{}0\n", usize::MAX))
                    .expect("unrepresentable primary-index fixture");
                prop_assert_eq!(detect_physical_parallelism(&fixture.0), None);
                write(&first_list, &original).expect("restore the complete sibling list");
                prop_assert_eq!(
                    detect_physical_parallelism(&fixture.0),
                    NonZeroUsize::new(cores)
                );
                remove_file(&first_list).expect("remove one sibling list");
                prop_assert_eq!(detect_physical_parallelism(&fixture.0), None);
                write(&first_list, original).expect("restore the removed sibling list");
                prop_assert_eq!(
                    detect_physical_parallelism(&fixture.0),
                    NonZeroUsize::new(cores)
                );
            }

            let oversized_cpu = fixture.0.join(format!("cpu{}0", usize::MAX));
            create_dir_all(oversized_cpu).expect("unrepresentable CPU-index fixture");
            prop_assert_eq!(detect_physical_parallelism(&fixture.0), None);
            Ok(())
        })
        .expect("topology discovery respects complete and incomplete CPU descriptions");
}

impl Drop for CpuDirectory {
    fn drop(&mut self) {
        let _cleanup = remove_dir_all(&self.0);
    }
}
