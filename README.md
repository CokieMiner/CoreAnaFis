# CoreAnaFis

Native limb types and the parallel execution engine of the AnaFis arithmetic crates.

`core_anafis` is `no_std`, allocates nothing, and has no dependencies. The `rayon`
feature adds Rayon. Rust 1.85 or later is required.

| Item | Use |
| :--- | :--- |
| `Limb`, `DoubleLimb` | The native word (`usize`) and an unsigned integer of twice its width |
| `LIMB_BITS`, `LIMB_BYTES` | Width of a limb |
| `ParallelExecutor` | A planning budget, `join` of two closures, and `on_workers` |
| `SequentialExecutor` | Runs every join on the calling thread |
| `DefaultExecutor` | Resolves the backend once per complete operation |
| `RayonExecutor`, `FixedParallelismExecutor` | The active Rayon pool, and a fixed budget over another executor (`rayon`) |

## Usage

```toml
[dependencies]
core_anafis = "0.1"
```

An algorithm takes any executor and splits its work with `join`:

```rust
use core_anafis::{DefaultExecutor, ParallelExecutor};

fn sum<E: ParallelExecutor>(executor: &E, values: &[u64]) -> u64 {
    if values.len() <= 1024 {
        return values.iter().sum();
    }
    let (left, right) = values.split_at(values.len() / 2);
    let (a, b) = executor.join(|| sum(executor, left), || sum(executor, right));
    a + b
}

let values: Vec<u64> = (0..100_000).collect();
let total = DefaultExecutor::with_resolved(|executor| sum(executor, &values));
assert_eq!(total, 4_999_950_000);
```

`parallelism()` is a budget for sizing scratch and limiting splits. It reserves no
thread. An operation with several parallel phases enters the pool once through
`on_workers`.

## Default executor

Without `rayon`, `DefaultExecutor` resolves `SequentialExecutor`. With `rayon`, it
resolves the active pool: the pool of the calling worker, or else the global pool.

The first resolution outside a Rayon worker initializes the global pool at the
physical-core count when that count is below the available parallelism. Linux reads it
from the sysfs sibling lists; other platforms keep Rayon's default. `RAYON_NUM_THREADS`
or a global pool built before that call keeps the caller's configuration. Idle workers on
the second hardware thread of a core slow the busy one, so a program that starts Rayon
first runs fastest with its pool at the physical-core count.

## Features

- **`rayon`**: executes joins in the active Rayon pool, and links `std`.

## License

Apache License, Version 2.0.
