# CoreAnaFis

Native limb types and parallel execution engine for the AnaFis arithmetic crates.

`core_anafis` is `no_std`, allocates nothing, and has no required dependencies. Rust 1.85+ is required.

## Components

| Item | Description |
| :--- | :--- |
| `Limb`, `DoubleLimb` | Native register word (`usize`) and unsigned integer of twice its width |
| `LIMB_BITS`, `LIMB_BYTES` | Bit and byte width of a limb |
| `Executor` | Concurrency abstraction: planning `budget`, `join`, and worker affinity (`on_workers`) |
| `Sequential` | Synchronous executor running every join on the calling thread |
| `Rayon` | Parallel executor running joins in the active Rayon thread pool (`rayon` feature) |
| `Budgeted` | Wraps an executor with a fixed planning budget; `Budgeted::resolve` provides the default executor |

## Usage

```toml
[dependencies]
core_anafis = "0.1"

# Or with parallel support:
core_anafis = { version = "0.1", features = ["rayon"] }
```

Algorithms write against `Executor`, which splits work with `join` and sizes scratch with `budget()`:

```rust
use core_anafis::{Budgeted, Executor};

fn parallel_sum<E: Executor>(executor: &E, values: &[u64]) -> u64 {
    if values.len() <= 1024 {
        return values.iter().sum();
    }
    let (left, right) = values.split_at(values.len() / 2);
    let (a, b) = executor.join(
        || parallel_sum(executor, left),
        || parallel_sum(executor, right),
    );
    a + b
}

let values: Vec<u64> = (0..100_000).collect();
let total = Budgeted::resolve(|executor| parallel_sum(executor, &values));
assert_eq!(total, 4_999_950_000);
```

### Execution Semantics

- **Planning Budget:** `executor.budget()` sizes preallocated scratch buffers and partition trees without reserving threads.
- **Single Pool Entry:** Algorithms with multiple parallel phases call `executor.on_workers` once at the outer boundary, avoiding thread transitions between phases.
- **Resolution (`Budgeted::resolve`):** Resolves once per operation. Without `rayon`, it passes `Sequential` (budget of 1). With `rayon`, it passes the active Rayon pool.
- **Hardware-Aware Tuning:** Under `rayon`, the first resolution outside a worker initializes the global pool at the physical-core count (detected on Linux via `/proc` and sysfs), preventing hyper-threading contention on compute-intensive arithmetic loops unless `RAYON_NUM_THREADS` is set.

## Features

- **`rayon`**: Enables the [`Rayon`] executor and links `std`.

## License

`core_anafis` is licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for the full text.
