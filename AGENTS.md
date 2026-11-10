# CoreAnaFis

`core_anafis` defines the native limb types and the synchronous parallel execution
engine shared across the AnaFis arithmetic crates (including `ntt_anafis`). Its public
surface is declared in [src/lib.rs](src/lib.rs).

**Priorities, in order:** exactness, zero allocation, portability, speed, ergonomics.
Speed never excuses an unproved invariant, and thread coordination must not allocate
or add avoidable synchronization overhead.

## Layout

| Path | Holds |
| :--- | :--- |
| `src/lib.rs` | Public re-exports, feature-gated `no_std`, native limb types (`Limb`, `DoubleLimb`, `LIMB_BITS`, `LIMB_BYTES`), crate documentation |
| `src/parallel.rs` | `Executor` trait, `Sequential`, `Rayon`, `Budgeted`, `Budgeted::resolve`, host topology discovery, and pool narrowing |
| `src/tests/` | Contract tests, panic propagation, Rayon worker pools, lifecycle scenarios, and sysfs mocking |

## Invariants

Each rule has one definition. Change it there, and cite it elsewhere.

| Rule | Defined in |
| :--- | :--- |
| Limb definitions, double-limb mappings, `LIMB_BITS`, `LIMB_BYTES` | `lib.rs` |
| `Executor` trait contract (`budget`, `join`, `on_workers`) | `parallel.rs` |
| `Sequential` executor: unit budget, synchronous in-place execution | `parallel.rs` |
| `Rayon` executor: pool width budget, work-stealing joins | `parallel.rs` |
| `Budgeted` adapter: budget clamping, join forwarding, in-place threshold | `parallel.rs` |
| Resolution semantics: single resolution per operation, cached global budget | `Budgeted::resolve`, `parallel.rs` |
| Global pool policy: physical-core narrowing, Linux sysfs discovery | `parallel.rs` |

These rules cross modules:

- **Limb representations:** `Limb` is `usize`, matching native register width on every
  target. Multiprecision operands store limbs least-significant first. `DoubleLimb` has
  twice the bit width of `Limb` (`u128` on 64-bit, `u64` on 32-bit, `u32` on 16-bit) to
  hold exact products, carries, and quotient steps without overflow. `LIMB_BITS` is
  `Limb::BITS as usize`, and `LIMB_BYTES` is `size_of::<Limb>()`.
- **Zero allocation:** `core_anafis` is `no_std` by default and allocates no heap memory.
  When the `rayon` feature is enabled, `std` is linked solely for thread synchronization
  and host topology queries; executor coordination allocates nothing.
- **Executor contract:**
  - `budget(&self) -> NonZeroUsize` returns the planning budget: the upper bound of tasks
    for which scratch space or partition trees are sized. It reserves no OS threads.
  - `join(&self, left, right)` executes two independent closures concurrently or
    sequentially and returns `(RA, RB)`. If either closure panics, the panic is propagated
    after all started work has completed.
  - `on_workers(&self, op)` runs `op` inside the execution pool. An operation with multiple
    parallel phases must call `on_workers` once at its outer entry point instead of entering
    and exiting the pool across individual phases. Sequential execution runs `op` in place.
- **Sequential vs Rayon behavior:**
  - `Sequential`: budget is always 1 (`NonZeroUsize::MIN`). Joins execute `left()` then
    `right()` sequentially on the calling thread. If `left()` panics, `right()` does not run.
  - `Rayon`: budget is `current_num_threads()` (pool width). Joins dispatch via `rayon::join`.
    If one closure panics, Rayon ensures the other started closure completes before unwinding.
    `on_workers` runs `op()` in place when called from a worker thread, or via
    `join(op, || ()).0` from non-worker threads to enter the pool once.
- **Budgeted behavior:**
  - `Budgeted<'e, E>` wraps an executor with an explicit planning budget.
  - When `budget.get() == 1`: execution is strictly sequential in place. Joins execute
    `(left(), right())` and `on_workers` runs `op()` in place, bypassing the underlying
    executor and avoiding thread transfers.
  - When `budget.get() > 1`: joins and `on_workers` forward directly to the underlying
    executor without creating or reserving workers.
  - `Budgeted` implements `Copy` and `Clone` across all executors, even when `E` is not `Clone`.
- **Resolution semantics (`Budgeted::resolve`):**
  - Drivers and algorithms resolve the executor once per complete operation, threading the
    resulting executor reference through internal stages.
  - Without `rayon`: `Budgeted::resolve` calls `action(&Sequential)`, providing a constant
    budget of 1 with zero branching or parallel overhead.
  - With `rayon`:
    - Inside a Rayon worker (`current_thread_index().is_some()`), it borrows `Rayon` with the
      active pool's budget, bypassing global pool initialization.
    - Outside Rayon workers, it queries `GLOBAL_BUDGET: OnceLock<NonZeroUsize>`. On first call,
      it runs pool narrowing before caching `Rayon.budget()`. Subsequent resolutions
      reuse the cached width.
- **Topology discovery & fallback:**
  - Global pool narrowing attempts to initialize Rayon's global pool at the host's
    physical-core count, avoiding throughput degradation caused by SMT thread contention on
    compute-intensive loops.
  - Narrowing applies only when: (1) `RAYON_NUM_THREADS` is unset, (2) the global pool has
    not yet been initialized, and (3) physical cores are fewer than `available_parallelism()`.
  - On Linux: reads allowed CPUs from `/proc/self/status` (`Cpus_allowed_list`) and core
    groupings from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`. A core counts
    once, at its lowest allowed hardware thread index.
  - Any discovery failure (missing or unreadable sysfs files, malformed ranges, or non-Linux
    targets) conservatively aborts narrowing and preserves Rayon's default configuration.

## Code Rules

- **Unsafe:** strictly forbidden (`unsafe_code = "deny"` in `Cargo.toml`). `core_anafis`
  contains zero unsafe code.
- **No-std by default:** `#![cfg_attr(not(feature = "rayon"), no_std)]`. Production code
  imports from `core::`, never `std::` or `alloc::`. Lints `std_instead_of_core` and
  `std_instead_of_alloc` are denied.
- **Lint attributes:** every `allow` or `expect` has an explicit `reason = "..."`. Never
  suppress `dead_code`; gate items with `#[cfg(...)]`.
- **Clippy enforcement:** pedantic, nursery, and strict arithmetic lints are denied
  (`arithmetic_side_effects`, `as_conversions`, `indexing_slicing`, `integer_division`,
  `unwrap_used`, `todo`, `unimplemented`, `shadow_unrelated`, `shadow_reuse`). Arithmetic
  and conversions must be checked or structurally proved.
- **No small helpers:** a function under about five lines of logic needs a concrete reason:
  an API boundary or trait implementation. Do not wrap single expressions.
- **Modules and visibility:** private `mod` with `pub use`, never `pub mod`. A `mod.rs` only
  registers children. Shared internal items are plain `pub` inside sealed modules.
- **Imports:** group `core`, `alloc` (tests only), external crates (`rayon`, `proptest`),
  `crate::`, `super::`. Use inline paths (`::core::…`, `$crate::…`) only inside macros.
- **Size:** files stay between 200 and 600 lines. Split along natural module boundaries,
  never solely to satisfy line counts.
- **Tests:** placed in `tests.rs` or `tests/` beside the module. Production code contains
  only `#[cfg(test)] mod tests;`. Property tests (`proptest`) cover input ranges, panic
  behavior, and mocked sysfs layouts.
- **Docs:** direct, descriptive, and scientific. Every public item is documented
  (`missing_docs = "deny"`). No commented-out code or version history.

## Performance Work

Work in this order: eliminate redundant thread hops, reduce pool entry synchronization,
and minimize closure state. Unchecked indexing or forced inlining requires an identified
measurement showing executed work reduction.

For algorithms with multiple parallel phases, call `on_workers` once at the outer boundary.
This eliminates repeated thread wakeup and rendezvous latency across phases.

## Verification

Run from `CoreAnaFis` with zero findings:

```sh
cargo test --no-default-features
cargo test --all-features
cargo clippy --all-targets --no-default-features -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
```

Process-isolated lifecycle tests and native sysfs tests in `src/tests/topology.rs` run
during `cargo test --all-features`. Under Miri, tests that require native host filesystem
queries or Rayon worker startup (`crossbeam-epoch`) are ignored.
