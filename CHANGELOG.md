# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project adheres to [Semantic Versioning](https://semver.org/).

## [0.7.0] — Phase 7: distribution and provenance

### Added

- `packaging/aur/PKGBUILD` (`-git` flavour) and `packaging/README.md`, covering
  Arch/Manjaro/EndeavourOS/CachyOS, Bazzite and Windows.
- `docs/verification.md`: every modelled layer anchored to the shipped
  `blender.exe` 5.3 beta through its PDB — symbol addresses, prefix counts, and
  an explicit list of what is *not* verified.
- README: AUR install path and a verification section.

### Verified

- `cargo package --offline` builds the crate standalone: 26 files, 149 KiB
  (42 KiB compressed). `cargo publish --dry-run` could not reach crates.io from
  this sandbox, so the upload step itself remains untested.

## [0.6.0] — Phase 6: cost shape

### Added

- `cpx sweep --shape clustered|interleaved|random`: where the expensive items sit
  becomes a variable. `random` uses a deterministic hash so runs are repeatable.
- Costs are now derived from the *actual* heavy count, so a random mask that
  draws a slightly different number of heavy items still hits the same total
  work.

### Learned

- With a 25% heavy share the crossover disappears for `interleaved` and `random`
  while it stays at grain 2048 for `clustered`. The hints only matter when the
  cost is lopsided *on the scale of a chunk*; a uniform mix leaves nothing to
  fix.

## [0.5.0] — Phase 5: the decision map

### Added

- `cpx sweep`: a grid over grain values and heavy shares that reports the winner
  per cell, the best speedup per cell, the crossover grain per heavy share, and
  an optional CSV export (`--csv`).
- `--concentration` controls the heavy/light cost ratio; total work is held
  constant across rows so cells are comparable.
- A 5% tolerance so a tie is not decided by measurement noise: the simpler
  strategy keeps the win.

### Corrected

- The single-scenario benchmark over-stated the value of the cost-aware hints.
  The sweep shows uniform chunks winning at every fine grain, with a crossover
  only at the coarsest grains (1024–2048) and never at a 50% heavy share. The
  hints are insurance against a bad partition, not a general speedup.

## [0.4.0] — Phase 4: measure instead of assume

### Added

- `cpx bench`: a two-scenario benchmark of the four splitting strategies over a
  workload whose cost is clustered, with a verified-identical work count for
  every strategy.
- The benchmark reports chunk counts, best and median wall clock, and speedup
  against the serial loop, for a fine and a coarse grain.
- Observed on the development machine (20k items, 12 workers): at a fine grain
  `static(size=1)` leads (6.00x) and the cost-aware hints pay overhead; at a
  coarse grain `static(size=1)` stops scaling entirely (1.02x) while
  `accumulated` reaches 6.38x.

### Changed

- Version 0.4.0; README documents the benchmark and how to read it.

## [0.3.0] — Phase 3: the modern `blender::threading` layer

### Added

- `threading::parallel_for` over `IndexRange` with `TaskSizeHints`, and the
  three splitting strategies Blender uses: uniform blocks for `Static`, greedy
  bucketing with an outer `min(grain, 512)` for `Individual`, recursive
  bisection for `Accumulated`.
- `use_single_thread`, so small work stays on the calling thread.
- `parallel_for_aligned` with `align_sub_range`.
- `parallel_reduce` (tree merge) and `parallel_deterministic_reduce` (ordered
  merge).
- `parallel_invoke2`, `max_threads_task` and `memory_bandwidth_bound_task`
  (8 MiB cache threshold, 8-thread bandwidth cap).
- `ExecutionMode::{Serial, Parallel, ParallelGrainSize}` with Blender's
  `grain_size(fallback)` semantics.
- `IndexMask` projection: range segments, chunked segments, index iteration and
  `foreach_segment_parallel` driven by an accumulated size hint.
- `cpx graph`: task-graph demonstrator with a live trace and an ASCII tree
  showing real per-node run counts.
- 13 new behavioural tests (34 total).

## [0.2.0] — Phase 2: task pools and dependency graphs

### Added

- `TaskPool` with all five Blender pool flavours: `Immediate`, `Background`,
  `BackgroundSerial`, `Suspended`, `NoThreads`, resolved against the worker
  count exactly as `intern/task_pool.cc` resolves them (including the
  single-worker fallbacks and the background → shared-pool promotion).
- `TaskGraph` with **edge-triggered** propagation: a node with two parents is
  triggered once per parent, not once as an AND-join.
- A shared process-wide runtime with one queue for every pool, and a waiting
  protocol that drains the queue (mirroring `tbb::task_group::wait()`), so
  nested pools cannot deadlock when all workers are busy.
- Per-scope completion counters, so waiting on one graph does not wait for
  unrelated pools.
- `init_scheduler` / `worker_threads` to size the worker pool.
- `Priority` recorded for fidelity but not acted upon, matching Blender on
  TBB 2021+.
- 13 new behavioural tests (`tests/pool.rs`, `tests/graph.rs`).

### Changed

- `lib.rs` now documents two layers; the crate version is 0.2.0.

## [0.1.0] — Phase 1: data-parallel ranges

### Added

- `parallel_range` with `grainsize = max(min_iter_per_thread, 1)`, `Auto` and
  `Static` partitioning, per-chunk `firstprivate` clones, and `n-1` reduction
  joins.
- The serial path that mutates the caller's chunk without copying, reported as
  `Stats::serial_fallback`.
- `MergeOrder::{Tree, Deterministic}` and the observed non-associativity delta.
- `current_thread_id` (the `BLI_task_parallel_thread_id` equivalent).
- `cpx` CLI demonstrator, 8 behavioural tests and a doctest.
- CI and tag-driven release workflows producing Windows MSVC and static musl
  Linux binaries (`x86_64`, `aarch64`), plus `scripts/install.sh`.
