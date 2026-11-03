# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project adheres to [Semantic Versioning](https://semver.org/).

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
