//! # chunked-parallel
//!
//! A small, dependency-free Rust model of Blender's task-scheduling semantics,
//! so the behaviour can be studied, tested and re-used outside the Blender
//! codebase.
//!
//! Two layers are modelled.
//!
//! ## Layer 1 — data-parallel ranges (`BLI_task_parallel_range`)
//!
//! | Blender (`BLI_task_c.hh` / `intern/task_range.cc`) | Here |
//! |---------------------------------------------------|------|
//! | `settings->min_iter_per_thread`                   | [`Settings::min_iter_per_thread`] |
//! | `grainsize = max(min_iter_per_thread, 1)`         | [`Settings::grain`] |
//! | `tbb::blocked_range<int>` + partitioner           | [`Partitioner`] |
//! | `RangeTask::init_chunk` (`memcpy` per split)      | [`Chunk::clone`] |
//! | `func(userdata, i, &tls)`                         | `func(i, &mut chunk)` |
//! | `RangeTask::join` → `func_reduce`                 | [`Chunk::join`] |
//! | `func_free`                                       | `Drop` |
//! | `parallel_reduce` / `parallel_deterministic_reduce` | [`MergeOrder`] |
//!
//! **Rule 1 — the serial path does not copy.** With one worker, or threading
//! disabled, the caller's chunk is mutated in place and returned: no clone, one
//! `Drop`, `Stats::serial_fallback == true`.
//!
//! **Rule 2 — the merge order is visible.** [`MergeOrder::Tree`] is the fast
//! pairwise reduction and is not bit-reproducible for floats.
//! [`MergeOrder::Deterministic`] folds chunks in index order and is
//! reproducible run-to-run, but still evaluates `(a+b)+(c+d)` rather than the
//! serial `((a+b)+c)+d`.
//!
//! ## Layer 2 — explicit tasks and dependency graphs
//!
//! | Blender | Here |
//! |---|---|
//! | `BLI_task_pool_create*` (five flavours) | [`TaskPool`], [`PoolKind`] |
//! | `BLI_task_pool_push` | [`TaskPool::push`] |
//! | `BLI_task_pool_work_and_wait` | [`TaskPool::work_and_wait`] |
//! | `BLI_task_graph_create` / `node_create` / `edge_create` | [`TaskGraph`] |
//! | `BLI_task_graph_node_push_work` | [`TaskGraph::push_work`] |
//! | `BLI_task_graph_work_and_wait` | [`TaskGraph::wait`] |
//! | central queue shared by all pools | one process-wide runtime |
//! | `tbb::task_group::wait()` may execute work | waiters drain the queue |
//!
//! **Rule 3 — graph propagation is edge-triggered.** A node with two parents
//! runs twice, once per parent, not once after both. See [`TaskGraph`].
//!
//! **Rule 4 — a suspended pool runs nothing until it is waited on.**
//!
//! ## Design note
//!
//! [`parallel_range`] runs on scoped threads so the callback may borrow data
//! without `'static` or `Arc`. Pools and graphs go through the shared runtime,
//! because tasks outlive the call that submitted them.
//!
//! ## Example
//!
//! ```
//! use chunked_parallel::{parallel_range, Chunk, MergeOrder, Settings};
//!
//! #[derive(Clone, Default)]
//! struct Sum(f64);
//!
//! impl Chunk for Sum {
//!     fn join(&mut self, other: &Self) {
//!         self.0 += other.0;
//!     }
//! }
//!
//! let settings = Settings::default()
//!     .with_grain(64)
//!     .with_merge(MergeOrder::Deterministic);
//!
//! let done = parallel_range(0, 1000, Sum::default(), &settings, |i, chunk: &mut Sum| {
//!     chunk.0 += 1.0 / (i as f64 + 1.0);
//! });
//!
//! assert!(done.chunk.0 > 7.0);
//! assert_eq!(done.stats.joins, done.stats.chunks - 1);
//! ```

mod chunk;
mod engine;
mod graph;
mod partitioner;
mod pool;
mod runtime;
mod settings;

pub use chunk::Chunk;
pub use engine::{current_thread_id, parallel_range, Executed, Stats};
pub use graph::{NodeId, TaskGraph};
pub use partitioner::Partitioner;
pub use pool::{PoolKind, Priority, TaskPool};
pub use settings::{MergeOrder, Settings};

pub use runtime::{init as init_scheduler, worker_threads};
