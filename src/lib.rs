//! # chunked-parallel
//!
//! A small, dependency-free Rust model of Blender's `BLI_task_parallel_range`
//! data-flow semantics, so the behaviour can be studied, tested and re-used
//! outside the Blender codebase.
//!
//! ## What is modelled
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
//! | `BLI_task_parallel_thread_id`                     | [`current_thread_id`] |
//! | `parallel_reduce` / `parallel_deterministic_reduce` | [`MergeOrder`] |
//!
//! ## Two rules that are easy to get wrong
//!
//! 1. **Serial path does not copy.** With one thread (or threading disabled)
//!    the caller's chunk is mutated in place and returned; `Drop` runs once.
//! 2. **Reduction order is visible.** A tree merge is fast but order-dependent,
//!    so floats are not bit-reproducible. `MergeOrder::Deterministic` folds the
//!    chunks in index order, which *is* reproducible run-to-run, but it still
//!    evaluates `(a+b)+(c+d)` instead of the serial `((a+b)+c)+d`, so it does
//!    not have to match the serial loop bit for bit.
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
mod partitioner;
mod settings;

pub use chunk::Chunk;
pub use engine::{current_thread_id, parallel_range, Executed, Stats};
pub use partitioner::Partitioner;
pub use settings::{MergeOrder, Settings};
