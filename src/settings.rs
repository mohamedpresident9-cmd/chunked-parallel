//! Execution settings, mirroring Blender's `TaskParallelSettings`.

use crate::partitioner::Partitioner;

/// How the per-chunk results are combined.
///
/// Blender has two reduction entry points and they differ only in this order:
/// `parallel_reduce` (tree-shaped, fast, floating-point results are not
/// bit-reproducible) and `parallel_deterministic_reduce` (fixed order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOrder {
    /// Pairwise tree reduction, as performed by `tbb::parallel_reduce`.
    ///
    /// This is what `BLI_task_parallel_range` uses when `func_reduce` is set.
    /// It is the fastest and it is **not** bit-reproducible for floats.
    Tree,
    /// Left-to-right merge in chunk order.
    ///
    /// Chunks are contiguous and ordered, so the merge tree becomes a plain
    /// fold: the result is reproducible run-to-run for identical settings,
    /// whatever the scheduling order was.
    ///
    /// It does **not** reproduce the serial loop bit for bit in general: each
    /// chunk accumulates from its own fresh copy of the initial value instead
    /// of continuing the caller's running total, so `(a+b)+(c+d)` is evaluated
    /// where the serial loop evaluates `((a+b)+c)+d`.
    Deterministic,
}

/// Parallel-range settings.
#[derive(Debug, Clone)]
pub struct Settings {
    /// `settings->use_threading`. When `false`, the range always runs serially.
    pub use_threading: bool,
    /// `settings->min_iter_per_thread`.
    ///
    /// `0` means "use the default heuristic", which in Blender resolves to a
    /// grain of `1` (`max(min_iter_per_thread, 1)`); the partitioner then
    /// decides the real split above that floor.
    pub min_iter_per_thread: usize,
    /// Worker count. `0` means "auto" (`available_parallelism`).
    ///
    /// Blender takes this from `BLI_task_scheduler_num_threads()`, which is
    /// the system thread count unless overridden at startup.
    pub threads: usize,
    /// Reduction order.
    pub merge: MergeOrder,
    /// Splitting strategy.
    pub partitioner: Partitioner,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            use_threading: true,
            min_iter_per_thread: 0,
            threads: 0,
            merge: MergeOrder::Tree,
            partitioner: Partitioner::Auto,
        }
    }
}

impl Settings {
    /// Single-threaded configuration, equivalent to Blender's `-t 1`.
    pub fn serial() -> Self {
        Self {
            use_threading: false,
            ..Self::default()
        }
    }

    /// Effective grain: `max(min_iter_per_thread, 1)`, exactly as Blender
    /// computes `grainsize` before constructing `tbb::blocked_range`.
    pub fn grain(&self) -> usize {
        self.min_iter_per_thread.max(1)
    }

    /// Resolved worker count.
    pub fn resolved_threads(&self) -> usize {
        if self.threads > 0 {
            self.threads
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }

    pub fn with_grain(mut self, grain: usize) -> Self {
        self.min_iter_per_thread = grain;
        self
    }

    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    pub fn with_merge(mut self, merge: MergeOrder) -> Self {
        self.merge = merge;
        self
    }

    pub fn with_partitioner(mut self, partitioner: Partitioner) -> Self {
        self.partitioner = partitioner;
        self
    }
}
