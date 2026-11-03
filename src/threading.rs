//! The modern `blender::threading` layer (`BLI_task.hh`, `BLI_task_size_hints.hh`,
//! `intern/task_range.cc`).
//!
//! This is the layer Blender itself moved to: `parallel_for` takes an
//! [`IndexRange`], a grain size, and a [`TaskSizeHints`] describing how large
//! the individual tasks are relative to each other. Three splitting strategies
//! follow from the hint type:
//!
//! | Hint | Strategy (from `parallel_for_impl`) |
//! |---|---|
//! | [`TaskSizeHints::Static`] | `final_grain = grain / task_size`, then `blocked_range` |
//! | [`TaskSizeHints::Individual`] | outer blocks of `min(grain, 512)`, per-task sizes, greedy bucketing until `counter >= grain` |
//! | [`TaskSizeHints::Accumulated`] | recursive bisection until `total_size <= grain` |
//!
//! plus the small-work escape hatch `use_single_thread`, where "invoking tbb
//! for small workloads has a large overhead".
//!
//! ## Simplifications, stated explicitly
//!
//! * The bisection in the accumulated strategy is computed serially and only
//!   the resulting leaves run in parallel. Blender recurses with
//!   `parallel_invoke`; the set of leaves is identical, the difference is where
//!   the splitting itself runs.
//! * `parallel_invoke` is variadic in C++; here it is [`parallel_invoke2`].
//! * [`isolate_task`](https://github.com/blender/blender) is not modelled: this
//!   executor joins scoped threads by blocking and never drains another scope's
//!   queue, so the nested-threading hazard isolation defends against cannot
//!   arise.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Half-open index range `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexRange {
    start: i64,
    end: i64,
}

impl IndexRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end: end.max(start),
        }
    }

    pub fn from_begin_end(begin: i64, end: i64) -> Self {
        Self::new(begin, end)
    }

    pub fn first(&self) -> i64 {
        self.start
    }

    pub fn one_after_last(&self) -> i64 {
        self.end
    }

    pub fn size(&self) -> i64 {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }

    pub fn shift(&self, offset: i64) -> Self {
        Self::new(self.start + offset, self.end + offset)
    }

    pub fn take_front(&self, count: i64) -> Self {
        let count = count.clamp(0, self.size());
        Self::new(self.start, self.start + count)
    }

    pub fn drop_front(&self, count: i64) -> Self {
        let count = count.clamp(0, self.size());
        Self::new(self.start + count, self.end)
    }

    /// `0..size()`, the index space of this range's elements.
    pub fn index_range(&self) -> Self {
        Self::new(0, self.size())
    }

    /// Iterate the covered indices.
    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.start..self.end
    }
}

type IndividualLookup = dyn Fn(IndexRange, &mut [i64]) + Send + Sync;
type AccumulatedLookup = dyn Fn(IndexRange) -> i64 + Send + Sync;

/// How large individual tasks are relative to each other.
#[derive(Clone)]
pub enum TaskSizeHints {
    /// Every task has the same size.
    Static { size: i64 },
    /// Task sizes must be looked up one by one; `full_size` is an optional
    /// cheap total used by [`use_single_thread`].
    Individual {
        lookup: Arc<IndividualLookup>,
        full_size: Option<i64>,
    },
    /// The accumulated size of any consecutive range can be computed in O(1).
    Accumulated { lookup: Arc<AccumulatedLookup> },
}

impl Default for TaskSizeHints {
    fn default() -> Self {
        Self::Static { size: 1 }
    }
}

impl TaskSizeHints {
    pub fn static_size(size: i64) -> Self {
        Self::Static { size: size.max(1) }
    }

    pub fn individual<F>(lookup: F, full_size: Option<i64>) -> Self
    where
        F: Fn(IndexRange, &mut [i64]) + Send + Sync + 'static,
    {
        Self::Individual {
            lookup: Arc::new(lookup),
            full_size,
        }
    }

    pub fn accumulated<F>(lookup: F) -> Self
    where
        F: Fn(IndexRange) -> i64 + Send + Sync + 'static,
    {
        Self::Accumulated {
            lookup: Arc::new(lookup),
        }
    }
}

/// Execution mode (`blender::exec_mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Serial,
    Parallel,
    ParallelGrainSize(i64),
}

impl ExecutionMode {
    pub fn is_parallel(self) -> bool {
        !matches!(self, Self::Serial)
    }

    /// `grain_size(fallback)`; `Serial` has no grain, reported as `i64::MAX`.
    pub fn grain_size(self, fallback: i64) -> i64 {
        match self {
            Self::Serial => i64::MAX,
            Self::Parallel => fallback,
            Self::ParallelGrainSize(size) => size.max(1),
        }
    }
}

thread_local! {
    /// Concurrency ceiling for the current thread, `0` meaning "no override".
    static BUDGET: Cell<usize> = const { Cell::new(0) };
}

fn worker_budget() -> usize {
    let base = crate::worker_threads();
    BUDGET.with(|budget| {
        let override_value = budget.get();
        if override_value == 0 {
            base
        } else {
            override_value.min(base)
        }
    })
}

fn with_budget<R>(limit: usize, function: impl FnOnce() -> R) -> R {
    let previous = BUDGET.with(|budget| {
        let previous = budget.get();
        let next = if previous == 0 {
            limit
        } else {
            previous.min(limit)
        };
        budget.set(next);
        previous
    });
    let result = function();
    BUDGET.with(|budget| budget.set(previous));
    result
}

/// Run `function` with at most `max_threads` participating threads.
///
/// Mirrors `max_threads_task_impl`: if the limit is not below the worker count,
/// the arena would be pointless, so the call runs directly.
pub fn max_threads_task<R>(max_threads: usize, function: impl FnOnce() -> R) -> R {
    assert!(max_threads >= 1, "max_threads must be at least 1");
    if max_threads >= crate::worker_threads() {
        return function();
    }
    with_budget(max_threads, function)
}

/// Blender's `memory_bandwidth_bound_task`.
///
/// Below 8 MiB the work stays in cache and is not throttled; above it, at most
/// eight threads touch memory at once, because extra threads buy no bandwidth.
pub fn memory_bandwidth_bound_task<R>(
    approximate_bytes_touched: i64,
    function: impl FnOnce() -> R,
) -> R {
    const CACHE_LIMIT: i64 = 8 * 1024 * 1024;
    const BANDWIDTH_THREADS: usize = 8;

    if approximate_bytes_touched <= CACHE_LIMIT {
        return function();
    }
    if BANDWIDTH_THREADS >= crate::worker_threads() {
        return function();
    }
    with_budget(BANDWIDTH_THREADS, function)
}

/// Whether the work is small enough that scheduling overhead would dominate.
pub fn use_single_thread(hints: &TaskSizeHints, range: IndexRange, threshold: i64) -> bool {
    match hints {
        TaskSizeHints::Static { size } => (*size).saturating_mul(range.size()) <= threshold,
        TaskSizeHints::Individual { full_size, .. } => match full_size {
            Some(full_size) => *full_size <= threshold,
            None => false,
        },
        TaskSizeHints::Accumulated { lookup } => lookup(range) <= threshold,
    }
}

fn split_uniform(range: IndexRange, grain: i64) -> Vec<IndexRange> {
    let grain = grain.max(1);
    let mut out = Vec::new();
    let mut at = range.first();
    while at < range.one_after_last() {
        let end = (at + grain).min(range.one_after_last());
        out.push(IndexRange::new(at, end));
        at = end;
    }
    out
}

fn bisect_accumulated(
    range: IndexRange,
    grain: i64,
    lookup: &AccumulatedLookup,
    out: &mut Vec<IndexRange>,
) {
    if range.size() == 1 || lookup(range) <= grain {
        out.push(range);
        return;
    }
    let middle = range.size() / 2;
    bisect_accumulated(range.take_front(middle), grain, lookup, out);
    bisect_accumulated(range.drop_front(middle), grain, lookup, out);
}

/// Compute the work chunks for a range under the given hints.
pub fn chunks_for(range: IndexRange, grain: i64, hints: &TaskSizeHints) -> Vec<IndexRange> {
    if range.is_empty() {
        return Vec::new();
    }
    let grain = grain.max(1);
    match hints {
        TaskSizeHints::Static { size } => {
            let final_grain = if *size == 1 {
                grain
            } else {
                (grain / (*size).max(1)).max(1)
            };
            split_uniform(range, final_grain)
        }
        TaskSizeHints::Individual { lookup, .. } => {
            // "Shouldn't be too small ... shouldn't be too large", hence the cap.
            let outer_grain = grain.clamp(1, 512);
            let mut out = Vec::new();
            for block in split_uniform(range, outer_grain) {
                let count = block.size();
                if count == 0 {
                    continue;
                }
                let mut sizes = vec![0i64; count as usize];
                lookup(block, &mut sizes);

                let mut offsets = Vec::new();
                offsets.push(0i64);
                let mut counter = 0i64;
                for index in 0..count {
                    counter += sizes[index as usize];
                    if counter >= grain {
                        offsets.push(index + 1);
                        counter = 0;
                    }
                }
                if *offsets.last().unwrap_or(&0) < count {
                    offsets.push(count);
                }
                for window in offsets.windows(2) {
                    if window[1] > window[0] {
                        out.push(IndexRange::new(
                            block.first() + window[0],
                            block.first() + window[1],
                        ));
                    }
                }
            }
            out
        }
        TaskSizeHints::Accumulated { lookup } => {
            let mut out = Vec::new();
            bisect_accumulated(range, grain, lookup.as_ref(), &mut out);
            out
        }
    }
}

/// Evaluate `function` for every chunk and collect results in chunk order.
fn map_chunks<C, F>(chunks: Vec<IndexRange>, function: F) -> Vec<C>
where
    C: Send,
    F: Fn(IndexRange) -> C + Sync,
{
    if chunks.is_empty() {
        return Vec::new();
    }
    let workers = worker_budget().min(chunks.len()).max(1);
    if workers == 1 {
        return chunks.into_iter().map(function).collect();
    }

    let cursor = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<C>>> = Mutex::new((0..chunks.len()).map(|_| None).collect());

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let cursor = &cursor;
            let chunks = &chunks;
            let function = &function;
            let results = &results;
            scope.spawn(move || loop {
                let index = cursor.fetch_add(1, Ordering::Relaxed);
                if index >= chunks.len() {
                    break;
                }
                let value = function(chunks[index]);
                results.lock().unwrap_or_else(|p| p.into_inner())[index] = Some(value);
            });
        }
    });

    results
        .into_inner()
        .unwrap_or_else(|p| p.into_inner())
        .into_iter()
        .map(|value| value.expect("every chunk is evaluated"))
        .collect()
}

/// Execute `function` for sub-ranges of `range`, potentially in parallel.
pub fn parallel_for<F>(range: IndexRange, grain: i64, hints: &TaskSizeHints, function: F)
where
    F: Fn(IndexRange) + Sync,
{
    if range.is_empty() {
        return;
    }
    if use_single_thread(hints, range, grain) {
        function(range);
        return;
    }
    let chunks = chunks_for(range, grain, hints);
    let _completed: Vec<()> = map_chunks(chunks, function);
    // The caller just waited; run whatever the workers deferred.
    crate::lazy_threading::run_hints();
}

/// Move sub-range boundaries down to the next aligned index.
pub fn align_sub_range(unaligned: IndexRange, alignment: i64, global: IndexRange) -> IndexRange {
    let alignment = alignment.max(1);
    let global_begin = global.first();
    let global_end = global.one_after_last();
    let mask = !(alignment - 1);
    let aligned_begin = (unaligned.first() & mask).max(global_begin);
    let aligned_end = if unaligned.one_after_last() == global_end {
        global_end
    } else {
        (unaligned.one_after_last() & mask).max(global_begin)
    };
    IndexRange::from_begin_end(aligned_begin, aligned_end)
}

/// [`parallel_for`] with sub-range sizes aligned to `alignment` multiples.
pub fn parallel_for_aligned<F>(range: IndexRange, grain: i64, alignment: i64, function: F)
where
    F: Fn(IndexRange) + Sync,
{
    parallel_for(range, grain, &TaskSizeHints::default(), |unaligned| {
        function(align_sub_range(unaligned, alignment, range));
    });
}

/// Parallel reduction with a tree-shaped merge (fast, order-dependent).
pub fn parallel_reduce<T, F, R>(
    range: IndexRange,
    grain: i64,
    hints: &TaskSizeHints,
    identity: T,
    function: F,
    reduction: R,
) -> T
where
    T: Clone + Send + Sync,
    F: Fn(IndexRange, T) -> T + Sync,
    R: Fn(T, T) -> T + Sync,
{
    if range.is_empty() {
        return identity;
    }
    if range.size() < grain || use_single_thread(hints, range, grain) {
        return function(range, identity);
    }
    let chunks = chunks_for(range, grain, hints);
    let mut partials = map_chunks(chunks, |chunk| function(chunk, identity.clone()));
    while partials.len() > 1 {
        let mut next = Vec::with_capacity(partials.len().div_ceil(2));
        let mut iter = partials.into_iter();
        while let Some(left) = iter.next() {
            match iter.next() {
                Some(right) => next.push(reduction(left, right)),
                None => next.push(left),
            }
        }
        partials = next;
    }
    partials.pop().unwrap_or(identity)
}

/// Parallel reduction with a fixed merge order (`parallel_deterministic_reduce`).
pub fn parallel_deterministic_reduce<T, F, R>(
    range: IndexRange,
    grain: i64,
    hints: &TaskSizeHints,
    identity: T,
    function: F,
    reduction: R,
) -> T
where
    T: Clone + Send + Sync,
    F: Fn(IndexRange, T) -> T + Sync,
    R: Fn(T, T) -> T + Sync,
{
    if range.is_empty() {
        return identity;
    }
    if range.size() < grain || use_single_thread(hints, range, grain) {
        return function(range, identity);
    }
    let chunks = chunks_for(range, grain, hints);
    let partials = map_chunks(chunks, |chunk| function(chunk, identity.clone()));
    let mut iter = partials.into_iter();
    let mut acc = iter.next().unwrap_or(identity);
    for partial in iter {
        acc = reduction(acc, partial);
    }
    acc
}

/// Run two closures, potentially in parallel.
pub fn parallel_invoke2<A, B>(first: A, second: B)
where
    A: FnOnce() + Send,
    B: FnOnce() + Send,
{
    if worker_budget() < 2 {
        first();
        second();
        return;
    }
    let first = Mutex::new(Some(first));
    let second = Mutex::new(Some(second));
    std::thread::scope(|scope| {
        let borrowed_first = &first;
        let borrowed_second = &second;
        scope.spawn(move || {
            if let Some(function) = borrowed_first
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
            {
                function();
            }
        });
        if let Some(function) = borrowed_second
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            function();
        }
    });
}

/// A set of index ranges, projected from `IndexMask` far enough to study its
/// segment-based parallelism.
#[derive(Debug, Clone, Default)]
pub struct IndexMask {
    segments: Vec<IndexRange>,
}

impl IndexMask {
    pub fn from_ranges(ranges: &[IndexRange]) -> Self {
        let mut segments: Vec<IndexRange> =
            ranges.iter().copied().filter(|r| !r.is_empty()).collect();
        segments.sort_unstable_by_key(|range| range.first());
        Self { segments }
    }

    pub fn from_range(range: IndexRange) -> Self {
        Self::from_ranges(&[range])
    }

    pub fn segments(&self) -> &[IndexRange] {
        &self.segments
    }

    /// Total number of indices covered.
    pub fn len(&self) -> i64 {
        self.segments.iter().map(IndexRange::size).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Split into segments of at most `grain` indices, preserving order.
    pub fn chunked_segments(&self, grain: i64) -> Vec<IndexRange> {
        let mut out = Vec::new();
        for segment in &self.segments {
            out.extend(split_uniform(*segment, grain));
        }
        out
    }

    /// Iterate the covered indices.
    pub fn iter_indices(&self) -> impl Iterator<Item = i64> + '_ {
        self.segments.iter().flat_map(IndexRange::iter)
    }

    /// `IndexMask::foreach_segment` with the parallel execution mode.
    ///
    /// Sizes are supplied through an accumulated hint, which is exactly the
    /// "efficiently determine the size of a consecutive range of tasks" case
    /// `TaskSizeHints::Accumulated` exists for.
    pub fn foreach_segment_parallel<F>(&self, grain: i64, function: F)
    where
        F: Fn(IndexRange) + Sync,
    {
        let segments = self.chunked_segments(grain);
        if segments.is_empty() {
            return;
        }

        // Prefix sums: `prefix[i]` is the accumulated size before segment `i`,
        // with a final entry for the total. Any range lookup is then O(1).
        let mut prefix: Vec<i64> = Vec::with_capacity(segments.len() + 1);
        let mut running = 0i64;
        prefix.push(0);
        for segment in &segments {
            running += segment.size();
            prefix.push(running);
        }
        let prefix = Arc::new(prefix);

        let hints = TaskSizeHints::accumulated({
            let prefix = Arc::clone(&prefix);
            move |range: IndexRange| {
                let begin = range.first().clamp(0, prefix.len() as i64 - 1) as usize;
                let end = range.one_after_last().clamp(0, prefix.len() as i64 - 1) as usize;
                prefix[end] - prefix[begin]
            }
        });

        parallel_for(
            IndexRange::new(0, segments.len() as i64),
            grain,
            &hints,
            |range| {
                for index in range.iter() {
                    function(segments[index as usize]);
                }
            },
        );
    }
}
