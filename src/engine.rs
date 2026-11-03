//! The executor: `BLI_task_parallel_range` semantics in Rust.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::chunk::Chunk;
use crate::partitioner::split;
use crate::settings::{MergeOrder, Settings};

/// Observable counters, so the data-flow rules can be asserted in tests
/// instead of taken on faith.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Number of work chunks (Blender: number of `RangeTask` instances).
    pub chunks: usize,
    /// Number of chunk copies made (`memcpy` of `userdata_chunk`).
    pub clones: usize,
    /// Number of `func_reduce` calls.
    pub joins: usize,
    /// Worker threads actually spawned.
    pub workers: usize,
    /// `true` when the serial path ran (no copies, `func_free` once).
    pub serial_fallback: bool,
}

/// Result of one parallel-range execution.
#[derive(Debug)]
pub struct Executed<C> {
    /// The reduced chunk (Blender: `settings->userdata_chunk` afterwards).
    pub chunk: C,
    pub stats: Stats,
}

/// Run `func` for every index in `[start, stop)` over a partitioned range.
///
/// Semantics, in the order Blender applies them:
///
/// 1. **Serial path** when `use_threading == false` or `threads <= 1`:
///    the caller's `initial` chunk is used directly (no clone), `func` runs on
///    the calling thread, and the chunk is returned as-is. This matters: no
///    copy happens and a `Drop`-based `func_free` fires once at the end.
/// 2. **Parallel path** otherwise: the range is split into contiguous chunks,
///    each worker clones `initial` (`firstprivate`), runs `func` on its own
///    copy, and the copies are merged with [`Chunk::join`].
///
/// Unlike Blender there is no `void*` and no function pointers: the chunk type
/// carries both the payload and the merge rule.
pub fn parallel_range<C, F>(
    start: i32,
    stop: i32,
    initial: C,
    settings: &Settings,
    func: F,
) -> Executed<C>
where
    C: Chunk,
    F: Fn(i32, &mut C) + Sync,
{
    if stop <= start {
        return Executed {
            chunk: initial,
            stats: Stats::default(),
        };
    }

    let threads = settings.resolved_threads();
    let grain = settings.grain();

    if !settings.use_threading || threads <= 1 {
        let mut chunk = initial;
        for index in start..stop {
            func(index, &mut chunk);
        }
        return Executed {
            chunk,
            stats: Stats {
                chunks: 1,
                clones: 0,
                joins: 0,
                workers: 1,
                serial_fallback: true,
            },
        };
    }

    let ranges = split(start, stop, grain, threads, settings.partitioner);
    let chunk_count = ranges.len();
    let workers = threads.min(chunk_count).max(1);
    let cursor = AtomicUsize::new(0);
    let mut collected: Vec<(usize, C)> = Vec::with_capacity(chunk_count);

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let cursor = &cursor;
            let ranges = &ranges;
            let initial = &initial;
            let func = &func;
            handles.push(scope.spawn(move || {
                let mut local: Vec<(usize, C)> = Vec::new();
                loop {
                    // Work stealing: workers pull chunks in completion order.
                    let index = cursor.fetch_add(1, Ordering::Relaxed);
                    if index >= ranges.len() {
                        break;
                    }
                    let (begin, end) = ranges[index];
                    let mut chunk = initial.clone();
                    for i in begin..end {
                        func(i, &mut chunk);
                    }
                    local.push((index, chunk));
                }
                local
            }));
        }
        for handle in handles {
            let part = handle.join().expect("worker thread panicked");
            collected.extend(part);
        }
    });

    collected.sort_unstable_by_key(|(index, _)| *index);
    let mut parts: Vec<C> = collected.into_iter().map(|(_, chunk)| chunk).collect();
    let mut joins = 0usize;

    let chunk = match settings.merge {
        MergeOrder::Deterministic => {
            let mut iter = parts.into_iter();
            let mut acc = iter.next().expect("at least one chunk");
            for next in iter {
                acc.join(&next);
                joins += 1;
            }
            acc
        }
        MergeOrder::Tree => {
            while parts.len() > 1 {
                let mut next_level = Vec::with_capacity(parts.len().div_ceil(2));
                let mut iter = parts.into_iter();
                while let Some(mut left) = iter.next() {
                    match iter.next() {
                        Some(right) => {
                            left.join(&right);
                            joins += 1;
                            next_level.push(left);
                        }
                        None => next_level.push(left),
                    }
                }
                parts = next_level;
            }
            parts.pop().expect("at least one chunk")
        }
    };

    Executed {
        chunk,
        stats: Stats {
            chunks: chunk_count,
            clones: chunk_count,
            joins,
            workers,
            serial_fallback: false,
        },
    }
}

thread_local! {
    static THREAD_ID: Cell<isize> = const { Cell::new(-1) };
}

static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

/// Stable per-thread identifier, mirroring `BLI_task_parallel_thread_id`.
///
/// Blender implements this with `tbb::enumerable_thread_specific<int>` and
/// marks it deprecated in favour of per-chunk state; prefer [`Chunk`].
pub fn current_thread_id() -> usize {
    THREAD_ID.with(|slot| {
        if slot.get() < 0 {
            let id = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
            slot.set(id as isize);
        }
        slot.get() as usize
    })
}
