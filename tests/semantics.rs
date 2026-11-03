//! Behavioural tests for the two rules that Blender's parallel range encodes:
//! the serial path does not copy, and the merge order is observable.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chunked_parallel::{
    current_thread_id, parallel_range, Chunk, MergeOrder, Partitioner, Settings,
};

// ---------------------------------------------------------------------------
// Test chunk: a sum plus a live-instance counter (stands in for func_free).
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Tracked {
    sum: f64,
    seen: u64,
    live: Arc<AtomicUsize>,
}

impl Tracked {
    fn new(live: &Arc<AtomicUsize>) -> Self {
        live.fetch_add(1, Ordering::SeqCst);
        Self {
            sum: 0.0,
            seen: 0,
            live: Arc::clone(live),
        }
    }
}

impl Clone for Tracked {
    fn clone(&self) -> Self {
        self.live.fetch_add(1, Ordering::SeqCst);
        Self {
            sum: self.sum,
            seen: self.seen,
            live: Arc::clone(&self.live),
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Chunk for Tracked {
    fn join(&mut self, other: &Self) {
        self.sum += other.sum;
        self.seen += other.seen;
    }
}

fn add_one(index: i32, chunk: &mut Tracked) {
    chunk.sum += 1.0 / (index as f64 + 1.0);
    chunk.seen += 1;
}

fn serial_sum(items: i32) -> f64 {
    let mut sum = 0.0;
    for i in 0..items {
        sum += 1.0 / (i as f64 + 1.0);
    }
    sum
}

// ---------------------------------------------------------------------------
// Rule 1: serial path mutates the caller's chunk in place, no copies.
// ---------------------------------------------------------------------------

#[test]
fn serial_path_does_not_clone() {
    let live = Arc::new(AtomicUsize::new(0));
    let settings = Settings::serial().with_threads(8);

    let executed = parallel_range(0, 1000, Tracked::new(&live), &settings, add_one);

    assert!(executed.stats.serial_fallback);
    assert_eq!(executed.stats.chunks, 1);
    assert_eq!(executed.stats.clones, 0);
    assert_eq!(executed.stats.joins, 0);
    // Exactly one instance ever existed: the caller's, returned to us.
    assert_eq!(live.load(Ordering::SeqCst), 1);
    assert_eq!(executed.chunk.seen, 1000);
}

#[test]
fn single_thread_also_takes_the_serial_path() {
    let live = Arc::new(AtomicUsize::new(0));
    let settings = Settings::default().with_threads(1);

    let executed = parallel_range(0, 500, Tracked::new(&live), &settings, add_one);

    assert!(executed.stats.serial_fallback);
    assert_eq!(executed.stats.clones, 0);
}

// ---------------------------------------------------------------------------
// Rule 2: parallel path clones once per chunk and merges chunks - 1 times.
// ---------------------------------------------------------------------------

#[test]
fn parallel_path_clones_per_chunk_and_joins() {
    let live = Arc::new(AtomicUsize::new(0));
    let settings = Settings::default()
        .with_threads(4)
        .with_grain(8)
        .with_partitioner(Partitioner::Static);

    let executed = parallel_range(0, 1000, Tracked::new(&live), &settings, add_one);
    let stats = executed.stats;

    assert!(!stats.serial_fallback);
    assert!(stats.chunks > 1, "expected several chunks, got {stats:?}");
    assert_eq!(stats.clones, stats.chunks, "one clone per chunk");
    assert_eq!(stats.joins, stats.chunks - 1, "n-1 merges");
    assert_eq!(stats.workers, 4);

    // All worker copies were dropped; only the returned chunk survives.
    assert_eq!(live.load(Ordering::SeqCst), 1);
    assert_eq!(executed.chunk.seen, 1000);
}

// ---------------------------------------------------------------------------
// Grain semantics: grain = max(min_iter_per_thread, 1) is a floor on chunk size.
// ---------------------------------------------------------------------------

#[test]
fn grain_controls_chunk_count() {
    let run = |grain: usize| {
        let settings = Settings::default()
            .with_threads(2)
            .with_grain(grain)
            .with_partitioner(Partitioner::Static);
        parallel_range(
            0,
            1000,
            Tracked::new(&Arc::new(AtomicUsize::new(0))),
            &settings,
            add_one,
        )
        .stats
        .chunks
    };

    // Static: ceil(len / grain), capped at threads * 4.
    assert_eq!(run(0), 8, "grain 0 floors to 1 -> 1000 chunks, capped at 8");
    assert_eq!(run(250), 4);
    assert_eq!(run(500), 2);
    assert_eq!(run(1000), 1);
    assert_eq!(
        run(10_000),
        1,
        "grain larger than the range yields one chunk"
    );
}

#[test]
fn auto_partitioner_splits_geometrically() {
    let settings = Settings::default()
        .with_threads(4)
        .with_grain(1)
        .with_partitioner(Partitioner::Auto);
    let stats = parallel_range(
        0,
        1024,
        Tracked::new(&Arc::new(AtomicUsize::new(0))),
        &settings,
        add_one,
    )
    .stats;

    assert_eq!(stats.chunks, 16, "threads * 4 over-decomposition");
    assert!(stats.chunks.is_power_of_two());
}

// ---------------------------------------------------------------------------
// Determinism: the ordered fold is reproducible run-to-run.
//
// It is *not* a bit-for-bit clone of the serial loop: every chunk starts from a
// fresh copy of the initial value, so the expression tree differs even though
// the chunk order is fixed.
// ---------------------------------------------------------------------------

#[test]
fn deterministic_merge_is_reproducible_across_runs() {
    let items = 20_000;
    let settings = Settings::default()
        .with_threads(4)
        .with_grain(7)
        .with_merge(MergeOrder::Deterministic);

    let run = || {
        parallel_range(
            0,
            items,
            Tracked::new(&Arc::new(AtomicUsize::new(0))),
            &settings,
            add_one,
        )
    };

    let first = run();
    assert_eq!(first.chunk.seen, items as u64);
    for _ in 0..8 {
        let again = run();
        assert_eq!(
            again.chunk.sum.to_bits(),
            first.chunk.sum.to_bits(),
            "ordered fold must be reproducible"
        );
    }
    // Whatever the value is, it stays within floating-point rounding of serial.
    assert!((first.chunk.sum - serial_sum(items)).abs() < 1e-9);
}

#[test]
fn tree_merge_is_stable_in_count_but_may_round_differently() {
    let items = 20_000;
    let settings = Settings::default()
        .with_threads(4)
        .with_grain(7)
        .with_merge(MergeOrder::Tree);

    let reference = serial_sum(items);
    let executed = parallel_range(
        0,
        items,
        Tracked::new(&Arc::new(AtomicUsize::new(0))),
        &settings,
        add_one,
    );

    assert_eq!(executed.chunk.seen, items as u64);
    // Same value up to floating-point rounding, never a lost update.
    assert!((executed.chunk.sum - reference).abs() < 1e-9);
}

// ---------------------------------------------------------------------------
// Thread identity helper (BLI_task_parallel_thread_id equivalent).
// ---------------------------------------------------------------------------

#[test]
fn thread_ids_are_stable_and_distinct_per_thread() {
    let here = current_thread_id();
    assert_eq!(here, current_thread_id(), "stable within a thread");

    let other = std::thread::spawn(current_thread_id).join().unwrap();
    assert_ne!(here, other, "distinct across threads");
}
