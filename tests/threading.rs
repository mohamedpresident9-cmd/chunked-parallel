//! Behavioural tests for the `blender::threading` projection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chunked_parallel::threading::{
    chunks_for, max_threads_task, memory_bandwidth_bound_task, parallel_deterministic_reduce,
    parallel_for, parallel_for_aligned, use_single_thread, ExecutionMode, IndexMask, IndexRange,
    TaskSizeHints,
};

#[test]
fn static_hints_divide_the_grain_by_task_size() {
    // grain 10 with task size 2 -> final grain 5 -> 20 chunks over 100 indices.
    let hints = TaskSizeHints::static_size(2);
    let chunks = chunks_for(IndexRange::new(0, 100), 10, &hints);
    assert_eq!(chunks.len(), 20);
    assert!(chunks.iter().all(|chunk| chunk.size() == 5));
    assert_eq!(chunks.first().unwrap().first(), 0);
    assert_eq!(chunks.last().unwrap().one_after_last(), 100);
}

#[test]
fn static_hints_of_size_one_keep_the_grain() {
    let hints = TaskSizeHints::static_size(1);
    let chunks = chunks_for(IndexRange::new(0, 100), 10, &hints);
    assert_eq!(chunks.len(), 10);
    assert!(chunks.iter().all(|chunk| chunk.size() == 10));
}

#[test]
fn individual_hints_bucket_until_the_grain_is_reached() {
    // Ten tasks whose sizes grow 1..=10; a bucket closes once it reaches 10.
    let hints = TaskSizeHints::individual(
        |range: IndexRange, sizes: &mut [i64]| {
            for (offset, slot) in sizes.iter_mut().enumerate() {
                *slot = range.first() + offset as i64 + 1;
            }
        },
        Some(55),
    );

    let chunks = chunks_for(IndexRange::new(0, 10), 10, &hints);
    // 1+2+3+4 = 10 -> first bucket covers indices 0..4.
    assert_eq!(chunks.first().unwrap(), &IndexRange::new(0, 4));
    // Coverage is complete and ordered.
    assert_eq!(chunks.first().unwrap().first(), 0);
    assert_eq!(chunks.last().unwrap().one_after_last(), 10);
    for pair in chunks.windows(2) {
        assert_eq!(pair[0].one_after_last(), pair[1].first());
    }
}

#[test]
fn accumulated_hints_bisect_until_below_the_grain() {
    let hints = TaskSizeHints::accumulated(|range: IndexRange| range.size());
    let chunks = chunks_for(IndexRange::new(0, 100), 10, &hints);

    assert!(chunks.len() > 1);
    assert!(
        chunks.iter().all(|chunk| chunk.size() <= 10),
        "every leaf must fit the grain"
    );
    assert_eq!(chunks.first().unwrap().first(), 0);
    assert_eq!(chunks.last().unwrap().one_after_last(), 100);
    let covered: i64 = chunks.iter().map(IndexRange::size).sum();
    assert_eq!(covered, 100);
}

#[test]
fn small_work_runs_inline_on_the_calling_thread() {
    let hints = TaskSizeHints::static_size(1);
    let range = IndexRange::new(0, 4);
    assert!(use_single_thread(&hints, range, 10));

    let caller = thread::current().id();
    let seen = Mutex::new(None);
    parallel_for(range, 10, &hints, |sub| {
        *seen.lock().unwrap() = Some(thread::current().id());
        assert_eq!(sub, range);
    });
    assert_eq!(*seen.lock().unwrap(), Some(caller));
}

#[test]
fn parallel_for_visits_every_index_exactly_once() {
    let range = IndexRange::new(0, 1000);
    let seen = Arc::new(Mutex::new(vec![0u32; 1000]));
    let hints = TaskSizeHints::static_size(1);

    parallel_for(range, 8, &hints, |sub| {
        for index in sub.iter() {
            seen.lock().unwrap()[index as usize] += 1;
        }
    });

    assert!(seen.lock().unwrap().iter().all(|count| *count == 1));
}

#[test]
fn aligned_sub_ranges_snap_to_alignment() {
    let range = IndexRange::new(0, 100);
    let starts = Arc::new(Mutex::new(Vec::new()));

    parallel_for_aligned(range, 7, 8, |sub| {
        assert_eq!(sub.first() % 8, 0, "start must be aligned: {sub:?}");
        if sub.one_after_last() != 100 {
            assert_eq!(sub.one_after_last() % 8, 0, "end must be aligned: {sub:?}");
        }
        starts.lock().unwrap().push(sub);
    });

    let visited: i64 = starts.lock().unwrap().iter().map(IndexRange::size).sum();
    assert!(visited > 0);
}

#[test]
fn max_threads_task_caps_concurrency() {
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let hints = TaskSizeHints::static_size(1);

    max_threads_task(2, || {
        parallel_for(IndexRange::new(0, 32), 1, &hints, |sub| {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(2));
            running.fetch_sub(1, Ordering::SeqCst);
            for index in sub.iter() {
                let _ = index;
            }
        });
    });

    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "concurrency exceeded the requested limit"
    );
}

#[test]
fn small_memory_footprint_is_not_throttled() {
    let caller = thread::current().id();
    let observed = memory_bandwidth_bound_task(1024, || thread::current().id());
    assert_eq!(observed, caller);
}

#[test]
fn deterministic_reduce_matches_the_ordered_fold() {
    let hints = TaskSizeHints::static_size(1);
    let range = IndexRange::new(0, 500);

    let value = parallel_deterministic_reduce(
        range,
        8,
        &hints,
        0i64,
        |sub, acc| acc + sub.size(),
        |left, right| left + right,
    );
    assert_eq!(value, 500);

    let value = parallel_deterministic_reduce(
        range,
        8,
        &hints,
        0.0f64,
        |sub, acc| acc + sub.iter().map(|i| 1.0 / (i as f64 + 1.0)).sum::<f64>(),
        |left, right| left + right,
    );
    let reference: f64 = (0..500).map(|i| 1.0 / (i as f64 + 1.0)).sum();
    assert!((value - reference).abs() < 1e-9);
}

#[test]
fn execution_mode_reports_grain() {
    assert_eq!(ExecutionMode::Parallel.grain_size(1000), 1000);
    assert_eq!(ExecutionMode::ParallelGrainSize(64).grain_size(1000), 64);
    assert!(!ExecutionMode::Serial.is_parallel());
    assert_eq!(ExecutionMode::Serial.grain_size(1000), i64::MAX);
}

#[test]
fn index_mask_splits_into_segments() {
    let mask = IndexMask::from_ranges(&[IndexRange::new(0, 5), IndexRange::new(10, 15)]);
    assert_eq!(mask.len(), 10);
    assert!(!mask.is_empty());

    let segments = mask.chunked_segments(3);
    assert_eq!(segments.len(), 4);
    assert!(segments.iter().all(|segment| segment.size() <= 3));
    let covered: i64 = segments.iter().map(IndexRange::size).sum();
    assert_eq!(covered, 10);

    let indices: Vec<i64> = mask.iter_indices().collect();
    assert_eq!(indices, vec![0, 1, 2, 3, 4, 10, 11, 12, 13, 14]);
}

#[test]
fn foreach_segment_parallel_visits_every_segment_once() {
    let mask = IndexMask::from_ranges(&[IndexRange::new(0, 100), IndexRange::new(1000, 1100)]);
    let visited = Arc::new(AtomicUsize::new(0));

    mask.foreach_segment_parallel(16, |segment| {
        visited.fetch_add(segment.size() as usize, Ordering::SeqCst);
    });

    assert_eq!(visited.load(Ordering::SeqCst), mask.len() as usize);
}
