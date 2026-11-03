//! Robustness: deferred hints and panicking tasks.
//!
//! These tests share one process-wide hint queue, so the ones that touch it
//! serialise on `HINT_LOCK`.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chunked_parallel::threading::{parallel_for, IndexRange, TaskSizeHints};
use chunked_parallel::{lazy_threading, PoolKind, TaskGraph, TaskPool};

static HINT_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn hints_sent_by_workers_run_on_the_waiting_thread() {
    let _guard = HINT_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let _ = lazy_threading::discard_hints();

    let caller = std::thread::current().id();
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_on = Arc::new(Mutex::new(Vec::new()));

    let hints = TaskSizeHints::static_size(1);
    let chunks = Arc::new(AtomicUsize::new(0));
    parallel_for(IndexRange::new(0, 32), 1, &hints, |_range| {
        chunks.fetch_add(1, Ordering::SeqCst);
        let ran = Arc::clone(&ran);
        let ran_on = Arc::clone(&ran_on);
        lazy_threading::send_hint(move || {
            ran.fetch_add(1, Ordering::SeqCst);
            ran_on
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(std::thread::current().id());
        });
    });

    assert_eq!(chunks.load(Ordering::SeqCst), 32, "every chunk ran");
    assert_eq!(
        ran.load(Ordering::SeqCst),
        32,
        "every deferred hint was drained"
    );
    assert_eq!(lazy_threading::pending(), 0);
    let ran_on = ran_on.lock().unwrap_or_else(|poison| poison.into_inner());
    assert!(
        ran_on.iter().all(|id| *id == caller),
        "hints must run on the thread that waited"
    );
}

#[test]
fn a_pool_wait_drains_hints_too() {
    let _guard = HINT_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let _ = lazy_threading::discard_hints();

    let ran = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(PoolKind::Immediate);
    for _ in 0..4 {
        let ran = Arc::clone(&ran);
        pool.push(move || {
            let ran = Arc::clone(&ran);
            lazy_threading::send_hint(move || {
                ran.fetch_add(1, Ordering::SeqCst);
            });
        });
    }
    pool.work_and_wait();

    assert_eq!(ran.load(Ordering::SeqCst), 4);
    assert_eq!(lazy_threading::pending(), 0);
}

#[test]
fn a_panicking_task_fails_the_wait_but_the_scheduler_survives() {
    // Every wait drains the shared hint queue, so serialize with the hint tests.
    let _guard = HINT_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let progress = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(PoolKind::Immediate);

    let counter = Arc::clone(&progress);
    pool.push(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    pool.push(|| panic!("task exploded"));

    let outcome = catch_unwind(AssertUnwindSafe(|| pool.work_and_wait()));
    assert!(
        outcome.is_err(),
        "the panic must surface from work_and_wait"
    );

    // The scheduler must still accept and complete work afterwards.
    let counter = Arc::clone(&progress);
    pool.push(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    pool.work_and_wait();
    assert_eq!(progress.load(Ordering::SeqCst), 2);
}

#[test]
fn a_panicking_node_fails_the_graph_wait() {
    let _guard = HINT_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let graph = TaskGraph::new();
    let node = graph.add_node(|| panic!("node exploded"));
    graph.push_work(node);

    let outcome = catch_unwind(AssertUnwindSafe(|| graph.wait()));
    assert!(outcome.is_err(), "the panic must surface from wait");
}

#[test]
fn a_panic_does_not_lose_the_remaining_tasks() {
    let _guard = HINT_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let finished = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(PoolKind::Immediate);

    for _ in 0..8 {
        let finished = Arc::clone(&finished);
        pool.push(move || {
            finished.fetch_add(1, Ordering::SeqCst);
        });
    }
    pool.push(|| panic!("one bad task"));

    let _ = catch_unwind(AssertUnwindSafe(|| pool.work_and_wait()));

    // Whatever the panic did, no submitted work may be silently dropped: the
    // pool still has to reach a quiescent state.
    let _ = catch_unwind(AssertUnwindSafe(|| pool.work_and_wait()));
    assert_eq!(finished.load(Ordering::SeqCst), 8);
}
