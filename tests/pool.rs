//! Behavioural tests for `TaskPool`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use chunked_parallel::{PoolKind, Priority, TaskPool};

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[test]
fn immediate_runs_every_task() {
    let pool = TaskPool::new(PoolKind::Immediate);
    let done = counter();

    for _ in 0..64 {
        let done = Arc::clone(&done);
        pool.push(move || {
            done.fetch_add(1, Ordering::SeqCst);
        });
    }
    pool.work_and_wait();

    assert_eq!(done.load(Ordering::SeqCst), 64);
    assert_eq!(pool.pending(), 0);
}

#[test]
fn suspended_runs_nothing_until_waited_on() {
    let pool = TaskPool::new(PoolKind::Suspended);
    let done = counter();

    for _ in 0..16 {
        let done = Arc::clone(&done);
        pool.push(move || {
            done.fetch_add(1, Ordering::SeqCst);
        });
    }

    // The documented contract: nothing executes before work_and_wait.
    thread::sleep(Duration::from_millis(60));
    assert_eq!(done.load(Ordering::SeqCst), 0);
    assert_eq!(pool.pending(), 16);

    pool.work_and_wait();
    assert_eq!(done.load(Ordering::SeqCst), 16);
    assert_eq!(pool.pending(), 0);
}

#[test]
fn no_threads_runs_inline_on_the_caller() {
    let pool = TaskPool::new(PoolKind::NoThreads);
    let done = counter();
    let caller = thread::current().id();

    let observed = Arc::new(std::sync::Mutex::new(None));
    {
        let done = Arc::clone(&done);
        let observed = Arc::clone(&observed);
        pool.push(move || {
            done.fetch_add(1, Ordering::SeqCst);
            *observed.lock().unwrap() = Some(thread::current().id());
        });
    }

    // Already finished, before any wait.
    assert_eq!(done.load(Ordering::SeqCst), 1);
    assert_eq!(*observed.lock().unwrap(), Some(caller));
    assert!(!pool.is_threaded());
}

#[test]
fn background_starts_without_blocking_the_caller() {
    let pool = Arc::new(TaskPool::new(PoolKind::Background));
    let (started_tx, started_rx) = mpsc::channel();

    let handle = Arc::clone(&pool);
    handle.push(move || {
        let _ = started_tx.send(());
        thread::sleep(Duration::from_millis(80));
    });

    // The task must begin on its own, before work_and_wait is called.
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("background task should start immediately");

    pool.work_and_wait();
    assert_eq!(pool.pending(), 0);
}

#[test]
fn background_serial_never_runs_two_tasks_at_once() {
    let pool = TaskPool::new(PoolKind::BackgroundSerial);
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let total = counter();

    for _ in 0..8 {
        let running = Arc::clone(&running);
        let peak = Arc::clone(&peak);
        let total = Arc::clone(&total);
        pool.push(move || {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            running.fetch_sub(1, Ordering::SeqCst);
            total.fetch_add(1, Ordering::SeqCst);
        });
    }

    pool.work_and_wait();

    assert_eq!(total.load(Ordering::SeqCst), 8);
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "serial pool must not overlap"
    );
}

#[test]
fn nested_pools_complete_without_deadlock() {
    // A task that creates another pool, fills it, and waits inside the task
    // that the outer pool is waiting for. Blender relies on wait() doing work
    // to avoid running out of threads here.
    let outer = Arc::new(TaskPool::new(PoolKind::Immediate));
    let completed = counter();

    for _ in 0..4 {
        let completed = Arc::clone(&completed);
        outer.push(move || {
            let inner = TaskPool::new(PoolKind::Immediate);
            let inner_done = counter();
            for _ in 0..4 {
                let inner_done = Arc::clone(&inner_done);
                inner.push(move || {
                    thread::sleep(Duration::from_millis(1));
                    inner_done.fetch_add(1, Ordering::SeqCst);
                });
            }
            inner.work_and_wait();
            assert_eq!(inner_done.load(Ordering::SeqCst), 4);
            completed.fetch_add(1, Ordering::SeqCst);
        });
    }

    outer.work_and_wait();
    assert_eq!(completed.load(Ordering::SeqCst), 4);
}

#[test]
fn priority_is_recorded_but_not_acted_on() {
    let pool = TaskPool::with_priority(PoolKind::NoThreads, Priority::High);
    assert_eq!(pool.priority(), Priority::High);
}
