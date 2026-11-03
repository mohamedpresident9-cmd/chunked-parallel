//! Shared scheduler runtime: one global queue, several worker threads.
//!
//! Blender's `BLI_task_scheduler` keeps "a single queue [that] holds the task
//! from all pools" (`BLI_task_c.hh`), and `tbb::task_group::wait()` is allowed
//! to *execute* pending work instead of only blocking. Both properties are
//! reproduced here:
//!
//! * one process-wide queue, shared by every [`TaskPool`](crate::TaskPool) and
//!   every [`TaskGraph`](crate::TaskGraph);
//! * [`Scope::wait`] participates: a waiting thread pops and runs queued jobs,
//!   which is what keeps nested pools from deadlocking when the worker pool is
//!   fully occupied.
//!
//! Waiting is per scope rather than global, so one graph does not wait for
//! unrelated pools.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Once, OnceLock};
use std::thread;
use std::time::Duration;

type TaskFn = Box<dyn FnOnce() + Send + 'static>;

/// A unit of work queued on the global scheduler.
struct Job {
    func: Option<TaskFn>,
    scope: Arc<Scope>,
}

/// Outstanding-work counter for one pool or graph.
///
/// Every submitted job increments it; the job's completion (including on panic)
/// decrements it and wakes the waiter when it reaches zero.
pub(crate) struct Scope {
    pending: AtomicUsize,
    lock: Mutex<()>,
    cv: Condvar,
    /// First panic raised by a job in this scope; re-raised on the waiter.
    panic: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

impl Scope {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: AtomicUsize::new(0),
            lock: Mutex::new(()),
            cv: Condvar::new(),
            panic: Mutex::new(None),
        })
    }

    fn record_panic(&self, payload: Box<dyn std::any::Any + Send>) {
        let mut slot = self.panic.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(payload);
        }
    }

    fn take_panic(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.panic.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn enter(&self) {
        self.pending.fetch_add(1, Ordering::AcqRel);
    }

    fn leave(&self) {
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _guard = self
                .lock
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            self.cv.notify_all();
        }
    }

    /// Number of jobs submitted but not yet finished.
    pub(crate) fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    /// Block until every job in this scope has completed.
    ///
    /// While waiting the caller drains the shared queue, mirroring
    /// `tbb::task_group::wait()` which "is called wait(), but internally it can
    /// actually do work".
    pub(crate) fn wait(&self) {
        let runtime = runtime();
        while self.pending() > 0 {
            let job = {
                let mut state = runtime.state.lock().unwrap_or_else(|p| p.into_inner());
                state.queue.pop_front()
            };
            if let Some(job) = job {
                run_job(job);
                continue;
            }
            // Nothing runnable in the queue: block briefly. The timeout bounds
            // the window in which a completion could be missed between the
            // counter check and the wait.
            let guard = self.lock.lock().unwrap_or_else(|p| p.into_inner());
            let _unused = self
                .cv
                .wait_timeout(guard, Duration::from_millis(5))
                .unwrap_or_else(|p| p.into_inner());
        }

        // This thread just finished waiting: run the hints workers deferred.
        crate::lazy_threading::run_hints();

        if let Some(payload) = self.take_panic() {
            // Fail the wait, the way a panicking task fails a join, without
            // destroying the worker that ran it.
            std::panic::resume_unwind(payload);
        }
    }
}

/// Decrements the scope counter even if the task panics.
struct ScopeGuard(Arc<Scope>);

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        self.0.leave();
    }
}

fn run_job(job: Job) {
    let scope = Arc::clone(&job.scope);
    let _guard = ScopeGuard(Arc::clone(&scope));
    if let Some(func) = job.func {
        // A panicking task must not take the worker down with it: the payload
        // is recorded on the scope and re-raised on whoever waits.
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(func)) {
            scope.record_panic(payload);
        }
    }
}

struct State {
    queue: VecDeque<Job>,
}

struct Runtime {
    threads: usize,
    state: Mutex<State>,
    cv: Condvar,
}

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static WORKERS_STARTED: Once = Once::new();
static DESIRED_THREADS: AtomicUsize = AtomicUsize::new(0);

/// Set the worker count used by the shared scheduler.
///
/// Must be called before the scheduler is first used; later calls only report
/// the effective value. Returns the number of workers actually running.
pub fn init(threads: usize) -> usize {
    if threads > 0 {
        DESIRED_THREADS.store(threads, Ordering::Relaxed);
    }
    worker_threads()
}

/// Worker threads backing the shared scheduler.
pub fn worker_threads() -> usize {
    runtime().threads
}

fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

fn runtime() -> &'static Runtime {
    let runtime = RUNTIME.get_or_init(|| {
        let desired = DESIRED_THREADS.load(Ordering::Relaxed);
        let threads = if desired > 0 {
            desired
        } else {
            default_threads()
        };
        Runtime {
            threads,
            state: Mutex::new(State {
                queue: VecDeque::new(),
            }),
            cv: Condvar::new(),
        }
    });

    WORKERS_STARTED.call_once(|| {
        for index in 0..runtime.threads {
            let spawned = thread::Builder::new()
                .name(format!("chunked-worker-{index}"))
                .spawn(move || worker_loop(runtime));
            if spawned.is_err() {
                // A thread we cannot start is one fewer worker; the queue is
                // still drained by `Scope::wait` participants.
                break;
            }
        }
    });

    runtime
}

fn worker_loop(runtime: &'static Runtime) {
    let mut state = runtime.state.lock().unwrap_or_else(|p| p.into_inner());
    loop {
        match state.queue.pop_front() {
            Some(job) => {
                drop(state);
                run_job(job);
                state = runtime.state.lock().unwrap_or_else(|p| p.into_inner());
            }
            None => {
                state = runtime.cv.wait(state).unwrap_or_else(|p| p.into_inner());
            }
        }
    }
}

/// Queue `func` on the shared scheduler under an explicit scope.
pub(crate) fn submit_to<F: FnOnce() + Send + 'static>(scope: Arc<Scope>, func: F) {
    let runtime = runtime();
    scope.enter();
    let mut state = runtime.state.lock().unwrap_or_else(|p| p.into_inner());
    state.queue.push_back(Job {
        func: Some(Box::new(func)),
        scope,
    });
    runtime.cv.notify_one();
}
