//! `TaskPool`: explicit tasks that may be waited for as a group.
//!
//! Mirrors `BLI_task_pool_create*` from `BLI_task_c.hh` and `intern/task_pool.cc`.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::runtime::{self, Scope};

type TaskFn = Box<dyn FnOnce() + Send + 'static>;

/// The five pool flavours Blender exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolKind {
    /// `BLI_task_pool_create`: tasks start as soon as they are pushed, on the
    /// caller's thread or a worker.
    Immediate,
    /// `BLI_task_pool_create_background`: never blocks the caller.
    ///
    /// With more than one worker Blender promotes this to the shared TBB pool
    /// (`if (type == TASK_POOL_BACKGROUND && use_threads) type = TASK_POOL_TBB;`);
    /// otherwise it falls back to one dedicated thread.
    Background,
    /// `BLI_task_pool_create_background_serial`: one task after another,
    /// never in parallel, always off the calling thread.
    BackgroundSerial,
    /// `BLI_task_pool_create_suspended`: nothing runs until
    /// [`TaskPool::work_and_wait`].
    Suspended,
    /// `BLI_task_pool_create_no_threads`: runs on the caller immediately;
    /// meant for debugging single-threaded behaviour.
    NoThreads,
}

/// Task priority.
///
/// Blender keeps the parameter for API compatibility but ignores it on
/// TBB 2021 and newer (`TBB_INTERFACE_VERSION_MAJOR >= 12`), which is what
/// Blender 5.x ships. It is recorded here for fidelity, not acted upon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Priority {
    #[default]
    Low,
    High,
}

/// How a [`PoolKind`] resolved for the current worker count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolved {
    Inline,
    Shared,
    Suspended,
    Dedicated,
}

/// A group of tasks executed by the shared scheduler.
#[must_use]
pub struct TaskPool {
    resolved: Resolved,
    scope: Arc<Scope>,
    suspended: Mutex<Vec<TaskFn>>,
    background: Option<Background>,
    priority: Priority,
    threads: usize,
}

impl TaskPool {
    pub fn new(kind: PoolKind) -> Self {
        Self::with_priority(kind, Priority::Low)
    }

    pub fn with_priority(kind: PoolKind, priority: Priority) -> Self {
        let threads = runtime::worker_threads();
        let resolved = match kind {
            PoolKind::NoThreads => Resolved::Inline,
            PoolKind::Suspended => Resolved::Suspended,
            PoolKind::Immediate => {
                if threads > 1 {
                    Resolved::Shared
                } else {
                    Resolved::Inline
                }
            }
            PoolKind::Background => {
                if threads > 1 {
                    // Blender promotes this to the shared scheduler.
                    Resolved::Shared
                } else {
                    Resolved::Dedicated
                }
            }
            PoolKind::BackgroundSerial => Resolved::Dedicated,
        };

        Self {
            resolved,
            scope: Scope::new(),
            suspended: Mutex::new(Vec::new()),
            background: match resolved {
                Resolved::Dedicated => Some(Background::start()),
                _ => None,
            },
            priority,
            threads,
        }
    }

    /// The pool kind after resolution against the worker count.
    pub fn is_threaded(&self) -> bool {
        matches!(self.resolved, Resolved::Shared | Resolved::Dedicated)
    }

    /// Recorded priority (not acted upon, see [`Priority`]).
    pub fn priority(&self) -> Priority {
        self.priority
    }

    /// Worker count this pool resolved against.
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Queue one task.
    ///
    /// Blender's `BLI_task_pool_push(pool, run, taskdata, free_taskdata,
    /// freedata)` splits ownership across four arguments; in Rust the closure
    /// owns `taskdata`, and `free_taskdata`/`freedata` collapse into its `Drop`.
    /// Share state between tasks with `Arc` and pass `Arc::clone` into each
    /// closure.
    pub fn push<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self.resolved {
            Resolved::Inline => task(),
            Resolved::Shared => runtime::submit_to(Arc::clone(&self.scope), task),
            Resolved::Suspended => self
                .suspended
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(Box::new(task)),
            Resolved::Dedicated => {
                if let Some(background) = &self.background {
                    background.submit(Box::new(task));
                }
            }
        }
    }

    /// Wait until every task of this pool has finished.
    ///
    /// For a suspended pool this also releases the buffered tasks, which is the
    /// documented contract: "don't execute tasks until work_and_wait is called".
    pub fn work_and_wait(&self) {
        match self.resolved {
            Resolved::Inline => {}
            Resolved::Shared => self.scope.wait(),
            Resolved::Suspended => {
                let buffered = {
                    let mut guard = self
                        .suspended
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    std::mem::take(&mut *guard)
                };
                for task in buffered {
                    runtime::submit_to(Arc::clone(&self.scope), task);
                }
                self.scope.wait();
            }
            Resolved::Dedicated => {
                if let Some(background) = &self.background {
                    background.wait_idle();
                }
            }
        }
    }

    /// Tasks pushed but not yet completed.
    pub fn pending(&self) -> usize {
        match self.resolved {
            Resolved::Inline => 0,
            Resolved::Shared => self.scope.pending(),
            Resolved::Suspended => {
                self.scope.pending()
                    + self
                        .suspended
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .len()
            }
            Resolved::Dedicated => self
                .background
                .as_ref()
                .map(|background| background.pending())
                .unwrap_or(0),
        }
    }
}

impl Drop for TaskPool {
    fn drop(&mut self) {
        // Blender's destructor waits for background work before joining the
        // thread, so a dropped pool never leaves a task half-run.
        if let Some(background) = &self.background {
            background.wait_idle();
        }
    }
}

// ---------------------------------------------------------------------------
// Dedicated background thread (single-thread fallback and serial pool).
// ---------------------------------------------------------------------------

struct BackgroundState {
    queue: VecDeque<TaskFn>,
    running: bool,
    shutdown: bool,
}

struct BackgroundShared {
    state: Mutex<BackgroundState>,
    cv: Condvar,
}

struct Background {
    shared: Arc<BackgroundShared>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Background {
    fn start() -> Self {
        let shared = Arc::new(BackgroundShared {
            state: Mutex::new(BackgroundState {
                queue: VecDeque::new(),
                running: false,
                shutdown: false,
            }),
            cv: Condvar::new(),
        });
        let worker = Arc::clone(&shared);
        let handle = thread::Builder::new()
            .name("chunked-background".to_string())
            .spawn(move || background_loop(worker))
            .ok();
        Self {
            shared,
            handle: Mutex::new(handle),
        }
    }

    fn submit(&self, task: TaskFn) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.queue.push_back(task);
        self.shared.cv.notify_all();
    }

    fn wait_idle(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while !state.queue.is_empty() || state.running {
            state = self
                .shared
                .cv
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
        }
    }

    fn pending(&self) -> usize {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.queue.len() + usize::from(state.running)
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.shutdown = true;
            self.shared.cv.notify_all();
        }
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(handle) = handle {
            let _joined = handle.join();
        }
    }
}

fn background_loop(shared: Arc<BackgroundShared>) {
    loop {
        let task = {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            loop {
                if let Some(task) = state.queue.pop_front() {
                    state.running = true;
                    break Some(task);
                }
                if state.shutdown {
                    break None;
                }
                state = shared
                    .cv
                    .wait(state)
                    .unwrap_or_else(|poison| poison.into_inner());
            }
        };

        match task {
            Some(task) => {
                task();
                let mut state = shared
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                state.running = false;
                shared.cv.notify_all();
            }
            None => break,
        }
    }
}
