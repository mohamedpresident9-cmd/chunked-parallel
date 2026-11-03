//! Lazy threading hints, mirroring `blender::lazy_threading`.
//!
//! A worker that discovers work which is cheaper to do once — a dependency
//! graph evaluation, a lazy cache build — does not do it inline while holding a
//! chunk of a parallel loop. It *sends a hint* and keeps going; the thread that
//! next becomes idle runs the hint.
//!
//! Blender implements this with `send_hint()` plus a `Receiver` that the main
//! thread drains while it waits. The model here keeps one process-wide queue and
//! drains it on the thread that just finished waiting, which is the same
//! delivery rule without a per-arena bookkeeping layer.
//!
//! # Panic safety
//!
//! Hints run on the draining thread. If one panics, the remaining hints stay
//! queued and are run by the next drain, so one broken hint cannot silently
//! discard the others.

use std::sync::Mutex;

type Hint = Box<dyn FnOnce() + Send + 'static>;

static QUEUE: Mutex<Vec<Hint>> = Mutex::new(Vec::new());

/// Queue a hint for whichever thread drains next.
pub fn send_hint(hint: impl FnOnce() + Send + 'static) {
    let mut queue = QUEUE.lock().unwrap_or_else(|poison| poison.into_inner());
    queue.push(Box::new(hint));
}

/// Number of hints waiting to run.
pub fn pending() -> usize {
    QUEUE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .len()
}

/// Run every queued hint on the calling thread, first-in first-out.
///
/// Returns how many hints ran. Hints queued *by* a hint are left for the next
/// drain, which keeps one drain bounded.
pub fn run_hints() -> usize {
    let mut taken = {
        let mut queue = QUEUE.lock().unwrap_or_else(|poison| poison.into_inner());
        std::mem::take(&mut *queue)
    };
    let count = taken.len();
    for hint in taken.drain(..) {
        hint();
    }
    count
}

/// Drop every queued hint without running it. For tests and teardown.
pub fn discard_hints() -> usize {
    let mut queue = QUEUE.lock().unwrap_or_else(|poison| poison.into_inner());
    let count = queue.len();
    queue.clear();
    count
}
