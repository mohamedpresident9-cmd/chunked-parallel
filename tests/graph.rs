//! Behavioural tests for `TaskGraph`, focused on edge-triggered propagation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chunked_parallel::TaskGraph;

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[test]
fn linear_chain_runs_in_order() {
    let graph = TaskGraph::new();
    let order = Arc::new(Mutex::new(Vec::new()));

    let mut root = None;
    let mut previous = None;
    for label in ["a", "b", "c"] {
        let order = Arc::clone(&order);
        let id = graph.add_node(move || order.lock().unwrap().push(label));
        if root.is_none() {
            root = Some(id);
        }
        if let Some(from) = previous {
            graph.add_edge(from, id);
        }
        previous = Some(id);
    }

    assert!(graph.push_work(root.expect("root")));
    graph.wait();

    assert_eq!(*order.lock().unwrap(), vec!["a", "b", "c"]);
}

#[test]
fn diamond_triggers_the_child_once_per_parent() {
    //   root
    //   /  \
    //  a    b
    //   \  /
    //    c        <- two parents, so c must run twice
    let graph = TaskGraph::new();
    let root_hits = counter();
    let a_hits = counter();
    let b_hits = counter();
    let c_hits = counter();

    let root = graph.add_node({
        let root_hits = Arc::clone(&root_hits);
        move || {
            root_hits.fetch_add(1, Ordering::SeqCst);
        }
    });
    let a = graph.add_node({
        let a_hits = Arc::clone(&a_hits);
        move || {
            a_hits.fetch_add(1, Ordering::SeqCst);
        }
    });
    let b = graph.add_node({
        let b_hits = Arc::clone(&b_hits);
        move || {
            b_hits.fetch_add(1, Ordering::SeqCst);
        }
    });
    let c = graph.add_node({
        let c_hits = Arc::clone(&c_hits);
        move || {
            c_hits.fetch_add(1, Ordering::SeqCst);
        }
    });

    graph.add_edge(root, a);
    graph.add_edge(root, b);
    graph.add_edge(a, c);
    graph.add_edge(b, c);

    graph.push_work(root);
    graph.wait();

    assert_eq!(root_hits.load(Ordering::SeqCst), 1);
    assert_eq!(a_hits.load(Ordering::SeqCst), 1);
    assert_eq!(b_hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        c_hits.load(Ordering::SeqCst),
        2,
        "edge-triggered: the child runs once per completed parent, not as an AND-join"
    );
}

#[test]
fn work_can_enter_at_any_node() {
    let graph = TaskGraph::new();
    let root_hits = counter();
    let mid_hits = counter();
    let tail_hits = counter();

    let root = graph.add_node({
        let root_hits = Arc::clone(&root_hits);
        move || {
            root_hits.fetch_add(1, Ordering::SeqCst);
        }
    });
    let mid = graph.add_node({
        let mid_hits = Arc::clone(&mid_hits);
        move || {
            mid_hits.fetch_add(1, Ordering::SeqCst);
        }
    });
    let tail = graph.add_node({
        let tail_hits = Arc::clone(&tail_hits);
        move || {
            tail_hits.fetch_add(1, Ordering::SeqCst);
        }
    });

    graph.add_edge(root, mid);
    graph.add_edge(mid, tail);

    // Start in the middle: the root must never run.
    graph.push_work(mid);
    graph.wait();

    assert_eq!(root_hits.load(Ordering::SeqCst), 0);
    assert_eq!(mid_hits.load(Ordering::SeqCst), 1);
    assert_eq!(tail_hits.load(Ordering::SeqCst), 1);
}

#[test]
fn graph_can_be_reused() {
    let graph = TaskGraph::new();
    let hits = counter();
    let node = graph.add_node({
        let hits = Arc::clone(&hits);
        move || {
            hits.fetch_add(1, Ordering::SeqCst);
        }
    });

    graph.push_work(node);
    graph.wait();
    graph.push_work(node);
    graph.wait();

    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[test]
fn payload_release_runs_on_drop() {
    let released = counter();
    {
        let graph = TaskGraph::new();
        let flag = Arc::clone(&released);
        let node = graph.add_node_with_free(
            || {},
            move || {
                flag.fetch_add(1, Ordering::SeqCst);
            },
        );
        graph.push_work(node);
        graph.wait();
        assert_eq!(
            released.load(Ordering::SeqCst),
            0,
            "released by drop, not by run"
        );
    }
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn pushing_an_unknown_node_is_rejected() {
    let graph = TaskGraph::new();
    let node = graph.add_node(|| {});
    assert!(graph.push_work(node));

    let other = TaskGraph::new();
    assert!(
        !other.push_work(node),
        "a NodeId from another graph must not resolve"
    );
    assert!(other.is_empty());
    graph.wait();
}
