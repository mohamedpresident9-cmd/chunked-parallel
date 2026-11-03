//! `TaskGraph`: dependency-ordered work with **edge-triggered** propagation.
//!
//! Mirrors `BLI_task_graph_*` from `BLI_task_c.hh`.
//!
//! The contract that surprises people, quoted from the header:
//!
//! > When a child node has multiple parents the child node will be triggered
//! > once for each parent.
//!
//! That is *not* an AND-join: a node with two incoming edges runs twice, once
//! per completed parent. A conventional DAG executor would run it once after
//! both parents finish. [`TaskGraph`] follows Blender.
//!
//! Cycles are unsupported, exactly as in Blender: execution follows edges
//! forward, so a cycle would recurse forever.

use std::sync::{Arc, Mutex};

use crate::runtime::{self, Scope};

/// Handle to a node owned by a [`TaskGraph`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(usize);

impl NodeId {
    /// Raw index, useful for logging.
    pub fn index(self) -> usize {
        self.0
    }
}

struct Node {
    run: Arc<dyn Fn() + Send + Sync>,
    children: Vec<usize>,
    on_free: Option<Box<dyn FnOnce() + Send>>,
}

struct Inner {
    nodes: Mutex<Vec<Node>>,
    scope: Arc<Scope>,
}

/// A forest of directed trees scheduled on the shared worker pool.
#[must_use]
pub struct TaskGraph {
    inner: Arc<Inner>,
}

impl Default for TaskGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskGraph {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                nodes: Mutex::new(Vec::new()),
                scope: Scope::new(),
            }),
        }
    }

    /// Create a node. `run` may be invoked more than once (see module docs).
    pub fn add_node<F>(&self, run: F) -> NodeId
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.add_node_with_free(run, || {})
    }

    /// Create a node whose payload is released when the graph is dropped.
    ///
    /// Blender passes `free_func` alongside `task_data`; here the closure
    /// itself is the owner, so `on_free` stands in for the explicit release
    /// callback when the payload needs deterministic teardown.
    pub fn add_node_with_free<F, G>(&self, run: F, on_free: G) -> NodeId
    where
        F: Fn() + Send + Sync + 'static,
        G: FnOnce() + Send + 'static,
    {
        let mut nodes = self
            .inner
            .nodes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let id = nodes.len();
        nodes.push(Node {
            run: Arc::new(run),
            children: Vec::new(),
            on_free: Some(Box::new(on_free)),
        });
        NodeId(id)
    }

    /// Add a directed edge `from -> to`.
    ///
    /// # Panics
    /// If either node does not belong to this graph.
    pub fn add_edge(&self, from: NodeId, to: NodeId) {
        let mut nodes = self
            .inner
            .nodes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert!(from.0 < nodes.len(), "edge from unknown node");
        assert!(to.0 < nodes.len(), "edge to unknown node");
        nodes[from.0].children.push(to.0);
    }

    /// Trigger a chain of work starting at `node`.
    ///
    /// Blender returns `bool` here; it is `false` for an unknown node.
    pub fn push_work(&self, node: NodeId) -> bool {
        let exists = {
            let nodes = self
                .inner
                .nodes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            node.0 < nodes.len()
        };
        if !exists {
            return false;
        }
        let inner = Arc::clone(&self.inner);
        runtime::submit_to(Arc::clone(&inner.scope), move || execute(inner, node.0));
        true
    }

    /// Wait until every triggered node and its descendants have finished.
    pub fn wait(&self) {
        self.inner.scope.wait();
    }

    /// Number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.inner
            .nodes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }

    /// Whether the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for TaskGraph {
    fn drop(&mut self) {
        // Blender frees node payloads in BLI_task_graph_free.
        let mut nodes = self
            .inner
            .nodes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for node in nodes.iter_mut() {
            if let Some(on_free) = node.on_free.take() {
                on_free();
            }
        }
    }
}

fn execute(inner: Arc<Inner>, id: usize) {
    let (run, children) = {
        let nodes = inner
            .nodes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match nodes.get(id) {
            Some(node) => (Arc::clone(&node.run), node.children.clone()),
            None => return,
        }
    };

    run();

    for child in children {
        let inner = Arc::clone(&inner);
        let scope = Arc::clone(&inner.scope);
        // One submission per edge: the child runs once per parent.
        runtime::submit_to(scope, move || execute(inner, child));
    }
}
