//! Per-chunk state ("firstprivate" data).
//!
//! Blender's `TaskParallelSettings` splits the lifecycle of this data into four
//! function pointers:
//!
//! | Blender              | Rust equivalent                    |
//! |----------------------|------------------------------------|
//! | `func_init`          | `Chunk::init` (or `Clone` source)  |
//! | copy at every split  | `Clone::clone`                     |
//! | `func_reduce`        | [`Chunk::join`]                    |
//! | `func_free`          | `Drop::drop`                       |
//!
//! The initial value built by the caller plays the role of
//! `settings.userdata_chunk`: it is the chunk that is copied into every worker
//! and the one that receives the final reduction result.

/// Per-chunk mutable state, copied once per work chunk.
///
/// `Send + Sync` is required because the initial chunk is shared by reference
/// while workers clone it. This replaces Blender's manual invariant that
/// workers must only write to disjoint index ranges.
pub trait Chunk: Clone + Send + Sync {
    /// Merge `other` into `self`.
    ///
    /// Must behave like `func_reduce`: no side effects, and associative enough
    /// that a tree-shaped merge produces a meaningful result. Commutativity is
    /// *not* required, but if the merge is order-sensitive you should select
    /// [`MergeOrder::Deterministic`](crate::MergeOrder::Deterministic).
    fn join(&mut self, other: &Self);
}
