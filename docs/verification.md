# Verification against the shipped Blender binary

This crate models behaviour. It does not copy Blender code. To keep the model
honest, every layer is anchored twice:

1. **the public source** (`BLI_task_c.hh`, `intern/task_range.cc`,
   `intern/task_pool.cc`, `BLI_task.hh`, `BLI_task_size_hints.hh`,
   `BLI_execution_mode.hh`), read from the upstream repository;
2. **the shipped artifact** — `blender.exe` 5.3 beta, SHA-256
   `b21e673caf8651e3c8d3b5160de22f623a45fb873eb55ff67e43afebcac8b145` —
   through its `blender.pdb`, which carries full public symbols.

## How the symbol map was produced

The PDB is an MSF 7.0 container. A small parser reads the superblock, the stream
directory, the DBI stream and its `SymRecordStream`, keeps the `S_PUB32`
records, and maps `section:offset` to an RVA through the PE section table.
Result: **318 602 public symbols**, addresses below are virtual addresses with
image base `0x140000000` (confirmed independently by `dbghelp`).

Symbol counts by prefix: `BLI_task*` 26, `task_scheduler*` 12, `parallel_*`
2045, `chunk*` 58, `WM_jobs*` 23, `TaskGraph*` 8, `TaskSizeHints` 280,
`threading@blender` 118, and **`chunk_size` 0** — no symbol or string of that
name exists, which is why this project models the grain as computed rather than
passed in.

## Layer 1 — parallel ranges

| Symbol | Address | Modelled by |
|---|---|---|
| `BLI_task_scheduler_init(bool)` | `0x1403a7f50` | `runtime::init` |
| `BLI_task_scheduler_exit()` | `0x1403a8080` | process teardown |
| `BLI_task_scheduler_num_threads()` | `0x1403a80f0` | `runtime::worker_threads` |
| `BLI_task_parallel_range(int,int,void*,fn,settings)` | `0x140760a40` | `parallel_range` |
| `BLI_task_parallel_thread_id(const TaskParallelTLS*)` | `0x140760d40` | `current_thread_id` |
| `tbb::detail::d1::task_scheduler_observer` ctor | `0x1403a82a0` | `FlushDenormalsObserver` note |
| `observe(task_scheduler_observer&, bool)` | `0x14526ffb0` | same |

The mangling of `BLI_task_parallel_range` carries the full signature and shows
**no chunk-size parameter** — the grain is computed, `max(min_iter_per_thread, 1)`.
The `task_scheduler_observer` symbols match `FlushDenormalsObserver` in
`task_scheduler.cc`, whose only job is setting flush-to-zero per worker so
results are reproducible regardless of which thread runs a computation.

## Layer 2 — pools and graphs

| Symbol | Address | Modelled by |
|---|---|---|
| `BLI_task_pool_create` | `0x14074bc60` | `TaskPool::new(Immediate)` |
| `BLI_task_pool_create_background` | `0x14074bca0` | `PoolKind::Background` |
| `BLI_task_pool_create_suspended` | `0x14074bcf0` | `PoolKind::Suspended` |
| `BLI_task_pool_create_no_threads` | `0x14074bd40` | `PoolKind::NoThreads` |
| `BLI_task_pool_push` | `0x14074be00` | `TaskPool::push` |
| `BLI_task_pool_work_and_wait` | `0x14074bfc0` | `TaskPool::work_and_wait` |
| `BLI_task_graph_create` | `0x141f6fb70` | `TaskGraph::new` |
| `BLI_task_graph_work_and_wait` | `0x141f6fe00` | `TaskGraph::wait` |

`BLI_task_graph_*` is used by the draw manager in this same binary
(`DRW_mesh_batch_cache_create_requested(TaskGraph&, ...)`), which confirms the
graph is the dependency mechanism for real work, not a demo API.

## Layer 3 — the threading layer

The binary contains 2045 `parallel_*` symbols and 118 `threading@blender`
symbols, including `blender::threading::parallel_for<...>` instantiations whose
template arguments name `TaskSizeHints` and `exec_mode::ParallelGrainSize`. That
is direct evidence the hint machinery is compiled in, not dead code.

## What is *not* verified

- The source read is the `main` branch; the binary is 5.3 beta. Structure and
  symbols agree, line-level details need not.
- No decompilation of `blender.exe` was completed (a 121 MB import exceeded the
  analysis window twice), so behaviour is inferred from source plus symbols, not
  from recovered pseudocode.
- Timing claims in the README come from one machine and are labelled as such.
