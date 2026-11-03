# chunked-parallel

A dependency-free Rust model of Blender's task scheduling: data-parallel ranges
with `firstprivate` chunk copies, plus explicit task pools and dependency graphs.

It exists to answer one question precisely: *what exactly happens to the data
between "split the range" and "one merged result"*, and what stays invariant
when the scheduler changes its mind about chunk boundaries.

This is **not** a Blender plugin and it does not link against Blender. It is a
faithful executable specification of a ~40-line kernel, written so the rules can
be unit-tested instead of assumed.

## What is modelled

| Blender (`blenlib`) | Here |
|---|---|
| `settings->min_iter_per_thread` | `Settings::min_iter_per_thread` |
| `grainsize = max(min_iter_per_thread, 1)` | `Settings::grain()` |
| `tbb::blocked_range<int>` + partitioner | `Partitioner::{Auto, Static}` |
| `RangeTask::init_chunk` (`memcpy` per split) | `Chunk::clone` |
| `func(userdata, i, &tls)` | `func(i, &mut chunk)` |
| `RangeTask::join` → `func_reduce` | `Chunk::join` |
| `func_free` | `Drop` |
| `BLI_task_parallel_thread_id` | `current_thread_id()` |
| `parallel_reduce` / `parallel_deterministic_reduce` | `MergeOrder::{Tree, Deterministic}` |

## The two rules people get wrong

**1. The serial path does not copy.** With `use_threading = false`, or one
worker thread, the caller's chunk is mutated in place and returned. There is no
clone, one `Drop`, and `Stats::serial_fallback` is `true`. Blender behaves the
same way, and code that assumes a fresh chunk will break there first.

**2. The merge order is observable.** `MergeOrder::Tree` is the fast pairwise
reduction and it is *not* bit-reproducible for floats. `MergeOrder::Deterministic`
folds chunks in index order and *is* reproducible run-to-run — but it still
evaluates `(a+b)+(c+d)` rather than the serial `((a+b)+c)+d`, so it does not have
to match the serial loop bit for bit.

Observed on this machine (200 000 items, grain 64, 4 threads):

```
serial   : 12.78329081042982018
parallel : 12.78329081042960524   delta -2.149e-13   (MergeOrder::Tree)
```

That delta is not a bug; it is the reason `parallel_deterministic_reduce` exists.

## Layer 2 — tasks and dependency graphs

`TaskPool` mirrors the five `BLI_task_pool_create*` flavours, resolved against
the worker count the way Blender resolves them:

| `PoolKind` | Behaviour |
|---|---|
| `Immediate` | starts on push; runs inline when there is a single worker |
| `Background` | never blocks the caller; **promoted to the shared pool when threads > 1** |
| `BackgroundSerial` | one task at a time, always off the caller's thread |
| `Suspended` | runs nothing until `work_and_wait()` |
| `NoThreads` | runs inline on the caller; for debugging |

One process-wide queue serves every pool — Blender's "single queue [that] holds
the task from all pools" — and `work_and_wait()` **drains the queue while it
waits**, the same reason `tbb::task_group::wait()` "can actually do work". That
is what stops a pool nested inside a task from deadlocking when every worker is
already busy.

`TaskGraph` is a forest of directed trees with **edge-triggered** propagation:

```rust
use chunked_parallel::TaskGraph;

let graph = TaskGraph::new();
let root = graph.add_node(|| println!("root"));
let a = graph.add_node(|| println!("a"));
let b = graph.add_node(|| println!("b"));
let c = graph.add_node(|| println!("c"));

graph.add_edge(root, a);
graph.add_edge(root, b);
graph.add_edge(a, c);
graph.add_edge(b, c); // two parents

graph.push_work(root);
graph.wait();
// c ran TWICE: once per completed parent edge, not once as an AND-join.
```

Priority is accepted and recorded but not acted upon: Blender drops it on
TBB 2021+ (`TBB_INTERFACE_VERSION_MAJOR >= 12`), which Blender 5.x ships.

## Layer 3 — the modern `blender::threading` layer

`threading::parallel_for(range, grain, &hints, f)` takes a `TaskSizeHints`, and
the hint type selects the splitting strategy — this is the "multi-part" logic in
its current form:

| Hint | Strategy (from `parallel_for_impl`) |
|---|---|
| `Static { size }` | `final_grain = grain / size`, uniform blocks |
| `Individual { .. }` | outer blocks of `min(grain, 512)`, per-task sizes, **greedy bucketing** until a bucket reaches the grain |
| `Accumulated { .. }` | **recursive bisection** until the accumulated size fits the grain |

`use_single_thread` short-circuits small work back to the caller, because
"invoking tbb for small workloads has a large overhead". Also modelled:
`parallel_for_aligned` / `align_sub_range`, `parallel_reduce`,
`parallel_deterministic_reduce`, `parallel_invoke2`, `max_threads_task`,
`memory_bandwidth_bound_task` (8 MiB cache threshold, 8-thread bandwidth cap) and
`ExecutionMode` (`Serial`, `Parallel`, `ParallelGrainSize`).

`IndexMask` is a projection: ranges in, chunked segments out, with
`foreach_segment_parallel` feeding segment sizes through an accumulated hint —
precisely the case that hint exists for.

## Install

### Prebuilt binaries (recommended)

Grab the latest release from
[github.com/mohamedpresident9-cmd/chunked-parallel/releases](https://github.com/mohamedpresident9-cmd/chunked-parallel/releases).

| Platform | Asset |
|---|---|
| Windows 10/11 x64 | `cpx-x86_64-pc-windows-msvc.zip` |
| Linux x86_64 (Arch, Manjaro, EndeavourOS, CachyOS, Bazzite, …) | `cpx-x86_64-unknown-linux-musl.tar.gz` |
| Linux aarch64 | `cpx-aarch64-unknown-linux-musl.tar.gz` |

The Linux builds are **statically linked against musl**: one file, no glibc
version dependency, no runtime libraries. That is what makes the same binary
work on Arch rolling-release and on Bazzite's immutable Fedora image alike.

### One-liner (Linux)

```sh
curl -fsSL https://raw.githubusercontent.com/mohamedpresident9-cmd/chunked-parallel/main/scripts/install.sh | sh
```

It installs to `/usr/local/bin` when writable, otherwise `~/.local/bin`.

### From source

Any platform with a Rust toolchain (1.73+):

```sh
cargo install --path .
# or
cargo build --release   # target/release/cpx
```

## Platform notes

**Windows** — `cargo install --path .` or unzip the asset; `cpx.exe` is
self-contained (MSVC runtime is linked statically by default for this crate).

**Arch Linux and derivatives** (Manjaro, EndeavourOS, CachyOS, Garuda, …):

```sh
sudo pacman -S --needed rust        # or: rustup
git clone https://github.com/mohamedpresident9-cmd/chunked-parallel
cd chunked-parallel && cargo build --release
sudo install -m755 target/release/cpx /usr/local/bin/cpx
```

**Bazzite** (immutable, Fedora atomic — `rpm-ostree` layers are discouraged):

```sh
# 1. easiest: the static musl release asset
curl -fsSL https://raw.githubusercontent.com/mohamedpresident9-cmd/chunked-parallel/main/scripts/install.sh | sh

# 2. or build inside a toolbox/distrobox container
toolbox create rust && toolbox run -c rust cargo build --release
```

Because the release binary is musl-static, it runs unmodified on the immutable
root filesystem — no layering, no `rpm-ostree`, no Flatpak packaging needed.

## Usage

```
cpx [OPTIONS]

  --items <N>     number of indices to process          [default: 1000000]
  --grain <N>     min_iter_per_thread (0 = default)     [default: 0]
  --threads <N>   worker threads (0 = auto)             [default: 0]
  --serial        disable threading (serial path)
  --static        static partitioner instead of auto
  --tree          tree-shaped merge (default)
  --det           deterministic (ordered) merge
  --repeats <N>   determinism check iterations          [default: 8]

GRAPH OPTIONS (cpx graph):
  --depth <N>     tree depth                            [default: 3]
  --fanout <N>    children per node                     [default: 2]
  --delay <MS>    simulated work per node               [default: 20]
  --diamond       build root -> a,b -> c (c has 2 parents)

BENCH OPTIONS (cpx bench):
  --items <N>     items in the synthetic workload       [default: 20000]
  --grain <N>     grain for the fine-grain scenario     [default: 64]
  --heavy <PCT>   share of indices that are expensive   [default: 10]
  --repeats <N>   timed runs per strategy               [default: 3]
```

Example:

```
$ cpx --items 200000 --grain 64 --threads 4
chunked-parallel 0.1.0
  items            : 200000
  grain (min_iter) : 64
  threads          : 4 (resolved)
  ...
  stats : chunks=16 clones=16 joins=15 workers=4 serial_fallback=false
bit-identical to serial : false
```

`chunks == clones` (one copy per chunk) and `joins == chunks - 1` (a tree of
`n-1` reductions) hold by construction; the tests assert both.

Graph mode prints a live trace and then the tree with real run counts:

```
$ cpx graph --diamond --delay 5
    0.41ms  run  root
    5.52ms    run  a
    5.55ms    run  b
   10.63ms      run  c
   10.65ms      run  c

root  ×1
├─ a  ×1
│  └─ c  ×2
└─ b  ×1
   └─ c  ×2
```

`c` appears under both parents and ran twice: edge-triggered propagation, made
visible instead of argued about.

## What the benchmark shows

`cpx bench` runs the same clustered workload twice — once with a fine grain, once
with a grain so coarse that ten chunks cover the whole range — and verifies that
every strategy computes an identical amount of work.

Observed on the development machine (20 000 items, 12 workers):

| scenario | strategy | chunks | best | speedup |
|---|---|---|---|---|
| fine (grain 64) | `static(size=1)` | 313 | 0.79 ms | **6.00x** |
| fine | `accumulated` | 2923 | 0.98 ms | 4.82x |
| coarse (grain 2000) | `static(size=1)` | 10 | 4.89 ms | **1.02x** |
| coarse | `accumulated` | 2033 | 0.78 ms | **6.38x** |
| both | `static(size=20000)` | 20000 | ~2.3 ms | ~2x |

Two lessons, visible in the numbers rather than asserted in prose:

1. **With many chunks, dynamic work-stealing already rebalances a clustered
   cost.** The cost-aware hints then mostly pay scheduling overhead, and can be
   slightly slower than plain uniform chunks.
2. **With a coarse partition one chunk can absorb the entire hot region** and
   parallelism collapses to the serial time (1.02x). That is exactly the case the
   cost-aware hints exist for.

The `static(size=20000)` row is the cautionary one: claiming a per-task size that
does not match reality explodes the chunk count to one item per chunk and caps
the speedup near 2x whatever the grain is.

## The sweep, and what it corrected

`cpx sweep` repeats the measurement over a grid of grain values and heavy shares,
holding total work constant so the rows stay comparable, and reports which
strategy wins each cell.

With a 100x cost concentration (a heavy item costs 100 light ones):

| heavy \ grain | 8 | 16 | 32 | 64 | 128 | 256 | 512 | 1024 | 2048 |
|---|---|---|---|---|---|---|---|---|---|
| 0%  | S | S | S | S | S | S | S | S | S |
| 10% | S | S | S | S | S | S | S | I | A |
| 25% | S | S | S | S | S | S | S | S | I |
| 50% | S | S | S | S | S | S | S | S | S |

`S = static(size=1)`, `I = individual`, `A = accumulated`.

The single-scenario benchmark above suggests the cost-aware hints win by a wide
margin. **The sweep narrows that claim considerably.** With total work held
constant:

- uniform chunks win almost everywhere, including every fine grain;
- the crossover happens only at the coarsest grains (1024–2048), where one chunk
  would otherwise hold most of the work;
- a 50% heavy share never crosses over, because ten coarse chunks still spread
  across twelve workers;
- `--concentration` controls how early the crossover appears (`15` pushes it to
  2048, a larger value pulls it earlier).

That is the useful result: the cost-aware hints are **insurance against a bad
partition**, not a general speedup. Reach for them when the grain is coarse and
the cost is lopsided — not by default.

## Library use

```rust
use chunked_parallel::{parallel_range, Chunk, MergeOrder, Settings};

#[derive(Clone, Default)]
struct Sum(f64);

impl Chunk for Sum {
    fn join(&mut self, other: &Self) { self.0 += other.0; }
}

let settings = Settings::default().with_grain(64).with_merge(MergeOrder::Deterministic);
let done = parallel_range(0, 1000, Sum::default(), &settings, |i, c: &mut Sum| {
    c.0 += 1.0 / (i as f64 + 1.0);
});
assert_eq!(done.stats.clones, done.stats.chunks);
```

## Verify

```sh
cargo test --release     # 34 behavioural tests + 1 doctest
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The suite asserts the invariants directly: no clone on the serial path, one
clone per chunk on the parallel path, `n-1` joins, grain flooring, the
`threads * 4` over-decomposition, run-to-run reproducibility of the ordered
fold, suspended pools staying idle until waited on, serial pools never
overlapping, nested pools not deadlocking, a diamond graph triggering its shared
child exactly twice, greedy bucketing and recursive bisection producing the
expected chunk boundaries, aligned sub-ranges snapping to their alignment, and
`max_threads_task` actually capping concurrency.

## Scope

Phases 1–3 are implemented:

1. data-parallel ranges with `firstprivate` chunks and reduction merge;
2. `TaskPool` (all five flavours) and `TaskGraph` with edge-triggered propagation;
3. the `blender::threading` layer — `TaskSizeHints`, aligned ranges, reduce
   variants, `max_threads_task`, `memory_bandwidth_bound_task`, `ExecutionMode`
   and an `IndexMask` projection.

Deliberately not modelled, and documented where it matters:

- `isolate_task`: this executor joins scoped threads by blocking and never
  drains another scope's queue, so the nested-threading hazard isolation
  defends against cannot arise.
- `parallel_invoke` is variadic in C++; here it is `parallel_invoke2`.
- The full `IndexMask` (expression trees, virtual arrays) is out of scope; only
  range segments and their parallel iteration are projected.

## License

MIT. Blender itself is GPL-2.0-or-later; no Blender code is copied here — the
algorithms are re-expressed from the documented semantics of the public headers
and `intern/task_range.cc`.

---

## بالعربية — البدء السريع

هذا المشروع يحاكي دلالات `BLI_task_parallel_range` في Blender: حجم القطعة
(`grain`)، نسخ الحالة لكل قطعة (`firstprivate`)، ثم دمج النتائج.

- **القاعدتان الأساسيتان:** المسار التسلسلي **لا ينسخ** الحالة ويُرجعها كما هي؛
  وترتيب الدمج **مرئي** في النتائج العشرية.
- **التثبيت السريع على لينكس** (Arch وتوزيعاتها وBazzite) عبر سكربت `install.sh`
  أو الملف التنفيذي الثابت `musl` من صفحة الإصدارات.
- **ويندوز:** فك ضغط `cpx-x86_64-pc-windows-msvc.zip` أو `cargo install --path .`.

اختبار سريع:

```sh
cpx --items 200000 --grain 64 --threads 4      # دمج شجري (غير حتمي بتًا-ببت)
cpx --items 200000 --grain 64 --threads 4 --det # دمج مرتّب (حتمي عبر التشغيلات)
```
