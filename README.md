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
cargo test --release     # 21 behavioural tests + 1 doctest
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The suite asserts the invariants directly: no clone on the serial path, one
clone per chunk on the parallel path, `n-1` joins, grain flooring, the
`threads * 4` over-decomposition, run-to-run reproducibility of the ordered
fold, suspended pools staying idle until waited on, serial pools never
overlapping, nested pools not deadlocking, and a diamond graph triggering its
shared child exactly twice.

## Scope

Phases 1–2 are implemented: data-parallel ranges, `TaskPool` (all five
flavours) and `TaskGraph` with edge-triggered propagation.

Not yet modelled: the newer `blender::threading` layer — `TaskSizeHints`
(`Static`, `IndividualLookup` with greedy bucketing, `AccumulatedLookup` with
recursive bisection), `IndexMask::foreach_segment`, `memory_bandwidth_bound_task`
and `max_threads_task`. That is Phase 3.

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
