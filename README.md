# chunked-parallel

A dependency-free Rust model of Blender's `BLI_task_parallel_range` **data-flow
semantics**: grain size, `firstprivate` chunk copies, and the reduction merge.

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
cargo test --release     # 8 behavioural tests + 1 doctest
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The suite asserts the invariants directly: no clone on the serial path, one
clone per chunk on the parallel path, `n-1` joins, grain flooring, the
`threads * 4` over-decomposition, and run-to-run reproducibility of the ordered
fold.

## Scope

Phase 1 of a larger study of Blender's scheduler. Not yet modelled:
`TaskPool` (immediate/background/suspended pools), `TaskGraph` (edge-triggered
DAG propagation), and the newer `blender::threading` layer with
`TaskSizeHints`. Those are Phase 2.

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
