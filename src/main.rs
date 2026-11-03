//! `cpx` — a demonstrator for the chunked-parallel engine.
//!
//! * `cpx` runs the range demonstrator: serial reference vs. chunked parallel.
//! * `cpx graph` runs the task-graph demonstrator and prints the live tree.

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chunked_parallel::{parallel_range, Chunk, MergeOrder, Partitioner, Settings, TaskGraph};

// ---------------------------------------------------------------------------
// Range demonstrator
// ---------------------------------------------------------------------------

/// Per-chunk accumulator: the "firstprivate" payload.
#[derive(Clone, Default)]
struct Accum {
    sum: f64,
    seen: u64,
    thread_ids: Vec<usize>,
}

impl Chunk for Accum {
    fn join(&mut self, other: &Self) {
        self.sum += other.sum;
        self.seen += other.seen;
        for id in &other.thread_ids {
            if !self.thread_ids.contains(id) {
                self.thread_ids.push(*id);
            }
        }
    }
}

fn contribute(index: i32, chunk: &mut Accum) {
    // Order-sensitive on purpose: 1/(i+1) accumulates rounding error, so the
    // merge order becomes observable in the low bits.
    chunk.sum += 1.0 / (index as f64 + 1.0);
    chunk.seen += 1;
    let id = chunked_parallel::current_thread_id();
    if !chunk.thread_ids.contains(&id) {
        chunk.thread_ids.push(id);
    }
}

fn serial_reference(items: i32) -> Accum {
    let mut acc = Accum::default();
    for i in 0..items {
        contribute(i, &mut acc);
    }
    acc
}

struct Options {
    items: i32,
    settings: Settings,
    repeats: usize,
}

fn range_usage() -> &'static str {
    "cpx — chunked parallel range demonstrator

USAGE:
    cpx [OPTIONS]
    cpx graph [OPTIONS]
    cpx bench [OPTIONS]
    cpx --help

RANGE OPTIONS:
    --items <N>     number of indices to process          [default: 1_000_000]
    --grain <N>     min_iter_per_thread (0 = default)     [default: 0]
    --threads <N>   worker threads (0 = auto)             [default: 0]
    --serial        disable threading (serial path)
    --static        static partitioner instead of auto
    --tree          tree-shaped merge (default)
    --det           deterministic (ordered) merge
    --repeats <N>   determinism check iterations          [default: 8]

GRAPH OPTIONS:
    --depth <N>     tree depth                            [default: 3]
    --fanout <N>    children per node                     [default: 2]
    --delay <MS>    simulated work per node               [default: 20]
    --diamond       build root -> a,b -> c (c has 2 parents)

BENCH OPTIONS:
    --items <N>     items in the synthetic workload       [default: 20_000]
    --grain <N>     grain passed to parallel_for          [default: 64]
    --heavy <PCT>   share of indices that are expensive   [default: 10]
    --repeats <N>   timed runs per strategy               [default: 3]

Graph mode prints a live start/finish trace and, when done, the tree with the
number of times each node actually ran. Ordering varies between runs: that is
real parallelism, not a bug. Bench mode compares the four splitting strategies
on a workload whose cost is clustered, then reports wall-clock timings."
}

fn parse_range(mut args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut options = Options {
        items: 1_000_000,
        settings: Settings::default(),
        repeats: 8,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--items" => {
                options.items = args
                    .next()
                    .ok_or("--items needs a value")?
                    .parse()
                    .map_err(|_| "--items expects an integer")?;
            }
            "--grain" => {
                let value: usize = args
                    .next()
                    .ok_or("--grain needs a value")?
                    .parse()
                    .map_err(|_| "--grain expects an integer")?;
                options.settings.min_iter_per_thread = value;
            }
            "--threads" => {
                options.settings.threads = args
                    .next()
                    .ok_or("--threads needs a value")?
                    .parse()
                    .map_err(|_| "--threads expects an integer")?;
            }
            "--repeats" => {
                options.repeats = args
                    .next()
                    .ok_or("--repeats needs a value")?
                    .parse()
                    .map_err(|_| "--repeats expects an integer")?;
            }
            "--serial" => options.settings.use_threading = false,
            "--static" => options.settings.partitioner = Partitioner::Static,
            "--tree" => options.settings.merge = MergeOrder::Tree,
            "--det" => options.settings.merge = MergeOrder::Deterministic,
            "-h" | "--help" => {
                println!("{}", range_usage());
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(options)
}

fn run_range(args: impl Iterator<Item = String>) -> Result<(), String> {
    let options = parse_range(args)?;
    let settings = &options.settings;

    println!("chunked-parallel {}", env!("CARGO_PKG_VERSION"));
    println!("  items            : {}", options.items);
    println!("  grain (min_iter) : {}", settings.grain());
    println!(
        "  threads          : {} (resolved)",
        settings.resolved_threads()
    );
    println!("  use_threading    : {}", settings.use_threading);
    println!("  partitioner      : {:?}", settings.partitioner);
    println!("  merge order      : {:?}", settings.merge);
    println!();

    let started = Instant::now();
    let reference = serial_reference(options.items);
    let serial_elapsed = started.elapsed();

    let started = Instant::now();
    let executed = parallel_range(0, options.items, Accum::default(), settings, contribute);
    let parallel_elapsed = started.elapsed();

    let stats = executed.stats;
    println!("serial reference");
    println!("  sum   : {:.17}", reference.sum);
    println!("  seen  : {}", reference.seen);
    println!("  time  : {:?}", serial_elapsed);
    println!();
    println!("parallel range");
    println!("  sum   : {:.17}", executed.chunk.sum);
    println!("  seen  : {}", executed.chunk.seen);
    println!("  time  : {:?}", parallel_elapsed);
    println!(
        "  stats : chunks={} clones={} joins={} workers={} serial_fallback={}",
        stats.chunks, stats.clones, stats.joins, stats.workers, stats.serial_fallback
    );
    println!();

    let bit_equal = executed.chunk.sum.to_bits() == reference.sum.to_bits();
    println!("bit-identical to serial : {bit_equal}");
    if !bit_equal {
        println!("  delta : {:.3e}", executed.chunk.sum - reference.sum);
        match settings.merge {
            MergeOrder::Tree => println!(
                "  (expected for MergeOrder::Tree: floating-point addition is not associative)"
            ),
            MergeOrder::Deterministic => println!(
                "  (ordered fold: reproducible run-to-run, but a different expression tree than the serial loop)"
            ),
        }
    }
    println!(
        "work accounted for      : {}",
        executed.chunk.seen == options.items as u64
    );
    println!();

    // Determinism check: repeat and compare bits.
    let mut stable = true;
    let first = executed.chunk.sum.to_bits();
    for _ in 1..options.repeats.max(1) {
        let again = parallel_range(0, options.items, Accum::default(), settings, contribute);
        if again.chunk.sum.to_bits() != first {
            stable = false;
            break;
        }
    }
    println!(
        "merge order reproducible : {stable} ({} runs)",
        options.repeats.max(1)
    );

    if executed.chunk.seen != options.items as u64 {
        return Err("work was lost or duplicated".to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Graph demonstrator
// ---------------------------------------------------------------------------

struct GraphOptions {
    depth: usize,
    fanout: usize,
    delay_ms: u64,
    diamond: bool,
}

struct NodeMeta {
    label: String,
    children: Vec<usize>,
    runs: Arc<AtomicUsize>,
}

fn parse_graph(mut args: impl Iterator<Item = String>) -> Result<GraphOptions, String> {
    let mut options = GraphOptions {
        depth: 3,
        fanout: 2,
        delay_ms: 20,
        diamond: false,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--depth" => {
                options.depth = args
                    .next()
                    .ok_or("--depth needs a value")?
                    .parse()
                    .map_err(|_| "--depth expects an integer")?
            }
            "--fanout" => {
                options.fanout = args
                    .next()
                    .ok_or("--fanout needs a value")?
                    .parse()
                    .map_err(|_| "--fanout expects an integer")?
            }
            "--delay" => {
                options.delay_ms = args
                    .next()
                    .ok_or("--delay needs a value")?
                    .parse()
                    .map_err(|_| "--delay expects an integer")?
            }
            "--diamond" => options.diamond = true,
            "-h" | "--help" => {
                println!("{}", range_usage());
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if options.fanout == 0 || options.depth == 0 {
        return Err("--depth and --fanout must be at least 1".to_string());
    }
    Ok(options)
}

fn emit(line_lock: &Mutex<()>, clock: Instant, level: usize, kind: &str, label: &str) {
    let _guard = line_lock
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let indent = "  ".repeat(level);
    let millis = clock.elapsed().as_secs_f64() * 1000.0;
    println!("{millis:>8.2}ms  {indent}{kind:<4} {label}");
}

fn build_subtree(
    graph: &TaskGraph,
    meta: &mut Vec<NodeMeta>,
    parent: Option<usize>,
    level: usize,
    options: &GraphOptions,
    clock: Instant,
    line_lock: &Arc<Mutex<()>>,
) -> usize {
    let runs = Arc::new(AtomicUsize::new(0));
    let label = if level == 0 {
        "root".to_string()
    } else {
        format!("node-{}", meta.len())
    };
    let node = graph.add_node({
        let runs = Arc::clone(&runs);
        let label = label.clone();
        let line_lock = Arc::clone(line_lock);
        let delay = options.delay_ms;
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            emit(&line_lock, clock, level, "run", &label);
            std::thread::sleep(std::time::Duration::from_millis(delay));
        }
    });
    let id = node.index();
    meta.push(NodeMeta {
        label,
        children: Vec::new(),
        runs,
    });
    if let Some(parent) = parent {
        graph.add_edge(chunked_parallel::NodeId::from_index(parent), node);
        meta[parent].children.push(id);
    }

    if level + 1 < options.depth {
        for _ in 0..options.fanout {
            build_subtree(graph, meta, Some(id), level + 1, options, clock, line_lock);
        }
    }
    id
}

fn build_diamond(
    graph: &TaskGraph,
    meta: &mut Vec<NodeMeta>,
    options: &GraphOptions,
    clock: Instant,
    line_lock: &Arc<Mutex<()>>,
) -> usize {
    let add = |graph: &TaskGraph, meta: &mut Vec<NodeMeta>, label: &str, level: usize| {
        let runs = Arc::new(AtomicUsize::new(0));
        let node = graph.add_node({
            let runs = Arc::clone(&runs);
            let label = label.to_string();
            let line_lock = Arc::clone(line_lock);
            let delay = options.delay_ms;
            move || {
                runs.fetch_add(1, Ordering::SeqCst);
                emit(&line_lock, clock, level, "run", &label);
                std::thread::sleep(std::time::Duration::from_millis(delay));
            }
        });
        let id = node.index();
        meta.push(NodeMeta {
            label: label.to_string(),
            children: Vec::new(),
            runs,
        });
        id
    };

    let root = add(graph, meta, "root", 0);
    let a = add(graph, meta, "a", 1);
    let b = add(graph, meta, "b", 1);
    let c = add(graph, meta, "c", 2);

    graph.add_edge(
        chunked_parallel::NodeId::from_index(root),
        chunked_parallel::NodeId::from_index(a),
    );
    graph.add_edge(
        chunked_parallel::NodeId::from_index(root),
        chunked_parallel::NodeId::from_index(b),
    );
    graph.add_edge(
        chunked_parallel::NodeId::from_index(a),
        chunked_parallel::NodeId::from_index(c),
    );
    graph.add_edge(
        chunked_parallel::NodeId::from_index(b),
        chunked_parallel::NodeId::from_index(c),
    );

    meta[root].children.push(a);
    meta[root].children.push(b);
    meta[a].children.push(c);
    meta[b].children.push(c);
    root
}

fn render(meta: &[NodeMeta], id: usize, prefix: &str, last: bool, out: &mut String) {
    let branch = if last { "└─ " } else { "├─ " };
    let runs = meta[id].runs.load(Ordering::SeqCst);
    out.push_str(&format!("{prefix}{branch}{}  ×{runs}\n", meta[id].label));

    let child_prefix = format!("{prefix}{}", if last { "   " } else { "│  " });
    let children = &meta[id].children;
    for (index, child) in children.iter().enumerate() {
        let child_last = index + 1 == children.len();
        render(meta, *child, &child_prefix, child_last, out);
    }
}

fn run_graph(args: impl Iterator<Item = String>) -> Result<(), String> {
    let options = parse_graph(args)?;
    let graph = TaskGraph::new();
    let mut meta: Vec<NodeMeta> = Vec::new();
    let clock = Instant::now();
    let line_lock = Arc::new(Mutex::new(()));

    let root = if options.diamond {
        build_diamond(&graph, &mut meta, &options, clock, &line_lock)
    } else {
        build_subtree(&graph, &mut meta, None, 0, &options, clock, &line_lock)
    };

    println!(
        "chunked-parallel {} — task graph",
        env!("CARGO_PKG_VERSION")
    );
    println!(
        "  shape   : {}",
        if options.diamond {
            "diamond (root -> a,b -> c)"
        } else {
            "tree"
        }
    );
    println!("  nodes   : {}", graph.len());
    println!("  workers : {}", chunked_parallel::worker_threads());
    println!("  delay   : {} ms per node", options.delay_ms);
    println!();
    println!("live trace (order varies: real parallelism)");

    graph.push_work(chunked_parallel::NodeId::from_index(root));
    graph.wait();

    let elapsed = clock.elapsed();
    println!();
    println!("tree with run counts");
    let mut text = String::new();
    let runs = meta[root].runs.load(Ordering::SeqCst);
    text.push_str(&format!("root  ×{runs}\n"));
    let children = meta[root].children.clone();
    for (index, child) in children.iter().enumerate() {
        render(&meta, *child, "", index + 1 == children.len(), &mut text);
    }
    print!("{text}");

    let total: usize = meta
        .iter()
        .map(|node| node.runs.load(Ordering::SeqCst))
        .sum();
    println!();
    println!("total node executions : {total}");
    println!("elapsed               : {:.2?}", elapsed);
    if options.diamond {
        let c = meta
            .iter()
            .find(|node| node.label == "c")
            .map(|node| node.runs.load(Ordering::SeqCst))
            .unwrap_or(0);
        println!(
            "edge-triggered check  : c ran {c} times (2 = once per parent edge, as Blender does)"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark demonstrator
// ---------------------------------------------------------------------------

/// Cost of one "heavy" item, in synthetic work units.
const HEAVY_COST: i64 = 20_000;
/// Cost of one ordinary item.
const LIGHT_COST: i64 = 2;

struct BenchOptions {
    items: i64,
    grain: i64,
    heavy_percent: i64,
    repeats: usize,
}

struct BenchRow {
    name: &'static str,
    chunks: usize,
    best: std::time::Duration,
    median: std::time::Duration,
}

fn parse_bench(mut args: impl Iterator<Item = String>) -> Result<BenchOptions, String> {
    let mut options = BenchOptions {
        items: 20_000,
        grain: 64,
        heavy_percent: 10,
        repeats: 3,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--items" => {
                options.items = args
                    .next()
                    .ok_or("--items needs a value")?
                    .parse()
                    .map_err(|_| "--items expects an integer")?
            }
            "--grain" => {
                options.grain = args
                    .next()
                    .ok_or("--grain needs a value")?
                    .parse()
                    .map_err(|_| "--grain expects an integer")?
            }
            "--heavy" => {
                options.heavy_percent = args
                    .next()
                    .ok_or("--heavy needs a value")?
                    .parse()
                    .map_err(|_| "--heavy expects an integer")?
            }
            "--repeats" => {
                options.repeats = args
                    .next()
                    .ok_or("--repeats needs a value")?
                    .parse()
                    .map_err(|_| "--repeats expects an integer")?
            }
            "-h" | "--help" => {
                println!("{}", range_usage());
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if options.items <= 0 || options.grain <= 0 || options.repeats == 0 {
        return Err("--items, --grain and --repeats must be positive".to_string());
    }
    options.heavy_percent = options.heavy_percent.clamp(0, 100);
    Ok(options)
}

/// Deterministic per-item cost: the first `heavy_end` indices are expensive,
/// the rest are cheap. Clustering the cost is what separates the strategies.
fn item_cost(index: i64, heavy_end: i64) -> i64 {
    if index < heavy_end {
        HEAVY_COST
    } else {
        LIGHT_COST
    }
}

/// Burn `units` of synthetic work and return a value to sum.
#[inline(never)]
fn burn(units: i64) -> u64 {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for step in 0..units as u64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(step);
    }
    std::hint::black_box(state)
}

fn measure(
    hints: &chunked_parallel::threading::TaskSizeHints,
    options: &BenchOptions,
    heavy_end: i64,
    expected_work: u64,
    grain: i64,
) -> Result<BenchRow, String> {
    use chunked_parallel::threading::{chunks_for, parallel_for, IndexRange};
    use std::sync::atomic::{AtomicU64, Ordering};

    let range = IndexRange::new(0, options.items);
    let chunks = chunks_for(range, grain, hints).len();
    let mut samples = Vec::with_capacity(options.repeats);

    for _ in 0..options.repeats {
        let work = AtomicU64::new(0);
        let started = Instant::now();
        parallel_for(range, grain, hints, |sub| {
            let mut local = 0u64;
            for index in sub.iter() {
                local = local.wrapping_add(burn(item_cost(index, heavy_end)));
            }
            work.fetch_add(local, Ordering::Relaxed);
        });
        samples.push(started.elapsed());

        if work.load(Ordering::Relaxed) != expected_work {
            return Err("a strategy computed a different amount of work".to_string());
        }
    }

    samples.sort_unstable();
    Ok(BenchRow {
        name: "",
        chunks,
        best: samples[0],
        median: samples[samples.len() / 2],
    })
}

struct Scenario {
    label: &'static str,
    grain: i64,
    serial: std::time::Duration,
    rows: Vec<BenchRow>,
}

fn run_scenario(
    label: &'static str,
    grain: i64,
    hints: &[(&'static str, &chunked_parallel::threading::TaskSizeHints)],
    options: &BenchOptions,
    heavy_end: i64,
) -> Result<Scenario, String> {
    let started = Instant::now();
    let mut serial_work = 0u64;
    for index in 0..options.items {
        serial_work = serial_work.wrapping_add(burn(item_cost(index, heavy_end)));
    }
    let serial = started.elapsed();

    let mut rows: Vec<BenchRow> = Vec::new();
    for (name, hint) in hints {
        let mut row = measure(hint, options, heavy_end, serial_work, grain)?;
        row.name = name;
        rows.push(row);
    }

    Ok(Scenario {
        label,
        grain,
        serial,
        rows,
    })
}

fn print_scenario(scenario: &Scenario) {
    println!("scenario: {} (grain = {})", scenario.label, scenario.grain);
    println!(
        "{:<18} {:>8} {:>12} {:>12} {:>10}",
        "strategy", "chunks", "best", "median", "speedup"
    );
    println!("{}", "-".repeat(64));
    println!(
        "{:<18} {:>8} {:>12} {:>12} {:>10}",
        "serial",
        1,
        format!("{:.2}ms", scenario.serial.as_secs_f64() * 1000.0),
        format!("{:.2}ms", scenario.serial.as_secs_f64() * 1000.0),
        "1.00x"
    );
    for row in &scenario.rows {
        let speedup = scenario.serial.as_secs_f64() / row.best.as_secs_f64().max(f64::MIN_POSITIVE);
        println!(
            "{:<18} {:>8} {:>12} {:>12} {:>9.2}x",
            row.name,
            row.chunks,
            format!("{:.2}ms", row.best.as_secs_f64() * 1000.0),
            format!("{:.2}ms", row.median.as_secs_f64() * 1000.0),
            speedup
        );
    }
    println!();
}

fn run_bench(args: impl Iterator<Item = String>) -> Result<(), String> {
    use chunked_parallel::threading::{IndexRange, TaskSizeHints};
    use std::sync::Arc;

    let options = parse_bench(args)?;
    let heavy_end = options.items * options.heavy_percent / 100;

    // Prefix sums over the costs: an O(1) accumulated lookup, which is the
    // point of the Accumulated hint.
    let mut prefix: Vec<i64> = Vec::with_capacity(options.items as usize + 1);
    prefix.push(0);
    for index in 0..options.items {
        let last = *prefix.last().unwrap_or(&0);
        prefix.push(last + item_cost(index, heavy_end));
    }
    let total_cost = *prefix.last().unwrap_or(&0);
    let prefix = Arc::new(prefix);

    let static_unit = TaskSizeHints::static_size(1);
    let static_heavy = TaskSizeHints::static_size(HEAVY_COST);
    let individual = TaskSizeHints::individual(
        move |range: IndexRange, sizes: &mut [i64]| {
            for (offset, slot) in sizes.iter_mut().enumerate() {
                *slot = item_cost(range.first() + offset as i64, heavy_end);
            }
        },
        Some(total_cost),
    );
    let accumulated = TaskSizeHints::accumulated({
        let prefix = Arc::clone(&prefix);
        move |range: IndexRange| {
            let begin = range.first().clamp(0, prefix.len() as i64 - 1) as usize;
            let end = range.one_after_last().clamp(0, prefix.len() as i64 - 1) as usize;
            prefix[end] - prefix[begin]
        }
    });

    let hints = [
        ("static(size=1)", &static_unit),
        ("static(size=20k)", &static_heavy),
        ("individual", &individual),
        ("accumulated", &accumulated),
    ];

    println!(
        "chunked-parallel {} — strategy benchmark",
        env!("CARGO_PKG_VERSION")
    );
    println!("  items        : {}", options.items);
    println!(
        "  cost shape   : first {}% heavy ({HEAVY_COST} units), rest light ({LIGHT_COST} units)",
        options.heavy_percent
    );
    println!("  total work   : {total_cost} units");
    println!("  workers      : {}", chunked_parallel::worker_threads());
    println!("  repeats      : {}", options.repeats);
    println!();

    let fine = options.grain;
    let coarse = (options.items / 10).max(1);
    let scenarios = [
        run_scenario("fine grain, many chunks", fine, &hints, &options, heavy_end)?,
        run_scenario(
            "coarse grain, one chunk can swallow the hot region",
            coarse,
            &hints,
            &options,
            heavy_end,
        )?,
    ];

    for scenario in &scenarios {
        print_scenario(scenario);
    }

    println!("reading the numbers (what this run shows, not a guarantee):");
    println!("  * with many chunks, pulling work dynamically already rebalances a");
    println!("    clustered cost, so the cost-aware hints mostly add scheduling");
    println!("    overhead and can be slightly slower;");
    println!("  * with a coarse partition, one chunk can absorb the whole hot region");
    println!("    and stop scaling entirely — there the cost-aware hints are the");
    println!("    difference between no speedup and a large one.");
    println!("timings are wall-clock on this machine: noisy, and meant for relative comparison");
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("graph") => run_graph(args.into_iter().skip(1)),
        Some("bench") => run_bench(args.into_iter().skip(1)),
        _ => run_range(args.into_iter()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}\n\n{}", range_usage());
            ExitCode::FAILURE
        }
    }
}
