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

Graph mode prints a live start/finish trace and, when done, the tree with the
number of times each node actually ran. Ordering varies between runs: that is
real parallelism, not a bug."
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

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("graph") => run_graph(args.into_iter().skip(1)),
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
