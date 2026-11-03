//! `cpx` — a small demonstrator for the chunked-parallel engine.
//!
//! It runs the same workload twice (serial reference vs. chunked parallel) and
//! prints the observable data-flow counters, so the difference between the
//! two execution paths is visible rather than assumed.

use std::env;
use std::process::ExitCode;
use std::time::Instant;

use chunked_parallel::{parallel_range, Chunk, MergeOrder, Partitioner, Settings};

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

fn usage() -> &'static str {
    "cpx — chunked parallel range demonstrator

USAGE:
    cpx [OPTIONS]

OPTIONS:
    --items <N>     number of indices to process          [default: 1_000_000]
    --grain <N>     min_iter_per_thread (0 = default)     [default: 0]
    --threads <N>   worker threads (0 = auto)             [default: 0]
    --serial        disable threading (serial path)
    --static        static partitioner instead of auto
    --tree          tree-shaped merge (default)
    --det           deterministic (ordered) merge
    --repeats <N>   determinism check iterations          [default: 8]
    -h, --help      print this help"
}

fn parse() -> Result<Options, String> {
    let mut options = Options {
        items: 1_000_000,
        settings: Settings::default(),
        repeats: 8,
    };
    let mut args = env::args().skip(1);
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
                println!("{}", usage());
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(options)
}

fn main() -> ExitCode {
    let options = match parse() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("error: {message}\n\n{}", usage());
            return ExitCode::FAILURE;
        }
    };

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
        eprintln!("error: work was lost or duplicated");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
