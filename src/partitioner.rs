//! Range splitting strategies.

/// How `[start, stop)` is cut into work chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Partitioner {
    /// Geometric halving, the behaviour `tbb::auto_partitioner` exposes:
    /// split the largest range in half until every piece is at most
    /// `2 * grain`, bounded by `threads * 4` tasks.
    Auto,
    /// Equal-sized chunks, `tbb::static_partitioner`-like.
    Static,
}

/// Number of chunks we are willing to create per call.
///
/// TBB over-decomposes relative to the thread count so that work stealing can
/// balance uneven chunks; `4x` is a reasonable stand-in.
const OVERDECOMPOSITION: usize = 4;

/// Split `[start, stop)` into ordered, contiguous, non-empty ranges.
pub fn split(
    start: i32,
    stop: i32,
    grain: usize,
    threads: usize,
    partitioner: Partitioner,
) -> Vec<(i32, i32)> {
    let len = (stop - start).max(0) as usize;
    if len == 0 {
        return Vec::new();
    }
    let grain = grain.max(1);
    let max_tasks = threads.max(1).saturating_mul(OVERDECOMPOSITION).max(1);

    match partitioner {
        Partitioner::Static => {
            let wanted = len.div_ceil(grain).clamp(1, max_tasks);
            let size = len.div_ceil(wanted);
            let mut out = Vec::with_capacity(wanted);
            let mut at = start;
            while at < stop {
                let end = (at + size as i32).min(stop);
                out.push((at, end));
                at = end;
            }
            out
        }
        Partitioner::Auto => {
            let mut ranges = vec![(start, stop)];
            while ranges.len() < max_tasks {
                // Split the largest splittable range, halving it.
                let mut pick: Option<(usize, usize)> = None;
                for (index, &(begin, end)) in ranges.iter().enumerate() {
                    let width = (end - begin) as usize;
                    if width > grain.saturating_mul(2) {
                        let better = match pick {
                            None => true,
                            Some((_, best)) => width > best,
                        };
                        if better {
                            pick = Some((index, width));
                        }
                    }
                }
                let Some((index, width)) = pick else { break };
                let (begin, end) = ranges[index];
                let middle = begin + (width / 2) as i32;
                ranges[index] = (begin, middle);
                ranges.push((middle, end));
            }
            ranges.sort_unstable_by_key(|range| range.0);
            ranges
        }
    }
}
