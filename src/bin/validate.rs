//! Checks a recorded SBE file: every message decodes, replayed updates
//! reproduce the snapshots, and nothing is structurally wrong.
//!
//! ```text
//! validate <file.sbe>
//! ```
//!
//! Exits 0 if the file is consistent, 1 if not, 2 on read errors.

use std::time::Instant;

use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use engineering_challenge::validate::{Validator, describe, file_size_problem};

fn main() {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: validate <file.sbe>");
        std::process::exit(2);
    };
    match run(&path) {
        Ok(consistent) => std::process::exit(if consistent { 0 } else { 1 }),
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(2);
        }
    }
}

fn run(path: &str) -> anyhow::Result<bool> {
    let started = Instant::now();
    let size_problem = file_size_problem(std::fs::metadata(path)?.len());
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    // Read chunks on a second thread while this one decodes.
    let prefetch = reader.create_prefetch_task()?;
    let prefetch = std::thread::spawn(prefetch);
    let mut v = Validator::new();
    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => v.snapshot(&SnapshotDomain::try_from_decoder(d)?),
            AnyMessage::PriceLevelUpdate(d) => v.update(&PriceLevelUpdateDomain::try_from_decoder(d)?),
            _ => anyhow::bail!("unknown message type in file"),
        }
    }
    reader.signal_stop_prefetch();
    let _ = prefetch.join();
    let r = v.finish();

    println!("file:                      {path}");
    println!("read in:                   {:.1?}", started.elapsed());
    println!("symbols:                   {}", r.symbols);
    println!("snapshots:                 {}", r.snapshots);
    println!("updates:                   {}", r.updates);
    println!(
        "snapshots compared:        {} (matched {}, mismatched {}, levels {})",
        r.compared, r.matched, r.mismatched, r.mismatched_levels
    );
    println!("updates without baseline:  {}", r.updates_without_baseline);
    println!("invalid values:            {}", r.invalid_values);
    println!("unsorted snapshots:        {}", r.unsorted_snapshots);
    println!("receive time reversals:    {}", r.time_reversals);
    for (symbol, m) in &r.first_mismatches {
        println!("  mismatch: {}", describe(symbol, m));
    }
    if let Some(problem) = &size_problem {
        println!("file size problem:         {problem}");
    }
    let consistent = r.file_is_consistent() && size_problem.is_none();
    println!("result:                    {}", if consistent { "CONSISTENT" } else { "INCONSISTENT" });
    if r.mismatched > 0 {
        println!(
            "note: mismatched snapshots should equal the collector's mismatched_books + \
             gap_mismatched_books (summed over connections). Any extra means the file \
             lost or reordered messages; a symbol removed and later re-added can also \
             add one, since the collector starts it fresh."
        );
    }
    Ok(consistent)
}
