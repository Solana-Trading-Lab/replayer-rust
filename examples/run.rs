//! Minimal CLI driver for the replayer.
//!
//! Usage:
//!   cargo run --example run -- \
//!     --start 2026-04-18T00 --end 2026-04-18T05 \
//!     --window 3 --dex pump --out ./tapes_cache
//!
//! Writes one JSON file per anchor hour into `--out` and prints a short summary.
//! `RUST_LOG=info` (or `debug`) enables progress logging.

use std::fs;
use std::path::PathBuf;

use pump_replayer::{Dex, DexFilter, Hour, HourRange, ReplayConfig, Replayer, Result};

fn parse_hour(s: &str) -> Hour {
    // Accepts "YYYY-MM-DDTHH" (UTC).
    let date_part = &s[..10];
    let hour_part = &s[11..];
    let y: i32 = date_part[0..4].parse().expect("year");
    let mo: u32 = date_part[5..7].parse().expect("month");
    let d: u32 = date_part[8..10].parse().expect("day");
    let h: u32 = hour_part.parse().expect("hour");
    Hour::from_ymdh(y, mo, d, h).expect("valid hour")
}

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    let start = parse_hour(&arg(&args, "--start").expect("--start YYYY-MM-DDTHH required"));
    let end = parse_hour(&arg(&args, "--end").expect("--end YYYY-MM-DDTHH required"));
    let window: u32 = arg(&args, "--window")
        .as_deref()
        .unwrap_or("3")
        .parse()
        .expect("window");
    let dex = arg(&args, "--dex")
        .as_deref()
        .map(|d| DexFilter::only(Dex::parse(d).expect("unknown dex")))
        .unwrap_or_else(DexFilter::all);
    let out = PathBuf::from(arg(&args, "--out").unwrap_or_else(|| "./tapes_out".into()));
    fs::create_dir_all(&out).expect("create out dir");

    let work_dir = out.join("_zst_cache");
    let cfg = ReplayConfig::new(HourRange::new(start, end), window, dex, work_dir);

    let mut replayer = Replayer::new(cfg)?;

    // Show the plan (and any clamping warnings) before doing the heavy work.
    let plan = replayer.plan()?;
    for w in &plan.warnings {
        eprintln!("WARNING: {w}");
    }
    eprintln!(
        "serviceable anchors: {} ({:?} .. {:?})",
        plan.effective_anchors.len(),
        plan.effective_anchors.first(),
        plan.effective_anchors.last()
    );

    let report = replayer.run(|step| {
        let file = out.join(format!("{}.json", step.anchor_hour.cache_file_name().replace(".jsonl.zst", "")));
        let json = serde_json::to_vec_pretty(step).expect("serialize step");
        fs::write(&file, json)?;
        println!("{} -> {} tapes", step.anchor_hour, step.tapes.len());
        Ok(())
    })?;

    println!(
        "done: {} steps, {} tapes total",
        report.steps_emitted, report.tapes_total
    );
    Ok(())
}
