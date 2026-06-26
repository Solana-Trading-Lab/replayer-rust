//! # pump-replayer
//!
//! A small, embeddable Rust module that turns [PumpApi historical replay
//! data](https://pumpapi.io/llms-full.txt) into per-token **trade tapes** for a
//! chosen DEX, period and tape-window — without ever keeping more than a sliding
//! window of hours on disk.
//!
//! ## Concepts
//!
//! - **Replay archive**: one zstd-compressed JSONL file per UTC hour at
//!   `https://replay.pumpapi.io/YYYY/MM/DD/HH.jsonl.zst`. A decompressed hour is
//!   ~2 GB, so we stream-decompress and keep only filtered events in RAM, then
//!   delete the compressed file once it slides out of the window.
//! - **Token tape**: the chronological trades for one token over a fixed
//!   *window* of `W` hours, anchored on the hour the token is *born* (its
//!   `create` / `createPool` / `migrate` event) on the selected DEX.
//! - **Period** `[a, b]`: the inclusive range of birth hours to produce tapes
//!   for. Serving it requires raw data for hours `a ..= b + (W - 1)` — the extra
//!   look-ahead lets a token born in hour `b` accumulate its full `W`-hour tape.
//!
//! ## Step-wise workflow (bounded storage)
//!
//! For a window of `W` hours the replayer keeps at most `W` hours live. To
//! process anchor hour `H` it ensures hours `H ..= H+W-1` are loaded, emits that
//! hour's tapes, then drops hour `H` (and deletes its `.zst`) before advancing to
//! `H+1` — which only needs to fetch the single new hour `H+W`. Exactly the
//! "download W hours, finish the first, drop it, fetch the next" cycle.
//!
//! ## Availability & alerting
//!
//! [`Replayer::plan`] lists the archive and reports which anchors are
//! serviceable. If the requested period runs past the archived data (or before
//! it began on 2026-04-18), the period is clamped and a warning is produced
//! instead of failing mid-run.
//!
//! ## Example
//!
//! ```no_run
//! use std::path::PathBuf;
//! use pump_replayer::{Dex, DexFilter, Hour, HourRange, ReplayConfig, Replayer};
//!
//! # fn main() -> pump_replayer::Result<()> {
//! let period = HourRange::new(
//!     Hour::from_ymdh(2026, 4, 18, 0).unwrap(),
//!     Hour::from_ymdh(2026, 4, 18, 5).unwrap(),
//! );
//! let cfg = ReplayConfig::new(
//!     period,
//!     3,                              // 3-hour tape window
//!     DexFilter::only(Dex::Pump),     // pump.fun launches only
//!     PathBuf::from("./replay_cache"),
//! );
//! let mut replayer = Replayer::new(cfg)?;
//! let report = replayer.run(|step| {
//!     for tape in &step.tapes {
//!         println!("{} {} trades over {}h", tape.mint, tape.events.len(), step.window_hours);
//!     }
//!     Ok(())
//! })?;
//! println!("emitted {} steps, {} tapes", report.steps_emitted, report.tapes_total);
//! # Ok(())
//! # }
//! ```

mod client;
mod dex;
mod error;
mod event;
mod replayer;
mod tape;
mod time;

pub use client::{archive_start, ReplayClient, DEFAULT_BASE_URL};
pub use dex::{Dex, DexFilter};
pub use error::{Error, Result};
pub use event::{
    parse_stream, EventKind, ParseStats, TapeEvent, TokenMeta, NATIVE_SOL_MINT, WSOL_MINT,
};
pub use replayer::{Plan, ReplayConfig, Replayer, RunReport};
pub use tape::{build_step, HourData, TapeStep, TokenTape};
pub use time::{Hour, HourRange};
