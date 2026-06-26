//! Persisting token tapes to disk, one file per token, grouped by day.
//!
//! Layout produced under `root`:
//!
//! ```text
//! root/
//!   2026-06-24/                 <- day of the token's birth (anchor) hour
//!     <mint-a>.json             <- one TokenTape per token
//!     <mint-b>.json
//!   2026-06-25/
//!     <mint-c>.json
//! ```
//!
//! Each token is born in exactly one anchor hour, so within a single run (fixed
//! DEX filter) each `<mint>.json` is written once. The file contains the full
//! [`TokenTape`] (birth event, metadata, and the chronological trades).

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::tape::{TapeStep, TokenTape};
use crate::time::Hour;

/// Writes token tapes as `root/<YYYY-MM-DD>/<mint>.json`.
pub struct TokenTapeWriter {
    root: PathBuf,
    pretty: bool,
}

impl TokenTapeWriter {
    /// Create a writer rooted at `root` (created lazily on first write).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        TokenTapeWriter {
            root: root.into(),
            pretty: false,
        }
    }

    /// Pretty-print the JSON (larger files, human-readable). Default: compact.
    pub fn pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }

    /// The day folder for a given hour (does not create it).
    pub fn day_dir(&self, hour: Hour) -> PathBuf {
        self.root.join(hour.date_str())
    }

    /// Persist every tape in `step` into its day folder. Returns how many tapes
    /// were written.
    pub fn write_step(&self, step: &TapeStep) -> Result<usize> {
        if step.tapes.is_empty() {
            return Ok(0);
        }
        let dir = self.day_dir(step.anchor_hour);
        fs::create_dir_all(&dir)?;
        for tape in &step.tapes {
            self.write_tape(&dir, tape)?;
        }
        Ok(step.tapes.len())
    }

    /// Persist a single tape into an explicit day folder.
    pub fn write_tape(&self, day_dir: &Path, tape: &TokenTape) -> Result<()> {
        let path = day_dir.join(format!("{}.json", tape.mint));
        let bytes = if self.pretty {
            serde_json::to_vec_pretty(tape)?
        } else {
            serde_json::to_vec(tape)?
        };
        fs::write(path, bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dex::Dex;
    use crate::event::{EventKind, TapeEvent};

    fn tape(mint: &str) -> TokenTape {
        let birth = TapeEvent {
            signature: "sig".into(),
            kind: EventKind::Create,
            dex: Dex::Pump,
            pool: "pump".into(),
            pool_id: None,
            mint: mint.into(),
            quote_mint: None,
            trader: None,
            token_amount: None,
            sol_amount: None,
            price: None,
            market_cap_sol: None,
            tokens_in_pool: None,
            sol_in_pool: None,
            timestamp_ms: 0,
            local_timestamp_ms: None,
            block: None,
            mayhem_mode: Some(false),
            raw: serde_json::Value::Null,
        };
        TokenTape {
            mint: mint.into(),
            dex: Dex::Pump,
            mayhem_mode: Some(false),
            birth,
            meta: None,
            window_start_ms: 0,
            window_end_ms: 3_600_000,
            events: vec![],
        }
    }

    #[test]
    fn writes_token_files_into_day_folder() {
        let dir = std::env::temp_dir().join(format!("pr-sink-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let writer = TokenTapeWriter::new(&dir).pretty(true);
        let step = TapeStep {
            anchor_hour: Hour::from_ymdh(2026, 6, 24, 3).unwrap(),
            window_hours: 3,
            tapes: vec![tape("AAApump"), tape("BBBpump")],
        };
        let n = writer.write_step(&step).unwrap();
        assert_eq!(n, 2);
        assert!(dir.join("2026-06-24/AAApump.json").exists());
        assert!(dir.join("2026-06-24/BBBpump.json").exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
