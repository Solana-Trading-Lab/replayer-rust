//! HTTP access to the PumpApi replay archive: hour-file downloads and archive
//! availability listing.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::time::Hour;

/// Default archive root.
pub const DEFAULT_BASE_URL: &str = "https://replay.pumpapi.io";

/// First hour for which archives exist (data collection began 2026-04-18 00:00 UTC).
pub fn archive_start() -> Hour {
    Hour::from_ymdh(2026, 4, 18, 0).expect("valid constant")
}

/// Thin wrapper over a blocking HTTP client pointed at the replay archive.
pub struct ReplayClient {
    base_url: String,
    http: reqwest::blocking::Client,
}

#[derive(Deserialize)]
struct DayListing {
    #[serde(default)]
    files: Vec<DayFile>,
}

#[derive(Deserialize)]
struct DayFile {
    key: String,
}

impl ReplayClient {
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Result<Self> {
        let http = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .user_agent("pump-replayer/0.1")
            .build()?;
        Ok(ReplayClient {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        })
    }

    fn url_for(&self, hour: Hour) -> String {
        format!("{}/{}", self.base_url, hour.url_path())
    }

    /// Download `hour`'s compressed file into `dir`, returning its path. If the
    /// file already exists locally it is reused (resumable across runs). Returns
    /// [`Error::NotFound`] on HTTP 404 so callers can clamp the period.
    pub fn download(&self, hour: Hour, dir: &Path) -> Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let dest = dir.join(hour.cache_file_name());
        if dest.exists() && fs::metadata(&dest)?.len() > 0 {
            log::debug!("reusing cached {}", dest.display());
            return Ok(dest);
        }

        let url = self.url_for(hour);
        log::info!("downloading {url}");
        let resp = self.http.get(&url).send()?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Error::NotFound(url));
        }
        let mut resp = resp.error_for_status()?;

        // Stream straight to disk; we never hold the whole compressed body in RAM.
        let tmp = dir.join(format!("{}.part", hour.cache_file_name()));
        {
            let mut file = fs::File::create(&tmp)?;
            std::io::copy(&mut resp, &mut file)?;
        }
        fs::rename(&tmp, &dest)?;
        Ok(dest)
    }

    /// List the hour files present for a single UTC day. Missing days (404) and
    /// other listing errors yield an empty set rather than failing the run.
    pub fn list_day(&self, day: Hour) -> BTreeSet<Hour> {
        let url = format!("{}/{}", self.base_url, day.day_path());
        let resp = match self.http.get(&url).send() {
            Ok(r) => r,
            Err(e) => {
                log::debug!("list_day {url} failed: {e}");
                return BTreeSet::new();
            }
        };
        if !resp.status().is_success() {
            return BTreeSet::new();
        }
        let listing: DayListing = match resp.json() {
            Ok(l) => l,
            Err(e) => {
                log::debug!("list_day {url} parse failed: {e}");
                return BTreeSet::new();
            }
        };
        listing
            .files
            .into_iter()
            .filter_map(|f| parse_key_to_hour(&f.key))
            .collect()
    }

    /// Set of hours actually present in the archive across the inclusive day span
    /// covering `[start, end]`. One listing request per day.
    pub fn available_hours(&self, start: Hour, end: Hour) -> BTreeSet<Hour> {
        let mut out = BTreeSet::new();
        // Nothing past the current hour can exist; clamp to avoid listing months
        // of empty future days when the caller asks for a far-future end.
        let now = Hour::from_datetime(Utc::now());
        let start = start.max(archive_start());
        let end = end.min(now);
        if start > end {
            return out;
        }
        // Step day by day from the start day to the end day.
        let day_secs = 86_400;
        let mut day = Hour::from_unix_hour(start.start().timestamp().div_euclid(day_secs) * 24);
        let last_day_idx = end.start().timestamp().div_euclid(day_secs);
        loop {
            let cur_day_idx = day.start().timestamp().div_euclid(day_secs);
            if cur_day_idx > last_day_idx {
                break;
            }
            for h in self.list_day(day) {
                if h >= start && h <= end {
                    out.insert(h);
                }
            }
            day = day.offset(24);
        }
        out
    }
}

/// Parse `"2026/04/18/00.jsonl.zst"` into the corresponding [`Hour`].
fn parse_key_to_hour(key: &str) -> Option<Hour> {
    let file = key.rsplit('/').next()?; // "00.jsonl.zst"
    let parts: Vec<&str> = key.split('/').collect();
    let n = parts.len();
    if n < 4 {
        return None;
    }
    let year: i32 = parts[n - 4].parse().ok()?;
    let month: u32 = parts[n - 3].parse().ok()?;
    let day: u32 = parts[n - 2].parse().ok()?;
    let hour: u32 = file.split('.').next()?.parse().ok()?;
    Hour::from_ymdh(year, month, day, hour)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_parsing() {
        let h = parse_key_to_hour("2026/04/18/00.jsonl.zst").unwrap();
        assert_eq!(h.url_path(), "2026/04/18/00.jsonl.zst");
        assert_eq!(parse_key_to_hour("2026/04/18/23.jsonl.zst").unwrap().url_path(), "2026/04/18/23.jsonl.zst");
        assert!(parse_key_to_hour("garbage").is_none());
    }
}
