//! Hour-granular UTC time helpers.
//!
//! The PumpApi replay archive is organized into one file per UTC hour:
//! `https://replay.pumpapi.io/YYYY/MM/DD/HH.jsonl.zst`, where the file named
//! `HH` contains every event whose timestamp falls in `[HH:00, HH+1:00)` UTC.
//! (Verified against real data; note the published docs example is off-by-one.)

use std::fmt;

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use serde::{Serialize, Serializer};

const MS_PER_HOUR: i64 = 3_600_000;
const SECS_PER_HOUR: i64 = 3_600;

/// A single UTC hour, identified by its index since the unix epoch.
///
/// `Hour(0)` is `1970-01-01T00:00Z`. The hour is always aligned to `:00:00`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hour(i64);

impl Hour {
    /// Build from a raw epoch-hour index.
    pub fn from_unix_hour(h: i64) -> Self {
        Hour(h)
    }

    /// Build from a `DateTime<Utc>`, truncating to the start of its hour.
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Hour(dt.timestamp().div_euclid(SECS_PER_HOUR))
    }

    /// Build from a millisecond unix timestamp (the `timestamp` field in events).
    pub fn from_millis(ms: i64) -> Self {
        Hour(ms.div_euclid(MS_PER_HOUR))
    }

    /// Build from explicit calendar components. Returns `None` for invalid dates.
    pub fn from_ymdh(year: i32, month: u32, day: u32, hour: u32) -> Option<Self> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
            .single()
            .map(Self::from_datetime)
    }

    /// The epoch-hour index.
    pub fn unix_hour(self) -> i64 {
        self.0
    }

    /// `DateTime<Utc>` for the start of this hour.
    pub fn start(self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.0 * SECS_PER_HOUR, 0).single().unwrap()
    }

    /// Millisecond timestamp of the start of this hour (inclusive).
    pub fn start_ms(self) -> i64 {
        self.0 * MS_PER_HOUR
    }

    /// Millisecond timestamp of the end of this hour (exclusive).
    pub fn end_ms(self) -> i64 {
        (self.0 + 1) * MS_PER_HOUR
    }

    /// The next hour.
    pub fn succ(self) -> Hour {
        Hour(self.0 + 1)
    }

    /// The previous hour.
    pub fn pred(self) -> Hour {
        Hour(self.0 - 1)
    }

    /// Shift by `n` hours (may be negative).
    pub fn offset(self, n: i64) -> Hour {
        Hour(self.0 + n)
    }

    /// Archive-relative path, e.g. `2026/04/18/00.jsonl.zst`.
    pub fn url_path(self) -> String {
        let dt = self.start();
        format!(
            "{:04}/{:02}/{:02}/{:02}.jsonl.zst",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour()
        )
    }

    /// Archive-relative directory of this hour's day, e.g. `2026/04/18/`.
    pub fn day_path(self) -> String {
        let dt = self.start();
        format!("{:04}/{:02}/{:02}/", dt.year(), dt.month(), dt.day())
    }

    /// A filesystem-safe cache file name, e.g. `2026-04-18-00.jsonl.zst`.
    pub fn cache_file_name(self) -> String {
        let dt = self.start();
        format!(
            "{:04}-{:02}-{:02}-{:02}.jsonl.zst",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour()
        )
    }
}

impl fmt::Display for Hour {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dt = self.start();
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:00Z",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour()
        )
    }
}

impl fmt::Debug for Hour {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hour({})", self)
    }
}

impl Serialize for Hour {
    /// Serializes as the ISO hour string, e.g. `"2026-04-18T00:00Z"`.
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

/// An inclusive range of hours `[start, end]`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HourRange {
    pub start: Hour,
    pub end: Hour,
}

impl HourRange {
    /// Construct an inclusive range. Panics if `start > end`; prefer
    /// [`HourRange::try_new`] when bounds come from user input.
    pub fn new(start: Hour, end: Hour) -> Self {
        assert!(start <= end, "HourRange start must be <= end");
        HourRange { start, end }
    }

    /// Fallible constructor that returns `None` when `start > end`.
    pub fn try_new(start: Hour, end: Hour) -> Option<Self> {
        (start <= end).then_some(HourRange { start, end })
    }

    /// Number of hours in the range (inclusive).
    pub fn len(self) -> u64 {
        (self.end.unix_hour() - self.start.unix_hour() + 1) as u64
    }

    pub fn is_empty(self) -> bool {
        self.start > self.end
    }

    pub fn contains(self, h: Hour) -> bool {
        self.start <= h && h <= self.end
    }

    /// Iterate every hour in the range, inclusive.
    pub fn iter(self) -> impl Iterator<Item = Hour> {
        (self.start.unix_hour()..=self.end.unix_hour()).map(Hour::from_unix_hour)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_and_mapping_roundtrip() {
        let h = Hour::from_ymdh(2026, 4, 18, 0).unwrap();
        assert_eq!(h.url_path(), "2026/04/18/00.jsonl.zst");
        assert_eq!(h.day_path(), "2026/04/18/");
        assert_eq!(h.cache_file_name(), "2026-04-18-00.jsonl.zst");
        // First event of file 01.jsonl.zst was 1776474000211 == 2026-04-18T01:00:00Z.
        assert_eq!(Hour::from_millis(1776474000211).url_path(), "2026/04/18/01.jsonl.zst");
    }

    #[test]
    fn range_iter() {
        let r = HourRange::new(
            Hour::from_ymdh(2026, 4, 18, 22).unwrap(),
            Hour::from_ymdh(2026, 4, 19, 1).unwrap(),
        );
        let v: Vec<_> = r.iter().map(|h| h.url_path()).collect();
        assert_eq!(
            v,
            vec![
                "2026/04/18/22.jsonl.zst",
                "2026/04/18/23.jsonl.zst",
                "2026/04/19/00.jsonl.zst",
                "2026/04/19/01.jsonl.zst",
            ]
        );
        assert_eq!(r.len(), 4);
    }
}
