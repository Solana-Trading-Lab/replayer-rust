//! The step-wise replayer: the sliding-window orchestrator that ties downloads,
//! parsing and tape building together while keeping disk/RAM bounded.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::client::{archive_start, ReplayClient, DEFAULT_BASE_URL};
use crate::dex::DexFilter;
use crate::error::{Error, Result};
use crate::event::{parse_stream, EventKind};
use crate::tape::{build_step, HourData, TapeStep};
use crate::time::{Hour, HourRange};

/// Configuration for a replay run.
///
/// `period` is the inclusive range of **birth hours** to produce tapes for. With
/// a `window_hours` of `W`, raw data is required for hours
/// `period.start ..= period.end + (W - 1)` — the extra look-ahead hours let a
/// token born in `period.end` accumulate its full `W`-hour tape.
pub struct ReplayConfig {
    /// Inclusive range of anchor (birth) hours.
    pub period: HourRange,
    /// Token tape window length, in hours. Must be >= 1.
    pub window_hours: u32,
    /// Which DEX(es)/pools to include.
    pub dex: DexFilter,
    /// Directory used to cache downloaded `.zst` files.
    pub work_dir: PathBuf,
    /// Archive root URL.
    pub base_url: String,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Event kinds that count as a token "birth" (anchor) during a hour.
    pub birth_kinds: HashSet<EventKind>,
    /// Event kinds collected into each tape.
    pub include_kinds: HashSet<EventKind>,
    /// Keep downloaded `.zst` files after they slide out of the window. Default
    /// `false`: each hour's file is deleted as soon as it is no longer needed, so
    /// at most `window_hours` compressed files exist on disk at once.
    pub keep_files: bool,
}

impl ReplayConfig {
    /// Sensible defaults: births = create/createPool/migrate, tape = buys+sells,
    /// default archive URL, 5-minute timeout, files deleted when no longer needed.
    pub fn new(period: HourRange, window_hours: u32, dex: DexFilter, work_dir: PathBuf) -> Self {
        ReplayConfig {
            period,
            window_hours,
            dex,
            work_dir,
            base_url: DEFAULT_BASE_URL.to_string(),
            request_timeout: Duration::from_secs(300),
            birth_kinds: HashSet::from([
                EventKind::Create,
                EventKind::CreatePool,
                EventKind::Migrate,
            ]),
            include_kinds: HashSet::from([EventKind::Buy, EventKind::Sell]),
            keep_files: false,
        }
    }

    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }
    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }
    pub fn birth_kinds<I: IntoIterator<Item = EventKind>>(mut self, kinds: I) -> Self {
        self.birth_kinds = kinds.into_iter().collect();
        self
    }
    pub fn include_kinds<I: IntoIterator<Item = EventKind>>(mut self, kinds: I) -> Self {
        self.include_kinds = kinds.into_iter().collect();
        self
    }
    pub fn keep_files(mut self, keep: bool) -> Self {
        self.keep_files = keep;
        self
    }

    /// Union of birth + include kinds: the set of kinds worth keeping in memory.
    fn keep_kinds(&self) -> HashSet<EventKind> {
        self.birth_kinds
            .union(&self.include_kinds)
            .copied()
            .collect()
    }
}

/// The resolved, availability-checked plan for a run.
#[derive(Clone, Debug)]
pub struct Plan {
    /// The anchor range the caller requested.
    pub requested: HourRange,
    /// Anchor hours that can actually be served (their full window is available).
    pub effective_anchors: Vec<Hour>,
    /// All hour files that must be readable to serve the effective anchors.
    pub required_hours: Option<HourRange>,
    /// Human-readable notices about clamping / missing data.
    pub warnings: Vec<String>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.effective_anchors.is_empty()
    }
}

/// Summary returned by [`Replayer::run`].
#[derive(Clone, Debug)]
pub struct RunReport {
    pub plan: Plan,
    pub steps_emitted: usize,
    pub tapes_total: usize,
}

/// Step-wise replayer. Construct with [`Replayer::new`], inspect availability
/// with [`Replayer::plan`], then drive it with [`Replayer::run`].
pub struct Replayer {
    cfg: ReplayConfig,
    client: ReplayClient,
    keep_kinds: HashSet<EventKind>,
    /// The sliding window: loaded, parsed hours keyed by hour.
    loaded: BTreeMap<Hour, HourData>,
    /// Paths of `.zst` files currently on disk, keyed by hour (for cleanup).
    files: BTreeMap<Hour, PathBuf>,
    /// Memoized availability plan (listing the archive is relatively expensive).
    cached_plan: Option<Plan>,
}

impl Replayer {
    pub fn new(cfg: ReplayConfig) -> Result<Self> {
        if cfg.window_hours == 0 {
            return Err(Error::Config("window_hours must be >= 1".into()));
        }
        if cfg.period.start > cfg.period.end {
            return Err(Error::Config("period start must be <= end".into()));
        }
        let client = ReplayClient::new(cfg.base_url.clone(), cfg.request_timeout)?;
        let keep_kinds = cfg.keep_kinds();
        Ok(Replayer {
            cfg,
            client,
            keep_kinds,
            loaded: BTreeMap::new(),
            files: BTreeMap::new(),
            cached_plan: None,
        })
    }

    /// The hours that must exist to fully serve the requested period:
    /// `[period.start, period.end + window - 1]`.
    fn required_span(&self) -> HourRange {
        let last = self.cfg.period.end.offset(self.cfg.window_hours as i64 - 1);
        HourRange::new(self.cfg.period.start, last)
    }

    /// Check archive availability and compute which anchors can be served,
    /// clamping the period and emitting warnings where data is missing. The
    /// result is memoized (the listing is reused by [`Replayer::run`]).
    pub fn plan(&mut self) -> Result<Plan> {
        if self.cached_plan.is_none() {
            let p = self.compute_plan()?;
            self.cached_plan = Some(p);
        }
        Ok(self.cached_plan.clone().unwrap())
    }

    fn compute_plan(&self) -> Result<Plan> {
        let requested = self.cfg.period;
        let span = self.required_span();
        let w = self.cfg.window_hours as i64;
        let mut warnings = Vec::new();

        // Anything before the archive start can never be served.
        let astart = archive_start();
        let effective_start = if requested.start < astart {
            warnings.push(format!(
                "requested start {} precedes archive start {}; clamped to {}",
                requested.start, astart, astart
            ));
            astart
        } else {
            requested.start
        };

        let available = self.client.available_hours(span.start, span.end);
        if available.is_empty() {
            return Ok(Plan {
                requested,
                effective_anchors: Vec::new(),
                required_hours: None,
                warnings: {
                    warnings.push(format!(
                        "no archived hours found in required span {}..={}",
                        span.start, span.end
                    ));
                    warnings
                },
            });
        }
        let data_end = *available.iter().next_back().unwrap();

        // An anchor H is serviceable iff every hour H..=H+w-1 is present.
        let is_serviceable = |h: Hour, avail: &BTreeSet<Hour>| -> bool {
            (0..w).all(|k| avail.contains(&h.offset(k)))
        };

        let mut effective_anchors = Vec::new();
        let mut missing_middle = 0usize;
        for h in HourRange::new(effective_start, requested.end).iter() {
            if is_serviceable(h, &available) {
                effective_anchors.push(h);
            } else {
                missing_middle += 1;
            }
        }

        // Tail clamp message: tokens born near the end need look-ahead hours that
        // may not be archived yet.
        let max_serviceable_anchor = data_end.offset(-(w - 1));
        if requested.end > max_serviceable_anchor {
            warnings.push(format!(
                "period end {} needs look-ahead up to {} but archive ends at {}; \
                 last serviceable anchor is {}",
                requested.end,
                requested.end.offset(w - 1),
                data_end,
                max_serviceable_anchor
            ));
        }
        if missing_middle > 0 {
            warnings.push(format!(
                "{missing_middle} anchor hour(s) skipped: their {w}-hour window \
                 contains gaps in the archive"
            ));
        }

        let required_hours = effective_anchors.first().map(|first| {
            let last = effective_anchors.last().unwrap().offset(w - 1);
            HourRange::new(*first, last)
        });

        Ok(Plan {
            requested,
            effective_anchors,
            required_hours,
            warnings,
        })
    }

    /// Run the full step-wise replay, invoking `on_step` once per serviceable
    /// anchor hour. The callback receives that hour's tapes; returning `Err`
    /// aborts the run. Disk and memory are bounded to the tape window throughout.
    pub fn run<F>(&mut self, mut on_step: F) -> Result<RunReport>
    where
        F: FnMut(&TapeStep) -> Result<()>,
    {
        let plan = self.plan()?;
        for w in &plan.warnings {
            log::warn!("{w}");
        }
        if plan.is_empty() {
            return Err(Error::NoData(format!(
                "no serviceable anchor hours for period {}..={} with window {}h",
                self.cfg.period.start, self.cfg.period.end, self.cfg.window_hours
            )));
        }

        let w = self.cfg.window_hours as i64;
        let mut steps_emitted = 0usize;
        let mut tapes_total = 0usize;

        for &anchor in &plan.effective_anchors {
            // 1. Ensure the window [anchor, anchor+w-1] is loaded in memory.
            for k in 0..w {
                let h = anchor.offset(k);
                if !self.loaded.contains_key(&h) {
                    self.load_hour(h)?;
                }
            }

            // 2. Build and emit this anchor's step.
            let window: Vec<HourData> = self.window_view(anchor, w);
            let step = build_step(
                anchor,
                self.cfg.window_hours,
                &self.cfg.dex,
                &self.cfg.birth_kinds,
                &self.cfg.include_kinds,
                &window,
            );
            // Put the borrowed hours back (window_view temporarily removed them).
            for hd in window {
                self.loaded.insert(hd.hour, hd);
            }

            tapes_total += step.tapes.len();
            steps_emitted += 1;
            log::info!(
                "anchor {} -> {} tape(s) [{} loaded hour(s) in window]",
                anchor,
                step.tapes.len(),
                w
            );
            on_step(&step)?;

            // 3. Slide: drop the anchor hour (the next anchor no longer needs it)
            //    and remove its cached file unless asked to keep it.
            self.evict_below(anchor.succ());
        }

        // Final cleanup of whatever remains in the window.
        self.evict_below(Hour::from_unix_hour(i64::MAX));

        Ok(RunReport {
            plan,
            steps_emitted,
            tapes_total,
        })
    }

    /// Download + decompress + parse a single hour into the in-memory window.
    fn load_hour(&mut self, hour: Hour) -> Result<()> {
        let path = self.client.download(hour, &self.cfg.work_dir)?;
        log::debug!("decompressing + parsing {}", path.display());
        let file = fs::File::open(&path)?;
        let decoder = zstd::stream::read::Decoder::new(file)?;
        let (events, metas, stats) = parse_stream(decoder, &self.cfg.dex, &self.keep_kinds)?;
        log::debug!(
            "{hour}: kept {} of {} candidate event line(s) ({} total lines, {} parse error(s))",
            stats.kept,
            stats.pool_lines,
            stats.lines,
            stats.parse_errors
        );
        if stats.looks_like_schema_break() {
            log::warn!(
                "{hour}: {} of {} candidate event lines failed to parse — the upstream \
                 schema may have changed; results for this hour are likely incomplete",
                stats.parse_errors,
                stats.pool_lines
            );
        }
        self.loaded.insert(
            hour,
            HourData {
                hour,
                events,
                metas,
            },
        );
        self.files.insert(hour, path);

        // If we delete files eagerly, the decompressed events now live in RAM and
        // the compressed file is no longer needed for *this* hour. We still keep
        // it until slide-out so a re-entrant window doesn't re-download; eviction
        // handles removal.
        Ok(())
    }

    /// Temporarily remove the window hours from `loaded` so they can be passed as
    /// a slice to `build_step` without borrow conflicts. Callers must reinsert.
    fn window_view(&mut self, anchor: Hour, w: i64) -> Vec<HourData> {
        (0..w)
            .filter_map(|k| self.loaded.remove(&anchor.offset(k)))
            .collect()
    }

    /// Drop every loaded hour strictly below `floor` and delete its cached file.
    fn evict_below(&mut self, floor: Hour) {
        let drop_hours: Vec<Hour> = self
            .loaded
            .keys()
            .copied()
            .filter(|h| *h < floor)
            .collect();
        for h in drop_hours {
            self.loaded.remove(&h);
        }
        let drop_files: Vec<Hour> = self
            .files
            .keys()
            .copied()
            .filter(|h| *h < floor)
            .collect();
        for h in drop_files {
            if let Some(path) = self.files.remove(&h) {
                if !self.cfg.keep_files {
                    if let Err(e) = fs::remove_file(&path) {
                        log::debug!("could not remove {}: {e}", path.display());
                    } else {
                        log::debug!("removed cached {}", path.display());
                    }
                }
            }
        }
    }

    /// Convenience: run and persist every token tape to disk as
    /// `root/<YYYY-MM-DD>/<mint>.json` (grouped by the token's birth day). This
    /// keeps the bounded-storage stepping — tapes are flushed per anchor hour,
    /// never all held at once.
    pub fn run_to_dir(&mut self, root: impl Into<PathBuf>) -> Result<RunReport> {
        let writer = crate::sink::TokenTapeWriter::new(root);
        self.run(|step| writer.write_step(step).map(|_| ()))
    }

    /// Convenience: run and collect every step into a Vec. Only suitable for
    /// small periods — defeats the memory bound for large ones.
    pub fn run_collect(&mut self) -> Result<(Vec<TapeStep>, RunReport)> {
        let mut steps = Vec::new();
        let report = self.run(|s| {
            steps.push(s.clone());
            Ok(())
        })?;
        Ok((steps, report))
    }
}
