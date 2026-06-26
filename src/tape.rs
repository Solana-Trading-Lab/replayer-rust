//! Token tapes and the pure (network-free) windowing logic that builds them.
//!
//! A **token tape** is the chronological sequence of trades for one token over a
//! fixed *tape window* of `W` hours, anchored on the hour the token was *born*
//! (its `create` / `createPool` / `migrate` event) on the selected DEX.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::dex::DexFilter;
use crate::event::{EventKind, TapeEvent, TokenMeta};
use crate::time::Hour;

/// The parsed, filtered events of a single hour file, held in the sliding
/// window. Only events matching the configured DEX filter and kept kinds are
/// present, so this is much smaller than the raw decompressed hour.
pub struct HourData {
    pub hour: Hour,
    pub events: Vec<TapeEvent>,
    pub metas: HashMap<String, TokenMeta>,
}

/// One token's tape: every kept trade for `mint` inside the window, in
/// chronological order, preceded by the birth event that anchored it.
#[derive(Clone, Debug, Serialize)]
pub struct TokenTape {
    pub mint: String,
    /// The DEX family this tape was built for.
    pub dex: crate::dex::Dex,
    /// The event that anchored this tape (a create/createPool/migrate).
    pub birth: TapeEvent,
    /// Token metadata, if a create event supplied it.
    pub meta: Option<TokenMeta>,
    /// Window start (inclusive), ms — equals the anchor hour start.
    pub window_start_ms: i64,
    /// Window end (exclusive), ms — anchor start + `window_hours`.
    pub window_end_ms: i64,
    /// pump.fun "mayhem mode" flag for this token, from its birth event.
    pub mayhem_mode: Option<bool>,
    /// Trades within the window, chronological. Each event's full original JSON
    /// is preserved in [`TapeEvent::raw`] when the replayer keeps raw records.
    pub events: Vec<TapeEvent>,
}

impl TokenTape {
    pub fn num_buys(&self) -> usize {
        self.events.iter().filter(|e| e.kind == EventKind::Buy).count()
    }
    pub fn num_sells(&self) -> usize {
        self.events.iter().filter(|e| e.kind == EventKind::Sell).count()
    }
    /// Total SOL volume across all trades in the tape.
    pub fn volume_sol(&self) -> f64 {
        self.events.iter().filter_map(|e| e.sol_amount).sum()
    }
    /// Last observed price in the window, if any trade carried one.
    pub fn last_price(&self) -> Option<f64> {
        self.events.iter().rev().find_map(|e| e.price)
    }
}

/// The result of processing one anchor hour: all token tapes born in that hour.
#[derive(Clone, Debug, Serialize)]
pub struct TapeStep {
    pub anchor_hour: Hour,
    pub window_hours: u32,
    pub tapes: Vec<TokenTape>,
}

/// Build the tapes for `anchor`, looking across the already-loaded window hours.
///
/// `loaded` must contain the hours `anchor ..= anchor + (window_hours - 1)`
/// (order does not matter; extra hours are ignored). A token is included iff it
/// has a *birth* event (kind in `birth_kinds`) during the anchor hour that
/// passes `filter`; its tape then collects every event whose kind is in
/// `include_kinds` for the same mint within the window.
pub fn build_step(
    anchor: Hour,
    window_hours: u32,
    filter: &DexFilter,
    birth_kinds: &HashSet<EventKind>,
    include_kinds: &HashSet<EventKind>,
    exclude_mayhem: bool,
    loaded: &[HourData],
) -> TapeStep {
    let window_start = anchor.start_ms();
    let window_end = window_start + window_hours as i64 * 3_600_000;

    // 1. Find births during the anchor hour: mint -> earliest birth event.
    let mut births: HashMap<&str, &TapeEvent> = HashMap::new();
    let mut meta_for: HashMap<&str, &TokenMeta> = HashMap::new();
    for hd in loaded.iter().filter(|hd| hd.hour == anchor) {
        for ev in &hd.events {
            if !birth_kinds.contains(&ev.kind) {
                continue;
            }
            if !filter.matches(&ev.pool) {
                continue;
            }
            // Drop mayhem-mode tokens entirely when asked to.
            if exclude_mayhem && ev.mayhem_mode == Some(true) {
                continue;
            }
            if ev.timestamp_ms < window_start || ev.timestamp_ms >= anchor.end_ms() {
                continue;
            }
            births
                .entry(&ev.mint)
                .and_modify(|cur| {
                    if ev.timestamp_ms < cur.timestamp_ms {
                        *cur = ev;
                    }
                })
                .or_insert(ev);
        }
        for (mint, meta) in &hd.metas {
            meta_for.entry(mint.as_str()).or_insert(meta);
        }
    }

    if births.is_empty() {
        return TapeStep {
            anchor_hour: anchor,
            window_hours,
            tapes: Vec::new(),
        };
    }

    // 2. Collect, in one pass over the window, the trades for each born mint.
    let mut trades: HashMap<&str, Vec<&TapeEvent>> = births
        .keys()
        .map(|m| (*m, Vec::new()))
        .collect();
    for hd in loaded {
        for ev in &hd.events {
            if !include_kinds.contains(&ev.kind) {
                continue;
            }
            if ev.timestamp_ms < window_start || ev.timestamp_ms >= window_end {
                continue;
            }
            if !filter.matches(&ev.pool) {
                continue;
            }
            if let Some(slot) = trades.get_mut(ev.mint.as_str()) {
                slot.push(ev);
            }
        }
    }

    // 3. Assemble tapes, sorted by birth time then mint for determinism.
    let mut mints: Vec<&str> = births.keys().copied().collect();
    mints.sort_by(|a, b| {
        let ta = births[a].timestamp_ms;
        let tb = births[b].timestamp_ms;
        ta.cmp(&tb).then_with(|| a.cmp(b))
    });

    let mut tapes = Vec::with_capacity(mints.len());
    for mint in mints {
        let birth = births[mint];
        let mut evs: Vec<TapeEvent> = trades
            .remove(mint)
            .unwrap_or_default()
            .into_iter()
            .cloned()
            .collect();
        evs.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then_with(|| a.signature.cmp(&b.signature))
        });
        tapes.push(TokenTape {
            mint: mint.to_string(),
            dex: birth.dex,
            mayhem_mode: birth.mayhem_mode,
            birth: birth.clone(),
            meta: meta_for.get(mint).map(|m| (*m).clone()),
            window_start_ms: window_start,
            window_end_ms: window_end,
            events: evs,
        });
    }

    TapeStep {
        anchor_hour: anchor,
        window_hours,
        tapes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dex::Dex;

    fn ev(mint: &str, kind: EventKind, ts: i64, pool: &str) -> TapeEvent {
        ev_mayhem(mint, kind, ts, pool, None)
    }

    fn ev_mayhem(
        mint: &str,
        kind: EventKind,
        ts: i64,
        pool: &str,
        mayhem: Option<bool>,
    ) -> TapeEvent {
        TapeEvent {
            signature: format!("{mint}-{ts}-{:?}", kind),
            kind,
            dex: Dex::of_pool(pool),
            pool: pool.to_string(),
            pool_id: None,
            mint: mint.to_string(),
            quote_mint: None,
            trader: None,
            token_amount: None,
            sol_amount: Some(1.0),
            price: Some(ts as f64),
            market_cap_sol: None,
            tokens_in_pool: None,
            sol_in_pool: None,
            timestamp_ms: ts,
            local_timestamp_ms: None,
            block: None,
            mayhem_mode: mayhem,
            raw: serde_json::Value::Null,
        }
    }

    fn birth_kinds() -> HashSet<EventKind> {
        HashSet::from([EventKind::Create, EventKind::CreatePool, EventKind::Migrate])
    }
    fn include_kinds() -> HashSet<EventKind> {
        HashSet::from([EventKind::Buy, EventKind::Sell])
    }

    #[test]
    fn three_hour_window_collects_forward() {
        let h0 = Hour::from_ymdh(2026, 4, 18, 0).unwrap();
        let h1 = h0.succ();
        let h2 = h1.succ();
        let s0 = h0.start_ms();

        // Token A is born in hour 0 and trades across all three hours.
        let hd0 = HourData {
            hour: h0,
            events: vec![
                ev("A", EventKind::Create, s0 + 1_000, "pump"),
                ev("A", EventKind::Buy, s0 + 2_000, "pump"),
                // Token B born later — should anchor its own (separate) hour, not h0.
                ev("B", EventKind::Create, s0 + 3_600_000 - 5, "pump"),
            ],
            metas: HashMap::new(),
        };
        let hd1 = HourData {
            hour: h1,
            events: vec![ev("A", EventKind::Sell, s0 + 3_600_000 + 10, "pump")],
            metas: HashMap::new(),
        };
        let hd2 = HourData {
            hour: h2,
            events: vec![ev("A", EventKind::Buy, s0 + 2 * 3_600_000 + 10, "pump")],
            metas: HashMap::new(),
        };

        let step = build_step(
            h0,
            3,
            &DexFilter::only(Dex::Pump),
            &birth_kinds(),
            &include_kinds(),
            true,
            &[hd0, hd1, hd2],
        );

        // Both A and B are born in hour 0.
        assert_eq!(step.tapes.len(), 2);
        let a = step.tapes.iter().find(|t| t.mint == "A").unwrap();
        // A's window spans 3 hours -> picks up the buy(h0), sell(h1), buy(h2).
        assert_eq!(a.events.len(), 3);
        assert_eq!(a.num_buys(), 2);
        assert_eq!(a.num_sells(), 1);
        assert_eq!(a.window_end_ms, s0 + 3 * 3_600_000);
    }

    #[test]
    fn dex_filter_excludes_other_pools() {
        let h0 = Hour::from_ymdh(2026, 4, 18, 0).unwrap();
        let s0 = h0.start_ms();
        let hd0 = HourData {
            hour: h0,
            events: vec![
                ev("R", EventKind::CreatePool, s0 + 1, "raydium-cpmm"),
                ev("R", EventKind::Buy, s0 + 2, "raydium-cpmm"),
                ev("P", EventKind::Create, s0 + 3, "pump"),
            ],
            metas: HashMap::new(),
        };
        // Filtering to pump should drop the raydium token entirely.
        let step = build_step(
            h0,
            1,
            &DexFilter::only(Dex::Pump),
            &birth_kinds(),
            &include_kinds(),
            true,
            &[hd0],
        );
        assert_eq!(step.tapes.len(), 1);
        assert_eq!(step.tapes[0].mint, "P");
    }

    #[test]
    fn mayhem_tokens_are_filtered_out() {
        let h0 = Hour::from_ymdh(2026, 4, 18, 0).unwrap();
        let s0 = h0.start_ms();
        let hd0 = HourData {
            hour: h0,
            events: vec![
                // M is a mayhem-mode token; N is normal.
                ev_mayhem("M", EventKind::Create, s0 + 1, "pump", Some(true)),
                ev_mayhem("M", EventKind::Buy, s0 + 2, "pump", Some(true)),
                ev_mayhem("N", EventKind::Create, s0 + 3, "pump", Some(false)),
                ev_mayhem("N", EventKind::Buy, s0 + 4, "pump", Some(false)),
            ],
            metas: HashMap::new(),
        };

        // exclude_mayhem = true -> only N survives.
        let step = build_step(
            h0, 1, &DexFilter::only(Dex::Pump), &birth_kinds(), &include_kinds(), true, &[hd0],
        );
        assert_eq!(step.tapes.len(), 1);
        assert_eq!(step.tapes[0].mint, "N");
        assert_eq!(step.tapes[0].mayhem_mode, Some(false));

        // exclude_mayhem = false -> both kept, mayhem flag exposed.
        let hd0 = HourData {
            hour: h0,
            events: vec![
                ev_mayhem("M", EventKind::Create, s0 + 1, "pump", Some(true)),
                ev_mayhem("N", EventKind::Create, s0 + 3, "pump", Some(false)),
            ],
            metas: HashMap::new(),
        };
        let step = build_step(
            h0, 1, &DexFilter::only(Dex::Pump), &birth_kinds(), &include_kinds(), false, &[hd0],
        );
        assert_eq!(step.tapes.len(), 2);
        let m = step.tapes.iter().find(|t| t.mint == "M").unwrap();
        assert_eq!(m.mayhem_mode, Some(true));
    }
}
