//! Event schema and streaming JSONL parsing.
//!
//! Each decompressed line is one JSON event in the exact format of the live
//! PumpApi data stream. We only deserialize the fields needed to build token
//! tapes; everything else is ignored. `transfer` events (which have no top-level
//! `pool`) are cheaply skipped before any JSON parsing.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read};

use serde::{Deserialize, Serialize};

use crate::dex::{Dex, DexFilter};
use crate::error::Result;

/// Wrapped-SOL mint, the usual quote token (`quoteMint`) for pump/pumpswap pools.
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Native SOL mint, seen as the quote on some older records.
pub const NATIVE_SOL_MINT: &str = "So11111111111111111111111111111111111111111";

/// The kind of an event, derived from the `txType` field.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EventKind {
    Create,
    CreatePool,
    Buy,
    Sell,
    Add,
    Remove,
    Migrate,
    Other,
}

impl EventKind {
    pub fn from_tx_type(tx: &str) -> EventKind {
        match tx {
            "create" => EventKind::Create,
            "createPool" => EventKind::CreatePool,
            "buy" => EventKind::Buy,
            "sell" => EventKind::Sell,
            "add" => EventKind::Add,
            "remove" => EventKind::Remove,
            "migrate" => EventKind::Migrate,
            _ => EventKind::Other,
        }
    }

    /// Is this a trade (buy/sell)?
    pub fn is_trade(self) -> bool {
        matches!(self, EventKind::Buy | EventKind::Sell)
    }
}

/// Raw deserialization target. All optional except the two fields present on
/// every event we keep.
#[derive(Deserialize)]
struct RawEvent {
    signature: String,
    // Schema note: an archive-wide rename happened mid-2026. Older files use
    // `txType` / `sol*` / `marketCapSol`; newer files use `action` / `quote*` /
    // `marketCapQuote`. Aliases let one parser read the whole archive.
    #[serde(rename = "txType", alias = "action")]
    tx_type: String,
    pool: Option<String>,
    mint: Option<String>,
    #[serde(rename = "quoteMint")]
    quote_mint: Option<String>,
    #[serde(rename = "poolId")]
    pool_id: Option<String>,
    #[serde(rename = "txSigner")]
    tx_signer: Option<String>,
    #[serde(rename = "tokenAmount")]
    token_amount: Option<f64>,
    #[serde(rename = "solAmount", alias = "quoteAmount")]
    sol_amount: Option<f64>,
    price: Option<f64>,
    #[serde(rename = "marketCapSol", alias = "marketCapQuote")]
    market_cap_sol: Option<f64>,
    #[serde(rename = "tokensInPool")]
    tokens_in_pool: Option<f64>,
    #[serde(rename = "solInPool", alias = "quoteInPool")]
    sol_in_pool: Option<f64>,
    timestamp: i64,
    #[serde(rename = "localTimestamp")]
    local_timestamp: Option<i64>,
    block: Option<u64>,
    // create / createPool / migrate metadata
    name: Option<String>,
    symbol: Option<String>,
    uri: Option<String>,
    supply: Option<f64>,
    decimals: Option<u32>,
    #[serde(rename = "initialBuy")]
    initial_buy: Option<f64>,
}

/// A compact, tape-ready event. Only the fields useful for downstream analysis
/// are retained, which keeps the in-memory sliding window far smaller than the
/// ~2 GB decompressed hour it came from.
#[derive(Clone, Debug, Serialize)]
pub struct TapeEvent {
    pub signature: String,
    pub kind: EventKind,
    pub dex: Dex,
    /// Raw `pool` string (e.g. `"pump-amm"`).
    pub pool: String,
    pub pool_id: Option<String>,
    pub mint: String,
    /// The pool's quote token mint (`quoteMint`). `None` on older records, where
    /// the quote is always SOL. See [`TapeEvent::is_sol_quoted`].
    pub quote_mint: Option<String>,
    /// Transaction signer / trader where applicable.
    pub trader: Option<String>,
    pub token_amount: Option<f64>,
    /// Quote-token amount of the trade. For SOL-quoted pools (the common case)
    /// this is SOL; check [`TapeEvent::is_sol_quoted`] for others.
    pub sol_amount: Option<f64>,
    pub price: Option<f64>,
    pub market_cap_sol: Option<f64>,
    pub tokens_in_pool: Option<f64>,
    pub sol_in_pool: Option<f64>,
    /// Millisecond on-chain timestamp.
    pub timestamp_ms: i64,
    /// Millisecond timestamp the Frankfurt server received the tx (replay only).
    pub local_timestamp_ms: Option<i64>,
    pub block: Option<u64>,
}

impl TapeEvent {
    /// True when the trade is quoted in SOL (so `sol_amount` is a SOL value).
    /// Records without an explicit `quoteMint` are treated as SOL-quoted.
    pub fn is_sol_quoted(&self) -> bool {
        match self.quote_mint.as_deref() {
            None => true,
            Some(m) => m == WSOL_MINT || m == NATIVE_SOL_MINT,
        }
    }
}

/// Token metadata, captured from `create` / `createPool` events.
#[derive(Clone, Debug, Serialize)]
pub struct TokenMeta {
    pub mint: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub uri: Option<String>,
    pub supply: Option<f64>,
    pub decimals: Option<u32>,
    pub initial_buy: Option<f64>,
}

/// Diagnostics for a parsed hour, so silent schema drift surfaces as a signal
/// rather than as empty output.
#[derive(Debug, Default, Clone, Copy)]
pub struct ParseStats {
    /// Total decompressed lines read.
    pub lines: u64,
    /// Lines that passed the cheap `"pool":` pre-filter (candidate events).
    pub pool_lines: u64,
    /// Candidate lines that failed JSON deserialization.
    pub parse_errors: u64,
    /// Events kept after DEX + kind filtering.
    pub kept: u64,
}

impl ParseStats {
    /// Heuristic: a large fraction of candidate lines failing to parse almost
    /// always means the upstream schema changed shape.
    pub fn looks_like_schema_break(&self) -> bool {
        self.pool_lines > 100 && self.parse_errors * 2 > self.pool_lines
    }
}

/// Parse a decompressed JSONL stream into compact events, keeping only events
/// that (a) match `filter` and (b) have a kind in `keep_kinds`.
///
/// Returns the kept events (in file order, i.e. chronological), a map of any
/// token metadata discovered, and [`ParseStats`] for diagnostics.
pub fn parse_stream<R: Read>(
    reader: R,
    filter: &DexFilter,
    keep_kinds: &HashSet<EventKind>,
) -> Result<(Vec<TapeEvent>, HashMap<String, TokenMeta>, ParseStats)> {
    let buf = BufReader::with_capacity(1 << 20, reader);
    let mut events = Vec::new();
    let mut metas: HashMap<String, TokenMeta> = HashMap::new();
    let mut stats = ParseStats::default();

    for line in buf.lines() {
        let line = line?;
        stats.lines += 1;
        // Fast reject: only tradable/pool events carry a top-level "pool" field.
        // `transfer` events (about half of all lines) never do.
        if !line.contains("\"pool\":") {
            continue;
        }
        stats.pool_lines += 1;
        let raw: RawEvent = match serde_json::from_str(&line) {
            Ok(r) => r,
            // Tolerate the occasional malformed/partial line rather than aborting
            // a multi-hundred-MB stream, but count it so a real schema break is
            // not mistaken for "no matching data".
            Err(_) => {
                stats.parse_errors += 1;
                continue;
            }
        };
        let (Some(pool), Some(mint)) = (raw.pool.as_deref(), raw.mint.as_deref()) else {
            continue;
        };
        if !filter.matches(pool) {
            continue;
        }
        let kind = EventKind::from_tx_type(&raw.tx_type);
        if !keep_kinds.contains(&kind) {
            continue;
        }

        if matches!(kind, EventKind::Create | EventKind::CreatePool) {
            metas.entry(mint.to_string()).or_insert_with(|| TokenMeta {
                mint: mint.to_string(),
                name: raw.name.clone(),
                symbol: raw.symbol.clone(),
                uri: raw.uri.clone(),
                supply: raw.supply,
                decimals: raw.decimals,
                initial_buy: raw.initial_buy,
            });
        }

        events.push(TapeEvent {
            signature: raw.signature,
            kind,
            dex: Dex::of_pool(pool),
            pool: pool.to_string(),
            pool_id: raw.pool_id,
            mint: mint.to_string(),
            quote_mint: raw.quote_mint,
            trader: raw.tx_signer,
            token_amount: raw.token_amount,
            sol_amount: raw.sol_amount,
            price: raw.price,
            market_cap_sol: raw.market_cap_sol,
            tokens_in_pool: raw.tokens_in_pool,
            sol_in_pool: raw.sol_in_pool,
            timestamp_ms: raw.timestamp,
            local_timestamp_ms: raw.local_timestamp,
            block: raw.block,
        });
        stats.kept += 1;
    }

    Ok((events, metas, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two real lines (a pump create and a pump-amm buy) plus a transfer that must
    // be skipped.
    const SAMPLE: &str = concat!(
        r#"{"signature":"s1","txType":"transfer","txSigner":"x","transfers":[],"timestamp":1}"#,
        "\n",
        r#"{"signature":"s2","txType":"create","poolId":"p","mint":"MINTpump","txSigner":"dev","solAmount":6.0,"price":4.0e-8,"name":"Asteroid","symbol":"AST","uri":"u","supply":1000000000,"decimals":6,"initialBuy":178833333.0,"pool":"pump","timestamp":1776474007203}"#,
        "\n",
        r#"{"signature":"s3","txType":"buy","poolId":"p2","mint":"MINTpump","txSigner":"alice","tokenAmount":123.0,"solAmount":0.5,"price":4.1e-7,"pool":"pump-amm","timestamp":1776474010000}"#,
        "\n",
        // New (mid-2026) schema: `action` + `quote*` instead of `txType` + `sol*`.
        r#"{"signature":"s4","action":"sell","poolId":"p3","mint":"MINTnew","quoteMint":"So11111111111111111111111111111111111111112","txSigner":"bob","tokenAmount":9.0,"quoteAmount":1.5,"quoteInPool":7.9,"price":5.0e-7,"marketCapQuote":50.0,"pool":"pump","timestamp":1782259200045}"#,
        "\n",
    );

    fn keep_all() -> HashSet<EventKind> {
        HashSet::from([
            EventKind::Create,
            EventKind::CreatePool,
            EventKind::Buy,
            EventKind::Sell,
            EventKind::Migrate,
        ])
    }

    #[test]
    fn parses_and_filters() {
        let (events, metas, stats) =
            parse_stream(SAMPLE.as_bytes(), &DexFilter::all(), &keep_all()).unwrap();
        assert_eq!(events.len(), 3); // transfer skipped, both schemas kept
        assert_eq!(stats.lines, 4);
        assert_eq!(stats.pool_lines, 3); // transfer line has no top-level "pool"
        assert_eq!(stats.parse_errors, 0);
        assert_eq!(stats.kept, 3);
        assert_eq!(events[0].kind, EventKind::Create);
        assert_eq!(events[0].dex, Dex::Pump);
        assert_eq!(events[1].kind, EventKind::Buy);
        assert_eq!(events[1].dex, Dex::PumpSwap);
        let m = metas.get("MINTpump").unwrap();
        assert_eq!(m.symbol.as_deref(), Some("AST"));
    }

    #[test]
    fn parses_new_schema_via_aliases() {
        let (events, _, _) =
            parse_stream(SAMPLE.as_bytes(), &DexFilter::all(), &keep_all()).unwrap();
        let e = events.iter().find(|e| e.mint == "MINTnew").unwrap();
        assert_eq!(e.kind, EventKind::Sell); // from "action"
        assert_eq!(e.sol_amount, Some(1.5)); // from "quoteAmount"
        assert_eq!(e.sol_in_pool, Some(7.9)); // from "quoteInPool"
        assert_eq!(e.market_cap_sol, Some(50.0)); // from "marketCapQuote"
        assert!(e.is_sol_quoted());
    }

    #[test]
    fn dex_filter_applies() {
        let (events, _, _) =
            parse_stream(SAMPLE.as_bytes(), &DexFilter::only(Dex::Pump), &keep_all()).unwrap();
        // pump create (s2) + new-schema pump sell (s4) match; pump-amm buy filtered.
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.pool == "pump"));
    }

    #[test]
    fn unknown_schema_is_counted_not_silently_dropped() {
        // Simulate a future rename where the action field is unknown: lines still
        // have "pool" but fail to deserialize (missing the required type field).
        let lines: String = (0..200)
            .map(|i| format!(r#"{{"signature":"x{i}","pool":"pump","mint":"m{i}","timestamp":1}}"#))
            .collect::<Vec<_>>()
            .join("\n");
        let (events, _, stats) =
            parse_stream(lines.as_bytes(), &DexFilter::all(), &keep_all()).unwrap();
        assert_eq!(events.len(), 0);
        assert_eq!(stats.pool_lines, 200);
        assert_eq!(stats.parse_errors, 200);
        assert!(stats.looks_like_schema_break());
    }
}
