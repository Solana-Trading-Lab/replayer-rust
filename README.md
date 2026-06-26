# pump-replayer

A small, embeddable Rust module that turns **PumpApi historical replay data** into
per-token **trade tapes** for a chosen DEX, period and tape window — while keeping
**at most a sliding window of hours on disk**, so you never need room for the full
(~2 GB/hour decompressed) archive.

Built against the live archive at <https://replay.pumpapi.io> and verified
end-to-end against real data.

## What it does

- **Downloads** hourly replay files `https://replay.pumpapi.io/YYYY/MM/DD/HH.jsonl.zst`
  (no auth required).
- **Stream-decompresses** zstd and parses JSONL without ever writing the ~2 GB
  decompressed form to disk. `transfer` events (≈half of all lines) are rejected
  before JSON parsing.
- **Builds token tapes**: for every token *born* (a `create` / `createPool` /
  `migrate` event) in an anchor hour, it collects that token's trades over the
  next `W` hours, on the DEX you selected.
- **Steps with bounded storage**: keeps only `W` hours live, dropping (and
  deleting) the oldest as it advances.
- **Checks availability & alerts**: lists the archive, clamps the period to what
  exists (data began **2026-04-18 00:00 UTC**), and warns instead of failing.

## Core concepts

| Term | Meaning |
|------|---------|
| **Token tape** | The chronological trades for one token over a fixed window of `W` hours, anchored on the hour the token was *born* on the selected DEX. |
| **Tape window `W`** | How many hours forward each tape spans (`window_hours`). |
| **Period `[a, b]`** | The inclusive range of *birth hours* to produce tapes for. |
| **DEX** | `pump` (bonding curve), `pumpswap` (`pump-amm`), `raydium`, `meteora` — grouped from the raw `pool` field. |

Serving period `[a, b]` with window `W` needs raw data for hours
`a ..= b + (W − 1)`: the look-ahead hours after `b` let a token born in hour `b`
accumulate its full `W`-hour tape. Hours beyond the archive are clamped away with
a warning.

## The step-wise workflow

For `W = 3` and period `[1, N]`:

```
load hours 1,2,3   → emit tapes born in hour 1   → drop+delete hour 1
load hour 4        → emit tapes born in hour 2   → drop+delete hour 2
load hour 5        → emit tapes born in hour 3   → drop+delete hour 3
...
```

Each step downloads exactly **one** new hour and deletes exactly one, so disk
holds at most `W` compressed files and RAM holds at most `W` hours of *filtered*
events (far smaller than the raw archive).

## Usage

```rust
use std::path::PathBuf;
use pump_replayer::{Dex, DexFilter, Hour, HourRange, ReplayConfig, Replayer};

let period = HourRange::new(
    Hour::from_ymdh(2026, 4, 18, 0).unwrap(),
    Hour::from_ymdh(2026, 4, 18, 5).unwrap(),
);

let cfg = ReplayConfig::new(
    period,
    3,                              // 3-hour tape window
    DexFilter::only(Dex::Pump),     // pump.fun launches only
    PathBuf::from("./replay_cache"),
);

let mut replayer = Replayer::new(cfg)?;

// Optional: inspect availability / clamping before the heavy work.
let plan = replayer.plan()?;
for w in &plan.warnings { eprintln!("WARNING: {w}"); }

// Drive it; the callback is invoked once per anchor hour with that hour's tapes.
let report = replayer.run(|step| {
    for tape in &step.tapes {
        // persist however your bigger project wants — tape is Serialize
        println!("{} {} trades, {:.2} SOL volume",
                 tape.mint, tape.events.len(), tape.volume_sol());
    }
    Ok(())
})?;
println!("{} steps, {} tapes", report.steps_emitted, report.tapes_total);
```

### Storing results token-by-token, by day

To persist each token tape as its own file under a per-day folder, use the
built-in [`TokenTapeWriter`] (or the `run_to_dir` shortcut):

```rust
// Shortcut: writes <root>/<YYYY-MM-DD>/<mint>.json for every tape.
replayer.run_to_dir("./tapes")?;

// Or drive the writer yourself (e.g. to also log / index per step):
use pump_replayer::TokenTapeWriter;
let writer = TokenTapeWriter::new("./tapes").pretty(true);
replayer.run(|step| { writer.write_step(step)?; Ok(()) })?;
```

Resulting layout (the day is the token's birth / anchor hour's date):

```text
tapes/
  2026-06-24/
    <mint-a>.json      # full TokenTape: birth, meta, chronological trades
    <mint-b>.json
  2026-06-25/
    <mint-c>.json
```

Each token is born in exactly one anchor hour, so within a run (fixed DEX
filter) each `<mint>.json` is written once. Files are flushed per anchor hour, so
the bounded-storage stepping is preserved — tapes are never all held in memory at
once.

### Configuration knobs

`ReplayConfig::new(period, window_hours, dex, work_dir)` then chain:

- `.base_url(url)` — point at a mirror (default `https://replay.pumpapi.io`).
- `.request_timeout(Duration)` — per-request timeout (default 300s).
- `.birth_kinds([..])` — what counts as a token birth (default `Create`,
  `CreatePool`, `Migrate`). E.g. for PumpSwap-only tapes, births come from
  `Migrate` (pump tokens migrating to `pump-amm`).
- `.include_kinds([..])` — which event kinds enter the tape (default `Buy`,
  `Sell`; add `Add`/`Remove` for liquidity events).
- `.keep_files(true)` — keep `.zst` files after they slide out (default `false`
  = delete to bound disk usage).
- `.exclude_mayhem(false)` — keep pump.fun "mayhem mode" tokens (default `true`
  = drop any token whose birth event has `mayhemMode: true`).
- `.keep_raw(false)` — drop the full per-transaction JSON (default `true` = keep
  the complete original event in `TapeEvent.raw`, so no tx info is lost; set
  `false` to lower memory when only the typed fields are needed).

`DexFilter`: `DexFilter::only(Dex::Pump)`, `DexFilter::dexes([Dex::Pump, Dex::PumpSwap])`,
`DexFilter::all()`, or `.with_pools(["raydium-cpmm"])` for exact raw pool strings.

## Example CLI

```bash
RUST_LOG=info cargo run --example run -- \
  --start 2026-06-24T00 --end 2026-06-24T05 \
  --window 3 --dex pump --out ./tapes
```

Writes one token tape per file under `--out/<YYYY-MM-DD>/<mint>.json` and prints a
summary. `--dex` accepts `pump`, `pumpswap`, `raydium`, `meteora` (omit for all).
Extra flags: `--include-mayhem` (keep mayhem-mode tokens), `--no-raw` (drop the
full per-tx JSON), `--cache <dir>` (where to put the transient `.zst` files).

## Output types

`TapeStep { anchor_hour, window_hours, tapes: Vec<TokenTape> }` where each
`TokenTape` has `mint`, `dex`, `mayhem_mode`, `birth` (the anchoring event),
`meta` (name/symbol/uri/supply if a create supplied them), `window_start_ms`,
`window_end_ms`, and `events` (chronological trades). Helpers: `num_buys()`,
`num_sells()`, `volume_sol()`, `last_price()`. All output types are `Serialize`.

Each `TapeEvent` keeps the typed fields (signature, kind, pool, amounts, price,
`block`, `timestamp_ms`, `mayhem_mode`, …) **plus** `raw` — the complete original
JSON object for that transaction (`priorityFee`, `postBalances`,
`tradersInvolved`, `tokenProgram`, bonding-curve reserves, etc.), so the full tx
record is available downstream. `raw` is present only when `keep_raw` is on
(default) and is omitted from the JSON when empty.

## Event schema reference

Replay lines match the live data stream. Fields the module reads:

- action: `transfer` (ignored), `create`, `createPool`, `buy`, `sell`, `add`,
  `remove`, `migrate` — read from `action` **or** the older `txType` (see below).
- `pool`: `pump`, `pump-amm`, `raydium-cpmm`, `raydium-launchpad`,
  `meteora-damm-v1`, `meteora-damm-v2`, `meteora-launchpad`.
- `mint`, `quoteMint`, `poolId`, `txSigner`, `tokenAmount`, quote amount, `price`,
  market-cap, `tokensInPool`, quote-in-pool, `timestamp` (ms), `localTimestamp`
  (ms, replay only), `block`, and create metadata `name`/`symbol`/`uri`/
  `supply`/`decimals`/`initialBuy`.

> **Schema versions:** PumpApi renamed fields mid-2026. Older files use
> `txType` / `solAmount` / `solInPool` / `marketCapSol`; newer files use
> `action` / `quoteAmount` / `quoteInPool` / `marketCapQuote` (+ `quoteMint`).
> The parser reads both via serde aliases, so a single run can span the rename.
> `TapeEvent.sol_amount` is the quote amount — SOL for SOL-quoted pools; call
> `TapeEvent::is_sol_quoted()` to check.

> Note: the file `HH.jsonl.zst` contains events in `[HH:00, HH+1:00)` UTC
> (confirmed against real data; the published docs example is off by one).

## Tests

`cargo test` runs offline unit tests (time math, DEX mapping, JSONL parsing,
window/tape building, archive-key parsing). Network is only touched at run time.
