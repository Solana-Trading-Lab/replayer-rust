//! DEX identification and filtering.
//!
//! Every tradable event in the replay stream carries a `pool` string. Observed
//! values: `pump`, `pump-amm`, `raydium-cpmm`, `raydium-launchpad`,
//! `meteora-damm-v1`, `meteora-damm-v2`, `meteora-launchpad`. We group those raw
//! pool strings into [`Dex`] families so callers can ask for "pump", "pumpswap"
//! or "raydium" tapes without memorising pool program names.

use std::collections::HashSet;

use serde::Serialize;

/// A DEX family. The raw pool string is preserved on each event for fine-grained
/// filtering; [`Dex`] is the coarse grouping most callers want.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Dex {
    /// pump.fun bonding curve (`pool == "pump"`).
    Pump,
    /// PumpSwap AMM, where pump tokens trade post-migration (`pool == "pump-amm"`).
    PumpSwap,
    /// Any Raydium pool (`pool` starts with `raydium`).
    Raydium,
    /// Any Meteora pool (`pool` starts with `meteora`).
    Meteora,
    /// Anything not recognised above.
    Other,
}

impl Dex {
    /// Map a raw `pool` string to its [`Dex`] family.
    pub fn of_pool(pool: &str) -> Dex {
        match pool {
            "pump" => Dex::Pump,
            "pump-amm" => Dex::PumpSwap,
            p if p.starts_with("raydium") => Dex::Raydium,
            p if p.starts_with("meteora") => Dex::Meteora,
            _ => Dex::Other,
        }
    }

    /// Parse a human-friendly DEX name. Accepts the aliases a caller is likely to
    /// type (`"pumpswap"`, `"pump-amm"`, `"pump-swap"`, `"ray"`, ...).
    pub fn parse(name: &str) -> Option<Dex> {
        match name.trim().to_ascii_lowercase().as_str() {
            "pump" | "pumpfun" | "pump.fun" | "pump-bonding" => Some(Dex::Pump),
            "pumpswap" | "pump-swap" | "pump-amm" | "pumpamm" => Some(Dex::PumpSwap),
            "raydium" | "ray" => Some(Dex::Raydium),
            "meteora" | "met" => Some(Dex::Meteora),
            "other" => Some(Dex::Other),
            _ => None,
        }
    }
}

/// Selects which trades end up in a tape.
///
/// A pool matches when (a) it belongs to one of the selected [`Dex`] families
/// (or any family if none were selected) **and** (b) it is in the exact-pool
/// allowlist (or the allowlist is unset). Use [`DexFilter::all`] to keep
/// everything.
#[derive(Clone, Debug, Default)]
pub struct DexFilter {
    dexes: Option<HashSet<Dex>>,
    pools: Option<HashSet<String>>,
}

impl DexFilter {
    /// Match every pool / DEX.
    pub fn all() -> Self {
        DexFilter::default()
    }

    /// Match a single DEX family.
    pub fn only(dex: Dex) -> Self {
        DexFilter {
            dexes: Some(HashSet::from([dex])),
            pools: None,
        }
    }

    /// Match any of the given DEX families.
    pub fn dexes<I: IntoIterator<Item = Dex>>(it: I) -> Self {
        DexFilter {
            dexes: Some(it.into_iter().collect()),
            pools: None,
        }
    }

    /// Restrict to exact raw `pool` strings (e.g. only `"raydium-cpmm"`).
    pub fn with_pools<I, S>(mut self, it: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.pools = Some(it.into_iter().map(Into::into).collect());
        self
    }

    /// True if no constraints are set (matches everything).
    pub fn is_unrestricted(&self) -> bool {
        self.dexes.is_none() && self.pools.is_none()
    }

    /// Does this raw pool string pass the filter?
    pub fn matches(&self, pool: &str) -> bool {
        if let Some(pools) = &self.pools {
            if !pools.contains(pool) {
                return false;
            }
        }
        if let Some(dexes) = &self.dexes {
            if !dexes.contains(&Dex::of_pool(pool)) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_mapping() {
        assert_eq!(Dex::of_pool("pump"), Dex::Pump);
        assert_eq!(Dex::of_pool("pump-amm"), Dex::PumpSwap);
        assert_eq!(Dex::of_pool("raydium-cpmm"), Dex::Raydium);
        assert_eq!(Dex::of_pool("raydium-launchpad"), Dex::Raydium);
        assert_eq!(Dex::of_pool("meteora-damm-v2"), Dex::Meteora);
        assert_eq!(Dex::of_pool("something-else"), Dex::Other);
    }

    #[test]
    fn filter_logic() {
        let f = DexFilter::only(Dex::Pump);
        assert!(f.matches("pump"));
        assert!(!f.matches("pump-amm"));

        let f = DexFilter::dexes([Dex::Pump, Dex::PumpSwap]);
        assert!(f.matches("pump"));
        assert!(f.matches("pump-amm"));
        assert!(!f.matches("raydium-cpmm"));

        let f = DexFilter::all();
        assert!(f.matches("anything"));

        let f = DexFilter::dexes([Dex::Raydium]).with_pools(["raydium-cpmm"]);
        assert!(f.matches("raydium-cpmm"));
        assert!(!f.matches("raydium-launchpad")); // excluded by pool allowlist
    }
}
