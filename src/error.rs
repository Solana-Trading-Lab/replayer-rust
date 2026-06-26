//! Error type for the replayer.

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// The requested hour file does not exist in the archive (HTTP 404).
    #[error("hour file not found in archive: {0}")]
    NotFound(String),

    /// No part of the requested period can be served from the archive.
    #[error("no data available: {0}")]
    NoData(String),

    /// The configuration is invalid (e.g. window_hours == 0, start > end).
    #[error("invalid config: {0}")]
    Config(String),
}
