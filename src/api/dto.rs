use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ----- Monitors -----

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateMonitor {
    pub address: String,
    /// Human-readable function signature, e.g.
    /// `function transfer(address to, uint256 value) returns (bool)`
    pub signature: String,
    pub start_block: i64,
    #[serde(default)]
    pub end_block: Option<i64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateMonitor {
    pub start_block: Option<i64>,
    /// `null` clears end_block (open-ended/live); a number sets a finite end.
    pub end_block: Option<Option<i64>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Health {
    pub status: &'static str,
    pub monitors: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

// ----- Results search -----

#[derive(Debug, Deserialize, ToSchema)]
pub struct ResultsQuery {
    /// Maximum number of results (default 50, clamped to 200).
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Pagination offset (default 0).
    #[serde(default)]
    pub offset: i64,
    /// Filter by transaction sender (normalized, case-insensitive).
    pub from_addr: Option<String>,
    /// Filter by transaction status (1 = success, 0 = reverted).
    pub status: Option<i16>,
}

fn default_limit() -> i64 {
    50
}
