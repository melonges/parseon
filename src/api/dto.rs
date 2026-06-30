use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ----- Monitors -----

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateMonitor {
    pub address: String,
    /// Optional human-readable label. Defaults to `{address}_{selector}` when
    /// omitted.
    #[serde(default)]
    pub name: Option<String>,
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
