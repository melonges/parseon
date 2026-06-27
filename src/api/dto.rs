use serde::{Deserialize, Serialize};

// ----- Monitors -----

#[derive(Debug, Deserialize)]
pub struct CreateMonitor {
    pub address: String,
    /// Human-readable function signature, e.g.
    /// `function transfer(address to, uint256 value) returns (bool)`
    pub signature: String,
    #[serde(default)]
    pub start_block: Option<i64>,
    #[serde(default)]
    pub end_block: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMonitor {
    pub start_block: Option<i64>,
    /// `null` clears end_block (open-ended/live); a number sets a finite end.
    pub end_block: Option<Option<i64>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct Health {
    pub status: &'static str,
    pub monitors: usize,
}
