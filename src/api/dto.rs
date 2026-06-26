use serde::{Deserialize, Serialize};

// ----- Chains -----

#[derive(Debug, Deserialize)]
pub struct CreateChain {
    pub name: String,
    pub chain_id: i64,
    pub rpc_url: String,
    #[serde(default)]
    pub start_block: i64,
    #[serde(default = "default_poll")]
    pub poll_interval_ms: i32,
    #[serde(default = "default_batch")]
    pub batch_size: i32,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_poll() -> i32 {
    2000
}
fn default_batch() -> i32 {
    10
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct UpdateChain {
    pub enabled: Option<bool>,
}

// ----- Monitors -----

#[derive(Debug, Deserialize)]
pub struct CreateMonitor {
    pub chain_id: i64,
    pub address: String,
    /// Human-readable function signature, e.g.
    /// `function transfer(address to, uint256 value) returns (bool)`
    pub signature: String,
    pub start_block: Option<i64>,
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
    pub chains: usize,
    pub monitors: usize,
}
