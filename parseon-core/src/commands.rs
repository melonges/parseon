//! Application-layer command and query DTOs.
//!
//! These types are the inputs to [`crate::services`]: HTTP handlers construct
//! them from request bodies and pass them to `ChainService` and
//! `MonitorService`. They are deliberately thin value objects with no
//! behavior — all validation lives in the services.

use crate::filter::{FilterExpression, FilterSample};
use crate::{Address, BlockNumber, ChainId, Url};

/// Command to register a new chain: its RPC URL and initial enabled state.
#[derive(Debug, Clone)]
pub struct CreateChain {
    /// Write-only RPC URL for the worker.
    pub rpc_url: Url,
    /// Whether the chain should start a worker on next startup.
    pub enabled: bool,
}

/// Command to update a chain's RPC URL and/or enabled state. Either field
/// may be `None` to leave it unchanged; at least one must be `Some`.
#[derive(Debug, Clone, Default)]
pub struct UpdateChain {
    /// New RPC URL, if changing.
    pub rpc_url: Option<Url>,
    /// New enabled state, if changing.
    pub enabled: Option<bool>,
}

/// Command to create a monitor.
#[derive(Debug, Clone)]
pub struct CreateMonitor {
    /// Chain the monitor belongs to.
    pub chain_id: ChainId,
    /// Contract address the monitor watches.
    pub address: Address,
    /// Human-readable Solidity function or event signature. Parsed once at
    /// creation; only the selector/topic0 is persisted.
    pub signature: String,
    /// First block to index (inclusive). If `None`, defaults to the chain's
    /// current finalized head at creation time.
    pub start_block: Option<BlockNumber>,
    /// Last block to index (inclusive), or `None` for live indexing.
    pub end_block: Option<BlockNumber>,
    /// Optional compiled filter expression.
    pub filter: Option<FilterExpression>,
}

/// Command to preview a filter against a sample decoded result without
/// creating a monitor.
#[derive(Debug, Clone)]
pub struct PreviewFilter {
    /// Human-readable Solidity signature for the target.
    pub signature: String,
    /// Filter expression to evaluate.
    pub filter: FilterExpression,
    /// Sample decoded result to evaluate against.
    pub sample: FilterSample,
}

/// Bounded page limit for result queries.
///
/// Clamped to `[1, 200]` so a malicious or accidental `limit=0` or
/// `limit=u64::MAX` cannot produce an empty or unbounded page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimit(u16);

impl PageLimit {
    /// Creates a page limit, clamping `value` to `[1, 200]`.
    pub fn new(value: u64) -> Self {
        Self(value.clamp(1, 200) as u16)
    }

    /// Returns the clamped limit.
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// A paginated result query: a bounded limit and an offset.
#[derive(Debug, Clone, Copy)]
pub struct ResultQuery {
    /// Maximum number of results to return.
    pub limit: PageLimit,
    /// Number of results to skip before the first returned result.
    pub offset: u64,
}

#[cfg(test)]
mod tests {
    use super::PageLimit;

    #[test]
    fn page_limit_is_always_bounded() {
        assert_eq!(PageLimit::new(0).get(), 1);
        assert_eq!(PageLimit::new(50).get(), 50);
        assert_eq!(PageLimit::new(u64::MAX).get(), 200);
    }
}
