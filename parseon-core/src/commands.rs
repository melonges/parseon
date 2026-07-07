use crate::{Address, BlockNumber, ChainId, Url};

#[derive(Debug, Clone)]
pub struct CreateChain {
    pub rpc_url: Url,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateChain {
    pub rpc_url: Option<Url>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CreateMonitor {
    pub chain_id: ChainId,
    pub address: Address,
    pub signature: String,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateMonitor {
    pub start_block: Option<BlockNumber>,
    pub end_block: Option<Option<BlockNumber>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimit(u16);

impl PageLimit {
    pub fn new(value: u64) -> Self {
        Self(value.clamp(1, 200) as u16)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResultQuery {
    pub limit: PageLimit,
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
