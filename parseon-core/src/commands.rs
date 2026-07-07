#[derive(Debug, Clone)]
pub struct CreateChain {
    pub rpc_url: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateChain {
    pub rpc_url: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CreateMonitor {
    pub chain_id: i64,
    pub address: String,
    pub signature: String,
    pub start_block: i64,
    pub end_block: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateMonitor {
    pub start_block: Option<i64>,
    pub end_block: Option<Option<i64>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
pub struct ResultQuery {
    pub limit: i64,
    pub offset: i64,
}
