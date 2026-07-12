use parseon_core::{Address, B256, MonitorId, Selector};

pub(crate) fn to_i64(value: u64, field: &str) -> anyhow::Result<i64> {
    i64::try_from(value).map_err(|_| anyhow::anyhow!("{field} exceeds PostgreSQL BIGINT range"))
}

pub(crate) fn from_i64(value: i64, field: &str) -> anyhow::Result<u64> {
    u64::try_from(value).map_err(|_| anyhow::anyhow!("stored {field} must be non-negative"))
}

pub(crate) fn to_monitor_id(value: MonitorId) -> anyhow::Result<i64> {
    to_i64(value.get(), "monitor id")
}

pub(crate) fn from_monitor_id(value: i64) -> anyhow::Result<MonitorId> {
    MonitorId::new(from_i64(value, "monitor id")?)
}

pub(crate) fn address(value: &[u8]) -> anyhow::Result<Address> {
    let bytes: [u8; 20] = value
        .try_into()
        .map_err(|_| anyhow::anyhow!("stored address must contain exactly 20 bytes"))?;
    Ok(Address::from(bytes))
}

pub(crate) fn selector(value: &[u8]) -> anyhow::Result<Selector> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| anyhow::anyhow!("stored selector must contain exactly 4 bytes"))?;
    Ok(Selector::from(bytes))
}

pub(crate) fn b256(value: &[u8], field: &str) -> anyhow::Result<B256> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| anyhow::anyhow!("stored {field} must contain exactly 32 bytes"))?;
    Ok(B256::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_postgres_integer_boundaries() {
        assert_eq!(to_i64(i64::MAX as u64, "value").unwrap(), i64::MAX);
        assert!(to_i64(i64::MAX as u64 + 1, "value").is_err());
        assert_eq!(from_i64(0, "value").unwrap(), 0);
        assert!(from_i64(-1, "value").is_err());
    }

    #[test]
    fn checks_fixed_byte_lengths() {
        assert!(address(&[0; 20]).is_ok());
        assert!(address(&[0; 19]).is_err());
        assert!(selector(&[0; 4]).is_ok());
        assert!(selector(&[0; 5]).is_err());
        assert!(b256(&[0; 32], "hash").is_ok());
        assert!(b256(&[0; 31], "hash").is_err());
    }
}
