use crate::core::DecodedCall;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Filter {
    #[default]
    All,
}

impl Filter {
    pub fn matches(self, _call: &DecodedCall) -> bool {
        match self {
            Self::All => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;

    #[test]
    fn default_filter_allows_calls() {
        assert_eq!(Filter::default(), Filter::All);
    }
}
