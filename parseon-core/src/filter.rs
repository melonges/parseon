use super::DecodedCall;

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
