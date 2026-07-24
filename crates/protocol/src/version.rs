#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V1,
}

impl Version {
    pub fn as_u8(self) -> u8 {
        match self {
            Version::V1 => 1,
        }
    }
}

impl TryFrom<u8> for Version {
    type Error = crate::error::ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Version::V1),
            _ => Err(crate::error::ProtocolError::InvalidVersion),
        }
    }
}
