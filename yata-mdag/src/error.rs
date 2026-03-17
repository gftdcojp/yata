#[derive(Debug, thiserror::Error)]
pub enum MdagError {
    #[error("CAS error: {0}")]
    Cas(String),
    #[error("CBOR error: {0}")]
    Cbor(String),
    #[error("block not found: {0}")]
    NotFound(String),
    #[error("deserialization error: {0}")]
    Deserialize(String),
}

pub type Result<T> = std::result::Result<T, MdagError>;

impl From<yata_cas::CasError> for MdagError {
    fn from(e: yata_cas::CasError) -> Self { Self::Cas(e.to_string()) }
}
impl From<yata_cbor::CborError> for MdagError {
    fn from(e: yata_cbor::CborError) -> Self { Self::Cbor(e.to_string()) }
}
