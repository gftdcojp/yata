#![allow(dead_code)]

//! CBOR codec utilities.
//!
//! Thin wrapper around `ciborium` providing encode/decode helpers and CID generation.
//!
//! NOTE: Authoritative source is `wproto::cbor`. This crate retains its own
//! implementation for backward compatibility (no circular dependency).
//! New code should prefer `wproto::cbor` when available.

use yata_core::Blake3Hash;

#[derive(thiserror::Error, Debug)]
pub enum CborError {
    #[error("cbor encode error: {0}")]
    Encode(String),
    #[error("cbor decode error: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, CborError>;

pub fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    ciborium::into_writer(v, &mut buf).map_err(|e| CborError::Encode(e.to_string()))?;
    Ok(buf)
}

pub fn decode<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
    ciborium::from_reader(std::io::Cursor::new(data)).map_err(|e| CborError::Decode(e.to_string()))
}

pub fn cbor_cid<T: serde::Serialize>(v: &T) -> Result<Blake3Hash> {
    let bytes = encode(v)?;
    Ok(Blake3Hash::of(&bytes))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CborBytes(pub Vec<u8>);

impl CborBytes {
    pub fn encode<T: serde::Serialize>(v: &T) -> Result<Self> {
        encode(v).map(Self)
    }

    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        decode(&self.0)
    }

    pub fn cid(&self) -> Blake3Hash {
        Blake3Hash::of(&self.0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for CborBytes {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<CborBytes> for Vec<u8> {
    fn from(c: CborBytes) -> Self {
        c.0
    }
}
