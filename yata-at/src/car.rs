//! CARv1 (Content Addressable aRchive) decoder for AT Protocol firehose blocks.
//!
//! CARv1 wire format:
//!   header : uvarint(len) || cbor({ version: 1, roots: [CID...] })
//!   blocks : repeated { uvarint(cid_len + data_len) || raw_cid_bytes || dag-cbor-bytes }
//!
//! CID bytes inside a block header are raw multibytes (no CBOR tag), starting with:
//!   uvarint(version=1) || uvarint(codec=0x71 dag-cbor) || multihash
//!
//! CIDs inside dag-cbor values use CBOR tag 42 with bytes `[0x00, ...raw_cid...]`.

use cid::Cid;

#[derive(thiserror::Error, Debug)]
pub enum CarError {
    #[error("truncated CARv1 data")]
    Truncated,
    #[error("invalid uvarint")]
    InvalidVarint,
    #[error("invalid CARv1 header CBOR: {0}")]
    HeaderCbor(String),
    #[error("invalid CID in block: {0}")]
    InvalidCid(#[from] cid::Error),
    #[error("CARv1 version {0} not supported (expected 1)")]
    UnsupportedVersion(u64),
}

/// A decoded block from a CARv1 archive.
#[derive(Debug, Clone)]
pub struct CarBlock {
    /// CIDv1 of this block (dag-cbor codec, sha2-256 multihash for AT Protocol).
    pub cid: Cid,
    /// Raw dag-cbor bytes of the block body.
    pub data: Vec<u8>,
}

/// Decoded CARv1 header.
#[derive(Debug, Clone)]
pub struct CarHeader {
    pub version: u64,
    pub roots: Vec<Cid>,
}

/// Decode a CARv1 byte slice into (header, blocks).
///
/// Returns `Ok((header, blocks))` on success. Non-fatal decode errors per-block
/// are silently skipped (e.g. unknown CID version) to be resilient to partial data.
pub fn decode_car(data: &[u8]) -> Result<(CarHeader, Vec<CarBlock>), CarError> {
    let (header_len, rest) = read_uvarint(data).ok_or(CarError::InvalidVarint)?;
    if rest.len() < header_len as usize {
        return Err(CarError::Truncated);
    }
    let (header_bytes, mut rest) = rest.split_at(header_len as usize);

    let header = decode_car_header(header_bytes)?;

    let mut blocks = Vec::new();
    while !rest.is_empty() {
        let (section_len, after_varint) = match read_uvarint(rest) {
            Some(v) => v,
            None => break,
        };
        if after_varint.len() < section_len as usize {
            break; // truncated — partial block, stop gracefully
        }
        let (section, remaining) = after_varint.split_at(section_len as usize);
        rest = remaining;

        // Parse CID from section start
        match Cid::read_bytes(std::io::Cursor::new(section)) {
            Ok(cid) => {
                let cid_len = cid_byte_len(&cid);
                if cid_len <= section.len() {
                    blocks.push(CarBlock { cid, data: section[cid_len..].to_vec() });
                }
            }
            Err(_) => continue, // skip malformed block
        }
    }

    Ok((header, blocks))
}

/// Find a block by CID string, returning its raw dag-cbor bytes.
pub fn find_block<'a>(blocks: &'a [CarBlock], cid_str: &str) -> Option<&'a [u8]> {
    blocks.iter()
        .find(|b| b.cid.to_string() == cid_str)
        .map(|b| b.data.as_slice())
}

// ── internal helpers ──────────────────────────────────────────────────────────

fn decode_car_header(bytes: &[u8]) -> Result<CarHeader, CarError> {
    use ciborium::value::Value;

    let val: Value = ciborium::from_reader(std::io::Cursor::new(bytes))
        .map_err(|e| CarError::HeaderCbor(e.to_string()))?;

    let map = match val {
        Value::Map(m) => m,
        _ => return Err(CarError::HeaderCbor("not a map".into())),
    };

    let version = map.iter().find_map(|(k, v)| {
        if let (Value::Text(key), Value::Integer(n)) = (k, v) {
            if key == "version" { return Some(i128::from(*n) as u64); }
        }
        None
    }).unwrap_or(0);

    if version != 1 {
        return Err(CarError::UnsupportedVersion(version));
    }

    let roots = map.iter().find_map(|(k, v)| {
        if let (Value::Text(key), Value::Array(arr)) = (k, v) {
            if key == "roots" {
                return Some(arr.iter().filter_map(|item| {
                    // roots are encoded as CBOR tag 42 with bytes [0x00, ...cid...]
                    let cid_bytes = extract_cid_bytes(item)?;
                    Cid::read_bytes(std::io::Cursor::new(cid_bytes)).ok()
                }).collect::<Vec<_>>());
            }
        }
        None
    }).unwrap_or_default();

    Ok(CarHeader { version, roots })
}

/// Extract CID bytes from a ciborium Value that is either:
/// - `Value::Tag(42, Box<Value::Bytes(b)>)` — standard dag-cbor CID link
/// - `Value::Bytes(b)` — raw bytes (fallback)
///
/// For tag-42, the first byte is the multibase identity prefix `0x00` and is stripped.
pub(crate) fn extract_cid_bytes(val: &ciborium::value::Value) -> Option<&[u8]> {
    use ciborium::value::Value;
    match val {
        Value::Tag(42, inner) => {
            if let Value::Bytes(b) = inner.as_ref() {
                // Strip multibase identity prefix 0x00
                Some(if b.first() == Some(&0x00) { &b[1..] } else { b.as_slice() })
            } else {
                None
            }
        }
        Value::Bytes(b) => Some(b.as_slice()),
        _ => None,
    }
}

/// Read an unsigned LEB128 varint from the start of `data`.
/// Returns `Some((value, remainder))` or `None` on overflow/underflow.
pub(crate) fn read_uvarint(data: &[u8]) -> Option<(u64, &[u8])> {
    let mut n: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 64 { return None; }
        n |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((n, &data[i + 1..]));
        }
        shift += 7;
    }
    None
}

/// Compute the byte length of a CID's raw binary representation.
fn cid_byte_len(cid: &Cid) -> usize {
    let mut buf = Vec::new();
    cid.write_bytes(&mut buf).unwrap_or(0);
    buf.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_uvarint_single_byte() {
        assert_eq!(read_uvarint(&[0x05, 0xff]), Some((5, &[0xff][..])));
    }

    #[test]
    fn test_read_uvarint_multibyte() {
        // 300 = 0b1_0010_1100 → [0xAC, 0x02]
        assert_eq!(read_uvarint(&[0xAC, 0x02]), Some((300, &[][..])));
    }

    #[test]
    fn test_decode_car_invalid_returns_error() {
        assert!(decode_car(&[0xff, 0xff, 0xff]).is_err());
    }
}
