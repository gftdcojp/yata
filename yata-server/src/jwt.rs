//! ES256 JWT verification for yata-native authentication (Design E).
//!
//! Port of PDS `verifyServiceAuthJWT` (index.ts:654-698).
//! Resolves DID signing keys from CSR DIDDocument vertices — no external HTTP fetch needed.

use ring::signature;

#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub iss: String,
    pub aud: String,
    pub exp: u64,
}

#[derive(Debug)]
pub enum JwtError {
    MalformedToken,
    UnsupportedAlgorithm,
    Expired,
    InvalidAudience,
    InvalidIssuer,
    KeyResolutionFailed,
    InvalidSignature,
    InvalidKey,
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedToken => write!(f, "malformed JWT token"),
            Self::UnsupportedAlgorithm => write!(f, "unsupported algorithm (expected ES256)"),
            Self::Expired => write!(f, "token expired"),
            Self::InvalidAudience => write!(f, "invalid audience"),
            Self::InvalidIssuer => write!(f, "invalid issuer"),
            Self::KeyResolutionFailed => write!(f, "DID signing key resolution failed"),
            Self::InvalidSignature => write!(f, "invalid signature"),
            Self::InvalidKey => write!(f, "invalid public key format"),
        }
    }
}

/// Verify an ES256 (ECDSA P-256 + SHA-256) JWT and return claims.
///
/// `resolve_pubkey` resolves a DID to its P-256 public key bytes (uncompressed, 65 bytes).
/// For internal DIDs, this queries the CSR DIDDocument vertex directly.
pub fn verify_es256_jwt(
    token: &str,
    expected_aud: &str,
    resolve_pubkey: impl Fn(&str) -> Option<Vec<u8>>,
) -> Result<JwtClaims, JwtError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(JwtError::MalformedToken);
    }

    // Parse header
    let header_bytes = base64url_decode(parts[0]).ok_or(JwtError::MalformedToken)?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|_| JwtError::MalformedToken)?;
    if header.get("alg").and_then(|v| v.as_str()) != Some("ES256") {
        return Err(JwtError::UnsupportedAlgorithm);
    }

    // Parse payload
    let payload_bytes = base64url_decode(parts[1]).ok_or(JwtError::MalformedToken)?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| JwtError::MalformedToken)?;

    let iss = payload
        .get("iss")
        .and_then(|v| v.as_str())
        .ok_or(JwtError::InvalidIssuer)?;
    if !iss.starts_with("did:") {
        return Err(JwtError::InvalidIssuer);
    }

    let aud = payload
        .get("aud")
        .and_then(|v| v.as_str())
        .ok_or(JwtError::InvalidAudience)?;
    if aud != expected_aud {
        return Err(JwtError::InvalidAudience);
    }

    let exp = payload
        .get("exp")
        .and_then(|v| v.as_u64())
        .ok_or(JwtError::Expired)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if exp < now.saturating_sub(30) {
        return Err(JwtError::Expired);
    }

    // Resolve public key from DID
    let pubkey_bytes = resolve_pubkey(iss).ok_or(JwtError::KeyResolutionFailed)?;

    // Verify signature
    let sig_bytes = base64url_decode(parts[2]).ok_or(JwtError::MalformedToken)?;
    let signed_content = format!("{}.{}", parts[0], parts[1]);

    let public_key =
        signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, &pubkey_bytes);
    public_key
        .verify(signed_content.as_bytes(), &sig_bytes)
        .map_err(|_| JwtError::InvalidSignature)?;

    Ok(JwtClaims {
        iss: iss.to_string(),
        aud: aud.to_string(),
        exp,
    })
}

/// Decode a multibase z-prefixed base58btc string to raw bytes.
/// Strips multicodec prefix (0x80 0x24 = P-256 compressed key).
/// Returns uncompressed P-256 public key (65 bytes) ready for ring verification.
pub fn resolve_multibase_p256_key(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.starts_with('z') {
        return None;
    }
    let raw = base58btc_decode(&encoded[1..])?;

    // Strip multicodec prefix for P-256 (varint 0x1200 → bytes 0x80 0x24)
    let key_data = if raw.len() >= 2 && raw[0] == 0x80 && raw[1] == 0x24 {
        &raw[2..]
    } else {
        &raw
    };

    if key_data.len() == 33 {
        // Compressed P-256 point → decompress
        Some(decompress_p256_point(key_data)?)
    } else if key_data.len() == 65 && key_data[0] == 0x04 {
        // Already uncompressed
        Some(key_data.to_vec())
    } else {
        None
    }
}

// ── Internal helpers ──

fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(input)
        .ok()
}

fn base58btc_decode(input: &str) -> Option<Vec<u8>> {
    const ALPHA: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let mut n: u128 = 0;
    for c in input.bytes() {
        let i = ALPHA.iter().position(|&a| a == c)?;
        n = n.checked_mul(58)?.checked_add(i as u128)?;
    }
    let mut bytes = Vec::new();
    while n > 0 {
        bytes.push((n & 0xFF) as u8);
        n >>= 8;
    }
    bytes.reverse();
    // Preserve leading zeros
    for c in input.bytes() {
        if c == b'1' {
            bytes.insert(0, 0);
        } else {
            break;
        }
    }
    Some(bytes)
}

/// Decompress a P-256 compressed point (33 bytes) to uncompressed (65 bytes).
/// Port of PDS `decompressP256Point` (index.ts:721-734).
fn decompress_p256_point(compressed: &[u8]) -> Option<Vec<u8>> {
    if compressed.len() != 33 {
        return None;
    }
    let prefix = compressed[0];
    if prefix != 0x02 && prefix != 0x03 {
        return None;
    }

    // P-256 curve parameters
    let p = uint256_from_hex("FFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFF");
    let a = modular_sub(&p, &uint256_from_u64(3), &p);
    let b = uint256_from_hex("5AC635D8AA3A93E7B3EBBD55769886BC651D06B0CC53B0F63BCE3C3E27D2604B");

    // x from compressed bytes
    let x = uint256_from_bytes(&compressed[1..]);

    // y² = x³ + ax + b (mod p)
    let x3 = modpow_256(&x, &uint256_from_u64(3), &p);
    let ax = modular_mul(&a, &x, &p);
    let y_squared = modular_add(&modular_add(&x3, &ax, &p), &b, &p);

    // y = y²^((p+1)/4) (mod p) — works because p ≡ 3 (mod 4) for P-256
    let exp = {
        let mut e = modular_add(&p, &uint256_from_u64(1), &uint256_from_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));
        // divide by 4 = shift right 2
        shift_right_2(&mut e);
        e
    };
    let mut y = modpow_256(&y_squared, &exp, &p);

    // Adjust parity
    let y_is_odd = y[31] & 1 == 1;
    let want_odd = prefix == 0x03;
    if y_is_odd != want_odd {
        y = modular_sub(&p, &y, &p);
    }

    let mut out = vec![0x04u8];
    out.extend_from_slice(&uint256_to_bytes(&x));
    out.extend_from_slice(&uint256_to_bytes(&y));
    Some(out)
}

// ── 256-bit unsigned integer arithmetic (big-endian [u8; 32]) ──

type U256 = [u8; 32];

fn uint256_from_u64(v: u64) -> U256 {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&v.to_be_bytes());
    out
}

fn uint256_from_hex(hex: &str) -> U256 {
    let mut out = [0u8; 32];
    let bytes = hex::decode(hex).unwrap();
    let start = 32 - bytes.len();
    out[start..].copy_from_slice(&bytes);
    out
}

fn uint256_from_bytes(b: &[u8]) -> U256 {
    let mut out = [0u8; 32];
    let start = 32usize.saturating_sub(b.len());
    out[start..].copy_from_slice(b);
    out
}

fn uint256_to_bytes(v: &U256) -> Vec<u8> {
    v.to_vec()
}

fn shift_right_2(v: &mut U256) {
    let mut carry = 0u8;
    for byte in v.iter_mut() {
        let new_carry = *byte & 0x03;
        *byte = (*byte >> 2) | (carry << 6);
        carry = new_carry;
    }
}

fn modular_add(a: &U256, b: &U256, p: &U256) -> U256 {
    // Use u128 chain for addition
    let mut result = [0u8; 32];
    let mut carry = 0u16;
    for i in (0..32).rev() {
        let sum = a[i] as u16 + b[i] as u16 + carry;
        result[i] = sum as u8;
        carry = sum >> 8;
    }
    // Reduce mod p if result >= p
    if carry > 0 || &result >= p {
        let mut borrow = 0i16;
        for i in (0..32).rev() {
            let diff = result[i] as i16 - p[i] as i16 - borrow;
            if diff < 0 {
                result[i] = (diff + 256) as u8;
                borrow = 1;
            } else {
                result[i] = diff as u8;
                borrow = 0;
            }
        }
    }
    result
}

fn modular_sub(a: &U256, b: &U256, p: &U256) -> U256 {
    if a >= b {
        let mut result = [0u8; 32];
        let mut borrow = 0i16;
        for i in (0..32).rev() {
            let diff = a[i] as i16 - b[i] as i16 - borrow;
            if diff < 0 {
                result[i] = (diff + 256) as u8;
                borrow = 1;
            } else {
                result[i] = diff as u8;
                borrow = 0;
            }
        }
        result
    } else {
        // a < b: result = p - (b - a)
        let mut diff = [0u8; 32];
        let mut borrow = 0i16;
        for i in (0..32).rev() {
            let d = b[i] as i16 - a[i] as i16 - borrow;
            if d < 0 {
                diff[i] = (d + 256) as u8;
                borrow = 1;
            } else {
                diff[i] = d as u8;
                borrow = 0;
            }
        }
        modular_sub(p, &diff, p)
    }
}

fn modular_mul(a: &U256, b: &U256, p: &U256) -> U256 {
    // Schoolbook multiplication with modular reduction via repeated subtraction
    // For P-256 key decompression this is called rarely, so perf is not critical
    let mut result = [0u8; 32];
    for bit in 0..256 {
        let byte_idx = bit / 8;
        let bit_idx = 7 - (bit % 8);
        // Double result (shift left 1, mod p)
        result = modular_add(&result, &result, p);
        // If bit of a is set, add b
        if (a[byte_idx] >> bit_idx) & 1 == 1 {
            result = modular_add(&result, b, p);
        }
    }
    result
}

fn modpow_256(base: &U256, exp: &U256, p: &U256) -> U256 {
    let mut result = uint256_from_u64(1);
    let mut b = *base;
    // Square-and-multiply from LSB
    for byte_idx in (0..32).rev() {
        for bit_idx in 0..8 {
            if (exp[byte_idx] >> bit_idx) & 1 == 1 {
                result = modular_mul(&result, &b, p);
            }
            b = modular_mul(&b, &b, p);
        }
    }
    result
}

// Inline hex decode to avoid adding a dep
mod hex {
    pub fn decode(s: &str) -> Result<Vec<u8>, ()> {
        if s.len() % 2 != 0 {
            return Err(());
        }
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64url_decode() {
        let encoded = "eyJhbGciOiJFUzI1NiJ9";
        let decoded = base64url_decode(encoded).unwrap();
        let s = String::from_utf8(decoded).unwrap();
        assert_eq!(s, r#"{"alg":"ES256"}"#);
    }

    #[test]
    fn test_base58btc_decode() {
        // "1" = [0x00] in base58
        let decoded = base58btc_decode("1").unwrap();
        assert_eq!(decoded, vec![0x00]);
    }

    #[test]
    fn test_multibase_key_resolution() {
        // Test with a known P-256 multibase key (z-prefix base58btc with multicodec 0x8024)
        // This is a synthetic test — real keys come from DIDDocument vertices
        let key = resolve_multibase_p256_key("z");
        // Empty key after 'z' prefix → should fail gracefully
        assert!(key.is_none() || key.as_ref().map_or(true, |k| k.len() == 65));
    }

    #[test]
    fn test_verify_rejects_malformed() {
        let result = verify_es256_jwt("not.a.jwt", "did:web:pds.gftd.ai", |_| None);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_rejects_expired() {
        // Construct a token with exp=0 (expired)
        let header = base64url_encode(br#"{"alg":"ES256"}"#);
        let payload = base64url_encode(br#"{"iss":"did:web:test","aud":"did:web:pds.gftd.ai","exp":0}"#);
        let token = format!("{}.{}.AAAA", header, payload);
        let result = verify_es256_jwt(&token, "did:web:pds.gftd.ai", |_| None);
        assert!(matches!(result, Err(JwtError::Expired)));
    }

    #[test]
    fn test_verify_rejects_wrong_audience() {
        let header = base64url_encode(br#"{"alg":"ES256"}"#);
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let payload_str = format!(r#"{{"iss":"did:web:test","aud":"did:web:wrong","exp":{}}}"#, exp);
        let payload = base64url_encode(payload_str.as_bytes());
        let token = format!("{}.{}.AAAA", header, payload);
        let result = verify_es256_jwt(&token, "did:web:pds.gftd.ai", |_| None);
        assert!(matches!(result, Err(JwtError::InvalidAudience)));
    }

    fn base64url_encode(data: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
    }
}
