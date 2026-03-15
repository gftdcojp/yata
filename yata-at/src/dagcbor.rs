//! dag-cbor to JSON converter for AT Protocol record blocks.
//!
//! dag-cbor is CBOR (RFC 7049) with one extension: CBOR tag 42 encodes a CID link.
//!   Tag 42 bytes layout: [0x00, ...raw_cid_bytes...]
//!   where raw_cid_bytes = uvarint(version=1) || uvarint(codec) || multihash
//!
//! CID links are serialised as `{ "/": "bafy..." }` (JSON-LD / AT Protocol convention).
//!
//! Float NaN/Inf are not valid in dag-cbor; we map them to `null`.

use cid::Cid;

#[derive(thiserror::Error, Debug)]
pub enum DagCborError {
    #[error("CBOR decode error: {0}")]
    Cbor(String),
}

/// Decode raw dag-cbor bytes into a `serde_json::Value`.
///
/// CID links (`{ "/": "bafy..." }`) are represented as JSON objects following
/// the AT Protocol / IPLD JSON convention so downstream consumers can detect them.
pub fn dagcbor_to_json(bytes: &[u8]) -> Result<serde_json::Value, DagCborError> {
    use ciborium::value::Value;
    let val: Value = ciborium::from_reader(std::io::Cursor::new(bytes))
        .map_err(|e| DagCborError::Cbor(e.to_string()))?;
    Ok(cbor_value_to_json(&val))
}

/// Recursively convert a `ciborium::value::Value` to `serde_json::Value`.
///
/// Handles CBOR tag 42 (CID link) by emitting `{ "/": "<cid-string>" }`.
pub fn cbor_value_to_json(val: &ciborium::value::Value) -> serde_json::Value {
    use ciborium::value::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => {
            let n = i128::from(*i);
            if let Ok(u) = u64::try_from(n) {
                serde_json::Value::Number(u.into())
            } else if let Ok(s) = i64::try_from(n) {
                serde_json::Value::Number(s.into())
            } else {
                serde_json::Value::String(n.to_string())
            }
        }
        Value::Float(f) => {
            match serde_json::Number::from_f64(*f) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::Null, // NaN / Inf are not valid JSON
            }
        }
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            // Raw bytes outside a CID tag: encode as `{ "$bytes": "<hex>" }`
            serde_json::json!({ "$bytes": hex::encode(b) })
        }
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(cbor_value_to_json).collect())
        }
        Value::Map(entries) => {
            let mut map = serde_json::Map::with_capacity(entries.len());
            for (k, v) in entries {
                let key = match k {
                    Value::Text(s) => s.clone(),
                    other => format!("{:?}", other),
                };
                map.insert(key, cbor_value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        // CBOR tag 42 = CID link (IPLD convention)
        Value::Tag(42, inner) => {
            let cid_str = try_decode_cid_tag(inner.as_ref())
                .unwrap_or_else(|| "<invalid-cid>".to_owned());
            serde_json::json!({ "/": cid_str })
        }
        // Other CBOR tags: unwrap and recurse
        Value::Tag(_, inner) => cbor_value_to_json(inner),
        _ => serde_json::Value::Null,
    }
}

/// Parse a CBOR tag-42 inner value into a CID string.
///
/// Tag-42 bytes: `[0x00, ...raw_cid_bytes...]`
/// The `0x00` is the multibase "identity" prefix (no encoding), stripped before CID parsing.
fn try_decode_cid_tag(val: &ciborium::value::Value) -> Option<String> {
    use ciborium::value::Value;
    let bytes = match val {
        Value::Bytes(b) => b.as_slice(),
        _ => return None,
    };
    let cid_bytes = if bytes.first() == Some(&0x00) { &bytes[1..] } else { bytes };
    Cid::read_bytes(std::io::Cursor::new(cid_bytes)).ok().map(|c| c.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciborium::value::Value;

    #[test]
    fn test_null() {
        assert_eq!(cbor_value_to_json(&Value::Null), serde_json::Value::Null);
    }

    #[test]
    fn test_bool() {
        assert_eq!(cbor_value_to_json(&Value::Bool(true)), serde_json::json!(true));
    }

    #[test]
    fn test_integer_positive() {
        assert_eq!(cbor_value_to_json(&Value::Integer(42.into())), serde_json::json!(42));
    }

    #[test]
    fn test_text() {
        assert_eq!(cbor_value_to_json(&Value::Text("hello".into())), serde_json::json!("hello"));
    }

    #[test]
    fn test_bytes_encodes_as_hex_object() {
        let v = cbor_value_to_json(&Value::Bytes(vec![0xde, 0xad]));
        assert_eq!(v, serde_json::json!({ "$bytes": "dead" }));
    }

    #[test]
    fn test_array() {
        let v = cbor_value_to_json(&Value::Array(vec![Value::Integer(1.into()), Value::Bool(false)]));
        assert_eq!(v, serde_json::json!([1, false]));
    }

    #[test]
    fn test_map() {
        let v = cbor_value_to_json(&Value::Map(vec![
            (Value::Text("key".into()), Value::Text("val".into())),
        ]));
        assert_eq!(v, serde_json::json!({ "key": "val" }));
    }

    #[test]
    fn test_float_nan_becomes_null() {
        assert_eq!(cbor_value_to_json(&Value::Float(f64::NAN)), serde_json::Value::Null);
    }

    #[test]
    fn test_dagcbor_roundtrip_simple() {
        // Encode a simple map with ciborium, decode with our function
        let original = serde_json::json!({ "text": "hello", "num": 42 });
        let mut buf = Vec::new();
        ciborium::into_writer(&ciborium::value::Value::Map(vec![
            (Value::Text("text".into()), Value::Text("hello".into())),
            (Value::Text("num".into()), Value::Integer(42.into())),
        ]), &mut buf).unwrap();
        let decoded = dagcbor_to_json(&buf).unwrap();
        assert_eq!(decoded["text"], "hello");
        assert_eq!(decoded["num"], 42);
    }
}
