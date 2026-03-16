//! PackStream v1 encoder/decoder — the binary serialization used by Bolt.
//!
//! Reference: <https://neo4j.com/docs/bolt/current/packstream/>

use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

/// PackStream value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn into_map(self) -> Option<Vec<(String, Value)>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }
}

// ── Marker bytes ────────────────────────────────────────────────────────────

const MARKER_TINY_STRING: u8 = 0x80;
const MARKER_STRING8: u8 = 0xD0;
const MARKER_STRING16: u8 = 0xD1;
const MARKER_STRING32: u8 = 0xD2;

const MARKER_TINY_LIST: u8 = 0x90;
const MARKER_LIST8: u8 = 0xD4;
const MARKER_LIST16: u8 = 0xD5;

const MARKER_TINY_MAP: u8 = 0xA0;
const MARKER_MAP8: u8 = 0xD8;
const MARKER_MAP16: u8 = 0xD9;

const MARKER_NULL: u8 = 0xC0;
const MARKER_TRUE: u8 = 0xC3;
const MARKER_FALSE: u8 = 0xC2;
const MARKER_INT8: u8 = 0xC8;
const MARKER_INT16: u8 = 0xC9;
const MARKER_INT32: u8 = 0xCA;
const MARKER_INT64: u8 = 0xCB;
const MARKER_FLOAT64: u8 = 0xC1;

const MARKER_TINY_STRUCT: u8 = 0xB0;

// ── Bolt structure signatures ───────────────────────────────────────────────

pub const SIG_HELLO: u8 = 0x01;
pub const SIG_LOGON: u8 = 0x6A;
pub const SIG_RUN: u8 = 0x10;
pub const SIG_PULL: u8 = 0x3F;
pub const SIG_DISCARD: u8 = 0x2F;
pub const SIG_BEGIN: u8 = 0x11;
pub const SIG_COMMIT: u8 = 0x12;
pub const SIG_ROLLBACK: u8 = 0x13;
pub const SIG_RESET: u8 = 0x0F;
pub const SIG_GOODBYE: u8 = 0x02;
pub const SIG_ROUTE: u8 = 0x66;

pub const SIG_SUCCESS: u8 = 0x70;
pub const SIG_RECORD: u8 = 0x71;
pub const SIG_IGNORED: u8 = 0x7E;
pub const SIG_FAILURE: u8 = 0x7F;

/// A Bolt structure: signature byte + fields.
#[derive(Debug)]
pub struct BoltStruct {
    pub signature: u8,
    pub fields: Vec<Value>,
}

// ── Encoder ─────────────────────────────────────────────────────────────────

pub fn encode_value(buf: &mut BytesMut, val: &Value) {
    match val {
        Value::Null => buf.put_u8(MARKER_NULL),
        Value::Bool(true) => buf.put_u8(MARKER_TRUE),
        Value::Bool(false) => buf.put_u8(MARKER_FALSE),
        Value::Int(n) => encode_int(buf, *n),
        Value::Float(f) => {
            buf.put_u8(MARKER_FLOAT64);
            buf.put_f64(*f);
        }
        Value::String(s) => encode_string(buf, s),
        Value::List(items) => {
            encode_list_header(buf, items.len());
            for item in items {
                encode_value(buf, item);
            }
        }
        Value::Map(entries) => {
            encode_map_header(buf, entries.len());
            for (k, v) in entries {
                encode_string(buf, k);
                encode_value(buf, v);
            }
        }
    }
}

fn encode_int(buf: &mut BytesMut, n: i64) {
    if (-16..=127).contains(&n) {
        buf.put_i8(n as i8);
    } else if (-128..=127).contains(&n) {
        buf.put_u8(MARKER_INT8);
        buf.put_i8(n as i8);
    } else if (-32768..=32767).contains(&n) {
        buf.put_u8(MARKER_INT16);
        buf.put_i16(n as i16);
    } else if (-2147483648..=2147483647).contains(&n) {
        buf.put_u8(MARKER_INT32);
        buf.put_i32(n as i32);
    } else {
        buf.put_u8(MARKER_INT64);
        buf.put_i64(n);
    }
}

fn encode_string(buf: &mut BytesMut, s: &str) {
    let len = s.len();
    if len < 16 {
        buf.put_u8(MARKER_TINY_STRING | len as u8);
    } else if len < 256 {
        buf.put_u8(MARKER_STRING8);
        buf.put_u8(len as u8);
    } else if len < 65536 {
        buf.put_u8(MARKER_STRING16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(MARKER_STRING32);
        buf.put_u32(len as u32);
    }
    buf.put_slice(s.as_bytes());
}

fn encode_list_header(buf: &mut BytesMut, len: usize) {
    if len < 16 {
        buf.put_u8(MARKER_TINY_LIST | len as u8);
    } else if len < 256 {
        buf.put_u8(MARKER_LIST8);
        buf.put_u8(len as u8);
    } else {
        buf.put_u8(MARKER_LIST16);
        buf.put_u16(len as u16);
    }
}

fn encode_map_header(buf: &mut BytesMut, len: usize) {
    if len < 16 {
        buf.put_u8(MARKER_TINY_MAP | len as u8);
    } else if len < 256 {
        buf.put_u8(MARKER_MAP8);
        buf.put_u8(len as u8);
    } else {
        buf.put_u8(MARKER_MAP16);
        buf.put_u16(len as u16);
    }
}

/// Encode a Bolt struct (signature + fields) into a chunked message.
pub fn encode_struct_message(sig: u8, fields: &[Value]) -> BytesMut {
    let mut body = BytesMut::new();
    // Struct header: TINY_STRUCT | num_fields, then signature
    body.put_u8(MARKER_TINY_STRUCT | fields.len() as u8);
    body.put_u8(sig);
    for f in fields {
        encode_value(&mut body, f);
    }
    chunk_message(&body)
}

/// Wrap a raw message body into Bolt chunked transport (length-prefixed chunks + 0x0000 end).
fn chunk_message(body: &[u8]) -> BytesMut {
    let mut out = BytesMut::new();
    for chunk in body.chunks(0xFFFF) {
        out.put_u16(chunk.len() as u16);
        out.put_slice(chunk);
    }
    out.put_u16(0); // end marker
    out
}

// ── Decoder ─────────────────────────────────────────────────────────────────

/// Read chunked Bolt message from buffer. Returns None if not enough data yet.
pub fn read_chunked_message(buf: &mut BytesMut) -> Option<BytesMut> {
    let mut assembled = BytesMut::new();
    let mut cursor = 0usize;

    loop {
        if buf.len() < cursor + 2 {
            return None;
        }
        let chunk_len = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]) as usize;
        cursor += 2;
        if chunk_len == 0 {
            // End of message
            buf.advance(cursor);
            return Some(assembled);
        }
        if buf.len() < cursor + chunk_len {
            return None;
        }
        assembled.put_slice(&buf[cursor..cursor + chunk_len]);
        cursor += chunk_len;
    }
}

/// Decode a PackStream value from buffer.
pub fn decode_value(buf: &mut &[u8]) -> anyhow::Result<Value> {
    if buf.is_empty() {
        anyhow::bail!("unexpected end of data");
    }
    let marker = buf[0];
    *buf = &buf[1..];

    match marker {
        MARKER_NULL => Ok(Value::Null),
        MARKER_TRUE => Ok(Value::Bool(true)),
        MARKER_FALSE => Ok(Value::Bool(false)),
        MARKER_FLOAT64 => {
            if buf.len() < 8 {
                anyhow::bail!("truncated float64");
            }
            let f = f64::from_be_bytes(buf[..8].try_into()?);
            *buf = &buf[8..];
            Ok(Value::Float(f))
        }
        MARKER_INT8 => {
            let v = buf[0] as i8;
            *buf = &buf[1..];
            Ok(Value::Int(v as i64))
        }
        MARKER_INT16 => {
            let v = i16::from_be_bytes(buf[..2].try_into()?);
            *buf = &buf[2..];
            Ok(Value::Int(v as i64))
        }
        MARKER_INT32 => {
            let v = i32::from_be_bytes(buf[..4].try_into()?);
            *buf = &buf[4..];
            Ok(Value::Int(v as i64))
        }
        MARKER_INT64 => {
            let v = i64::from_be_bytes(buf[..8].try_into()?);
            *buf = &buf[8..];
            Ok(Value::Int(v))
        }
        // Tiny int (-16..=127)
        m if (m as i8) >= -16 => Ok(Value::Int(m as i8 as i64)),
        // Tiny string (0x80..0x8F)
        m if (m & 0xF0) == MARKER_TINY_STRING => {
            let len = (m & 0x0F) as usize;
            decode_string_body(buf, len)
        }
        MARKER_STRING8 => {
            let len = buf[0] as usize;
            *buf = &buf[1..];
            decode_string_body(buf, len)
        }
        MARKER_STRING16 => {
            let len = u16::from_be_bytes(buf[..2].try_into()?) as usize;
            *buf = &buf[2..];
            decode_string_body(buf, len)
        }
        MARKER_STRING32 => {
            let len = u32::from_be_bytes(buf[..4].try_into()?) as usize;
            *buf = &buf[4..];
            decode_string_body(buf, len)
        }
        // Tiny list
        m if (m & 0xF0) == MARKER_TINY_LIST => {
            let len = (m & 0x0F) as usize;
            decode_list_body(buf, len)
        }
        MARKER_LIST8 => {
            let len = buf[0] as usize;
            *buf = &buf[1..];
            decode_list_body(buf, len)
        }
        MARKER_LIST16 => {
            let len = u16::from_be_bytes(buf[..2].try_into()?) as usize;
            *buf = &buf[2..];
            decode_list_body(buf, len)
        }
        // Tiny map
        m if (m & 0xF0) == MARKER_TINY_MAP => {
            let len = (m & 0x0F) as usize;
            decode_map_body(buf, len)
        }
        MARKER_MAP8 => {
            let len = buf[0] as usize;
            *buf = &buf[1..];
            decode_map_body(buf, len)
        }
        MARKER_MAP16 => {
            let len = u16::from_be_bytes(buf[..2].try_into()?) as usize;
            *buf = &buf[2..];
            decode_map_body(buf, len)
        }
        // Tiny struct (handled in decode_struct)
        m if (m & 0xF0) == MARKER_TINY_STRUCT => {
            // Shouldn't reach here via decode_value normally, but handle gracefully
            let n_fields = (m & 0x0F) as usize;
            let sig = buf[0];
            *buf = &buf[1..];
            let mut fields = Vec::with_capacity(n_fields);
            for _ in 0..n_fields {
                fields.push(decode_value(buf)?);
            }
            // Return as a map with __sig key
            let mut entries = vec![("__sig".to_string(), Value::Int(sig as i64))];
            for (i, f) in fields.into_iter().enumerate() {
                entries.push((format!("f{i}"), f));
            }
            Ok(Value::Map(entries))
        }
        _ => anyhow::bail!("unsupported PackStream marker: 0x{marker:02X}"),
    }
}

fn decode_string_body(buf: &mut &[u8], len: usize) -> anyhow::Result<Value> {
    if buf.len() < len {
        anyhow::bail!("truncated string");
    }
    let s = std::str::from_utf8(&buf[..len])?.to_string();
    *buf = &buf[len..];
    Ok(Value::String(s))
}

fn decode_list_body(buf: &mut &[u8], len: usize) -> anyhow::Result<Value> {
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        items.push(decode_value(buf)?);
    }
    Ok(Value::List(items))
}

fn decode_map_body(buf: &mut &[u8], len: usize) -> anyhow::Result<Value> {
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
        let key = match decode_value(buf)? {
            Value::String(s) => s,
            other => anyhow::bail!("map key must be string, got {other:?}"),
        };
        let val = decode_value(buf)?;
        entries.push((key, val));
    }
    Ok(Value::Map(entries))
}

/// Decode a Bolt struct from a raw message body.
pub fn decode_struct(data: &[u8]) -> anyhow::Result<BoltStruct> {
    let mut buf: &[u8] = data;
    if buf.is_empty() {
        anyhow::bail!("empty struct");
    }
    let marker = buf[0];
    buf = &buf[1..];
    if (marker & 0xF0) != MARKER_TINY_STRUCT {
        anyhow::bail!("expected struct marker, got 0x{marker:02X}");
    }
    let n_fields = (marker & 0x0F) as usize;
    if buf.is_empty() {
        anyhow::bail!("missing signature byte");
    }
    let signature = buf[0];
    buf = &buf[1..];
    let mut fields = Vec::with_capacity(n_fields);
    for _ in 0..n_fields {
        fields.push(decode_value(&mut buf)?);
    }
    Ok(BoltStruct { signature, fields })
}
