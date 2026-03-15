use crate::{
    firehose::decode_firehose_message,
    types::{AtCollection, AtDid, AtRepoOp, AtRkey, AtUri, DIDDocument, DIDService},
};

// ── AtUri ───────────────────────────────────────────────────────────────────

#[test]
fn test_at_uri_parse_valid() {
    let uri = AtUri::parse("at://did:plc:abc123/ai.gftd.cmd/rkey1").unwrap();
    assert_eq!(uri.did.as_str(), "did:plc:abc123");
    assert_eq!(uri.collection.as_str(), "ai.gftd.cmd");
    assert_eq!(uri.rkey.as_str(), "rkey1");
}

#[test]
fn test_at_uri_parse_missing_prefix_returns_none() {
    assert!(AtUri::parse("did:plc:abc123/col/rkey").is_none());
}

#[test]
fn test_at_uri_parse_missing_rkey_returns_none() {
    assert!(AtUri::parse("at://did:plc:abc123/ai.gftd.cmd").is_none());
}

#[test]
fn test_at_uri_display() {
    let uri = AtUri::new("did:plc:abc", "ai.gftd.cmd", "rkey42");
    assert_eq!(uri.to_string(), "at://did:plc:abc/ai.gftd.cmd/rkey42");
}

#[test]
fn test_at_uri_roundtrip() {
    let s = "at://did:plc:xyz/app.bsky.feed.post/3jwc3";
    let uri = AtUri::parse(s).unwrap();
    assert_eq!(uri.to_string(), s);
}

// ── AtRepoOp ────────────────────────────────────────────────────────────────

#[test]
fn test_at_repo_op_collection_and_rkey() {
    let op = AtRepoOp {
        action: "create".to_owned(),
        path: "ai.gftd.cmd/rkey999".to_owned(),
        cid: None,
    };
    assert_eq!(op.collection(), "ai.gftd.cmd");
    assert_eq!(op.rkey(), "rkey999");
}

#[test]
fn test_at_repo_op_missing_slash() {
    let op = AtRepoOp {
        action: "delete".to_owned(),
        path: "just-collection".to_owned(),
        cid: None,
    };
    assert_eq!(op.collection(), "just-collection");
    assert_eq!(op.rkey(), "");
}

// ── AtDid / AtRkey / AtCollection ───────────────────────────────────────────

#[test]
fn test_at_did_from_string() {
    let did = AtDid::from("did:plc:test");
    assert_eq!(did.as_str(), "did:plc:test");
    assert_eq!(did.to_string(), "did:plc:test");
}

#[test]
fn test_at_rkey_display() {
    let rkey = AtRkey("self".to_owned());
    assert_eq!(rkey.as_str(), "self");
}

#[test]
fn test_at_collection_display() {
    let col = AtCollection("app.bsky.actor.profile".to_owned());
    assert_eq!(col.as_str(), "app.bsky.actor.profile");
}

// ── DIDDocument ─────────────────────────────────────────────────────────────

#[test]
fn test_did_document_pds_endpoint() {
    let doc = DIDDocument {
        context: vec![],
        id: "did:plc:abc".to_owned(),
        also_known_as: vec!["at://alice.test".to_owned()],
        verification_method: vec![],
        service: vec![DIDService {
            id: "#atproto_pds".to_owned(),
            service_type: "AtprotoPersonalDataServer".to_owned(),
            service_endpoint: "https://pds.gftd.ai".to_owned(),
        }],
    };
    assert_eq!(doc.pds_endpoint(), Some("https://pds.gftd.ai"));
    assert_eq!(doc.handle(), Some("alice.test"));
}

#[test]
fn test_did_document_no_pds() {
    let doc = DIDDocument {
        context: vec![],
        id: "did:plc:abc".to_owned(),
        also_known_as: vec![],
        verification_method: vec![],
        service: vec![],
    };
    assert!(doc.pds_endpoint().is_none());
    assert!(doc.handle().is_none());
}

// ── decode_firehose_message ──────────────────────────────────────────────────

/// Build a two-CBOR firehose binary message (header + body).
fn make_commit_cbor(seq: i64, repo: &str, action: &str, collection: &str, rkey: &str) -> Vec<u8> {
    use ciborium::value::Value;

    let header = Value::Map(vec![
        (Value::Text("op".to_owned()), Value::Integer(1.into())),
        (Value::Text("t".to_owned()), Value::Text("#commit".to_owned())),
    ]);

    let op = Value::Map(vec![
        (Value::Text("action".to_owned()), Value::Text(action.to_owned())),
        (Value::Text("path".to_owned()), Value::Text(format!("{}/{}", collection, rkey))),
        (Value::Text("cid".to_owned()), Value::Bytes(vec![0x01, 0x02, 0x03])),
    ]);

    let body = Value::Map(vec![
        (Value::Text("seq".to_owned()), Value::Integer(seq.into())),
        (Value::Text("repo".to_owned()), Value::Text(repo.to_owned())),
        (Value::Text("rev".to_owned()), Value::Text("rev1".to_owned())),
        (Value::Text("ops".to_owned()), Value::Array(vec![op])),
        (Value::Text("blocks".to_owned()), Value::Bytes(vec![0x0a, 0x0b])),
        (Value::Text("time".to_owned()), Value::Text("2024-01-01T00:00:00Z".to_owned())),
    ]);

    let mut buf = Vec::new();
    ciborium::into_writer(&header, &mut buf).unwrap();
    ciborium::into_writer(&body, &mut buf).unwrap();
    buf
}

fn make_non_commit_cbor() -> Vec<u8> {
    use ciborium::value::Value;
    let header = Value::Map(vec![
        (Value::Text("op".to_owned()), Value::Integer(1.into())),
        (Value::Text("t".to_owned()), Value::Text("#handle".to_owned())),
    ]);
    let body = Value::Map(vec![
        (Value::Text("seq".to_owned()), Value::Integer(42.into())),
    ]);
    let mut buf = Vec::new();
    ciborium::into_writer(&header, &mut buf).unwrap();
    ciborium::into_writer(&body, &mut buf).unwrap();
    buf
}

#[test]
fn test_decode_firehose_commit() {
    let data = make_commit_cbor(123, "did:plc:alice", "create", "ai.gftd.cmd", "rkey1");
    let commit = decode_firehose_message(&data).unwrap();

    assert_eq!(commit.seq, 123);
    assert_eq!(commit.repo, "did:plc:alice");
    assert_eq!(commit.rev, "rev1");
    assert_eq!(commit.ts, "2024-01-01T00:00:00Z");
    assert_eq!(commit.ops.len(), 1);
    assert_eq!(commit.ops[0].action, "create");
    assert_eq!(commit.ops[0].collection(), "ai.gftd.cmd");
    assert_eq!(commit.ops[0].rkey(), "rkey1");
    assert_eq!(commit.ops[0].cid, Some(vec![0x01, 0x02, 0x03]));
    assert_eq!(commit.blocks, vec![0x0a, 0x0b]);
}

#[test]
fn test_decode_firehose_non_commit_returns_empty() {
    let data = make_non_commit_cbor();
    let commit = decode_firehose_message(&data).unwrap();
    // Non-#commit messages return an empty commit with no ops
    assert!(commit.ops.is_empty());
    assert!(commit.repo.is_empty());
}

#[test]
fn test_decode_firehose_completely_empty_returns_error() {
    // Completely empty slice: ciborium can't parse even the header CBOR
    let result = decode_firehose_message(&[]);
    assert!(result.is_err());
}

#[test]
fn test_decode_firehose_non_map_header_returns_empty_commit() {
    // A single CBOR integer (not a map) → header parsed but no #commit type → empty commit
    let result = decode_firehose_message(&[0x01, 0x02]);
    // Non-commit → no ops, no panic
    match result {
        Ok(commit) => assert!(commit.ops.is_empty()),
        Err(_) => {} // also acceptable
    }
}

#[test]
fn test_decode_firehose_multiple_ops() {
    use ciborium::value::Value;

    let header = Value::Map(vec![
        (Value::Text("op".to_owned()), Value::Integer(1.into())),
        (Value::Text("t".to_owned()), Value::Text("#commit".to_owned())),
    ]);

    let ops = vec![
        Value::Map(vec![
            (Value::Text("action".to_owned()), Value::Text("create".to_owned())),
            (Value::Text("path".to_owned()), Value::Text("ai.gftd.cmd/rkey1".to_owned())),
        ]),
        Value::Map(vec![
            (Value::Text("action".to_owned()), Value::Text("delete".to_owned())),
            (Value::Text("path".to_owned()), Value::Text("ai.gftd.cmd/rkey2".to_owned())),
        ]),
    ];

    let body = Value::Map(vec![
        (Value::Text("seq".to_owned()), Value::Integer(1.into())),
        (Value::Text("repo".to_owned()), Value::Text("did:plc:bob".to_owned())),
        (Value::Text("rev".to_owned()), Value::Text("r".to_owned())),
        (Value::Text("ops".to_owned()), Value::Array(ops)),
        (Value::Text("blocks".to_owned()), Value::Bytes(vec![])),
        (Value::Text("time".to_owned()), Value::Text("2024-01-01T00:00:00Z".to_owned())),
    ]);

    let mut data = Vec::new();
    ciborium::into_writer(&header, &mut data).unwrap();
    ciborium::into_writer(&body, &mut data).unwrap();

    let commit = decode_firehose_message(&data).unwrap();
    assert_eq!(commit.ops.len(), 2);
    assert_eq!(commit.ops[0].action, "create");
    assert_eq!(commit.ops[1].action, "delete");
}
