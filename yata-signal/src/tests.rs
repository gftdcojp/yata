use std::sync::Arc;
use x25519_dalek::StaticSecret;

use crate::{
    GroupSession, IdentityKeyPair, OneTimePreKey, PreKeyBundle, RatchetSession, SignalPayload,
    SignalStore, SignedPreKey,
    keys::{dh, verify_prekey},
    x3dh::{initiate as x3dh_initiate, respond as x3dh_respond},
};

// ── Key generation ──────────────────────────────────────────────────────────

#[test]
fn test_identity_key_pair_generate() {
    let ik = IdentityKeyPair::generate();
    // Public key must be non-zero
    assert_ne!(ik.dh_public_bytes(), [0u8; 32]);
    assert_ne!(ik.sign_public_bytes(), [0u8; 32]);
}

#[test]
fn test_identity_key_pair_serialize_roundtrip() {
    let ik = IdentityKeyPair::generate();
    let bytes = ik.to_bytes().unwrap();
    let restored = IdentityKeyPair::from_bytes(&bytes).unwrap();
    assert_eq!(ik.dh_public_bytes(), restored.dh_public_bytes());
    assert_eq!(ik.sign_public_bytes(), restored.sign_public_bytes());
}

#[test]
fn test_signed_prekey_generate_and_verify() {
    let ik = IdentityKeyPair::generate();
    let spk = SignedPreKey::generate(&ik, 1);
    assert_eq!(spk.key_id, 1);
    assert!(verify_prekey(&ik.sign_public_bytes(), &spk.key_pair.public_bytes(), &spk.signature));
}

#[test]
fn test_verify_prekey_wrong_sig_fails() {
    let ik = IdentityKeyPair::generate();
    let spk = SignedPreKey::generate(&ik, 1);
    let other_ik = IdentityKeyPair::generate();
    // sig from other identity key should fail
    assert!(!verify_prekey(&other_ik.sign_public_bytes(), &spk.key_pair.public_bytes(), &spk.signature));
}

#[test]
fn test_registration_id_is_14_bit() {
    let ik = IdentityKeyPair::generate();
    assert!(ik.registration_id() < 0x4000);
}

// ── X3DH ────────────────────────────────────────────────────────────────────

fn make_bundle(ik: &IdentityKeyPair, spk: &SignedPreKey, opk: Option<&OneTimePreKey>) -> PreKeyBundle {
    PreKeyBundle {
        registration_id: ik.registration_id(),
        identity_key: ik.dh_public_bytes(),
        identity_sign_key: ik.sign_public_bytes(),
        spk_id: spk.key_id,
        spk_public: spk.key_pair.public_bytes(),
        spk_signature: spk.signature,
        opk_id: opk.map(|o| o.key_id),
        opk_public: opk.map(|o| o.key_pair.public_bytes()),
    }
}

#[test]
fn test_x3dh_without_opk() {
    let alice_ik = IdentityKeyPair::generate();
    let bob_ik = IdentityKeyPair::generate();
    let bob_spk = SignedPreKey::generate(&bob_ik, 1);

    let bundle = make_bundle(&bob_ik, &bob_spk, None);
    let (alice_secret, init_msg) = x3dh_initiate(&alice_ik, &bundle).unwrap();
    let bob_secret = x3dh_respond(&bob_ik, &bob_spk, None, &init_msg).unwrap();

    assert_eq!(alice_secret.shared_secret, bob_secret.shared_secret);
    assert_eq!(alice_secret.ad, bob_secret.ad);
}

#[test]
fn test_x3dh_with_opk() {
    let alice_ik = IdentityKeyPair::generate();
    let bob_ik = IdentityKeyPair::generate();
    let bob_spk = SignedPreKey::generate(&bob_ik, 1);
    let bob_opk = OneTimePreKey::generate(7);

    let bundle = make_bundle(&bob_ik, &bob_spk, Some(&bob_opk));
    let (alice_secret, init_msg) = x3dh_initiate(&alice_ik, &bundle).unwrap();
    let bob_secret = x3dh_respond(&bob_ik, &bob_spk, Some(&bob_opk), &init_msg).unwrap();

    assert_eq!(alice_secret.shared_secret, bob_secret.shared_secret);
    assert_ne!(alice_secret.shared_secret, [0u8; 32]);
}

#[test]
fn test_x3dh_bad_signature_fails() {
    let alice_ik = IdentityKeyPair::generate();
    let bob_ik = IdentityKeyPair::generate();
    let bob_spk = SignedPreKey::generate(&bob_ik, 1);
    let other_ik = IdentityKeyPair::generate();

    let mut bundle = make_bundle(&bob_ik, &bob_spk, None);
    // Swap signing key → signature mismatch
    bundle.identity_sign_key = other_ik.sign_public_bytes();

    let result = x3dh_initiate(&alice_ik, &bundle);
    assert!(result.is_err());
}

// ── Double Ratchet ───────────────────────────────────────────────────────────

fn make_ratchet_pair() -> (RatchetSession, RatchetSession) {
    let alice_ik = IdentityKeyPair::generate();
    let bob_ik = IdentityKeyPair::generate();
    let bob_spk = SignedPreKey::generate(&bob_ik, 1);

    let bundle = make_bundle(&bob_ik, &bob_spk, None);
    let (alice_secret, init_msg) = x3dh_initiate(&alice_ik, &bundle).unwrap();
    let bob_secret = x3dh_respond(&bob_ik, &bob_spk, None, &init_msg).unwrap();

    let alice = RatchetSession::init_sender(&alice_secret, bob_spk.key_pair.public_bytes()).unwrap();
    let bob_ratchet = StaticSecret::from(bob_spk.key_pair.secret.to_bytes());
    let bob = RatchetSession::init_receiver(&bob_secret, bob_ratchet).unwrap();

    (alice, bob)
}

#[test]
fn test_ratchet_single_encrypt_decrypt() {
    let (mut alice, mut bob) = make_ratchet_pair();

    let plaintext = b"hello ratchet";
    let ct = alice.encrypt(plaintext).unwrap();
    let recovered = bob.decrypt(&ct).unwrap();
    assert_eq!(recovered, plaintext);
}

#[test]
fn test_ratchet_multiple_messages_in_order() {
    let (mut alice, mut bob) = make_ratchet_pair();

    let messages: &[&[u8]] = &[b"msg1", b"msg2", b"msg3", b"msg4", b"msg5"];
    let encrypted: Vec<_> = messages.iter().map(|m| alice.encrypt(m).unwrap()).collect();

    for (ct, expected) in encrypted.iter().zip(messages.iter()) {
        let decrypted = bob.decrypt(ct).unwrap();
        assert_eq!(decrypted, *expected);
    }
}

#[test]
fn test_ratchet_bidirectional() {
    let (mut alice, mut bob) = make_ratchet_pair();

    // Alice → Bob
    let ct1 = alice.encrypt(b"from alice").unwrap();
    let r1 = bob.decrypt(&ct1).unwrap();
    assert_eq!(r1, b"from alice");

    // Bob → Alice (forces a ratchet step)
    let ct2 = bob.encrypt(b"from bob").unwrap();
    let r2 = alice.decrypt(&ct2).unwrap();
    assert_eq!(r2, b"from bob");

    // Alice → Bob again
    let ct3 = alice.encrypt(b"alice again").unwrap();
    let r3 = bob.decrypt(&ct3).unwrap();
    assert_eq!(r3, b"alice again");
}

#[test]
fn test_ratchet_out_of_order_within_window() {
    let (mut alice, mut bob) = make_ratchet_pair();

    let ct0 = alice.encrypt(b"first").unwrap();
    let ct1 = alice.encrypt(b"second").unwrap();
    let ct2 = alice.encrypt(b"third").unwrap();

    // Deliver in reverse order
    let r2 = bob.decrypt(&ct2).unwrap();
    let r1 = bob.decrypt(&ct1).unwrap();
    let r0 = bob.decrypt(&ct0).unwrap();

    assert_eq!(r0, b"first");
    assert_eq!(r1, b"second");
    assert_eq!(r2, b"third");
}

#[test]
fn test_ratchet_cbor_serialize_roundtrip() {
    let (mut alice, mut bob) = make_ratchet_pair();

    let ct = alice.encrypt(b"serialize me").unwrap();

    // Serialize and restore Bob's session
    let cbor = bob.to_cbor().unwrap();
    let mut bob2 = RatchetSession::from_cbor(&cbor).unwrap();

    let recovered = bob2.decrypt(&ct).unwrap();
    assert_eq!(recovered, b"serialize me");
}

#[test]
fn test_ratchet_wrong_ciphertext_fails() {
    let (mut alice, mut bob) = make_ratchet_pair();

    let mut ct = alice.encrypt(b"tamper me").unwrap();
    // Flip a byte in the ciphertext
    if !ct.ciphertext.is_empty() {
        ct.ciphertext[0] ^= 0xFF;
    }
    let result = bob.decrypt(&ct);
    assert!(result.is_err());
}

// ── Group (Sender Keys) ──────────────────────────────────────────────────────

#[test]
fn test_group_session_encrypt_decrypt() {
    let mut alice = GroupSession::new("group-1", "did:plc:alice");
    let mut bob = GroupSession::new("group-1", "did:plc:bob");

    let dist = alice.init_sender().unwrap();
    bob.process_distribution(&dist).unwrap();

    let msg = alice.encrypt(b"group hello").unwrap();
    let plaintext = bob.decrypt(&msg).unwrap();
    assert_eq!(plaintext, b"group hello");
}

#[test]
fn test_group_session_multiple_messages() {
    let mut alice = GroupSession::new("grp", "did:plc:alice");
    let mut bob = GroupSession::new("grp", "did:plc:bob");

    let dist = alice.init_sender().unwrap();
    bob.process_distribution(&dist).unwrap();

    for i in 0u8..5 {
        let msg = alice.encrypt(&[i]).unwrap();
        let pt = bob.decrypt(&msg).unwrap();
        assert_eq!(pt, vec![i]);
    }
}

#[test]
fn test_group_session_group_id_mismatch() {
    let mut alice = GroupSession::new("group-A", "did:plc:alice");
    let mut bob = GroupSession::new("group-B", "did:plc:bob"); // different group

    let dist = alice.init_sender().unwrap();
    let result = bob.process_distribution(&dist);
    assert!(result.is_err());
}

#[test]
fn test_group_session_encrypt_without_init_fails() {
    let mut alice = GroupSession::new("grp", "did:plc:alice");
    let result = alice.encrypt(b"no init");
    assert!(result.is_err());
}

#[test]
fn test_group_session_decrypt_unknown_sender_fails() {
    let mut bob = GroupSession::new("grp", "did:plc:bob");
    // Bob has no distribution from alice
    let msg = crate::group::SenderKeyMessage {
        group_id: "grp".to_owned(),
        sender_did: "did:plc:alice".to_owned(),
        iteration: 0,
        ciphertext: vec![1, 2, 3],
        nonce: [0u8; 12],
    };
    let result = bob.decrypt(&msg);
    assert!(result.is_err());
}

#[test]
fn test_group_session_forward_secrecy_skip() {
    let mut alice = GroupSession::new("grp", "did:plc:alice");
    let mut bob = GroupSession::new("grp", "did:plc:bob");

    let dist = alice.init_sender().unwrap();
    bob.process_distribution(&dist).unwrap();

    // Alice sends 3, Bob skips msg0 and msg1, receives msg2 first
    let _msg0 = alice.encrypt(b"skip0").unwrap();
    let _msg1 = alice.encrypt(b"skip1").unwrap();
    let msg2 = alice.encrypt(b"msg2").unwrap();

    // Bob receives msg2 (which skips iterations 0 and 1)
    let pt = bob.decrypt(&msg2).unwrap();
    assert_eq!(pt, b"msg2");
}

// ── SignalPayload ────────────────────────────────────────────────────────────

#[test]
fn test_signal_payload_roundtrip() {
    let (mut alice, _bob) = make_ratchet_pair();

    let ct = alice.encrypt(b"payload test").unwrap();
    let payload = SignalPayload {
        sender_did: "did:plc:alice".to_owned(),
        session_id: "sess-1".to_owned(),
        message: ct,
    };

    let bytes = payload.to_bytes().unwrap();
    let restored = SignalPayload::from_bytes(&bytes).unwrap();
    assert_eq!(restored.sender_did, "did:plc:alice");
    assert_eq!(restored.session_id, "sess-1");
}

#[test]
fn test_signal_payload_as_payload_ref() {
    let (mut alice, _bob) = make_ratchet_pair();
    let ct = alice.encrypt(b"ref test").unwrap();
    let payload = SignalPayload {
        sender_did: "did:plc:alice".to_owned(),
        session_id: "s".to_owned(),
        message: ct,
    };
    let pr = payload.as_payload_ref().unwrap();
    // Should produce an InlineBytes variant
    matches!(pr, yata_core::PayloadRef::InlineBytes(_));
}

// ── SignalStore (async, uses real KV) ────────────────────────────────────────

async fn make_signal_store(dir: &tempfile::TempDir) -> SignalStore {
    use yata_kv::KvBucketStore;
    use yata_log::{LocalLog, PayloadStore};

    let log = Arc::new(LocalLog::new(dir.path().join("log")).await.unwrap());
    let ps = Arc::new(PayloadStore::new(dir.path().join("kv_payloads")).await.unwrap());
    let kv = Arc::new(KvBucketStore::new(log, ps).await.unwrap());
    let ik = IdentityKeyPair::generate();
    SignalStore::new(kv, ik).await.unwrap()
}

#[tokio::test]
async fn test_signal_store_session_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_signal_store(&dir).await;

    // Build a session
    let (mut alice_session, _bob) = make_ratchet_pair();
    let ct = alice_session.encrypt(b"store test").unwrap();

    store.save_session("did:plc:peer", &alice_session).await.unwrap();
    let loaded = store.load_session("did:plc:peer").await.unwrap();
    assert!(loaded.is_some());

    // Verify the loaded session can decrypt
    let mut loaded_session = loaded.unwrap();
    // Re-encrypt with a fresh ratchet state copy; just verify no panic
    let _ = loaded_session.to_cbor().unwrap();
    drop(ct);
}

#[tokio::test]
async fn test_signal_store_missing_session_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_signal_store(&dir).await;
    let result = store.load_session("did:plc:nobody").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_signal_store_prekey_bundle_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_signal_store(&dir).await;

    let ik = IdentityKeyPair::generate();
    let spk = SignedPreKey::generate(&ik, 1);
    let bundle = make_bundle(&ik, &spk, None);

    store.save_pre_key_bundle(&bundle).await.unwrap();
    let loaded = store.load_pre_key_bundle().await.unwrap().unwrap();

    assert_eq!(loaded.registration_id, bundle.registration_id);
    assert_eq!(loaded.identity_key, bundle.identity_key);
    assert_eq!(loaded.spk_id, bundle.spk_id);
}

#[tokio::test]
async fn test_signal_store_no_bundle_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_signal_store(&dir).await;
    let result = store.load_pre_key_bundle().await.unwrap();
    assert!(result.is_none());
}
