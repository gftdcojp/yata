//! Commit signing and verification using Ed25519 over CBOR.
//!
//! Pattern: set signature=None → CBOR encode → Blake3 hash → Ed25519 sign.
//! Same approach as `CasCapability::signable_bytes()` in yata-sync.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::blocks::GraphRootBlock;
use crate::error::{MdagError, Result};

/// Sign a GraphRootBlock in place. Sets `signer_did` and `signature`.
pub fn sign_root(root: &mut GraphRootBlock, signing_key: &SigningKey, signer_did: &str) {
    root.signer_did = signer_did.to_string();
    root.signature = Vec::new();
    let msg = signable_bytes(root);
    let sig = signing_key.sign(&msg);
    root.signature = sig.to_bytes().to_vec();
}

/// Verify a signed GraphRootBlock.
/// Returns Ok(()) if signature is valid, or error if verification fails.
pub fn verify_root(root: &GraphRootBlock, signer_public_key: &[u8; 32]) -> Result<()> {
    let vk = VerifyingKey::from_bytes(signer_public_key)
        .map_err(|_| MdagError::Verify("invalid public key".into()))?;

    let sig_arr: [u8; 64] = root
        .signature
        .as_slice()
        .try_into()
        .map_err(|_| MdagError::Verify("signature must be 64 bytes".into()))?;
    let sig = Signature::from_bytes(&sig_arr);

    let mut unsigned = root.clone();
    unsigned.signature = Vec::new();
    let msg = signable_bytes(&unsigned);

    vk.verify(&msg, &sig)
        .map_err(|_| MdagError::Verify("Ed25519 signature verification failed".into()))?;
    Ok(())
}

/// Compute the signable bytes: CBOR encode with signature=[], then Blake3 hash.
fn signable_bytes(root: &GraphRootBlock) -> Vec<u8> {
    let cbor = yata_cbor::encode(root).unwrap_or_default();
    blake3::hash(&cbor).as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocks::GraphRootBlock;
    use rand::rngs::OsRng;
    use yata_core::{Blake3Hash, PartitionId};

    fn presigned_root() -> GraphRootBlock {
        GraphRootBlock {
            version: 1,
            partition_id: PartitionId::from(0),
            parent: None,
            schema_cid: Blake3Hash::of(b"schema"),
            vertex_groups: vec![Blake3Hash::of(b"vg1")],
            edge_groups: vec![],
            vertex_count: 10,
            edge_count: 0,
            timestamp_ns: 1234567890,
            message: "test commit".into(),
            index_cids: vec![],
            vex_cid: None,
            vineyard_ids: None,
            signer_did: String::new(),
            signature: Vec::new(),
        }
    }

    #[test]
    fn test_sign_and_verify() {
        let sk = SigningKey::generate(&mut OsRng);
        let pk = sk.verifying_key().to_bytes();
        let mut root = presigned_root();

        sign_root(&mut root, &sk, "did:key:z6MkTest");
        assert_eq!(root.signature.len(), 64);
        assert_eq!(root.signer_did, "did:key:z6MkTest");
        assert!(verify_root(&root, &pk).is_ok());
    }

    #[test]
    fn test_tampered_block_fails_verify() {
        let sk = SigningKey::generate(&mut OsRng);
        let pk = sk.verifying_key().to_bytes();
        let mut root = presigned_root();
        sign_root(&mut root, &sk, "did:key:z6MkTest");

        // Tamper with the message
        root.message = "tampered".into();
        assert!(verify_root(&root, &pk).is_err());
    }

    #[test]
    fn test_wrong_key_fails_verify() {
        let sk1 = SigningKey::generate(&mut OsRng);
        let sk2 = SigningKey::generate(&mut OsRng);
        let pk2 = sk2.verifying_key().to_bytes();
        let mut root = presigned_root();
        sign_root(&mut root, &sk1, "did:key:z6MkTest");

        assert!(verify_root(&root, &pk2).is_err());
    }
}
