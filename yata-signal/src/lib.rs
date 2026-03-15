#![allow(dead_code)]

pub use keys::{DHKeyPair, IdentityKeyPair, OneTimePreKey, PreKeyBundle, SignedPreKey};
pub use x3dh::{X3DHInitMessage, X3DHSharedSecret, initiate as x3dh_initiate, respond as x3dh_respond};
pub use ratchet::{EncryptedMessage, MessageHeader, RatchetSession};
pub use group::{GroupSession, SenderKeyDistribution, SenderKeyMessage};
pub use store::SignalStore;
pub use payload::SignalPayload;

#[derive(thiserror::Error, Debug)]
pub enum SignalError {
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("decryption failed")]
    DecryptionFailed,
    #[error("session not found for peer: {0}")]
    SessionNotFound(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("storage error: {0}")]
    Storage(#[from] yata_core::YataError),
}
pub type Result<T> = std::result::Result<T, SignalError>;

pub mod keys {
    use super::SignalError;
    use ed25519_dalek::SigningKey;
    use x25519_dalek::{StaticSecret, PublicKey};
    use rand::rngs::OsRng;

    /// Long-term identity key pair.
    /// X25519 for DH, Ed25519 for signing prekeys.
    pub struct IdentityKeyPair {
        pub dh_secret: StaticSecret,
        pub dh_public: PublicKey,
        pub sign_key: SigningKey,
    }

    impl IdentityKeyPair {
        pub fn generate() -> Self {
            let dh_secret = StaticSecret::random_from_rng(&mut OsRng);
            let dh_public = PublicKey::from(&dh_secret);
            let sign_key = SigningKey::generate(&mut OsRng);
            Self { dh_secret, dh_public, sign_key }
        }

        pub fn dh_public_bytes(&self) -> [u8; 32] {
            self.dh_public.to_bytes()
        }

        pub fn sign_public_bytes(&self) -> [u8; 32] {
            self.sign_key.verifying_key().to_bytes()
        }

        pub fn sign_prekey(&self, prekey_public: &[u8; 32]) -> [u8; 64] {
            use ed25519_dalek::Signer;
            self.sign_key.sign(prekey_public).to_bytes()
        }

        pub fn registration_id(&self) -> u32 {
            use sha2::{Sha256, Digest};
            let hash = Sha256::digest(self.dh_public_bytes());
            let id = u16::from_be_bytes([hash[0], hash[1]]);
            (id as u32) & 0x3FFF
        }

        /// Serialize for yata-kv storage (CBOR).
        pub fn to_bytes(&self) -> super::Result<Vec<u8>> {
            let data = SerializedIdentity {
                dh_secret: self.dh_secret.to_bytes(),
                sign_secret: self.sign_key.to_bytes(),
            };
            yata_cbor::encode(&data)
                .map_err(|e| super::SignalError::Serialization(e.to_string()))
        }

        pub fn from_bytes(data: &[u8]) -> super::Result<Self> {
            let s: SerializedIdentity = yata_cbor::decode(data)
                .map_err(|e| super::SignalError::Serialization(e.to_string()))?;
            let dh_secret = StaticSecret::from(s.dh_secret);
            let dh_public = PublicKey::from(&dh_secret);
            let sign_key = SigningKey::from_bytes(&s.sign_secret);
            Ok(Self { dh_secret, dh_public, sign_key })
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct SerializedIdentity {
        dh_secret: [u8; 32],
        sign_secret: [u8; 32],
    }

    /// Ephemeral or pre-key Curve25519 key pair.
    pub struct DHKeyPair {
        pub secret: StaticSecret,
        pub public: PublicKey,
    }
    impl DHKeyPair {
        pub fn generate() -> Self {
            let secret = StaticSecret::random_from_rng(&mut OsRng);
            let public = PublicKey::from(&secret);
            Self { secret, public }
        }
        pub fn public_bytes(&self) -> [u8; 32] { self.public.to_bytes() }
        pub fn from_secret_bytes(secret: [u8; 32]) -> Self {
            let secret = StaticSecret::from(secret);
            let public = PublicKey::from(&secret);
            Self { secret, public }
        }
        pub fn secret_bytes(&self) -> [u8; 32] { self.secret.to_bytes() }
    }

    /// Signed prekey.
    pub struct SignedPreKey {
        pub key_id: u32,
        pub key_pair: DHKeyPair,
        pub signature: [u8; 64],
    }
    impl SignedPreKey {
        pub fn generate(identity: &IdentityKeyPair, key_id: u32) -> Self {
            let key_pair = DHKeyPair::generate();
            let signature = identity.sign_prekey(&key_pair.public_bytes());
            Self { key_id, key_pair, signature }
        }
    }

    /// One-time prekey.
    pub struct OneTimePreKey {
        pub key_id: u32,
        pub key_pair: DHKeyPair,
    }
    impl OneTimePreKey {
        pub fn generate(key_id: u32) -> Self {
            Self { key_id, key_pair: DHKeyPair::generate() }
        }
    }

    /// Prekey bundle published by a device.
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    pub struct PreKeyBundle {
        pub registration_id: u32,
        pub identity_key: [u8; 32],
        pub identity_sign_key: [u8; 32],
        pub spk_id: u32,
        pub spk_public: [u8; 32],
        #[serde(with = "sig64_serde")]
        pub spk_signature: [u8; 64],
        pub opk_id: Option<u32>,
        pub opk_public: Option<[u8; 32]>,
    }

    mod sig64_serde {
        pub fn serialize<S: serde::Serializer>(sig: &[u8; 64], s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(sig)
        }
        pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<[u8; 64], D::Error> {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = [u8; 64];
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "64 bytes") }
                fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<[u8; 64], E> {
                    v.try_into().map_err(|_| E::custom("expected 64 bytes"))
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<[u8; 64], A::Error> {
                    let mut arr = [0u8; 64];
                    for (i, b) in arr.iter_mut().enumerate() {
                        *b = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                    }
                    Ok(arr)
                }
            }
            d.deserialize_bytes(V)
        }
    }

    /// Verify a signed prekey against an Ed25519 identity signing key.
    pub fn verify_prekey(sign_public: &[u8; 32], prekey_public: &[u8; 32], sig: &[u8; 64]) -> bool {
        use ed25519_dalek::{VerifyingKey, Verifier};
        let Ok(vk) = VerifyingKey::from_bytes(sign_public) else { return false };
        let signature = ed25519_dalek::Signature::from_bytes(sig);
        vk.verify(prekey_public, &signature).is_ok()
    }

    /// X25519 Diffie-Hellman.
    pub fn dh(secret: &StaticSecret, public: &[u8; 32]) -> [u8; 32] {
        let pk = PublicKey::from(*public);
        secret.diffie_hellman(&pk).to_bytes()
    }
}

pub mod x3dh {
    use super::keys::*;
    use super::{Result, SignalError};
    use x25519_dalek::{StaticSecret, PublicKey};
    use rand::rngs::OsRng;

    const X3DH_INFO: &[u8] = b"GFTD Signal X3DH v1";

    /// Message sent by initiator to establish an X3DH session.
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    pub struct X3DHInitMessage {
        pub registration_id: u32,
        pub sender_identity_key: [u8; 32],
        pub sender_sign_key: [u8; 32],
        pub ephemeral_key: [u8; 32],
        pub recipient_spk_id: u32,
        pub recipient_opk_id: Option<u32>,
    }

    /// Shared secret output of X3DH.
    pub struct X3DHSharedSecret {
        pub shared_secret: [u8; 32],
        pub ad: Vec<u8>,
    }

    /// Initiate X3DH as Alice (sender).
    pub fn initiate(sender_ik: &IdentityKeyPair, bundle: &PreKeyBundle) -> Result<(X3DHSharedSecret, X3DHInitMessage)> {
        if !verify_prekey(&bundle.identity_sign_key, &bundle.spk_public, &bundle.spk_signature) {
            return Err(SignalError::Crypto("signed prekey signature verification failed".into()));
        }

        let ek_secret = StaticSecret::random_from_rng(&mut OsRng);
        let ek_public = PublicKey::from(&ek_secret);

        let dh1 = dh(&sender_ik.dh_secret, &bundle.spk_public);
        let dh2 = dh(&ek_secret, &bundle.identity_key);
        let dh3 = dh(&ek_secret, &bundle.spk_public);

        let mut dh_material = Vec::with_capacity(128);
        dh_material.extend_from_slice(&dh1);
        dh_material.extend_from_slice(&dh2);
        dh_material.extend_from_slice(&dh3);

        let opk_id = if let (Some(opk_id), Some(opk_pub)) = (bundle.opk_id, bundle.opk_public) {
            dh_material.extend_from_slice(&dh(&ek_secret, &opk_pub));
            Some(opk_id)
        } else {
            None
        };

        let mut ad = vec![0u8; 64];
        ad[..32].copy_from_slice(&sender_ik.dh_public_bytes());
        ad[32..].copy_from_slice(&bundle.identity_key);

        let shared_secret = x3dh_kdf(&dh_material, &ad)?;
        let init_msg = X3DHInitMessage {
            registration_id: sender_ik.registration_id(),
            sender_identity_key: sender_ik.dh_public_bytes(),
            sender_sign_key: sender_ik.sign_public_bytes(),
            ephemeral_key: ek_public.to_bytes(),
            recipient_spk_id: bundle.spk_id,
            recipient_opk_id: opk_id,
        };
        Ok((X3DHSharedSecret { shared_secret, ad }, init_msg))
    }

    /// Respond to X3DH as Bob (recipient).
    pub fn respond(
        recipient_ik: &IdentityKeyPair,
        spk: &SignedPreKey,
        opk: Option<&OneTimePreKey>,
        msg: &X3DHInitMessage,
    ) -> Result<X3DHSharedSecret> {
        if spk.key_id != msg.recipient_spk_id {
            return Err(SignalError::InvalidKey(format!("SPK ID mismatch: got {} want {}", spk.key_id, msg.recipient_spk_id)));
        }

        let dh1 = dh(&spk.key_pair.secret, &msg.sender_identity_key);
        let dh2 = dh(&recipient_ik.dh_secret, &msg.ephemeral_key);
        let dh3 = dh(&spk.key_pair.secret, &msg.ephemeral_key);

        let mut dh_material = Vec::with_capacity(128);
        dh_material.extend_from_slice(&dh1);
        dh_material.extend_from_slice(&dh2);
        dh_material.extend_from_slice(&dh3);

        if let Some(opk_id) = msg.recipient_opk_id {
            let opk = opk.ok_or_else(|| SignalError::InvalidKey("OPK required but not provided".into()))?;
            if opk.key_id != opk_id {
                return Err(SignalError::InvalidKey(format!("OPK ID mismatch: got {} want {}", opk.key_id, opk_id)));
            }
            let dh4 = dh(&opk.key_pair.secret, &msg.ephemeral_key);
            dh_material.extend_from_slice(&dh4);
        }

        let mut ad = vec![0u8; 64];
        ad[..32].copy_from_slice(&msg.sender_identity_key);
        ad[32..].copy_from_slice(&recipient_ik.dh_public_bytes());

        let shared_secret = x3dh_kdf(&dh_material, &ad)?;
        Ok(X3DHSharedSecret { shared_secret, ad })
    }

    fn x3dh_kdf(dh_material: &[u8], salt: &[u8]) -> Result<[u8; 32]> {
        use hkdf::Hkdf;
        use sha2::Sha256;
        let hk = Hkdf::<Sha256>::new(Some(salt), dh_material);
        let mut out = [0u8; 32];
        hk.expand(X3DH_INFO, &mut out)
            .map_err(|e| SignalError::Crypto(format!("HKDF expand failed: {:?}", e)))?;
        Ok(out)
    }
}

pub mod ratchet {
    use super::{Result, SignalError};
    use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce, KeyInit, aead::Aead};
    use hkdf::Hkdf;
    use sha2::Sha256;
    use std::collections::HashMap;
    use x25519_dalek::{StaticSecret, PublicKey};

    const MAX_SKIP: u32 = 100;
    const RATCHET_INFO: &[u8] = b"GFTD Signal Ratchet v1";

    /// Header prepended to every ratchet-encrypted message.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct MessageHeader {
        pub dh_public: [u8; 32],
        pub pn: u32,
        pub n: u32,
    }

    /// Ratchet-encrypted message.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct EncryptedMessage {
        pub header: MessageHeader,
        pub ciphertext: Vec<u8>,
        pub nonce: [u8; 12],
    }

    /// Double Ratchet session state.
    pub struct RatchetSession {
        dhs_secret: StaticSecret,
        dhs_public: PublicKey,
        dhr: Option<[u8; 32]>,
        rk: [u8; 32],
        cks: Option<[u8; 32]>,
        ckr: Option<[u8; 32]>,
        ns: u32,
        nr: u32,
        pn: u32,
        skipped: HashMap<([u8; 32], u32), [u8; 32]>,
        ad: Vec<u8>,
    }

    impl RatchetSession {
        /// Initialize as sender (Alice).
        pub fn init_sender(shared_secret: &super::x3dh::X3DHSharedSecret, recipient_ratchet_key: [u8; 32]) -> Result<Self> {
            let dhs_secret = StaticSecret::random_from_rng(&mut rand::rngs::OsRng);
            let dhs_public = PublicKey::from(&dhs_secret);
            let (rk, cks) = kdf_rk(&shared_secret.shared_secret, &dhs_secret, &recipient_ratchet_key)?;
            Ok(Self {
                dhs_secret,
                dhs_public,
                dhr: Some(recipient_ratchet_key),
                rk,
                cks: Some(cks),
                ckr: None,
                ns: 0, nr: 0, pn: 0,
                skipped: HashMap::new(),
                ad: shared_secret.ad.clone(),
            })
        }

        /// Initialize as receiver (Bob).
        pub fn init_receiver(shared_secret: &super::x3dh::X3DHSharedSecret, our_ratchet_secret: StaticSecret) -> Result<Self> {
            let our_public = PublicKey::from(&our_ratchet_secret);
            Ok(Self {
                dhs_secret: our_ratchet_secret,
                dhs_public: our_public,
                dhr: None,
                rk: shared_secret.shared_secret,
                cks: None,
                ckr: None,
                ns: 0, nr: 0, pn: 0,
                skipped: HashMap::new(),
                ad: shared_secret.ad.clone(),
            })
        }

        pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<EncryptedMessage> {
            let (cks, mk) = kdf_ck(self.cks.as_ref().ok_or_else(|| SignalError::Crypto("no sending chain key".into()))?)?;
            self.cks = Some(cks);
            let header = MessageHeader { dh_public: self.dhs_public.to_bytes(), pn: self.pn, n: self.ns };
            self.ns += 1;
            let (ciphertext, nonce) = encrypt_message(&mk, plaintext, &self.ad)?;
            Ok(EncryptedMessage { header, ciphertext, nonce })
        }

        pub fn decrypt(&mut self, msg: &EncryptedMessage) -> Result<Vec<u8>> {
            let skipped_key = (msg.header.dh_public, msg.header.n);
            if let Some(mk) = self.skipped.remove(&skipped_key) {
                return decrypt_message(&mk, &msg.ciphertext, &msg.nonce, &self.ad);
            }

            let dhr_bytes = msg.header.dh_public;

            if self.dhr.as_ref() != Some(&dhr_bytes) {
                if let Some(ckr) = self.ckr {
                    let old_dhr = self.dhr.unwrap_or([0u8; 32]);
                    self.skip_message_keys(old_dhr, msg.header.pn, ckr)?;
                }

                let (rk, ckr) = kdf_rk(&self.rk, &StaticSecret::from(self.dhs_secret.to_bytes()), &dhr_bytes)?;

                let new_dhs = StaticSecret::random_from_rng(&mut rand::rngs::OsRng);
                let new_dhs_pub = PublicKey::from(&new_dhs);
                let (rk2, cks) = kdf_rk(&rk, &new_dhs, &dhr_bytes)?;

                self.pn = self.ns;
                self.ns = 0;
                self.nr = 0;
                self.dhr = Some(dhr_bytes);
                self.rk = rk2;
                self.cks = Some(cks);
                self.ckr = Some(ckr);
                self.dhs_secret = new_dhs;
                self.dhs_public = new_dhs_pub;
            }

            let ckr = self.ckr.ok_or_else(|| SignalError::Crypto("no receiving chain key".into()))?;
            self.skip_message_keys(dhr_bytes, msg.header.n, ckr)?;

            let (new_ckr, mk) = kdf_ck(self.ckr.as_ref().unwrap())?;
            self.ckr = Some(new_ckr);
            self.nr += 1;

            decrypt_message(&mk, &msg.ciphertext, &msg.nonce, &self.ad)
        }

        fn skip_message_keys(&mut self, dh_pub: [u8; 32], until: u32, mut ck: [u8; 32]) -> Result<()> {
            if self.nr + MAX_SKIP < until {
                return Err(SignalError::Crypto(format!("too many skipped messages: {} > {}", until, self.nr + MAX_SKIP)));
            }
            while self.nr < until {
                let (new_ck, mk) = kdf_ck(&ck)?;
                self.skipped.insert((dh_pub, self.nr), mk);
                ck = new_ck;
                self.ckr = Some(ck);
                self.nr += 1;
            }
            Ok(())
        }

        pub fn to_cbor(&self) -> super::Result<Vec<u8>> {
            let s = SerializedSession {
                dhs_secret: self.dhs_secret.to_bytes(),
                dhs_public: self.dhs_public.to_bytes(),
                dhr: self.dhr,
                rk: self.rk,
                cks: self.cks,
                ckr: self.ckr,
                ns: self.ns, nr: self.nr, pn: self.pn,
                skipped: self.skipped.iter().map(|((dh, n), mk)| (*dh, *n, *mk)).collect(),
                ad: self.ad.clone(),
            };
            yata_cbor::encode(&s).map_err(|e| super::SignalError::Serialization(e.to_string()))
        }

        pub fn from_cbor(data: &[u8]) -> super::Result<Self> {
            let s: SerializedSession = yata_cbor::decode(data)
                .map_err(|e| super::SignalError::Serialization(e.to_string()))?;
            let dhs_secret = StaticSecret::from(s.dhs_secret);
            let dhs_public = PublicKey::from(&dhs_secret);
            let skipped = s.skipped.into_iter().map(|(dh, n, mk)| ((dh, n), mk)).collect();
            Ok(Self {
                dhs_secret, dhs_public,
                dhr: s.dhr, rk: s.rk, cks: s.cks, ckr: s.ckr,
                ns: s.ns, nr: s.nr, pn: s.pn,
                skipped, ad: s.ad,
            })
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct SerializedSession {
        dhs_secret: [u8; 32],
        dhs_public: [u8; 32],
        dhr: Option<[u8; 32]>,
        rk: [u8; 32],
        cks: Option<[u8; 32]>,
        ckr: Option<[u8; 32]>,
        ns: u32, nr: u32, pn: u32,
        skipped: Vec<([u8; 32], u32, [u8; 32])>,
        ad: Vec<u8>,
    }

    fn kdf_rk(rk: &[u8; 32], dhs: &StaticSecret, dhr: &[u8; 32]) -> Result<([u8; 32], [u8; 32])> {
        let dh_out = StaticSecret::from(dhs.to_bytes()).diffie_hellman(&PublicKey::from(*dhr)).to_bytes();
        let hk = Hkdf::<Sha256>::new(Some(rk), &dh_out);
        let mut out = [0u8; 64];
        hk.expand(RATCHET_INFO, &mut out)
            .map_err(|e| SignalError::Crypto(format!("kdf_rk HKDF failed: {:?}", e)))?;
        let mut rk_new = [0u8; 32];
        let mut ck_new = [0u8; 32];
        rk_new.copy_from_slice(&out[..32]);
        ck_new.copy_from_slice(&out[32..]);
        Ok((rk_new, ck_new))
    }

    fn kdf_ck(ck: &[u8; 32]) -> Result<([u8; 32], [u8; 32])> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac1 = <HmacSha256 as Mac>::new_from_slice(ck)
            .map_err(|e| SignalError::Crypto(e.to_string()))?;
        mac1.update(&[0x01]);
        let mk = mac1.finalize().into_bytes();
        let mut mac2 = <HmacSha256 as Mac>::new_from_slice(ck)
            .map_err(|e| SignalError::Crypto(e.to_string()))?;
        mac2.update(&[0x02]);
        let ck_new = mac2.finalize().into_bytes();
        let mut mk_arr = [0u8; 32];
        let mut ck_arr = [0u8; 32];
        mk_arr.copy_from_slice(&mk);
        ck_arr.copy_from_slice(&ck_new);
        Ok((ck_arr, mk_arr))
    }

    fn encrypt_message(mk: &[u8; 32], plaintext: &[u8], ad: &[u8]) -> Result<(Vec<u8>, [u8; 12])> {
        use rand::RngCore;
        let cipher = ChaCha20Poly1305::new(Key::from_slice(mk));
        let mut nonce_bytes = [0u8; 12];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher.encrypt(nonce, chacha20poly1305::aead::Payload { msg: plaintext, aad: ad })
            .map_err(|_| SignalError::DecryptionFailed)?;
        Ok((ciphertext, nonce_bytes))
    }

    fn decrypt_message(mk: &[u8; 32], ciphertext: &[u8], nonce: &[u8; 12], ad: &[u8]) -> Result<Vec<u8>> {
        let cipher = ChaCha20Poly1305::new(Key::from_slice(mk));
        let nonce = Nonce::from_slice(nonce);
        cipher.decrypt(nonce, chacha20poly1305::aead::Payload { msg: ciphertext, aad: ad })
            .map_err(|_| SignalError::DecryptionFailed)
    }
}

pub mod group {
    use super::{Result, SignalError};
    use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce, KeyInit, aead::Aead};
    use rand::RngCore;
    use std::collections::HashMap;

    /// Distribution message sent (via 1:1 DM) to group members.
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    pub struct SenderKeyDistribution {
        pub group_id: String,
        pub sender_did: String,
        pub iteration: u32,
        pub chain_key: [u8; 32],
    }

    /// Sender Key encrypted group message.
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    pub struct SenderKeyMessage {
        pub group_id: String,
        pub sender_did: String,
        pub iteration: u32,
        pub ciphertext: Vec<u8>,
        pub nonce: [u8; 12],
    }

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct SenderState {
        chain_key: [u8; 32],
        iteration: u32,
    }

    /// Group session managing Sender Key encryption for a group channel.
    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct GroupSession {
        pub group_id: String,
        pub our_did: String,
        our_state: Option<SenderState>,
        members: HashMap<String, SenderState>,
    }

    impl GroupSession {
        pub fn new(group_id: impl Into<String>, our_did: impl Into<String>) -> Self {
            Self { group_id: group_id.into(), our_did: our_did.into(), our_state: None, members: HashMap::new() }
        }

        /// Generate our sender key. Returns distribution message to send to members.
        pub fn init_sender(&mut self) -> Result<SenderKeyDistribution> {
            let mut ck = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut ck);
            self.our_state = Some(SenderState { chain_key: ck, iteration: 0 });
            Ok(SenderKeyDistribution { group_id: self.group_id.clone(), sender_did: self.our_did.clone(), iteration: 0, chain_key: ck })
        }

        /// Process a distribution message from a remote member.
        pub fn process_distribution(&mut self, dist: &SenderKeyDistribution) -> Result<()> {
            if dist.group_id != self.group_id {
                return Err(SignalError::Crypto(format!("group ID mismatch: {} != {}", dist.group_id, self.group_id)));
            }
            self.members.insert(dist.sender_did.clone(), SenderState { chain_key: dist.chain_key, iteration: dist.iteration });
            Ok(())
        }

        /// Encrypt a plaintext for the group using our sender key.
        pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<SenderKeyMessage> {
            let state = self.our_state.as_mut().ok_or_else(|| SignalError::Crypto("sender not initialized".into()))?;
            let mk = advance_chain(&mut state.chain_key);
            let cipher = ChaCha20Poly1305::new(Key::from_slice(&mk));
            let mut nonce_bytes = [0u8; 12];
            rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
            let nonce = Nonce::from_slice(&nonce_bytes);
            let ciphertext = cipher.encrypt(nonce, plaintext).map_err(|_| SignalError::DecryptionFailed)?;
            let iteration = state.iteration;
            state.iteration += 1;
            Ok(SenderKeyMessage { group_id: self.group_id.clone(), sender_did: self.our_did.clone(), iteration, ciphertext, nonce: nonce_bytes })
        }

        /// Decrypt a group message from a known sender.
        pub fn decrypt(&mut self, msg: &SenderKeyMessage) -> Result<Vec<u8>> {
            if msg.group_id != self.group_id {
                return Err(SignalError::Crypto("group ID mismatch".into()));
            }
            let state = self.members.get_mut(&msg.sender_did)
                .ok_or_else(|| SignalError::SessionNotFound(msg.sender_did.clone()))?;

            let target = msg.iteration;
            while state.iteration < target {
                advance_chain(&mut state.chain_key);
                state.iteration += 1;
            }
            let mk = advance_chain(&mut state.chain_key);
            state.iteration += 1;

            let cipher = ChaCha20Poly1305::new(Key::from_slice(&mk));
            let nonce = Nonce::from_slice(&msg.nonce);
            cipher.decrypt(nonce, msg.ciphertext.as_slice()).map_err(|_| SignalError::DecryptionFailed)
        }

        pub fn to_json(&self) -> super::Result<Vec<u8>> {
            serde_json::to_vec(self).map_err(|e| super::SignalError::Serialization(e.to_string()))
        }

        pub fn from_json(data: &[u8]) -> super::Result<Self> {
            serde_json::from_slice(data).map_err(|e| super::SignalError::Serialization(e.to_string()))
        }
    }

    fn advance_chain(ck: &mut [u8; 32]) -> [u8; 32] {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let mut mac1 = <HmacSha256 as Mac>::new_from_slice(ck).unwrap();
        mac1.update(&[0x01]);
        let mk = mac1.finalize().into_bytes();

        let mut mac2 = <HmacSha256 as Mac>::new_from_slice(ck).unwrap();
        mac2.update(&[0x02]);
        let new_ck = mac2.finalize().into_bytes();

        ck.copy_from_slice(&new_ck);
        let mut mk_arr = [0u8; 32];
        mk_arr.copy_from_slice(&mk);
        mk_arr
    }
}

pub mod store {
    use super::keys::{IdentityKeyPair, PreKeyBundle};
    use super::ratchet::RatchetSession;
    use super::{Result, SignalError};
    use std::sync::Arc;
    use yata_core::{BucketId, KvPutRequest, KvStore};
    use yata_kv::KvBucketStore;

    const SESSIONS_BUCKET: &str = "signal.sessions";
    const PREKEYS_BUCKET: &str = "signal.prekeys";

    pub struct SignalStore {
        kv: Arc<KvBucketStore>,
        pub identity: IdentityKeyPair,
    }

    impl SignalStore {
        pub async fn new(kv: Arc<KvBucketStore>, identity: IdentityKeyPair) -> Result<Self> {
            Ok(Self { kv, identity })
        }

        pub async fn save_session(&self, peer_did: &str, session: &RatchetSession) -> Result<()> {
            let cbor = session.to_cbor()?;
            self.kv.put(KvPutRequest {
                bucket: BucketId::from(SESSIONS_BUCKET),
                key: peer_did.to_owned(),
                value: bytes::Bytes::from(cbor),
                expected_revision: None,
                ttl_secs: None,
            }).await.map_err(SignalError::Storage)?;
            Ok(())
        }

        pub async fn load_session(&self, peer_did: &str) -> Result<Option<RatchetSession>> {
            let entry = self.kv.get(&BucketId::from(SESSIONS_BUCKET), peer_did).await
                .map_err(SignalError::Storage)?;
            match entry {
                None => Ok(None),
                Some(e) => RatchetSession::from_cbor(&e.value).map(Some),
            }
        }

        pub async fn save_pre_key_bundle(&self, bundle: &PreKeyBundle) -> Result<()> {
            let cbor = yata_cbor::encode(bundle)
                .map_err(|e| SignalError::Serialization(e.to_string()))?;
            self.kv.put(KvPutRequest {
                bucket: BucketId::from(PREKEYS_BUCKET),
                key: "current".to_owned(),
                value: bytes::Bytes::from(cbor),
                expected_revision: None,
                ttl_secs: None,
            }).await.map_err(SignalError::Storage)?;
            Ok(())
        }

        pub async fn load_pre_key_bundle(&self) -> Result<Option<PreKeyBundle>> {
            let entry = self.kv.get(&BucketId::from(PREKEYS_BUCKET), "current").await
                .map_err(SignalError::Storage)?;
            match entry {
                None => Ok(None),
                Some(e) => {
                    let bundle = yata_cbor::decode(&e.value)
                        .map_err(|e| SignalError::Serialization(e.to_string()))?;
                    Ok(Some(bundle))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

pub mod payload {
    use super::{Result, SignalError};
    use yata_core::PayloadRef;

    /// A Signal-protocol-encrypted YATA payload.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct SignalPayload {
        pub sender_did: String,
        pub session_id: String,
        pub message: super::ratchet::EncryptedMessage,
    }

    impl SignalPayload {
        pub fn to_bytes(&self) -> Result<Vec<u8>> {
            yata_cbor::encode(self).map_err(|e| SignalError::Serialization(e.to_string()))
        }
        pub fn from_bytes(data: &[u8]) -> Result<Self> {
            yata_cbor::decode(data).map_err(|e| SignalError::Serialization(e.to_string()))
        }
        pub fn as_payload_ref(&self) -> Result<PayloadRef> {
            let bytes = bytes::Bytes::from(self.to_bytes()?);
            Ok(PayloadRef::InlineBytes(bytes))
        }
    }
}

/// High-level synchronous API for the magatama host plugin.
/// All I/O uses JSON-serialized bytes for maximum WIT compatibility.
pub mod host_api {
    use super::keys::{DHKeyPair, IdentityKeyPair, OneTimePreKey, PreKeyBundle, SignedPreKey};
    use super::ratchet::RatchetSession;
    use super::group::GroupSession;
    use super::{Result, SignalError};
    use x25519_dalek::StaticSecret;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct SpkJson {
        key_id: u32,
        secret: [u8; 32],
        public: [u8; 32],
        signature: Vec<u8>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct OpkJson {
        key_id: u32,
        secret: [u8; 32],
        public: [u8; 32],
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct X3DHResultJson {
        shared_secret: [u8; 32],
        ad: Vec<u8>,
        init_msg: Option<super::x3dh::X3DHInitMessage>,
    }

    pub fn generate_identity() -> Result<Vec<u8>> {
        IdentityKeyPair::generate().to_bytes()
    }

    pub fn generate_signed_prekey(identity_cbor: &[u8], key_id: u32) -> Result<Vec<u8>> {
        let ik = IdentityKeyPair::from_bytes(identity_cbor)?;
        let spk = SignedPreKey::generate(&ik, key_id);
        let mut sig = vec![0u8; 64];
        sig.copy_from_slice(&spk.signature);
        let j = SpkJson {
            key_id: spk.key_id,
            secret: spk.key_pair.secret.to_bytes(),
            public: spk.key_pair.public_bytes(),
            signature: sig,
        };
        serde_json::to_vec(&j).map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn generate_one_time_prekey(key_id: u32) -> Result<Vec<u8>> {
        let opk = OneTimePreKey::generate(key_id);
        let j = OpkJson {
            key_id: opk.key_id,
            secret: opk.key_pair.secret.to_bytes(),
            public: opk.key_pair.public_bytes(),
        };
        serde_json::to_vec(&j).map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn build_pre_key_bundle(identity_cbor: &[u8], spk_json: &[u8], opk_json: Option<&[u8]>) -> Result<Vec<u8>> {
        let ik = IdentityKeyPair::from_bytes(identity_cbor)?;
        let spk: SpkJson = serde_json::from_slice(spk_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let sig: [u8; 64] = spk.signature.as_slice().try_into()
            .map_err(|_| SignalError::InvalidKey("signature must be 64 bytes".into()))?;
        let opk_fields = if let Some(opk_bytes) = opk_json {
            let opk: OpkJson = serde_json::from_slice(opk_bytes)
                .map_err(|e| SignalError::Serialization(e.to_string()))?;
            (Some(opk.key_id), Some(opk.public))
        } else {
            (None, None)
        };
        let bundle = PreKeyBundle {
            registration_id: ik.registration_id(),
            identity_key: ik.dh_public_bytes(),
            identity_sign_key: ik.sign_public_bytes(),
            spk_id: spk.key_id,
            spk_public: spk.public,
            spk_signature: sig,
            opk_id: opk_fields.0,
            opk_public: opk_fields.1,
        };
        serde_json::to_vec(&bundle).map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn x3dh_initiate(sender_ik_cbor: &[u8], bundle_json: &[u8]) -> Result<Vec<u8>> {
        let ik = IdentityKeyPair::from_bytes(sender_ik_cbor)?;
        let bundle: PreKeyBundle = serde_json::from_slice(bundle_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let (secret, init_msg) = super::x3dh::initiate(&ik, &bundle)?;
        let result = X3DHResultJson {
            shared_secret: secret.shared_secret,
            ad: secret.ad,
            init_msg: Some(init_msg),
        };
        serde_json::to_vec(&result).map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn x3dh_respond(recipient_ik_cbor: &[u8], spk_json: &[u8], opk_json: Option<&[u8]>, init_msg_json: &[u8]) -> Result<Vec<u8>> {
        let ik = IdentityKeyPair::from_bytes(recipient_ik_cbor)?;
        let spk_data: SpkJson = serde_json::from_slice(spk_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let sig: [u8; 64] = spk_data.signature.as_slice().try_into()
            .map_err(|_| SignalError::InvalidKey("signature must be 64 bytes".into()))?;
        let spk = SignedPreKey {
            key_id: spk_data.key_id,
            key_pair: DHKeyPair::from_secret_bytes(spk_data.secret),
            signature: sig,
        };
        let opk = if let Some(opk_bytes) = opk_json {
            let opk_data: OpkJson = serde_json::from_slice(opk_bytes)
                .map_err(|e| SignalError::Serialization(e.to_string()))?;
            Some(OneTimePreKey {
                key_id: opk_data.key_id,
                key_pair: DHKeyPair::from_secret_bytes(opk_data.secret),
            })
        } else {
            None
        };
        let init_msg: super::x3dh::X3DHInitMessage = serde_json::from_slice(init_msg_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let secret = super::x3dh::respond(&ik, &spk, opk.as_ref(), &init_msg)?;
        let result = X3DHResultJson {
            shared_secret: secret.shared_secret,
            ad: secret.ad,
            init_msg: None,
        };
        serde_json::to_vec(&result).map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn ratchet_init_sender(x3dh_result_json: &[u8], recipient_ratchet_public: [u8; 32]) -> Result<Vec<u8>> {
        let r: X3DHResultJson = serde_json::from_slice(x3dh_result_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let ss = super::x3dh::X3DHSharedSecret { shared_secret: r.shared_secret, ad: r.ad };
        let session = RatchetSession::init_sender(&ss, recipient_ratchet_public)?;
        session.to_cbor()
    }

    pub fn ratchet_init_receiver(x3dh_result_json: &[u8], our_ratchet_secret: [u8; 32]) -> Result<Vec<u8>> {
        let r: X3DHResultJson = serde_json::from_slice(x3dh_result_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let ss = super::x3dh::X3DHSharedSecret { shared_secret: r.shared_secret, ad: r.ad };
        let session = RatchetSession::init_receiver(&ss, StaticSecret::from(our_ratchet_secret))?;
        session.to_cbor()
    }

    pub fn ratchet_encrypt(session_cbor: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut session = RatchetSession::from_cbor(session_cbor)?;
        let msg = session.encrypt(plaintext)?;
        let session_bytes = session.to_cbor()?;
        #[derive(serde::Serialize)]
        struct Out { session: Vec<u8>, msg: super::ratchet::EncryptedMessage }
        serde_json::to_vec(&Out { session: session_bytes, msg })
            .map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn ratchet_decrypt(session_cbor: &[u8], msg_json: &[u8]) -> Result<Vec<u8>> {
        let mut session = RatchetSession::from_cbor(session_cbor)?;
        let msg: super::ratchet::EncryptedMessage = serde_json::from_slice(msg_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let plaintext = session.decrypt(&msg)?;
        let session_bytes = session.to_cbor()?;
        #[derive(serde::Serialize)]
        struct Out { session: Vec<u8>, plaintext: Vec<u8> }
        serde_json::to_vec(&Out { session: session_bytes, plaintext })
            .map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn group_init_sender(group_id: &str, our_did: &str) -> Result<Vec<u8>> {
        let mut gs = GroupSession::new(group_id, our_did);
        let dist = gs.init_sender()?;
        let session_json = gs.to_json()?;
        #[derive(serde::Serialize)]
        struct Out { session: Vec<u8>, distribution: super::group::SenderKeyDistribution }
        serde_json::to_vec(&Out { session: session_json, distribution: dist })
            .map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn group_process_distribution(session_json: &[u8], dist_json: &[u8]) -> Result<Vec<u8>> {
        let mut gs = GroupSession::from_json(session_json)?;
        let dist: super::group::SenderKeyDistribution = serde_json::from_slice(dist_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        gs.process_distribution(&dist)?;
        gs.to_json()
    }

    pub fn group_encrypt(session_json: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut gs = GroupSession::from_json(session_json)?;
        let msg = gs.encrypt(plaintext)?;
        let session_bytes = gs.to_json()?;
        #[derive(serde::Serialize)]
        struct Out { session: Vec<u8>, msg: super::group::SenderKeyMessage }
        serde_json::to_vec(&Out { session: session_bytes, msg })
            .map_err(|e| SignalError::Serialization(e.to_string()))
    }

    pub fn group_decrypt(session_json: &[u8], msg_json: &[u8]) -> Result<Vec<u8>> {
        let mut gs = GroupSession::from_json(session_json)?;
        let msg: super::group::SenderKeyMessage = serde_json::from_slice(msg_json)
            .map_err(|e| SignalError::Serialization(e.to_string()))?;
        let plaintext = gs.decrypt(&msg)?;
        let session_bytes = gs.to_json()?;
        #[derive(serde::Serialize)]
        struct Out { session: Vec<u8>, plaintext: Vec<u8> }
        serde_json::to_vec(&Out { session: session_bytes, plaintext })
            .map_err(|e| SignalError::Serialization(e.to_string()))
    }
}
