#![allow(dead_code)]

pub use types::*;
pub use client::{AtClient, AtError, resolve_did, resolve_handle};
pub use firehose::AtFirehose;
pub use bridge::AtFirehoseBridge;

pub mod types {
    /// AT Protocol DID identifier.
    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct AtDid(pub String);
    impl AtDid {
        pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
        pub fn as_str(&self) -> &str { &self.0 }
    }
    impl std::fmt::Display for AtDid {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
    }
    impl From<String> for AtDid { fn from(s: String) -> Self { Self(s) } }
    impl From<&str> for AtDid { fn from(s: &str) -> Self { Self(s.to_owned()) } }

    /// AT Protocol record key.
    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct AtRkey(pub String);
    impl AtRkey { pub fn as_str(&self) -> &str { &self.0 } }
    impl std::fmt::Display for AtRkey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
    }

    /// AT Protocol collection NSID.
    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct AtCollection(pub String);
    impl AtCollection { pub fn as_str(&self) -> &str { &self.0 } }
    impl std::fmt::Display for AtCollection {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
    }

    /// AT Protocol URI: at://<did>/<collection>/<rkey>
    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct AtUri {
        pub did: AtDid,
        pub collection: AtCollection,
        pub rkey: AtRkey,
    }
    impl AtUri {
        pub fn new(did: impl Into<AtDid>, collection: impl Into<String>, rkey: impl Into<String>) -> Self {
            Self { did: did.into(), collection: AtCollection(collection.into()), rkey: AtRkey(rkey.into()) }
        }
        pub fn parse(s: &str) -> Option<Self> {
            let s = s.strip_prefix("at://")?;
            let mut parts = s.splitn(3, '/');
            let did = parts.next()?.to_owned();
            let collection = parts.next()?.to_owned();
            let rkey = parts.next()?.to_owned();
            Some(Self { did: AtDid(did), collection: AtCollection(collection), rkey: AtRkey(rkey) })
        }
    }
    impl std::fmt::Display for AtUri {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "at://{}/{}/{}", self.did, self.collection, self.rkey)
        }
    }

    /// A decoded AT Protocol repository operation from the firehose.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct AtRepoOp {
        pub action: String,
        pub path: String,
        pub cid: Option<Vec<u8>>,
    }
    impl AtRepoOp {
        pub fn collection(&self) -> &str {
            self.path.splitn(2, '/').next().unwrap_or("")
        }
        pub fn rkey(&self) -> &str {
            let mut parts = self.path.splitn(2, '/');
            parts.next();
            parts.next().unwrap_or("")
        }
    }

    /// A decoded AT Protocol commit event from the firehose.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct AtCommit {
        pub seq: i64,
        pub repo: String,
        pub rev: String,
        pub ops: Vec<AtRepoOp>,
        pub blocks: Vec<u8>,
        pub ts: String,
    }

    /// A fully resolved AT Protocol record.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct AtRecord {
        pub uri: AtUri,
        pub cid: String,
        pub value: serde_json::Value,
    }

    /// DID document.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct DIDDocument {
        #[serde(rename = "@context", default)]
        pub context: Vec<String>,
        pub id: String,
        #[serde(rename = "alsoKnownAs", default)]
        pub also_known_as: Vec<String>,
        #[serde(rename = "verificationMethod", default)]
        pub verification_method: Vec<serde_json::Value>,
        #[serde(default)]
        pub service: Vec<DIDService>,
    }
    impl DIDDocument {
        pub fn pds_endpoint(&self) -> Option<&str> {
            self.service.iter()
                .find(|s| s.service_type == "AtprotoPersonalDataServer")
                .map(|s| s.service_endpoint.as_str())
        }
        pub fn handle(&self) -> Option<&str> {
            self.also_known_as.iter()
                .find(|s| s.starts_with("at://"))
                .map(|s| s.trim_start_matches("at://"))
        }
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct DIDService {
        pub id: String,
        #[serde(rename = "type")]
        pub service_type: String,
        #[serde(rename = "serviceEndpoint")]
        pub service_endpoint: String,
    }
}

pub mod client {
    use super::types::*;
    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use tokio::sync::RwLock;

    #[derive(thiserror::Error, Debug)]
    pub enum AtError {
        #[error("xrpc error {status}: {code} — {message}")]
        Xrpc { status: u16, code: String, message: String },
        #[error("http error: {0}")]
        Http(#[from] reqwest::Error),
        #[error("json error: {0}")]
        Json(#[from] serde_json::Error),
        #[error("{0}")]
        Other(String),
    }

    pub type Result<T> = std::result::Result<T, AtError>;

    /// AT Protocol XRPC client.
    pub struct AtClient {
        host: String,
        http: reqwest::Client,
        access_jwt: RwLock<Option<String>>,
        refresh_jwt: RwLock<Option<String>>,
        did: RwLock<Option<String>>,
    }

    impl AtClient {
        pub fn new(host: &str) -> Self {
            Self {
                host: host.trim_end_matches('/').to_owned(),
                http: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .unwrap(),
                access_jwt: RwLock::new(None),
                refresh_jwt: RwLock::new(None),
                did: RwLock::new(None),
            }
        }

        pub async fn did(&self) -> Option<String> {
            self.did.read().await.clone()
        }

        pub async fn create_session(&self, identifier: &str, password: &str) -> Result<()> {
            #[derive(serde::Serialize)]
            struct Req<'a> { identifier: &'a str, password: &'a str }
            #[derive(serde::Deserialize)]
            struct Resp {
                #[serde(rename = "accessJwt")] access_jwt: String,
                #[serde(rename = "refreshJwt")] refresh_jwt: String,
                did: String,
            }
            let resp: Resp = self.procedure("com.atproto.server.createSession", &Req { identifier, password }).await?;
            *self.access_jwt.write().await = Some(resp.access_jwt);
            *self.refresh_jwt.write().await = Some(resp.refresh_jwt);
            *self.did.write().await = Some(resp.did);
            Ok(())
        }

        pub async fn refresh_session(&self) -> Result<()> {
            let refresh = self.refresh_jwt.read().await.clone()
                .ok_or_else(|| AtError::Other("no refresh token".into()))?;
            #[derive(serde::Deserialize)]
            struct Resp {
                #[serde(rename = "accessJwt")] access_jwt: String,
                #[serde(rename = "refreshJwt")] refresh_jwt: String,
            }
            let url = format!("{}/xrpc/com.atproto.server.refreshSession", self.host);
            let resp = self.http.post(&url)
                .header(AUTHORIZATION, format!("Bearer {}", refresh))
                .send().await?
                .error_for_status()?
                .json::<Resp>().await?;
            *self.access_jwt.write().await = Some(resp.access_jwt);
            *self.refresh_jwt.write().await = Some(resp.refresh_jwt);
            Ok(())
        }

        pub async fn query<T: serde::de::DeserializeOwned>(&self, nsid: &str, params: &[(&str, &str)]) -> Result<T> {
            let url = format!("{}/xrpc/{}", self.host, nsid);
            let mut req = self.http.get(&url);
            if !params.is_empty() { req = req.query(params); }
            if let Some(jwt) = self.access_jwt.read().await.as_deref() {
                req = req.header(AUTHORIZATION, format!("Bearer {}", jwt));
            }
            let resp = req.send().await?;
            if !resp.status().is_success() {
                let status = resp.status().as_u16();
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(AtError::Xrpc {
                    status,
                    code: body.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown").to_owned(),
                    message: body.get("message").and_then(|v| v.as_str()).unwrap_or("").to_owned(),
                });
            }
            Ok(resp.json::<T>().await?)
        }

        pub async fn procedure<B: serde::Serialize, T: serde::de::DeserializeOwned>(&self, nsid: &str, body: &B) -> Result<T> {
            let url = format!("{}/xrpc/{}", self.host, nsid);
            let mut req = self.http.post(&url)
                .header(CONTENT_TYPE, "application/json")
                .json(body);
            if let Some(jwt) = self.access_jwt.read().await.as_deref() {
                req = req.header(AUTHORIZATION, format!("Bearer {}", jwt));
            }
            let resp = req.send().await?;
            if !resp.status().is_success() {
                let status = resp.status().as_u16();
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(AtError::Xrpc {
                    status,
                    code: body.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown").to_owned(),
                    message: body.get("message").and_then(|v| v.as_str()).unwrap_or("").to_owned(),
                });
            }
            Ok(resp.json::<T>().await?)
        }

        pub async fn create_record(&self, repo: &str, collection: &str, rkey: Option<&str>, record: &serde_json::Value) -> Result<AtRecord> {
            let mut body = serde_json::json!({ "repo": repo, "collection": collection, "record": record });
            if let Some(k) = rkey { body["rkey"] = serde_json::json!(k); }
            #[derive(serde::Deserialize)]
            struct Resp { uri: String, cid: String }
            let resp: Resp = self.procedure("com.atproto.repo.createRecord", &body).await?;
            let uri = AtUri::parse(&resp.uri)
                .ok_or_else(|| AtError::Other(format!("invalid AT URI: {}", resp.uri)))?;
            Ok(AtRecord { uri, cid: resp.cid, value: record.clone() })
        }

        pub async fn get_record(&self, uri: &AtUri) -> Result<AtRecord> {
            #[derive(serde::Deserialize)]
            struct Resp { uri: String, cid: String, value: serde_json::Value }
            let resp: Resp = self.query("com.atproto.repo.getRecord", &[
                ("repo", uri.did.as_str()),
                ("collection", uri.collection.as_str()),
                ("rkey", uri.rkey.as_str()),
            ]).await?;
            let parsed_uri = AtUri::parse(&resp.uri).unwrap_or_else(|| uri.clone());
            Ok(AtRecord { uri: parsed_uri, cid: resp.cid, value: resp.value })
        }

        pub async fn delete_record(&self, uri: &AtUri) -> Result<()> {
            let body = serde_json::json!({
                "repo": uri.did.as_str(),
                "collection": uri.collection.as_str(),
                "rkey": uri.rkey.as_str()
            });
            self.procedure::<_, serde_json::Value>("com.atproto.repo.deleteRecord", &body).await?;
            Ok(())
        }
    }

    /// Resolve a DID document from plc.directory or did:web.
    pub async fn resolve_did(did: &str) -> Result<DIDDocument> {
        let url = if did.starts_with("did:plc:") {
            format!("https://plc.directory/{}", did)
        } else if did.starts_with("did:web:") {
            let host = did.trim_start_matches("did:web:");
            format!("https://{}/.well-known/did.json", host)
        } else {
            return Err(AtError::Other(format!("unsupported DID method: {}", did)));
        };
        let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?;
        Ok(client.get(&url).send().await?.error_for_status()?.json::<DIDDocument>().await?)
    }

    /// Resolve an AT Protocol handle to a DID using the given PDS.
    pub async fn resolve_handle(pds_url: &str, handle: &str) -> Result<String> {
        let c = AtClient::new(pds_url);
        #[derive(serde::Deserialize)]
        struct Resp { did: String }
        let resp: Resp = c.query("com.atproto.identity.resolveHandle", &[("handle", handle)]).await?;
        if resp.did.is_empty() {
            return Err(AtError::Other(format!("handle {} resolved to empty DID", handle)));
        }
        Ok(resp.did)
    }
}

pub mod firehose {
    use super::types::{AtCommit, AtRepoOp};
    use futures::{StreamExt, SinkExt};
    use tokio_tungstenite::tungstenite::Message;

    #[derive(thiserror::Error, Debug)]
    pub enum FirehoseError {
        #[error("websocket error: {0}")]
        Ws(#[from] tokio_tungstenite::tungstenite::Error),
        #[error("cbor decode error: {0}")]
        Cbor(String),
        #[error("connect error: {0}")]
        Connect(String),
    }

    pub type Result<T> = std::result::Result<T, FirehoseError>;

    /// AT Protocol firehose subscriber (com.atproto.sync.subscribeRepos).
    pub struct AtFirehose {
        relay_url: String,
    }

    impl AtFirehose {
        pub fn new(relay_url: &str) -> Self {
            Self { relay_url: relay_url.trim_end_matches('/').to_owned() }
        }

        /// Subscribe to the firehose. Returns a stream of decoded AtCommit events.
        /// `cursor = None` starts from the current head.
        pub async fn subscribe(
            &self,
            cursor: Option<i64>,
        ) -> Result<impl futures::Stream<Item = Result<AtCommit>> + Send + '_> {
            let mut url = format!("{}/xrpc/com.atproto.sync.subscribeRepos", self.relay_url);
            if let Some(c) = cursor {
                url = format!("{}?cursor={}", url, c);
            }
            let (ws, _) = tokio_tungstenite::connect_async(&url).await
                .map_err(|e| FirehoseError::Connect(e.to_string()))?;
            let (_, read) = ws.split();
            let stream = read.filter_map(|msg| async move {
                match msg {
                    Ok(Message::Binary(data)) => Some(decode_firehose_message(&data)),
                    Ok(Message::Close(_)) => None,
                    Ok(_) => None,
                    Err(e) => Some(Err(FirehoseError::Ws(e))),
                }
            });
            Ok(stream)
        }
    }

    /// Decode a raw WebSocket binary message from the AT Protocol firehose.
    /// Format: two concatenated CBOR items — {op, t} header and body.
    pub fn decode_firehose_message(data: &[u8]) -> Result<AtCommit> {
        use ciborium::value::Value;
        use std::io::Cursor;

        // Parse the first CBOR value (header map)
        let mut cursor = Cursor::new(data);
        let header_val: Value = ciborium::from_reader(&mut cursor)
            .map_err(|e: ciborium::de::Error<std::io::Error>| FirehoseError::Cbor(e.to_string()))?;

        let t_str = match &header_val {
            Value::Map(entries) => {
                entries.iter().find_map(|(k, v)| {
                    if let Value::Text(key) = k {
                        if key == "t" {
                            if let Value::Text(t) = v { return Some(t.clone()); }
                        }
                    }
                    None
                })
            }
            _ => None,
        };

        let t = t_str.unwrap_or_default();
        if t != "#commit" {
            return Ok(AtCommit {
                seq: 0,
                repo: String::new(),
                rev: String::new(),
                ops: vec![],
                blocks: vec![],
                ts: String::new(),
            });
        }

        // Parse the body CBOR value
        let body_val: Value = ciborium::from_reader(&mut cursor)
            .map_err(|e: ciborium::de::Error<std::io::Error>| FirehoseError::Cbor(e.to_string()))?;

        fn get_str(map: &[(Value, Value)], key: &str) -> String {
            map.iter().find_map(|(k, v)| {
                if let Value::Text(kstr) = k {
                    if kstr == key {
                        if let Value::Text(s) = v { return Some(s.clone()); }
                    }
                }
                None
            }).unwrap_or_default()
        }
        fn get_i64(map: &[(Value, Value)], key: &str) -> i64 {
            map.iter().find_map(|(k, v)| {
                if let Value::Text(kstr) = k {
                    if kstr == key {
                        match v {
                            Value::Integer(i) => return Some(i128::from(*i) as i64),
                            _ => {}
                        }
                    }
                }
                None
            }).unwrap_or(0)
        }

        let entries = match &body_val {
            Value::Map(m) => m.clone(),
            _ => return Err(FirehoseError::Cbor("body is not a map".into())),
        };

        let seq = get_i64(&entries, "seq");
        let repo = get_str(&entries, "repo");
        let rev = get_str(&entries, "rev");
        let ts = get_str(&entries, "time");

        let blocks = entries.iter().find_map(|(k, v)| {
            if let Value::Text(kstr) = k {
                if kstr == "blocks" {
                    if let Value::Bytes(b) = v { return Some(b.clone()); }
                }
            }
            None
        }).unwrap_or_default();

        let ops_val = entries.iter().find_map(|(k, v)| {
            if let Value::Text(kstr) = k {
                if kstr == "ops" {
                    if let Value::Array(arr) = v { return Some(arr.clone()); }
                }
            }
            None
        }).unwrap_or_default();

        let ops = ops_val.into_iter().filter_map(|op_val| {
            if let Value::Map(op_map) = op_val {
                let action = get_str(&op_map, "action");
                let path = get_str(&op_map, "path");
                let cid = op_map.iter().find_map(|(k, v)| {
                    if let Value::Text(kstr) = k {
                        if kstr == "cid" {
                            if let Value::Bytes(b) = v { return Some(b.clone()); }
                        }
                    }
                    None
                });
                if !action.is_empty() && !path.is_empty() {
                    return Some(AtRepoOp { action, path, cid });
                }
            }
            None
        }).collect();

        Ok(AtCommit { seq, repo, rev, ops, blocks, ts })
    }
}

pub mod bridge {
    use super::firehose::AtFirehose;
    use std::sync::Arc;
    use yata_core::{Blake3Hash, Envelope, PayloadRef, PublishRequest, AppendLog, SchemaId, StreamId, Subject};
    use yata_server::Broker;

    /// Bridges the AT Protocol firehose into YATA streams.
    ///
    /// Subject format: `at.<collection>.<rkey>`
    /// Stream: `<stream_prefix>` (default: "at.firehose")
    pub struct AtFirehoseBridge {
        pub firehose: AtFirehose,
        pub broker: Arc<Broker>,
        /// Filter to these collections (empty = all).
        pub collections: Vec<String>,
        pub stream_prefix: String,
    }

    impl AtFirehoseBridge {
        pub fn new(relay_url: &str, broker: Arc<Broker>) -> Self {
            Self {
                firehose: AtFirehose::new(relay_url),
                broker,
                collections: vec![],
                stream_prefix: "at.firehose".to_owned(),
            }
        }

        pub fn with_collections(mut self, collections: Vec<String>) -> Self {
            self.collections = collections;
            self
        }

        pub fn with_stream_prefix(mut self, prefix: impl Into<String>) -> Self {
            self.stream_prefix = prefix.into();
            self
        }

        /// Run the bridge: subscribe to firehose and publish matching events to YATA.
        /// Runs until the stream ends or an error occurs.
        pub async fn run(&self, cursor: Option<i64>) -> yata_core::Result<u64> {
            use futures::StreamExt;

            let stream = self.firehose.subscribe(cursor).await
                .map_err(|e| yata_core::YataError::Storage(e.to_string()))?;
            futures::pin_mut!(stream);

            let col_filter: std::collections::HashSet<&str> = self.collections.iter().map(|s| s.as_str()).collect();
            let mut published = 0u64;

            while let Some(result) = stream.next().await {
                let commit = match result {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("firehose decode error: {}", e);
                        continue;
                    }
                };
                if commit.repo.is_empty() { continue; }

                for op in &commit.ops {
                    let collection = op.collection();
                    if !col_filter.is_empty() && !col_filter.contains(collection) {
                        continue;
                    }
                    let rkey = op.rkey();
                    let subject = Subject(format!("at.{}.{}", collection, rkey));
                    let stream_id = StreamId::from(self.stream_prefix.as_str());

                    let payload_bytes = serde_json::to_vec(&serde_json::json!({
                        "seq": commit.seq,
                        "repo": commit.repo,
                        "rev": commit.rev,
                        "action": op.action,
                        "collection": collection,
                        "rkey": rkey,
                        "cid": op.cid.as_ref().map(|b| hex::encode(b)),
                        "ts": commit.ts,
                    })).unwrap_or_default();

                    let content_hash = Blake3Hash::of(&payload_bytes);
                    let schema_id = SchemaId::from("at.firehose.commit");
                    let mut envelope = Envelope::new(subject.clone(), schema_id, content_hash);
                    envelope.headers.insert("at.repo".to_owned(), commit.repo.clone());
                    envelope.headers.insert("at.collection".to_owned(), collection.to_owned());
                    envelope.headers.insert("at.rkey".to_owned(), rkey.to_owned());
                    envelope.headers.insert("at.action".to_owned(), op.action.clone());

                    let req = PublishRequest {
                        stream: stream_id,
                        subject,
                        envelope,
                        payload: PayloadRef::InlineBytes(bytes::Bytes::from(payload_bytes)),
                        expected_last_seq: None,
                    };

                    if let Err(e) = self.broker.log.append(req).await {
                        tracing::warn!("failed to publish AT commit to YATA: {}", e);
                    } else {
                        published += 1;
                    }
                }
            }
            Ok(published)
        }
    }
}
