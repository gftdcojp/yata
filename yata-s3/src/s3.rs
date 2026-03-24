//! Minimal S3 client using ureq (pure sync) + SigV4 signing.
//!
//! Uses `ureq` instead of `reqwest` to avoid tokio runtime deadlocks
//! in Cloudflare Container environments where `block_on` nests.

use std::io::Read;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
}

/// S3-compatible client using ureq (pure sync, no tokio dependency).
#[derive(Clone)]
pub struct S3Client {
    endpoint: String,
    bucket: String,
    key_id: String,
    secret_key: String,
    region: String,
}

impl S3Client {
    pub fn new(endpoint: &str, bucket: &str, key_id: &str, secret_key: &str, region: &str) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            bucket: bucket.to_string(),
            key_id: key_id.to_string(),
            secret_key: secret_key.to_string(),
            region: region.to_string(),
        }
    }

    fn url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, key)
    }

    fn host(&self) -> &str {
        self.endpoint
            .strip_prefix("https://")
            .or_else(|| self.endpoint.strip_prefix("http://"))
            .unwrap_or(&self.endpoint)
    }

    /// PUT an object (sync).
    pub fn put_sync(&self, key: &str, data: Bytes) -> Result<(), S3Error> {
        let now = chrono::Utc::now();
        let payload_hash = hex::encode(Sha256::digest(&data));
        let headers = self.sign_headers("PUT", key, &now, &payload_hash, None)?;

        let mut req = ureq::put(&self.url(key));
        for (k, v) in &headers {
            req = req.set(k, v);
        }
        req = req.set("content-length", &data.len().to_string());
        match req.send_bytes(&data) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(S3Error::Remote(format!("PUT {key}: {code} {body}")))
            }
            Err(e) => Err(S3Error::Remote(e.to_string())),
        }
    }

    /// GET an object (sync). Returns None if 404.
    pub fn get_sync(&self, key: &str) -> Result<Option<Bytes>, S3Error> {
        let now = chrono::Utc::now();
        let headers = self.sign_headers("GET", key, &now, EMPTY_SHA256, None)?;

        let mut req = ureq::get(&self.url(key));
        for (k, v) in &headers {
            req = req.set(k, v);
        }
        match req.call() {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader().read_to_end(&mut buf)
                    .map_err(|e| S3Error::Remote(e.to_string()))?;
                Ok(Some(Bytes::from(buf)))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(S3Error::Remote(format!("GET {key}: {code} {body}")))
            }
            Err(e) => Err(S3Error::Remote(e.to_string())),
        }
    }

    /// HEAD (sync).
    pub fn head_sync(&self, key: &str) -> Result<bool, S3Error> {
        let now = chrono::Utc::now();
        let headers = self.sign_headers("HEAD", key, &now, EMPTY_SHA256, None)?;

        let mut req = ureq::head(&self.url(key));
        for (k, v) in &headers {
            req = req.set(k, v);
        }
        match req.call() {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(S3Error::Remote(e.to_string())),
        }
    }

    /// List objects (sync).
    pub fn list_sync(&self, prefix: &str) -> Result<Vec<S3Object>, S3Error> {
        let query = format!("list-type=2&prefix={}", urlencoding::encode(prefix));
        let url = format!("{}/{}?{}", self.endpoint, self.bucket, query);
        let now = chrono::Utc::now();
        let headers = self.sign_headers("GET", "", &now, EMPTY_SHA256, Some(&query))?;

        let mut req = ureq::get(&url);
        for (k, v) in &headers {
            req = req.set(k, v);
        }
        match req.call() {
            Ok(resp) => {
                let body = resp.into_string().unwrap_or_default();
                let (items, _) = parse_list_response(&body);
                Ok(items)
            }
            Err(e) => Err(S3Error::Remote(e.to_string())),
        }
    }

    // ── Async wrappers (backward compat) ──

    pub async fn put(&self, key: &str, data: Bytes) -> Result<(), S3Error> { self.put_sync(key, data) }
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, S3Error> { self.get_sync(key) }
    pub async fn head(&self, key: &str) -> Result<bool, S3Error> { self.head_sync(key) }
    pub async fn list(&self, prefix: &str) -> Result<Vec<S3Object>, S3Error> { self.list_sync(prefix) }

    // ── SigV4 signing ──

    fn sign_headers(
        &self, method: &str, key: &str, now: &chrono::DateTime<chrono::Utc>,
        payload_hash: &str, query_string: Option<&str>,
    ) -> Result<Vec<(String, String)>, S3Error> {
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let host = self.host();
        let canonical_uri = if key.is_empty() {
            format!("/{}/", self.bucket)
        } else {
            format!("/{}/{}", self.bucket,
                key.split('/').map(|s| urlencoding::encode(s).into_owned()).collect::<Vec<_>>().join("/"))
        };
        let canonical_querystring = query_string.unwrap_or("");
        let canonical_headers = format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!("{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
        let credential_scope = format!("{date_stamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes())));
        let signing_key = self.derive_signing_key(&date_stamp);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.key_id);
        Ok(vec![
            ("host".into(), host.into()),
            ("x-amz-date".into(), amz_date),
            ("x-amz-content-sha256".into(), payload_hash.into()),
            ("authorization".into(), authorization),
        ])
    }

    fn derive_signing_key(&self, date_stamp: &str) -> Vec<u8> {
        let k_date = hmac_sha256(format!("AWS4{}", self.secret_key).as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }
}

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn parse_list_response(xml: &str) -> (Vec<S3Object>, Option<String>) {
    let mut objects = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Contents>") {
        let after = &rest[start + 10..];
        let end = after.find("</Contents>").unwrap_or(after.len());
        let block = &after[..end];
        rest = &after[end..];
        let key = extract_xml_tag(block, "Key");
        let size = extract_xml_tag(block, "Size").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        if let Some(key) = key { objects.push(S3Object { key, size }); }
    }
    let next_token = extract_xml_tag(xml, "NextContinuationToken");
    (objects, next_token)
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

#[derive(Debug)]
pub enum S3Error {
    Remote(String),
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { S3Error::Remote(msg) => write!(f, "S3 error: {msg}") }
    }
}

impl std::error::Error for S3Error {}
