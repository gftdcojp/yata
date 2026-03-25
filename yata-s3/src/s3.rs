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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> S3Client {
        S3Client::new(
            "https://abc123.r2.cloudflarestorage.com",
            "my-bucket",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "auto",
        )
    }

    // ── URL construction ────────────────────────────────────────────────

    #[test]
    fn url_basic() {
        let c = test_client();
        assert_eq!(
            c.url("snap/fragment/meta.json"),
            "https://abc123.r2.cloudflarestorage.com/my-bucket/snap/fragment/meta.json"
        );
    }

    #[test]
    fn url_empty_key() {
        let c = test_client();
        assert_eq!(
            c.url(""),
            "https://abc123.r2.cloudflarestorage.com/my-bucket/"
        );
    }

    #[test]
    fn url_trailing_slash_stripped_from_endpoint() {
        let c = S3Client::new(
            "https://example.com/",
            "b",
            "k",
            "s",
            "us-east-1",
        );
        // Trailing slash is stripped in constructor
        assert_eq!(c.url("key"), "https://example.com/b/key");
    }

    // ── Host extraction ─────────────────────────────────────────────────

    #[test]
    fn host_https() {
        let c = test_client();
        assert_eq!(c.host(), "abc123.r2.cloudflarestorage.com");
    }

    #[test]
    fn host_http() {
        let c = S3Client::new("http://localhost:9000", "b", "k", "s", "us-east-1");
        assert_eq!(c.host(), "localhost:9000");
    }

    #[test]
    fn host_no_scheme() {
        let c = S3Client::new("minio.local:9000", "b", "k", "s", "us-east-1");
        assert_eq!(c.host(), "minio.local:9000");
    }

    // ── EMPTY_SHA256 constant ───────────────────────────────────────────

    #[test]
    fn empty_sha256_constant_is_correct() {
        let hash = hex::encode(Sha256::digest(b""));
        assert_eq!(EMPTY_SHA256, hash);
    }

    // ── hmac_sha256 ─────────────────────────────────────────────────────

    #[test]
    fn hmac_sha256_deterministic() {
        let a = hmac_sha256(b"key", b"data");
        let b = hmac_sha256(b"key", b"data");
        assert_eq!(a, b);
    }

    #[test]
    fn hmac_sha256_different_keys_produce_different_results() {
        let a = hmac_sha256(b"key1", b"data");
        let b = hmac_sha256(b"key2", b"data");
        assert_ne!(a, b);
    }

    #[test]
    fn hmac_sha256_output_length() {
        let result = hmac_sha256(b"key", b"data");
        assert_eq!(result.len(), 32);
    }

    // ── derive_signing_key ──────────────────────────────────────────────

    #[test]
    fn derive_signing_key_deterministic() {
        let c = test_client();
        let a = c.derive_signing_key("20260325");
        let b = c.derive_signing_key("20260325");
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn derive_signing_key_different_dates() {
        let c = test_client();
        let a = c.derive_signing_key("20260325");
        let b = c.derive_signing_key("20260326");
        assert_ne!(a, b);
    }

    // ── sign_headers ────────────────────────────────────────────────────

    #[test]
    fn sign_headers_returns_four_headers() {
        let c = test_client();
        let now = chrono::Utc::now();
        let headers = c
            .sign_headers("GET", "test/key", &now, EMPTY_SHA256, None)
            .unwrap();
        assert_eq!(headers.len(), 4);
        let names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"host"));
        assert!(names.contains(&"x-amz-date"));
        assert!(names.contains(&"x-amz-content-sha256"));
        assert!(names.contains(&"authorization"));
    }

    #[test]
    fn sign_headers_authorization_format() {
        let c = test_client();
        let now = chrono::Utc::now();
        let headers = c
            .sign_headers("PUT", "obj", &now, EMPTY_SHA256, None)
            .unwrap();
        let auth = headers.iter().find(|(k, _)| k == "authorization").unwrap();
        assert!(auth.1.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"));
        assert!(auth.1.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(auth.1.contains("Signature="));
    }

    #[test]
    fn sign_headers_canonical_uri_encodes_segments() {
        let c = test_client();
        let now = chrono::Utc::now();
        // Should not panic on keys with special characters
        let headers = c
            .sign_headers("GET", "path/with spaces/key", &now, EMPTY_SHA256, None)
            .unwrap();
        assert_eq!(headers.len(), 4);
    }

    #[test]
    fn sign_headers_empty_key_uses_bucket_uri() {
        let c = test_client();
        let now = chrono::Utc::now();
        let query = "list-type=2&prefix=test";
        let headers = c
            .sign_headers("GET", "", &now, EMPTY_SHA256, Some(query))
            .unwrap();
        assert_eq!(headers.len(), 4);
    }

    #[test]
    fn sign_headers_deterministic_for_same_timestamp() {
        let c = test_client();
        let now = chrono::DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let a = c
            .sign_headers("GET", "key", &now, EMPTY_SHA256, None)
            .unwrap();
        let b = c
            .sign_headers("GET", "key", &now, EMPTY_SHA256, None)
            .unwrap();
        assert_eq!(a, b);
    }

    // ── extract_xml_tag ─────────────────────────────────────────────────

    #[test]
    fn extract_xml_tag_basic() {
        let xml = "<Root><Key>my-object</Key></Root>";
        assert_eq!(extract_xml_tag(xml, "Key"), Some("my-object".to_string()));
    }

    #[test]
    fn extract_xml_tag_missing() {
        let xml = "<Root><Key>val</Key></Root>";
        assert_eq!(extract_xml_tag(xml, "Size"), None);
    }

    #[test]
    fn extract_xml_tag_empty_value() {
        let xml = "<Root><Key></Key></Root>";
        assert_eq!(extract_xml_tag(xml, "Key"), Some("".to_string()));
    }

    #[test]
    fn extract_xml_tag_nested() {
        let xml = "<A><B><Key>inner</Key></B></A>";
        assert_eq!(extract_xml_tag(xml, "Key"), Some("inner".to_string()));
    }

    // ── parse_list_response ─────────────────────────────────────────────

    #[test]
    fn parse_list_response_single_object() {
        let xml = r#"<ListBucketResult>
            <Contents><Key>snap/fragment/meta.json</Key><Size>1024</Size></Contents>
        </ListBucketResult>"#;
        let (objects, next) = parse_list_response(xml);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key, "snap/fragment/meta.json");
        assert_eq!(objects[0].size, 1024);
        assert!(next.is_none());
    }

    #[test]
    fn parse_list_response_multiple_objects() {
        let xml = r#"<ListBucketResult>
            <Contents><Key>a</Key><Size>10</Size></Contents>
            <Contents><Key>b</Key><Size>20</Size></Contents>
            <Contents><Key>c</Key><Size>30</Size></Contents>
        </ListBucketResult>"#;
        let (objects, _) = parse_list_response(xml);
        assert_eq!(objects.len(), 3);
        assert_eq!(objects[0].key, "a");
        assert_eq!(objects[1].key, "b");
        assert_eq!(objects[2].key, "c");
        assert_eq!(objects[0].size, 10);
        assert_eq!(objects[2].size, 30);
    }

    #[test]
    fn parse_list_response_empty() {
        let xml = "<ListBucketResult></ListBucketResult>";
        let (objects, next) = parse_list_response(xml);
        assert!(objects.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn parse_list_response_with_continuation_token() {
        let xml = r#"<ListBucketResult>
            <Contents><Key>x</Key><Size>5</Size></Contents>
            <NextContinuationToken>abc123</NextContinuationToken>
        </ListBucketResult>"#;
        let (objects, next) = parse_list_response(xml);
        assert_eq!(objects.len(), 1);
        assert_eq!(next, Some("abc123".to_string()));
    }

    #[test]
    fn parse_list_response_missing_size_defaults_to_zero() {
        let xml = r#"<ListBucketResult>
            <Contents><Key>nosize</Key></Contents>
        </ListBucketResult>"#;
        let (objects, _) = parse_list_response(xml);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].size, 0);
    }

    // ── S3Error ─────────────────────────────────────────────────────────

    #[test]
    fn s3_error_display() {
        let err = S3Error::Remote("connection refused".into());
        assert_eq!(format!("{err}"), "S3 error: connection refused");
    }

    #[test]
    fn s3_error_debug() {
        let err = S3Error::Remote("test".into());
        let dbg = format!("{err:?}");
        assert!(dbg.contains("Remote"));
        assert!(dbg.contains("test"));
    }
}
