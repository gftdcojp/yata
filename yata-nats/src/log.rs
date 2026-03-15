//! NATS JetStream AppendLog implementation.
//!
//! Each yata StreamId maps to a JetStream stream.
//! Subjects: `yata.log.<stream_id>.<subject>`

use async_trait::async_trait;
use futures::StreamExt;
use std::pin::Pin;
use yata_core::{
    Ack, AppendLog, Blake3Hash, LogEntry, PayloadKind, PublishRequest,
    Result, Sequence, StreamId, Subject, YataError,
};

fn time_to_nanos(t: time::OffsetDateTime) -> i64 {
    t.unix_timestamp_nanos() as i64
}

/// Header key for payload kind discriminator.
const HDR_PAYLOAD_KIND: &str = "Yata-Payload-Kind";
/// Header key for envelope hash.
const HDR_ENVELOPE_HASH: &str = "Yata-Envelope-Hash";
/// Header key for message id.
const HDR_MESSAGE_ID: &str = "Yata-Message-Id";
/// Header key for payload ref string.
const HDR_PAYLOAD_REF: &str = "Yata-Payload-Ref";

pub struct NatsAppendLog {
    js: async_nats::jetstream::Context,
}

impl NatsAppendLog {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    fn stream_name(stream: &StreamId) -> String {
        format!("YATA_LOG_{}", stream.0.replace('.', "_").replace('-', "_"))
    }

    fn subject(stream: &StreamId, subject: &Subject) -> String {
        format!(
            "yata.log.{}.{}",
            stream.0.replace('.', "-"),
            subject.0.replace('.', "-")
        )
    }

    async fn ensure_stream(
        &self,
        stream: &StreamId,
    ) -> std::result::Result<async_nats::jetstream::stream::Stream, YataError> {
        let name = Self::stream_name(stream);
        let subjects = vec![format!(
            "yata.log.{}.>",
            stream.0.replace('.', "-")
        )];

        let config = async_nats::jetstream::stream::Config {
            name: name.clone(),
            subjects,
            retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
            storage: async_nats::jetstream::stream::StorageType::File,
            ..Default::default()
        };

        self.js
            .get_or_create_stream(config)
            .await
            .map_err(|e| YataError::Storage(format!("jetstream ensure_stream: {e}")))
    }
}

#[async_trait]
impl AppendLog for NatsAppendLog {
    async fn append(&self, req: PublishRequest) -> Result<Ack> {
        self.ensure_stream(&req.stream).await?;

        let subject = Self::subject(&req.stream, &req.subject);

        // Encode envelope + payload metadata as NATS headers
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            HDR_MESSAGE_ID,
            req.envelope.message_id.to_string().as_str(),
        );
        headers.insert(
            HDR_ENVELOPE_HASH,
            req.envelope.content_hash.hex().as_str(),
        );
        headers.insert(
            HDR_PAYLOAD_KIND,
            match req.payload.kind() {
                PayloadKind::InlineBytes => "inline",
                PayloadKind::ArrowIpc => "arrow",
                PayloadKind::Blob => "blob",
                PayloadKind::Manifest => "manifest",
            },
        );
        headers.insert(HDR_PAYLOAD_REF, req.payload.to_ref_str().as_str());
        for (k, v) in &req.envelope.headers {
            headers.insert(k.as_str(), v.as_str());
        }

        // Payload is the message body (Arrow IPC bytes or inline bytes)
        let body: bytes::Bytes = match &req.payload {
            yata_core::PayloadRef::InlineBytes(b) => b.clone(),
            yata_core::PayloadRef::ArrowIpc(b) => b.clone(),
            yata_core::PayloadRef::Blob(_) => bytes::Bytes::new(),
            yata_core::PayloadRef::Manifest(_) => bytes::Bytes::new(),
        };

        let ack = self
            .js
            .publish_with_headers(subject, headers, body)
            .await
            .map_err(|e| YataError::Storage(format!("nats publish: {e}")))?
            .await
            .map_err(|e| YataError::Storage(format!("nats ack: {e}")))?;

        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Ok(Ack {
            message_id: req.envelope.message_id,
            stream_id: req.stream,
            seq: Sequence(ack.sequence),
            ts_ns,
        })
    }

    async fn read_from(
        &self,
        stream: &StreamId,
        from_seq: Sequence,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<LogEntry>> + Send>>> {
        let js_stream = self.ensure_stream(stream).await?;

        let deliver_policy = if from_seq.0 > 0 {
            async_nats::jetstream::consumer::DeliverPolicy::ByStartSequence {
                start_sequence: from_seq.0,
            }
        } else {
            async_nats::jetstream::consumer::DeliverPolicy::All
        };

        let consumer = js_stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                deliver_policy,
                ..Default::default()
            })
            .await
            .map_err(|e| YataError::Storage(format!("create consumer: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| YataError::Storage(format!("consumer messages: {e}")))?;

        let stream_id = stream.clone();
        let mapped = messages.map(move |msg_result| {
            let msg = msg_result.map_err(|e| YataError::Storage(format!("msg recv: {e}")))?;

            let headers = msg.headers.as_ref();
            let message_id_str = headers
                .and_then(|h| h.get(HDR_MESSAGE_ID))
                .map(|v| v.to_string())
                .unwrap_or_default();
            let envelope_hash = headers
                .and_then(|h| h.get(HDR_ENVELOPE_HASH))
                .map(|v| v.to_string())
                .unwrap_or_default();
            let payload_kind_str = headers
                .and_then(|h| h.get(HDR_PAYLOAD_KIND))
                .map(|v| v.to_string())
                .unwrap_or_else(|| "inline".into());
            let payload_ref_str = headers
                .and_then(|h| h.get(HDR_PAYLOAD_REF))
                .map(|v| v.to_string())
                .unwrap_or_default();

            let payload_kind = match payload_kind_str.as_str() {
                "arrow" => PayloadKind::ArrowIpc,
                "blob" => PayloadKind::Blob,
                "manifest" => PayloadKind::Manifest,
                _ => PayloadKind::InlineBytes,
            };

            let info = msg.info().map_err(|e| YataError::Storage(format!("msg info: {e}")))?;
            let seq = Sequence(info.stream_sequence);
            let ts_ns = time_to_nanos(info.published);

            // Extract user headers (skip Yata-* headers)
            let mut user_headers = indexmap::IndexMap::new();
            if let Some(h) = headers {
                for (k, values) in h.iter() {
                    let key = k.to_string();
                    if !key.starts_with("Yata-") {
                        if let Some(v) = values.iter().next() {
                            user_headers.insert(key, v.to_string());
                        }
                    }
                }
            }

            let subject_str = msg.subject.to_string();
            // Parse subject: yata.log.<stream>.<subject>
            let subject_part = subject_str
                .strip_prefix("yata.log.")
                .and_then(|s| s.split_once('.'))
                .map(|(_, subj)| subj.to_owned())
                .unwrap_or(subject_str.clone());

            Ok(LogEntry {
                seq,
                stream_id: stream_id.clone(),
                subject: Subject(subject_part),
                ts_ns,
                envelope_hash: envelope_hash
                    .parse()
                    .unwrap_or(Blake3Hash([0u8; 32])),
                payload_kind,
                payload_ref_str,
                headers: user_headers,
            })
        });

        Ok(Box::pin(mapped))
    }

    async fn last_seq(&self, stream: &StreamId) -> Result<Option<Sequence>> {
        let name = Self::stream_name(stream);
        match self.js.get_stream(&name).await {
            Ok(mut stream) => {
                let info = stream
                    .info()
                    .await
                    .map_err(|e| YataError::Storage(format!("stream info: {e}")))?;
                if info.state.messages == 0 {
                    Ok(None)
                } else {
                    Ok(Some(Sequence(info.state.last_sequence)))
                }
            }
            Err(_) => Ok(None),
        }
    }
}
