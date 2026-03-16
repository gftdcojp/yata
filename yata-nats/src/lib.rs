#![allow(dead_code)]

//! yata-nats — NATS JetStream backend for yata storage primitives.
//!
//! Implements `AppendLog`, `KvStore`, `ObjectStorage` from yata-core over
//! NATS JetStream, with Arrow IPC as the native payload wire format.
//!
//! ## Stream mapping
//!
//! | yata concept     | NATS JetStream primitive |
//! |------------------|--------------------------|
//! | `AppendLog`      | JetStream stream         |
//! | `KvStore`        | JetStream KV bucket      |
//! | `ObjectStorage`  | JetStream Object Store   |
//! | Arrow payload    | Message payload (IPC bytes) |
//!
//! ## Arrow payload protocol
//!
//! All Arrow RecordBatch payloads are transmitted as Arrow IPC stream format
//! bytes. This is the same format LanceDB uses for ingestion, making the
//! payloads directly compatible with `lancedb::Table::add()`.
//!
//! Subject naming: `yata.<stream>.<subject>` with payload kind header.

pub mod config;
pub mod log;
pub mod kv;
pub mod object;
pub mod arrow_payload;
pub mod lance_writer;
pub mod consumer;
pub mod cdc;

pub use config::NatsConfig;
pub use log::NatsAppendLog;
pub use kv::NatsKvStore;
pub use object::NatsObjectStore;
pub use arrow_payload::NatsArrowPublisher;
pub use lance_writer::NatsLanceWriter;
pub use consumer::NatsConsumerGroup;
pub use cdc::{NatsCdcPublisher, NatsCdcConsumer};

/// Shared NATS connection handle.
#[derive(Clone)]
pub struct NatsBackend {
    pub client: async_nats::Client,
    pub jetstream: async_nats::jetstream::Context,
}

impl NatsBackend {
    pub async fn connect(config: &NatsConfig) -> Result<Self, async_nats::ConnectError> {
        let mut opts = async_nats::ConnectOptions::new();
        if let Some(ref user) = config.user {
            if let Some(ref pass) = config.password {
                opts = opts.user_and_password(user.clone(), pass.clone());
            }
        }
        if let Some(ref token) = config.token {
            opts = opts.token(token.clone());
        }
        if let Some(ref name) = config.client_name {
            opts = opts.name(name);
        }

        let client = opts.connect(&config.url).await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self { client, jetstream })
    }

    pub fn append_log(&self) -> NatsAppendLog {
        NatsAppendLog::new(self.jetstream.clone())
    }

    pub fn kv_store(&self) -> NatsKvStore {
        NatsKvStore::new(self.jetstream.clone())
    }

    pub fn object_store(&self) -> NatsObjectStore {
        NatsObjectStore::new(self.jetstream.clone())
    }

    pub fn arrow_publisher(&self) -> NatsArrowPublisher {
        NatsArrowPublisher::new(self.jetstream.clone())
    }

    pub fn consumer_group(&self) -> NatsConsumerGroup {
        NatsConsumerGroup::new(self.jetstream.clone())
    }

    pub fn cdc_publisher(&self) -> NatsCdcPublisher {
        NatsCdcPublisher::new(self.jetstream.clone())
    }

    pub fn cdc_consumer(&self) -> NatsCdcConsumer {
        NatsCdcConsumer::new(self.jetstream.clone())
    }
}
