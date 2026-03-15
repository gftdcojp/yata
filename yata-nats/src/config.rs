#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NatsConfig {
    /// NATS server URL (e.g. "nats://localhost:4222").
    pub url: String,
    /// Optional username for auth.
    pub user: Option<String>,
    /// Optional password for auth.
    pub password: Option<String>,
    /// Optional token for auth.
    pub token: Option<String>,
    /// Optional client name.
    pub client_name: Option<String>,
    /// JetStream domain (for leaf nodes / hub-spoke).
    pub js_domain: Option<String>,
    /// Subject prefix for all yata streams (default: "yata").
    pub subject_prefix: Option<String>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://localhost:4222".into(),
            user: None,
            password: None,
            token: None,
            client_name: Some("yata".into()),
            js_domain: None,
            subject_prefix: Some("yata".into()),
        }
    }
}

impl NatsConfig {
    pub fn prefix(&self) -> &str {
        self.subject_prefix.as_deref().unwrap_or("yata")
    }
}
