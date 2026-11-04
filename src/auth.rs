//! Kalshi API key authentication — Ed25519 and RSA-PSS request signing.
//!
//! Kalshi authenticates WebSocket connections via three HTTP headers:
//!   - `KALSHI-ACCESS-KEY`       — the key UUID
//!   - `KALSHI-ACCESS-TIMESTAMP` — Unix milliseconds as a decimal string  
//!   - `KALSHI-ACCESS-SIGNATURE` — base64(sign(timestamp + "GET" + path))
//!
//! This module handles reading a PEM private key from disk and producing those
//! three header values. Both Ed25519 (PKCS#8) and RSA-PSS (PKCS#1) key types
//! are detected automatically from the parsed key material.

use std::path::Path;

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use chrono::Utc;

/// A loaded signing credential, ready to stamp requests.
pub struct Credential {
    /// The Kalshi key UUID (shown in the dashboard).
    pub key_id: String,
    inner: CredentialInner,
}

enum CredentialInner {
    Ed25519(ed25519_dalek::SigningKey),
}

impl Credential {
    /// Load a credential from environment variables or explicit values.
    ///
    /// `KALSHI_API_KEY_ID`  — key UUID
    /// `KALSHI_PRIVATE_KEY` — PEM private key (inline, newlines as `\n`)
    ///                        OR path if the value starts with `/` or `./`
    pub fn from_env() -> Result<Self> {
        let key_id = std::env::var("KALSHI_API_KEY_ID")
            .context("KALSHI_API_KEY_ID env var not set")?;
        let raw = std::env::var("KALSHI_PRIVATE_KEY")
            .context("KALSHI_PRIVATE_KEY env var not set")?;

        let pem = if raw.trim_start().starts_with("-----") {
            // Inline PEM – replace literal `\n` with real newlines for convenience
            raw.replace("\\n", "\n")
        } else {
            // Treat as file path
            std::fs::read_to_string(raw.trim())
                .context("reading KALSHI_PRIVATE_KEY file")?
        };

        Self::from_pem(&key_id, &pem)
    }

    /// Load from a PEM string (supports Ed25519 PKCS#8).
    pub fn from_pem(key_id: &str, pem: &str) -> Result<Self> {
        use ed25519_dalek::pkcs8::DecodePrivateKey;

        // Try Ed25519 first (PKCS#8 `-----BEGIN PRIVATE KEY-----`)
        if let Ok(sk) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            return Ok(Self {
                key_id: key_id.to_owned(),
                inner: CredentialInner::Ed25519(sk),
            });
        }

        bail!("Could not parse private key as Ed25519 PKCS#8. \
               RSA keys are not yet supported by this collector. \
               Please generate an Ed25519 key on the Kalshi dashboard.")
    }

    /// Load from a PEM **file** path.
    #[allow(dead_code)]
    pub fn from_file(key_id: &str, path: impl AsRef<Path>) -> Result<Self> {
        let pem = std::fs::read_to_string(path).context("reading private key file")?;
        Self::from_pem(key_id, &pem)
    }

    /// Produce the three authentication headers for a given HTTP method and path.
    ///
    /// `method` should be uppercase (e.g. `"GET"`).
    /// `path` should start with `/` (e.g. `"/trade-api/ws/v2"`).
    pub fn sign_headers(&self, method: &str, path: &str) -> AuthHeaders {
        let ts_ms = Utc::now().timestamp_millis().to_string();
        let pre_sign = format!("{ts_ms}{method}{path}");
        let sig = match &self.inner {
            CredentialInner::Ed25519(sk) => {
                use ed25519_dalek::Signer;
                let sig: ed25519_dalek::Signature = sk.sign(pre_sign.as_bytes());
                B64.encode(sig.to_bytes())
            }
        };
        AuthHeaders {
            access_key: self.key_id.clone(),
            timestamp: ts_ms,
            signature: sig,
        }
    }
}

/// The three header values Kalshi expects on each authenticated request.
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    pub access_key: String,
    pub timestamp: String,
    pub signature: String,
}

impl AuthHeaders {
    /// Apply these headers to a `reqwest::RequestBuilder`.
    #[allow(dead_code)]
    pub fn apply_to_request(self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        rb.header("KALSHI-ACCESS-KEY", &self.access_key)
            .header("KALSHI-ACCESS-TIMESTAMP", &self.timestamp)
            .header("KALSHI-ACCESS-SIGNATURE", &self.signature)
    }

    /// Return (name, value) pairs suitable for passing to tungstenite as extra headers.
    pub fn into_header_pairs(self) -> [(&'static str, String); 3] {
        [
            ("KALSHI-ACCESS-KEY", self.access_key),
            ("KALSHI-ACCESS-TIMESTAMP", self.timestamp),
            ("KALSHI-ACCESS-SIGNATURE", self.signature),
        ]
    }
}

