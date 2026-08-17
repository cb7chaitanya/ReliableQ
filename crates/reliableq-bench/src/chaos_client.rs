//! Thin client for `fake-charge`'s `/v1/test/*` control surface
//! (`crates/fake-charge/src/chaos.rs`), reused as-is by every scenario
//! that needs deterministic downstream latency/failure injection or the
//! `peak_inflight` bounded-concurrency evidence.

use serde_json::{Value, json};

#[derive(Clone)]
pub struct ChaosClient {
    base_url: String,
    client: reqwest::Client,
}

impl ChaosClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn control(&self, body: Value) -> anyhow::Result<()> {
        let resp = self
            .client
            .post(format!("{}/v1/test/control", self.base_url))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("chaos control returned {}", resp.status());
        }
        Ok(())
    }

    pub async fn set_normal(&self) -> anyhow::Result<()> {
        self.control(json!({"mode": "normal"})).await
    }

    pub async fn set_delay_ms(&self, ms: u64) -> anyhow::Result<()> {
        self.control(json!({"mode": "delay_ms", "ms": ms})).await
    }

    pub async fn set_fail_rate(&self, rate: f64, status: u16, seed: u64) -> anyhow::Result<()> {
        self.control(json!({"mode": "fail_rate", "rate": rate, "status": status, "seed": seed}))
            .await
    }

    pub async fn peak_inflight(&self) -> anyhow::Result<u64> {
        let resp: Value = self
            .client
            .get(format!("{}/v1/test/inflight", self.base_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp["peak_inflight"].as_u64().unwrap_or(0))
    }

    pub async fn reset_inflight(&self) -> anyhow::Result<()> {
        let resp = self
            .client
            .post(format!("{}/v1/test/inflight/reset", self.base_url))
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("inflight reset returned {}", resp.status());
        }
        Ok(())
    }
}
