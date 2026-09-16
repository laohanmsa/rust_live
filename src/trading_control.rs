//! One-way dashboard dry-run latch, shared by both trading lanes.
use anyhow::{Result, ensure};
use futures_util::StreamExt;
use serde::Deserialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

pub const SUBJECT: &str = "trading.control.dry_run";

#[derive(Deserialize)]
struct Snapshot {
    schema_version: u64,
    source: String,
    dry_run_enabled: bool,
    triggered_at_ms: u64,
}

#[derive(Default)]
pub struct TradingControl {
    latched: AtomicBool,
    checked_at_ms: AtomicU64,
}

impl TradingControl {
    pub fn latched(&self) -> bool {
        self.latched.load(Ordering::SeqCst)
    }

    pub fn dry_run(&self) -> bool {
        self.latched()
            || crate::now_ms().abs_diff(self.checked_at_ms.load(Ordering::SeqCst)) > 15_000
    }

    pub fn apply(&self, payload: &[u8]) -> Result<()> {
        ensure!(payload.len() <= 4096, "oversized trading control");
        let snapshot: Snapshot = serde_json::from_slice(payload)?;
        ensure!(
            snapshot.schema_version == 1
                && snapshot.source == "dashboard"
                && snapshot.triggered_at_ms > 0,
            "invalid trading control"
        );
        if snapshot.dry_run_enabled && !self.latched.swap(true, Ordering::SeqCst) {
            eprintln!(
                "{}",
                serde_json::json!({"event":"dashboard_dry_run_latched","at_ms":crate::now_ms()})
            );
        }
        Ok(())
    }

    pub(crate) fn confirm_snapshot(&self, payload: &[u8]) -> Result<()> {
        let snapshot: Snapshot = serde_json::from_slice(payload)?;
        ensure!(
            crate::now_ms().abs_diff(snapshot.triggered_at_ms) <= 15_000,
            "stale control snapshot"
        );
        self.apply(payload)?;
        self.checked_at_ms.store(crate::now_ms(), Ordering::SeqCst);
        Ok(())
    }

    pub async fn run(self: Arc<Self>, nats_url: String, django_url: String, http: reqwest::Client) {
        loop {
            if self.session(&nats_url, &django_url, &http).await.is_err() {
                eprintln!("trading_control_unavailable: new live submissions disabled");
            }
            self.checked_at_ms.store(0, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn session(
        self: &Arc<Self>,
        nats_url: &str,
        django_url: &str,
        http: &reqwest::Client,
    ) -> Result<()> {
        let (closed, mut disconnected) = tokio::sync::watch::channel(false);
        let control = self.clone();
        let client = async_nats::ConnectOptions::new()
            .connection_timeout(Duration::from_secs(2))
            .event_callback(move |event| {
                let control = control.clone();
                let closed = closed.clone();
                async move {
                    if matches!(
                        event,
                        async_nats::Event::Disconnected | async_nats::Event::Closed
                    ) {
                        control.checked_at_ms.store(0, Ordering::SeqCst);
                        let _ = closed.send(true);
                    }
                }
            })
            .connect(nats_url)
            .await?;
        // Every sidecar gets its own copy. Never use a queue subscription here.
        let mut sub = client.subscribe(SUBJECT).await?;
        client.flush().await?;
        let url = reqwest::Url::parse(django_url)?.join("/api/trading-control/")?;
        let snapshots = async {
            loop {
                let payload = http
                    .get(url.clone())
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;
                // This lease is renewed only by a successful authoritative read.
                self.confirm_snapshot(&payload)?;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        let messages = async {
            while let Some(message) = sub.next().await {
                self.apply(&message.payload)?;
            }
            anyhow::bail!("control subscription ended")
        };
        tokio::select! {
            biased;
            _ = disconnected.changed() => anyhow::bail!("control disconnected"),
            result = messages => result,
            result = snapshots => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_stale_and_latched_states_cannot_submit() -> Result<()> {
        let control = TradingControl::default();
        assert!(control.dry_run());
        let payload = |enabled| {
            serde_json::to_vec(&serde_json::json!({
            "schema_version":1,"source":"dashboard","dry_run_enabled":enabled,"triggered_at_ms":1
        })).unwrap()
        };
        control.apply(&payload(false))?;
        assert!(
            control.dry_run(),
            "a bus message cannot authorize live trading"
        );
        control
            .checked_at_ms
            .store(crate::now_ms(), Ordering::SeqCst);
        assert!(!control.dry_run());
        control
            .checked_at_ms
            .store(crate::now_ms() - 15_001, Ordering::SeqCst);
        assert!(control.dry_run());
        control
            .checked_at_ms
            .store(crate::now_ms(), Ordering::SeqCst);
        control.apply(&payload(true))?;
        control.apply(&payload(false))?;
        assert!(control.dry_run(), "false cannot release a latched stop");
        assert!(
            control
                .apply(br#"{"schema_version":1,"dry_run_enabled":"true"}"#)
                .is_err()
        );
        Ok(())
    }
}
