//! Export the existing durable simulation journal after submission, off the hot path.
use crate::journal::{Journal, Stored};
use anyhow::{Context, Result, ensure};
use polymarket_client_sdk_v2::types::Decimal;
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio::sync::Mutex;

pub fn payload(entry: &Stored) -> Result<Value> {
    let r = &entry.reply;
    let amount = entry.order["takerAmount"]
        .as_str()
        .context("missing signed share amount")?;
    let shares = Decimal::from_str(amount)? / Decimal::from(1_000_000);
    let submitted = r.submitted_at_ms.or_else(|| {
        r.source_to_dispatch_ms
            .map(|elapsed| entry.signal.observed_at_ms + elapsed as u64)
    });
    Ok(json!({
        "schema_version":1,"dry_run":true,"signal_id":entry.signal.id,
        "token_id":entry.signal.token_id.to_string(),"price":entry.signal.ask.to_string(),
        "size":shares.normalize().to_string(),"signal_at_ms":entry.signal.observed_at_ms,
        "submitted_at_ms":submitted,"input_kind":"live_ober","result":r.state,
        "reason":r.reason,"order_hash":r.order_hash,
        "timings":{"policy_ms":r.policy_ms,"sign_ms":r.sign_ms,"journal_ms":r.journal_ms,
            "dispatch_ms":r.dispatch_ms,"source_to_dispatch_ms":r.source_to_dispatch_ms,
            "post_ms":r.post_ms,"total_ms":r.total_ms}
    }))
}

pub async fn export_once(
    http: &reqwest::Client,
    url: &str,
    journal: &Arc<Mutex<Journal>>,
) -> Result<usize> {
    // ponytail: scan at most 50,000 bounded journal rows; add a pending index if profiling warrants it.
    let batch = {
        let ledger = journal.lock().await;
        ledger
            .orders
            .values()
            .filter(|o| {
                o.signal.id.starts_with("shadow-")
                    && matches!(o.reply.state.as_str(), "accepted" | "unknown")
                    && !ledger.exported.contains(&o.signal.id)
            })
            .take(25)
            .cloned()
            .collect::<Vec<_>>()
    };
    let mut count = 0;
    for entry in batch {
        let body = payload(&entry)?;
        let response: Value = http
            .post(url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            response["dry_run"] == true
                && response["signal_id"] == entry.signal.id
                && response["id"].as_u64().is_some(),
            "invalid history acknowledgement"
        );
        let ledger = journal.clone();
        tokio::task::spawn_blocking(move || ledger.blocking_lock().ack_history(&entry.signal.id))
            .await??;
        count += 1;
    }
    Ok(count)
}
