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
    let mut body = json!({
        "schema_version":1,"dry_run":true,"signal_id":entry.signal.id,
        "token_id":entry.signal.token_id.to_string(),"price":entry.signal.ask.to_string(),
        "size":shares.normalize().to_string(),"signal_at_ms":entry.signal.observed_at_ms,
        "submitted_at_ms":submitted,"input_kind":"live_ober","result":r.state,
        "reason":r.reason,"order_hash":r.order_hash,
        "timings":{"policy_ms":r.policy_ms,"sign_ms":r.sign_ms,"journal_ms":r.journal_ms,
            "dispatch_ms":r.dispatch_ms,"source_to_dispatch_ms":r.source_to_dispatch_ms,
            "post_ms":r.post_ms,"total_ms":r.total_ms}
    });
    if let Some(uma) = &r.uma {
        body["input_kind"] = json!("uma_propose");
        body["market_id"] = uma["event"]["market_id"].clone();
        body["request_id"] = uma["event"]["request_id"].clone();
        body["uma"] = uma.clone();
    }
    // Old journal entries omit this field, preserving their receipt retry digest.
    if (entry.signal.id.starts_with("live-") || r.uma.is_some())
        && let Some(cash) = r.submitted_amount
    {
        body["submitted_amount"] = json!(cash.normalize().to_string());
    }
    Ok(body)
}

pub async fn export_once(
    http: &reqwest::Client,
    url: &str,
    journal: &Arc<Mutex<Journal>>,
) -> Result<usize> {
    export(http, url, journal, None).await
}

pub async fn export_live_once(
    http: &reqwest::Client,
    url: &str,
    journal: &Arc<Mutex<Journal>>,
    account: &str,
    token: &str,
) -> Result<usize> {
    export(http, url, journal, Some((account, token))).await
}

async fn export(
    http: &reqwest::Client,
    url: &str,
    journal: &Arc<Mutex<Journal>>,
    live: Option<(&str, &str)>,
) -> Result<usize> {
    let ledger = journal.clone();
    let is_live = live.is_some();
    let batch = tokio::task::spawn_blocking(move || ledger.blocking_lock().pending_batch(is_live))
        .await??;
    let mut count = 0;
    for entry in batch {
        let mut body = payload(&entry)?;
        let mut request = http.post(url);
        if let Some((account, token)) = live {
            body["dry_run"] = json!(false);
            body["account_name"] = json!(account);
            body["clob_response"] = entry
                .reply
                .clob_response
                .clone()
                .unwrap_or_else(|| json!({}));
            request = request.bearer_auth(token);
        }
        let response: Value = request
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            response["dry_run"] == live.is_none()
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
