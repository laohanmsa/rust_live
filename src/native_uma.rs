//! Gap-aware consumer for the dedicated UMA feed. Snapshots replace all transient proofs.
use crate::{
    now_ms,
    shadow_state::{Lifecycle, MarketContext, Resolution},
    uma::Event,
};
use anyhow::{Context, Result, ensure};
use serde_json::Value;

pub fn snapshot(value: &Value) -> Result<(String, u64, Lifecycle)> {
    let h = &value["health"];
    ensure!(
        h["schema_version"] == 1 && h["ready"] == true,
        "UMA snapshot not ready"
    );
    ensure!(
        now_ms().abs_diff(h["captured_at_ms"].as_u64().context("snapshot time")?) < 5000,
        "stale UMA snapshot"
    );
    let epoch = h["epoch"]
        .as_str()
        .filter(|v| !v.is_empty())
        .context("missing feed epoch")?
        .to_owned();
    let sequence = h["sequence"].as_u64().context("missing sequence")?;
    let events: Vec<Event> = serde_json::from_value(value["events"].clone())?;
    ensure!(events.len() <= 100_000, "UMA snapshot capacity");
    let mut life = Lifecycle::default();
    let mut previous = None;
    for event in events {
        event.validate()?;
        let position = (event.block_number, event.log_index);
        ensure!(previous.is_none_or(|p| p < position), "unsorted snapshot");
        previous = Some(position);
        life.apply(&event.channel, &serde_json::to_value(&event)?);
    }
    Ok((epoch, sequence, life))
}
pub fn next_sequence(epoch: &str, sequence: u64, msg: &Value) -> Result<bool> {
    ensure!(msg["epoch"].as_str() == Some(epoch), "UMA feed restarted");
    let incoming = msg["sequence"].as_u64().context("missing sequence")?;
    match msg["kind"].as_str() {
        Some("reset") => anyhow::bail!("UMA rescan required"),
        Some("heartbeat") => {
            ensure!(incoming == sequence, "UMA heartbeat exposes a gap");
            Ok(false)
        }
        Some("event") => {
            if incoming <= sequence {
                return Ok(false);
            }
            ensure!(Some(incoming) == sequence.checked_add(1), "UMA event gap");
            Ok(true)
        }
        _ => anyhow::bail!("unsupported feed message"),
    }
}
pub fn overlay(context: &mut MarketContext, life: &Lifecycle, now: u64) {
    context.eligible = false;
    let Some(mark) = life.marks.get(&context.market_id) else {
        return;
    };
    let Some(at) = mark.proposed_at_ms else {
        return;
    };
    if mark.status != "proposed"
        || mark.request.is_empty()
        || now.saturating_sub(at) > 10_800_000
        || at > now + 15_000
        || life.has_dispute(&context.market_id)
        || context.has_disputed_resolution
    {
        return;
    }
    if let Some(old) = &context.resolution {
        let newest = old
            .block_number
            .unwrap_or(0)
            .max(old.dispute_block_number.unwrap_or(0))
            .max(old.settle_block_number.unwrap_or(0));
        if newest > mark.block || (newest == mark.block && (old.disputed || old.settled)) {
            return;
        }
    }
    if context
        .settled_request_blocks
        .get(&mark.request)
        .is_some_and(|b| *b >= mark.block)
    {
        return;
    }
    let Some(price) = mark.proposed_price else {
        return;
    };
    context.resolution = Some(Resolution {
        id: 0,
        request_id: Some(mark.request.clone()),
        status: "proposed".into(),
        proposed_price: price,
        propose_time_ms: at,
        block_number: Some(mark.block),
        dispute_block_number: None,
        settle_block_number: None,
        disputed: false,
        settled: false,
    });
    context.eligible = true;
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn gaps_restarts_and_reorgs_require_a_snapshot_but_duplicates_do_not() {
        assert!(next_sequence("a", 3, &json!({"epoch":"a","sequence":4,"kind":"event"})).unwrap());
        assert!(!next_sequence("a", 3, &json!({"epoch":"a","sequence":2,"kind":"event"})).unwrap());
        for value in [
            json!({"epoch":"b","sequence":4,"kind":"event"}),
            json!({"epoch":"a","sequence":5,"kind":"event"}),
            json!({"epoch":"a","sequence":4,"kind":"heartbeat"}),
            json!({"epoch":"a","sequence":3,"kind":"reset"}),
        ] {
            assert!(next_sequence("a", 3, &value).is_err());
        }
    }
}
