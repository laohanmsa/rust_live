//! Opt-in integration test against an isolated loopback NATS broker, never production.
use anyhow::{Result, ensure};
use axum::{Json, Router, http::StatusCode, routing::get};
use polym_rust_demo::{
    now_ms,
    trading_control::{SUBJECT, TradingControl},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

async fn until(mut condition: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires RUST_CONTROL_TEST_NATS_URL pointing at an isolated loopback broker"]
async fn control_broadcast_reaches_both_lanes_and_snapshot_repairs_missed_events() -> Result<()> {
    let url = std::env::var("RUST_CONTROL_TEST_NATS_URL")?;
    let parsed = reqwest::Url::parse(&url)?;
    ensure!(
        matches!(parsed.host_str(), Some("127.0.0.1" | "localhost")),
        "test broker must be loopback"
    );
    let enabled = Arc::new(AtomicBool::new(false));
    let available = Arc::new(AtomicBool::new(true));
    let (state, up) = (enabled.clone(), available.clone());
    let router = Router::new().route("/api/trading-control/", get(move || {
        let (state, up) = (state.clone(), up.clone());
        async move {
            (if up.load(Ordering::SeqCst) {StatusCode::OK} else {StatusCode::SERVICE_UNAVAILABLE},
             Json(json!({"schema_version":1,"source":"dashboard","dry_run_enabled":state.load(Ordering::SeqCst),"triggered_at_ms":now_ms()})))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let django = format!("http://{}/api/trading-context/", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let a = Arc::new(TradingControl::default());
    let b = Arc::new(TradingControl::default());
    let http = reqwest::Client::builder().no_proxy().build()?;
    let first = tokio::spawn(a.clone().run(url.clone(), django.clone(), http.clone()));
    let second = tokio::spawn(b.clone().run(url.clone(), django.clone(), http.clone()));
    until(|| !a.dry_run() && !b.dry_run()).await?;
    let client = async_nats::connect(&url).await?;
    let payload = |enabled| {
        serde_json::to_vec(&json!({"schema_version":1,"source":"dashboard","dry_run_enabled":enabled,"triggered_at_ms":now_ms()})).unwrap()
    };
    client.publish(SUBJECT, payload(true).into()).await?;
    client.flush().await?;
    until(|| a.dry_run() && b.dry_run()).await?;
    client.publish(SUBJECT, payload(false).into()).await?;
    client.flush().await?;
    // A subscriber arriving after the Core NATS message must recover via snapshot.
    enabled.store(true, Ordering::SeqCst);
    let c = Arc::new(TradingControl::default());
    let third = tokio::spawn(c.clone().run(url.clone(), django.clone(), http.clone()));
    until(|| c.latched()).await?;
    enabled.store(false, Ordering::SeqCst);
    // An unlatched connection also becomes safe when the authoritative endpoint fails.
    let d = Arc::new(TradingControl::default());
    let fourth = tokio::spawn(d.clone().run(url, django, http));
    until(|| !d.dry_run()).await?;
    available.store(false, Ordering::SeqCst);
    until(|| d.dry_run()).await?;
    assert!(a.dry_run() && b.dry_run() && c.dry_run());
    for task in [first, second, third, fourth] {
        task.abort();
    }
    server.abort();
    Ok(())
}
