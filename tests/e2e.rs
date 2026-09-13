use polym_rust_demo::{Config, Service, Signal, demo, exchange::Exchange};
use serde_json::Value;
use std::{path::PathBuf, sync::atomic::Ordering, time::Duration};

struct Fixture {
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    worker: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn start(cfg: Config, mock: &demo::MockExchange) -> anyhow::Result<Self> {
        let service =
            Service::start(cfg, Exchange::demo(&mock.url).await?, demo::ACCESS.into()).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let server =
            tokio::spawn(async move { axum::serve(listener, service.router).await.unwrap() });
        let client = reqwest::Client::builder()
            .no_proxy()
            .pool_max_idle_per_host(0)
            .build()?;
        Ok(Self {
            url,
            client,
            server,
            worker: service.worker,
        })
    }
    async fn send(&self, s: &Signal) -> anyhow::Result<Value> {
        Ok(self
            .client
            .post(format!("{}/signal", self.url))
            .bearer_auth(demo::ACCESS)
            .json(s)
            .send()
            .await?
            .json()
            .await?)
    }
    async fn close(self) -> anyhow::Result<()> {
        drop(self.client);
        self.server.abort();
        let _ = self.server.await;
        tokio::time::timeout(Duration::from_secs(2), self.worker).await??;
        Ok(())
    }
}
fn directory() -> PathBuf {
    std::env::temp_dir().join(format!(
        "polym-demo-test-{}",
        polymarket_client_sdk_v2::auth::Uuid::new_v4()
    ))
}

#[tokio::test]
async fn http_signature_deduplication_policy_budget_and_restart() -> anyhow::Result<()> {
    let mock = demo::MockExchange::start().await?;
    let dir = directory();
    let mut cfg = demo::config(dir.join("orders.jsonl").to_string_lossy().into());
    cfg.total_budget_pusd = "2".parse()?;
    let app = Fixture::start(cfg.clone(), &mock).await?;
    let first = demo::signal("first");
    assert_eq!(
        app.client
            .post(format!("{}/signal", app.url))
            .json(&first)
            .send()
            .await?
            .status(),
        401
    );
    let accepted = app.send(&first).await?;
    assert_eq!(accepted["state"], "accepted", "{accepted}");
    assert_eq!(accepted["submitted_amount"], "0.99");
    assert!(accepted["dispatch_ms"].as_f64().is_some());
    assert_eq!(
        app.send(&first).await?["order_hash"],
        accepted["order_hash"]
    );
    let mut conflict = first.clone();
    conflict.ask = "0.51".parse()?;
    assert_eq!(app.send(&conflict).await?["state"], "conflict");
    for (id, reason) in [
        ("stale", "stale_signal"),
        ("book", "invalid_book"),
        ("edge", "edge_below_threshold"),
        ("price", "price_off_tick"),
        ("token", "token_not_allowed"),
    ] {
        let mut s = demo::signal(id);
        match id {
            "stale" => s.observed_at_ms -= 1000,
            "book" => s.book_valid = false,
            "edge" => s.fair_value = "0.50".parse()?,
            "price" => s.ask = "0.501".parse()?,
            _ => s.token_id = "3".parse()?,
        };
        assert_eq!(app.send(&s).await?["reason"], reason);
    }
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        app.send(&demo::signal("second")).await?["state"],
        "accepted"
    );
    assert_eq!(
        app.send(&demo::signal("third")).await?["reason"],
        "budget_exhausted"
    );
    assert_eq!(
        mock.state.public_reads.load(Ordering::SeqCst),
        3,
        "no metadata reads in order path"
    );
    app.close().await?;
    let app = Fixture::start(cfg, &mock).await?;
    assert_eq!(
        app.send(&first).await?["order_hash"],
        accepted["order_hash"]
    );
    assert_eq!(
        app.send(&demo::signal("fourth")).await?["reason"],
        "budget_exhausted"
    );
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 2);
    app.close().await?;
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn timeout_never_retries_and_keeps_budget_and_stop_across_restart() -> anyhow::Result<()> {
    let mock = demo::MockExchange::start().await?;
    mock.state.delay_ms.store(300, Ordering::SeqCst);
    let dir = directory();
    let mut cfg = demo::config(dir.join("orders.jsonl").to_string_lossy().into());
    cfg.request_timeout_ms = 100;
    cfg.max_inflight = 1;
    let app = Fixture::start(cfg.clone(), &mock).await?;
    let original = demo::signal("uncertain");
    let c = app.client.clone();
    let url = app.url.clone();
    let s = original.clone();
    let first = tokio::spawn(async move {
        c.post(format!("{url}/signal"))
            .bearer_auth(demo::ACCESS)
            .json(&s)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while mock.state.posts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    assert_eq!(app.send(&demo::signal("busy")).await?["state"], "busy");
    let result = first.await?;
    assert_eq!(result["state"], "unknown");
    assert_eq!(app.send(&original).await?["state"], "unknown");
    assert_eq!(app.send(&demo::signal("after")).await?["reason"], "stopped");
    app.close().await?;
    let app = Fixture::start(cfg, &mock).await?;
    assert_eq!(
        app.send(&demo::signal("after-restart")).await?["reason"],
        "stopped"
    );
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    app.close().await?;
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[test]
fn journal_lock_scope_and_torn_record_are_fail_closed() -> anyhow::Result<()> {
    use polym_rust_demo::journal::Journal;
    use std::io::Write;
    let dir = directory();
    let path = dir.join("orders.jsonl");
    let j = Journal::open(&path, "demo:a")?;
    assert!(Journal::open(&path, "demo:a").is_err());
    drop(j);
    assert!(Journal::open(&path, "live:a").is_err());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"{\"kind\":")?;
    assert!(Journal::open(&path, "demo:a").is_err());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[test]
fn request_auth_matches_official_sdk_known_vector() -> anyhow::Result<()> {
    // Public fixture from SDK 0.8.0 src/auth.rs, not an account credential.
    assert_eq!(
        polym_rust_demo::exchange::auth_signature(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "1000000",
            "test-sign",
            "/orders",
            br#"{"hash":"0x123"}"#,
        )?,
        "4gJVbox-R6XlDK4nlaicig0_ANVL1qdcahiL8CXfXLM="
    );
    Ok(())
}

#[tokio::test]
async fn crash_after_preparation_recovers_unknown_without_resending() -> anyhow::Result<()> {
    let mock = demo::MockExchange::start().await?;
    let dir = directory();
    let path = dir.join("orders.jsonl");
    let cfg = demo::config(path.to_string_lossy().into());
    let app = Fixture::start(cfg.clone(), &mock).await?;
    let s = demo::signal("crash");
    assert_eq!(app.send(&s).await?["state"], "accepted");
    app.close().await?;
    // Simulate a crash after submission but before the response was recorded.
    let log = std::fs::read_to_string(&path)?;
    let prepared = log.lines().take(2).collect::<Vec<_>>().join("\n") + "\n";
    std::fs::write(&path, prepared)?;
    let app = Fixture::start(cfg, &mock).await?;
    assert_eq!(app.send(&s).await?["state"], "unknown");
    assert_eq!(app.send(&demo::signal("new")).await?["reason"], "stopped");
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    app.close().await?;
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn metrics_cover_rejections_and_exclude_replays_from_latency() -> anyhow::Result<()> {
    let mock = demo::MockExchange::start().await?;
    let dir = directory();
    let app = Fixture::start(
        demo::config(dir.join("orders.jsonl").to_string_lossy().into()),
        &mock,
    )
    .await?;
    let s = demo::signal("observed");
    assert_eq!(app.send(&s).await?["state"], "accepted");
    app.send(&s).await?;
    let mut stale = demo::signal("stale-observed");
    stale.observed_at_ms -= 1000;
    app.send(&stale).await?;
    let response = app
        .client
        .get(format!("{}/metrics", app.url))
        .bearer_auth(demo::ACCESS)
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    let metrics: Value = response.json().await?;
    assert_eq!(metrics["received"], 3);
    assert_eq!(metrics["completed"], 3);
    assert_eq!(metrics["replayed"], 1);
    assert_eq!(metrics["latency_ms"]["dispatch_ms"]["n"], 1);
    assert_eq!(metrics["latency_ms"]["post_ms"]["n"], 1);
    assert_eq!(metrics["reasons"]["stale_signal"], 1);
    app.close().await?;
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
