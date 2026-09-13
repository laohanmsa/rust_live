use anyhow::Result;
use axum::{Json, Router, http::StatusCode, routing::post};
use polym_rust_demo::{
    demo,
    exchange::Exchange,
    journal::{Journal, Stored},
    shadow_history::export_once,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Mutex;

#[tokio::test]
async fn signed_mock_order_retries_history_and_recovers_ack_without_resubmission() -> Result<()> {
    let mock = demo::MockExchange::start().await?;
    let exchange = Exchange::shadow(&mock.url).await?;
    let mut signal = demo::signal(&format!("shadow-{}", "a".repeat(64)));
    signal.token_id = "42".parse()?;
    let signed = exchange
        .sign_shadow(&signal, "10".parse()?, "0.01".parse()?, false)
        .await?;
    let mut reply: polym_rust_demo::Reply = serde_json::from_value(json!({
        "id":signal.id,"state":"prepared","reason":"","order_hash":signed.hash,
        "exchange_status":null,"policy_ms":0.1,"post_ms":null,"finalize_ms":null,
        "source_to_dispatch_ms":3.0,"queue_ms":0.1,"sign_ms":0.2,"journal_ms":0.3,
        "dispatch_ms":0.7,"total_ms":52.0
    }))?;
    let path = std::env::temp_dir().join(format!("shadow-history-{}.jsonl", std::process::id()));
    let scope = exchange.scope();
    let mut journal = Journal::open(&path, &scope)?;
    journal.prepare(
        Stored {
            signal: signal.clone(),
            reserved: "10".parse()?,
            order: signed.journal_order.clone(),
            reply: reply.clone(),
        },
        "100".parse()?,
    )?;
    let (status, _) = exchange.post(signed).await?;
    assert_eq!(status, 200);
    reply.state = "accepted".into();
    reply.post_ms = Some(51.0);
    journal.finish(reply)?;
    let journal = Arc::new(Mutex::new(journal));
    let bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let fail = Arc::new(AtomicBool::new(true));
    let (captured, fails) = (bodies.clone(), fail.clone());
    let router = Router::new().route(
        "/history",
        post(move |Json(body): Json<Value>| {
            let (captured, fails) = (captured.clone(), fails.clone());
            async move {
                captured.lock().await.push(body.clone());
                (
                    if fails.load(Ordering::SeqCst) {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        StatusCode::CREATED
                    },
                    Json(json!({"id":1,"signal_id":body["signal_id"],"dry_run":true})),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/history", listener.local_addr()?);
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    let http = reqwest::Client::new();
    assert!(export_once(&http, &url, &journal).await.is_err());
    assert!(journal.lock().await.exported.is_empty());
    fail.store(false, Ordering::SeqCst);
    assert_eq!(export_once(&http, &url, &journal).await?, 1);
    let sent = bodies.lock().await;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0], sent[1]);
    assert_eq!(sent[1]["dry_run"], true);
    assert_eq!(sent[1]["size"], "10");
    assert_eq!(sent[1]["token_id"], "42");
    assert!(sent[1].get("account_id").is_none());
    drop(sent);
    drop(journal);
    let recovered = Arc::new(Mutex::new(Journal::open(&path, &scope)?));
    assert_eq!(export_once(&http, &url, &recovered).await?, 0);
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    task.abort();
    drop(recovered);
    std::fs::remove_file(path.with_extension("history-acks.jsonl"))?;
    std::fs::remove_file(path)?;
    Ok(())
}
