use anyhow::{Result, ensure};
use polym_rust_demo::{Config, Service, Signal, demo, exchange::Exchange, now_ms};
use serde_json::{Value, json};
use std::{path::Path, sync::atomic::Ordering, time::Duration};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str).unwrap_or("demo") {
        "demo" => run_demo(args.get(1).map(|x| x.parse()).transpose()?.unwrap_or(1)).await,
        "context-db" => {
            ensure!(args.len() == 3, "usage: context-db CREDENTIALS MARKET_ID");
            let database =
                polym_rust_demo::postgres_context::PostgresContext::read(Path::new(&args[1]), 1)?;
            let start = std::time::Instant::now();
            let (rows, policy) = database.fetch(&args[2]).await?;
            println!(
                "{}",
                json!({"results":rows,"config":policy,"elapsed_ms":start.elapsed().as_secs_f64()*1000.0})
            );
            Ok(())
        }
        "uma" => {
            ensure!(args.len() == 2, "usage: uma CONFIG");
            polym_rust_demo::uma::serve(Path::new(&args[1])).await
        }
        "shadow" => {
            polym_rust_demo::shadow::serve(polym_rust_demo::shadow::Settings::read(Path::new(
                args.get(1)
                    .map(String::as_str)
                    .unwrap_or("deploy/shadow.json"),
            ))?)
            .await
        }
        "trade-live" => {
            ensure!(
                args.len() == 4,
                "usage: trade-live CONFIG CREDENTIALS ACCOUNT"
            );
            polym_rust_demo::shadow::serve_live(
                polym_rust_demo::shadow::Settings::read(Path::new(&args[1]))?,
                Path::new(&args[2]),
                &args[3],
            )
            .await
        }
        "serve" => {
            let live = args.iter().any(|x| x == "--live");
            ensure!(args.len() <= 3, "usage: serve [config.json] [--live]");
            let path = args
                .iter()
                .skip(1)
                .find(|x| x.as_str() != "--live")
                .map(String::as_str)
                .unwrap_or("config.demo.json");
            let cfg = Config::read(Path::new(path))?;
            let mock = if live {
                None
            } else {
                Some(demo::MockExchange::start().await?)
            };
            if let Some(mock) = &mock {
                let delay: u64 = std::env::var("DEMO_MOCK_DELAY_MS")
                    .unwrap_or_else(|_| "0".into())
                    .parse()?;
                ensure!(delay <= 1000, "mock delay must be <=1000ms");
                mock.state.delay_ms.store(delay, Ordering::SeqCst);
            }
            let key = if live {
                let key = std::env::var("DEMO_ACCESS_TOKEN")?;
                ensure!(
                    key.len() >= 32,
                    "live service requires a bearer token of at least 32 bytes"
                );
                key
            } else {
                demo::ACCESS.into()
            };
            let exchange = if live {
                Exchange::live().await?
            } else {
                Exchange::demo(&mock.as_ref().unwrap().url).await?
            };
            let service = Service::start(cfg, exchange, key).await?;
            let bind = std::env::var("DEMO_BIND").unwrap_or_else(|_| "127.0.0.1:8787".into());
            let listener = TcpListener::bind(&bind).await?;
            println!(
                "{}",
                json!({"mode":if live{"live"}else{"demo"},"listen":format!("http://{bind}"),"orders":"POST /signal","status":"GET /orders/{id}"})
            );
            let stopped = service.stopped.clone();
            axum::serve(listener, service.router)
                .with_graceful_shutdown(async move {
                    let mut terminate =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .expect("termination signal");
                    tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
                    stopped.store(true, Ordering::SeqCst);
                })
                .await?;
            service.worker.await?;
            drop(mock);
            Ok(())
        }
        "send" => {
            ensure!(
                (4..=5).contains(&args.len()),
                "usage: send TOKEN ASK FAIR_VALUE [SIGNAL_ID]"
            );
            let s = Signal {
                id: args
                    .get(4)
                    .cloned()
                    .unwrap_or_else(|| format!("manual-{}", now_ms())),
                token_id: args[1].parse()?,
                ask: args[2].parse()?,
                fair_value: args[3].parse()?,
                observed_at_ms: now_ms(),
                book_valid: true,
            };
            let access = std::env::var("DEMO_ACCESS_TOKEN").unwrap_or_else(|_| demo::ACCESS.into());
            let response = reqwest::Client::new()
                .post("http://127.0.0.1:8787/signal")
                .bearer_auth(access)
                .json(&s)
                .send()
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response.json::<Value>().await?)?
            );
            Ok(())
        }
        _ => anyhow::bail!(
            "commands: demo [COUNT], serve [CONFIG] [--live], send TOKEN ASK FAIR_VALUE [ID]"
        ),
    }
}
async fn run_demo(count: usize) -> Result<()> {
    ensure!((1..=100).contains(&count), "demo count must be 1..100");
    let mock = demo::MockExchange::start().await?;
    mock.state.delay_ms.store(50, Ordering::SeqCst);
    let dir = Path::new("data").join(format!(
        "demo-{}",
        polymarket_client_sdk_v2::auth::Uuid::new_v4()
    ));
    let cfg = demo::config(dir.join("orders.jsonl").to_string_lossy().into());
    let service =
        Service::start(cfg, Exchange::demo(&mock.url).await?, demo::ACCESS.into()).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, service.router)
            .await
            .expect("demo signal server")
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    let mut jobs = tokio::task::JoinSet::new();
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    for i in 0..count {
        interval.tick().await;
        let c = client.clone();
        let url = url.clone();
        jobs.spawn(async move {
            c.post(format!("{url}/signal"))
                .bearer_auth(demo::ACCESS)
                .json(&demo::signal(&format!("demo-{i}")))
                .send()
                .await?
                .json::<Value>()
                .await
        });
    }
    let mut results = Vec::new();
    while let Some(r) = jobs.join_next().await {
        results.push(r??)
    }
    server.abort();
    let _ = server.await;
    service.worker.await?;
    let accepted = results.iter().filter(|r| r["state"] == "accepted").count();
    let mut dispatch: Vec<f64> = results
        .iter()
        .filter_map(|r| r["dispatch_ms"].as_f64())
        .collect();
    dispatch.sort_by(f64::total_cmp);
    let percentile = |p: f64| -> Option<f64> {
        if dispatch.is_empty() {
            None
        } else {
            Some(dispatch[((dispatch.len() - 1) as f64 * p).ceil() as usize])
        }
    };
    let summary = json!({"mode":"loopback_demo","orders":count,"accepted":accepted,"not_accepted":count-accepted,"signature_verified_posts":mock.state.posts.load(Ordering::SeqCst),"mock_response_delay_ms":50,"dispatch_ms":{"p50":percentile(0.50),"p95":percentile(0.95),"p99":percentile(0.99)},"boundary":"local HTTP handler receipt to exchange request dispatch; includes signature and durable journal; excludes public network", "journal":dir.join("orders.jsonl"),"results":results});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    ensure!(
        accepted == count,
        "some demo orders did not reach the mock exchange"
    );
    Ok(())
}
