use super::*;

#[test]
fn uma_configuration_matches_live_sizing() -> Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/uma-trader.json");
    let settings = Settings::read(&path)?;
    let live = Settings::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/live.json"))?;
    assert_eq!(settings.max_order_budget_pusd, live.max_order_budget_pusd);
    assert_eq!(json!(settings.order_sizing), json!(live.order_sizing));
    assert!(settings.uma_url.is_some());
    assert!(!settings.account_ready(&json!({"ready":true,"trade_capacity_pusd":"100"})));
    assert!(settings.account_ready(
        &json!({"ready":true,"trade_capacity_pusd":"100","supported_strategies":["rust_uma"]})
    ));
    assert!(settings.journal.contains("rust-uma"));
    Ok(())
}

#[tokio::test]
async fn proposal_reads_context_books_and_uses_live_sizing() -> Result<()> {
    let now = now_ms();
    let page = json!({"schema_version":1,"captured_at_ms":now,"has_more":false,"next_after_id":null,
        "config":{"valuation_key":"m5_expected_payout","manual_trade_shutdown_enabled":false,"strategy_enabled":true,
            "max_ask_price":"0.999","max_orders_per_market":1,"ev_threshold":"0.0002","order_size_usd":"20",
            "low_price_order_size_usd":"20","low_depth_099_order_size_usd":"20"},
        "results":[{"market_id":"123","question":"Synthetic market","market_volume":"0","event_volume":"0",
            "tags":[],"eligible":false,"token_id_yes":"42","token_id_no":"43","active":true,"closed":false,
            "accepting_orders":true,"auto_archived":false,"min_tick_size":"0.01","min_order_size":"5","neg_risk":false,
            "fees_enabled":false,"fee_schedule":null,"fee_verification_status":"unverified","has_disputed_resolution":false,
            "existing_order_count":0,"valuation":null,"resolution":null}]});
    let context = Arc::new(RwLock::new(page));
    let books = Arc::new(RwLock::new(json!({
        "42":{"token_id":"42","market_id":"123","condition_id":"0xabc","timestamp":now*1000,"tick_size":"0.01","min_order_size":"5","neg_risk":false,
            "asks":[{"price":"0.99","size":"2"},{"price":"0.90","size":"20"}],
            "bids":[{"price":"0.80","size":"50"},{"price":"0.82","size":"100"}]},
        "43":{"token_id":"43","market_id":"123","condition_id":"0xabc","timestamp":now*1000,"tick_size":"0.01","min_order_size":"5","neg_risk":false,
            "asks":[],"bids":[{"price":"0.10","size":"10"}]}
    })));
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (current, max_active) = (active.clone(), peak.clone());
    let (c, log) = (context.clone(), calls.clone());
    let (b, book_log) = (books.clone(), calls.clone());
    let warm_peer = Arc::new(Mutex::new(None));
    let book_peers = Arc::new(Mutex::new(Vec::new()));
    let (warm_capture, book_capture) = (warm_peer.clone(), book_peers.clone());
    let trusted = Arc::new(AtomicBool::new(true));
    let changed = Arc::new(AtomicBool::new(false));
    let changed_best = changed.clone();
    let (best_books, best_trusted) = (books.clone(), trusted.clone());
    let router = Router::new()
        .route("/book/{token}/best",get(move |axum::extract::Path(token):axum::extract::Path<String>| {
            let (books,trusted,changed)=(best_books.clone(),best_trusted.clone(),changed_best.clone());
            async move {
                if !trusted.load(Ordering::SeqCst) {return (StatusCode::NOT_FOUND,Json(json!({"error":"untrusted"})));}
                let number=token.parse::<u64>().unwrap();
                let source=books.read().await;
                let b=&source[if number%2==0 {"42"} else {"43"}];
                let best=|side:&str| {
                    let mut rows=b[side].as_array().unwrap().clone();
                    rows.sort_by_key(|r|crate::shadow_state::decimal(&r["price"]).unwrap());
                    if side=="bids" {rows.reverse();}
                    rows.into_iter().next()
                };
                let bid=best("bids");let mut ask=if number==400 {Some(json!({"price":"0.998","size":"10"}))} else {best("asks")};
                if changed.load(Ordering::SeqCst) { ask=Some(json!({"price":"0.91","size":"20"})); }
                (StatusCode::OK,Json(json!({"token_id":token,"market_id":if number<400 {"123".to_owned()}else{(number/2).to_string()},
                    "best_bid":bid.as_ref().map(|r|&r["price"]),"best_bid_size":bid.as_ref().map(|r|&r["size"]),
                    "best_ask":ask.as_ref().map(|r|&r["price"]),"best_ask_size":ask.as_ref().map(|r|&r["size"])})))
            }
        }))
        .route(
            "/health",
            get(
                move |axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<
                    std::net::SocketAddr,
                >| {
                    let capture = warm_capture.clone();
                    async move {
                        *capture.lock().await = Some(peer);
                        Json(now_ms() / 1000)
                    }
                },
            ),
        )
        .route(
            "/context",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let (c, log, current, max_active) =
                    (c.clone(), log.clone(), current.clone(), max_active.clone());
                async move {
                    let id = &query["market_ids"];
                    let running = current.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(running, Ordering::SeqCst);
                    log.lock().await.push("django".into());
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let mut page = c.read().await.clone();
                    if id != "123" {
                        let number = id.parse::<u64>().unwrap();
                        page["results"][0]["market_id"] = json!(id);
                        page["results"][0]["token_id_yes"] = json!((number * 2).to_string());
                        page["results"][0]["token_id_no"] = json!((number * 2 + 1).to_string());
                    }
                    current.fetch_sub(1, Ordering::SeqCst);
                    Json(page)
                }
            }),
        )
        .route(
            "/book/{token}",
            get(
                move |axum::extract::Path(token): axum::extract::Path<String>,
                      axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<
                    std::net::SocketAddr,
                >| {
                    let (b, log, capture) = (b.clone(), book_log.clone(), book_capture.clone());
                    async move {
                        capture.lock().await.push(peer);
                        log.lock().await.push(token.clone());
                        let number = token.parse::<u64>().unwrap();
                        let template = if number % 2 == 0 { "42" } else { "43" };
                        let mut book = b.read().await[template].clone();
                        book["token_id"] = json!(token);
                        book["market_id"] = json!(if number<400 {"123".to_owned()} else {(number/2).to_string()});
                        if number==400 {book["asks"]=json!([{"price":"0.998","size":"10"}]);book["tick_size"]=json!("0.001");}
                        Json(book)
                    }
                },
            ),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
    });
    let mock = demo::MockExchange::start().await?;
    let exchange = Exchange::shadow(&mock.url).await?;
    let path =
        std::env::temp_dir().join(format!("rust-uma-test-{}-{now}.jsonl", std::process::id()));
    let journal = Journal::open(&path, &exchange.scope())?;
    let settings: Settings = serde_json::from_value(json!({
        "django_url":format!("{url}/context"),"history_url":format!("{url}/history"),"ober_url":url,
        "nats_url":"","redis_url":"","uma_url":"http://unused","bind":"127.0.0.1:0","journal":path,
        "max_signal_age_ms":5000,"max_inflight":8,"context_max_age_ms":90000,
        "max_order_budget_pusd":"30","total_budget_pusd":null,
        "order_sizing":serde_json::from_slice::<Value>(include_bytes!("../deploy/live.json"))?["order_sizing"],
        "uma_trade":true
    }))?;
    let app = Arc::new(App {
        control: Arc::default(),
        database: None,
        live: Some(LiveAccount {
            name: "synthetic-account".into(),
            id: 1,
            signer: "synthetic-signer".into(),
            funder: "synthetic-funder".into(),
            token: "synthetic-access".into(),
        }),
        history_notify: Notify::new(),
        settings,
        data: RwLock::new(Data {
            history_ready_at_ms: now,
            live_ready_at_ms: now,
            native_uma_at_ms: now,
            native_uma_health: json!({"ready":true}),
            ..Data::default()
        }),
        telemetry: Telemetry::default(),
        exchange,
        journal: Arc::new(Mutex::new(journal)),
        slots: Arc::new(Semaphore::new(8)),
        proposal_queue: Arc::new(Semaphore::new(1024)),
        notify: Notify::new(),
        nats_up: AtomicBool::new(false),
        redis_up: AtomicBool::new(true),
        redis_epoch: AtomicU64::new(1),
        stopped: AtomicBool::new(false),
        stop_reason: std::sync::Mutex::new(None),
        boot_at_ms: now,
        http: reqwest::Client::new(),
    });
    app.warm_book_connection().await?;
    app.control.confirm_snapshot(&serde_json::to_vec(&json!({"schema_version":1,"source":"dashboard","dry_run_enabled":false,"triggered_at_ms":now_ms()}))?)?;
    let event = crate::uma::Event {
        channel: "uma:resolution".into(),
        market_id: "123".into(),
        request_id: "a".repeat(64),
        request_timestamp: now / 1000,
        oracle_address: "0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1".into(),
        block_number: 10,
        log_index: 2,
        block_hash: format!("0x{}", "b".repeat(64)),
        tx_hash: format!("0x{}", "c".repeat(64)),
        block_timestamp: now / 1000,
        proposed_price: "1".into(),
        received_at_ms: now,
    };
    app.data
        .write()
        .await
        .lifecycle
        .apply(&event.channel, &json!(event));
    // Missing market is skipped once, without fetching books or attempting hydration.
    let rows = context.write().await["results"].take();
    context.write().await["results"] = json!([]);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    assert_eq!(*calls.lock().await, vec!["django"]);
    assert_eq!(
        app.telemetry.snapshot(60)["reasons"]["django_market_missing"],
        1
    );
    context.write().await["results"] = rows;
    trusted.store(false, Ordering::SeqCst);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    trusted.store(true, Ordering::SeqCst);
    changed.store(true, Ordering::SeqCst);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    assert_eq!(
        app.telemetry.snapshot(60)["reasons"]["ober_book_changed_during_read"],
        1
    );
    changed.store(false, Ordering::SeqCst);
    let original_asks = books.read().await["42"]["asks"].clone();
    books.write().await["42"]["tick_size"] = json!("0.001");
    books.write().await["42"]["asks"] = json!([{"price":"0.999","size":"10"}]);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    assert_eq!(app.telemetry.snapshot(60)["reasons"]["max_ask_price"], 1);
    books.write().await["42"]["asks"] = original_asks;

    // Empty winner asks also stop before signing.
    let asks = books.write().await["42"]["asks"].take();
    books.write().await["42"]["asks"] = json!([]);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    books.write().await["42"]["asks"] = asks;
    context.write().await["results"][0]["tags"] = json!(["Trump"]);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    context.write().await["results"][0]["tags"] = json!([]);
    context.write().await["config"]["ev_threshold"] = json!("0.99");
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 0);
    assert_eq!(
        app.telemetry.snapshot(60)["reasons"]["ev_below_threshold"],
        1
    );
    context.write().await["config"]["ev_threshold"] = json!("0.0002");
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    let id = uma_trade::signal_id(&event, true);
    let entry = app.journal.lock().await.get(&id)?.unwrap();
    assert_eq!(entry.reply.state, "accepted");
    assert_eq!(entry.reply.submitted_amount, Some("9.90".parse()?));
    assert_eq!(entry.reserved, Decimal::from(10));
    let body = crate::shadow_history::payload(&entry)?;
    if let Ok(path) = std::env::var("RUST_UMA_TEST_RECEIPT") {
        std::fs::write(path, serde_json::to_vec_pretty(&body)?)?;
    }
    assert_eq!(body["input_kind"], "uma_propose");
    assert_eq!(body["uma"]["book_source"], "ober");
    assert_eq!(body["market_id"], "123");
    assert_eq!(body["uma"]["winner_book"]["asks"][0]["price"], "0.90");
    assert!(body["uma"]["django_ms"].as_f64().unwrap() >= 10.0);
    assert!(body["uma"]["expected_payout"].as_str().is_some());
    assert!(body["size"].as_str().unwrap().parse::<Decimal>()? > Decimal::from(5));
    // Identity ignores arrival time: a replay cannot submit again.
    let mut duplicate = event.clone();
    duplicate.received_at_ms += 1;
    assert_eq!(
        uma_trade::signal_id(&event, true),
        uma_trade::signal_id(&duplicate, true)
    );
    app.receive_uma(duplicate, Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    // Later dispute invalidates a queued proposal even before dashboard access.
    let mut dispute = json!(event);
    dispute["block_number"] = json!(11);
    app.data
        .write()
        .await
        .lifecycle
        .apply("uma:dispute_price", &dispute);
    app.receive_uma(event.clone(), Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
    // Eight different proposals overlap dashboard reads while respecting executor capacity.

    for market in 200..208 {
        let mut next = event.clone();
        next.market_id = market.to_string();
        next.log_index = market;
        next.received_at_ms = now_ms();
        app.data
            .write()
            .await
            .lifecycle
            .apply(&next.channel, &json!(next));
        app.dispatch_uma(next);
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while mock.state.posts.load(Ordering::SeqCst) < 9 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _permits = app.slots.acquire_many(8).await.unwrap();
    })
    .await?;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 9);
    let orders = app.journal.lock().await.pending_batch(true)?;
    assert!(orders.iter().all(|o| o.signal.ask <= uma_trade::MAX_PRICE));
    assert!(orders.iter().any(|o| o.signal.ask == uma_trade::MAX_PRICE));
    assert!(peak.load(Ordering::SeqCst) > 1);
    assert!(peak.load(Ordering::SeqCst) <= 8);
    let metrics = app.telemetry.snapshot(60);
    assert!(metrics["latency_ms"]["django_ms"]["n"].as_u64().unwrap() >= 8);
    // A new feed epoch cannot reuse queued work from an earlier connection.
    app.redis_epoch.store(2, Ordering::SeqCst);
    app.receive_uma(event, Instant::now(), 1).await;
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 9);
    let warmed = warm_peer.lock().await.context("warm request missing")?;
    assert!(
        book_peers.lock().await.contains(&warmed),
        "book request must reuse the warmed connection"
    );
    server.abort();
    Ok(())
}

#[test]
fn dry_run_config_requires_database_and_never_selects_live_history() -> Result<()> {
    let s = Settings::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/uma-dry-run.json"))?;
    assert!(s.uma_trade);
    assert!(s.history_url.ends_with("/api/shadow-orders/"));
    let raw: Value = serde_json::from_slice(&std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/uma-dry-run.json"),
    )?)?;
    assert_eq!(raw["database_config"], "/run/secrets/database_reader");
    let live = Settings::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/live.json"))?;
    assert_eq!(s.max_order_budget_pusd, live.max_order_budget_pusd);
    assert_eq!(json!(s.order_sizing), json!(live.order_sizing));
    Ok(())
}
