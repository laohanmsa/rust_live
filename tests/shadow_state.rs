use polym_rust_demo::shadow_state::Lifecycle;
use serde_json::json;

#[test]
fn lifecycle_cannot_resurrect_or_cross_request_rounds() {
    let mut state = Lifecycle::default();
    let propose = json!({"market_id":"m","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":10,"log_index":1,"tx_hash":"a","proposed_price":1});
    assert!(state.apply("uma:resolution", &propose));
    assert!(state.is_proposed("m", "r", 10));
    let dispute = json!({"market_id":"m","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":11,"log_index":1,"tx_hash":"b"});
    state.apply("uma:dispute_price", &dispute);
    assert!(!state.is_proposed("m", "r", 10));
    assert!(!state.apply("uma:resolution", &propose));
    let next = json!({"market_id":"m","request_id":"new","request_timestamp":200,"oracle_address":"o","block_number":12,"log_index":1,"tx_hash":"c","proposed_price":0});
    state.apply("uma:resolution", &next);
    assert!(state.is_proposed("m", "new", 12));
    assert!(!state.apply("uma:settle", &dispute));
    assert!(state.is_proposed("m", "new", 12));
}

#[test]
fn guards_use_cached_context_and_keep_threshold_boundaries() -> anyhow::Result<()> {
    use polym_rust_demo::shadow_state::{MarketContext, Policy, Reservations, decide};
    use std::sync::Arc;
    let context: MarketContext = serde_json::from_value(json!({
        "market_id":"m","question":"Synthetic market","tags":["Sports"],"eligible":true,
        "token_id_yes":"1","token_id_no":"2","active":true,"closed":false,"accepting_orders":true,
        "auto_archived":false,"min_tick_size":"0.001","min_order_size":"5","neg_risk":false,
        "fees_enabled":false,"fee_schedule":null,"fee_verification_status":"unverified",
        "has_disputed_resolution":false,"existing_order_count":0,
        "resolution":{"id":1,"request_id":"r","status":"proposed","proposed_price":"1","propose_time_ms":1000,"block_number":10,"dispute_block_number":null,"settle_block_number":null,"disputed":false,"settled":false},
        "market_volume":"0","event_volume":"0",
        "valuation":{"expected_payout":"0.999","calculated_at_ms":1000}
    }))?;
    let policy: Policy = serde_json::from_value(
        json!({"valuation_key":"m5_expected_payout","manual_trade_shutdown_enabled":false,"strategy_enabled":true,"max_ask_price":"0.999","max_orders_per_market":5,"ev_threshold":"0.0001","order_size_usd":"5","low_price_order_size_usd":"10","low_depth_099_order_size_usd":"10"}),
    )?;
    let mut life = Lifecycle::default();
    life.seed("m", context.resolution.as_ref().unwrap());
    let test = |price: &str, depth: &str, ctx: MarketContext| -> Option<&'static str> {
        let book = json!({"token_id":"1","best_ask":{"price":price,"size":depth},"tick_size":"0.001","loser_bid":"0"});
        decide(
            Arc::new(ctx),
            &policy,
            &life,
            &book,
            &mut Reservations::default(),
            1000,
            "10".parse().unwrap(),
        )
        .err()
    };
    assert_eq!(test("0.799", "3000", context.clone()), None);
    assert_eq!(test("0.80", "200", context.clone()), None);
    assert_eq!(
        test("0.80", "201", context.clone()),
        Some("cheap_winner_ask_wall")
    );
    assert_eq!(test("0.89", "800", context.clone()), None);
    assert_eq!(
        test("0.89", "801", context.clone()),
        Some("cheap_winner_ask_wall")
    );
    let mut tagged = context.clone();
    tagged.tags = vec![" TRUMP ".into()];
    assert_eq!(
        test("0.90", "100", tagged),
        Some("block_restricted_event_tags")
    );
    let mut disputed = context.clone();
    disputed.has_disputed_resolution = true;
    assert_eq!(test("0.90", "100", disputed), Some("market_not_disputed"));
    let mut fees = context.clone();
    fees.fees_enabled = true;
    assert_eq!(test("0.90", "100", fees), Some("fee_schedule_unavailable"));
    let mut exhausted = context;
    exhausted.existing_order_count = 5;
    assert_eq!(test("0.90", "100", exhausted), None);
    Ok(())
}

#[test]
fn django_settlement_proof_clears_a_missed_terminal_event() {
    let mut life = Lifecycle::default();
    let proposal = json!({"market_id":"m","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":10,"log_index":1,"proposed_price":1});
    let dispute = json!({"market_id":"m","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":11,"log_index":1});
    life.apply("uma:resolution", &proposal);
    life.apply("uma:dispute_price", &dispute);
    assert!(life.has_dispute("m"));
    life.reconcile_settles("m", &std::collections::HashMap::from([("r".into(), 12)]));
    assert!(!life.has_dispute("m"));
    assert!(!life.apply("uma:dispute_price", &dispute));
    assert!(!life.apply("uma:resolution", &proposal));
}

#[tokio::test]
async fn shadow_signs_real_shaped_tokens_without_public_metadata_requests() -> anyhow::Result<()> {
    use polym_rust_demo::{demo, exchange::Exchange};
    use std::sync::atomic::Ordering;
    let mock = demo::MockExchange::start().await?;
    let exchange = Exchange::shadow(&mock.url).await?;
    for neg_risk in [false, true] {
        let mut signal = demo::signal("shadow-sign");
        signal.token_id = "42".parse()?;
        let signed = exchange
            .sign_shadow(&signal, "10".parse()?, "0.01".parse()?, neg_risk)
            .await?;
        let (status, result) = exchange.post(signed).await?;
        assert_eq!(status, 200);
        assert_eq!(result["success"], true);
    }
    assert_eq!(mock.state.public_reads.load(Ordering::SeqCst), 1);
    assert_eq!(mock.state.posts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test]
fn changed_proposal_price_invalidates_the_prepared_winner() {
    let mut life = Lifecycle::default();
    let first = json!({"market_id":"m","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":10,"log_index":1,"proposed_price":1});
    life.apply("uma:resolution", &first);
    assert!(life.matches_proposal("m", "r", 10, "1".parse().unwrap()));
    let second = json!({"market_id":"m","request_id":"r","request_timestamp":101,"oracle_address":"o","block_number":10,"log_index":2,"proposed_price":0});
    life.apply("uma:resolution", &second);
    assert!(!life.matches_proposal("m", "r", 10, "1".parse().unwrap()));
}
