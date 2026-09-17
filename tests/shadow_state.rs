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
    for question in [
        "Hipfl vs. Salazar: Match O/U 21.5",
        "Forti/Romano vs. Clarke/Gill: Match O/U 22.5",
        "A vs. B: Set 1 Games O/U 9.5",
        "A vs. B: Set 2 Games O/U 10.5",
        "A vs. B: Total Sets O/U 2.5",
    ] {
        let mut tennis = context.clone();
        tennis.question = question.into();
        tennis.tags = vec![" Tennis ".into()];
        assert_eq!(test("0.52", "100", tennis), Some("tennis_ou_paused"));
    }
    for kind in [
        "tennis_match_totals",
        "tennis_first_set_totals",
        "tennis_set_games_totals",
        "tennis_set_totals",
    ] {
        let mut typed = context.clone();
        typed.sports_market_type = kind.into();
        assert_eq!(test("0.52", "100", typed), Some("tennis_ou_paused"));
    }
    let mut tennis_winner = context.clone();
    tennis_winner.tags = vec!["Tennis".into()];
    assert_eq!(test("0.52", "100", tennis_winner), None);
    let mut basketball = context.clone();
    basketball.question = "A vs B: O/U 220.5".into();
    basketball.tags = vec!["Basketball".into()];
    assert_eq!(test("0.52", "100", basketball), None);
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

#[test]
fn lifecycle_pruning_keeps_current_markets_and_terminal_proofs() {
    use polym_rust_demo::shadow_state::Resolution;
    use std::collections::HashSet;
    let mut life = Lifecycle::default();
    for i in 0..20_001 {
        let event = json!({"market_id":format!("old-{i}"),"request_id":"r","request_timestamp":100,
            "oracle_address":"o","block_number":11,"log_index":1});
        life.apply("uma:settle", &event);
        life.marks.get_mut(&format!("old-{i}")).unwrap().received_ms = 1;
    }
    let event = |market: &str, block| {
        json!({"market_id":market,"request_id":"r","request_timestamp":100,
        "oracle_address":"o","block_number":block,"log_index":1,"proposed_price":1})
    };
    life.apply("uma:resolution", &event("active", 10));
    life.marks.get_mut("active").unwrap().received_ms = 1;
    life.apply("uma:dispute_price", &event("disputed", 11));
    life.marks.get_mut("disputed").unwrap().received_ms = 1;
    life.apply("uma:resolution", &event("recent", 10));
    let now = polym_rust_demo::now_ms();
    life.marks.get_mut("recent").unwrap().received_ms = now;
    assert_eq!(
        life.prune_inactive(&HashSet::from(["active".into()]), now),
        20_001
    );
    assert_eq!(life.marks.len(), 3);
    assert!(life.is_proposed("active", "r", 10));
    assert!(life.is_proposed("recent", "r", 10));
    assert!(life.has_dispute("disputed"));
    assert!(!life.needs_refresh.contains("old-0"));
    assert!(!life.apply("uma:resolution", &event("old-0", 10)));
    let stale: Resolution =
        serde_json::from_value(json!({"id":1,"request_id":"r","status":"proposed",
        "proposed_price":"1","propose_time_ms":1,"block_number":10,"dispute_block_number":null,
        "settle_block_number":null,"disputed":false,"settled":false}))
        .unwrap();
    life.seed("old-0", &stale);
    assert!(!life.is_proposed("old-0", "r", 10));
    let mut fresh = stale;
    fresh.block_number = Some(20);
    life.seed("old-0", &fresh);
    assert!(life.is_proposed("old-0", "r", 20));
}

#[test]
fn native_uma_overlay_uses_chain_time_and_never_overrides_terminal_evidence() -> anyhow::Result<()>
{
    use polym_rust_demo::{native_uma::overlay, shadow_state::MarketContext};
    let now = polym_rust_demo::now_ms();
    let mut context: MarketContext = serde_json::from_value(
        json!({"market_id":"1","question":"Synthetic market","tags":[],"eligible":false,
        "token_id_yes":"1","token_id_no":"2","active":true,"closed":false,"accepting_orders":true,"auto_archived":false,
        "min_tick_size":"0.001","min_order_size":"5","neg_risk":false,"fees_enabled":false,"fee_schedule":null,
        "fee_verification_status":"unverified","has_disputed_resolution":false,"existing_order_count":0,"resolution":null,"valuation":null}),
    )?;
    let proposal = json!({"market_id":"1","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":10,"log_index":1,"proposed_price":1,"block_timestamp":now/1000});
    let mut life = Lifecycle::default();
    life.apply("uma:resolution", &proposal);
    overlay(&mut context, &life, now);
    assert!(context.eligible);
    assert_eq!(
        context.resolution.as_ref().unwrap().propose_time_ms,
        (now / 1000) * 1000
    );
    let mut stale = context.clone();
    overlay(&mut stale, &life, now + 10_800_001);
    assert!(!stale.eligible);
    let mut terminal = context.clone();
    terminal.settled_request_blocks.insert("r".into(), 11);
    overlay(&mut terminal, &life, now);
    assert!(!terminal.eligible);
    let mut disputed = context.clone();
    disputed.has_disputed_resolution = true;
    overlay(&mut disputed, &life, now);
    assert!(!disputed.eligible);
    life.apply("uma:dispute_price",&json!({"market_id":"1","request_id":"r","request_timestamp":100,"oracle_address":"o","block_number":11,"log_index":1}));
    overlay(&mut context, &life, now);
    assert!(!context.eligible);
    Ok(())
}

#[test]
fn live_sizing_uses_approved_bands_and_keeps_liquidity_and_fee_checks() -> anyhow::Result<()> {
    use polym_rust_demo::shadow_state::{MarketContext, Policy, Reservations, decide};
    use std::sync::Arc;
    let context: MarketContext = serde_json::from_value(json!({
        "market_id":"m","question":"Synthetic","tags":[],"eligible":true,
        "token_id_yes":"1","token_id_no":"2","active":true,"closed":false,
        "accepting_orders":true,"auto_archived":false,"min_tick_size":"0.001",
        "min_order_size":"5","neg_risk":false,"fees_enabled":false,
        "fee_schedule":null,"fee_verification_status":"unverified",
        "has_disputed_resolution":false,"existing_order_count":0,"valuation":null,
        "market_volume":"0","event_volume":"0",
        "resolution":{"id":1,"request_id":"r","status":"proposed","proposed_price":"1",
        "propose_time_ms":1000,"block_number":10,"dispute_block_number":null,
        "settle_block_number":null,"disputed":false,"settled":false}
    }))?;
    let policy: Policy = serde_json::from_value(json!({
        "valuation_key":"m5_expected_payout","manual_trade_shutdown_enabled":false,
        "strategy_enabled":true,"max_ask_price":"0.999","max_orders_per_market":1,
        "ev_threshold":"0.0002","order_size_usd":"5","low_price_order_size_usd":"10",
        "low_depth_099_order_size_usd":"10","order_sizing":{
        "standard":"5","below_005":"5","below_080":"20","through_098":"10","at_099_low_depth":"20"}
    }))?;
    let mut life = Lifecycle::default();
    life.seed("m", context.resolution.as_ref().unwrap());
    for (price, depth, budget, shares) in [
        ("0.04", "1000", "5", "125"),
        ("0.05", "1000", "20", "400"),
        ("0.50", "100", "20", "40"),
        ("0.799", "100", "20", "25"),
        ("0.80", "100", "10", "12"),
        ("0.98", "100", "10", "10"),
        ("0.981", "100", "5", "5"),
        ("0.99", "49", "20", "20"),
        ("0.99", "50", "20", "20"),
        ("0.99", "50.999", "20", "20"),
        ("0.99", "51", "5", "5"),
        ("0.999", "50", "5", "5"),
        // FAK may fill partially: the larger Rust budget must not suppress a signal.
        ("0.99", "10", "20", "20"),
        ("0.99", "1", "20", "20"),
        ("0.98", "5", "10", "10"),
        ("0.999", "0.5", "5", "5"),
        ("0.50", "39", "20", "40"),
    ] {
        let book = json!({"token_id":"1","best_ask":{"price":price,"size":depth},
            "tick_size":"0.001","loser_bid":"0"});
        let d = decide(
            Arc::new(context.clone()),
            &policy,
            &life,
            &book,
            &mut Reservations::default(),
            1000,
            "30".parse()?,
        )
        .map_err(anyhow::Error::msg)?;
        assert_eq!(d.budget, budget.parse()?, "price {price}, depth {depth}");
        assert_eq!(d.shares, shares.parse()?, "price {price}, depth {depth}");
    }
    let book = json!({"token_id":"1","best_ask":{"price":"0.50","size":"0"},
        "tick_size":"0.001","loser_bid":"0"});
    assert_eq!(
        decide(
            Arc::new(context),
            &policy,
            &life,
            &book,
            &mut Reservations::default(),
            1000,
            "30".parse()?
        )
        .err(),
        Some("invalid_ask")
    );
    Ok(())
}
