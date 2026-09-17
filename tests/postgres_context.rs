use anyhow::Result;
use polym_rust_demo::{
    postgres_context::QUERY,
    shadow_state::{MarketContext, Policy},
};
use serde_json::Value;

#[tokio::test]
#[ignore = "requires the isolated rust_uma_test PostgreSQL database; CI runs this explicitly"]
async fn postgres_context_schema_contract() -> Result<()> {
    let url = std::env::var("RUST_UMA_TEST_DATABASE_URL")?;
    let config: tokio_postgres::Config = url.parse()?;
    anyhow::ensure!(
        config.get_dbname() == Some("rust_uma_test"),
        "isolated database required"
    );
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host,
        _ => anyhow::bail!("TCP fixture required"),
    };
    anyhow::ensure!(
        ["localhost", "127.0.0.1", "::1"].contains(&host.as_str()),
        "loopback fixture required"
    );
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute(r#"
CREATE TABLE market_data_event (id text PRIMARY KEY,volume numeric);
CREATE TABLE market_data_market (id text PRIMARY KEY,event_id text,question text,volume numeric,token_id_yes text,token_id_no text,
 active boolean,closed boolean,accepting_orders boolean,auto_archived boolean,min_tick_size numeric,min_order_size numeric,
 neg_risk boolean,fees_enabled boolean,fee_verification_status text,fee_schedule_rate numeric,fee_schedule_exponent numeric,fee_schedule_key text,fee_category text,raw_data jsonb);
CREATE TABLE market_data_tag (id text PRIMARY KEY,label text);
CREATE TABLE market_data_event_tags (event_id text,tag_id text);
CREATE TABLE market_data_resolution (id bigint,market_id text,market_id_external text,uma_request_id text,status text,proposed_price numeric,
 propose_time timestamptz,block_number bigint,dispute_block_number bigint,settle_block_number bigint,dispute_timestamp timestamptz,settle_timestamp timestamptz);
CREATE TABLE market_data_autotradeconfig (id bigint,manual_trade_shutdown_enabled boolean,max_ask_price numeric,max_orders_per_market bigint,ev_threshold numeric,
 order_size_usd numeric,low_price_order_size_usd numeric,low_depth_099_order_size_usd numeric);
CREATE TABLE strategy_tradelinedefinition (line_key text,valuation_key text,strategy_key text,is_active boolean);
INSERT INTO market_data_event VALUES ('e',200);
INSERT INTO market_data_market VALUES ('1','e','Synthetic',100,'42','43',true,false,true,false,.01,5,false,true,'unverified',NULL,NULL,'sports',NULL,'{"sportsMarketType":"tennis_match_totals"}');
INSERT INTO market_data_tag VALUES ('t','Sports');
INSERT INTO market_data_event_tags VALUES ('e','t');
INSERT INTO market_data_autotradeconfig VALUES (1,false,.999,1,.0002,5,10,10);
INSERT INTO strategy_tradelinedefinition VALUES ('post_propose_winner.default','m5_expected_payout','post_propose_winner',true);
INSERT INTO market_data_resolution VALUES (1,NULL,'1','r','proposed',1,now(),10,NULL,NULL,NULL,NULL);
"#).await?;
    // All connections must be prepared before a proposal burst, then remain reusable.
    use std::os::unix::fs::OpenOptionsExt;
    let path = std::env::temp_dir().join(format!("pg-pool-test-{}.json", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)?;
    serde_json::to_writer(
        &mut file,
        &serde_json::json!({"host":host,"port":config.get_ports().first().copied().unwrap_or(5432),
        "dbname":"rust_uma_test","user":config.get_user(),"password":std::str::from_utf8(config.get_password().unwrap_or_default())?}),
    )?;
    drop(file);
    let reader = polym_rust_demo::postgres_context::PostgresContext::read(&path, 3)?;
    std::fs::remove_file(path)?;
    reader.warm_up().await?;
    assert_eq!(reader.pool_status()["size"], 3);
    assert_eq!(reader.pool_status()["available"], 3);
    assert!(reader.ready().await);
    let before=client.query("SELECT pid FROM pg_stat_activity WHERE application_name='rust_uma_reader' ORDER BY pid",&[]).await?.iter().map(|r|r.get::<_,i32>(0)).collect::<Vec<_>>();
    assert_eq!(before.len(), 3);
    for _ in 0..3 {
        let results = futures_util::future::try_join_all((0..3).map(|_| reader.fetch("1"))).await?;
        assert!(results.iter().all(|(rows, _)| rows.len() == 1));
    }
    let after=client.query("SELECT pid FROM pg_stat_activity WHERE application_name='rust_uma_reader' ORDER BY pid",&[]).await?.iter().map(|r|r.get::<_,i32>(0)).collect::<Vec<_>>();
    assert_eq!(before, after);
    let row = client.query_one(QUERY, &[&"1"]).await?;
    let market: MarketContext = serde_json::from_value(row.get::<_, Value>(0))?;
    let policy: Policy = serde_json::from_value(row.get::<_, Value>(1))?;
    assert_eq!(market.token_id_yes.as_deref(), Some("42"));
    assert_eq!(
        market.fee_schedule.as_ref().unwrap().rate.to_string(),
        "0.03"
    );
    assert_eq!(
        market.resolution.as_ref().unwrap().request_id.as_deref(),
        Some("r")
    );
    assert_eq!(market.tags, vec!["Sports"]);
    assert_eq!(market.sports_market_type, "tennis_match_totals");
    assert!(policy.strategy_enabled);
    assert!(client.query_opt(QUERY, &[&"missing"]).await?.is_none());
    client.batch_execute("UPDATE market_data_market SET fee_schedule_rate=.07,fee_schedule_exponent=.5; UPDATE market_data_resolution SET status='settled',settle_block_number=12,settle_timestamp=now();").await?;
    let row = client.query_one(QUERY, &[&"1"]).await?;
    let market: MarketContext = serde_json::from_value(row.get::<_, Value>(0))?;
    assert_eq!(
        market.fee_schedule.as_ref().unwrap().rate.to_string(),
        "0.07"
    );
    assert_eq!(market.settled_request_blocks.get("r"), Some(&12));
    assert!(market.resolution.as_ref().unwrap().settled);
    client
        .batch_execute("UPDATE market_data_autotradeconfig SET manual_trade_shutdown_enabled=true")
        .await?;
    let (fresh, policy) = reader.fetch("1").await?;
    assert!(fresh[0].resolution.as_ref().unwrap().settled);
    assert_eq!(
        fresh[0].fee_schedule.as_ref().unwrap().rate.to_string(),
        "0.07"
    );
    assert!(policy.unwrap().manual_trade_shutdown_enabled);
    drop(reader);
    client.batch_execute("DROP TABLE market_data_event,market_data_market,market_data_tag,market_data_event_tags,market_data_resolution,market_data_autotradeconfig,strategy_tradelinedefinition").await?;
    Ok(())
}
