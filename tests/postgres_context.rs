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
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute(r#"
CREATE TEMP TABLE market_data_event (id text PRIMARY KEY,volume numeric);
CREATE TEMP TABLE market_data_market (id text PRIMARY KEY,event_id text,question text,volume numeric,token_id_yes text,token_id_no text,
 active boolean,closed boolean,accepting_orders boolean,auto_archived boolean,min_tick_size numeric,min_order_size numeric,
 neg_risk boolean,fees_enabled boolean,fee_verification_status text,fee_schedule_rate numeric,fee_schedule_exponent numeric,fee_schedule_key text,fee_category text);
CREATE TEMP TABLE market_data_tag (id text PRIMARY KEY,label text);
CREATE TEMP TABLE market_data_event_tags (event_id text,tag_id text);
CREATE TEMP TABLE market_data_resolution (id bigint,market_id text,market_id_external text,uma_request_id text,status text,proposed_price numeric,
 propose_time timestamptz,block_number bigint,dispute_block_number bigint,settle_block_number bigint,dispute_timestamp timestamptz,settle_timestamp timestamptz);
CREATE TEMP TABLE market_data_autotradeconfig (id bigint,manual_trade_shutdown_enabled boolean,max_ask_price numeric,max_orders_per_market bigint,ev_threshold numeric,
 order_size_usd numeric,low_price_order_size_usd numeric,low_depth_099_order_size_usd numeric);
CREATE TEMP TABLE strategy_tradelinedefinition (line_key text,valuation_key text,strategy_key text,is_active boolean);
INSERT INTO market_data_event VALUES ('e',200);
INSERT INTO market_data_market VALUES ('1','e','Synthetic',100,'42','43',true,false,true,false,.01,5,false,true,'unverified',NULL,NULL,'sports',NULL);
INSERT INTO market_data_tag VALUES ('t','Sports');
INSERT INTO market_data_event_tags VALUES ('e','t');
INSERT INTO market_data_autotradeconfig VALUES (1,false,.999,1,.0002,5,10,10);
INSERT INTO strategy_tradelinedefinition VALUES ('post_propose_winner.default','m5_expected_payout','post_propose_winner',true);
INSERT INTO market_data_resolution VALUES (1,NULL,'1','r','proposed',1,now(),10,NULL,NULL,NULL,NULL);
"#).await?;
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
    Ok(())
}
