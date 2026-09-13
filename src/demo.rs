//! Loopback-only exchange. The public test key must never be funded.
use crate::{Config, Signal, exchange::auth_signature, now_ms};
use alloy::{
    primitives::{Signature, U256},
    sol_types::{SolStruct, eip712_domain},
};
use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use polymarket_client_sdk_v2::{POLYGON, clob::types::OrderV2, contract_config};
use serde_json::{Value, json};
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::{net::TcpListener, task::JoinHandle};

pub const KEY: &str = "0000000000000000000000000000000000000000000000000000000000000001";
pub const SECRET: &str = "ZGVtby1vbmx5LXNlY3JldA==";
pub const ACCESS: &str = "demo-local-only";
pub const CONDITION: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
#[derive(Default)]
pub struct MockState {
    pub posts: AtomicUsize,
    pub public_reads: AtomicUsize,
    pub delay_ms: AtomicU64,
    pub response_code: AtomicU64,
}
pub struct MockExchange {
    pub url: String,
    pub state: Arc<MockState>,
    pub task: JoinHandle<()>,
}
impl MockExchange {
    pub async fn start() -> Result<Self> {
        let state = Arc::new(MockState::default());
        let router = Router::new()
            .route("/version", get(version))
            .route("/markets-by-token/{token}", get(market))
            .route("/clob-markets/{condition}", get(info))
            .route("/order", post(order))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let task =
            tokio::spawn(
                async move { axum::serve(listener, router).await.expect("mock exchange") },
            );
        Ok(Self { url, state, task })
    }
}
impl Drop for MockExchange {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn version(State(s): State<Arc<MockState>>) -> Json<Value> {
    s.public_reads.fetch_add(1, Ordering::SeqCst);
    Json(json!({"version":2}))
}
async fn market(State(s): State<Arc<MockState>>, Path(token): Path<String>) -> Json<Value> {
    s.public_reads.fetch_add(1, Ordering::SeqCst);
    Json(json!({"condition_id":CONDITION,"primary_token_id":token,"secondary_token_id":"2"}))
}
async fn info(State(s): State<Arc<MockState>>) -> Json<Value> {
    s.public_reads.fetch_add(1, Ordering::SeqCst);
    Json(
        json!({"c":CONDITION,"t":[{"t":"1","o":"Yes"},{"t":"2","o":"No"}],"mts":"0.01","mos":"1","nr":false,"fd":{"r":"0.02","e":1,"to":true}}),
    )
}
fn verify(headers: &HeaderMap, bytes: &[u8]) -> Result<String> {
    fn header<'a>(h: &'a HeaderMap, k: &str) -> Result<&'a str> {
        Ok(h.get(k)
            .ok_or_else(|| anyhow::anyhow!("missing header"))?
            .to_str()?)
    }
    let timestamp = header(headers, "POLY_TIMESTAMP")?;
    ensure!(
        header(headers, "POLY_SIGNATURE")?
            == auth_signature(SECRET, timestamp, "POST", "/order", bytes)?,
        "bad HMAC"
    );
    ensure!(
        header(headers, "POLY_API_KEY")? == "00000000-0000-0000-0000-000000000000"
            && header(headers, "POLY_PASSPHRASE")? == "demo-passphrase",
        "bad demo credentials"
    );
    let body: Value = serde_json::from_slice(bytes)?;
    let o = &body["order"];
    let u = |k: &str| -> Result<U256> {
        Ok(U256::from_str(
            &o[k]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| o[k].to_string()),
        )?)
    };
    let mut order = OrderV2::default();
    order.salt = u("salt")?;
    order.maker = o["maker"].as_str().unwrap_or("").parse()?;
    order.signer = o["signer"].as_str().unwrap_or("").parse()?;
    order.tokenId = u("tokenId")?;
    order.makerAmount = u("makerAmount")?;
    order.takerAmount = u("takerAmount")?;
    ensure!(
        o["side"] == "BUY" && body["orderType"] == "FAK",
        "wrong order family"
    );
    order.side = 0;
    order.signatureType = o["signatureType"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("bad signature type"))?
        .try_into()?;
    ensure!(order.signatureType == 0, "demo uses EOA signatures");
    order.timestamp = u("timestamp")?;
    order.metadata = o["metadata"].as_str().unwrap_or("").parse()?;
    order.builder = o["builder"].as_str().unwrap_or("").parse()?;
    ensure!(
        order.makerAmount > U256::ZERO && order.takerAmount > U256::ZERO,
        "empty order"
    );
    let domain = eip712_domain! {name:"Polymarket CTF Exchange",version:"2",chain_id:POLYGON,verifying_contract:contract_config(POLYGON,false).unwrap().exchange_v2.unwrap(),};
    let hash = order.eip712_signing_hash(&domain);
    let sig: Signature = o["signature"].as_str().unwrap_or("").parse()?;
    ensure!(
        sig.recover_address_from_prehash(&hash)? == order.signer,
        "bad EIP-712 signature"
    );
    ensure!(
        header(headers, "POLY_ADDRESS")?.parse::<alloy::primitives::Address>()? == order.signer,
        "wrong signer header"
    );
    Ok(hash.to_string())
}
async fn order(
    State(s): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    match verify(&headers, &body) {
        Ok(hash) => {
            s.posts.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(
                s.delay_ms.load(Ordering::SeqCst),
            ))
            .await;
            if s.response_code.load(Ordering::SeqCst) != 0 {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({"error":"rate_limited"})),
                );
            }
            (
                StatusCode::OK,
                Json(
                    json!({"success":true,"status":"matched","orderID":hash,"makingAmount":"1","takingAmount":"2","tradeIDs":[],"transactionsHashes":[]}),
                ),
            )
        }
        Err(_) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"mock_signature_validation_failed"})),
        ),
    }
}
pub fn config(journal: String) -> Config {
    Config {
        tokens: vec![U256::from(1)],
        order_budget_pusd: "1.00".parse().unwrap(),
        total_budget_pusd: "100.00".parse().unwrap(),
        min_edge: "0.01".parse().unwrap(),
        max_price: "0.99".parse().unwrap(),
        max_signal_age_ms: 200,
        max_inflight: 16,
        queue_capacity: 64,
        request_timeout_ms: 1000,
        metadata_ttl_ms: 300000,
        journal,
    }
}
pub fn signal(id: &str) -> Signal {
    Signal {
        id: id.into(),
        token_id: U256::from(1),
        ask: "0.50".parse().unwrap(),
        fair_value: "0.60".parse().unwrap(),
        observed_at_ms: now_ms(),
        book_valid: true,
    }
}
