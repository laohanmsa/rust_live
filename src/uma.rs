//! Dedicated, read-only UMA feed. WebSocket speed plus independently scanned canonical logs.
use crate::now_ms;
use alloy::{
    primitives::{Address, B256, keccak256},
    sol,
    sol_types::SolEvent,
};
use anyhow::{Context, Result, bail, ensure};
use axum::{Json, Router, extract::State, routing::get};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

sol! {
    event ProposePrice(address indexed requester, address indexed proposer, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 proposedPrice, uint256 expirationTimestamp, address currency);
    event DisputePrice(address indexed requester, address indexed proposer, address indexed disputer, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 proposedPrice);
    event Settle(address indexed requester, address indexed proposer, address indexed disputer, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 price, uint256 payout);
}
const SUBJECT: &str = "rust.uma.events";
const ORACLES: [&str; 2] = [
    "0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1",
    "0xee3afe347d5c74317041e2618c49534daf887c24",
];
const MAX_EVENTS: usize = 100_000;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http_urls: Vec<String>,
    pub ws_urls: Vec<String>,
    pub nats_url: String,
    pub bind: String,
    pub retention_seconds: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub channel: String,
    pub market_id: String,
    pub request_id: String,
    pub request_timestamp: u64,
    pub oracle_address: String,
    pub block_number: u64,
    pub log_index: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub block_timestamp: u64,
    pub proposed_price: String,
    pub received_at_ms: u64,
}
impl Event {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.market_id.is_empty()
                && self.market_id.len() <= 40
                && self.market_id.bytes().all(|b| b.is_ascii_digit()),
            "invalid market ID"
        );
        ensure!(
            self.request_id.len() == 64 && self.request_id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid request ID"
        );
        ensure!(
            ORACLES.contains(&self.oracle_address.as_str())
                && ["uma:resolution", "uma:dispute_price", "uma:settle"]
                    .contains(&self.channel.as_str()),
            "unsupported UMA event"
        );
        self.block_hash.parse::<B256>()?;
        self.tx_hash.parse::<B256>()?;
        ensure!(
            self.proposed_price.len() <= 80 && self.block_timestamp <= now_ms() / 1000 + 15,
            "invalid UMA event fields"
        );
        Ok(())
    }
    fn key(&self) -> (u64, u64) {
        (self.block_number, self.log_index)
    }
}
fn hex_u64(v: &Value) -> Result<u64> {
    Ok(u64::from_str_radix(
        v.as_str()
            .context("missing hex integer")?
            .trim_start_matches("0x"),
        16,
    )?)
}
fn requesters(oracle: &str) -> &'static [&'static str] {
    match oracle {
        "0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1" => &[
            "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7",
            "0x69c47de9d4d3dad79590d61b9e05918e03775f24",
        ],
        "0xee3afe347d5c74317041e2618c49534daf887c24" => &[
            "0x2f5e3684cb1f318ec51b00edba38d79ac2c0aa9d",
            "0x6a9d222616c90fca5754cd1333cfd9b7fb6a4f74",
            "0x157ce2d672854c848c9b79c49a8cc6cc89176a49",
        ],
        _ => &[],
    }
}
fn topics() -> Vec<String> {
    [
        ProposePrice::SIGNATURE_HASH,
        DisputePrice::SIGNATURE_HASH,
        Settle::SIGNATURE_HASH,
    ]
    .map(|t| t.to_string())
    .to_vec()
}
pub fn decode(log: &Value, block_timestamp: u64) -> Result<Option<Event>> {
    let oracle = log["address"]
        .as_str()
        .context("missing oracle")?
        .to_ascii_lowercase();
    ensure!(ORACLES.contains(&oracle.as_str()), "unexpected oracle");
    let topics: Vec<B256> = log["topics"]
        .as_array()
        .context("missing topics")?
        .iter()
        .map(|v| {
            v.as_str()
                .context("invalid topic")?
                .parse()
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<_>>()?;
    let data = alloy::primitives::hex::decode(log["data"].as_str().context("missing data")?)?;
    ensure!(data.len() <= 64 * 1024, "oversized UMA event");
    let (channel, requester, timestamp, ancillary, price) = match topics.first() {
        Some(t) if *t == ProposePrice::SIGNATURE_HASH => {
            let e = ProposePrice::decode_raw_log_validate(topics, &data)?;
            (
                "uma:resolution",
                e.requester,
                e.timestamp,
                e.ancillaryData,
                e.proposedPrice,
            )
        }
        Some(t) if *t == DisputePrice::SIGNATURE_HASH => {
            let e = DisputePrice::decode_raw_log_validate(topics, &data)?;
            (
                "uma:dispute_price",
                e.requester,
                e.timestamp,
                e.ancillaryData,
                e.proposedPrice,
            )
        }
        Some(t) if *t == Settle::SIGNATURE_HASH => {
            let e = Settle::decode_raw_log_validate(topics, &data)?;
            (
                "uma:settle",
                e.requester,
                e.timestamp,
                e.ancillaryData,
                e.price,
            )
        }
        _ => bail!("unexpected topic"),
    };
    if !requesters(&oracle)
        .iter()
        .any(|a| a.parse::<Address>().ok() == Some(requester))
    {
        return Ok(None);
    }
    let text = std::str::from_utf8(&ancillary)?;
    let Some((_, tail)) = text.split_once("market_id:") else {
        return Ok(None);
    };
    let market: String = tail
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    if market.is_empty() {
        return Ok(None);
    }
    let scaled = price
        .to_string()
        .parse::<rust_decimal::Decimal>()
        .ok()
        .map(|v| {
            (v / rust_decimal::Decimal::from(1_000_000_000_000_000_000u64))
                .normalize()
                .to_string()
        })
        .unwrap_or_else(|| "non_binary".into());
    Ok(Some(Event {
        channel: channel.into(),
        market_id: market,
        request_id: keccak256(&ancillary)
            .to_string()
            .trim_start_matches("0x")
            .into(),
        request_timestamp: timestamp.try_into()?,
        oracle_address: oracle,
        block_number: hex_u64(&log["blockNumber"])?,
        log_index: hex_u64(&log["logIndex"])?,
        block_hash: log["blockHash"]
            .as_str()
            .context("missing block hash")?
            .parse::<B256>()?
            .to_string(),
        tx_hash: log["transactionHash"]
            .as_str()
            .context("missing tx hash")?
            .parse::<B256>()?
            .to_string(),
        block_timestamp,
        proposed_price: scaled,
        received_at_ms: now_ms(),
    }))
}
#[derive(Default)]
pub struct Feed {
    pub events: BTreeMap<(u64, u64), Event>,
    pub sequence: u64,
    pub last_scan_ms: u64,
    pub head_block: u64,
    pub scanned_block: u64,
    pub scanned_hash: String,
    pub head_timestamp: u64,
    pub initialized: bool,
    pub fault: String,
    pub pruned: u64,
    pub reorgs: u64,
    pub recovered_logs: u64,
}
impl Feed {
    pub fn ready(&self, now: u64) -> bool {
        self.initialized
            && self.fault.is_empty()
            && now.saturating_sub(self.last_scan_ms) <= 10_000
            && now.saturating_sub(self.head_timestamp * 1000) <= 15_000
            && self.head_block.saturating_sub(self.scanned_block) <= 6
    }
    pub fn prune(&mut self, cutoff: u64) {
        let before = self.events.len();
        self.events.retain(|_, e| e.block_timestamp >= cutoff);
        self.pruned += (before - self.events.len()) as u64;
    }
}
struct Rpc {
    http: reqwest::Client,
    urls: Vec<String>,
}
impl Rpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        for url in &self.urls {
            let result = async {
                let r = self
                    .http
                    .post(url)
                    .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
                    .send()
                    .await?
                    .error_for_status()?;
                let bytes = r.bytes().await?;
                ensure!(bytes.len() <= 32 * 1024 * 1024, "RPC response limit");
                let value: Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    value["id"] == 1 && value.get("error").is_none(),
                    "RPC rejected request"
                );
                if method == "eth_getBlockByNumber" && params[0] == "latest" {
                    let ts = hex_u64(&value["result"]["timestamp"])?;
                    ensure!(now_ms().abs_diff(ts * 1000) < 15_000, "stale RPC head");
                }
                value
                    .get("result")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .context("empty RPC result")
            }
            .await;
            if result.is_ok() {
                return result;
            }
        }
        bail!("all RPC providers failed")
    }
    async fn header(&self, block: &str) -> Result<Value> {
        self.call("eth_getBlockByNumber", json!([block, false]))
            .await
    }
}
struct App {
    cfg: Config,
    rpc: Rpc,
    feed: RwLock<Feed>,
    // All publications, snapshots and resets use this ordering boundary.
    publish: Mutex<()>,
    nats: async_nats::Client,
    epoch: String,
    ws: RwLock<Vec<Value>>,
    headers: Mutex<BTreeMap<u64, (String, u64)>>,
}
impl App {
    async fn health(&self) -> Value {
        let f = self.feed.read().await;
        let now = now_ms();
        json!({"schema_version":1,"mode":"rust_uma","epoch":self.epoch,"sequence":f.sequence,"ready":f.ready(now),"fault":f.fault,"captured_at_ms":now,"head_block":f.head_block,"scanned_block":f.scanned_block,"head_age_ms":now.saturating_sub(f.head_timestamp*1000),"scan_age_ms":now.saturating_sub(f.last_scan_ms),"events_cached":f.events.len(),"event_capacity":MAX_EVENTS,"pruned":f.pruned,"reorgs":f.reorgs,"recovered_logs":f.recovered_logs,"ws":*self.ws.read().await})
    }
    async fn timestamp(&self, log: &Value) -> Result<u64> {
        let block = hex_u64(&log["blockNumber"])?;
        let hash = log["blockHash"].as_str().context("block hash")?;
        if let Some((cached, ts)) = self.headers.lock().await.get(&block).cloned()
            && cached == hash
        {
            return Ok(ts);
        }
        let h = self.rpc.header(&format!("0x{block:x}")).await?;
        ensure!(h["hash"].as_str() == Some(hash), "noncanonical event block");
        let ts = hex_u64(&h["timestamp"])?;
        let mut headers = self.headers.lock().await;
        headers.insert(block, (hash.into(), ts));
        while headers.len() > 16_384 {
            headers.pop_first();
        }
        Ok(ts)
    }
    async fn insert(&self, event: Event, recovered: bool) -> Result<()> {
        let _order = self.publish.lock().await;
        let mut f = self.feed.write().await;
        if event.block_timestamp * 1000 > now_ms() + 15_000 {
            bail!("future block timestamp");
        }
        if event.block_timestamp < now_ms() / 1000 - self.cfg.retention_seconds {
            return Ok(());
        }
        if let Some(old) = f.events.get(&event.key())
            && old.block_hash == event.block_hash
            && old.tx_hash == event.tx_hash
        {
            return Ok(());
        }
        if f.events.contains_key(&event.key()) {
            f.reorgs += 1;
            f.fault = "reorg_rescan".into();
        }
        f.prune(now_ms() / 1000 - self.cfg.retention_seconds);
        ensure!(f.events.len() < MAX_EVENTS, "UMA cache capacity");
        f.events.insert(event.key(), event.clone());
        f.sequence += 1;
        if recovered {
            f.recovered_logs += 1;
        }
        let msg = json!({"kind":"event","epoch":self.epoch,"sequence":f.sequence,"event":event,"ready":f.ready(now_ms())});
        drop(f);
        self.nats
            .publish(SUBJECT, serde_json::to_vec(&msg)?.into())
            .await?;
        Ok(())
    }
    async fn rescan(&self, from: u64, to: u64, head: Value) -> Result<()> {
        let mut last = None;
        for url in &self.rpc.urls {
            let rpc = Rpc {
                http: self.rpc.http.clone(),
                urls: vec![url.clone()],
            };
            match self.rescan_on(&rpc, from, to, head.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no usable scan provider")))
    }
    async fn rescan_on(&self, rpc: &Rpc, from: u64, to: u64, head: Value) -> Result<()> {
        let logs=rpc.call("eth_getLogs",json!([{"fromBlock":format!("0x{from:x}"),"toBlock":format!("0x{to:x}"),"address":ORACLES,"topics":[topics()]}])).await?;
        let logs = logs.as_array().context("logs must be an array")?;
        let mut decoded = Vec::new();
        let mut blocks = std::collections::BTreeSet::new();
        for log in logs {
            blocks.insert(hex_u64(&log["blockNumber"])?);
        }
        blocks.extend(
            self.feed
                .read()
                .await
                .events
                .range((from, 0)..=(to, u64::MAX))
                .map(|((b, _), _)| *b),
        );
        let headers = futures_util::stream::iter(blocks)
            .map(|block| async move {
                let h = rpc.header(&format!("0x{block:x}")).await?;
                ensure!(hex_u64(&h["number"])? == block, "wrong header number");
                Ok::<_, anyhow::Error>((
                    block,
                    (
                        h["hash"].as_str().context("header hash")?.to_owned(),
                        hex_u64(&h["timestamp"])?,
                    ),
                ))
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashMap<_, _>>>()?;
        for log in logs {
            let block = hex_u64(&log["blockNumber"])?;
            ensure!(
                (from..=to).contains(&block) && log["removed"] != true,
                "invalid scan range"
            );
            let (hash, ts) = &headers[&block];
            ensure!(
                log["blockHash"].as_str() == Some(hash),
                "noncanonical event block"
            );
            if let Some(e) = decode(log, *ts)? {
                decoded.push(e);
            }
        }
        // Validate the head used for this range after loading its logs.
        let check = rpc.header(&format!("0x{to:x}")).await?;
        ensure!(check["hash"] == head["hash"], "chain changed during scan");
        {
            let _order = self.publish.lock().await;
            let mut f = self.feed.write().await;
            let removed: Vec<_> = f
                .events
                .range((from, 0)..=(to, u64::MAX))
                .filter(|(_, e)| {
                    headers
                        .get(&e.block_number)
                        .is_some_and(|(hash, _)| hash != &e.block_hash)
                })
                .map(|(k, _)| *k)
                .collect();
            if !removed.is_empty() {
                for k in removed {
                    f.events.remove(&k);
                }
                f.sequence += 1;
                f.reorgs += 1;
                f.fault = "reorg_rescan".into();
                self.nats
                    .publish(
                        SUBJECT,
                        serde_json::to_vec(
                            &json!({"kind":"reset","epoch":self.epoch,"sequence":f.sequence}),
                        )?
                        .into(),
                    )
                    .await?;
            }
        }
        decoded.sort_by_key(Event::key);
        for e in decoded {
            self.insert(e, true).await?;
        }
        let mut f = self.feed.write().await;
        f.head_block = to;
        f.scanned_block = to;
        f.scanned_hash = head["hash"]
            .as_str()
            .context("scanned block hash")?
            .to_owned();
        f.head_timestamp = hex_u64(&head["timestamp"])?;
        f.last_scan_ms = now_ms();
        f.fault.clear();
        f.prune(now_ms() / 1000 - self.cfg.retention_seconds);
        Ok(())
    }
    async fn scan_loop(self: Arc<Self>) {
        loop {
            let result = async {
                ensure!(
                    hex_u64(&self.rpc.call("eth_chainId", json!([])).await?)? == 137,
                    "wrong chain"
                );
                let head = self.rpc.header("latest").await?;
                let height = hex_u64(&head["number"])?;
                let (initialized, anchor, hash) = {let f=self.feed.read().await;(f.initialized,f.scanned_block,f.scanned_hash.clone())};
                if anchor > 0 {
                    let canonical=self.rpc.header(&format!("0x{anchor:x}")).await?;
                    if canonical["hash"].as_str()!=Some(&hash) {
                        let _order=self.publish.lock().await;let mut f=self.feed.write().await;
                        f.initialized=false;f.events.clear();f.scanned_block=0;f.scanned_hash.clear();f.sequence+=1;f.reorgs+=1;f.fault="reorg_rescan".into();
                        self.headers.lock().await.clear();
                        self.nats.publish(SUBJECT,serde_json::to_vec(&json!({"kind":"reset","epoch":self.epoch,"sequence":f.sequence}))?.into()).await?;
                        bail!("reorg requires full reconstruction");
                    }
                }
                if !initialized {
                    let target = now_ms() / 1000 - self.cfg.retention_seconds;
                    let (mut low, mut high) = (height.saturating_sub(20_000), height);
                    while low>0 && hex_u64(&self.rpc.header(&format!("0x{low:x}")).await?["timestamp"])? > target {low=low.saturating_sub(20_000);}
                    while low < high {
                        let mid = low + (high - low) / 2;
                        let h = self.rpc.header(&format!("0x{mid:x}")).await?;
                        if hex_u64(&h["timestamp"])? < target {
                            low = mid + 1;
                        } else {
                            high = mid;
                        }
                    }
                    let mut from = low.max(self.feed.read().await.scanned_block.saturating_sub(64));
                    while from <= height {
                        let to = (from + 499).min(height);
                        let h = self.rpc.header(&format!("0x{to:x}")).await?;
                        self.rescan(from, to, h).await?;
                        from = to + 1;
                        println!(
                            "{}",
                            json!({"event":"uma_backfill","scanned_block":to,"target_block":height})
                        );
                    }
                    let verified=self.rpc.header(&format!("0x{height:x}")).await?;
                    if verified["hash"] != head["hash"] {
                        let mut f=self.feed.write().await;f.events.clear();f.scanned_block=0;f.scanned_hash.clear();f.fault="bootstrap_reorg".into();
                        bail!("bootstrap chain changed");
                    }
                    self.feed.write().await.initialized = true;
                } else {
                    let from = self.feed.read().await.scanned_block.saturating_sub(64);
                    let to = height.min(from + 499);
                    let h = if to == height {
                        head
                    } else {
                        self.rpc.header(&format!("0x{to:x}")).await?
                    };
                    self.rescan(from, to, h).await?;
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                let text = error.to_string();
                let detail = if text.contains("://") {
                    "rpc_transport_error".to_owned()
                } else {
                    text.chars().filter(|c| !c.is_control()).take(160).collect()
                };
                self.feed.write().await.fault = format!("rpc_scan_failed: {detail}");
                println!(
                    "{}",
                    json!({"event":"uma_scan_failed","detail":detail,"at_ms":now_ms()})
                );
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    async fn ws_loop(self: Arc<Self>, index: usize, url: String) {
        loop {
            let result: Result<()> =async {
                let (mut socket,_)=tokio::time::timeout(Duration::from_secs(5),connect_async(&url)).await??;
                socket.send(Message::Text(json!({"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["logs",{"address":ORACLES,"topics":[topics()]}]}).to_string().into())).await?;
                socket.send(Message::Text(json!({"jsonrpc":"2.0","id":2,"method":"eth_subscribe","params":["newHeads"]}).to_string().into())).await?;
                let mut subscriptions=HashMap::new();
                loop {
                    let msg=tokio::time::timeout(Duration::from_secs(12),socket.next()).await?.context("WS ended")??;
                    if let Message::Text(text)=msg {
                        ensure!(text.len()<128*1024,"oversized WS message");let v:Value=serde_json::from_str(&text)?;
                        ensure!(v.get("error").is_none(),"WS subscription error");
                        if let (Some(id),Some(sub))=(v["id"].as_u64(),v["result"].as_str()) {subscriptions.insert(sub.to_owned(),id);}
                        if let Some(sub)=v["params"]["subscription"].as_str() {
                            let result=&v["params"]["result"];
                            self.ws.write().await[index]=json!({"connected":true,"last_message_ms":now_ms(),"index":index});
                            match subscriptions.get(sub) {
                                Some(2)=> {let n=hex_u64(&result["number"])?;let ts=hex_u64(&result["timestamp"])?;let hash=result["hash"].as_str().context("head hash")?.to_owned();let mut h=self.headers.lock().await;h.insert(n,(hash,ts));while h.len()>16384 {h.pop_first();}},
                                Some(1)=> {
                                    if result["removed"]==true {
                                        let _order=self.publish.lock().await;let mut f=self.feed.write().await;
                                        let key=(hex_u64(&result["blockNumber"])?,hex_u64(&result["logIndex"])?);
                                        if f.events.get(&key).is_some_and(|e|Some(e.block_hash.as_str())==result["blockHash"].as_str()) {f.events.remove(&key);}
                                        f.fault="reorg_rescan".into();f.sequence+=1;
                                        self.nats.publish(SUBJECT,serde_json::to_vec(&json!({"kind":"reset","epoch":self.epoch,"sequence":f.sequence}))?.into()).await?;
                                    } else {
                                        let ts=self.timestamp(result).await?;
                                        if let Some(event)=decode(result,ts)? {self.insert(event,false).await?;}
                                    }
                                },
                                _=>bail!("unexpected subscription"),
                            }
                        }
                    } else if msg.is_close() {bail!("WS closed");}
                }
            }.await;
            self.ws.write().await[index] =
                json!({"connected":false,"index":index,"last_failure_ms":now_ms()});
            if result.is_err() {
                println!(
                    "{}",
                    json!({"event":"uma_ws_reconnect","index":index,"at_ms":now_ms()})
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
async fn snapshot(State(app): State<Arc<App>>) -> Json<Value> {
    let _order = app.publish.lock().await;
    let health = app.health().await;
    let events = app
        .feed
        .read()
        .await
        .events
        .values()
        .cloned()
        .collect::<Vec<_>>();
    Json(json!({"health":health,"events":events}))
}
async fn health(State(app): State<Arc<App>>) -> Json<Value> {
    Json(app.health().await)
}
pub async fn serve(path: &Path) -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cfg: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    ensure!(
        (14_400..=86_400).contains(&cfg.retention_seconds)
            && !cfg.http_urls.is_empty()
            && !cfg.ws_urls.is_empty()
            && cfg.ws_urls.len() <= 2,
        "invalid UMA config"
    );
    ensure!(
        cfg.http_urls.iter().all(|u| u.starts_with("https://"))
            && cfg.ws_urls.iter().all(|u| u.starts_with("wss://")),
        "TLS required"
    );
    let bind = cfg.bind.clone();
    let urls = cfg.ws_urls.clone();
    let rpc = Rpc {
        http: reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(4))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        urls: cfg.http_urls.clone(),
    };
    let nats = async_nats::connect(&cfg.nats_url).await?;
    let app = Arc::new(App {
        cfg,
        rpc,
        feed: RwLock::new(Feed::default()),
        publish: Mutex::new(()),
        nats,
        epoch: polymarket_client_sdk_v2::auth::Uuid::new_v4().to_string(),
        ws: RwLock::new(vec![json!({"connected":false}); urls.len()]),
        headers: Mutex::new(BTreeMap::new()),
    });
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(app.clone().scan_loop());
    for (i, url) in urls.into_iter().enumerate() {
        tasks.spawn(app.clone().ws_loop(i, url));
    }
    let heartbeat = app.clone();
    tasks.spawn(async move {
        loop {
            let _order = heartbeat.publish.lock().await;
            let h = heartbeat.health().await;
            let msg = json!({"kind":"heartbeat","epoch":heartbeat.epoch,"sequence":h["sequence"],"health":h});
            if let Ok(bytes) = serde_json::to_vec(&msg) {
                let _ = heartbeat.nats.publish(SUBJECT, bytes.into()).await;
            }
            drop(_order);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let server = axum::serve(
        tokio::net::TcpListener::bind(bind).await?,
        Router::new()
            .route("/health", get(health))
            .route("/snapshot", get(snapshot))
            .route(
                "/ready",
                get(|State(app): State<Arc<App>>| async move {
                    if app.feed.read().await.ready(now_ms()) {
                        axum::http::StatusCode::NO_CONTENT
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    }
                }),
            )
            .with_state(app),
    );
    tokio::select! {
        result=server => {result?;},
        _=tasks.join_next() => {bail!("UMA background task ended; process restart required");}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn log_for<E: SolEvent>(e: E) -> Value {
        let log = e.encode_log_data();
        json!({"address":ORACLES[0],"topics":log.topics(),"data":alloy::primitives::hex::encode_prefixed(&log.data),"blockNumber":"0xa","logIndex":"0x1","blockHash":B256::repeat_byte(1),"transactionHash":B256::repeat_byte(2)})
    }
    #[test]
    fn decoder_matches_existing_topics_and_rejects_other_adapters() -> Result<()> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _ = rustls::ClientConfig::builder();
        assert_eq!(
            ProposePrice::SIGNATURE_HASH.to_string(),
            "0x6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1"
        );
        let mut p = ProposePrice {
            requester: requesters(ORACLES[0])[0].parse()?,
            proposer: Address::ZERO,
            identifier: B256::ZERO,
            timestamp: alloy::primitives::U256::from(100),
            ancillaryData: b"question; market_id: 123".to_vec().into(),
            proposedPrice: alloy::primitives::I256::try_from(1_000_000_000_000_000_000u64)?,
            expirationTimestamp: alloy::primitives::U256::from(200),
            currency: Address::ZERO,
        };
        let e = decode(&log_for(p.clone()), 1000)?.unwrap();
        assert_eq!(e.market_id, "123");
        assert_eq!(e.proposed_price, "1");
        assert_eq!(
            e.request_id,
            keccak256(&p.ancillaryData)
                .to_string()
                .trim_start_matches("0x")
        );
        assert_eq!(
            DisputePrice::SIGNATURE_HASH.to_string(),
            "0x5165909c3d1c01c5d1e121ac6f6d01dda1ba24bc9e1f975b5a375339c15be7f3"
        );
        assert_eq!(
            Settle::SIGNATURE_HASH.to_string(),
            "0x3f384afb4bd9f0aef0298c80399950011420eb33b0e1a750b20966270247b9a0"
        );
        let disputed = DisputePrice {
            requester: p.requester,
            proposer: p.proposer,
            disputer: Address::ZERO,
            identifier: p.identifier,
            timestamp: p.timestamp,
            ancillaryData: p.ancillaryData.clone(),
            proposedPrice: p.proposedPrice,
        };
        assert_eq!(
            decode(&log_for(disputed), 1000)?.unwrap().channel,
            "uma:dispute_price"
        );
        let settled = Settle {
            requester: p.requester,
            proposer: p.proposer,
            disputer: Address::ZERO,
            identifier: p.identifier,
            timestamp: p.timestamp,
            ancillaryData: p.ancillaryData.clone(),
            price: p.proposedPrice,
            payout: alloy::primitives::U256::ZERO,
        };
        assert_eq!(
            decode(&log_for(settled), 1000)?.unwrap().channel,
            "uma:settle"
        );
        p.requester = Address::ZERO;
        assert!(decode(&log_for(p), 1000)?.is_none());
        Ok(())
    }
    #[test]
    fn quiet_chain_is_healthy_only_with_recent_complete_scan_and_cache_is_bounded() {
        let now = 1_000_000;
        let mut f = Feed {
            initialized: true,
            last_scan_ms: now,
            head_timestamp: now / 1000,
            head_block: 10,
            scanned_block: 10,
            ..Feed::default()
        };
        assert!(f.ready(now));
        assert!(!f.ready(now + 10_001));
        f.fault = "reorg_rescan".into();
        assert!(!f.ready(now));
        let e = Event {
            channel: "uma:settle".into(),
            market_id: "1".into(),
            request_id: "r".into(),
            request_timestamp: 1,
            oracle_address: ORACLES[0].into(),
            block_number: 1,
            log_index: 1,
            block_hash: "h".into(),
            tx_hash: "t".into(),
            block_timestamp: 1,
            proposed_price: "0".into(),
            received_at_ms: now,
        };
        for i in 0..25_000 {
            let mut row = e.clone();
            row.block_number = i;
            f.events.insert(row.key(), row);
        }
        f.prune(2);
        assert!(f.events.is_empty());
        assert_eq!(f.pruned, 25_000);
    }
}

// Test-only replacement gates. No code in this module enters production builds.
#[cfg(test)]
#[path = "../tests/uma_replacement/mod.rs"]
mod replacement_tests;
