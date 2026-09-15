//! Opt-in acceptance requirements, intentionally allowed to fail on the current implementation.
//! Every network address comes from a loopback fixture; the runner additionally disables networking.
use super::*;
use alloy::primitives::U256;
use axum::{http::StatusCode, routing::post};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{net::TcpListener, task::JoinHandle};

alloy::sol! {
    event RequestPrice(address indexed requester, bytes32 identifier, uint256 timestamp, bytes ancillaryData, address currency, uint256 reward, uint256 finalFee);
}
const HEAD: u64 = 1000;
fn hash(n: u64) -> String {
    format!("0x{n:064x}")
}
fn raw<E: SolEvent>(event: E, oracle: &str, block: u64, index: u64) -> Value {
    let log = event.encode_log_data();
    json!({"address":oracle,"topics":log.topics(),"data":alloy::primitives::hex::encode_prefixed(&log.data),
        "blockNumber":format!("0x{block:x}"),"logIndex":format!("0x{index:x}"),"blockHash":hash(block+1),
        "transactionHash":hash(block*100+index),"transactionIndex":"0x0","removed":false})
}
fn fixture(kind: &str, oracle: &str, requester: &str, block: u64, index: u64) -> Value {
    let requester = requester.parse::<Address>().unwrap();
    let ancillary = alloy::primitives::Bytes::from_static(b"q: isolated fixture, market_id: 101");
    let one = alloy::primitives::I256::try_from(1_000_000_000_000_000_000u64).unwrap();
    match kind {
        "request" => raw(
            RequestPrice {
                requester,
                identifier: B256::ZERO,
                timestamp: U256::from(42),
                ancillaryData: ancillary,
                currency: Address::ZERO,
                reward: U256::ZERO,
                finalFee: U256::ZERO,
            },
            oracle,
            block,
            index,
        ),
        "propose" => raw(
            ProposePrice {
                requester,
                proposer: Address::ZERO,
                identifier: B256::ZERO,
                timestamp: U256::from(42),
                ancillaryData: ancillary,
                proposedPrice: one,
                expirationTimestamp: U256::from(99),
                currency: Address::ZERO,
            },
            oracle,
            block,
            index,
        ),
        "dispute" => raw(
            DisputePrice {
                requester,
                proposer: Address::ZERO,
                disputer: Address::ZERO,
                identifier: B256::ZERO,
                timestamp: U256::from(42),
                ancillaryData: ancillary,
                proposedPrice: one,
            },
            oracle,
            block,
            index,
        ),
        "settle" => raw(
            Settle {
                requester,
                proposer: Address::ZERO,
                disputer: Address::ZERO,
                identifier: B256::ZERO,
                timestamp: U256::from(42),
                ancillaryData: ancillary,
                price: one,
                payout: U256::ZERO,
            },
            oracle,
            block,
            index,
        ),
        _ => panic!("unknown fixture"),
    }
}
fn proposal(block: u64, index: u64) -> Value {
    fixture(
        "propose",
        ORACLES[0],
        requesters(ORACLES[0])[0],
        block,
        index,
    )
}
#[derive(Clone, Copy)]
enum Mode {
    Good,
    RateLimit,
    Empty,
    WrongLogs,
    BadHeadAndUnavailableLogs,
}
struct Model {
    logs: Vec<Value>,
    mode: Mode,
    seconds_per_block: u64,
    transactions: usize,
    calls: Mutex<Vec<(String, Value)>>,
    bytes: AtomicU64,
}
impl Model {
    fn timestamp(&self, block: u64) -> u64 {
        now_ms() / 1000 - HEAD.saturating_sub(block) * self.seconds_per_block
    }
    fn header(&self, block: u64) -> Value {
        let h = if matches!(self.mode, Mode::BadHeadAndUnavailableLogs) && block == HEAD {
            hash(99_999)
        } else {
            hash(block + 1)
        };
        json!({"number":format!("0x{block:x}"),"hash":h,"parentHash":hash(block),"timestamp":format!("0x{:x}",self.timestamp(block)),
            "transactions":(0..self.transactions).map(|n|hash(n as u64+1)).collect::<Vec<_>>()})
    }
}
async fn handler(
    State(model): State<Arc<Model>>,
    Json(req): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let method = req["method"].as_str().unwrap_or("");
    let params = &req["params"];
    model
        .calls
        .lock()
        .await
        .push((method.into(), params.clone()));
    let failure = matches!(model.mode, Mode::RateLimit)
        || (matches!(model.mode, Mode::BadHeadAndUnavailableLogs) && method == "eth_getLogs");
    let (status, result) = if failure {
        (
            StatusCode::TOO_MANY_REQUESTS,
            json!({"jsonrpc":"2.0","id":req["id"],"error":{"code":429,"message":"fixture rate limit"}}),
        )
    } else {
        let value = match method {
            "eth_chainId" => json!("0x89"),
            "eth_getBlockByNumber" => {
                let n = if params[0] == "latest" {
                    HEAD
                } else {
                    hex_u64(&params[0]).unwrap()
                };
                model.header(n)
            }
            "eth_getLogs" => {
                let from = hex_u64(&params[0]["fromBlock"]).unwrap();
                let to = hex_u64(&params[0]["toBlock"]).unwrap();
                let mut rows: Vec<Value> = model
                    .logs
                    .iter()
                    .filter(|l| (from..=to).contains(&hex_u64(&l["blockNumber"]).unwrap()))
                    .cloned()
                    .collect();
                if matches!(model.mode, Mode::Empty) {
                    rows.clear();
                }
                if matches!(model.mode, Mode::WrongLogs) {
                    for r in &mut rows {
                        r["blockHash"] = json!(hash(88_888));
                    }
                }
                json!(rows)
            }
            _ => panic!("unexpected test RPC method: {method}"),
        };
        (
            StatusCode::OK,
            json!({"jsonrpc":"2.0","id":req["id"],"result":value}),
        )
    };
    model.bytes.fetch_add(
        serde_json::to_vec(&result).unwrap().len() as u64,
        Ordering::Relaxed,
    );
    (status, Json(result))
}
struct Node {
    url: String,
    model: Arc<Model>,
    task: JoinHandle<()>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Node {
    async fn start(mode: Mode, logs: Vec<Value>, step: u64, transactions: usize) -> Self {
        let model = Arc::new(Model {
            logs,
            mode,
            seconds_per_block: step,
            transactions,
            calls: Mutex::new(Vec::new()),
            bytes: AtomicU64::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/", post(handler))
            .with_state(model.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, model, task }
    }
    async fn count_header(&self, block: u64) -> usize {
        self.model
            .calls
            .lock()
            .await
            .iter()
            .filter(|(m, p)| m == "eth_getBlockByNumber" && p[0] == format!("0x{block:x}"))
            .count()
    }
}
async fn app(nodes: &[&Node], initialized: bool) -> Arc<App> {
    assert!(!nodes.is_empty());
    assert!(nodes.iter().all(|n| n.url.starts_with("http://127.0.0.1:")));
    let nats_url = std::env::var("UMA_REPLACEMENT_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let nats = async_nats::connect(&nats_url)
        .await
        .expect("isolated NATS fixture is required");
    Arc::new(App {
        cfg: Config {
            http_urls: nodes.iter().map(|n| n.url.clone()).collect(),
            ws_urls: vec![],
            nats_url,
            bind: "127.0.0.1:0".into(),
            retention_seconds: 14_400,
        },
        rpc: Rpc {
            http: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
            urls: nodes.iter().map(|n| n.url.clone()).collect(),
        },
        feed: RwLock::new(Feed {
            initialized,
            last_scan_ms: now_ms(),
            head_timestamp: now_ms() / 1000,
            head_block: HEAD,
            scanned_block: if initialized { HEAD } else { 0 },
            scanned_hash: if initialized {
                hash(HEAD + 1)
            } else {
                String::new()
            },
            ..Feed::default()
        }),
        publish: Mutex::new(()),
        nats,
        epoch: "isolated-test-epoch".into(),
        ws: RwLock::new(vec![json!({"connected":false})]),
        headers: Mutex::new(BTreeMap::new()),
    })
}
fn evidence(id: &str, value: Value) {
    println!("EVIDENCE_JSON {}", json!({"id":id,"observed":value}));
}
async fn collect(sub: &mut async_nats::Subscriber) -> Vec<Value> {
    let mut rows = Vec::new();
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(80), sub.next()).await {
        rows.push(serde_json::from_slice(&m.payload).unwrap());
    }
    rows
}

#[test]
#[ignore = "replacement acceptance gate"]
fn compat_request_price() {
    let event = fixture("request", ORACLES[0], requesters(ORACLES[0])[0], 900, 1);
    let result = decode(&event, now_ms() / 1000);
    evidence(
        "COMPAT-01",
        json!({"decoded":result.is_ok(),"subscribed":topics().contains(&RequestPrice::SIGNATURE_HASH.to_string())}),
    );
    assert!(
        result.is_ok(),
        "RequestPrice must be decoded: {:?}",
        result.err()
    );
    assert!(
        topics().contains(&RequestPrice::SIGNATURE_HASH.to_string()),
        "RequestPrice must be subscribed"
    );
}

#[test]
#[ignore = "replacement acceptance gate"]
fn decoder_all_existing_adapters() {
    let mut checked = 0;
    for oracle in ORACLES {
        for requester in requesters(oracle) {
            for kind in ["propose", "dispute", "settle"] {
                let e = decode(&fixture(kind, oracle, requester, 900, 1), now_ms() / 1000)
                    .unwrap()
                    .unwrap();
                assert_eq!(e.market_id, "101");
                assert_eq!(e.oracle_address, oracle);
                assert_eq!(e.proposed_price, "1");
                checked += 1;
            }
        }
    }
    evidence(
        "DECODE-01",
        json!({"oracle_adapter_event_combinations":checked}),
    );
    assert!(
        decode(
            &fixture("propose", ORACLES[0], &Address::ZERO.to_string(), 900, 1),
            0
        )
        .unwrap()
        .is_none()
    );
}

#[test]
#[ignore = "replacement acceptance gate"]
fn decoder_recorded_chain_samples() {
    let rows: Value =
        serde_json::from_str(include_str!("../fixtures/uma_replacement_real.json")).unwrap();
    let rows = rows["cases"].as_array().unwrap();
    assert!(!rows.is_empty());
    for row in rows {
        let e = decode(&row["raw"], 0).unwrap().unwrap();
        let expected = &row["expected"];
        assert_eq!(e.market_id, expected["market_id"].as_str().unwrap());
        assert_eq!(e.request_id, expected["request_id"].as_str().unwrap());
        assert_eq!(
            e.request_timestamp,
            expected["request_timestamp"].as_u64().unwrap()
        );
        assert_eq!(e.channel, expected["channel"].as_str().unwrap());
        assert_eq!(
            e.proposed_price.parse::<rust_decimal::Decimal>().unwrap(),
            expected["price"]
                .as_str()
                .unwrap()
                .parse::<rust_decimal::Decimal>()
                .unwrap()
        );
    }
    evidence(
        "DECODE-02",
        json!({"recorded_events":rows.len(),"timestamp_check":"not covered by this decoder fixture"}),
    );
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn compat_legacy_redis_delivery() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    let redis_url = std::env::var("UMA_REPLACEMENT_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let mut sub = redis::Client::open(redis_url)
        .unwrap()
        .get_async_pubsub()
        .await
        .unwrap();
    sub.subscribe("uma:resolution").await.unwrap();
    app.insert(
        decode(&proposal(999, 1), now_ms() / 1000).unwrap().unwrap(),
        false,
    )
    .await
    .unwrap();
    app.nats.flush().await.unwrap();
    let mut stream = sub.on_message();
    let message = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
    evidence(
        "COMPAT-02",
        json!({"legacy_message_received":matches!(message,Ok(Some(_))),"field_contract":"checked only after delivery"}),
    );
    let message = message
        .expect("old Redis consumer must receive the proposal")
        .unwrap();
    let value: Value = serde_json::from_slice(&message.get_payload::<Vec<u8>>().unwrap()).unwrap();
    for key in [
        "requester",
        "identifier_hex",
        "ancillary_data",
        "market_id",
        "request_id",
        "request_timestamp",
        "block_number",
        "log_index",
        "tx_hash",
        "proposed_price",
        "listener_received_ts",
    ] {
        assert!(!value[key].is_null(), "legacy field missing: {key}");
    }
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn duplicate_is_published_once() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    let mut sub = app.nats.subscribe(SUBJECT).await.unwrap();
    app.nats.flush().await.unwrap();
    let e = decode(&proposal(999, 1), now_ms() / 1000).unwrap().unwrap();
    app.insert(e.clone(), false).await.unwrap();
    app.insert(e, true).await.unwrap();
    app.nats.flush().await.unwrap();
    let rows = collect(&mut sub).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(app.feed.read().await.sequence, 1);
    evidence("STATE-01", json!({"publications":rows.len()}));
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn terminal_state_survives_out_of_order_events() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    for (kind, block) in [("settle", 999), ("propose", 997), ("dispute", 998)] {
        let e = decode(
            &fixture(kind, ORACLES[0], requesters(ORACLES[0])[0], block, 1),
            now_ms() / 1000,
        )
        .unwrap()
        .unwrap();
        app.insert(e, false).await.unwrap();
    }
    let value = snapshot(State(app)).await.0;
    let (_, _, life) = crate::native_uma::snapshot(&value).unwrap();
    assert_eq!(life.marks["101"].status, "settled");
    assert!(!life.has_dispute("101"));
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn stale_source_is_not_ready() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    app.feed.write().await.last_scan_ms = now_ms() - 10_001;
    let snap = snapshot(State(app)).await.0;
    assert_eq!(snap["health"]["ready"], false);
    assert!(crate::native_uma::snapshot(&snap).is_err());
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn rate_limit_uses_healthy_backup() {
    let logs = vec![proposal(999, 1)];
    let bad = Node::start(Mode::RateLimit, logs.clone(), 1, 0).await;
    let good = Node::start(Mode::Good, logs, 1, 0).await;
    let app = app(&[&bad, &good], true).await;
    app.rescan(990, HEAD, good.model.header(HEAD))
        .await
        .unwrap();
    assert_eq!(app.feed.read().await.events.len(), 1);
    assert!(!bad.model.calls.lock().await.is_empty());
    assert!(!good.model.calls.lock().await.is_empty());
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn inconsistent_logs_use_healthy_backup() {
    let logs = vec![proposal(999, 1)];
    let bad = Node::start(Mode::WrongLogs, logs.clone(), 1, 0).await;
    let good = Node::start(Mode::Good, logs, 1, 0).await;
    let app = app(&[&bad, &good], true).await;
    app.rescan(990, HEAD, good.model.header(HEAD))
        .await
        .unwrap();
    assert_eq!(app.feed.read().await.events.len(), 1);
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn bad_primary_head_must_not_pin_healthy_backup() {
    let logs = vec![proposal(999, 1)];
    let bad = Node::start(Mode::BadHeadAndUnavailableLogs, logs.clone(), 1, 0).await;
    let good = Node::start(Mode::Good, logs, 1, 0).await;
    let app = app(&[&bad, &good], true).await;
    let head = app.rpc.header("latest").await.unwrap();
    let result = app.rescan(990, HEAD, head).await;
    evidence(
        "RPC-01",
        json!({"success":result.is_ok(),"error":result.as_ref().err().map(ToString::to_string),"backup_requests":good.model.calls.lock().await.len()}),
    );
    assert!(
        result.is_ok(),
        "a bad primary head must not prevent a coherent backup scan"
    );
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn silent_empty_responses_must_not_report_complete() {
    let logs = vec![proposal(999, 1)];
    let bad = Node::start(Mode::Empty, logs.clone(), 1, 0).await;
    let good = Node::start(Mode::Good, logs, 1, 0).await;
    let app = app(&[&bad, &good], true).await;
    for _ in 0..3 {
        app.rescan(990, HEAD, good.model.header(HEAD))
            .await
            .unwrap();
    }
    let f = app.feed.read().await;
    evidence(
        "RPC-02",
        json!({"events":f.events.len(),"reported_ready":f.ready(now_ms()),"backup_requests":good.model.calls.lock().await.len()}),
    );
    assert!(
        f.events.len() == 1 || !f.ready(now_ms()),
        "three silent omissions cannot be reported as complete healthy data"
    );
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn reorg_removes_orphan_and_emits_reset() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    let mut orphan = decode(&proposal(999, 1), now_ms() / 1000).unwrap().unwrap();
    orphan.block_hash = hash(777);
    app.feed.write().await.events.insert(orphan.key(), orphan);
    let mut sub = app.nats.subscribe(SUBJECT).await.unwrap();
    app.nats.flush().await.unwrap();
    app.rescan(990, HEAD, node.model.header(HEAD))
        .await
        .unwrap();
    app.nats.flush().await.unwrap();
    assert!(app.feed.read().await.events.is_empty());
    assert!(collect(&mut sub).await.iter().any(|r| r["kind"] == "reset"));
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn unchanged_window_must_not_download_old_blocks_again() {
    let node = Node::start(
        Mode::Good,
        vec![proposal(998, 1), proposal(999, 1)],
        1,
        1000,
    )
    .await;
    let app = app(&[&node], true).await;
    app.rescan(990, HEAD, node.model.header(HEAD))
        .await
        .unwrap();
    let first_bytes = node.model.bytes.load(Ordering::Relaxed);
    let first_count = node.count_header(998).await;
    app.rescan(990, HEAD, node.model.header(HEAD))
        .await
        .unwrap();
    let second_count = node.count_header(998).await;
    evidence(
        "COST-01",
        json!({"first_scan_body_bytes":first_bytes,"second_scan_body_bytes":node.model.bytes.load(Ordering::Relaxed)-first_bytes,"historical_block_reads_after_first":first_count,"historical_block_reads_after_second":second_count,"transaction_hashes_per_block":1000,"fixture":"synthetic; excludes HTTP/TLS overhead"}),
    );
    assert_eq!(
        second_count, first_count,
        "unchanged historical block body should be reused after canonical validation"
    );
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn consumer_can_rebuild_current_state_after_missing_messages() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    app.insert(
        decode(&proposal(999, 1), now_ms() / 1000).unwrap().unwrap(),
        false,
    )
    .await
    .unwrap();
    app.nats.flush().await.unwrap();
    let value = snapshot(State(app)).await.0;
    let (_, _, life) = crate::native_uma::snapshot(&value).unwrap();
    assert_eq!(life.marks["101"].status, "proposed");
}

#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn long_gap_must_replay_events_older_than_trading_window() {
    let node = Node::start(Mode::Good, vec![proposal(200, 1), proposal(990, 1)], 30, 0).await;
    let app = app(&[&node], false).await;
    {
        let mut f = app.feed.write().await;
        f.scanned_block = 100;
        f.scanned_hash = hash(101);
    }
    let mut sub = app.nats.subscribe(SUBJECT).await.unwrap();
    app.nats.flush().await.unwrap();
    let worker = tokio::spawn(app.clone().scan_loop());
    for _ in 0..100 {
        if app.feed.read().await.initialized {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let initialized = app.feed.read().await.initialized;
    worker.abort();
    app.nats.flush().await.unwrap();
    assert!(
        initialized,
        "fixture must finish bootstrap before evaluating replay"
    );
    let events = collect(&mut sub).await;
    let old_seen = events.iter().any(|v| v["event"]["block_number"] == 200);
    evidence(
        "RECOVERY-01",
        json!({"supplied_resume_block":100,"missed_event_block":200,"event_age_seconds":24_000,"old_event_replayed":old_seen,"bootstrap_finished":initialized}),
    );
    assert!(
        old_seen,
        "global recovery must not truncate a supplied recovery cursor to the four-hour trading cache"
    );
}

async fn ws_connection(socket: tokio::net::TcpStream, log: Value) {
    let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
    for _ in 0..2 {
        let req: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        let id = req["id"].as_u64().unwrap();
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0","id":id,"result":if id==1 {"logs"}else{"heads"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    }
    ws.send(Message::Text(json!({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"logs","result":log}}).to_string().into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _ = ws.close(None).await;
}
#[tokio::test]
#[ignore = "replacement acceptance gate"]
async fn websocket_disconnect_reconnects_and_receives_next_event() {
    let node = Node::start(Mode::Good, vec![], 1, 0).await;
    let app = app(&[&node], true).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for index in 1..=2 {
            let (socket, _) = listener.accept().await.unwrap();
            ws_connection(socket, proposal(999, index)).await;
        }
    });
    let worker = tokio::spawn(app.clone().ws_loop(0, url));
    for _ in 0..150 {
        if app.feed.read().await.events.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    worker.abort();
    server.abort();
    assert_eq!(
        app.feed.read().await.events.len(),
        2,
        "both messages around a real loopback socket disconnect must arrive"
    );
}
