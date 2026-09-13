pub mod demo;
pub mod exchange;
pub mod journal;
pub mod shadow;
pub mod shadow_history;
pub mod shadow_state;
pub mod shadow_valuation;
pub mod telemetry;

use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path as ApiPath, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use exchange::{Exchange, Market};
use journal::Journal;
use polymarket_client_sdk_v2::types::{Decimal, U256};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64
}
fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub tokens: Vec<U256>,
    pub order_budget_pusd: Decimal,
    pub total_budget_pusd: Decimal,
    pub min_edge: Decimal,
    pub max_price: Decimal,
    pub max_signal_age_ms: u64,
    pub max_inflight: usize,
    pub queue_capacity: usize,
    pub request_timeout_ms: u64,
    pub metadata_ttl_ms: u64,
    pub journal: String,
}
impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        let c: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.tokens.is_empty() && self.tokens.len() <= 100,
            "configure 1..100 allowed tokens"
        );
        ensure!(
            self.tokens.iter().all(|t| *t != U256::ZERO),
            "zero token is invalid"
        );
        ensure!(
            self.order_budget_pusd > Decimal::ZERO && self.order_budget_pusd <= Decimal::from(1000),
            "order budget must be >0 and <=1000"
        );
        ensure!(
            self.order_budget_pusd.scale() <= 2,
            "order budget supports cents"
        );
        ensure!(
            self.total_budget_pusd >= self.order_budget_pusd
                && self.total_budget_pusd <= Decimal::from(10000),
            "invalid total budget"
        );
        ensure!(
            self.min_edge >= Decimal::ZERO && self.min_edge < Decimal::ONE,
            "invalid minimum edge"
        );
        ensure!(
            self.max_price > Decimal::ZERO && self.max_price < Decimal::ONE,
            "invalid maximum price"
        );
        ensure!(
            (1..=64).contains(&self.max_inflight) && (1..=1024).contains(&self.queue_capacity),
            "invalid concurrency or queue capacity"
        );
        ensure!(
            (1..=10000).contains(&self.max_signal_age_ms),
            "invalid signal age"
        );
        ensure!(
            (10..=10000).contains(&self.request_timeout_ms),
            "invalid request timeout"
        );
        ensure!(
            (1000..=300000).contains(&self.metadata_ttl_ms),
            "metadata lifetime must be 1..300 seconds"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signal {
    pub id: String,
    pub token_id: U256,
    pub ask: Decimal,
    pub fair_value: Decimal,
    pub observed_at_ms: u64,
    pub book_valid: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reply {
    #[serde(default)]
    pub submitted_at_ms: Option<u64>,
    #[serde(default)]
    pub replayed: bool,
    pub policy_ms: Option<f64>,
    pub post_ms: Option<f64>,
    pub finalize_ms: Option<f64>,
    pub source_to_dispatch_ms: Option<f64>,
    pub id: String,
    pub state: String,
    pub reason: String,
    pub order_hash: Option<String>,
    pub exchange_status: Option<String>,
    pub queue_ms: f64,
    pub sign_ms: f64,
    pub journal_ms: f64,
    /// Application receipt to HTTP dispatch, not a packet-level wire timestamp.
    pub dispatch_ms: Option<f64>,
    pub total_ms: f64,
}
impl Reply {
    fn blocked(id: &str, reason: &str) -> Self {
        Self {
            submitted_at_ms: None,
            replayed: false,
            policy_ms: None,
            post_ms: None,
            finalize_ms: None,
            source_to_dispatch_ms: None,
            id: id.into(),
            state: "blocked".into(),
            reason: reason.into(),
            order_hash: None,
            exchange_status: None,
            queue_ms: 0.0,
            sign_ms: 0.0,
            journal_ms: 0.0,
            dispatch_ms: None,
            total_ms: 0.0,
        }
    }
}
type ApiReply = (StatusCode, Json<Value>);
fn reply(r: Reply) -> ApiReply {
    let status = match r.state.as_str() {
        "busy" => StatusCode::SERVICE_UNAVAILABLE,
        "conflict" => StatusCode::CONFLICT,
        "blocked" => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::OK,
    };
    (
        status,
        Json(serde_json::to_value(r).expect("finite timing values")),
    )
}
fn authorized(headers: &HeaderMap, key: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.strip_prefix("Bearer ") == Some(key))
}
fn unauthorized() -> ApiReply {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error":"unauthorized"})),
    )
}

struct Work {
    signal: Signal,
    received: Instant,
    response: oneshot::Sender<Reply>,
}
struct ContextState {
    telemetry: telemetry::Telemetry,
    boot_at_ms: u64,
    config: Config,
    exchange: Exchange,
    markets: HashMap<U256, Market>,
    journal: Arc<Mutex<Journal>>,
    stopped: Arc<AtomicBool>,
}
#[derive(Clone)]
struct HttpState {
    tx: mpsc::Sender<Work>,
    context: Arc<ContextState>,
    key: Arc<String>,
}

pub struct Service {
    pub router: Router,
    pub stopped: Arc<AtomicBool>,
    pub worker: JoinHandle<()>,
}
impl Service {
    pub async fn start(config: Config, exchange: Exchange, key: String) -> Result<Self> {
        config.validate()?;
        ensure!(!key.is_empty(), "empty service bearer token");
        let scope = exchange.scope();
        let journal = Journal::open(Path::new(&config.journal), &scope)?;
        ensure!(
            journal.used <= config.total_budget_pusd,
            "journal already exceeds configured budget"
        );
        let markets = exchange.warm(&config.tokens).await?;
        let stopped =
            Arc::new(AtomicBool::new(journal.orders.values().any(|o| {
                o.reply.state == "unknown" || o.reply.state == "prepared"
            })));
        let context = Arc::new(ContextState {
            telemetry: telemetry::Telemetry::default(),
            boot_at_ms: now_ms(),
            config: config.clone(),
            exchange,
            markets,
            journal: Arc::new(Mutex::new(journal)),
            stopped: stopped.clone(),
        });
        let (tx, mut rx) = mpsc::channel::<Work>(config.queue_capacity);
        let worker_context = context.clone();
        let worker = tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(config.max_inflight));
            let mut jobs = JoinSet::new();
            loop {
                tokio::select! {
                    result=jobs.join_next(), if !jobs.is_empty() => { if result.is_some_and(|r| r.is_err()) {worker_context.stopped.store(true, Ordering::SeqCst);} }
                    work=rx.recv() => {
                        let Some(work)=work else {break};
                        match slots.clone().try_acquire_owned() {
                            Ok(permit) => { let ctx=worker_context.clone(); ctx.telemetry.started(); jobs.spawn(async move { let result=process(ctx.clone(), &work.signal, work.received).await; ctx.telemetry.finished(&result,elapsed_ms(work.received),true); let _=work.response.send(result); drop(permit); }); }
                            Err(_) => { let mut r=Reply::blocked(&work.signal.id,"inflight_limit"); r.state="busy".into(); worker_context.telemetry.finished(&r,elapsed_ms(work.received),false); let _=work.response.send(r); }
                        }
                    }
                }
            }
            while jobs.join_next().await.is_some() {}
        });
        let router = Router::new()
            .route("/health", get(health))
            .route("/metrics", get(metrics))
            .route("/signal", post(signal))
            .route("/orders/{id}", get(order))
            .route("/stop", post(stop))
            .layer(DefaultBodyLimit::max(8192))
            .with_state(HttpState {
                tx,
                context,
                key: Arc::new(key),
            });
        Ok(Self {
            router,
            stopped,
            worker,
        })
    }
}
async fn health(State(s): State<HttpState>) -> Json<Value> {
    let ledger = s.context.journal.lock().await;
    Json(
        json!({"mode":s.context.exchange.mode(),"ready":!s.context.stopped.load(Ordering::SeqCst) && (s.context.exchange.mode()=="demo" || s.context.markets.values().all(|m|m.loaded.elapsed().as_millis()<=s.context.config.metadata_ttl_ms as u128)),"stopped":s.context.stopped.load(Ordering::SeqCst),"budget_used":ledger.used.to_string(),"budget_limit":s.context.config.total_budget_pusd.to_string()}),
    )
}
#[derive(Deserialize)]
struct Window {
    window_seconds: Option<u64>,
}
async fn metrics(
    State(s): State<HttpState>,
    Query(window): Query<Window>,
    headers: HeaderMap,
) -> ApiReply {
    if !authorized(&headers, &s.key) {
        return unauthorized();
    }
    let window = window.window_seconds.unwrap_or(3600);
    if !(1..=86400).contains(&window) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid_window"})),
        );
    }
    let mut data = s.context.telemetry.snapshot(window);
    data["queued"] = json!(s.context.config.queue_capacity - s.tx.capacity());
    data["boot_at_ms"] = json!(s.context.boot_at_ms);
    data["mode"] = json!(s.context.exchange.mode());
    (StatusCode::OK, Json(data))
}
async fn stop(State(s): State<HttpState>, headers: HeaderMap) -> ApiReply {
    if !authorized(&headers, &s.key) {
        return unauthorized();
    };
    s.context.stopped.store(true, Ordering::SeqCst);
    (
        StatusCode::OK,
        Json(
            json!({"stopped":true,"note":"requests already admitted to dispatch may still be sent or finish"}),
        ),
    )
}
async fn order(
    State(s): State<HttpState>,
    ApiPath(id): ApiPath<String>,
    headers: HeaderMap,
) -> ApiReply {
    if !authorized(&headers, &s.key) {
        return unauthorized();
    };
    match s.context.journal.lock().await.orders.get(&id) {
        Some(r) => reply(r.reply.clone()),
        None => (StatusCode::NOT_FOUND, Json(json!({"error":"not_found"}))),
    }
}
async fn signal(
    State(s): State<HttpState>,
    headers: HeaderMap,
    Json(signal): Json<Signal>,
) -> ApiReply {
    if !authorized(&headers, &s.key) {
        return unauthorized();
    };
    s.context.telemetry.received();
    let received = Instant::now();
    let id = signal.id.clone();
    let (response, rx) = oneshot::channel();
    if s.tx
        .try_send(Work {
            signal,
            received,
            response,
        })
        .is_err()
    {
        let mut r = Reply::blocked(&id, "queue_full_or_closed");
        r.state = "busy".into();
        s.context
            .telemetry
            .finished(&r, elapsed_ms(received), false);
        return reply(r);
    }
    reply(
        rx.await
            .unwrap_or_else(|_| Reply::blocked(&id, "worker_stopped")),
    )
}
fn validate(s: &Signal, cfg: &Config, market: &Market, live: bool) -> Result<()> {
    ensure!(
        !s.id.is_empty()
            && s.id.len() <= 100
            && s.id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid_signal_id"
    );
    ensure!(s.book_valid, "invalid_book");
    let now = now_ms();
    ensure!(s.observed_at_ms <= now.saturating_add(10), "future_signal");
    ensure!(
        now.saturating_sub(s.observed_at_ms) <= cfg.max_signal_age_ms,
        "stale_signal"
    );
    ensure!(
        !live || market.loaded.elapsed().as_millis() <= cfg.metadata_ttl_ms as u128,
        "metadata_expired_restart_required"
    );
    ensure!(
        s.ask >= market.tick
            && s.ask <= cfg.max_price
            && s.ask <= Decimal::ONE - market.tick
            && s.ask.scale() <= 6,
        "invalid_price"
    );
    ensure!(s.ask % market.tick == Decimal::ZERO, "price_off_tick");
    ensure!(
        s.fair_value > Decimal::ZERO && s.fair_value <= Decimal::ONE,
        "invalid_fair_value"
    );
    ensure!(s.fair_value - s.ask >= cfg.min_edge, "edge_below_threshold");
    Ok(())
}
async fn process(ctx: Arc<ContextState>, s: &Signal, received: Instant) -> Reply {
    let queue_ms = elapsed_ms(received);
    if let Some(old) = ctx.journal.lock().await.orders.get(&s.id) {
        if old.signal == *s {
            let mut r = old.reply.clone();
            r.replayed = true;
            return r;
        }
        let mut r = Reply::blocked(&s.id, "id_conflict");
        r.state = "conflict".into();
        return r;
    }
    if ctx.stopped.load(Ordering::SeqCst) {
        return Reply::blocked(&s.id, "stopped");
    }
    let Some(market) = ctx.markets.get(&s.token_id) else {
        return Reply::blocked(&s.id, "token_not_allowed");
    };
    let policy_start = Instant::now();
    if let Err(e) = validate(s, &ctx.config, market, ctx.exchange.mode() == "live") {
        let mut r = Reply::blocked(&s.id, &e.to_string());
        r.policy_ms = Some(elapsed_ms(policy_start));
        return r;
    }
    let policy_ms = elapsed_ms(policy_start);
    let sign_start = Instant::now();
    let signed = match tokio::time::timeout(
        Duration::from_millis(ctx.config.request_timeout_ms),
        ctx.exchange.sign(s, ctx.config.order_budget_pusd, market),
    )
    .await
    {
        Ok(Ok(signed)) => signed,
        _ => return Reply::blocked(&s.id, "signing_failed_or_below_minimum_size"),
    };
    let mut result = Reply::blocked(&s.id, "");
    result.policy_ms = Some(policy_ms);
    result.state = "prepared".into();
    result.order_hash = Some(signed.hash.clone());
    result.queue_ms = queue_ms;
    result.sign_ms = elapsed_ms(sign_start);
    let entry = journal::Stored {
        signal: s.clone(),
        reserved: ctx.config.order_budget_pusd,
        order: signed.journal_order.clone(),
        reply: result.clone(),
    };
    let journal_start = Instant::now();
    let ledger = ctx.journal.clone();
    let budget = ctx.config.total_budget_pusd;
    // ponytail: one durable writer per process; move to the shared PostgreSQL ledger before multiple executors.
    match tokio::task::spawn_blocking(move || ledger.blocking_lock().prepare(entry, budget)).await {
        Ok(Ok(Some(mut existing))) => {
            existing.replayed = true;
            return existing;
        }
        Ok(Ok(None)) => {}
        Ok(Err(e)) => {
            let reason = e.to_string();
            let mut r = Reply::blocked(&s.id, &reason);
            if reason == "id_conflict" {
                r.state = "conflict".into();
            } else if reason != "budget_exhausted" && reason != "journal_capacity" {
                ctx.stopped.store(true, Ordering::SeqCst);
                r.reason = "journal_prepare_failed".into();
            }
            return r;
        }
        Err(_) => {
            ctx.stopped.store(true, Ordering::SeqCst);
            return Reply::blocked(&s.id, "journal_worker_failed");
        }
    }
    result.journal_ms = elapsed_ms(journal_start);
    // Recheck after both signing and durable reservation, so queue/storage delays cannot send expired input.
    if ctx.stopped.load(Ordering::SeqCst)
        || validate(s, &ctx.config, market, ctx.exchange.mode() == "live").is_err()
    {
        result.state = "blocked".into();
        result.reason = "stopped_or_expired_before_dispatch".into();
    } else {
        result.dispatch_ms = Some(elapsed_ms(received));
        result.source_to_dispatch_ms = Some(now_ms().saturating_sub(s.observed_at_ms) as f64);
        let post_start = Instant::now();
        result.state = "unknown".into();
        result.reason = "submission_uncertain".into();
        if let Ok(Ok((status, body))) = tokio::time::timeout(
            Duration::from_millis(ctx.config.request_timeout_ms),
            ctx.exchange.post(signed),
        )
        .await
        {
            let exchange_status = body["status"].as_str().unwrap_or("").to_ascii_lowercase();
            if (200..300).contains(&status)
                && body["success"] == true
                && body["orderID"].as_str() == result.order_hash.as_deref()
                && ["matched", "delayed", "live", "unmatched"].contains(&exchange_status.as_str())
            {
                result.state = "accepted".into();
                result.reason.clear();
                result.exchange_status = Some(exchange_status);
            } else if [400, 401, 403, 404, 422, 429].contains(&status)
                || ((200..300).contains(&status)
                    && body["success"] == false
                    && body["orderID"].as_str().is_none_or(str::is_empty))
            {
                result.state = "rejected".into();
                result.reason = format!("exchange_rejected_http_{status}");
            }
        }
        result.post_ms = Some(elapsed_ms(post_start));
        if result.state == "unknown" {
            ctx.stopped.store(true, Ordering::SeqCst);
        }
    }
    result.total_ms = elapsed_ms(received);
    let ledger = ctx.journal.clone();
    let stored = result.clone();
    let finalize_start = Instant::now();
    if !matches!(
        tokio::task::spawn_blocking(move || ledger.blocking_lock().finish(stored)).await,
        Ok(Ok(()))
    ) {
        ctx.stopped.store(true, Ordering::SeqCst);
        result.state = "unknown".into();
        result.reason = "journal_result_failed".into();
    }
    result.finalize_ms = Some(elapsed_ms(finalize_start));
    result
}
