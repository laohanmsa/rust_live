//! Real-data input with an execution destination confined to the in-process mock exchange.
use crate::shadow_state::{
    Decision, Lifecycle, MarketContext, Page, Policy, Reservations, decide, event_ms,
};
use crate::{
    Reply, Signal, authorized, demo, elapsed_ms,
    exchange::Exchange,
    journal::{Journal, Stored},
    now_ms,
    telemetry::Telemetry,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use futures_util::StreamExt;
use polymarket_client_sdk_v2::types::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};

async fn input_permit(
    slots: &Arc<Semaphore>,
    signal_ms: u64,
    max_age_ms: u64,
) -> Result<tokio::sync::OwnedSemaphorePermit, &'static str> {
    let remaining = max_age_ms.saturating_sub(now_ms().saturating_sub(signal_ms));
    if remaining == 0 {
        return Err("signal_expired_waiting_for_slot");
    }
    tokio::time::timeout(
        Duration::from_millis(remaining),
        slots.clone().acquire_owned(),
    )
    .await
    .map_err(|_| "signal_expired_waiting_for_slot")?
    .map_err(|_| "executor_unavailable")
}

fn parse_ober_input(payload: &[u8], max_age_ms: u64) -> Result<Value, &'static str> {
    if payload.len() > 1_048_576 {
        return Err("invalid_ober_payload");
    }
    let book: Value = serde_json::from_slice(payload).map_err(|_| "invalid_ober_payload")?;
    let timestamp = event_ms(&book["timestamp"]);
    if timestamp.is_none_or(|ts| {
        ts > now_ms().saturating_add(10) || now_ms().saturating_sub(ts) > max_age_ms
    }) {
        return Err("stale_or_invalid_ober_time");
    }
    Ok(book)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub django_url: String,
    pub history_url: String,
    pub ober_url: String,
    pub nats_url: String,
    pub redis_url: String,
    #[serde(default)]
    pub uma_url: Option<String>,
    pub bind: String,
    pub journal: String,
    pub max_signal_age_ms: u64,
    pub max_inflight: usize,
    pub context_max_age_ms: u64,
    pub max_order_budget_pusd: Decimal,
    #[serde(default)]
    pub order_sizing: Option<crate::shadow_state::OrderSizing>,
    pub total_budget_pusd: Option<Decimal>,
}
impl Settings {
    fn account_ready(&self, response: &Value) -> bool {
        response["ready"] == true
            && crate::shadow_state::decimal(&response["trade_capacity_pusd"])
                .is_some_and(|value| value >= self.max_order_budget_pusd)
    }

    pub fn read(path: &Path) -> Result<Self> {
        let s: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!((1..=32).contains(&s.max_inflight), "invalid concurrency");
        ensure!(
            (1..=10000).contains(&s.max_signal_age_ms),
            "invalid signal lifetime"
        );
        ensure!(
            (1000..=300000).contains(&s.context_max_age_ms),
            "invalid context lifetime"
        );
        ensure!(
            s.max_order_budget_pusd > Decimal::ZERO
                && s.max_order_budget_pusd <= Decimal::from(100),
            "invalid per-order mock budget"
        );
        ensure!(
            s.total_budget_pusd.is_none_or(
                |budget| budget >= s.max_order_budget_pusd && budget <= Decimal::from(10000)
            ),
            "invalid mock session budget"
        );
        if let Some(sizing) = &s.order_sizing {
            ensure!(
                [
                    sizing.standard,
                    sizing.below_005,
                    sizing.below_080,
                    sizing.through_098,
                    sizing.at_099_low_depth
                ]
                .iter()
                .all(|value| *value > Decimal::ZERO && *value <= s.max_order_budget_pusd),
                "invalid sizing budget"
            );
        }
        Ok(s)
    }
}
#[derive(Default)]
struct Data {
    markets: HashMap<String, Arc<MarketContext>>,
    tokens: HashMap<String, String>,
    lifecycle: Lifecycle,
    policy: Option<Policy>,
    reservations: Reservations,
    last_sync_ms: u64,
    synced_epoch: Option<u64>,
    context_error: String,
    uma_counts: BTreeMap<String, u64>,
    last_uma_ms: u64,
    last_ober_ms: u64,
    decisions: VecDeque<Value>,
    restore: Vec<(String, u64)>,
    clocks: BTreeMap<String, u64>,
    history_error: String,
    live_ready_at_ms: u64,
    lifecycle_pruned: u64,
    native_uma_at_ms: u64,
    native_uma_health: Value,
    rejections: VecDeque<Value>,
}
impl Data {
    fn restore_orders(&mut self) {
        let seeds = std::mem::take(&mut self.restore);
        for (token, at) in seeds {
            if now_ms().saturating_sub(at) >= 10_800_000 {
                continue;
            }
            if let Some(market) = self.tokens.get(&token).cloned() {
                self.reservations
                    .slots
                    .entry(market.clone())
                    .or_default()
                    .push_back(at);
                self.reservations
                    .last_order
                    .entry(market)
                    .and_modify(|old| *old = (*old).max(at))
                    .or_insert(at);
            } else {
                self.restore.push((token, at));
            }
        }
    }
    fn prune_lifecycle(&mut self) {
        let active = self.markets.keys().cloned().collect();
        self.lifecycle_pruned += self.lifecycle.prune_inactive(&active, now_ms()) as u64;
    }
}
struct LiveAccount {
    name: String,
    id: u64,
    signer: String,
    funder: String,
    token: String,
}

struct App {
    live: Option<LiveAccount>,
    history_notify: Notify,
    settings: Settings,
    data: RwLock<Data>,
    telemetry: Telemetry,
    exchange: Exchange,
    journal: Arc<Mutex<Journal>>,
    slots: Arc<Semaphore>,
    notify: Notify,
    nats_up: AtomicBool,
    redis_up: AtomicBool,
    redis_epoch: AtomicU64,
    stopped: AtomicBool,
    stop_reason: std::sync::Mutex<Option<&'static str>>,
    boot_at_ms: u64,
    http: reqwest::Client,
}
impl App {
    fn halt(&self, reason: &'static str) {
        self.stopped.store(true, Ordering::SeqCst);
        let mut stored = self.stop_reason.lock().expect("stop reason mutex");
        if stored.is_none() {
            *stored = Some(reason);
            println!(
                "{}",
                json!({"event":"trading_stopped","reason":reason,"at_ms":now_ms()})
            );
        }
    }

    fn access_token(&self) -> &str {
        self.live
            .as_ref()
            .map_or(demo::ACCESS, |a| a.token.as_str())
    }
    async fn history_loop(self: Arc<Self>) {
        loop {
            let result = if let Some(account) = &self.live {
                let ready = async {
                    let response: Value = self
                        .http
                        .get(&self.settings.history_url)
                        .bearer_auth(&account.token)
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    anyhow::ensure!(
                        response["account_name"] == account.name
                            && response["account_id"] == account.id
                            && response["signer_address"]
                                .as_str()
                                .is_some_and(|s| s.eq_ignore_ascii_case(&account.signer))
                            && response["funder"]
                                .as_str()
                                .is_some_and(|s| s.eq_ignore_ascii_case(&account.funder)),
                        "live identity mismatch"
                    );
                    Ok::<bool, anyhow::Error>(self.settings.account_ready(&response))
                }
                .await;
                self.data.write().await.live_ready_at_ms = if matches!(ready, Ok(true)) {
                    now_ms()
                } else {
                    0
                };
                crate::shadow_history::export_live_once(
                    &self.http,
                    &self.settings.history_url,
                    &self.journal,
                    &account.name,
                    &account.token,
                )
                .await
            } else {
                crate::shadow_history::export_once(
                    &self.http,
                    &self.settings.history_url,
                    &self.journal,
                )
                .await
            };
            self.data.write().await.history_error = match result {
                Ok(_) => String::new(),
                Err(_) => "history_export_failed_retrying".into(),
            };
            tokio::select! { _=self.history_notify.notified()=>{}, _=tokio::time::sleep(Duration::from_secs(2))=>{} }
        }
    }
    fn ready(&self, data: &Data) -> bool {
        (self.settings.uma_url.is_none()
            || (now_ms().saturating_sub(data.native_uma_at_ms) < 5_000
                && data.native_uma_health["ready"] == true))
            && (self.live.is_none()
                || (now_ms().saturating_sub(data.live_ready_at_ms) < 10_000
                    && data.history_error.is_empty()))
            && !self.stopped.load(Ordering::SeqCst)
            && self.nats_up.load(Ordering::SeqCst)
            && self.redis_up.load(Ordering::SeqCst)
            && data.synced_epoch == Some(self.redis_epoch.load(Ordering::SeqCst))
            && now_ms().saturating_sub(data.last_sync_ms) <= self.settings.context_max_age_ms
    }
    async fn fetch(&self, ids: Option<&[String]>) -> Result<(Vec<MarketContext>, Option<Policy>)> {
        let mut cursor = String::new();
        let mut rows = Vec::new();
        let mut policy = None;
        for _ in 0..100 {
            let mut request = self
                .http
                .get(&self.settings.django_url)
                .query(&[("limit", "200"), ("after_id", cursor.as_str())]);
            if let Some(ids) = ids {
                request = request.query(&[("market_ids", ids.join(","))]);
            }
            let page: Page = request.send().await?.error_for_status()?.json().await?;
            ensure!(page.schema_version == 1, "unsupported context schema");
            ensure!(
                now_ms().saturating_sub(page.captured_at_ms) < self.settings.context_max_age_ms,
                "stale Django snapshot"
            );
            if page.config.is_some() {
                policy = page.config.map(|mut p| {
                    p.order_sizing = self.settings.order_sizing.clone();
                    p
                });
            }
            rows.extend(page.results);
            if !page.has_more {
                return Ok((rows, policy));
            }
            let next = page.next_after_id.context("missing pagination cursor")?;
            ensure!(next > cursor, "non-advancing context cursor");
            cursor = next;
        }
        anyhow::bail!("context population exceeds 20000 rows")
    }
    async fn sync_loop(self: Arc<Self>) {
        let mut last_full = Instant::now() - Duration::from_secs(60);
        loop {
            if !self.redis_up.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let epoch = self.redis_epoch.load(Ordering::SeqCst);
            let (full, ids) = {
                let data = self.data.read().await;
                (
                    last_full.elapsed() >= Duration::from_secs(30)
                        || data.synced_epoch != Some(epoch),
                    data.lifecycle
                        .needs_refresh
                        .iter()
                        .take(200)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };
            if full || !ids.is_empty() {
                let result = self.fetch(if full { None } else { Some(&ids) }).await;
                match result {
                    Ok((rows, policy)) => {
                        let mut data = self.data.write().await;
                        if full {
                            data.markets.clear();
                            data.tokens.clear();
                        }
                        for mut context in rows {
                            data.lifecycle.reconcile_settles(
                                &context.market_id,
                                &context.settled_request_blocks,
                            );
                            if self.settings.uma_url.is_some() {
                                crate::native_uma::overlay(&mut context, &data.lifecycle, now_ms());
                            } else if let Some(r) = &context.resolution {
                                data.lifecycle.seed(&context.market_id, r);
                            }
                            for token in [&context.token_id_yes, &context.token_id_no]
                                .into_iter()
                                .flatten()
                            {
                                data.tokens.insert(token.clone(), context.market_id.clone());
                            }
                            let waiting =
                                data.lifecycle.needs_refresh.contains(&context.market_id)
                                    && data.lifecycle.marks.get(&context.market_id).is_some_and(
                                        |m| {
                                            m.status == "proposed"
                                                && now_ms().saturating_sub(m.received_ms) < 30_000
                                        },
                                    )
                                    && context.resolution.as_ref().is_none_or(|r| {
                                        !data.lifecycle.is_proposed(
                                            &context.market_id,
                                            r.request_id.as_deref().unwrap_or(""),
                                            r.block_number.unwrap_or(0),
                                        )
                                    });
                            if !waiting {
                                data.lifecycle.needs_refresh.remove(&context.market_id);
                            }
                            data.markets
                                .insert(context.market_id.clone(), Arc::new(context));
                        }
                        data.restore_orders();
                        if policy.is_some() {
                            data.policy = policy;
                        }
                        data.context_error.clear();
                        if full
                            && epoch == self.redis_epoch.load(Ordering::SeqCst)
                            && self.redis_up.load(Ordering::SeqCst)
                        {
                            data.prune_lifecycle();
                            data.synced_epoch = Some(epoch);
                            data.last_sync_ms = now_ms();
                            last_full = Instant::now();
                        }
                    }
                    Err(_) => {
                        self.data.write().await.context_error = "context_fetch_failed".into();
                    }
                }
            }
            tokio::select! { _=self.notify.notified()=>{tokio::time::sleep(Duration::from_millis(100)).await;}, _=tokio::time::sleep(Duration::from_secs(2))=>{} }
        }
    }
    async fn uma_loop(self: Arc<Self>) {
        if self.settings.uma_url.is_none() {
            return self.redis_loop().await;
        }
        loop {
            if self.native_session().await.is_err() {
                self.redis_up.store(false, Ordering::SeqCst);
                self.redis_epoch.fetch_add(1, Ordering::SeqCst);
                self.data.write().await.native_uma_health =
                    json!({"ready":false,"fault":"native_feed_resync"});
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    async fn native_session(&self) -> Result<()> {
        let nats = async_nats::connect(&self.settings.nats_url).await?;
        let mut sub = nats.subscribe("rust.uma.events").await?;
        nats.flush().await?;
        let mut refresh = tokio::time::interval(Duration::from_secs(30));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut epoch = String::new();
        let mut sequence = 0;
        loop {
            tokio::select! {
                biased;
                _ = refresh.tick() => {
                    if epoch.is_empty() {self.redis_up.store(false, Ordering::SeqCst);}
                    let url = format!("{}/snapshot", self.settings.uma_url.as_deref().context("UMA URL")?.trim_end_matches('/'));
                    let response = self.http.get(url).send().await?.error_for_status()?.bytes().await?;
                    ensure!(response.len() <= 64 * 1024 * 1024, "UMA snapshot too large");
                    let value: Value = serde_json::from_slice(&response)?;
                    let (new_epoch, new_sequence, mut lifecycle) = crate::native_uma::snapshot(&value)?;
                    lifecycle.needs_refresh.retain(|market|lifecycle.marks.get(market).is_some_and(|m|m.status=="proposed"));
                    let mut data = self.data.write().await;
                    data.lifecycle = lifecycle;
                    let Data {markets,lifecycle,..} = &mut *data;
                    for context in markets.values_mut() {crate::native_uma::overlay(Arc::make_mut(context),lifecycle,now_ms());}
                    data.native_uma_health = value["health"].clone();
                    data.native_uma_at_ms = now_ms();
                    if epoch != new_epoch {self.redis_epoch.fetch_add(1, Ordering::SeqCst);}
                    epoch = new_epoch; sequence = new_sequence;
                    self.redis_up.store(true, Ordering::SeqCst);
                    drop(data);self.notify.notify_one();
                }
                message = tokio::time::timeout(Duration::from_secs(5), sub.next()) => {
                    let message = message?.context("UMA feed ended")?;
                    ensure!(message.payload.len() <= 128 * 1024, "oversized UMA message");
                    let value: Value = serde_json::from_slice(&message.payload)?;
                    // The subscription was installed before the snapshot. Discard its covered messages.
                    if value["epoch"].as_str() == Some(&epoch) && value["sequence"].as_u64().is_some_and(|s| s < sequence) {continue;}
                    let apply = crate::native_uma::next_sequence(&epoch,sequence,&value)?;
                    let mut data = self.data.write().await;
                    if apply {
                        let e: crate::uma::Event = serde_json::from_value(value["event"].clone())?;
                        e.validate()?;
                        data.lifecycle.apply(&e.channel,&value["event"]);
                        if e.channel != "uma:resolution" {data.lifecycle.needs_refresh.remove(&e.market_id);}
                        let Data {markets,lifecycle,..} = &mut *data;
                        if let Some(context)=markets.get_mut(&e.market_id) {crate::native_uma::overlay(Arc::make_mut(context),lifecycle,now_ms());}
                        *data.uma_counts.entry(e.channel).or_default() += 1;
                        data.last_uma_ms = now_ms();
                        sequence = value["sequence"].as_u64().context("sequence")?;
                        if value["ready"] == false {data.native_uma_health["ready"] = json!(false);}
                    } else if value["kind"] == "heartbeat" {
                        let captured=value["health"]["captured_at_ms"].as_u64().context("heartbeat timestamp")?;
                        if now_ms().abs_diff(captured)>5_000 {continue;}
                        data.native_uma_health = value["health"].clone();
                        data.native_uma_at_ms = now_ms();
                    }
                    drop(data);self.notify.notify_one();
                }
            }
        }
    }
    async fn redis_loop(self: Arc<Self>) {
        loop {
            if let Err(_error) = self.redis_session().await {
                self.redis_up.store(false, Ordering::SeqCst);
                self.redis_epoch.fetch_add(1, Ordering::SeqCst);
                self.data.write().await.context_error =
                    "lifecycle_disconnected_resync_required".into();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    async fn redis_session(&self) -> Result<()> {
        let client = redis::Client::open(self.settings.redis_url.as_str())?;
        let mut subscriber = client.get_async_pubsub().await?;
        subscriber
            .subscribe(vec!["uma:resolution", "uma:dispute_price", "uma:settle"])
            .await?;
        self.redis_epoch.fetch_add(1, Ordering::SeqCst);
        self.redis_up.store(true, Ordering::SeqCst);
        self.notify.notify_one();
        let mut stream = subscriber.on_message();
        while let Some(message) = stream.next().await {
            let channel = message.get_channel_name();
            let payload: Vec<u8> = message.get_payload()?;
            if payload.len() > 1048576 {
                continue;
            }
            let Ok(value) = serde_json::from_slice::<Value>(&payload) else {
                continue;
            };
            let mut data = self.data.write().await;
            *data.uma_counts.entry(channel.into()).or_default() += 1;
            data.last_uma_ms = now_ms();
            if data.lifecycle.marks.len() > 20000 {
                data.prune_lifecycle();
            }
            if data.lifecycle.marks.len() > 20000 {
                self.halt("lifecycle_capacity");
                data.context_error = "lifecycle_capacity".into();
                continue;
            }
            data.lifecycle.apply(channel, &value);
            drop(data);
            self.notify.notify_one();
        }
        anyhow::bail!("lifecycle stream ended")
    }
    async fn nats_loop(self: Arc<Self>) {
        loop {
            let app = self.clone();
            // Reuse NATS's bounded subscription buffer while waiting for an executor slot.
            let options = async_nats::ConnectOptions::new()
                .subscription_capacity(1024)
                .event_callback(move |event| {
                    let app = app.clone();
                    async move {
                        match event {
                            async_nats::Event::Connected => {
                                app.nats_up.store(true, Ordering::SeqCst);
                            }
                            async_nats::Event::Disconnected => {
                                app.nats_up.store(false, Ordering::SeqCst);
                            }
                            _ => {}
                        }
                    }
                });
            if let Ok(client) = options.connect(&self.settings.nats_url).await
                && let Ok(mut sub) = client.subscribe("ober.*.best").await
            {
                self.nats_up.store(true, Ordering::SeqCst);
                while let Some(message) = sub.next().await {
                    self.telemetry.received();
                    let received = Instant::now();
                    // Empty/expired books never occupy execution slots or wait behind a submit.
                    let book =
                        match parse_ober_input(&message.payload, self.settings.max_signal_age_ms) {
                            Ok(book) => book,
                            Err(reason) => {
                                self.telemetry.finished(
                                    &Reply::blocked("", reason),
                                    elapsed_ms(received),
                                    false,
                                );
                                continue;
                            }
                        };
                    {
                        let mut data = self.data.write().await;
                        data.last_ober_ms = now_ms();
                        let clock = if book["timestamp"]
                            .as_u64()
                            .is_some_and(|t| t >= 100_000_000_000_000)
                        {
                            "ober_receive_us"
                        } else {
                            "exchange_snapshot_ms"
                        };
                        *data.clocks.entry(clock.into()).or_default() += 1;
                    }
                    if book["best_ask"].is_null() {
                        self.telemetry.finished(
                            &Reply::blocked("", "missing_ask"),
                            elapsed_ms(received),
                            false,
                        );
                        continue;
                    }
                    match input_permit(
                        &self.slots,
                        event_ms(&book["timestamp"]).unwrap_or(0),
                        self.settings.max_signal_age_ms,
                    )
                    .await
                    {
                        Ok(permit) => {
                            let app = self.clone();
                            tokio::spawn(async move {
                                app.receive(&message.payload, permit, received).await;
                            });
                        }
                        Err(reason) => {
                            self.remember_rejection(&book, reason).await;
                            self.telemetry.finished(
                                &Reply::blocked("", reason),
                                elapsed_ms(received),
                                false,
                            );
                        }
                    }
                }
            }
            self.nats_up.store(false, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    async fn receive(
        self: &Arc<Self>,
        payload: &[u8],
        permit: tokio::sync::OwnedSemaphorePermit,
        received: Instant,
    ) {
        let id = format!(
            "{}-{:x}",
            if self.live.is_some() {
                "live"
            } else {
                "shadow"
            },
            Sha256::digest(payload)
        );
        let mut reply = Reply::blocked(&id, "");
        // Revalidate after slot waiting; the source clock remains the original message clock.
        let mut book = match parse_ober_input(payload, self.settings.max_signal_age_ms) {
            Ok(book) => book,
            Err(reason) => {
                reply.reason = reason.into();
                self.telemetry.finished(&reply, elapsed_ms(received), false);
                return;
            }
        };
        let timestamp = event_ms(&book["timestamp"]);
        let local_clock = book["timestamp"]
            .as_u64()
            .is_some_and(|t| t >= 100_000_000_000_000);
        if book["best_ask"].is_null() {
            reply.reason = "missing_ask".into();
            self.telemetry.finished(&reply, elapsed_ms(received), false);
            return;
        }
        let remaining = self
            .settings
            .max_signal_age_ms
            .saturating_sub(now_ms().saturating_sub(timestamp.unwrap_or(0)));
        let preparation = tokio::time::timeout(
            Duration::from_millis(remaining),
            self.prepare_inputs(&mut book),
        )
        .await;
        if !matches!(preparation, Ok(Ok(()))) {
            let reason = match preparation {
                Ok(Err(reason)) => reason,
                _ => "input_preparation_expired",
            };
            reply.reason = reason.into();
            self.remember_rejection(&book, reason).await;
            self.telemetry.finished(&reply, elapsed_ms(received), false);
            return;
        }
        let decision = {
            let mut data = self.data.write().await;
            let market = book["market_id"].as_str().map(str::to_owned).or_else(|| {
                data.tokens
                    .get(book["token_id"].as_str().unwrap_or(""))
                    .cloned()
            });
            if !self.ready(&data) {
                Err("context_not_ready")
            } else if let Some(context) = market.and_then(|id| data.markets.get(&id).cloned()) {
                if let Some(policy) = data.policy.clone() {
                    let Data {
                        lifecycle,
                        reservations,
                        ..
                    } = &mut *data;
                    decide(
                        context,
                        &policy,
                        lifecycle,
                        &book,
                        reservations,
                        now_ms(),
                        self.settings.max_order_budget_pusd,
                    )
                } else {
                    Err("missing_strategy_config")
                }
            } else {
                Err("market_not_eligible")
            }
        };
        reply.policy_ms = Some(elapsed_ms(received));
        let policy_ms = elapsed_ms(received);
        match decision {
            Ok(decision) => {
                let app = self.clone();
                self.telemetry.started();
                tokio::spawn(async move {
                    let result = app
                        .execute(
                            decision,
                            id,
                            timestamp.unwrap_or(0),
                            received,
                            policy_ms,
                            local_clock,
                        )
                        .await;
                    app.telemetry.finished(&result, elapsed_ms(received), true);
                    app.remember(&result).await;
                    drop(permit);
                });
            }
            Err(reason) => {
                self.remember_rejection(&book, reason).await;
                reply.reason = reason.into();
                self.telemetry.finished(&reply, elapsed_ms(received), false);
            }
        }
    }
    async fn prepare_inputs(&self, book: &mut Value) -> Result<(), &'static str> {
        let market = book["market_id"]
            .as_str()
            .ok_or("missing_market_id")?
            .to_owned();
        let needs_fetch = {
            let data = self.data.read().await;
            if !self.ready(&data) {
                return Err("context_not_ready");
            }
            if data
                .lifecycle
                .marks
                .get(&market)
                .is_some_and(|m| m.status != "proposed")
                || data.lifecycle.has_dispute(&market)
            {
                return Err("not_proposed_or_lifecycle_changed");
            }
            data.markets.get(&market).is_none_or(|c| {
                c.resolution.as_ref().is_none_or(|r| {
                    !data.lifecycle.matches_proposal(
                        &market,
                        r.request_id.as_deref().unwrap_or(""),
                        r.block_number.unwrap_or(0),
                        r.proposed_price,
                    )
                })
            })
        };
        if needs_fetch {
            let (rows, policy) = self
                .fetch(Some(std::slice::from_ref(&market)))
                .await
                .map_err(|_| "context_fetch_failed")?;
            let mut data = self.data.write().await;
            for mut context in rows {
                data.lifecycle
                    .reconcile_settles(&context.market_id, &context.settled_request_blocks);
                if self.settings.uma_url.is_some() {
                    crate::native_uma::overlay(&mut context, &data.lifecycle, now_ms());
                } else if let Some(r) = &context.resolution {
                    data.lifecycle.seed(&context.market_id, r);
                }
                for token in [&context.token_id_yes, &context.token_id_no]
                    .into_iter()
                    .flatten()
                {
                    data.tokens.insert(token.clone(), context.market_id.clone());
                }
                data.markets
                    .insert(context.market_id.clone(), Arc::new(context));
            }
            data.restore_orders();
            if policy.is_some() {
                data.policy = policy;
            }
        }
        let loser = {
            let data = self.data.read().await;
            let c = data.markets.get(&market).ok_or("market_not_eligible")?;
            let r = c.resolution.as_ref().ok_or("missing_resolution")?;
            if !c.eligible
                || !data.lifecycle.matches_proposal(
                    &market,
                    r.request_id.as_deref().unwrap_or(""),
                    r.block_number.unwrap_or(0),
                    r.proposed_price,
                )
                || data.lifecycle.has_dispute(&market)
            {
                return Err("not_proposed_or_lifecycle_changed");
            }
            let (winner, loser) = if r.proposed_price == Decimal::ONE {
                (&c.token_id_yes, &c.token_id_no)
            } else if r.proposed_price == Decimal::ZERO {
                (&c.token_id_no, &c.token_id_yes)
            } else {
                return Err("non_binary_proposal");
            };
            if winner.as_deref() != book["token_id"].as_str() {
                return Err("not_winner_token");
            }
            loser.clone().ok_or("missing_loser_token")?
        };
        // Existing OBer in-memory best endpoint rejects quarantined/unsynchronized books.
        let quote: Value = self
            .http
            .get(format!(
                "{}/book/{}/best",
                self.settings.ober_url.trim_end_matches('/'),
                loser
            ))
            .send()
            .await
            .map_err(|_| "loser_book_unavailable")?
            .error_for_status()
            .map_err(|_| "loser_book_unavailable")?
            .json()
            .await
            .map_err(|_| "invalid_loser_book")?;
        if quote["token_id"].as_str() != Some(loser.as_str()) {
            return Err("loser_token_mismatch");
        }
        book["loser_bid"] = if quote["best_bid"].is_null() {
            json!(0)
        } else {
            quote["best_bid"].clone()
        };
        Ok(())
    }
    async fn remember_rejection(&self, book: &Value, reason: &str) {
        let mut data = self.data.write().await;
        let market = book["market_id"].as_str().unwrap_or("");
        let sample = json!({"event":"signal_rejected","at_ms":now_ms(),"signal_at_ms":event_ms(&book["timestamp"]),"token_id":book["token_id"],"market_id":market,"reason":reason,"ask":book["best_ask"],
            "context_status":data.markets.get(market).and_then(|c|c.resolution.as_ref()).map(|r|&r.status),
            "lifecycle_status":data.lifecycle.marks.get(market).map(|m|&m.status)});
        // Keep decision skips in the existing bounded container logs for later order comparisons.
        if reason != "not_winner_token" {
            eprintln!("{sample}");
        }
        if data.rejections.len() == 256 {
            data.rejections.pop_front();
        }
        data.rejections.push_back(sample);
    }
    async fn remember(&self, reply: &Reply) {
        let mut data = self.data.write().await;
        if data.decisions.len() == 50 {
            data.decisions.pop_front();
        }
        data.decisions.push_back(json!(reply));
    }
    async fn execute(
        &self,
        decision: Decision,
        id: String,
        source_ms: u64,
        received: Instant,
        policy_ms: f64,
        local_clock: bool,
    ) -> Reply {
        let mut r = Reply::blocked(&id, "");
        r.policy_ms = Some(policy_ms);
        r.queue_ms = (elapsed_ms(received) - policy_ms).max(0.0);
        let token = if decision
            .context
            .resolution
            .as_ref()
            .is_some_and(|v| v.proposed_price == Decimal::ONE)
        {
            decision.context.token_id_yes.as_deref()
        } else {
            decision.context.token_id_no.as_deref()
        };
        let Some(token_id) = token.and_then(|t| t.parse().ok()) else {
            r.reason = "invalid_token_id".into();
            return r;
        };
        let signal = Signal {
            id,
            token_id,
            ask: decision.price,
            fair_value: decision.fair_value,
            observed_at_ms: source_ms,
            book_valid: true,
        };
        let started = Instant::now();
        let signed = match self
            .exchange
            .sign_shadow(
                &signal,
                decision.shares,
                decision.tick,
                decision.context.neg_risk,
            )
            .await
        {
            Ok(v) => v,
            Err(_) => {
                r.reason = "shadow_sign_failed".into();
                return r;
            }
        };
        r.sign_ms = elapsed_ms(started);
        r.order_hash = Some(signed.hash.clone());
        r.submitted_amount = Some(signed.cash_amount);
        r.state = "prepared".into();
        let entry = Stored {
            signal: signal.clone(),
            reserved: decision.budget,
            order: signed.journal_order.clone(),
            reply: r.clone(),
        };
        let ledger = self.journal.clone();
        let budget = self.settings.total_budget_pusd;
        let started = Instant::now();
        match tokio::task::spawn_blocking(move || ledger.blocking_lock().prepare(entry, budget))
            .await
        {
            Ok(Ok(None)) => {}
            Ok(Ok(Some(mut old))) => {
                old.replayed = true;
                return old;
            }
            _ => {
                r.state = "blocked".into();
                r.reason = "shadow_journal_or_budget_limit".into();
                self.halt("shadow_journal_or_budget_limit");
                return r;
            }
        }
        r.journal_ms = elapsed_ms(started);
        let valid = {
            let data = self.data.read().await;
            self.ready(&data)
                && !data.lifecycle.has_dispute(&decision.context.market_id)
                && decision.context.resolution.as_ref().is_some_and(|r| {
                    data.lifecycle.matches_proposal(
                        &decision.context.market_id,
                        &decision.request_id,
                        decision.block,
                        r.proposed_price,
                    )
                })
                && data
                    .policy
                    .as_ref()
                    .is_some_and(|p| p.strategy_enabled && !p.manual_trade_shutdown_enabled)
        };
        if !valid || now_ms().saturating_sub(source_ms) > self.settings.max_signal_age_ms {
            r.state = "blocked".into();
            r.reason = "state_changed_or_expired_before_submit".into();
        } else {
            r.submitted_at_ms = Some(now_ms());
            r.dispatch_ms = Some(elapsed_ms(received));
            if local_clock {
                r.source_to_dispatch_ms = Some(now_ms().saturating_sub(source_ms) as f64);
            }
            let started = Instant::now();
            match tokio::time::timeout(Duration::from_secs(5), self.exchange.post(signed)).await {
                Ok(Ok((status, body))) if self.live.is_some() => {
                    crate::apply_exchange_response(&mut r, status, &body);
                    if r.state == "unknown" {
                        self.halt("submission_uncertain");
                    } else if [401, 403].contains(&status) {
                        self.halt("exchange_auth_rejected");
                    }
                }
                Ok(Ok((200, body)))
                    if body["success"] == true
                        && body["orderID"].as_str() == r.order_hash.as_deref() =>
                {
                    r.state = "accepted".into();
                    r.exchange_status = Some("mock_matched".into());
                }
                _ => {
                    r.state = "unknown".into();
                    r.reason = "submission_uncertain".into();
                    self.halt("submission_uncertain");
                }
            }
            r.post_ms = Some(elapsed_ms(started));
        }
        r.total_ms = elapsed_ms(received);
        let ledger = self.journal.clone();
        let record = r.clone();
        let started = Instant::now();
        if !matches!(
            tokio::task::spawn_blocking(move || ledger.blocking_lock().finish(record)).await,
            Ok(Ok(()))
        ) {
            r.state = "unknown".into();
            r.reason = "shadow_result_record_failed".into();
            self.halt("shadow_result_record_failed");
        }
        r.finalize_ms = Some(elapsed_ms(started));
        self.history_notify.notify_one();
        r
    }
}
#[derive(Deserialize)]
struct Window {
    window_seconds: Option<u64>,
}
async fn health(State(app): State<Arc<App>>) -> Json<Value> {
    let data = app.data.read().await;
    Json(
        json!({"mode":app.exchange.mode(),"account":app.live.as_ref().map(|a|&a.name),"ready":app.ready(&data),"stopped":app.stopped.load(Ordering::SeqCst),"stop_reason":*app.stop_reason.lock().expect("stop reason mutex"),"data_sources":"Django + UMA + OBer","execution":if app.live.is_some(){"clob_direct"}else{"loopback_mock_only"}}),
    )
}
async fn metrics(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(w): Query<Window>,
) -> (StatusCode, Json<Value>) {
    if !authorized(&headers, app.access_token()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        );
    }
    let window = w.window_seconds.unwrap_or(3600).clamp(1, 86400);
    let mut result = app.telemetry.snapshot(window);
    let data = app.data.read().await;
    result["queued"] = json!(0);
    result["boot_at_ms"] = json!(app.boot_at_ms);
    result["mode"] = json!(app.exchange.mode());
    result["order_sizing"] = json!(app.settings.order_sizing);
    result["max_order_budget_pusd"] = json!(app.settings.max_order_budget_pusd);
    result["sources"] = json!({"nats_connected":app.nats_up.load(Ordering::SeqCst),"uma_connected":app.redis_up.load(Ordering::SeqCst),"uma_epoch":app.redis_epoch.load(Ordering::SeqCst),"synced_epoch":data.synced_epoch,"context_markets":data.markets.len(),"eligible_markets":data.markets.values().filter(|c|c.eligible&&!data.lifecycle.has_dispute(&c.market_id)&&c.resolution.as_ref().is_some_and(|r|data.lifecycle.is_proposed(&c.market_id,r.request_id.as_deref().unwrap_or(""),r.block_number.unwrap_or(0)))).count(),"valuation_mode":"local_m5","valuation_inputs":data.markets.values().filter(|c|c.market_volume.is_some()&&c.event_volume.is_some()).count(),"pending_context":data.lifecycle.needs_refresh.len(),"context_age_ms":now_ms().saturating_sub(data.last_sync_ms),"context_error":data.context_error,"uma_counts":data.uma_counts,"last_uma_ms":data.last_uma_ms,"last_ober_ms":data.last_ober_ms,"timestamp_kinds":data.clocks,"recent_mock_orders":data.decisions});
    result["sources"]["recent_rejections"] = json!(data.rejections);
    result["sources"]["lifecycle_marks"] = json!(data.lifecycle.marks.len());
    result["sources"]["lifecycle_pruned"] = json!(data.lifecycle_pruned);
    result["sources"]["lifecycle_terminal_proofs"] = json!(data.lifecycle.terminal_proof_count());
    result["sources"]["uma_source"] = json!(if app.settings.uma_url.is_some() {
        "rust_direct"
    } else {
        "legacy_redis"
    });
    result["sources"]["native_uma"] = data.native_uma_health.clone();
    result["sources"]["native_uma_age_ms"] = json!(now_ms().saturating_sub(data.native_uma_at_ms));
    result["sources"]["account_ready_age_ms"] =
        json!(now_ms().saturating_sub(data.live_ready_at_ms));
    let history_error = data.history_error.clone();
    drop(data);
    let ledger = app.journal.clone();
    let limit = app.settings.total_budget_pusd;
    let history = tokio::task::spawn_blocking(move || {
        let ledger = ledger.blocking_lock();
        Ok::<_, anyhow::Error>(json!({"exported":ledger.exported_count,"pending":ledger.pending_count()?,
            "budget_used":ledger.used.to_string(),"budget_limit":limit.map(|v|v.to_string()),"error":history_error,
            "journal_entries":ledger.count,"journal_capacity":crate::journal::ORDER_CAPACITY,"unresolved":ledger.unresolved_count()?}))
    }).await;
    match history {
        Ok(Ok(history)) => result["history"] = history,
        _ => {
            app.halt("journal_read_failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"journal_read_failed"})),
            );
        }
    }
    (StatusCode::OK, Json(result))
}
async fn markets(State(app): State<Arc<App>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !authorized(&headers, app.access_token()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        );
    }
    let data = app.data.read().await;
    let rows:Vec<_>=data.markets.values().take(200).map(|c|json!({"market_id":c.market_id,"question":c.question,"eligible":c.eligible && !data.lifecycle.has_dispute(&c.market_id) && c.resolution.as_ref().is_some_and(|r|data.lifecycle.is_proposed(&c.market_id,r.request_id.as_deref().unwrap_or(""),r.block_number.unwrap_or(0))),"resolution_status":data.lifecycle.marks.get(&c.market_id).map(|m|&m.status),"fee_verification_status":c.fee_verification_status,"valuation_available":c.valuation.is_some()})).collect();
    (
        StatusCode::OK,
        Json(json!({"total":data.markets.len(),"shown":rows.len(),"results":rows})),
    )
}
async fn stop(State(app): State<Arc<App>>, headers: HeaderMap) -> StatusCode {
    if !authorized(&headers, app.access_token()) {
        return StatusCode::UNAUTHORIZED;
    }
    app.halt("manual_stop");
    StatusCode::OK
}

pub async fn serve(settings: Settings) -> Result<()> {
    serve_inner(settings, None).await
}

pub async fn serve_live(settings: Settings, path: &Path, account: &str) -> Result<()> {
    ensure!(
        settings.max_order_budget_pusd <= Decimal::from(30),
        "live per-order limit is 30"
    );
    ensure!(
        settings.history_url.ends_with("/api/rust-live-orders/"),
        "live history endpoint required"
    );
    let credentials = crate::exchange::LiveCredentials::read(path, account)?;
    serve_inner(settings, Some(credentials)).await
}

async fn serve_inner(
    settings: Settings,
    credentials: Option<crate::exchange::LiveCredentials>,
) -> Result<()> {
    let mock = if credentials.is_none() {
        Some(demo::MockExchange::start().await?)
    } else {
        None
    };
    if let Some(mock) = &mock {
        mock.state.delay_ms.store(50, Ordering::SeqCst);
    }
    let exchange = if let Some(c) = &credentials {
        Exchange::authorized_live(c).await?
    } else {
        Exchange::shadow(&mock.as_ref().context("missing mock")?.url).await?
    };
    let live = credentials.map(|c| LiveAccount {
        name: c.account_name,
        id: c.account_id,
        signer: c.signer_address.to_string(),
        funder: c.funder.to_string(),
        token: c.access_token,
    });
    let journal = Journal::open(Path::new(&settings.journal), &exchange.scope())?;
    let stopped = journal.unresolved_count()? > 0;
    let restore = journal.recent_orders(now_ms().saturating_sub(10_800_000))?;
    let bind = settings.bind.clone();
    let max = settings.max_inflight;
    let app = Arc::new(App {
        live,
        history_notify: Notify::new(),
        settings,
        data: RwLock::new(Data {
            restore,
            ..Data::default()
        }),
        telemetry: Telemetry::default(),
        exchange,
        journal: Arc::new(Mutex::new(journal)),
        slots: Arc::new(Semaphore::new(max)),
        notify: Notify::new(),
        nats_up: AtomicBool::new(false),
        redis_up: AtomicBool::new(false),
        redis_epoch: AtomicU64::new(0),
        stopped: AtomicBool::new(stopped),
        stop_reason: std::sync::Mutex::new(stopped.then_some("unconfirmed_journal")),
        boot_at_ms: now_ms(),
        http: reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    });
    let tasks = [
        tokio::spawn(app.clone().uma_loop()),
        tokio::spawn(app.clone().nats_loop()),
        tokio::spawn(app.clone().sync_loop()),
        tokio::spawn(app.clone().history_loop()),
    ];
    let router = Router::new()
        .route("/health", get(health))
        .route(
            "/ready",
            get(|State(app): State<Arc<App>>| async move {
                let data = app.data.read().await;
                if app.ready(&data) {
                    StatusCode::NO_CONTENT
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route("/metrics", get(metrics))
        .route("/markets", get(markets))
        .route("/stop", post(stop))
        .with_state(app.clone());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!(
        "{}",
        json!({"mode":app.exchange.mode(),"account":app.live.as_ref().map(|a|&a.name),"execution":if app.live.is_some(){"clob_direct"}else{"loopback_mock_only"},"subscribed":"ober.*.best + UMA lifecycle"})
    );
    let shutdown = app.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            if let Ok(mut signal) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! {_=tokio::signal::ctrl_c()=>{},_=signal.recv()=>{}}
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
            shutdown.halt("shutdown");
        })
        .await?;
    for task in tasks {
        task.abort();
    }
    let _permits = app.slots.acquire_many(max as u32).await?;
    drop(mock);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn busy_executor_waits_for_a_fresh_signal_but_never_replays_an_expired_one() {
        let slots = Arc::new(Semaphore::new(1));
        let held = slots.clone().acquire_owned().await.unwrap();
        let start = now_ms();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(held);
        });
        let permit = input_permit(&slots, start, 200).await;
        release.await.unwrap();
        assert!(
            permit.is_ok(),
            "temporary concurrency must not discard a fresh signal"
        );
        drop(permit);
        assert!(input_permit(&slots, now_ms() - 201, 200).await.is_err());
        let _held = slots.clone().acquire_owned().await.unwrap();
        assert!(input_permit(&slots, now_ms(), 10).await.is_err());
    }

    #[test]
    fn live_config_requires_full_cap_and_valid_sizing() -> Result<()> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/live.json");
        let settings = Settings::read(&path)?;
        assert_eq!(settings.max_order_budget_pusd, Decimal::from(30));
        assert!(settings.account_ready(&json!({"ready":true,"trade_capacity_pusd":"30"})));
        for response in [
            json!({"ready":true,"trade_capacity_pusd":"29.99"}),
            json!({"ready":false,"trade_capacity_pusd":"1000"}),
            json!({"ready":true}),
            json!({"ready":true,"trade_capacity_pusd":"NaN"}),
        ] {
            assert!(!settings.account_ready(&response));
        }
        Ok(())
    }

    #[tokio::test]
    async fn first_signal_with_no_cached_valuation_submits_independently_of_live_quota()
    -> Result<()> {
        for native in [false, true] {
            let now = now_ms();
            let page = json!({"schema_version":1,"captured_at_ms":now,"has_more":false,"next_after_id":null,
            "config":{"valuation_key":"m5_expected_payout","manual_trade_shutdown_enabled":false,"strategy_enabled":true,
                "max_ask_price":"0.999","max_orders_per_market":if native {2}else{1},"ev_threshold":"0.0002","order_size_usd":"5",
                "low_price_order_size_usd":"10","low_depth_099_order_size_usd":"10"},
            "results":[{"market_id":"test-market","question":"Synthetic market","market_volume":"0","event_volume":"0",
                "tags":["Sports"],"eligible":true,"token_id_yes":"42","token_id_no":"43","active":true,"closed":false,
                "accepting_orders":true,"auto_archived":false,"min_tick_size":"0.01","min_order_size":"5","neg_risk":false,
                "fees_enabled":false,"fee_schedule":null,"fee_verification_status":"unverified","has_disputed_resolution":false,
                "existing_order_count":1,"valuation":null,
                "resolution":{"id":1,"request_id":"r","status":"proposed","proposed_price":"1","propose_time_ms":now,
                    "block_number":10,"dispute_block_number":null,"settle_block_number":null,"disputed":false,"settled":false}}]});
            let router = Router::new()
                .route(
                    "/context",
                    get(move || {
                        let page = page.clone();
                        async move { Json(page) }
                    }),
                )
                .route(
                    "/book/43/best",
                    get(|| async { Json(json!({"token_id":"43","best_bid":"0.10"})) }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let url = format!("http://{}", listener.local_addr()?);
            let server = tokio::spawn(async move { axum::serve(listener, router).await });
            let mock = demo::MockExchange::start().await?;
            let exchange = Exchange::shadow(&mock.url).await?;
            let path = std::env::temp_dir()
                .join(format!("shadow-parity-{}-{now}.jsonl", std::process::id()));
            let journal = Journal::open(&path, &exchange.scope())?;
            let app = Arc::new(App {
                live: None,
                history_notify: Notify::new(),
                settings: Settings {
                    django_url: format!("{url}/context"),
                    history_url: format!("{url}/history"),
                    ober_url: url,
                    nats_url: String::new(),
                    redis_url: String::new(),
                    uma_url: native.then_some("http://unused-uma-in-interface-test".into()),
                    bind: String::new(),
                    journal: path.to_string_lossy().into(),
                    max_signal_age_ms: 5000,
                    max_inflight: 8,
                    context_max_age_ms: 90000,
                    max_order_budget_pusd: if native {
                        Decimal::from(30)
                    } else {
                        Decimal::from(10)
                    },
                    order_sizing: if native {
                        Some(crate::shadow_state::OrderSizing {
                            standard: Decimal::from(5),
                            below_005: Decimal::from(5),
                            below_080: Decimal::from(20),
                            through_098: Decimal::from(10),
                            at_099_low_depth: Decimal::from(20),
                        })
                    } else {
                        None
                    },
                    total_budget_pusd: Some("100".parse()?),
                },
                data: RwLock::new(Data {
                    restore: if native {
                        vec![("42".into(), now - 61_000)]
                    } else {
                        Vec::new()
                    },
                    last_sync_ms: now,
                    synced_epoch: Some(1),
                    native_uma_at_ms: now,
                    native_uma_health: json!({"ready":true}),
                    ..Data::default()
                }),
                telemetry: Telemetry::default(),
                exchange,
                journal: Arc::new(Mutex::new(journal)),
                slots: Arc::new(Semaphore::new(8)),
                notify: Notify::new(),
                nats_up: AtomicBool::new(true),
                redis_up: AtomicBool::new(true),
                redis_epoch: AtomicU64::new(1),
                stopped: AtomicBool::new(false),
                stop_reason: std::sync::Mutex::new(None),
                boot_at_ms: now,
                http: reqwest::Client::new(),
            });
            if native {
                app.data.write().await.lifecycle.apply("uma:resolution",&json!({"market_id":"test-market","request_id":"r","block_number":10,"proposed_price":1,"block_timestamp":now/1000}));
            }
            let mut book = json!({"market_id":"test-market","token_id":"42","timestamp":now_ms()*1000,
            "best_ask":{"price":"0.90","size":"20"},"best_bid":{"price":"0.82","size":"100"},"tick_size":"0.001",
            "top_5_asks":[{"price":"0.90","size":"20"}],"top_5_bids":[{"price":"0.82","size":"100"}]});
            if native {
                // The complete receive/sign/submit path must permit a partial FAK.
                book["best_ask"] = json!({"price":"0.50","size":"10"});
                book["top_5_asks"] = json!([{"price":"0.50","size":"10"}]);
            }
            app.receive(
                &serde_json::to_vec(&book)?,
                app.slots.clone().acquire_owned().await?,
                Instant::now(),
            )
            .await;
            let permits = app.slots.acquire_many(8).await?;
            assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
            let id = format!("shadow-{:x}", Sha256::digest(serde_json::to_vec(&book)?));
            assert_eq!(
                app.journal.lock().await.get(&id)?.unwrap().reply.state,
                "accepted"
            );
            assert_eq!(
                app.journal
                    .lock()
                    .await
                    .get(&id)?
                    .unwrap()
                    .reply
                    .submitted_amount,
                Some(if native {
                    Decimal::from(20)
                } else {
                    "4.50".parse()?
                })
            );
            drop(permits);
            book["timestamp"] = json!(now_ms() * 1000);
            app.receive(
                &serde_json::to_vec(&book)?,
                app.slots.clone().acquire_owned().await?,
                Instant::now(),
            )
            .await;
            assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
            assert_eq!(
                app.data.read().await.rejections.back().unwrap()["reason"],
                "max_orders_per_market"
            );
            app.data.write().await.lifecycle.apply(
                "uma:dispute_price",
                &json!({"market_id":"test-market","request_id":"r","block_number":11}),
            );
            app.receive(
                &serde_json::to_vec(&book)?,
                app.slots.clone().acquire_owned().await?,
                Instant::now(),
            )
            .await;
            assert_eq!(mock.state.posts.load(Ordering::SeqCst), 1);
            assert_eq!(
                app.data.read().await.rejections.back().unwrap()["reason"],
                "not_proposed_or_lifecycle_changed"
            );
            app.halt("lifecycle_capacity");
            app.data.write().await.context_error.clear();
            app.halt("manual_stop");
            let state = health(State(app.clone())).await.0;
            assert_eq!(state["ready"], false);
            assert_eq!(state["stop_reason"], "lifecycle_capacity");
            server.abort();
            drop(app);
            std::fs::remove_file(path.with_extension("history-acks.jsonl"))?;
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}
