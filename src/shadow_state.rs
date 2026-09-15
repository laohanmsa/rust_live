//! Pure lifecycle and guard logic, shared by live-data shadow input and replay tests.
use crate::now_ms;
use polymarket_client_sdk_v2::types::Decimal;
use rust_decimal::MathematicalOps;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct Mark {
    pub request: String,
    pub request_key: String,
    pub block: u64,
    pub index: u64,
    pub status: String,
    pub received_ms: u64,
    pub proposed_price: Option<Decimal>,
}
#[derive(Default)]
pub struct Lifecycle {
    pub marks: HashMap<String, Mark>,
    terminals: HashMap<String, (u64, u64, String)>,
    settled: HashSet<String>,
    disputes: HashMap<String, HashSet<String>>,
    pub needs_refresh: HashSet<String>,
}
impl Lifecycle {
    pub fn prune_inactive(&mut self, active: &HashSet<String>, now: u64) -> usize {
        let before = self.marks.len();
        self.marks.retain(|market, mark| {
            active.contains(market)
                || now.saturating_sub(mark.received_ms) < 10_800_000
                || self.disputes.get(market).is_some_and(|d| !d.is_empty())
        });
        self.needs_refresh
            .retain(|market| self.marks.contains_key(market));
        // Keep terminal proofs: evicting a cache entry must not revive a late proposal.
        before - self.marks.len()
    }
    pub fn terminal_proof_count(&self) -> usize {
        self.terminals.len()
    }

    pub fn apply(&mut self, channel: &str, value: &Value) -> bool {
        let Some(market) = value["market_id"].as_str().filter(|s| !s.is_empty()) else {
            return false;
        };
        let Some(request) = value["request_id"].as_str().filter(|s| !s.is_empty()) else {
            return false;
        };
        let Some(block) = value["block_number"].as_u64() else {
            return false;
        };
        let index = value["log_index"].as_u64().unwrap_or(0);
        let key = format!(
            "{market}/{request}/{}/{}",
            value["request_timestamp"], value["oracle_address"]
        );
        let status = match channel {
            "uma:resolution" => "proposed",
            "uma:dispute_price" => "disputed",
            "uma:settle" => "settled",
            _ => return false,
        };
        self.needs_refresh.insert(market.into());
        if status != "proposed" {
            if self
                .terminals
                .get(&key)
                .is_some_and(|(b, i, status)| (*b, *i) >= (block, index) || status == "settled")
            {
                return false;
            }
            self.terminals
                .insert(key.clone(), (block, index, status.into()));
            if status == "settled" {
                self.settled.insert(key.clone());
                if let Some(disputes) = self.disputes.get_mut(market) {
                    disputes.remove(&key);
                }
            } else if !self.settled.contains(&key) {
                self.disputes
                    .entry(market.into())
                    .or_default()
                    .insert(key.clone());
            }
        }
        if status == "proposed" && self.terminals.contains_key(&key) {
            return false;
        }
        if let Some(old) = self.marks.get(market) {
            if (block, index) <= (old.block, old.index) {
                return false;
            }
            if status != "proposed"
                && (old.request != request
                    || (!old.request_key.is_empty() && old.request_key != key))
            {
                return false;
            }
            if status == "proposed" && old.status != "proposed" && old.request_key == key {
                return false;
            }
        }
        self.marks.insert(
            market.into(),
            Mark {
                request: request.into(),
                request_key: key,
                block,
                index,
                status: status.into(),
                received_ms: now_ms(),
                proposed_price: if status == "proposed" {
                    decimal(&value["proposed_price"])
                } else {
                    None
                },
            },
        );
        true
    }
    pub fn has_dispute(&self, market: &str) -> bool {
        self.disputes.get(market).is_some_and(|d| !d.is_empty())
    }
    pub fn reconcile_settles(&mut self, market: &str, settled: &HashMap<String, u64>) {
        for (request, block) in settled {
            let prefix = format!("{market}/{request}/");
            let keys = self
                .terminals
                .iter()
                .filter(|(key, (b, _, _))| key.starts_with(&prefix) && b <= block)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in keys {
                self.settled.insert(key.clone());
                self.terminals
                    .insert(key.clone(), (*block, 0, "settled".into()));
                if let Some(d) = self.disputes.get_mut(market) {
                    d.remove(&key);
                }
            }
        }
    }
    pub fn seed(&mut self, market: &str, resolution: &Resolution) {
        let block = resolution
            .block_number
            .unwrap_or(0)
            .max(resolution.dispute_block_number.unwrap_or(0))
            .max(resolution.settle_block_number.unwrap_or(0));
        if self.marks.get(market).is_some_and(|old| old.block >= block) {
            return;
        }
        let status = if resolution.settled {
            "settled"
        } else if resolution.disputed {
            "disputed"
        } else {
            &resolution.status
        };
        if status == "proposed" {
            let prefix = format!(
                "{market}/{}/",
                resolution.request_id.as_deref().unwrap_or("")
            );
            if self.terminals.iter().any(|(key, (terminal_block, _, _))| {
                key.starts_with(&prefix) && *terminal_block >= block
            }) {
                return;
            }
        }
        self.marks.insert(
            market.into(),
            Mark {
                request: resolution.request_id.clone().unwrap_or_default(),
                request_key: String::new(),
                block,
                index: 0,
                status: status.into(),
                received_ms: now_ms(),
                proposed_price: Some(resolution.proposed_price),
            },
        );
    }
    pub fn matches_proposal(
        &self,
        market: &str,
        request: &str,
        block: u64,
        price: Decimal,
    ) -> bool {
        self.is_proposed(market, request, block)
            && self
                .marks
                .get(market)
                .is_some_and(|m| m.proposed_price == Some(price))
    }
    pub fn is_proposed(&self, market: &str, request: &str, block: u64) -> bool {
        self.marks
            .get(market)
            .is_some_and(|m| m.status == "proposed" && m.request == request && m.block == block)
    }
}
#[derive(Clone, Deserialize)]
pub struct Resolution {
    pub id: u64,
    pub request_id: Option<String>,
    pub status: String,
    pub proposed_price: Decimal,
    pub propose_time_ms: u64,
    pub block_number: Option<u64>,
    pub dispute_block_number: Option<u64>,
    pub settle_block_number: Option<u64>,
    pub disputed: bool,
    pub settled: bool,
}
#[derive(Clone, Deserialize)]
pub struct Fee {
    pub rate: Decimal,
    pub exponent: Decimal,
}
#[derive(Clone, Deserialize)]
pub struct Valuation {
    pub expected_payout: Decimal,
    pub calculated_at_ms: u64,
}
#[derive(Clone, Deserialize)]
pub struct MarketContext {
    pub market_id: String,
    pub question: String,
    pub market_volume: Option<Decimal>,
    pub event_volume: Option<Decimal>,
    pub tags: Vec<String>,
    pub eligible: bool,
    pub token_id_yes: Option<String>,
    pub token_id_no: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
    pub auto_archived: bool,
    pub min_tick_size: Decimal,
    pub min_order_size: Decimal,
    pub neg_risk: bool,
    pub fees_enabled: bool,
    pub fee_schedule: Option<Fee>,
    pub fee_verification_status: String,
    #[serde(default)]
    pub settled_request_blocks: HashMap<String, u64>,
    pub has_disputed_resolution: bool,
    pub existing_order_count: u64,
    pub resolution: Option<Resolution>,
    pub valuation: Option<Valuation>,
}
#[derive(Clone, Deserialize)]
pub struct Policy {
    pub valuation_key: Option<String>,
    pub manual_trade_shutdown_enabled: bool,
    pub strategy_enabled: bool,
    pub max_ask_price: Decimal,
    pub max_orders_per_market: u64,
    pub ev_threshold: Decimal,
    pub order_size_usd: Decimal,
    pub low_price_order_size_usd: Decimal,
    pub low_depth_099_order_size_usd: Decimal,
}
#[derive(Deserialize)]
pub struct Page {
    pub schema_version: u64,
    pub captured_at_ms: u64,
    pub config: Option<Policy>,
    pub results: Vec<MarketContext>,
    pub has_more: bool,
    pub next_after_id: Option<String>,
}
#[derive(Clone)]
pub struct Decision {
    pub context: Arc<MarketContext>,
    pub price: Decimal,
    pub shares: Decimal,
    pub tick: Decimal,
    pub budget: Decimal,
    pub fair_value: Decimal,
    pub request_id: String,
    pub block: u64,
}
pub fn decimal(value: &Value) -> Option<Decimal> {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
        .parse()
        .ok()
}
pub fn event_ms(value: &Value) -> Option<u64> {
    let n = value.as_u64().or_else(|| value.as_str()?.parse().ok())?;
    Some(if n >= 100_000_000_000_000 {
        n / 1000
    } else {
        n
    })
}

#[derive(Default)]
pub struct Reservations {
    pub slots: HashMap<String, VecDeque<u64>>,
    pub last_order: HashMap<String, u64>,
}

pub fn decide(
    context: Arc<MarketContext>,
    policy: &Policy,
    lifecycle: &Lifecycle,
    book: &Value,
    reservations: &mut Reservations,
    now: u64,
    max_budget: Decimal,
) -> Result<Decision, &'static str> {
    let r = context.resolution.as_ref().ok_or("missing_resolution")?;
    let request = r
        .request_id
        .as_deref()
        .filter(|r| !r.is_empty())
        .ok_or("missing_request_identity")?;
    if !context.eligible
        || !lifecycle.matches_proposal(
            &context.market_id,
            request,
            r.block_number.unwrap_or(0),
            r.proposed_price,
        )
    {
        return Err("not_proposed_or_lifecycle_changed");
    }
    let winner = if r.proposed_price == Decimal::ONE {
        context.token_id_yes.as_deref()
    } else if r.proposed_price == Decimal::ZERO {
        context.token_id_no.as_deref()
    } else {
        return Err("non_binary_proposal");
    };
    if winner != book["token_id"].as_str() {
        return Err("not_winner_token");
    }
    if policy.manual_trade_shutdown_enabled || !policy.strategy_enabled {
        return Err("strategy_stopped");
    }
    if !context.active || context.closed || context.auto_archived || !context.accepting_orders {
        return Err("market_closed");
    }
    if context
        .tags
        .iter()
        .any(|t| ["middle east", "israel", "trump"].contains(&t.trim().to_lowercase().as_str()))
    {
        return Err("block_restricted_event_tags");
    }
    if context.has_disputed_resolution || lifecycle.has_dispute(&context.market_id) {
        return Err("market_not_disputed");
    }
    let price = decimal(&book["best_ask"]["price"]).ok_or("missing_ask")?;
    let depth = if let Some(levels) = book["top_5_asks"].as_array() {
        levels
            .iter()
            .take(5)
            .map(|v| {
                decimal(&v["size"])
                    .filter(|s| *s >= Decimal::ZERO)
                    .ok_or("invalid_ask_depth")
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum()
    } else {
        decimal(&book["best_ask"]["size"]).ok_or("missing_ask_depth")?
    };
    if price <= Decimal::ZERO || price >= Decimal::ONE || depth <= Decimal::ZERO {
        return Err("invalid_ask");
    }
    let d = |s: &str| s.parse::<Decimal>().unwrap_or(Decimal::ZERO);
    if price >= d("0.80")
        && ((price < d("0.89") && depth > d("200"))
            || (price < d("0.98") && depth > d("800"))
            || (price <= d("0.999") && depth > d("1920")))
    {
        return Err("cheap_winner_ask_wall");
    }
    if price > policy.max_ask_price {
        return Err("max_ask_price");
    }
    if context.min_order_size <= Decimal::ZERO {
        return Err("missing_market_rules");
    };
    let tick = decimal(&book["tick_size"]).unwrap_or(context.min_tick_size);
    if tick <= Decimal::ZERO {
        return Err("invalid_tick_metadata");
    }
    if price < tick || price > Decimal::ONE - tick || price % tick != Decimal::ZERO {
        return Err("min_tick_price_or_alignment");
    }
    if policy.valuation_key.as_deref() != Some("m5_expected_payout") {
        return Err("unsupported_valuation_model");
    }
    let bid_depth = if let Some(levels) = book["top_5_bids"].as_array() {
        levels
            .iter()
            .take(5)
            .map(|v| {
                decimal(&v["size"])
                    .filter(|s| *s >= Decimal::ZERO)
                    .ok_or("invalid_bid_depth")
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum()
    } else {
        decimal(&book["best_bid"]["size"]).unwrap_or_default()
    };
    let fair_value = crate::shadow_valuation::expected_payout(&crate::shadow_valuation::M5Input {
        market_volume: context.market_volume.ok_or("missing_market_volume")?,
        event_volume: context.event_volume.ok_or("missing_event_volume")?,
        proposed_price: r.proposed_price,
        seconds_after_propose: now.saturating_sub(r.propose_time_ms) / 1000,
        winner_bid: decimal(&book["best_bid"]["price"]).unwrap_or_default(),
        winner_ask: price,
        winner_bid_depth: bid_depth,
        loser_bid: decimal(&book["loser_bid"]).ok_or("missing_loser_book")?,
    })?;
    let fee = if context.fees_enabled {
        let fee = context
            .fee_schedule
            .as_ref()
            .ok_or("fee_schedule_unavailable")?;
        if fee.rate < Decimal::ZERO
            || fee.rate > Decimal::ONE
            || fee.exponent < Decimal::ZERO
            || fee.exponent > d("4")
        {
            return Err("invalid_fee_schedule");
        }
        price * fee.rate * (price * (Decimal::ONE - price)).powd(fee.exponent)
    } else {
        Decimal::ZERO
    };
    if fair_value - price - fee <= policy.ev_threshold {
        return Err("ev_below_threshold");
    }
    let budget = if price < d("0.05") {
        Decimal::ONE
    } else if price < d("0.5") {
        policy.order_size_usd.max(policy.low_price_order_size_usd)
    } else if price == d("0.99") && depth < d("50") {
        policy
            .order_size_usd
            .max(policy.low_depth_099_order_size_usd)
    } else {
        policy.order_size_usd
    };
    let budget = budget.min(max_budget);
    let shares = (budget / (price + fee)).floor();
    if shares < context.min_order_size || shares <= Decimal::ZERO {
        return Err("below_market_minimum_size");
    }
    if depth < shares {
        return Err("insufficient_ask_liquidity");
    }
    let history = reservations
        .slots
        .entry(context.market_id.clone())
        .or_default();
    while history
        .front()
        .is_some_and(|ts| now.saturating_sub(*ts) >= 10_800_000)
    {
        history.pop_front();
    }
    if history.len() as u64 >= policy.max_orders_per_market {
        return Err("max_orders_per_market");
    }
    if reservations
        .last_order
        .get(&context.market_id)
        .is_some_and(|at| now.saturating_sub(*at) < 60_000)
    {
        return Err("market_cooldown");
    }
    let request_id = request.to_owned();
    let block = r.block_number.unwrap_or(0);
    history.push_back(now);
    reservations
        .last_order
        .insert(context.market_id.clone(), now);
    Ok(Decision {
        context,
        price,
        shares,
        tick,
        budget,
        fair_value,
        request_id,
        block,
    })
}
