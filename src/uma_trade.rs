//! Proposal-triggered lane. Shared UMA lifecycle, direct database context and trusted OBer books.
use super::*;
use crate::shadow_state::decimal;

pub(super) const MAX_PRICE: Decimal = Decimal::from_parts(998, 0, 0, false, 3);

pub(super) fn signal_id(event: &crate::uma::Event, live: bool) -> String {
    let identity = format!(
        "rust_uma/{}/{}/{}/{}",
        event.oracle_address, event.block_hash, event.tx_hash, event.log_index
    );
    format!(
        "{}-{:x}",
        if live { "live" } else { "shadow" },
        Sha256::digest(identity)
    )
}

fn normalize_book(mut book: Value, token: &str) -> Result<Value, &'static str> {
    if book["asset_id"].as_str() != Some(token) {
        return Err("orderbook_token_mismatch");
    }
    for side in ["bids", "asks"] {
        let levels = book[side].as_array_mut().ok_or("invalid_orderbook")?;
        if levels.len() > 10_000 {
            return Err("orderbook_too_large");
        }
        let mut parsed = Vec::with_capacity(levels.len());
        for level in levels.iter() {
            let price = decimal(&level["price"]).ok_or("invalid_orderbook_price")?;
            let size = decimal(&level["size"]).ok_or("invalid_orderbook_size")?;
            if price <= Decimal::ZERO || price >= Decimal::ONE || size <= Decimal::ZERO {
                return Err("invalid_orderbook_level");
            }
            parsed.push((price, level.clone()));
        }
        parsed.sort_by(|a, b| {
            if side == "asks" {
                a.0.cmp(&b.0)
            } else {
                b.0.cmp(&a.0)
            }
        });
        *levels = parsed.into_iter().map(|(_, v)| v).collect();
    }
    Ok(book)
}

impl App {
    pub(super) async fn warm_book_connection(&self) -> Result<()> {
        let started = Instant::now();
        let mut response = self
            .http
            .get(format!(
                "{}/health",
                self.settings.ober_url.trim_end_matches('/')
            ))
            .timeout(Duration::from_secs(2))
            .send()
            .await?
            .error_for_status()?;
        let version = response.version();
        let mut bytes = 0;
        while let Some(chunk) = response.chunk().await? {
            bytes += chunk.len();
            ensure!(bytes <= 64 * 1024, "oversized OBer health response");
        }
        println!(
            "{}",
            json!({"event":"ober_connection_warmed","http_version":format!("{version:?}"),"elapsed_ms":elapsed_ms(started)})
        );
        Ok(())
    }

    pub(super) fn dispatch_uma(self: &Arc<Self>, event: crate::uma::Event) {
        self.telemetry.received();
        let received = Instant::now();
        let epoch = self.redis_epoch.load(Ordering::SeqCst);
        // Keep lifecycle/dispute ingestion independent of slow context reads and execution.
        let Ok(waiting) = self.proposal_queue.clone().try_acquire_owned() else {
            let reply = Reply::blocked(
                &signal_id(&event, self.live.is_some()),
                "proposal_queue_full",
            );
            eprintln!(
                "{}",
                json!({"event":"rust_uma_skipped","market_id":event.market_id,"reason":reply.reason})
            );
            self.telemetry.finished(&reply, 0.0, false);
            return;
        };
        let app = self.clone();
        tokio::spawn(async move {
            app.receive_uma(event, received, epoch).await;
            drop(waiting);
        });
    }

    pub(super) async fn receive_uma(
        self: &Arc<Self>,
        event: crate::uma::Event,
        received: Instant,
        epoch: u64,
    ) {
        let id = signal_id(&event, self.live.is_some());
        let mut evidence = json!({"event":event,"feed_epoch":epoch,
            "feed_to_handler_ms":now_ms().saturating_sub(event.received_at_ms) as f64});
        let result = async {
            event.validate().map_err(|_| "invalid_uma_proposal")?;
            if event.channel != "uma:resolution"
                || !["0", "1"].contains(&event.proposed_price.as_str())
            {
                return Err("non_binary_proposal");
            }
            if event.received_at_ms < self.boot_at_ms
                || event.received_at_ms > now_ms().saturating_add(10)
                || now_ms().saturating_sub(event.block_timestamp.saturating_mul(1000)) > 10_000
            {
                return Err("historical_or_invalid_proposal");
            }
            let permit = input_permit(
                &self.slots,
                event.received_at_ms,
                self.settings.max_signal_age_ms,
            )
            .await?;
            evidence["queue_ms"] = json!(elapsed_ms(received));
            {
                let data = self.data.read().await;
                if !self.ready(&data) || epoch != self.redis_epoch.load(Ordering::SeqCst) {
                    return Err("context_not_ready");
                }
                if !data.lifecycle.matches_proposal(
                    &event.market_id,
                    &event.request_id,
                    event.block_number,
                    event
                        .proposed_price
                        .parse()
                        .map_err(|_| "invalid_proposed_price")?,
                ) {
                    return Err("not_proposed_or_lifecycle_changed");
                }
            }
            let remaining = self
                .settings
                .max_signal_age_ms
                .saturating_sub(now_ms().saturating_sub(event.received_at_ms));
            let prepared = tokio::time::timeout(
                Duration::from_millis(remaining),
                self.uma_inputs(&event, &mut evidence),
            )
            .await;
            let (context, policy, book) = prepared.map_err(|_| "input_preparation_expired")??;
            let guard = Instant::now();
            let decision = {
                let mut data = self.data.write().await;
                if !self.ready(&data) || epoch != self.redis_epoch.load(Ordering::SeqCst) {
                    return Err("context_not_ready");
                }
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
            };
            evidence["guard_ms"] = json!(elapsed_ms(guard));
            let decision = decision?;
            evidence["expected_payout"] = json!(decision.fair_value.to_string());
            evidence["budget"] = json!(decision.budget.to_string());
            self.telemetry.started();
            let reply = self
                .execute(
                    decision,
                    id.clone(),
                    event.received_at_ms,
                    received,
                    elapsed_ms(received),
                    true,
                    Some(evidence.clone()),
                )
                .await;
            self.telemetry.finished(&reply, elapsed_ms(received), true);
            self.remember(&reply).await;
            eprintln!(
                "{}",
                json!({"event":"rust_uma_result","market_id":event.market_id,"signal_id":id,
                "state":reply.state,"reason":reply.reason,"django_ms":evidence["django_ms"],
                "book_ms":evidence["book_ms"],"total_ms":reply.total_ms})
            );
            drop(permit);
            Ok::<(), &'static str>(())
        }
        .await;
        if let Err(reason) = result {
            let mut reply = Reply::blocked(&id, reason);
            reply.uma = Some(evidence.clone());
            reply.total_ms = elapsed_ms(received);
            self.telemetry.finished(&reply, reply.total_ms, false);
            self.remember_rejection(&json!({"market_id":event.market_id}), reason)
                .await;
            eprintln!(
                "{}",
                json!({"event":"rust_uma_skipped","market_id":event.market_id,"signal_id":id,
                "reason":reason,"timings":evidence.as_object().map(|m|m.iter().filter(|(k,_)|k.ends_with("_ms")).collect::<BTreeMap<_,_>>())})
            );
        }
    }

    async fn uma_book(&self, token: &str, market: &str) -> Result<(Value, f64), &'static str> {
        if token.is_empty() || token.len() > 78 || !token.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid_token_id");
        }
        let started = Instant::now();
        let url = format!(
            "{}/book/{token}",
            self.settings.ober_url.trim_end_matches('/')
        );
        let mut response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|_| "ober_book_unavailable")?
            .error_for_status()
            .map_err(|_| "ober_book_http_error")?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "ober_book_read_failed")?
        {
            if bytes.len() + chunk.len() > 512 * 1024 {
                return Err("orderbook_too_large");
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut book: Value = serde_json::from_slice(&bytes).map_err(|_| "invalid_ober_book")?;
        if book["token_id"].as_str() != Some(token) || book["market_id"].as_str() != Some(market) {
            return Err("ober_market_or_token_mismatch");
        }
        // Retain native identity/clock and add the existing receipt aliases.
        book["asset_id"] = book["token_id"].clone();
        book["market"] = book["condition_id"].clone();
        book["ober_timestamp"] = book["timestamp"].clone();
        book["timestamp"] = json!(
            event_ms(&book["timestamp"])
                .filter(|t| *t > 0)
                .ok_or("invalid_ober_timestamp")?
        );
        let book = normalize_book(book, token)?;
        // Full books include diagnostic/quarantined data. Best reads enforce OBer's trust gate.
        let trusted: Value = self
            .http
            .get(format!("{url}/best"))
            .send()
            .await
            .map_err(|_| "ober_trust_check_failed")?
            .error_for_status()
            .map_err(|_| "ober_untrusted_book")?
            .json()
            .await
            .map_err(|_| "invalid_ober_best")?;
        if trusted["token_id"].as_str() != Some(token)
            || trusted["market_id"].as_str() != Some(market)
        {
            return Err("ober_market_or_token_mismatch");
        }
        for (side, price, size) in [
            ("bids", "best_bid", "best_bid_size"),
            ("asks", "best_ask", "best_ask_size"),
        ] {
            let level = book[side].as_array().and_then(|rows| rows.first());
            if level.and_then(|r| decimal(&r["price"])) != decimal(&trusted[price])
                || level.and_then(|r| decimal(&r["size"])) != decimal(&trusted[size])
            {
                return Err("ober_book_changed_during_read");
            }
        }
        if let (Some(bid), Some(ask)) =
            (decimal(&trusted["best_bid"]), decimal(&trusted["best_ask"]))
            && bid >= ask
        {
            return Err("crossed_orderbook");
        }
        Ok((book, elapsed_ms(started)))
    }

    async fn uma_inputs(
        &self,
        event: &crate::uma::Event,
        evidence: &mut Value,
    ) -> Result<(Arc<MarketContext>, Policy, Value), &'static str> {
        let started = Instant::now();
        let source = if self.database.is_some() {
            "postgresql"
        } else {
            "django"
        };
        evidence["context_source"] = json!(source);
        let result = if let Some(database) = &self.database {
            database.fetch(&event.market_id).await
        } else {
            self.fetch(Some(std::slice::from_ref(&event.market_id)))
                .await
        };
        evidence[if self.database.is_some() {
            "database_ms"
        } else {
            "django_ms"
        }] = json!(elapsed_ms(started));
        let (rows, policy) = result.map_err(|_| {
            if self.database.is_some() {
                "database_context_failed"
            } else {
                "django_context_failed"
            }
        })?;
        let mut context = rows
            .into_iter()
            .find(|c| c.market_id == event.market_id)
            .ok_or(if self.database.is_some() {
                "database_market_missing"
            } else {
                "django_market_missing"
            })?;
        let mut policy = policy.ok_or("missing_strategy_config")?;
        policy.order_sizing = self.settings.order_sizing.clone();
        policy.max_ask_price = policy.max_ask_price.min(MAX_PRICE);
        evidence["book_source"] = json!("ober");
        let (winner, loser) = if event.proposed_price == "1" {
            (&context.token_id_yes, &context.token_id_no)
        } else {
            (&context.token_id_no, &context.token_id_yes)
        };
        let winner = winner.clone().ok_or("missing_winner_token")?;
        let loser = loser.clone().ok_or("missing_loser_token")?;
        let started = Instant::now();
        // M5 needs the opposite outcome's bid. These independent reads share one client pool.
        let (winner_result, loser_result) = tokio::join!(
            self.uma_book(&winner, &event.market_id),
            self.uma_book(&loser, &event.market_id)
        );
        evidence["book_ms"] = json!(elapsed_ms(started));
        let (winner_book, winner_ms) = winner_result?;
        evidence["winner_book_ms"] = json!(winner_ms);
        evidence["winner_book"] = winner_book.clone();
        let (loser_book, loser_ms) = loser_result?;
        evidence["loser_book_ms"] = json!(loser_ms);
        evidence["loser_book"] = loser_book.clone();
        evidence["book_received_at_ms"] = json!(now_ms());
        if winner_book["asks"].as_array().is_none_or(Vec::is_empty) {
            return Err("missing_winner_ask");
        }
        if winner_book["market"] != loser_book["market"] {
            return Err("orderbook_market_rules_mismatch");
        }
        let asks = winner_book["asks"].as_array().ok_or("invalid_orderbook")?;
        let bids = winner_book["bids"].as_array().ok_or("invalid_orderbook")?;
        let book = json!({"market_id":event.market_id,"token_id":winner,"timestamp":now_ms()*1000,
            "best_ask":asks.first(),"best_bid":bids.first(),"tick_size":winner_book["tick_size"],
            "top_5_asks":asks.iter().take(5).collect::<Vec<_>>(),"top_5_bids":bids.iter().take(5).collect::<Vec<_>>(),
            "loser_bid":loser_book["bids"].as_array().and_then(|b|b.first()).map(|v|v["price"].clone()).unwrap_or(json!("0"))});
        let mut data = self.data.write().await;
        data.lifecycle
            .reconcile_settles(&context.market_id, &context.settled_request_blocks);
        crate::native_uma::overlay(&mut context, &data.lifecycle, now_ms());
        evidence["market_context"] = json!(context);
        evidence["policy"] = json!(policy);
        let context = Arc::new(context);
        // Only retain recent proposal contexts; Django remains authoritative on every trigger.
        data.markets.retain(|_, c| {
            c.resolution
                .as_ref()
                .is_some_and(|r| now_ms().saturating_sub(r.propose_time_ms) < 10_800_000)
        });
        let Data {
            markets, tokens, ..
        } = &mut *data;
        tokens.retain(|_, market| markets.contains_key(market));
        for token in [winner, loser] {
            data.tokens.insert(token, event.market_id.clone());
        }
        data.markets
            .insert(event.market_id.clone(), context.clone());
        data.restore_orders();
        data.lifecycle.needs_refresh.remove(&event.market_id);
        data.policy = Some(policy.clone());
        Ok((context, policy, book))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_or_wrong_token_books_cannot_reach_guards() {
        for book in [
            json!({"asset_id":"wrong","bids":[],"asks":[]}),
            json!({"asset_id":"42","bids":[],"asks":[{"price":"NaN","size":"1"}]}),
            json!({"asset_id":"42","bids":[],"asks":[{"price":"0.9","size":"-1"}]}),
            json!({"asset_id":"42","asks":[]}),
        ] {
            assert!(normalize_book(book, "42").is_err());
        }
    }
}
