# rust_uma proposal lane

This is a second process with a separate account, journal, receipt strategy and execution limit.
The original trader keeps its existing book-driven behavior.
Both processes subscribe independently to the existing `rust.uma.events` subject and read the same UMA service snapshot on connection or recovery.
No second oracle listener is created.
The dry-run lane uses a dedicated PostgreSQL read-only role and a bounded pool of eight connections.

## Processing

1. Accept a fresh binary proposal from the shared sequenced feed.
2. Read market context and strategy policy directly from PostgreSQL in one parameterized statement.
   Missing database rows are logged as `database_market_missing` and skipped without hydration or retry.
3. Fetch winner and opposite `/book?token_id=...` responses concurrently from Polymarket with a persistent request client.
   The opposite best bid is required by the existing M5 valuation.
   Sort and validate the returned levels; do not assume response ordering.
4. Require a winner ask, then run the shared lifecycle, market status, tag, dispute, price, tick, ask-wall, fee, local M5, market-count and cooldown checks.
5. Use the same price/depth budget bands, fee-aware share rounding and BUY/FAK signing path as Rust live.
   FAK means immediate execution with any unfilled remainder canceled; acceptance is not proof of a fill.
6. Persist the prepared order before submission, and persist the response afterward.
   Export the receipt to Dashboard as `rust_uma` using the existing durable retry/acknowledgement path.

The budgets match the deployed Rust live configuration: below 0.05 uses 5; 0.05 to below 0.80 uses 20; 0.80 through 0.98 uses 10; exactly 0.99 with the first five ask levels totaling less than 51 shares uses 20; all other eligible prices use 5.
The configured per-order ceiling is 30.
As in Rust live, shares are floored after including the estimated fee, then signed cash is truncated to cents; actual signed cash can be below the selected budget.
For example, with no fee, a 0.90 ask selects budget 10, plans 11 shares and submits 9.90 cash.
The price limit is the observed best winner ask.
Orders below the maintained minimum share size are skipped.
The displayed requested shares come from the actual signed order, including the exchange client's rounding.

## Concurrency and recovery

The default is eight concurrent handlers, with at most 1,024 pending proposal tasks.
Slot waiting consumes the configured 2,000 ms signal lifetime.
Proposal processing does not block dispute, settlement or heartbeat ingestion.
Freshness is measured from the UMA service receipt clock; proposals from before process startup or more than ten seconds after their block time are rejected.
These limits do not assert that an order book request always takes 30 ms.

The new lane reads an authoritative snapshot on initial connection and after sequence gaps, resets or feed epoch changes.
It does not dispatch historical proposals from snapshots.
Unlike the book-driven lane, it does not periodically replace the feed cursor with a snapshot while connected, since doing so could swallow live proposal triggers.
Uncertain submissions stop further orders; they are never automatically resubmitted.
The final check revalidates the lifecycle identity, feed epoch, account readiness, stop flags and signal lifetime immediately before submission.
A recovered journal restores market quotas and prevents resubmission of an already prepared proposal.

## Observation

Per-order evidence includes the complete validated winner/opposite snapshots, their exchange timestamps, proposal identity, maintained market context, applied policy, expected payout and signed cash.
The receipt carries queue wait, Django lookup, each OBer book request, parallel book wall time, guard/valuation, signing, prepared journal, dispatch and exchange-response durations.
Dashboard records receipt ingestion time separately.
The metrics endpoint reports stage percentiles over its bounded completed-sample window and explicitly reports truncation.
Books remain in the durable order journal/history, while telemetry retains compact records to avoid multiplying snapshot memory by 2,048 samples.
Rejected signals are logged with their market, reason and available timings; they do not pretend to be orders.

A read-only amster-p probe on 2026-09-17 at 01:34 Singapore time queried eight existing market IDs in three short waves per concurrency level.

| Concurrent requests | Samples | Median ms | 95th percentile ms | Maximum ms |
| --- | --- | --- | --- | --- |
| 1, before | 3 | 40.98 | 67.56 | 67.56 |
| 2 | 6 | 47.55 | 52.88 | 52.88 |
| 4 | 12 | 54.38 | 108.34 | 108.34 |
| 8 | 24 | 134.56 | 250.00 | 261.01 |
| 1, after | 3 | 35.70 | 40.62 | 40.62 |

Every response succeeded.
This short sample demonstrates increasing latency for this endpoint, but does not isolate database time or establish degradation of other Dashboard pages.
It is not a sustained-load capacity estimate and does not justify replacing the query layer without further measurement.
Raw local measurements are in `outputs/rust-uma/context-results.jsonl`.

## Delivery and activation

The companion `polym` branch `codex/rust-uma-history` extends the existing receipt serializer and order expansion.
Deploy that additive Dashboard change first.
The new trader requires `rust_uma` in the account readiness endpoint's `supported_strategies` list, preventing orders against an old history API.
The existing account readiness boolean still requires at least ten available cash units.

Use `deploy/compose.uma.yaml` with `RUST_UMA_IMAGE` set to an immutable image digest, `RUST_UMA_ACCOUNT` set to the separately authorized account and `RUST_UMA_CREDENTIALS` pointing to its restricted credential file.
The credential schema is the existing `LiveCredentials` structure; account name and signing/funding identity are checked before execution.
Do not run the old `provision_live_credentials.py` unchanged for this lane: it is explicitly scoped to the original `airdrop_224` account.
The compose file requires the explicit `live` profile, joins the existing UMA and Polym data networks, exposes health only on host loopback port 18789, and uses its own persistent volume.
It never recreates the original trader or UMA service.
The dry-run compose file `deploy/compose.uma-dry.yaml` runs `shadow /app/uma-dry-run.json`, mounts only the read-only database credential, and uses `/api/shadow-orders/` for history.
It does not mount account credentials and can only submit to its loopback mock exchange.
Its records use strategy `rust_uma`, have no account, show `DRY_RUN`, and never schedule reconciliation or claim real fills.
A compatible history endpoint and a healthy read-only database connection are required before processing proposals.
Deploy the additive Dashboard change before starting the dry-run container.

## Validation

Run `cargo test --locked`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo fmt --check`.
The proposal tests use loopback Django/book/exchange fixtures, real order construction and signing with synthetic credentials, and validate missing markets, empty asks, restricted tags, live-equivalent budget and cash rounding, partial-fill eligibility, duplicate identity, late disputes, connection epochs and eight overlapping context queries.
No live account or production state is used by these tests.
The GitHub Actions workflow runs the same Rust checks without credentials.

## Direct database contract

`sql/market_context.sql` uses maintained market/event/tag/resolution/policy tables only.
No wallet or order table is available to the reader role.
The active proposal is overlaid from the shared UMA lifecycle, and M5 is computed locally; cached Django valuation and shared strategy order counts are intentionally not queried.
Fees use the same normalized-rate precedence and named schedules as Dashboard.
The query has a 750 ms server deadline, and each connection defaults to read-only operations.
The credential is created on amster-p and never sent to the build host or committed.

A read-only comparison against the existing Dashboard context builder on eight live markets found no differences in the fields consumed by the guards or strategy.
Direct SQL execution took 21.869 ms on the first sample and 4.575-7.603 ms on the remaining seven samples.
Those numbers do not include connection pool acquisition or Rust decoding; the deployed per-proposal `database_ms` measurement does.

The isolated PostgreSQL schema contract test runs explicitly in GitHub Actions using a disposable `rust_uma_test` database.
It covers absent markets, market/token mapping, policy, normalized and named fees, proposal identity and settlement proofs.

## Order-time book display

The two complete OBer order books used for the decision are persisted in the existing prepared-order journal and exported with the order receipt.
Dashboard now renders the current book beside the saved order book using its existing orderbook component.
`/airdrop/orderbook/<token>/?order_snapshot_id=<order>&book_side=winner` reads only that order's immutable receipt; `book_side=loser` selects the opposite outcome.
Snapshot reads are always read-only, validate token/side identity, and never fall back to a live book.
The snapshot label shows its capture time and the elapsed time from collection to submission.
No second public request is added to the hot path, no duplicate snapshot table is introduced, and prior `rust_uma` orders work without backfill.

## Connection reuse and query planning

Database and public-book clients were already persistent before this optimization.
The database pool now creates all configured connections, prepares the context and readiness statements, and executes an empty-key context read before accepting proposals.
Only these read-only sessions use `plan_cache_mode=force_generic_plan`, avoiding PostgreSQL's initial per-execution custom planning for this indexed single-market query.
Query results are not cached: changed market state, settlements, fees and shutdown flags are read again on every proposal.
`/metrics` exposes `database_pool` size, availability, waiting requests and configured maximum.

The existing shared HTTP client explicitly enables HTTP/2 support and warms its public exchange connection with one bounded `/time` read at startup.
The response body is fully drained so the pooled connection can be reused; an exchange warm-up failure logs a diagnostic and leaves the normal bounded book-fetch path available.
A successful warm-up logs the negotiated protocol.
Both outcome books still use concurrent GET requests, the existing 90-second idle pool lifetime and normal reconnection behavior.
No extra public request is added to each proposal, and no periodic external keepalive job is introduced.

A controlled amster-p comparison used the same eight market IDs at concurrency eight, forty reads per variant.
The original planning policy measured median 5.963 ms / p95 10.639 ms; generic plans measured 2.480 ms / p95 4.388 ms.
An initial planning/execution inspection found custom planning around 3-5 ms and generic planning around 0.04 ms.
Reversing trial order measured generic-plan median 3.090 ms / p95 55.322 ms and auto-plan median 5.552 ms / p95 8.593 ms.
The median benefit reproduced, but tail latency did not improve consistently across these short trials.
These are controlled short samples, not a guarantee against occasional slow requests.

The official [batch books endpoint](https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body) was compared with parallel GETs using reused connections and three active market pairs.
Twenty warm samples per variant measured parallel GET median 27.94 ms / p95 31.79 ms, and batch POST median 26.08 ms / p95 33.68 ms.
Because batch requests did not improve the tail in that sample, the existing parallel path and full per-outcome snapshots were retained.
Long idle periods or peer disconnects can still require a new transport connection.

Tests verify that startup warming creates all pool sessions, later concurrent queries retain the same backend IDs, generic plans still see changed rows, and a book request reuses the connection opened by the time request.

## OBer source and authorized live account (2026-09-19)

The UMA lane now reads `GET /book/<token>` from its configured OBer service, with no Polymarket orderbook REST request or fallback.
It fetches both complete sides concurrently and then checks `/book/<token>/best` for each outcome.
The full endpoint is diagnostic and may expose quarantined data; the best endpoint enforces the existing trust gate.
A failed trust check, mismatched identity, changed top price/size or crossed book aborts the attempt.
No OBer service code or runtime restart is needed.
On a cache miss, OBer's existing endpoint behavior may subscribe the token and return an empty book; the trader skips that incomplete response.

Both full snapshots remain in the order receipt.
Native OBer token/market identity and original `ober_timestamp` are retained, with compatible `asset_id`/`market` aliases and a normalized millisecond timestamp for history rendering.
Minimum size and negative-risk metadata come from the maintained database context; the quoted tick comes from OBer when available.
The receipt reports `book_source=ober`, and its book timing includes the trust check.

The UMA-specific price ceiling is 0.998, applied as the minimum of that limit and the maintained strategy limit, with a second check before submission.
Other Rust live strategies retain their existing price limit.
The existing fee-aware sizing bands are unchanged.

The authorized live account is `airdrop_224`.
`python3 scripts/deploy_uma_dry.py --live-account airdrop_224` explicitly selects the live compose profile; omitting the argument still selects shadow mode.
The process reuses the existing protected account credential on amster-p and the existing read-only database credential.
The original Rust live and shared UMA containers are not recreated.
The live journal is `/app/data/rust-uma-live-airdrop-224.jsonl`, separate from the retained shadow journal.
No cash reservation or shared cash-allocation service is introduced, and `total_budget_pusd` remains null.
The journal's historical budget counter is audit data, not a cash hold.
Market-attempt deduplication, per-market limits, account readiness, current main's unknown-order reconciliation, tennis totals restriction and global dry-run controls remain active.
Do not disable the global dry-run switch automatically if it blocks activation.

The live-path test uses a synthetic account and a loopback exchange, checks 0.999 rejection and 0.998 acceptance, and covers untrusted OBer books without sending a real order.
