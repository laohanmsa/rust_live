# rust_uma proposal lane

This is a second process with a separate account, journal, receipt strategy and execution limit.
The original trader keeps its existing book-driven behavior.
Both processes subscribe independently to the existing `rust.uma.events` subject and read the same UMA service snapshot on connection or recovery.
No second oracle listener or database connection is created.

## Processing

1. Accept a fresh binary proposal from the shared sequenced feed.
2. Query `/api/trading-context/?market_ids=<market>` once for the trigger.
   Missing market rows are logged as `django_market_missing` and skipped without hydration or retry.
3. Fetch winner and opposite `/book?token_id=...` responses concurrently from Polymarket with a persistent request client.
   The opposite best bid is required by the existing M5 valuation.
   Sort and validate the returned levels; do not assume response ordering.
4. Require a winner ask, then run the shared lifecycle, market status, tag, dispute, price, tick, ask-wall, fee, local M5, market-count and cooldown checks.
5. Sign an exact five-unit cash BUY/FAK order, with its share quantity calculated by the existing exchange client.
   FAK means immediate execution with any unfilled remainder canceled; acceptance is not proof of a fill.
6. Persist the prepared order before submission, and persist the response afterward.
   Export the receipt to Dashboard as `rust_uma` using the existing durable retry/acknowledgement path.

The cash amount is fixed at 5; it is not five shares and does not use the first lane's price-dependent budgets.
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
The receipt carries queue wait, Django lookup, each public book request, parallel book wall time, guard/valuation, signing, prepared journal, dispatch and exchange-response durations.
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
No account was supplied for this task, and this lane has not been deployed or enabled.

## Validation

Run `cargo test --locked`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo fmt --check`.
The proposal tests use loopback Django/book/exchange fixtures, real order construction and signing with synthetic credentials, and validate missing markets, empty asks, restricted tags, exact cash sizing, partial-fill eligibility, duplicate identity, late disputes, connection epochs and eight overlapping context queries.
No live account or production state is used by these tests.
The GitHub Actions workflow runs the same Rust checks without credentials.
