use crate::{Reply, now_ms};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

const CAPACITY: usize = 2048;
#[derive(Default)]
struct Data {
    received: u64,
    completed: u64,
    replayed: u64,
    active: usize,
    overwritten: u64,
    samples: VecDeque<(u64, Reply, f64)>,
}
#[derive(Default)]
pub struct Telemetry {
    data: Mutex<Data>,
}
impl Telemetry {
    pub fn received(&self) {
        self.data.lock().unwrap().received += 1;
    }
    pub fn started(&self) {
        self.data.lock().unwrap().active += 1;
    }
    pub fn finished(&self, reply: &Reply, handler_ms: f64, active: bool) {
        let mut d = self.data.lock().unwrap();
        d.completed += 1;
        d.replayed += u64::from(reply.replayed);
        if active {
            d.active = d.active.saturating_sub(1);
        }
        if d.samples.len() == CAPACITY {
            d.samples.pop_front();
            d.overwritten += 1;
        }
        let mut compact = reply.clone();
        if let Some(uma) = compact.uma.as_mut().and_then(Value::as_object_mut) {
            uma.remove("winner_book");
            uma.remove("loser_book");
        }
        d.samples.push_back((now_ms(), compact, handler_ms));
    }
    pub fn snapshot(&self, window: u64) -> Value {
        let d = self.data.lock().unwrap();
        let since = now_ms().saturating_sub(window * 1000);
        let rows: Vec<_> = d.samples.iter().filter(|(at, _, _)| *at >= since).collect();
        let mut states = BTreeMap::<String, usize>::new();
        let mut reasons = BTreeMap::<String, usize>::new();
        let mut samples = BTreeMap::<&str, Vec<f64>>::new();
        for key in [
            "queue_ms",
            "policy_ms",
            "sign_ms",
            "journal_ms",
            "dispatch_ms",
            "source_to_dispatch_ms",
            "post_ms",
            "finalize_ms",
            "handler_total_ms",
        ] {
            samples.insert(key, Vec::new());
        }
        for key in [
            "django_ms",
            "book_ms",
            "winner_book_ms",
            "loser_book_ms",
            "guard_ms",
            "feed_to_handler_ms",
            "proposal_queue_ms",
        ] {
            samples.insert(key, Vec::new());
        }
        for (_, r, total) in &rows {
            *states.entry(r.state.clone()).or_default() += 1;
            if !r.reason.is_empty() {
                *reasons.entry(r.reason.clone()).or_default() += 1;
            }
            if r.replayed {
                continue;
            }
            if let Some(uma) = &r.uma {
                for key in [
                    "django_ms",
                    "book_ms",
                    "winner_book_ms",
                    "loser_book_ms",
                    "guard_ms",
                    "feed_to_handler_ms",
                    "proposal_queue_ms",
                ] {
                    let field = if key == "proposal_queue_ms" {
                        "queue_ms"
                    } else {
                        key
                    };
                    if let Some(v) = uma[field].as_f64()
                        && let Some(values) = samples.get_mut(key)
                    {
                        values.push(v);
                    }
                }
            }
            samples.get_mut("handler_total_ms").unwrap().push(*total);
            if r.sign_ms > 0.0 {
                samples.get_mut("sign_ms").unwrap().push(r.sign_ms);
                samples.get_mut("queue_ms").unwrap().push(r.queue_ms);
            }
            if r.journal_ms > 0.0 {
                samples.get_mut("journal_ms").unwrap().push(r.journal_ms);
            }
            for (k, v) in [
                ("policy_ms", r.policy_ms),
                ("dispatch_ms", r.dispatch_ms),
                ("source_to_dispatch_ms", r.source_to_dispatch_ms),
                ("post_ms", r.post_ms),
                ("finalize_ms", r.finalize_ms),
            ] {
                if let Some(v) = v {
                    samples.get_mut(k).unwrap().push(v);
                }
            }
        }
        let latency: BTreeMap<_, _> = samples
            .into_iter()
            .map(|(k, mut values)| {
                values.sort_by(f64::total_cmp);
                let n = values.len();
                let q = |p: f64| {
                    if n == 0 {
                        None
                    } else {
                        Some(values[((n - 1) as f64 * p).ceil() as usize])
                    }
                };
                (
                    k,
                    json!({"n":n,"p50":q(0.5),"p95":q(0.95),"p99":q(0.99),"max":values.last()}),
                )
            })
            .collect();
        json!({"received":d.received,"completed":d.completed,"replayed":d.replayed,"active":d.active,
            "sample_capacity":CAPACITY,"window_seconds":window,"window_samples":rows.len(),
            "window_truncated":d.overwritten>0&&d.samples.front().is_some_and(|(t,_,_)|*t>since),
            "states":states,"reasons":reasons,"latency_ms":latency,
            "coverage":"authorized parsed signals; counters since boot; bounded completed-sample window; replays excluded from latency"})
    }
}
