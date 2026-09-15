use anyhow::Result;
use polym_rust_demo::{
    Reply, demo,
    journal::{Journal, Stored},
};
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    os::unix::fs::OpenOptionsExt,
};

#[test]
fn legacy_log_recovers_past_old_capacity_without_losing_dedup_or_pending() -> Result<()> {
    let count: usize = std::env::var("JOURNAL_SCALE_ORDERS")
        .unwrap_or_else(|_| "50001".into())
        .parse()?;
    let root = std::env::temp_dir().join(format!(
        "rust-journal-scale-{}-{}",
        std::process::id(),
        polym_rust_demo::now_ms()
    ));
    std::fs::create_dir(&root)?;
    let path = root.join("orders.jsonl");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "{}",
        json!({"kind":"Scope","scope":"live:synthetic"})
    )?;
    let mut last = None;
    for n in 0..count {
        let mut signal = demo::signal(&format!("live-{n:064x}"));
        if n + 1 < count {
            signal.observed_at_ms = 1;
        }
        let mut reply: Reply = serde_json::from_value(
            json!({"id":signal.id,"state":"prepared","reason":"","queue_ms":0,"sign_ms":0,"journal_ms":0,"total_ms":0}),
        )?;
        reply.state = "prepared".into();
        let entry = Stored {
            signal,
            reserved: "5".parse()?,
            order: json!({"takerAmount":"5000000"}),
            reply,
        };
        writeln!(writer, "{}", json!({"kind":"Prepared","entry":entry}))?;
        let mut reply = entry.reply.clone();
        reply.state = "accepted".into();
        writeln!(writer, "{}", json!({"kind":"Result","reply":reply}))?;
        last = Some(entry);
    }
    writer.flush()?;
    drop(writer);
    File::open(&path)?.sync_all()?;
    let started = std::time::Instant::now();
    let mut journal = Journal::open(&path, "live:synthetic")?;
    assert_eq!(
        journal.used,
        polymarket_client_sdk_v2::types::Decimal::from(count)
            * polymarket_client_sdk_v2::types::Decimal::from(5)
    );
    assert_eq!(journal.count, count);
    assert_eq!(journal.pending_count()?, count);
    assert_eq!(journal.unresolved_count()?, 0);
    assert_eq!(
        journal
            .recent_orders(polym_rust_demo::now_ms() - 10_800_000)?
            .len(),
        1
    );
    let batch = journal.pending_batch(true)?;
    assert_eq!(batch.len(), count.min(25));
    journal.ack_history(&batch[0].signal.id)?;
    assert_eq!(journal.pending_count()?, count - 1);
    let mut next = last.as_ref().unwrap().clone();
    next.signal.id = "live-new-after-capacity".into();
    next.reply.id = next.signal.id.clone();
    if count == polym_rust_demo::journal::ORDER_CAPACITY {
        assert!(journal.prepare(next, None).is_err());
    }
    let replay = journal.prepare(last.unwrap(), None)?.unwrap();
    assert_eq!(replay.state, "accepted");
    println!("recovered {count} legacy orders in {:?}", started.elapsed());
    drop(journal);
    let recovered = Journal::open(&path, "live:synthetic")?;
    assert_eq!(recovered.exported_count, 1);
    assert_eq!(recovered.pending_count()?, count - 1);
    drop(recovered);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
