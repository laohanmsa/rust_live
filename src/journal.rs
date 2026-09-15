use crate::{Reply, Signal};
use anyhow::{Context, Result, ensure};
use polymarket_client_sdk_v2::types::Decimal;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

pub const ORDER_CAPACITY: usize = 1_000_000;

#[derive(Clone, Serialize, Deserialize)]
pub struct Stored {
    pub signal: Signal,
    pub reserved: Decimal,
    pub order: Value,
    pub reply: Reply,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind")]
enum Record {
    Scope { scope: String },
    Prepared { entry: Stored },
    Result { reply: Reply },
}

/// The append-only files remain authoritative. SQLite only indexes their offsets.
/// Rebuild the disposable index at startup so a crash cannot hide a prepared order.
pub struct Journal {
    file: File,
    history_file: File,
    index: Connection,
    length: u64,
    pub used: Decimal,
    pub count: usize,
    pub exported_count: usize,
    failed: bool,
}
impl Journal {
    pub fn open(path: &Path, scope: &str) -> Result<Self> {
        if let Some(p) = path.parent().filter(|p| !p.as_os_str().is_empty())
            && !p.exists()
        {
            std::fs::create_dir_all(p)?;
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .mode(0o600)
            .open(path)?;
        file.try_lock()
            .context("journal is already open by another process")?;
        ensure!(
            file.metadata()?.permissions().mode() & 0o077 == 0,
            "journal permissions must be 0600"
        );
        let index_path = path.with_extension("index.sqlite");
        // Only this derived index is replaced; never truncate either authoritative log.
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&index_path)?;
        index_file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        drop(index_file);
        let index = Connection::open(index_path)?;
        index.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-8192; PRAGMA mmap_size=0; PRAGMA temp_store=FILE;
            CREATE TABLE orders (id TEXT PRIMARY KEY, prep_offset INTEGER NOT NULL, prep_len INTEGER NOT NULL,
                result_offset INTEGER, result_len INTEGER, state TEXT NOT NULL, token TEXT NOT NULL,
                observed INTEGER NOT NULL, exported INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0);
            BEGIN;")?;
        let mut used = Decimal::ZERO;
        let mut count = 0;
        let mut length = 0;
        let mut has_scope = false;
        let mut reader = BufReader::new(&file);
        let mut buf = String::new();
        let mut line = 0;
        loop {
            buf.clear();
            if reader.read_line(&mut buf)? == 0 {
                break;
            }
            line += 1;
            ensure!(
                line <= 2 * ORDER_CAPACITY + 1,
                "journal record capacity reached"
            );
            ensure!(
                buf.ends_with('\n'),
                "incomplete journal record; inspect before restarting"
            );
            let record: Record =
                serde_json::from_str(&buf).context("invalid journal record; refuse recovery")?;
            match record {
                Record::Scope { scope: stored } => {
                    ensure!(
                        !has_scope && line == 1 && stored == scope,
                        "journal account or mode mismatch"
                    );
                    has_scope = true;
                }
                Record::Prepared { mut entry } => {
                    ensure!(has_scope && count < ORDER_CAPACITY, "journal_capacity");
                    if entry.reply.state == "prepared" {
                        entry.reply.state = "unknown".into();
                    }
                    Self::index_prepared(&index, &entry, length, buf.len())?;
                    used += entry.reserved;
                    count += 1;
                }
                Record::Result { reply } => {
                    ensure!(has_scope, "result without scope");
                    Self::index_result(&index, &reply, length, buf.len())?;
                }
            }
            length += buf.len() as u64;
        }
        if !has_scope {
            let bytes = serde_json::to_vec(&Record::Scope {
                scope: scope.into(),
            })?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            length = bytes.len() as u64 + 1;
            if let Some(p) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                File::open(p)?.sync_all()?;
            }
        }
        let history_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .mode(0o600)
            .open(path.with_extension("history-acks.jsonl"))?;
        let mut exported_count = 0;
        for line in BufReader::new(&history_file).lines() {
            if let Ok(id) = serde_json::from_str::<String>(&line?) {
                let old: Option<bool> = index
                    .query_row("SELECT exported FROM orders WHERE id=?1", [&id], |r| {
                        r.get(0)
                    })
                    .optional()?;
                ensure!(old.is_some(), "history acknowledgement without order");
                if old == Some(false) {
                    index.execute("UPDATE orders SET exported=1,pending=0 WHERE id=?1", [&id])?;
                    exported_count += 1;
                }
            }
            // Preserve legacy recovery: a torn acknowledgement is retried by unique ID.
        }
        index.execute_batch(
            "CREATE INDEX pending_orders ON orders(id) WHERE pending=1;
            CREATE INDEX unresolved_orders ON orders(state) WHERE state IN ('unknown','prepared');
            CREATE INDEX recent_orders ON orders(observed); COMMIT;",
        )?;
        Ok(Self {
            file,
            history_file,
            index,
            length,
            used,
            count,
            exported_count,
            failed: false,
        })
    }
    fn pending(id: &str, state: &str) -> bool {
        (id.starts_with("live-") && matches!(state, "accepted" | "unknown" | "rejected"))
            || (id.starts_with("shadow-") && matches!(state, "accepted" | "unknown"))
    }
    fn index_prepared(
        index: &Connection,
        entry: &Stored,
        offset: u64,
        length: usize,
    ) -> Result<()> {
        ensure!(
            entry.signal.id == entry.reply.id && entry.reserved > Decimal::ZERO,
            "invalid preparation"
        );
        index.prepare_cached("INSERT INTO orders(id,prep_offset,prep_len,state,token,observed,pending) VALUES (?1,?2,?3,?4,?5,?6,?7)")?
            .execute(params![entry.signal.id,i64::try_from(offset)?,i64::try_from(length)?,entry.reply.state,entry.signal.token_id.to_string(),i64::try_from(entry.signal.observed_at_ms)?,
                Self::pending(&entry.signal.id,&entry.reply.state)])?;
        Ok(())
    }
    fn index_result(index: &Connection, reply: &Reply, offset: u64, length: usize) -> Result<()> {
        let changed = index.prepare_cached("UPDATE orders SET result_offset=?2,result_len=?3,state=?4,pending=(?5 AND NOT exported) WHERE id=?1 AND result_offset IS NULL")?
            .execute(params![reply.id,i64::try_from(offset)?,i64::try_from(length)?,reply.state,Self::pending(&reply.id,&reply.state)])?;
        ensure!(
            changed == 1,
            "result without preparation or duplicate final result"
        );
        Ok(())
    }
    fn record(&self, offset: u64, length: usize) -> Result<Record> {
        let mut bytes = vec![0; length];
        self.file.read_exact_at(&mut bytes, offset)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Fetch only the requested order's records instead of retaining every payload in RAM.
    pub fn get(&self, id: &str) -> Result<Option<Stored>> {
        let row = self.index.query_row("SELECT prep_offset,prep_len,result_offset,result_len,state FROM orders WHERE id=?1",[id],|r|
            Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<i64>>(2)?,r.get::<_,Option<i64>>(3)?,r.get::<_,String>(4)?))).optional()?;
        let Some((offset, length, result_offset, result_len, state)) = row else {
            return Ok(None);
        };
        let Record::Prepared { mut entry } = self.record(offset.try_into()?, length.try_into()?)?
        else {
            anyhow::bail!("invalid prepared index")
        };
        ensure!(entry.signal.id == id, "journal index identity mismatch");
        if let (Some(offset), Some(length)) = (result_offset, result_len) {
            let Record::Result { reply } = self.record(offset.try_into()?, length.try_into()?)?
            else {
                anyhow::bail!("invalid result index")
            };
            ensure!(reply.id == id, "journal result identity mismatch");
            entry.reply = reply;
        } else if state == "unknown" {
            entry.reply.state = "unknown".into();
            entry.reply.reason = "recovered_unconfirmed_submission".into();
        }
        Ok(Some(entry))
    }
    /// Indexed, bounded history batch. Call on a blocking worker.
    pub fn pending_batch(&self, live: bool) -> Result<Vec<Stored>> {
        let prefix = if live { "live-" } else { "shadow-" };
        let mut query = self.index.prepare(
            "SELECT id FROM orders WHERE pending=1 AND substr(id,1,?1)=?2 ORDER BY id LIMIT 25",
        )?;
        let ids = query
            .query_map(params![i64::try_from(prefix.len())?, prefix], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter()
            .map(|id| self.get(id)?.context("missing pending order"))
            .collect()
    }
    pub fn pending_count(&self) -> Result<usize> {
        let count: i64 =
            self.index
                .query_row("SELECT count(*) FROM orders WHERE pending=1", [], |r| {
                    r.get(0)
                })?;
        Ok(count.try_into()?)
    }
    pub fn unresolved_count(&self) -> Result<usize> {
        let count: i64 = self.index.query_row(
            "SELECT count(*) FROM orders WHERE state IN ('unknown','prepared')",
            [],
            |r| r.get(0),
        )?;
        Ok(count.try_into()?)
    }
    pub fn recent_orders(&self, cutoff: u64) -> Result<Vec<(String, u64)>> {
        let mut query = self
            .index
            .prepare("SELECT token,observed FROM orders WHERE observed>=?1 ORDER BY observed")?;
        let rows = query.query_map([i64::try_from(cutoff)?], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        rows.map(|row| {
            let (token, at) = row?;
            Ok((token, at.try_into()?))
        })
        .collect()
    }
    fn append(&mut self, r: &Record) -> Result<(u64, usize)> {
        ensure!(!self.failed, "journal is poisoned");
        let mut bytes = serde_json::to_vec(r)?;
        bytes.push(b'\n');
        if let Err(e) = self
            .file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_all())
        {
            self.failed = true;
            return Err(e.into());
        }
        let offset = self.length;
        self.length += bytes.len() as u64;
        Ok((offset, bytes.len()))
    }
    pub fn prepare(&mut self, entry: Stored, budget: Option<Decimal>) -> Result<Option<Reply>> {
        ensure!(!self.failed, "journal is poisoned");
        if let Some(old) = self.get(&entry.signal.id)? {
            ensure!(old.signal == entry.signal, "id_conflict");
            return Ok(Some(old.reply));
        }
        ensure!(
            entry.signal.id == entry.reply.id && entry.reserved > Decimal::ZERO,
            "invalid preparation"
        );
        ensure!(
            entry.signal.observed_at_ms <= i64::MAX as u64,
            "invalid preparation time"
        );
        ensure!(self.count < ORDER_CAPACITY, "journal_capacity");
        ensure!(
            budget.is_none_or(|limit| self.used + entry.reserved <= limit),
            "budget_exhausted"
        );
        let (offset, length) = self.append(&Record::Prepared {
            entry: entry.clone(),
        })?;
        if let Err(e) = Self::index_prepared(&self.index, &entry, offset, length) {
            self.failed = true;
            return Err(e);
        }
        self.used += entry.reserved;
        self.count += 1;
        Ok(None)
    }
    pub fn finish(&mut self, reply: Reply) -> Result<()> {
        let unfinished: Option<bool> = self
            .index
            .query_row(
                "SELECT result_offset IS NULL FROM orders WHERE id=?1",
                [&reply.id],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(
            unfinished == Some(true),
            "result without preparation or duplicate final result"
        );
        let (offset, length) = self.append(&Record::Result {
            reply: reply.clone(),
        })?;
        if let Err(e) = Self::index_result(&self.index, &reply, offset, length) {
            self.failed = true;
            return Err(e);
        }
        Ok(())
    }
    pub fn ack_history(&mut self, id: &str) -> Result<()> {
        ensure!(!self.failed, "journal is poisoned");
        let exported: Option<bool> = self
            .index
            .query_row("SELECT exported FROM orders WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(exported.is_some(), "missing history order");
        if exported == Some(false) {
            let mut record = serde_json::to_vec(id)?;
            record.push(b'\n');
            self.history_file.write_all(&record)?;
            self.history_file.sync_all()?;
            self.index
                .execute("UPDATE orders SET exported=1,pending=0 WHERE id=?1", [id])?;
            self.exported_count += 1;
        }
        Ok(())
    }
}
