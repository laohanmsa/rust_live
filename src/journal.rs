use crate::{Reply, Signal};
use anyhow::{Context, Result, ensure};
use polymarket_client_sdk_v2::types::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

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
pub struct Journal {
    file: File,
    history_file: File,
    pub orders: HashMap<String, Stored>,
    pub used: Decimal,
    pub exported: HashSet<String>,
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
        let mut orders = HashMap::new();
        let mut used = Decimal::ZERO;
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
            ensure!(line <= 100001, "prototype journal limit reached");
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
                    if entry.reply.state == "prepared" {
                        entry.reply.state = "unknown".into();
                        entry.reply.reason = "recovered_unconfirmed_submission".into();
                    }
                    ensure!(
                        has_scope && !orders.contains_key(&entry.signal.id),
                        "invalid duplicate preparation"
                    );
                    used += entry.reserved;
                    orders.insert(entry.signal.id.clone(), entry);
                }
                Record::Result { reply } => {
                    let stored = orders
                        .get_mut(&reply.id)
                        .context("result without preparation")?;
                    stored.reply = reply;
                }
            }
        }
        if !has_scope {
            serde_json::to_writer(
                &mut file,
                &Record::Scope {
                    scope: scope.into(),
                },
            )?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            if let Some(p) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                File::open(p)?.sync_all()?;
            }
        }
        // Keep acknowledgement records separate so the previous binary can roll back safely.
        let history_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .mode(0o600)
            .open(path.with_extension("history-acks.jsonl"))?;
        let mut exported = HashSet::new();
        for line in BufReader::new(&history_file).lines() {
            if let Ok(id) = serde_json::from_str::<String>(&line?) {
                ensure!(
                    orders.contains_key(&id),
                    "history acknowledgement without order"
                );
                exported.insert(id);
            }
            // A torn acknowledgement is retried using the server's unique signal ID.
        }
        Ok(Self {
            file,
            history_file,
            orders,
            used,
            exported,
            failed: false,
        })
    }
    fn append(&mut self, r: &Record) -> Result<()> {
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
        Ok(())
    }
    pub fn prepare(&mut self, entry: Stored, budget: Option<Decimal>) -> Result<Option<Reply>> {
        ensure!(!self.failed, "journal is poisoned");
        if let Some(old) = self.orders.get(&entry.signal.id) {
            ensure!(old.signal == entry.signal, "id_conflict");
            return Ok(Some(old.reply.clone()));
        }
        ensure!(self.orders.len() < 50000, "journal_capacity");
        ensure!(
            budget.is_none_or(|limit| self.used + entry.reserved <= limit),
            "budget_exhausted"
        );
        self.append(&Record::Prepared {
            entry: entry.clone(),
        })?;
        self.used += entry.reserved;
        self.orders.insert(entry.signal.id.clone(), entry);
        Ok(None)
    }
    pub fn finish(&mut self, reply: Reply) -> Result<()> {
        self.append(&Record::Result {
            reply: reply.clone(),
        })?;
        let stored = self
            .orders
            .get_mut(&reply.id)
            .context("missing prepared order")?;
        stored.reply = reply;
        Ok(())
    }
    pub fn ack_history(&mut self, id: &str) -> Result<()> {
        ensure!(self.orders.contains_key(id), "missing history order");
        if !self.exported.contains(id) {
            let mut record = serde_json::to_vec(id)?;
            record.push(b'\n');
            self.history_file.write_all(&record)?;
            self.history_file.sync_all()?;
            self.exported.insert(id.into());
        }
        Ok(())
    }
}
