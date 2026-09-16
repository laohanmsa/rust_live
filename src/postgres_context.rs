//! Bounded, read-only PostgreSQL context reads; order history uses the existing API.
use crate::shadow_state::{MarketContext, Policy};
use anyhow::{Context, Result, ensure};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::Deserialize;
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio_postgres::NoTls;

pub const QUERY: &str = include_str!("../sql/market_context.sql");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    host: String,
    port: u16,
    dbname: String,
    user: String,
    password: String,
}

pub struct PostgresContext {
    pool: Pool,
}
impl PostgresContext {
    /// Read a protected runtime secret; all connections default to read-only statements.
    pub fn read(path: &Path, size: usize) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
            "database secret must be private"
        );
        let credentials: Credentials = serde_json::from_slice(&std::fs::read(path)?)?;
        let mut config = tokio_postgres::Config::new();
        config.host(&credentials.host).port(credentials.port).dbname(&credentials.dbname)
            .user(&credentials.user).password(&credentials.password)
            .application_name("rust_uma_reader").connect_timeout(Duration::from_secs(2))
            .options("-c default_transaction_read_only=on -c statement_timeout=750 -c idle_in_transaction_session_timeout=2000");
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(size)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_millis(750)))
            .create_timeout(Some(Duration::from_secs(2)))
            .build()?;
        Ok(Self { pool })
    }

    pub async fn fetch(&self, id: &str) -> Result<(Vec<MarketContext>, Option<Policy>)> {
        let client = self
            .pool
            .get()
            .await
            .context("context connection unavailable")?;
        let statement = client
            .prepare_cached(QUERY)
            .await
            .context("context statement unavailable")?;
        let Some(row) = client
            .query_opt(&statement, &[&id])
            .await
            .context("context query failed")?
        else {
            return Ok((Vec::new(), None));
        };
        let context: MarketContext = serde_json::from_value(row.get::<_, Value>(0))?;
        let policy: Option<Policy> = row
            .get::<_, Option<Value>>(1)
            .map(serde_json::from_value)
            .transpose()?;
        Ok((vec![context], policy))
    }

    pub async fn ready(&self) -> bool {
        let Ok(client) = self.pool.get().await else {
            return false;
        };
        matches!(client.query_one("SELECT current_setting('default_transaction_read_only')",&[]).await,
            Ok(row) if row.get::<_,String>(0)=="on")
    }
}
