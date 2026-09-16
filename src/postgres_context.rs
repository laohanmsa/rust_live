//! Bounded, read-only PostgreSQL context reads; order history uses the existing API.
use crate::shadow_state::{MarketContext, Policy};
use anyhow::{Context, Result, ensure};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio_postgres::NoTls;

pub const QUERY: &str = include_str!("../sql/market_context.sql");
const READY_QUERY: &str = "SELECT current_setting('default_transaction_read_only')='on' AND current_setting('plan_cache_mode')='force_generic_plan'";

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
            .options("-c default_transaction_read_only=on -c statement_timeout=750 -c idle_in_transaction_session_timeout=2000 -c plan_cache_mode=force_generic_plan");
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

    /// Hold all pool slots while preparing them, so a burst does not open cold connections.
    pub async fn warm_up(&self) -> Result<()> {
        let clients = futures_util::future::try_join_all(
            (0..self.pool.status().max_size).map(|_| self.pool.get()),
        )
        .await?;
        futures_util::future::try_join_all(clients.iter().map(|client| async move {
            let query = client.prepare_cached(QUERY).await?;
            // Execute an empty-key read to build the generic plan without loading market data.
            let _ = client.query_opt(&query, &[&""]).await?;
            let health = client.prepare_cached(READY_QUERY).await?;
            ensure!(
                client.query_one(&health, &[]).await?.get::<_, bool>(0),
                "context connection must be read-only with a reusable plan"
            );
            Ok::<_, anyhow::Error>(())
        }))
        .await?;
        Ok(())
    }

    /// Report connection reuse without exposing credentials or account state.
    pub fn pool_status(&self) -> Value {
        let status = self.pool.status();
        json!({"size":status.size,"available":status.available,"waiting":status.waiting,"max_size":status.max_size})
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
        let Ok(query) = client.prepare_cached(READY_QUERY).await else {
            return false;
        };
        matches!(client.query_one(&query,&[]).await, Ok(row) if row.get::<_,bool>(0))
    }
}
