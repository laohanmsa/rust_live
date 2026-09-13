use crate::{Signal, now_ms};
use alloy::{
    signers::local::PrivateKeySigner,
    sol_types::{SolStruct, eip712_domain},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use hmac::{Hmac, Mac};
use polymarket_client_sdk_v2::{
    POLYGON,
    auth::{Credentials, ExposeSecret, Normal, Signer, Uuid, state::Authenticated},
    clob::{
        Client, Config,
        types::{Amount, OrderPayload, OrderType, Side, SignatureType},
    },
    contract_config,
    types::{Address, Decimal, U256},
};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, Instant},
};

pub struct Market {
    pub tick: Decimal,
    pub min_size: Decimal,
    pub neg_risk: bool,
    pub loaded: Instant,
}
pub struct Signed {
    pub wire: Vec<u8>,
    pub journal_order: Value,
    pub hash: String,
}
pub struct Exchange {
    pub client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    credentials: Credentials,
    http: reqwest::Client,
    mode: &'static str,
    funder: Option<Address>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCredentials {
    pub account_name: String,
    pub account_id: u64,
    pub signer_address: Address,
    pub funder: Address,
    pub private_key: String,
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    pub access_token: String,
}
impl LiveCredentials {
    pub fn read(path: &Path, account: &str) -> Result<Self> {
        ensure!(
            std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
            "credentials must not be group/world readable"
        );
        let value: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            value.account_name == account && value.account_id > 0 && value.access_token.len() >= 32,
            "live account authorization mismatch"
        );
        Ok(value)
    }
}
impl Exchange {
    pub async fn authorized_live(c: &LiveCredentials) -> Result<Self> {
        let exchange = Self::connect(
            "https://clob.polymarket.com",
            "live",
            &c.private_key,
            Credentials::new(
                c.api_key.parse().context("invalid API key format")?,
                c.api_secret.clone(),
                c.api_passphrase.clone(),
            ),
            SignatureType::GnosisSafe,
            Some(c.funder),
        )
        .await?;
        ensure!(
            exchange.signer.address() == c.signer_address,
            "private key does not match selected account"
        );
        ensure!(
            tokio::time::timeout(Duration::from_secs(5), exchange.client.version()).await?? == 2,
            "CLOB v2 required"
        );
        tokio::time::timeout(Duration::from_secs(5), exchange.client.api_keys()).await??;
        Ok(exchange)
    }
    pub async fn demo(host: &str) -> Result<Self> {
        let url: reqwest::Url = host.parse()?;
        ensure!(
            url.scheme() == "http" && url.host_str() == Some("127.0.0.1"),
            "demo transport must be loopback"
        );
        Self::connect(
            host,
            "demo",
            crate::demo::KEY,
            Credentials::new(
                Uuid::nil(),
                crate::demo::SECRET.into(),
                "demo-passphrase".into(),
            ),
            SignatureType::Eoa,
            None,
        )
        .await
    }
    pub async fn shadow(host: &str) -> Result<Self> {
        let mut exchange = Self::demo(host).await?;
        exchange.mode = "shadow";
        ensure!(
            exchange.client.version().await? == 2,
            "shadow mock must use V2"
        );
        Ok(exchange)
    }
    pub async fn sign_shadow(
        &self,
        signal: &Signal,
        shares: Decimal,
        tick: Decimal,
        neg_risk: bool,
    ) -> Result<Signed> {
        ensure!(
            matches!(self.mode, "shadow" | "live"),
            "cached signing requires an initialized trading exchange"
        );
        self.client.set_tick_size(signal.token_id, tick.try_into()?);
        self.client.set_neg_risk(signal.token_id, neg_risk);
        let order = self
            .client
            .limit_order()
            .token_id(signal.token_id)
            .side(Side::Buy)
            .order_type(OrderType::FAK)
            .price(signal.ask)
            .size(shares)
            .build()
            .await?;
        let OrderPayload::V2(payload) = &order.payload else {
            anyhow::bail!("unexpected protocol")
        };
        let domain = eip712_domain! {name:"Polymarket CTF Exchange",version:"2",chain_id:POLYGON,verifying_contract:contract_config(POLYGON,neg_risk).and_then(|c|c.exchange_v2).context("missing exchange contract")?,};
        let hash = payload.order.eip712_signing_hash(&domain).to_string();
        let signed = self.client.sign(&self.signer, order).await?;
        Ok(Signed {
            wire: serde_json::to_vec(&signed)?,
            journal_order: serde_json::to_value(&signed)?["order"].clone(),
            hash,
        })
    }
    pub async fn live() -> Result<Self> {
        fn env(k: &str) -> Result<String> {
            std::env::var(k).with_context(|| format!("missing {k}"))
        }
        let credentials = Credentials::new(
            env("POLY_API_KEY")?
                .parse()
                .context("invalid API key format")?,
            env("POLY_API_SECRET")?,
            env("POLY_API_PASSPHRASE")?,
        );
        let signature_type = match env("POLY_SIGNATURE_TYPE")?.as_str() {
            "0" => SignatureType::Eoa,
            "1" => SignatureType::Proxy,
            "2" => SignatureType::GnosisSafe,
            _ => anyhow::bail!("prototype supports signature types 0, 1 and 2 only"),
        };
        let funder = if signature_type == SignatureType::Eoa {
            None
        } else {
            Some(env("POLY_FUNDER")?.parse().context("invalid funder")?)
        };
        Self::connect(
            "https://clob.polymarket.com",
            "live",
            &env("POLY_PRIVATE_KEY")?,
            credentials,
            signature_type,
            funder,
        )
        .await
    }
    async fn connect(
        host: &str,
        mode: &'static str,
        key: &str,
        credentials: Credentials,
        signature_type: SignatureType,
        funder: Option<Address>,
    ) -> Result<Self> {
        ensure!(
            mode != "live"
                || (key.trim_start_matches("0x") != crate::demo::KEY
                    && credentials.key() != Uuid::nil()),
            "demo credentials cannot be used in live mode"
        );
        let signer = PrivateKeySigner::from_str(key)
            .map_err(|_| anyhow::anyhow!("invalid private key"))?
            .with_chain_id(Some(POLYGON));
        ensure!(
            !credentials.secret().expose_secret().is_empty(),
            "empty API secret"
        );
        URL_SAFE
            .decode(credentials.secret().expose_secret())
            .context("invalid API secret encoding")?;
        let mut builder = Client::new(host, Config::default())?
            .authentication_builder(&signer)
            .credentials(credentials.clone())
            .signature_type(signature_type);
        if let Some(funder) = funder {
            builder = builder.funder(funder)
        }
        let client = builder
            .authenticate()
            .await
            .context("client initialization failed")?;
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()?;
        Ok(Self {
            client,
            signer,
            credentials,
            http,
            mode,
            funder,
        })
    }
    pub fn mode(&self) -> &'static str {
        self.mode
    }
    pub fn scope(&self) -> String {
        format!(
            "{}:{}:{}",
            self.mode,
            self.signer.address(),
            self.funder.unwrap_or(self.signer.address())
        )
    }
    pub async fn warm(&self, tokens: &[U256]) -> Result<HashMap<U256, Market>> {
        ensure!(
            tokio::time::timeout(Duration::from_secs(5), self.client.version()).await?? == 2,
            "only token-backed exchange protocol V2 is supported"
        );
        let mut markets = HashMap::new();
        for token in tokens {
            let m =
                tokio::time::timeout(Duration::from_secs(5), self.client.market_by_token(*token))
                    .await??;
            let info = tokio::time::timeout(
                Duration::from_secs(5),
                self.client.clob_market_info(&m.condition_id.to_string()),
            )
            .await??;
            ensure!(
                info.tokens.iter().flatten().any(|t| t.token_id == *token),
                "token absent from market metadata"
            );
            ensure!(info.fee_details.is_some(), "missing fee information");
            markets.insert(
                *token,
                Market {
                    tick: info.min_tick_size.as_decimal(),
                    min_size: info.min_order_size,
                    neg_risk: info.neg_risk,
                    loaded: Instant::now(),
                },
            );
        }
        Ok(markets)
    }
    pub async fn sign(&self, s: &Signal, budget: Decimal, market: &Market) -> Result<Signed> {
        let order = self
            .client
            .market_order()
            .token_id(s.token_id)
            .side(Side::Buy)
            .order_type(OrderType::FAK)
            .price(s.ask)
            .amount(Amount::usdc(budget)?)
            .user_usdc_balance(budget)
            .build()
            .await?;
        let OrderPayload::V2(payload) = &order.payload else {
            anyhow::bail!("unexpected protocol")
        };
        ensure!(
            Decimal::from_str(&payload.order.takerAmount.to_string())? / Decimal::from(1_000_000)
                >= market.min_size,
            "below_market_minimum_size"
        );
        let domain = eip712_domain! {name:"Polymarket CTF Exchange",version:"2",chain_id:POLYGON,verifying_contract:contract_config(POLYGON,market.neg_risk).and_then(|c|c.exchange_v2).context("missing exchange contract")?,};
        let hash = payload.order.eip712_signing_hash(&domain).to_string();
        let signed = self.client.sign(&self.signer, order).await?;
        let wire = serde_json::to_vec(&signed)?;
        // Store signed economic terms, not the API key in the outer SDK owner field.
        let journal_order = serde_json::to_value(&signed)?["order"].clone();
        Ok(Signed {
            wire,
            journal_order,
            hash,
        })
    }
    pub async fn post(&self, signed: Signed) -> Result<(u16, Value)> {
        let timestamp = (now_ms() / 1000).to_string();
        let signature = auth_signature(
            self.credentials.secret().expose_secret(),
            &timestamp,
            "POST",
            "/order",
            &signed.wire,
        )?;
        let mut headers = HeaderMap::new();
        for (key, val) in [
            ("POLY_ADDRESS", self.signer.address().to_string()),
            ("POLY_API_KEY", self.credentials.key().to_string()),
            (
                "POLY_PASSPHRASE",
                self.credentials.passphrase().expose_secret().into(),
            ),
            ("POLY_SIGNATURE", signature),
            ("POLY_TIMESTAMP", timestamp),
        ] {
            let mut v = HeaderValue::from_str(&val)
                .map_err(|_| anyhow::anyhow!("invalid authentication header"))?;
            v.set_sensitive(true);
            headers.insert(reqwest::header::HeaderName::from_bytes(key.as_bytes())?, v);
        }
        // SDK post_order also waits for trade settlement hashes. Submit exactly once here.
        let mut response = self
            .http
            .post(format!("{}order", self.client.host()))
            .headers(headers)
            .header("content-type", "application/json")
            .body(signed.wire)
            .send()
            .await?;
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(body.len() + chunk.len() <= 65536, "oversized response");
            body.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
        Ok((status, value))
    }
}
pub fn auth_signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(&URL_SAFE.decode(secret)?)?;
    mac.update(format!("{timestamp}{method}{path}").as_bytes());
    mac.update(body);
    Ok(URL_SAFE.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod live_tests {
    use super::*;
    #[tokio::test]
    async fn proxy_signing_and_single_submit_use_the_configured_funder() -> Result<()> {
        let mock = crate::demo::MockExchange::start().await?;
        let funder: Address = "0x3434343434343434343434343434343434343434".parse()?;
        let exchange = Exchange::connect(
            &mock.url,
            "shadow",
            crate::demo::KEY,
            Credentials::new(
                Uuid::nil(),
                crate::demo::SECRET.into(),
                "demo-passphrase".into(),
            ),
            SignatureType::GnosisSafe,
            Some(funder),
        )
        .await?;
        let signed = exchange
            .sign_shadow(
                &crate::demo::signal("proxy"),
                "10".parse()?,
                "0.01".parse()?,
                false,
            )
            .await?;
        assert_eq!(signed.journal_order["maker"], funder.to_string());
        assert_eq!(signed.journal_order["signatureType"], 2);
        let (status, body) = exchange.post(signed).await?;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["success"], true);
        Ok(())
    }
}
