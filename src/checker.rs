use crate::checkpoint::DomainStatus;
use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::{
    fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
    Identity, ProviderBuilder, RootProvider,
};
use alloy::sol;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use reqwest::Client;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

pub const ETH_REGISTRAR_CONTROLLER: &str = "0x253553366Da8546fC250F225fe3d25d0C782303b";
pub const PUBLIC_ETH_RPC: &str = "https://ethereum-rpc.publicnode.com";
pub const RDAP_BOX_ENDPOINT: &str = "https://rdap.centralnic.com/box/domain";
pub const RDAP_ID_ENDPOINT: &str = "https://rdap.pandi.id/rdap/domain";

pub const DEFAULT_MAX_RETRIES: u32 = 5;
pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 500;
pub const DEFAULT_MAX_BACKOFF_SECS: u64 = 30;

#[derive(Error, Debug)]
pub enum CheckError {
    #[error("Rate limited: {0}")]
    RateLimited(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("HTTP error: {status} - {message}")]
    Http { status: u16, message: String },

    #[error("Other error: {0}")]
    Other(String),
}

impl CheckError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            CheckError::RateLimited(_)
                | CheckError::Timeout(_)
                | CheckError::Connection(_)
                | CheckError::Http {
                    status: 429 | 500..=599,
                    ..
                }
        )
    }
}

#[derive(Debug)]
pub enum CheckResult {
    Available,
    Taken,
    Error(CheckError),
}

impl CheckResult {
    pub fn to_status(&self) -> DomainStatus {
        match self {
            CheckResult::Available => DomainStatus::Available,
            CheckResult::Taken => DomainStatus::Taken,
            CheckResult::Error(_) => DomainStatus::Error,
        }
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            CheckResult::Error(e) => Some(e.to_string()),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            CheckResult::Error(e) => e.is_retryable(),
            _ => false,
        }
    }
}

pub type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub struct DualRateLimiter {
    pub eth: Arc<Limiter>,
    pub box_domain: Arc<Limiter>,
    pub id_domain: Arc<Limiter>,
}

impl DualRateLimiter {
    pub fn new(eth_rps: u32, box_rps: u32, id_rps: u32) -> Self {
        let eth_quota = Quota::per_second(NonZeroU32::new(eth_rps.max(1)).unwrap());
        let box_quota = Quota::per_second(NonZeroU32::new(box_rps.max(1)).unwrap());
        let id_quota = Quota::per_second(NonZeroU32::new(id_rps.max(1)).unwrap());

        Self {
            eth: Arc::new(RateLimiter::direct(eth_quota)),
            box_domain: Arc::new(RateLimiter::direct(box_quota)),
            id_domain: Arc::new(RateLimiter::direct(id_quota)),
        }
    }

    pub async fn wait_eth(&self) {
        self.eth.until_ready().await;
    }

    pub async fn wait_box(&self) {
        self.box_domain.until_ready().await;
    }

    pub async fn wait_id(&self) {
        self.id_domain.until_ready().await;
    }
}

pub fn calculate_backoff(attempt: u32, base_ms: u64, max_secs: u64) -> Duration {
    use rand::Rng;

    let exponential_ms = base_ms.saturating_mul(1u64 << attempt.min(10));
    let max_ms = max_secs * 1000;
    let capped_ms = exponential_ms.min(max_ms);

    let jittered_ms = if capped_ms > 0 {
        rand::thread_rng().gen_range(0..=capped_ms)
    } else {
        0
    };

    Duration::from_millis(jittered_ms)
}

sol! {
    #[sol(rpc)]
    interface IETHRegistrarController {
        function available(string memory name) external view returns (bool);
    }
}

type EthHttpProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider<Ethereum>,
    Ethereum,
>;

pub struct EnsChecker {
    provider: Arc<EthHttpProvider>,
    controller_address: Address,
}

impl EnsChecker {
    pub async fn new() -> Result<Self, CheckError> {
        Self::with_rpc(PUBLIC_ETH_RPC).await
    }

    pub async fn with_rpc(rpc_url: &str) -> Result<Self, CheckError> {
        let url = rpc_url
            .parse()
            .map_err(|e| CheckError::Other(format!("Invalid RPC URL: {}", e)))?;

        let provider = ProviderBuilder::new().connect_http(url);

        let controller_address = Address::from_str(ETH_REGISTRAR_CONTROLLER)
            .map_err(|e| CheckError::Other(format!("Invalid controller address: {}", e)))?;

        Ok(Self {
            provider: Arc::new(provider),
            controller_address,
        })
    }

    pub async fn check_available(&self, name: &str) -> CheckResult {
        let contract =
            IETHRegistrarController::new(self.controller_address, self.provider.clone());

        match tokio::time::timeout(
            Duration::from_secs(30),
            contract.available(name.to_string()).call(),
        )
        .await
        {
            Ok(Ok(is_available)) => {
                if is_available {
                    CheckResult::Available
                } else {
                    CheckResult::Taken
                }
            }
            Ok(Err(e)) => {
                let error_str = e.to_string().to_lowercase();
                if error_str.contains("rate limit") || error_str.contains("too many requests") {
                    CheckResult::Error(CheckError::RateLimited(e.to_string()))
                } else if error_str.contains("timeout") {
                    CheckResult::Error(CheckError::Timeout(e.to_string()))
                } else if error_str.contains("connection") {
                    CheckResult::Error(CheckError::Connection(e.to_string()))
                } else {
                    CheckResult::Error(CheckError::Rpc(e.to_string()))
                }
            }
            Err(_) => CheckResult::Error(CheckError::Timeout(
                "Request timed out after 30s".to_string(),
            )),
        }
    }
}

pub struct RdapChecker {
    client: Client,
    base_url: String,
}

impl RdapChecker {
    pub fn new() -> Result<Self, CheckError> {
        Self::with_endpoint(RDAP_BOX_ENDPOINT)
    }

    pub fn with_endpoint(endpoint: &str) -> Result<Self, CheckError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .map_err(|e| CheckError::Other(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url: endpoint.to_string(),
        })
    }

    pub async fn check_available(&self, name: &str) -> CheckResult {
        let url = format!("{}/{}.box", self.base_url, name);

        match self.client.get(&url).send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                match status {
                    404 => CheckResult::Available,
                    200 => CheckResult::Taken,
                    429 => CheckResult::Error(CheckError::RateLimited(
                        "Rate limited by RDAP server".to_string(),
                    )),
                    500..=599 => CheckResult::Error(CheckError::Http {
                        status,
                        message: "Server error".to_string(),
                    }),
                    _ => CheckResult::Error(CheckError::Http {
                        status,
                        message: format!("Unexpected status code: {}", status),
                    }),
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    CheckResult::Error(CheckError::Timeout(e.to_string()))
                } else if e.is_connect() {
                    CheckResult::Error(CheckError::Connection(e.to_string()))
                } else {
                    CheckResult::Error(CheckError::Other(e.to_string()))
                }
            }
        }
    }
}

impl Default for RdapChecker {
    fn default() -> Self {
        Self::new().expect("Failed to create default RdapChecker")
    }
}

pub struct IdChecker {
    client: Client,
    base_url: String,
}

impl IdChecker {
    pub fn new() -> Result<Self, CheckError> {
        Self::with_endpoint(RDAP_ID_ENDPOINT)
    }

    pub fn with_endpoint(endpoint: &str) -> Result<Self, CheckError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .map_err(|e| CheckError::Other(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url: endpoint.to_string(),
        })
    }

    pub async fn check_available(&self, name: &str) -> CheckResult {
        let url = format!("{}/{}.id", self.base_url, name);

        match self.client.get(&url).send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                match status {
                    404 => CheckResult::Available,
                    200 => CheckResult::Taken,
                    429 => CheckResult::Error(CheckError::RateLimited(
                        "Rate limited by RDAP server".to_string(),
                    )),
                    500..=599 => CheckResult::Error(CheckError::Http {
                        status,
                        message: "Server error".to_string(),
                    }),
                    _ => CheckResult::Error(CheckError::Http {
                        status,
                        message: format!("Unexpected status code: {}", status),
                    }),
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    CheckResult::Error(CheckError::Timeout(e.to_string()))
                } else if e.is_connect() {
                    CheckResult::Error(CheckError::Connection(e.to_string()))
                } else {
                    CheckResult::Error(CheckError::Other(e.to_string()))
                }
            }
        }
    }
}

impl Default for IdChecker {
    fn default() -> Self {
        Self::new().expect("Failed to create default IdChecker")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_backoff() {
        for _ in 0..10 {
            let backoff = calculate_backoff(0, 500, 30);
            assert!(backoff <= Duration::from_millis(500));
        }

        for _ in 0..10 {
            let backoff = calculate_backoff(10, 500, 30);
            assert!(backoff <= Duration::from_secs(30));
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_ens_check_available() {
        let checker = EnsChecker::new().await.unwrap();
        let result = checker.check_available("ethereum").await;
        assert!(matches!(result, CheckResult::Taken));
    }

    #[tokio::test]
    #[ignore]
    async fn test_rdap_check_available() {
        let checker = RdapChecker::new().unwrap();
        let result = checker.check_available("hello").await;
        println!("hello.box: {:?}", result);
    }
}
