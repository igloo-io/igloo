use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_prune_interval_secs")]
    pub prune_interval_secs: u64,
    #[serde(default = "default_worker_timeout_secs")]
    pub worker_timeout_secs: i64,
}

fn default_prune_interval_secs() -> u64 {
    10
}

fn default_worker_timeout_secs() -> i64 {
    30
}

impl Settings {
    pub fn new() -> Result<Self, config::ConfigError> {
        let config_file_path = std::env::var("COORDINATOR_CONFIG_PATH")
            .unwrap_or_else(|_| "crates/coordinator/config/default.toml".to_string());

        let s = config::Config::builder()
            .add_source(config::File::with_name(&config_file_path).required(true))
            .add_source(config::Environment::with_prefix("IGLOO_COORDINATOR").separator("__"))
            .build()?;
        s.try_deserialize()
    }

    pub fn server_address(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}
