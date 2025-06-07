use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub worker_host: String,
    pub worker_port: u16,
    pub coordinator_address: String,
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
}

fn default_heartbeat_interval_secs() -> u64 {
    5
}

impl Settings {
    pub fn new() -> Result<Self, config::ConfigError> {
        let config_file_path = std::env::var("WORKER_CONFIG_PATH")
            .unwrap_or_else(|_| "crates/worker/config/default.toml".to_string());

        let s = config::Config::builder()
            .add_source(config::File::with_name(&config_file_path).required(true))
            .add_source(config::Environment::with_prefix("IGLOO_WORKER").separator("__"))
            .build()?;
        s.try_deserialize()
    }

    pub fn worker_server_address(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.worker_host, self.worker_port).parse()
    }
}
