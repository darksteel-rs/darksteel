use anyhow::Result;
use std::net::IpAddr;

/// This is a trait for objects that tell the Raft node how to discover its
/// peers.
#[crate::async_trait]
pub trait Discovery: Send + Sync + 'static {
    /// Return a list of discovered IP addresses
    async fn discover(&self) -> Result<Vec<IpAddr>>;
}

pub struct HostLookup(String);

impl HostLookup {
    pub fn new<S: Into<String>>(host: S) -> Self {
        Self(host.into())
    }
}

#[crate::async_trait]
impl Discovery for HostLookup {
    async fn discover(&self) -> Result<Vec<IpAddr>> {
        Ok(dns_lookup::lookup_host(self.0.as_str())?)
    }
}
