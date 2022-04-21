use anyhow::Result;
use std::net::IpAddr;

/// This is a trait for objects that tell the Raft node how to discover its
/// peers.
#[crate::async_trait]
pub trait Discovery: Send + Sync + 'static {
    /// Return a list of discovered IP addresses
    async fn discover(&self) -> Result<Vec<IpAddr>>;
}

/// A basic host lookup discovery method. This is useful in the context of
/// Kubernetes deployments where all the pod IPs for a deployment will be under
/// one hostname.
pub struct HostLookup(String);

impl HostLookup {
    /// Create a new [`HostLookup`] with a given hostname.
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
