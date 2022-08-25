use std::{convert::Infallible, fmt::Display, str::FromStr};

use async_trait::async_trait;
use nanorpc::nanorpc_derive;
use smol_str::SmolStr;

/// This represents a backhaul-specific melnet address. It's essentially an immutable string.
#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Address(SmolStr);

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Address {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.into()))
    }
}

impl From<&str> for Address {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl From<String> for Address {
    fn from(s: String) -> Self {
        Self(s.into())
    }
}

/// This trait defines the melnet *control* protocol
#[nanorpc_derive]
#[async_trait]
pub trait ControlProtocol {
    /// Returns the melnet swarm ID. Used to make sure different P2P networks are not confused with each other, as well as acting as a "ping" method.
    async fn __mn_get_swarmid(&self) -> String;

    /// Obtains *one* random peer.
    async fn __mn_get_random_peer(&self) -> Vec<String>;
}
