use std::{convert::Infallible, fmt::Display, str::FromStr};

use async_trait::async_trait;
use nanorpc::nanorpc_derive;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// This represents a backhaul-specific melnet address. It's essentially an immutable string, and is not guaranteed to be valid in any way.
///
/// [Address] implements the `From<T>` where `T: AsRef<str>`, so any "stringy" type can be converted into an [Address].
#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct Address(SmolStr);

impl<T: AsRef<str>> From<T> for Address
where
    SmolStr: From<T>,
{
    fn from(t: T) -> Self {
        Self(t.into())
    }
}

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

/// This trait defines the melnet *control* protocol
#[nanorpc_derive]
#[async_trait]
pub trait ControlProtocol {
    /// Returns the melnet swarm ID. Used to make sure different P2P networks are not confused with each other, as well as acting as a "ping" method.
    async fn __mn_get_swarm_id(&self) -> String;

    /// Obtains random peers.
    async fn __mn_get_random_peers(&self) -> Vec<Address>;

    /// Attempts to add a peer to the table. Does not return success or failure.
    async fn __mn_advertise_peer(&self, peer: Address) -> bool;
}
