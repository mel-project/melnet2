use async_trait::async_trait;
use nanorpc::nanorpc_derive;

/// The mn2-gossip protocol
#[nanorpc_derive]
#[async_trait]
pub trait GossipProtocol {
    /// Forwards a message to this peer. Does not return anything interesting.
    async fn forward(&self, msg: String) -> bool;
}
