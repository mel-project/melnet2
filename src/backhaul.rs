use async_trait::async_trait;
use nanorpc::{RpcService, RpcTransport};

use crate::protocol::Address;

/// An implementation of an underlying transport (e.g. TCP) should implement this trait.
#[async_trait]
pub trait Backhaul: Send + Sync + 'static {
    type RpcTransport: RpcTransport;
    type ConnectError: std::error::Error + Send + Sync;
    type ListenError;

    /// Connect to a remote address.
    async fn connect(&self, remote_addr: Address)
        -> Result<Self::RpcTransport, Self::ConnectError>;

    /// Listen for incoming requests at the given address, directing them to to this responder. Fill in `None` as the address to listen at all available addresses. Stopping listening is currently not yet supported.
    async fn start_listen(
        &self,
        local_addr: Address,
        handler: impl RpcService,
    ) -> Result<(), Self::ListenError>;
}
