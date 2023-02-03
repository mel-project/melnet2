use std::fmt::Display;

use async_trait::async_trait;
use nanorpc::{JrpcRequest, JrpcResponse, RpcService, RpcTransport};
use smol::future::Boxed;
use thiserror::Error;

use crate::protocol::Address;

/// An implementation of an underlying transport (e.g. TCP) should implement this trait.
#[async_trait]
pub trait Backhaul: Send + Sync + Clone + 'static {
    type RpcTransport: RpcTransport;
    type ConnectError: std::error::Error + Send + Sync;
    type ListenError;

    /// Connect to a remote address.
    async fn connect(&self, remote_addr: Address)
        -> Result<Self::RpcTransport, Self::ConnectError>;

    /// Return an RpcTransport that tries to connect to the remote address only when used.
    async fn connect_lazy(
        &self,
        remote_addr: Address,
    ) -> AutoconnectTransport<Self::RpcTransport, Self::ConnectError> {
        let this = self.clone();
        AutoconnectTransport::new(move || {
            let this = this.clone();
            let remote_addr = remote_addr.clone();
            Box::pin(async move { this.connect(remote_addr).await })
        })
    }

    /// Listen for incoming requests at the given address, directing them to to this responder. Fill in `None` as the address to listen at all available addresses. Stopping listening is currently not yet supported.
    async fn start_listen(
        &self,
        local_addr: Address,
        handler: impl RpcService,
    ) -> Result<(), Self::ListenError>;
}

/// An RpcTransport that wraps around a function that produces RpcTransports and provides an "immortal" RpcTransport that creates the actual RpcTransport on demand.
pub struct AutoconnectTransport<Inner: RpcTransport, ConnectError: Display> {
    produce: Box<dyn Fn() -> Boxed<Result<Inner, ConnectError>> + Send + Sync + 'static>,
}

impl<Inner: RpcTransport, ConnectError: Display> AutoconnectTransport<Inner, ConnectError> {
    /// Create a new auto-connecting RpcTransport, given a function that returns some other RpcTransport.
    pub fn new(
        produce: impl Fn() -> Boxed<Result<Inner, ConnectError>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            produce: Box::new(produce),
        }
    }
}

#[async_trait]
impl<Inner: RpcTransport, ConnectError: Display + 'static + Send + Sync> RpcTransport
    for AutoconnectTransport<Inner, ConnectError>
where
    Inner::Error: Display,
{
    type Error = AutoconnectError<ConnectError, Inner::Error>;
    async fn call_raw(&self, req: JrpcRequest) -> Result<JrpcResponse, Self::Error> {
        let conn = (self.produce)()
            .await
            .map_err(AutoconnectError::ConnectError)?;
        let resp = conn
            .call_raw(req)
            .await
            .map_err(AutoconnectError::TransportError)?;
        Ok(resp)
    }
}

/// Errors that an [AutoconnectTransport] can run into.
#[derive(Error, Debug)]
pub enum AutoconnectError<E: Display, F: Display> {
    #[error("connect error: {0}")]
    ConnectError(E),
    #[error("transport: {0}")]
    TransportError(F),
}
