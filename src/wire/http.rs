use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;

use dashmap::DashMap;

use moka::sync::Cache;
use nanorpc::RpcService;
use nanorpc_http::{client::HttpRpcTransport, server::HttpRpcServer};
use smol::Task;

use crate::{protocol::Address, Backhaul};

/// A backhaul implementation over raw, pipelined TCP connections.
#[allow(clippy::type_complexity)]
#[derive(Clone)]
pub struct HttpBackhaul {
    /// A connection pool.
    pool: Arc<Cache<SocketAddr, Arc<HttpRpcTransport>>>,

    /// A mapping between addresses and listeners.
    listeners: Arc<DashMap<SocketAddr, Task<()>>>,
}

#[async_trait]
impl Backhaul for HttpBackhaul {
    type RpcTransport = Arc<HttpRpcTransport>;
    type ConnectError = std::io::Error;
    type ListenError = std::io::Error;

    async fn connect(
        &self,
        remote_addr: Address,
    ) -> Result<Self::RpcTransport, Self::ConnectError> {
        let addr = match SocketAddr::from_str(&remote_addr.to_string()) {
            Ok(addr) => addr,
            Err(_) => {
                let resolved: Vec<SocketAddr> = smol::net::resolve(remote_addr.to_string()).await?;
                if resolved.is_empty() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "invalid remote_addr",
                    ));
                } else {
                    resolved.first().cloned().unwrap()
                }
            }
        };

        self.get_conn(addr).await.map_err(Into::into)
    }

    async fn start_listen(
        &self,
        local_addr: Address,
        handler: impl RpcService,
    ) -> Result<(), Self::ListenError> {
        let addr: SocketAddr = SocketAddr::from_str(&local_addr.to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Unsupported, e.to_string()))?;
        let listener = HttpRpcServer::bind(addr).await?;
        let task = smolscale::spawn(async move {
            listener.run(handler).await.expect("listener died randomly")
        });
        self.listeners.insert(addr, task);
        Ok(())
    }
}

impl Default for HttpBackhaul {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpBackhaul {
    /// Creates a new TcpBackhaul.
    pub fn new() -> Self {
        let pool = Arc::new(
            Cache::builder()
                .time_to_idle(Duration::from_secs(60))
                .build(),
        );
        let listeners = Arc::new(DashMap::new());
        Self { pool, listeners }
    }

    async fn get_conn(&self, dest: SocketAddr) -> Result<Arc<HttpRpcTransport>, std::io::Error> {
        if let Some(conn) = self.pool.get(&dest) {
            Ok(conn)
        } else {
            let pipe = Arc::new(HttpRpcTransport::new(dest));
            self.pool.insert(dest, pipe.clone());
            Ok(pipe)
        }
    }
}

#[cfg(test)]
mod tests {
    use nanorpc::{FnService, RpcTransport};

    use super::*;

    #[test]
    fn transport_basic() {
        smolscale::block_on(async move {
            let server_addr = Address::from("127.0.0.1:12345");
            let backhaul = HttpBackhaul::new();
            backhaul
                .start_listen(
                    server_addr.clone(),
                    FnService::new(|_, args| async move {
                        Some(Ok(serde_json::to_value(args).unwrap()))
                    }),
                )
                .await
                .unwrap();
            let conn = backhaul.connect(server_addr).await.unwrap();
            let result = conn
                .call("hello", &[serde_json::to_value("world").unwrap()])
                .await
                .unwrap();
            assert_eq!(result, Some(Ok(serde_json::json!(["world"]))))
        });
    }
}
