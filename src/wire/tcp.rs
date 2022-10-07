use std::{convert::Infallible, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dashmap::DashMap;
use futures_util::{future::Shared, Future, FutureExt};
use moka::sync::Cache;
use nanorpc::{JrpcRequest, JrpcResponse, RpcService, RpcTransport};
use smol::{
    channel::{Receiver, Sender},
    future::FutureExt as SmolFutureExt,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    Task,
};
use smol_timeout::TimeoutExt;
use thiserror::Error;

use crate::{protocol::Address, Backhaul};

/// A backhaul implementation over raw, pipelined TCP connections.
#[allow(clippy::type_complexity)]
pub struct TcpBackhaul {
    /// A connection pool. This weird type is to keep track of *in-flight* connection attempts
    pool: Arc<Cache<SocketAddr, Shared<Task<Result<Pipeline, Arc<std::io::Error>>>>>>,

    /// A mapping between addresses and listeners.
    listeners: Arc<DashMap<SocketAddr, Task<()>>>,
}

#[async_trait]
impl Backhaul for TcpBackhaul {
    type RpcTransport = Pipeline;
    type ConnectError = std::io::Error;
    type ListenError = std::io::Error;

    async fn connect(
        &self,
        remote_addr: Address,
    ) -> Result<Self::RpcTransport, Self::ConnectError> {
        let addr: SocketAddr = SocketAddr::from_str(&remote_addr.to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Unsupported, e.to_string()))?;
        let conn = self.get_conn(addr).await?;
        Ok(conn)
    }

    async fn start_listen(
        &self,
        local_addr: Address,
        handler: impl RpcService,
    ) -> Result<(), Self::ListenError> {
        let addr: SocketAddr = SocketAddr::from_str(&local_addr.to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Unsupported, e.to_string()))?;
        let listener = TcpListener::bind(addr).await?;
        let handler = Arc::new(handler);
        let task = smolscale::spawn(async move {
            loop {
                let (conn, _) = uob(listener.accept()).await;
                let handler = handler.clone();
                let t: Task<anyhow::Result<()>> = smolscale::spawn(async move {
                    let mut down = BufWriter::new(conn.clone());
                    let mut up = BufReader::new(conn);
                    let mut line = String::new();
                    let handler = handler.clone();
                    loop {
                        line.clear();
                        (&mut up).take(MAX_LINE_LENGTH).read_line(&mut line).await?;
                        let response = handler.respond_raw(serde_json::from_str(&line)?).await;
                        let response = serde_json::to_vec(&response)?;
                        down.write_all(&response).await?;
                        down.write_all(b"\n").await?;
                        down.flush().await?;
                    }
                });
                t.detach();
            }
        });
        self.listeners.insert(addr, task);
        Ok(())
    }
}

impl Default for TcpBackhaul {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpBackhaul {
    /// Creates a new TcpBackhaul.
    pub fn new() -> Self {
        let pool = Arc::new(
            Cache::builder()
                .max_capacity(256)
                .time_to_live(Duration::from_secs(60))
                .build(),
        );
        let listeners = Arc::new(DashMap::new());
        Self { pool, listeners }
    }

    async fn get_conn(&self, dest: SocketAddr) -> Result<Pipeline, std::io::Error> {
        loop {
            if let Some(conn) = self.pool.get(&dest) {
                match conn.await {
                    Ok(conn) => return Ok(conn),
                    Err(err) => {
                        self.pool.invalidate(&dest);
                        return Err(std::io::Error::new(err.kind(), err.to_string()));
                    }
                }
            } else {
                // make a task that resolves to a pipeline
                let pipe_task = smolscale::spawn(async move {
                    let tcp_conn = smol::net::TcpStream::connect(dest)
                        .or(async {
                            smol::Timer::after(Duration::from_secs(5)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "TCP connect timed out",
                            ))
                        })
                        .await;
                    Ok(Pipeline::new(tcp_conn?))
                });
                self.pool.insert(dest, pipe_task.shared());
            }
        }
    }
}

/// The client-side of a fully pipelined TCP JSON-RPC connection. This is the basic transport protocol that TcpBackhaul runs over.
#[derive(Clone)]
pub struct Pipeline {
    send_req: Sender<(String, Sender<String>)>,
    recv_err: Shared<Task<Result<Infallible, Arc<std::io::Error>>>>,
}

/// Errors that a [Pipeline] can run into.
#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("I/O failed: {0}")]
    IoError(std::io::Error),
    #[error("JSON error: {0}")]
    JsonError(serde_json::Error),
}

#[async_trait]
impl RpcTransport for Pipeline {
    type Error = PipelineError;

    async fn call_raw(&self, req: JrpcRequest) -> Result<JrpcResponse, Self::Error> {
        let fallible = async {
            let to_send = serde_json::to_string(&req).map_err(PipelineError::JsonError)?;
            let result = self
                .request(to_send)
                .await
                .map_err(PipelineError::IoError)?;
            let result: JrpcResponse =
                serde_json::from_str(&result).map_err(PipelineError::JsonError)?;
            Ok(result)
        };
        Ok(fallible
            .timeout(Duration::from_secs(10))
            .await
            .ok_or_else(|| {
                PipelineError::IoError(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out in pipeline",
                ))
            })??)
    }
}

impl Pipeline {
    /// Wraps a Pipeline around the given TCP stream
    pub fn new(stream: TcpStream) -> Self {
        let (send_req, recv_req) = smol::channel::bounded(16);
        let task = smolscale::spawn(pipeline_inner(stream, recv_req));
        Self {
            send_req,
            recv_err: task.shared(),
        }
    }

    /// Does a single request onto the pipeline.
    pub async fn request(&self, req: String) -> Result<String, std::io::Error> {
        let (send_resp, recv_resp) = smol::channel::bounded(1);
        let _ = self.send_req.send((req, send_resp)).await;
        let recv_err = self.recv_err.clone();
        async { Ok(uob(recv_resp.recv()).await) }
            .or(async { Err(recv_err.await.unwrap_err()) })
            .await
            .map_err(|e: Arc<std::io::Error>| std::io::Error::new(e.kind(), e.to_string()))
    }
}

const MAX_LINE_LENGTH: u64 = 100 * 1024 * 1024;

async fn pipeline_inner(
    mut ustream: TcpStream,
    recv_req: Receiver<(String, Sender<String>)>,
) -> Result<Infallible, Arc<std::io::Error>> {
    let queue = ConcurrentQueue::unbounded();
    let mut dstream = BufReader::new(ustream.clone());
    let up = async {
        loop {
            let (req, send_resp) = uob(recv_req.recv()).await;
            queue.push(send_resp).unwrap();
            ustream.write_all((req + "\n").as_bytes()).await?;
        }
    };
    let down = async {
        loop {
            let mut line = String::new();
            (&mut dstream)
                .take(MAX_LINE_LENGTH)
                .read_line(&mut line)
                .await?;
            if let Ok(send_resp) = queue.pop() {
                let _ = send_resp.try_send(line);
            }
        }
    };
    up.race(down).await
}

async fn uob<T, E>(f: impl Future<Output = Result<T, E>>) -> T {
    match f.await {
        Ok(t) => t,
        _ => smol::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use nanorpc::FnService;

    use super::*;

    #[test]
    fn transport_basic() {
        smolscale::block_on(async move {
            let server_addr = Address::from("127.0.0.1:12345");
            let backhaul = TcpBackhaul::new();
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
