use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use concurrent_queue::ConcurrentQueue;
use futures_util::{future::Shared, Future, FutureExt};
use moka::sync::Cache;
use smol::{
    channel::{Receiver, Sender},
    future::FutureExt as SmolFutureExt,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    Task,
};

/// A backhaul implementation over raw, pipelined TCP connections.
pub struct TcpBackhaul {
    /// A connection pool. This weird type is to keep track of *in-flight* connection attempts
    pool: Arc<Cache<SocketAddr, Shared<Task<Result<Pipeline, Arc<std::io::Error>>>>>>,
}

const MAX_POOLED_CONNS: usize = 32;

impl TcpBackhaul {
    async fn get_conn(&self, dest: SocketAddr) -> Result<Pipeline, std::io::Error> {
        if let Some(conn) = self.pool.get(&dest) {
            match conn.await {
                Ok(conn) => Ok(conn),
                Err(err) => {
                    self.pool.invalidate(&dest);
                    Err(std::io::Error::new(err.kind(), err.to_string()))
                }
            }
        } else {
        }
    }
}

/// A fully pipelined TCP req/resp connection.
#[derive(Clone)]
struct Pipeline {
    send_req: Sender<(String, Sender<String>)>,
    recv_err: Shared<Task<Result<Infallible, Arc<std::io::Error>>>>,
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

async fn pipeline_inner(
    mut ustream: TcpStream,
    recv_req: Receiver<(String, Sender<String>)>,
) -> Result<Infallible, Arc<std::io::Error>> {
    const MAX_LINE_LENGTH: u64 = 1024 * 1024;

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
