use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use itertools::Itertools;
use melnet2::{wire::tcp::TcpBackhaul, Backhaul, Swarm};
use protocol::{GossipClient, GossipProtocol, GossipService};
use smol::io::{AsyncBufReadExt, BufReader};
mod protocol;

struct Forwarder {
    swarm: Swarm<TcpBackhaul, GossipClient<<TcpBackhaul as Backhaul>::RpcTransport>>,
    seen: Mutex<HashSet<String>>,
}

#[async_trait::async_trait]
impl GossipProtocol for Forwarder {
    async fn forward(&self, msg: String) -> bool {
        if !self.seen.lock().unwrap().insert(msg.clone()) {
            return false;
        }
        println!("\n< {msg}");
        for route in self.swarm.routes().await {
            let swarm = self.swarm.clone();
            let msg = msg.clone();
            smolscale::spawn(async move {
                let connection = swarm.connect(route).await?;
                connection.forward(msg).await?;
                anyhow::Ok(())
            })
            .detach();
        }
        return true;
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    smolscale::block_on(async move {
        let swarm = Swarm::new(TcpBackhaul::new(), GossipClient, "spamswarm");
        let addr: SocketAddr = std::env::args()
            .collect_vec()
            .get(1)
            .context("must provide listening address")?
            .parse()?;
        let args = std::env::args().collect_vec();
        for rest in args.into_iter().skip(2) {
            let addr: SocketAddr = rest.parse()?;
            swarm.add_route(addr.to_string().into(), false).await;
        }
        swarm
            .start_listen(
                addr.to_string().into(),
                Some(addr.to_string().into()),
                GossipService(Forwarder {
                    swarm: swarm.clone(),
                    seen: Default::default(),
                }),
            )
            .await?;
        let mut stdin = BufReader::new(smol::Unblock::new(std::io::stdin()));
        let mut line = String::new();
        loop {
            stdin.read_line(&mut line).await?;
            if let Some(f) = swarm.routes().await.get(0) {
                swarm
                    .connect(f.clone())
                    .await?
                    .forward(format!(
                        "{}: {}",
                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                        line.trim()
                    ))
                    .await?;
            }
        }
    })
}
