use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use futures_util::TryFutureExt;
use itertools::Itertools;
use nanorpc::{OrService, RpcService, RpcTransport};
use smol::Task;
use smol_timeout::TimeoutExt;

use crate::{
    protocol::{Address, ControlClient, ControlProtocol, ControlService},
    Backhaul,
};

use self::routedb::RouteDb;

const ROUTE_LIMIT: usize = 32;

/// Represents a node in an independent P2P swarm that implements a particular RPC protocol.
///
/// This is generic over a backhaul, so different transports can be plugged in easily, as well as a client-level handle (e.g. the `FoobarClient` that `nanorpc`'s macros generate from a `FoobarProtocol` trait). We are intentionally generic over the second one to statically prevent confusion between swarms that run incompatible RPC protocols; the combination of backhaul and client uniquely identifies a network compatible with a particular protocol.
pub struct Swarm<B: Backhaul, C> {
    /// Backhaul
    haul: Arc<B>,
    /// Route storage. Protected by an *async* RwLock
    routes: Arc<smol::lock::RwLock<RouteDb>>,
    /// Something that creates an RpcClient from a backhaul RpcTransport
    open_client: Arc<dyn Fn(B::RpcTransport) -> C + Sync + Send + 'static>,
    /// Swarm ID
    swarm_id: String,

    /// The route maintenance task
    _route_maintain_task: Arc<Task<Infallible>>,
}

impl<B: Backhaul, C> Clone for Swarm<B, C> {
    fn clone(&self) -> Self {
        Self {
            haul: self.haul.clone(),
            routes: self.routes.clone(),
            open_client: self.open_client.clone(),
            swarm_id: self.swarm_id.clone(),
            _route_maintain_task: self._route_maintain_task.clone(),
        }
    }
}

impl<B: Backhaul, C: 'static> Swarm<B, C>
where
    <B::RpcTransport as RpcTransport>::Error: std::error::Error + Send + Sync,
{
    /// Creates a new [Swarm] given a backhaul, and a function that maps a raw transport to a client-level handle.
    pub fn new(
        backhaul: B,
        client_map_fn: impl Fn(B::RpcTransport) -> C + Sync + Send + 'static,
        swarm_id: &str,
    ) -> Self {
        let haul = Arc::new(backhaul);
        let routes = Arc::new(smol::lock::RwLock::new(RouteDb::default()));
        let open_client = Arc::new(client_map_fn);
        Self {
            haul: haul.clone(),
            routes: routes.clone(),
            open_client: open_client.clone(),
            swarm_id: swarm_id.to_string(),

            _route_maintain_task: smolscale::spawn(Self::route_maintain(
                haul,
                routes,
                open_client,
                swarm_id.to_string(),
            ))
            .into(),
        }
    }

    /// Obtains a connection to a peer.
    pub async fn connect(&self, addr: Address) -> Result<C, B::ConnectError> {
        Ok((self.open_client)(self.haul.connect(addr).await?))
    }

    /// Starts a listener on the given address. If `advertise_addr` is present, then advertise this address to peers asking for routes.
    pub async fn start_listen(
        &self,
        listen_addr: Address,
        advertise_addr: Option<Address>,
        service: impl RpcService,
    ) -> Result<(), B::ListenError> {
        let self2 = self.clone();
        self.haul
            .start_listen(listen_addr, OrService::new(service, ControlService(self2)))
            .await?;
        if let Some(advertise_addr) = advertise_addr {
            self.routes
                .write()
                .await
                .insert(advertise_addr, Duration::from_millis(0), true)
        }
        Ok(())
    }

    /// Obtains routes.
    pub async fn routes(&self) -> Vec<Address> {
        self.routes
            .read()
            .await
            .random_iter()
            .map(|s| s.addr)
            .collect_vec()
    }

    /// Adds a route, specifying whether or not it's "sticky" (will never be evicted)
    pub async fn add_route(&self, addr: Address, sticky: bool) {
        let mut v = self.routes.write().await;
        v.insert(addr, Duration::from_secs(1), sticky);
    }

    /// Background loop for route maintenance.
    async fn route_maintain(
        haul: Arc<B>,
        routes: Arc<smol::lock::RwLock<RouteDb>>,
        _open_client: Arc<dyn Fn(B::RpcTransport) -> C + Sync + Send + 'static>,
        swarm_id: String,
    ) -> Infallible {
        const PULSE: Duration = Duration::from_secs(1);
        let mut timer = smol::Timer::interval(PULSE);
        let exec = smol::Executor::new();
        // we run timers on a roughly Poisson distribution in order to avoid synchronized patterns emerging
        exec.run(async {
            loop {
                if fastrand::f64() * 3.0 < 1.0 {
                    log::debug!("[{swarm_id}] push pulse");
                    if let Some(random) = routes.read().await.random_iter().next() {
                        if let Some(to_send) = routes
                            .read()
                            .await
                            .random_iter()
                            .find(|r| r.addr != random.addr)
                        {
                            let random = random.addr;
                            let to_send = to_send.addr;
                            log::debug!("[{swarm_id}] push {to_send} => {random}");
                            let random2 = random.clone();
                            exec.spawn(
                                async {
                                    let to_send = to_send;
                                    let random = random;
                                    let conn = ControlClient(
                                        haul.connect(random)
                                            .timeout(Duration::from_secs(60))
                                            .await
                                            .context("connect timeout")??,
                                    );
                                    conn.__mn_advertise_peer(to_send)
                                        .timeout(Duration::from_secs(60))
                                        .await
                                        .context("advertise timeout")??;
                                    anyhow::Ok(())
                                }
                                .unwrap_or_else(|e| {
                                    let random2 = random2;
                                    log::warn!("[{swarm_id}] push failed to {}: {e}", random2)
                                }),
                            )
                            .detach();
                        }
                    }
                }

                if fastrand::f64() * 10.0 < 1.0 {
                    let current_count = routes.read().await.count();
                    log::debug!("[{swarm_id}] pull pulse {current_count}/{ROUTE_LIMIT}");
                    if current_count < ROUTE_LIMIT {
                        // we request more routes from a random peer
                        if let Some(route) = routes.read().await.random_iter().next() {
                            log::debug!("[{swarm_id}] getting more routes from {}", route.addr);
                            let route2 = route.clone();
                            exec.spawn(
                                async {
                                    let route = route;
                                    let conn = ControlClient(
                                        haul.connect(route.addr.clone())
                                            .timeout(Duration::from_secs(60))
                                            .await
                                            .context("connect timeout")??,
                                    );
                                    for peer in conn
                                        .__mn_get_random_peers()
                                        .timeout(Duration::from_secs(60))
                                        .await
                                        .context("get peers timeout")??
                                    {
                                        log::debug!(
                                            "[{swarm_id}] got route {} from {}",
                                            route.addr,
                                            peer
                                        );
                                        let ping = Self::test_ping(&haul, peer.clone(), &swarm_id)
                                            .await
                                            .context("ping failed")?;
                                        routes.write().await.insert(peer, ping, false)
                                    }
                                    anyhow::Ok(())
                                }
                                .unwrap_or_else(|e| {
                                    let route = route2;
                                    log::warn!(
                                        "[{swarm_id}] get more routes failed from {}: {e}",
                                        route.addr
                                    )
                                }),
                            )
                            .detach();
                        } else if current_count > ROUTE_LIMIT {
                            // delete the worst route
                            let mut routes = routes.write().await;
                            routes.remove_worst();
                        }
                    }
                }
                if fastrand::f64() * 300.0 < 1.0 {
                    log::debug!("[{swarm_id}] ping pulse...");
                    let routes_guard = routes.read().await;
                    for route in routes_guard.random_iter() {
                        exec.spawn(async {
                            let route = route;
                            if let Err(err) =
                                Self::test_ping(&haul, route.addr.clone(), &swarm_id).await
                            {
                                if route.sticky {
                                    log::debug!(
                                        "[{swarm_id}] keeping sticky {} despite ping-fail: {err}",
                                        route.addr
                                    );
                                } else {
                                    log::debug!(
                                        "[{swarm_id}] ping-failing non-sticky {}: {err}",
                                        route.addr
                                    );
                                    routes.write().await.remove(route.addr);
                                }
                            }
                        })
                        .detach();
                    }
                }
                (&mut timer).await;
            }
        })
        .await
    }

    async fn test_ping(haul: &B, addr: Address, swarm_id: &str) -> anyhow::Result<Duration> {
        let start = Instant::now();
        let client = ControlClient(
            haul.connect(addr)
                .timeout(Duration::from_secs(5))
                .await
                .context("connect timed out after 5 seconds")??,
        );
        let their_swarm_id = client
            .__mn_get_swarm_id()
            .timeout(Duration::from_secs(5))
            .await
            .context("ping timed out after 5 seconds")??;
        if their_swarm_id != swarm_id {
            anyhow::bail!(
                "their swarm ID {:?} is not our swarm ID {:?}",
                their_swarm_id,
                swarm_id
            );
        }
        Ok(start.elapsed())
    }
}

#[async_trait]
impl<B: Backhaul, C: 'static> ControlProtocol for Swarm<B, C>
where
    <B::RpcTransport as RpcTransport>::Error: std::error::Error + Send + Sync,
{
    async fn __mn_get_swarm_id(&self) -> String {
        self.swarm_id.clone()
    }

    async fn __mn_get_random_peers(&self) -> Vec<Address> {
        self.routes
            .read()
            .await
            .random_iter()
            .take(8)
            .map(|r| r.addr)
            .collect()
    }

    async fn __mn_advertise_peer(&self, addr: Address) -> bool {
        if self.routes.read().await.count() >= ROUTE_LIMIT {
            return false;
        }
        if let Ok(ping) = Self::test_ping(&self.haul, addr.clone(), &self.swarm_id).await {
            self.routes.write().await.insert(addr, ping, false);
        }
        return true;
    }
}

mod routedb {
    use itertools::Itertools;

    use super::*;
    #[derive(Default)]
    pub struct RouteDb {
        /// An assoc-vector of routes. This structure is used because it enables efficient random access.
        routes: Vec<Route>,
    }

    impl RouteDb {
        pub fn get_route(&self, addr: Address) -> Option<Route> {
            self.routes.iter().find(|r| r.addr == addr).cloned()
        }

        fn get_route_mut(&mut self, addr: Address) -> Option<&mut Route> {
            self.routes.iter_mut().find(|r| r.addr == addr)
        }

        pub fn insert(&mut self, addr: Address, ping: Duration, sticky: bool) {
            if let Some(r) = self.get_route_mut(addr.clone()) {
                r.last_ping = ping;
                r.last_seen = Instant::now();
                r.sticky = sticky;
            } else {
                self.routes.push(Route {
                    addr,
                    last_ping: ping,
                    last_seen: Instant::now(),
                    sticky,
                })
            }
        }

        pub fn random_iter(&self) -> impl Iterator<Item = Route> + '_ {
            std::iter::repeat_with(|| fastrand::usize(0..self.routes.len()))
                .map(|i| self.routes[i].clone())
                .unique()
                .take(self.routes.len())
        }

        pub fn remove(&mut self, addr: Address) {
            self.routes.retain(|s| s.addr != addr);
        }

        pub fn remove_worst(&mut self) {
            self.routes.sort_unstable_by_key(|s| s.last_ping);
            let _ = self.routes.pop();
        }

        pub fn count(&self) -> usize {
            self.routes.len()
        }
    }

    #[derive(Clone, Hash, PartialEq, Eq)]
    pub struct Route {
        pub addr: Address,
        pub last_ping: Duration,
        pub last_seen: Instant,
        pub sticky: bool,
    }
}
