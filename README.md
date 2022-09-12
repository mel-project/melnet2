## melnet2: Themelio's peer-to-peer protocol

`melnet2` is Themelio's peer-to-peer protocol. It is an overlay network that can be built over _any_ `nanorpc` transport. The two important items are:

- `Backhaul`, which is a trait that fully describes an underlying protocol (e.g. JSON-RPC over TCP, JSON-RPC over HTTP)
  - We provide `TcpBackhaul`, an high-performance, pipelined implementation of JSON-RPC over TCP
- `Swarm`, which wraps around a `Backhaul` to implement an _auto-peering_ peer-to-peer network of nodes implementing some `nanorpc` RPC protocol.
  - `Swarm` essentialy adds functionality for discovering peers and forming a randomly structured gossip network to any `nanorpc`-based system.

See `examples/mn2-gossip` for an example program.
