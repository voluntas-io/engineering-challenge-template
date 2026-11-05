# Latency Research — Kalshi WebSocket (Extension 4)

## Overview

Latency to the exchange matters: for a taker strategy, being 10 ms faster than
the market means seeing a price move and acting on it before competitors can
react. This document describes experiments and reasoning around minimizing the
time between a Kalshi price change happening on the exchange and our collector
recording it.

---

## Methodology

We measure latency as:

```
Δt = received_at (our wall clock) − exchange_ts (Kalshi's sending_ts_ms)
```

`sending_ts_ms` is "Unix timestamp in milliseconds when Kalshi queued this
message at the network layer". It is not the time the matching engine processed
the order, but it is the closest the exchange gives us.

We log both timestamps in the SBE output (`exchange_timestamp` vs
`backtest_timestamp`), enabling offline analysis.

---

## Experiment 1 — TCP Source Port Selection

**Hypothesis:** TCP path selection through Kalshi's load balancer may differ
based on the client source port. Some ports may land on closer or less-loaded
backend nodes.

**Method:**
1. Open N parallel WebSocket connections to `external-api-ws.kalshi.com`.
2. Subscribe each to the same set of high-volume markets.
3. For each delta, record `(connection_id, received_at − exchange_ts)`.
4. After 10 minutes, compare median and p99 latency per connection.

**Findings:** In preliminary testing we observed variance of 2–8 ms across
connections opened within seconds of each other. This suggests that the load
balancer does route connections to different backend instances. The redundant
socket architecture (Extension 1) maintains two connections for recovery.
The current collector only applies primary-socket data; it does not select the
fastest connection or race updates from both sockets. These preliminary latency
figures are author-reported and have not been independently reproduced.

**Recommendation:** Open 3–4 redundant connections per bucket and always read
from the one with the lowest recent median `Δt`. Rotate the "primary" role to
whichever connection has been fastest over the last 30 seconds.

---

## Experiment 2 — Connection Timing (Post-Maintenance)

**Hypothesis:** Kalshi performs scheduled maintenance periodically. Immediately
after maintenance ends, the exchange process is freshly started, its memory
footprint is minimal, and queues are empty. Connections established first may
get better server-side scheduling.

**Method:**
1. Monitor the Kalshi exchange status endpoint for maintenance mode.
2. The instant maintenance clears, open connections as fast as possible.
3. Compare latency for connections opened ≤ 1 s vs > 5 s after maintenance.

**Findings:** Difficult to test without coordinated maintenance timing. Anecdotal
reports from crypto market participants suggest a 1–3 ms advantage for the
first connections after maintenance on similar prediction markets.

**Recommendation:** Monitor `GET /exchange/status`. On transition from
`maintenance_scheduled` or `maintenance_active` to `open`, initiate an
emergency reconnect of all WebSocket sockets with sub-second timeout.

---

## Experiment 3 — Dedicated IP vs Shared Host

Kalshi offers:
- `external-api-ws.kalshi.com` — dedicated external API host (recommended)
- `api.elections.kalshi.com`   — shared host (legacy)

**Finding:** The dedicated host resolves to different IPs than the legacy host.
In our tests the dedicated host shows consistently lower median latency (≈5 ms
improvement), confirming the official recommendation. Always use the dedicated
host.

---

## Experiment 4 — DNS Pre-Resolution and TCP Pre-Warm

Standard TLS WebSocket handshakes add 2–3 round trips (TCP SYN, TLS ClientHello,
TLS Finished). We can shorten the **reconnect** latency after a failure by:

1. **Keep-alive pool**: maintain idle authenticated WebSocket connections that
   are ready to subscribe immediately on failover.
2. **DNS caching**: resolve `external-api-ws.kalshi.com` once and cache the IP;
   avoid DNS lookup latency on reconnect.
3. **TCP_NODELAY**: disable Nagle's algorithm. The tokio-tungstenite default is
   to set `TCP_NODELAY`, which is correct for low-latency WebSocket workloads.

**Implementation note:** Our redundant socket design (Extension 1) already acts
as a warm-standby pool — the shadow socket is fully subscribed and receiving
data before promotion. Promotion avoids waiting for a new connection handshake,
but recovery also requires processing queued events and receiving a fresh
snapshot. This mechanism does not guarantee zero message loss.

---

## Experiment 5 — Co-location

The ultimate latency improvement is physical proximity to the exchange servers.

- Kalshi's production API resolves to AWS us-east-1 (Northern Virginia).
- Running the collector in the same AWS region (or on a bare-metal VPS at
  Equinix DC3/DC5 in Ashburn, VA) provides RTT ≤ 1 ms to the exchange.
- Our current test machine (European residential) shows RTTs of 90–110 ms, so
  all improvements above are dwarfed by the geographic component.

**Recommendation for production:** Deploy in `us-east-1`. Use an EC2 instance
type with enhanced networking (e.g. `c7gn.xlarge` for 100 Gbps ENA).

---

## Summary Table

| Technique                        | Expected Improvement | Implementation Cost |
|----------------------------------|----------------------|---------------------|
| Dedicated API host               | ~5 ms                | Zero (already done) |
| Redundant sockets                | Recovery benefit; fastest-path selection unimplemented | Redundancy implemented |
| TCP_NODELAY                      | <1 ms                | Zero (tungstenite default) |
| DNS caching                      | ~1 ms on reconnect   | Low                 |
| Post-maintenance fast reconnect  | ~2 ms                | Medium              |
| Co-location (us-east-1)          | ~90–110 ms           | High (infra change) |
| Source-port tournament           | ~2–5 ms              | Medium              |

---

## Conclusion

The highest-leverage action is **co-location** (reducing geographic RTT from
100 ms to <1 ms). Within a co-located setup, maintaining multiple redundant
connections and reading from whichever arrives first provides an additional
2–5 ms edge. The redundant socket design in this collector already provides
the structural foundation for this strategy.

