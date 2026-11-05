# Latency Research — My Findings and Next Steps

For Extension 4, I reviewed the collector's timing path and possible ways to
reduce delivery and recovery delays. My priority is complete, correct market
data. I have not established a reproducible production latency baseline, so I
separate implementation findings from experiments I would run next. I do not
claim a measured advantage from source-port selection, host selection,
post-maintenance connection timing, or deployment location.

## What my collector currently records

In [`src/collector.rs`](src/collector.rs), I capture `received_at` with
`Utc::now()` after the WebSocket library returns a complete text message and
before JSON decoding. For accepted deltas, I write this value to the SBE
`backtest_timestamp` field. It is an application receive timestamp, not a
kernel packet-arrival timestamp or the time the message reaches disk.

In [`src/kalshi.rs`](src/kalshi.rs), I read the optional `msg.ts_ms` field and
pass it to SBE as `exchange_timestamp`. This is different from the envelope's
`sending_ts_ms`, which my decoder does not currently retain. Kalshi documents
`ts_ms` as the time the orderbook change was recorded and `sending_ts_ms` as
the time it queued the message for sending. [Kalshi WebSocket specification][1]

For deltas with a valid exchange timestamp, I could calculate:

```text
observed event age = backtest_timestamp − exchange_timestamp
```

I would interpret this as an observed age across exchange and local clocks,
not pure network latency or matching-engine latency. Clock offset, timestamp
resolution, exchange processing, transport, and local scheduling can affect
it. I would synchronize the host clock and report its uncertainty before
comparing small differences.

If `ts_ms` is absent or invalid, my collector substitutes `received_at`.
Those records produce a zero difference by construction, not evidence of
instant delivery. The SBE message does not preserve a flag identifying that
fallback. I would add timestamp provenance before producing latency statistics.
I would also exclude snapshots from this calculation: the current snapshot
paths supply a local timestamp as `exchange_timestamp`.

## What redundancy provides today

I maintain a primary and a subscribed shadow connection for each bucket. Only
primary events update the canonical book and output. When a primary disconnects
or is recycled, I promote the other socket, emit the current in-memory book, hold
back that bucket's deltas until a fresh snapshot replaces each book, and replace
the old socket.

This avoids requiring a new connection handshake before promotion, but it does
not guarantee a healthy backup, immediate resynchronization, or lossless
failover. My collector does not select the fastest socket or race both feeds.
Older clocks are rejected before state mutation and SBE output. I check `seq` on
each connection, and a gap on the primary triggers the same snapshot recovery.

## What I would measure next

| Experiment | How I would evaluate it | Current status |
| --- | --- | --- |
| Parallel connections and source ports | Match the same events across sockets and compare arrival times, disconnects, and recovery. Record ports and control other variables before attributing differences to routing. | Not measured reproducibly; fastest-feed selection is unimplemented. |
| Deployment location | Compare candidate regions using the same markets and observation windows; record RTT and application event-age distributions separately. | No verified exchange region or sub-millisecond RTT claim. |
| Reconnect preparation | Measure DNS, TCP/TLS/WebSocket setup, subscription, and fresh-snapshot completion separately. Evaluate DNS caching while respecting DNS changes and TLS hostname validation. | No measured improvement claimed. |
| TCP_NODELAY | Compare otherwise identical connections with and without it; measure outgoing command and recovery behavior as well as feed arrival. My client's setting cannot control exchange-side buffering. | My `connect_async(request)` call does not explicitly enable it. |
| Maintenance recovery | Observe documented exchange-status fields and rebuild a fresh baseline after maintenance before comparing subsequent events. | No automatic maintenance-triggered reconnect policy or measured timing advantage. |

The earlier statement that `TCP_NODELAY` was already enabled by default was
incorrect: in tokio-tungstenite 0.26.2, `connect_async` passes
`disable_nagle = false`. I would need to configure it explicitly and measure
its effect. [Dependency source][2]

For maintenance monitoring, I would use fields such as `exchange_active` and
`trading_active`, rather than the previously listed string states. [Exchange
status API][3] I would also account for the documented possibility of market
clocks decreasing during maintenance when designing recovery. [Orderbook
update rules][4]

## My conclusion

I have implemented redundancy and recovery, but I have not demonstrated a
production latency improvement. Before optimizing, I would add timestamp
provenance, per-connection event identity, and monotonic measurements of local
processing and output stages. I would then publish the capture conditions,
sample counts, p50/p95/p99 results, and recovery observations alongside the raw
data. Local benchmark completion would remain separate from those live-feed
measurements.

[1]: https://docs.kalshi.com/asyncapi.yaml
[2]: https://docs.rs/tokio-tungstenite/0.26.2/src/tokio_tungstenite/connect.rs.html
[3]: https://docs.kalshi.com/api-reference/exchange/get-exchange-status
[4]: https://docs.kalshi.com/websockets/orderbook-updates
