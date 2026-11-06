# Deployment Notes

## Binary Installation

```bash
# Build release binary
cargo build --release --locked

# Install
sudo install -m 755 target/release/engineering-challenge /usr/local/bin/
sudo useradd --system --home-dir /var/lib/engineering-challenge --shell /usr/sbin/nologin collector
sudo mkdir -p /etc/engineering-challenge
sudo install -m 644 config.example.toml /etc/engineering-challenge/settings.toml
sudo install -d -o collector -g collector /var/lib/engineering-challenge /var/log/engineering-challenge
```

## Directory Layout

```
/etc/engineering-challenge/
  settings.toml          # Main configuration (root readable)

/var/lib/engineering-challenge/
  capture.sbe            # Primary SBE output (rotated segments here)
  capture.manifest.json  # Rotation manifest
  metadata.json          # Metadata sidecar (latest-wins)
  catalog.json           # Catalog checkpoint (latest-wins)

/var/log/engineering-challenge/
  collector.log          # Structured JSON logs (stdout/stderr captured by journald)
```

## systemd Service

Install `deploy/engineering-challenge.service` to `/etc/systemd/system/` and enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now engineering-challenge
```

### Service Type

The service uses `Type=simple` (not `notify`) because the collector does not implement `sd_notify`. Readiness is determined via the HTTP `/readyz` endpoint, not systemd readiness.

### Resource Limits

| Limit | Value | Rationale |
|-------|-------|-----------|
| `LimitNOFILE` | 65536 | 512 connections × 2 (read/write) + files |
| `LimitNPROC` | 4096 | Worker tasks + coordinator |
| `MemoryMax` | 4G | Covers 100k symbols (~700 MB) + headroom |
| `CPUQuota` | 200% | 2 cores for coordinator + writer + workers |

Adjust `MemoryMax` for larger deployments (full universe would need ~2.2 GB + overhead).

## Configuration Management

### Settings File

`/etc/engineering-challenge/settings.toml` contains all runtime configuration. Key sections:

```toml
[collector]
shards = 0                    # 0 = auto-derive from max_symbols_per_socket
replicas = 2
max_symbols_per_socket = 5000
max_shards = 64
max_total_connections = 512
max_estimated_book_memory_mb = 4096
max_snapshot_bytes_per_sec = 67108864
snapshot_interval_secs = 60
discovery_interval_secs = 60
duration_secs = 0             # 0 = run until signal
final_snapshots = true

[output]
path = "/var/lib/engineering-challenge/capture.sbe"
chunk_size = 67108864           # 64 MiB; template-reader compatible
rotate_bytes = 1073741824     # 1 GiB segments
rotate_secs = 3600            # 1 hour max segment age
min_free_bytes = 10737418240  # 10 GiB watermark
fsync_interval_secs = 10
overwrite = false             # Safety: refuse existing artifacts
metadata_path = "/var/lib/engineering-challenge/metadata.json"
report_path = "/var/lib/engineering-challenge/report.json"

[metrics]
listen = "127.0.0.1:9090"     # Bind to localhost; expose via sidecar/proxy

[network]
connect_timeout_secs = 10
request_timeout_secs = 30
```

### Exchange-Specific Settings

```toml
# Polymarket
[polymarket]
gamma_url = "https://gamma-api.polymarket.com"
ws_url = "wss://ws.polymarket.com/ws"
batch_size = 500
discovery_page_size = 100
request_timeout_secs = 5
discovery_page_delay_ms = 0
require_accepting_orders = true

# Kalshi
[kalshi]
base_url = "https://api.elections.kalshi.com"
ws_url = "wss://api.elections.kalshi.com/ws"
email = "collector@example.com"
key_file = "/etc/engineering-challenge/kalshi-key.pem"
key_id = "your-key-id"
request_timeout_secs = 5
```

### Secrets

- Kalshi private key: `/etc/engineering-challenge/kalshi-key.pem` (mode 600, owned by collector)
- Never commit secrets to version control. Use `systemd` `LoadCredential` or external secret manager.

## Log Rotation

The collector writes structured JSON logs to stdout. Configure `journald` or a log shipper (Vector, Fluent Bit) to rotate:

```ini
# /etc/systemd/journald.conf.d/engineering-challenge.conf
[Journal]
SystemMaxUse=2G
MaxFileSec=1d
ForwardToSyslog=no
```

Or use a sidecar container in Kubernetes to tail `/var/log/engineering-challenge/collector.log`.

## Monitoring

### Prometheus Scrape Config

```yaml
scrape_configs:
- job_name: engineering-challenge
  static_configs:
  - targets: ['localhost:9090']
  metrics_path: /metrics
  scrape_interval: 15s
  scrape_timeout: 10s
```

### Alert Rules

Define Prometheus Alertmanager rules for the readiness and failure signals
described in `docs/operating-envelope.md` and exposed by the metrics endpoint.

### Grafana Dashboards

Key panels:
- Delta emission rate (`rate(collector_deltas_emitted_total[1m])`)
- Anchor fraction (`collector_symbols_anchored / collector_symbols_expected`)
- Source connectivity (`collector_sources_connected / collector_sources_configured`)
- Writer queue depth (`collector_writer_queue_depth`)
- Catalog age (`collector_catalog_age_seconds`)
- Memory usage (process RSS from node_exporter)

## Health Checks

| Endpoint | Purpose | Expected | Timeout |
|----------|---------|----------|---------|
| `GET /livez` | Liveness (k8s livenessProbe) | 200 OK, `{"status":"alive"}` | 2s |
| `GET /readyz` | Readiness (k8s readinessProbe, LB) | 200 OK, `{"ready":true}` | 5s |
| `GET /healthz` | Source health (legacy) | 200 OK, `{"status":"ok"}` | 5s |
| `GET /metrics` | Prometheus scrape | 200 OK, text format | 10s |

**Do not** route traffic to the collector until `/readyz` returns `ready: true`.

## Capacity Planning

### Minimum Resources (1k symbols)

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 0.5 cores | 1 core |
| Memory | 50 MB | 256 MB |
| Disk | 100 MB | 1 GB |
| Network | 1 Mbps | 10 Mbps |

### 100k Symbols (Measured)

| Resource | Measured | Recommended |
|----------|----------|-------------|
| CPU | ~1.5 cores | 4 cores |
| Memory | 696 MB RSS | 2 GB |
| Disk | 25 MB/s burst | 100 GB SSD |
| Network | 50 Mbps | 1 Gbps |

### Full Universe (548k symbols, Projected)

| Resource | Projected | Notes |
|----------|-----------|-------|
| CPU | 8+ cores | Coordinator tick loop scales with shards |
| Memory | 2.2 GB + overhead | 4 GB minimum |
| Disk | 140 MB/s snapshots | NVMe required |
| Network | 500 Mbps | 10 Gbps recommended |
| FDs | 110 shards × 2 replicas = 220 | `max_total_connections` must increase |

**Full universe not validated.** Do not deploy without measured evidence.

## Rolling Updates

1. Drain traffic: remove from load balancer (wait for `/readyz` to go 503).
2. Stop service: `systemctl stop engineering-challenge`.
3. Replace binary and/or config.
4. Start service: `systemctl start engineering-challenge`.
5. Wait for `/readyz` to return `ready: true`.
6. Re-add to load balancer.

**Zero-downtime not supported** for config changes that alter shard count or catalog mode. The collector must rebuild subscriptions from scratch.

## Backup and Recovery

### Capture Files

- SBE captures are append-only; back up the entire `/var/lib/engineering-challenge/` directory.
- Rotation segments are independently verifiable; no need to back up partial (`.partial`) files.
- Manifest (`capture.manifest.json`) enables integrity verification of the full chain.

### Metadata/Catalog

- `metadata.json` and `catalog.json` are latest-wins; back up periodically.
- On restore, the collector will use the catalog checkpoint if `catalog_mode = warm|require|stale` and the checksum verifies.

### Disaster Recovery

1. Restore `/var/lib/engineering-challenge/` from backup.
2. Ensure `settings.toml` matches the original run (especially `catalog_mode` and `catalog_path`).
3. Start collector. It will resume from the catalog checkpoint (if valid) or run fresh discovery.

## Security Checklist

- [ ] Binary built with `cargo build --release --locked` on trusted machine.
- [ ] `cargo audit` passes (no known vulnerabilities in dependencies).
- [ ] `cargo deny check` passes (license, bans, sources).
- [ ] Kalshi private key stored at `/etc/engineering-challenge/kalshi-key.pem` mode 600.
- [ ] Service runs as unprivileged `collector` user.
- [ ] systemd hardening enabled (see service file).
- [ ] Output directory on dedicated mount with `noexec,nodev,nosuid`.
- [ ] Metrics endpoint bound to localhost only (expose via sidecar).
- [ ] Log shipping configured (no local log retention beyond 7 days).
- [ ] Alerting configured for all critical metrics (see Metric Runbook).

## Unresolved Deployment Items

| Item | Status | Owner |
|------|--------|-------|
| systemd `sd_notify` integration | Not implemented | — |
| Kubernetes Helm chart | Not created | — |
| Log rotation config (logrotate) | Not created | — |
| SELinux/AppArmor profile | Not created | — |
| Supply chain verification (SLSA) | Not configured | — |
| Dependency audit in CI | Not configured | — |

These must be completed before production deployment.
