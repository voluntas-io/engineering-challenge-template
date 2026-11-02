//! Small helpers shared by the exchange websocket sessions.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt, stream::FuturesUnordered};
use tokio::net::{TcpSocket, TcpStream, lookup_host};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{self, Message},
};
use url::Url;

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One application payload read from a websocket.
#[derive(Debug)]
pub enum WsPayload {
    Text(String),
    /// Binary data frame, preserved as received.
    Binary(Vec<u8>),
    /// Control/heartbeat frame; the connection is still alive.
    Idle,
}

/// Transport controls for a websocket connection.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Set `TCP_NODELAY` on the connected socket.
    pub tcp_nodelay: bool,
    /// Local IP to bind before connecting.
    pub source_ip: Option<IpAddr>,
    /// Fixed local source port, or an ephemeral port when absent.
    pub source_port: Option<u16>,
    /// Number of simultaneous handshakes to race. The first success wins and
    /// every other attempt is dropped, closing its socket.
    pub attempts: usize,
    /// Deadline for the whole attempt: DNS resolution, TCP connect, TLS
    /// negotiation, and the websocket upgrade, across every raced candidate.
    pub connect_timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            source_ip: None,
            source_port: None,
            attempts: 1,
            connect_timeout: Duration::from_secs(30),
        }
    }
}

impl ConnectOptions {
    /// Derive the options for one connection identified by `ordinal`.
    ///
    /// A configured source port is a base: connection *n* binds
    /// `base + ordinal`, so parallel connections never collide on one local
    /// port. The caller must pass a distinct ordinal per connection (for
    /// example `shard * 256 + replica`).
    pub fn for_ordinal(&self, ordinal: u32) -> anyhow::Result<Self> {
        let mut options = self.clone();
        if let Some(base) = options.source_port {
            let port = u32::from(base)
                .checked_add(ordinal)
                .and_then(|port| u16::try_from(port).ok())
                .with_context(|| {
                    format!(
                        "source port base {base} leaves no room for connection ordinal {ordinal} \
                         (the effective port must stay within 65535)"
                    )
                })?;
            options.source_port = Some(port);
        }
        Ok(options)
    }
}

/// Open a websocket, optionally adding handshake headers.
///
/// The TCP connection is established before the upgrade so the collector can apply
/// `TCP_NODELAY`, bind a source address, and race several full handshakes.
/// Racing covers TLS and the websocket upgrade as well as the TCP handshake,
/// so a connection is only selected after it is usable.
///
/// The entire attempt -- name resolution through the websocket upgrade,
/// across every raced candidate -- is bounded by
/// [`ConnectOptions::connect_timeout`]; exceeding it returns an error so the
/// caller can retry with backoff like any other transient failure.
pub async fn connect(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    options: &ConnectOptions,
) -> anyhow::Result<WsStream> {
    match tokio::time::timeout(
        options.connect_timeout,
        connect_inner(url, headers, options),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "websocket connection to {url} timed out after {:?}",
            options.connect_timeout
        ),
    }
}

async fn connect_inner(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    options: &ConnectOptions,
) -> anyhow::Result<WsStream> {
    let parsed = Url::parse(url).with_context(|| format!("parsing websocket URL {url}"))?;
    let raw_host = parsed
        .host_str()
        .with_context(|| format!("websocket URL {url} has no host"))?;
    // `Url::host_str` keeps the brackets on IPv6 literals; `lookup_host`
    // expects the bare address.
    let host = raw_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(raw_host);
    let port = parsed
        .port_or_known_default()
        .with_context(|| format!("websocket URL {url} has no known port"))?;
    let mut addresses: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {host}:{port}"))?
        .collect();
    if let Some(source_ip) = options.source_ip {
        addresses.retain(|address| address.is_ipv4() == source_ip.is_ipv4());
        anyhow::ensure!(
            !addresses.is_empty(),
            "no resolved address for {host}:{port} matches source IP {source_ip}"
        );
    }
    anyhow::ensure!(
        !addresses.is_empty(),
        "resolving {host}:{port} returned no addresses"
    );

    let attempts = options.attempts.max(1);
    let mut pending = FuturesUnordered::new();
    for attempt in 0..attempts {
        let address = addresses[attempt % addresses.len()];
        let options = options.clone();
        let url = url.to_owned();
        let headers = headers.to_vec();
        pending.push(async move {
            connect_once(&url, &headers, &options, address)
                .await
                .with_context(|| format!("connection attempt {} to {address}", attempt + 1))
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = pending.next().await {
        match result {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                // An authentication rejection is definitive; do not spend the
                // remaining attempts or hide the HTTP status in an aggregate.
                if matches!(http_status(&error), Some(401 | 403)) {
                    return Err(error);
                }
                errors.push(format!("{error:#}"));
            }
        }
    }
    bail!(
        "all {attempts} websocket connection attempt(s) to {url} failed: {}",
        errors.join("; ")
    )
}

async fn connect_once(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    options: &ConnectOptions,
    address: SocketAddr,
) -> anyhow::Result<WsStream> {
    let socket = match address {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }
    .context("creating TCP socket")?;
    let local_ip = options.source_ip.unwrap_or(match address {
        SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    });
    socket
        .bind(SocketAddr::new(local_ip, options.source_port.unwrap_or(0)))
        .with_context(|| {
            format!(
                "binding local address {local_ip}:{}",
                options.source_port.unwrap_or(0)
            )
        })?;
    let stream = socket
        .connect(address)
        .await
        .with_context(|| format!("TCP connect to {address}"))?;
    stream
        .set_nodelay(options.tcp_nodelay)
        .context("setting TCP_NODELAY")?;

    let mut request = url
        .into_client_request()
        .with_context(|| format!("building websocket request for {url}"))?;
    for (name, value) in headers {
        request.headers_mut().insert(name, value.clone());
    }
    let (stream, response) = client_async_tls_with_config(request, stream, None, None)
        .await
        .with_context(|| format!("websocket handshake with {url}"))?;
    let status = response.status();
    if status != http::StatusCode::SWITCHING_PROTOCOLS {
        bail!("websocket handshake returned HTTP {status}");
    }
    Ok(stream)
}

/// Extract an HTTP status from a failed websocket handshake, if the underlying
/// tungstenite error carried one. Authentication failures can then be treated as
/// fatal instead of being retried forever.
pub fn http_status(error: &anyhow::Error) -> Option<u16> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<tungstenite::Error>()
            .and_then(|error| match error {
                tungstenite::Error::Http(response) => Some(response.status().as_u16()),
                _ => None,
            })
    })
}

/// Read the next data payload, transparently answering control-frame pings.
pub async fn next_payload(stream: &mut WsStream) -> anyhow::Result<WsPayload> {
    loop {
        let message = stream.next().await.context("websocket stream ended")??;
        match message {
            Message::Text(text) => return Ok(WsPayload::Text(text.to_string())),
            Message::Binary(bytes) => return Ok(WsPayload::Binary(bytes.to_vec())),
            Message::Ping(payload) => {
                stream.send(Message::Pong(payload)).await?;
                return Ok(WsPayload::Idle);
            }
            Message::Pong(_) => return Ok(WsPayload::Idle),
            Message::Close(frame) => {
                let detail = frame.map_or_else(
                    || "without frame".to_owned(),
                    |f| format!("code={} reason={}", f.code, f.reason),
                );
                bail!("websocket closed by peer ({detail})");
            }
            Message::Frame(_) => {}
        }
    }
}

pub async fn send_text(stream: &mut WsStream, text: String) -> anyhow::Result<()> {
    stream.send(Message::Text(text.into())).await?;
    Ok(())
}

pub async fn send_ping(stream: &mut WsStream) -> anyhow::Result<()> {
    stream.send(Message::Ping(Vec::new().into())).await?;
    Ok(())
}

pub async fn close(mut stream: WsStream) -> anyhow::Result<()> {
    // Best effort: a peer that already vanished must not turn shutdown into an
    // error path.
    let _ = stream.close(None).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_connect_options_disable_nagle_and_do_not_race() {
        let options = ConnectOptions::default();
        assert!(options.tcp_nodelay);
        assert_eq!(options.attempts, 1);
        assert!(options.source_ip.is_none());
        assert!(options.source_port.is_none());
        assert!(options.connect_timeout > Duration::ZERO);
    }

    #[tokio::test]
    async fn stalled_handshake_is_bounded_by_the_connect_timeout() {
        // A server that accepts the TCP connection but never answers the
        // websocket upgrade must not hang the caller past the deadline.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncReadExt as _;
                let mut buffer = [0u8; 1024];
                let _ = stream.read(&mut buffer).await;
                std::future::pending::<()>().await;
            }
        });

        let options = ConnectOptions {
            connect_timeout: Duration::from_millis(250),
            ..ConnectOptions::default()
        };
        let started = tokio::time::Instant::now();
        let error = connect(&format!("ws://{address}"), &[], &options)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("timed out"),
            "unexpected error: {error:#}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the deadline must bound the attempt, took {:?}",
            started.elapsed()
        );
        server.abort();
    }

    #[tokio::test]
    async fn dropping_a_read_future_loses_no_frame() {
        // The session supervisor may drop a frame future when another
        // `select!` branch wins. The read state lives in the stream, so the
        // dropped poll must not consume or lose either frame.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            ws.send(Message::Text("first".into())).await.unwrap();
            ws.send(Message::Text("second".into())).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut stream = connect(&format!("ws://{address}"), &[], &ConnectOptions::default())
            .await
            .unwrap();
        // Poll once against an immediately-ready branch and drop the read
        // while it is still waiting for the first frame.
        tokio::select! {
            biased;
            _ = next_payload(&mut stream) => panic!("no frame should be ready yet"),
            _ = std::future::ready(()) => {}
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Text(text) => assert_eq!(text, "first"),
            WsPayload::Binary(_) | WsPayload::Idle => panic!("expected a text frame"),
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Text(text) => assert_eq!(text, "second"),
            WsPayload::Binary(_) | WsPayload::Idle => panic!("expected a text frame"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn websocket_ping_and_pong_are_reported_as_idle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.send(Message::Ping(b"keepalive".to_vec().into()))
                .await
                .unwrap();
            ws.send(Message::Pong(b"ack".to_vec().into()))
                .await
                .unwrap();
            ws.send(Message::Text("after-control".into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut stream = connect(&format!("ws://{address}"), &[], &ConnectOptions::default())
            .await
            .unwrap();
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Idle => {}
            WsPayload::Text(_) | WsPayload::Binary(_) => panic!("a ping must be idle"),
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Idle => {}
            WsPayload::Text(_) | WsPayload::Binary(_) => panic!("a pong must be idle"),
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Text(text) => assert_eq!(text, "after-control"),
            WsPayload::Binary(_) | WsPayload::Idle => panic!("expected a text frame"),
        }
        server.await.unwrap();
    }

    #[test]
    fn pinned_source_ports_are_derived_per_connection() {
        let options = ConnectOptions {
            source_port: Some(40_000),
            ..ConnectOptions::default()
        };
        assert_eq!(
            options.for_ordinal(0).unwrap().source_port,
            Some(40_000),
            "the first connection keeps the base port"
        );
        assert_eq!(
            options.for_ordinal(3).unwrap().source_port,
            Some(40_003),
            "later connections shift by their ordinal"
        );
        assert_eq!(
            options.for_ordinal(65_536).unwrap_err().to_string(),
            "source port base 40000 leaves no room for connection ordinal 65536 (the effective port must stay within 65535)"
        );
    }

    #[test]
    fn ephemeral_ports_ignore_the_ordinal() {
        let options = ConnectOptions::default();
        assert_eq!(options.for_ordinal(500).unwrap().source_port, None);
    }
}
