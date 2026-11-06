use super::*;

fn raw() -> RawMarket {
    RawMarket {
        ticker: "KXTEST-26-T1".to_owned(),
        event_ticker: Some("KXTEST-26".to_owned()),
        title: Some("Test market".to_owned()),
        yes_sub_title: None,
        status: Some("active".to_owned()),
        market_type: Some("binary".to_owned()),
        close_time: Some("2026-10-24T00:00:00Z".to_owned()),
        mve_collection_ticker: None,
        mve_selected_legs: None,
        price_ranges: Some(vec![RawPriceRange {
            start: Some("0.00".to_owned()),
            end: Some("1.00".to_owned()),
            step: Some("0.0100".to_owned()),
        }]),
        price_level_structure: Some("linear_cent".to_owned()),
    }
}

#[test]
fn converts_active_non_mve_market() {
    let info = raw().into_market().unwrap().unwrap();
    assert_eq!(info.symbol, "KXTEST-26-T1");
    assert_eq!(info.tick_size_us, Some(10_000));
    assert_eq!(info.exchange_market_id.as_deref(), Some("KXTEST-26"));
}

#[test]
fn rejects_inactive_and_mve_markets() {
    let mut market = raw();
    market.status = Some("closed".to_owned());
    assert!(market.into_market().unwrap().is_none());

    let mut market = raw();
    market.mve_collection_ticker = Some("KXMVECROSS".to_owned());
    assert!(market.into_market().unwrap().is_none());

    let mut market = raw();
    market.mve_selected_legs = Some(serde_json::json!([{"market":"X"}]));
    assert!(market.into_market().unwrap().is_none());
}

#[test]
fn preserves_tapered_price_ranges_instead_of_label_fallback() {
    let mut market = raw();
    market.price_level_structure = Some("tapered_deci_cent".to_owned());
    market.price_ranges = Some(vec![
        RawPriceRange {
            start: Some("0.00".to_owned()),
            end: Some("0.10".to_owned()),
            step: Some("0.001".to_owned()),
        },
        RawPriceRange {
            start: Some("0.10".to_owned()),
            end: Some("0.90".to_owned()),
            step: Some("0.01".to_owned()),
        },
        RawPriceRange {
            start: Some("0.90".to_owned()),
            end: Some("1.00".to_owned()),
            step: Some("0.001".to_owned()),
        },
    ]);
    let info = market.into_market().unwrap().unwrap();
    assert_eq!(info.tick_size_us, Some(1_000));
    assert_eq!(info.price_ranges.len(), 3);
    assert_eq!(info.price_ranges[1].step_us, 10_000);
    assert_eq!(
        info.price_level_structure.as_deref(),
        Some("tapered_deci_cent")
    );
}

#[test]
fn does_not_guess_tick_size_without_price_ranges() {
    let mut market = raw();
    market.price_ranges = None;
    market.price_level_structure = Some("center_whole_edge_half_cent".to_owned());
    let info = market.into_market().unwrap().unwrap();
    assert_eq!(info.tick_size_us, None);
    assert!(info.price_ranges.is_empty());
}

#[test]
fn rejects_malformed_price_ranges() {
    let mut market = raw();
    market.price_ranges = Some(vec![RawPriceRange {
        start: Some("0.00".to_owned()),
        end: Some("1.00".to_owned()),
        step: None,
    }]);
    assert!(market.into_market().is_err());
}

#[test]
fn one_malformed_record_does_not_abort_the_page() {
    let mut valid = raw();
    valid.ticker = "KXVALID-26-T1".to_owned();
    let mut malformed = raw();
    malformed.ticker = String::new();
    let mut markets = Vec::new();
    let mut skipped = 0u64;
    convert_kalshi_page(vec![malformed, valid], 0, &mut markets, &mut skipped).unwrap();
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].symbol, "KXVALID-26-T1");
    assert_eq!(skipped, 1);
}

#[test]
fn filtered_records_are_not_failures() {
    let mut inactive = raw();
    inactive.status = Some("settled".to_owned());
    let mut markets = Vec::new();
    let mut skipped = 0u64;
    convert_kalshi_page(vec![inactive], 0, &mut markets, &mut skipped).unwrap();
    assert!(markets.is_empty());
    assert_eq!(skipped, 0);
}

#[test]
fn a_page_of_only_malformed_records_fails_the_round() {
    let mut malformed_a = raw();
    malformed_a.status = None;
    let mut malformed_b = raw();
    malformed_b.price_ranges = Some(vec![RawPriceRange {
        start: None,
        end: None,
        step: None,
    }]);
    let mut markets = Vec::new();
    let mut skipped = 0u64;
    let error = convert_kalshi_page(
        vec![malformed_a, malformed_b],
        0,
        &mut markets,
        &mut skipped,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("failed to decode"),
        "unexpected error: {error:#}"
    );
    assert!(markets.is_empty());
    assert_eq!(skipped, 2);
}

#[test]
fn retry_policy_covers_timeouts_rate_limits_and_server_errors() {
    assert!(retryable_status(408));
    assert!(retryable_status(429));
    assert!(retryable_status(503));
    assert!(!retryable_status(400));
    assert!(!retryable_status(404));
    assert_eq!(retry_delay(0, Some(2)), std::time::Duration::from_secs(2));
    assert_eq!(
        retry_delay(0, Some(600)),
        std::time::Duration::from_secs(30),
        "a server hint must not stall discovery indefinitely"
    );
    assert_eq!(
        retry_delay(1, None),
        std::time::Duration::from_millis(1_000)
    );
}

#[tokio::test]
async fn fetch_page_retries_408_and_honors_retry_after() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&requests);
    tokio::spawn(async move {
        for response in [
            "HTTP/1.1 408 Request Timeout\r\nRetry-After: 0\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n"
                .to_owned(),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
             Content-Length: 28\r\nConnection: close\r\n\r\n\
             {\"markets\":[],\"cursor\":null}"
                .to_owned(),
        ] {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buffer = [0u8; 4096];
            let _ = socket.read(&mut buffer).await;
            observed.fetch_add(1, Ordering::SeqCst);
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        }
    });

    let exchange = KalshiExchange::new(
        crate::config::KalshiSettings::default(),
        crate::config::NetworkSettings::default(),
        false,
    )
    .unwrap();
    let url = Url::parse(&format!("http://{address}/markets")).unwrap();
    let (page, retries) =
        tokio::time::timeout(std::time::Duration::from_secs(5), exchange.fetch_page(&url))
            .await
            .expect("408 retry timed out")
            .unwrap();
    assert!(page.markets.is_empty());
    assert_eq!(retries, 1);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

fn test_exchange() -> crate::exchange::kalshi::KalshiExchange {
    let settings = crate::config::KalshiSettings {
        discovery_page_delay_ms: 0,
        discovery_page_size: 100,
        ..Default::default()
    };
    crate::exchange::kalshi::KalshiExchange::new(
        settings,
        crate::config::NetworkSettings::default(),
        false,
    )
    .unwrap()
}

fn market_json(ticker: &str) -> serde_json::Value {
    serde_json::json!({
        "ticker": ticker,
        "event_ticker": "EV",
        "title": format!("Market {ticker}"),
        "status": "active",
        "market_type": "binary",
        "close_time": "2027-01-01T00:00:00Z",
        "price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}],
        "price_level_structure": "linear_cent"
    })
}

fn market_json_status(ticker: &str, status: &str) -> serde_json::Value {
    let mut value = market_json(ticker);
    value["status"] = serde_json::json!(status);
    value
}

fn market_json_mve(ticker: &str) -> serde_json::Value {
    let mut value = market_json(ticker);
    value["mve_collection_ticker"] = serde_json::json!("KXMVECROSS");
    value
}

fn page_json(markets: Vec<serde_json::Value>, cursor: Option<&str>) -> String {
    serde_json::json!({"markets": markets, "cursor": cursor}).to_string()
}

fn http_response(status_line: &str, body: &str) -> Vec<u8> {
    format!(
        "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

async fn serve(responses: Vec<Vec<u8>>) -> (url::Url, tokio::task::JoinHandle<Vec<String>>) {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = url::Url::parse(&format!("http://{address}/")).unwrap();
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read until header terminator, bounded.
            loop {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    socket.read(&mut chunk),
                )
                .await
                {
                    Ok(Ok(n)) if n > 0 => {
                        buffer.extend_from_slice(&chunk[..n]);
                        if buffer.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            let request = String::from_utf8_lossy(&buffer).to_string();
            let request_line = request.lines().next().unwrap_or("").to_owned();
            requests.push(request_line);
            let _ = socket.write_all(&response).await;
            let _ = socket.shutdown().await;
        }
        requests
    });
    (base, handle)
}

fn parse_query(request_line: &str) -> Vec<(String, String)> {
    let target = request_line.split_whitespace().nth(1).unwrap_or("");
    let Some((_, query)) = target.split_once('?') else {
        return Vec::new();
    };
    url::form_urlencoded::parse(query.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

#[tokio::test]
async fn paginates_with_opaque_cursor_and_returns_sorted_markets() {
    let cursor = "opaque +/%?=&";
    let page1 = page_json(vec![market_json("Z-1"), market_json("A-1")], Some(cursor));
    let page2 = page_json(vec![market_json("M-1")], None);
    let (base, handle) = serve(vec![
        http_response("HTTP/1.1 200 OK", &page1),
        http_response("HTTP/1.1 200 OK", &page2),
    ])
    .await;
    let exchange = test_exchange();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        exchange.fetch_markets_from(&base, 0),
    )
    .await
    .expect("pagination timed out")
    .unwrap();
    let requests = handle.await.unwrap();
    assert_eq!(requests.len(), 2);
    let symbols: Vec<&str> = result
        .markets
        .iter()
        .map(|m| m.native_symbol.as_str())
        .collect();
    assert_eq!(symbols, vec!["A-1", "M-1", "Z-1"]);
    assert_eq!(result.stats.pages, 2);
    assert_eq!(result.stats.selected_markets, 3);
    assert_eq!(result.stats.outcome_symbols, 3);
    // First request omits cursor; every request carries the filters.
    for request in &requests {
        let query = parse_query(request);
        assert!(query.contains(&("status".to_owned(), "open".to_owned())));
        assert!(query.contains(&("mve_filter".to_owned(), "exclude".to_owned())));
        assert!(query.contains(&("limit".to_owned(), "100".to_owned())));
    }
    let first_query = parse_query(&requests[0]);
    assert!(first_query.iter().all(|(k, _)| k != "cursor"));
    let second_query = parse_query(&requests[1]);
    let cursor_param = second_query
        .iter()
        .find(|(k, _)| k == "cursor")
        .map(|(_, v)| v.clone())
        .expect("second request must carry cursor");
    assert_eq!(cursor_param, cursor);
}

#[tokio::test]
async fn repeated_cursor_rejects_uncapped_discovery_round() {
    let page1 = page_json(vec![market_json("A-1")], Some("cursor-1"));
    let page2 = page_json(vec![market_json("B-1")], Some("cursor-1"));
    let (base, _handle) = serve(vec![
        http_response("HTTP/1.1 200 OK", &page1),
        http_response("HTTP/1.1 200 OK", &page2),
    ])
    .await;
    let exchange = test_exchange();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        exchange.fetch_markets_from(&base, 0),
    )
    .await
    .expect("repeated cursor timed out");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cursor repeated"));
}

#[tokio::test]
async fn market_cap_counts_eligible_markets_and_terminates_round() {
    let page1 = page_json(
        vec![
            market_json_status("INACTIVE-1", "settled"),
            market_json_mve("MVE-1"),
            market_json("A-1"),
        ],
        Some("cursor-1"),
    );
    let page2 = page_json(
        vec![market_json("B-1"), market_json("C-1")],
        Some("cursor-1"),
    );
    let (base, handle) = serve(vec![
        http_response("HTTP/1.1 200 OK", &page1),
        http_response("HTTP/1.1 200 OK", &page2),
    ])
    .await;
    let exchange = test_exchange();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        exchange.fetch_markets_from(&base, 2),
    )
    .await
    .expect("capped discovery timed out")
    .unwrap();
    let requests = handle.await.unwrap();
    assert_eq!(requests.len(), 2);
    let symbols: Vec<&str> = result
        .markets
        .iter()
        .map(|m| m.native_symbol.as_str())
        .collect();
    assert_eq!(symbols, vec!["A-1", "B-1"]);
    assert_eq!(result.stats.pages, 2);
    assert_eq!(result.stats.selected_markets, 2);
    assert_eq!(result.stats.outcome_symbols, 2);
}

#[tokio::test]
async fn duplicate_tickers_across_pages_reject_discovery_round() {
    let page1 = page_json(vec![market_json("A-1")], Some("cursor-1"));
    let page2 = page_json(vec![market_json("A-1")], None);
    let (base, _handle) = serve(vec![
        http_response("HTTP/1.1 200 OK", &page1),
        http_response("HTTP/1.1 200 OK", &page2),
    ])
    .await;
    let exchange = test_exchange();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        exchange.fetch_markets_from(&base, 0),
    )
    .await
    .expect("duplicate ticker timed out");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("duplicate ticker"));
}

#[tokio::test]
async fn non_retryable_http_status_is_not_retried() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = std::sync::Arc::clone(&requests);
    let server = tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let mut buffer = [0u8; 4096];
        let _ = socket.read(&mut buffer).await;
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = socket
            .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 12\r\nConnection: close\r\n\r\nunauthorized")
            .await;
        let _ = socket.shutdown().await;
    });
    let exchange = test_exchange();
    let url = url::Url::parse(&format!("http://{address}/markets")).unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), exchange.fetch_page(&url))
        .await
        .expect("non-retryable timed out");
    assert!(result.is_err());
    let error = result.unwrap_err().to_string();
    assert!(error.contains("401"), "error must contain status: {error}");
    assert!(
        error.contains("unauthorized"),
        "error must contain body: {error}"
    );
    server.await.unwrap();
    assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 1);
}
