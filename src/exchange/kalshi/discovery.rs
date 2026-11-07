//! Kalshi market discovery.
//!
//! Kalshi's `/markets` endpoint is paginated by an opaque cursor. Discovery
//! requests open markets with the server-side multivariate-event exclusion
//! and repeats the same filter locally. Discovery is independent of
//! websocket subscriptions so a transient REST failure never tears down
//! live books.

use std::collections::BTreeSet;

use anyhow::{Context, bail};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, warn};
use url::Url;

use crate::{
    exchange::{DiscoveryResult, DiscoveryStats, kalshi::KalshiExchange},
    fixed::parse_micros,
    model::{MarketInfo, NumericSpec, PriceRange, PriceUnit, QuantityUnit, Ratio},
    time::{now_us, parse_rfc3339_us},
};

/// Convert one discovery page, isolating malformed records.
///
/// A record that fails conversion is counted and skipped with a warning so a
/// single bad entry cannot abort reconciliation for every valid market. If
/// every record on a non-empty page fails, the response is untrustworthy and
/// an error is returned so the round is retried instead of silently
/// reconciling a partial set. Records removed by the active/MVE filters
/// are not failures.
fn convert_kalshi_page(
    page: Vec<RawMarket>,
    max_markets: usize,
    markets: &mut Vec<MarketInfo>,
    skipped: &mut u64,
) -> anyhow::Result<()> {
    let mut processed = 0usize;
    let mut page_skipped = 0usize;
    for raw in page {
        match raw.into_market() {
            Ok(Some(info)) => {
                processed += 1;
                markets.push(info);
                if max_markets != 0 && markets.len() >= max_markets {
                    break;
                }
            }
            // Removed by the active/MVE filters: processed, not a failure.
            Ok(None) => processed += 1,
            Err(error) => {
                page_skipped += 1;
                warn!(error = %format!("{error:#}"), "skipping malformed Kalshi market record");
            }
        }
    }
    *skipped += page_skipped as u64;
    if processed == 0 && page_skipped > 0 {
        bail!(
            "every convertible Kalshi market on a discovery page ({page_skipped} records) \
             failed to decode"
        );
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct MarketsPage {
    markets: Vec<RawMarket>,
    cursor: Value,
}

fn parse_page_cursor(value: Value) -> anyhow::Result<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(cursor) if cursor.is_empty() => Ok(None),
        Value::String(cursor) => Ok(Some(cursor)),
        _ => bail!("Kalshi discovery cursor must be a string or null"),
    }
}

const MAX_PAGE_ATTEMPTS: u32 = 5;

#[derive(Debug, Deserialize)]
struct RawMarket {
    ticker: String,
    #[serde(default)]
    event_ticker: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    yes_sub_title: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    market_type: Option<String>,
    #[serde(default)]
    close_time: Option<String>,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
    #[serde(default)]
    mve_selected_legs: Option<Value>,
    #[serde(default)]
    price_ranges: Option<Vec<RawPriceRange>>,
    #[serde(default)]
    price_level_structure: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawPriceRange {
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default)]
    step: Option<String>,
}

impl KalshiExchange {
    pub(crate) async fn fetch_markets(
        &self,
        max_markets: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        let base = Url::parse(&format!(
            "{}/",
            self.settings()
                .environment
                .rest_base()
                .trim_end_matches('/')
        ))
        .context("invalid Kalshi REST base URL")?;

        self.fetch_markets_from(&base, max_markets).await
    }

    async fn fetch_markets_from(
        &self,
        base: &Url,
        max_markets: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        let started = std::time::Instant::now();
        let started_at_us = now_us().unwrap_or(0);
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut markets = Vec::new();
        let mut pages = 0usize;
        let mut skipped = 0u64;
        let mut retries = 0u64;
        loop {
            if max_markets != 0 && markets.len() >= max_markets {
                break;
            }
            let mut url = base
                .join("markets")
                .context("building Kalshi markets URL")?;
            {
                let mut query = url.query_pairs_mut();
                query
                    .append_pair("status", "open")
                    .append_pair("mve_filter", "exclude")
                    .append_pair("limit", &self.settings().discovery_page_size.to_string());
                if let Some(cursor) = &cursor {
                    query.append_pair("cursor", cursor);
                }
            }
            let (page, page_retries) = self.fetch_page(&url).await?;
            retries = retries.saturating_add(page_retries);
            pages = pages.saturating_add(1);
            let page_cursor = parse_page_cursor(page.cursor)?;
            debug!(
                page = pages,
                count = page.markets.len(),
                cursor = page_cursor.as_deref().unwrap_or(""),
                "Kalshi discovery page"
            );
            convert_kalshi_page(page.markets, max_markets, &mut markets, &mut skipped)?;

            if max_markets != 0 && markets.len() >= max_markets {
                break;
            }

            let Some(next) = page_cursor.filter(|cursor| !cursor.is_empty()) else {
                break;
            };
            if !seen_cursors.insert(next.clone()) {
                // A repeated cursor means the page set is incomplete. Returning
                // it would make every symbol outside the partial set look
                // removed, so fail the whole discovery round instead.
                bail!("Kalshi discovery cursor repeated; refusing partial market set");
            }
            cursor = Some(next);
            if self.settings().discovery_page_delay_ms > 0 {
                sleep(std::time::Duration::from_millis(
                    self.settings().discovery_page_delay_ms,
                ))
                .await;
            }
        }
        markets.sort_by(|left, right| left.native_symbol.cmp(&right.native_symbol));
        if let Some(pair) = markets
            .windows(2)
            .find(|pair| pair[0].native_symbol == pair[1].native_symbol)
        {
            bail!(
                "Kalshi discovery returned duplicate ticker `{}`",
                pair[0].native_symbol
            );
        }
        if skipped > 0 {
            warn!(
                skipped,
                "Kalshi discovery skipped malformed market records this round"
            );
        }
        let finished_at_us = now_us().unwrap_or(started_at_us);
        Ok(DiscoveryResult {
            stats: DiscoveryStats {
                pages: pages as u64,
                // Kalshi publishes one symbol per market record.
                selected_markets: markets.len() as u64,
                outcome_symbols: markets.len() as u64,
                malformed_records: skipped,
                retries,
                duration_ms: started.elapsed().as_millis() as u64,
                started_at_us,
                finished_at_us,
            },
            markets,
        })
    }

    async fn fetch_page(&self, url: &Url) -> anyhow::Result<(MarketsPage, u64)> {
        let mut last_error = None;
        for attempt in 0..MAX_PAGE_ATTEMPTS {
            let mut request = self.client().get(url.clone());
            if let Some(auth) = self.auth() {
                let headers = auth
                    .headers("GET", url.path())
                    .context("signing Kalshi discovery request")?;
                request = headers.apply_rest(request);
            }
            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let retry_after = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok());
                    if status.is_success() {
                        let body = response
                            .text()
                            .await
                            .context("reading Kalshi discovery response")?;
                        let page = serde_json::from_str(&body).with_context(|| {
                            format!("decoding Kalshi discovery page from {url}")
                        })?;
                        return Ok((page, u64::from(attempt)));
                    }
                    let retryable = retryable_status(status.as_u16());
                    let body = response.text().await.unwrap_or_default();
                    let message = format!(
                        "Kalshi discovery HTTP {}: {}",
                        status.as_u16(),
                        body.chars().take(240).collect::<String>()
                    );
                    if !retryable {
                        bail!("{message}");
                    }
                    last_error = Some(message);
                    if let Some(delay) = retry_delay_before_next_attempt(attempt, retry_after) {
                        sleep(delay).await;
                    }
                    continue;
                }
                Err(error) => {
                    last_error = Some(format!("Kalshi discovery request failed: {error}"));
                }
            }
            if let Some(delay) = retry_delay_before_next_attempt(attempt, None) {
                sleep(delay).await;
            }
        }
        Err(anyhow::anyhow!(
            "{}",
            last_error.unwrap_or_else(|| "Kalshi discovery failed".to_owned())
        ))
    }
}

/// Retry transient discovery responses, including an explicit timeout.
fn retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..=599).contains(&status)
}

/// Return the backoff delay before the next attempt, or `None` when the
/// current attempt was the last one. Keeping this decision in one place makes
/// the absent final sleep directly testable instead of inferring it from wall
/// clock time.
fn retry_delay_before_next_attempt(
    attempt: u32,
    retry_after: Option<u64>,
) -> Option<std::time::Duration> {
    (attempt + 1 < MAX_PAGE_ATTEMPTS).then(|| retry_delay(attempt, retry_after))
}

/// Honor a server `Retry-After` hint, capped so a hostile or mistaken value
/// cannot stall a discovery round indefinitely.
fn retry_delay(attempt: u32, retry_after: Option<u64>) -> std::time::Duration {
    match retry_after {
        Some(seconds) => std::time::Duration::from_secs(seconds.min(30)),
        None => std::time::Duration::from_millis(500u64.saturating_mul(1u64 << attempt.min(4))),
    }
}

impl RawMarket {
    fn into_market(self) -> anyhow::Result<Option<MarketInfo>> {
        if self.market_type.as_deref() == Some("mve")
            || self
                .mve_collection_ticker
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            || self
                .mve_selected_legs
                .as_ref()
                .is_some_and(|legs| match legs {
                    Value::Array(legs) => !legs.is_empty(),
                    Value::Null => false,
                    _ => true,
                })
        {
            return Ok(None);
        }
        let status = self
            .status
            .as_deref()
            .context("Kalshi market discovery entry is missing status")?;
        if status != "active" {
            return Ok(None);
        }
        if self.ticker.trim().is_empty() {
            bail!("Kalshi market discovery entry has an empty ticker");
        }
        let title = self
            .title
            .filter(|title| !title.trim().is_empty())
            .or_else(|| self.yes_sub_title.filter(|title| !title.trim().is_empty()));
        let close_time_us = self
            .close_time
            .as_deref()
            .map(parse_rfc3339_us)
            .transpose()
            .context("invalid Kalshi close_time")?;
        let price_ranges = parse_price_ranges(self.price_ranges.as_deref())?;
        let tick_size_us = price_ranges.iter().map(|range| range.step_us).min();
        let numeric_spec = NumericSpec {
            price_scale: 6,
            quantity_scale: 6,
            price_unit: PriceUnit::Currency("USD".to_owned()),
            quantity_unit: QuantityUnit::Contracts,
            tick_schedule: price_ranges
                .iter()
                .copied()
                .map(PriceRange::to_band)
                .collect(),
            quantity_step: None,
            lot_size: None,
            contract_multiplier: Some(Ratio::new(1, 1)?),
        };
        let info = MarketInfo {
            exchange: "kalshi".to_owned(),
            native_symbol: self.ticker.clone(),
            symbol: self.ticker,
            title,
            outcome: None,
            exchange_market_id: self.event_ticker,
            close_time_us,
            tick_size_us,
            price_ranges,
            price_level_structure: self.price_level_structure,
            numeric_spec,
        };
        info.validate()?;
        Ok(Some(info))
    }
}

fn parse_price_ranges(ranges: Option<&[RawPriceRange]>) -> anyhow::Result<Vec<PriceRange>> {
    let Some(ranges) = ranges else {
        // Older API responses may omit price_ranges. Do not guess a tick size
        // from the human-readable structure label.
        return Ok(Vec::new());
    };
    if ranges.is_empty() {
        bail!("Kalshi price_ranges is present but empty");
    }
    ranges
        .iter()
        .map(|range| {
            let start = range
                .start
                .as_deref()
                .context("Kalshi price range missing start")?;
            let end = range
                .end
                .as_deref()
                .context("Kalshi price range missing end")?;
            let step = range
                .step
                .as_deref()
                .context("Kalshi price range missing step")?;
            let range = PriceRange {
                start_us: parse_micros(start).context("invalid Kalshi price range start")?,
                end_us: parse_micros(end).context("invalid Kalshi price range end")?,
                step_us: parse_micros(step).context("invalid Kalshi price range step")?,
            };
            range.validate_probability()?;
            Ok(range)
        })
        .collect()
}

#[cfg(test)]
mod tests;
