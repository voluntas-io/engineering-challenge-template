//! Polymarket Gamma discovery.
//!
//! Gamma is used instead of the CLOB `/markets` endpoint because the latter's
//! pagination can surface closed or stale markets. Each Gamma market contains
//! the per-outcome trading identifiers used by the CLOB market stream: the
//! JSON-encoded `clobTokenIds` for CTF markets, or the `positionIds` JSON
//! array for Protocol V2 markets (`version == "v2"`). Both identifier forms
//! are valid subscriptions for the raw market channel, which takes "a token
//! ID for a CTF market or a position ID for a Polymarket Protocol V2 market".
//!
//! One malformed market record is isolated: it is skipped with a visible
//! warning instead of aborting the whole discovery round, which would freeze
//! reconciliation for every valid market. A page whose records all fail
//! conversion is treated as an untrustworthy response and fails the round.

use std::collections::BTreeSet;

use anyhow::{Context, bail};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, warn};
use url::Url;

use crate::{
    exchange::{DiscoveryResult, DiscoveryStats},
    fixed::{SCALE, parse_micros},
    model::{MarketInfo, NumericSpec, PriceBand, PriceUnit, QuantityUnit, Ratio},
    time::{now_us, parse_rfc3339_us},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMarket {
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    title: Option<String>,
    // `slug` is not modeled: it is mutable presentation
    // metadata and must never influence the output symbol.
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    archived: Option<bool>,
    #[serde(default)]
    enable_order_book: Option<bool>,
    #[serde(default)]
    accepting_orders: Option<bool>,
    /// Market protocol generation: `v2` selects `positionIds`; a missing
    /// field (or `ctf`) selects the JSON-encoded `clobTokenIds`.
    #[serde(default)]
    version: Option<Value>,
    #[serde(default)]
    clob_token_ids: Option<Value>,
    #[serde(default)]
    position_ids: Option<Value>,
    #[serde(default)]
    outcomes: Option<Value>,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default)]
    end_date_iso: Option<String>,
    #[serde(default)]
    order_price_min_tick_size: Option<Value>,
}

/// One response from Gamma's keyset-paginated market endpoint.
///
/// `next_cursor` is documented as present only when the page is full. Treating
/// its absence as exhaustion is therefore correct for both a short final page
/// and an exact-multiple final page.
#[derive(Debug, Clone, Deserialize)]
struct KeysetPage {
    #[serde(default)]
    markets: Vec<RawMarket>,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// Outcome identifier flavour for one market record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentifierKind {
    /// JSON-encoded `clobTokenIds` (the default for CTF markets).
    Ctf,
    /// `positionIds` JSON array (Protocol V2 markets).
    PositionV2,
}

impl RawMarket {
    /// Select the identifier flavour from the market's declared `version`.
    fn identifier_kind(&self) -> anyhow::Result<IdentifierKind> {
        let raw = match &self.version {
            None | Some(Value::Null) => return Ok(IdentifierKind::Ctf),
            Some(Value::String(text)) => text.trim().to_ascii_lowercase(),
            Some(Value::Number(number)) => number.to_string(),
            Some(other) => bail!("Polymarket market version has unsupported type {other}"),
        };
        match raw.as_str() {
            "" | "ctf" | "v1" => Ok(IdentifierKind::Ctf),
            "v2" | "2" => Ok(IdentifierKind::PositionV2),
            other => bail!("unsupported Polymarket market version `{other}`"),
        }
    }

    fn into_markets(self, require_accepting_orders: bool) -> anyhow::Result<Vec<MarketInfo>> {
        // `acceptingOrders` is nullable in the Gamma schema. When the strict
        // policy is enabled, only an explicit `true` is accepted: a missing
        // value is unconfirmed and must not be silently treated as tradable.
        // Operators who need to include unknown records can disable the policy
        // and rely on the active/closed/archived/order-book filters.
        let accepting_orders_ok = !require_accepting_orders || self.accepting_orders == Some(true);
        if self.active == Some(false)
            || self.closed == Some(true)
            || self.archived == Some(true)
            || self.enable_order_book == Some(false)
            || !accepting_orders_ok
        {
            return Ok(Vec::new());
        }
        let identifier_kind = self.identifier_kind()?;
        let condition_id = self
            .condition_id
            .filter(|value| !value.trim().is_empty())
            .context("active Polymarket market is missing conditionId")?;
        let (identifier_field, identifiers) = match identifier_kind {
            IdentifierKind::Ctf => (
                "clobTokenIds",
                parse_string_array(self.clob_token_ids.as_ref())
                    .context("Polymarket clobTokenIds is malformed")?,
            ),
            IdentifierKind::PositionV2 => (
                "positionIds",
                parse_string_array(self.position_ids.as_ref())
                    .context("Polymarket positionIds is malformed")?,
            ),
        };
        let outcomes = parse_string_array(self.outcomes.as_ref())
            .context("Polymarket outcomes is malformed")?;
        if identifiers.is_empty() {
            bail!("active Polymarket market has no {identifier_field}");
        }
        if identifiers.len() != outcomes.len() {
            bail!(
                "Polymarket {identifier_field}/outcomes length mismatch: {} identifiers, {} outcomes",
                identifiers.len(),
                outcomes.len()
            );
        }
        if identifiers.iter().any(|id| id.trim().is_empty())
            || outcomes.iter().any(|outcome| outcome.trim().is_empty())
        {
            bail!("Polymarket market has an empty identifier or outcome");
        }
        let title = self
            .question
            .or(self.title)
            .filter(|title| !title.trim().is_empty());
        let close_time_us = match self.end_date.as_deref() {
            Some(value) if !value.trim().is_empty() => {
                Some(parse_rfc3339_us(value).context("invalid Polymarket endDate")?)
            }
            _ => match self.end_date_iso.as_deref() {
                Some(value) if !value.trim().is_empty() => Some(
                    parse_rfc3339_us(&format!("{value}T00:00:00Z"))
                        .context("invalid Polymarket endDateIso")?,
                ),
                _ => None,
            },
        };
        let tick_size_us = match self.order_price_min_tick_size.as_ref() {
            Some(value) => {
                let value = scalar_string(value)
                    .context("Polymarket orderPriceMinTickSize is not a string or number")?;
                let tick =
                    parse_micros(&value).context("invalid Polymarket orderPriceMinTickSize")?;
                anyhow::ensure!(
                    tick > 0,
                    "Polymarket orderPriceMinTickSize must be positive"
                );
                Some(tick)
            }
            None => None,
        };
        let tick_schedule = tick_size_us
            .map(|step| vec![PriceBand::from_micros(0, SCALE, step, 0)])
            .unwrap_or_default();
        let mut markets = Vec::with_capacity(identifiers.len());
        for (index, (identifier, outcome)) in identifiers.into_iter().zip(outcomes).enumerate() {
            // The output symbol is derived from immutable venue identity only:
            // the condition id and the outcome's array index. Slug and title
            // are mutable metadata; a slug change must never split one
            // market's SBE stream across two output symbols.
            let symbol = format!("polymarket:{condition_id}:{index}");
            let info = MarketInfo {
                exchange: "polymarket".to_owned(),
                native_symbol: identifier,
                symbol,
                title: title.clone(),
                outcome: Some(outcome),
                exchange_market_id: Some(condition_id.clone()),
                close_time_us,
                tick_size_us,
                price_ranges: Vec::new(),
                price_level_structure: None,
                numeric_spec: NumericSpec {
                    price_scale: 6,
                    quantity_scale: 6,
                    price_unit: PriceUnit::Currency("USDC".to_owned()),
                    quantity_unit: QuantityUnit::Contracts,
                    tick_schedule: tick_schedule.clone(),
                    quantity_step: None,
                    lot_size: None,
                    contract_multiplier: Some(Ratio::new(1, 1)?),
                },
            };
            info.validate()?;
            markets.push(info);
        }
        Ok(markets)
    }
}

fn parse_string_array(value: Option<&Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    match value {
        Value::String(text) => serde_json::from_str(text)
            .with_context(|| format!("invalid JSON array string `{text}`")),
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(ToOwned::to_owned)
                    .with_context(|| format!("array item is not a string: {item}"))
            })
            .collect(),
        _ => bail!("expected an array or JSON-encoded array"),
    }
}

fn scalar_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// Convert one discovery page, isolating malformed records.
///
/// A record that fails conversion is counted and skipped with a warning so a
/// single bad entry cannot abort reconciliation for every valid market. If
/// every record on a non-empty page fails, the response is untrustworthy and
/// an error is returned so the round is retried instead of silently
/// reconciling a partial set.
///
/// `max_markets` caps selected venue market records, not outcome tokens. A
/// selected market contributes its complete outcome set, so the cap only ever
/// stops between records. Policy-filtered records (closed, archived, not
/// accepting orders, no order book) are parsed but not selected, and they do
/// not consume the budget.
fn convert_page(
    page: Vec<RawMarket>,
    require_accepting_orders: bool,
    max_markets: usize,
    selected_records: &mut usize,
    markets: &mut Vec<MarketInfo>,
    skipped: &mut u64,
) -> anyhow::Result<()> {
    let mut converted = 0usize;
    let mut page_skipped = 0usize;
    for raw in page {
        if max_markets != 0 && *selected_records >= max_markets {
            break;
        }
        let infos = match raw.into_markets(require_accepting_orders) {
            Ok(infos) => infos,
            Err(error) => {
                page_skipped += 1;
                warn!(error = %format!("{error:#}"), "skipping malformed Polymarket market record");
                continue;
            }
        };
        converted += 1;
        if infos.is_empty() {
            // Parsed but filtered by policy; not selected.
            continue;
        }
        // Every outcome of a selected market is kept; the record cap is
        // never applied inside one market's outcome list.
        markets.extend(infos);
        *selected_records += 1;
    }
    *skipped += page_skipped as u64;
    if converted == 0 && page_skipped > 0 {
        bail!(
            "every Polymarket market on a discovery page ({page_skipped} records) \
             failed to decode"
        );
    }
    Ok(())
}

impl crate::exchange::polymarket::PolymarketExchange {
    pub(crate) async fn fetch_markets(
        &self,
        max_markets: usize,
    ) -> anyhow::Result<DiscoveryResult> {
        let started = std::time::Instant::now();
        let started_at_us = now_us().unwrap_or(0);
        let base = self.settings().gamma_url.trim_end_matches('/').to_owned();
        // Gamma's keyset endpoint rejects more than 100 records per page with
        // HTTP 422. The configuration validator enforces the same bound; the
        // clamp keeps a directly constructed settings value safe.
        let page_size = self.settings().discovery_page_size.clamp(1, 100);
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut markets = Vec::new();
        // `max_markets` is a venue-record budget. Outcome tokens are counted
        // separately because one selected market can contribute several.
        let mut selected_records = 0usize;
        let mut pages = 0usize;
        let mut skipped = 0u64;
        let mut retries = 0u64;
        const MAX_DISCOVERY_PAGES: usize = 10_000;
        loop {
            anyhow::ensure!(
                pages < MAX_DISCOVERY_PAGES,
                "Polymarket discovery exceeded {MAX_DISCOVERY_PAGES} pages"
            );
            if max_markets != 0 && selected_records >= max_markets {
                break;
            }
            let mut url = keyset_url(&base)?;
            {
                let mut query = url.query_pairs_mut();
                query
                    .append_pair("closed", "false")
                    .append_pair("limit", &page_size.to_string());
                if let Some(cursor) = &cursor {
                    query.append_pair("after_cursor", cursor);
                }
            }
            let (page, page_retries) = self.fetch_page(&url).await?;
            retries = retries.saturating_add(page_retries);
            pages = pages.saturating_add(1);
            let received = page.markets.len();
            let next_cursor = page.next_cursor.filter(|value| !value.is_empty());
            debug!(
                page = pages,
                count = received,
                has_next_cursor = next_cursor.is_some(),
                cursor = cursor.as_deref().unwrap_or(""),
                "Polymarket discovery page"
            );
            if received == 0 {
                anyhow::ensure!(
                    next_cursor.is_none(),
                    "Polymarket discovery returned an empty page with a next_cursor"
                );
                break;
            }
            convert_page(
                page.markets,
                self.settings().require_accepting_orders,
                max_markets,
                &mut selected_records,
                &mut markets,
                &mut skipped,
            )
            .with_context(|| format!("converting Polymarket discovery page {pages}"))?;
            debug!(
                selected_markets = selected_records,
                outcome_symbols = markets.len(),
                "Polymarket discovery selection"
            );
            if max_markets != 0 && selected_records >= max_markets {
                break;
            }
            let Some(next_cursor) = next_cursor else {
                break;
            };
            // A cursor is opaque, so the only safe cycle check is exact
            // repetition. Refuse to loop rather than silently under-collecting.
            anyhow::ensure!(
                seen_cursors.insert(next_cursor.clone()),
                "Polymarket discovery returned a repeated next_cursor"
            );
            cursor = Some(next_cursor);
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
                "Polymarket discovery returned duplicate token id `{}`",
                pair[0].native_symbol
            );
        }
        // Output symbols must also be unique: two distinct native identifiers
        // mapping to one SBE symbol would corrupt every downstream consumer
        // keyed on `symbol`.
        if let Some(pair) = markets
            .windows(2)
            .find(|pair| pair[0].symbol == pair[1].symbol)
        {
            bail!(
                "Polymarket discovery returned duplicate output symbol `{}`",
                pair[0].symbol
            );
        }
        if skipped > 0 {
            warn!(
                skipped,
                "Polymarket discovery skipped malformed market records this round"
            );
        }
        let finished_at_us = now_us().unwrap_or(started_at_us);
        Ok(DiscoveryResult {
            stats: DiscoveryStats {
                pages: pages as u64,
                selected_markets: selected_records as u64,
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

    async fn fetch_page(&self, url: &Url) -> anyhow::Result<(KeysetPage, u64)> {
        let mut last_error = None;
        for attempt in 0..5u32 {
            let request = self
                .client()
                .get(url.clone())
                .header(reqwest::header::ACCEPT, "application/json");
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
                            .context("reading Polymarket discovery response")?;
                        let page = serde_json::from_str(&body).with_context(|| {
                            format!("decoding Polymarket discovery page from {url}")
                        })?;
                        return Ok((page, u64::from(attempt)));
                    }
                    let retryable = status.as_u16() == 429
                        || status.as_u16() == 408
                        || status.is_server_error();
                    let body = response.text().await.unwrap_or_default();
                    let message = format!(
                        "Polymarket discovery HTTP {}: {}",
                        status.as_u16(),
                        body.chars().take(240).collect::<String>()
                    );
                    if !retryable {
                        bail!("{message}");
                    }
                    last_error = Some(message);
                    if let Some(seconds) = retry_after {
                        sleep(std::time::Duration::from_secs(seconds.min(30))).await;
                        continue;
                    }
                }
                Err(error) => {
                    last_error = Some(format!("Polymarket discovery request failed: {error}"));
                }
            }
            let backoff = 500u64.saturating_mul(1u64 << attempt.min(4));
            sleep(std::time::Duration::from_millis(backoff)).await;
        }
        Err(anyhow::anyhow!(
            "{}",
            last_error.unwrap_or_else(|| "Polymarket discovery failed".to_owned())
        ))
    }
}

/// Build the keyset endpoint beneath a configured Gamma base URL.
fn keyset_url(base: &str) -> anyhow::Result<Url> {
    let mut url = Url::parse(base).context("building Polymarket Gamma keyset URL")?;
    let path = format!("{}/markets/keyset", url.path().trim_end_matches('/'));
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };

    use super::*;

    fn raw() -> RawMarket {
        RawMarket {
            condition_id: Some("0xabc".to_owned()),
            question: Some("Will it rain?".to_owned()),
            title: None,
            active: Some(true),
            closed: Some(false),
            archived: Some(false),
            enable_order_book: Some(true),
            accepting_orders: Some(true),
            version: None,
            clob_token_ids: Some(Value::String(r#"["111","222"]"#.to_owned())),
            position_ids: None,
            outcomes: Some(Value::String(r#"["Yes","No"]"#.to_owned())),
            end_date: Some("2026-10-24T00:00:00Z".to_owned()),
            end_date_iso: Some("2026-10-24".to_owned()),
            order_price_min_tick_size: Some(Value::Number(
                serde_json::Number::from_f64(0.001).unwrap(),
            )),
        }
    }

    #[test]
    fn expands_outcome_tokens() {
        let markets = raw().into_markets(true).unwrap();
        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0].native_symbol, "111");
        assert_eq!(markets[0].outcome.as_deref(), Some("Yes"));
        assert_eq!(markets[1].native_symbol, "222");
        assert_eq!(markets[1].tick_size_us, Some(1_000));
        assert_eq!(markets[0].exchange_market_id.as_deref(), Some("0xabc"));
        // Output symbols are derived from immutable identity: condition id
        // plus outcome index. No slug, no title.
        assert_eq!(markets[0].symbol, "polymarket:0xabc:0");
        assert_eq!(markets[1].symbol, "polymarket:0xabc:1");
    }

    #[test]
    fn output_symbol_is_stable_across_slug_and_title_changes() {
        let first = raw().into_markets(true).unwrap();
        let mut rediscovered = raw();
        rediscovered.question = Some("Will it rain (renamed)?".to_owned());
        rediscovered.title = Some("Renamed market".to_owned());
        let second = rediscovered.into_markets(true).unwrap();
        assert_eq!(first[0].symbol, second[0].symbol);
        assert_eq!(first[1].symbol, second[1].symbol);
        assert_ne!(first[0].title, second[0].title);
    }

    #[test]
    fn version_v2_uses_position_ids() {
        let mut market = raw();
        market.version = Some(Value::String("v2".to_owned()));
        market.clob_token_ids = None;
        market.position_ids = Some(Value::Array(vec![
            Value::String(
                "651150819117105875331414918119047680898421632356043490229292782433651916800"
                    .to_owned(),
            ),
            Value::String(
                "651150819117105875331414918119047680898421632356043490229292782433651916801"
                    .to_owned(),
            ),
        ]));
        let markets = market.into_markets(true).unwrap();
        assert_eq!(markets.len(), 2);
        assert_eq!(
            markets[0].native_symbol,
            "651150819117105875331414918119047680898421632356043490229292782433651916800"
        );
        assert_eq!(markets[0].symbol, "polymarket:0xabc:0");

        // A v2 market without positionIds is malformed.
        let mut market = raw();
        market.version = Some(Value::String("v2".to_owned()));
        market.clob_token_ids = Some(Value::String(r#"["111","222"]"#.to_owned()));
        market.position_ids = None;
        let error = market.into_markets(true).unwrap_err();
        assert!(
            error.to_string().contains("positionIds"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn version_policy() {
        // Missing version and explicit CTF select clobTokenIds.
        assert_eq!(raw().identifier_kind().unwrap(), IdentifierKind::Ctf);
        let mut market = raw();
        market.version = Some(Value::String("CTF".to_owned()));
        assert_eq!(market.identifier_kind().unwrap(), IdentifierKind::Ctf);
        let mut market = raw();
        market.version = Some(Value::Null);
        assert_eq!(market.identifier_kind().unwrap(), IdentifierKind::Ctf);
        let mut market = raw();
        market.version = Some(Value::Number(serde_json::Number::from(2)));
        assert_eq!(
            market.identifier_kind().unwrap(),
            IdentifierKind::PositionV2
        );
        // An unknown future version is an explicit, isolated failure.
        let mut market = raw();
        market.version = Some(Value::String("v3".to_owned()));
        assert!(market.identifier_kind().is_err());
        let mut market = raw();
        market.version = Some(Value::Bool(true));
        assert!(market.identifier_kind().is_err());
    }

    #[test]
    fn rejects_closed_or_non_orderbook_markets() {
        let mut market = raw();
        market.closed = Some(true);
        assert!(market.into_markets(true).unwrap().is_empty());

        let mut market = raw();
        market.enable_order_book = Some(false);
        assert!(market.into_markets(true).unwrap().is_empty());

        let mut market = raw();
        market.accepting_orders = Some(false);
        assert!(market.clone().into_markets(true).unwrap().is_empty());
        assert_eq!(market.into_markets(false).unwrap().len(), 2);

        // Missing is not implicitly true under the strict policy.
        let mut market = raw();
        market.accepting_orders = None;
        assert!(market.clone().into_markets(true).unwrap().is_empty());
        assert_eq!(market.into_markets(false).unwrap().len(), 2);

        // Explicit true remains included.
        let mut market = raw();
        market.accepting_orders = Some(true);
        assert_eq!(market.into_markets(true).unwrap().len(), 2);
    }

    #[test]
    fn malformed_identifier_and_outcome_records_are_rejected() {
        // Mismatched identifier/outcome counts.
        let mut market = raw();
        market.outcomes = Some(Value::String(r#"["Yes"]"#.to_owned()));
        assert!(market.into_markets(true).is_err());
        // Malformed identifier array.
        let mut market = raw();
        market.clob_token_ids = Some(Value::String("not json".to_owned()));
        assert!(market.into_markets(true).is_err());
        // Non-string array items.
        let mut market = raw();
        market.clob_token_ids = Some(Value::Array(vec![Value::Number(1.into())]));
        assert!(market.into_markets(true).is_err());
        // Empty identifiers.
        let mut market = raw();
        market.clob_token_ids = None;
        assert!(market.into_markets(true).is_err());
    }

    #[test]
    fn parses_array_and_json_string_forms() {
        assert_eq!(
            parse_string_array(Some(&Value::Array(vec![
                Value::String("a".to_owned()),
                Value::String("b".to_owned())
            ])))
            .unwrap(),
            ["a", "b"]
        );
        assert_eq!(
            parse_string_array(Some(&Value::String(r#"["x"]"#.to_owned()))).unwrap(),
            ["x"]
        );
    }

    #[test]
    fn one_malformed_record_does_not_abort_the_page() {
        let mut valid = raw();
        valid.condition_id = Some("0xvalid".to_owned());
        let mut malformed = raw();
        malformed.condition_id = None;
        let mut selected = 0usize;
        let mut markets = Vec::new();
        let mut skipped = 0u64;
        convert_page(
            vec![malformed, valid],
            true,
            0,
            &mut selected,
            &mut markets,
            &mut skipped,
        )
        .unwrap();
        assert_eq!(markets.len(), 2);
        assert_eq!(selected, 1);
        assert_eq!(skipped, 1);
        assert_eq!(markets[0].symbol, "polymarket:0xvalid:0");
    }

    #[test]
    fn market_cap_counts_records_and_keeps_each_outcome_set() {
        let mut first = raw();
        first.condition_id = Some("0xfirst".to_owned());
        let mut second = raw();
        second.condition_id = Some("0xsecond".to_owned());
        let mut selected = 0usize;
        let mut markets = Vec::new();
        let mut skipped = 0u64;

        convert_page(
            vec![first.clone(), second.clone()],
            true,
            1,
            &mut selected,
            &mut markets,
            &mut skipped,
        )
        .unwrap();

        // One selected Gamma record, both of its outcome tokens preserved.
        assert_eq!(selected, 1);
        assert_eq!(markets.len(), 2);
        assert!(
            markets
                .iter()
                .all(|market| market.symbol.starts_with("polymarket:0xfirst:"))
        );
        assert_eq!(skipped, 0);

        // The cap stops between records, never inside an outcome list.
        let mut next_page = Vec::new();
        let mut next_selected = selected;
        convert_page(
            vec![second],
            true,
            1,
            &mut next_selected,
            &mut next_page,
            &mut skipped,
        )
        .unwrap();
        assert_eq!(next_selected, 1);
        assert!(next_page.is_empty());
    }

    #[test]
    fn filtered_and_malformed_records_do_not_consume_the_market_budget() {
        let mut filtered = raw();
        filtered.closed = Some(true);
        let mut malformed = raw();
        malformed.condition_id = None;
        let mut valid = raw();
        valid.condition_id = Some("0xvalid".to_owned());
        let mut selected = 0usize;
        let mut markets = Vec::new();
        let mut skipped = 0u64;

        convert_page(
            vec![filtered, malformed, valid],
            true,
            1,
            &mut selected,
            &mut markets,
            &mut skipped,
        )
        .unwrap();

        assert_eq!(selected, 1);
        assert_eq!(markets.len(), 2);
        assert_eq!(skipped, 1);
        assert!(
            markets
                .iter()
                .all(|market| market.symbol.starts_with("polymarket:0xvalid:"))
        );
    }

    #[test]
    fn zero_market_cap_selects_every_eligible_record() {
        let mut first = raw();
        first.condition_id = Some("0xa".to_owned());
        let mut second = raw();
        second.condition_id = Some("0xb".to_owned());
        let mut selected = 0usize;
        let mut markets = Vec::new();
        let mut skipped = 0u64;

        convert_page(
            vec![first, second],
            true,
            0,
            &mut selected,
            &mut markets,
            &mut skipped,
        )
        .unwrap();

        assert_eq!(selected, 2);
        assert_eq!(markets.len(), 4);
    }

    #[test]
    fn a_page_of_only_malformed_records_fails_the_round() {
        let mut markets = Vec::new();
        let mut skipped = 0u64;
        let mut selected = 0usize;
        let mut malformed_a = raw();
        malformed_a.condition_id = None;
        let mut malformed_b = raw();
        malformed_b.clob_token_ids = Some(Value::String("not json".to_owned()));
        let error = convert_page(
            vec![malformed_a, malformed_b],
            true,
            0,
            &mut selected,
            &mut markets,
            &mut skipped,
        )
        .unwrap_err();
        assert_eq!(selected, 0);
        assert!(
            error.to_string().contains("failed to decode"),
            "unexpected error: {error:#}"
        );
        assert!(markets.is_empty());
        assert_eq!(skipped, 2);
    }

    #[test]
    fn keyset_url_preserves_a_configured_path_prefix() {
        assert_eq!(
            keyset_url("https://gamma.example").unwrap().as_str(),
            "https://gamma.example/markets/keyset"
        );
        assert_eq!(
            keyset_url("https://gamma.example/api/").unwrap().as_str(),
            "https://gamma.example/api/markets/keyset"
        );
    }

    #[test]
    fn keyset_response_accepts_an_absent_cursor() {
        let page: KeysetPage = serde_json::from_value(json!({
            "markets": [],
        }))
        .unwrap();
        assert!(page.markets.is_empty());
        assert!(page.next_cursor.is_none());
    }

    fn one_outcome_market(condition: &str, token: &str) -> Value {
        json!({
            "conditionId": condition,
            "question": "Will the keyset test pass?",
            "active": true,
            "closed": false,
            "archived": false,
            "enableOrderBook": true,
            "acceptingOrders": true,
            "version": "v1",
            "clobTokenIds": format!(r#"["{token}"]"#),
            "outcomes": r#"["Yes"]"#,
            "orderPriceMinTickSize": "0.01",
        })
    }

    async fn serve_pages(pages: Vec<Value>) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        tokio::spawn(async move {
            for page in pages {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = vec![0u8; 16 * 1024];
                let Ok(read) = socket.read(&mut buffer).await else {
                    return;
                };
                recorded
                    .lock()
                    .await
                    .push(String::from_utf8_lossy(&buffer[..read]).into_owned());
                let body = page.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\
                     \r\n{}",
                    body.len(),
                    body
                );
                if socket.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
                let _ = socket.shutdown().await;
            }
        });
        (address, requests)
    }

    fn exchange_for(
        address: std::net::SocketAddr,
    ) -> crate::exchange::polymarket::PolymarketExchange {
        let settings = crate::config::PolymarketSettings {
            gamma_url: format!("http://{address}"),
            discovery_page_size: 2,
            discovery_page_delay_ms: 0,
            ..crate::config::PolymarketSettings::default()
        };
        crate::exchange::polymarket::PolymarketExchange::new(
            settings,
            crate::config::NetworkSettings::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn follows_keyset_cursors_without_using_offset() {
        let (address, requests) = serve_pages(vec![
            json!({
                "markets": [
                    one_outcome_market("0xaaa", "111"),
                    one_outcome_market("0xbbb", "222"),
                ],
                "next_cursor": "cursor-one",
            }),
            json!({
                "markets": [one_outcome_market("0xccc", "333")],
            }),
        ])
        .await;
        let exchange = exchange_for(address);
        let markets =
            tokio::time::timeout(std::time::Duration::from_secs(5), exchange.fetch_markets(0))
                .await
                .expect("keyset pagination timed out")
                .unwrap();
        assert_eq!(markets.stats.pages, 2);
        assert_eq!(markets.stats.selected_markets, 3);
        assert_eq!(markets.stats.outcome_symbols, 3);
        assert_eq!(
            markets
                .markets
                .iter()
                .map(|market| market.native_symbol.as_str())
                .collect::<Vec<_>>(),
            ["111", "222", "333"]
        );
        let requests = requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains("GET /markets/keyset?"));
        assert!(requests[0].contains("closed=false"));
        assert!(requests[0].contains("limit=2"));
        assert!(!requests[0].contains("offset="));
        assert!(requests[1].contains("after_cursor=cursor-one"));
        assert!(!requests[1].contains("offset="));
    }

    #[tokio::test]
    async fn rejects_a_repeated_keyset_cursor() {
        let (address, _requests) = serve_pages(vec![
            json!({
                "markets": [one_outcome_market("0xaaa", "111")],
                "next_cursor": "loop",
            }),
            json!({
                "markets": [one_outcome_market("0xbbb", "222")],
                "next_cursor": "loop",
            }),
        ])
        .await;
        let exchange = exchange_for(address);
        let error =
            tokio::time::timeout(std::time::Duration::from_secs(5), exchange.fetch_markets(0))
                .await
                .expect("keyset cycle test timed out")
                .unwrap_err();
        assert!(
            error.to_string().contains("repeated next_cursor"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn retries_408_and_honors_retry_after() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(0usize));
        let observed = Arc::clone(&requests);
        tokio::spawn(async move {
            for response in [
                "HTTP/1.1 408 Request Timeout\r\nRetry-After: 0\r\nContent-Length: 0\r\n\
                 Connection: close\r\n\r\n"
                    .to_owned(),
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    json!({"markets": [one_outcome_market("0xaaa", "111")]})
                        .to_string()
                        .len(),
                    json!({"markets": [one_outcome_market("0xaaa", "111")]})
                ),
            ] {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = [0u8; 4096];
                let _ = socket.read(&mut buffer).await;
                *observed.lock().await += 1;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        let exchange = exchange_for(address);
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), exchange.fetch_markets(0))
                .await
                .expect("408 retry timed out")
                .unwrap();
        assert_eq!(result.stats.retries, 1);
        assert_eq!(result.stats.pages, 1);
        assert_eq!(result.stats.selected_markets, 1);
        assert_eq!(*requests.lock().await, 2);
    }
}
