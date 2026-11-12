//! Kalshi websocket payload decoding.
//!
//! The decoder is independent of the transport and of the
//! collector core. It turns one JSON text frame into either a normalized event
//! or a control acknowledgement that the session must process. Keeping this
//! layer pure makes the wire contract easy to test and audit.

use anyhow::{Context, bail};
use serde_json::{Map, Value};

use crate::{
    Side,
    fixed::{
        SCALE, cents_to_micros, contracts_to_micros, parse_micros, parse_price, parse_quantity,
    },
    model::{
        BookSnapshot, EventMeta, ExchangeEvent, Level, LifecycleKind, Price, PriceRange, Quantity,
        QuantityChange, QuantityDelta,
    },
    time::{epoch_ms_to_us, epoch_secs_to_us},
};

/// One decoded Kalshi frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KalshiMessage {
    Snapshot {
        sid: i64,
        seq: u64,
        event: ExchangeEvent,
    },
    Delta {
        sid: i64,
        seq: u64,
        event: ExchangeEvent,
    },
    Lifecycle {
        sid: i64,
        seq: u64,
        event: ExchangeEvent,
    },
    Subscribed {
        id: Option<u64>,
        channel: String,
        sid: i64,
    },
    Ok {
        id: Option<u64>,
        sid: Option<i64>,
        seq: Option<u64>,
        /// The full market list the server holds on this subscription
        /// after the update, when the acknowledgement carries one.
        full_tickers: Option<Vec<String>>,
    },
    Error {
        id: Option<u64>,
        code: Option<i64>,
        message: String,
    },
    Unsubscribed {
        id: Option<u64>,
        sid: Option<i64>,
    },
    Ignore {
        kind: String,
    },
}

/// Decode one Kalshi websocket text payload. `received_at_us` must be captured
/// by the transport immediately after the frame is read.
pub fn decode_kalshi_message(payload: &str, received_at_us: i64) -> anyhow::Result<KalshiMessage> {
    let value: Value = serde_json::from_str(payload).context("invalid Kalshi JSON")?;
    let object = value
        .as_object()
        .context("Kalshi payload is not an object")?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .context("Kalshi payload has no string `type`")?;
    let id = object.get("id").and_then(Value::as_u64);
    let sending_ts_us = object
        .get("sending_ts_ms")
        .and_then(Value::as_i64)
        .map(epoch_ms_to_us)
        .transpose()
        .context("invalid Kalshi sending_ts_ms")?;

    match kind {
        "orderbook_snapshot" => {
            let (sid, seq) = sid_and_seq(object)?;
            let message = message_object(object)?;
            let native_symbol = required_str(message, "market_ticker")?.to_owned();
            let clock = message.get("clock").and_then(Value::as_i64);
            let mut book = BookSnapshot::default();
            parse_snapshot_side(
                message,
                "yes_dollars_fp",
                "yes",
                Side::Buy,
                false,
                &mut book,
            )?;
            parse_snapshot_side(message, "no_dollars_fp", "no", Side::Sell, true, &mut book)?;
            let meta = EventMeta {
                received_at_us,
                exchange_ts_us: sending_ts_us,
                seq: Some(seq),
                clock,
                connection_epoch: 0,
                receive_seq: 0,
            };
            Ok(KalshiMessage::Snapshot {
                sid,
                seq,
                event: ExchangeEvent::Snapshot {
                    native_symbol,
                    book,
                    meta,
                },
            })
        }
        "orderbook_delta" => {
            let (sid, seq) = sid_and_seq(object)?;
            let message = message_object(object)?;
            let native_symbol = required_str(message, "market_ticker")?.to_owned();
            let clock = message.get("clock").and_then(Value::as_i64);
            let side = match required_str(message, "side")? {
                "yes" => Side::Buy,
                "no" => Side::Sell,
                other => bail!("unknown Kalshi orderbook side `{other}`"),
            };
            let (price, delta) = if let Some(price) = message.get("price_dollars") {
                (
                    parse_price(&scalar_string(price)?)?,
                    parse_micros(&scalar_string(
                        message
                            .get("delta_fp")
                            .context("Kalshi delta missing delta_fp")?,
                    )?)?,
                )
            } else {
                let cents = parse_i64_scalar(
                    message
                        .get("price")
                        .context("Kalshi delta missing price_dollars/price")?,
                )?;
                let contracts = parse_i64_scalar(
                    message
                        .get("delta")
                        .context("Kalshi delta missing delta_fp/delta")?,
                )?;
                (cents_to_micros(cents)?, contracts_to_micros(contracts)?)
            };
            let price = if side == Side::Sell {
                SCALE
                    .checked_sub(price)
                    .context("Kalshi NO price exceeds one dollar")?
            } else {
                price
            };
            anyhow::ensure!(
                (0..=SCALE).contains(&price),
                "mapped Kalshi price outside [0, 1]"
            );
            let exchange_ts_us = message
                .get("ts_ms")
                .and_then(Value::as_i64)
                .map(epoch_ms_to_us)
                .transpose()
                .context("invalid Kalshi delta ts_ms")?
                .or(sending_ts_us);
            let meta = EventMeta {
                received_at_us,
                exchange_ts_us,
                seq: Some(seq),
                clock,
                connection_epoch: 0,
                receive_seq: 0,
            };
            Ok(KalshiMessage::Delta {
                sid,
                seq,
                event: ExchangeEvent::LevelChange {
                    native_symbol,
                    side,
                    price: Price::from_raw(price),
                    change: QuantityChange::Delta(QuantityDelta::from_raw(delta)),
                    meta,
                },
            })
        }
        "market_lifecycle_v2" => {
            let (sid, seq) = sid_and_seq(object)?;
            let message = message_object(object)?;
            let native_symbol = required_str(message, "market_ticker")?.to_owned();
            let event_type = required_str(message, "event_type")?;
            let kind = match event_type {
                "created" => LifecycleKind::Created,
                "activated" => LifecycleKind::Activated,
                "deactivated" => LifecycleKind::Deactivated,
                "close_date_updated" => LifecycleKind::CloseDateUpdated,
                "determined" => LifecycleKind::Determined,
                "settled" => LifecycleKind::Settled,
                "price_level_structure_updated" => LifecycleKind::PriceStructureUpdated,
                "metadata_updated" => LifecycleKind::MetadataUpdated,
                other => LifecycleKind::Other(other.to_owned()),
            };
            let close_time_us = message
                .get("close_ts")
                .and_then(Value::as_i64)
                .map(epoch_secs_to_us)
                .transpose()
                .context("invalid Kalshi close_ts")?;
            let title = message
                .get("additional_metadata")
                .and_then(Value::as_object)
                .and_then(|metadata| {
                    metadata
                        .get("title")
                        .or_else(|| metadata.get("yes_sub_title"))
                })
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    message
                        .get("yes_sub_title")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                });
            let price_ranges = parse_lifecycle_price_ranges(message)?;
            let price_level_structure = message
                .get("price_level_structure")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let meta = EventMeta {
                received_at_us,
                exchange_ts_us: sending_ts_us,
                seq: Some(seq),
                clock: None,
                connection_epoch: 0,
                receive_seq: 0,
            };
            Ok(KalshiMessage::Lifecycle {
                sid,
                seq,
                event: ExchangeEvent::Lifecycle {
                    native_symbol,
                    kind,
                    close_time_us,
                    title,
                    tick_size_us: price_ranges
                        .as_deref()
                        .and_then(|ranges| ranges.iter().map(|range| range.step_us).min()),
                    price_ranges,
                    price_level_structure,
                    meta,
                },
            })
        }
        "subscribed" => {
            let message = message_object(object)?;
            Ok(KalshiMessage::Subscribed {
                id,
                channel: required_str(message, "channel")?.to_owned(),
                sid: required_i64(message, "sid")?,
            })
        }
        "ok" => Ok(KalshiMessage::Ok {
            id,
            sid: object.get("sid").and_then(Value::as_i64),
            seq: object.get("seq").and_then(Value::as_u64),
            full_tickers: object
                .get("msg")
                .and_then(|message| message.get("market_tickers"))
                .and_then(Value::as_array)
                .map(|tickers| {
                    tickers
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                }),
        }),
        "error" => {
            let message = object.get("msg").and_then(Value::as_object);
            let code = message
                .and_then(|message| message.get("code"))
                .and_then(Value::as_i64);
            let text = message
                .and_then(|message| message.get("msg"))
                .and_then(Value::as_str)
                .unwrap_or("unknown Kalshi websocket error")
                .to_owned();
            Ok(KalshiMessage::Error {
                id,
                code,
                message: text,
            })
        }
        "unsubscribed" => Ok(KalshiMessage::Unsubscribed {
            id,
            sid: object.get("sid").and_then(Value::as_i64),
        }),
        other => Ok(KalshiMessage::Ignore {
            kind: other.to_owned(),
        }),
    }
}

fn parse_lifecycle_price_ranges(
    message: &Map<String, Value>,
) -> anyhow::Result<Option<Vec<PriceRange>>> {
    let Some(value) = message.get("price_ranges") else {
        return Ok(None);
    };
    let ranges = value
        .as_array()
        .context("Kalshi price_ranges is not an array")?;
    let mut parsed = Vec::with_capacity(ranges.len());
    for range in ranges {
        let object = range
            .as_object()
            .context("Kalshi price range is not an object")?;
        let start = object
            .get("start")
            .and_then(Value::as_str)
            .context("Kalshi price range missing string start")?;
        let end = object
            .get("end")
            .and_then(Value::as_str)
            .context("Kalshi price range missing string end")?;
        let step = object
            .get("step")
            .and_then(Value::as_str)
            .context("Kalshi price range missing string step")?;
        let range = PriceRange {
            start_us: parse_micros(start).context("invalid Kalshi price range start")?,
            end_us: parse_micros(end).context("invalid Kalshi price range end")?,
            step_us: parse_micros(step).context("invalid Kalshi price range step")?,
        };
        range.validate_probability()?;
        parsed.push(range);
    }
    Ok(Some(parsed))
}

fn sid_and_seq(object: &Map<String, Value>) -> anyhow::Result<(i64, u64)> {
    Ok((required_i64(object, "sid")?, required_u64(object, "seq")?))
}

fn message_object(object: &Map<String, Value>) -> anyhow::Result<&Map<String, Value>> {
    object
        .get("msg")
        .and_then(Value::as_object)
        .context("Kalshi payload has no object `msg`")
}

fn required_str<'a>(object: &'a Map<String, Value>, field: &str) -> anyhow::Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("Kalshi payload missing string `{field}`"))
}

fn required_i64(object: &Map<String, Value>, field: &str) -> anyhow::Result<i64> {
    object
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("Kalshi payload missing integer `{field}`"))
}

fn required_u64(object: &Map<String, Value>, field: &str) -> anyhow::Result<u64> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("Kalshi payload missing unsigned integer `{field}`"))
}

fn scalar_string(value: &Value) -> anyhow::Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        _ => bail!("expected a string or number, found {value}"),
    }
}

fn parse_i64_scalar(value: &Value) -> anyhow::Result<i64> {
    match value {
        Value::Number(number) => number.as_i64().context("integer out of i64 range"),
        Value::String(text) => text.parse().context("invalid integer string"),
        _ => bail!("expected an integer, found {value}"),
    }
}

/// Parse one snapshot side.
///
/// Per the official Kalshi orderbook schema, `yes_dollars_fp`/`no_dollars_fp`
/// (and their legacy `yes`/`no` forms) are **optional**: "This key will not
/// exist if there are no Yes/No offers in the orderbook." A missing side is
/// therefore a legitimate empty side, not a malformed snapshot. Only a side
/// that is *present but malformed* — null, non-array, non-pair levels, or
/// invalid price/quantity values — is rejected.
///
/// The modern fixed-point form takes precedence when both forms appear.
fn parse_snapshot_side(
    message: &Map<String, Value>,
    modern_field: &str,
    legacy_field: &str,
    side: Side,
    invert_price: bool,
    book: &mut BookSnapshot,
) -> anyhow::Result<()> {
    if let Some(levels) = message.get(modern_field) {
        for level in parse_level_array(levels, false)? {
            let price = if invert_price {
                SCALE
                    .checked_sub(level.price.raw())
                    .context("Kalshi NO price exceeds one dollar")?
            } else {
                level.price.raw()
            };
            push_level(book, side, price, level.quantity.raw())?;
        }
        return Ok(());
    }
    if let Some(levels) = message.get(legacy_field) {
        for level in parse_level_array(levels, true)? {
            let price = if invert_price {
                SCALE
                    .checked_sub(level.price.raw())
                    .context("Kalshi NO price exceeds one dollar")?
            } else {
                level.price.raw()
            };
            push_level(book, side, price, level.quantity.raw())?;
        }
    }
    Ok(())
}

fn parse_level_array(value: &Value, legacy: bool) -> anyhow::Result<Vec<Level>> {
    let array = value
        .as_array()
        .with_context(|| format!("Kalshi levels are not an array: {value}"))?;
    let mut levels = Vec::with_capacity(array.len());
    for entry in array {
        let pair = entry
            .as_array()
            .with_context(|| format!("Kalshi level is not a pair: {entry}"))?;
        anyhow::ensure!(pair.len() == 2, "Kalshi level does not have two fields");
        let price = if legacy {
            parse_legacy_price(&pair[0])?
        } else {
            parse_price(&scalar_string(&pair[0])?)?
        };
        let quantity = if legacy {
            parse_legacy_quantity(&pair[1])?
        } else {
            parse_quantity(&scalar_string(&pair[1])?)?
        };
        levels.push(Level {
            price: Price::from_raw(price),
            quantity: Quantity::new(quantity)?,
        });
    }
    Ok(levels)
}

fn parse_legacy_price(value: &Value) -> anyhow::Result<i64> {
    match value {
        Value::String(text) if text.contains('.') => parse_price(text),
        Value::Number(number) if number.is_f64() => parse_price(&number.to_string()),
        _ => cents_to_micros(parse_i64_scalar(value)?),
    }
}

fn parse_legacy_quantity(value: &Value) -> anyhow::Result<i64> {
    match value {
        Value::String(text) if text.contains('.') => parse_quantity(text),
        Value::Number(number) if number.is_f64() => parse_quantity(&number.to_string()),
        _ => {
            let contracts = parse_i64_scalar(value)?;
            anyhow::ensure!(contracts >= 0, "negative Kalshi quantity");
            contracts_to_micros(contracts)
        }
    }
}

fn push_level(
    book: &mut BookSnapshot,
    side: Side,
    price: i64,
    quantity: i64,
) -> anyhow::Result<()> {
    anyhow::ensure!((0..=SCALE).contains(&price), "Kalshi price outside [0, 1]");
    let quantity = Quantity::new(quantity).context("negative Kalshi quantity")?;
    let level = Level {
        price: Price::from_raw(price),
        quantity,
    };
    match side {
        Side::Buy => book.bids.push(level),
        Side::Sell => book.asks.push(level),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_700_000_000_123_456;

    #[test]
    fn maps_yes_and_no_snapshot_levels() {
        let payload = r#"{
            "type":"orderbook_snapshot","sid":2,"seq":2,"sending_ts_ms":1669149841234,
            "msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"id","clock":4182057,
              "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],
              "no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}
        }"#;
        let message = decode_kalshi_message(payload, NOW).unwrap();
        let KalshiMessage::Snapshot { sid, seq, event } = message else {
            panic!("expected snapshot");
        };
        assert_eq!((sid, seq), (2, 2));
        let ExchangeEvent::Snapshot { book, meta, .. } = event else {
            panic!("expected event");
        };
        assert_eq!(
            book.bids[0],
            Level {
                price: Price::from_raw(80_000),
                quantity: Quantity::new(300_000_000).unwrap()
            }
        );
        assert_eq!(
            book.asks[0],
            Level {
                price: Price::from_raw(460_000),
                quantity: Quantity::new(20_000_000).unwrap()
            }
        );
        assert_eq!(meta.clock, Some(4_182_057));
        assert_eq!(meta.exchange_ts_us, Some(1_669_149_841_234_000));
    }

    #[test]
    fn maps_no_delta_to_yes_ask() {
        let payload = r#"{
            "type":"orderbook_delta","sid":2,"seq":3,"sending_ts_ms":1669149841123,
            "msg":{"market_ticker":"T","clock":4182061,"price_dollars":"0.9600",
                   "delta_fp":"-54.00","side":"no","ts_ms":1669149841000}
        }"#;
        let message = decode_kalshi_message(payload, NOW).unwrap();
        let KalshiMessage::Delta { event, .. } = message else {
            panic!("expected delta");
        };
        let ExchangeEvent::LevelChange {
            side,
            price,
            change,
            ..
        } = event
        else {
            panic!("expected level change");
        };
        assert_eq!(side, Side::Sell);
        assert_eq!(price, Price::from_raw(40_000));
        assert_eq!(
            change,
            QuantityChange::Delta(QuantityDelta::from_raw(-54_000_000))
        );
    }

    #[test]
    fn parses_legacy_cent_prices_and_contract_counts() {
        let payload = r#"{
            "type":"orderbook_snapshot","sid":1,"seq":1,
            "msg":{"market_ticker":"T","clock":1,"yes":[[8,300]],"no":[[54,20]]}
        }"#;
        let message = decode_kalshi_message(payload, NOW).unwrap();
        let KalshiMessage::Snapshot { event, .. } = message else {
            panic!("expected snapshot");
        };
        let ExchangeEvent::Snapshot { book, .. } = event else {
            panic!("expected event");
        };
        assert_eq!(
            book.bids[0],
            Level {
                price: Price::from_raw(80_000),
                quantity: Quantity::new(300_000_000).unwrap()
            }
        );
        assert_eq!(
            book.asks[0],
            Level {
                price: Price::from_raw(460_000),
                quantity: Quantity::new(20_000_000).unwrap()
            }
        );
    }

    #[test]
    fn parses_lifecycle() {
        let payload = r#"{
            "type":"market_lifecycle_v2","sid":13,"seq":3,"sending_ts_ms":1669149841234,
            "msg":{"market_ticker":"T","close_ts":1694721600,"event_type":"created",
                   "additional_metadata":{"title":"Title","yes_sub_title":"Sub"}}
        }"#;
        let message = decode_kalshi_message(payload, NOW).unwrap();
        let KalshiMessage::Lifecycle { event, .. } = message else {
            panic!("expected lifecycle");
        };
        let ExchangeEvent::Lifecycle {
            kind,
            close_time_us,
            title,
            ..
        } = event
        else {
            panic!("expected lifecycle event");
        };
        assert_eq!(kind, LifecycleKind::Created);
        assert_eq!(close_time_us, Some(1_694_721_600_000_000));
        assert_eq!(title.as_deref(), Some("Title"));
    }

    #[test]
    fn parses_lifecycle_price_structure() {
        let payload = r#"{
            "type":"market_lifecycle_v2","sid":13,"seq":4,"sending_ts_ms":1669149841234,
            "msg":{"market_ticker":"T","event_type":"price_level_structure_updated",
                   "price_level_structure":"tapered_deci_cent",
                   "price_ranges":[
                     {"start":"0.00","end":"0.10","step":"0.001"},
                     {"start":"0.10","end":"0.90","step":"0.01"},
                     {"start":"0.90","end":"1.00","step":"0.001"}
                   ]}
        }"#;
        let message = decode_kalshi_message(payload, NOW).unwrap();
        let KalshiMessage::Lifecycle { event, .. } = message else {
            panic!("expected lifecycle");
        };
        let ExchangeEvent::Lifecycle {
            kind,
            tick_size_us,
            price_ranges,
            price_level_structure,
            ..
        } = event
        else {
            panic!("expected lifecycle event");
        };
        assert_eq!(kind, LifecycleKind::PriceStructureUpdated);
        assert_eq!(tick_size_us, Some(1_000));
        assert_eq!(price_ranges.as_ref().map(Vec::len), Some(3));
        assert_eq!(price_level_structure.as_deref(), Some("tapered_deci_cent"));
    }

    #[test]
    fn parses_control_messages() {
        let payload = r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":9}}"#;
        assert_eq!(
            decode_kalshi_message(payload, NOW).unwrap(),
            KalshiMessage::Subscribed {
                id: Some(1),
                channel: "orderbook_delta".to_owned(),
                sid: 9
            }
        );
        let payload = r#"{"id":2,"type":"error","msg":{"code":7,"msg":"bad"}}"#;
        assert_eq!(
            decode_kalshi_message(payload, NOW).unwrap(),
            KalshiMessage::Error {
                id: Some(2),
                code: Some(7),
                message: "bad".to_owned()
            }
        );
    }

    #[test]
    fn rejects_malformed_payloads() {
        assert!(decode_kalshi_message("not json", NOW).is_err());
        assert!(decode_kalshi_message(r#"{"type":"orderbook_delta"}"#, NOW).is_err());
        assert!(decode_kalshi_message(
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"T","side":"maybe","price_dollars":"0.5","delta_fp":"1"}}"#,
            NOW
        )
        .is_err());
    }

    #[test]
    fn snapshot_side_matrix() {
        // A side that is absent is a legitimate empty side per the official
        // schema ("this key will not exist if there are no Yes offers").
        let message = decode_kalshi_message(
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
                "msg":{"market_ticker":"T","clock":1,"yes_dollars_fp":[["0.40","10.00"]]}}"#,
            NOW,
        )
        .unwrap();
        let KalshiMessage::Snapshot { event, .. } = message else {
            panic!("expected snapshot");
        };
        let ExchangeEvent::Snapshot { book, .. } = event else {
            panic!("expected book");
        };
        assert_eq!(book.bids.len(), 1);
        assert!(book.asks.is_empty());

        // Both sides absent: a valid "no offers" book.
        let message = decode_kalshi_message(
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
                "msg":{"market_ticker":"T","clock":0}}"#,
            NOW,
        )
        .unwrap();
        let KalshiMessage::Snapshot { event, .. } = message else {
            panic!("expected snapshot");
        };
        let ExchangeEvent::Snapshot { book, .. } = event else {
            panic!("expected book");
        };
        assert!(book.is_empty());

        // Present but null, non-array, or malformed levels are rejected.
        for msg in [
            r#"{"market_ticker":"T","clock":1,"yes_dollars_fp":null}"#,
            r#"{"market_ticker":"T","clock":1,"yes_dollars_fp":{}}"#,
            r#"{"market_ticker":"T","clock":1,"yes_dollars_fp":[["0.40"]]}"#,
            r#"{"market_ticker":"T","clock":1,"yes_dollars_fp":[["0.40","10.00","extra"]]}"#,
            r#"{"market_ticker":"T","clock":1,"yes_dollars_fp":[["not-a-price","10.00"]]}"#,
            r#"{"market_ticker":"T","clock":1,"no_dollars_fp":[["1.40","10.00"]]}"#,
            r#"{"market_ticker":"T","clock":1,"yes_dollars_fp":[["0.40","-10.00"]]}"#,
        ] {
            let payload = format!(r#"{{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{msg}}}"#);
            assert!(
                decode_kalshi_message(&payload, NOW).is_err(),
                "expected rejection for {msg}"
            );
        }

        // The modern fixed-point form wins when both forms are present.
        let message = decode_kalshi_message(
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
                "msg":{"market_ticker":"T","clock":1,
                       "yes_dollars_fp":[["0.40","10.00"]],
                       "yes":[[99,999]]}}"#,
            NOW,
        )
        .unwrap();
        let KalshiMessage::Snapshot { event, .. } = message else {
            panic!("expected snapshot");
        };
        let ExchangeEvent::Snapshot { book, .. } = event else {
            panic!("expected book");
        };
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].price, Price::from_raw(400_000));
    }
}
