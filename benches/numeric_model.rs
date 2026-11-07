//! Throughput and memory benchmarks for the general numeric model.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]
//! Run with `cargo bench --bench numeric_model`. The harness is `std`-only //! (`harness = false`), so this works on stable Rust and prints timings that
//! can be compared across commits. The workloads mirror the 100,000-symbol
//! scale used by the collector E2E tests: parsing, book updates, snapshots,
//! and the `i64` vs `i128` storage comparison.

use std::time::Instant;

use engineering_challenge::{
    fixed::{format_scaled, parse_scaled},
    model::{BookSnapshot, Level, Price, Quantity, QuantityChange, QuantityDelta},
    orderbook::OrderBook,
};

const SYMBOLS: usize = 100_000;
const LEVELS_PER_SIDE: usize = 10;

fn books() -> Vec<OrderBook> {
    let mut books = Vec::with_capacity(SYMBOLS);
    for index in 0..SYMBOLS {
        let mut book = OrderBook::new();
        let base = 100_000 + (index % 10_000) as i64 * 100;
        for level in 0..LEVELS_PER_SIDE {
            let price = Price::from_raw(base + level as i64 * 100);
            book.apply(
                engineering_challenge::Side::Buy,
                price,
                QuantityChange::Absolute(Quantity::new(1_000_000).unwrap()),
            )
            .unwrap();
            let ask = Price::from_raw(base + 10_000 + level as i64 * 100);
            book.apply(
                engineering_challenge::Side::Sell,
                ask,
                QuantityChange::Absolute(Quantity::new(1_000_000).unwrap()),
            )
            .unwrap();
        }
        books.push(book);
    }
    books
}

fn bench_parse() {
    let inputs: Vec<String> = (0..SYMBOLS)
        .map(|index| format!("{}.{:09}", 100 + index % 900, index * 7919 % 1_000_000_000))
        .collect();
    let start = Instant::now();
    let mut total = 0i64;
    for input in &inputs {
        total = total.wrapping_add(parse_scaled(input, 9).unwrap());
    }
    let elapsed = start.elapsed();
    println!(
        "parse_scaled 100k strings (scale 9): {:?} total={total}",
        elapsed
    );

    let start = Instant::now();
    let mut formatted_bytes = 0usize;
    for line in inputs.iter().take(SYMBOLS) {
        let value = parse_scaled(line, 9).unwrap();
        formatted_bytes += format_scaled(value, 9).len();
    }
    println!(
        "format_scaled 100k values (scale 9): {:?} bytes={formatted_bytes}",
        start.elapsed()
    );
}

fn bench_updates(books: &mut [OrderBook]) {
    let start = Instant::now();
    for round in 0..VIEW {
        let delta = QuantityDelta::from_raw(i64::from(round) * 10_000);
        for (index, book) in books.iter_mut().enumerate() {
            let price = Price::from_raw(100_000 + (index % 10_000) as i64 * 100);
            book.apply(
                engineering_challenge::Side::Buy,
                price,
                QuantityChange::Delta(delta),
            )
            .unwrap();
        }
    }
    println!(
        "book updates {VIEW} rounds x 100k symbols: {:?} ({:.0} updates/sec)",
        start.elapsed(),
        VIEW as f64 * SYMBOLS as f64 / start.elapsed().as_secs_f64()
    );
}

fn bench_snapshots(books: &[OrderBook]) {
    let start = Instant::now();
    let mut levels = 0usize;
    for book in books {
        let snapshot: BookSnapshot = book.to_snapshot();
        levels += snapshot.bids.len() + snapshot.asks.len();
    }
    println!(
        "snapshots 100k books ({levels} levels): {:?}",
        start.elapsed()
    );
}

fn compare_i64_i128() {
    use std::collections::BTreeMap;
    println!(
        "size_of Price            : {} bytes",
        std::mem::size_of::<Price>()
    );
    println!(
        "size_of Quantity         : {} bytes",
        std::mem::size_of::<Quantity>()
    );
    println!(
        "size_of Level            : {} bytes",
        std::mem::size_of::<Level>()
    );
    println!(
        "size_of i128             : {} bytes",
        std::mem::size_of::<i128>()
    );
    println!(
        "size_of BTreeMap<i64,i64> : {} bytes",
        std::mem::size_of::<BTreeMap<i64, i64>>()
    );
    println!(
        "size_of BTreeMap<i128,i128>: {} bytes",
        std::mem::size_of::<BTreeMap<i128, i128>>()
    );
    let entries = SYMBOLS * LEVELS_PER_SIDE * 2;
    let key_value_i64 = entries * (std::mem::size_of::<i64>() * 2);
    let key_value_i128 = entries * (std::mem::size_of::<i128>() * 2);
    println!(
        "100k books x {LEVELS_PER_SIDE} levels/side key+value bytes: i64={key_value_i64} i128={key_value_i128} delta={}",
        key_value_i128 - key_value_i64
    );

    let mut i64_map = BTreeMap::<i64, i64>::new();
    let mut i128_map = BTreeMap::<i128, i128>::new();
    let start = Instant::now();
    for index in 0..entries {
        let key = (index % 10_000) as i64;
        i64_map.insert(key, key * 1_000);
    }
    let i64_elapsed = start.elapsed();
    let start = Instant::now();
    for index in 0..entries {
        let key = (index % 10_000) as i128;
        i128_map.insert(key, key * 1_000);
    }
    let i128_elapsed = start.elapsed();
    println!("BTreeMap insert i64  ({entries} ops): {i64_elapsed:?}");
    println!("BTreeMap insert i128 ({entries} ops): {i128_elapsed:?}");
    println!(
        "entry overhead i64={} i128={}",
        i64_map.len() * (std::mem::size_of::<i64>() * 2),
        i128_map.len() * (std::mem::size_of::<i128>() * 2)
    );
}

const VIEW: u32 = 5;

fn main() {
    println!("=== general numeric model benchmark ===");
    compare_i64_i128();
    bench_parse();
    let mut books = books();
    println!("built {SYMBOLS} books x {LEVELS_PER_SIDE} levels/side");
    bench_updates(&mut books);
    bench_snapshots(&books);
}
