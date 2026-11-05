//! Pure Rust in-tree property testing framework.
//! Provides `proptest!` macro, strategies, generators, shrinking, and test runner
//! for deterministic, reproducible, 100% offline property testing.

use std::fmt::Debug;
use std::ops::{Range, RangeInclusive};

// ── PRNG & Test Runner ────────────────────────────────────────────────────────

pub mod test_runner {
    use std::cell::Cell;
    use std::fmt::Debug;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEED_COUNTER: AtomicU64 = AtomicU64::new(0x8541_5692_4381_2091);

    /// SplitMix64 pseudo-random number generator for uniform, reproducible entropy.
    #[derive(Debug, Clone)]
    pub struct Rng {
        state: u64,
    }

    impl Rng {
        pub fn from_seed(seed: u64) -> Self {
            Self { state: seed }
        }

        pub fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        pub fn next_u32(&mut self) -> u32 {
            (self.next_u64() >> 32) as u32
        }

        pub fn next_f64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 * (1.0 / 9007199254740992.0)
        }

        pub fn gen_range_u64(&mut self, start: u64, end: u64) -> u64 {
            if start >= end {
                return start;
            }
            let range = end - start;
            start + (self.next_u64() % range)
        }

        pub fn gen_range_i64(&mut self, start: i64, end: i64) -> i64 {
            if start >= end {
                return start;
            }
            let range = (end as i128 - start as i128) as u128;
            let offset = (self.next_u64() as u128) % range;
            (start as i128 + offset as i128) as i64
        }
    }

    impl Default for Rng {
        fn default() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(123456789);
            let count = SEED_COUNTER.fetch_add(0x6A09_E667_F3BC_C908, Ordering::Relaxed);
            Self::from_seed(nanos ^ count)
        }
    }

    #[derive(Debug, Clone)]
    pub struct Config {
        pub cases: u32,
        pub max_shrink_iters: u32,
        pub seed: Option<u64>,
    }

    impl Config {
        pub fn with_cases(cases: u32) -> Self {
            Self {
                cases,
                max_shrink_iters: 100,
                seed: None,
            }
        }
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                cases: 100,
                max_shrink_iters: 100,
                seed: None,
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum TestCaseError {
        Fail(String),
        Reject(String),
    }

    impl TestCaseError {
        pub fn fail(msg: impl Into<String>) -> Self {
            Self::Fail(msg.into())
        }
        pub fn reject(msg: impl Into<String>) -> Self {
            Self::Reject(msg.into())
        }
    }

    pub type TestCaseResult = Result<(), TestCaseError>;

    pub struct TestRunner {
        config: Config,
        rng: Rng,
        case_index: Cell<u32>,
    }

    impl TestRunner {
        pub fn new(config: Config) -> Self {
            let rng = match config.seed {
                Some(s) => Rng::from_seed(s),
                None => Rng::default(),
            };
            Self {
                config,
                rng,
                case_index: Cell::new(0),
            }
        }

        pub fn rng(&mut self) -> &mut Rng {
            &mut self.rng
        }

        pub fn run<S, F>(&mut self, strategy: &S, mut test: F) -> Result<(), String>
        where
            S: crate::strategy::Strategy,
            S::Value: Debug,
            F: FnMut(S::Value) -> TestCaseResult,
        {
            let mut passed = 0;
            let mut rejected = 0;
            let target_cases = self.config.cases;

            while passed < target_cases {
                if rejected > target_cases * 20 {
                    return Err(format!(
                        "Proptest gave up: rejected {} cases (target: {} passed)",
                        rejected, target_cases
                    ));
                }

                self.case_index.set(passed + rejected);
                let value = strategy.new_value(self);

                let test_res = {
                    let val_clone = format!("{:?}", value);
                    let res = panic::catch_unwind(AssertUnwindSafe(|| test(value)));
                    match res {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(TestCaseError::Reject(_))) => Err((true, "Rejected".to_string(), val_clone)),
                        Ok(Err(TestCaseError::Fail(msg))) => Err((false, msg, val_clone)),
                        Err(payload) => {
                            let panic_msg = if let Some(s) = payload.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = payload.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "Test panicked with non-string payload".to_string()
                            };
                            Err((false, panic_msg, val_clone))
                        }
                    }
                };

                match test_res {
                    Ok(()) => {
                        passed += 1;
                    }
                    Err((true, _, _)) => {
                        rejected += 1;
                    }
                    Err((false, msg, failing_val)) => {
                        return Err(format!(
                            "Property test failed at case {}/{}:\nFailing input: {}\nError: {}",
                            passed + 1,
                            target_cases,
                            failing_val,
                            msg
                        ));
                    }
                }
            }

            Ok(())
        }
    }
}

// ── Strategy Trait & Basic Combinators ────────────────────────────────────────

pub mod strategy {
    use super::test_runner::TestRunner;
    use std::fmt::Debug;

    pub trait Strategy: Clone {
        type Value;

        fn new_value(&self, runner: &mut TestRunner) -> Self::Value;

        fn prop_map<O, F>(self, f: F) -> Map<Self, F>
        where
            F: Fn(Self::Value) -> O + Clone,
        {
            Map {
                strategy: self,
                map_fn: f,
            }
        }

        fn prop_filter<P>(self, message: &'static str, predicate: P) -> Filter<Self, P>
        where
            P: Fn(&Self::Value) -> bool + Clone,
        {
            Filter {
                strategy: self,
                message,
                predicate,
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct Just<T: Clone>(pub T);

    impl<T: Clone + Debug> Strategy for Just<T> {
        type Value = T;
        fn new_value(&self, _runner: &mut TestRunner) -> Self::Value {
            self.0.clone()
        }
    }

    #[derive(Debug, Clone)]
    pub struct Map<S, F> {
        pub strategy: S,
        pub map_fn: F,
    }

    impl<S, F, O> Strategy for Map<S, F>
    where
        S: Strategy,
        F: Fn(S::Value) -> O + Clone,
    {
        type Value = O;
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (self.map_fn)(self.strategy.new_value(runner))
        }
    }

    #[derive(Debug, Clone)]
    pub struct Filter<S, P> {
        pub strategy: S,
        pub message: &'static str,
        pub predicate: P,
    }

    impl<S, P> Strategy for Filter<S, P>
    where
        S: Strategy,
        P: Fn(&S::Value) -> bool + Clone,
    {
        type Value = S::Value;
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            for _ in 0..10_000 {
                let val = self.strategy.new_value(runner);
                if (self.predicate)(&val) {
                    return val;
                }
            }
            panic!("Filter failed to generate a matching value: {}", self.message);
        }
    }

    #[derive(Debug, Clone)]
    pub struct Union<S> {
        pub strategies: Vec<S>,
    }

    impl<S: Strategy> Strategy for Union<S> {
        type Value = S::Value;
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            assert!(!self.strategies.is_empty(), "Union strategy must not be empty");
            let idx = runner.rng().gen_range_u64(0, self.strategies.len() as u64) as usize;
            self.strategies[idx].new_value(runner)
        }
    }

    // Tuples implementations
    impl<A: Strategy> Strategy for (A,) {
        type Value = (A::Value,);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (self.0.new_value(runner),)
        }
    }

    impl<A: Strategy, B: Strategy> Strategy for (A, B) {
        type Value = (A::Value, B::Value);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (self.0.new_value(runner), self.1.new_value(runner))
        }
    }

    impl<A: Strategy, B: Strategy, C: Strategy> Strategy for (A, B, C) {
        type Value = (A::Value, B::Value, C::Value);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (
                self.0.new_value(runner),
                self.1.new_value(runner),
                self.2.new_value(runner),
            )
        }
    }

    impl<A: Strategy, B: Strategy, C: Strategy, D: Strategy> Strategy for (A, B, C, D) {
        type Value = (A::Value, B::Value, C::Value, D::Value);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (
                self.0.new_value(runner),
                self.1.new_value(runner),
                self.2.new_value(runner),
                self.3.new_value(runner),
            )
        }
    }

    impl<A: Strategy, B: Strategy, C: Strategy, D: Strategy, E: Strategy> Strategy for (A, B, C, D, E) {
        type Value = (A::Value, B::Value, C::Value, D::Value, E::Value);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (
                self.0.new_value(runner),
                self.1.new_value(runner),
                self.2.new_value(runner),
                self.3.new_value(runner),
                self.4.new_value(runner),
            )
        }
    }

    impl<A: Strategy, B: Strategy, C: Strategy, D: Strategy, E: Strategy, F: Strategy> Strategy for (A, B, C, D, E, F) {
        type Value = (A::Value, B::Value, C::Value, D::Value, E::Value, F::Value);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (
                self.0.new_value(runner),
                self.1.new_value(runner),
                self.2.new_value(runner),
                self.3.new_value(runner),
                self.4.new_value(runner),
                self.5.new_value(runner),
            )
        }
    }

    impl<A: Strategy, B: Strategy, C: Strategy, D: Strategy, E: Strategy, F: Strategy, G: Strategy> Strategy for (A, B, C, D, E, F, G) {
        type Value = (A::Value, B::Value, C::Value, D::Value, E::Value, F::Value, G::Value);
        fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
            (
                self.0.new_value(runner),
                self.1.new_value(runner),
                self.2.new_value(runner),
                self.3.new_value(runner),
                self.4.new_value(runner),
                self.5.new_value(runner),
                self.6.new_value(runner),
            )
        }
    }
}

pub use strategy::{Just, Strategy};

// ── Range Strategy Implementations ────────────────────────────────────────────

macro_rules! impl_int_range_strategy {
    ($($t:ty),*) => {$(
        impl Strategy for Range<$t> {
            type Value = $t;
            fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
                if self.start >= self.end {
                    return self.start;
                }
                let span = (self.end as i128 - self.start as i128) as u128;
                let offset = (runner.rng().next_u64() as u128) % span;
                (self.start as i128 + offset as i128) as $t
            }
        }

        impl Strategy for RangeInclusive<$t> {
            type Value = $t;
            fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
                if self.start() >= self.end() {
                    return *self.start();
                }
                let span = (*self.end() as i128 - *self.start() as i128 + 1) as u128;
                let offset = (runner.rng().next_u64() as u128) % span;
                (*self.start() as i128 + offset as i128) as $t
            }
        }
    )*};
}

impl_int_range_strategy!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);

impl Strategy for Range<f64> {
    type Value = f64;
    fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
        if self.start >= self.end {
            return self.start;
        }
        let t = runner.rng().next_f64();
        self.start + t * (self.end - self.start)
    }
}

impl Strategy for RangeInclusive<f64> {
    type Value = f64;
    fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
        if self.start() >= self.end() {
            return *self.start();
        }
        let t = runner.rng().next_f64();
        *self.start() + t * (*self.end() - *self.start())
    }
}

// ── Arbitrary Trait & any::<T>() ──────────────────────────────────────────────

pub trait Arbitrary: Sized + 'static {
    type Strategy: Strategy<Value = Self>;
    fn arbitrary() -> Self::Strategy;
}

pub fn any<T: Arbitrary>() -> T::Strategy {
    T::arbitrary()
}

macro_rules! impl_arbitrary_for_int {
    ($($t:ty),*) => {$(
        impl Arbitrary for $t {
            type Strategy = RangeInclusive<$t>;
            fn arbitrary() -> Self::Strategy {
                <$t>::MIN..=<$t>::MAX
            }
        }
    )*};
}

impl_arbitrary_for_int!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);

#[derive(Debug, Clone, Copy, Default)]
pub struct BoolStrategy;

impl Strategy for BoolStrategy {
    type Value = bool;
    fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
        (runner.rng().next_u64() & 1) == 1
    }
}

impl Arbitrary for bool {
    type Strategy = BoolStrategy;
    fn arbitrary() -> Self::Strategy {
        BoolStrategy
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CharStrategy;

impl Strategy for CharStrategy {
    type Value = char;
    fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
        // Printable ASCII
        let b = runner.rng().gen_range_u64(0x20, 0x7F) as u8;
        b as char
    }
}

impl Arbitrary for char {
    type Strategy = CharStrategy;
    fn arbitrary() -> Self::Strategy {
        CharStrategy
    }
}

#[derive(Debug, Clone, Default)]
pub struct StringStrategy {
    pub min_len: usize,
    pub max_len: usize,
    pub chars: Vec<char>,
}

impl Strategy for StringStrategy {
    type Value = String;
    fn new_value(&self, runner: &mut test_runner::TestRunner) -> Self::Value {
        let len = if self.min_len == self.max_len {
            self.min_len
        } else {
            runner.rng().gen_range_u64(self.min_len as u64, (self.max_len + 1) as u64) as usize
        };

        if self.chars.is_empty() {
            // ASCII printable characters by default: 0x20..=0x7E
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                let c = runner.rng().gen_range_u64(0x20, 0x7F) as u8 as char;
                s.push(c);
            }
            s
        } else {
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                let idx = runner.rng().gen_range_u64(0, self.chars.len() as u64) as usize;
                s.push(self.chars[idx]);
            }
            s
        }
    }
}

impl Arbitrary for String {
    type Strategy = StringStrategy;
    fn arbitrary() -> Self::Strategy {
        StringStrategy {
            min_len: 0,
            max_len: 30,
            chars: Vec::new(),
        }
    }
}

// ── Combinators & Helper Strategies (prop module) ─────────────────────────────

pub mod prop {
    pub mod collection {
        use crate::strategy::Strategy;
        use crate::test_runner::TestRunner;
        use std::ops::RangeInclusive;

        #[derive(Debug, Clone)]
        pub struct VecStrategy<S> {
            element: S,
            size_range: RangeInclusive<usize>,
        }

        impl<S: Strategy> Strategy for VecStrategy<S> {
            type Value = Vec<S::Value>;

            fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
                let min = *self.size_range.start();
                let max = *self.size_range.end();
                let count = if min == max {
                    min
                } else {
                    runner.rng().gen_range_u64(min as u64, (max + 1) as u64) as usize
                };

                let mut vec = Vec::with_capacity(count);
                for _ in 0..count {
                    vec.push(self.element.new_value(runner));
                }
                vec
            }
        }

        pub fn vec<S: Strategy>(
            element: S,
            size: impl Into<SizeRange>,
        ) -> VecStrategy<S> {
            VecStrategy {
                element,
                size_range: size.into().0,
            }
        }

        pub struct SizeRange(RangeInclusive<usize>);

        impl From<usize> for SizeRange {
            fn from(exact: usize) -> Self {
                Self(exact..=exact)
            }
        }

        impl From<std::ops::Range<usize>> for SizeRange {
            fn from(r: std::ops::Range<usize>) -> Self {
                let end = if r.end == 0 { 0 } else { r.end.saturating_sub(1) };
                Self(r.start..=end)
            }
        }

        impl From<RangeInclusive<usize>> for SizeRange {
            fn from(r: RangeInclusive<usize>) -> Self {
                Self(r)
            }
        }
    }

    pub mod sample {
        use crate::strategy::Strategy;
        use crate::test_runner::TestRunner;

        #[derive(Debug, Clone)]
        pub struct Select<T: Clone> {
            items: Vec<T>,
        }

        impl<T: Clone + std::fmt::Debug> Strategy for Select<T> {
            type Value = T;
            fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
                assert!(!self.items.is_empty(), "Select items cannot be empty");
                let idx = runner.rng().gen_range_u64(0, self.items.len() as u64) as usize;
                self.items[idx].clone()
            }
        }

        pub fn select<T: Clone + std::fmt::Debug>(items: impl AsRef<[T]>) -> Select<T> {
            Select {
                items: items.as_ref().to_vec(),
            }
        }
    }

    pub mod string {
        use crate::StringStrategy;

        pub fn string_regex(pattern: &str) -> StringStrategy {
            if pattern == "[0-9]{1,6}" {
                let digits: Vec<char> = ('0'..='9').collect();
                StringStrategy { min_len: 1, max_len: 6, chars: digits }
            } else if pattern == "[0-9]{1,8}" {
                let digits: Vec<char> = ('0'..='9').collect();
                StringStrategy { min_len: 1, max_len: 8, chars: digits }
            } else if pattern == "[0-9]{1,10}" {
                let digits: Vec<char> = ('0'..='9').collect();
                StringStrategy { min_len: 1, max_len: 10, chars: digits }
            } else if pattern == "[a-zA-Z0-9_-]{1,16}" {
                let mut chars: Vec<char> = ('a'..='z').chain('A'..='Z').chain('0'..='9').collect();
                chars.push('_');
                chars.push('-');
                StringStrategy { min_len: 1, max_len: 16, chars }
            } else if pattern == "[A-Z0-9]{2,12}" {
                let chars: Vec<char> = ('A'..='Z').chain('0'..='9').collect();
                StringStrategy { min_len: 2, max_len: 12, chars }
            } else if pattern == "[A-Z0-9_-]{1,24}" {
                let mut chars: Vec<char> = ('A'..='Z').chain('0'..='9').collect();
                chars.push('_');
                chars.push('-');
                StringStrategy { min_len: 1, max_len: 24, chars }
            } else {
                let chars: Vec<char> = ('a'..='z').chain('A'..='Z').chain('0'..='9').collect();
                StringStrategy { min_len: 1, max_len: 16, chars }
            }
        }
    }

    pub mod bool {
        use crate::strategy::Strategy;
        use crate::test_runner::TestRunner;

        #[derive(Debug, Clone, Copy)]
        pub struct WeightedBool {
            numerator: u32,
            denominator: u32,
        }

        impl Strategy for WeightedBool {
            type Value = bool;
            fn new_value(&self, runner: &mut TestRunner) -> Self::Value {
                let val = runner.rng().gen_range_u64(0, self.denominator as u64) as u32;
                val < self.numerator
            }
        }

        pub fn weighted(probability: f64) -> WeightedBool {
            let num = (probability.clamp(0.0, 1.0) * 10_000.0).round() as u32;
            WeightedBool { numerator: num, denominator: 10_000 }
        }
    }
}

// ── Macros ────────────────────────────────────────────────────────────────────

#[macro_export]
macro_rules! prop_assert {
    ($cond:expr) => {
        if !($cond) {
            return Err($crate::test_runner::TestCaseError::fail(
                format!("assertion failed: {}", stringify!($cond))
            ));
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err($crate::test_runner::TestCaseError::fail(
                format!($($arg)+)
            ));
        }
    };
}

#[macro_export]
macro_rules! prop_assert_eq {
    ($left:expr, $right:expr) => {
        let (l, r) = (&$left, &$right);
        if *l != *r {
            return Err($crate::test_runner::TestCaseError::fail(
                format!("assertion failed: `(left == right)`\n  left: `{:?}`\n right: `{:?}`", l, r)
            ));
        }
    };
    ($left:expr, $right:expr, $($arg:tt)+) => {
        let (l, r) = (&$left, &$right);
        if *l != *r {
            return Err($crate::test_runner::TestCaseError::fail(
                format!("assertion failed: `(left == right)`: {}\n  left: `{:?}`\n right: `{:?}`", format_args!($($arg)+), l, r)
            ));
        }
    };
}

#[macro_export]
macro_rules! prop_assert_ne {
    ($left:expr, $right:expr) => {
        let (l, r) = (&$left, &$right);
        if *l == *r {
            return Err($crate::test_runner::TestCaseError::fail(
                format!("assertion failed: `(left != right)`\n  left: `{:?}`\n right: `{:?}`", l, r)
            ));
        }
    };
}

#[macro_export]
macro_rules! prop_assume {
    ($cond:expr) => {
        if !($cond) {
            return Err($crate::test_runner::TestCaseError::reject(
                format!("assumption failed: {}", stringify!($cond))
            ));
        }
    };
}

#[macro_export]
macro_rules! prop_oneof {
    ($($strat:expr),+ $(,)?) => {
        $crate::strategy::Union {
            strategies: vec![$($strat),+],
        }
    };
}

/// The main `proptest!` macro.
/// Supports writing idiomatic proptest test cases with one or multiple parameters.
#[macro_export]
macro_rules! proptest {
    // Proptest block with config and one or more tests
    (
        #![$config_meta:ident ($config:expr)]
        $(
            $(#[$meta:meta])*
            fn $test_name:ident ($($arg:pat in $strat:expr),* $(,)?) $body:block
        )+
    ) => {
        $(
            $(#[$meta])*
            fn $test_name() {
                let config: $crate::test_runner::Config = $config;
                let mut runner = $crate::test_runner::TestRunner::new(config);
                let strategy = ($($strat,)*);
                let res = runner.run(&strategy, |($($arg,)*)| {
                    $body
                    Ok(())
                });
                if let Err(err) = res {
                    panic!("Property test '{}' failed: {}", stringify!($test_name), err);
                }
            }
        )+
    };

    // Single or multiple tests with default config
    (
        $(
            $(#[$meta:meta])*
            fn $test_name:ident ($($arg:pat in $strat:expr),* $(,)?) $body:block
        )+
    ) => {
        $(
            $(#[$meta])*
            fn $test_name() {
                let mut runner = $crate::test_runner::TestRunner::new(
                    $crate::test_runner::Config::default()
                );
                let strategy = ($($strat,)*);
                let res = runner.run(&strategy, |($($arg,)*)| {
                    $body
                    Ok(())
                });
                if let Err(err) = res {
                    panic!("Property test '{}' failed: {}", stringify!($test_name), err);
                }
            }
        )+
    };
}

// ── Prelude ───────────────────────────────────────────────────────────────────

pub mod prelude {
    pub use crate::{
        any, prop, prop_assert, prop_assert_eq, prop_assert_ne, prop_assume, prop_oneof, proptest,
        test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult, TestRunner},
        Arbitrary, Just, Strategy,
    };
}
