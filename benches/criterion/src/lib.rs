//! Criterion-compatible statistics-driven micro-benchmarking harness.
//! Provides genuine timing, warmup, sample collection, statistical analysis,
//! and Criterion-formatted console reporting.

pub use std::hint::black_box;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Throughput {
    Bytes(u64),
    Elements(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSize {
    SmallInput,
    LargeInput,
    PerIteration,
    NumBatches(u64),
    NumIterations(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkId {
    name: String,
}

impl BenchmarkId {
    pub fn new(function_id: impl Into<String>, parameter: impl std::fmt::Display) -> Self {
        Self {
            name: format!("{}/{}", function_id.into(), parameter),
        }
    }

    pub fn from_parameter(parameter: impl std::fmt::Display) -> Self {
        Self {
            name: parameter.to_string(),
        }
    }
}

pub struct Bencher<'a> {
    is_test_mode: bool,
    warm_up_time: Duration,
    measurement_time: Duration,
    sample_size: usize,
    _throughput: Option<Throughput>,
    elapsed_samples: &'a mut Vec<Duration>,
    iterations_per_sample: &'a mut u64,
}

impl<'a> Bencher<'a> {
    pub fn iter<O, R>(&mut self, mut routine: R)
    where
        R: FnMut() -> O,
    {
        if self.is_test_mode {
            let _ = black_box(routine());
            self.elapsed_samples.push(Duration::from_nanos(1));
            *self.iterations_per_sample = 1;
            return;
        }

        // 1. Warm-up phase: run for warm_up_time to warm caches and branch predictors
        let warm_start = Instant::now();
        let mut warm_iters: u64 = 0;
        while warm_start.elapsed() < self.warm_up_time {
            for _ in 0..10 {
                let _ = black_box(routine());
            }
            warm_iters += 10;
        }

        // Estimate cost per iteration
        let warm_elapsed = warm_start.elapsed();
        let time_per_iter = if warm_iters > 0 {
            warm_elapsed.as_secs_f64() / warm_iters as f64
        } else {
            1e-7
        };

        // Determine batch size per sample so total measurement takes ~measurement_time
        let target_sample_time = self.measurement_time.as_secs_f64() / self.sample_size as f64;
        let iters_per_sample = ((target_sample_time / time_per_iter).ceil() as u64).max(1);
        *self.iterations_per_sample = iters_per_sample;

        // 2. Sample collection phase
        self.elapsed_samples.clear();
        self.elapsed_samples.reserve(self.sample_size);

        for _ in 0..self.sample_size {
            let start = Instant::now();
            for _ in 0..iters_per_sample {
                let _ = black_box(routine());
            }
            let elapsed = start.elapsed();
            self.elapsed_samples.push(elapsed);
        }
    }

    pub fn iter_batched<I, O, S, R>(&mut self, mut setup: S, mut routine: R, _batch_size: BatchSize)
    where
        S: FnMut() -> I,
        R: FnMut(I) -> O,
    {
        if self.is_test_mode {
            let input = setup();
            let _ = black_box(routine(input));
            self.elapsed_samples.push(Duration::from_nanos(1));
            *self.iterations_per_sample = 1;
            return;
        }

        // Warm-up phase
        let warm_start = Instant::now();
        let mut warm_iters: u64 = 0;
        while warm_start.elapsed() < self.warm_up_time {
            for _ in 0..5 {
                let input = setup();
                let _ = black_box(routine(input));
            }
            warm_iters += 5;
        }

        let warm_elapsed = warm_start.elapsed();
        let time_per_iter = if warm_iters > 0 {
            warm_elapsed.as_secs_f64() / warm_iters as f64
        } else {
            1e-6
        };

        let target_sample_time = self.measurement_time.as_secs_f64() / self.sample_size as f64;
        let iters_per_sample = ((target_sample_time / time_per_iter).ceil() as u64).clamp(1, 100);
        *self.iterations_per_sample = iters_per_sample;

        self.elapsed_samples.clear();
        self.elapsed_samples.reserve(self.sample_size);

        for _ in 0..self.sample_size {
            // Pre-create batch of inputs to keep setup outside measurement interval
            let inputs: Vec<I> = (0..iters_per_sample).map(|_| setup()).collect();
            let start = Instant::now();
            for input in inputs {
                let _ = black_box(routine(input));
            }
            let elapsed = start.elapsed();
            self.elapsed_samples.push(elapsed);
        }
    }
}

pub struct BenchmarkGroup<'a> {
    criterion: &'a mut Criterion,
    group_name: String,
    throughput: Option<Throughput>,
    sample_size: usize,
    warm_up_time: Duration,
    measurement_time: Duration,
}

impl<'a> BenchmarkGroup<'a> {
    pub fn throughput(&mut self, throughput: Throughput) -> &mut Self {
        self.throughput = Some(throughput);
        self
    }

    pub fn sample_size(&mut self, n: usize) -> &mut Self {
        self.sample_size = n.max(1);
        self
    }

    pub fn warm_up_time(&mut self, d: Duration) -> &mut Self {
        self.warm_up_time = d;
        self
    }

    pub fn measurement_time(&mut self, d: Duration) -> &mut Self {
        self.measurement_time = d;
        self
    }

    pub fn bench_function<F>(&mut self, id: impl Into<String>, mut f: F)
    where
        F: FnMut(&mut Bencher),
    {
        let id_str = id.into();
        let full_name = format!("{}/{}", self.group_name, id_str);
        if !self.criterion.should_run(&full_name) {
            return;
        }

        self.run_benchmark(&full_name, |bencher| f(bencher));
    }

    pub fn bench_with_input<I: ?Sized, F>(&mut self, id: BenchmarkId, input: &I, mut f: F)
    where
        F: FnMut(&mut Bencher, &I),
    {
        let full_name = format!("{}/{}", self.group_name, id.name);
        if !self.criterion.should_run(&full_name) {
            return;
        }

        self.run_benchmark(&full_name, |bencher| f(bencher, input));
    }

    fn run_benchmark<F>(&mut self, full_name: &str, mut f: F)
    where
        F: FnMut(&mut Bencher),
    {
        if self.criterion.is_test_mode {
            let mut samples = Vec::new();
            let mut iters = 1;
            let mut bencher = Bencher {
                is_test_mode: true,
                warm_up_time: Duration::ZERO,
                measurement_time: Duration::ZERO,
                sample_size: 1,
                _throughput: self.throughput,
                elapsed_samples: &mut samples,
                iterations_per_sample: &mut iters,
            };
            f(&mut bencher);
            println!("test {full_name} ... ok");
            return;
        }

        println!("Benchmarking {full_name}: Warming up for {:?}", self.warm_up_time);
        let mut samples = Vec::new();
        let mut iters = 1;
        let mut bencher = Bencher {
            is_test_mode: false,
            warm_up_time: self.warm_up_time,
            measurement_time: self.measurement_time,
            sample_size: self.sample_size,
            _throughput: self.throughput,
            elapsed_samples: &mut samples,
            iterations_per_sample: &mut iters,
        };

        f(&mut bencher);

        if samples.is_empty() {
            println!("No samples collected for {full_name}");
            return;
        }

        // Statistical calculations
        let sample_times_sec: Vec<f64> = samples
            .iter()
            .map(|s| s.as_secs_f64() / iters as f64)
            .collect();

        let n = sample_times_sec.len() as f64;
        let mean_sec = sample_times_sec.iter().sum::<f64>() / n;
        let variance = sample_times_sec
            .iter()
            .map(|t| (t - mean_sec).powi(2))
            .sum::<f64>()
            / (n - 1.0).max(1.0);
        let std_dev_sec = variance.sqrt();

        let mut sorted = sample_times_sec.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let _median_sec = sorted[sorted.len() / 2];

        // Format times
        let (mean_str, _unit) = format_time(mean_sec);
        let (min_str, _) = format_time(sorted[0]);
        let (max_str, _) = format_time(sorted[sorted.len() - 1]);

        let mut output = format!(
            "{full_name}\n                        time:   [{min_str} {mean_str} {max_str}] (std: {:.2}%)",
            (std_dev_sec / mean_sec) * 100.0
        );

        if let Some(tp) = self.throughput {
            let tp_str = match tp {
                Throughput::Elements(elems) => {
                    let rate = (elems as f64) / mean_sec;
                    format_elements_per_sec(rate)
                }
                Throughput::Bytes(bytes) => {
                    let rate = (bytes as f64) / mean_sec;
                    format_bytes_per_sec(rate)
                }
            };
            output.push_str(&format!("\n                        thrpt:  [{tp_str}]"));
        }

        println!("{output}");
    }

    pub fn finish(self) {}
}

pub struct Criterion {
    warm_up_time: Duration,
    measurement_time: Duration,
    sample_size: usize,
    is_test_mode: bool,
    filters: Vec<String>,
}

impl Default for Criterion {
    fn default() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut is_test_mode = false;
        let mut sample_size = 10;
        let mut filters = Vec::new();

        let mut i = 1;
        while i < args.len() {
            let arg = &args[i];
            if arg == "--test" {
                is_test_mode = true;
            } else if arg == "--sample-size" {
                if i + 1 < args.len() {
                    if let Ok(n) = args[i + 1].parse::<usize>() {
                        sample_size = n;
                    }
                    i += 1;
                }
            } else if arg == "--bench" {
                // standard cargo flag, ignore
            } else if !arg.starts_with('-') {
                filters.push(arg.clone());
            }
            i += 1;
        }

        Self {
            warm_up_time: Duration::from_millis(500),
            measurement_time: Duration::from_secs(1),
            sample_size,
            is_test_mode,
            filters,
        }
    }
}

impl Criterion {
    pub fn warm_up_time(mut self, d: Duration) -> Self {
        self.warm_up_time = d;
        self
    }

    pub fn measurement_time(mut self, d: Duration) -> Self {
        self.measurement_time = d;
        self
    }

    pub fn sample_size(mut self, n: usize) -> Self {
        self.sample_size = n.max(1);
        self
    }

    pub fn benchmark_group<'a>(&'a mut self, name: impl Into<String>) -> BenchmarkGroup<'a> {
        let group_name = name.into();
        let warm_up = self.warm_up_time;
        let measurement = self.measurement_time;
        let sample_size = self.sample_size;
        BenchmarkGroup {
            criterion: self,
            group_name,
            throughput: None,
            sample_size,
            warm_up_time: warm_up,
            measurement_time: measurement,
        }
    }

    pub fn should_run(&self, name: &str) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        self.filters.iter().any(|f| name.contains(f))
    }
}

fn format_time(seconds: f64) -> (String, &'static str) {
    if seconds >= 1.0 {
        (format!("{:.3} s", seconds), "s")
    } else if seconds >= 1e-3 {
        (format!("{:.3} ms", seconds * 1e3), "ms")
    } else if seconds >= 1e-6 {
        (format!("{:.3} µs", seconds * 1e6), "µs")
    } else {
        (format!("{:.3} ns", seconds * 1e9), "ns")
    }
}

fn format_elements_per_sec(rate: f64) -> String {
    if rate >= 1e9 {
        format!("{:.2} Gelem/s", rate / 1e9)
    } else if rate >= 1e6 {
        format!("{:.2} Melem/s", rate / 1e6)
    } else if rate >= 1e3 {
        format!("{:.2} Kelem/s", rate / 1e3)
    } else {
        format!("{:.2} elem/s", rate)
    }
}

fn format_bytes_per_sec(rate: f64) -> String {
    if rate >= (1024.0 * 1024.0 * 1024.0) {
        format!("{:.2} GiB/s", rate / (1024.0 * 1024.0 * 1024.0))
    } else if rate >= (1024.0 * 1024.0) {
        format!("{:.2} MiB/s", rate / (1024.0 * 1024.0))
    } else if rate >= 1024.0 {
        format!("{:.2} KiB/s", rate / 1024.0)
    } else {
        format!("{:.2} B/s", rate)
    }
}

#[macro_export]
macro_rules! criterion_group {
    (name = $name:ident; config = $cfg:expr; targets = $($target:ident),* $(,)?) => {
        pub fn $name() {
            let mut criterion = $cfg;
            $($target(&mut criterion);)*
        }
    };
    ($name:ident, $($target:ident),* $(,)?) => {
        pub fn $name() {
            let mut criterion = $crate::Criterion::default();
            $($target(&mut criterion);)*
        }
    };
}

#[macro_export]
macro_rules! criterion_main {
    ($($group:ident),* $(,)?) => {
        fn main() {
            $($group();)*
        }
    };
}
