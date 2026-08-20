use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

use metrics::{Metric, MetricSnapshot, MetricType, len, visit};

fn register_sequential(c: &mut Criterion) {
    c.bench_function("register_sync_sequential", |b| {
        let mut cnt = 0u64;
        b.iter(|| {
            cnt = cnt.wrapping_add(1);
            let name = format!("seq_reg_metric_{cnt}");
            let m = Metric::register(&name, MetricType::Redis);
            black_box(m);
        });
    });
}

fn register_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("register_sync_concurrent");
    for worker_count in [1usize, 2, 4, 8].iter() {
        group.throughput(Throughput::Elements(*worker_count as u64));
        let workers = *worker_count;
        let bench_name = format!("register_sync_concurrent_{workers}_threads");
        group.bench_function(&bench_name, move |b| {
            b.iter(|| {
                let rounds = 5000usize;
                let mut handles = Vec::with_capacity(workers);
                for w in 0..workers {
                    handles.push(std::thread::spawn(move || {
                        for i in 0..rounds {
                            let name = format!("conc_{workers}_{w}_{i}");
                            let m = Metric::register(&name, MetricType::Redis);
                            black_box(m);
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }
    group.finish();
}

fn record_latency(c: &mut Criterion) {
    let mut metrics: Vec<Metric> = Vec::with_capacity(10_000);
    for i in 0..10_000usize {
        metrics.push(Metric::register(
            &format!("record_metric_{i}"),
            MetricType::Redis,
        ));
    }

    c.bench_function("record_sync", |b| {
        let mut seq = 0u64;
        b.iter(|| {
            let idx = (seq % metrics.len() as u64) as usize;
            seq = seq.wrapping_add(1);
            let m = metrics[idx];
            let elapsed_ns = 1_000_000 + (idx as u64 % 9_000_000);
            m.record(Duration::from_nanos(elapsed_ns), (idx & 1) == 0);
            black_box(idx);
        });
    });
}

fn visit_cost(c: &mut Criterion) {
    let mut sum = MetricSnapshot {
        total: 0,
        success: 0,
        failure: 0,
        elapsed_ns: 0,
        slow: 0,
        intervals: [0; 5],
    };

    c.bench_function("visit", |b| {
        b.iter(|| {
            sum = MetricSnapshot {
                total: 0,
                success: 0,
                failure: 0,
                elapsed_ns: 0,
                slow: 0,
                intervals: [0; 5],
            };
            visit(|_name, _kind, s| {
                sum.total = sum.total.wrapping_add(s.total);
                sum.success = sum.success.wrapping_add(s.success);
                sum.failure = sum.failure.wrapping_add(s.failure);
                sum.elapsed_ns = sum.elapsed_ns.wrapping_add(s.elapsed_ns);
            });
            black_box(sum.total + sum.success + sum.failure + sum.elapsed_ns);
        });
    });
}

#[allow(dead_code)]
fn registry_len_probe() {
    println!("registered metrics: {}", len());
}

#[allow(dead_code)]
fn main_probe() {
    // quick manual smoke check for standalone `cargo run --bin metrics` if needed
    let start = Instant::now();
    let a = Metric::register("probe", MetricType::Redis);
    a.record(Duration::from_millis(1), true);
    visit(|name, kind, s| {
        println!(
            "{name} {:?} total={} success={} fail={} elapsed_ns={}",
            kind, s.total, s.success, s.failure, s.elapsed_ns
        );
    });
    println!("probe done in {:?}", start.elapsed());
}

criterion_group!(
    benches,
    register_sequential,
    register_concurrent,
    record_latency,
    visit_cost
);
criterion_main!(benches);
