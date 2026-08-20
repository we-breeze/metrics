//! Process-wide, append-only metrics with ProfileUtil-compatible periodic logging.
//!
//! Registration is sharded for concurrent service startup. `visit` is always available: it reads
//! an immutable per-shard chunk index through `ds::Cow`, so it allocates nothing and never takes a
//! registration mutex. A new metadata chunk is published copy-on-write only once per 256 new
//! metrics in the same shard.

mod metric;
mod profile;
mod registry;

pub use metric::{Metric, MetricSnapshot, MetricType};

use profile::start_default_profile_logger;
use registry::Registry;
use std::sync::OnceLock;

static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

impl Metric {
    /// Returns the metric registered for `(name, metric_type)`.
    ///
    /// Registration takes only the mutex for the metric's shard. A new chunk index is published
    /// only when this shard crosses a 256-metric chunk boundary.
    pub fn register(name: &str, metric_type: MetricType) -> Self {
        let registry = registry();
        start_default_profile_logger(registry);
        registry.register(name, metric_type)
    }
}

/// Visits every metric visible at the per-shard prefixes observed during this call.
///
/// The traversal has no heap allocation and takes no registration mutex. Its order is fixed by
/// shard and chunk, but is not part of the public API contract.
pub fn visit<F>(mut f: F)
where
    F: FnMut(&str, MetricType, MetricSnapshot),
{
    registry().visit(|name, kind, snapshot| f(name, kind, snapshot));
}

/// Returns the current sum of the published prefix lengths across all shards.
pub fn len() -> usize {
    registry().len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn records_and_visits() {
        let registry = Registry::new();
        let first = registry.register("first", MetricType::Redis);
        let second = registry.register("second", MetricType::Redis);

        first.record(Duration::from_millis(11), true);
        second.record(Duration::from_millis(17), false);

        let mut snapshots = HashMap::new();
        registry.visit(|name, _, metric| {
            snapshots.insert(name.to_owned(), metric);
        });

        assert_eq!(registry.len(), 2);
        assert_eq!(snapshots["first"].total, 1);
        assert_eq!(snapshots["first"].success, 1);
        assert_eq!(snapshots["first"].elapsed_ns, 11_000_000);
        assert_eq!(snapshots["second"].failure, 1);
        assert_eq!(snapshots["second"].elapsed_ns, 17_000_000);
    }

    #[test]
    fn profile_log_matches_profile_util_fields_and_uses_plus_eight_clock() {
        let registry = Registry::new();
        let name = "cache.metric";
        let metric = registry.register(name, MetricType::Redis);
        metric.record(Duration::from_millis(5), true);
        metric.record(Duration::from_millis(50), false);
        metric.record(Duration::from_millis(250), true);

        let path = std::env::temp_dir().join(format!(
            "metrics-profile-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 8, 13, 17, 34, 18)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();

        let expected = concat!(
            "2026-08-13 17:34:18 ",
            "{\"type\":\"REDIS\",\"name\":\"cache.metric\",\"slowThreshold\":50,",
            "\"total_count\":3,\"error_count\":1,\"slow_count\":2,\"avg_time\":\"101.67\",",
            "\"interval1\":1,\"interval2\":0,\"interval3\":1,\"interval4\":0,\"interval5\":1}\n",
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), expected);

        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = content.lines().collect();
        assert_eq!(
            lines[1],
            concat!(
                "2026-08-13 17:34:18 ",
                "{\"type\":\"REDIS\",\"name\":\"cache.metric\",\"slowThreshold\":50,",
                "\"total_count\":0,\"error_count\":0,\"slow_count\":0,\"avg_time\":0.0,",
                "\"interval1\":0,\"interval2\":0,\"interval3\":0,\"interval4\":0,\"interval5\":0}",
            )
        );
        assert!(buffer.capacity() >= profile::PROFILE_BUFFER_LIMIT);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rpc_service_whole_profile_uses_access_statistic_shape() {
        let registry = Registry::new();
        let metric = registry.register(
            "org.example.EchoService.echo(java.lang.String)",
            MetricType::RpcServiceWhole,
        );
        metric.record(Duration::from_millis(150), true);
        metric.record(Duration::from_millis(250), false);

        let path = std::env::temp_dir().join(format!(
            "metrics-rpc-profile-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 8, 19, 10, 27, 13)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            concat!(
                "2026-08-19 10:27:13 ",
                "{\"type\":\"RPC_SERVICE_WHOLE\",\"name\":\"org.example.EchoService.echo(java.lang.String)\",",
                "\"slowThreshold\":\"200\",\"total_count\":\"2\",\"slow_count\":\"1\",\"avg_time\":\"200.00\",",
                "\"interval1\":\"0\",\"interval2\":\"0\",\"interval3\":\"0\",\"interval4\":\"2\",\"interval5\":\"0\",",
                "\"p75\":\"0.00\",\"p95\":\"0.00\",\"p98\":\"0.00\",\"p99\":\"0.00\",\"p999\":\"0.00\",",
                "\"biz_excp\":\"0\",\"other_excp\":\"1\",\"avg_tps\":\"0\",\"max_tps\":\"0\",\"min_tps\":\"0\"}\n"
            )
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn startup_registration_is_concurrent() {
        const WORKERS: usize = 16;
        const PER_WORKER: usize = 1_000;

        let registry = Arc::new(Registry::new());
        let start = Arc::new(Barrier::new(WORKERS));
        thread::scope(|scope| {
            for worker in 0..WORKERS {
                let registry = Arc::clone(&registry);
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    start.wait();
                    for index in 0..PER_WORKER {
                        registry.register(&format!("startup.{worker}.{index}"), MetricType::Redis);
                    }
                });
            }
        });

        let mut count = 0;
        registry.visit(|_, _, _| count += 1);
        assert_eq!(count, WORKERS * PER_WORKER);
        assert_eq!(registry.len(), WORKERS * PER_WORKER);
    }

    #[test]
    fn shard_index_changes_only_when_a_chunk_is_added() {
        let shard = registry::Shard::new();
        shard.register("metric.0", MetricType::Redis);
        let first_index = shard.index_reader.get();

        for index in 1..registry::CHUNK_SIZE {
            shard.register(&format!("metric.{index}"), MetricType::Redis);
        }
        let full_index = shard.index_reader.get();
        assert!(std::ptr::eq(&*first_index, &*full_index));

        shard.register("metric.next_chunk", MetricType::Redis);
        let next_index = shard.index_reader.get();
        assert!(!std::ptr::eq(&*full_index, &*next_index));
        assert_eq!(next_index.chunks.len(), 2);
    }

    #[test]
    fn visit_is_safe_while_registrations_are_running() {
        let registry = Arc::new(Registry::new());
        let writer = {
            let registry = Arc::clone(&registry);
            thread::spawn(move || {
                for index in 0..10_000 {
                    registry.register(&format!("racing.{index}"), MetricType::Redis);
                }
            })
        };

        while !writer.is_finished() {
            registry.visit(|_, _, _| {});
        }
        writer.join().unwrap();
        assert_eq!(registry.len(), 10_000);
    }
}
