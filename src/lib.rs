//! Process-wide, append-only metrics with ProfileUtil-compatible periodic logging.
//!
//! Registration is sharded for concurrent service startup. `visit` is always available: it reads
//! an immutable per-shard chunk index through `ArcSwap`, so it allocates nothing and never takes a
//! registration mutex. A new metadata chunk is published copy-on-write only once per 256 new
//! metrics in the same shard.

mod metric;
mod plan;
mod profile;
mod registry;
mod state;

pub use metric::{Metric, MetricSnapshot};

/// Registration primitives for producers that need several profile views of
/// one physical counter slot. Ordinary integrations should use the typed
/// [`Metric`] constructors instead.
pub mod producer {
    pub use crate::plan::{MetricPolicy, ProfileFormat, ProfilePlan};

    use crate::{Metric, registry, start_default_profile_logger};

    /// Registers an immutable multi-output profile rendering plan.
    pub fn register_profile(plan: ProfilePlan) -> Metric {
        let registry = registry();
        start_default_profile_logger(registry);
        registry.register_profile(plan)
    }
}

use profile::start_default_profile_logger;
use registry::Registry;
use state::StateType;
use std::fmt::Display;
use std::sync::OnceLock;

static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

impl Metric {
    fn register_single(name: &str, metric_type: metric::MetricType) -> Self {
        let registry = registry();
        start_default_profile_logger(registry);
        registry.register(name, metric_type)
    }

    /// Registers one process logging counter.
    pub fn log(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Log)
    }

    /// Registers one Redis endpoint metric.
    pub fn redis(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Redis)
    }

    /// Registers one MySQL host and operation metric.
    pub fn mysql(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Mysql)
    }

    /// Registers one Memcache port-level metric.
    pub fn mc(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Mc)
    }

    /// Registers one Memcache `port_up` or `port_down` detail metric.
    pub fn mc_detail(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::McDetail)
    }

    /// Registers one outbound HTTP endpoint metric.
    pub fn http(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Http)
    }

    /// Registers a whole-request HTTP metric using the supplied name.
    pub fn http_all(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::HttpAll)
    }

    /// Registers an inbound API metric with the service latency policy.
    pub fn api(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Api)
    }

    /// Registers an inbound API 3xx metric using the route template as name.
    pub fn api_3xx(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Api3xx)
    }

    /// Registers an inbound API 4xx metric using the route template as name.
    pub fn api_4xx(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Api4xx)
    }

    /// Registers an inbound API 5xx metric using the route template as name.
    pub fn api_5xx(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Api5xx)
    }

    /// Registers an inbound API timeout metric using the route template as name.
    pub fn api_timeout(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::ApiTimeout)
    }

    /// Registers a service metric with the legacy 200 ms slow threshold.
    pub fn service(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Service)
    }

    /// Registers a Motan provider whole-request metric.
    pub fn rpc_service_whole(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::RpcServiceWhole)
    }
}

/// Records one field of the latest process-level RPC state.
///
/// This function acquires the process-wide state lock. Call it only from asynchronous
/// control-plane or connection-state handling; do not call it from a request hot path.
/// Profile output maps this typed state to the legacy `MOTAN_CLUSTER_STAT` label.
pub fn record_rpc_state<K, V>(name: &str, key: K, value: V)
where
    K: Display,
    V: Display,
{
    let key = key.to_string();
    let value = value.to_string();
    let registry = registry();
    start_default_profile_logger(registry);
    registry.record_state(StateType::Rpc, name, &key, &value);
}

/// Visits every metric visible at the per-shard prefixes observed during this call.
///
/// The traversal has no heap allocation and takes no registration mutex. Its order is fixed by
/// shard and chunk, but is not part of the public API contract. Direct fanout aliases are visited;
/// flush-time aggregate rows are produced only by the profile writer.
pub fn visit<F>(mut f: F)
where
    F: FnMut(&str, &str, MetricSnapshot),
{
    registry().visit(|meta, snapshot| match &meta.profile {
        plan::ProfileMeta::Single { name, kind } => {
            let (metric_type, _) = kind.output();
            f(name, metric_type, snapshot);
        }
        plan::ProfileMeta::Fanout(plan) => {
            for output in plan.outputs.iter() {
                f(&output.name, &output.metric_type, snapshot);
            }
        }
    });
}

/// Returns the current sum of the published prefix lengths across all shards.
pub fn len() -> usize {
    registry().len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::MetricType;
    use crate::plan::ProfileMeta;
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
        registry.visit(|meta, metric| {
            let ProfileMeta::Single { name, .. } = &meta.profile else {
                panic!("test registered only single-output metrics");
            };
            snapshots.insert(name.to_string(), metric);
        });

        assert_eq!(registry.len(), 2);
        assert_eq!(snapshots["first"].total, 1);
        assert_eq!(snapshots["first"].success, 1);
        assert_eq!(snapshots["first"].elapsed_us, 11_000);
        assert_eq!(snapshots["second"].failure, 1);
        assert_eq!(snapshots["second"].elapsed_us, 17_000);
    }

    #[test]
    fn log_metric_increment_records_only_a_count() {
        let registry = Registry::new();
        let metric = registry.register("queue_dropped", MetricType::Log);

        metric.increment();
        metric.increment();

        let snapshot = metric.snapshot();
        assert_eq!(snapshot.total, 2);
        assert_eq!(snapshot.success, 2);
        assert_eq!(snapshot.failure, 0);
        assert_eq!(snapshot.elapsed_us, 0);
        assert_eq!(snapshot.slow, 0);
        assert_eq!(snapshot.slow_elapsed_us, 0);
        assert_eq!(snapshot.failure_elapsed_us, 0);
        assert_eq!(
            MetricType::Log.output(),
            ("LOG", crate::plan::ProfileFormat::Resource)
        );
    }

    fn profile_rows(registry: &Registry, interval: Duration) -> Vec<serde_json::Value> {
        let path = std::env::temp_dir().join(format!(
            "metrics-profile-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let timestamp = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS)
            .unwrap()
            .with_ymd_and_hms(2026, 8, 13, 17, 34, 18)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer, interval)
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let rows = content
            .lines()
            .map(|line| {
                assert!(line.starts_with("2026-08-13 17:34:18 "));
                let row: serde_json::Value = serde_json::from_str(&line[20..]).unwrap();
                assert!(
                    !row.as_object()
                        .unwrap()
                        .keys()
                        .any(|key| key.starts_with("interval"))
                );
                row
            })
            .collect();
        assert!(buffer.capacity() >= profile::PROFILE_BUFFER_LIMIT);
        fs::remove_file(path).unwrap();
        rows
    }

    #[test]
    fn profile_log_reports_three_groups_using_actual_window_and_milliseconds() {
        let registry = Registry::new();
        let metric = registry.register("cache.metric", MetricType::Redis);
        metric.record(Duration::from_millis(5), true);
        metric.record(Duration::from_millis(50), false);
        metric.record(Duration::from_millis(250), true);

        let rows = profile_rows(&registry, Duration::from_millis(2500));
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            serde_json::json!({
                "type": "REDIS", "name": "cache.metric", "slowThreshold": 50.00,
                "total_count": 3, "error_count": 1, "slow_count": 2,
                "qps": "1.20", "total_time": "305.00", "avg_time": "101.67",
                "slow_qps": "0.80", "slow_time": "300.00", "slow_avg_time": "150.00",
                "error_qps": "0.40", "error_time": "50.00", "error_avg_time": "50.00"
            })
        );
        assert_eq!(rows[1]["name"], "other://profile_baseline");
        assert_eq!(rows[1]["avg_time"], "1.00");
        assert_eq!(metric.snapshot(), MetricSnapshot::default());

        let empty = profile_rows(&registry, Duration::from_secs(10));
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0]["name"], "other://profile_baseline");
    }

    #[test]
    fn rpc_state_is_persistent_and_uses_the_legacy_profile_shape() {
        let registry = Registry::new();
        registry.record_state(StateType::Rpc, "group.Service", "nodes", "4");
        registry.record_state(StateType::Rpc, "group.Service", "unavailable", "1");
        let first = profile_rows(&registry, Duration::from_secs(10));
        assert_eq!(
            first[0],
            serde_json::json!({
                "type": "MOTAN_CLUSTER_STAT", "name": "group.Service", "nodes": "4", "unavailable": "1"
            })
        );
        registry.record_state(StateType::Rpc, "group.Service", "unavailable", "0");
        let second = profile_rows(&registry, Duration::from_secs(10));
        assert_eq!(second[0]["nodes"], "4");
        assert_eq!(second[0]["unavailable"], "0");
        assert_eq!(second[1]["name"], "other://profile_baseline");
    }

    #[test]
    fn service_profile_matches_resource_format() {
        let registry = Registry::new();
        registry
            .register("example.Service.operation", MetricType::Service)
            .record(Duration::from_millis(201), true);
        let rows = profile_rows(&registry, Duration::from_secs(10));
        assert_eq!(rows[0]["type"], "SERVICE");
        assert_eq!(rows[0]["slowThreshold"], 200.0);
        assert_eq!(rows[0]["total_count"], 1);
        assert_eq!(rows[0]["avg_time"], "201.00");
        assert_eq!(rows[0]["slow_avg_time"], "201.00");
        assert_eq!(rows[0]["error_avg_time"], "0.00");
        assert_eq!(rows[0]["error_qps"], "0.00");
    }

    #[test]
    fn memcache_profile_matches_mc_and_mcdetail_formats() {
        let registry = Registry::new();
        for kind in [MetricType::Mc, MetricType::McDetail] {
            let metric = registry.register("15138", kind);
            metric.record(Duration::from_millis(5), true);
            metric.record(Duration::from_millis(60), false);
        }
        let rows = profile_rows(&registry, Duration::from_secs(10));
        assert_eq!(rows.len(), 3);
        for row in &rows[..2] {
            assert!(row["type"] == "MC" || row["type"] == "MCDETAIL");
            assert_eq!(row["total_count"], 2);
            assert_eq!(row["avg_time"], "32.50");
            assert_eq!(row["slow_count"], 1);
            assert_eq!(row["slow_avg_time"], "60.00");
            assert_eq!(row["error_count"], 1);
            assert_eq!(row["error_avg_time"], "60.00");
        }
    }

    #[test]
    fn http_profile_uses_endpoint_and_whole_request_thresholds() {
        let registry = Registry::new();
        registry
            .register("http://service/api", MetricType::Http)
            .record(Duration::from_millis(60), true);
        registry
            .register("all_http://service/api", MetricType::HttpAll)
            .record(Duration::from_millis(60), true);
        let rows = profile_rows(&registry, Duration::from_secs(10));
        let endpoint = rows
            .iter()
            .find(|row| row["name"] == "http://service/api")
            .unwrap();
        let whole = rows
            .iter()
            .find(|row| row["name"] == "all_http://service/api")
            .unwrap();
        assert_eq!(endpoint["slowThreshold"], 50.0);
        assert_eq!(endpoint["slow_count"], 1);
        assert_eq!(endpoint["slow_avg_time"], "60.00");
        assert_eq!(whole["slowThreshold"], 200.0);
        assert_eq!(whole["slow_count"], 0);
        assert_eq!(whole["slow_avg_time"], "0.00");
        assert_eq!(whole["avg_time"], "60.00");
    }

    #[test]
    fn rpc_service_whole_profile_uses_access_statistic_shape() {
        let registry = Registry::new();
        let metric = registry.register("example.EchoService.echo", MetricType::RpcServiceWhole);
        metric.record(Duration::from_millis(150), true);
        metric.record(Duration::from_millis(250), false);
        let rows = profile_rows(&registry, Duration::from_secs(4));
        let row = &rows[0];
        assert_eq!(row["type"], "RPC_SERVICE_WHOLE");
        assert_eq!(row["slowThreshold"], "200.00");
        assert_eq!(row["total_count"], "2");
        assert_eq!(row["error_count"], "1");
        assert_eq!(row["slow_count"], "1");
        assert_eq!(row["qps"], "0.50");
        assert_eq!(row["avg_tps"], "0.50");
        assert_eq!(row["avg_time"], "200.00");
        assert_eq!(row["slow_qps"], "0.25");
        assert_eq!(row["slow_avg_time"], "250.00");
        assert_eq!(row["error_qps"], "0.25");
        assert_eq!(row["error_avg_time"], "250.00");
        assert_eq!(row["other_excp"], "1");
    }

    #[test]
    fn one_slot_fans_out_and_flush_time_aggregate_merges_methods() {
        use crate::plan::{MetricPolicy, ProfileFormat, ProfilePlan};
        let registry = Registry::new();
        let first = registry.register_profile(
            ProfilePlan::new(
                "first",
                MetricPolicy::Access,
                ProfileFormat::AccessStatistic,
                "RPC_SERVICE_CLIENT",
                "example.Service.first",
            )
            .alias("SERVICE", "example.Service.first")
            .alias("RPC_SERVICE_CLIENT_V2", "module_Service.first")
            .aggregate("SERVICE", "app_module"),
        );
        let second = registry.register_profile(
            ProfilePlan::new(
                "second",
                MetricPolicy::Access,
                ProfileFormat::AccessStatistic,
                "RPC_SERVICE_CLIENT",
                "example.Service.second",
            )
            .alias("SERVICE", "example.Service.second")
            .alias("RPC_SERVICE_CLIENT_V2", "module_Service.second")
            .aggregate("SERVICE", "app_module"),
        );
        first.record(Duration::from_millis(250), false);
        first.record(Duration::from_millis(150), true);
        second.record(Duration::from_millis(450), true);
        let rows = profile_rows(&registry, Duration::from_secs(2));
        assert_eq!(rows.len(), 8);
        let aliases: Vec<_> = rows
            .iter()
            .filter(|row| {
                row["name"] == "example.Service.first" || row["name"] == "module_Service.first"
            })
            .collect();
        assert_eq!(aliases.len(), 3);
        for alias in aliases {
            assert_eq!(alias["total_count"], "2");
            assert_eq!(alias["avg_time"], "200.00");
            assert_eq!(alias["error_avg_time"], "250.00");
        }
        let aggregate = rows.iter().find(|row| row["name"] == "app_module").unwrap();
        assert_eq!(aggregate["total_count"], "3");
        assert_eq!(aggregate["total_time"], "850.00");
        assert_eq!(aggregate["qps"], "1.50");
        assert_eq!(aggregate["avg_time"], "283.33");
        assert_eq!(aggregate["slow_count"], "2");
        assert_eq!(aggregate["slow_time"], "700.00");
        assert_eq!(aggregate["slow_qps"], "1.00");
        assert_eq!(aggregate["slow_avg_time"], "350.00");
        assert_eq!(aggregate["error_count"], "1");
        assert_eq!(aggregate["error_avg_time"], "250.00");
    }

    #[test]
    fn profile_strings_round_trip_without_injecting_json_or_log_lines() {
        use crate::plan::{MetricPolicy, ProfileFormat, ProfilePlan};

        let registry = Registry::new();
        let hostile = format!(
            "quoted\"\\ unicode-测试 {}\n{{\"type\":\"FORGED\"}}",
            (0u8..=31).map(char::from).collect::<String>()
        );
        for (identity, policy, format) in [
            ("resource", MetricPolicy::Resource, ProfileFormat::Resource),
            (
                "access",
                MetricPolicy::Access,
                ProfileFormat::AccessStatistic,
            ),
        ] {
            registry
                .register_profile(ProfilePlan::new(
                    identity, policy, format, &*hostile, &*hostile,
                ))
                .increment();
        }
        registry.record_state(StateType::Rpc, &hostile, &hostile, &hostile);
        let directory = std::env::temp_dir().join(format!(
            "metrics-json-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("profile.log");
        let timestamp = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS)
            .unwrap()
            .with_ymd_and_hms(2026, 9, 8, 12, 0, 0)
            .single()
            .unwrap();
        registry
            .write_profile_log(&path, timestamp, &mut Vec::new(), Duration::from_secs(10))
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let rows: Vec<serde_json::Value> = content
            .lines()
            .map(|line| serde_json::from_str(&line[20..]).unwrap())
            .collect();
        assert_eq!(rows.len(), 4);
        for row in &rows[..2] {
            assert_eq!(row["type"], hostile);
            assert_eq!(row["name"], hostile);
        }
        assert_eq!(rows[2]["type"], "MOTAN_CLUSTER_STAT");
        assert_eq!(rows[2]["name"], hostile);
        assert_eq!(rows[2][&hostile], hostile);
        assert_eq!(rows[3]["name"], "other://profile_baseline");
        fs::remove_dir_all(directory).unwrap();
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
        registry.visit(|_, _| count += 1);
        assert_eq!(count, WORKERS * PER_WORKER);
        assert_eq!(registry.len(), WORKERS * PER_WORKER);
    }

    #[test]
    fn shard_index_changes_only_when_a_chunk_is_added() {
        let shard = registry::Shard::new();
        shard.register("metric.0", MetricType::Redis);
        let first_index = shard.published_index.load();

        for index in 1..registry::CHUNK_SIZE {
            shard.register(&format!("metric.{index}"), MetricType::Redis);
        }
        let full_index = shard.published_index.load();
        assert!(Arc::ptr_eq(&first_index, &full_index));

        shard.register("metric.next_chunk", MetricType::Redis);
        let next_index = shard.published_index.load();
        assert!(!Arc::ptr_eq(&full_index, &next_index));
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
            registry.visit(|_, _| {});
        }
        writer.join().unwrap();
        assert_eq!(registry.len(), 10_000);
    }
}
