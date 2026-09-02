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

    /// Registers the `all_`-prefixed whole-request HTTP metric.
    pub fn http_all(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::HttpAll)
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
        assert_eq!(snapshots["first"].elapsed_ns, 11_000_000);
        assert_eq!(snapshots["second"].failure, 1);
        assert_eq!(snapshots["second"].elapsed_ns, 17_000_000);
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
        assert_eq!(snapshot.elapsed_ns, 0);
        assert_eq!(snapshot.slow, 0);
        assert_eq!(snapshot.intervals, [2, 0, 0, 0, 0]);
        assert_eq!(
            MetricType::Log.output(),
            ("LOG", crate::plan::ProfileFormat::Resource)
        );
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
            "2026-08-13 17:34:18 ",
            "{\"type\":\"OTHER\",\"name\":\"other://profile_baseline\",\"total_count\":10,",
            "\"error_count\":1,\"slow_count\":1,\"avg_time\":1.0,\"interval1\":6,",
            "\"interval2\":1,\"interval3\":1,\"interval4\":1,\"interval5\":1}\n",
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), expected);

        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = content.lines().collect();
        assert_eq!(
            lines[2],
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
    fn rpc_state_is_persistent_and_uses_the_legacy_profile_shape() {
        let registry = Registry::new();
        registry.record_state(StateType::Rpc, "group_com.example.Service", "nodes", "4");
        registry.record_state(
            StateType::Rpc,
            "group_com.example.Service",
            "unavailable",
            "1",
        );

        let path = std::env::temp_dir().join(format!(
            "metrics-rpc-state-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 9, 1, 4, 30, 20)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);

        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains(concat!(
            "2026-09-01 04:30:20 ",
            "{\"type\":\"MOTAN_CLUSTER_STAT\",\"name\":\"group_com.example.Service\",",
            "\"nodes\":\"4\",\"unavailable\":\"1\"}\n",
        )));

        registry.record_state(
            StateType::Rpc,
            "group_com.example.Service",
            "unavailable",
            "0",
        );
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.ends_with(concat!(
            "2026-09-01 04:30:20 ",
            "{\"type\":\"OTHER\",\"name\":\"other://profile_baseline\",",
            "\"total_count\":10,\"error_count\":1,\"slow_count\":1,\"avg_time\":1.0,",
            "\"interval1\":6,\"interval2\":1,\"interval3\":1,\"interval4\":1,",
            "\"interval5\":1}\n",
        )));
        assert!(content.contains(concat!(
            "{\"type\":\"MOTAN_CLUSTER_STAT\",\"name\":\"group_com.example.Service\",",
            "\"nodes\":\"4\",\"unavailable\":\"0\"}\n",
        )));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn service_profile_matches_resource_format() {
        let registry = Registry::new();
        let metric = registry.register(
            "org.example.service.ExampleService.operation",
            MetricType::Service,
        );
        metric.record(Duration::from_millis(201), true);

        let path = std::env::temp_dir().join(format!(
            "metrics-service-profile-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 8, 23, 22, 25, 38)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            concat!(
                "2026-08-23 22:25:38 ",
                "{\"type\":\"SERVICE\",\"name\":\"org.example.service.ExampleService.operation\",",
                "\"slowThreshold\":200,\"total_count\":1,\"error_count\":0,\"slow_count\":1,\"avg_time\":\"201.00\",",
                "\"interval1\":0,\"interval2\":0,\"interval3\":0,\"interval4\":0,\"interval5\":1}\n",
                "2026-08-23 22:25:38 ",
                "{\"type\":\"OTHER\",\"name\":\"other://profile_baseline\",\"total_count\":10,",
                "\"error_count\":1,\"slow_count\":1,\"avg_time\":1.0,\"interval1\":6,",
                "\"interval2\":1,\"interval3\":1,\"interval4\":1,\"interval5\":1}\n",
            )
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn memcache_profile_matches_mc_and_mcdetail_formats() {
        let registry = Registry::new();
        let aggregate = registry.register("15138", MetricType::Mc);
        let detail = registry.register("15138_down", MetricType::McDetail);
        aggregate.record(Duration::from_millis(5), true);
        aggregate.record(Duration::from_millis(60), false);
        detail.record(Duration::from_millis(5), true);
        detail.record(Duration::from_millis(60), false);

        let path = std::env::temp_dir().join(format!(
            "metrics-memcache-profile-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 9, 1, 12, 31, 31)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 3);
        assert!(content.contains(concat!(
            "{\"type\":\"MC\",\"name\":\"15138\",\"slowThreshold\":50,",
            "\"total_count\":2,\"error_count\":1,\"slow_count\":1,\"avg_time\":\"32.50\",",
            "\"interval1\":1,\"interval2\":0,\"interval3\":1,\"interval4\":0,\"interval5\":0}"
        )));
        assert!(content.contains(concat!(
            "{\"type\":\"MCDETAIL\",\"name\":\"15138_down\",\"slowThreshold\":50,",
            "\"total_count\":2,\"error_count\":1,\"slow_count\":1,\"avg_time\":\"32.50\",",
            "\"interval1\":1,\"interval2\":0,\"interval3\":1,\"interval4\":0,\"interval5\":0}"
        )));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn http_profile_uses_endpoint_and_whole_request_thresholds() {
        let registry = Registry::new();
        let endpoint = registry.register(
            "http://service.example.com/api/config",
            MetricType::Http,
        );
        let whole = registry.register(
            "all_http://service.example.com/api/config",
            MetricType::HttpAll,
        );
        endpoint.record(Duration::from_millis(60), true);
        whole.record(Duration::from_millis(60), true);

        let path = std::env::temp_dir().join(format!(
            "metrics-http-profile-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 9, 1, 12, 31, 31)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains(concat!(
            "{\"type\":\"HTTP\",\"name\":\"http://service.example.com/api/config\",",
            "\"slowThreshold\":50,\"total_count\":1,\"error_count\":0,",
            "\"slow_count\":1,\"avg_time\":\"60.00\",\"interval1\":0,",
            "\"interval2\":0,\"interval3\":1,\"interval4\":0,\"interval5\":0}"
        )));
        assert!(content.contains(concat!(
            "{\"type\":\"HTTP\",\"name\":\"all_http://service.example.com/api/config\",",
            "\"slowThreshold\":200,\"total_count\":1,\"error_count\":0,",
            "\"slow_count\":0,\"avg_time\":\"60.00\",\"interval1\":0,",
            "\"interval2\":0,\"interval3\":1,\"interval4\":0,\"interval5\":0}"
        )));
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
                "\"biz_excp\":\"0\",\"other_excp\":\"1\",\"avg_tps\":\"0\",\"max_tps\":\"0\",\"min_tps\":\"0\"}\n",
                "2026-08-19 10:27:13 ",
                "{\"type\":\"OTHER\",\"name\":\"other://profile_baseline\",\"total_count\":10,",
                "\"error_count\":1,\"slow_count\":1,\"avg_time\":1.0,\"interval1\":6,",
                "\"interval2\":1,\"interval3\":1,\"interval4\":1,\"interval5\":1}\n",
            )
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn one_slot_fans_out_and_flush_time_aggregate_merges_methods() {
        use crate::plan::{MetricPolicy, ProfileFormat, ProfilePlan};

        let registry = Registry::new();
        let first = registry.register_profile(
            ProfilePlan::new(
                "client|app|module|Service.first(void)",
                MetricPolicy::Access,
                ProfileFormat::AccessStatistic,
                "RPC_SERVICE_CLIENT",
                "example.Service.first(void)",
            )
            .alias("SERVICE", "example.Service.first(void)")
            .alias("RPC_SERVICE_CLIENT_V2", "module_Service.first(void)")
            .aggregate("SERVICE", "app_module"),
        );
        let second = registry.register_profile(
            ProfilePlan::new(
                "client|app|module|Service.second(void)",
                MetricPolicy::Access,
                ProfileFormat::AccessStatistic,
                "RPC_SERVICE_CLIENT",
                "example.Service.second(void)",
            )
            .alias("SERVICE", "example.Service.second(void)")
            .alias("RPC_SERVICE_CLIENT_V2", "module_Service.second(void)")
            .aggregate("SERVICE", "app_module"),
        );
        first.record(Duration::from_millis(10), true);
        second.record(Duration::from_millis(30), true);

        let path = std::env::temp_dir().join(format!(
            "metrics-profile-fanout-test-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let offset = FixedOffset::east_opt(profile::SHANGHAI_OFFSET_SECONDS).unwrap();
        let timestamp = offset
            .with_ymd_and_hms(2026, 9, 1, 13, 0, 0)
            .single()
            .unwrap();
        let mut buffer = Vec::with_capacity(profile::PROFILE_BUFFER_LIMIT);
        registry
            .write_profile_log(&path, timestamp, &mut buffer)
            .unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 8);
        assert!(content.contains(
            "\"type\":\"RPC_SERVICE_CLIENT_V2\",\"name\":\"module_Service.first(void)\""
        ));
        assert!(content.contains("\"type\":\"SERVICE\",\"name\":\"example.Service.second(void)\""));
        assert!(content.contains(concat!(
            "{\"type\":\"SERVICE\",\"name\":\"app_module\",\"slowThreshold\":\"200\",",
            "\"total_count\":\"2\",\"slow_count\":\"0\",\"avg_time\":\"20.00\",",
            "\"interval1\":\"0\",\"interval2\":\"2\""
        )));
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
