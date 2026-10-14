//! Process-wide cumulative metrics with streaming Prometheus text exposition.
//!
//! Registration is sharded for concurrent service startup. `visit` is always available: it reads
//! an immutable per-shard chunk index through `ArcSwap`, so it allocates nothing and never takes a
//! registration mutex. A new metadata chunk is published copy-on-write only once per 256 new
//! metrics in the same shard.

mod metric;
mod plan;
pub mod prometheus;
mod registry;
mod resource;
mod scalar;
mod state;

pub use metric::{Metric, MetricSnapshot, MetricType, ResourceMetric};
pub use resource::{ConnectionRole, MetricAccess, ResourceLabels};
pub use scalar::{ConnMetric, ConnectionGuard, CounterMetric, GaugeMetric};
/// Cumulative request timing handle.
pub type TimingMetric = Metric;
pub use prometheus::PrometheusReader;

/// Registration primitives for producers that need several exported views of
/// one physical counter slot. Ordinary integrations should use the typed
/// [`Metric`] constructors instead.
pub mod producer {
    pub use crate::plan::{MetricPolicy, ProfileFormat, ProfilePlan};

    use crate::{Metric, registry};

    /// Registers an immutable multi-output export plan.
    pub fn register_profile(plan: ProfilePlan) -> Metric {
        let registry = registry();
        registry.register_profile(plan)
    }
}

use registry::Registry;
use state::StateType;
use std::fmt::Display;
use std::sync::{Arc, OnceLock};

static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();

fn registry() -> &'static Arc<Registry> {
    REGISTRY.get_or_init(|| Arc::new(Registry::new()))
}

impl Metric {
    fn register_single(name: &str, metric_type: metric::MetricType) -> Self {
        let registry = registry();
        registry.register(name, metric_type)
    }

    /// Registers one process logging counter.
    pub fn log(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Log)
    }

    /// Registers one Redis endpoint metric.
    pub fn redis(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Redis(crate::ResourceMetric::Req))
    }

    /// Registers one MySQL host and operation metric.
    pub fn mysql(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Mysql(crate::ResourceMetric::Req))
    }

    /// Registers one Memcache port-level metric.
    pub fn mc(name: &str) -> Self {
        Self::register_single(
            name,
            metric::MetricType::Memcache(crate::ResourceMetric::Req),
        )
    }

    /// Registers one Memcache `port_up` or `port_down` detail metric.
    pub fn mc_detail(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::McDetail)
    }

    /// Registers one outbound HTTP endpoint metric.
    pub fn http(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::Http(crate::ResourceMetric::Req))
    }

    /// Registers a whole-request HTTP metric using the supplied name.
    pub fn http_all(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::HttpAll)
    }

    /// Registers an inbound API metric for all requests with the 200 ms slow policy.
    pub fn api(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::ApiAll)
    }

    /// Registers a count-only API response metric for an exact HTTP status code.
    ///
    /// Use `increment()` for each response. Exposition emits only
    /// `brz_api_resp_total` with `type="API"`, `name`, and `status_code` labels.
    /// This does not also record an all-request metric; the producer owns that observation.
    ///
    /// # Panics
    /// Panics if the status code is outside the HTTP status range 100..=599.
    pub fn api_status(name: &str, status_code: u16) -> Self {
        assert!(
            (100..=599).contains(&status_code),
            "invalid HTTP status code"
        );
        Self::register_single(name, metric::MetricType::Api(status_code))
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

    /// Registers a count-only timeout metric using the route template as name.
    /// Exposition emits `brz_api_timeout_total`; use `increment()` per timeout.
    pub fn api_timeout(name: &str) -> Self {
        Self::register_single(name, metric::MetricType::ApiTimeout)
    }

    /// Registers a service metric with the legacy 200 ms slow threshold.
    pub fn service(name: &str) -> Self {
        Self::register_single(
            name,
            metric::MetricType::Service(crate::ResourceMetric::Req),
        )
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
/// Prometheus exports finite numeric fields as gauges with type `MOTAN_CLUSTER_STAT`.
/// Non-numeric fields remain stored but are omitted from exposition.
pub fn record_rpc_state<K, V>(name: &str, key: K, value: V)
where
    K: Display,
    V: Display,
{
    let key = key.to_string();
    let value = value.to_string();
    let registry = registry();
    registry.record_state(StateType::Rpc, name, &key, &value);
}

/// Visits every metric visible at the per-shard prefixes observed during this call.
///
/// The traversal has no heap allocation and takes no registration mutex. Its order is fixed by
/// shard and chunk, but is not part of the public API contract. Direct fanout aliases are visited;
/// aggregate rows are produced only by the Prometheus reader.
pub fn visit<F>(mut f: F)
where
    F: FnMut(&str, &str, MetricSnapshot),
{
    registry().visit(|meta, snapshot| match &meta.profile {
        plan::ProfileMeta::Single { name, kind } => {
            let (metric_type, _) = kind.output();
            f(name, metric_type, snapshot);
        }
        plan::ProfileMeta::Resource { labels, kind } => f(&labels.name, kind.output().0, snapshot),
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
    use std::collections::HashMap;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn records_and_visits() {
        let registry = Registry::new();
        let first = registry.register("first", MetricType::Redis(crate::ResourceMetric::Req));
        let second = registry.register("second", MetricType::Redis(crate::ResourceMetric::Req));

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
                        registry.register(
                            &format!("startup.{worker}.{index}"),
                            MetricType::Redis(crate::ResourceMetric::Req),
                        );
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
        shard.register("metric.0", MetricType::Redis(crate::ResourceMetric::Req));
        let first_index = shard.published_index.load();

        for index in 1..registry::CHUNK_SIZE {
            shard.register(
                &format!("metric.{index}"),
                MetricType::Redis(crate::ResourceMetric::Req),
            );
        }
        let full_index = shard.published_index.load();
        assert!(Arc::ptr_eq(&first_index, &full_index));

        shard.register(
            "metric.next_chunk",
            MetricType::Redis(crate::ResourceMetric::Req),
        );
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
                    registry.register(
                        &format!("racing.{index}"),
                        MetricType::Redis(crate::ResourceMetric::Req),
                    );
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
