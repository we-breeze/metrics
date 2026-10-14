use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::plan::{MetricPolicy, ProfileFormat};

/// Counts and microsecond duration sums for all, slow, and failed operations.
/// Six counters occupy 48 bytes, padded to one 64-byte cache line.
#[repr(C, align(64))]
#[derive(Debug, Default)]
pub(crate) struct Slot {
    count: AtomicU64,
    elapsed_us: AtomicU64,
    slow_count: AtomicU64,
    slow_elapsed_us: AtomicU64,
    failure_count: AtomicU64,
    failure_elapsed_us: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ItemPtr(NonNull<Slot>);

unsafe impl Send for ItemPtr {}
unsafe impl Sync for ItemPtr {}

impl ItemPtr {
    pub(crate) fn from_raw(slot: *mut Slot) -> Self {
        Self(NonNull::new(slot).expect("slot chunk cannot contain a null pointer"))
    }

    #[inline]
    fn as_ref(&self) -> &Slot {
        // Safety: a slot is stored in an append-only boxed chunk owned by its shard. Chunk
        // addresses never change and a slot is initialized before its pointer is returned.
        unsafe { self.0.as_ref() }
    }
}

/// Business resource metric category, independent of the exported metric name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResourceMetric {
    Req,
    Conn,
}

/// Business identity supplied by producers when registering a metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricType {
    Log,
    Redis(ResourceMetric),
    Mysql(ResourceMetric),
    Memcache(ResourceMetric),
    McDetail,
    Http(ResourceMetric),
    HttpAll,
    ApiAll,
    Api(u16),
    Api3xx,
    Api4xx,
    Api5xx,
    ApiTimeout,
    Rpc(ResourceMetric),
    Service(ResourceMetric),
    RpcServiceWhole,
}

impl MetricType {
    pub(crate) fn is_conn(self) -> bool {
        matches!(
            self,
            Self::Redis(ResourceMetric::Conn)
                | Self::Mysql(ResourceMetric::Conn)
                | Self::Memcache(ResourceMetric::Conn)
                | Self::Http(ResourceMetric::Conn)
                | Self::Rpc(ResourceMetric::Conn)
                | Self::Service(ResourceMetric::Conn)
        )
    }

    pub(crate) fn validate(self) {
        if let Self::Api(code) = self {
            assert!((100..=599).contains(&code), "invalid HTTP status code");
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MetricSpec {
    pub(crate) slow_threshold_us: u64,
}

impl MetricType {
    #[inline]
    pub(crate) fn policy(self) -> MetricPolicy {
        match self {
            Self::Log
            | Self::Redis(_)
            | Self::Mysql(_)
            | Self::Memcache(_)
            | Self::McDetail
            | Self::Http(_) => MetricPolicy::Resource,
            Self::HttpAll
            | Self::Service(_)
            | Self::ApiAll
            | Self::Api(_)
            | Self::Api3xx
            | Self::Api4xx
            | Self::Api5xx
            | Self::ApiTimeout => MetricPolicy::Service,
            Self::Rpc(_) | Self::RpcServiceWhole => MetricPolicy::Access,
        }
    }

    pub(crate) fn output(self) -> (&'static str, ProfileFormat) {
        match self {
            Self::Log => ("LOG", ProfileFormat::Resource),
            Self::Redis(_) => ("REDIS", ProfileFormat::Resource),
            Self::Mysql(_) => ("MYSQL", ProfileFormat::Resource),
            Self::Memcache(_) => ("MC", ProfileFormat::Resource),
            Self::McDetail => ("MCDETAIL", ProfileFormat::Resource),
            Self::Http(_) | Self::HttpAll => ("HTTP", ProfileFormat::Resource),
            Self::ApiAll => ("API", ProfileFormat::Resource),
            Self::Api(_) => ("API_RESPONSE", ProfileFormat::Resource),
            Self::Api3xx => ("API3XX", ProfileFormat::Resource),
            Self::Api4xx => ("API4XX", ProfileFormat::Resource),
            Self::Api5xx => ("API5XX", ProfileFormat::Resource),
            Self::ApiTimeout => ("APITO", ProfileFormat::Resource),
            Self::Service(_) => ("SERVICE", ProfileFormat::Resource),
            Self::Rpc(_) => ("RPC", ProfileFormat::AccessStatistic),
            Self::RpcServiceWhole => ("RPC_SERVICE_WHOLE", ProfileFormat::AccessStatistic),
        }
    }
}

impl MetricPolicy {
    #[inline]
    pub(crate) fn spec(self) -> MetricSpec {
        match self {
            Self::Resource => MetricSpec {
                slow_threshold_us: 50_000,
            },
            Self::Service | Self::Access => MetricSpec {
                slow_threshold_us: 200_000,
            },
        }
    }
}

/// An eventually-consistent view of the process-lifetime cumulative counters.
/// Duration sums use microseconds. Slow and failed operations may overlap.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetricSnapshot {
    pub total: u64,
    pub success: u64,
    pub failure: u64,
    pub elapsed_us: u64,
    pub slow: u64,
    pub slow_elapsed_us: u64,
    pub failure_elapsed_us: u64,
}

/// Copyable handle used by the record hot path.
#[derive(Debug, Clone, Copy)]
pub struct Metric {
    item: ItemPtr,
    policy: MetricPolicy,
}

impl Metric {
    /// Registers cumulative request counts and timing for one business object.
    pub fn new(kind: MetricType, name: &str) -> Self {
        kind.validate();
        assert!(
            !kind.is_conn(),
            "connection values use CounterMetric/GaugeMetric/ConnMetric"
        );
        crate::registry().register(name, kind)
    }

    /// Registers resource metadata once; recording retains only the compact handle.
    pub fn new_resource(kind: MetricType, labels: &crate::ResourceLabels) -> Self {
        kind.validate();
        assert!(!kind.is_conn(), "connection values use ConnMetric");
        crate::registry().register_resource(kind, labels)
    }

    pub(crate) fn from_item(item: ItemPtr, policy: MetricPolicy) -> Self {
        Self { item, policy }
    }

    /// Records a completed operation, truncating its duration to whole microseconds.
    ///
    /// Prometheus output converts microsecond sums to seconds.
    /// A slow failed operation contributes to all three count/duration pairs.
    #[inline]
    pub fn record(&self, elapsed: Duration, success: bool) {
        let slot = self.item.as_ref();
        let profile = self.policy.spec();
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

        if !success {
            slot.failure_count.fetch_add(1, Ordering::Relaxed);
            slot.failure_elapsed_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
        }
        slot.elapsed_us.fetch_add(elapsed_us, Ordering::Relaxed);
        if elapsed_us >= profile.slow_threshold_us {
            slot.slow_count.fetch_add(1, Ordering::Relaxed);
            slot.slow_elapsed_us
                .fetch_add(elapsed_us, Ordering::Relaxed);
        }
        slot.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one count-only occurrence without timing or failure data.
    ///
    /// Only the total count changes; all duration, failure, and slow counters remain unchanged.
    #[inline]
    pub fn increment(&self) {
        self.item.as_ref().count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns an eventually-consistent snapshot without blocking `record`.
    #[inline]
    pub fn snapshot(&self) -> MetricSnapshot {
        snapshot(self.item)
    }
}

#[inline]
pub(crate) fn snapshot(item: ItemPtr) -> MetricSnapshot {
    let slot = item.as_ref();
    MetricSnapshot::from_counters(
        slot.count.load(Ordering::Relaxed),
        slot.elapsed_us.load(Ordering::Relaxed),
        slot.slow_count.load(Ordering::Relaxed),
        slot.slow_elapsed_us.load(Ordering::Relaxed),
        slot.failure_count.load(Ordering::Relaxed),
        slot.failure_elapsed_us.load(Ordering::Relaxed),
    )
}

/// The six exported counters, ordered by Prometheus metric family.
#[derive(Clone, Copy)]
pub(crate) enum Counter {
    Total,
    Failure,
    Slow,
    Elapsed,
    FailureElapsed,
    SlowElapsed,
}

impl Counter {
    pub(crate) fn read(self, item: ItemPtr) -> u64 {
        let slot = item.as_ref();
        let counter = match self {
            Self::Total => &slot.count,
            Self::Failure => &slot.failure_count,
            Self::Slow => &slot.slow_count,
            Self::Elapsed => &slot.elapsed_us,
            Self::FailureElapsed => &slot.failure_elapsed_us,
            Self::SlowElapsed => &slot.slow_elapsed_us,
        };
        counter.load(Ordering::Relaxed)
    }
}

impl MetricSnapshot {
    #[inline]
    fn from_counters(
        total: u64,
        elapsed_us: u64,
        slow: u64,
        slow_elapsed_us: u64,
        failure: u64,
        failure_elapsed_us: u64,
    ) -> Self {
        Self {
            total,
            success: total.saturating_sub(failure),
            failure,
            elapsed_us,
            slow,
            slow_elapsed_us,
            failure_elapsed_us,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;

    #[test]
    fn slot_remains_one_cache_line() {
        assert_eq!(std::mem::size_of::<Slot>(), 64);
        assert_eq!(std::mem::align_of::<Slot>(), 64);
    }

    #[test]
    fn microsecond_counters_cover_thresholds_and_overlapping_failures() {
        for (kind, threshold_us) in [
            (MetricType::Redis(ResourceMetric::Req), 50_000),
            (MetricType::Service(ResourceMetric::Req), 200_000),
            (MetricType::RpcServiceWhole, 200_000),
        ] {
            let registry = Registry::new();
            let metric = registry.register("threshold", kind);
            metric.record(Duration::from_nanos(999), true);
            metric.record(Duration::from_micros(1_234), false);
            metric.record(Duration::from_nanos(threshold_us * 1_000 - 1), true);
            metric.record(Duration::from_micros(threshold_us), false);
            assert_eq!(
                metric.snapshot(),
                MetricSnapshot {
                    total: 4,
                    success: 2,
                    failure: 2,
                    elapsed_us: threshold_us * 2 + 1_233,
                    slow: 1,
                    slow_elapsed_us: threshold_us,
                    failure_elapsed_us: threshold_us + 1_234,
                },
                "{kind:?}"
            );
        }
    }

    #[test]
    fn oversized_duration_is_clamped_in_microseconds() {
        let registry = Registry::new();
        let metric = registry.register("duration-max", MetricType::Redis(ResourceMetric::Req));
        metric.record(Duration::MAX, false);
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.total, 1);
        assert_eq!(snapshot.elapsed_us, u64::MAX);
        assert_eq!(snapshot.slow_elapsed_us, u64::MAX);
        assert_eq!(snapshot.failure_elapsed_us, u64::MAX);
    }

    #[test]
    fn concurrent_record_and_snapshot_preserve_cumulative_counters() {
        const WORKERS: u64 = 4;
        const RECORDS: u64 = 10_000;
        let registry = Registry::new();
        let metric = registry.register("concurrent", MetricType::Redis(ResourceMetric::Req));
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..WORKERS)
                .map(|_| {
                    scope.spawn(move || {
                        for index in 0..RECORDS {
                            if index % 2 == 0 {
                                metric.record(Duration::from_millis(250), false);
                            } else {
                                metric.record(Duration::from_millis(5), true);
                            }
                        }
                    })
                })
                .collect();
            let mut previous = MetricSnapshot::default();
            while workers.iter().any(|worker| !worker.is_finished()) {
                let current = metric.snapshot();
                assert!(current.total >= previous.total);
                assert!(current.failure >= previous.failure);
                assert!(current.slow >= previous.slow);
                assert!(current.elapsed_us >= previous.elapsed_us);
                assert!(current.failure_elapsed_us >= previous.failure_elapsed_us);
                assert!(current.slow_elapsed_us >= previous.slow_elapsed_us);
                previous = current;
            }
            for worker in workers {
                worker.join().unwrap();
            }
        });
        let total = WORKERS * RECORDS;
        let expected = MetricSnapshot {
            total,
            success: total / 2,
            failure: total / 2,
            slow: total / 2,
            elapsed_us: total / 2 * 255_000,
            slow_elapsed_us: total / 2 * 250_000,
            failure_elapsed_us: total / 2 * 250_000,
        };
        assert_eq!(metric.snapshot(), expected);
        assert_eq!(metric.snapshot(), expected);
    }
}
