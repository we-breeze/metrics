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

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MetricType {
    /// Process logging counters such as bounded-queue drops.
    Log,
    Redis,
    Mysql,
    Mc,
    McDetail,
    /// One outbound HTTP request, keyed by its stable endpoint URL.
    Http,
    /// Whole-request HTTP timing, including response-body consumption.
    HttpAll,
    /// One inbound API route and HTTP status class.
    Api,
    Api3xx,
    Api4xx,
    Api5xx,
    ApiTimeout,
    /// ProfileUtil service metric. This uses the resource-shaped JSON fields
    /// but has the service slow threshold expected by legacy dashboards.
    Service,
    RpcServiceWhole,
}

#[derive(Clone, Copy)]
pub(crate) struct MetricSpec {
    pub(crate) slow_threshold_us: u64,
}

impl MetricType {
    #[inline]
    pub(crate) fn policy(self) -> MetricPolicy {
        match self {
            Self::Log | Self::Redis | Self::Mysql | Self::Mc | Self::McDetail | Self::Http => {
                MetricPolicy::Resource
            }
            Self::HttpAll
            | Self::Service
            | Self::Api
            | Self::Api3xx
            | Self::Api4xx
            | Self::Api5xx
            | Self::ApiTimeout => MetricPolicy::Service,
            Self::RpcServiceWhole => MetricPolicy::Access,
        }
    }

    pub(crate) fn output(self) -> (&'static str, ProfileFormat) {
        match self {
            Self::Log => ("LOG", ProfileFormat::Resource),
            Self::Redis => ("REDIS", ProfileFormat::Resource),
            Self::Mysql => ("MYSQL", ProfileFormat::Resource),
            Self::Mc => ("MC", ProfileFormat::Resource),
            Self::McDetail => ("MCDETAIL", ProfileFormat::Resource),
            Self::Http | Self::HttpAll => ("HTTP", ProfileFormat::Resource),
            Self::Api => ("API", ProfileFormat::Resource),
            Self::Api3xx => ("API3XX", ProfileFormat::Resource),
            Self::Api4xx => ("API4XX", ProfileFormat::Resource),
            Self::Api5xx => ("API5XX", ProfileFormat::Resource),
            Self::ApiTimeout => ("APITO", ProfileFormat::Resource),
            Self::Service => ("SERVICE", ProfileFormat::Resource),
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

/// An eventually-consistent view of the current interval's counters.
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
    pub(crate) fn new(item: ItemPtr, policy: MetricPolicy) -> Self {
        Self { item, policy }
    }

    /// Records a completed operation, truncating its duration to whole microseconds.
    ///
    /// Profile output formats durations in milliseconds with two decimal places.
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

#[inline]
pub(crate) fn drain(item: ItemPtr) -> MetricSnapshot {
    let slot = item.as_ref();
    MetricSnapshot::from_counters(
        slot.count.swap(0, Ordering::AcqRel),
        slot.elapsed_us.swap(0, Ordering::AcqRel),
        slot.slow_count.swap(0, Ordering::AcqRel),
        slot.slow_elapsed_us.swap(0, Ordering::AcqRel),
        slot.failure_count.swap(0, Ordering::AcqRel),
        slot.failure_elapsed_us.swap(0, Ordering::AcqRel),
    )
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

    pub(crate) fn merge(&mut self, other: Self) {
        self.total = self.total.saturating_add(other.total);
        self.failure = self.failure.saturating_add(other.failure);
        self.success = self.total.saturating_sub(self.failure);
        self.elapsed_us = self.elapsed_us.saturating_add(other.elapsed_us);
        self.slow = self.slow.saturating_add(other.slow);
        self.slow_elapsed_us = self.slow_elapsed_us.saturating_add(other.slow_elapsed_us);
        self.failure_elapsed_us = self
            .failure_elapsed_us
            .saturating_add(other.failure_elapsed_us);
    }

    pub(crate) fn is_empty(self) -> bool {
        self.total == 0
            && self.elapsed_us == 0
            && self.slow == 0
            && self.slow_elapsed_us == 0
            && self.failure == 0
            && self.failure_elapsed_us == 0
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
            (MetricType::Redis, 50_000),
            (MetricType::Service, 200_000),
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
        let metric = registry.register("duration-max", MetricType::Redis);
        metric.record(Duration::MAX, false);
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.total, 1);
        assert_eq!(snapshot.elapsed_us, u64::MAX);
        assert_eq!(snapshot.slow_elapsed_us, u64::MAX);
        assert_eq!(snapshot.failure_elapsed_us, u64::MAX);
    }

    #[test]
    fn concurrent_record_and_drain_preserve_all_six_counters() {
        const WORKERS: u64 = 4;
        const RECORDS: u64 = 10_000;
        let registry = Registry::new();
        let metric = registry.register("concurrent", MetricType::Redis);
        let mut sum = MetricSnapshot::default();
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
            let mut collect = || {
                let sample = drain(metric.item);
                if !sample.is_empty() {
                    sum.merge(sample);
                }
            };
            while workers.iter().any(|worker| !worker.is_finished()) {
                collect();
            }
            for worker in workers {
                worker.join().unwrap();
            }
            collect();
        });
        let total = WORKERS * RECORDS;
        assert_eq!(sum.total, total);
        assert_eq!(sum.success, total / 2);
        assert_eq!(sum.failure, total / 2);
        assert_eq!(sum.slow, total / 2);
        assert_eq!(sum.elapsed_us, total / 2 * 255_000);
        assert_eq!(sum.slow_elapsed_us, total / 2 * 250_000);
        assert_eq!(sum.failure_elapsed_us, total / 2 * 250_000);
        assert_eq!(metric.snapshot(), MetricSnapshot::default());
    }

    #[test]
    fn duration_only_windows_are_logged_instead_of_discarded() {
        for kind in [MetricType::Redis, MetricType::RpcServiceWhole] {
            let registry = Registry::new();
            let metric = registry.register("partial", kind);
            // Model a sample boundary that falls between duration and count updates.
            let slot = metric.item.as_ref();
            slot.elapsed_us.store(12_346, Ordering::Relaxed);
            slot.slow_elapsed_us.store(2_000, Ordering::Relaxed);
            slot.failure_elapsed_us.store(3_000, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "metrics-partial-{}-{:?}.log",
                std::process::id(),
                kind,
            ));
            let _ = std::fs::remove_file(&path);
            registry
                .write_profile_log(
                    &path,
                    chrono::Utc::now().fixed_offset(),
                    &mut Vec::new(),
                    Duration::from_secs(10),
                )
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            std::fs::remove_file(path).unwrap();
            let rows: Vec<serde_json::Value> = content
                .lines()
                .map(|line| serde_json::from_str(&line[20..]).unwrap())
                .collect();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0]["name"], "partial");
            assert_eq!(rows[0]["total_time"], "12.35");
            assert_eq!(rows[0]["slow_time"], "2.00");
            assert_eq!(rows[0]["error_time"], "3.00");
            for field in ["avg_time", "slow_avg_time", "error_avg_time", "qps"] {
                assert_eq!(rows[0][field], "0.00");
            }
            assert_eq!(metric.snapshot(), MetricSnapshot::default());
        }
    }
}
