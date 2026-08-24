use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// ProfileUtil counters for one metric.
///
/// Profile output derives total from its interval buckets. Slow counts need a separate counter:
/// an operation can cross the slow threshold without crossing an interval boundary (for example,
/// RPC's 200 ms threshold inside its 100..500 ms bucket).
#[repr(C, align(64))]
#[derive(Debug, Default)]
pub(crate) struct Slot {
    failure: AtomicU64,
    elapsed_ns: AtomicU64,
    slow: AtomicU64,
    interval1: AtomicU64,
    interval2: AtomicU64,
    interval3: AtomicU64,
    interval4: AtomicU64,
    interval5: AtomicU64,
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
pub enum MetricType {
    Redis,
    /// ProfileUtil service metric. This uses the resource-shaped JSON fields
    /// but has the service slow threshold expected by legacy dashboards.
    Service,
    RpcServiceWhole,
}

#[derive(Clone, Copy)]
pub(crate) struct ProfileSpec {
    pub(crate) label: &'static str,
    pub(crate) slow_threshold_ms: u64,
    pub(crate) intervals_ms: [u64; 4],
    pub(crate) log_format: ProfileLogFormat,
}

impl MetricType {
    #[inline]
    pub(crate) fn profile(self) -> ProfileSpec {
        match self {
            // ProfileUtil's REDIS resource buckets: <10, <50, <100, <200, >=200 ms.
            Self::Redis => ProfileSpec {
                label: "REDIS",
                slow_threshold_ms: 50,
                intervals_ms: [10, 50, 100, 200],
                log_format: ProfileLogFormat::Resource,
            },
            // Java UserInfoServiceImpl.localMcHit/localMcSet use SERVICE with
            // the same bucket layout as resource metrics and a 200 ms slow
            // threshold.
            Self::Service => ProfileSpec {
                label: "SERVICE",
                slow_threshold_ms: 200,
                intervals_ms: [10, 50, 100, 200],
                log_format: ProfileLogFormat::Resource,
            },
            // Access-statistic's RPC service whole-time buckets: <10, <50, <100, <500, >=500 ms.
            Self::RpcServiceWhole => ProfileSpec {
                label: "RPC_SERVICE_WHOLE",
                slow_threshold_ms: 200,
                intervals_ms: [10, 50, 100, 500],
                log_format: ProfileLogFormat::RpcServiceWhole,
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ProfileLogFormat {
    Resource,
    RpcServiceWhole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSnapshot {
    pub total: u64,
    pub success: u64,
    pub failure: u64,
    pub elapsed_ns: u64,
    pub slow: u64,
    pub intervals: [u64; 5],
}

/// Copyable handle used by the record hot path.
#[derive(Debug, Clone, Copy)]
pub struct Metric {
    item: ItemPtr,
    metric_type: MetricType,
}

impl Metric {
    pub(crate) fn new(item: ItemPtr, metric_type: MetricType) -> Self {
        Self { item, metric_type }
    }

    /// Records a completed operation with nanosecond-precision input.
    ///
    /// Profile output retains the established millisecond fields and two-decimal average format;
    /// only that output conversion loses precision. `success == false` increments the
    /// corresponding failure field in the next log interval.
    #[inline]
    pub fn record(&self, elapsed: Duration, success: bool) {
        let slot = self.item.as_ref();
        let profile = self.metric_type.profile();
        let elapsed_ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;

        if !success {
            slot.failure.fetch_add(1, Ordering::Relaxed);
        }
        slot.elapsed_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        if elapsed_ns >= profile.slow_threshold_ms.saturating_mul(1_000_000) {
            slot.slow.fetch_add(1, Ordering::Relaxed);
        }

        let interval = match elapsed_ns {
            elapsed if elapsed < profile.intervals_ms[0].saturating_mul(1_000_000) => {
                &slot.interval1
            }
            elapsed if elapsed < profile.intervals_ms[1].saturating_mul(1_000_000) => {
                &slot.interval2
            }
            elapsed if elapsed < profile.intervals_ms[2].saturating_mul(1_000_000) => {
                &slot.interval3
            }
            elapsed if elapsed < profile.intervals_ms[3].saturating_mul(1_000_000) => {
                &slot.interval4
            }
            _ => &slot.interval5,
        };
        interval.fetch_add(1, Ordering::Relaxed);
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
    let intervals = [
        slot.interval1.load(Ordering::Relaxed),
        slot.interval2.load(Ordering::Relaxed),
        slot.interval3.load(Ordering::Relaxed),
        slot.interval4.load(Ordering::Relaxed),
        slot.interval5.load(Ordering::Relaxed),
    ];
    MetricSnapshot::from_counters(
        slot.failure.load(Ordering::Relaxed),
        slot.elapsed_ns.load(Ordering::Relaxed),
        slot.slow.load(Ordering::Relaxed),
        intervals,
    )
}

#[inline]
pub(crate) fn drain(item: ItemPtr) -> MetricSnapshot {
    let slot = item.as_ref();
    let intervals = [
        slot.interval1.swap(0, Ordering::AcqRel),
        slot.interval2.swap(0, Ordering::AcqRel),
        slot.interval3.swap(0, Ordering::AcqRel),
        slot.interval4.swap(0, Ordering::AcqRel),
        slot.interval5.swap(0, Ordering::AcqRel),
    ];
    MetricSnapshot::from_counters(
        slot.failure.swap(0, Ordering::AcqRel),
        slot.elapsed_ns.swap(0, Ordering::AcqRel),
        slot.slow.swap(0, Ordering::AcqRel),
        intervals,
    )
}

impl MetricSnapshot {
    #[inline]
    fn from_counters(failure: u64, elapsed_ns: u64, slow: u64, intervals: [u64; 5]) -> Self {
        let total = intervals
            .iter()
            .fold(0u64, |count, interval| count.wrapping_add(*interval));
        Self {
            total,
            success: total.saturating_sub(failure),
            failure,
            elapsed_ns,
            slow,
            intervals,
        }
    }
}
