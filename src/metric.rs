use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::plan::{MetricPolicy, ProfileFormat};

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
pub(crate) enum MetricType {
    /// Process logging counters such as bounded-queue drops.
    Log,
    Redis,
    Mc,
    McDetail,
    /// One outbound HTTP request, keyed by its stable endpoint URL.
    Http,
    /// Whole-request HTTP timing. The HTTP SDK registers this with an
    /// `all_`-prefixed endpoint name to match the legacy profile contract.
    HttpAll,
    /// ProfileUtil service metric. This uses the resource-shaped JSON fields
    /// but has the service slow threshold expected by legacy dashboards.
    Service,
    RpcServiceWhole,
}

#[derive(Clone, Copy)]
pub(crate) struct MetricSpec {
    pub(crate) slow_threshold_ms: u64,
    pub(crate) intervals_ms: [u64; 4],
}

impl MetricType {
    #[inline]
    pub(crate) fn policy(self) -> MetricPolicy {
        match self {
            Self::Log | Self::Redis | Self::Mc | Self::McDetail | Self::Http => {
                MetricPolicy::Resource
            }
            Self::HttpAll | Self::Service => MetricPolicy::Service,
            Self::RpcServiceWhole => MetricPolicy::Access,
        }
    }

    pub(crate) fn output(self) -> (&'static str, ProfileFormat) {
        match self {
            Self::Log => ("LOG", ProfileFormat::Resource),
            Self::Redis => ("REDIS", ProfileFormat::Resource),
            Self::Mc => ("MC", ProfileFormat::Resource),
            Self::McDetail => ("MCDETAIL", ProfileFormat::Resource),
            Self::Http | Self::HttpAll => ("HTTP", ProfileFormat::Resource),
            Self::Service => ("SERVICE", ProfileFormat::Resource),
            Self::RpcServiceWhole => ("RPC_SERVICE_WHOLE", ProfileFormat::AccessStatistic),
        }
    }
}

impl MetricPolicy {
    #[inline]
    pub(crate) fn spec(self) -> MetricSpec {
        match self {
            // ProfileUtil resource buckets: <10, <50, <100, <200, >=200 ms.
            Self::Resource => MetricSpec {
                slow_threshold_ms: 50,
                intervals_ms: [10, 50, 100, 200],
            },
            // Whole-request and SERVICE metrics retain resource buckets but use 200 ms slow.
            Self::Service => MetricSpec {
                slow_threshold_ms: 200,
                intervals_ms: [10, 50, 100, 200],
            },
            // Access-statistic access buckets: <10, <50, <100, <500, >=500 ms.
            Self::Access => MetricSpec {
                slow_threshold_ms: 200,
                intervals_ms: [10, 50, 100, 500],
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
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
    policy: MetricPolicy,
}

impl Metric {
    pub(crate) fn new(item: ItemPtr, policy: MetricPolicy) -> Self {
        Self { item, policy }
    }

    /// Records a completed operation with nanosecond-precision input.
    ///
    /// Profile output retains the established millisecond fields and two-decimal average format;
    /// only that output conversion loses precision. `success == false` increments the
    /// corresponding failure field in the next log interval.
    #[inline]
    pub fn record(&self, elapsed: Duration, success: bool) {
        let slot = self.item.as_ref();
        let profile = self.policy.spec();
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

    /// Records one count-only occurrence without timing or failure data.
    ///
    /// The occurrence is represented in `total_count` and `interval1`; elapsed,
    /// failure, and slow counters remain unchanged.
    #[inline]
    pub fn increment(&self) {
        self.item.as_ref().interval1.fetch_add(1, Ordering::Relaxed);
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

    pub(crate) fn merge(&mut self, other: Self) {
        self.total = self.total.saturating_add(other.total);
        self.success = self.success.saturating_add(other.success);
        self.failure = self.failure.saturating_add(other.failure);
        self.elapsed_ns = self.elapsed_ns.saturating_add(other.elapsed_ns);
        self.slow = self.slow.saturating_add(other.slow);
        for (target, value) in self.intervals.iter_mut().zip(other.intervals) {
            *target = target.saturating_add(value);
        }
    }
}
