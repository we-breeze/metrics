//! Compact single-value storage for counters and gauges.

use crate::MetricType;
use crate::registry::{CHUNK_SIZE, MetaChunk, ShardIndex};
use ahash::RandomState;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SHARDS: usize = 64;
type Names = HashMap<Arc<str>, ScalarPtr, RandomState>;

#[derive(Default)]
struct ScalarSlot(AtomicU64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ScalarPtr(NonNull<ScalarSlot>);

// Slots belong to append-only boxed chunks and contain only an atomic value.
unsafe impl Send for ScalarPtr {}
unsafe impl Sync for ScalarPtr {}

impl ScalarPtr {
    #[inline]
    fn atomic(&self) -> &AtomicU64 {
        // Production handles refer to the process-global registry. Internal test
        // registries retain their storage until all reads/updates have finished.
        unsafe { &self.0.as_ref().0 }
    }

    #[inline]
    pub(crate) fn read(self) -> u64 {
        self.atomic().load(Ordering::Relaxed)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ScalarKind {
    Counter,
    Gauge,
}

#[derive(Clone)]
pub(crate) struct ScalarMeta {
    pub(crate) name: Arc<str>,
    pub(crate) kind: MetricType,
    pub(crate) mode: ScalarKind,
    pub(crate) item: ScalarPtr,
}

struct State {
    by_type: HashMap<(MetricType, ScalarKind), Names, RandomState>,
    // Handles retain raw slot pointers: chunks must not move when this Vec grows.
    #[allow(clippy::vec_box)]
    slots: Vec<Box<[ScalarSlot; CHUNK_SIZE]>>,
    chunks: Vec<Arc<MetaChunk<ScalarMeta>>>,
    len: usize,
}

struct Shard {
    state: Mutex<State>,
    index: ArcSwap<ShardIndex<ScalarMeta>>,
}

impl Shard {
    fn new() -> Self {
        Self {
            state: Mutex::new(State {
                by_type: HashMap::default(),
                slots: Vec::new(),
                chunks: Vec::new(),
                len: 0,
            }),
            index: ArcSwap::from_pointee(ShardIndex::empty()),
        }
    }

    fn register(&self, kind: MetricType, mode: ScalarKind, name: &str) -> ScalarPtr {
        let mut state = self.state.lock().expect("scalar shard mutex poisoned");
        if let Some(item) = state
            .by_type
            .get(&(kind, mode))
            .and_then(|names| names.get(name))
        {
            return *item;
        }
        let chunk = state.len / CHUNK_SIZE;
        let offset = state.len % CHUNK_SIZE;
        let added = state.chunks.len() <= chunk;
        if added {
            state
                .slots
                .push(Box::new(std::array::from_fn(|_| ScalarSlot::default())));
            state.chunks.push(Arc::new(MetaChunk::new()));
        }
        let item = ScalarPtr(NonNull::from(&mut state.slots[chunk][offset]));
        let name: Arc<str> = Arc::from(name);
        state.chunks[chunk].publish(
            offset,
            ScalarMeta {
                name: Arc::clone(&name),
                kind,
                mode,
                item,
            },
        );
        state
            .by_type
            .entry((kind, mode))
            .or_default()
            .insert(name, item);
        state.len += 1;
        if added {
            self.index.store(Arc::new(ShardIndex {
                chunks: Arc::from(state.chunks.clone().into_boxed_slice()),
            }));
        }
        item
    }
}

pub(crate) struct ScalarRegistry {
    shards: [Shard; SHARDS],
    hasher: RandomState,
}

impl ScalarRegistry {
    pub(crate) fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Shard::new()),
            hasher: RandomState::new(),
        }
    }

    pub(crate) fn register(&self, kind: MetricType, mode: ScalarKind, name: &str) -> ScalarPtr {
        kind.validate();
        let shard = self.hasher.hash_one((kind, mode, name)) as usize & (SHARDS - 1);
        self.shards[shard].register(kind, mode, name)
    }

    pub(crate) fn for_each(&self, mut f: impl FnMut(&ScalarMeta)) {
        for shard in &self.shards {
            let index = shard.index.load();
            for chunk in index.chunks.iter() {
                let len = chunk.published_len();
                for offset in 0..len {
                    f(chunk.get(offset));
                }
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let index = shard.index.load();
                index
                    .chunks
                    .iter()
                    .map(|chunk| chunk.published_len())
                    .sum::<usize>()
            })
            .sum()
    }
}

/// A cumulative count. The business kind determines its implicit exported family.
#[derive(Clone, Copy, Debug)]
pub struct CounterMetric {
    item: ScalarPtr,
}

impl CounterMetric {
    pub fn new(kind: MetricType, name: &str) -> Self {
        Self {
            item: crate::registry()
                .scalars
                .register(kind, ScalarKind::Counter, name),
        }
    }
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }
    #[inline]
    pub fn add(&self, value: u64) {
        self.item.atomic().fetch_add(value, Ordering::Relaxed);
    }
    #[inline]
    pub fn get(&self) -> u64 {
        self.item.read()
    }
}

/// A current signed value. Updates are atomic and do not reset at scrape time.
#[derive(Clone, Copy, Debug)]
pub struct GaugeMetric {
    item: ScalarPtr,
}

impl GaugeMetric {
    pub fn new(kind: MetricType, name: &str) -> Self {
        Self {
            item: crate::registry()
                .scalars
                .register(kind, ScalarKind::Gauge, name),
        }
    }
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }
    #[inline]
    pub fn dec(&self) {
        self.add(-1);
    }
    #[inline]
    pub fn add(&self, value: i64) {
        self.item
            .atomic()
            .fetch_add(value as u64, Ordering::Relaxed);
    }
    #[inline]
    pub fn set(&self, value: i64) {
        self.item.atomic().store(value as u64, Ordering::Relaxed);
    }
    #[inline]
    pub fn get(&self) -> i64 {
        self.item.read() as i64
    }
}

/// Current connections plus cumulative successful establishments for one resource.
#[derive(Clone, Copy, Debug)]
pub struct ConnMetric {
    current: GaugeMetric,
    total: CounterMetric,
}

impl ConnMetric {
    pub fn new(kind: MetricType, name: &str) -> Self {
        assert!(
            kind.is_conn(),
            "ConnMetric requires a resource's Conn category"
        );
        Self {
            current: GaugeMetric::new(kind, name),
            total: CounterMetric::new(kind, name),
        }
    }
    #[inline]
    pub fn connected(&self) {
        self.current.inc();
        self.total.inc();
    }
    #[inline]
    pub fn disconnected(&self) {
        self.current.dec();
    }
    #[inline]
    pub fn current(&self) -> i64 {
        self.current.get()
    }
    #[inline]
    pub fn total(&self) -> u64 {
        self.total.get()
    }
    #[inline]
    pub fn connection(&self) -> ConnectionGuard {
        self.connected();
        ConnectionGuard { metric: *self }
    }
}

/// Owns one successful connection; drop decrements the gauge exactly once.
#[derive(Debug)]
pub struct ConnectionGuard {
    metric: ConnMetric,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metric.disconnected();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceMetric;

    #[test]
    fn single_values_and_handles_stay_compact() {
        assert_eq!(std::mem::size_of::<ScalarSlot>(), 8);
        assert_eq!(std::mem::size_of::<CounterMetric>(), 8);
        assert_eq!(std::mem::size_of::<GaugeMetric>(), 8);
        assert_eq!(std::mem::size_of::<ConnMetric>(), 16);
    }

    #[test]
    fn connections_share_resource_identity_and_guards_balance_on_drop() {
        let kind = MetricType::Redis(ResourceMetric::Conn);
        let conn = ConnMetric::new(kind, "scalar-connection-test");
        let other = ConnMetric::new(kind, "scalar-connection-test");
        let first = conn.connection();
        let second = other.connection();
        assert_eq!(conn.current(), 2);
        assert_eq!(conn.total(), 2);
        drop(first);
        assert_eq!(other.current(), 1);
        assert_eq!(other.total(), 2);
        drop(second);
        assert_eq!(conn.current(), 0);
        let reconnect = conn.connection();
        assert_eq!(conn.current(), 1);
        assert_eq!(conn.total(), 3);
        drop(reconnect);
        assert_eq!(conn.current(), 0);
    }

    #[test]
    fn concurrent_scalar_updates_do_not_lose_values() {
        let counter = CounterMetric::new(MetricType::Log, "scalar-concurrency-test");
        let gauge = GaugeMetric::new(
            MetricType::Service(ResourceMetric::Req),
            "scalar-concurrency-test",
        );
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(move || {
                    for _ in 0..10_000 {
                        counter.inc();
                        gauge.inc();
                        gauge.dec();
                    }
                });
            }
        });
        assert_eq!(counter.get(), 80_000);
        assert_eq!(gauge.get(), 0);
        gauge.set(-7);
        gauge.add(3);
        assert_eq!(gauge.get(), -4);
    }
}
