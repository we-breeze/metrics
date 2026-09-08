use crate::metric::MetricType;
use crate::metric::{ItemPtr, Metric, MetricSnapshot, Slot, snapshot};
use crate::plan::{MetricPolicy, ProfileMeta, ProfilePlan};
use crate::state::{StateRegistry, StateSnapshot, StateType};
use ahash::RandomState;
use arc_swap::ArcSwap;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) const CHUNK_SIZE: usize = 256;
const SHARD_COUNT: usize = 64;

type FastHashMap<K, V> = HashMap<K, V, RandomState>;

#[derive(Clone)]
pub(crate) struct MetricMeta {
    pub(crate) item: ItemPtr,
    pub(crate) profile: ProfileMeta,
}

/// Metadata for up to `CHUNK_SIZE` metrics.
///
/// Entries are written once, in increasing order. `published_len` is the release/acquire boundary
/// that prevents visitors from touching a not-yet-initialized entry.
pub(crate) struct MetaChunk {
    entries: [UnsafeCell<MaybeUninit<MetricMeta>>; CHUNK_SIZE],
    published_len: AtomicUsize,
}

// Safety: writers initialize each entry exactly once before publishing its index with a Release
// store. Readers load the prefix length with Acquire and never access an entry beyond that prefix.
// Each entry has its own UnsafeCell, so references to published entries never cover
// a different entry being initialized concurrently. Once initialized, an entry is immutable. The sole writer for a chunk is serialized by its shard
// mutex, so no two writers access the same entry.
unsafe impl Sync for MetaChunk {}

impl MetaChunk {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
            published_len: AtomicUsize::new(0),
        }
    }

    fn publish(&self, offset: usize, meta: MetricMeta) {
        debug_assert!(offset < CHUNK_SIZE);
        debug_assert_eq!(self.published_len.load(Ordering::Relaxed), offset);

        // Safety: see `Sync` above. This slot is currently outside the published prefix and is
        // assigned by only this shard's serialized writer.
        unsafe {
            (*self.entries[offset].get()).as_mut_ptr().write(meta);
        }
        self.published_len.store(offset + 1, Ordering::Release);
    }

    #[inline]
    fn published_len(&self) -> usize {
        self.published_len.load(Ordering::Acquire)
    }

    #[inline]
    fn get(&self, offset: usize) -> &MetricMeta {
        debug_assert!(offset < self.published_len());
        // Safety: `visit` accesses only offsets below an Acquire-loaded published prefix. Such an
        // entry was fully initialized before the corresponding Release store and is immutable.
        unsafe { (&*self.entries[offset].get()).assume_init_ref() }
    }
}

impl Drop for MetaChunk {
    fn drop(&mut self) {
        let published = self.published_len.load(Ordering::Relaxed);
        for offset in 0..published {
            // Safety: every offset below `published_len` was initialized by `publish`.
            unsafe { self.entries[offset].get_mut().assume_init_drop() };
        }
    }
}

/// Immutable index published for one shard. The index owns only chunk pointers, not metadata.
#[derive(Clone)]
pub(crate) struct ShardIndex {
    pub(crate) chunks: Arc<[Arc<MetaChunk>]>,
}

impl ShardIndex {
    fn empty() -> Self {
        Self {
            chunks: Arc::from([]),
        }
    }
}

/// Mutable registration state, accessed exclusively through its owning shard mutex.
struct ShardState {
    /// Splitting by kind permits `get(name)` to borrow `&str`, avoiding a temporary allocation for
    /// repeated registrations.
    by_type: FastHashMap<MetricType, FastHashMap<Arc<str>, ItemPtr>>,
    by_profile: FastHashMap<MetricPolicy, FastHashMap<Arc<str>, ItemPtr>>,
    slots: Vec<Box<[Slot; CHUNK_SIZE]>>,
    meta_chunks: Vec<Arc<MetaChunk>>,
    len: usize,
}

impl ShardState {
    fn new() -> Self {
        Self {
            by_type: FastHashMap::default(),
            by_profile: FastHashMap::default(),
            slots: Vec::new(),
            meta_chunks: Vec::new(),
            len: 0,
        }
    }

    fn existing(&self, name: &str, kind: MetricType) -> Option<ItemPtr> {
        self.by_type.get(&kind)?.get(name).copied()
    }

    fn existing_profile(&self, identity: &str, policy: MetricPolicy) -> Option<ItemPtr> {
        self.by_profile.get(&policy)?.get(identity).copied()
    }

    fn allocate(
        &mut self,
        published_index: &ArcSwap<ShardIndex>,
        profile: ProfileMeta,
        policy: MetricPolicy,
    ) -> (Metric, ItemPtr) {
        let index = self.len;
        let chunk_index = index / CHUNK_SIZE;
        let offset = index % CHUNK_SIZE;
        let is_new_chunk = self.meta_chunks.len() <= chunk_index;

        if is_new_chunk {
            self.slots
                .push(Box::new(std::array::from_fn(|_| Slot::default())));
            self.meta_chunks.push(Arc::new(MetaChunk::new()));
        }

        let slot = {
            let chunk = self.slots[chunk_index].as_mut_ptr();
            // Safety: `offset < CHUNK_SIZE`; this boxed array is never relocated.
            unsafe { chunk.add(offset) }
        };
        let item = ItemPtr::from_raw(slot);
        let meta = MetricMeta { item, profile };

        self.meta_chunks[chunk_index].publish(offset, meta);
        self.len += 1;

        if is_new_chunk {
            // Only the pointer list is copied. Existing chunks and all their metadata are shared
            // with readers of the previous index.
            let index = ShardIndex {
                chunks: Arc::from(self.meta_chunks.clone().into_boxed_slice()),
            };
            published_index.store(Arc::new(index));
        }

        (Metric::new(item, policy), item)
    }

    fn insert(
        &mut self,
        published_index: &ArcSwap<ShardIndex>,
        name: Arc<str>,
        kind: MetricType,
    ) -> Metric {
        let profile = ProfileMeta::Single {
            name: Arc::clone(&name),
            kind,
        };
        let (metric, item) = self.allocate(published_index, profile, kind.policy());
        self.by_type.entry(kind).or_default().insert(name, item);
        metric
    }

    fn insert_profile(
        &mut self,
        published_index: &ArcSwap<ShardIndex>,
        plan: Arc<ProfilePlan>,
    ) -> Metric {
        let identity = Arc::clone(&plan.identity);
        let policy = plan.policy;
        let (metric, item) = self.allocate(published_index, ProfileMeta::Fanout(plan), policy);
        self.by_profile
            .entry(policy)
            .or_default()
            .insert(identity, item);
        metric
    }
}

/// A registration shard and its independently published visitor index.
#[repr(align(64))]
pub(crate) struct Shard {
    state: Mutex<ShardState>,
    pub(crate) published_index: ArcSwap<ShardIndex>,
}

impl Shard {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ShardState::new()),
            published_index: ArcSwap::from_pointee(ShardIndex::empty()),
        }
    }

    pub(crate) fn register(&self, name: &str, kind: MetricType) -> Metric {
        let mut state = self.state.lock().expect("metrics shard mutex poisoned");
        if let Some(item) = state.existing(name, kind) {
            return Metric::new(item, kind.policy());
        }
        state.insert(&self.published_index, Arc::from(name), kind)
    }

    pub(crate) fn register_profile(&self, plan: ProfilePlan) -> Metric {
        let mut state = self.state.lock().expect("metrics shard mutex poisoned");
        if let Some(item) = state.existing_profile(&plan.identity, plan.policy) {
            return Metric::new(item, plan.policy);
        }
        state.insert_profile(&self.published_index, Arc::new(plan))
    }
}

pub(crate) struct Registry {
    shards: [Shard; SHARD_COUNT],
    states: StateRegistry,
    /// A keyed fast hasher avoids repeatedly paying SipHash's long-string cost while preserving
    /// collision-steering resistance for shard selection.
    shard_hasher: RandomState,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Shard::new()),
            states: StateRegistry::default(),
            shard_hasher: RandomState::new(),
        }
    }

    fn shard(&self, name: &str, kind: MetricType) -> usize {
        // `SHARD_COUNT` is a power of two. AHash's mixed output makes selecting the low bits safe.
        (self.shard_hasher.hash_one((name, kind)) as usize) & (SHARD_COUNT - 1)
    }

    pub(crate) fn register(&self, name: &str, kind: MetricType) -> Metric {
        self.shards[self.shard(name, kind)].register(name, kind)
    }

    pub(crate) fn register_profile(&self, plan: ProfilePlan) -> Metric {
        let shard = (self.shard_hasher.hash_one((&plan.identity, plan.policy)) as usize)
            & (SHARD_COUNT - 1);
        self.shards[shard].register_profile(plan)
    }

    pub(crate) fn record_state(&self, state_type: StateType, name: &str, key: &str, value: &str) {
        self.states.record(state_type, name, key, value);
    }

    pub(crate) fn state_snapshot(&self) -> Vec<StateSnapshot> {
        self.states.snapshot()
    }

    pub(crate) fn visit<F>(&self, mut f: F)
    where
        F: FnMut(&MetricMeta, MetricSnapshot),
    {
        self.for_each_meta(|meta| f(meta, snapshot(meta.item)));
    }

    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let index = shard.published_index.load();
                index
                    .chunks
                    .iter()
                    .map(|chunk| chunk.published_len())
                    .sum::<usize>()
            })
            .sum()
    }

    pub(crate) fn for_each_meta(&self, mut f: impl FnMut(&MetricMeta)) {
        for shard in &self.shards {
            // The ArcSwap guard protects the immutable index without allocation or the shard mutex.
            let index = shard.published_index.load();
            for chunk in index.chunks.iter() {
                // A visitor intentionally observes a stable prefix of each chunk. A concurrent
                // registration may become visible in this visit or the next one, never as an
                // uninitialized entry.
                let len = chunk.published_len();
                for offset in 0..len {
                    f(chunk.get(offset));
                }
            }
        }
    }
}
