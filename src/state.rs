use ahash::RandomState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type FastHashMap<K, V> = HashMap<K, V, RandomState>;
type StateFields = FastHashMap<Arc<str>, Arc<str>>;
type StateMap = FastHashMap<StateIdentity, StateFields>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum StateType {
    Rpc,
}

impl StateType {
    pub(crate) const fn profile_type(self) -> &'static str {
        match self {
            Self::Rpc => "MOTAN_CLUSTER_STAT",
        }
    }
}

#[derive(Debug)]
pub(crate) struct StateSnapshot {
    pub(crate) state_type: StateType,
    pub(crate) name: Arc<str>,
    pub(crate) fields: Vec<(Arc<str>, Arc<str>)>,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct StateIdentity {
    state_type: StateType,
    name: Arc<str>,
}

#[derive(Default)]
pub(crate) struct StateRegistry {
    states: Mutex<StateMap>,
}

impl StateRegistry {
    pub(crate) fn record(&self, state_type: StateType, name: &str, key: &str, value: &str) {
        let mut states = self.states.lock().expect("metrics state mutex poisoned");
        let fields = states
            .entry(StateIdentity {
                state_type,
                name: Arc::from(name),
            })
            .or_default();
        if fields
            .get(key)
            .is_some_and(|current| current.as_ref() == value)
        {
            return;
        }
        fields.insert(Arc::from(key), Arc::from(value));
    }

    pub(crate) fn snapshot(&self) -> Vec<StateSnapshot> {
        let states = self.states.lock().expect("metrics state mutex poisoned");
        let mut snapshots = states
            .iter()
            .map(|(identity, fields)| StateSnapshot {
                state_type: identity.state_type,
                name: Arc::clone(&identity.name),
                fields: fields
                    .iter()
                    .map(|(key, value)| (Arc::clone(key), Arc::clone(value)))
                    .collect(),
            })
            .collect::<Vec<_>>();
        drop(states);

        snapshots.sort_unstable_by(|left, right| {
            (left.state_type.profile_type(), left.name.as_ref())
                .cmp(&(right.state_type.profile_type(), right.name.as_ref()))
        });
        for snapshot in &mut snapshots {
            snapshot
                .fields
                .sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        }
        snapshots
    }
}
