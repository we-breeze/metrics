use std::sync::Arc;

/// Counter policy applied by [`crate::Metric::record`].
///
/// This describes only threshold and bucket behavior. Protocol-specific
/// output names remain in [`ProfilePlan`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MetricPolicy {
    /// Standard resource buckets with a 50 ms slow threshold.
    Resource,
    /// Standard resource buckets with a 200 ms slow threshold.
    Service,
    /// Access-statistic buckets with a 200 ms slow threshold.
    Access,
}

/// JSON shape used for one profile output plan.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProfileFormat {
    /// ProfileUtil resource fields with numeric counters.
    Resource,
    /// RPC access-statistic fields with string counters.
    AccessStatistic,
}

/// One `type`/`name` view emitted from a metric snapshot.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProfileOutput {
    pub(crate) metric_type: Arc<str>,
    pub(crate) name: Arc<str>,
}

impl ProfileOutput {
    fn new(metric_type: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            metric_type: metric_type.into(),
            name: name.into(),
        }
    }
}

/// Immutable profile rendering plan for one counter slot.
///
/// Direct outputs all receive the same drained snapshot. An aggregate output
/// contributes that snapshot to a process-wide sum keyed by its `type` and
/// `name`; the sum is emitted after every direct metric has been drained.
#[derive(Clone, Debug)]
pub struct ProfilePlan {
    pub(crate) identity: Arc<str>,
    pub(crate) policy: MetricPolicy,
    pub(crate) format: ProfileFormat,
    pub(crate) outputs: Arc<[ProfileOutput]>,
    pub(crate) aggregate: Option<ProfileOutput>,
}

impl ProfilePlan {
    /// Starts a plan with its canonical output.
    #[must_use]
    pub fn new(
        identity: impl Into<Arc<str>>,
        policy: MetricPolicy,
        format: ProfileFormat,
        metric_type: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            identity: identity.into(),
            policy,
            format,
            outputs: Arc::from([ProfileOutput::new(metric_type, name)]),
            aggregate: None,
        }
    }

    /// Adds another direct profile view of the same snapshot.
    #[must_use]
    pub fn alias(mut self, metric_type: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        let mut outputs = self.outputs.to_vec();
        outputs.push(ProfileOutput::new(metric_type, name));
        self.outputs = outputs.into();
        self
    }

    /// Adds one flush-time aggregate target for this snapshot.
    #[must_use]
    pub fn aggregate(
        mut self,
        metric_type: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        self.aggregate = Some(ProfileOutput::new(metric_type, name));
        self
    }
}

#[derive(Clone)]
pub(crate) enum ProfileMeta {
    Single {
        name: Arc<str>,
        kind: crate::metric::MetricType,
    },
    Fanout(Arc<ProfilePlan>),
}
