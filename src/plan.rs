use std::sync::Arc;

/// Counter policy applied by [`crate::Metric::record`].
///
/// This describes only slow-threshold behavior. Protocol-specific
/// output names remain in [`ProfilePlan`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MetricPolicy {
    /// Resource metrics with a 50 ms slow threshold.
    Resource,
    /// Service metrics with a 200 ms slow threshold.
    Service,
    /// Access-statistic metrics with a 200 ms slow threshold.
    Access,
}

/// Legacy profile format marker retained for producer source compatibility.
///
/// Prometheus output uses numeric counters for either variant.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProfileFormat {
    /// ProfileUtil resource fields with numeric counters.
    Resource,
    /// RPC access-statistic fields with string counters.
    AccessStatistic,
}

/// One `type`/`name` view emitted from a metric snapshot.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProfileOutput {
    pub(crate) metric_type: Arc<str>,
    pub(crate) name: Arc<str>,
    pub(crate) resource: Option<crate::ResourceLabels>,
}

impl ProfileOutput {
    fn new(metric_type: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            metric_type: metric_type.into(),
            name: name.into(),
            resource: None,
        }
    }
}

/// Immutable export plan for one counter slot.
///
/// Direct outputs expose the same cumulative counters. An aggregate output
/// contributes those counters to a sum keyed by its `type` and `name`.
#[derive(Clone, Debug)]
pub struct ProfilePlan {
    pub(crate) identity: Arc<str>,
    pub(crate) policy: MetricPolicy,
    pub(crate) outputs: Arc<[ProfileOutput]>,
    pub(crate) aggregate: Option<ProfileOutput>,
}

impl ProfilePlan {
    /// Starts a plan with its canonical output.
    #[must_use]
    pub fn new(
        identity: impl Into<Arc<str>>,
        policy: MetricPolicy,
        _format: ProfileFormat,
        metric_type: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            identity: identity.into(),
            policy,
            outputs: Arc::from([ProfileOutput::new(metric_type, name)]),
            aggregate: None,
        }
    }

    /// Adds another direct view of the same cumulative counters.
    #[must_use]
    pub fn alias(mut self, metric_type: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        let mut outputs = self.outputs.to_vec();
        outputs.push(ProfileOutput::new(metric_type, name));
        self.outputs = outputs.into();
        self
    }

    /// Adds one scrape-time aggregate target for these counters.
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
    Resource {
        labels: crate::ResourceLabels,
        kind: crate::MetricType,
    },
    Fanout(Arc<ProfilePlan>),
}
