//! Immutable resource labels registered before entering the request path.
use std::sync::Arc;

/// Logical operation, independent of which connection serves it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MetricAccess {
    Read,
    Write,
    Other,
}
impl MetricAccess {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Other => "other",
        }
    }
}
/// Purpose of one physical connection. Shared connections are counted once.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectionRole {
    Read,
    Write,
    Shared,
}
impl ConnectionRole {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Shared => "shared",
        }
    }
}
/// Business resource identity; cloning only increments immutable string references.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceLabels {
    pub(crate) name: Arc<str>,
    pub(crate) endpoint: Arc<str>,
    pub(crate) access: Option<MetricAccess>,
    pub(crate) role: Option<ConnectionRole>,
}
impl ResourceLabels {
    pub fn new(name: impl Into<Arc<str>>, endpoint: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into(),
            access: None,
            role: None,
        }
    }
    #[must_use]
    pub fn with_access(mut self, access: MetricAccess) -> Self {
        self.access = Some(access);
        self
    }
    #[must_use]
    pub fn with_role(mut self, role: ConnectionRole) -> Self {
        self.role = Some(role);
        self
    }
}
