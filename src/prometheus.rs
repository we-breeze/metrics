//! Streaming Prometheus text exposition. This module never drains counters.

use crate::metric::{Counter, ItemPtr, MetricType};
use crate::plan::{ProfileMeta, ProfileOutput};
use crate::registry::Registry;
use crate::scalar::{ScalarKind, ScalarPtr};
use crate::state::StateSnapshot;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::Arc;

/// Content-Type for an HTTP response containing this reader's output.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

const FAMILIES: [(&str, Counter, bool); 10] = [
    ("brz_req_total", Counter::Total, false),
    ("brz_err_total", Counter::Failure, false),
    ("brz_slow_total", Counter::Slow, false),
    ("brz_req_duration_secs_total", Counter::Elapsed, true),
    ("brz_err_duration_secs_total", Counter::FailureElapsed, true),
    ("brz_slow_duration_secs_total", Counter::SlowElapsed, true),
    ("brz_conn_total", Counter::Total, false),
    ("brz_count_total", Counter::Total, false),
    ("brz_conn", Counter::Total, false),
    ("brz_value", Counter::Total, false),
];

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum SourceKind {
    Full,
    ApiSummary,
    Api(u16),
    Timeout,
    ConnCounter,
    Count,
    ConnGauge,
    Gauge,
}

impl SourceKind {
    fn supports(self, family: usize) -> bool {
        match self {
            Self::Full => family < 6,
            Self::ApiSummary => (1..6).contains(&family),
            Self::Api(_) | Self::Timeout => family == 0,
            Self::ConnCounter => family == 6,
            Self::Count => family == 7,
            Self::ConnGauge => family == 8,
            Self::Gauge => family == 9,
        }
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum SourceItem {
    Timing(ItemPtr),
    Counter(ScalarPtr),
    Gauge(ScalarPtr),
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct Source {
    output: ProfileOutput,
    kind: SourceKind,
    item: SourceItem,
}

/// A one-pass reader over cumulative counters in Prometheus text format.
///
/// Creating a reader captures registered output metadata and the current RPC state.
/// Counters are loaded as each metric family is read; a scrape is eventually consistent,
/// not an atomic snapshot. New registrations are left for the next reader.
///
/// Text is encoded one sample at a time. Metadata uses memory proportional to the
/// number of exported views, sharing their strings with the registry. Duplicate views
/// of one slot are emitted once; distinct slots with the same `type`/`name` are summed.
/// Neither reading nor dropping a reader modifies any counter.
///
/// With the `tokio` feature this same type also implements `tokio::io::AsyncRead`.
pub struct PrometheusReader {
    // Keep slots alive even when an internal reader is built from a non-global registry.
    _registry: Arc<Registry>,
    sources: Vec<Source>,
    states: Vec<StateSnapshot>,
    family: usize,
    source: usize,
    header: bool,
    state: usize,
    state_field: usize,
    buffer: Vec<u8>,
    offset: usize,
}

impl PrometheusReader {
    /// Creates a reader for the process-wide registry without resetting any counters.
    #[must_use]
    pub fn new() -> Self {
        Self::from_registry(Arc::clone(crate::registry()))
    }

    fn from_registry(registry: Arc<Registry>) -> Self {
        let mut sources = Vec::new();
        let mut types = HashMap::<&'static str, Arc<str>>::new();
        registry.for_each_meta(|meta| {
            let mut add = |output: ProfileOutput, kind: SourceKind| {
                sources.push(Source {
                    output,
                    kind,
                    item: SourceItem::Timing(meta.item),
                });
            };
            match &meta.profile {
                ProfileMeta::Single { name, kind } => {
                    let (metric_type, source_kind) = match kind {
                        MetricType::Api(code) => ("API", SourceKind::Api(*code)),
                        MetricType::ApiTimeout => ("API", SourceKind::Timeout),
                        MetricType::ApiAll => ("API", SourceKind::ApiSummary),
                        _ => (kind.output().0, SourceKind::Full),
                    };
                    add(
                        ProfileOutput {
                            metric_type: Arc::clone(
                                types
                                    .entry(metric_type)
                                    .or_insert_with(|| Arc::from(metric_type)),
                            ),
                            name: Arc::clone(name),
                            resource: None,
                        },
                        source_kind,
                    );
                }
                ProfileMeta::Resource { labels, kind } => {
                    add(
                        ProfileOutput {
                            metric_type: Arc::from(match kind {
                                MetricType::Api(_) | MetricType::ApiTimeout => "API",
                                _ => kind.output().0,
                            }),
                            name: Arc::clone(&labels.name),
                            resource: Some(labels.clone()),
                        },
                        match kind {
                            MetricType::ApiAll => SourceKind::ApiSummary,
                            MetricType::Api(code) => SourceKind::Api(*code),
                            MetricType::ApiTimeout => SourceKind::Timeout,
                            _ => SourceKind::Full,
                        },
                    );
                }
                ProfileMeta::Fanout(plan) => {
                    for output in plan.outputs.iter() {
                        add(
                            output.clone(),
                            if &*output.metric_type == "API" {
                                SourceKind::ApiSummary
                            } else {
                                SourceKind::Full
                            },
                        );
                    }
                    if let Some(output) = &plan.aggregate {
                        add(
                            output.clone(),
                            if &*output.metric_type == "API" {
                                SourceKind::ApiSummary
                            } else {
                                SourceKind::Full
                            },
                        );
                    }
                }
            }
        });
        registry.scalars.for_each(|meta| {
            let source_kind = match (meta.mode, meta.kind) {
                (ScalarKind::Counter, MetricType::Api(code)) => SourceKind::Api(code),
                (ScalarKind::Counter, MetricType::ApiTimeout) => SourceKind::Timeout,
                (ScalarKind::Counter, kind) if kind.is_conn() => SourceKind::ConnCounter,
                (ScalarKind::Counter, _) => SourceKind::Count,
                (ScalarKind::Gauge, kind) if kind.is_conn() => SourceKind::ConnGauge,
                (ScalarKind::Gauge, _) => SourceKind::Gauge,
            };
            let metric_type = match meta.kind {
                MetricType::Api(_) | MetricType::ApiTimeout => "API",
                _ => meta.kind.output().0,
            };
            sources.push(Source {
                output: ProfileOutput {
                    metric_type: Arc::clone(
                        types
                            .entry(metric_type)
                            .or_insert_with(|| Arc::from(metric_type)),
                    ),
                    name: Arc::clone(&meta.name),
                    resource: meta.resource.clone(),
                },
                kind: source_kind,
                item: match meta.mode {
                    ScalarKind::Counter => SourceItem::Counter(meta.item),
                    ScalarKind::Gauge => SourceItem::Gauge(meta.item),
                },
            });
        });
        sources.sort_unstable();
        sources.dedup();
        let states = registry.state_snapshot();
        Self {
            _registry: registry,
            sources,
            states,
            family: 0,
            source: 0,
            header: false,
            state: 0,
            state_field: 0,
            buffer: Vec::with_capacity(256),
            offset: 0,
        }
    }

    // Encode at most one sample (or one TYPE line) so tiny reads never require
    // building the entire response body. Samples of each family remain grouped.
    fn next_sample(&mut self) -> bool {
        self.buffer.clear();
        self.offset = 0;
        while self.family < FAMILIES.len() {
            let (name, counter, seconds) = FAMILIES[self.family];
            if !self.header {
                self.header = true;
                let mode = if self.family >= 8 { "gauge" } else { "counter" };
                writeln!(self.buffer, "# TYPE {name} {mode}").unwrap();
                return true;
            }
            while self.source < self.sources.len()
                && !self.sources[self.source].kind.supports(self.family)
            {
                self.source += 1;
            }
            if self.source < self.sources.len() {
                let first = self.source;
                let output = &self.sources[first].output;
                let kind = self.sources[first].kind;
                let mut value = 0u64;
                let mut gauge = 0i64;
                while self.source < self.sources.len()
                    && self.sources[self.source].output == *output
                    && self.sources[self.source].kind == kind
                {
                    match self.sources[self.source].item {
                        SourceItem::Timing(item) => {
                            value = value.saturating_add(counter.read(item))
                        }
                        SourceItem::Counter(item) => value = value.saturating_add(item.read()),
                        SourceItem::Gauge(item) => gauge = gauge.saturating_add(item.read() as i64),
                    }
                    self.source += 1;
                }
                write!(self.buffer, "{name}{{type=\"").unwrap();
                label(&mut self.buffer, &output.metric_type);
                self.buffer.extend_from_slice(b"\",name=\"");
                label(&mut self.buffer, &output.name);
                if let Some(resource) = &output.resource {
                    self.buffer.extend_from_slice(b"\",endpoint=\"");
                    label(&mut self.buffer, &resource.endpoint);
                    if let Some(access) = resource.access {
                        self.buffer.extend_from_slice(b"\",access=\"");
                        label(&mut self.buffer, access.label());
                    }
                    if let Some(role) = resource.role {
                        self.buffer.extend_from_slice(b"\",role=\"");
                        label(&mut self.buffer, role.label());
                    }
                }
                match kind {
                    SourceKind::Api(code) => {
                        write!(self.buffer, "\",status_code=\"{code}").unwrap();
                    }
                    SourceKind::Timeout => {
                        self.buffer.extend_from_slice(b"\",status_code=\"timeout")
                    }
                    _ => {}
                }
                self.buffer.extend_from_slice(b"\"} ");
                if self.family >= 8 {
                    writeln!(self.buffer, "{gauge}").unwrap();
                } else if seconds {
                    // Round the cumulative microseconds to hundredths of a second.
                    // Divide before rounding so even u64::MAX cannot overflow.
                    let hundredths = value / 10_000 + u64::from(value % 10_000 >= 5_000);
                    writeln!(self.buffer, "{}.{:02}", hundredths / 100, hundredths % 100).unwrap();
                } else {
                    writeln!(self.buffer, "{value}").unwrap();
                }
                return true;
            }
            self.family += 1;
            self.source = 0;
            self.header = false;
        }

        if !self.header {
            self.header = true;
            self.buffer
                .extend_from_slice(b"# TYPE brz_rpc_state gauge\n");
            return true;
        }
        while self.state < self.states.len() {
            let state = &self.states[self.state];
            if self.state_field == state.fields.len() {
                self.state += 1;
                self.state_field = 0;
                continue;
            }
            let (field, value) = &state.fields[self.state_field];
            self.state_field += 1;
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            if !value.is_finite() {
                continue;
            }
            self.buffer.extend_from_slice(b"brz_rpc_state{type=\"");
            label(&mut self.buffer, state.state_type.metric_type());
            self.buffer.extend_from_slice(b"\",name=\"");
            label(&mut self.buffer, &state.name);
            self.buffer.extend_from_slice(b"\",field=\"");
            label(&mut self.buffer, field);
            writeln!(self.buffer, "\"}} {value}").unwrap();
            return true;
        }
        false
    }
}

impl Default for PrometheusReader {
    fn default() -> Self {
        Self::new()
    }
}

impl Read for PrometheusReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            if self.offset == self.buffer.len() && !self.next_sample() {
                break;
            }
            let n = (buf.len() - filled).min(self.buffer.len() - self.offset);
            buf[filled..filled + n].copy_from_slice(&self.buffer[self.offset..self.offset + n]);
            self.offset += n;
            filled += n;
        }
        Ok(filled)
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for PrometheusReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let reader = self.get_mut();
        while buf.remaining() > 0 {
            if reader.offset == reader.buffer.len() && !reader.next_sample() {
                break;
            }
            let n = buf.remaining().min(reader.buffer.len() - reader.offset);
            buf.put_slice(&reader.buffer[reader.offset..reader.offset + n]);
            reader.offset += n;
        }
        std::task::Poll::Ready(Ok(()))
    }
}

fn label(buffer: &mut Vec<u8>, value: &str) {
    for byte in value.bytes() {
        match byte {
            b'\\' => buffer.extend_from_slice(b"\\\\"),
            b'"' => buffer.extend_from_slice(b"\\\""),
            b'\n' => buffer.extend_from_slice(b"\\n"),
            byte => buffer.push(byte),
        }
    }
}

#[cfg(test)]
mod tests;
