# brz-metrics

Minimal in-crate metric registry focused on:

- sharded, synchronous registration during service startup
- fixed 64-byte-aligned `Slot` layout with six `AtomicU64` counters
- lock-free record path
- allocation-free periodic visit at every lifecycle stage, using `ArcSwap`
- keyed AHash routing and lookup for long metric names

## API

- Typed constructors such as `Metric::redis(name)`, `Metric::http(name)`, and `Metric::api(name)`
- `metric.record(elapsed: Duration, success)`
- `brz_metrics::visit(|name, kind, snapshot| { ... })`

Each shard appends metadata into 256-item chunks. Adding an item to an existing chunk publishes
only its per-chunk length; adding a new chunk copy-on-writes that shard's chunk-pointer index.
`visit` sees per-shard published prefixes, so a concurrent registration is observed either in the
current traversal or the next one, never as uninitialized metadata.

## Business-facing handles

Metric names are implicit. Producers provide a typed resource/category and a stable
business identity; the reader owns Prometheus names and labels.

```rust
use brz_metrics::{CounterMetric, GaugeMetric, ConnMetric, TimingMetric, MetricType, ResourceMetric::{Req, Conn}};

let count = CounterMetric::new(MetricType::Redis(Conn), "cache.example.com:6379");
count.inc();
let value = GaugeMetric::new(MetricType::Redis(Conn), "cache.example.com:6379");
value.inc();
value.dec();
let requests = TimingMetric::new(MetricType::Redis(Req), "cache");
requests.record(std::time::Duration::from_millis(60), false);
let connections = ConnMetric::new(MetricType::Memcache(Conn), "11211");
let connection = connections.connection(); // increment current and total
drop(connection); // decrement current only
```

`Rpc(Req)` and `Service(Req)` use the 200 ms slow threshold. `Redis(Req)`,
`Mysql(Req)`, `Memcache(Req)` and `Http(Req)` use 50 ms. `TimingMetric` retains
six atomics in a 64-byte aligned slot. Counters and gauges use independent 8-byte
slots, and their handles are copyable. `ConnMetric` combines two handles and
provides an optional RAII guard. Registrations deduplicate by business kind, name,
and storage kind; counter and gauge values never share the same atomic.

Connection current values use `brz_conn` gauges; successful establishments use
`brz_conn_total` counters. Redis uses its configured logical `host:port` as `name`,
matching request metrics; Memcache groups connections by TCP port across hosts.
Protocol-ready connections are
counted; retries that fail to establish/authenticate are not. Node reuse, SDK clones,
and request reuse do not increment total. A reconnect increments it again. MySQL
connection instrumentation is not enabled in this revision.

```promql
sum by (type, name) (brz_conn)
sum by (type, name) (rate(brz_conn_total[5m]))
```

Other `CounterMetric` values use `brz_count_total`, and other gauges use `brz_value`.
`Api(code)` counters use `brz_api_resp_total` with an exact `status_code` label;
`ApiTimeout` counters use `brz_api_timeout_total`. Request timing uses the six
families below. The existing `Metric` name and convenience constructors remain
available, but SDK producers use the typed handle API. `visit` visits timing slots;
`len` counts both timing and scalar slots.

## Cumulative counters

Registration never starts a background thread or writes files. The profile logger,
log rotation, baseline sentinel, and `BREEZE_PROFILE_LOG_PATH` /
`BREEZE_PROFILE_LOG_ROTATION` configuration have been removed. Counters accumulate
for the process lifetime; `visit`, `snapshot`, and Prometheus reads never reset them.

Each 64-byte `Slot` holds six `AtomicU64` counters: count and microsecond duration
sum for all, slow, and failed operations. Recording truncates durations to whole
microseconds. Slow failed requests contribute to all three groups. Resource metrics
use a 50 ms slow threshold; service/API/access metrics use 200 ms. `increment()`
changes only the total count. There are no latency buckets or P95/P99 calculations.

`MetricSnapshot` exposes `total`, `success`, `failure`, `elapsed_us`, `slow`,
`slow_elapsed_us`, and `failure_elapsed_us`. `success` is derived as
`total.saturating_sub(failure)`. Counter fields are independently atomic, so snapshots
are eventually consistent rather than atomic across fields. A completed record is
never consumed by a scrape. Under continuous writes, different fields and series
can reflect slightly different instants.

## Prometheus reader

```rust
use brz_metrics::{Metric, PrometheusReader};
use std::io::Read;
use std::time::Duration;

Metric::redis("cache").record(Duration::from_millis(60), false);
let mut reader = PrometheusReader::new();
let mut output = String::new();
reader.read_to_string(&mut output)?;
assert!(output.contains("brz_req_total"));
# Ok::<(), std::io::Error>(())
```

The six metric families are cumulative **counters**, each with `type` and `name`
labels. Duration sums are rendered in **seconds**, rounded to exactly two decimal
places. Internal counters retain microsecond precision; rounding happens only after
source counters are summed. Exposition has 0.01-second resolution, so low-traffic
series may show unchanged duration sums across scrapes before a rounded increment
appears. Counts are not rounded.

| Group | Count | Duration sum |
| --- | --- | --- |
| All | `brz_req_total` | `brz_req_duration_secs_total` |
| Failed | `brz_err_total` | `brz_err_duration_secs_total` |
| Slow | `brz_slow_total` | `brz_slow_duration_secs_total` |

For example:

```text
# TYPE brz_req_total counter
brz_req_total{type="REDIS",name="cache"} 1
```

Zero samples remain present once registered. A reader captures the registered export
metadata and current RPC state at construction; requests continue accumulating while
it is read. Registrations after its per-shard prefixes were captured appear in the
next reader. Counter samples are loaded one family at a time. The encoded buffer holds
one sample at a time, rather than the entire response. Captured metadata takes memory
proportional to the exported views, sharing label strings; construction sorts these
views to group families and merge duplicate series.

Existing `producer::ProfilePlan` and `register_profile` APIs remain available for
source compatibility. `ProfileFormat` is an ignored compatibility marker. Aliases
export the same slot under their declared `type`/`name`. Aggregate targets sum source
slots during exposition. Duplicate views of the same slot count once; distinct slots
with identical `type`/`name` are summed, including overlaps between direct and aggregate
outputs. `visit` exposes direct aliases and leaves aggregation to the reader. When
summing in PromQL, select the desired type/name to avoid counting aliases twice.

RPC state finite numeric fields are exposed as
`brz_rpc_state{type="MOTAN_CLUSTER_STAT",name="...",field="..."}` gauges.
Non-numeric, NaN, and infinite fields remain stored but are omitted from exposition.

### HTTP integration

The application owns the `/metrics` route, response body, and authentication. Use
`brz_metrics::prometheus::CONTENT_TYPE` for the response Content-Type
(`text/plain; version=0.0.4; charset=utf-8`). Create a fresh reader for every scrape.
The library does not start a server or register a service-discovery target.

Enable the optional Tokio adapter in a release containing this change (or use this
checkout as a local path dependency):

```toml
brz-metrics = { version = "0.0.10", features = ["tokio"] }
```

`PrometheusReader` then implements both `std::io::Read` and
`tokio::io::AsyncRead`. Its async reads copy initialized bytes directly to `ReadBuf`
and return `Ready`: this is in-memory encoding, with no runtime or background task
required. The HTTP transport handles network I/O and backpressure. For frameworks
accepting a stream, the application can add `tokio-util` with its `io` feature and use:

```rust,ignore
let stream = tokio_util::io::ReaderStream::new(brz_metrics::PrometheusReader::new());
// Pass `stream` to the HTTP framework's streaming response body.
```

### Queries

QPS, error ratio, and average latency over the chosen window are computed by Prometheus:

```promql
rate(brz_req_total[5m])
rate(brz_err_total[5m]) / rate(brz_req_total[5m])
rate(brz_req_duration_secs_total[5m]) / rate(brz_req_total[5m])
```

A zero rate denominator produces NaN. For multiple instances, apply `rate` to each
series first, then aggregate the numerator and denominator separately. Prometheus
recognizes process-restart counter resets. Do not interpret these values as interval
counts or precomputed QPS.

### Exact HTTP status codes

`Metric::api(name)` records the six all-request counters with type `API`.
`CounterMetric::new(MetricType::Api(code), name)` uses the internal `Api(u16)` variant and exposes
only a count:

```text
brz_api_resp_total{type="API",name="/users/:id",status_code="503"} 12
```

Codes are bounded to 100..=599. There is no `status_class` label. Producers register
only observed codes and call `inc()`; they record the overall request once in
`Metric::api(name)`, including its timing and success. Per-code counters do not repeat
the six duration/error/slow series. `CounterMetric::new(MetricType::ApiTimeout, name)` exports only
`brz_api_timeout_total{type="API",name="..."}` and is incremented for timeouts.
A timed-out response may also have an HTTP status, such as 408; its status counter and
its timeout counter describe different dimensions of the same overall request.

The sibling HTTP server implements lazy per-code handle caching, with all requests
in `API`, errors for statuses >=400 or timeouts, and the existing 200 ms slow threshold.
Legacy `api_3xx`, `api_4xx`, and `api_5xx` constructors remain for source compatibility;
the new HTTP server does not use them. `visit` retains its legacy three-argument shape:
exact-code slots are reported as `API_RESPONSE`; the code label is available in the
Prometheus reader. MySQL metrics retain type `MYSQL` and the 50 ms slow threshold.

## Operational safety

Use stable, bounded names such as route templates. The process registry retains
metric names and RPC state keys for its lifetime. Avoid raw URLs, user IDs, or arbitrary
request values. Prometheus labels are escaped but are not redacted. Each full exported view
adds six counter series; observed API codes and timeout metrics each add one. Aliases
and aggregates add full views. Concurrent scrapes each
capture their own export metadata, so account for scrape concurrency when sizing memory.

## Installation

After the first successful publish, use the package with the existing Rust
library name:

```toml
[dependencies]
brz-metrics = "0.0.10"
```

## CI and publishing

Pushes and pull requests run rustfmt, Clippy with warnings denied, all test
and benchmark targets with default and all features, and release-mode library tests
with all features. This crate
has no Loom models; its concurrent registration and traversal tests run in
both debug and release modes.

Before publishing:

1. Grant this repository access to the `we-breeze` organization Actions secret
   `CARGO_REGISTRY_TOKEN`, or configure a repository secret with the same name.
   The token must allow creating and publishing `brz-metrics`; the first publish
   creates the crate automatically. Organization secrets shared with public
   repositories are available to this public repository.
2. Ensure repository rules allow Actions to push version commits to `main`
   and create release tags. Publish requests `contents: write` permission.
3. Push these workflow files to the GitHub default branch, `main`.

Use **Actions → Publish → Run workflow**, select `main`, and leave `retry_tag`
empty for a new release. Merging or pushing code only runs CI; publishing is
manual. Publish increments the greatest `v0.0.x` tag, updates Cargo.toml and
Cargo.lock, runs checks and `cargo publish --dry-run`, atomically pushes the
release commit and annotated tag, then uploads to crates.io. With existing
`v0.0.1` and `v0.0.2` tags, the first new release will be `v0.0.3`. The initial
unpublished Cargo version `0.1.0` is replaced by this sequence.

Publishing is serialized and rejects stale checkouts. If an upload fails after
the tag is pushed, start a new Publish run and set `retry_tag` to that existing
tag. Do not enter a new version in this field: it only retries an existing
release with matching Cargo metadata. Check crates.io before retrying an
upload timeout; published versions cannot be overwritten. Source fixes require
a new release. No GitHub Release is created.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE-APACHE](LICENSE-APACHE).

## Crate naming

The package name is `brz-metrics`; the Rust library name is `brz_metrics`.
Use `brz_metrics::...` in Rust code. This replaces the previous `metrics`
library name. Existing explicit dependency aliases remain supported.

```toml
[dependencies]
brz-metrics = "0.0.10"
```
