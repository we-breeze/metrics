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

The first metric registration starts a 10-second background profile logger. It drains interval
counters and appends ProfileUtil-compatible lines to `../logs/profile.log`; timestamps
use the `+08:00` clock but intentionally have no timezone suffix. Set
`BREEZE_PROFILE_LOG_PATH` before the first registration to override the path.
The default path is relative to the process working directory.
Metric rows whose current interval has all six counters equal to zero are not written.
State rows and the fixed baseline liveness row are still written every interval.

Profile log rotation is disabled by default. Set `BREEZE_PROFILE_LOG_ROTATION=hourly`
before the first metric registration to enable hourly rotation. The active file keeps
the configured name, while completed UTC+8 hours are archived with names such as
`profile.log.20260921-00`; the UTC+8 offset is not included in the file name. Rotation
does not delete or compress archives. If an archive name already exists, a numeric
suffix such as `.1` is appended instead of overwriting it.

`Slot` stores three count/duration-sum pairs: all requests, slow requests, and failed
requests. Durations are truncated to whole microseconds when recorded. A slow failed
request contributes to all three pairs. Six counters occupy 48 bytes; with alignment,
each slot occupies 64 bytes. No latency buckets or `interval1`–`interval5` fields are
maintained or logged. `success` is derived as `total - failure`, saturating at zero.

`MetricSnapshot` exposes `total`, `success`, `failure`, `elapsed_us`, `slow`,
`slow_elapsed_us`, and `failure_elapsed_us`. This replaces the old `elapsed_ns` and
`intervals` fields; callers using those fields must migrate when upgrading.

Profile rows retain `total_count`, `slow_count`, and `error_count` and add:

| Requests | QPS | Duration sum (ms) | Average duration (ms) |
| --- | --- | --- | --- |
| All | `qps` | `total_time` | `avg_time` |
| Slow | `slow_qps` | `slow_time` | `slow_avg_time` |
| Failed | `error_qps` | `error_time` | `error_avg_time` |

QPS uses the actual elapsed sampling window measured by a monotonic clock, including
logging delays and intervals skipped because the log file could not be opened or
rotated. Rates and durations are JSON strings with exactly two decimal places;
duration sums and averages are in **milliseconds**. An average with a zero count is
`"0.00"`. `slowThreshold` is also in milliseconds, with two decimal places. Resource
counts remain JSON numbers; access-statistic counts remain strings. Access-statistic
`avg_tps` now reports the same measured rate as `qps`; percentile, min/max TPS, and
business-exception fields remain compatibility placeholders.

Counters are read and drained independently, so snapshots and log windows are
eventually consistent rather than atomic across all six fields. A record racing a
sample can put its count and duration in neighboring windows. Rows containing only
duration data are retained with zero QPS/average and nonzero duration sums, so those
drained values are not silently discarded. Log writes remain best effort: an I/O
failure after counters have been drained can lose that batch.

HTTP server route metrics use profile types `API`, `API3XX`, `API4XX`,
`API5XX`, and `APITO`, all with the unmodified route template as `name` and the
service latency policy (200 ms slow threshold). `API` represents 2xx responses.

MySQL 客户端使用 `Metric::mysql(name)` 注册 `MYSQL` 类型指标，按 host 和 get/list/update/transaction 操作聚合，沿用资源指标的 50ms 慢调用阈值。

## Operational safety

Use stable, bounded metric names such as route templates. The process-wide
registry retains metric names and RPC state keys for the lifetime of the
process; raw URLs, user IDs, or arbitrary request values can cause unbounded
memory growth and disclose data in logs. JSON strings are escaped when written,
but escaping does not redact sensitive values.

Configure a log directory writable only by the service account. The logger follows
the configured filesystem path and does not enforce a disk quota or delete old files.
Leave built-in rotation disabled if an external log rotator manages the file.

## Installation

After the first successful publish, use the package with the existing Rust
library name:

```toml
[dependencies]
brz-metrics = "0.0.4"
```

## CI and publishing

Pushes and pull requests run rustfmt, Clippy with warnings denied, all test
and benchmark targets in test mode, and release-mode library tests. This crate
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
brz-metrics = "0.0.4"
```
