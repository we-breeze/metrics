# metrics

Minimal in-crate metric registry focused on:

- sharded, synchronous registration during service startup
- fixed `Slot` layout with six `AtomicU64` counters
- lock-free record path
- allocation-free periodic visit at every lifecycle stage, using `ArcSwap`
- keyed AHash routing and lookup for long metric names

## API

- Typed constructors such as `Metric::redis(name)`, `Metric::http(name)`, and `Metric::api(name)`
- `metric.record(elapsed: Duration, success)`
- `metrics::visit(|name, kind, snapshot| { ... })`

Each shard appends metadata into 256-item chunks. Adding an item to an existing chunk publishes
only its per-chunk length; adding a new chunk copy-on-writes that shard's chunk-pointer index.
`visit` sees per-shard published prefixes, so a concurrent registration is observed either in the
current traversal or the next one, never as uninitialized metadata.

The first metric registration starts a 10-second background profile logger. It drains interval
counters and appends ProfileUtil-compatible lines to `../logs/profile.log`; timestamps
use the `+08:00` clock but intentionally have no timezone suffix. Set
`BREEZE_PROFILE_LOG_PATH` before the first registration to override the path.
The default path is relative to the process working directory.

`Slot` stores error, elapsed-nanosecond sum, and five latency buckets. `total` and `slow` are
derived from the buckets; `success` is derived as `total - error` in snapshots.

`Metric::api(name)` emits profile type `API`, with the service latency policy
(200 ms slow threshold). HTTP server API exports register four names per route
template: `<path>_2xx`, `<path>_3xx`, `<path>_4xx`, and `<path>_5xx`.

MySQL 客户端使用 `Metric::mysql(name)` 注册 `MYSQL` 类型指标，按 host 和 get/list/update/transaction 操作聚合，沿用资源指标的 50ms 慢调用阈值。

## Operational safety

Use stable, bounded metric names such as route templates. The process-wide
registry retains metric names and RPC state keys for the lifetime of the
process; raw URLs, user IDs, or arbitrary request values can cause unbounded
memory growth and disclose data in logs. JSON strings are escaped when written,
but escaping does not redact sensitive values.

Configure a log directory writable only by the service account and rotate the
profile log externally. The logger follows the configured filesystem path and
does not enforce a disk quota or rotate files itself.
