# metrics

Minimal in-crate metric registry focused on:

- sharded, synchronous registration during service startup
- fixed `Slot` layout with six `AtomicU64` counters
- lock-free record path
- allocation-free periodic visit at every lifecycle stage, using `ArcSwap`
- keyed AHash routing and lookup for long metric names

## API

- `Metric::register(name, kind) -> Metric`
- `metric.record(elapsed: Duration, success)`
- `metrics::visit(|name, kind, snapshot| { ... })`

Each shard appends metadata into 256-item chunks. Adding an item to an existing chunk publishes
only its per-chunk length; adding a new chunk copy-on-writes that shard's chunk-pointer index.
`visit` sees per-shard published prefixes, so a concurrent registration is observed either in the
current traversal or the next one, never as uninitialized metadata.

The first `Metric::register` starts a 10-second background profile logger. It drains interval
counters and appends ProfileUtil-compatible lines to `../logs/profile.log`; timestamps
use the `+08:00` clock but intentionally have no timezone suffix. Set
`BREEZE_PROFILE_LOG_PATH` before the first registration to override the path.

`Slot` stores error, elapsed-nanosecond sum, and five latency buckets. `total` and `slow` are
derived from the buckets; `success` is derived as `total - error` in snapshots.
