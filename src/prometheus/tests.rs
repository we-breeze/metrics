use super::*;
use crate::metric::{MetricSnapshot, MetricType};
use crate::plan::{MetricPolicy, ProfileFormat, ProfilePlan};
use crate::state::StateType;
use std::collections::BTreeMap;
use std::time::Duration;

fn text(registry: &Arc<Registry>) -> String {
    let mut output = String::new();
    PrometheusReader::from_registry(Arc::clone(registry))
        .read_to_string(&mut output)
        .unwrap();
    output
}

fn samples(output: &str) -> BTreeMap<&str, &str> {
    let mut result = BTreeMap::new();
    for line in output.lines().filter(|line| !line.starts_with('#')) {
        let (key, value) = line.rsplit_once(' ').unwrap();
        assert!(
            result.insert(key, value).is_none(),
            "duplicate sample {key}"
        );
    }
    result
}

#[test]
fn counters_are_cumulative_and_durations_use_seconds() {
    let registry = Arc::new(Registry::new());
    let metric = registry.register("cache", MetricType::Redis(crate::ResourceMetric::Req));
    metric.record(Duration::from_millis(5), true);
    metric.record(Duration::from_millis(50), false);
    metric.record(Duration::from_micros(250_001), true);
    let before = metric.snapshot();
    let output = text(&registry);
    assert_eq!(text(&registry), output);
    assert_eq!(metric.snapshot(), before);
    let values = samples(&output);
    for (name, expected) in [
        ("brz_req_total", "3"),
        ("brz_err_total", "1"),
        ("brz_slow_total", "2"),
        ("brz_req_duration_secs_total", "0.31"),
        ("brz_err_duration_secs_total", "0.05"),
        ("brz_slow_duration_secs_total", "0.30"),
    ] {
        assert_eq!(
            values[format!("{name}{{type=\"REDIS\",name=\"cache\"}}").as_str()],
            expected
        );
        assert_eq!(
            output.matches(&format!("# TYPE {name} counter\n")).count(),
            1
        );
    }
    metric.record(Duration::from_millis(1), false);
    let next = text(&registry);
    assert_eq!(
        samples(&next)["brz_req_total{type=\"REDIS\",name=\"cache\"}"],
        "4"
    );
    assert_eq!(metric.snapshot().elapsed_us, 306_001);
}

#[test]
fn duration_rounding_handles_half_centiseconds_carry_and_large_totals() {
    for (micros, expected) in [
        (0, "0.00"),
        (4_999, "0.00"),
        (5_000, "0.01"),
        (994_999, "0.99"),
        (995_000, "1.00"),
        (12_345_674_999, "12345.67"),
        (12_345_675_000, "12345.68"),
        (u64::MAX, "18446744073709.55"),
    ] {
        let registry = Arc::new(Registry::new());
        let metric = registry.register("rounding", MetricType::Redis(crate::ResourceMetric::Req));
        metric.record(Duration::from_micros(micros), false);
        let output = text(&registry);
        assert_eq!(
            samples(&output)["brz_req_duration_secs_total{type=\"REDIS\",name=\"rounding\"}"],
            expected,
        );
        assert_eq!(metric.snapshot().elapsed_us, micros);
        assert_eq!(metric.snapshot().total, 1);
    }
}

#[test]
fn one_byte_reads_empty_reads_and_repeated_eof_preserve_output() {
    let registry = Arc::new(Registry::new());
    registry
        .register(
            "long unicode-测试".repeat(40).as_str(),
            MetricType::Service(crate::ResourceMetric::Req),
        )
        .increment();
    let expected = text(&registry);
    for size in [1, 2, 7, 127, 4096] {
        let mut reader = PrometheusReader::from_registry(Arc::clone(&registry));
        let mut output = Vec::new();
        let mut buffer = vec![0; size];
        loop {
            assert_eq!(reader.read(&mut []).unwrap(), 0);
            let n = reader.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..n]);
        }
        assert_eq!(String::from_utf8(output).unwrap(), expected);
        assert_eq!(reader.read(&mut buffer).unwrap(), 0);
        assert_eq!(reader.read(&mut buffer).unwrap(), 0);
    }
}

#[test]
fn labels_are_escaped_and_zero_series_are_retained() {
    let registry = Arc::new(Registry::new());
    let hostile = "quoted\"\\ unicode-测试\n# TYPE injected gauge";
    registry.register_profile(ProfilePlan::new(
        "hostile",
        MetricPolicy::Resource,
        ProfileFormat::Resource,
        hostile,
        hostile,
    ));
    let output = text(&registry);
    let mut escaped = Vec::new();
    label(&mut escaped, hostile);
    let escaped = String::from_utf8(escaped).unwrap();
    let expected = format!("type=\"{escaped}\",name=\"{escaped}\"");
    assert_eq!(
        output.lines().filter(|line| line.starts_with('#')).count(),
        13
    );
    assert_eq!(
        output
            .lines()
            .filter(|line| line.contains(&expected))
            .count(),
        6
    );
    for value in samples(&output).values() {
        assert!(matches!(*value, "0" | "0.00"));
    }
}

#[test]
fn aliases_and_aggregates_merge_unique_slots_without_duplicate_series() {
    let registry = Arc::new(Registry::new());
    let first = registry.register_profile(
        ProfilePlan::new(
            "first",
            MetricPolicy::Access,
            ProfileFormat::AccessStatistic,
            "RPC_SERVICE_CLIENT",
            "Service.first",
        )
        .alias("SERVICE", "Service.first")
        .alias("SERVICE", "Service.first")
        .aggregate("SERVICE", "app"),
    );
    let second = registry.register_profile(
        ProfilePlan::new(
            "second",
            MetricPolicy::Access,
            ProfileFormat::AccessStatistic,
            "RPC_SERVICE_CLIENT",
            "Service.second",
        )
        .aggregate("SERVICE", "app"),
    );
    // A direct output may overlap an aggregate; its contribution is counted once.
    let third = registry.register_profile(
        ProfilePlan::new(
            "third",
            MetricPolicy::Access,
            ProfileFormat::Resource,
            "SERVICE",
            "app",
        )
        .aggregate("SERVICE", "app"),
    );
    first.record(Duration::from_millis(250), false);
    first.record(Duration::from_millis(150), true);
    second.record(Duration::from_millis(450), true);
    third.record(Duration::from_millis(100), true);
    let output = text(&registry);
    let values = samples(&output);
    assert_eq!(values.len(), 4 * 6);
    assert_eq!(values["brz_req_total{type=\"SERVICE\",name=\"app\"}"], "4");
    assert_eq!(
        values["brz_req_duration_secs_total{type=\"SERVICE\",name=\"app\"}"],
        "0.95"
    );
    assert_eq!(values["brz_err_total{type=\"SERVICE\",name=\"app\"}"], "1");
    assert_eq!(values["brz_slow_total{type=\"SERVICE\",name=\"app\"}"], "2");
    assert_eq!(
        values["brz_req_total{type=\"SERVICE\",name=\"Service.first\"}"],
        "2"
    );
    assert_eq!(text(&registry), output);
    assert_eq!(first.snapshot().total, 2);
}

#[test]
fn numeric_rpc_state_is_a_gauge_and_is_captured_per_reader() {
    let registry = Arc::new(Registry::new());
    registry.record_state(StateType::Rpc, "Service", "nodes", "4");
    registry.record_state(StateType::Rpc, "Service", "unavailable", "1");
    registry.record_state(StateType::Rpc, "Service", "description", "not numeric");
    registry.record_state(StateType::Rpc, "Service", "infinite", "inf");
    registry.record_state(StateType::Rpc, "Service", "not-a-number", "NaN");
    let mut reader = PrometheusReader::from_registry(Arc::clone(&registry));
    registry.record_state(StateType::Rpc, "Service", "unavailable", "0");
    let mut output = String::new();
    reader.read_to_string(&mut output).unwrap();
    let key = "brz_rpc_state{type=\"MOTAN_CLUSTER_STAT\",name=\"Service\",field=\"unavailable\"}";
    assert_eq!(samples(&output).len(), 2);
    assert_eq!(samples(&output)[key], "1");
    assert_eq!(samples(&text(&registry))[key], "0");
}

#[test]
fn reader_freezes_registration_range_and_owns_slot_storage() {
    let registry = Arc::new(Registry::new());
    registry
        .register("early", MetricType::Redis(crate::ResourceMetric::Req))
        .increment();
    let mut reader = PrometheusReader::from_registry(Arc::clone(&registry));
    registry
        .register("late", MetricType::Redis(crate::ResourceMetric::Req))
        .increment();
    let mut output = String::new();
    reader.read_to_string(&mut output).unwrap();
    assert!(!output.contains("name=\"late\""));
    assert!(text(&registry).contains("name=\"late\""));
    let mut reader = PrometheusReader::from_registry(Arc::clone(&registry));
    drop(registry);
    output.clear();
    reader.read_to_string(&mut output).unwrap();
    assert_eq!(samples(&output).len(), 12);
}

#[test]
fn concurrent_registration_and_scrapes_are_safe() {
    let registry = Arc::new(Registry::new());
    let writer_registry = Arc::clone(&registry);
    let writer = std::thread::spawn(move || {
        for i in 0..2000 {
            writer_registry
                .register(
                    &format!("racing.{i}"),
                    MetricType::Redis(crate::ResourceMetric::Req),
                )
                .increment();
        }
    });
    for _ in 0..4 {
        samples(&text(&registry));
    }
    writer.join().unwrap();
    let output = text(&registry);
    assert_eq!(samples(&output).len(), 2000 * 6);
    assert!(
        samples(&output)
            .iter()
            .filter(|(key, _)| key.starts_with("brz_req_total"))
            .all(|(_, value)| *value == "1")
    );
}

#[test]
fn count_only_metrics_do_not_invent_duration_or_failures() {
    let registry = Arc::new(Registry::new());
    let metric = registry.register("drops", MetricType::Log);
    metric.increment();
    metric.increment();
    let output = text(&registry);
    assert_eq!(
        samples(&output)["brz_req_total{type=\"LOG\",name=\"drops\"}"],
        "2"
    );
    assert_eq!(
        metric.snapshot(),
        MetricSnapshot {
            total: 2,
            success: 2,
            ..MetricSnapshot::default()
        }
    );
}

#[test]
fn concurrent_scalar_registration_and_scraping_publish_complete_metadata() {
    use crate::scalar::ScalarKind;
    let registry = Arc::new(Registry::new());
    let writers = (0..4)
        .map(|worker| {
            let registry = Arc::clone(&registry);
            std::thread::spawn(move || {
                for index in 0..1500 {
                    let name = format!("scalar-racing.{worker}.{index}");
                    let kind = MetricType::Redis(crate::ResourceMetric::Conn);
                    for mode in [ScalarKind::Counter, ScalarKind::Gauge] {
                        let item = registry.scalars.register(kind, mode, &name);
                        let duplicate = registry.scalars.register(kind, mode, &name);
                        assert_eq!(item, duplicate);
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for _ in 0..4 {
        samples(&text(&registry));
    }
    for writer in writers {
        writer.join().unwrap();
    }
    let output = text(&registry);
    let values = samples(&output);
    assert_eq!(registry.len(), 12_000);
    assert_eq!(values.len(), 12_000);
    assert!(values.values().all(|value| *value == "0"));
    assert_eq!(text(&registry), output);
}

#[test]
fn typed_counter_gauge_rpc_and_service_use_implicit_families() {
    use crate::{CounterMetric, GaugeMetric, ResourceMetric, TimingMetric};
    let conn_kind = MetricType::Redis(ResourceMetric::Conn);
    let counter = CounterMetric::new(conn_kind, "typed-export-test");
    let gauge = GaugeMetric::new(conn_kind, "typed-export-test");
    counter.add(12);
    gauge.set(4);
    let status = CounterMetric::new(MetricType::Api(503), "typed-export-test");
    status.inc();
    let rpc = TimingMetric::new(MetricType::Rpc(ResourceMetric::Req), "typed-export-test");
    let service = TimingMetric::new(
        MetricType::Service(ResourceMetric::Req),
        "typed-export-test",
    );
    rpc.record(Duration::from_millis(250), false);
    service.record(Duration::from_millis(50), true);
    let output = text(crate::registry());
    let values = samples(&output);
    assert_eq!(
        values["brz_conn_total{type=\"REDIS\",name=\"typed-export-test\"}"],
        "12"
    );
    assert_eq!(
        values["brz_conn{type=\"REDIS\",name=\"typed-export-test\"}"],
        "4"
    );
    assert_eq!(
        values["brz_api_resp_total{type=\"API\",name=\"typed-export-test\",status_code=\"503\"}"],
        "1"
    );
    assert_eq!(
        values["brz_slow_total{type=\"RPC\",name=\"typed-export-test\"}"],
        "1"
    );
    assert_eq!(
        values["brz_slow_total{type=\"SERVICE\",name=\"typed-export-test\"}"],
        "0"
    );
    assert!(output.contains("# TYPE brz_conn gauge\n"));
    assert!(output.contains("# TYPE brz_conn_total counter\n"));
    assert_eq!(counter.get(), 12);
    assert_eq!(gauge.get(), 4);
}

#[test]
fn exact_api_codes_are_labels_and_export_only_response_counts() {
    let registry = Arc::new(Registry::new());
    let all = registry.register("/users/:id", MetricType::ApiAll);
    for code in [200, 201, 404, 503] {
        registry
            .register("/users/:id", MetricType::Api(code))
            .increment();
        all.record(Duration::from_millis(250), code < 400);
    }
    registry
        .register("/users/:id", MetricType::Api(503))
        .increment();
    registry
        .register("/users/:id", MetricType::ApiTimeout)
        .increment();
    let output = text(&registry);
    let values = samples(&output);
    assert_eq!(values.len(), 6 + 4 + 1);
    assert_eq!(
        values["brz_req_total{type=\"API\",name=\"/users/:id\"}"],
        "4"
    );
    assert_eq!(
        values["brz_err_total{type=\"API\",name=\"/users/:id\"}"],
        "2"
    );
    assert_eq!(
        values["brz_api_resp_total{type=\"API\",name=\"/users/:id\",status_code=\"503\"}"],
        "2"
    );
    assert_eq!(
        values["brz_api_timeout_total{type=\"API\",name=\"/users/:id\"}"],
        "1"
    );
    assert!(!output.contains("status_class"));
    assert_eq!(
        output
            .lines()
            .filter(|line| line.contains("status_code="))
            .count(),
        4
    );
    assert_eq!(text(&registry), output);
}

#[cfg(feature = "tokio")]
#[test]
fn async_reads_match_sync_without_a_runtime_and_support_mixed_reads() {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use tokio::io::{AsyncRead, ReadBuf};

    let registry = Arc::new(Registry::new());
    registry
        .register("async", MetricType::Redis(crate::ResourceMetric::Req))
        .record(Duration::from_micros(50_001), false);
    let expected = text(&registry);
    let mut cx = Context::from_waker(Waker::noop());
    for size in [1, 13, 4096] {
        let mut reader = PrometheusReader::from_registry(Arc::clone(&registry));
        let mut buffer = vec![0; size];
        let mut output = Vec::new();
        loop {
            let mut empty = ReadBuf::new(&mut []);
            assert!(matches!(
                Pin::new(&mut reader).poll_read(&mut cx, &mut empty),
                Poll::Ready(Ok(()))
            ));
            let mut buf = ReadBuf::new(&mut buffer);
            assert!(matches!(
                Pin::new(&mut reader).poll_read(&mut cx, &mut buf),
                Poll::Ready(Ok(()))
            ));
            if buf.filled().is_empty() {
                break;
            }
            output.extend_from_slice(buf.filled());
            let mut sync = [0; 3];
            let n = reader.read(&mut sync).unwrap();
            output.extend_from_slice(&sync[..n]);
        }
        assert_eq!(String::from_utf8(output).unwrap(), expected);
        let mut eof = ReadBuf::new(&mut buffer);
        assert!(matches!(
            Pin::new(&mut reader).poll_read(&mut cx, &mut eof),
            Poll::Ready(Ok(()))
        ));
        assert!(eof.filled().is_empty());
    }
}
