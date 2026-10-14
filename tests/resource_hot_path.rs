use brz_metrics::{
    ConnMetric, ConnectionRole, MetricAccess, MetricType, PrometheusReader, ResourceLabels,
    ResourceMetric, TimingMetric,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Read;
use std::time::Duration;

struct CountingAllocator;
thread_local! { static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) }; }
fn allocated() {
    ALLOCATIONS.with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn resource_handles_allocate_only_when_registered_and_keep_distinct_labels() {
    let labels = ResourceLabels::new("back\"end", "cache.example:6379");
    let kind = MetricType::Redis(ResourceMetric::Req);
    let read = TimingMetric::new_resource(kind, &labels.clone().with_access(MetricAccess::Read));
    let write = TimingMetric::new_resource(kind, &labels.clone().with_access(MetricAccess::Write));
    let other = TimingMetric::new_resource(
        kind,
        &ResourceLabels::new("other", "cache.example:6379").with_access(MetricAccess::Read),
    );
    let connection = ConnMetric::new_resource(
        MetricType::Redis(ResourceMetric::Conn),
        &labels.with_role(ConnectionRole::Shared),
    );
    ALLOCATIONS.with(|count| count.set(Some(0)));
    // Includes the first update, successes, failures, slow requests, and lifecycle guards.
    for i in 0..1000 {
        read.record(Duration::from_millis(300), i % 2 == 0);
        write.record(Duration::from_millis(10), true);
        drop(connection.connection());
    }
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(allocations, 0);
    assert_eq!(read.snapshot().total, 1000);
    assert_eq!(write.snapshot().total, 1000);
    assert_eq!(other.snapshot().total, 0);
    assert_eq!((connection.current(), connection.total()), (0, 1000));
    let mut output = String::new();
    PrometheusReader::new().read_to_string(&mut output).unwrap();
    assert!(output.contains("brz_req_total{type=\"REDIS\",name=\"back\\\"end\",endpoint=\"cache.example:6379\",access=\"read\"} 1000\n"));
    assert!(output.contains("brz_req_duration_secs_total{type=\"REDIS\",name=\"back\\\"end\",endpoint=\"cache.example:6379\",access=\"write\"} 10.00\n"));
    assert!(output.contains("brz_conn{type=\"REDIS\",name=\"back\\\"end\",endpoint=\"cache.example:6379\",role=\"shared\"} 0\n"));
}
