use crate::metric::{MetricSnapshot, ProfileLogFormat, ProfileSpec, drain};
use crate::registry::{MetricMeta, Registry};
use chrono::{DateTime, FixedOffset, Utc};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

const PROFILE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_PROFILE_LOG_PATH: &str = "../logs/profile.log";
const PROFILE_LOG_PATH_ENV: &str = "BREEZE_PROFILE_LOG_PATH";
pub(crate) const PROFILE_BUFFER_LIMIT: usize = 1024 * 1024;
pub(crate) const SHANGHAI_OFFSET_SECONDS: i32 = 8 * 60 * 60;

static PROFILE_LOGGER_STARTED: OnceLock<()> = OnceLock::new();

pub(crate) fn start_default_profile_logger(registry: &'static Registry) {
    PROFILE_LOGGER_STARTED.get_or_init(|| {
        let path = profile_log_path();
        if let Err(error) = thread::Builder::new()
            .name("metrics-profile-log".to_owned())
            .spawn(move || {
                let mut buffer = Vec::with_capacity(PROFILE_BUFFER_LIMIT);
                loop {
                    thread::sleep(PROFILE_INTERVAL);
                    let offset = FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS)
                        .expect("+08:00 must be a valid fixed offset");
                    let timestamp = Utc::now().with_timezone(&offset);
                    if let Err(error) = registry.write_profile_log(&path, timestamp, &mut buffer) {
                        eprintln!("metrics: failed to write {}: {error}", path.display());
                    }
                }
            })
        {
            eprintln!("metrics: failed to start profile logger: {error}");
        }
    });
}

fn profile_log_path() -> PathBuf {
    env::var_os(PROFILE_LOG_PATH_ENV)
        .filter(|path| !path.is_empty())
        .map(Into::into)
        .unwrap_or_else(|| DEFAULT_PROFILE_LOG_PATH.into())
}

impl Registry {
    pub(crate) fn write_profile_log(
        &self,
        path: &Path,
        timestamp: DateTime<FixedOffset>,
        buffer: &mut Vec<u8>,
    ) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Reopen for each interval so an external log rotator can rename the old file safely.
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let timestamp = timestamp.format("%Y-%m-%d %H:%M:%S").to_string();
        let mut writer = ProfileLogWriter::new(file, buffer, timestamp);
        self.for_each_meta(|meta| writer.write_entry(meta));
        // Java's ProfileUtil.logBaselineAccessStaticstic() appends a fixed
        // sentinel entry after every interval so monitors can confirm the
        // profiler is alive. The values are constant by design.
        writer.write_baseline_entry();
        writer.finish()
    }
}

/// Owns interval-log serialization and its bounded reusable write buffer.
struct ProfileLogWriter<'a> {
    file: File,
    buffer: &'a mut Vec<u8>,
    timestamp: String,
    error: Option<io::Error>,
}

impl<'a> ProfileLogWriter<'a> {
    fn new(file: File, buffer: &'a mut Vec<u8>, timestamp: String) -> Self {
        buffer.clear();
        Self {
            file,
            buffer,
            timestamp,
            error: None,
        }
    }

    fn write_entry(&mut self, meta: &MetricMeta) {
        if self.error.is_some() {
            return;
        }

        // `swap(0)` keeps the interval accounting lossless. A record racing this drain can have
        // fields split across adjacent intervals, but each individual counter is emitted once.
        append_profile_entry(
            self.buffer,
            &self.timestamp,
            &meta.name,
            meta.kind.profile(),
            drain(meta.item),
        );
        if self.buffer.len() >= PROFILE_BUFFER_LIMIT {
            self.flush_buffer();
        }
    }

    /// Appends the fixed `other://profile_baseline` sentinel, mirroring Java's
    /// `ProfileUtil.logBaselineAccessStaticstic()`. Values are constant by design.
    fn write_baseline_entry(&mut self) {
        if self.error.is_some() {
            return;
        }
        // type=OTHER, total=10, error=1, slow=1, avg=1.00,
        // interval1=6, interval2=1, interval3=1, interval4=1, interval5=1
        writeln!(
            self.buffer,
            "{} {{\"type\":\"OTHER\",\"name\":\"other://profile_baseline\",\"total_count\":10,\"error_count\":1,\"slow_count\":1,\"avg_time\":1.0,\"interval1\":6,\"interval2\":1,\"interval3\":1,\"interval4\":1,\"interval5\":1}}",
            self.timestamp,
        )
        .expect("writing to a Vec<u8> cannot fail");
        if self.buffer.len() >= PROFILE_BUFFER_LIMIT {
            self.flush_buffer();
        }
    }

    fn flush_buffer(&mut self) {
        if self.buffer.is_empty() || self.error.is_some() {
            return;
        }

        if let Err(error) = self.file.write_all(self.buffer) {
            // Do not let an unavailable filesystem turn into an unbounded in-memory log buffer.
            self.buffer.clear();
            self.error = Some(error);
            return;
        }
        self.buffer.clear();
    }

    fn finish(mut self) -> io::Result<()> {
        self.flush_buffer();
        if let Some(error) = self.error {
            return Err(error);
        }
        self.file.flush()
    }
}

fn append_profile_entry(
    buffer: &mut Vec<u8>,
    timestamp: &str,
    name: &str,
    profile: ProfileSpec,
    snapshot: MetricSnapshot,
) {
    match profile.log_format {
        ProfileLogFormat::Resource => {
            append_resource_entry(buffer, timestamp, name, profile, snapshot)
        }
        ProfileLogFormat::RpcServiceWhole => {
            append_rpc_service_whole_entry(buffer, timestamp, name, profile, snapshot);
        }
    }
}

fn append_resource_entry(
    buffer: &mut Vec<u8>,
    timestamp: &str,
    name: &str,
    profile: ProfileSpec,
    snapshot: MetricSnapshot,
) {
    write!(
        buffer,
        "{timestamp} {{\"type\":\"{}\",\"name\":\"{}\",\"slowThreshold\":{},\"total_count\":{},\"error_count\":{},\"slow_count\":{},\"avg_time\":",
        profile.label,
        name,
        profile.slow_threshold_ms,
        snapshot.total,
        snapshot.failure,
        snapshot.slow,
    )
    .expect("writing to a Vec<u8> cannot fail");
    append_resource_average(buffer, snapshot);
    writeln!(
        buffer,
        ",\"interval1\":{},\"interval2\":{},\"interval3\":{},\"interval4\":{},\"interval5\":{}}}",
        snapshot.intervals[0],
        snapshot.intervals[1],
        snapshot.intervals[2],
        snapshot.intervals[3],
        snapshot.intervals[4],
    )
    .expect("writing to a Vec<u8> cannot fail");
}

fn append_resource_average(buffer: &mut Vec<u8>, snapshot: MetricSnapshot) {
    if snapshot.total == 0 {
        buffer.extend_from_slice(b"0.0");
    } else {
        write!(
            buffer,
            "\"{:.2}\"",
            snapshot.elapsed_ns as f64 / snapshot.total as f64 / 1_000_000.0
        )
        .expect("writing to a Vec<u8> cannot fail");
    }
}

fn append_rpc_service_whole_entry(
    buffer: &mut Vec<u8>,
    timestamp: &str,
    name: &str,
    profile: ProfileSpec,
    snapshot: MetricSnapshot,
) {
    write!(
        buffer,
        "{timestamp} {{\"type\":\"{}\",\"name\":\"{}\",\"slowThreshold\":\"{}\",\"total_count\":\"{}\",\"slow_count\":\"{}\",\"avg_time\":",
        profile.label,
        name,
        profile.slow_threshold_ms,
        snapshot.total,
        snapshot.slow,
    )
    .expect("writing to a Vec<u8> cannot fail");
    if snapshot.total == 0 {
        buffer.extend_from_slice(b"\"0\"");
    } else {
        write!(
            buffer,
            "\"{:.2}\"",
            snapshot.elapsed_ns as f64 / snapshot.total as f64 / 1_000_000.0
        )
        .expect("writing to a Vec<u8> cannot fail");
    }
    writeln!(
        buffer,
        ",\"interval1\":\"{}\",\"interval2\":\"{}\",\"interval3\":\"{}\",\"interval4\":\"{}\",\"interval5\":\"{}\",\"p75\":\"0.00\",\"p95\":\"0.00\",\"p98\":\"0.00\",\"p99\":\"0.00\",\"p999\":\"0.00\",\"biz_excp\":\"0\",\"other_excp\":\"{}\",\"avg_tps\":\"0\",\"max_tps\":\"0\",\"min_tps\":\"0\"}}",
        snapshot.intervals[0],
        snapshot.intervals[1],
        snapshot.intervals[2],
        snapshot.intervals[3],
        snapshot.intervals[4],
        snapshot.failure,
    )
    .expect("writing to a Vec<u8> cannot fail");
}
