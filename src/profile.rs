use crate::metric::{MetricSnapshot, MetricSpec, drain};
use crate::plan::{MetricPolicy, ProfileFormat, ProfileMeta, ProfileOutput};
use crate::registry::{MetricMeta, Registry};
use crate::state::StateSnapshot;
use chrono::{DateTime, FixedOffset, Utc};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

const PROFILE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_PROFILE_LOG_PATH: &str = "../logs/profile.log";
const PROFILE_LOG_PATH_ENV: &str = "BREEZE_PROFILE_LOG_PATH";
const PROFILE_LOG_ROTATION_ENV: &str = "BREEZE_PROFILE_LOG_ROTATION";
pub(crate) const PROFILE_BUFFER_LIMIT: usize = 1024 * 1024;
pub(crate) const SHANGHAI_OFFSET_SECONDS: i32 = 8 * 60 * 60;

static PROFILE_LOGGER_STARTED: OnceLock<()> = OnceLock::new();

pub(crate) fn start_default_profile_logger(registry: &'static Registry) {
    PROFILE_LOGGER_STARTED.get_or_init(|| {
        let path = profile_log_path();
        let rotation = profile_log_rotation();
        if let Err(error) = thread::Builder::new()
            .name("metrics-profile-log".to_owned())
            .spawn(move || {
                let mut buffer = Vec::with_capacity(PROFILE_BUFFER_LIMIT);
                let mut log_file = ProfileLogFile::new(path, rotation);
                let offset = FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS)
                    .expect("+08:00 must be a valid fixed offset");
                loop {
                    thread::sleep(PROFILE_INTERVAL);
                    let timestamp = Utc::now().with_timezone(&offset);
                    if let Err(error) = log_file.rotate_if_needed(timestamp) {
                        eprintln!(
                            "metrics: failed to rotate {}: {error}",
                            log_file.path().display()
                        );
                        continue;
                    }
                    if let Err(error) =
                        registry.write_profile_log(log_file.path(), timestamp, &mut buffer)
                    {
                        eprintln!(
                            "metrics: failed to write {}: {error}",
                            log_file.path().display()
                        );
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ProfileLogRotation {
    #[default]
    Never,
    Hourly,
}

impl FromStr for ProfileLogRotation {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "never" => Ok(Self::Never),
            "hourly" => Ok(Self::Hourly),
            _ => Err(format!(
                "unsupported profile log rotation policy {value:?}; expected never or hourly"
            )),
        }
    }
}

fn profile_log_rotation() -> ProfileLogRotation {
    match env::var(PROFILE_LOG_ROTATION_ENV) {
        Ok(value) => match value.parse() {
            Ok(rotation) => rotation,
            Err(error) => {
                eprintln!("metrics: {error}; profile log rotation remains disabled");
                ProfileLogRotation::Never
            }
        },
        Err(env::VarError::NotPresent) => ProfileLogRotation::Never,
        Err(env::VarError::NotUnicode(_)) => {
            eprintln!(
                "metrics: {PROFILE_LOG_ROTATION_ENV} is not valid Unicode; profile log rotation remains disabled"
            );
            ProfileLogRotation::Never
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct ProfileLogHour(i64);

fn profile_log_hour(timestamp: DateTime<FixedOffset>) -> ProfileLogHour {
    ProfileLogHour((timestamp.timestamp() + i64::from(SHANGHAI_OFFSET_SECONDS)).div_euclid(60 * 60))
}

fn profile_log_archive_suffix(hour: ProfileLogHour) -> io::Result<String> {
    let local_start = hour
        .0
        .checked_mul(60 * 60)
        .ok_or_else(|| io::Error::other("profile log rotation hour is out of range"))?;
    let utc_start = local_start
        .checked_sub(i64::from(SHANGHAI_OFFSET_SECONDS))
        .ok_or_else(|| io::Error::other("profile log rotation hour is out of range"))?;
    let timestamp = DateTime::<Utc>::from_timestamp(utc_start, 0)
        .ok_or_else(|| io::Error::other("profile log rotation hour is out of range"))?
        .with_timezone(
            &FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS)
                .expect("+08:00 must be a valid fixed offset"),
        );
    Ok(timestamp.format("%Y%m%d-%H").to_string())
}

struct ProfileLogFile {
    path: PathBuf,
    rotation: ProfileLogRotation,
    current_hour: Option<ProfileLogHour>,
}

impl ProfileLogFile {
    fn new(path: PathBuf, rotation: ProfileLogRotation) -> Self {
        Self {
            path,
            rotation,
            current_hour: None,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn rotate_if_needed(&mut self, timestamp: DateTime<FixedOffset>) -> io::Result<()> {
        if self.rotation == ProfileLogRotation::Never {
            return Ok(());
        }

        let observed_hour = profile_log_hour(timestamp);
        match self.current_hour {
            None => archive_stale_profile_log(&self.path, observed_hour)?,
            Some(current_hour) if observed_hour > current_hour => {
                archive_profile_log(&self.path, &profile_log_archive_suffix(current_hour)?)?;
            }
            Some(_) => return Ok(()),
        }
        self.current_hour = Some(observed_hour);
        Ok(())
    }
}

fn archive_stale_profile_log(path: &Path, current_hour: ProfileLogHour) -> io::Result<()> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.len() == 0 {
        return Ok(());
    }

    let offset = FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS)
        .expect("+08:00 must be a valid fixed offset");
    let modified = DateTime::<Utc>::from(metadata.modified()?).with_timezone(&offset);
    let modified_hour = profile_log_hour(modified);
    if modified_hour < current_hour {
        archive_profile_log(path, &profile_log_archive_suffix(modified_hour)?)?;
    }
    Ok(())
}

fn archive_profile_log(path: &Path, suffix: &str) -> io::Result<()> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.len() == 0 {
        return Ok(());
    }

    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::other("profile log path has no file name"))?
        .to_string_lossy();
    let base_name = format!("{file_name}.{suffix}");
    let mut archive = path.with_file_name(&base_name);
    let mut collision = 0_u32;
    while archive.exists() {
        collision = collision
            .checked_add(1)
            .ok_or_else(|| io::Error::other("too many colliding profile log archives"))?;
        archive = path.with_file_name(format!("{base_name}.{collision}"));
    }
    fs::rename(path, archive)
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
        writer.write_aggregate_entries();
        for state in self.state_snapshot() {
            writer.write_state_entry(&state);
        }
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
    aggregates: HashMap<AggregateKey, MetricSnapshot>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AggregateKey {
    output: ProfileOutput,
    policy: MetricPolicy,
    format: ProfileFormat,
}

impl<'a> ProfileLogWriter<'a> {
    fn new(file: File, buffer: &'a mut Vec<u8>, timestamp: String) -> Self {
        buffer.clear();
        Self {
            file,
            buffer,
            timestamp,
            error: None,
            aggregates: HashMap::new(),
        }
    }

    fn write_entry(&mut self, meta: &MetricMeta) {
        if self.error.is_some() {
            return;
        }

        // `swap(0)` keeps the interval accounting lossless. Empty interval rows
        // add no profile value, so omit them from the log.
        let snapshot = drain(meta.item);
        if snapshot.total == 0 {
            return;
        }
        match &meta.profile {
            ProfileMeta::Single { name, kind } => {
                let (metric_type, format) = kind.output();
                self.append_entry(metric_type, name, kind.policy(), format, snapshot);
            }
            ProfileMeta::Fanout(plan) => {
                for output in plan.outputs.iter() {
                    self.append_entry(
                        &output.metric_type,
                        &output.name,
                        plan.policy,
                        plan.format,
                        snapshot,
                    );
                }
                if let Some(output) = &plan.aggregate {
                    self.aggregates
                        .entry(AggregateKey {
                            output: output.clone(),
                            policy: plan.policy,
                            format: plan.format,
                        })
                        .or_default()
                        .merge(snapshot);
                }
            }
        }
    }

    fn write_aggregate_entries(&mut self) {
        let mut entries: Vec<_> = self.aggregates.drain().collect();
        entries.sort_unstable_by(|(left, _), (right, _)| {
            (&left.output.metric_type, &left.output.name)
                .cmp(&(&right.output.metric_type, &right.output.name))
        });
        for (key, snapshot) in entries {
            self.append_entry(
                &key.output.metric_type,
                &key.output.name,
                key.policy,
                key.format,
                snapshot,
            );
        }
    }

    fn write_state_entry(&mut self, state: &StateSnapshot) {
        if self.error.is_some() {
            return;
        }
        append_entry_header(
            self.buffer,
            &self.timestamp,
            state.state_type.profile_type(),
            &state.name,
        );
        for (key, value) in &state.fields {
            self.buffer.push(b',');
            append_json_string(self.buffer, key);
            self.buffer.push(b':');
            append_json_string(self.buffer, value);
        }
        writeln!(self.buffer, "}}").expect("writing to a Vec<u8> cannot fail");
        if self.buffer.len() >= PROFILE_BUFFER_LIMIT {
            self.flush_buffer();
        }
    }

    fn append_entry(
        &mut self,
        metric_type: &str,
        name: &str,
        policy: MetricPolicy,
        format: ProfileFormat,
        snapshot: MetricSnapshot,
    ) {
        append_profile_entry(
            self.buffer,
            &self.timestamp,
            metric_type,
            name,
            policy.spec(),
            format,
            snapshot,
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

/// Serialize every caller-provided string so it cannot inject fields or log lines.
fn append_json_string(buffer: &mut Vec<u8>, value: &str) {
    serde_json::to_writer(buffer, value).expect("writing a JSON string to a Vec<u8> cannot fail");
}

fn append_entry_header(buffer: &mut Vec<u8>, timestamp: &str, metric_type: &str, name: &str) {
    write!(buffer, "{timestamp} {{\"type\":").expect("writing to a Vec<u8> cannot fail");
    append_json_string(buffer, metric_type);
    buffer.extend_from_slice(b",\"name\":");
    append_json_string(buffer, name);
}

fn append_profile_entry(
    buffer: &mut Vec<u8>,
    timestamp: &str,
    metric_type: &str,
    name: &str,
    profile: MetricSpec,
    format: ProfileFormat,
    snapshot: MetricSnapshot,
) {
    match format {
        ProfileFormat::Resource => {
            append_resource_entry(buffer, timestamp, metric_type, name, profile, snapshot)
        }
        ProfileFormat::AccessStatistic => {
            append_access_statistic_entry(buffer, timestamp, metric_type, name, profile, snapshot);
        }
    }
}

fn append_resource_entry(
    buffer: &mut Vec<u8>,
    timestamp: &str,
    metric_type: &str,
    name: &str,
    profile: MetricSpec,
    snapshot: MetricSnapshot,
) {
    append_entry_header(buffer, timestamp, metric_type, name);
    write!(
        buffer,
        ",\"slowThreshold\":{},\"total_count\":{},\"error_count\":{},\"slow_count\":{},\"avg_time\":",
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

fn append_access_statistic_entry(
    buffer: &mut Vec<u8>,
    timestamp: &str,
    metric_type: &str,
    name: &str,
    profile: MetricSpec,
    snapshot: MetricSnapshot,
) {
    append_entry_header(buffer, timestamp, metric_type, name);
    write!(
        buffer,
        ",\"slowThreshold\":\"{}\",\"total_count\":\"{}\",\"slow_count\":\"{}\",\"avg_time\":",
        profile.slow_threshold_ms, snapshot.total, snapshot.slow,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn timestamp(hour: u32, minute: u32) -> DateTime<FixedOffset> {
        FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS)
            .unwrap()
            .with_ymd_and_hms(2026, 9, 21, hour, minute, 0)
            .single()
            .unwrap()
    }

    fn temp_profile_log() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "metrics-profile-rotation-{}-{nonce}",
                std::process::id()
            ))
            .join("profile.log")
    }

    #[test]
    fn parses_profile_log_rotation_case_insensitively() {
        assert_eq!("never".parse(), Ok(ProfileLogRotation::Never));
        assert_eq!(" HOURLY ".parse(), Ok(ProfileLogRotation::Hourly));
        assert!("daily".parse::<ProfileLogRotation>().is_err());
    }

    #[test]
    fn archive_suffix_uses_the_fixed_utc_plus_eight_hour() {
        let utc = Utc
            .with_ymd_and_hms(2026, 9, 20, 16, 30, 0)
            .single()
            .unwrap();
        let local = utc.with_timezone(&FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS).unwrap());

        assert_eq!(
            profile_log_archive_suffix(profile_log_hour(local)).unwrap(),
            "20260921-00"
        );
    }

    #[test]
    fn rotates_only_at_an_hour_boundary_and_keeps_the_active_name() {
        let path = temp_profile_log();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut log_file = ProfileLogFile::new(path.clone(), ProfileLogRotation::Hourly);

        log_file.rotate_if_needed(timestamp(16, 10)).unwrap();
        fs::write(&path, b"first hour\n").unwrap();
        log_file.rotate_if_needed(timestamp(16, 59)).unwrap();
        assert!(!path.with_file_name("profile.log.20260921-16").exists());

        log_file.rotate_if_needed(timestamp(17, 0)).unwrap();
        assert_eq!(
            fs::read(path.with_file_name("profile.log.20260921-16")).unwrap(),
            b"first hour\n"
        );
        assert!(!path.exists());

        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn rotation_is_disabled_by_default() {
        let path = temp_profile_log();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"unrotated\n").unwrap();
        let mut log_file = ProfileLogFile::new(path.clone(), ProfileLogRotation::default());

        log_file.rotate_if_needed(timestamp(16, 10)).unwrap();
        log_file.rotate_if_needed(timestamp(17, 0)).unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"unrotated\n");
        assert!(!path.with_file_name("profile.log.20260921-16").exists());
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn archives_a_stale_active_file_on_startup() {
        let path = temp_profile_log();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"stale\n").unwrap();
        let offset = FixedOffset::east_opt(SHANGHAI_OFFSET_SECONDS).unwrap();
        let modified = DateTime::<Utc>::from(fs::metadata(&path).unwrap().modified().unwrap())
            .with_timezone(&offset);
        let modified_hour = profile_log_hour(modified);
        let archive = path.with_file_name(format!(
            "profile.log.{}",
            profile_log_archive_suffix(modified_hour).unwrap()
        ));

        archive_stale_profile_log(&path, ProfileLogHour(modified_hour.0 + 1)).unwrap();

        assert_eq!(fs::read(archive).unwrap(), b"stale\n");
        assert!(!path.exists());
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn never_overwrites_an_existing_profile_archive() {
        let path = temp_profile_log();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let archive = path.with_file_name("profile.log.20260921-16");
        fs::write(&archive, b"existing archive\n").unwrap();
        let mut log_file = ProfileLogFile::new(path.clone(), ProfileLogRotation::Hourly);

        log_file.rotate_if_needed(timestamp(16, 10)).unwrap();
        fs::write(&path, b"new archive\n").unwrap();
        log_file.rotate_if_needed(timestamp(17, 0)).unwrap();

        assert_eq!(fs::read(&archive).unwrap(), b"existing archive\n");
        assert_eq!(
            fs::read(path.with_file_name("profile.log.20260921-16.1")).unwrap(),
            b"new archive\n"
        );
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
