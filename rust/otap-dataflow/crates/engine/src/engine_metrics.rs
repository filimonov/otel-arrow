// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Engine-level metrics for the OTAP dataflow engine.
//!
//! Unlike per-pipeline metrics (which are sampled on each pipeline thread),
//! engine metrics are emitted **once per engine instance** by a dedicated
//! background task spawned by the controller.
//!
//! **Metrics**
//! - `memory_rss` (`ObserveUpDownCounter<u64>`, `{By}`):
//!   Process-wide Resident Set Size -- physical memory currently held in RAM.
//!   Matches what external tools report (e.g. `kubectl top pod`, `htop`, `ps rss`).
//!
//! - `cpu_utilization` (`Gauge<f64>`, `{1}`):
//!   Process-wide CPU utilization as a ratio in `[0, 1]`, normalized across **all
//!   logical CPU cores on the system** (not just the cores assigned to the engine).
//!   Computed as `cpu_delta / (wall_delta x num_system_cores)` over the last
//!   measurement interval. A value of `1.0` means 100% of all system cores are
//!   in use; `0.5` on an 8-core machine corresponds to 4 fully loaded cores.
//!   Aligned with the OTel semantic convention `process.cpu.utilization`.
//!
//! - `memory_pressure_state` (`Gauge<u64>`, `{state}`):
//!   Process-wide memory limiter state encoded as `0=normal`, `1=soft`, `2=hard`.
//!
//! - `process_memory_usage_bytes`, `process_memory_soft_limit_bytes`,
//!   `process_memory_hard_limit_bytes` (`Gauge<u64>`, `{By}`):
//!   Process-wide memory limiter sample and effective limits.
//!
//!   We emit utilization directly (rather than a cumulative `cpu_time` counter)
//!   so that users can read the metric as-is without requiring PromQL `rate()`
//!   or similar query-time derivations.
//!
//!   TODO: Also emit a cumulative `cpu_time` counter (like the Go Collector's
//!   `process_cpu_seconds_total`) for users who prefer query-time computation.

use crate::memory_limiter::MemoryPressureState;
use cpu_time::ProcessTime;
use otel_arrow_dfe_telemetry::instrument::{Gauge, ObserveUpDownCounter};
use otel_arrow_dfe_telemetry::metrics::{MetricSet, MetricSetHandler, MetricSetSnapshot};
use otel_arrow_dfe_telemetry::registry::{EntityKey, TelemetryRegistryHandle};
use otel_arrow_dfe_telemetry::reporter::{MetricsReporter, ReportOutcome};
use otel_arrow_dfe_telemetry_macros::metric_set;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

/// The state a monitor and the workers it accounts for share.
#[derive(Debug, Default)]
struct AccountingState {
    /// Registered workers, including workers currently retaining zero bytes.
    workers: usize,
    /// Sum of what those workers most recently accounted for.
    accounted: u64,
    /// Whether some monitor already owns registering and reporting the residual.
    ///
    /// A process may run more than one engine monitor; only one of them reports
    /// the residual, because the residual describes the process, not the engine.
    reporter_owned: bool,
}

/// Series exporter memory accounting shared by the workers and the monitor.
///
/// Worker registration, a worker's accounted bytes, and the monitor's decision
/// to register, materialize or drop the residual all move through one guard,
/// so a residual is reported only while a worker is registered.
///
/// The guard is held across the registry calls the decision implies, so the
/// lock order is always accounting then registry; nothing takes the registry
/// lock first and then this one.
#[derive(Debug, Default)]
pub struct SeriesAccounting {
    /// The shared state; see the type comment for why one guard covers it all.
    state: Mutex<AccountingState>,
}

/// The single instance production workers and monitors share.
static PROCESS_ACCOUNTING: LazyLock<Arc<SeriesAccounting>> =
    LazyLock::new(|| Arc::new(SeriesAccounting::default()));

impl SeriesAccounting {
    /// The process-wide accounting every engine monitor and exporter worker uses.
    #[must_use]
    pub fn process() -> Arc<Self> {
        Arc::clone(&PROCESS_ACCOUNTING)
    }

    /// An accounting instance of its own, isolated from the process-wide one,
    /// so one test's workers and reporter ownership are invisible to others.
    #[must_use]
    pub fn isolated() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Takes the guard, tolerating poisoning from a panic elsewhere.
    ///
    /// The counters are plain numbers updated in one critical section, so a
    /// poisoned guard carries no torn value.
    fn lock(&self) -> MutexGuard<'_, AccountingState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One worker's contribution to process-wide series exporter memory accounting.
///
/// A worker holds this for its whole life, so the residual distinguishes a
/// worker that currently retains nothing from no worker at all. Dropping it
/// withdraws exactly this worker's bytes and its registration, once.
#[derive(Debug)]
pub struct SeriesMemoryAccounting {
    /// The accounting this worker is registered with.
    accounting: Arc<SeriesAccounting>,
    /// What this worker last published, so a later value can be applied as a delta.
    bytes: u64,
}

impl SeriesMemoryAccounting {
    /// Register one active exporter worker, including workers currently retaining zero bytes.
    #[must_use]
    pub fn register() -> Self {
        Self::register_with(SeriesAccounting::process())
    }

    /// Register one active exporter worker against a specific accounting instance.
    #[must_use]
    pub fn register_with(accounting: Arc<SeriesAccounting>) -> Self {
        accounting.lock().workers += 1;
        Self {
            accounting,
            bytes: 0,
        }
    }

    /// What this worker last published.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Replace this worker's accounted bytes without disturbing other workers.
    pub fn set(&mut self, bytes: u64) {
        let mut state = self.accounting.lock();
        state.accounted = state
            .accounted
            .saturating_sub(self.bytes)
            .saturating_add(bytes);
        drop(state);
        self.bytes = bytes;
    }
}

impl Drop for SeriesMemoryAccounting {
    fn drop(&mut self) {
        let mut state = self.accounting.lock();
        state.accounted = state.accounted.saturating_sub(self.bytes);
        state.workers = state.workers.saturating_sub(1);
    }
}

/// Process-scoped exporter residual; the engine monitor is its single reporter.
#[metric_set(name = "exporter.series_parquet")]
#[derive(Debug, Default, Clone)]
pub struct SeriesProcessMetrics {
    /// RSS minus the accounted sum, clamped to zero.
    #[metric(name = "memory.unaccounted_rss_bytes", unit = "By")]
    pub residual: Gauge<u64>,
}

/// Engine-wide metrics emitted once per engine instance.
#[metric_set(name = "engine")]
#[derive(Debug, Default, Clone)]
pub struct EngineMetrics {
    /// Process-wide Resident Set Size -- physical RAM currently used by the process.
    /// Matches what external tools report (e.g. `kubectl top pod`, `htop`, `ps rss`).
    #[metric(unit = "{By}")]
    pub memory_rss: ObserveUpDownCounter<u64>,

    /// Process-wide CPU utilization as a ratio in [0, 1], normalized across all
    /// logical CPU cores on the system (not just engine-assigned cores).
    /// Aligned with the OTel semantic convention `process.cpu.utilization`.
    ///
    /// The `cpu.mode` attribute is not set; this reports combined user + system time.
    #[metric(unit = "{1}")]
    pub cpu_utilization: Gauge<f64>,

    /// Process-wide memory limiter state encoded as `0=normal`, `1=soft`, `2=hard`.
    #[metric(unit = "{state}")]
    pub memory_pressure_state: Gauge<u64>,

    /// Most recent process-wide memory limiter sample, in bytes.
    #[metric(unit = "{By}")]
    pub process_memory_usage_bytes: Gauge<u64>,

    /// Effective process-wide memory limiter soft limit, in bytes.
    #[metric(unit = "{By}")]
    pub process_memory_soft_limit_bytes: Gauge<u64>,

    /// Effective process-wide memory limiter hard limit, in bytes.
    #[metric(unit = "{By}")]
    pub process_memory_hard_limit_bytes: Gauge<u64>,
}

/// Monitors and reports engine-wide metrics.
///
/// Created by the controller and driven by a periodic timer in a dedicated
/// background task. Call [`update`](Self::update) to sample current values
/// and [`report`](Self::report) to flush them to the metrics pipeline.
pub struct EngineMetricsMonitor {
    metrics: MetricSet<EngineMetrics>,
    reporter: MetricsReporter,
    registry: TelemetryRegistryHandle,
    /// Wall-clock anchor for the current measurement interval.
    wall_start: Instant,
    /// Process-wide CPU time anchor for the current measurement interval.
    cpu_start: ProcessTime,
    /// Total number of logical CPU cores available on the system.
    num_cores: usize,
    /// Shared process-wide memory limiter state.
    memory_pressure_state: MemoryPressureState,
    /// The process residual set, present only while a series worker is registered
    /// and this monitor is the one that owns the reporting of it.
    series: Option<MetricSet<SeriesProcessMetrics>>,
    /// The engine entity the residual is reported on; there is no worker entity here.
    series_entity: EntityKey,
    /// The accounting this monitor reads the worker count and accounted bytes from.
    accounting: Arc<SeriesAccounting>,
    /// Test hook run after the worker count has been checked and before the
    /// residual snapshot is materialized, inside the accounting guard. It
    /// receives the state the monitor is deciding on.
    #[cfg(test)]
    on_materialize: Option<Box<dyn FnMut(usize, u64)>>,
}

impl EngineMetricsMonitor {
    /// Creates a new engine metrics monitor.
    ///
    /// The caller must have already registered the engine entity via
    /// [`ControllerContext::register_engine_entity`](crate::context::ControllerContext::register_engine_entity).
    #[must_use]
    pub fn new(
        registry: TelemetryRegistryHandle,
        entity_key: EntityKey,
        reporter: MetricsReporter,
        memory_pressure_state: MemoryPressureState,
    ) -> Self {
        Self::with_accounting(
            registry,
            entity_key,
            reporter,
            memory_pressure_state,
            SeriesAccounting::process(),
        )
    }

    /// Creates a monitor reading a specific series accounting instance;
    /// [`new`](Self::new) passes [`SeriesAccounting::process`].
    #[must_use]
    pub fn with_accounting(
        registry: TelemetryRegistryHandle,
        entity_key: EntityKey,
        reporter: MetricsReporter,
        memory_pressure_state: MemoryPressureState,
        accounting: Arc<SeriesAccounting>,
    ) -> Self {
        let metrics = registry.register_metric_set_for_entity::<EngineMetrics>(entity_key);
        let num_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self {
            metrics,
            reporter,
            registry,
            wall_start: Instant::now(),
            cpu_start: ProcessTime::now(),
            num_cores,
            memory_pressure_state,
            series: None,
            series_entity: entity_key,
            accounting,
            #[cfg(test)]
            on_materialize: None,
        }
    }

    /// Brings the residual registration in line with the live worker count.
    ///
    /// Takes the fields rather than `&mut self` so a caller can hold the
    /// accounting guard, which borrows `self.accounting`, across the call.
    ///
    /// The residual is meaningless without a worker to be a residual of, so no
    /// metric set exists at zero workers. With at least one worker, the first
    /// monitor to claim `reporter_owned` registers and reports it; a second
    /// monitor leaves it alone, and picks it up on a later update if the owner
    /// is dropped.
    fn sync_locked(
        series: &mut Option<MetricSet<SeriesProcessMetrics>>,
        registry: &TelemetryRegistryHandle,
        entity: EntityKey,
        state: &mut AccountingState,
    ) {
        if state.workers == 0 {
            if let Some(set) = series.take() {
                let _ = registry.unregister_metric_set(set.metric_set_key());
                state.reporter_owned = false;
            }
        } else if series.is_none() && !state.reporter_owned {
            state.reporter_owned = true;
            *series = Some(registry.register_metric_set_for_entity::<SeriesProcessMetrics>(entity));
        }
    }

    /// Records the residual for `rss` against the current accounted total.
    ///
    /// The worker count and the accounted sum are read under the same guard
    /// that a registering or dropping worker takes, so the value recorded here
    /// always belongs to a worker state that really existed.
    fn observe_series_residual(&mut self, rss: u64) {
        let mut state = self.accounting.lock();
        Self::sync_locked(
            &mut self.series,
            &self.registry,
            self.series_entity,
            &mut state,
        );
        if let Some(series) = &mut self.series {
            series.residual.set(rss.saturating_sub(state.accounted));
        }
    }

    /// Materializes the residual snapshot, if one is due, under the guard.
    ///
    /// The registration check and the snapshot happen in one critical section,
    /// so the last worker cannot drop between them and leave a residual to be
    /// emitted for a process that no longer has a series exporter. The
    /// returned snapshot is sent by the caller after the guard is released,
    /// which is what keeps the caller free to await.
    fn take_series_snapshot(&mut self) -> Option<MetricSetSnapshot> {
        let mut state = self.accounting.lock();
        Self::sync_locked(
            &mut self.series,
            &self.registry,
            self.series_entity,
            &mut state,
        );
        #[cfg(test)]
        if let Some(hook) = &mut self.on_materialize {
            hook(state.workers, state.accounted);
        }
        let series = self.series.as_ref()?;
        series.needs_flush().then(|| series.snapshot())
    }

    /// Clears the residual after its snapshot was accepted.
    ///
    /// A deferred snapshot leaves the values in place so the next report
    /// retries it, matching how the reporter treats every other metric set.
    fn clear_series_after(&mut self, outcome: ReportOutcome) {
        if matches!(outcome, ReportOutcome::Sent)
            && let Some(series) = &mut self.series
        {
            series.clear_values();
        }
    }

    /// Samples current engine-wide metrics (RSS, CPU utilization, etc.).
    pub fn update(&mut self) {
        // One RSS read per monitor update, shared by the engine gauge and the
        // series residual: an exporter must never cost the process its own
        // `/proc` sample.
        let rss = get_rss_bytes();
        self.metrics.memory_rss.observe(rss);
        self.observe_series_residual(rss);

        // Compute process-wide CPU utilization normalized across all cores.
        let now_wall = Instant::now();
        let now_cpu = ProcessTime::now();
        let wall_delta = now_wall.duration_since(self.wall_start);
        let cpu_delta = now_cpu.duration_since(self.cpu_start);
        let wall_secs = wall_delta.as_secs_f64();
        if wall_secs > 0.0 {
            let utilization =
                (cpu_delta.as_secs_f64() / (wall_secs * self.num_cores as f64)).clamp(0.0, 1.0);
            self.metrics.cpu_utilization.set(utilization);
        } else {
            self.metrics.cpu_utilization.set(0.0);
        }
        self.metrics
            .memory_pressure_state
            .set(self.memory_pressure_state.level() as u64);
        self.metrics
            .process_memory_usage_bytes
            .set(self.memory_pressure_state.usage_bytes());
        self.metrics
            .process_memory_soft_limit_bytes
            .set(self.memory_pressure_state.soft_limit_bytes());
        self.metrics
            .process_memory_hard_limit_bytes
            .set(self.memory_pressure_state.hard_limit_bytes());
        self.wall_start = now_wall;
        self.cpu_start = now_cpu;
    }

    /// Flushes sampled metrics to the reporting pipeline.
    ///
    /// Returns an error only if the metrics channel is permanently closed.
    /// A full channel is silently tolerated (non-blocking, try-send semantics).
    pub fn report(&mut self) -> Result<(), otel_arrow_dfe_telemetry::error::Error> {
        // Rechecked here, not just in `update`: the worker state is re-read and
        // the snapshot materialized in one critical section, so the last worker
        // leaving never has its residual reported after it is gone.
        if let Some(snapshot) = self.take_series_snapshot() {
            let outcome = self.reporter.try_report_snapshot_with_outcome(snapshot)?;
            self.clear_series_after(outcome);
        }
        self.reporter.report(&mut self.metrics)
    }

    /// Samples and reliably hands off final values without exceeding `deadline`.
    ///
    /// The collector barrier completes before this monitor unregisters its
    /// metric-set key, preventing an accepted terminal snapshot from arriving
    /// after the registry entry has been removed.
    pub async fn finish_reporting_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), otel_arrow_dfe_telemetry::error::Error> {
        self.update();
        // Materialized under the accounting guard and sent after it is
        // released, because this send waits on downstream capacity and a guard
        // must never be held across an await.
        if let Some(snapshot) = self.take_series_snapshot() {
            let outcome = self
                .reporter
                .report_snapshot_reliably_until(snapshot, deadline)
                .await?;
            self.clear_series_after(outcome);
        }
        let _ = self
            .reporter
            .report_reliably_until(&mut self.metrics, deadline)
            .await?;
        self.reporter.flush_until(deadline).await
    }
}

/// Returns the current process-wide RSS (Resident Set Size) in bytes.
fn get_rss_bytes() -> u64 {
    memory_stats::memory_stats()
        .map(|stats| stats.physical_mem as u64)
        .unwrap_or(0)
}

impl Drop for EngineMetricsMonitor {
    fn drop(&mut self) {
        if let Some(series) = self.series.take() {
            let _ = self.registry.unregister_metric_set(series.metric_set_key());
            self.accounting.lock().reporter_owned = false;
        }
        let _ = self
            .registry
            .unregister_metric_set(self.metrics.metric_set_key());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ControllerContext;
    use otel_arrow_dfe_telemetry::metrics::MetricValue;
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A monitor over accounting nothing else in this binary can see; see
    /// `SeriesAccounting::isolated`.
    struct Harness {
        monitor: EngineMetricsMonitor,
        receiver: flume::Receiver<MetricSetSnapshot>,
        accounting: Arc<SeriesAccounting>,
    }

    fn harness() -> Harness {
        harness_on(SeriesAccounting::isolated())
    }

    fn harness_on(accounting: Arc<SeriesAccounting>) -> Harness {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let entity_key = controller.register_engine_entity();
        let (receiver, reporter) = MetricsReporter::create_new_and_receiver(16);
        let monitor = EngineMetricsMonitor::with_accounting(
            registry,
            entity_key,
            reporter,
            controller.memory_pressure_state(),
            Arc::clone(&accounting),
        );
        Harness {
            monitor,
            receiver,
            accounting,
        }
    }

    /// The residual values in everything the receiver holds, oldest first.
    fn residuals(receiver: &flume::Receiver<MetricSetSnapshot>) -> Vec<u64> {
        receiver
            .drain()
            .filter(|snapshot| snapshot.descriptor().name == "exporter.series_parquet")
            .map(|snapshot| match snapshot.get_metrics() {
                [MetricValue::U64(value)] => *value,
                other => panic!("residual snapshot should hold one u64, got {other:?}"),
            })
            .collect()
    }

    /// Scenario: the engine monitor samples the process once.
    /// Guarantees: `memory_rss` carries the real process RSS rather than zero.
    #[test]
    fn engine_metrics_reports_nonzero_rss() {
        let mut harness = harness();

        // `memory_stats` reads /proc, which can fail transiently on a loaded
        // machine and then reports zero, so a few reads are allowed.
        let mut rss = 0;
        for _ in 0..5 {
            harness.monitor.update();
            rss = harness.monitor.metrics.memory_rss.get();
            if rss > 0 {
                break;
            }
        }

        assert!(rss > 0, "memory_rss should report non-zero process RSS");
    }

    /// Scenario: a sampled monitor flushes to an open reporting channel.
    /// Guarantees: reporting succeeds and does not error on the engine set.
    #[test]
    fn engine_metrics_report_succeeds() {
        let mut harness = harness();
        harness.monitor.update();
        assert!(harness.monitor.report().is_ok());
    }

    /// Scenario: the process burns CPU briefly and the monitor samples it.
    /// Guarantees: `cpu_utilization` stays a ratio inside [0, 1].
    #[test]
    fn engine_metrics_cpu_utilization_in_range() {
        let mut harness = harness();

        // Do a small busy-spin so there is measurable CPU time.
        let start = Instant::now();
        while start.elapsed() < std::time::Duration::from_millis(10) {
            let _ = std::hint::black_box(0u64.wrapping_add(1));
        }

        harness.monitor.update();
        let util = harness.monitor.metrics.cpu_utilization.get();
        assert!(
            (0.0..=1.0).contains(&util),
            "cpu_utilization should be in [0, 1], got {util}"
        );
    }

    /// Scenario: the memory limiter holds a configured sample and limits.
    /// Guarantees: the monitor republishes that sample and both limits verbatim.
    #[test]
    fn engine_metrics_expose_process_memory_limiter_usage_and_limits() {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let state = controller.memory_pressure_state();
        state.configure(crate::memory_limiter::MemoryPressureBehaviorConfig {
            retry_after_secs: 1,
            fail_readiness_on_hard: true,
            mode: otel_arrow_dfe_config::policy::MemoryLimiterMode::Enforce,
        });
        state.set_sample_for_tests(
            crate::memory_limiter::MemoryPressureLevel::Soft,
            95,
            90,
            100,
        );

        let entity_key = controller.register_engine_entity();
        let (_rx, reporter) = MetricsReporter::create_new_and_receiver(16);
        let mut monitor = EngineMetricsMonitor::with_accounting(
            registry,
            entity_key,
            reporter,
            state,
            SeriesAccounting::isolated(),
        );

        monitor.update();

        assert_eq!(monitor.metrics.memory_pressure_state.get(), 1);
        assert_eq!(monitor.metrics.process_memory_usage_bytes.get(), 95);
        assert_eq!(monitor.metrics.process_memory_soft_limit_bytes.get(), 90);
        assert_eq!(monitor.metrics.process_memory_hard_limit_bytes.get(), 100);
    }

    /// Scenario: two exporter workers account memory and one exits.
    /// Guarantees: the process total removes the exited worker and never subtracts another worker twice.
    #[test]
    fn series_accounting_releases_each_worker_once() {
        let accounting = SeriesAccounting::isolated();
        let accounted = || accounting.lock().accounted;
        let mut a = SeriesMemoryAccounting::register_with(Arc::clone(&accounting));
        let mut b = SeriesMemoryAccounting::register_with(Arc::clone(&accounting));
        a.set(128);
        b.set(256);
        a.set(192);
        assert_eq!(accounted(), 448);
        drop(a);
        assert_eq!(accounted(), 256);
        b.set(0);
        assert_eq!(accounted(), 0);
        drop(b);
        assert_eq!(accounted(), 0);
        assert_eq!(accounting.lock().workers, 0);
    }

    /// Scenario: a process has no series workers, then two, then no workers again.
    /// Guarantees: only active workers expose residual telemetry and residual reuses the engine RSS sample.
    #[test]
    fn series_accounting_controls_process_metric_presence() {
        let mut harness = harness();
        harness.monitor.update();
        assert!(harness.monitor.series.is_none());
        let mut a = SeriesMemoryAccounting::register_with(Arc::clone(&harness.accounting));
        let b = SeriesMemoryAccounting::register_with(Arc::clone(&harness.accounting));
        a.set(128);
        harness.monitor.update();
        assert_eq!(
            harness
                .monitor
                .series
                .as_ref()
                .expect("registered")
                .residual
                .get(),
            harness.monitor.metrics.memory_rss.get().saturating_sub(128)
        );
        drop(a);
        harness.monitor.update();
        assert!(
            harness.monitor.series.is_some(),
            "zero-byte worker is still registered"
        );
        drop(b);
        harness.monitor.update();
        assert!(harness.monitor.series.is_none());
    }

    /// Scenario: the monitor materializes a residual snapshot with one worker
    /// registered, and that worker drops before the next report.
    /// Guarantees: the worker state the snapshot is built from is read under
    /// the accounting guard, and once the last worker is gone no further
    /// residual is emitted.
    #[test]
    fn series_residual_is_materialized_under_the_accounting_guard() {
        let mut harness = harness();
        let mut worker = SeriesMemoryAccounting::register_with(Arc::clone(&harness.accounting));
        worker.set(128);

        // Record what the monitor sees at the instant it decides to snapshot.
        // Nothing can register or drop between this observation and the
        // snapshot, because both happen inside one critical section.
        let observed = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&observed);
        harness.monitor.on_materialize = Some(Box::new(move |workers, accounted| {
            recorder
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((workers, accounted));
        }));

        harness.monitor.update();
        let rss = harness.monitor.metrics.memory_rss.get();
        harness.monitor.report().expect("report");

        assert_eq!(
            *observed.lock().unwrap_or_else(PoisonError::into_inner),
            vec![(1, 128)],
            "the snapshot decision reads the guarded worker state"
        );
        assert_eq!(residuals(&harness.receiver), vec![rss.saturating_sub(128)]);

        // The last worker leaves, so nothing further may be emitted.
        drop(worker);
        harness.monitor.update();
        harness.monitor.report().expect("report");
        assert!(harness.monitor.series.is_none());
        assert_eq!(
            *observed.lock().unwrap_or_else(PoisonError::into_inner),
            vec![(1, 128), (0, 0)],
            "the second decision sees no workers"
        );
        assert!(
            residuals(&harness.receiver).is_empty(),
            "no residual is emitted once the last worker has dropped"
        );
    }

    /// Scenario: the last worker drops between a monitor's update and its report.
    /// Guarantees: the report emits no residual, because the registration is
    /// rechecked in the same critical section that materializes the snapshot.
    #[test]
    fn series_residual_is_not_reported_after_a_drop_between_update_and_report() {
        let mut harness = harness();
        let mut worker = SeriesMemoryAccounting::register_with(Arc::clone(&harness.accounting));
        worker.set(128);
        harness.monitor.update();
        assert!(harness.monitor.series.is_some());

        drop(worker);
        harness.monitor.report().expect("report");

        assert!(harness.monitor.series.is_none());
        assert!(
            residuals(&harness.receiver).is_empty(),
            "a residual sampled before the drop must not be emitted after it"
        );
    }

    /// Scenario: workers register and drop on another thread while a monitor
    /// updates and reports in a loop.
    /// Guarantees: the accounting never underflows, and once every worker has
    /// gone the residual is unregistered and nothing more is emitted.
    #[test]
    fn series_accounting_survives_concurrent_registration_and_reporting() {
        let mut harness = harness();
        let accounting = Arc::clone(&harness.accounting);
        let churn = std::thread::spawn(move || {
            for round in 0..200u64 {
                let mut worker = SeriesMemoryAccounting::register_with(Arc::clone(&accounting));
                worker.set(round * 8);
                let mut second = SeriesMemoryAccounting::register_with(Arc::clone(&accounting));
                second.set(16);
                drop(worker);
            }
        });
        for _ in 0..200 {
            harness.monitor.update();
            harness.monitor.report().expect("report");
        }
        churn.join().expect("churn thread");

        let state = harness.accounting.lock();
        assert_eq!(state.workers, 0);
        assert_eq!(state.accounted, 0);
        drop(state);

        let _ = residuals(&harness.receiver);
        harness.monitor.update();
        harness.monitor.report().expect("report");
        assert!(harness.monitor.series.is_none());
        assert!(
            residuals(&harness.receiver).is_empty(),
            "no residual survives the last worker"
        );
    }

    /// Scenario: the last series worker is dropped from another thread at the
    /// exact instant the monitor has checked the worker count and is about to
    /// materialize the residual snapshot.
    /// Guarantees: that whole decision is one critical section, so the drop
    /// cannot land inside it; the residual that is emitted was decided with
    /// the worker still counted, the drop completes once the monitor releases
    /// the guard, and the next report emits nothing.
    #[test]
    fn a_worker_drop_cannot_interleave_with_the_residual_decision() {
        let mut harness = harness();
        let mut worker = SeriesMemoryAccounting::register_with(Arc::clone(&harness.accounting));
        worker.set(128);
        harness.monitor.update();
        let rss = harness.monitor.metrics.memory_rss.get();

        // What the monitor saw when it decided, and whether a drop could have
        // proceeded at that instant.
        let counted = Arc::new(AtomicUsize::new(usize::MAX));
        let drop_could_proceed = Arc::new(AtomicBool::new(true));
        // The dropper thread, parked until the monitor lets go of the guard.
        let dropper = Arc::new(Mutex::new(None));

        let accounting = Arc::clone(&harness.accounting);
        let observed = Arc::clone(&counted);
        let probed = Arc::clone(&drop_could_proceed);
        let handle = Arc::clone(&dropper);
        let mut slot = Some(worker);
        harness.monitor.on_materialize = Some(Box::new(move |workers, _accounted| {
            observed.store(workers, Ordering::SeqCst);
            let Some(worker) = slot.take() else {
                return;
            };
            let accounting = Arc::clone(&accounting);
            let (probe_tx, probe_rx) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                // Report whether a worker could take the accounting state right
                // now, then actually drop, which parks on the guard until the
                // monitor is done deciding.
                probe_tx
                    .send(accounting.state.try_lock().is_ok())
                    .expect("probe result");
                drop(worker);
            });
            // Blocking receive, so the probe is known to have run while the
            // monitor is mid-decision. No sleeping and no timing assumption.
            probed.store(probe_rx.recv().expect("probe result"), Ordering::SeqCst);
            *handle.lock().unwrap_or_else(PoisonError::into_inner) = Some(thread);
        }));

        harness.monitor.report().expect("report");

        assert_eq!(
            counted.load(Ordering::SeqCst),
            1,
            "the decision counted the still-registered worker"
        );
        assert!(
            !drop_could_proceed.load(Ordering::SeqCst),
            "a worker drop must not be able to land between the worker count \
             being checked and the snapshot being materialized"
        );
        assert_eq!(
            residuals(&harness.receiver),
            vec![rss.saturating_sub(128)],
            "the emitted residual is the one decided with the worker counted"
        );

        // The guard is released now, so the parked drop completes.
        dropper
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .expect("dropper thread")
            .join()
            .expect("dropper thread");
        assert_eq!(harness.accounting.lock().workers, 0);

        harness.monitor.report().expect("report");
        assert!(harness.monitor.series.is_none());
        assert!(
            residuals(&harness.receiver).is_empty(),
            "no residual is emitted once the drop has landed"
        );
    }

    /// Scenario: two monitors share one accounting instance while a worker is registered.
    /// Guarantees: exactly one of them reports the residual, and the other takes
    /// it over once the owner is dropped.
    #[test]
    fn series_residual_has_one_reporter_and_is_taken_over_on_drop() {
        let accounting = SeriesAccounting::isolated();
        let mut first = harness_on(Arc::clone(&accounting));
        let mut second = harness_on(Arc::clone(&accounting));
        let worker = SeriesMemoryAccounting::register_with(Arc::clone(&accounting));

        first.monitor.update();
        second.monitor.update();
        assert!(first.monitor.series.is_some(), "the first monitor owns it");
        assert!(
            second.monitor.series.is_none(),
            "a second monitor does not report the same process residual"
        );

        first.monitor.report().expect("report");
        second.monitor.report().expect("report");
        assert_eq!(residuals(&first.receiver).len(), 1);
        assert!(residuals(&second.receiver).is_empty());

        // The owner goes away; the survivor picks the residual up.
        drop(first);
        assert!(!accounting.lock().reporter_owned);
        second.monitor.update();
        assert!(second.monitor.series.is_some(), "ownership was taken over");
        second.monitor.report().expect("report");
        assert_eq!(residuals(&second.receiver).len(), 1);

        drop(worker);
        second.monitor.update();
        assert!(second.monitor.series.is_none());
        assert!(!accounting.lock().reporter_owned);
    }
}
