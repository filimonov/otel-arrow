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
use otel_arrow_dfe_telemetry::metrics::MetricSet;
use otel_arrow_dfe_telemetry::registry::{EntityKey, TelemetryRegistryHandle};
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::metric_set;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Number of series exporter workers currently registered in this process.
static SERIES_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// Sum of the bytes those workers have most recently accounted for.
static SERIES_ACCOUNTED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Whether some engine monitor already owns the process residual metric.
///
/// A process may run more than one engine monitor; only one of them registers
/// and reports the residual, because the residual describes the process, not
/// the engine.
static SERIES_REPORTER_OWNED: AtomicBool = AtomicBool::new(false);

/// One worker's contribution to process-wide series exporter memory accounting.
///
/// A worker holds this for its whole life, so the residual distinguishes a
/// worker that currently retains nothing from no worker at all. Dropping it
/// withdraws exactly this worker's bytes and its registration, once.
#[derive(Debug)]
pub struct SeriesMemoryAccounting {
    /// What this worker last published, so a later value can be applied as a delta.
    bytes: u64,
}

impl SeriesMemoryAccounting {
    /// Register one active exporter worker, including workers currently retaining zero bytes.
    #[must_use]
    pub fn register() -> Self {
        let _ = SERIES_WORKERS.fetch_add(1, Ordering::AcqRel);
        Self { bytes: 0 }
    }

    /// What this worker last published.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Replace this worker's accounted bytes without disturbing other workers.
    pub fn set(&mut self, bytes: u64) {
        if bytes >= self.bytes {
            let _ = SERIES_ACCOUNTED_BYTES.fetch_add(bytes - self.bytes, Ordering::Relaxed);
        } else {
            let _ = SERIES_ACCOUNTED_BYTES.fetch_sub(self.bytes - bytes, Ordering::Relaxed);
        }
        self.bytes = bytes;
    }
}

impl Drop for SeriesMemoryAccounting {
    fn drop(&mut self) {
        self.set(0);
        let _ = SERIES_WORKERS.fetch_sub(1, Ordering::AcqRel);
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
        }
    }

    /// Registers or unregisters the process residual to match the live worker count.
    ///
    /// The residual is meaningless without a worker to be a residual of, so no
    /// metric set exists at zero workers. At least one worker, the first
    /// monitor to win `SERIES_REPORTER_OWNED` reports it; a second monitor
    /// leaves it alone, and can pick it up on a later update if the first
    /// monitor is dropped.
    fn sync_series_registration(&mut self) {
        if SERIES_WORKERS.load(Ordering::Acquire) == 0 {
            if let Some(series) = self.series.take() {
                let _ = self.registry.unregister_metric_set(series.metric_set_key());
                SERIES_REPORTER_OWNED.store(false, Ordering::Release);
            }
        } else if self.series.is_none()
            && SERIES_REPORTER_OWNED
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.series = Some(
                self.registry
                    .register_metric_set_for_entity::<SeriesProcessMetrics>(self.series_entity),
            );
        }
    }

    /// Samples current engine-wide metrics (RSS, CPU utilization, etc.).
    pub fn update(&mut self) {
        // One RSS read per monitor update, shared by the engine gauge and the
        // series residual: an exporter must never cost the process its own
        // `/proc` sample.
        let rss = get_rss_bytes();
        self.metrics.memory_rss.observe(rss);
        self.sync_series_registration();
        if let Some(series) = &mut self.series {
            series
                .residual
                .set(rss.saturating_sub(SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed)));
        }

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
        // Rechecked here, not just in `update`, so the last worker leaving
        // between the two never has its residual reported after it is gone.
        self.sync_series_registration();
        if let Some(series) = &mut self.series {
            self.reporter.report(series)?;
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
        if let Some(series) = &mut self.series {
            let _ = self
                .reporter
                .report_reliably_until(series, deadline)
                .await?;
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
            SERIES_REPORTER_OWNED.store(false, Ordering::Release);
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
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

    #[test]
    fn engine_metrics_reports_nonzero_rss() {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let entity_key = controller.register_engine_entity();
        let (_rx, reporter) = MetricsReporter::create_new_and_receiver(16);

        let mut monitor = EngineMetricsMonitor::new(
            registry,
            entity_key,
            reporter,
            controller.memory_pressure_state(),
        );
        // `memory_stats` reads /proc, which can fail transiently under a loaded
        // machine and then reports zero. Retrying a few times keeps the
        // guarantee -- the monitor does report real RSS -- without failing the
        // suite on one unlucky read.
        let mut rss = 0;
        for _ in 0..5 {
            monitor.update();
            rss = monitor.metrics.memory_rss.get();
            if rss > 0 {
                break;
            }
        }

        assert!(rss > 0, "memory_rss should report non-zero process RSS");
    }

    #[test]
    fn engine_metrics_report_succeeds() {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let entity_key = controller.register_engine_entity();
        let (_rx, reporter) = MetricsReporter::create_new_and_receiver(16);

        let mut monitor = EngineMetricsMonitor::new(
            registry,
            entity_key,
            reporter,
            controller.memory_pressure_state(),
        );
        monitor.update();
        assert!(monitor.report().is_ok());
    }

    #[test]
    fn engine_metrics_cpu_utilization_in_range() {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let entity_key = controller.register_engine_entity();
        let (_rx, reporter) = MetricsReporter::create_new_and_receiver(16);

        let mut monitor = EngineMetricsMonitor::new(
            registry,
            entity_key,
            reporter,
            controller.memory_pressure_state(),
        );

        // Do a small busy-spin so there is measurable CPU time.
        let start = Instant::now();
        while start.elapsed() < std::time::Duration::from_millis(10) {
            let _ = std::hint::black_box(0u64.wrapping_add(1));
        }

        monitor.update();
        let util = monitor.metrics.cpu_utilization.get();
        assert!(
            (0.0..=1.0).contains(&util),
            "cpu_utilization should be in [0, 1], got {util}"
        );
    }

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
        let mut monitor = EngineMetricsMonitor::new(registry, entity_key, reporter, state);

        monitor.update();

        assert_eq!(monitor.metrics.memory_pressure_state.get(), 1);
        assert_eq!(monitor.metrics.process_memory_usage_bytes.get(), 95);
        assert_eq!(monitor.metrics.process_memory_soft_limit_bytes.get(), 90);
        assert_eq!(monitor.metrics.process_memory_hard_limit_bytes.get(), 100);
    }

    /// Serializes the process-global accounting tests.
    ///
    /// Both of them assert on process-wide counters, so the workspace suite
    /// running them on two threads at once would make each one observe the
    /// other's registrations.
    static SERIES_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Takes the shared lock, tolerating poisoning from an earlier failure.
    ///
    /// A failing assertion unwinds through the registration guards, which
    /// restore the counters on the way out, so the next test still starts from
    /// a consistent process state and should report its own failure rather
    /// than a poisoned lock.
    fn series_test_lock() -> std::sync::MutexGuard<'static, ()> {
        SERIES_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Scenario: two exporter workers account memory and one exits.
    /// Guarantees: the process total removes the exited worker and never subtracts another worker twice.
    #[test]
    fn series_accounting_releases_each_worker_once() {
        let _guard = series_test_lock();
        let baseline = SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed);
        let mut a = SeriesMemoryAccounting::register();
        let mut b = SeriesMemoryAccounting::register();
        a.set(128);
        b.set(256);
        a.set(192);
        assert_eq!(
            SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed),
            baseline + 448
        );
        drop(a);
        assert_eq!(
            SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed),
            baseline + 256
        );
        b.set(0);
        assert_eq!(SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed), baseline);
        drop(b);
        assert_eq!(SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed), baseline);
    }

    /// Scenario: a process has no series workers, then two, then no workers again.
    /// Guarantees: only active workers expose residual telemetry and residual reuses the engine RSS sample.
    #[test]
    fn series_accounting_controls_process_metric_presence() {
        let _guard = series_test_lock();
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let entity = controller.register_engine_entity();
        let (_rx, reporter) = MetricsReporter::create_new_and_receiver(16);
        let mut monitor = EngineMetricsMonitor::new(
            registry,
            entity,
            reporter,
            controller.memory_pressure_state(),
        );
        monitor.update();
        assert!(monitor.series.is_none());
        let mut a = SeriesMemoryAccounting::register();
        let b = SeriesMemoryAccounting::register();
        a.set(128);
        monitor.update();
        assert_eq!(
            monitor.series.as_ref().expect("registered").residual.get(),
            monitor.metrics.memory_rss.get().saturating_sub(128)
        );
        drop(a);
        monitor.update();
        assert!(
            monitor.series.is_some(),
            "zero-byte worker is still registered"
        );
        drop(b);
        monitor.update();
        assert!(monitor.series.is_none());
    }
}
