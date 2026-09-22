// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Aligned wall-clock windows driven by one persistent monotonic sleep.
//!
//! Rotation is aligned to wall-clock time -- a 15 s window starts at
//! `:00`, `:15`, `:30` and `:45` -- because the window start is written into
//! the file layout and two writers of the same lake must agree on it. The
//! waiting, though, is done on the engine's monotonic clock: a wall clock can
//! step, and a sleep armed against it could either fire early in a storm or
//! not at all. So the next boundary is computed in wall time, converted once
//! into a monotonic delay, and slept on with [`clock::sleep_until`].
//!
//! The sleep is a plain future owned by the worker rather than an engine
//! periodic timer, because the engine cancels periodic timers before it drains
//! a node's receivers: a timer-driven rotation would stop exactly when the
//! backlog still has to be written. It is also persistent: it is re-armed on
//! every wake, including a wake that produced no rotation, so the node never
//! polls an already-elapsed sleep in a loop and never loses its timer.
//!
//! [`WindowClock`] owns the arithmetic and the guarantees that go with it. A
//! boundary that has been consumed is never consumed again, so a wall clock
//! that steps backwards cannot reopen a window that was already flushed, and
//! several boundaries missed while the node was busy coalesce into a single
//! rotation at the latest one.

use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake::clock::{WakeOutcome, WallClock, WindowClock, nanos_to_secs};
use std::sync::Arc;
use std::time::Duration;

/// The window boundary the worker is currently waiting for.
pub(super) struct Window {
    /// Source of wall-clock time; the boundaries are aligned to it.
    pub(super) wall: Arc<dyn WallClock>,
    /// Boundary arithmetic and the last boundary that was consumed.
    pub(super) clock: WindowClock,
    /// Monotonic sleep that expires at the next boundary.
    pub(super) sleep: clock::Sleep,
}

impl Window {
    /// Start a window aligned to the wall clock, with the next boundary armed.
    pub(super) fn new(interval: Duration, wall: Arc<dyn WallClock>) -> Self {
        let nanos = wall.now_unix_nanos();
        let clock = WindowClock::new(interval, nanos_to_secs(nanos));
        let sleep = Self::arm(clock.next_boundary(nanos_to_secs(nanos)), nanos);
        Self { wall, clock, sleep }
    }

    /// A monotonic sleep expiring when the wall clock reaches `target_secs`.
    ///
    /// The distance is measured in wall time and then handed to the monotonic
    /// clock, so a wall-clock step is absorbed by the wake that follows it
    /// rather than by an arbitrarily long or negative sleep. A target that is
    /// already in the past becomes a zero delay; callers only ever arm a
    /// target strictly ahead of `now_nanos`, so this does not spin.
    fn arm(target_secs: i64, now_nanos: i64) -> clock::Sleep {
        let delay = (i128::from(target_secs) * 1_000_000_000 - i128::from(now_nanos)).max(0);
        let nanos = u64::try_from(delay).unwrap_or(u64::MAX);
        clock::sleep_until(clock::now() + Duration::from_nanos(nanos))
    }

    /// Consume an expired sleep, re-arm it, and say whether to rotate.
    ///
    /// Re-arming is unconditional: a wake that crossed no boundary -- the wall
    /// clock stepped backwards, or the sleep was woken early -- still leaves a
    /// sleep armed for the boundary that is still owed. Nothing here depends
    /// on the rotation actually happening, so a rotation blocked by an
    /// outstanding flush keeps the window timer running.
    pub(super) fn wake(&mut self) -> bool {
        let nanos = self.wall.now_unix_nanos();
        let now = nanos_to_secs(nanos);
        let (rotate, target) = match self.clock.on_wake(now) {
            WakeOutcome::RotationRequested { .. } => (true, self.clock.next_boundary(now)),
            WakeOutcome::TooEarly { sleep_until } => (false, sleep_until),
        };
        self.sleep = Self::arm(target, nanos);
        rotate
    }

    /// The window a request extracted at `admission_secs` belongs to.
    ///
    /// A request that arrives past the boundary the node is waiting for
    /// settles that boundary itself rather than waiting for the sleep: the
    /// boundary is consumed here and the sleep is re-armed for the next one.
    /// Without that, the request would be assigned to a window the timer would
    /// then fire for a second time.
    ///
    /// The result never moves backwards, because `effective_boundary` is
    /// floored by the last consumed boundary. That is what keeps a wall clock
    /// that steps back from producing a block for a window that has already
    /// been written, and what keeps a parked request's window reachable by the
    /// block opened for it.
    pub(super) fn admission_boundary(&mut self, admission_secs: i64) -> i64 {
        let boundary = self.clock.effective_boundary(admission_secs);
        if boundary > self.clock.last_boundary() {
            let _ = self.clock.on_wake(admission_secs);
            let nanos = self.wall.now_unix_nanos();
            self.sleep = Self::arm(self.clock.next_boundary(nanos_to_secs(nanos)), nanos);
        }
        boundary
    }
}
