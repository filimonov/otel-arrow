// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Hooks that let tests of dependent crates stall Quiver's blocking paths.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

type Gate = Arc<(Mutex<bool>, Condvar)>;

static WAL_DROP_STALLS: Mutex<Vec<(PathBuf, Gate)>> = Mutex::new(Vec::new());

/// Holds the drop of every WAL writer whose file lies under a directory,
/// as a wedged disk would hold its drop-time sync, until released.
pub struct WalDropStall {
    dir: PathBuf,
    gate: Gate,
}

impl WalDropStall {
    /// Stalls the drop of WAL writers under `dir` from now on.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let gate: Gate = Arc::new((Mutex::new(false), Condvar::new()));
        WAL_DROP_STALLS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((dir.clone(), Arc::clone(&gate)));
        Self { dir, gate }
    }

    /// Lets every stalled and every later drop under the directory finish.
    pub fn release(&self) {
        let (released, wake) = &*self.gate;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_all();
    }
}

impl Drop for WalDropStall {
    fn drop(&mut self) {
        self.release();
        WAL_DROP_STALLS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(dir, _)| dir != &self.dir);
    }
}

/// Blocks while a [`WalDropStall`] holds a directory containing `path`.
pub(crate) fn before_wal_drop(path: &Path) {
    let gate = WAL_DROP_STALLS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .find(|(dir, _)| path.starts_with(dir))
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        let (released, wake) = &*gate;
        let mut released = released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*released {
            released = wake
                .wait(released)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}
