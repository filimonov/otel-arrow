// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The global allocator of the bench executables: jemalloc configured as the
//! engine's `src/main.rs` configures it, or DHAT's in the `bench-heap` build.

#[cfg(feature = "bench-heap")]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

#[cfg(all(not(windows), not(feature = "bench-heap")))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc's compiled-in options, the engine's: its background purging thread.
///
/// The definition and its platform guard mirror the engine's `malloc_conf` in
/// `src/main.rs`, where the reason for the thread is documented.
#[cfg(all(target_os = "linux", target_env = "gnu", not(feature = "bench-heap")))]
#[allow(non_upper_case_globals, unsafe_code)]
#[unsafe(export_name = "malloc_conf")]
static malloc_conf: &[u8; 23] = b"background_thread:true\0";

/// The installed allocator as the harness fingerprints it.
///
/// The background thread is read back from jemalloc, so a build whose
/// compiled-in option did not take effect names itself `jemalloc`.
pub fn name() -> &'static str {
    #[cfg(feature = "bench-heap")]
    {
        "dhat"
    }
    #[cfg(all(not(windows), not(feature = "bench-heap")))]
    {
        if matches!(tikv_jemalloc_ctl::opt::background_thread::read(), Ok(true)) {
            "jemalloc+background_thread"
        } else {
            "jemalloc"
        }
    }
    #[cfg(all(windows, not(feature = "bench-heap")))]
    {
        "system"
    }
}
