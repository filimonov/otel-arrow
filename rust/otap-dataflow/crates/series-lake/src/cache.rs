// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded LRU of series ids to the partition their descriptor was last
//! committed to. Losing entries only causes descriptor re-emission
//! (spec invariant 3).

use std::num::NonZeroUsize;

use lru::LruCache;

use crate::canonical::SeriesId;
use crate::clock::PartitionId;

/// Cache counters since creation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    /// Lookups that found the id with the requested partition.
    pub hits: u64,
    /// Lookups that did not.
    pub misses: u64,
    /// Entries evicted by capacity.
    pub evictions: u64,
}

/// The series cache.
pub struct SeriesCache {
    inner: LruCache<SeriesId, Option<PartitionId>>,
    stats: CacheStats,
}

impl SeriesCache {
    /// Create a cache holding at most `max_entries` ids (at least 1).
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).expect("max(1) is non-zero");
        Self {
            inner: LruCache::new(cap),
            stats: CacheStats::default(),
        }
    }

    /// Whether the descriptor is known to be committed in `partition`. Touches the entry.
    pub fn is_committed(&mut self, id: &SeriesId, partition: PartitionId) -> bool {
        let hit = self.inner.get(id).is_some_and(|p| *p == Some(partition));
        if hit {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        hit
    }

    /// Insert the id if absent (no committed partition) and mark it most recently used.
    pub fn touch(&mut self, id: SeriesId) {
        if self.inner.get(&id).is_none() {
            self.insert(id, None);
        }
    }

    /// Record that the descriptor was committed in `partition`.
    pub fn mark_committed(&mut self, id: SeriesId, partition: PartitionId) {
        self.insert(id, Some(partition));
    }

    fn insert(&mut self, id: SeriesId, p: Option<PartitionId>) {
        if self.inner.len() == self.inner.cap().get() && !self.inner.contains(&id) {
            self.stats.evictions += 1;
        }
        let _ = self.inner.put(id, p);
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::PartitionId;

    fn id(n: u8) -> SeriesId {
        [n; 16]
    }

    /// Scenario: a series is touched, then committed for hour P, then asked about P and Q.
    /// Guarantees: committed only for the partition it was marked with.
    #[test]
    fn committed_partition_tracking() {
        let p = PartitionId {
            date: 20_000,
            hour: 3,
        };
        let q = PartitionId {
            date: 20_000,
            hour: 4,
        };
        let mut c = SeriesCache::new(10);
        assert!(!c.is_committed(&id(1), p));
        c.touch(id(1));
        assert!(!c.is_committed(&id(1), p));
        c.mark_committed(id(1), p);
        assert!(c.is_committed(&id(1), p));
        assert!(!c.is_committed(&id(1), q));
        // Four lookups: absent -> miss, present with no partition -> miss,
        // present with p -> hit, asked for q -> miss.
        assert_eq!(c.stats().hits, 1);
        assert_eq!(c.stats().misses, 3);
    }

    /// Scenario: capacity 2, three distinct ids touched in order.
    /// Guarantees: the least recently used id is evicted and counted; a lost entry reads as not committed.
    #[test]
    fn evicts_least_recently_used() {
        let p = PartitionId { date: 1, hour: 0 };
        let mut c = SeriesCache::new(2);
        c.mark_committed(id(1), p);
        c.mark_committed(id(2), p);
        assert!(c.is_committed(&id(1), p)); // 1 becomes most recent
        c.mark_committed(id(3), p); // evicts 2
        assert_eq!(c.len(), 2);
        assert_eq!(c.stats().evictions, 1);
        assert!(!c.is_committed(&id(2), p));
        assert!(c.is_committed(&id(1), p));
    }
}
