//! Lease coordinator (pure, offline-tested): the "brain" that turns raw lease
//! rows into take decisions across scan ticks.
//!
//! Expiry is detected by **`leaseCounter` freshness**: a lease owned by another
//! worker is considered expired once its counter has not advanced between two
//! scans (the owner stopped heartbeating). This mirrors KCL
//! `DynamoDBLeaseTaker`, which tracks counter-increment freshness over the lease
//! duration. Feeding the derived snapshot to [`crate::taker::compute_leases_to_take`]
//! yields the leases this worker should claim. See core/REFERENCES.md.

use crate::taker::{compute_leases_to_take, LeaseSnapshot};
use std::collections::HashMap;

/// A raw lease-table row as read from a scan.
#[derive(Clone, Debug)]
pub struct RawLease {
    pub lease_key: String,
    pub owner: Option<String>,
    pub lease_counter: u64,
    pub completed: bool,
}

/// Stateful, single-worker view of the lease table used to decide takes.
pub struct LeaseCoordinator {
    me: String,
    max_take: usize,
    /// Last `leaseCounter` observed per lease, for freshness/expiry detection.
    last_seen: HashMap<String, u64>,
}

impl LeaseCoordinator {
    pub fn new(me: impl Into<String>, max_take: usize) -> Self {
        Self { me: me.into(), max_take, last_seen: HashMap::new() }
    }

    /// Process one scan of the lease table. Updates freshness tracking and
    /// returns the lease keys this worker should attempt to take this tick.
    ///
    /// Freshness rule: a lease owned by ANOTHER worker is expired iff we saw it
    /// on a previous tick and its counter has not advanced since. A first
    /// sighting is given one tick to prove liveness (never stolen immediately).
    pub fn tick(&mut self, rows: &[RawLease]) -> Vec<String> {
        let snapshot: Vec<LeaseSnapshot> = rows
            .iter()
            .map(|r| LeaseSnapshot {
                lease_key: r.lease_key.clone(),
                owner: r.owner.clone(),
                expired: self.is_expired(r),
                completed: r.completed,
            })
            .collect();

        // Update freshness AFTER deriving expiry for this tick.
        for r in rows {
            self.last_seen.insert(r.lease_key.clone(), r.lease_counter);
        }
        // Forget leases that disappeared (e.g. deleted after completion).
        let present: std::collections::HashSet<&str> =
            rows.iter().map(|r| r.lease_key.as_str()).collect();
        self.last_seen.retain(|k, _| present.contains(k.as_str()));

        compute_leases_to_take(&snapshot, &self.me, self.max_take)
    }

    fn is_expired(&self, r: &RawLease) -> bool {
        match &r.owner {
            None => false,                              // unowned → available, not "expired"
            Some(o) if o == &self.me => false,          // mine
            Some(_) => match self.last_seen.get(&r.lease_key) {
                Some(&prev) => prev == r.lease_counter, // seen before and stalled
                None => false,                          // first sighting → give a tick
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, owner: Option<&str>, counter: u64, completed: bool) -> RawLease {
        RawLease { lease_key: key.into(), owner: owner.map(|o| o.into()), lease_counter: counter, completed }
    }

    #[test]
    fn other_workers_leases_not_stolen_on_first_sighting() {
        // Balanced by count (w1: a,b; w2: c,d) so balancing never triggers —
        // isolates expiry. First sighting of w2 → alive → take nothing.
        let mut c = LeaseCoordinator::new("w1", 10);
        let rows = vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 5, false),
            row("d", Some("w2"), 5, false),
        ];
        assert!(c.tick(&rows).is_empty());
    }

    #[test]
    fn stalled_counter_becomes_expired_and_is_taken() {
        let mut c = LeaseCoordinator::new("w1", 10);
        let rows = vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 5, false),
            row("d", Some("w2"), 5, false),
        ];
        assert!(c.tick(&rows).is_empty()); // tick 1: first sighting, balanced
        // tick 2: w2 counters unchanged → expired → w1 takes both dead leases.
        let mut take = c.tick(&rows);
        take.sort();
        assert_eq!(take, vec!["c", "d"]);
    }

    #[test]
    fn advancing_counter_stays_alive() {
        let mut c = LeaseCoordinator::new("w1", 10);
        let t1 = vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 5, false),
            row("d", Some("w2"), 5, false),
        ];
        c.tick(&t1);
        // w2 heartbeated (counters advanced) → alive → still balanced → nothing.
        let t2 = vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 6, false),
            row("d", Some("w2"), 7, false),
        ];
        assert!(c.tick(&t2).is_empty());
    }

    #[test]
    fn unowned_taken_immediately() {
        let mut c = LeaseCoordinator::new("w1", 10);
        let rows = vec![row("a", None, 0, false), row("b", None, 0, false)];
        let mut take = c.tick(&rows);
        take.sort();
        assert_eq!(take, vec!["a", "b"]);
    }

    #[test]
    fn forgets_disappeared_leases() {
        let mut c = LeaseCoordinator::new("w1", 10);
        c.tick(&[row("a", Some("w2"), 5, false)]);
        // "a" gone next scan; a brand-new "a" later must get a fresh first-sighting.
        c.tick(&[]);
        assert!(c.tick(&[row("a", Some("w2"), 9, false)]).is_empty());
    }
}
