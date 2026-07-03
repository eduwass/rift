//! A pure, testable debounce clock.
//!
//! [`Debouncer`] owns only the "when should the next flush happen" logic: each
//! [`Debouncer::mark_dirty`] pushes the deadline out by `interval`, and
//! [`Debouncer::poll`] reports (and clears) a single fire once the deadline
//! passes. It carries no OS timer — the CFRunLoop glue that re-arms a
//! [`crate::sys::timer::Timer`] from these deadlines lives at the call site and
//! stays untested (it only runs on the main run loop). Keeping the arithmetic
//! here means the save-pipeline timing is unit-testable off the run loop.

use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Debouncer {
    interval: Duration,
    deadline: Option<Instant>,
}

impl Debouncer {
    pub fn new(interval: Duration) -> Self {
        Self { interval, deadline: None }
    }

    /// Mark state dirty as of `now`, (re-)arming the deadline to `now + interval`.
    pub fn mark_dirty(&mut self, now: Instant) {
        self.deadline = Some(now + self.interval);
    }

    /// Whether a flush is currently pending (dirty and not yet fired).
    pub fn is_dirty(&self) -> bool {
        self.deadline.is_some()
    }

    /// The instant at which the pending flush becomes due, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Discard any pending flush without firing.
    pub fn clear(&mut self) {
        self.deadline = None;
    }

    /// If a flush is pending and its deadline has passed by `now`, clear it and
    /// return `true` (the caller should flush). Otherwise return `false`.
    pub fn poll(&mut self, now: Instant) -> bool {
        match self.deadline {
            Some(deadline) if now >= deadline => {
                self.deadline = None;
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_debouncer_is_clean_and_never_fires() {
        let mut d = Debouncer::new(Duration::from_secs(1));
        let now = Instant::now();
        assert!(!d.is_dirty());
        assert_eq!(d.deadline(), None);
        assert!(!d.poll(now));
    }

    #[test]
    fn poll_before_deadline_does_not_fire() {
        let interval = Duration::from_secs(1);
        let mut d = Debouncer::new(interval);
        let now = Instant::now();
        d.mark_dirty(now);
        assert!(d.is_dirty());
        assert!(!d.poll(now + Duration::from_millis(999)));
        assert!(d.is_dirty(), "still pending until the deadline passes");
    }

    #[test]
    fn poll_at_or_after_deadline_fires_exactly_once() {
        let interval = Duration::from_secs(1);
        let mut d = Debouncer::new(interval);
        let now = Instant::now();
        d.mark_dirty(now);
        assert!(d.poll(now + interval), "fires once the deadline is reached");
        assert!(!d.is_dirty(), "cleared after firing");
        assert!(!d.poll(now + interval * 2), "does not fire again while clean");
    }

    #[test]
    fn mark_dirty_reextends_the_deadline() {
        let interval = Duration::from_secs(1);
        let mut d = Debouncer::new(interval);
        let t0 = Instant::now();
        d.mark_dirty(t0);
        // A second edit half an interval in pushes the deadline out from that point.
        let t1 = t0 + Duration::from_millis(500);
        d.mark_dirty(t1);
        assert!(!d.poll(t0 + interval), "original deadline no longer applies");
        assert!(d.poll(t1 + interval), "fires relative to the latest mark_dirty");
    }

    #[test]
    fn clear_cancels_a_pending_flush() {
        let mut d = Debouncer::new(Duration::from_secs(1));
        let now = Instant::now();
        d.mark_dirty(now);
        d.clear();
        assert!(!d.is_dirty());
        assert!(!d.poll(now + Duration::from_secs(10)));
    }
}
