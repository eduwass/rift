//! Reactor-side layout persistence: snapshot assembly, restore activation,
//! space remap, exact window adoption, and pruning.
//!
//! The [`LayoutEngine`](crate::layout_engine::LayoutEngine) serializes its own
//! trees and workspace state, but it is deliberately reactor-agnostic: it does
//! not know a window's window-server id, bundle id, or which display uuid a space
//! lives on. Those durable identities live in the reactor's OS-mirror managers.
//! This module owns the *join*: at save time it pairs engine state with reactor
//! identity into a [`Snapshot`]; at load time it lifts the matching arrangement
//! back into the engine and drives re-adoption as windows are rediscovered.
//!
//! Kept cohesive in one module so the whole persistence feature can be reviewed —
//! and offered upstream — as a single unit.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use objc2_core_foundation::CGRect;
use tracing::info;

use super::Reactor;
use crate::actor::app::{WindowId, pid_t};
use crate::common::collections::{HashMap, HashSet};
use crate::common::config;
use crate::common::debounce::Debouncer;
use crate::layout_engine::LayoutEngine;
use crate::layout_engine::snapshot::{Arrangement, Snapshot, WindowIdentity};
use crate::model::virtual_workspace::{AppRuleAssignment, AppRuleResult, WorkspaceError};
use crate::sys::screen::{ScreenInfo, SpaceId};
use crate::sys::window_server::WindowServerId;

/// How long after a layout mutation the debounced save fires.
const SAVE_DEBOUNCE: Duration = Duration::from_millis(1000);
/// Global backstop: unclaimed adoption entries (windows whose apps never came
/// back) are dropped this long after a restore, so pruned state gets persisted.
const ADOPTION_SETTLE_TIMEOUT: Duration = Duration::from_secs(180);

/// One pending re-adoption: a window that existed before the restart, keyed for
/// lookup by its (durable) window-server id.
struct AdoptionEntry {
    /// The pre-restart `WindowId` occupying this window's slot in the restored
    /// engine — the id to rewrite *from* once the live window is found.
    old_wid: WindowId,
    /// Durable attributes, keyed by the saved (pre-restart) window-server id.
    /// The exact stage matches on `identity.server_id`; the fuzzy (reboot) stage
    /// matches on `bundle_id`/`title`/`ax_role`/`frame` when server ids changed.
    identity: WindowIdentity,
}

/// Windows from a restored arrangement still awaiting a live match. Exact
/// (crash-restart) adoption keys on `WindowServerId`, which is stable while a
/// window lives. Fuzzy (reboot) matching is a later phase.
#[derive(Default)]
pub struct AdoptionTable {
    by_server_id: HashMap<WindowServerId, AdoptionEntry>,
}

impl AdoptionTable {
    pub(super) fn from_windows(windows: HashMap<WindowId, WindowIdentity>) -> Self {
        let mut by_server_id = HashMap::default();
        for (old_wid, identity) in windows {
            by_server_id.insert(identity.server_id, AdoptionEntry { old_wid, identity });
        }
        AdoptionTable { by_server_id }
    }

    fn is_empty(&self) -> bool {
        self.by_server_id.is_empty()
    }

    /// The pre-restart id waiting on `server_id`, without consuming the entry.
    fn peek(&self, server_id: WindowServerId) -> Option<WindowId> {
        self.by_server_id.get(&server_id).map(|e| e.old_wid)
    }

    /// Consume the entry keyed by its saved `server_id` (claim-once). For an exact
    /// match the live and saved server ids are the same; for a fuzzy match the key
    /// is the entry's *saved* server id (the live one has changed since reboot).
    fn claim(&mut self, saved_server_id: WindowServerId) {
        self.by_server_id.remove(&saved_server_id);
    }

    /// Rank unclaimed fuzzy candidates for a discovered window's durable
    /// attributes, best first, returning each candidate's key (its saved
    /// window-server id) and pre-restart id. A candidate must share the same
    /// non-empty `bundle_id`, the exact `title`, and the same `ax_role`. Ties
    /// break on frame similarity, then within-app discovery order (the pre-restart
    /// id). Blank titles and missing bundle ids never match — too ambiguous for
    /// mid-launch terminals/browsers.
    fn fuzzy_candidates(
        &self,
        bundle_id: &str,
        title: &str,
        ax_role: &str,
        frame: CGRect,
    ) -> Vec<(WindowServerId, WindowId)> {
        if bundle_id.is_empty() || title.trim().is_empty() {
            return Vec::new();
        }
        let mut ranked: Vec<(WindowServerId, WindowId, f64)> = self
            .by_server_id
            .iter()
            .filter(|(_, e)| {
                !e.identity.bundle_id.is_empty()
                    && e.identity.bundle_id == bundle_id
                    && e.identity.title == title
                    && e.identity.ax_role == ax_role
            })
            .map(|(k, e)| (*k, e.old_wid, frame_distance(frame, e.identity.frame)))
            .collect();
        ranked.sort_by(|a, b| a.2.total_cmp(&b.2).then(a.1.cmp(&b.1)));
        ranked.into_iter().map(|(k, wid, _)| (k, wid)).collect()
    }

    /// Remove every unclaimed entry belonging to a launched app, returning their
    /// pre-restart ids so the caller can evict them from the engine. An entry
    /// matches on stable `pid` (a plain rift restart keeps pids) *or* on durable
    /// `bundle_id` (after a reboot pids change but bundle ids do not).
    fn prune_app(&mut self, pid: pid_t, bundle_id: Option<&str>) -> Vec<WindowId> {
        let keys: Vec<WindowServerId> = self
            .by_server_id
            .iter()
            .filter(|(_, e)| {
                e.old_wid.pid == pid
                    || bundle_id.is_some_and(|b| !b.is_empty() && e.identity.bundle_id == b)
            })
            .map(|(k, _)| *k)
            .collect();
        keys.into_iter().filter_map(|k| self.by_server_id.remove(&k)).map(|e| e.old_wid).collect()
    }

    /// Remove and return every remaining entry's pre-restart id.
    fn drain_all(&mut self) -> Vec<WindowId> {
        std::mem::take(&mut self.by_server_id).into_values().map(|e| e.old_wid).collect()
    }
}

/// L1 distance between two frames (summed origin + size deltas). Used only to
/// tiebreak fuzzy candidates that already agree on bundle/title/role, so the
/// absolute scale is irrelevant — only the ordering is.
fn frame_distance(a: CGRect, b: CGRect) -> f64 {
    (a.origin.x - b.origin.x).abs()
        + (a.origin.y - b.origin.y).abs()
        + (a.size.width - b.size.width).abs()
        + (a.size.height - b.size.height).abs()
}

/// All persistence state the reactor carries. Disabled (no restore path) in the
/// test harness so unit tests never touch the real `~/.rift/layout.ron`.
pub struct PersistenceState {
    /// `Some` in production (points at `~/.rift/layout.ron`); `None` under tests,
    /// which drive assembly/adoption directly without disk IO.
    restore_path: Option<PathBuf>,
    /// Loaded at startup, consumed on the first screen-parameters event once the
    /// display fingerprint is known.
    pending_restore: Option<Snapshot>,
    /// True once the restore attempt has run (whether or not it found a match), so
    /// it happens exactly once.
    activated: bool,
    debouncer: Debouncer,
    adoption: AdoptionTable,
    /// Saved space identities awaiting resolution to a live `SpaceId`:
    /// `saved SpaceId -> (display_uuid, ordinal)`.
    saved_spaces: HashMap<SpaceId, (String, u32)>,
    /// Pre-restart ids of windows that were pinned topmost, awaiting adoption.
    /// When a window with one of these ids is adopted, its live id is re-pinned
    /// (see [`Reactor::adopt_entry`]); entries never adopted are simply dropped.
    pending_topmost: HashSet<WindowId>,
    /// When the global adoption settle timeout expires (armed while adopting).
    settle_deadline: Option<Instant>,
    /// Match bookkeeping for the current restore, logged at settle so reboot
    /// re-adoption quality is observable: windows re-adopted by exact server id,
    /// by fuzzy attributes, and windows that matched nothing (normal placement).
    adopt_exact: u32,
    adopt_fuzzy: u32,
    adopt_fallthrough: u32,
    adopt_pruned: u32,
    /// Display fingerprint the live engine currently belongs to. Set when the
    /// first arrangement is activated and updated on every arrangement switch;
    /// a mismatch against the connected display set is what triggers a switch.
    active_fingerprint: Option<String>,
}

impl Default for PersistenceState {
    fn default() -> Self {
        PersistenceState {
            restore_path: None,
            pending_restore: None,
            activated: false,
            debouncer: Debouncer::new(SAVE_DEBOUNCE),
            adoption: AdoptionTable::default(),
            saved_spaces: HashMap::default(),
            pending_topmost: HashSet::default(),
            settle_deadline: None,
            adopt_exact: 0,
            adopt_fuzzy: 0,
            adopt_fallthrough: 0,
            adopt_pruned: 0,
            active_fingerprint: None,
        }
    }
}

impl PersistenceState {
    fn enabled(&self) -> bool {
        self.restore_path.is_some()
    }
}

impl Reactor {
    /// Turn on layout persistence: point the writer at `path` and load whatever
    /// arrangement set is already on disk for later activation. Called from
    /// [`Reactor::spawn`]; the test harness leaves persistence disabled.
    pub fn enable_persistence(&mut self, path: PathBuf) {
        let snapshot = Snapshot::load_or_default(&path);
        self.persistence.restore_path = Some(path);
        self.persistence.pending_restore = Some(snapshot);
    }

    // ---- save pipeline ------------------------------------------------------

    /// Mark layout state dirty; the debounced tick flushes it ~1s later. Called
    /// from the layout-event funnel. No-op when persistence is disabled.
    pub(super) fn mark_layout_dirty(&mut self) {
        if self.persistence.enabled() {
            self.persistence.debouncer.mark_dirty(Instant::now());
        }
    }

    /// Periodic driver (fired by [`Event::PersistTick`](super::Event)): flush a
    /// due debounced save and run the global adoption settle timeout.
    pub(super) fn persistence_tick(&mut self) {
        if !self.persistence.enabled() {
            return;
        }
        let now = Instant::now();
        // Order matters: while a save is suppressed (topology churn) we must NOT
        // poll the debouncer, or its pending deadline would be cleared and the
        // dirty state lost. Checking `can_persist_now` first leaves the deadline
        // armed so the flush fires on the first tick after the topology settles.
        if self.can_persist_now() && self.persistence.debouncer.poll(now) {
            self.flush_snapshot();
        }
        if let Some(deadline) = self.persistence.settle_deadline {
            if now >= deadline {
                self.prune_settled_adoptions();
            }
        }
    }

    /// Whether it is safe to write a snapshot right now. While `display_topology`
    /// is churning or awaiting its commit snapshot, the engine is mid-migration —
    /// suppress writes so a half-migrated arrangement is never persisted. The
    /// debouncer stays dirty and the flush fires on the first tick after settle.
    fn can_persist_now(&self) -> bool {
        !self.display_topology_manager.is_churning_or_awaiting_commit()
    }

    /// Read-modify-write the on-disk snapshot: preserve arrangements for other
    /// display fingerprints, replace the current one. Best-effort — a failed
    /// write just leaves the previous (≤ debounce-stale) file in place.
    fn flush_snapshot(&mut self) {
        let fingerprint = self.current_fingerprint();
        self.flush_snapshot_under(&fingerprint);
    }

    /// Persist the live engine under an explicit fingerprint, leaving every other
    /// arrangement in the file untouched. Used by the debounced flush (current
    /// fingerprint) and by the pre-switch save (the arrangement being left).
    fn flush_snapshot_under(&mut self, fingerprint: &str) {
        let Some(path) = self.persistence.restore_path.clone() else {
            return;
        };
        let mut snapshot = Snapshot::load_or_default(&path);
        snapshot.arrangements.insert(fingerprint.to_string(), self.assemble_arrangement());
        if let Err(e) = snapshot.save(&path) {
            tracing::warn!("failed to persist layout snapshot to {path:?}: {e}");
        }
    }

    /// Synchronous save used by the SaveAndExit command. Same read-modify-write as
    /// the debounced flush; correctness never depends on it (crash-safe by the
    /// debounced writer), it just makes a clean exit's file current.
    pub(super) fn save_snapshot_now(&mut self) -> std::io::Result<()> {
        let path =
            self.persistence.restore_path.clone().unwrap_or_else(config::restore_file);
        let mut snapshot = Snapshot::load_or_default(&path);
        snapshot.arrangements.insert(self.current_fingerprint(), self.assemble_arrangement());
        snapshot.save(&path)
    }

    /// A single-arrangement snapshot of current state, without touching disk.
    /// Used by tests (and as the assembly primitive).
    pub(super) fn assemble_single_snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();
        snapshot.arrangements.insert(self.current_fingerprint(), self.assemble_arrangement());
        snapshot
    }

    /// Join the live engine with reactor-side identity into one [`Arrangement`]:
    /// the engine's own serde output plus the sidecars (durable window and space
    /// identities) needed to re-adopt it into a fresh session.
    fn assemble_arrangement(&self) -> Arrangement {
        let engine = &self.layout_manager.layout_engine;

        // Clone the engine via its own serde. Cheap relative to the debounce, and
        // guarantees the on-disk form matches the live one without a Clone impl.
        // ponytail: serde round-trip clone; swap for a borrowing serializer if it
        // ever shows up in a profile.
        let engine_owned: LayoutEngine = ron::from_str(&engine.serialize_to_string())
            .expect("engine serialization round-trips");

        let mut windows = HashMap::default();
        for wid in engine.all_window_ids() {
            let Some(state) = self.window_manager.windows.get(&wid) else {
                continue;
            };
            let Some(server_id) = state.info.sys_id else {
                continue;
            };
            let bundle_id = state
                .info
                .bundle_id
                .clone()
                .or_else(|| {
                    self.app_manager.apps.get(&wid.pid).and_then(|a| a.info.bundle_id.clone())
                })
                .unwrap_or_default();
            windows.insert(
                wid,
                WindowIdentity {
                    server_id,
                    bundle_id,
                    title: state.info.title.clone(),
                    ax_role: state.info.ax_role.clone().unwrap_or_default(),
                    frame: state.frame_monotonic,
                },
            );
        }

        let mut spaces = HashMap::default();
        for (space, uuid_opt) in engine.spaces_with_display_uuid() {
            let uuid = uuid_opt.or_else(|| self.display_uuid_for_space(space));
            if let Some(uuid) = uuid {
                spaces.insert(space, (uuid.clone(), self.space_ordinal(space, &uuid)));
            }
        }

        Arrangement {
            engine: engine_owned,
            spaces,
            windows,
            // Persist only explicit pins. Implicit `floating_windows_topmost`
            // pins are a pure function of floating state (already serialized) and
            // are re-derived by `sync_floating_topmost` after restore, so saving
            // them would wrongly resurrect them as explicit pins.
            topmost: self
                .topmost_windows
                .iter()
                .filter(|(_, state)| !state.implicit)
                .map(|(wid, _)| *wid)
                .collect(),
        }
    }

    /// Fingerprint of the currently connected display set — the arrangement key.
    fn current_fingerprint(&self) -> String {
        Self::fingerprint_of(&self.space_manager.screens)
    }

    /// Sorted, deduped, `+`-joined set of display uuids. The single derivation
    /// shared by save and switch detection: order-independent, so reordering the
    /// same displays maps to the same arrangement.
    pub(super) fn fingerprint_of(screens: &[ScreenInfo]) -> String {
        let mut uuids: Vec<String> =
            screens.iter().map(|s| s.display_uuid.clone()).filter(|u| !u.is_empty()).collect();
        uuids.sort();
        uuids.dedup();
        uuids.join("+")
    }

    /// A space's ordinal within its display: its position among the currently
    /// connected screens sharing `uuid`. With one space per display (the common
    /// case and every test) this is always 0.
    fn space_ordinal(&self, space: SpaceId, uuid: &str) -> u32 {
        let mut same_uuid: Vec<SpaceId> = self
            .space_manager
            .screens
            .iter()
            .filter(|s| s.display_uuid == uuid)
            .filter_map(|s| s.space)
            .collect();
        same_uuid.sort();
        same_uuid.iter().position(|s| *s == space).unwrap_or(0) as u32
    }

    // ---- restore pipeline ---------------------------------------------------

    /// On the first screen-parameters event, pick the arrangement matching the
    /// current display fingerprint, lift it into the engine, and stage adoption.
    /// Runs exactly once; a missing/mismatched arrangement just starts fresh.
    pub(super) fn activate_restore_if_ready(&mut self) {
        if self.persistence.activated || self.space_manager.screens.is_empty() {
            return;
        }
        let Some(mut snapshot) = self.persistence.pending_restore.take() else {
            return;
        };
        self.persistence.activated = true;

        let fingerprint = self.current_fingerprint();
        // The engine now belongs to this fingerprint (whether or not a saved
        // arrangement matched); later screen-parameters events compare against it
        // to detect an arrangement switch.
        self.persistence.active_fingerprint = Some(fingerprint.clone());
        let Some(arrangement) = snapshot.arrangements.remove(&fingerprint) else {
            info!("no saved layout arrangement for display fingerprint {fingerprint:?}; starting fresh");
            return;
        };

        let Arrangement { mut engine, spaces, windows, topmost } = arrangement;
        engine.rehydrate_restored(
            &self.config.virtual_workspaces,
            &self.config.settings.layout,
            Some(self.communication_manager.event_broadcaster.clone()),
        );
        self.layout_manager.layout_engine = engine;
        self.install_restore_state(AdoptionTable::from_windows(windows), spaces);
        self.persistence.pending_topmost = topmost.into_iter().collect();
        info!(
            "restored layout arrangement for display fingerprint {fingerprint:?} ({} windows pending adoption)",
            self.persistence.adoption.by_server_id.len()
        );
    }

    /// Stage a restored arrangement's sidecars for adoption and space remap. Split
    /// out so tests can install state onto a `new_for_test` engine directly,
    /// bypassing the disk/fingerprint path.
    pub(super) fn install_restore_state(
        &mut self,
        adoption: AdoptionTable,
        saved_spaces: HashMap<SpaceId, (String, u32)>,
    ) {
        let arm_settle = !adoption.is_empty();
        self.persistence.adoption = adoption;
        self.persistence.saved_spaces = saved_spaces;
        // The caller sets `pending_topmost` right after (from the arrangement's
        // saved set); clear any leftovers from a previous restore first.
        self.persistence.pending_topmost = HashSet::default();
        self.persistence.adopt_exact = 0;
        self.persistence.adopt_fuzzy = 0;
        self.persistence.adopt_fallthrough = 0;
        self.persistence.adopt_pruned = 0;
        if arm_settle {
            self.persistence.settle_deadline = Some(Instant::now() + ADOPTION_SETTLE_TIMEOUT);
        }
    }

    // ---- arrangement switching ----------------------------------------------

    /// Called at the start of a display-set change, before rift migrates windows
    /// off the disconnected display(s): persist the still-intact engine under the
    /// arrangement being left (`old` fingerprint, derived from the *current*
    /// screens), so replugging can restore it exactly. Synchronous and not
    /// churn-gated — the point is to capture the pre-migration state.
    pub(super) fn save_arrangement_before_switch(&mut self, new_screens: &[ScreenInfo]) {
        if !self.persistence.enabled() {
            return;
        }
        let old_fingerprint = self.current_fingerprint();
        let new_fingerprint = Self::fingerprint_of(new_screens);
        if old_fingerprint.is_empty() || old_fingerprint == new_fingerprint {
            return;
        }
        self.flush_snapshot_under(&old_fingerprint);
    }

    /// Called after the connected display set (and thus the fingerprint) has been
    /// updated: if it changed and the new arrangement has a saved entry, lift that
    /// entry into the engine and stage its sidecars for adoption — the same
    /// machinery as startup restore, but against already-live windows. A missing
    /// entry leaves rift's default topology migration in charge; either way the
    /// active fingerprint is updated so the next switch is detected.
    pub(super) fn load_arrangement_after_switch(&mut self) {
        if !self.persistence.enabled() {
            return;
        }
        let new_fingerprint = self.current_fingerprint();
        if new_fingerprint.is_empty() {
            return;
        }
        match self.persistence.active_fingerprint.as_deref() {
            Some(active) if active == new_fingerprint => return,
            None => {
                self.persistence.active_fingerprint = Some(new_fingerprint);
                return;
            }
            Some(_) => {}
        }

        if let Some(path) = self.persistence.restore_path.clone() {
            let mut snapshot = Snapshot::load_or_default(&path);
            if let Some(arrangement) = snapshot.arrangements.remove(&new_fingerprint) {
                let Arrangement { mut engine, spaces, windows, topmost } = arrangement;
                engine.rehydrate_restored(
                    &self.config.virtual_workspaces,
                    &self.config.settings.layout,
                    Some(self.communication_manager.event_broadcaster.clone()),
                );
                self.layout_manager.layout_engine = engine;
                self.install_restore_state(AdoptionTable::from_windows(windows), spaces);
                self.persistence.pending_topmost = topmost.into_iter().collect();
                info!(
                    "switched to saved layout arrangement for display fingerprint {new_fingerprint:?}"
                );
            }
        }
        self.persistence.active_fingerprint = Some(new_fingerprint);
    }

    /// Resolve saved space identities to live `SpaceId`s and migrate engine state
    /// onto them. Matches each connected screen's `(uuid, ordinal)` against the
    /// saved set; identity matches are a no-op. Drains resolved entries so repeat
    /// screen-parameters events don't remap twice.
    pub(super) fn remap_restored_spaces(&mut self) {
        if self.persistence.saved_spaces.is_empty() {
            return;
        }
        let live: Vec<(String, u32, SpaceId)> = self
            .space_manager
            .screens
            .iter()
            .filter_map(|s| s.space.map(|space| (s.display_uuid.clone(), space)))
            .filter(|(uuid, _)| !uuid.is_empty())
            .map(|(uuid, space)| {
                let ordinal = self.space_ordinal(space, &uuid);
                (uuid, ordinal, space)
            })
            .collect();

        for (uuid, ordinal, live_space) in live {
            let matched = self
                .persistence
                .saved_spaces
                .iter()
                .find(|(_, (u, o))| *u == uuid && *o == ordinal)
                .map(|(saved_space, _)| *saved_space);
            if let Some(saved_space) = matched {
                self.persistence.saved_spaces.remove(&saved_space);
                if saved_space != live_space {
                    self.layout_manager.layout_engine.remap_space(saved_space, live_space);
                }
            }
        }
    }

    // ---- window adoption ----------------------------------------------------

    /// Window adoption, consulted before app-rules at discovery time. Two stages:
    ///
    /// 1. **Exact:** the discovered window still carries a window-server id we
    ///    recorded — the crash/restart path (server ids survive). A recorded id
    ///    always belongs to exact matching, never fuzzy.
    /// 2. **Fuzzy:** no recorded server id matched (a reboot changed them all), so
    ///    match by durable attributes (`bundle_id` + exact `title` + `ax_role`),
    ///    picking the closest-frame candidate on this space.
    ///
    /// Either stage rewrites the pre-restart `WindowId` onto the live one
    /// (inheriting its exact tree slot) and returns the restored assignment.
    /// Returns `None` to fall through to normal assignment.
    pub(super) fn try_adopt_window(
        &mut self,
        wid: WindowId,
        space: SpaceId,
    ) -> Option<Result<AppRuleResult, WorkspaceError>> {
        if self.persistence.adoption.is_empty() {
            return None;
        }

        // Stage 1 — exact. A window whose server id we recorded is handled here
        // even if it surfaces on the wrong space (then adopt_entry returns `None`
        // to defer it to its own space's pass): a recorded id is never fuzzy.
        if let Some(server_id) = self.window_manager.windows.get(&wid).and_then(|w| w.info.sys_id) {
            if let Some(old_wid) = self.persistence.adoption.peek(server_id) {
                let result = self.adopt_entry(server_id, old_wid, wid, space);
                if result.is_some() {
                    self.persistence.adopt_exact += 1;
                    self.finish_adoption_if_drained();
                }
                return result;
            }
        }

        // Stage 2 — fuzzy. Best attribute-matched candidate on this space wins;
        // candidates on other spaces are left for their own pass.
        if let Some((bundle_id, title, ax_role, frame)) = self.live_identity(wid) {
            let candidates =
                self.persistence.adoption.fuzzy_candidates(&bundle_id, &title, &ax_role, frame);
            for (saved_key, old_wid) in candidates {
                if let Some(result) = self.adopt_entry(saved_key, old_wid, wid, space) {
                    self.persistence.adopt_fuzzy += 1;
                    self.finish_adoption_if_drained();
                    return Some(result);
                }
            }
        }

        self.persistence.adopt_fallthrough += 1;
        None
    }

    /// Complete an adoption once a table entry is chosen: verify its restored
    /// placement is on `space` (else `None`, to defer it to that space's pass),
    /// claim its slot by the entry's *saved* server id, rewrite the pre-restart id
    /// onto the live one, and return the restored assignment.
    fn adopt_entry(
        &mut self,
        saved_key: WindowServerId,
        old_wid: WindowId,
        wid: WindowId,
        space: SpaceId,
    ) -> Option<Result<AppRuleResult, WorkspaceError>> {
        let engine = &self.layout_manager.layout_engine;
        let workspace_id =
            engine.virtual_workspace_manager().workspace_for_window(space, old_wid)?;
        let floating = engine.is_window_floating(old_wid);

        self.persistence.adoption.claim(saved_key);
        // Topmost is snapshot-side (keyed by the pre-restart id), so map it here
        // as we rewrite: if this window was pinned before the restart, re-pin its
        // live id via the same path the toggle uses.
        let was_topmost = self.persistence.pending_topmost.remove(&old_wid);
        if old_wid != wid {
            self.layout_manager.layout_engine.rewrite_window_id(old_wid, wid);
        }
        if was_topmost {
            self.pin_topmost_window(wid);
        }
        self.mark_layout_dirty();
        // The drain check runs in the caller *after* the match counter is bumped
        // (see `try_adopt_window`), so finalizing here would reset the counters
        // before that increment.

        Some(Ok(AppRuleResult::Managed(AppRuleAssignment {
            workspace_id,
            floating,
            prev_rule_decision: false,
        })))
    }

    /// A discovered window's durable attributes for fuzzy matching, mirroring how
    /// [`assemble_arrangement`](Self::assemble_arrangement) records them: the
    /// window's own bundle id, falling back to its app's; title; ax role; frame.
    fn live_identity(&self, wid: WindowId) -> Option<(String, String, String, CGRect)> {
        let state = self.window_manager.windows.get(&wid)?;
        let bundle_id = state
            .info
            .bundle_id
            .clone()
            .or_else(|| self.app_manager.apps.get(&wid.pid).and_then(|a| a.info.bundle_id.clone()))
            .unwrap_or_default();
        let title = state.info.title.clone();
        let ax_role = state.info.ax_role.clone().unwrap_or_default();
        Some((bundle_id, title, ax_role, state.frame_monotonic))
    }

    // ---- pruning ------------------------------------------------------------

    /// After an app's discovery pass, drop any of its still-unclaimed adoption
    /// entries: those are windows that existed before the restart but no longer
    /// do (closed, or the app relaunched with fewer windows). Removes them from
    /// the engine so the next save persists the pruned state. Resolves the
    /// long-standing "remove apps no longer running from restored state" gap.
    pub(super) fn prune_app_adoptions(&mut self, pid: pid_t) {
        if self.persistence.adoption.is_empty() {
            return;
        }
        // Prune by the launched app's durable bundle id, not its pid: a reboot
        // gives relaunched apps fresh pids, so the pre-restart `old_wid.pid` no
        // longer identifies them. `prune_app` still honours a pid match too, so a
        // plain restart (pids stable) keeps working.
        let bundle_id = self.app_manager.apps.get(&pid).and_then(|a| a.info.bundle_id.clone());
        let stale = self.persistence.adoption.prune_app(pid, bundle_id.as_deref());
        self.evict_pruned_windows(stale);
    }

    /// Global settle backstop: evict every remaining unclaimed entry (dead apps
    /// that never came back) once the timeout passes. Eviction drains the table,
    /// which finalizes the restore (disarms + logs) via `finish_adoption_if_drained`;
    /// the trailing call covers the degenerate "nothing to evict" case.
    fn prune_settled_adoptions(&mut self) {
        let stale = self.persistence.adoption.drain_all();
        self.evict_pruned_windows(stale);
        self.finish_adoption();
    }

    fn evict_pruned_windows(&mut self, stale: Vec<WindowId>) {
        if stale.is_empty() {
            return;
        }
        self.persistence.adopt_pruned += stale.len() as u32;
        for wid in stale {
            self.layout_manager.layout_engine.prune_window(wid);
        }
        self.mark_layout_dirty();
        let _ = self.update_layout_or_warn(false, false);
        self.finish_adoption_if_drained();
    }

    /// Finalize the restore as soon as its adoption table empties — every window
    /// was either re-adopted or pruned — instead of idling until the global
    /// settle timeout. Emits the tally once (a clean restart no longer waits ~3
    /// minutes to log). A no-op when no restore is in flight.
    fn finish_adoption_if_drained(&mut self) {
        if self.persistence.adoption.is_empty() {
            self.finish_adoption();
        }
    }

    /// Disarm the settle timer and log the restore's match tally once, so reboot
    /// re-adoption quality is observable and the counters are reset for the next
    /// restore. Guarded on the armed deadline so both callers (early drain and
    /// the global timeout) log exactly once.
    fn finish_adoption(&mut self) {
        if self.persistence.settle_deadline.is_none() {
            return;
        }
        self.persistence.settle_deadline = None;
        info!(
            "adoption settled: {} exact, {} fuzzy, {} fell through, {} pruned",
            self.persistence.adopt_exact,
            self.persistence.adopt_fuzzy,
            self.persistence.adopt_fallthrough,
            self.persistence.adopt_pruned,
        );
        self.persistence.adopt_exact = 0;
        self.persistence.adopt_fuzzy = 0;
        self.persistence.adopt_fallthrough = 0;
        self.persistence.adopt_pruned = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};

    use objc2_core_foundation::{CGPoint, CGRect, CGSize};

    use super::super::testing::*;
    use super::{AdoptionTable, WindowIdentity};
    use crate::actor;
    use crate::actor::app::{AppThreadHandle, WindowId, pid_t};
    use crate::actor::reactor::Reactor;
    use crate::common::collections::{HashMap, HashSet};
    use crate::layout_engine::{LayoutEngine, LayoutEvent};
    use crate::model::reactor::{AppState, WindowState};
    use crate::model::virtual_workspace::AppRuleResult;
    use crate::sys::app::{AppInfo, WindowInfo};
    use crate::sys::screen::{ScreenInfo, SpaceId};
    use crate::sys::skylight::DisplayReconfigFlags;
    use crate::sys::window_server::WindowServerId;

    // NOTE: these tests drive the persistence seams directly (engine population +
    // `assemble`/`try_adopt_window`/`remap_restored_spaces`/`prune_app_adoptions`)
    // rather than through `Apps`/`simulate_until_quiet`. On this branch the
    // `new_for_test` discovery pipeline does not assign windows to the engine (the
    // same pre-existing breakage behind the known reactor/main_window baseline
    // failures, e.g. `it_manages_windows_on_enabled_spaces`), so an end-to-end
    // discovery-driven roundtrip is not expressible here without fixing that first.

    fn fresh_engine() -> LayoutEngine {
        LayoutEngine::new(
            &crate::common::config::VirtualWorkspaceSettings::default(),
            &crate::common::config::LayoutSettings::default(),
            None,
        )
    }

    /// Populate the reactor's engine with `wids` tiled on `space` (as a restored
    /// snapshot's engine would arrive pre-populated), driving the engine directly.
    /// All ids must share a pid.
    fn place_in_engine(reactor: &mut Reactor, space: SpaceId, wids: &[WindowId]) {
        let engine = &mut reactor.layout_manager.layout_engine;
        let _ = engine.handle_event(LayoutEvent::SpaceExposed(space, CGSize::new(1000., 1000.)));
        let windows = wids
            .iter()
            .map(|wid| (*wid, None, None, None, true, CGSize::new(0., 0.), None, None))
            .collect();
        let _ =
            engine.handle_event(LayoutEvent::WindowsOnScreenUpdated(space, wids[0].pid, windows, None));
    }

    /// Register a live (rediscovered) window in the reactor's OS-mirror manager so
    /// adoption/assembly can read its durable window-server id.
    fn register_live(reactor: &mut Reactor, wid: WindowId, server: u32) {
        reactor.window_manager.windows.insert(wid, WindowState::from(win(wid.idx.get(), server)));
    }

    /// A single test screen whose synthesized display uuid is `test-display-0`.
    fn one_screen_snapshots(space: SpaceId) -> Vec<ScreenInfo> {
        make_screen_snapshots(
            vec![CGRect::new(CGPoint::new(0., 0.), CGSize::new(1000., 1000.))],
            vec![Some(space)],
        )
    }

    /// Two side-by-side test screens: `test-display-0` (space `s1`) and
    /// `test-display-1` (space `s2`).
    fn two_screen_snapshots(s1: SpaceId, s2: SpaceId) -> Vec<ScreenInfo> {
        make_screen_snapshots(
            vec![
                CGRect::new(CGPoint::new(0., 0.), CGSize::new(1000., 1000.)),
                CGRect::new(CGPoint::new(1000., 0.), CGSize::new(1000., 1000.)),
            ],
            vec![Some(s1), Some(s2)],
        )
    }

    /// A window whose window-server id is decoupled from its position, so the same
    /// durable `server` can reappear under a different runtime `WindowId` after a
    /// restart. `idx` is the app-local window index the harness assigns.
    fn win(idx: u32, server: u32) -> WindowInfo {
        let mut info = make_window(idx as usize);
        info.sys_id = Some(WindowServerId::new(server));
        info.title = format!("win-{server}");
        info
    }

    fn identity(server: u32) -> WindowIdentity {
        WindowIdentity {
            server_id: WindowServerId::new(server),
            bundle_id: "com.example".to_string(),
            title: format!("win-{server}"),
            ax_role: "AXWindow".to_string(),
            frame: CGRect::new(CGPoint::new(0., 0.), CGSize::new(10., 10.)),
        }
    }

    /// A durable identity with fully specified fuzzy attributes and frame — for the
    /// reboot path, where the saved server id no longer matches any live window.
    fn identity_full(server: u32, bundle: &str, title: &str, role: &str, frame: CGRect) -> WindowIdentity {
        WindowIdentity {
            server_id: WindowServerId::new(server),
            bundle_id: bundle.to_string(),
            title: title.to_string(),
            ax_role: role.to_string(),
            frame,
        }
    }

    /// Register a live window with fully specified fuzzy attributes (bundle/title/
    /// role/frame), as a post-reboot rediscovery would arrive: a new `WindowId`,
    /// a new (or absent) server id, but the same durable attributes.
    fn register_live_full(
        reactor: &mut Reactor,
        wid: WindowId,
        server: Option<u32>,
        bundle: &str,
        title: &str,
        role: &str,
        frame: CGRect,
    ) {
        let mut info = make_window(wid.idx.get() as usize);
        info.sys_id = server.map(WindowServerId::new);
        info.bundle_id = Some(bundle.to_string());
        info.title = title.to_string();
        info.ax_role = Some(role.to_string());
        info.frame = frame;
        reactor.window_manager.windows.insert(wid, WindowState::from(info));
    }

    /// Register a launched app in the OS-mirror manager so bundle-keyed pruning can
    /// resolve a pid to its durable bundle id.
    fn register_app(reactor: &mut Reactor, pid: pid_t, bundle: &str) {
        let (tx, _rx) = actor::channel();
        reactor.app_manager.apps.insert(
            pid,
            AppState {
                info: AppInfo { bundle_id: Some(bundle.to_string()), localized_name: None },
                handle: AppThreadHandle::new_for_test(tx),
            },
        );
    }

    fn sorted(mut v: Vec<WindowId>) -> Vec<WindowId> {
        v.sort();
        v
    }

    // ---- AdoptionTable (pure) ----------------------------------------------

    #[test]
    fn adoption_table_matches_by_server_id_and_claims_once() {
        let old_a = WindowId::new(1, 1);
        let old_b = WindowId::new(1, 2);
        let mut windows = HashMap::default();
        windows.insert(old_a, identity(101));
        windows.insert(old_b, identity(102));
        let mut table = AdoptionTable::from_windows(windows);

        // Exact server-id match returns the pre-restart id without consuming it.
        assert_eq!(table.peek(WindowServerId::new(101)), Some(old_a));
        assert_eq!(table.peek(WindowServerId::new(101)), Some(old_a), "peek does not consume");
        // Unknown server id: no match (fall-through to normal assignment).
        assert_eq!(table.peek(WindowServerId::new(999)), None);

        // Claim consumes exactly one entry.
        table.claim(WindowServerId::new(101));
        assert_eq!(table.peek(WindowServerId::new(101)), None, "claimed entry is gone");
        assert_eq!(table.peek(WindowServerId::new(102)), Some(old_b), "other entry untouched");
    }

    #[test]
    fn adoption_table_prunes_by_pid_or_bundle_and_drains_the_rest() {
        let mut windows = HashMap::default();
        windows.insert(WindowId::new(1, 1), identity(101));
        windows.insert(WindowId::new(1, 2), identity(102));
        windows.insert(WindowId::new(2, 1), identity(201));
        let mut table = AdoptionTable::from_windows(windows);

        // Plain restart: pids are stable, so pruning by the launched app's pid
        // drops its entries (bundle unknown -> `None`).
        let pruned = sorted(table.prune_app(1, None));
        assert_eq!(pruned, vec![WindowId::new(1, 1), WindowId::new(1, 2)]);
        assert_eq!(table.peek(WindowServerId::new(101)), None);
        assert_eq!(table.peek(WindowServerId::new(201)), Some(WindowId::new(2, 1)));

        // Reboot: the surviving entry's pid (2) matches no live app, but its durable
        // bundle id does — bundle-keyed pruning still reaps it despite the mismatch.
        assert_eq!(table.prune_app(999, Some("com.example")), vec![WindowId::new(2, 1)]);
        assert!(table.is_empty());
    }

    // ---- exact window adoption ----------------------------------------------

    #[test]
    fn try_adopt_window_rewrites_matched_ids_and_falls_through_otherwise() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        // The restored engine arrives pre-populated with the pre-restart ids.
        let old1 = WindowId::new(1, 1);
        let old2 = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[old1, old2]);

        // Stage the durable identities as the restore path would.
        let mut windows = HashMap::default();
        windows.insert(old1, identity(101));
        windows.insert(old2, identity(102));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());

        // Window 1 reappears under a NEW pid (post-restart) but the same server id.
        let new1 = WindowId::new(2, 1);
        register_live(&mut reactor, new1, 101);
        let adopted = reactor.try_adopt_window(new1, space);
        assert!(
            matches!(adopted, Some(Ok(AppRuleResult::Managed(_)))),
            "server-id match adopts the window: {adopted:?}",
        );
        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        // The pre-restart id was rewritten onto the live one, in place.
        assert!(vwm.workspace_for_window_any(old1).is_none(), "old id rewritten away");
        assert!(vwm.workspace_for_window_any(new1).is_some(), "live id inherits the slot");
        // The still-unmatched window keeps its pre-restart id, awaiting its own event.
        assert!(vwm.workspace_for_window_any(old2).is_some());

        // No-match: a window whose server id is not staged falls through (None), so
        // the caller runs normal assignment.
        let stranger = WindowId::new(3, 9);
        register_live(&mut reactor, stranger, 999);
        assert!(reactor.try_adopt_window(stranger, space).is_none(), "unknown server id falls through");

        // Claim-once: server 101 was already consumed, so a second window carrying
        // it also falls through instead of double-adopting.
        let dup = WindowId::new(2, 5);
        register_live(&mut reactor, dup, 101);
        assert!(reactor.try_adopt_window(dup, space).is_none(), "claimed entry is not reused");
    }

    // ---- fuzzy window adoption (reboot) -------------------------------------

    #[test]
    fn fuzzy_adopts_matching_attributes_when_server_id_changed() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        // The pre-reboot engine held this window under old id (1, 1).
        let old = WindowId::new(1, 1);
        place_in_engine(&mut reactor, space, &[old]);

        let frame = CGRect::new(CGPoint::new(0., 0.), CGSize::new(100., 100.));
        let mut windows = HashMap::default();
        // Saved server id 500 will never reappear — the reboot changed them all.
        windows.insert(old, identity_full(500, "com.foo", "Editor", "AXWindow", frame));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());

        // The window returns under a new pid AND a new server id (900), but with the
        // same durable attributes: fuzzy matching re-adopts it.
        let live = WindowId::new(2, 1);
        register_live_full(&mut reactor, live, Some(900), "com.foo", "Editor", "AXWindow", frame);
        let adopted = reactor.try_adopt_window(live, space);
        assert!(
            matches!(adopted, Some(Ok(AppRuleResult::Managed(_)))),
            "attribute match adopts the window across a server-id change: {adopted:?}",
        );

        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert!(vwm.workspace_for_window_any(old).is_none(), "old id rewritten away");
        assert!(vwm.workspace_for_window_any(live).is_some(), "live id inherits the slot");

        // Claim-once: a second identical window finds no unclaimed entry.
        let dup = WindowId::new(2, 2);
        register_live_full(&mut reactor, dup, Some(901), "com.foo", "Editor", "AXWindow", frame);
        assert!(reactor.try_adopt_window(dup, space).is_none(), "claimed entry is not reused");
    }

    #[test]
    fn fuzzy_tiebreak_prefers_closest_frame() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        let near = WindowId::new(1, 1);
        let far = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[near, far]);

        // Two entries agree on every fuzzy attribute but sat at different frames.
        let near_frame = CGRect::new(CGPoint::new(0., 0.), CGSize::new(100., 100.));
        let far_frame = CGRect::new(CGPoint::new(500., 500.), CGSize::new(100., 100.));
        let mut windows = HashMap::default();
        windows.insert(near, identity_full(500, "com.foo", "Doc", "AXWindow", near_frame));
        windows.insert(far, identity_full(501, "com.foo", "Doc", "AXWindow", far_frame));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());

        // The live window's frame is nearest `near_frame`: that candidate wins.
        let live = WindowId::new(2, 1);
        register_live_full(
            &mut reactor,
            live,
            Some(900),
            "com.foo",
            "Doc",
            "AXWindow",
            CGRect::new(CGPoint::new(10., 10.), CGSize::new(100., 100.)),
        );
        assert!(reactor.try_adopt_window(live, space).is_some(), "closest-frame candidate adopted");

        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert!(vwm.workspace_for_window_any(near).is_none(), "closest candidate rewritten onto live id");
        assert!(vwm.workspace_for_window_any(live).is_some());
        assert!(vwm.workspace_for_window_any(far).is_some(), "the farther candidate stays unclaimed");
    }

    #[test]
    fn fuzzy_guards_reject_blank_title_and_missing_bundle() {
        let space = SpaceId::new(1);
        let frame = CGRect::new(CGPoint::new(0., 0.), CGSize::new(100., 100.));

        // Blank title is too ambiguous (mid-launch terminal/browser) — no match.
        {
            let mut reactor = Reactor::new_for_test(fresh_engine());
            let old = WindowId::new(1, 1);
            place_in_engine(&mut reactor, space, &[old]);
            let mut windows = HashMap::default();
            windows.insert(old, identity_full(500, "com.foo", "", "AXWindow", frame));
            reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());
            let live = WindowId::new(2, 1);
            register_live_full(&mut reactor, live, Some(900), "com.foo", "", "AXWindow", frame);
            assert!(reactor.try_adopt_window(live, space).is_none(), "blank title does not fuzzy-match");
        }

        // A missing bundle id (window has none, no app registered) — no match.
        {
            let mut reactor = Reactor::new_for_test(fresh_engine());
            let old = WindowId::new(1, 1);
            place_in_engine(&mut reactor, space, &[old]);
            let mut windows = HashMap::default();
            windows.insert(old, identity_full(500, "com.foo", "Editor", "AXWindow", frame));
            reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());
            let live = WindowId::new(2, 1);
            register_live_full(&mut reactor, live, Some(900), "", "Editor", "AXWindow", frame);
            assert!(
                reactor.try_adopt_window(live, space).is_none(),
                "missing bundle id does not fuzzy-match"
            );
        }
    }

    #[test]
    fn exact_match_beats_fuzzy_candidate() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        let by_attr = WindowId::new(1, 1);
        let by_server = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[by_attr, by_server]);

        let frame = CGRect::new(CGPoint::new(0., 0.), CGSize::new(100., 100.));
        // Two entries share every fuzzy attribute; they differ only by saved server
        // id. On frame+discovery-order tiebreak alone, fuzzy would pick `by_attr`.
        let mut windows = HashMap::default();
        windows.insert(by_attr, identity_full(500, "com.foo", "Doc", "AXWindow", frame));
        windows.insert(by_server, identity_full(900, "com.foo", "Doc", "AXWindow", frame));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());

        // The live window still carries server id 900 (crash/restart, not reboot): it
        // must take the exact entry (`by_server`), never the fuzzy one (`by_attr`).
        let live = WindowId::new(2, 1);
        register_live_full(&mut reactor, live, Some(900), "com.foo", "Doc", "AXWindow", frame);
        assert!(reactor.try_adopt_window(live, space).is_some());

        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert!(vwm.workspace_for_window_any(by_server).is_none(), "exact entry claimed and rewritten");
        assert!(vwm.workspace_for_window_any(live).is_some());
        assert!(
            vwm.workspace_for_window_any(by_attr).is_some(),
            "fuzzy candidate untouched when an exact hit wins"
        );
    }

    #[test]
    fn prune_app_adoptions_keys_on_bundle_after_reboot() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = one_screen_snapshots(space);
        // Pre-reboot the window lived under pid 1.
        let dead = WindowId::new(1, 1);
        place_in_engine(&mut reactor, space, &[dead]);
        let frame = CGRect::new(CGPoint::new(0., 0.), CGSize::new(100., 100.));
        let mut windows = HashMap::default();
        windows.insert(dead, identity_full(500, "com.reboot", "Gone", "AXWindow", frame));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());

        // After a reboot the app relaunches under a NEW pid (7), same bundle, and
        // never brings this window back. Its discovery pass must still prune the
        // stale entry, though pid 7 does not equal the entry's pre-restart pid 1.
        register_app(&mut reactor, 7, "com.reboot");
        reactor.prune_app_adoptions(7);

        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert!(
            vwm.workspace_for_window_any(dead).is_none(),
            "bundle-keyed prune reaps the entry despite the pid change"
        );
    }

    // ---- space remap --------------------------------------------------------

    #[test]
    fn remap_restored_spaces_migrates_state_to_the_live_space_id() {
        let saved_space = SpaceId::new(1);
        let live_space = SpaceId::new(42);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        let w = WindowId::new(1, 1);
        place_in_engine(&mut reactor, saved_space, &[w]);

        // Live screen: same display uuid (test-display-0), but a different SpaceId.
        reactor.space_manager.screens = one_screen_snapshots(live_space);
        let mut saved = HashMap::default();
        saved.insert(saved_space, ("test-display-0".to_string(), 0u32));
        reactor.install_restore_state(AdoptionTable::default(), saved);

        reactor.remap_restored_spaces();

        // The window's per-space engine state moved from the saved id onto the live
        // one, resolved via (uuid, ordinal).
        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert!(vwm.workspace_for_window(live_space, w).is_some(), "state migrated onto the live space");
        assert!(vwm.workspace_for_window(saved_space, w).is_none(), "nothing left on the saved space id");
    }

    // ---- pruning ------------------------------------------------------------

    #[test]
    fn prune_app_adoptions_evicts_windows_the_app_did_not_bring_back() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = one_screen_snapshots(space);
        let keep = WindowId::new(1, 1);
        let dead = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[keep, dead]);

        let mut windows = HashMap::default();
        windows.insert(keep, identity(101));
        windows.insert(dead, identity(102));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());

        // Only `keep` comes back (same pid — a plain rift restart); it is claimed.
        register_live(&mut reactor, keep, 101);
        let _ = reactor.try_adopt_window(keep, space);

        // The app's discovery pass finishes: its still-unclaimed entry (`dead`) is a
        // window that no longer exists and gets pruned from the engine.
        reactor.prune_app_adoptions(1);

        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert!(vwm.workspace_for_window_any(keep).is_some(), "claimed window kept");
        assert!(vwm.workspace_for_window_any(dead).is_none(), "unreturned window pruned, no zombie");
    }

    // ---- topmost re-application (P4) ----------------------------------------

    #[test]
    fn restored_topmost_window_is_repinned_on_adoption() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        // The restored engine held two windows; only one was pinned topmost.
        let pinned = WindowId::new(1, 1);
        let plain = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[pinned, plain]);

        let mut windows = HashMap::default();
        windows.insert(pinned, identity(101));
        windows.insert(plain, identity(102));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());
        // The saved topmost set is keyed by the pre-restart id.
        reactor.persistence.pending_topmost = [pinned].into_iter().collect();

        // Both windows reappear under new ids; adopting the pinned one re-applies
        // topmost onto its live id, mapping the pre-restart id through the rewrite.
        let live_pinned = WindowId::new(2, 1);
        register_live(&mut reactor, live_pinned, 101);
        assert!(reactor.try_adopt_window(live_pinned, space).is_some());
        assert!(
            reactor.topmost_windows.contains_key(&live_pinned),
            "restored topmost pin re-applied onto the live id"
        );
        assert!(
            !reactor.topmost_windows.contains_key(&pinned),
            "the pre-restart id is not left pinned"
        );

        // A window that was not in the saved topmost set is adopted untouched.
        let live_plain = WindowId::new(2, 2);
        register_live(&mut reactor, live_plain, 102);
        assert!(reactor.try_adopt_window(live_plain, space).is_some());
        assert!(
            !reactor.topmost_windows.contains_key(&live_plain),
            "a window that was not topmost stays unpinned"
        );
    }

    #[test]
    fn assemble_persists_only_explicit_topmost_pins() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = one_screen_snapshots(space);
        let explicit = WindowId::new(1, 1);
        let implicit = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[explicit, implicit]);
        register_live(&mut reactor, explicit, 101);
        register_live(&mut reactor, implicit, 102);

        // Pin both, then mark one as an implicit float-sweep pin.
        reactor.pin_topmost_window(explicit);
        reactor.pin_topmost_window(implicit);
        reactor.topmost_windows.get_mut(&implicit).unwrap().implicit = true;

        let snapshot = reactor.assemble_single_snapshot();
        let arrangement = snapshot.arrangements.get("test-display-0").unwrap();
        assert_eq!(
            arrangement.topmost,
            vec![explicit],
            "only the explicit pin is persisted; the implicit one is re-derived after restore"
        );
    }

    #[test]
    fn adoption_drain_finalizes_restore_and_resets_counters() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        let w1 = WindowId::new(1, 1);
        let w2 = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[w1, w2]);
        let mut windows = HashMap::default();
        windows.insert(w1, identity(101));
        windows.insert(w2, identity(102));
        reactor.install_restore_state(AdoptionTable::from_windows(windows), HashMap::default());
        assert!(reactor.persistence.settle_deadline.is_some(), "settle timer armed while adopting");

        let live1 = WindowId::new(2, 1);
        register_live(&mut reactor, live1, 101);
        assert!(reactor.try_adopt_window(live1, space).is_some());
        assert!(
            reactor.persistence.settle_deadline.is_some(),
            "one window still pending: restore not finished"
        );

        // Adopting the last window drains the table, finalizing the restore right
        // away instead of idling until the global settle timeout.
        let live2 = WindowId::new(2, 2);
        register_live(&mut reactor, live2, 102);
        assert!(reactor.try_adopt_window(live2, space).is_some());
        assert!(
            reactor.persistence.settle_deadline.is_none(),
            "drained restore disarms the settle timer"
        );
        assert_eq!(
            reactor.persistence.adopt_exact, 0,
            "the tally is logged once and its counters reset"
        );
    }

    #[test]
    fn custom_workspace_name_roundtrips_through_the_snapshot() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = one_screen_snapshots(space);
        let w = WindowId::new(1, 1);
        place_in_engine(&mut reactor, space, &[w]);
        register_live(&mut reactor, w, 101);

        let ws_id = reactor
            .layout_manager
            .layout_engine
            .virtual_workspace_manager_mut()
            .list_workspaces(space)[0]
            .0;
        reactor
            .layout_manager
            .layout_engine
            .virtual_workspace_manager_mut()
            .rename_workspace(space, ws_id, "mine".to_string());

        let snapshot = reactor.assemble_single_snapshot();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        snapshot.save(&path).unwrap();

        let mut loaded = super::Snapshot::load_or_default(&path);
        let engine = loaded.arrangements.remove("test-display-0").unwrap().engine;
        assert_eq!(
            engine.workspace_name(space, ws_id).as_deref(),
            Some("mine"),
            "custom workspace name survives the snapshot save/load path"
        );
    }

    // ---- SaveAndExit assembler output is loadable ---------------------------

    #[test]
    fn assembled_snapshot_carries_identities_and_reloads() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = one_screen_snapshots(space);
        let w1 = WindowId::new(1, 1);
        let w2 = WindowId::new(1, 2);
        place_in_engine(&mut reactor, space, &[w1, w2]);
        register_live(&mut reactor, w1, 101);
        register_live(&mut reactor, w2, 102);

        // The assembler joins engine state with the reactor's durable identities.
        let snapshot = reactor.assemble_single_snapshot();
        let arrangement =
            snapshot.arrangements.get("test-display-0").expect("arrangement for the live fingerprint");
        let servers: BTreeSet<u32> =
            arrangement.windows.values().map(|w| w.server_id.as_u32()).collect();
        assert_eq!(servers, [101, 102].into_iter().collect(), "both server ids captured");
        assert_eq!(arrangement.spaces.len(), 1, "the single live space was recorded");

        // The SaveAndExit path writes this and it reloads clean — the file the
        // debounced writer produces is never a half-snapshot.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        snapshot.save(&path).unwrap();
        let loaded = super::Snapshot::load_or_default(&path);
        assert!(loaded.arrangements.contains_key("test-display-0"), "assembler output is loadable");
    }

    // ---- arrangement fingerprints (P2) --------------------------------------

    #[test]
    fn fingerprint_tracks_the_display_set_and_ignores_order() {
        let two = two_screen_snapshots(SpaceId::new(1), SpaceId::new(2));
        let one = one_screen_snapshots(SpaceId::new(1));

        // Removing a display yields a different arrangement key.
        assert_ne!(
            Reactor::fingerprint_of(&two),
            Reactor::fingerprint_of(&one),
            "disconnecting a display changes the fingerprint"
        );

        // The same set in a different order maps to the same arrangement.
        let mut reordered = two.clone();
        reordered.reverse();
        assert_eq!(
            Reactor::fingerprint_of(&two),
            Reactor::fingerprint_of(&reordered),
            "reordering the same displays is fingerprint-stable"
        );
    }

    #[test]
    fn churn_gate_suppresses_flush_until_topology_settles() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = one_screen_snapshots(space);
        let w = WindowId::new(1, 1);
        place_in_engine(&mut reactor, space, &[w]);
        register_live(&mut reactor, w, 101);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        reactor.persistence.restore_path = Some(path.clone());

        // Make the debounced save already due (deadline in the past), so the only
        // thing that can hold it back is the churn gate.
        reactor.persistence.debouncer.mark_dirty(Instant::now() - Duration::from_secs(10));

        // Enter display churn, then tick: the write is suppressed and the pending
        // dirty state survives (deadline is NOT consumed).
        reactor.display_topology_manager.begin_churn(1, DisplayReconfigFlags::REMOVE, HashSet::default());
        reactor.persistence_tick();
        assert!(!path.exists(), "no snapshot written while topology is churning");
        assert!(reactor.persistence.debouncer.is_dirty(), "the pending flush is preserved");

        // Topology settles: the next tick flushes exactly once and clears dirty.
        reactor.display_topology_manager.mark_stable();
        reactor.persistence_tick();
        assert!(path.exists(), "the suppressed flush fires once the topology settles");
        assert!(!reactor.persistence.debouncer.is_dirty(), "flushed exactly once");

        // A further tick does not write again (nothing left pending).
        reactor.persistence_tick();
        assert!(!reactor.persistence.debouncer.is_dirty());
    }

    #[test]
    fn switching_displays_saves_the_departing_arrangement_and_the_new_one() {
        let s1 = SpaceId::new(1);
        let s2 = SpaceId::new(2);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = two_screen_snapshots(s1, s2);
        let wa = WindowId::new(1, 1);
        let wb = WindowId::new(1, 2);
        place_in_engine(&mut reactor, s1, &[wa]);
        place_in_engine(&mut reactor, s2, &[wb]);
        register_live(&mut reactor, wa, 101);
        register_live(&mut reactor, wb, 102);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        reactor.persistence.restore_path = Some(path.clone());
        let fp_two = reactor.current_fingerprint();
        reactor.persistence.active_fingerprint = Some(fp_two.clone());

        // Switch to a single display. The pre-switch save captures the intact
        // two-display arrangement; the migrated single-display state is then saved
        // under its own fingerprint (the debounced writer's job, forced here).
        let one = one_screen_snapshots(s1);
        reactor.save_arrangement_before_switch(&one);
        reactor.space_manager.screens = one;
        reactor.load_arrangement_after_switch();
        reactor.flush_snapshot();

        let fp_one = reactor.current_fingerprint();
        let saved = super::Snapshot::load_or_default(&path);
        assert!(saved.arrangements.contains_key(&fp_two), "departing two-display arrangement saved");
        assert!(saved.arrangements.contains_key(&fp_one), "new single-display arrangement saved");

        // The departing arrangement kept both windows' durable identities.
        let servers: BTreeSet<u32> = saved
            .arrangements
            .get(&fp_two)
            .unwrap()
            .windows
            .values()
            .map(|w| w.server_id.as_u32())
            .collect();
        assert_eq!(servers, [101, 102].into_iter().collect(), "both windows saved under the old fingerprint");
    }

    #[test]
    fn reconnecting_a_display_restores_its_saved_arrangement() {
        let s1 = SpaceId::new(1);
        let s2 = SpaceId::new(2);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_manager.screens = two_screen_snapshots(s1, s2);
        let wa = WindowId::new(1, 1);
        let wb = WindowId::new(1, 2);
        place_in_engine(&mut reactor, s1, &[wa]);
        place_in_engine(&mut reactor, s2, &[wb]);
        register_live(&mut reactor, wa, 101);
        register_live(&mut reactor, wb, 102);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        reactor.persistence.restore_path = Some(path.clone());
        let fp_two = reactor.current_fingerprint();
        reactor.persistence.active_fingerprint = Some(fp_two.clone());

        // The two-display arrangement's placement, captured before the round trip.
        let (orig_a, orig_b) = {
            let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
            (
                vwm.workspace_for_window(s1, wa).expect("wa on space1"),
                vwm.workspace_for_window(s2, wb).expect("wb on space2"),
            )
        };
        reactor.flush_snapshot();

        // Unplug the second display. rift's default migration is simulated here by
        // dropping to a fresh engine, so a naive reconnect would lose the layout.
        let one = one_screen_snapshots(s1);
        reactor.save_arrangement_before_switch(&one);
        reactor.space_manager.screens = one;
        reactor.layout_manager.layout_engine = fresh_engine();
        reactor.load_arrangement_after_switch();
        assert!(
            reactor
                .layout_manager
                .layout_engine
                .virtual_workspace_manager()
                .workspace_for_window_any(wa)
                .is_none(),
            "two-display state is gone while the display is unplugged"
        );

        // Replug: the saved two-display arrangement is lifted back into the engine.
        let two = two_screen_snapshots(s1, s2);
        reactor.save_arrangement_before_switch(&two);
        reactor.space_manager.screens = two;
        reactor.load_arrangement_after_switch();
        reactor.remap_restored_spaces();

        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        assert_eq!(
            vwm.workspace_for_window(s1, wa),
            Some(orig_a),
            "wa restored to its original workspace on space1"
        );
        assert_eq!(
            vwm.workspace_for_window(s2, wb),
            Some(orig_b),
            "wb restored to its original workspace on space2"
        );
    }
}
