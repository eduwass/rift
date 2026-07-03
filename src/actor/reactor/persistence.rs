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

use tracing::info;

use super::Reactor;
use crate::actor::app::{WindowId, pid_t};
use crate::common::collections::HashMap;
use crate::common::config;
use crate::common::debounce::Debouncer;
use crate::layout_engine::LayoutEngine;
use crate::layout_engine::snapshot::{Arrangement, Snapshot, WindowIdentity};
use crate::model::virtual_workspace::{AppRuleAssignment, AppRuleResult, WorkspaceError};
use crate::sys::screen::SpaceId;
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
    #[allow(dead_code)]
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

    /// Consume the entry for `server_id` (claim-once).
    fn claim(&mut self, server_id: WindowServerId) {
        self.by_server_id.remove(&server_id);
    }

    /// Remove every unclaimed entry belonging to `pid`, returning their
    /// pre-restart ids so the caller can evict them from the engine.
    fn prune_pid(&mut self, pid: pid_t) -> Vec<WindowId> {
        let keys: Vec<WindowServerId> = self
            .by_server_id
            .iter()
            .filter(|(_, e)| e.old_wid.pid == pid)
            .map(|(k, _)| *k)
            .collect();
        keys.into_iter().filter_map(|k| self.by_server_id.remove(&k)).map(|e| e.old_wid).collect()
    }

    /// Remove and return every remaining entry's pre-restart id.
    fn drain_all(&mut self) -> Vec<WindowId> {
        std::mem::take(&mut self.by_server_id).into_values().map(|e| e.old_wid).collect()
    }
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
    /// When the global adoption settle timeout expires (armed while adopting).
    settle_deadline: Option<Instant>,
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
            settle_deadline: None,
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
        if self.persistence.debouncer.poll(now) && self.can_persist_now() {
            self.flush_snapshot();
        }
        if let Some(deadline) = self.persistence.settle_deadline {
            if now >= deadline {
                self.prune_settled_adoptions();
            }
        }
    }

    /// Whether it is safe to write a snapshot right now. P1 always allows it; the
    /// churn-suppression seam for P2 lives here — while `display_topology` is
    /// churning/awaiting-commit, this should return false so a half-migrated
    /// arrangement is never persisted (the debouncer stays dirty and flushes on
    /// settle).
    fn can_persist_now(&self) -> bool {
        true
    }

    /// Read-modify-write the on-disk snapshot: preserve arrangements for other
    /// display fingerprints, replace the current one. Best-effort — a failed
    /// write just leaves the previous (≤ debounce-stale) file in place.
    fn flush_snapshot(&mut self) {
        let Some(path) = self.persistence.restore_path.clone() else {
            return;
        };
        let mut snapshot = Snapshot::load_or_default(&path);
        snapshot.arrangements.insert(self.current_fingerprint(), self.assemble_arrangement());
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
            let Some(state) = self.state.windows.window(wid) else {
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
            // Topmost persistence + re-apply is P4; carry the set so the data is
            // not lost across saves once that phase re-applies it.
            topmost: self.topmost_windows.keys().copied().collect(),
        }
    }

    /// Sorted, `+`-joined set of connected display uuids — the arrangement key.
    fn current_fingerprint(&self) -> String {
        let mut uuids: Vec<String> = self
            .space_state
            .screens
            .iter()
            .map(|s| s.display_uuid.clone())
            .filter(|u| !u.is_empty())
            .collect();
        uuids.sort();
        uuids.dedup();
        uuids.join("+")
    }

    /// A space's ordinal within its display: its position among the currently
    /// connected screens sharing `uuid`. With one space per display (the common
    /// case and every test) this is always 0.
    fn space_ordinal(&self, space: SpaceId, uuid: &str) -> u32 {
        let mut same_uuid: Vec<SpaceId> = self
            .space_state
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
        if self.persistence.activated || self.space_state.screens.is_empty() {
            return;
        }
        let Some(mut snapshot) = self.persistence.pending_restore.take() else {
            return;
        };
        self.persistence.activated = true;

        let fingerprint = self.current_fingerprint();
        let Some(arrangement) = snapshot.arrangements.remove(&fingerprint) else {
            info!("no saved layout arrangement for display fingerprint {fingerprint:?}; starting fresh");
            return;
        };

        let Arrangement { mut engine, spaces, windows, topmost: _ } = arrangement;
        engine.rehydrate_restored(
            &self.config.virtual_workspaces,
            &self.config.settings.layout,
            Some(self.communication_manager.event_broadcaster.clone()),
        );
        self.layout_manager.layout_engine = engine;
        self.install_restore_state(AdoptionTable::from_windows(windows), spaces);
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
        if arm_settle {
            self.persistence.settle_deadline = Some(Instant::now() + ADOPTION_SETTLE_TIMEOUT);
        }
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
            .space_state
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

    /// Exact-match adoption, consulted before app-rules at discovery time. If the
    /// discovered window's window-server id matches an unclaimed entry whose
    /// restored placement is on `space`, rewrite the pre-restart id onto the live
    /// one (inheriting its exact tree slot) and return the restored assignment.
    /// Returns `None` to fall through to normal assignment.
    pub(super) fn try_adopt_window(
        &mut self,
        wid: WindowId,
        space: SpaceId,
    ) -> Option<Result<AppRuleResult, WorkspaceError>> {
        if self.persistence.adoption.is_empty() {
            return None;
        }
        let server_id = self.state.windows.window(wid)?.info.sys_id?;
        let old_wid = self.persistence.adoption.peek(server_id)?;

        let engine = &self.layout_manager.layout_engine;
        // Only adopt on the space the snapshot placed this window on; otherwise
        // leave the entry for its correct space's discovery pass.
        let workspace_id = engine.virtual_workspace_manager().workspace_for_window(
            &self.state.windows,
            space,
            old_wid,
        )?;
        let floating = engine.is_window_floating(old_wid);

        self.persistence.adoption.claim(server_id);
        if old_wid != wid {
            self.layout_manager.layout_engine.rewrite_window_id(old_wid, wid);
        }
        self.mark_layout_dirty();

        Some(Ok(AppRuleResult::Managed(AppRuleAssignment {
            workspace_id,
            floating,
            prev_rule_decision: false,
        })))
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
        let stale = self.persistence.adoption.prune_pid(pid);
        self.evict_pruned_windows(stale);
    }

    /// Global settle backstop: evict every remaining unclaimed entry (dead apps
    /// that never came back) once the timeout passes.
    fn prune_settled_adoptions(&mut self) {
        self.persistence.settle_deadline = None;
        let stale = self.persistence.adoption.drain_all();
        self.evict_pruned_windows(stale);
    }

    fn evict_pruned_windows(&mut self, stale: Vec<WindowId>) {
        if stale.is_empty() {
            return;
        }
        for wid in stale {
            self.layout_manager.layout_engine.prune_window(wid);
        }
        self.persistence.settle_deadline = if self.persistence.adoption.is_empty() {
            None
        } else {
            self.persistence.settle_deadline
        };
        self.mark_layout_dirty();
        let _ = self.update_layout_or_warn(false, false);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use objc2_core_foundation::{CGPoint, CGRect, CGSize};

    use super::super::testing::*;
    use super::{AdoptionTable, WindowIdentity};
    use crate::actor::app::WindowId;
    use crate::actor::reactor::Reactor;
    use crate::common::collections::HashMap;
    use crate::layout_engine::{LayoutEngine, LayoutEvent};
    use crate::model::reactor::WindowState;
    use crate::model::virtual_workspace::AppRuleResult;
    use crate::sys::app::WindowInfo;
    use crate::sys::screen::{ScreenInfo, SpaceId};
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
        let window_store = &mut reactor.state.windows;
        let _ = engine.handle_event(window_store, LayoutEvent::SpaceExposed(space, CGSize::new(1000., 1000.)));
        let windows = wids
            .iter()
            .map(|wid| (*wid, None, None, None, true, CGSize::new(0., 0.), None, None))
            .collect();
        let _ = engine.handle_event(
            window_store,
            LayoutEvent::WindowsOnScreenUpdated(space, wids[0].pid, windows, None),
        );
    }

    /// Register a live (rediscovered) window in the reactor's OS-mirror manager so
    /// adoption/assembly can read its durable window-server id.
    fn register_live(reactor: &mut Reactor, wid: WindowId, server: u32) {
        reactor
            .state
            .windows
            .insert_window(wid, WindowState::from(win(wid.idx.get(), server)));
    }

    /// A single test screen whose synthesized display uuid is `test-display-0`.
    fn one_screen_snapshots(space: SpaceId) -> Vec<ScreenInfo> {
        make_screen_snapshots(
            vec![CGRect::new(CGPoint::new(0., 0.), CGSize::new(1000., 1000.))],
            vec![Some(space)],
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
    fn adoption_table_prunes_by_pid_and_drains_the_rest() {
        let mut windows = HashMap::default();
        windows.insert(WindowId::new(1, 1), identity(101));
        windows.insert(WindowId::new(1, 2), identity(102));
        windows.insert(WindowId::new(2, 1), identity(201));
        let mut table = AdoptionTable::from_windows(windows);

        // Per-app prune drops every unclaimed entry whose pre-restart pid matches.
        let pruned = sorted(table.prune_pid(1));
        assert_eq!(pruned, vec![WindowId::new(1, 1), WindowId::new(1, 2)]);
        assert_eq!(table.peek(WindowServerId::new(101)), None);
        assert_eq!(table.peek(WindowServerId::new(201)), Some(WindowId::new(2, 1)));

        // The global settle drain empties whatever remains.
        assert_eq!(sorted(table.drain_all()), vec![WindowId::new(2, 1)]);
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
        let windows = &reactor.state.windows;
        // The pre-restart id was rewritten onto the live one, in place.
        assert!(vwm.workspace_for_window_any(windows, old1).is_none(), "old id rewritten away");
        assert!(vwm.workspace_for_window_any(windows, new1).is_some(), "live id inherits the slot");
        // The still-unmatched window keeps its pre-restart id, awaiting its own event.
        assert!(vwm.workspace_for_window_any(windows, old2).is_some());

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

    // ---- space remap --------------------------------------------------------

    #[test]
    fn remap_restored_spaces_migrates_state_to_the_live_space_id() {
        let saved_space = SpaceId::new(1);
        let live_space = SpaceId::new(42);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        let w = WindowId::new(1, 1);
        place_in_engine(&mut reactor, saved_space, &[w]);

        // Live screen: same display uuid (test-display-0), but a different SpaceId.
        reactor.space_state.screens = one_screen_snapshots(live_space);
        let mut saved = HashMap::default();
        saved.insert(saved_space, ("test-display-0".to_string(), 0u32));
        reactor.install_restore_state(AdoptionTable::default(), saved);

        reactor.remap_restored_spaces();

        // The window's per-space engine state moved from the saved id onto the live
        // one, resolved via (uuid, ordinal).
        let vwm = reactor.layout_manager.layout_engine.virtual_workspace_manager();
        let windows = &reactor.state.windows;
        assert!(
            vwm.workspace_for_window(windows, live_space, w).is_some(),
            "state migrated onto the live space"
        );
        assert!(
            vwm.workspace_for_window(windows, saved_space, w).is_none(),
            "nothing left on the saved space id"
        );
    }

    // ---- pruning ------------------------------------------------------------

    #[test]
    fn prune_app_adoptions_evicts_windows_the_app_did_not_bring_back() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_state.screens = one_screen_snapshots(space);
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
        let windows = &reactor.state.windows;
        assert!(vwm.workspace_for_window_any(windows, keep).is_some(), "claimed window kept");
        assert!(vwm.workspace_for_window_any(windows, dead).is_none(), "unreturned window pruned, no zombie");
    }

    // ---- SaveAndExit assembler output is loadable ---------------------------

    #[test]
    fn assembled_snapshot_carries_identities_and_reloads() {
        let space = SpaceId::new(1);
        let mut reactor = Reactor::new_for_test(fresh_engine());
        reactor.space_state.screens = one_screen_snapshots(space);
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
}
