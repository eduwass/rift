//! The Reactor's job is to maintain coherence between the system and model state.
//!
//! It takes events from the rest of the system and builds a coherent picture of
//! what is going on. It shares this with the layout actor, and reacts to layout
//! changes by sending requests out to the other actors in the system.

mod animation;
mod events;
mod main_window;
mod managers;
mod persistence;
mod query;
mod replay;
pub mod transaction_manager;
mod utils;

#[cfg(test)]
mod testing;

#[cfg(test)]
#[allow(non_snake_case)]
mod SpaceEventHandler {
    pub use super::events::space::WindowServerLifecyclePayload;

    pub fn handle_window_server_destroyed(
        reactor: &mut super::Reactor,
        payload: WindowServerLifecyclePayload,
    ) -> anyhow::Result<super::EventOutcome> {
        let wsid = payload.window_server_id;
        let tracked_window = reactor.state.windows.tracked_window_id(wsid);
        let assigned_space =
            tracked_window.and_then(|window| reactor.assigned_space_for_window_id(window));
        let observations = super::events::space::WindowServerDestroyedObservations {
            resolved_space: reactor.resolve_native_space(wsid, None),
            active_spaces: reactor.active_spaces.clone(),
            mission_control_active: reactor.is_mission_control_active(),
            ordered_in: crate::sys::window_server::window_is_ordered_in(wsid),
            assigned_space,
            last_known_user_space: super::events::space::resolve_last_known_user_space(
                tracked_window.and_then(|window| reactor.best_space_for_window_id(window)),
                reactor.space_state.iter_known_spaces().next(),
            ),
        };
        let outcome = super::events::space::handle_window_server_destroyed(
            &mut reactor.state,
            &reactor.transaction_manager,
            &mut reactor.drag_manager,
            payload,
            observations,
        )?;
        reactor.apply_event_outcome(outcome);
        Ok(super::EventOutcome::default())
    }

    pub fn handle_window_server_appeared(
        reactor: &mut super::Reactor,
        window_server_id: crate::sys::window_server::WindowServerId,
        space: crate::sys::screen::SpaceId,
        kind: super::SpaceEventKind,
    ) {
        reactor.handle_event(super::Event::WindowServerAppeared(window_server_id, space, kind));
    }
}

#[cfg(test)]
mod tests;

use std::thread;
use std::time::{Duration, Instant};

use animation::Sender as AnimationSender;
use dispatchr::queue;
use dispatchr::time::Time;
use events::{
    EventOutcome, app as application_workflow, command as command_workflow,
    drag as interaction_workflow, focus as focus_service, space as topology_workflow,
    system as system_workflow, window as window_workflow,
};
use main_window::MainWindowTracker;
use managers::LayoutManager;
use objc2_app_kit::NSRunningApplication;
use objc2_core_foundation::{CGPoint, CGRect, CGSize};
pub use replay::{Record, replay};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::{debug, info, instrument, trace, warn};
use transaction_manager::TransactionId;

use super::{event_tap, gesture_tap};
use crate::actor::app::{AppInfo, AppThreadHandle, Quiet, RaiseKind, Request, WindowId, WindowInfo, pid_t};
use crate::actor::raise_manager::{self, RaiseManager, RaiseRequest};
use crate::actor::reactor::events::window_discovery;
use crate::actor::spaces::{ForwardedSpaceState, TopologyWindowDelta};
use crate::actor::{self, menu_bar, stack_line};
use crate::common::collections::{BTreeMap, HashMap, HashSet};
use crate::common::config::Config;
use crate::layout_engine::{self as layout, Direction, LayoutEngine, LayoutEvent};
use crate::model::RiftState;
use crate::model::broadcast::{BroadcastEvent, BroadcastSender};
use crate::sys::dispatch::DispatchExt;
use crate::model::space_activation::{SpaceActivationConfig, SpaceActivationPolicy};
use crate::model::tx_store::WindowTxStore;
use crate::model::virtual_workspace::AppRuleResult;
use crate::sys::event::MouseState;
use crate::sys::executor::Executor;
use crate::sys::geometry::{CGRectDef, CGRectExt};
pub use crate::sys::screen::ScreenInfo;
use crate::sys::screen::{SpaceId, order_visible_spaces_by_position};
use crate::sys::window_server::{
    self, WindowServerId, WindowServerInfo, current_cursor_location, window_level,
    window_sub_level,
};

pub(crate) fn topmost_sample_points(frame: CGRect) -> [CGPoint; 5] {
    let min_x = frame.origin.x;
    let min_y = frame.origin.y;
    let max_x = frame.origin.x + frame.size.width;
    let max_y = frame.origin.y + frame.size.height;
    let mid_x = frame.origin.x + frame.size.width / 2.0;
    let mid_y = frame.origin.y + frame.size.height / 2.0;
    let inset_x = (frame.size.width / 4.0).min(24.0);
    let inset_y = (frame.size.height / 4.0).min(24.0);
    [
        CGPoint::new(mid_x, mid_y),
        CGPoint::new(min_x + inset_x, min_y + inset_y),
        CGPoint::new(max_x - inset_x, min_y + inset_y),
        CGPoint::new(min_x + inset_x, max_y - inset_y),
        CGPoint::new(max_x - inset_x, max_y - inset_y),
    ]
}

pub type Sender = actor::Sender<Event>;
type Receiver = actor::Receiver<Event>;
use managers::RefreshQuarantineState;
pub use query::ReactorQueryHandle;

pub(crate) use crate::model::reactor::{AppState, WindowFilter, WindowState};
pub use crate::model::reactor::{
    Command, DisplaySelector, DragSession, DragState, MenuState, MissionControlState,
    ReactorCommand, RefocusState, Requested, StaleCleanupState, WorkspaceSwitchOrigin,
    WorkspaceSwitchState,
};

#[derive(Clone)]
pub struct ReactorHandle {
    sender: Sender,
    queries: ReactorQueryHandle,
}

impl ReactorHandle {
    pub fn new(sender: Sender, queries: ReactorQueryHandle) -> Self { Self { sender, queries } }

    pub fn sender(&self) -> Sender { self.sender.clone() }

    pub fn send(&self, event: Event) { self.sender.send(event) }

    pub fn try_send(
        &self,
        event: Event,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<(tracing::Span, Event)>> {
        self.sender.try_send(event)
    }
}

impl std::ops::Deref for ReactorHandle {
    type Target = ReactorQueryHandle;

    fn deref(&self) -> &Self::Target { &self.queries }
}

use crate::model::server::WindowData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpaceEventKind {
    User,
    Fullscreen,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    #[serde(skip)]
    SpaceStateChanged(ForwardedSpaceState),

    /// An application was launched. This event is also sent for every running
    /// application on startup.
    ///
    /// Both WindowInfo (accessibility) and WindowServerInfo are collected for
    /// any already-open windows when the launch event is sent. Since this
    /// event isn't ordered with respect to the Space events, it is possible to
    /// receive this event for a space we just switched off of.. FIXME. The same
    /// is true of WindowCreated events.
    ApplicationLaunched {
        pid: pid_t,
        info: AppInfo,
        #[serde(skip, default = "replay::deserialize_app_thread_handle")]
        handle: AppThreadHandle,
        is_frontmost: bool,
        main_window: Option<WindowId>,
        visible_windows: Vec<(WindowId, WindowInfo)>,
        window_server_info: Vec<WindowServerInfo>,
    },
    ApplicationTerminated(pid_t),
    ApplicationThreadTerminated(pid_t),
    ApplicationActivated(pid_t, Quiet),
    ApplicationDeactivated(pid_t),
    ApplicationGloballyActivated(pid_t),
    ApplicationGloballyDeactivated(pid_t),
    ApplicationMainWindowChanged(pid_t, Option<WindowId>, Quiet),

    WindowsDiscovered {
        pid: pid_t,
        new: Vec<(WindowId, WindowInfo)>,
        known_visible: Vec<WindowId>,
    },
    WindowCreated(
        WindowId,
        WindowInfo,
        Option<WindowServerInfo>,
        Option<MouseState>,
    ),
    WindowDestroyed(WindowId),
    #[serde(skip)]
    WindowServerDestroyed(
        crate::sys::window_server::WindowServerId,
        SpaceId,
        SpaceEventKind,
    ),
    #[serde(skip)]
    WindowServerAppeared(
        crate::sys::window_server::WindowServerId,
        SpaceId,
        SpaceEventKind,
    ),
    #[serde(skip)]
    SpaceCreated(SpaceId),
    #[serde(skip)]
    SpaceDestroyed(SpaceId),
    WindowMinimized(WindowId),
    WindowDeminiaturized(WindowId),
    WindowFrameChanged(
        WindowId,
        #[serde(with = "CGRectDef")] CGRect,
        Option<TransactionId>,
        Requested,
        Option<MouseState>,
    ),
    WindowTitleChanged(WindowId, String),
    /// Re-assert a window's tile frame a short time after a cross-display move.
    ///
    /// macOS can CLAMP the move's SetWindowFrame to the source display at apply-time (the window
    /// can't exceed `source.max_x - new_origin_x` until it has been adopted by the target display),
    /// leaving it stuck narrow while rift's optimistic `frame_monotonic` still reads the requested
    /// full frame — so every later re-tile is a dedup no-op. This event is scheduled (via a GCD
    /// `after`) by the move handler and fires once the window has certainly landed on the target
    /// display, where a forced re-tile is no longer clamped and sticks.
    ReassertDisplayMove(WindowId),
    ReassertTopmost,

    /// Periodic tick: reclaim space from windows that closed without notifying rift
    /// (see [`Reactor::reconcile_orphan_windows`]).
    ReconcileOrphans,

    /// Periodic tick driving the debounced layout-snapshot save and the adoption
    /// settle timeout (see [`Reactor::persistence_tick`]).
    #[serde(skip)]
    PersistTick,
    MenuOpened(pid_t),
    MenuClosed(pid_t),

    /// Left mouse button was released.
    ///
    /// Layout changes are suppressed while the button is down so that they
    /// don't interfere with drags. This event is used to update the layout in
    /// case updates were supressed while the button was down.
    ///
    /// FIXME: This can be interleaved incorrectly with the MouseState in app
    /// actor events.
    MouseUp,
    /// A mouse button was pressed. Used as an early topmost-reassert trigger:
    /// the system's click-raise (which can bury a pinned window) happens on
    /// mouse-down, well before mouse-up.
    MouseDown,
    /// Sent by the event tap only when the cursor enters a different window.
    /// Window resolution and transition deduplication stay on the input
    /// thread; the reactor only applies the model-dependent focus/raise work.
    MouseMoved(WindowServerId),
    /// The mouse cursor moved within the active desktop. Used to remember the
    /// latest point per workspace even when focus-follows-mouse does not emit a
    /// window-change event.
    CursorMoved,
    /// Forwarded by the spaces actor after wake has been observed.
    ///
    /// The spaces actor is the authority for sleep/lock/display lifecycle.
    /// The reactor uses this only to reopen refresh gating and resubscribe
    /// WindowServer notifications once the topology authority says wake
    /// processing has advanced.
    SystemWoke,
    #[serde(skip)]
    SystemWillSleep,
    #[serde(skip)]
    SessionDidResignActive,
    #[serde(skip)]
    SessionDidBecomeActive,

    #[serde(skip)]
    DisplayChurnBegin,
    #[serde(skip)]
    DisplayChurnEnd,

    #[serde(skip)]
    MissionControlNativeEntered,
    #[serde(skip)]
    MissionControlNativeExited,

    /// A raise request completed. Used by the raise manager to track when
    /// all raise requests in a sequence have finished.
    RaiseCompleted {
        window_id: WindowId,
        sequence_id: u64,
    },

    /// A raise sequence timed out. Used by the raise manager to clean up
    /// pending raises that took too long.
    RaiseTimeout {
        sequence_id: u64,
    },

    #[serde(skip)]
    Query(query::QueryRequest),

    Command(Command),

    #[serde(skip)]
    RegisterWmSender(crate::actor::wm_controller::Sender),

    #[serde(skip)]
    ConfigUpdated(Config),
}

pub struct Reactor {
    pub config: Config,
    pub one_space: bool,
    app_manager: managers::AppManager,
    layout_manager: managers::LayoutManager,
    pub(crate) state: RiftState,
    space_state: ForwardedSpaceState,
    space_activation_policy: SpaceActivationPolicy,
    main_window_tracker: MainWindowTracker,
    drag_manager: managers::DragManager,
    workspace_switch_manager: managers::WorkspaceSwitchManager,
    recording_manager: managers::RecordingManager,
    communication_manager: managers::CommunicationManager,
    notification_manager: managers::NotificationManager,
    transaction_manager: transaction_manager::TransactionManager,
    menu_manager: managers::MenuManager,
    mission_control_manager: managers::MissionControlManager,
    refocus_manager: managers::RefocusManager,
    refresh_quarantine_manager: managers::RefreshQuarantineManager,
    pending_space_change_manager: managers::PendingSpaceChangeManager,
    active_spaces: HashSet<SpaceId>,
    pub animation_tx: Option<AnimationSender>,
    // After move-window-to-display, the cursor warp must wait until the window has physically
    // landed on its destination; warping immediately lands on a neighbour (because the move is
    // async) and focus-follows-mouse then steals focus. Holds (window, destination display rect,
    // deadline) and fires once the window's centre is inside that rect or the deadline passes.
    pending_display_move_warp: Option<(WindowId, CGRect, std::time::Instant)>,
    topmost_windows: HashMap<WindowId, TopmostWindowState>,
    /// Windows explicitly un-pinned via toggle-topmost while
    /// `floating_windows_topmost` is on, so the float sweep doesn't re-pin them.
    topmost_optout: HashSet<WindowId>,
    persistence: persistence::PersistenceState,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TopmostWindowState {
    last_reassert: Option<Instant>,
    failed_reasserts: u8,
    /// True when the pin came from `floating_windows_topmost` rather than an
    /// explicit toggle; implicit pins follow the window's floating state.
    implicit: bool,
}

const TOPMOST_REASSERT_DEBOUNCE: Duration = Duration::from_millis(200);
/// Raise attempts per burial episode: 1 order-only + 2 escalations, then wait
/// for the next user-driven trigger. Never unpins.
const TOPMOST_MAX_FAILED_REASSERTS: u8 = 3;

impl Reactor {
    pub fn spawn(
        config: Config,
        layout_engine: LayoutEngine,
        record: Record,
        event_tap_tx: event_tap::Sender,
        broadcast_tx: BroadcastSender,
        menu_tx: menu_bar::Sender,
        stack_line_tx: stack_line::Sender,
        window_notify: Option<(crate::actor::window_notify::Sender, WindowTxStore)>,
        gesture_tap_tx: Option<gesture_tap::Sender>,
        one_space: bool,
    ) -> ReactorHandle {
        let (events_tx, events) = actor::channel();
        let events_tx_clone = events_tx.clone();
        let mut reactor = Reactor::new(
            config,
            layout_engine,
            record,
            broadcast_tx,
            window_notify,
            one_space,
        );
        reactor.communication_manager.event_tap_tx = Some(event_tap_tx);
        reactor.menu_manager.menu_tx = Some(menu_tx);
        reactor.communication_manager.stack_line_tx = Some(stack_line_tx);
        reactor.communication_manager.gesture_tap_tx = gesture_tap_tx;
        reactor.communication_manager.events_tx = Some(events_tx_clone.clone());
        reactor.enable_persistence(crate::common::config::restore_file());
        let query_handle = ReactorQueryHandle::new(events_tx_clone.clone());
        thread::Builder::new()
            .name("reactor".to_string())
            .spawn(move || {
                Executor::run(Reactor::run(reactor, events, events_tx_clone));
            })
            .unwrap();
        ReactorHandle::new(events_tx, query_handle)
    }

    pub fn new(
        config: Config,
        layout_engine: LayoutEngine,
        mut record: Record,
        broadcast_tx: BroadcastSender,
        window_notify: Option<(crate::actor::window_notify::Sender, WindowTxStore)>,
        one_space: bool,
    ) -> Reactor {
        // Restored windows whose apps did not come back are pruned by the
        // adoption settle pipeline (see reactor/persistence.rs).
        record.start(&config, &layout_engine);
        let (raise_manager_tx, _rx) = actor::channel();
        let (window_notify_tx, window_tx_store) = match window_notify {
            Some((tx, store)) => (Some(tx), store),
            None => (None, WindowTxStore::new()),
        };
        let reactor = Reactor {
            config: config.clone(),
            one_space,
            app_manager: managers::AppManager::new(),
            layout_manager: managers::LayoutManager { layout_engine },
            state: RiftState::default(),
            space_state: ForwardedSpaceState::default(),
            space_activation_policy: SpaceActivationPolicy::new(),
            main_window_tracker: MainWindowTracker::default(),
            drag_manager: managers::DragManager {
                drag_state: DragState::Inactive,
                drag_swap_manager: crate::actor::drag_swap::DragManager::new(
                    config.settings.window_snapping,
                ),
                skip_layout_for_window: None,
            },
            workspace_switch_manager: managers::WorkspaceSwitchManager {
                workspace_switch_state: WorkspaceSwitchState::Inactive,
                workspace_switch_generation: 0,
                active_workspace_switch: None,
                pending_workspace_switch_origin: None,
                pending_workspace_cursor_warp: None,
                pending_workspace_mouse_warp: None,
                saved_workspace_cursors: HashMap::default(),
            },
            recording_manager: managers::RecordingManager { record },
            communication_manager: managers::CommunicationManager {
                event_tap_tx: None,
                gesture_tap_tx: None,
                stack_line_tx: None,
                raise_manager_tx,
                event_broadcaster: broadcast_tx,
                wm_sender: None,
                events_tx: None,
            },
            notification_manager: managers::NotificationManager {
                last_sls_notification_ids: Vec::new(),
                last_layout_modes_by_space: HashMap::default(),
                _window_notify_tx: window_notify_tx,
            },
            transaction_manager: transaction_manager::TransactionManager::new(window_tx_store),
            menu_manager: managers::MenuManager {
                menu_state: MenuState::Closed,
                menu_tx: None,
            },
            mission_control_manager: managers::MissionControlManager {
                mission_control_state: MissionControlState::Inactive,
                pending_mission_control_refresh: HashSet::default(),
            },
            refocus_manager: managers::RefocusManager {
                stale_cleanup_state: StaleCleanupState::Enabled,
                refocus_state: RefocusState::None,
            },
            refresh_quarantine_manager: managers::RefreshQuarantineManager {
                sleeping: false,
                session_inactive: false,
                display_churn_active: false,
                awaiting_post_wake_snapshot: false,
                awaiting_post_session_snapshot: false,
                pending_visible_refresh: false,
                deferred_refresh_tracks_mission_control: false,
            },
            pending_space_change_manager: managers::PendingSpaceChangeManager {
                pending_space_change: None,
            },
            active_spaces: HashSet::default(),
            animation_tx: None,
            pending_display_move_warp: None,
            topmost_windows: HashMap::default(),
            topmost_optout: HashSet::default(),
            persistence: persistence::PersistenceState::default(),
        };
        reactor
    }

    fn set_active_spaces(&mut self, spaces: &[Option<SpaceId>]) {
        self.active_spaces.clear();
        for space in spaces.iter().flatten().copied() {
            self.active_spaces.insert(space);
        }
    }

    fn is_space_active(&self, space: SpaceId) -> bool { self.active_spaces.contains(&space) }

    fn iter_active_spaces(&self) -> impl Iterator<Item = SpaceId> + '_ {
        self.active_spaces.iter().copied()
    }

    fn active_space_ids(&self) -> Vec<u64> {
        self.active_spaces.iter().map(|space| space.get()).collect()
    }

    fn is_window_on_active_space(&self, wid: WindowId) -> bool {
        self.best_space_for_window_id(wid)
            .is_some_and(|space| self.is_space_active(space))
    }

    fn activation_cfg(&self) -> SpaceActivationConfig {
        SpaceActivationConfig {
            default_disable: self.config.settings.default_disable,
            one_space: self.one_space,
        }
    }

    fn screens_for_current_spaces(&self) -> Vec<ScreenInfo> { self.space_state.screens.clone() }

    fn display_uuids_for_current_screens(&self) -> Vec<Option<String>> {
        self.space_state
            .screens
            .iter()
            .map(|screen| screen.display_uuid_owned())
            .collect()
    }

    #[cfg(test)]
    fn raw_spaces_for_current_screens(&self) -> Vec<Option<SpaceId>> {
        self.space_state.screens.iter().map(|s| s.space).collect()
    }

    fn display_uuid_for_space(&self, space: SpaceId) -> Option<String> {
        self.space_state
            .screen_by_space(space)
            .and_then(|screen| screen.display_uuid_owned())
    }

    fn expose_space_if_known(&mut self, space: SpaceId) {
        let Some(screen) = self.space_state.screen_by_space(space) else {
            return;
        };
        self.layout_manager
            .layout_engine
            .virtual_workspace_manager_mut()
            .list_workspaces(space);
        self.send_layout_event(LayoutEvent::SpaceExposed(space, screen.frame.size));
    }

    fn recompute_and_set_active_spaces(&mut self, spaces: &[Option<SpaceId>]) {
        let cfg = self.activation_cfg();
        let display_uuids = self.display_uuids_for_current_screens();
        let active_spaces =
            self.space_activation_policy.compute_active_spaces(cfg, spaces, &display_uuids);
        let previous_active = self.active_spaces.clone();
        self.set_active_spaces(&active_spaces);
        self.handle_active_space_change(previous_active);
    }

    fn recompute_and_set_active_spaces_from_current_screens(&mut self) {
        let raw_spaces = self.authoritative_spaces_for_current_screens();
        self.recompute_and_set_active_spaces(&raw_spaces);
    }

    fn authoritative_spaces_for_current_screens(&self) -> Vec<Option<SpaceId>> {
        self.space_state
            .screens
            .iter()
            .map(|screen| {
                screen.space.filter(|space| self.space_state.active_spaces.contains(space))
            })
            .collect()
    }

    fn handle_active_space_change(&mut self, previous_active: HashSet<SpaceId>) {
        if previous_active == self.active_spaces {
            return;
        }

        let deactivated: Vec<SpaceId> =
            previous_active.difference(&self.active_spaces).copied().collect();
        let activated: Vec<SpaceId> =
            self.active_spaces.difference(&previous_active).copied().collect();

        // Do not remove windows when a space is merely deactivated (e.g. macOS Space
        // switches). Removing them clears workspace assignments and causes windows
        // without app rules to be re-assigned to the current workspace.

        if !activated.is_empty() {
            for space in &activated {
                self.expose_space_if_known(*space);
            }
        }

        if !activated.is_empty() || !deactivated.is_empty() {
            self.refresh_window_server_snapshot_for_active_spaces();
            self.check_for_new_windows();
        }

        if !activated.is_empty() {
            self.apply_app_rules_for_activated_spaces(&activated);
        }
    }

    fn apply_app_rules_for_activated_spaces(&mut self, activated: &[SpaceId]) {
        let activated_set: HashSet<SpaceId> = activated.iter().copied().collect();
        let mut windows_by_pid: HashMap<pid_t, Vec<WindowId>> = HashMap::default();

        for (wid, state) in self.state.windows.iter_windows() {
            if !state.matches_filter(WindowFilter::Manageable) {
                continue;
            }
            let Some(space) = self.best_space_for_window_id(wid) else {
                continue;
            };

            if !activated_set.contains(&space) {
                continue;
            }

            windows_by_pid.entry(wid.pid).or_default().push(wid);
        }

        for (pid, window_ids) in windows_by_pid {
            let Some(app_state) = self.app_manager.apps.get(&pid) else {
                continue;
            };

            self.process_windows_for_app_rules(pid, window_ids, app_state.info.clone());
        }
    }

    fn refresh_window_server_snapshot_for_active_spaces(&mut self) {
        let active_windows = self.authoritative_active_space_windows();
        self.reconcile_authoritative_active_window_snapshot(active_windows, false);
    }

<<<<<<< HEAD
    fn orphan_reconcile_outcome(&mut self) -> EventOutcome {
        let mut outcome = EventOutcome::finalized_event(None, false, false, false);
||||||| parent of 5fdc286 (fix: reap minimized windows for dead apps)
    /// Re-query an app's visible windows so the stale-window reconciliation in
    /// `WindowsDiscovered` can reap any window AX/the window server no longer reports.
    /// Called from the focus/main-window-change paths for a fast reap in the common
    /// case; the periodic sweep in [`Self::reconcile_orphan_windows`] is the guaranteed
    /// backstop for apps that emit no event at all on close (e.g. Slack).
    fn schedule_orphan_reconcile(&mut self, pid: pid_t) {
        if let Some(app) = self.app_manager.apps.get(&pid) {
            let _ = app.handle.send(Request::GetVisibleWindows);
        }
    }

    /// Reliably reclaim space left by windows that closed without telling rift.
    ///
    /// Some apps — notably Electron ones (Slack, ChatGPT, …) — order a window out on
    /// close instead of destroying it, stay alive, and emit no destroy/focus/main-window
    /// notification at all, so no event-driven path fires. Run on a timer: any window rift
    /// still tiles on the active workspace that is no longer on screen is an orphan.
    /// Re-query just those apps so the existing `WindowsDiscovered` reconciliation reaps
    /// them. Scoped to the active workspace so windows parked on inactive workspaces
    /// (legitimately off screen) never trigger work.
    fn reconcile_orphan_windows(&mut self) {
=======
    /// Re-query an app's visible windows so the stale-window reconciliation in
    /// `WindowsDiscovered` can reap any window AX/the window server no longer reports.
    /// Called from the focus/main-window-change paths for a fast reap in the common
    /// case; the periodic sweep in [`Self::reconcile_orphan_windows`] is the guaranteed
    /// backstop for apps that emit no event at all on close (e.g. Slack).
    fn schedule_orphan_reconcile(&mut self, pid: pid_t) {
        if let Some(app) = self.app_manager.apps.get(&pid) {
            let _ = app.handle.send(Request::GetVisibleWindows);
        }
    }

    /// Reliably reclaim space left by windows that closed without telling rift.
    ///
    /// Some apps — notably Electron ones (Slack, ChatGPT, …) — order a window out on
    /// close instead of destroying it, stay alive, and emit no destroy/focus/main-window
    /// notification at all, so no event-driven path fires. Run on a timer: any window rift
    /// still tiles on the active workspace that is no longer on screen is an orphan.
    /// Re-query just those apps so the existing `WindowsDiscovered` reconciliation reaps
    /// them. Scoped to the active workspace so windows parked on inactive workspaces
    /// (legitimately off screen) never trigger work.
    ///
    /// Dead-process backstop: every pid rift still tracks (app actor or window state)
    /// is liveness-checked, deliberately ignoring the workspace/minimized/on-screen
    /// scoping above — a window of a dead process is garbage wherever it is parked.
    /// Without this, an Electron app that orders its window out on close (marking it
    /// minimized) and quits later without any notification leaves a ghost node tiling
    /// empty screen space forever. Restored-but-unadopted windows are unaffected:
    /// restore populates only the layout trees, never these maps.
    fn reconcile_orphan_windows(&mut self) {
>>>>>>> 5fdc286 (fix: reap minimized windows for dead apps)
        if self.is_mission_control_active() || self.is_in_drag() {
            return outcome;
        }
        let mut tracked_pids: HashSet<pid_t> = self.app_manager.apps.keys().copied().collect();
        tracked_pids.extend(self.window_manager.windows.keys().map(|wid| wid.pid));
        let mut dead_pids: HashSet<pid_t> = HashSet::default();
        for pid in tracked_pids {
            if NSRunningApplication::runningApplicationWithProcessIdentifier(pid).is_none() {
                dead_pids.insert(pid);
            }
        }

        let on_screen: HashSet<WindowServerId> =
            window_server::get_visible_windows_with_layer(None).into_iter().map(|i| i.id).collect();
        let mut pids: HashSet<pid_t> = HashSet::default();
<<<<<<< HEAD
        let mut dead_pids: HashSet<pid_t> = HashSet::default();
        for space in self.iter_active_spaces() {
            for wid in self
                .layout_manager
                .layout_engine
                .windows_in_active_workspace(&self.state.windows, space)
            {
||||||| parent of 5fdc286 (fix: reap minimized windows for dead apps)
        let mut dead_pids: HashSet<pid_t> = HashSet::default();
        for space in self.active_spaces.clone() {
            for wid in self.layout_manager.layout_engine.windows_in_active_workspace(space) {
=======
        for space in self.active_spaces.clone() {
            for wid in self.layout_manager.layout_engine.windows_in_active_workspace(space) {
                if dead_pids.contains(&wid.pid) {
                    continue;
                }
>>>>>>> 5fdc286 (fix: reap minimized windows for dead apps)
                if self.layout_manager.layout_engine.is_window_floating(wid) {
                    continue;
                }
                let Some(window) = self.state.windows.window(wid) else {
                    continue;
                };
                if window.info.is_minimized {
                    continue;
                }
                if let Some(ws_id) = window.info.sys_id
                    && !on_screen.contains(&ws_id)
                {
                    pids.insert(wid.pid);
                }
            }
        }
        for pid in dead_pids {
            self.app_manager.apps.remove(&pid);
            let dead_windows: Vec<WindowId> = self
                .state
                .windows
                .iter_windows()
                .filter_map(|(wid, _)| (wid.pid == pid).then_some(wid))
                .collect();
            for wid in dead_windows {
                if let Ok(destroyed) = window_workflow::handle_window_destroyed(
                    &mut self.state,
                    &self.transaction_manager,
                    &mut self.drag_manager,
                    window_workflow::WindowDestroyedPayload {
                        window: wid,
                        suppress_if_window_alive: false,
                        platform_window_alive: false,
                    },
                ) {
                    outcome.absorb(destroyed);
                }
            }
            outcome = outcome.with_layout_event(LayoutEvent::AppClosed(pid));
        }
        for pid in pids {
            if self.app_manager.apps.contains_key(&pid) {
                outcome = outcome.with_app_request(pid, Request::GetVisibleWindows);
            }
        }
        outcome
    }

    /// Rebuild only the on-screen window-server id set from live state, without touching
    /// cached frames/info. Apps that order a window out on close without emitting a
    /// destroy notification otherwise leave a stale id in the visible set, which blocks
    /// orphan reconciliation. Cheap enough to run before each discovery pass.
    pub(crate) fn refresh_visible_windows_snapshot(&mut self) {
        self.state.windows.clear_visible_windows();
        self.state.windows.set_visible_windows(
            window_server::get_visible_windows_with_layer(None).into_iter().map(|info| info.id),
        );
    }

    fn authoritative_active_space_windows(&self) -> Vec<(WindowServerId, Option<SpaceId>)> {
        let mut queried = HashMap::default();
        for space in self.iter_active_spaces() {
            for wsid in window_server::space_window_list_for_connection(&[space.get()], 0, false)
                .into_iter()
                .map(WindowServerId::new)
            {
                queried.entry(wsid).or_insert(space);
            }
        }

        // A refresh can be partial while WindowServer is waking. Keep the last
        // forwarded per-space sample in that case, but never use the global
        // visible-window union as a substitute for querying each active space.
        let membership = if queried.is_empty() {
            self.space_state.active_window_spaces.clone()
        } else {
            queried
        };

        let mut membership: Vec<_> = membership
            .into_iter()
            .map(|(wsid, space)| (wsid, self.resolve_native_space(wsid, Some(space))))
            .collect();
        membership.sort_by_key(|(wsid, _)| *wsid);
        membership
    }

    fn has_known_windows_for_active_spaces(&self) -> bool {
        self.state.windows.iter_windows().any(|(wid, _)| {
            self.authoritative_space_for_window_id(wid)
                .is_some_and(|space| self.is_space_active(space))
        })
    }

    fn refresh_active_space_window_membership(
        &mut self,
        active_windows: Vec<(WindowServerId, Option<SpaceId>)>,
    ) {
        let active_wsids: HashSet<WindowServerId> =
            active_windows.iter().map(|(wsid, _)| *wsid).collect();

        // An empty active-space list is valid, but an empty WS-id result while we
        // already know about windows assigned to the active space is typically the
        // transient post-wake race on same-display space switches. Preserve the
        // existing visibility basis in that case and let the follow-up AX refresh
        // reconcile instead of blanking the workspace immediately.
        if active_wsids.is_empty() && self.has_known_windows_for_active_spaces() {
            return;
        }

        let previously_visible_wsids: Vec<_> =
            self.state.windows.iter_visible_window_server_ids().collect();
        for wsid in previously_visible_wsids {
            if !active_wsids.contains(&wsid) {
                self.state.windows.mark_window_hidden(wsid);
            }
        }

        for (wsid, space) in active_windows {
            let space = self.resolve_native_space(wsid, space);
            if let Some(space) = space {
                self.state.windows.set_window_server_space(wsid, Some(space));
                self.clear_pending_target_if_confirmed_space(wsid, space);
            }
            self.state.windows.mark_window_visible(wsid);
            self.state.windows.clear_window_server_observed(wsid);
        }
    }

    fn remove_windows_missing_from_active_space_snapshot(
        &mut self,
        previously_visible_wsids: Vec<WindowServerId>,
        preserve_assignments: bool,
    ) {
        for wsid in previously_visible_wsids {
            if self.state.windows.is_window_visible(wsid) {
                continue;
            }
            let Some(wid) = self.state.windows.tracked_window_id(wsid) else {
                continue;
            };
            let Some(space) = self.assigned_space_for_window_id(wid) else {
                continue;
            };
            if !self.is_space_active(space) {
                continue;
            }

            let inactive_target = self
                .resolve_native_space(wsid, None)
                .filter(|current_space| *current_space != space)
                .filter(|current_space| {
                    #[cfg(test)]
                    {
                        let _ = current_space;
                        true
                    }
                    #[cfg(not(test))]
                    {
                        window_server::space_is_user(current_space.get())
                    }
                })
                .filter(|current_space| !self.is_space_active(*current_space));
            if let Some(current_space) = inactive_target {
                self.state.windows.set_window_server_space(wsid, Some(current_space));
                let _ = self.reassign_window_to_authoritative_space(wid, current_space);
                continue;
            }

            if preserve_assignments {
                debug!(
                    ?wid,
                    ?wsid,
                    "Preserving workspace assignment omitted from partial authoritative snapshot"
                );
                continue;
            }

            // If the authoritative active-space snapshot no longer includes a
            // previously visible window and WindowServer cannot confirm a new
            // native space for it, drop the stale origin-space ownership. Keeping
            // the old assignment lets later discovery/MC refresh rebuild the
            // origin layout from stale workspace state.
            self.state.windows.set_window_server_space(wsid, None);
            self.send_layout_event(LayoutEvent::WindowRemoved(wid));
        }
    }

    fn reconcile_authoritative_active_window_snapshot(
        &mut self,
        active_windows: Vec<(WindowServerId, Option<SpaceId>)>,
        preserve_missing_assignments: bool,
    ) {
        let previously_visible_wsids: Vec<_> =
            self.state.windows.iter_visible_window_server_ids().collect();
        self.refresh_active_space_window_membership(active_windows);
        self.remove_windows_missing_from_active_space_snapshot(
            previously_visible_wsids,
            preserve_missing_assignments,
        );
        self.reconcile_windows_with_authoritative_spaces();
    }

    fn is_login_window_pid(&self, pid: pid_t) -> bool {
        self.app_manager.apps.get(&pid).and_then(|a| a.info.bundle_id.as_deref())
            == Some("com.apple.loginwindow")
    }

    // fn store_txid(&self, wsid: Option<WindowServerId>, txid: TransactionId, target: CGRect) {
    //     self.transaction_manager.store_txid(wsid, txid, target);
    // }
    //
    // fn update_txid_entries<I>(&self, entries: I)
    // where
    //     I: IntoIterator<Item = (WindowServerId, TransactionId, CGRect)>,
    // {
    //     self.transaction_manager.update_entries(entries);
    // }
    //
    // fn remove_txid_for_window(&self, wsid: Option<WindowServerId>) {
    //     self.transaction_manager.remove_for_window(wsid);
    // }

    fn clear_pending_hidden_window_targets(&self) {
        for (wid, window) in self.state.windows.iter_windows() {
            if self.hidden_assigned_space_for_window_id(wid).is_none() {
                continue;
            }
            if let Some(wsid) = window.info.sys_id {
                self.transaction_manager.clear_target_for_window(wsid);
            }
        }
    }

    fn clear_pending_target_if_confirmed_space(
        &self,
        wsid: WindowServerId,
        confirmed_space: SpaceId,
    ) {
        if self.pending_target_space_for_window_server_id(wsid) == Some(confirmed_space) {
            self.transaction_manager.clear_target_for_window(wsid);
        }
    }

    fn is_in_drag(&self) -> bool {
        matches!(
            self.drag_manager.drag_state,
            DragState::Active { .. } | DragState::PendingSwap { .. }
        )
    }

    fn is_mission_control_active(&self) -> bool {
        matches!(
            self.mission_control_manager.mission_control_state,
            MissionControlState::Active
        )
    }

    fn get_pending_drag_swap(&self) -> Option<(WindowId, WindowId)> {
        if let DragState::PendingSwap { session, target } = &self.drag_manager.drag_state {
            Some((session.window, *target))
        } else {
            None
        }
    }

    fn get_active_drag_session(&self) -> Option<&DragSession> {
        if let DragState::Active { session } = &self.drag_manager.drag_state {
            Some(session)
        } else {
            None
        }
    }

    fn take_active_drag_session(&mut self) -> Option<DragSession> {
        match std::mem::replace(&mut self.drag_manager.drag_state, DragState::Inactive) {
            DragState::Active { session } => Some(session),
            DragState::PendingSwap { session, .. } => Some(session),
            _ => None,
        }
    }

    async fn run(mut reactor: Reactor, events: Receiver, events_tx: Sender) {
        let (raise_manager_tx, raise_manager_rx) = actor::channel();
        let (animation_tx, animation_rx) = tokio::sync::mpsc::unbounded_channel();
        reactor.communication_manager.raise_manager_tx = raise_manager_tx.clone();
        reactor.animation_tx = Some(animation_tx);
        let event_tap_tx = reactor.communication_manager.event_tap_tx.clone();
        let reconcile_tx = events_tx.clone();
        let persist_tx = events_tx.clone();
        let reactor_task = Self::run_reactor_loop(reactor, events);
        let raise_manager_task = RaiseManager::run(raise_manager_rx, events_tx, event_tap_tx);
        let animation_task = animation::AnimationManager::run(animation_rx);
        let reconcile_task = async move {
            loop {
                crate::sys::timer::Timer::sleep(Duration::from_millis(1000)).await;
                if reconcile_tx.try_send(Event::ReconcileOrphans).is_err() {
                    break;
                }
            }
        };
        // Drive the debounced layout-snapshot save and the adoption settle timeout.
        let persist_task = async move {
            loop {
                crate::sys::timer::Timer::sleep(Duration::from_millis(500)).await;
                if persist_tx.try_send(Event::PersistTick).is_err() {
                    break;
                }
            }
        };
        let _ = tokio::join!(
            reactor_task,
            raise_manager_task,
            animation_task,
            reconcile_task,
            persist_task
        );
    }

    async fn run_reactor_loop(mut reactor: Reactor, mut events: Receiver) {
        const MAX_EVENT_BATCH: usize = 64;

        while let Some((span, event)) = events.recv().await {
            let _guard = span.enter();
            reactor.handle_loop_event(event);
            // Drain a bounded batch to reduce recv/select overhead.
            for _ in 1..MAX_EVENT_BATCH {
                let Ok((span, event)) = events.try_recv() else {
                    break;
                };
                let _guard = span.enter();
                reactor.handle_loop_event(event);
            }
        }
    }

    fn handle_loop_event(&mut self, event: Event) {
        if let Event::Query(req) = event {
            self.handle_query_request(req);
            return;
        }
        if self.should_quarantine_space_lifecycle_event(&event) {
            trace!(?event, state = ?self.refresh_quarantine_state(), "quarantined space lifecycle event");
            return;
        }
        if self.should_quarantine_during_display_churn(&event) {
            trace!(?event, "quarantined during display churn");
            return;
        }
        Self::note_windowserver_activity(&event);
        self.handle_event(event);
        #[cfg(any(test, debug_assertions))]
        self.state.windows.debug_assert_invariants();
    }

    fn note_windowserver_activity(event: &Event) {
        let wsid = match event {
            Event::WindowFrameChanged(wid, ..) => Some(wid.idx.get()),
            Event::WindowCreated(wid, ..) => Some(wid.idx.get()),
            Event::WindowDestroyed(wid) => Some(wid.idx.get()),
            Event::WindowMinimized(wid) => Some(wid.idx.get()),
            Event::WindowDeminiaturized(wid) => Some(wid.idx.get()),
            Event::MouseMoved(_) => None,
            Event::WindowServerDestroyed(wsid, ..) => Some(wsid.as_u32()),
            Event::WindowServerAppeared(wsid, ..) => Some(wsid.as_u32()),
            _ => None,
        };
        if let Some(wsid) = wsid {
            window_server::note_windowserver_activity(wsid);
        }
    }

    fn log_event(&self, event: &Event) {
        match event {
            Event::WindowFrameChanged(..) | Event::MouseUp | Event::MouseDown
                | Event::MouseMoved(_) | Event::CursorMoved => {
                trace!(?event, "Event")
            }
            _ => debug!(?event, "Event"),
        }
    }

    fn should_update_notifications(event: &Event) -> bool {
        matches!(
            event,
            Event::WindowCreated(..)
                | Event::WindowDestroyed(..)
                | Event::WindowServerDestroyed(..)
                | Event::WindowServerAppeared(..)
                | Event::WindowsDiscovered { .. }
                | Event::ApplicationLaunched { .. }
                | Event::ApplicationTerminated(..)
                | Event::ApplicationThreadTerminated(..)
                | Event::SpaceStateChanged(..)
        )
    }

    fn should_quarantine_during_display_churn(&self, event: &Event) -> bool {
        if !crate::sys::display_churn::is_active() {
            return false;
        }

        matches!(
            event,
            Event::WindowCreated(..)
                | Event::WindowDestroyed(..)
                | Event::WindowServerDestroyed(..)
                | Event::WindowServerAppeared(..)
                | Event::WindowFrameChanged(..)
                | Event::WindowMinimized(..)
                | Event::WindowDeminiaturized(..)
                | Event::WindowTitleChanged(..)
                | Event::WindowsDiscovered { .. }
                | Event::SpaceCreated(..)
                | Event::SpaceDestroyed(..)
        )
    }

    fn should_quarantine_space_lifecycle_event(&self, event: &Event) -> bool {
        self.refreshes_blocked()
            && matches!(event, Event::SpaceCreated(..) | Event::SpaceDestroyed(..))
    }

    fn refresh_quarantine_state(&self) -> RefreshQuarantineState {
        self.refresh_quarantine_manager.state()
    }

    fn refreshes_blocked(&self) -> bool { self.refresh_quarantine_manager.blocks_refreshes() }

    fn defer_visible_refresh(&mut self, track_mission_control_refresh: bool) {
        self.refresh_quarantine_manager.pending_visible_refresh = true;
        self.refresh_quarantine_manager.deferred_refresh_tracks_mission_control |=
            track_mission_control_refresh;
    }

    fn flush_deferred_visible_refresh(&mut self) {
        if self.refreshes_blocked() || !self.refresh_quarantine_manager.pending_visible_refresh {
            return;
        }

        let track_mission_control_refresh =
            self.refresh_quarantine_manager.deferred_refresh_tracks_mission_control;
        self.refresh_quarantine_manager.pending_visible_refresh = false;
        self.refresh_quarantine_manager.deferred_refresh_tracks_mission_control = false;
        self.request_visible_windows_for_apps(track_mission_control_refresh);
    }

    // All lifecycle churn is upstreamed through the spaces actor. The reactor
    // only remembers that one visibility refresh is owed, then flushes it once
    // every upstream gate is open again.
    fn request_refresh_when_spaces_actor_stabilizes(&mut self) {
        self.defer_visible_refresh(true);
        self.flush_deferred_visible_refresh();
    }

    fn release_post_instability_quarantine_after_authoritative_snapshot(&mut self) {
        let released_wake = self.refresh_quarantine_manager.awaiting_post_wake_snapshot;
        let released_session = self.refresh_quarantine_manager.awaiting_post_session_snapshot;

        if !released_wake && !released_session {
            return;
        }

        self.refresh_quarantine_manager.awaiting_post_wake_snapshot = false;
        self.refresh_quarantine_manager.awaiting_post_session_snapshot = false;
        if released_wake {
            self.refresh_quarantine_manager.sleeping = false;
        }
        if released_session {
            self.refresh_quarantine_manager.session_inactive = false;
        }
        self.flush_deferred_visible_refresh();
    }

    #[instrument(name = "reactor::handle_event", skip(self), fields(event=?event))]
    fn handle_event(&mut self, event: Event) {
        let should_reassert_topmost = matches!(
            &event,
            Event::ApplicationActivated(_, _)
                | Event::ApplicationMainWindowChanged(_, _, _)
                | Event::MouseMoved(_)
                | Event::MouseUp
                | Event::MouseDown
        );
        let schedule_topmost_verification = matches!(&event, Event::RaiseCompleted { .. });
        match self.dispatch_workflow(event) {
            Ok(outcome) => self.apply_event_outcome(outcome),
            Err(error) => warn!(%error, "reactor workflow failed"),
        }
        if schedule_topmost_verification {
            self.schedule_topmost_verification();
        }
        if should_reassert_topmost {
            self.schedule_topmost_reassert();
        }
    }

    /// Dispatches one event and returns all ordered follow-up work without
    /// applying it. This is the migration boundary used by the individual
    /// workflow modules.
    fn dispatch_workflow(&mut self, event: Event) -> anyhow::Result<EventOutcome> {
        self.log_event(&event);
        self.recording_manager.record.on_event(&event);

        match event {
            Event::SystemWillSleep => {
                self.refresh_quarantine_manager.sleeping = true;
                self.refresh_quarantine_manager.awaiting_post_wake_snapshot = false;
                return Ok(EventOutcome::default());
            }
            Event::SystemWoke => {
                self.refresh_quarantine_manager.sleeping = true;
                self.refresh_quarantine_manager.awaiting_post_wake_snapshot = true;
                let outcome = system_workflow::handle_system_woke()?;
                self.defer_visible_refresh(true);
                return Ok(outcome);
            }
            Event::SessionDidResignActive => {
                self.refresh_quarantine_manager.session_inactive = true;
                self.refresh_quarantine_manager.awaiting_post_session_snapshot = false;
                return Ok(EventOutcome::default());
            }
            Event::SessionDidBecomeActive => {
                self.refresh_quarantine_manager.session_inactive = true;
                self.refresh_quarantine_manager.awaiting_post_session_snapshot = true;
                self.defer_visible_refresh(true);
                return Ok(EventOutcome::default());
            }
            Event::DisplayChurnBegin => {
                self.refresh_quarantine_manager.display_churn_active = true;
                return Ok(EventOutcome::default());
            }
            Event::DisplayChurnEnd => {
                self.refresh_quarantine_manager.display_churn_active = false;
                self.request_refresh_when_spaces_actor_stabilizes();
                return Ok(EventOutcome::default());
            }
            _ => {}
        }

        let should_update_notifications = Self::should_update_notifications(&event);

        let main_window_changed = match &event {
            Event::ApplicationMainWindowChanged(_, Some(wid), Quiet::No) => Some(*wid),
            _ => None,
        };
        let raised_window = self.main_window_tracker.handle_event(&event);
        if let Some(wid) = main_window_changed {
            self.workspace_switch_manager.pending_workspace_mouse_warp = Some(wid);
        }
        match event {
            Event::ApplicationLaunched {
                pid,
                info,
                handle,
                visible_windows,
                window_server_info,
                is_frontmost,
                main_window,
            } => {
                let _ = (is_frontmost, main_window);
                let mut outcome = application_workflow::handle_application_launched(
                    &mut self.app_manager,
                    application_workflow::ApplicationLaunchedPayload {
                        pid,
                        info,
                        handle,
                        visible_windows,
                        window_server_info,
                    },
                )?;
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::ApplicationTerminated(pid) => {
                return application_workflow::handle_application_terminated(pid);
            }
            Event::ApplicationThreadTerminated(pid) => {
                self.clear_menu_state_for_pid(pid);
                return application_workflow::handle_application_thread_terminated(
                    &mut self.app_manager,
                    pid,
                );
            }
            Event::ApplicationActivated(pid, quiet) => {
                self.clear_menu_state_for_non_owner(pid);
                let mut outcome = application_workflow::handle_application_activated(
                    application_workflow::ApplicationActivatedPayload { pid, quiet },
                )?;
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::ApplicationDeactivated(pid) => {
                self.clear_menu_state_for_pid(pid);
                if self.app_manager.apps.contains_key(&pid) {
                    return Ok(EventOutcome::finalized_event(None, false, false, false)
                        .with_app_request(pid, Request::GetVisibleWindows));
                }
            }
            Event::ApplicationGloballyDeactivated(pid) => {
                self.clear_menu_state_for_pid(pid);
            }
            Event::ApplicationGloballyActivated(pid) => {
                self.clear_menu_state_for_non_owner(pid);
                if !self.is_login_window_pid(pid) {
                    self.request_visible_windows_for_pid(pid, false);
                    if self.main_window_tracker.take_global_activation_quiet(pid) == Quiet::No {
                        self.handle_app_activation_workspace_switch(pid);
                    } else {
                        debug!(
                            pid,
                            "Skipping auto workspace switch for quiet global activation (initiated by Rift)"
                        );
                    }
                }
            }
            Event::RegisterWmSender(sender) => {
                return Ok(system_workflow::handle_register_wm_sender(
                    &mut self.communication_manager,
                    sender,
                )?);
            }
            Event::WindowsDiscovered { pid, new, known_visible } => {
                if self.refreshes_blocked() {
                    debug!(
                        pid,
                        state = ?self.refresh_quarantine_state(),
                        "Ignoring windows discovery while refresh quarantine is active"
                    );
                    self.defer_visible_refresh(true);
                    return Ok(EventOutcome::default());
                }
                let mut outcome = application_workflow::handle_windows_discovered(
                    application_workflow::WindowsDiscoveredPayload { pid, new, known_visible },
                )?;
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::WindowCreated(wid, window, ws_info, mouse_state) => {
                let _ = mouse_state;
                let mut outcome = window_workflow::handle_window_created(
                    &mut self.state,
                    &self.transaction_manager,
                    window_workflow::WindowCreatedPayload {
                        window_id: wid,
                        window,
                        window_server_info: ws_info,
                    },
                )?;
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::WindowDestroyed(wid) => {
                let window_server_id =
                    self.state.windows.window(wid).and_then(|window| window.info.sys_id);

                // AX emits spurious destroy notifications for windows whose
                // elements are transiently invalidated while the session is
                // locked, the system is sleeping, or displays are churning
                // (e.g. AXError -25202 during unlock). The window server is
                // authoritative in those states: if it still knows the window,
                // keep its state (and workspace assignment) and let the
                // post-quarantine refresh reconcile.
                let suppress_if_window_alive = !self.has_user_space_context()
                    || self.is_mission_control_active()
                    || self.refreshes_blocked();

                let platform_window_alive = window_server_id.is_some_and(|window_server_id| {
                    window_server::get_window(window_server_id)
                        .is_some_and(|info| info.pid == wid.pid)
                });
                let mut outcome = window_workflow::handle_window_destroyed(
                    &mut self.state,
                    &self.transaction_manager,
                    &mut self.drag_manager,
                    window_workflow::WindowDestroyedPayload {
                        window: wid,
                        suppress_if_window_alive,
                        platform_window_alive,
                    },
                )?;
                self.topmost_windows.remove(&wid);
                self.topmost_optout.remove(&wid);
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::WindowServerDestroyed(wsid, sid, kind) => {
                let tracked_window = self.state.windows.tracked_window_id(wsid);
                let assigned_space =
                    tracked_window.and_then(|window| self.assigned_space_for_window_id(window));
                let last_known_user_space = topology_workflow::resolve_last_known_user_space(
                    tracked_window.and_then(|window| self.best_space_for_window_id(window)),
                    self.space_state.iter_known_spaces().next(),
                );
                let observations = topology_workflow::WindowServerDestroyedObservations {
                    resolved_space: self.resolve_native_space(wsid, None),
                    active_spaces: self.active_spaces.clone(),
                    mission_control_active: self.is_mission_control_active(),
                    ordered_in: window_server::window_is_ordered_in(wsid),
                    assigned_space,
                    last_known_user_space,
                };
                return topology_workflow::handle_window_server_destroyed(
                    &mut self.state,
                    &self.transaction_manager,
                    &mut self.drag_manager,
                    topology_workflow::WindowServerLifecyclePayload {
                        window_server_id: wsid,
                        space: sid,
                        kind,
                    },
                    observations,
                );
            }
            Event::WindowServerAppeared(wsid, sid, kind) => {
                let tracked_window = self.state.windows.tracked_window_id(wsid);
                let assigned_space =
                    tracked_window.and_then(|window| self.assigned_space_for_window_id(window));
                let last_known_user_space = topology_workflow::resolve_last_known_user_space(
                    tracked_window.and_then(|window| self.best_space_for_window_id(window)),
                    self.space_state.iter_known_spaces().next(),
                );
                let window_server_info = window_server::get_window(wsid);
                let owner_pid = window_server_info.as_ref().map(|info| info.pid);
                let app_known =
                    owner_pid.is_some_and(|pid| self.app_manager.apps.contains_key(&pid));
                let running_app_info = owner_pid.filter(|_| !app_known).and_then(|pid| {
                    objc2_app_kit::NSRunningApplication::runningApplicationWithProcessIdentifier(
                        pid,
                    )
                    .map(|app| AppInfo::from(&*app))
                });
                let observations = topology_workflow::WindowServerAppearedObservations {
                    resolved_space: self.resolve_native_space(wsid, Some(sid)),
                    active_spaces: self.active_spaces.clone(),
                    mission_control_active: self.is_mission_control_active(),
                    assigned_space,
                    last_known_user_space,
                    window_server_info,
                    app_known,
                    running_app_info,
                };
                return topology_workflow::handle_window_server_appeared(
                    &mut self.state,
                    topology_workflow::WindowServerLifecyclePayload {
                        window_server_id: wsid,
                        space: sid,
                        kind,
                    },
                    observations,
                );
            }
            Event::SpaceCreated(space) => {
                return topology_workflow::handle_space_lifecycle(
                    &mut self.space_activation_policy,
                    topology_workflow::SpaceLifecyclePayload { space, created: true },
                );
            }
            Event::SpaceDestroyed(space) => {
                return topology_workflow::handle_space_lifecycle(
                    &mut self.space_activation_policy,
                    topology_workflow::SpaceLifecyclePayload { space, created: false },
                );
            }
            Event::WindowMinimized(wid) => {
                self.topmost_windows.remove(&wid);
                return window_workflow::handle_window_minimized(&mut self.state, wid);
            }
            Event::WindowDeminiaturized(wid) => {
                let active_space = self.state.windows.window(wid).and_then(|window| {
                    self.best_space_for_window(&window.frame_monotonic, window.info.sys_id)
                        .filter(|space| self.is_space_active(*space))
                        .or_else(|| {
                            window
                                .info
                                .sys_id
                                .is_none()
                                .then(|| self.workspace_command_space())
                                .flatten()
                        })
                });
                return window_workflow::handle_window_deminiaturized(
                    &mut self.state,
                    window_workflow::WindowDeminiaturizedPayload { window: wid, active_space },
                );
            }
            Event::WindowFrameChanged(wid, new_frame, last_seen, requested, mouse_state) => {
                let effective_mouse_state = mouse_state.or_else(crate::sys::event::get_mouse_state);
                let (server_id, old_frame) = self
                    .state
                    .windows
                    .window(wid)
                    .map(|window| (window.info.sys_id, window.frame_monotonic))
                    .unwrap_or((None, new_frame));
                let old_space = self.geometry_space_for_window(&old_frame, server_id);
                let new_space = self.geometry_space_for_window(&new_frame, server_id);
                let old_space_active = old_space.is_some_and(|space| self.is_space_active(space));
                let new_space_active = new_space.is_some_and(|space| self.is_space_active(space));
                let best_resize_space = self.best_space_for_window(&new_frame, server_id);
                let active_resize_space =
                    best_resize_space.filter(|space| self.is_space_active(*space)).or_else(|| {
                        server_id.is_none().then(|| self.workspace_command_space()).flatten()
                    });
                let pending_target_space = server_id
                    .and_then(|server| self.pending_target_space_for_window_server_id(server));
                let assigned_space = self.assigned_space_for_window_id(wid);
                let keep_assigned_for_scrolling = old_space.is_some_and(|space| {
                    self.layout_manager.layout_engine.active_layout_mode_at(space)
                        == crate::common::config::LayoutMode::Scrolling
                        && !self.layout_manager.layout_engine.is_window_floating(wid)
                        && self
                            .layout_manager
                            .layout_engine
                            .virtual_workspace_manager()
                            .workspace_for_window(&self.state.windows, space, wid)
                            .is_some()
                });
                let screens = self
                    .space_state
                    .screens
                    .iter()
                    .filter_map(|screen| {
                        Some((screen.space?, screen.frame, screen.display_uuid_owned()))
                    })
                    .collect();
                let mission_control_active = self.is_mission_control_active();
                let mut outcome = window_workflow::handle_window_frame_changed(
                    &mut self.state,
                    &mut self.layout_manager,
                    &self.transaction_manager,
                    &mut self.drag_manager,
                    window_workflow::WindowFrameChangedPayload {
                        window: wid,
                        new_frame,
                        last_seen,
                        requested,
                        mouse_state: effective_mouse_state,
                        mission_control_active,
                        old_space,
                        new_space,
                        old_space_active,
                        new_space_active,
                        active_resize_space,
                        pending_target_space,
                        assigned_space,
                        keep_assigned_for_scrolling,
                        screens,
                    },
                )?;
                // Frame acknowledgements and no-op geometry changes can return
                // early from the reducer. Mouse release still has to terminate
                // an existing drag session in those cases.
                if effective_mouse_state == Some(crate::sys::event::MouseState::Up)
                    && (matches!(
                        self.drag_manager.drag_state,
                        DragState::Active { .. } | DragState::PendingSwap { .. }
                    ) || self.drag_manager.skip_layout_for_window.is_some())
                {
                    outcome.dispatch_mouse_up = true;
                }
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::WindowTitleChanged(wid, new_title) => {
                let mut outcome = window_workflow::handle_window_title_changed(
                    &mut self.state,
                    window_workflow::WindowTitleChangedPayload { window: wid, title: new_title },
                )?;
                outcome.focused_window = raised_window;
                return Ok(outcome);
            }
            Event::SpaceStateChanged(space_state) => {
                let releases_lifecycle_refresh_quarantine =
                    space_state.releases_lifecycle_refresh_quarantine;
                let releases_display_churn_refresh_quarantine =
                    space_state.releases_display_churn_refresh_quarantine;
                let outcome = self.handle_authoritative_space_snapshot(space_state)?;
                if releases_lifecycle_refresh_quarantine {
                    self.release_post_instability_quarantine_after_authoritative_snapshot();
                }
                if releases_display_churn_refresh_quarantine {
                    self.refresh_quarantine_manager.display_churn_active = false;
                    self.request_refresh_when_spaces_actor_stabilizes();
                }
                return Ok(outcome);
            }
            Event::MouseDown => {}
            Event::MouseUp => {
                let pending_swap = self.get_pending_drag_swap();
                let (visible_spaces, visible_space_centers) = self.visible_spaces_for_layout(true);
                let swap_space = pending_swap
                    .and_then(|(dragged, _)| {
                        self.state.windows.window(dragged).and_then(|window| {
                            self.best_space_for_window(&window.frame_monotonic, window.info.sys_id)
                        })
                    })
                    .or_else(|| {
                        self.drag_manager
                            .drag_swap_manager
                            .origin_frame()
                            .and_then(|frame| self.best_space_for_frame(&frame))
                    })
                    .or_else(|| self.space_state.screens.iter().find_map(|screen| screen.space));
                let session = match &self.drag_manager.drag_state {
                    DragState::Active { session } | DragState::PendingSwap { session, .. } => {
                        Some(session.clone())
                    }
                    DragState::Inactive => None,
                };
                let final_space = session.as_ref().and_then(|session| {
                    session
                        .settled_space
                        .or_else(|| self.best_space_for_frame(&session.last_frame))
                        .or_else(|| self.best_space_for_window_id(session.window))
                });
                let focused = self.window_id_under_cursor().and_then(|window| {
                    self.best_space_for_window_id(window).map(|space| (space, window))
                });
                let mut outcome = interaction_workflow::handle_mouse_up(
                    &mut self.state,
                    &mut self.layout_manager,
                    &mut self.drag_manager,
                    interaction_workflow::MouseUpPayload {
                        pending_swap,
                        swap_space,
                        final_space,
                        visible_spaces,
                        visible_space_centers,
                    },
                )?;
                if let Some((space, window)) = focused {
                    outcome = outcome.with_layout_event(LayoutEvent::WindowFocused(space, window));
                }
                return Ok(outcome);
            }
            Event::MenuOpened(pid) => {
                return Ok(system_workflow::handle_menu_opened(&mut self.menu_manager, pid)?);
            }
            Event::MenuClosed(pid) => {
                return Ok(system_workflow::handle_menu_closed(&mut self.menu_manager, pid)?);
            }
            Event::MouseMoved(wsid) => {
                let window = self.state.windows.tracked_window_id(wsid);
                let active_space = window.and_then(|window| {
                    self.state.windows.window(window).and_then(|state| {
                        self.best_space_for_window(&state.frame_monotonic, state.info.sys_id)
                            .filter(|space| self.is_space_active(*space))
                            .or_else(|| {
                                state
                                    .info
                                    .sys_id
                                    .is_none()
                                    .then(|| self.workspace_command_space())
                                    .flatten()
                            })
                    })
                });
                return window_workflow::handle_mouse_moved_over_window(
                    &self.app_manager,
                    window_workflow::MouseMovedPayload {
                        window,
                        should_sync: window
                            .is_some_and(|window| self.should_raise_on_mouse_over(window)),
                        is_main: window.is_some_and(|window| self.main_window() == Some(window)),
                        needs_layout_sync: window.is_some_and(|window| {
                            self.layout_manager.layout_engine.focused_window() != Some(window)
                        }),
                        active_space,
                    },
                );
            }
            Event::CursorMoved => {
                if self.workspace_switch_manager.workspace_switch_state
                    == WorkspaceSwitchState::Inactive
                {
                    self.save_cursor_for_cursor_workspace();
                }
                return;
            }
            Event::MissionControlNativeEntered => {
                let outcome = topology_workflow::handle_mission_control_native_entered(
                    &mut self.mission_control_manager,
                    &mut self.drag_manager,
                )?;
                self.broadcast_native_mission_control_entered();
                return Ok(outcome);
            }
            Event::MissionControlNativeExited => {
                let outcome = topology_workflow::handle_mission_control_native_exited(
                    &mut self.mission_control_manager,
                )?;
                self.broadcast_native_mission_control_exited();
                return Ok(outcome);
            }
            Event::RaiseCompleted { window_id, sequence_id } => {
                return Ok(system_workflow::handle_raise_completed(
                    system_workflow::RaiseCompletedPayload {
                        window: window_id,
                        sequence: sequence_id,
                    },
                )?);
            }
            Event::RaiseTimeout { sequence_id } => {
                return Ok(system_workflow::handle_raise_timeout(sequence_id)?);
            }
            Event::ConfigUpdated(new_cfg) => {
                return command_workflow::handle_config_updated(
                    &mut self.config,
                    &mut self.layout_manager,
                    &self.state,
                    &mut self.drag_manager,
                    new_cfg,
                );
            }
            Event::Command(Command::Metrics(cmd)) => {
                return command_workflow::handle_command_metrics(cmd);
            }
            Event::Command(Command::Reactor(ReactorCommand::Debug)) => {
                return command_workflow::handle_command_reactor_debug(
                    &self.layout_manager,
                    &self.space_state,
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::SaveAndExit)) => {
                match self.save_snapshot_now() {
                    Ok(()) => std::process::exit(0),
                    Err(e) => {
                        error!("Could not save layout: {e}");
                        std::process::exit(3);
                    }
                }
            }
            Event::Command(Command::Reactor(ReactorCommand::Serialize)) => {
                let serialized = self.serialize_state();
                return command_workflow::handle_command_reactor_serialize(serialized);
            }
            Event::Command(Command::Reactor(ReactorCommand::SwitchSpace(direction))) => {
                return command_workflow::handle_switch_native_space(direction);
            }
            Event::Command(Command::Reactor(ReactorCommand::ToggleSpaceActivated)) => {
                let space = self.active_display_space();
                let display_uuid = space.and_then(|space| {
                    self.space_state
                        .screen_by_space(space)
                        .and_then(|screen| screen.display_uuid_owned())
                });
                let config = self.activation_cfg();
                return command_workflow::handle_command_reactor_toggle_space_activated(
                    &mut self.space_activation_policy,
                    command_workflow::ToggleSpacePayload { config, space, display_uuid },
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::ShowMissionControlAll)) => {
                return command_workflow::handle_mission_control_command(
                    crate::actor::wm_controller::WmCmd::ShowMissionControlAll,
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::ShowMissionControlCurrent)) => {
                return command_workflow::handle_mission_control_command(
                    crate::actor::wm_controller::WmCmd::ShowMissionControlCurrent,
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::DismissMissionControl)) => {
                return command_workflow::handle_mission_control_command(
                    crate::actor::wm_controller::WmCmd::DismissMissionControl,
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::CloseWindow { window_server_id })) => {
                return command_workflow::handle_close_window(window_server_id);
            }
            Event::Command(Command::Reactor(ReactorCommand::ToggleTopmostWindow)) => {
                self.handle_toggle_topmost_window();
                return Ok(EventOutcome::finalized_event(None, false, false, false));
            }
            Event::Command(Command::Reactor(ReactorCommand::FocusWindow {
                window_id,
                window_server_id,
            })) => {
                let resolved_space = self.best_space_for_window_id(window_id).or_else(|| {
                    self.state.windows.window(window_id).and_then(|window| {
                        self.best_space_for_window(&window.frame_monotonic, window.info.sys_id)
                    })
                });
                return command_workflow::handle_command_reactor_focus_window(
                    &self.state,
                    &self.app_manager,
                    command_workflow::FocusWindowPayload {
                        window_id,
                        window_server_id,
                        resolved_space,
                        space_is_active: resolved_space
                            .is_some_and(|space| self.is_space_active(space)),
                    },
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::MoveMouseToDisplay(selector))) => {
                let screen = self.screen_for_selector(&selector, None).cloned();
                let focus_window = screen.as_ref().and_then(|screen| {
                    let space = screen.space?;
                    self.last_focused_window_in_space(space).or_else(|| {
                        self.layout_manager
                            .layout_engine
                            .windows_in_active_workspace(&self.state.windows, space)
                            .into_iter()
                            .next()
                    })
                });
                let target_is_active = screen
                    .as_ref()
                    .and_then(|screen| screen.space)
                    .is_none_or(|space| self.is_space_active(space));
                return command_workflow::handle_move_mouse_to_display(
                    command_workflow::DisplayFocusPayload {
                        screen,
                        target_is_active,
                        focus_window,
                    },
                );
            }
            Event::Command(Command::Reactor(ReactorCommand::FocusDisplay(selector))) => {
                let screen = self.screen_for_selector(&selector, None).cloned();
                let focus_window = screen.as_ref().and_then(|screen| {
                    let space = screen.space?;
                    self.last_focused_window_in_space(space).or_else(|| {
                        self.layout_manager
                            .layout_engine
                            .windows_in_active_workspace(&self.state.windows, space)
                            .into_iter()
                            .next()
                    })
                });
                let target_is_active = screen
                    .as_ref()
                    .and_then(|screen| screen.space)
                    .is_none_or(|space| self.is_space_active(space));
                return command_workflow::handle_focus_display(
                    command_workflow::DisplayFocusPayload {
                        screen,
                        target_is_active,
                        focus_window,
                    },
                );
            }
            Event::Command(Command::Layout(command)) => {
                let command_space = self.command_context_space();
                if let Some(space) = command_space
                    && matches!(
                        command,
                        layout::LayoutCommand::NextWorkspace(_)
                            | layout::LayoutCommand::PrevWorkspace(_)
                            | layout::LayoutCommand::SwitchToWorkspace(_)
                            | layout::LayoutCommand::SwitchToLastWorkspace
                    )
                {
                    self.save_cursor_for_workspace(space);
                }
                let (visible_spaces, visible_space_centers) = self.visible_spaces_for_layout(false);
                return command_workflow::handle_command_layout(
                    &mut self.state,
                    &mut self.layout_manager,
                    &mut self.workspace_switch_manager,
                    command_workflow::LayoutCommandPayload {
                        command: command.clone(),
                        command_space,
                        visible_spaces,
                        visible_space_centers,
                    },
                )
                .map(|outcome| {
                    if matches!(command, layout::LayoutCommand::SetWorkspaceName { .. }) {
                        self.mark_layout_dirty();
                    }
                    outcome
                });
            }
            Event::Command(Command::Reactor(ReactorCommand::MoveWindowToDisplay {
                selector,
                window_id,
            })) => {
                if self.is_in_drag() {
                    warn!("Ignoring move-window-to-display while a drag is active");
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                }
                let command_space = self.workspace_command_space();
                let resolved_window = {
                    let workspaces = self.layout_manager.layout_engine.virtual_workspace_manager();
                    match window_id {
                        Some(index) => command_space
                            .and_then(|space| {
                                workspaces.find_window_by_idx(&self.state.windows, space, index)
                            })
                            .or_else(|| {
                                self.iter_active_spaces().find_map(|space| {
                                    workspaces.find_window_by_idx(&self.state.windows, space, index)
                                })
                            }),
                        None => self
                            .main_window()
                            .or_else(|| self.window_id_under_cursor())
                            .or_else(|| {
                                command_space.and_then(|space| {
                                    workspaces.find_window_by_idx(&self.state.windows, space, 0)
                                })
                            }),
                    }
                };
                let Some(window) = resolved_window else {
                    warn!("Move window to display ignored because no target window was resolved");
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                };
                let Some(window_state) = self.state.windows.window(window) else {
                    warn!(?window, "Move window to display ignored: unknown window");
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                };
                let window_server_id = window_state.info.sys_id;
                let window_frame = window_state.frame_monotonic;
                let source_space = self
                    .assigned_space_for_window_id(window)
                    .or_else(|| self.best_space_for_window_id(window))
                    .or_else(|| self.best_space_for_window(&window_frame, window_server_id));
                let Some(source_space) = source_space.filter(|space| self.is_space_active(*space))
                else {
                    warn!(
                        ?window,
                        "Move window to display ignored: source space unavailable"
                    );
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                };
                let origin = self
                    .space_state
                    .screen_by_space(source_space)
                    .map(|screen| screen.frame.mid())
                    .or_else(|| self.current_screen_center());
                let Some(target_screen) = self.screen_for_selector(&selector, origin).cloned()
                else {
                    warn!(
                        ?selector,
                        "Move window to display ignored: target display not found"
                    );
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                };
                let Some(target_space) =
                    target_screen.space.filter(|space| self.is_space_active(*space))
                else {
                    warn!(
                        ?selector,
                        "Move window to display ignored: target space unavailable"
                    );
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                };
                if source_space == target_space {
                    return Ok(EventOutcome::finalized_event(None, false, false, false));
                }
                let mut target_frame = window_frame;
                let mut origin = target_screen.frame.mid();
                origin.x -= window_frame.size.width / 2.0;
                origin.y -= window_frame.size.height / 2.0;
                let min = target_screen.frame.min();
                let max = target_screen.frame.max();
                origin.x = origin.x.max(min.x).min(max.x - window_frame.size.width);
                origin.y = origin.y.max(min.y).min(max.y - window_frame.size.height);
                target_frame.origin = origin;
                return command_workflow::handle_command_reactor_move_window_to_display(
                    &mut self.state,
                    &mut self.layout_manager,
                    &self.app_manager,
                    command_workflow::MoveWindowToDisplayPayload {
                        window,
                        window_server_id,
                        source_space,
                        target_space,
                        target_screen: target_screen.frame,
                        target_frame,
                    },
                )
                .map(|outcome| {
                    if !self.layout_manager.layout_engine.is_window_floating(window) {
                        self.schedule_display_move_reasserts(window);
                    }
                    outcome
                });
            }
            Event::ReassertDisplayMove(window_id) => {
                return command_workflow::handle_reassert_display_move(
                    &mut self.state,
                    &self.transaction_manager,
                    window_id,
                );
            }
            Event::ReassertTopmost => {
                self.reassert_topmost_windows(None);
                return Ok(EventOutcome::default());
            }
            Event::ApplicationMainWindowChanged(pid, _, _) => {
                if self.app_manager.apps.contains_key(&pid) {
                    return Ok(EventOutcome::finalized_event(None, false, false, false)
                        .with_app_request(pid, Request::GetVisibleWindows));
                }
            }
            Event::ReconcileOrphans => {
                return Ok(self.orphan_reconcile_outcome());
            }
            Event::PersistTick => {
                self.persistence_tick();
            }
            _ => (),
        }

        Ok(EventOutcome::finalized_event(
            raised_window,
            false,
            false,
            should_update_notifications,
        ))
    }

    /// Applies workflow follow-up requests in one stable order.
    ///
    /// Explicit transition frames are written before layout calculation so the
    /// resulting layout remains authoritative. Focus selection follows layout
    /// writes, then UI/platform presentation state is refreshed. Broadcast and
    /// discovery requests made directly by a workflow are consequently observed
    /// only after its model mutation is complete.
    fn apply_event_outcome(&mut self, outcome: EventOutcome) {
        if !outcome.window_server_updates.is_empty() {
            self.update_partial_window_server_info(outcome.window_server_updates);
        }
        if outcome.recompute_active_spaces {
            self.recompute_and_set_active_spaces_from_current_screens();
        }
        if outcome.repair_spaces_after_mission_control {
            self.repair_spaces_after_mission_control();
        }
        if outcome.refresh_after_mission_control {
            self.refresh_windows_after_mission_control();
        }
        if outcome.force_refresh_all_windows {
            self.force_refresh_all_windows();
        }
        // Discovery responses reconcile model state before layout. Requests
        // which schedule new discovery are deferred to the final phase below.
        for discovery in outcome.discoveries {
            self.on_windows_discovered_with_app_info(
                discovery.pid,
                discovery.new,
                discovery.known_visible,
                discovery.app_info,
            );
        }
        if let Some(pid) = outcome.activate_application {
            self.handle_app_activation_workspace_switch(pid);
        }

        for window in outcome.reapply_app_rules {
            self.maybe_reapply_app_rules_for_window(window);
        }
        for window in outcome.finalize_created_windows {
            let active_space = self.state.windows.window(window).and_then(|state| {
                self.best_space_for_window(&state.frame_monotonic, state.info.sys_id)
                    .filter(|space| self.is_space_active(*space))
                    .or_else(|| {
                        state
                            .info
                            .sys_id
                            .is_none()
                            .then(|| self.workspace_command_space())
                            .flatten()
                    })
            });
            if let Some(space) = active_space {
                if let Some(app_info) =
                    self.app_manager.apps.get(&window.pid).map(|app| app.info.clone())
                {
                    if let Some(window_server_id) =
                        self.state.windows.window(window).and_then(|state| state.info.sys_id)
                    {
                        self.state.windows.mark_wsids_recent(std::iter::once(window_server_id));
                    }
                    self.process_windows_for_app_rules(window.pid, vec![window], app_info);
                }
                if self
                    .state
                    .windows
                    .window(window)
                    .is_some_and(|state| state.matches_filter(WindowFilter::EffectivelyManageable))
                {
                    self.send_layout_event(LayoutEvent::WindowAdded(space, window));
                    self.send_layout_event(LayoutEvent::WindowFocused(space, window));
                    self.workspace_switch_manager.pending_workspace_mouse_warp = Some(window);
                    self.raise_window(window, Quiet::No, None);
                }
            }
        }

        for (window_server_id, space) in outcome.confirmed_window_spaces {
            self.clear_pending_target_if_confirmed_space(window_server_id, space);
        }
        for (window_server_id, space, window) in outcome.fullscreen_restorations {
            let mut nested = EventOutcome::default();
            if self
                .restore_fullscreen_window_to_user_space(
                    window_server_id,
                    space,
                    window,
                    &mut nested,
                )
                .is_none()
            {
                self.reassign_window_to_authoritative_space(window, space);
            }
            self.apply_event_outcome(nested);
        }
        for reassignment in outcome.topology_reassignments {
            if reassignment.preserve_workspace_ordinal {
                self.reassign_window_to_authoritative_space_preserving_workspace_ordinal(
                    reassignment.window,
                    reassignment.space,
                );
            } else {
                self.reassign_window_to_authoritative_space(
                    reassignment.window,
                    reassignment.space,
                );
            }
        }

        // Some transitions need to place a window on its destination display
        // before arranging that display. Keep these writes ahead of both layout
        // responses and the arrange pass so tiling always supplies the final frame.
        for write in outcome.pre_layout_window_frame_writes {
            let window_server_id =
                self.state.windows.window(write.window).and_then(|window| window.info.sys_id);
            let transaction = if let Some(window_server_id) = window_server_id {
                let transaction = self.transaction_manager.generate_next_txid(window_server_id);
                self.transaction_manager.store_txid(window_server_id, transaction, write.frame);
                transaction
            } else {
                TransactionId::default()
            };
            if let Some(app) = self.app_manager.apps.get(&write.window.pid)
                && let Err(error) = app.handle.send(Request::SetWindowFrame(
                    write.window,
                    write.frame,
                    transaction,
                    write.requested,
                ))
            {
                warn!(window = ?write.window, %error, "failed to write requested window frame");
            }
        }

        for event in outcome.layout_events {
            self.send_layout_event(event);
        }
        for (response, workspace_switch_space) in outcome.layout_responses {
            self.handle_layout_response(response, workspace_switch_space, false);
        }
        for (window, frame) in outcome.drag_swap_evaluations {
            self.maybe_swap_on_drag(window, frame);
        }
        if outcome.dispatch_mouse_up {
            self.handle_event(Event::MouseUp);
        }

        let mut layout_changed = false;
        if outcome.arrange.requested && (!self.is_in_drag() || outcome.arrange.window_was_destroyed)
        {
            for _ in 0..outcome.arrange.passes.max(1) {
                layout_changed |= self.update_layout_or_warn(
                    outcome.arrange.is_resize,
                    matches!(
                        self.workspace_switch_manager.workspace_switch_state,
                        WorkspaceSwitchState::Active
                    ),
                );
            }
            self.maybe_send_menu_update();
        }

        for request in outcome.raise_requests {
            if let Err(error) = self.communication_manager.raise_manager_tx.try_send(request) {
                warn!(%error, "failed to send raise request");
            }
        }

        if let Some((space, window)) =
            focus_service::resolve(outcome.focused_window, |wid| self.best_space_for_window_id(wid))
        {
            self.send_layout_event(LayoutEvent::WindowFocused(space, window));
        }

        if let Some(direction) = outcome.switch_native_space {
            unsafe { window_server::switch_space(direction) };
        }

        for (pid, window) in outcome.make_key_windows {
            if let Err(error) = window_server::make_key_window(pid, window) {
                warn!(?error, "failed to make key window");
            }
        }
        if let Some(pending) = outcome.pending_display_move_warp.take() {
            self.pending_display_move_warp = Some(pending);
        }
        for point in outcome.mouse_warps {
            self.warp_mouse(point);
        }

        for command in outcome.wm_commands {
            let is_dismiss = matches!(
                command,
                crate::actor::wm_controller::WmCmd::DismissMissionControl
            );
            if let Some(wm) = self.communication_manager.wm_sender.as_ref() {
                wm.send(crate::actor::wm_controller::WmEvent::Command(
                    crate::actor::wm_controller::WmCommand::Wm(command),
                ));
            } else if is_dismiss {
                self.set_mission_control_active(false);
            }
        }
        for event in outcome.wm_events {
            if let Some(wm) = self.communication_manager.wm_sender.as_ref() {
                wm.send(event);
            }
        }

        if let Some(window_server_id) = outcome.close_window {
            let target = match window_server_id {
                Some(wsid) => self.state.windows.tracked_window_id(wsid),
                None => self.main_window(),
            };
            if let Some(window) = target {
                self.request_close_window(window.pid, window_server_id);
            } else {
                warn!(?window_server_id, "Close target not found");
            }
        }

        if let Some((config, keys_changed)) = outcome.service_config_update {
            if let Some(tx) = &self.communication_manager.stack_line_tx
                && let Err(error) = tx.try_send(stack_line::Event::ConfigUpdated(config.clone()))
            {
                warn!(%error, "failed to update stack line config");
            }
            if let Some(tx) = &self.menu_manager.menu_tx
                && let Err(error) = tx.try_send(menu_bar::Event::ConfigUpdated(config.clone()))
            {
                warn!(%error, "failed to update menu bar config");
            }
            if keys_changed && let Some(wm) = &self.communication_manager.wm_sender {
                wm.send(crate::actor::wm_controller::WmEvent::ConfigUpdated(config));
            }
        }
        for line in outcome.stdout_lines {
            println!("{line}");
        }
        let was_manual_workspace_switch = self.workspace_switch_manager.manual_switch_in_progress();
        self.workspace_switch_manager.mark_workspace_switch_inactive();
        if self.workspace_switch_manager.active_workspace_switch.is_some() && !layout_changed {
            self.workspace_switch_manager.active_workspace_switch = None;
            trace!("Workspace switch stabilized with no further frame changes");
        }

        // Execute deferred mouse warp only for command-driven workspace switches. Hover/focus
        // churn can briefly look like a workspace switch and must never hijack physical mouse
        // movement by recentering onto the focused window. Prefer the exact cursor position
        // saved for the destination workspace so switching back resumes where typing happened.
        if was_manual_workspace_switch
            && let Some(target) = self.workspace_switch_manager.pending_workspace_cursor_warp.take()
        {
            if let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref() {
                event_tap_tx.send(crate::actor::event_tap::Request::Warp(target));
            }
        } else if was_manual_workspace_switch
            && let Some(wid) = self.workspace_switch_manager.pending_workspace_mouse_warp.take()
        {
            let restored_cursor = self
                .config
                .settings
                .restore_cursor_position_per_workspace
                .then(|| self.restored_cursor_for_workspace_window(wid))
                .flatten();
            if let Some(target) = restored_cursor.or_else(|| self.window_center_on_known_screen(wid))
            {
                self.warp_mouse(target);
            }
        } else {
            self.workspace_switch_manager.pending_workspace_cursor_warp = None;
            self.workspace_switch_manager.pending_workspace_mouse_warp = None;
        }

        // Deferred cursor warp for a completed move-window-to-display. Fire once the moved window
        // has landed on the target display (centre inside `dest_rect`), or once a short deadline
        // passes as a safety net.
        if let Some((wid, dest_rect, deadline)) = self.pending_display_move_warp {
            let settled = match self.state.windows.window(wid) {
                Some(window) => dest_rect.contains(window.frame_monotonic.mid()),
                None => true,
            };
            if settled || std::time::Instant::now() >= deadline {
                if let Some(target) = self.window_center_on_known_screen(wid)
                    && let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref()
                {
                    event_tap_tx.send(crate::actor::event_tap::Request::WarpSilent(target));
                }
                self.pending_display_move_warp = None;
            }
        }

        if outcome.refresh_window_notifications {
            let mut ids: Vec<u32> = self
                .state
                .windows
                .iter_tracked_window_server_ids()
                .map(|wsid| wsid.as_u32())
                .collect();
            ids.sort_unstable();

            if ids != self.notification_manager.last_sls_notification_ids {
                crate::sys::window_notify::update_window_notifications(&ids);

                self.notification_manager.last_sls_notification_ids = ids;
            }
        }
        if outcome.refresh_focus_follows_mouse {
            self.update_focus_follows_mouse_state();
        }
        if outcome.refresh_layout_mode {
            self.update_event_tap_layout_mode();
        }
        for broadcast in outcome.window_title_broadcasts {
            self.broadcast_window_title_changed(
                broadcast.window,
                broadcast.previous_title,
                broadcast.new_title,
            );
        }
        // Requests which schedule fresh discovery are last so observers see
        // the fully reconciled model, layout, UI, and broadcasts.
        for (pid, request) in outcome.app_requests {
            if let Some(app) = self.app_manager.apps.get(&pid)
                && let Err(error) = app.handle.send(request)
            {
                warn!(pid, %error, "failed to send deferred application request");
            }
        }
        if self.workspace_switch_manager.workspace_switch_state == WorkspaceSwitchState::Inactive {
            self.save_cursor_for_cursor_workspace();
        }
    }

    fn create_window_data(&self, window_id: WindowId) -> Option<WindowData> {
        let window_state = self.state.windows.window(window_id)?;
        if !window_state.matches_filter(WindowFilter::EffectivelyManageable) {
            return None;
        }
        let app = self.app_manager.apps.get(&window_id.pid)?;

        let app_name = app.info.localized_name.clone();
        let bundle_id = app.info.bundle_id.clone();

        Some(WindowData {
            id: window_id,
            is_floating: self.layout_manager.layout_engine.is_window_floating(window_id),
            is_topmost: self.topmost_windows.contains_key(&window_id),
            is_focused: self.main_window() == Some(window_id),
            app_name,
            info: WindowInfo {
                title: window_state.info.title.clone(),
                frame: window_state.frame_monotonic,
                bundle_id,
                ..window_state.info.clone()
            },
        })
    }

    fn update_complete_window_server_info(&mut self, ws_info: Vec<WindowServerInfo>) {
        self.state.windows.clear_visible_windows();
        self.update_partial_window_server_info(ws_info);
    }

    fn update_partial_window_server_info(&mut self, ws_info: Vec<WindowServerInfo>) {
        // Mark visible windows and remove any corresponding observed WSID markers
        // for ids we now have server info for.
        self.state.windows.set_visible_windows(ws_info.iter().map(|info| info.id));
        for info in ws_info.iter() {
            // If we've been observing this server id from SLS callbacks, clear it.
            self.state.windows.clear_window_server_observed(info.id);
            self.state.windows.track_window_server_info(*info);

            if let Some(wid) = self.state.windows.tracked_window_id(info.id) {
                let (server_id, is_minimized, is_ax_standard, is_ax_root, was_manageable) =
                    if let Some(window) = self.state.windows.window_mut(wid) {
                        if info.layer == 0 {
                            window.frame_monotonic = info.frame;
                        }
                        (
                            window.info.sys_id,
                            window.info.is_minimized,
                            window.info.is_standard,
                            window.info.is_root,
                            window.matches_filter(WindowFilter::EffectivelyManageable),
                        )
                    } else {
                        continue;
                    };
                let manageable = utils::compute_window_manageability(
                    server_id,
                    is_minimized,
                    is_ax_standard,
                    is_ax_root,
                    |wsid| self.state.windows.get_window_server_info(wsid),
                );
                if let Some(window) = self.state.windows.window_mut(wid) {
                    window.is_manageable = manageable;
                }

                if was_manageable && !manageable {
                    self.send_layout_event(LayoutEvent::WindowRemoved(wid));
                }
            }
        }
    }

    fn check_for_new_windows(&mut self) {
        // AX discovery remains the source of truth for enumerating app windows.
        // Native-space membership/visibility is supplied separately by the spaces
        // actor; do not replace this with the global CG on-screen window list.
        self.request_visible_windows_for_apps(false);
    }

    fn request_visible_windows_for_pid(&mut self, pid: pid_t, track_mission_control_refresh: bool) {
        if self.refreshes_blocked() {
            self.defer_visible_refresh(track_mission_control_refresh);
            return;
        }

        let sent = self
            .app_manager
            .apps
            .get(&pid)
            .is_some_and(|app| app.handle.send(Request::GetVisibleWindows).is_ok());
        if sent && track_mission_control_refresh {
            self.mission_control_manager.pending_mission_control_refresh.insert(pid);
        }
    }

    fn request_visible_windows_for_apps(&mut self, track_mission_control_refresh: bool) {
        if self.refreshes_blocked() {
            self.defer_visible_refresh(track_mission_control_refresh);
            return;
        }

        let mut refreshed_pids = Vec::new();
        for (&pid, app) in &self.app_manager.apps {
            // Errors mean the app terminated (and a termination event is coming); ignore.
            if app.handle.send(Request::GetVisibleWindows).is_ok() {
                refreshed_pids.push(pid);
            }
        }

        if track_mission_control_refresh {
            self.mission_control_manager
                .pending_mission_control_refresh
                .extend(refreshed_pids);
        }
    }

    fn restore_windows_after_fullscreen_exit(&mut self, spaces: &[Option<SpaceId>]) {
        let refresh_spaces: Vec<SpaceId> = spaces
            .iter()
            .copied()
            .flatten()
            .filter(|space| !self.is_fullscreen_space(*space))
            .collect();

        for space in refresh_spaces {
            let records: Vec<_> = self
                .state
                .windows
                .iter_native_fullscreen_records()
                .filter(|record| {
                    record.last_known_user_space == Some(space)
                        || record.workspace.is_some_and(|workspace| workspace.space == space)
                })
                .collect();

            if records.is_empty() {
                continue;
            }

            for record in records {
                let _ = self
                    .state
                    .windows
                    .restore_window_from_native_fullscreen(record.current_window_id);

                if let Some(app) = self.app_manager.apps.get(&record.current_window_id.pid) {
                    if let Err(e) = app.handle.send(Request::GetVisibleWindows) {
                        warn!(
                            "Failed to send GetVisibleWindows to app {}: {}",
                            record.current_window_id.pid, e
                        );
                    }
                }

                let live_window_id = record
                    .window_server_id
                    .and_then(|wsid| self.state.windows.tracked_window_id(wsid))
                    .or_else(|| {
                        self.state
                            .windows
                            .contains_window(record.current_window_id)
                            .then_some(record.current_window_id)
                    });

                let target_space = record
                    .workspace
                    .map(|workspace| workspace.space)
                    .or(record.last_known_user_space);

                if let (Some(window_id), Some(target_space)) = (live_window_id, target_space)
                    && let Some(source_space) =
                        self.best_space_for_window_id(window_id).or(Some(target_space))
                    && source_space != target_space
                {
                    let target_screen_size = self
                        .space_state
                        .screen_by_space(target_space)
                        .map(|screen| screen.frame.size)
                        .unwrap_or_else(|| CGSize::new(0.0, 0.0));

                    let response = self.layout_manager.layout_engine.move_window_to_space(
                        &mut self.state.windows,
                        source_space,
                        target_space,
                        target_screen_size,
                        window_id,
                    );
                    self.handle_layout_response(response, None, false);
                }
            }

            self.refocus_manager.refocus_state = RefocusState::Pending(space);
            self.update_layout_or_warn(false, false);
            self.update_focus_follows_mouse_state();
        }
    }

    fn is_fullscreen_space(&self, space: SpaceId) -> bool {
        self.space_state.fullscreen_spaces.contains(&space)
    }

    fn finalize_space_change(
        &mut self,
        spaces: &[Option<SpaceId>],
        active_windows: Vec<(WindowServerId, Option<SpaceId>)>,
        preserve_missing_assignments: bool,
    ) {
        self.refocus_manager.stale_cleanup_state = if spaces.iter().all(|space| space.is_none()) {
            StaleCleanupState::Suppressed
        } else {
            StaleCleanupState::Enabled
        };
        self.expose_all_spaces();
        if let Some(main_window) = self.main_window() {
            if let Some(space) = self.main_window_space() {
                self.send_layout_event(LayoutEvent::WindowFocused(space, main_window));
            }
        }
        self.reconcile_authoritative_active_window_snapshot(
            active_windows,
            preserve_missing_assignments,
        );
        self.check_for_new_windows();

        if let Some(space) = self
            .workspace_command_space()
            .or_else(|| spaces.iter().copied().flatten().find(|space| self.is_space_active(*space)))
        {
            if let Some((workspace_id, workspace_name)) =
                self.layout_manager.layout_engine.ensure_active_workspace_info(space)
            {
                let display_uuid = self.display_uuid_for_space(space);
                let broadcast_event = BroadcastEvent::WorkspaceChanged {
                    workspace_id,
                    workspace_name,
                    space_id: space,
                    display_uuid,
                };
                _ = self.communication_manager.event_broadcaster.send(broadcast_event);
            }
        }
    }

    fn broadcast_window_title_changed(
        &mut self,
        window_id: WindowId,
        previous_title: String,
        new_title: String,
    ) {
        if previous_title != new_title
            && let Some(space) = self.best_space_for_window_id(window_id)
            && self.is_space_active(space)
            && let Some(workspace_id) = self.layout_manager.layout_engine.active_workspace(space)
        {
            let workspace_index = self.layout_manager.layout_engine.active_workspace_idx(space);

            let workspace_name = self
                .layout_manager
                .layout_engine
                .workspace_name(space, workspace_id)
                .unwrap_or_else(|| format!("Workspace {:?}", workspace_id));

            let display_uuid = self.display_uuid_for_space(space);

            let event = BroadcastEvent::WindowTitleChanged {
                window_id,
                workspace_id,
                workspace_index,
                workspace_name,
                previous_title,
                new_title,
                space_id: space,
                display_uuid,
            };
            let _ = self.communication_manager.event_broadcaster.send(event);
        }
    }

    pub(crate) fn broadcast_native_mission_control_entered(&self) {
        let _ = self
            .communication_manager
            .event_broadcaster
            .send(BroadcastEvent::MissionControlNativeEntered);
    }

    pub(crate) fn broadcast_native_mission_control_exited(&self) {
        let _ = self
            .communication_manager
            .event_broadcaster
            .send(BroadcastEvent::MissionControlNativeExited);
    }

    fn maybe_reapply_app_rules_for_window(&mut self, window_id: WindowId) {
        if !self.config.virtual_workspaces.reapply_app_rules_on_title_change {
            return;
        }

        let Some(space) = self.best_space_for_window_id(window_id) else {
            return;
        };
        if !self.is_space_active(space) {
            return;
        }

        let (is_manageable, wsid) = match self.state.windows.window(window_id) {
            Some(window_state) => (
                window_state.matches_filter(WindowFilter::Manageable),
                window_state.info.sys_id,
            ),
            None => return,
        };

        if !is_manageable {
            return;
        }

        let app_info = match self.app_manager.apps.get(&window_id.pid) {
            Some(app_state) => app_state.info.clone(),
            None => return,
        };

        if let Some(window_server_id) = wsid {
            self.state.windows.mark_wsids_recent(std::iter::once(window_server_id));
        }

        self.process_windows_for_app_rules(window_id.pid, vec![window_id], app_info);
    }

    fn handle_authoritative_space_snapshot(
        &mut self,
        space_state: ForwardedSpaceState,
    ) -> anyhow::Result<EventOutcome> {
        let mut outcome = EventOutcome::finalized_event(None, false, false, true);
        let analysis = topology_workflow::analyze_space_snapshot(
            &self.space_state,
            &self.active_spaces,
            &self.space_activation_policy,
            self.activation_cfg(),
            &space_state,
        );
        let pending_space_state = space_state.clone();
        let ForwardedSpaceState {
            screens,
            fullscreen_spaces,
            has_seen_display_set,
            active_spaces,
            menu_bar_space,
            command_space,
            display_space_ids,
            last_user_space_by_display,
            space_remaps,
            display_set_changed,
            should_force_refresh_layout,
            releases_lifecycle_refresh_quarantine,
            resized_spaces,
            topology_window_delta,
            active_window_spaces,
            ..
        } = space_state;
        self.space_state.active_window_spaces = active_window_spaces;
        let activation_config = self.activation_cfg();
        let topology_workflow::SpaceSnapshotAnalysis {
            spaces,
            authoritative_spaces,
            command_space_only_update,
            invalidates_pending_targets,
        } = analysis;

        self.space_state.has_seen_display_set = has_seen_display_set;
        self.space_state.fullscreen_spaces = fullscreen_spaces;
        self.space_state.active_spaces = active_spaces;
        if command_space_only_update {
            self.space_state.menu_bar_space = menu_bar_space;
            self.space_state.command_space = command_space;
            return Ok(outcome);
        }
        if display_set_changed
            && !screens.is_empty()
            && !self.space_state.screens.is_empty()
        {
            self.save_arrangement_before_switch(&screens);
        }
        if display_set_changed {
            let active_displays: Vec<String> =
                screens.iter().map(|screen| screen.display_uuid.clone()).collect();
            self.layout_manager.layout_engine.prune_display_state(&active_displays);
        }
        self.space_state.menu_bar_space = menu_bar_space;
        self.space_state.command_space = command_space;
        self.space_state.display_space_ids = display_space_ids;
        self.space_state.last_user_space_by_display = last_user_space_by_display;

        if screens.is_empty() {
            self.refocus_manager.stale_cleanup_state = StaleCleanupState::Suppressed;
            if !self.space_state.screens.is_empty() {
                self.space_state.screens.clear();
                self.expose_all_spaces();
            }
            self.recompute_and_set_active_spaces(&[]);
            self.update_complete_window_server_info(Vec::new());
            self.try_apply_pending_space_change();
            return Ok(outcome);
        }

        self.refocus_manager.stale_cleanup_state = StaleCleanupState::Enabled;
        self.space_state.screens = screens;
        self.activate_restore_if_ready();
        self.load_arrangement_after_switch();
        self.remap_restored_spaces();
        if invalidates_pending_targets {
            self.clear_pending_hidden_window_targets();
        }
        if self.is_mission_control_active() {
            self.pending_space_change_manager.pending_space_change = Some(pending_space_state);
            return Ok(outcome);
        }
        for (previous_space, space) in space_remaps {
            self.layout_manager.layout_engine.remap_space(
                &mut self.state.windows,
                previous_space,
                space,
            );
        }
        for screen in &self.space_state.screens {
            let (Some(space), Some(display_uuid)) = (screen.space, screen.display_uuid_opt())
            else {
                continue;
            };
            self.layout_manager
                .layout_engine
                .update_space_display(space, Some(display_uuid.to_string()));
        }
        let current_screens = self.screens_for_current_spaces();
        self.space_activation_policy
            .on_spaces_updated(activation_config, &current_screens);
        self.recompute_and_set_active_spaces(&authoritative_spaces);
        self.restore_windows_after_fullscreen_exit(&spaces);

        for (space, size) in resized_spaces {
            if !self.is_space_active(space) {
                continue;
            }
            self.layout_manager
                .layout_engine
                .virtual_workspace_manager_mut()
                .list_workspaces(space);
            outcome = outcome.with_layout_event(LayoutEvent::SpaceExposed(space, size));
        }
        if let Some(delta) = topology_window_delta {
            outcome.absorb(self.apply_topology_window_delta(delta));
        }
        let active_windows = self.authoritative_active_space_windows();
        self.finalize_space_change(&spaces, active_windows, releases_lifecycle_refresh_quarantine);
        self.try_apply_pending_space_change();
        if should_force_refresh_layout {
            outcome = outcome.with_force_window_refresh().with_arrange_passes(1);
        }
        Ok(outcome)
    }

    fn try_apply_pending_space_change(&mut self) {
        if let Some(pending) = self.pending_space_change_manager.pending_space_change.take() {
            if pending.screens.len() == self.space_state.screens.len() {
                // During native Mission Control we must preserve the full forwarded snapshot,
                // not just the raw spaces vector, otherwise command-space and per-display space
                // metadata can remain stale after exit.
                if let Ok(outcome) = self.handle_authoritative_space_snapshot(pending) {
                    self.apply_event_outcome(outcome);
                }
            } else {
                self.pending_space_change_manager.pending_space_change = Some(pending);
            }
        }
    }

    fn repair_spaces_after_mission_control(&mut self) {
        // First, apply any SpaceChanged that arrived while MC was active.
        self.try_apply_pending_space_change();
    }

    fn on_windows_discovered_with_app_info(
        &mut self,
        pid: pid_t,
        new: Vec<(WindowId, WindowInfo)>,
        known_visible: Vec<WindowId>,
        app_info: Option<AppInfo>,
    ) {
        let app_info =
            app_info.or_else(|| self.app_manager.apps.get(&pid).map(|app| app.info.clone()));
        // Refresh the on-screen window-server snapshot from live state first: an app may
        // have ordered a window out without a destroy notification, leaving a stale entry
        // that would otherwise mask the orphan from the reconciliation below.
        self.refresh_visible_windows_snapshot();
        let inactive_windows = self
            .state
            .windows
            .iter_windows()
            .filter_map(|(wid, _)| {
                (wid.pid == pid && self.is_window_on_known_inactive_space(wid)).then_some(wid)
            })
            .collect();
        let server_observations = self
            .state
            .windows
            .iter_windows()
            .filter_map(|(wid, window)| (wid.pid == pid).then_some(window.info.sys_id).flatten())
            .map(|wsid| {
                let info = self
                    .state
                    .windows
                    .get_window_server_info(wsid)
                    .or_else(|| window_server::get_window(wsid));
                (wsid, window_discovery::StaleWindowObservation {
                    info,
                    suitable: window_server::app_window_suitable(wsid),
                    ordered_in: window_server::window_is_ordered_in(wsid),
                })
            })
            .collect();
        let stale_snapshot = window_discovery::StaleCleanupSnapshot {
            pending_refresh: self
                .mission_control_manager
                .pending_mission_control_refresh
                .contains(&pid),
            suppressed: matches!(
                self.refocus_manager.stale_cleanup_state,
                StaleCleanupState::Suppressed
            ),
            mission_control_active: self.is_mission_control_active(),
            drag_active: self.is_in_drag(),
            inactive_windows,
            server_observations,
        };
        let (stale_windows, pending_refresh) = window_discovery::identify_stale_windows(
            &self.state,
            pid,
            &known_visible,
            &stale_snapshot,
        );
        let mut outcome = match window_discovery::cleanup_stale_windows(
            &mut self.state,
            &self.transaction_manager,
            &mut self.drag_manager,
            &mut self.mission_control_manager,
            pid,
            stale_windows,
            pending_refresh,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(%error, pid, "window discovery cleanup failed");
                return;
            }
        };
        let observed_windows = new
            .into_iter()
            .map(|(wid, info)| {
                let current_native_space =
                    info.sys_id.and_then(|wsid| self.resolve_native_space(wsid, None));
                let active_space = self
                    .best_space_for_window(&info.frame, info.sys_id)
                    .filter(|space| self.is_space_active(*space))
                    .or_else(|| {
                        info.sys_id.is_none().then(|| self.workspace_command_space()).flatten()
                    });
                window_discovery::ObservedWindow {
                    wid,
                    info,
                    current_native_space,
                    active_space,
                }
            })
            .collect();
        let (new_windows, process_outcome) = window_discovery::process_window_list(
            &mut self.state,
            &mut self.layout_manager,
            observed_windows,
            &app_info,
        );
        outcome.absorb(process_outcome);
        window_discovery::update_window_states(&mut self.state, new_windows);

        let candidate_windows: HashSet<WindowId> = self
            .state
            .windows
            .iter_windows()
            .filter_map(|(wid, _)| (wid.pid == pid).then_some(wid))
            .chain(known_visible.iter().copied().filter(|wid| wid.pid == pid))
            .collect();
        let discovery_spaces = candidate_windows
            .iter()
            .filter_map(|wid| self.discovery_space_for_window_id(*wid).map(|space| (*wid, space)))
            .collect();
        let authoritative_spaces = candidate_windows
            .iter()
            .filter_map(|wid| {
                self.authoritative_space_for_window_id(*wid).map(|space| (*wid, space))
            })
            .collect();
        let active_spaces = self
            .space_state
            .screens
            .iter()
            .filter_map(|screen| screen.space)
            .filter(|space| self.is_space_active(*space))
            .collect();
        let focused_window = self.focused_window_for_discovery(pid);
        outcome.absorb(window_discovery::emit_layout_events(
            &mut self.state,
            &mut self.layout_manager,
            window_discovery::EmitLayoutPayload {
                pid,
                known_visible: &known_visible,
                app_info: &app_info,
                discovery_spaces,
                authoritative_spaces,
                active_spaces,
                focused_window,
            },
        ));
        self.prune_app_adoptions(pid);
        self.apply_event_outcome(outcome);
    }

    fn best_space_for_window(
        &self,
        frame: &CGRect,
        window_server_id: Option<WindowServerId>,
    ) -> Option<SpaceId> {
        if let Some(wsid) = window_server_id
            && self.is_known_fullscreen_window(wsid)
        {
            return None;
        }

        if let Some(wsid) = window_server_id {
            if let Some(space) = self.resolve_native_space(wsid, None) {
                return Some(space);
            }
        }

        if let Some(space) = self.hidden_assigned_space_for_frame(window_server_id, frame) {
            return Some(space);
        }

        self.best_space_for_frame(frame)
    }

    fn best_space_for_frame(&self, frame: &CGRect) -> Option<SpaceId> {
        let center = frame.mid();
        self.screen_for_point(center).and_then(|screen| screen.space).or_else(|| {
            self.space_state
                .screens
                .iter()
                .filter_map(|screen| {
                    let space = screen.space?;
                    let area = screen.frame.intersection(frame).area() as i64;
                    if area > 0 { Some((area, space)) } else { None }
                })
                .max_by_key(|(area, _)| *area)
                .map(|(_, space)| space)
        })
    }

    #[cfg(test)]
    fn ensure_active_drag(&mut self, wid: WindowId, frame: &CGRect) {
        let needs_new_session =
            self.get_active_drag_session().is_none_or(|session| session.window != wid);
        if needs_new_session {
            let server_id = self.state.windows.window(wid).and_then(|window| window.info.sys_id);
            let origin_space = self.best_space_for_window(frame, server_id);
            self.drag_manager.drag_state = DragState::Active {
                session: DragSession {
                    window: wid,
                    last_frame: *frame,
                    origin_space,
                    settled_space: origin_space,
                    layout_dirty: false,
                },
            };
        }
        self.drag_manager.skip_layout_for_window = Some(wid);
    }

    fn best_space_for_window_state(&self, window: &WindowState) -> Option<SpaceId> {
        self.best_space_for_window(&window.frame_monotonic, window.info.sys_id)
    }

    fn hidden_assigned_space_for_frame(
        &self,
        window_server_id: Option<WindowServerId>,
        _frame: &CGRect,
    ) -> Option<SpaceId> {
        let wsid = window_server_id?;
        let wid = self.state.windows.tracked_window_id(wsid)?;
        let assigned_space = self.assigned_space_for_window_id(wid)?;
        if !self.is_space_active(assigned_space)
            || !self.window_in_non_active_workspace(assigned_space, wid)
        {
            return None;
        }

        Some(assigned_space)
    }

    fn hidden_assigned_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        let window = self.state.windows.window(wid)?;
        self.hidden_assigned_space_for_frame(window.info.sys_id, &window.frame_monotonic)
    }

    fn assigned_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        self.layout_manager
            .layout_engine
            .virtual_workspace_manager()
            .workspace_info_for_window_any(&self.state.windows, wid)
            .map(|info| info.space)
    }

    fn pending_target_space_for_window_server_id(&self, wsid: WindowServerId) -> Option<SpaceId> {
        let wid = self.state.windows.tracked_window_id(wsid)?;
        let target_frame = self.transaction_manager.get_target_frame(wsid)?;
        let assigned_space = self.assigned_space_for_window_id(wid)?;
        let target_space = self
            .hidden_assigned_space_for_frame(Some(wsid), &target_frame)
            .or_else(|| self.best_space_for_frame(&target_frame))?;
        (target_space == assigned_space).then_some(target_space)
    }

    fn reassign_window_to_authoritative_space(
        &mut self,
        wid: WindowId,
        authoritative_space: SpaceId,
    ) -> bool {
        self.reassign_window_to_authoritative_space_with_workspace_preservation(
            wid,
            authoritative_space,
            false,
        )
    }

    fn apply_topology_window_delta(&mut self, delta: TopologyWindowDelta) -> EventOutcome {
        let appeared: HashMap<WindowServerId, SpaceId> = delta.appeared.into_iter().collect();
        let disappeared: HashMap<WindowServerId, SpaceId> = delta.disappeared.into_iter().collect();
        let window_server_ids: HashSet<WindowServerId> =
            appeared.keys().chain(disappeared.keys()).copied().collect();
        let mut outcome = EventOutcome::default();

        for window_server_id in window_server_ids {
            let appeared_space = appeared.get(&window_server_id).copied();
            let disappeared_space = disappeared.get(&window_server_id).copied();
            let authoritative_space = self.resolve_native_space(window_server_id, appeared_space);
            if let Some(target_space) = authoritative_space {
                self.state.windows.set_window_server_space(window_server_id, Some(target_space));
                if appeared_space == Some(target_space) {
                    self.clear_pending_target_if_confirmed_space(window_server_id, target_space);
                }
                if self.is_space_active(target_space) {
                    self.state.windows.mark_window_visible(window_server_id);
                } else {
                    self.state.windows.mark_window_hidden(window_server_id);
                }
                if let Some(window) = self.state.windows.tracked_window_id(window_server_id) {
                    let restored = self.restore_fullscreen_window_to_user_space(
                        window_server_id,
                        target_space,
                        window,
                        &mut outcome,
                    );
                    if restored.is_none() {
                        self.reassign_window_to_authoritative_space_preserving_workspace_ordinal(
                            window,
                            target_space,
                        );
                    }
                }
            } else if let Some(previous_space) = disappeared_space {
                self.state
                    .windows
                    .set_window_server_space(window_server_id, Some(previous_space));
                self.state.windows.mark_window_hidden(window_server_id);
                if let Some(window) = self.state.windows.tracked_window_id(window_server_id)
                    && self.assigned_space_for_window_id(window) == Some(previous_space)
                    && self.is_space_active(previous_space)
                {
                    outcome = outcome
                        .with_layout_event(LayoutEvent::WindowRemovedPreserveFloating(window));
                }
            }
        }
        outcome
    }

    fn restore_fullscreen_window_to_user_space(
        &mut self,
        window_server_id: WindowServerId,
        space: SpaceId,
        original_window: WindowId,
        outcome: &mut EventOutcome,
    ) -> Option<bool> {
        let restored = self
            .state
            .windows
            .restore_window_from_native_fullscreen_by_window_server_id(window_server_id)
            .or_else(|| {
                self.state.windows.restore_window_from_native_fullscreen(original_window)
            })?;
        let owner = self
            .state
            .windows
            .contains_window(restored.current_window_id)
            .then_some(restored.current_window_id)
            .or_else(|| {
                restored
                    .window_server_id
                    .and_then(|id| self.state.windows.tracked_window_id(id))
            })
            .or_else(|| self.state.windows.tracked_window_id(window_server_id))
            .or_else(|| {
                self.state.windows.contains_window(original_window).then_some(original_window)
            })?;
        if owner != original_window && self.assigned_space_for_window_id(original_window).is_some()
        {
            *outcome = std::mem::take(outcome)
                .with_layout_event(LayoutEvent::WindowRemoved(original_window));
        }
        *outcome = std::mem::take(outcome).with_app_request(owner.pid, Request::GetVisibleWindows);
        Some(if self.assigned_space_for_window_id(owner) == Some(space) {
            self.is_space_active(space)
                && self.restore_window_to_active_layout_if_visible(owner, space)
        } else {
            self.reassign_window_to_authoritative_space(owner, space)
        })
    }

    pub(crate) fn reassign_window_to_authoritative_space_preserving_workspace_ordinal(
        &mut self,
        wid: WindowId,
        authoritative_space: SpaceId,
    ) -> bool {
        self.reassign_window_to_authoritative_space_with_workspace_preservation(
            wid,
            authoritative_space,
            true,
        )
    }

    fn reassign_window_to_authoritative_space_with_workspace_preservation(
        &mut self,
        wid: WindowId,
        authoritative_space: SpaceId,
        preserve_workspace_ordinal: bool,
    ) -> bool {
        // Native WindowServer visibility is not enough to participate in Rift's
        // layout. Fullscreen exit can surface transient AppKit/Electron windows
        // that are visible and space-owned but are filtered out of query output.
        // Treat this as the single gate for authoritative-space reconciliation:
        // if a window is not query-manageable, remove any stale layout/workspace
        // membership instead of re-assigning it from the WindowServer snapshot.
        if !self
            .state
            .windows
            .window(wid)
            .is_some_and(|window| window.matches_filter(WindowFilter::EffectivelyManageable))
        {
            let changed_space = self.assigned_space_for_window_id(wid);
            self.send_layout_event(LayoutEvent::WindowRemoved(wid));
            return changed_space.is_some_and(|space| self.is_space_active(space));
        }

        let assigned_space = self.assigned_space_for_window_id(wid);
        if assigned_space == Some(authoritative_space) {
            return self.restore_window_to_active_layout_if_visible(wid, authoritative_space);
        }

        self.send_layout_event(LayoutEvent::WindowRemovedPreserveFloating(wid));

        let _ = self
            .layout_manager
            .layout_engine
            .virtual_workspace_manager_mut()
            .list_workspaces(authoritative_space);

        let assigned = if preserve_workspace_ordinal {
            self.layout_manager
                .layout_engine
                .virtual_workspace_manager_mut()
                .assign_window_to_workspace_preserving_ordinal(
                    &mut self.state.windows,
                    authoritative_space,
                    wid,
                )
                .is_some()
        } else {
            let Some(target_workspace) = self
                .layout_manager
                .layout_engine
                .ensure_active_workspace_info(authoritative_space)
                .map(|(workspace_id, _)| workspace_id)
                .or_else(|| {
                    self.layout_manager.layout_engine.active_workspace(authoritative_space)
                })
            else {
                return assigned_space.is_some_and(|space| self.is_space_active(space));
            };

            self.layout_manager
                .layout_engine
                .virtual_workspace_manager_mut()
                .assign_window_to_workspace(
                    &mut self.state.windows,
                    authoritative_space,
                    wid,
                    target_workspace,
                )
        };
        if !assigned {
            return assigned_space.is_some_and(|space| self.is_space_active(space));
        }

        let target_active = self.is_space_active(authoritative_space);
        let _ = self.restore_window_to_active_layout_if_visible(wid, authoritative_space);

        assigned_space.is_some_and(|space| self.is_space_active(space)) || target_active
    }

    fn restore_window_to_active_layout_if_visible(
        &mut self,
        wid: WindowId,
        authoritative_space: SpaceId,
    ) -> bool {
        if !self.is_space_active(authoritative_space) {
            return false;
        }

        let Some(window) = self.state.windows.window(wid) else {
            return false;
        };
        // Same invariant as `reassign_window_to_authoritative_space`: a visible
        // WindowServer id may be a transient fullscreen projection. Do not let
        // visibility alone add it back to the active layout.
        if !window.matches_filter(WindowFilter::EffectivelyManageable) {
            self.send_layout_event(LayoutEvent::WindowRemoved(wid));
            return false;
        }

        let Some(wsid) = window.info.sys_id else {
            return false;
        };
        if !self.state.windows.is_window_visible(wsid) {
            return false;
        }

        let was_on_active_space = self.is_window_on_active_space(wid);
        self.send_layout_event(LayoutEvent::WindowAdded(authoritative_space, wid));
        !was_on_active_space && self.is_window_on_active_space(wid)
    }

    fn reconcile_windows_with_authoritative_spaces(&mut self) -> bool {
        if self.refreshes_blocked() {
            self.defer_visible_refresh(true);
            return false;
        }

        let windows: Vec<_> = self.state.windows.iter_windows().map(|(wid, _)| wid).collect();
        let mut layout_changed = false;

        for wid in windows {
            let Some(authoritative_space) = self.authoritative_space_for_window_id(wid) else {
                continue;
            };
            layout_changed |= self.reassign_window_to_authoritative_space(wid, authoritative_space);
        }

        layout_changed
    }

    fn current_reported_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        self.state
            .windows
            .window(wid)
            .and_then(|window| window.info.sys_id)
            .and_then(|wsid| self.resolve_native_space(wsid, None))
    }

    fn authoritative_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        let reported_space = self.current_reported_space_for_window_id(wid);
        if let Some(hidden_assigned_space) = self.hidden_assigned_space_for_window_id(wid) {
            return match reported_space {
                Some(space) if space != hidden_assigned_space => Some(space),
                _ => Some(hidden_assigned_space),
            };
        }

        reported_space.or_else(|| self.assigned_space_for_window_id(wid))
    }

    /// Resolve native space ownership from the strongest available source.
    ///
    /// `observation` is a direct per-space membership observation. A pending
    /// Rift move wins over an observation that is not backed by the live
    /// WindowServer state, while a live conflict is treated as a newer external
    /// move. With no direct observation, the live WindowServer query wins over
    /// the accepted prior observation and the pending target wins over stale
    /// cached state.
    pub(crate) fn resolve_native_space(
        &self,
        wsid: WindowServerId,
        observation: Option<SpaceId>,
    ) -> Option<SpaceId> {
        let pending = self.pending_target_space_for_window_server_id(wsid);
        let live = window_server::window_space(wsid);
        let prior = self.state.windows.window_server_space(wsid);

        match (observation, pending) {
            (Some(observed), Some(target)) if observed != target => {
                if live == Some(observed) {
                    Some(observed)
                } else {
                    Some(target)
                }
            }
            (Some(observed), _) => Some(observed),
            (None, _) => live.or(pending).or(prior),
        }
    }

    fn best_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        self.authoritative_space_for_window_id(wid).or_else(|| {
            self.state
                .windows
                .window(wid)
                .and_then(|window| self.best_space_for_window_state(window))
        })
    }

    fn is_window_on_known_inactive_space(&self, wid: WindowId) -> bool {
        self.authoritative_space_for_window_id(wid)
            .is_some_and(|space| !self.is_space_active(space))
    }

    fn discovery_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        let window = self.state.windows.window(wid)?;
        let authoritative = self.authoritative_space_for_window_id(wid);
        if let Some(space) = authoritative {
            return Some(space);
        }

        if let Some(space) = self.best_space_for_frame(&window.frame_monotonic)
            && self.is_space_active(space)
        {
            return Some(space);
        }

        self.best_space_for_window_id(wid)
    }

    pub(crate) fn geometry_space_for_window(
        &self,
        frame: &CGRect,
        window_server_id: Option<WindowServerId>,
    ) -> Option<SpaceId> {
        if let Some(wsid) = window_server_id
            && self.is_known_fullscreen_window(wsid)
        {
            return None;
        }

        if let Some(space) = self.hidden_assigned_space_for_frame(window_server_id, frame) {
            return Some(space);
        }

        self.best_space_for_frame(frame)
    }

    fn is_known_fullscreen_window(&self, wsid: WindowServerId) -> bool {
        self.state.windows.is_window_server_id_native_fullscreen_suspended(wsid)
    }

    fn window_center_on_known_screen(&self, wid: WindowId) -> Option<CGPoint> {
        let window_center = self.state.windows.window(wid)?.frame_monotonic.mid();
        self.screen_for_point(window_center).map(|_| window_center)
    }

    fn schedule_display_move_reasserts(&self, window_id: WindowId) {
        let Some(events_tx) = self.communication_manager.events_tx.clone() else {
            return;
        };
        for delay_ms in [200i64, 450, 800] {
            let events_tx = events_tx.clone();
            queue::main().after_f_s(
                Time::new_after(Time::NOW, delay_ms * 1_000_000),
                (events_tx, window_id),
                |(events_tx, window_id)| {
                    events_tx.send(Event::ReassertDisplayMove(window_id));
                },
            );
        }
    }

    fn topmost_feature_active(&self) -> bool {
        !self.topmost_windows.is_empty() || self.config.settings.floating_windows_topmost
    }

    fn schedule_topmost_reassert(&mut self) {
        if !self.topmost_feature_active() {
            return;
        }
        for state in self.topmost_windows.values_mut() {
            state.failed_reasserts =
                state.failed_reasserts.min(TOPMOST_MAX_FAILED_REASSERTS - 1);
        }
        self.schedule_topmost_verification();
    }

    fn schedule_topmost_verification(&self) {
        if !self.topmost_feature_active() {
            return;
        }
        let Some(events_tx) = self.communication_manager.events_tx.clone() else {
            return;
        };
        for delay_ms in [60i64, 120, 260, 500] {
            let events_tx = events_tx.clone();
            queue::main().after_f_s(
                Time::new_after(Time::NOW, delay_ms * 1_000_000),
                events_tx,
                |events_tx| {
                    events_tx.send(Event::ReassertTopmost);
                },
            );
        }
    }

    fn handle_toggle_topmost_window(&mut self) {
        let focused = self
            .layout_manager
            .layout_engine
            .focused_window()
            .or_else(|| self.main_window());
        let cursor_floating = self.window_id_under_cursor().filter(|wid| {
            self.layout_manager.layout_engine.is_window_floating(*wid)
        });
        let only_floating = self.workspace_command_space().and_then(|space| {
            let floating: Vec<_> = self
                .layout_manager
                .layout_engine
                .windows_in_active_workspace(&self.state.windows, space)
                .into_iter()
                .filter(|wid| self.layout_manager.layout_engine.is_window_floating(*wid))
                .collect();
            (floating.len() == 1).then_some(floating[0])
        });
        let window_id = focused
            .filter(|wid| self.layout_manager.layout_engine.is_window_floating(*wid))
            .or(cursor_floating)
            .or(only_floating)
            .or(focused);
        let Some(window_id) = window_id else {
            warn!("Toggle topmost ignored: no focused, hovered, or floating window");
            return;
        };
        if self.state.windows.window(window_id).is_none() {
            warn!(?window_id, "Toggle topmost ignored: unknown window");
            return;
        }

        self.toggle_topmost_window(window_id);
    }

    pub(crate) fn toggle_topmost_window(&mut self, wid: WindowId) {
        if self.topmost_windows.remove(&wid).is_some() {
            if self.config.settings.floating_windows_topmost {
                self.topmost_optout.insert(wid);
            }
            info!(?wid, "Unpinned topmost window");
            return;
        }

        info!(?wid, "Pinned topmost window");
        self.pin_topmost_window(wid);
    }

    /// Pin `wid` above other windows with a fresh reassert state and raise it now.
    /// Shared by the explicit toggle and by restore re-adoption so both seed the
    /// yield bookkeeping (`TopmostWindowState`) identically; the reassert loop and
    /// burial detection then treat a restored pin exactly like a freshly toggled
    /// one. Always an explicit pin (`implicit: false`); implicit float-sweep pins
    /// are re-derived from floating state by `sync_floating_topmost`.
    pub(crate) fn pin_topmost_window(&mut self, wid: WindowId) {
        self.topmost_optout.remove(&wid);
        self.topmost_windows.insert(wid, TopmostWindowState::default());
        self.raise_topmost_windows(vec![wid]);
    }

    fn sync_floating_topmost(&mut self) {
        if !self.config.settings.floating_windows_topmost {
            return;
        }
        self.topmost_optout
            .retain(|wid| self.state.windows.window(*wid).is_some());

        let spaces: Vec<SpaceId> = self
            .space_state
            .screens
            .iter()
            .filter_map(|screen| screen.space)
            .filter(|space| self.is_space_active(*space))
            .collect();
        let mut floating: HashSet<WindowId> = HashSet::default();
        for space in spaces {
            for wid in self
                .layout_manager
                .layout_engine
                .windows_in_active_workspace(&self.state.windows, space)
            {
                if self.layout_manager.layout_engine.is_window_floating(wid) {
                    floating.insert(wid);
                }
            }
        }

        self.topmost_windows
            .retain(|wid, state| !state.implicit || floating.contains(wid));
        for wid in floating {
            if !self.topmost_optout.contains(&wid) {
                self.topmost_windows.entry(wid).or_insert(TopmostWindowState {
                    implicit: true,
                    ..Default::default()
                });
            }
        }
    }

    fn reassert_topmost_windows(&mut self, except: Option<WindowId>) {
        if crate::sys::display_churn::is_active()
            || self.refresh_quarantine_manager.display_churn_active
            || !matches!(
                self.workspace_switch_manager.workspace_switch_state,
                WorkspaceSwitchState::Inactive
            )
        {
            return;
        }

        self.sync_floating_topmost();

        let now = Instant::now();
        let candidates: Vec<WindowId> = self.topmost_windows.keys().copied().collect();
        let mut order_only = Vec::new();

        for wid in candidates {
            if Some(wid) == except || !self.is_window_on_active_space(wid) {
                continue;
            }
            let Some(state) = self.topmost_windows.get(&wid).copied() else {
                continue;
            };
            if state
                .last_reassert
                .is_some_and(|last| now.duration_since(last) < TOPMOST_REASSERT_DEBOUNCE)
            {
                continue;
            }
            let Some(burier) = self.topmost_burier(wid) else {
                if let Some(state) = self.topmost_windows.get_mut(&wid) {
                    if state.failed_reasserts != 0 {
                        info!(
                            ?wid,
                            after_attempts = state.failed_reasserts,
                            "Topmost window surfaced; resetting reassert counter"
                        );
                    }
                    state.failed_reasserts = 0;
                }
                continue;
            };
            let burier_wid = self.state.windows.tracked_window_id(burier);
            if burier_wid.is_some() && burier_wid == self.main_window() {
                if let Some(state) = self.topmost_windows.get_mut(&wid) {
                    state.failed_reasserts = 0;
                }
                continue;
            }
            if state.failed_reasserts >= TOPMOST_MAX_FAILED_REASSERTS {
                debug!(?wid, "Topmost reassert attempts exhausted; awaiting next trigger");
                continue;
            }

            if let Some(s) = self.topmost_windows.get_mut(&wid) {
                s.last_reassert = Some(now);
                s.failed_reasserts += 1;
            }
            info!(
                ?wid,
                attempts = state.failed_reasserts,
                burier = burier.as_u32(),
                "Topmost window buried by unfocused window; sending order-only raise"
            );
            order_only.push(wid);
        }

        self.raise_topmost_windows(order_only);
    }

    fn raise_topmost_windows(&mut self, windows: Vec<WindowId>) {
        if windows.is_empty() {
            return;
        }

        let mut app_handles = HashMap::default();
        let raise_windows: Vec<Vec<WindowId>> = windows
            .into_iter()
            .filter(|wid| {
                self.insert_app_handle_for_window(&mut app_handles, *wid);
                app_handles.contains_key(&wid.pid)
            })
            .map(|wid| vec![wid])
            .collect();
        if raise_windows.is_empty() {
            return;
        }

        let msg = raise_manager::Event::RaiseRequest(RaiseRequest {
            raise_windows,
            focus_window: None,
            app_handles,
            focus_quiet: Quiet::Yes,
            kind: RaiseKind::OrderOnly,
        });
        if let Err(e) = self.communication_manager.raise_manager_tx.try_send(msg) {
            warn!("Failed to send order-only topmost raise request: {}", e);
        }
    }

    fn topmost_burier(&self, wid: WindowId) -> Option<WindowServerId> {
        let window = self.state.windows.window(wid)?;
        let wsid = window.info.sys_id?;
        if !self.state.windows.is_window_visible(wsid) {
            return None;
        }

        let frame = window_server::get_window(wsid)
            .map(|info| info.frame)
            .unwrap_or(window.info.frame);
        let points = topmost_sample_points(frame);

        points.iter().find_map(|point| {
            let hit = window_server::get_window_at_point(*point)?;
            (hit != wsid
                && !self.topmost_windows.keys().any(|topmost| {
                    self.state.windows.window(*topmost).and_then(|state| state.info.sys_id)
                        == Some(hit)
                }))
            .then_some(hit)
        })
    }

    pub fn warp_mouse(&mut self, point: CGPoint) {
        let Some(event_tap_tx) = self.communication_manager.event_tap_tx.clone() else {
            return;
        };
        _ = event_tap_tx.send(crate::actor::event_tap::Request::Warp(point));
    }

    fn space_for_cursor_screen(&self) -> Option<SpaceId> {
        current_cursor_location().ok().and_then(|point| self.screen_for_point(point)?.space)
    }

    pub(crate) fn save_cursor_for_workspace(&mut self, space: SpaceId) {
        let Some(workspace_id) = self.layout_manager.layout_engine.active_workspace(space) else {
            return;
        };
        let Ok(point) = current_cursor_location() else {
            return;
        };
        self.workspace_switch_manager
            .saved_workspace_cursors
            .insert((space, workspace_id), point);
    }

    fn save_cursor_for_cursor_workspace(&mut self) {
        let Some(space) = self.space_for_cursor_screen() else {
            return;
        };
        if self.is_space_active(space) {
            self.save_cursor_for_workspace(space);
        }
    }

    fn restored_cursor_for_workspace_window(&self, wid: WindowId) -> Option<CGPoint> {
        let space = self.best_space_for_window_id(wid)?;
        let workspace_id = self.layout_manager.layout_engine.active_workspace(space)?;
        let point = *self
            .workspace_switch_manager
            .saved_workspace_cursors
            .get(&(space, workspace_id))?;
        let frame = self.state.windows.window(wid)?.frame_monotonic;
        if frame.contains(point) && self.screen_for_point(point).is_some() {
            Some(point)
        } else {
            None
        }
    }

    fn restored_cursor_for_space(&self, space: SpaceId) -> Option<CGPoint> {
        let workspace_id = self.layout_manager.layout_engine.active_workspace(space)?;
        let point = *self
            .workspace_switch_manager
            .saved_workspace_cursors
            .get(&(space, workspace_id))?;
        self.screen_for_point(point).map(|_| point)
    }

    fn warp_mouse_to_space_center(&mut self, space: SpaceId) -> bool {
        let Some(screen) = self.space_state.screen_by_space(space) else {
            return false;
        };
        self.warp_mouse(screen.frame.mid());
        true
    }

    fn try_focus_or_warp_without_raise(
        &mut self,
        warp_space: Option<SpaceId>,
        focus_window: &mut Option<WindowId>,
    ) -> bool {
        if let Some(wid) = self.window_id_under_cursor() {
            *focus_window = Some(wid);
            return false;
        }
        if self.focus_untracked_window_under_cursor() {
            return true;
        }
        self.config.settings.mouse_follows_focus
            && warp_space.is_some_and(|space| self.warp_mouse_to_space_center(space))
    }

    fn insert_app_handle_for_window(
        &self,
        app_handles: &mut HashMap<pid_t, AppThreadHandle>,
        wid: WindowId,
    ) {
        if let Some(app) = self.app_manager.apps.get(&wid.pid) {
            app_handles.insert(wid.pid, app.handle.clone());
        }
    }

    fn expose_all_spaces(&mut self) {
        let spaces: Vec<SpaceId> = self
            .space_state
            .screens
            .iter()
            .filter_map(|screen| screen.space)
            .filter(|space| self.is_space_active(*space))
            .collect();
        for space in spaces {
            self.expose_space_if_known(space);
        }
    }

    fn window_is_standard(&self, id: WindowId) -> bool {
        self.state
            .windows
            .window(id)
            .is_some_and(|window| window.matches_filter(WindowFilter::EffectivelyManageable))
    }

    pub(crate) fn visible_spaces_for_layout(
        &self,
        include_inactive: bool,
    ) -> (Vec<SpaceId>, HashMap<SpaceId, CGPoint>) {
        let visible_spaces_input: Vec<(SpaceId, CGPoint)> = self
            .space_state
            .screens
            .iter()
            .filter_map(|screen| {
                let space = screen.space?;
                if !include_inactive && !self.is_space_active(space) {
                    return None;
                }
                Some((space, screen.frame.mid()))
            })
            .collect();

        let mut visible_space_centers = HashMap::default();
        for (space, center) in &visible_spaces_input {
            visible_space_centers.insert(*space, *center);
        }

        let visible_spaces = order_visible_spaces_by_position(visible_spaces_input.iter().cloned());

        (visible_spaces, visible_space_centers)
    }

    fn send_layout_event(&mut self, event: LayoutEvent) {
        let event_clone = event.clone();
        let response =
            self.layout_manager.layout_engine.handle_event(&mut self.state.windows, event);
        self.prepare_refocus_after_layout_event(&event_clone);
        let force_focus_warp = matches!(
            event_clone,
            LayoutEvent::WindowRemoved(_) | LayoutEvent::WindowRemovedPreserveFloating(_)
        );
        if force_focus_warp {
            let _ = self.update_layout_or_warn(false, false);
        }
        self.handle_layout_response(response, None, force_focus_warp);
        for space in self.space_state.iter_known_spaces() {
            self.layout_manager.layout_engine.debug_tree_desc(space, "after event", false);
        }
        // Any layout event mutated engine state; schedule a debounced save.
        self.mark_layout_dirty();
    }

    // Returns true if the window should be raised on mouse over considering
    // active workspace membership and potential occlusion of floating windows above it.
    pub(crate) fn should_raise_on_mouse_over(&self, wid: WindowId) -> bool {
        let Some(window) = self.state.windows.window(wid) else {
            return false;
        };

        // Float-aware FFM: when enabled, hovering a floating window never focuses/raises it.
        // Floats are reached by click or alt-tab; this stops mouse sweeps from focus-thrashing
        // through overlapping floats under default_floating (whitelist) mode.
        if self.config.settings.focus_follows_mouse_tiled_only
            && self.layout_manager.layout_engine.is_window_floating(wid)
        {
            return false;
        }

        if !window.matches_filter(WindowFilter::EffectivelyManageable)
            && !self.layout_manager.layout_engine.is_window_floating(wid)
        {
            return false;
        }

        let candidate_frame = window.frame_monotonic;

        if matches!(self.menu_manager.menu_state, MenuState::Open(_)) {
            trace!(?wid, "Skipping autoraise while menu open");
            return false;
        }

        let Some(space) = self.best_space_for_window(&candidate_frame, window.info.sys_id) else {
            return false;
        };
        if !self.is_space_active(space) {
            return false;
        }

        if !self.layout_manager.layout_engine.is_window_in_active_workspace(
            &self.state.windows,
            space,
            wid,
        ) {
            trace!("Ignoring mouse over window {:?} - not in active workspace", wid);
            return false;
        }

        if self.topmost_windows.contains_key(&wid) {
            return false;
        }

        let Some(candidate_wsid) = window.info.sys_id else {
            return true;
        };

        // FFM occlusion cache: the z-order list and per-window level queries below are synchronous
        // WindowServer round-trips on the reactor's serial thread, and this gate runs on EVERY window
        // crossing. The ffm-perf probe measured them at 10–148 ms per call during a fast multi-display
        // sweep — the reactor fell behind the cursor and window_focused emits trailed by 100–500 ms (the
        // "border lags the mouse" jank). Z-order/levels don't change at mouse-sweep timescales, so a
        // short-TTL cache makes repeat crossings free while a real occlusion change is picked up within
        // ~100 ms (one TTL). Reactor is single-threaded → thread_local is safe and lock-free.
        use std::cell::RefCell;
        use std::time::Instant;
        type NSWindowLevelT = objc2_app_kit::NSWindowLevel;
        const OCCLUSION_TTL: Duration = Duration::from_millis(100);
        thread_local! {
            static ZORDER_CACHE: RefCell<HashMap<u64, (Instant, Vec<u32>)>> =
                RefCell::new(HashMap::default());
            static LEVEL_CACHE: RefCell<HashMap<u32, (Instant, Option<NSWindowLevelT>, i32)>> =
                RefCell::new(HashMap::default());
        }
        fn cached_zorder(space_id: u64) -> Vec<u32> {
            ZORDER_CACHE.with(|c| {
                let mut cache = c.borrow_mut();
                let now = Instant::now();
                match cache.get(&space_id) {
                    Some((at, order)) if at.elapsed() < OCCLUSION_TTL => order.clone(),
                    _ => {
                        let order = crate::sys::window_server::space_window_list_for_connection(
                            &[space_id],
                            0,
                            false,
                        );
                        cache.insert(space_id, (now, order.clone()));
                        order
                    }
                }
            })
        }
        fn cached_levels(wid: u32) -> (Option<NSWindowLevelT>, i32) {
            LEVEL_CACHE.with(|c| {
                let mut cache = c.borrow_mut();
                let now = Instant::now();
                match cache.get(&wid) {
                    Some((at, level, sub)) if at.elapsed() < OCCLUSION_TTL => (*level, *sub),
                    _ => {
                        let level = window_level(wid);
                        let sub = window_sub_level(wid);
                        cache.insert(wid, (now, level, sub));
                        (level, sub)
                    }
                }
            })
        }

        let order = cached_zorder(space.get());
        let candidate_u32 = candidate_wsid.as_u32();
        let (candidate_level, candidate_sub_level) = cached_levels(candidate_u32);

        for above_u32 in order {
            if above_u32 == candidate_u32 {
                break;
            }

            let above_wsid = WindowServerId::new(above_u32);
            let Some(above_wid) = self.state.windows.tracked_window_id(above_wsid) else {
                continue;
            };

            let Some(above_state) = self.state.windows.window(above_wid) else {
                continue;
            };
            let above_frame = above_state.frame_monotonic;
            if candidate_frame.intersection(&above_frame).area() <= 64.0 {
                continue;
            }

            let (above_level, above_sub_level) = cached_levels(above_u32);
            if candidate_level
                .zip(above_level)
                .is_some_and(|(candidate, above)| candidate == above)
                && candidate_sub_level == above_sub_level
            {
                return false;
            }
        }

        true
    }

    fn process_windows_for_app_rules(
        &mut self,
        pid: pid_t,
        window_ids: Vec<WindowId>,
        app_info: AppInfo,
    ) {
        if window_ids.is_empty() {
            return;
        }

        let mut windows_by_space: BTreeMap<SpaceId, Vec<WindowId>> = BTreeMap::new();
        for &wid in &window_ids {
            let Some(state) = self.state.windows.window(wid) else {
                continue;
            };
            if !state.matches_filter(WindowFilter::Manageable) {
                continue;
            }
            let Some(space) = self.best_space_for_window_id(wid) else {
                continue;
            };
            windows_by_space.entry(space).or_default().push(wid);
        }

        for (space, wids) in windows_by_space {
            if !self.is_space_active(space) {
                continue;
            }
            let mut windows_needing_layout_refresh: Vec<WindowId> = Vec::new();

            for wid in &wids {
                let (was_assigned, was_floating, was_ignored) = {
                    let engine = &self.layout_manager.layout_engine;
                    (
                        engine
                            .virtual_workspace_manager()
                            .workspace_for_window(&self.state.windows, space, *wid)
                            .is_some(),
                        engine.is_window_floating(*wid),
                        self.state
                            .windows
                            .window(*wid)
                            .map(|window| window.ignore_app_rule)
                            .unwrap_or(false),
                    )
                };
                let assign_result = if let Some(adopted) = self.try_adopt_window(*wid, space) {
                    adopted
                } else {
                    let window_metadata = self.state.windows.window(*wid).map(|window| {
                        (
                            window.info.title.clone(),
                            window.info.ax_role.clone(),
                            window.info.ax_subrole.clone(),
                        )
                    });
                    self.layout_manager.layout_engine.assign_window_with_app_info(
                        &mut self.state.windows,
                        *wid,
                        space,
                        app_info.bundle_id.as_deref(),
                        app_info.localized_name.as_deref(),
                        window_metadata.as_ref().map(|metadata| metadata.0.as_str()),
                        window_metadata.as_ref().and_then(|metadata| metadata.1.as_deref()),
                        window_metadata.as_ref().and_then(|metadata| metadata.2.as_deref()),
                    )
                };

                match assign_result {
                    Ok(AppRuleResult::Managed(assignment)) => {
                        if let Some(window) = self.state.windows.window_mut(*wid) {
                            window.ignore_app_rule = false;
                        }

                        let effective_floating =
                            assignment.floating || (!assignment.prev_rule_decision && was_floating);
                        let needs_layout_refresh =
                            !was_assigned || was_floating != effective_floating || was_ignored;
                        if needs_layout_refresh {
                            windows_needing_layout_refresh.push(*wid);
                        }
                    }
                    Ok(AppRuleResult::Unmanaged) => {
                        if let Some(window) = self.state.windows.window_mut(*wid) {
                            window.ignore_app_rule = true;
                        }

                        let needs_removal = {
                            let engine = &self.layout_manager.layout_engine;
                            engine
                                .virtual_workspace_manager()
                                .workspace_for_window(&self.state.windows, space, *wid)
                                .is_some()
                                || engine.is_window_floating(*wid)
                        };
                        if needs_removal {
                            self.send_layout_event(LayoutEvent::WindowRemoved(*wid));
                        }
                    }
                    Err(e) => {
                        warn!("Failed to assign window {:?} to workspace: {:?}", wid, e);
                        if let Some(window) = self.state.windows.window_mut(*wid) {
                            window.ignore_app_rule = false;
                        }

                        if !was_assigned || was_ignored {
                            windows_needing_layout_refresh.push(*wid);
                        }
                    }
                }
            }

            if windows_needing_layout_refresh.is_empty() {
                continue;
            }

            let windows_with_titles: Vec<(
                WindowId,
                Option<String>,
                Option<String>,
                Option<String>,
                bool,
                CGSize,
                Option<CGSize>,
                Option<CGSize>,
            )> = windows_needing_layout_refresh
                .iter()
                .map(|&wid| {
                    let window = self.state.windows.window(wid);
                    let title_opt = window.map(|w| w.info.title.clone());
                    let ax_role = window.and_then(|w| w.info.ax_role.clone());
                    let ax_subrole = window.and_then(|w| w.info.ax_subrole.clone());
                    let is_resizable = window.map_or(true, |w| w.info.is_resizable);
                    let size_hint =
                        window.map_or(CGSize::new(0.0, 0.0), |w| w.frame_monotonic.size);
                    let min_size = window.and_then(|w| w.info.min_size);
                    let max_size = window.and_then(|w| w.info.max_size);
                    (
                        wid,
                        title_opt,
                        ax_role,
                        ax_subrole,
                        is_resizable,
                        size_hint,
                        min_size,
                        max_size,
                    )
                })
                .collect();

            self.send_layout_event(LayoutEvent::WindowsOnScreenUpdated(
                space,
                pid,
                windows_with_titles,
                Some(app_info.clone()),
            ));
        }
    }

    fn handle_app_activation_workspace_switch(&mut self, pid: pid_t) {
        use objc2_app_kit::NSRunningApplication;

        use crate::sys::app::NSRunningApplicationExt;

        if self.workspace_switch_manager.active_workspace_switch.is_some() {
            trace!(
                "Skipping auto workspace switch for pid {} because a workspace switch is in progress",
                pid
            );
            return;
        }

        if self.workspace_switch_manager.manual_switch_in_progress() {
            debug!(
                "Skipping auto workspace switch for pid {} because a manual switch is in progress",
                pid
            );
            return;
        }

        if let Some(active_space) = self.raw_command_space()
            && self.is_fullscreen_space(active_space)
        {
            debug!(
                "Skipping auto workspace switch for pid {} because the active space is fullscreen",
                pid
            );
            return;
        }

        if let Some(wsid) = self.activation_from_unmanageable_window(pid) {
            debug!(
                ?wsid,
                "Skipping auto workspace switch for pid {} because the activated window is not manageable",
                pid
            );
            return;
        }

        let visible_spaces: HashSet<SpaceId> = self.iter_active_spaces().collect();
        let app_is_on_visible_workspace =
            self.state.windows.iter_windows().any(|(wid, _window_state)| {
                if wid.pid != pid {
                    return false;
                }
                let Some(space) = self.best_space_for_window_id(wid) else {
                    return false;
                };
                if !visible_spaces.contains(&space) {
                    return false;
                }
                let Some(active_workspace) =
                    self.layout_manager.layout_engine.active_workspace(space)
                else {
                    return false;
                };
                self.layout_manager
                    .layout_engine
                    .virtual_workspace_manager()
                    .workspace_for_window(&self.state.windows, space, wid)
                    .is_some_and(|window_workspace| window_workspace == active_workspace)
            });

        if app_is_on_visible_workspace {
            debug!("App {} is already on a visible workspace, not switching.", pid);
            return;
        }

        let Some(app) = NSRunningApplication::with_process_id(pid) else {
            return;
        };
        let Some(bundle_id) = app.bundle_id() else {
            return;
        };
        let bundle_id_str = bundle_id.to_string();

        if self.config.settings.auto_focus_blacklist.contains(&bundle_id_str) {
            debug!(
                "App {} is blacklisted for auto-focus workspace switching, ignoring activation",
                bundle_id_str
            );
            return;
        }

        debug!(
            "App activation detected: {} (pid: {}), checking for workspace switch",
            bundle_id_str, pid
        );

        let app_window = self
            .main_window()
            .filter(|wid| wid.pid == pid && self.window_is_standard(*wid))
            .or_else(|| {
                self.state
                    .windows
                    .window_ids_for_pid(pid)
                    .find(|wid| self.window_is_standard(*wid))
            });

        let Some(app_window_id) = app_window else {
            return;
        };

        let Some(window_space) = self.best_space_for_window_id(app_window_id) else {
            return;
        };

        self.maybe_auto_switch_to_window_workspace(pid, app_window_id, window_space);
    }

    fn maybe_auto_switch_to_window_workspace(
        &mut self,
        pid: pid_t,
        app_window_id: WindowId,
        window_space: SpaceId,
    ) {
        let workspace_state = self.layout_manager.layout_engine.virtual_workspace_manager();
        let Some(window_workspace) =
            workspace_state.workspace_for_window(&self.state.windows, window_space, app_window_id)
        else {
            return;
        };

        let Some(current_workspace) =
            self.layout_manager.layout_engine.active_workspace(window_space)
        else {
            return;
        };

        if window_workspace != current_workspace {
            let workspaces = self
                .layout_manager
                .layout_engine
                .virtual_workspace_manager_mut()
                .list_workspaces(window_space);
            if let Some((workspace_index, _)) =
                workspaces.iter().enumerate().find(|(_, (ws_id, _))| *ws_id == window_workspace)
            {
                debug!(
                    "Auto-switching to workspace {} for activated app (pid: {})",
                    workspace_index, pid
                );

                self.store_current_floating_positions(window_space);
                self.workspace_switch_manager
                    .start_workspace_switch(WorkspaceSwitchOrigin::Auto);

                let response = self.layout_manager.layout_engine.switch_to_workspace_with_focus(
                    &self.state.windows,
                    window_space,
                    workspace_index,
                    app_window_id,
                );
                self.handle_layout_response(response, Some(window_space), false);
                self.update_event_tap_layout_mode();
            }
        }
    }

    fn handle_layout_response(
        &mut self,
        response: layout::EventResponse,
        workspace_switch_space: Option<SpaceId>,
        force_focus_warp: bool,
    ) {
        if self.is_in_drag() {
            self.workspace_switch_manager.mark_workspace_switch_inactive();
            return;
        }

        let mut pending_refocus_space =
            match std::mem::replace(&mut self.refocus_manager.refocus_state, RefocusState::None) {
                RefocusState::Pending(space) => Some(space),
                RefocusState::None => None,
            };
        let layout::EventResponse {
            raise_windows,
            mut focus_window,
            boundary_hit,
        } = response;

        if let Some(space) = workspace_switch_space
            && matches!(
                self.workspace_switch_manager.workspace_switch_state,
                WorkspaceSwitchState::Active
            )
        {
            focus_window = self.visible_focus_candidate_in_active_workspace(space, focus_window);
        }

        if let Some(dir) = boundary_hit
            && self.config.settings.layout.scrolling.gestures.propagate_to_workspace_swipe
        {
            let skip_empty = self.config.settings.gestures.skip_empty;
            let invert_horizontal =
                self.config.settings.layout.scrolling.gestures.invert_horizontal;
            let cmd = if invert_horizontal {
                match dir {
                    Direction::Left => Some(layout::LayoutCommand::NextWorkspace(Some(skip_empty))),
                    Direction::Right => {
                        Some(layout::LayoutCommand::PrevWorkspace(Some(skip_empty)))
                    }
                    _ => None,
                }
            } else {
                match dir {
                    Direction::Left => Some(layout::LayoutCommand::PrevWorkspace(Some(skip_empty))),
                    Direction::Right => {
                        Some(layout::LayoutCommand::NextWorkspace(Some(skip_empty)))
                    }
                    _ => None,
                }
            };
            if let Some(cmd) = cmd {
                let space = workspace_switch_space.or_else(|| self.command_context_space());
                if let Some(space) = space {
                    let resp = self.layout_manager.layout_engine.handle_virtual_workspace_command(
                        &mut self.state.windows,
                        space,
                        &cmd,
                    );

                    if self.config.settings.gestures.haptics_enabled {
                        let _ = crate::sys::haptics::perform_haptic(
                            self.config.settings.gestures.haptic_pattern,
                        );
                    }

                    // Recurse to handle the new response (e.g. focus window on the new workspace)
                    self.handle_layout_response(resp, Some(space), false);
                    self.update_event_tap_layout_mode();
                    return;
                }
            }
        }

        let original_focus = focus_window;

        if self.config.settings.restore_cursor_position_per_workspace
            && self.workspace_switch_manager.manual_switch_in_progress()
            && let Some(space) = workspace_switch_space
        {
            self.workspace_switch_manager.pending_workspace_cursor_warp =
                self.restored_cursor_for_space(space);
        }

        let focus_quiet = workspace_switch_space.map_or(Quiet::No, |_| Quiet::Yes);

        let handled_without_raise = if raise_windows.is_empty() && focus_window.is_none() {
            if matches!(
                self.workspace_switch_manager.workspace_switch_state,
                WorkspaceSwitchState::Active
            ) && !self.is_in_drag()
            {
                if let Some(wid) = workspace_switch_space
                    .filter(|_| self.workspace_switch_manager.manual_switch_in_progress())
                    .and_then(|space| self.last_focused_window_in_space(space))
                {
                    focus_window = Some(wid);
                    false
                } else if let Some(wid) = self.window_id_under_cursor() {
                    // Avoid duplicate focus events for the already focused window.
                    if self.main_window() != Some(wid) {
                        focus_window = Some(wid);
                    }
                    false
                } else {
                    let skip_center_warp = workspace_switch_space
                        .map(|space| {
                            self.layout_manager
                                .layout_engine
                                .windows_in_active_workspace(&self.state.windows, space)
                                .is_empty()
                        })
                        .unwrap_or(false);
                    let warp_space = if skip_center_warp {
                        None
                    } else {
                        workspace_switch_space.or_else(|| self.command_context_space())
                    };
                    self.try_focus_or_warp_without_raise(warp_space, &mut focus_window)
                }
            } else if let Some(space) = pending_refocus_space.take() {
                if let Some(wid) = self.last_focused_window_in_space(space) {
                    focus_window = Some(wid);
                    false
                } else if !self.is_in_drag() {
                    self.try_focus_or_warp_without_raise(Some(space), &mut focus_window)
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if let Some(wid) = focus_window
            && let Some(state) = self.state.windows.window(wid)
            && let Some(wsid) = state.info.sys_id
        {
            let is_visible = self.state.windows.is_window_visible(wsid);
            let best_space = self.best_space_for_window_state(state);
            if !is_visible {
                focus_window = None;
                if let Some(space) = workspace_switch_space
                    && !self.is_in_drag()
                {
                    let _ = self.try_focus_or_warp_without_raise(Some(space), &mut focus_window);
                }
            } else if !best_space.is_some_and(|space| self.is_space_active(space)) {
                focus_window = None;
            }
        }

        if raise_windows.is_empty() && focus_window.is_none() {
            if handled_without_raise {
                self.workspace_switch_manager.mark_workspace_switch_inactive();
            }
            if handled_without_raise
                || matches!(
                    self.workspace_switch_manager.workspace_switch_state,
                    WorkspaceSwitchState::Inactive
                )
            {
                return;
            }
        }

        if let Some(space) = pending_refocus_space {
            // Preserve the pending refocus request if it was not consumed above.
            if matches!(self.refocus_manager.refocus_state, RefocusState::None) {
                self.refocus_manager.refocus_state = RefocusState::Pending(space);
            }
        }

        let mut app_handles = HashMap::default();
        for &wid in raise_windows.iter() {
            self.insert_app_handle_for_window(&mut app_handles, wid);
        }

        if let Some(wid) = original_focus {
            self.insert_app_handle_for_window(&mut app_handles, wid);
        }

        let raise_windows: Vec<WindowId> = raise_windows
            .into_iter()
            .filter(|wid| self.is_window_on_active_space(*wid))
            .collect();
        let focus_window = focus_window.filter(|wid| self.is_window_on_active_space(*wid));
        if let Some(space) = workspace_switch_space {
            self.layout_manager.layout_engine.commit_workspace_focus(
                &mut self.state.windows,
                space,
                focus_window,
            );
        }
        let mut windows_by_app_and_screen = HashMap::default();
        for &wid in &raise_windows {
            windows_by_app_and_screen
                .entry((wid.pid, self.best_space_for_window_id(wid)))
                .or_insert(vec![])
                .push(wid);
        }
        let focus_window_with_warp = focus_window.map(|wid| {
            let warp = if force_focus_warp {
                self.window_center_on_known_screen(wid)
            } else if self.config.settings.mouse_follows_focus {
                if self.workspace_switch_manager.workspace_switch_state
                    == WorkspaceSwitchState::Active
                {
                    // During workspace switches, defer mouse warping until after layout completes.
                    self.workspace_switch_manager.pending_workspace_mouse_warp = Some(wid);
                    None
                } else {
                    self.window_center_on_known_screen(wid)
                }
            } else {
                None
            };
            (wid, warp)
        });

        let msg = raise_manager::Event::RaiseRequest(RaiseRequest {
            raise_windows: windows_by_app_and_screen.into_values().collect(),
            focus_window: focus_window_with_warp,
            app_handles,
            focus_quiet,
            kind: RaiseKind::Focus,
        });

        if let Err(e) = self.communication_manager.raise_manager_tx.try_send(msg) {
            warn!("Failed to send raise request to raise manager: {}", e);
        }
    }

    fn collect_drag_swap_candidates(
        &self,
        wid: WindowId,
        space: SpaceId,
    ) -> Vec<(WindowId, CGRect)> {
        self.state
            .windows
            .iter_windows()
            .filter_map(|(other_wid, other_state)| {
                if other_wid == wid {
                    return None;
                }
                let other_space = self.best_space_for_window_state(other_state)?;
                if other_space != space
                    || !self.layout_manager.layout_engine.is_window_in_active_workspace(
                        &self.state.windows,
                        space,
                        other_wid,
                    )
                    || self.layout_manager.layout_engine.is_window_floating(other_wid)
                {
                    return None;
                }
                Some((other_wid, other_state.frame_monotonic))
            })
            .collect()
    }

    fn maybe_swap_on_drag(&mut self, wid: WindowId, new_frame: CGRect) {
        if !self.is_in_drag() {
            trace!(?wid, "Skipping swap: not in drag (mouse up received)");
            return;
        }

        let server_id = {
            let Some(window) = self.state.windows.window(wid) else {
                return;
            };
            window.info.sys_id
        };

        let Some(space) = self
            .get_active_drag_session()
            .and_then(|session| session.settled_space)
            .or_else(|| self.best_space_for_window(&new_frame, server_id))
        else {
            return;
        };

        let origin_space_hint = self
            .get_active_drag_session()
            .and_then(|session| session.origin_space)
            .or_else(|| {
                self.drag_manager
                    .origin_frame()
                    .and_then(|frame| self.best_space_for_window(&frame, server_id))
            });

        if let Some(origin_space) = origin_space_hint
            && origin_space != space
        {
            if let Some((pending_wid, pending_target)) = self.get_pending_drag_swap()
                && pending_wid == wid
            {
                trace!(
                    ?wid,
                    ?pending_target,
                    ?origin_space,
                    ?space,
                    "Clearing pending drag swap; dragged window entered new space"
                );
                self.drag_manager.drag_state = DragState::Inactive;
            }
            trace!(
                ?wid,
                ?origin_space,
                ?space,
                "Resetting drag swap tracking after space change"
            );
            self.drag_manager.drag_swap_manager.reset();
            return;
        }

        if !self.layout_manager.layout_engine.is_window_in_active_workspace(
            &self.state.windows,
            space,
            wid,
        ) {
            return;
        }

        let candidates = self.collect_drag_swap_candidates(wid, space);

        let previous_pending = self.get_pending_drag_swap();
        let new_candidate =
            self.drag_manager.drag_swap_manager.on_frame_change(wid, new_frame, &candidates);
        let active_target = self.drag_manager.drag_swap_manager.last_target();
        if let Some(target_wid) = active_target {
            if new_candidate.is_some() || previous_pending != Some((wid, target_wid)) {
                trace!(
                    ?wid,
                    ?target_wid,
                    "Detected swap candidate; deferring until MouseUp"
                );
            }

            if let Some(session) = self.take_active_drag_session() {
                self.drag_manager.drag_state =
                    DragState::PendingSwap { session, target: target_wid };
            } else {
                trace!(
                    ?wid,
                    ?target_wid,
                    "Skipping pending swap; no active drag session"
                );
                self.drag_manager.drag_state = DragState::Inactive;
                self.drag_manager.skip_layout_for_window = None;
                return;
            }

            self.drag_manager.skip_layout_for_window = Some(wid);
            return;
        }

        if let Some((pending_wid, pending_target)) = previous_pending
            && pending_wid == wid
        {
            trace!(
                ?wid,
                ?pending_target,
                "Clearing pending drag swap; overlap ended before MouseUp"
            );
            if let Some(session) = self.take_active_drag_session() {
                self.drag_manager.drag_state = DragState::Active { session };
            } else {
                self.drag_manager.drag_state = DragState::Inactive;
            }
        }

        if self.drag_manager.skip_layout_for_window == Some(wid) {
            self.drag_manager.skip_layout_for_window = None;
        }
        // wait for mouse::up before doing *anything*
    }

    pub(crate) fn window_id_under_cursor(&self) -> Option<WindowId> {
        self.tracked_window_under_cursor().map(|(_, wid)| wid)
    }

    fn window_server_id_under_cursor(&self) -> Option<WindowServerId> {
        window_server::window_under_cursor()
    }

    fn tracked_window_under_cursor(&self) -> Option<(WindowServerId, WindowId)> {
        let wsid = self.window_server_id_under_cursor()?;
        let wid = self.state.windows.tracked_window_id(wsid)?;
        Some((wsid, wid))
    }

    fn activation_from_unmanageable_window(&self, pid: pid_t) -> Option<WindowServerId> {
        let (wsid, wid) = self.tracked_window_under_cursor()?;
        let window = self.state.windows.window(wid)?;
        (wid.pid == pid && !window.matches_filter(WindowFilter::EffectivelyManageable))
            .then_some(wsid)
    }

    fn focus_untracked_window_under_cursor(&mut self) -> bool {
        let Some(wsid) = self.window_server_id_under_cursor() else {
            return false;
        };
        if self.state.windows.tracked_window_id(wsid).is_some() {
            return false;
        }

        let window_info = self
            .state
            .windows
            .get_window_server_info(wsid)
            .or_else(|| window_server::get_window(wsid));

        let Some(info) = window_info else { return false };
        window_server::make_key_window(info.pid, wsid).is_ok()
    }

    fn last_focused_window_in_space(&self, space: SpaceId) -> Option<WindowId> {
        let active_workspace = self.layout_manager.layout_engine.active_workspace(space)?;
        let wid = self
            .layout_manager
            .layout_engine
            .virtual_workspace_manager()
            .last_focused_window(space, active_workspace)?;
        let window = self.state.windows.window(wid)?;

        if self.best_space_for_window_id(wid)? != space {
            return None;
        }
        if window
            .info
            .sys_id
            .is_some_and(|wsid| !self.state.windows.is_window_visible(wsid))
        {
            return None;
        }
        Some(wid)
    }

    fn visible_focus_candidate_in_active_workspace(
        &self,
        space: SpaceId,
        preferred: Option<WindowId>,
    ) -> Option<WindowId> {
        let is_visible_in_space = |wid: WindowId| {
            let Some(window) = self.state.windows.window(wid) else {
                return false;
            };
            let Some(wsid) = window.info.sys_id else {
                return false;
            };
            self.state.windows.is_window_visible(wsid)
                && self.best_space_for_window_id(wid) == Some(space)
                && self.layout_manager.layout_engine.is_window_in_active_workspace(
                    &self.state.windows,
                    space,
                    wid,
                )
        };

        if let Some(wid) = preferred.filter(|wid| is_visible_in_space(*wid)) {
            return Some(wid);
        }

        if let Some(wid) =
            self.last_focused_window_in_space(space).filter(|wid| is_visible_in_space(*wid))
        {
            return Some(wid);
        }

        self.layout_manager
            .layout_engine
            .windows_in_active_workspace(&self.state.windows, space)
            .into_iter()
            .find(|wid| is_visible_in_space(*wid))
    }

    fn request_refocus_if_hidden(&mut self, space: SpaceId, window_id: WindowId) {
        if self.window_in_non_active_workspace(space, window_id) {
            self.refocus_manager.refocus_state = RefocusState::Pending(space);
        }
    }

    fn window_in_non_active_workspace(&self, space: SpaceId, window_id: WindowId) -> bool {
        let Some(active_workspace) = self.layout_manager.layout_engine.active_workspace(space)
        else {
            return false;
        };
        self.layout_manager
            .layout_engine
            .virtual_workspace_manager()
            .workspace_for_window(&self.state.windows, space, window_id)
            .is_some_and(|window_workspace| window_workspace != active_workspace)
    }

    fn prepare_refocus_after_layout_event(&mut self, event: &LayoutEvent) {
        match event {
            LayoutEvent::WindowAdded(space, wid) => {
                self.request_refocus_if_hidden(*space, *wid);
            }
            LayoutEvent::WindowsOnScreenUpdated(space, _, windows, _) => {
                let hidden_exists = windows.iter().any(|(wid, _, _, _, _, _, _, _)| {
                    self.window_in_non_active_workspace(*space, *wid)
                });
                if hidden_exists {
                    self.refocus_manager.refocus_state = RefocusState::Pending(*space);
                }
            }
            _ => {}
        }
    }

    #[instrument(skip(self))]
    fn clear_menu_state_for_pid(&mut self, pid: pid_t) {
        if matches!(self.menu_manager.menu_state, MenuState::Open(owner) if owner == pid) {
            debug!(pid, "Clearing menu-open state for deactivated app");
            self.menu_manager.menu_state = MenuState::Closed;
            self.update_focus_follows_mouse_state();
        }
    }

    fn clear_menu_state_for_non_owner(&mut self, pid: pid_t) {
        if matches!(self.menu_manager.menu_state, MenuState::Open(owner) if owner != pid) {
            debug!(pid, "Clearing stale menu-open state after app focus changed");
            self.menu_manager.menu_state = MenuState::Closed;
            self.update_focus_follows_mouse_state();
        }
    }

    fn set_focus_follows_mouse_enabled(&self, enabled: bool) {
        if let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref() {
            event_tap_tx.send(event_tap::Request::SetFocusFollowsMouseEnabled(enabled));
        }
    }

    fn update_focus_follows_mouse_state(&mut self) {
        let should_enable = self.config.settings.focus_follows_mouse
            && matches!(self.menu_manager.menu_state, MenuState::Closed)
            && !self.is_mission_control_active();
        self.set_focus_follows_mouse_enabled(should_enable);
    }

    fn update_event_tap_layout_mode(&mut self) {
        let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref() else {
            return;
        };

        let last_modes = &self.notification_manager.last_layout_modes_by_space;
        let mut modes: Vec<(SpaceId, crate::common::config::LayoutMode)> =
            Vec::with_capacity(self.space_state.screens.len());
        let mut changed = false;

        for screen in &self.space_state.screens {
            let Some(space) = screen.space else {
                continue;
            };

            // Keep first occurrence only if multiple screens briefly report the same space.
            if modes.iter().any(|(existing, _)| *existing == space) {
                continue;
            }

            let mode = self.layout_manager.layout_engine.active_layout_mode_at(space);
            if last_modes.get(&space).copied() != Some(mode) {
                changed = true;
            }
            modes.push((space, mode));
        }

        if modes.is_empty() || (!changed && modes.len() == last_modes.len()) {
            return;
        }

        let modes_by_space = modes.iter().copied().collect();
        self.notification_manager.last_layout_modes_by_space = modes_by_space;
        if let Some(gesture_tap_tx) = self.communication_manager.gesture_tap_tx.as_ref() {
            gesture_tap_tx.send(gesture_tap::GestureRequest::LayoutModesChanged(modes.clone()));
        }
        event_tap_tx.send(crate::actor::event_tap::Request::LayoutModesChanged(modes));
    }

    fn set_mission_control_active(&mut self, active: bool) {
        let new_state = if active {
            MissionControlState::Active
        } else {
            MissionControlState::Inactive
        };
        if self.is_mission_control_active() == active {
            return;
        }
        self.mission_control_manager.mission_control_state = new_state;
        self.update_focus_follows_mouse_state();
    }

    fn refresh_windows_after_mission_control(&mut self) {
        debug!("Refreshing window state after Mission Control");
        // Skip when on a fullscreen space: kAXWindowsAttribute is space-filtered, so
        // apps omit their Desktop windows. check_for_new_windows sends an untracked
        // GetVisibleWindows whose response bypasses pending_mission_control_refresh,
        // causing those Desktop windows to be dropped from the layout, and other
        // windows in the layout to be incorrecctly resized.
        if !self.has_user_space_context() {
            return;
        }
        let active_windows = self.authoritative_active_space_windows();
        self.refresh_windows_after_mission_control_with_active_windows(active_windows);
    }

    fn refresh_windows_after_mission_control_with_active_windows(
        &mut self,
        active_windows: Vec<(WindowServerId, Option<SpaceId>)>,
    ) {
        if self.refreshes_blocked() {
            self.defer_visible_refresh(true);
            return;
        }

        // Mission Control can move windows between native spaces without emitting a
        // matching destroy/appear pair for the origin space. Reconcile the active
        // spaces from the same space-aware WS-id list used everywhere else so we do
        // not depend on the global CG on-screen window list during recovery.
        self.reconcile_authoritative_active_window_snapshot(active_windows, false);
        self.mission_control_manager.pending_mission_control_refresh.clear();
        self.force_refresh_all_windows();
        self.check_for_new_windows();
        self.update_layout_or_warn(false, false);
        self.maybe_send_menu_update();
    }

    // Uses the same "pending refresh" path as Mission Control recovery so a bulk
    // visibility rediscovery can reconcile tracked windows without treating a
    // transient empty AX window list as authoritative removal.
    fn force_refresh_all_windows(&mut self) { self.request_visible_windows_for_apps(true); }

    fn has_user_space_context(&self) -> bool {
        self.raw_command_space().is_some_and(|space| !self.is_fullscreen_space(space))
    }

    fn request_close_window(&mut self, pid: pid_t, window_server_id: Option<WindowServerId>) {
        if let Some(app) = self.app_manager.apps.get(&pid) {
            if let Err(err) = app.handle.send(Request::CloseWindow(window_server_id)) {
                warn!(
                    pid,
                    ?window_server_id,
                    "Failed to send close window request: {}",
                    err
                );
            }
        }
    }

    pub(crate) fn main_window(&self) -> Option<WindowId> { self.main_window_tracker.main_window() }

    fn main_window_space(&self) -> Option<SpaceId> {
        // TODO: Optimize this with a cache or something.
        let wid = self.main_window()?;
        self.best_space_for_window_id(wid)
    }

    /// Window discovery is scoped to one application. It may restore that
    /// application's current focus after its windows have been inserted into
    /// the layout, but it must never replay another application's global main
    /// window. Requiring the command space also prevents a refresh racing an
    /// active-display change from restoring focus on the display being left.
    fn focused_window_for_discovery(&self, pid: pid_t) -> Option<(SpaceId, WindowId)> {
        let window = self.main_window().filter(|window| window.pid == pid)?;
        let space = self.main_window_space()?;
        (self.workspace_command_space() == Some(space)).then_some((space, window))
    }

    fn raw_command_space(&self) -> Option<SpaceId> { self.space_state.command_space }

    fn active_display_space(&self) -> Option<SpaceId> {
        self.raw_command_space()
            .filter(|space| {
                self.space_state.active_spaces.contains(space)
                    && self.space_state.screens.iter().any(|screen| screen.space == Some(*space))
            })
            .or_else(|| {
                self.space_state
                    .screens
                    .iter()
                    .filter_map(|screen| screen.space)
                    .find(|space| self.space_state.active_spaces.contains(space))
            })
    }

    fn workspace_command_space(&self) -> Option<SpaceId> {
        self.active_display_space().filter(|space| self.is_space_active(*space))
    }

    fn command_context_space(&self) -> Option<SpaceId> {
        self.workspace_command_space().or_else(|| {
            self.layout_manager
                .layout_engine
                .focused_window()
                .and_then(|wid| {
                    self.assigned_space_for_window_id(wid)
                        .or_else(|| self.best_space_for_window_id(wid))
                })
                .filter(|space| self.is_space_active(*space))
                .or_else(|| self.main_window_space().filter(|space| self.is_space_active(*space)))
        })
    }

    fn screen_for_point(&self, point: CGPoint) -> Option<&ScreenInfo> {
        self.space_state.screens.iter().find(|screen| screen.frame.contains(point))
    }

    fn current_screen_center(&self) -> Option<CGPoint> {
        if let Some(space) = self.raw_command_space() {
            if let Some(screen) = self.space_state.screen_by_space(space) {
                return Some(screen.frame.mid());
            }
        }

        self.space_state.screens.first().map(|screen| screen.frame.mid())
    }

    fn screen_for_direction_from_point(
        &self,
        origin: CGPoint,
        direction: Direction,
    ) -> Option<&ScreenInfo> {
        fn interval_gap(a_min: f64, a_max: f64, b_min: f64, b_max: f64) -> f64 {
            if a_max < b_min {
                b_min - a_max
            } else if b_max < a_min {
                a_min - b_max
            } else {
                0.0
            }
        }

        let mut best: Option<(f64, f64, &ScreenInfo)> = None;

        for screen in &self.space_state.screens {
            let frame = screen.frame;

            if frame.contains(origin) {
                continue;
            }

            let min = frame.min();
            let max = frame.max();

            let (primary_dist, orth_gap) = match direction {
                Direction::Left => {
                    if max.x > origin.x {
                        continue;
                    }
                    (origin.x - max.x, interval_gap(min.y, max.y, origin.y, origin.y))
                }
                Direction::Right => {
                    if min.x < origin.x {
                        continue;
                    }
                    (min.x - origin.x, interval_gap(min.y, max.y, origin.y, origin.y))
                }
                Direction::Up => {
                    // Smaller y means visually "up".
                    if max.y > origin.y {
                        continue;
                    }
                    (origin.y - max.y, interval_gap(min.x, max.x, origin.x, origin.x))
                }
                Direction::Down => {
                    if min.y < origin.y {
                        continue;
                    }
                    (min.y - origin.y, interval_gap(min.x, max.x, origin.x, origin.x))
                }
            };

            // Prefer screens that overlap on the orthogonal axis (i.e. are on the
            // same row for left/right, or same column for up/down) before ranking
            // by distance in the requested direction. Ranking by distance first
            // lets a screen on a *different* row win just because its edge happens
            // to be closer — e.g. an ultrawide spanning the top would steal a
            // "right" move from a bottom-row neighbour.
            let should_replace = best.as_ref().map_or(true, |(best_primary, best_orth, _)| {
                orth_gap < *best_orth || (orth_gap == *best_orth && primary_dist < *best_primary)
            });

            if should_replace {
                best = Some((primary_dist, orth_gap, screen));
            }
        }

        best.map(|(_, _, screen)| screen)
    }

    fn screen_for_selector(
        &self,
        selector: &DisplaySelector,
        origin_override: Option<CGPoint>,
    ) -> Option<&ScreenInfo> {
        match selector {
            DisplaySelector::Direction(direction) => {
                let origin = origin_override.or_else(|| self.current_screen_center())?;
                self.screen_for_direction_from_point(origin, *direction)
            }
            DisplaySelector::Index(index) => self.screens_in_physical_order().get(*index).copied(),
            DisplaySelector::Uuid(uuid) => {
                self.space_state.screens.iter().find(|screen| screen.display_uuid == *uuid)
            }
        }
    }

    fn screens_in_physical_order(&self) -> Vec<&ScreenInfo> {
        let mut screens: Vec<&ScreenInfo> = self.space_state.screens.iter().collect();
        screens.sort_by(|a, b| {
            let x_order = a.frame.origin.x.total_cmp(&b.frame.origin.x);
            if x_order == std::cmp::Ordering::Equal {
                a.frame.origin.y.total_cmp(&b.frame.origin.y)
            } else {
                x_order
            }
        });
        screens
    }

    fn store_current_floating_positions(&mut self, space: SpaceId) {
        let floating_windows_in_workspace = self
            .layout_manager
            .layout_engine
            .windows_in_active_workspace(&self.state.windows, space)
            .into_iter()
            .filter(|&wid| self.layout_manager.layout_engine.is_window_floating(wid))
            .filter_map(|wid| {
                self.state
                    .windows
                    .window(wid)
                    .map(|window_state| (wid, window_state.frame_monotonic))
            })
            .collect::<Vec<_>>();

        if !floating_windows_in_workspace.is_empty() {
            self.layout_manager
                .layout_engine
                .store_floating_window_positions(space, &floating_windows_in_workspace);
        }
    }

    pub(crate) fn update_layout_or_warn(
        &mut self,
        is_resize: bool,
        is_workspace_switch: bool,
    ) -> bool {
        self.update_layout_or_warn_with(is_resize, is_workspace_switch, "Layout update failed")
    }

    pub(crate) fn update_layout_or_warn_with(
        &mut self,
        is_resize: bool,
        is_workspace_switch: bool,
        context: &'static str,
    ) -> bool {
        LayoutManager::update_layout(self, is_resize, is_workspace_switch).unwrap_or_else(|e| {
            warn!(error = ?e, "{}", context);
            false
        })
    }
}
