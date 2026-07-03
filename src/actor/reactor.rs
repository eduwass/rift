//! The Reactor's job is to maintain coherence between the system and model state.
//!
//! It takes events from the rest of the system and builds a coherent picture of
//! what is going on. It shares this with the layout actor, and reacts to layout
//! changes by sending requests out to the other actors in the system.

mod animation;
mod display_topology;
mod events;
mod main_window;
mod managers;
mod query;
mod replay;
pub mod transaction_manager;
mod utils;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod tests;

use std::thread;
use std::time::{Duration, Instant};

use dispatchr::queue;
use dispatchr::time::Time;
use events::app::AppEventHandler;
use events::command::CommandEventHandler;
use events::drag::DragEventHandler;
use events::space::SpaceEventHandler;
use events::system::SystemEventHandler;
use events::window::WindowEventHandler;
use main_window::MainWindowTracker;
use managers::LayoutManager;
use objc2_app_kit::NSRunningApplication;
use objc2_core_foundation::{CGPoint, CGRect, CGSize};
pub use replay::{Record, replay};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::{debug, info, instrument, trace, warn};
use transaction_manager::TransactionId;

use super::event_tap;
use super::gesture_tap;
use crate::actor::app::{
    AppInfo, AppThreadHandle, Quiet, RaiseKind, Request, WindowId, WindowInfo, pid_t,
};
use crate::actor::broadcast::{BroadcastEvent, BroadcastSender};
use crate::actor::raise_manager::{self, RaiseManager, RaiseRequest};
use crate::actor::reactor::events::window_discovery::WindowDiscoveryHandler;
use crate::actor::{self, menu_bar, stack_line};
use crate::common::collections::{BTreeMap, HashMap, HashSet};
use crate::common::config::Config;
use crate::layout_engine::{self as layout, Direction, LayoutEngine, LayoutEvent};
use crate::model::space_activation::{SpaceActivationConfig, SpaceActivationPolicy};
use crate::model::tx_store::WindowTxStore;
use crate::model::virtual_workspace::AppRuleResult;
use crate::sys::dispatch::DispatchExt;
use crate::sys::event::MouseState;
use crate::sys::executor::Executor;
use crate::sys::geometry::{CGRectDef, CGRectExt};
pub use crate::sys::screen::ScreenInfo;
use crate::sys::screen::{SpaceId, get_active_space_number, order_visible_spaces_by_position};
use crate::sys::window_server::{
    self, WindowServerId, WindowServerInfo, current_cursor_location, space_is_fullscreen,
    wait_for_native_fullscreen_transition,
};

fn topmost_sample_points(frame: CGRect) -> [CGPoint; 5] {
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
pub use query::ReactorQueryHandle;

pub(crate) use crate::model::reactor::{
    AppState, FullscreenSpaceTrack, FullscreenWindowTrack, PendingSpaceChange, WindowFilter,
    WindowState,
};
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
    pub fn new(sender: Sender, queries: ReactorQueryHandle) -> Self {
        Self { sender, queries }
    }

    pub fn sender(&self) -> Sender {
        self.sender.clone()
    }

    pub fn send(&self, event: Event) {
        self.sender.send(event)
    }

    pub fn try_send(
        &self,
        event: Event,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<(tracing::Span, Event)>> {
        self.sender.try_send(event)
    }
}

impl std::ops::Deref for ReactorHandle {
    type Target = ReactorQueryHandle;

    fn deref(&self) -> &Self::Target {
        &self.queries
    }
}

use display_topology::{DisplaySnapshot, DisplayTopologyManager, WindowSnapshot};

use crate::model::server::WindowData;

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    /// The screen layout, including resolution, changed. This is always the
    /// first event sent on startup.
    ///
    /// The first vec is the snapshot for each screen. The main screen is always
    /// first in the list.
    ScreenParametersChanged(Vec<ScreenInfo>),

    /// The current space changed.
    ///
    /// There is one SpaceId per screen in the last ScreenParametersChanged
    /// event. `None` in the SpaceId vec disables managing windows on that
    /// screen until the next space change.
    SpaceChanged(Vec<Option<SpaceId>>),

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
    WindowServerDestroyed(crate::sys::window_server::WindowServerId, SpaceId),
    #[serde(skip)]
    WindowServerAppeared(crate::sys::window_server::WindowServerId, SpaceId),
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
    ResyncAppForWindow(WindowServerId),
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
    /// The mouse cursor moved over a new window. Only sent if focus-follows-
    /// mouse is enabled.
    MouseMovedOverWindow(WindowServerId),
    /// The mouse cursor moved within the active desktop. Used to remember the
    /// latest point per workspace even when focus-follows-mouse does not emit a
    /// window-change event.
    MouseMoved,
    /// System woke from sleep; used to re-subscribe SLS notifications.
    SystemWoke,

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
    window_manager: managers::WindowManager,
    window_server_info_manager: managers::WindowServerInfoManager,
    space_manager: managers::SpaceManager,
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
    pending_space_change_manager: managers::PendingSpaceChangeManager,
    active_spaces: HashSet<SpaceId>,
    display_topology_manager: DisplayTopologyManager,
    // After move-window-to-display, the cursor warp must wait until the window has physically
    // landed on its destination; warping immediately lands on a neighbour (because the move is
    // async) and focus-follows-mouse then steals focus. Holds (window, destination display rect,
    // deadline) and fires once the window's centre is inside that rect or the deadline passes.
    pending_display_move_warp: Option<(WindowId, CGRect, std::time::Instant)>,
    topmost_windows: HashMap<WindowId, TopmostWindowState>,
    /// Windows explicitly un-pinned via toggle-topmost while
    /// `floating_windows_topmost` is on, so the float sweep doesn't re-pin them.
    topmost_optout: HashSet<WindowId>,
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
        // FIXME: Remove apps that are no longer running from restored state.
        record.start(&config, &layout_engine);
        let (raise_manager_tx, _rx) = actor::channel();
        let (window_notify_tx, window_tx_store) = match window_notify {
            Some((tx, store)) => (Some(tx), store),
            None => (None, WindowTxStore::new()),
        };
        Reactor {
            config: config.clone(),
            one_space,
            app_manager: managers::AppManager::new(),
            layout_manager: managers::LayoutManager { layout_engine },
            window_manager: managers::WindowManager {
                windows: HashMap::default(),
                window_ids: HashMap::default(),
                visible_windows: HashSet::default(),
                observed_window_server_ids: HashSet::default(),
            },
            window_server_info_manager: managers::WindowServerInfoManager {
                window_server_info: HashMap::default(),
            },
            space_manager: managers::SpaceManager {
                screens: vec![],
                fullscreen_by_space: HashMap::default(),
                has_seen_display_set: false,
            },
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
            pending_space_change_manager: managers::PendingSpaceChangeManager {
                pending_space_change: None,
                topology_relayout_pending: false,
            },
            active_spaces: HashSet::default(),
            display_topology_manager: DisplayTopologyManager::default(),
            pending_display_move_warp: None,
            topmost_windows: HashMap::default(),
            topmost_optout: HashSet::default(),
        }
    }

    fn set_active_spaces(&mut self, spaces: &[Option<SpaceId>]) {
        self.active_spaces.clear();
        for space in spaces.iter().flatten().copied() {
            self.active_spaces.insert(space);
        }
    }

    fn is_space_active(&self, space: SpaceId) -> bool {
        self.active_spaces.contains(&space)
    }

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

    fn screens_for_current_spaces(&self) -> Vec<ScreenInfo> {
        self.space_manager.screens.clone()
    }

    fn screens_for_spaces(&self, spaces: &[Option<SpaceId>]) -> Vec<ScreenInfo> {
        self.space_manager
            .screens
            .iter()
            .zip(spaces.iter().copied())
            .map(|(screen, space)| ScreenInfo { space, ..screen.clone() })
            .collect()
    }

    fn display_uuids_for_current_screens(&self) -> Vec<Option<String>> {
        self.space_manager
            .screens
            .iter()
            .map(|screen| screen.display_uuid_owned())
            .collect()
    }

    fn raw_spaces_for_current_screens(&self) -> Vec<Option<SpaceId>> {
        self.space_manager.screens.iter().map(|s| s.space).collect()
    }

    fn display_uuid_for_space(&self, space: SpaceId) -> Option<String> {
        self.space_manager
            .screen_by_space(space)
            .and_then(|screen| screen.display_uuid_owned())
    }

    fn expose_space_if_known(&mut self, space: SpaceId) {
        let Some(screen) = self.space_manager.screen_by_space(space) else {
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
        let raw_spaces = self.raw_spaces_for_current_screens();
        self.recompute_and_set_active_spaces(&raw_spaces);
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

        for (&wid, state) in &self.window_manager.windows {
            if !state.matches_filter(WindowFilter::Manageable) {
                continue;
            }
            let Some(space) = self.best_space_for_window_state(state) else {
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
        let ws_info = self.authoritative_window_snapshot_for_active_spaces();
        self.update_complete_window_server_info(ws_info);
    }

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
        if self.is_mission_control_active() || self.is_in_drag() {
            return;
        }
        let on_screen: HashSet<WindowServerId> =
            window_server::get_visible_windows_with_layer(None)
                .into_iter()
                .map(|i| i.id)
                .collect();
        let mut pids: HashSet<pid_t> = HashSet::default();
        let mut dead_pids: HashSet<pid_t> = HashSet::default();
        for space in self.active_spaces.clone() {
            for wid in self.layout_manager.layout_engine.windows_in_active_workspace(space) {
                if self.layout_manager.layout_engine.is_window_floating(wid) {
                    continue;
                }
                let Some(state) = self.window_manager.windows.get(&wid) else {
                    continue;
                };
                if state.info.is_minimized {
                    continue;
                }
                if let Some(ws_id) = state.info.sys_id
                    && !on_screen.contains(&ws_id)
                {
                    if NSRunningApplication::runningApplicationWithProcessIdentifier(wid.pid)
                        .is_none()
                    {
                        dead_pids.insert(wid.pid);
                    } else {
                        pids.insert(wid.pid);
                    }
                }
            }
        }
        for pid in dead_pids {
            pids.remove(&pid);
            self.app_manager.apps.remove(&pid);
            let dead_windows: Vec<WindowId> = self
                .window_manager
                .windows
                .keys()
                .copied()
                .filter(|wid| wid.pid == pid)
                .collect();
            for wid in dead_windows {
                WindowEventHandler::handle_window_destroyed(self, wid);
            }
            self.send_layout_event(LayoutEvent::AppClosed(pid));
        }
        for pid in pids {
            if let Some(app) = self.app_manager.apps.get(&pid) {
                let _ = app.handle.send(Request::GetVisibleWindows);
            }
        }
    }

    /// Rebuild only the on-screen window-server id set from live state, without touching
    /// cached frames/info. Apps that order a window out on close without emitting a
    /// destroy notification (Electron: Slack, ChatGPT, …) otherwise leave a stale id in
    /// `visible_windows`, which makes rift believe the window is still on screen and
    /// blocks orphan reconciliation. Cheap enough to run before each discovery pass.
    pub(crate) fn refresh_visible_windows_snapshot(&mut self) {
        let ws_info = self.authoritative_window_snapshot_for_active_spaces();
        self.window_manager.visible_windows.clear();
        self.window_manager.visible_windows.extend(ws_info.iter().map(|info| info.id));
    }

    fn authoritative_window_snapshot_for_active_spaces(&self) -> Vec<WindowServerInfo> {
        let ws_info = window_server::get_visible_windows_with_layer(None);
        self.filter_ws_info_to_active_spaces(ws_info)
    }

    fn build_display_snapshot(&self, ws_info: Vec<WindowServerInfo>) -> DisplaySnapshot {
        let ordered_screens = self.space_manager.screens.clone();
        let active_spaces = self.active_spaces.clone();

        let mut inactive_spaces: HashSet<SpaceId> = HashSet::default();
        for space in ordered_screens.iter().filter_map(|s| s.space) {
            if !active_spaces.contains(&space) {
                inactive_spaces.insert(space);
            }
        }

        let windows = ws_info.into_iter().map(|info| (info.id, WindowSnapshot { info })).collect();

        DisplaySnapshot {
            ordered_screens,
            active_spaces,
            inactive_spaces,
            windows,
        }
    }

    fn maybe_commit_display_topology_snapshot(&mut self) {
        let Some((epoch, started_at, flags, pre_known_wsids)) =
            self.display_topology_manager.take_awaiting_commit()
        else {
            return;
        };

        if self.space_manager.screens.is_empty()
            || self.space_manager.screens.iter().any(|screen| screen.space.is_none())
        {
            // Topology is not stable yet; keep waiting for the next complete snapshot.
            self.display_topology_manager.restore_awaiting_commit(
                epoch,
                started_at,
                flags,
                pre_known_wsids,
            );
            return;
        }

        let ws_info = self.authoritative_window_snapshot_for_active_spaces();
        let snapshot = self.build_display_snapshot(ws_info);
        self.reconcile_windows_after_topology_commit(
            epoch,
            started_at,
            flags,
            pre_known_wsids,
            snapshot,
        );
        self.display_topology_manager.mark_stable();
    }

    fn reconcile_windows_after_topology_commit(
        &mut self,
        epoch: u64,
        started_at: std::time::Instant,
        flags: crate::sys::skylight::DisplayReconfigFlags,
        pre_known_wsids: HashSet<WindowServerId>,
        snapshot: DisplaySnapshot,
    ) {
        let post_visible_wsids: HashSet<WindowServerId> =
            snapshot.windows.keys().copied().collect();
        let appeared: Vec<WindowServerId> =
            post_visible_wsids.difference(&pre_known_wsids).copied().collect();
        let disappeared: Vec<WindowServerId> =
            pre_known_wsids.difference(&post_visible_wsids).copied().collect();

        let mut synthetic_appeared = 0u64;
        let mut synthetic_destroyed = 0u64;

        for wsid in appeared {
            let Some(snapshot_window) = snapshot.windows.get(&wsid) else {
                continue;
            };
            if snapshot_window.info.layer != 0 {
                continue;
            }
            let Some(space) = window_server::window_space(wsid) else {
                continue;
            };
            if !self.is_space_active(space) && !window_server::space_is_user(space.get()) {
                continue;
            }
            SpaceEventHandler::handle_window_server_appeared(self, wsid, space);
            synthetic_appeared += 1;
        }

        for wsid in disappeared {
            let still_exists = window_server::get_window(wsid).is_some();
            let spaces = window_server::window_spaces(wsid);
            let in_user_or_active = spaces.iter().any(|space| {
                window_server::space_is_user(space.get()) || self.is_space_active(*space)
            });
            if still_exists && in_user_or_active {
                continue;
            }
            let sid = window_server::window_space(wsid)
                .or_else(|| self.space_manager.first_known_space());
            let Some(sid) = sid else {
                continue;
            };
            SpaceEventHandler::handle_window_server_destroyed(self, wsid, sid);
            synthetic_destroyed += 1;
        }

        self.force_refresh_all_windows();
        let _ = self.update_layout_or_warn_with(
            false,
            false,
            "Layout update failed after display churn commit",
        );

        info!(
            epoch,
            flags = ?flags,
            duration_ms = started_at.elapsed().as_millis(),
            synthetic_appeared,
            synthetic_destroyed,
            active_spaces = snapshot.active_spaces.len(),
            inactive_spaces = snapshot.inactive_spaces.len(),
            screens = snapshot.ordered_screens.len(),
            "display topology commit reconciled"
        );
    }

    fn filter_ws_info_to_active_spaces(
        &self,
        ws_info: Vec<WindowServerInfo>,
    ) -> Vec<WindowServerInfo> {
        let active_space_ids = self.active_space_ids();
        if active_space_ids.is_empty() {
            return Vec::new();
        }

        let active_window_ids: std::collections::HashSet<u32> =
            crate::sys::window_server::space_window_list_for_connection(
                &active_space_ids,
                0,
                false,
            )
            .into_iter()
            .collect();

        ws_info
            .into_iter()
            .filter(|w| active_window_ids.contains(&w.id.as_u32()))
            .collect()
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

    fn get_active_drag_session_mut(&mut self) -> Option<&mut DragSession> {
        if let DragState::Active { session } = &mut self.drag_manager.drag_state {
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
        reactor.communication_manager.raise_manager_tx = raise_manager_tx.clone();
        let event_tap_tx = reactor.communication_manager.event_tap_tx.clone();
        let reconcile_tx = events_tx.clone();
        let reactor_task = Self::run_reactor_loop(reactor, events);
        let raise_manager_task = RaiseManager::run(raise_manager_rx, events_tx, event_tap_tx);
        // Backstop for windows that close without notifying rift (e.g. Slack): tick a
        // reconciliation so the space is reclaimed within ~1s even when no event fires.
        let reconcile_task = async move {
            loop {
                crate::sys::timer::Timer::sleep(Duration::from_millis(1000)).await;
                if reconcile_tx.try_send(Event::ReconcileOrphans).is_err() {
                    break;
                }
            }
        };
        let _ = tokio::join!(reactor_task, raise_manager_task, reconcile_task);
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
        if self.maybe_quarantine_during_churn(&event) {
            Self::note_windowserver_activity(&event);
            trace!(?event, "quarantined event during display churn");
            return;
        }
        Self::note_windowserver_activity(&event);
        self.handle_event(event);
    }

    fn note_windowserver_activity(event: &Event) {
        let wsid = match event {
            Event::WindowFrameChanged(wid, ..) => Some(wid.idx.get()),
            Event::WindowCreated(wid, ..) => Some(wid.idx.get()),
            Event::WindowDestroyed(wid) => Some(wid.idx.get()),
            Event::WindowMinimized(wid) => Some(wid.idx.get()),
            Event::WindowDeminiaturized(wid) => Some(wid.idx.get()),
            Event::MouseMovedOverWindow(wsid) => Some(wsid.as_u32()),
            Event::ResyncAppForWindow(wsid) => Some(wsid.as_u32()),
            Event::WindowServerDestroyed(wsid, _) => Some(wsid.as_u32()),
            Event::WindowServerAppeared(wsid, _) => Some(wsid.as_u32()),
            _ => None,
        };
        if let Some(wsid) = wsid {
            window_server::note_windowserver_activity(wsid);
        }
    }

    fn log_event(&self, event: &Event) {
        match event {
            Event::MouseMoved => trace!(?event, "Event"),
            Event::WindowFrameChanged(..) | Event::MouseUp | Event::MouseDown => {
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
                | Event::SpaceChanged(..)
                | Event::ScreenParametersChanged(..)
        )
    }

    fn should_process_during_churn(event: &Event) -> bool {
        matches!(
            event,
            Event::DisplayChurnBegin
                | Event::DisplayChurnEnd
                | Event::ScreenParametersChanged(..)
                | Event::SpaceChanged(..)
                | Event::SpaceCreated(..)
                | Event::SpaceDestroyed(..)
                | Event::MissionControlNativeEntered
                | Event::MissionControlNativeExited
                | Event::SystemWoke
                | Event::ApplicationLaunched { .. }
                | Event::ApplicationTerminated(..)
                | Event::ApplicationThreadTerminated(..)
                | Event::ApplicationActivated(..)
                | Event::ApplicationDeactivated(..)
                | Event::ApplicationGloballyActivated(..)
                | Event::ApplicationGloballyDeactivated(..)
                | Event::ApplicationMainWindowChanged(..)
                | Event::RegisterWmSender(..)
                | Event::ConfigUpdated(..)
                | Event::Command(..)
                | Event::RaiseCompleted { .. }
                | Event::RaiseTimeout { .. }
                | Event::MenuOpened(..)
                | Event::MenuClosed(..)
        )
    }

    fn maybe_quarantine_during_churn(&mut self, event: &Event) -> bool {
        if !self.display_topology_manager.is_churning_or_awaiting_commit() {
            return false;
        }
        if Self::should_process_during_churn(event) {
            return false;
        }

        match event {
            Event::ResyncAppForWindow(..) => self.display_topology_manager.quarantine_resync(),
            Event::WindowServerDestroyed(..) => {
                self.display_topology_manager.quarantine_destroyed()
            }
            Event::WindowServerAppeared(..) => self.display_topology_manager.quarantine_appeared(),
            _ => {}
        }
        true
    }

    fn set_login_window_active(&mut self, active: bool) {
        self.space_activation_policy.set_login_window_active(active);
        self.recompute_and_set_active_spaces_from_current_screens();
    }

    fn handle_space_lifecycle(&mut self, space: SpaceId, created: bool) {
        if created {
            self.space_activation_policy.on_space_created(space);
        } else {
            self.space_activation_policy.on_space_destroyed(space);
        }
        self.recompute_and_set_active_spaces_from_current_screens();
    }

    #[instrument(name = "reactor::handle_event", skip(self), fields(event=?event))]
    fn handle_event(&mut self, event: Event) {
        self.log_event(&event);
        self.recording_manager.record.on_event(&event);

        match event {
            Event::DisplayChurnBegin => {
                let mut pre_known_wsids: HashSet<WindowServerId> = HashSet::default();
                pre_known_wsids.extend(self.window_manager.window_ids.keys().copied());
                pre_known_wsids
                    .extend(self.window_server_info_manager.window_server_info.keys().copied());
                pre_known_wsids.extend(self.window_manager.visible_windows.iter().copied());

                let epoch = crate::sys::display_churn::epoch();
                let flags = crate::sys::display_churn::flags();
                self.display_topology_manager.begin_churn(epoch, flags, pre_known_wsids);
                return;
            }
            Event::DisplayChurnEnd => {
                let (epoch, _, flags) = self.display_topology_manager.current_churn().unwrap_or((
                    crate::sys::display_churn::epoch(),
                    std::time::Instant::now(),
                    crate::sys::display_churn::flags(),
                ));
                self.display_topology_manager.end_churn_to_awaiting(epoch, flags);
                return;
            }
            _ => {}
        }

        if self.maybe_quarantine_during_churn(&event) {
            trace!(?event, "quarantined event during display churn");
            return;
        }

        let should_update_notifications = Self::should_update_notifications(&event);
        let should_reassert_topmost = matches!(
            &event,
            Event::ApplicationActivated(_, _)
                | Event::ApplicationMainWindowChanged(_, _, _)
                | Event::MouseMovedOverWindow(_)
                | Event::MouseUp
                | Event::MouseDown
        );
        let main_window_changed = match &event {
            Event::ApplicationMainWindowChanged(_, Some(wid), Quiet::No) => Some(*wid),
            _ => None,
        };
        let raised_window = self.main_window_tracker.handle_event(&event);
        if let Some(wid) = main_window_changed {
            self.workspace_switch_manager.pending_workspace_mouse_warp = Some(wid);
        }
        let mut is_resize = false;
        let mut window_was_destroyed = false;

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
                AppEventHandler::handle_application_launched(
                    self,
                    pid,
                    info,
                    handle,
                    visible_windows,
                    window_server_info,
                    is_frontmost,
                    main_window,
                );
            }
            Event::ApplicationTerminated(pid) => {
                AppEventHandler::handle_application_terminated(self, pid);
            }
            Event::ApplicationThreadTerminated(pid) => {
                self.clear_menu_state_for_pid(pid);
                AppEventHandler::handle_application_thread_terminated(self, pid);
            }
            Event::ApplicationActivated(pid, quiet) => {
                self.clear_menu_state_for_non_owner(pid);
                AppEventHandler::handle_application_activated(self, pid, quiet);
            }
            Event::ApplicationDeactivated(pid) => {
                self.clear_menu_state_for_pid(pid);
                self.schedule_orphan_reconcile(pid);
            }
            Event::ApplicationGloballyDeactivated(pid) => {
                self.clear_menu_state_for_pid(pid);
                if self.is_login_window_pid(pid) {
                    self.set_login_window_active(false);
                }
            }
            Event::ResyncAppForWindow(wsid) => {
                AppEventHandler::handle_resync_app_for_window(self, wsid);
            }
            Event::ApplicationGloballyActivated(pid) => {
                self.clear_menu_state_for_non_owner(pid);
                if self.is_login_window_pid(pid) {
                    self.set_login_window_active(true);

                    let raw_spaces = self.raw_spaces_for_current_screens();
                    self.reconcile_spaces_with_display_history(&raw_spaces, false);

                    self.force_refresh_all_windows();
                } else if self.space_activation_policy.login_window_active {
                    // macOS sometimes activates loginwindow during wake without sending a
                    // corresponding deactivation. Any subsequent non-login activation
                    // indicates the user is back, so clear suppression.
                    self.set_login_window_active(false);
                }
            }
            Event::RegisterWmSender(sender) => {
                SystemEventHandler::handle_register_wm_sender(self, sender)
            }
            Event::WindowsDiscovered { pid, new, known_visible } => {
                AppEventHandler::handle_windows_discovered(self, pid, new, known_visible);
            }
            Event::WindowCreated(wid, window, ws_info, mouse_state) => {
                WindowEventHandler::handle_window_created(self, wid, window, ws_info, mouse_state);
            }
            Event::WindowDestroyed(wid) => {
                window_was_destroyed = WindowEventHandler::handle_window_destroyed(self, wid);
            }
            Event::WindowServerDestroyed(wsid, sid) => {
                SpaceEventHandler::handle_window_server_destroyed(self, wsid, sid);
            }
            Event::WindowServerAppeared(wsid, sid) => {
                SpaceEventHandler::handle_window_server_appeared(self, wsid, sid);
            }
            Event::SpaceCreated(space) => {
                self.handle_space_lifecycle(space, true);
            }
            Event::SpaceDestroyed(space) => {
                self.handle_space_lifecycle(space, false);
            }
            Event::WindowMinimized(wid) => {
                WindowEventHandler::handle_window_minimized(self, wid);
            }
            Event::WindowDeminiaturized(wid) => {
                WindowEventHandler::handle_window_deminiaturized(self, wid);
            }
            Event::WindowFrameChanged(wid, new_frame, last_seen, requested, mouse_state) => {
                is_resize = WindowEventHandler::handle_window_frame_changed(
                    self,
                    wid,
                    new_frame,
                    last_seen,
                    requested,
                    mouse_state,
                );
            }
            Event::WindowTitleChanged(wid, new_title) => {
                WindowEventHandler::handle_window_title_changed(self, wid, new_title);
            }
            Event::ScreenParametersChanged(screens) => {
                SpaceEventHandler::handle_screen_parameters_changed(self, screens);
            }
            Event::SpaceChanged(spaces) => {
                SpaceEventHandler::handle_space_changed(self, spaces);
            }
            Event::MouseDown => {}
            Event::MouseUp => {
                DragEventHandler::handle_mouse_up(self);
            }
            Event::MenuOpened(pid) => SystemEventHandler::handle_menu_opened(self, pid),
            Event::MenuClosed(pid) => SystemEventHandler::handle_menu_closed(self, pid),
            Event::MouseMovedOverWindow(wsid) => {
                WindowEventHandler::handle_mouse_moved_over_window(self, wsid);
            }
            Event::MouseMoved => {
                if self.workspace_switch_manager.workspace_switch_state
                    == WorkspaceSwitchState::Inactive
                {
                    self.save_cursor_for_cursor_workspace();
                }
                return;
            }
            Event::SystemWoke => SystemEventHandler::handle_system_woke(self),
            Event::MissionControlNativeEntered => {
                SpaceEventHandler::handle_mission_control_native_entered(self);
            }
            Event::MissionControlNativeExited => {
                SpaceEventHandler::handle_mission_control_native_exited(self);
            }
            Event::RaiseCompleted { window_id, sequence_id } => {
                SystemEventHandler::handle_raise_completed(self, window_id, sequence_id);
                // Any completed raise can change z-order; verify pinned windows
                // once the dust settles. The reassert counter only resets when a
                // buried-check comes back clean, not on raise completion.
                self.schedule_topmost_verification();
            }
            Event::RaiseTimeout { sequence_id } => {
                SystemEventHandler::handle_raise_timeout(self, sequence_id);
            }
            Event::ConfigUpdated(new_cfg) => {
                CommandEventHandler::handle_config_updated(self, new_cfg);
            }
            Event::Command(cmd) => {
                CommandEventHandler::handle_command(self, cmd);
            }
            Event::ReassertDisplayMove(window_id) => {
                CommandEventHandler::handle_reassert_display_move(self, window_id);
            }
            Event::ReassertTopmost => {
                self.reassert_topmost_windows(None);
            }
            Event::ApplicationMainWindowChanged(pid, _, _) => {
                self.schedule_orphan_reconcile(pid);
            }
            Event::ReconcileOrphans => {
                self.reconcile_orphan_windows();
            }
            _ => (),
        }

        self.finalize_event_processing(
            raised_window,
            is_resize,
            window_was_destroyed,
            should_update_notifications,
        );
        if should_reassert_topmost {
            self.schedule_topmost_reassert();
        }
    }

    /// User-driven trigger (click, app activation, hover): schedules
    /// verification shots and grants exhausted windows one more raise attempt,
    /// so a permanently contested window retries once per user action instead
    /// of looping or giving up forever.
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

    /// Trailing re-checks: clicks and raises land asynchronously, so a single
    /// immediate check races the z-order change it's trying to observe.
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

    fn finalize_event_processing(
        &mut self,
        raised_window: Option<WindowId>,
        is_resize: bool,
        window_was_destroyed: bool,
        should_update_notifications: bool,
    ) {
        if self.display_topology_manager.is_churning_or_awaiting_commit() {
            return;
        }

        if let Some(raised_window) = raised_window {
            if let Some(space) = self.best_space_for_window_id(raised_window) {
                self.send_layout_event(LayoutEvent::WindowFocused(space, raised_window));
            }
        }

        let mut layout_changed = false;
        if !self.is_in_drag() || window_was_destroyed {
            layout_changed = self.update_layout_or_warn(
                is_resize,
                matches!(
                    self.workspace_switch_manager.workspace_switch_state,
                    WorkspaceSwitchState::Active
                ),
            );
            self.maybe_send_menu_update();
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
            if let Some(target) =
                restored_cursor.or_else(|| self.window_center_on_known_screen(wid))
                && let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref()
            {
                event_tap_tx.send(crate::actor::event_tap::Request::Warp(target));
            }
        } else {
            self.workspace_switch_manager.pending_workspace_cursor_warp = None;
            self.workspace_switch_manager.pending_workspace_mouse_warp = None;
        }

        // Deferred cursor warp for a completed move-window-to-display. Fire once the moved window
        // has actually re-tiled away from the centered frame we seeded (so the cursor lands on the
        // window, not a neighbour that would steal focus-follows-mouse), or once a short deadline
        // passes as a safety net.
        if let Some((wid, dest_rect, deadline)) = self.pending_display_move_warp {
            let settled = match self.window_manager.windows.get(&wid) {
                Some(window) => dest_rect.contains(window.frame_monotonic.mid()),
                None => true,
            };
            if settled || std::time::Instant::now() >= deadline {
                // Warp silently: focus is already on the moved window (we raised it), and a synthetic
                // mouse-moved event here would let focus-follows-mouse re-pick a neighbour that is
                // still overlapping mid-relayout, stealing focus back.
                if let Some(target) = self.window_center_on_known_screen(wid)
                    && let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref()
                {
                    event_tap_tx.send(crate::actor::event_tap::Request::WarpSilent(target));
                }
                self.pending_display_move_warp = None;
            }
        }

        if should_update_notifications {
            let mut ids: Vec<u32> =
                self.window_manager.window_ids.keys().map(|wsid| wsid.as_u32()).collect();
            ids.sort_unstable();

            if ids != self.notification_manager.last_sls_notification_ids {
                crate::sys::window_notify::update_window_notifications(&ids);

                self.notification_manager.last_sls_notification_ids = ids;
            }
        }
        if self.workspace_switch_manager.workspace_switch_state == WorkspaceSwitchState::Inactive {
            self.save_cursor_for_cursor_workspace();
        }
        self.update_event_tap_layout_mode();
    }

    fn create_window_data(&self, window_id: WindowId) -> Option<WindowData> {
        let window_state = self.window_manager.windows.get(&window_id)?;
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
        self.window_manager.visible_windows.clear();
        self.update_partial_window_server_info(ws_info);
    }

    fn update_partial_window_server_info(&mut self, ws_info: Vec<WindowServerInfo>) {
        // Mark visible windows and remove any corresponding observed WSID markers
        // for ids we now have server info for.
        self.window_manager.visible_windows.extend(ws_info.iter().map(|info| info.id));
        for info in ws_info.iter() {
            // If we've been observing this server id from SLS callbacks, clear it.
            self.window_manager.observed_window_server_ids.remove(&info.id);
            self.window_server_info_manager.window_server_info.insert(info.id, *info);

            if let Some(wid) = self.window_manager.window_ids.get(&info.id).copied() {
                let (server_id, is_minimized, is_ax_standard, is_ax_root) =
                    if let Some(window) = self.window_manager.windows.get_mut(&wid) {
                        if info.layer == 0 {
                            window.frame_monotonic = info.frame;
                        }
                        (
                            window.info.sys_id,
                            window.info.is_minimized,
                            window.info.is_standard,
                            window.info.is_root,
                        )
                    } else {
                        continue;
                    };
                let manageable = utils::compute_window_manageability(
                    server_id,
                    is_minimized,
                    is_ax_standard,
                    is_ax_root,
                    &self.window_server_info_manager.window_server_info,
                );
                if let Some(window) = self.window_manager.windows.get_mut(&wid) {
                    window.is_manageable = manageable;
                }
            }
        }
    }

    fn check_for_new_windows(&mut self) {
        // TODO: Do this correctly/more optimally using CGWindowListCopyWindowInfo
        // (see notes for on_windows_discovered below).
        self.request_visible_windows_for_apps(false);
    }

    fn request_visible_windows_for_apps(&mut self, track_mission_control_refresh: bool) {
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

    fn handle_fullscreen_space_transition(&mut self, spaces: &mut Vec<Option<SpaceId>>) -> bool {
        self.preserve_user_spaces_during_fullscreen_transition(spaces);

        let mut saw_fullscreen = false;
        let mut all_fullscreen = !spaces.is_empty();
        let mut refresh_spaces = Vec::new();

        for slot in spaces.iter_mut() {
            match slot {
                Some(space) if self.is_fullscreen_space(*space) => {
                    saw_fullscreen = true;
                    *slot = None;
                }
                Some(space) => {
                    all_fullscreen = false;
                    refresh_spaces.push(*space);
                }
                None => {
                    all_fullscreen = false;
                }
            }
        }

        if saw_fullscreen && all_fullscreen {
            return true;
        }

        for space in refresh_spaces {
            if let Some(track) = self.space_manager.fullscreen_by_space.remove(&space.get()) {
                wait_for_native_fullscreen_transition();
                thread::sleep(Duration::from_millis(50));

                for window in track.windows {
                    if let Some(app) = self.app_manager.apps.get(&window.pid) {
                        if let Err(e) = app.handle.send(Request::GetVisibleWindows) {
                            warn!("Failed to send GetVisibleWindows to app {}: {}", window.pid, e);
                        }
                    }

                    if let (Some(window_id), Some(target_space)) =
                        (window.window_id, window.last_known_user_space)
                    {
                        if let Some(source_space) = self
                            .best_space_for_window_id(window_id)
                            .or(window.last_known_user_space)
                        {
                            if source_space != target_space {
                                let target_screen_size = self
                                    .space_manager
                                    .screen_by_space(target_space)
                                    .map(|screen| screen.frame.size)
                                    .unwrap_or_else(|| CGSize::new(0.0, 0.0));

                                let response =
                                    self.layout_manager.layout_engine.move_window_to_space(
                                        source_space,
                                        target_space,
                                        target_screen_size,
                                        window_id,
                                    );
                                self.handle_layout_response(response, None, false);
                            }
                        }
                    }
                }

                self.refocus_manager.refocus_state = RefocusState::Pending(space);
                self.update_layout_or_warn(false, false);
                self.update_focus_follows_mouse_state();
            }
        }

        false
    }

    fn is_fullscreen_space(&self, space: SpaceId) -> bool {
        space_is_fullscreen(space.get())
            || self.space_manager.fullscreen_by_space.contains_key(&space.get())
    }

    fn preserve_user_spaces_during_fullscreen_transition(&self, spaces: &mut [Option<SpaceId>]) {
        let entering_fullscreen =
            self.space_manager.screens.iter().zip(spaces.iter()).any(|(screen, slot)| {
                let Some(new_space) = *slot else {
                    return false;
                };
                if !self.is_fullscreen_space(new_space) {
                    return false;
                }
                screen
                    .space
                    .is_some_and(|previous_space| !self.is_fullscreen_space(previous_space))
            });
        if !entering_fullscreen {
            return;
        }

        for (screen, slot) in self.space_manager.screens.iter().zip(spaces.iter_mut()) {
            let Some(new_space) = *slot else {
                continue;
            };
            if self.is_fullscreen_space(new_space) {
                continue;
            }
            let Some(previous_space) = screen.space else {
                continue;
            };
            if previous_space == new_space || self.is_fullscreen_space(previous_space) {
                continue;
            }

            debug!(
                display_uuid = %screen.display_uuid,
                ?previous_space,
                ?new_space,
                "Preserving previous user space during fullscreen transition"
            );
            *slot = Some(previous_space);
        }
    }

    fn set_screen_spaces(&mut self, spaces: &[Option<SpaceId>]) {
        for (space, screen) in spaces.iter().copied().zip(&mut self.space_manager.screens) {
            screen.space = space;
        }
    }

    fn reconcile_spaces_with_display_history(
        &mut self,
        spaces: &[Option<SpaceId>],
        allow_remap: bool,
    ) {
        let mut seen_displays: HashSet<String> = HashSet::default();

        for (screen, space_opt) in self.space_manager.screens.iter().zip(spaces.iter()) {
            let Some(space) = space_opt else {
                continue;
            };
            let is_fullscreen_space = window_server::space_is_fullscreen(space.get())
                || self.space_manager.fullscreen_by_space.contains_key(&space.get());
            if is_fullscreen_space {
                continue;
            }
            let Some(display_uuid) = screen.display_uuid_opt() else {
                continue;
            };
            if !seen_displays.insert(display_uuid.to_string()) {
                continue;
            }

            let seen_before = self.layout_manager.layout_engine.display_seen_before(display_uuid);
            let last_space = if allow_remap && seen_before {
                self.layout_manager.layout_engine.last_space_for_display_uuid(display_uuid)
            } else {
                None
            };

            // When a display reconnects, remap the most recent space observed for
            // that display to the newly reported space so layout state follows the
            // monitor. During routine space switches (allow_remap=false), we simply
            // record the mapping without remapping.
            if allow_remap {
                if let Some(previous_space) = last_space {
                    if previous_space != *space {
                        self.layout_manager.layout_engine.remap_space(previous_space, *space);
                    }
                }
            }
            self.layout_manager
                .layout_engine
                .update_space_display(*space, Some(display_uuid.to_string()));
        }
    }

    fn finalize_space_change(
        &mut self,
        spaces: &[Option<SpaceId>],
        ws_info: Vec<WindowServerInfo>,
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
        let ws_info = self.filter_ws_info_to_active_spaces(ws_info);
        self.update_complete_window_server_info(ws_info);
        self.check_for_new_windows();

        if let Some(space) =
            spaces.iter().copied().flatten().find(|space| self.is_space_active(*space))
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

        let (is_manageable, wsid) = match self.window_manager.windows.get(&window_id) {
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
            self.app_manager.mark_wsids_recent(std::iter::once(window_server_id));
        }

        self.process_windows_for_app_rules(window_id.pid, vec![window_id], app_info);
    }

    fn try_apply_pending_space_change(&mut self) {
        if let Some(mut pending) = self.pending_space_change_manager.pending_space_change.take() {
            if pending.spaces.len() == self.space_manager.screens.len() {
                if self.handle_fullscreen_space_transition(&mut pending.spaces) {
                    return;
                }
                // A pending space change is queued specifically when Mission Control is active.
                // When we apply it later, we must also recompute active spaces (normally done in
                // the regular SpaceChanged handler) to avoid staying "space-less" until the next
                // user-initiated space switch.
                self.recompute_and_set_active_spaces(&pending.spaces);
                self.set_screen_spaces(&pending.spaces);
                let ws_info = self.authoritative_window_snapshot_for_active_spaces();
                self.finalize_space_change(&pending.spaces, ws_info);
            } else {
                self.pending_space_change_manager.pending_space_change = Some(pending);
            }
        }
    }

    fn repair_spaces_after_mission_control(&mut self) {
        // First, apply any SpaceChanged that arrived while MC was active.
        self.try_apply_pending_space_change();

        // If we still have missing space ids (or no active spaces), proactively rebuild
        // per-display current spaces via CGS. This covers the common case where macOS emits
        // a transient "all None" spaces vector during Mission Control and then doesn't emit
        // a corresponding steady-state update when exiting back to the same space.
        let needs_repair = self.active_spaces.is_empty()
            || self.space_manager.screens.iter().all(|s| s.space.is_none());
        if !needs_repair || self.space_manager.screens.is_empty() {
            return;
        }

        let spaces: Vec<Option<SpaceId>> = self
            .space_manager
            .screens
            .iter()
            .map(|s| {
                crate::sys::screen::current_space_for_display_uuid(&s.display_uuid).or(s.space)
            })
            .collect();

        if spaces.iter().any(|s| s.is_some()) && spaces.len() == self.space_manager.screens.len() {
            self.set_screen_spaces(&spaces);
            self.recompute_and_set_active_spaces(&spaces);
        }
    }

    fn on_windows_discovered_with_app_info(
        &mut self,
        pid: pid_t,
        new: Vec<(WindowId, WindowInfo)>,
        known_visible: Vec<WindowId>,
        app_info: Option<AppInfo>,
    ) {
        WindowDiscoveryHandler::handle_discovery(self, pid, new, known_visible, app_info);
    }

    fn best_space_for_window(
        &self,
        frame: &CGRect,
        window_server_id: Option<WindowServerId>,
    ) -> Option<SpaceId> {
        if let Some(space) = window_server_id.and_then(crate::sys::window_server::window_space) {
            // Return None for windows whose resolved space is not a user space (e.g. native
            // fullscreen app spaces, SLSSpaceGetType != 0). Without this guard, fullscreen
            // windows fall through to best_space_for_frame which matches by geometry — and
            // fullscreen windows cover the whole screen, so they match the current user space
            // and bleed into its tile layout after Mission Control (fixes #357).
            if !crate::sys::window_server::space_is_user(space.get()) {
                return None;
            }
            if self.space_manager.screen_by_space(space).is_some() {
                return Some(space);
            }
        }

        if let Some(space) = self.best_space_for_frame(frame) {
            return Some(space);
        }

        None
    }

    fn best_space_for_frame(&self, frame: &CGRect) -> Option<SpaceId> {
        let center = frame.mid();
        self.screen_for_point(center).and_then(|screen| screen.space).or_else(|| {
            self.space_manager
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

    fn ensure_active_drag(&mut self, wid: WindowId, frame: &CGRect) {
        let needs_new_session =
            self.get_active_drag_session().map_or(true, |session| session.window != wid);
        if needs_new_session {
            let server_id =
                self.window_manager.windows.get(&wid).and_then(|window| window.info.sys_id);
            let origin_space = self.best_space_for_window(frame, server_id);
            let session = DragSession {
                window: wid,
                last_frame: *frame,
                origin_space,
                settled_space: origin_space,
                layout_dirty: false,
            };
            self.drag_manager.drag_state = DragState::Active { session };
        }
        self.drag_manager.skip_layout_for_window = Some(wid);
    }

    fn update_active_drag(&mut self, wid: WindowId, new_frame: &CGRect) {
        let resolved_space = match self.get_active_drag_session() {
            Some(session) if session.window == wid => self.resolve_drag_space(session, new_frame),
            _ => return,
        };

        if let Some(session) = self.get_active_drag_session_mut() {
            let frame_changed = session.last_frame != *new_frame;
            session.last_frame = *new_frame;
            if frame_changed {
                session.layout_dirty = true;
            }
            if session.settled_space != resolved_space {
                session.settled_space = resolved_space;
                session.layout_dirty = true;
                self.drag_manager.skip_layout_for_window = Some(session.window);
            }
        }
    }

    fn drag_space_candidate(&self, frame: &CGRect) -> Option<SpaceId> {
        let center = frame.mid();
        self.screen_for_point(center).and_then(|screen| screen.space)
    }

    fn resolve_drag_space(&self, session: &DragSession, frame: &CGRect) -> Option<SpaceId> {
        let server_id = self
            .window_manager
            .windows
            .get(&session.window)
            .and_then(|window| window.info.sys_id);
        if frame.area() <= 0.0 {
            return session.settled_space.or_else(|| self.best_space_for_window(frame, server_id));
        }

        self.drag_space_candidate(frame)
            .or_else(|| self.best_space_for_window(frame, server_id))
            .or(session.settled_space)
    }

    fn best_space_for_window_state(&self, window: &WindowState) -> Option<SpaceId> {
        self.best_space_for_window(&window.frame_monotonic, window.info.sys_id)
    }

    fn best_space_for_window_id(&self, wid: WindowId) -> Option<SpaceId> {
        self.window_manager
            .windows
            .get(&wid)
            .and_then(|window| self.best_space_for_window_state(window))
    }

    fn finalize_active_drag(&mut self) -> bool {
        let Some(session) = self.take_active_drag_session() else {
            return false;
        };
        let wid = session.window;

        // During a drag the window server can continue reporting the origin
        // space even after the user has moved the window onto another display.
        // Trust the drag session’s resolved space (or the final frame’s screen)
        // before falling back to the server-reported space so that cross-display
        // drags do not snap the window back to the original monitor.
        let final_space = session
            .settled_space
            .or_else(|| self.best_space_for_frame(&session.last_frame))
            .or_else(|| self.best_space_for_window_id(wid));

        let needs_layout = if session.origin_space != final_space {
            if session.origin_space.is_some() {
                self.send_layout_event(LayoutEvent::WindowRemoved(wid));
            }
            if let Some(space) = final_space {
                if let Some(active_ws) = self.layout_manager.layout_engine.active_workspace(space) {
                    let assigned = self
                        .layout_manager
                        .layout_engine
                        .virtual_workspace_manager_mut()
                        .assign_window_to_workspace(space, wid, active_ws);
                    if !assigned {
                        warn!("Failed to assign window {:?} to workspace {:?}", wid, active_ws);
                    }
                }
                self.send_layout_event(LayoutEvent::WindowAdded(space, wid));
            }
            self.drag_manager.skip_layout_for_window = Some(wid);
            true
        } else if session.layout_dirty {
            self.drag_manager.skip_layout_for_window = Some(wid);
            true
        } else {
            false
        };

        if let Some(space) = final_space {
            if self.layout_manager.layout_engine.is_window_floating(wid) {
                if let Some(ws_id) = self
                    .layout_manager
                    .layout_engine
                    .virtual_workspace_manager()
                    .workspace_for_window(space, wid)
                    .or_else(|| self.layout_manager.layout_engine.active_workspace(space))
                {
                    // Drop any floating position stored under the source workspace before
                    // recording the new one. Otherwise the origin display's layout pass keeps
                    // re-positioning the window (get_workspace_floating_positions only checks
                    // is_floating, not current assignment) while the destination positions it
                    // too — the window ping-pongs between displays after a cross-display drag.
                    self.layout_manager
                        .layout_engine
                        .virtual_workspace_manager_mut()
                        .remove_floating_position(wid);
                    self.layout_manager
                        .layout_engine
                        .virtual_workspace_manager_mut()
                        .store_floating_position(space, ws_id, wid, session.last_frame);
                }
            }
        }

        needs_layout
    }

    fn window_center_on_known_screen(&self, wid: WindowId) -> Option<CGPoint> {
        let window_center = self.window_manager.windows.get(&wid)?.frame_monotonic.mid();
        self.screen_for_point(window_center).map(|_| window_center)
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
        let frame = self.window_manager.windows.get(&wid)?.frame_monotonic;
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

    fn has_visible_window_server_ids_for_pid(&self, pid: pid_t) -> bool {
        self.window_manager
            .visible_windows
            .iter()
            .any(|wsid| self.window_manager.window_ids.get(wsid).is_some_and(|wid| wid.pid == pid))
    }

    fn warp_mouse_to_space_center(&self, space: SpaceId) -> bool {
        let Some(screen) = self.space_manager.screen_by_space(space) else {
            return false;
        };
        let Some(event_tap_tx) = self.communication_manager.event_tap_tx.as_ref() else {
            return false;
        };
        event_tap_tx.send(crate::actor::event_tap::Request::Warp(screen.frame.mid()));
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

    pub(crate) fn toggle_topmost_window(&mut self, wid: WindowId) {
        if self.topmost_windows.remove(&wid).is_some() {
            // Under floating_windows_topmost the float sweep would re-pin
            // this window on the next pass; remember the opt-out.
            if self.config.settings.floating_windows_topmost {
                self.topmost_optout.insert(wid);
            }
            info!(?wid, "Unpinned topmost window");
            return;
        }

        self.topmost_optout.remove(&wid);
        self.topmost_windows.insert(wid, TopmostWindowState::default());
        info!(?wid, "Pinned topmost window");
        self.raise_topmost_windows(vec![wid]);
    }

    /// With `floating_windows_topmost` enabled, every floating window in an
    /// active workspace is implicitly pinned (unless opted out); implicit pins
    /// are dropped again when the window stops floating.
    fn sync_floating_topmost(&mut self) {
        if !self.config.settings.floating_windows_topmost {
            return;
        }
        self.topmost_optout.retain(|wid| self.window_manager.windows.contains_key(wid));

        let spaces: Vec<SpaceId> = self
            .space_manager
            .screens
            .iter()
            .filter_map(|screen| screen.space)
            .filter(|space| self.is_space_active(*space))
            .collect();
        let mut floating: HashSet<WindowId> = HashSet::default();
        for space in spaces {
            for wid in self.layout_manager.layout_engine.windows_in_active_workspace(space) {
                if self.layout_manager.layout_engine.is_window_floating(wid) {
                    floating.insert(wid);
                }
            }
        }

        self.topmost_windows.retain(|wid, state| !state.implicit || floating.contains(wid));
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
        if self.display_topology_manager.is_churning_or_awaiting_commit()
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
            // The focused window is allowed to cover the pin while it holds
            // focus: no public macOS API can order another app's window above
            // the active app's key window without stealing focus (that needs
            // SLSSetWindowLevel, i.e. a SIP-disabled scripting addition).
            // Fighting it reactively just produces flicker. When focus moves
            // on, the next trigger re-raises the pin above it.
            let burier_wid = self.window_manager.window_ids.get(&burier).copied();
            if burier_wid.is_some() && burier_wid == self.main_window() {
                if let Some(state) = self.topmost_windows.get_mut(&wid) {
                    state.failed_reasserts = 0;
                }
                continue;
            }
            if state.failed_reasserts >= TOPMOST_MAX_FAILED_REASSERTS {
                // Never unpin. Stop raising until the next user-driven trigger
                // (schedule_topmost_reassert) grants another attempt.
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

    /// Returns the window-server id of the first non-topmost window found
    /// covering one of the pinned window's sample points, or `None` if the
    /// pinned window is on top.
    fn topmost_burier(&self, wid: WindowId) -> Option<WindowServerId> {
        let window = self.window_manager.windows.get(&wid)?;
        let wsid = window.info.sys_id?;
        if !self.window_manager.visible_windows.contains(&wsid) {
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
                    self.window_manager.windows.get(topmost).and_then(|state| state.info.sys_id)
                        == Some(hit)
                }))
            .then_some(hit)
        })
    }

    fn expose_all_spaces(&mut self) {
        let spaces: Vec<SpaceId> = self
            .space_manager
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
        self.window_manager
            .windows
            .get(&id)
            .is_some_and(|window| window.matches_filter(WindowFilter::EffectivelyManageable))
    }

    pub(crate) fn visible_spaces_for_layout(
        &self,
        include_inactive: bool,
    ) -> (Vec<SpaceId>, HashMap<SpaceId, CGPoint>) {
        let visible_spaces_input: Vec<(SpaceId, CGPoint)> = self
            .space_manager
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
        let response = self.layout_manager.layout_engine.handle_event(event);
        self.prepare_refocus_after_layout_event(&event_clone);
        let force_focus_warp = matches!(
            event_clone,
            LayoutEvent::WindowRemoved(_) | LayoutEvent::WindowRemovedPreserveFloating(_)
        );
        if force_focus_warp {
            let _ = self.update_layout_or_warn(false, false);
        }
        self.handle_layout_response(response, None, force_focus_warp);
        for space in self.space_manager.iter_known_spaces() {
            self.layout_manager.layout_engine.debug_tree_desc(space, "after event", false);
        }
    }

    // Returns true if the window should be raised on mouse over considering
    // active workspace membership and potential occlusion of floating windows above it.
    fn should_raise_on_mouse_over(&self, wid: WindowId) -> bool {
        let Some(window) = self.window_manager.windows.get(&wid) else {
            return false;
        };

        if self.main_window() == Some(wid) {
            return false;
        }

        if self.topmost_windows.contains_key(&wid) {
            return false;
        }

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

        if !self.layout_manager.layout_engine.is_window_in_active_workspace(space, wid) {
            trace!("Ignoring mouse over window {:?} - not in active workspace", wid);
            return false;
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
            let Some(state) = self.window_manager.windows.get(&wid) else {
                continue;
            };
            if !state.matches_filter(WindowFilter::Manageable) {
                continue;
            }
            let Some(space) = self.best_space_for_window_state(state) else {
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
                            .workspace_for_window(space, *wid)
                            .is_some(),
                        engine.is_window_floating(*wid),
                        self.window_manager
                            .windows
                            .get(wid)
                            .map(|window| window.ignore_app_rule)
                            .unwrap_or(false),
                    )
                };
                let assign_result = {
                    let window = self.window_manager.windows.get(wid);
                    self.layout_manager
                        .layout_engine
                        .virtual_workspace_manager_mut()
                        .assign_window_with_app_info(
                            *wid,
                            space,
                            app_info.bundle_id.as_deref(),
                            app_info.localized_name.as_deref(),
                            window.map(|w| w.info.title.as_str()),
                            window.and_then(|w| w.info.ax_role.as_deref()),
                            window.and_then(|w| w.info.ax_subrole.as_deref()),
                        )
                };

                match assign_result {
                    Ok(AppRuleResult::Managed(assignment)) => {
                        if let Some(window) = self.window_manager.windows.get_mut(wid) {
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
                        if let Some(window) = self.window_manager.windows.get_mut(wid) {
                            window.ignore_app_rule = true;
                        }

                        let needs_removal = {
                            let engine = &self.layout_manager.layout_engine;
                            engine
                                .virtual_workspace_manager()
                                .workspace_for_window(space, *wid)
                                .is_some()
                                || engine.is_window_floating(*wid)
                        };
                        if needs_removal {
                            self.send_layout_event(LayoutEvent::WindowRemoved(*wid));
                        }
                    }
                    Err(e) => {
                        warn!("Failed to assign window {:?} to workspace: {:?}", wid, e);
                        if let Some(window) = self.window_manager.windows.get_mut(wid) {
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
                    let window = self.window_manager.windows.get(&wid);
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

        if let Some(active_space) = get_active_space_number()
            && space_is_fullscreen(active_space.get())
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
            self.window_manager.windows.iter().any(|(wid, window_state)| {
                if wid.pid != pid {
                    return false;
                }
                let Some(space) = self.best_space_for_window_state(window_state) else {
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
                    .workspace_for_window(space, *wid)
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
                self.window_manager
                    .windows
                    .keys()
                    .find(|wid| wid.pid == pid && self.window_is_standard(**wid))
                    .copied()
            });

        let Some(app_window_id) = app_window else {
            return;
        };

        let Some(window_state) = self.window_manager.windows.get(&app_window_id) else {
            return;
        };
        let Some(window_space) = self.best_space_for_window_state(window_state) else {
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
        let workspace_manager = self.layout_manager.layout_engine.virtual_workspace_manager();
        let Some(window_workspace) =
            workspace_manager.workspace_for_window(window_space, app_window_id)
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

                let response = self.layout_manager.layout_engine.handle_virtual_workspace_command(
                    window_space,
                    &layout::LayoutCommand::SwitchToWorkspace(workspace_index),
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

        if let Some(dir) = boundary_hit
            && self.config.settings.layout.scrolling.gestures.propagate_to_workspace_swipe
        {
            let skip_empty = self.config.settings.gestures.skip_empty;
            let cmd = if self.config.settings.gestures.invert_horizontal_swipe {
                match dir {
                    Direction::Left => Some(layout::LayoutCommand::PrevWorkspace(Some(skip_empty))),
                    Direction::Right => {
                        Some(layout::LayoutCommand::NextWorkspace(Some(skip_empty)))
                    }
                    _ => None,
                }
            } else {
                match dir {
                    Direction::Left => Some(layout::LayoutCommand::NextWorkspace(Some(skip_empty))),
                    Direction::Right => {
                        Some(layout::LayoutCommand::PrevWorkspace(Some(skip_empty)))
                    }
                    _ => None,
                }
            };
            if let Some(cmd) = cmd {
                let space = workspace_switch_space.or_else(|| self.workspace_command_space());
                if let Some(space) = space {
                    let resp = self
                        .layout_manager
                        .layout_engine
                        .handle_virtual_workspace_command(space, &cmd);

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
                                .windows_in_active_workspace(space)
                                .is_empty()
                        })
                        .unwrap_or(false);
                    let warp_space = if skip_center_warp {
                        None
                    } else {
                        workspace_switch_space.or_else(|| self.workspace_command_space())
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

        let require_visible_focus = matches!(
            self.workspace_switch_manager.workspace_switch_state,
            WorkspaceSwitchState::Inactive
        );

        if let Some(wid) = focus_window
            && let Some(state) = self.window_manager.windows.get(&wid)
            && let Some(wsid) = state.info.sys_id
        {
            if require_visible_focus && !self.window_manager.visible_windows.contains(&wsid) {
                focus_window = None;
            } else if !self
                .best_space_for_window_state(state)
                .is_some_and(|space| self.is_space_active(space))
            {
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
            kind: crate::actor::app::RaiseKind::Focus,
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
        self.window_manager
            .windows
            .iter()
            .filter_map(|(&other_wid, other_state)| {
                if other_wid == wid {
                    return None;
                }
                let other_space = self.best_space_for_window_state(other_state)?;
                if other_space != space
                    || !self
                        .layout_manager
                        .layout_engine
                        .is_window_in_active_workspace(space, other_wid)
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
            let Some(window) = self.window_manager.windows.get(&wid) else {
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

        if !self.layout_manager.layout_engine.is_window_in_active_workspace(space, wid) {
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

    fn window_id_under_cursor(&self) -> Option<WindowId> {
        self.tracked_window_under_cursor().map(|(_, wid)| wid)
    }

    fn window_server_id_under_cursor(&self) -> Option<WindowServerId> {
        window_server::window_under_cursor()
    }

    fn tracked_window_under_cursor(&self) -> Option<(WindowServerId, WindowId)> {
        let wsid = self.window_server_id_under_cursor()?;
        let wid = *self.window_manager.window_ids.get(&wsid)?;
        Some((wsid, wid))
    }

    fn activation_from_unmanageable_window(&self, pid: pid_t) -> Option<WindowServerId> {
        let (wsid, wid) = self.tracked_window_under_cursor()?;
        let window = self.window_manager.windows.get(&wid)?;
        (wid.pid == pid && !window.matches_filter(WindowFilter::EffectivelyManageable))
            .then_some(wsid)
    }

    fn focus_untracked_window_under_cursor(&mut self) -> bool {
        let Some(wsid) = self.window_server_id_under_cursor() else {
            return false;
        };
        if self.window_manager.window_ids.contains_key(&wsid) {
            return false;
        }

        let window_info = self
            .window_server_info_manager
            .window_server_info
            .get(&wsid)
            .copied()
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
        let window = self.window_manager.windows.get(&wid)?;

        if self.best_space_for_window_id(wid)? != space {
            return None;
        }
        if window
            .info
            .sys_id
            .is_some_and(|wsid| !self.window_manager.visible_windows.contains(&wsid))
        {
            return None;
        }
        Some(wid)
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
            .workspace_for_window(space, window_id)
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
    fn raise_window(&mut self, wid: WindowId, quiet: Quiet, warp: Option<CGPoint>) {
        let mut app_handles = HashMap::default();
        if let Some(app) = self.app_manager.apps.get(&wid.pid) {
            app_handles.insert(wid.pid, app.handle.clone());
        }
        _ = self
            .communication_manager
            .raise_manager_tx
            .send(raise_manager::Event::RaiseRequest(RaiseRequest {
                raise_windows: vec![vec![wid]],
                focus_window: Some((wid, warp)),
                app_handles,
                focus_quiet: quiet,
                kind: crate::actor::app::RaiseKind::Focus,
            }));
    }

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

    fn update_focus_follows_mouse_state(&self) {
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
            Vec::with_capacity(self.space_manager.screens.len());
        let mut changed = false;

        for screen in &self.space_manager.screens {
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
        if !crate::sys::window_server::active_space_is_user() {
            return;
        }
        let ws_info = window_server::get_visible_windows_with_layer(None);
        self.update_partial_window_server_info(ws_info);
        self.mission_control_manager.pending_mission_control_refresh.clear();
        self.force_refresh_all_windows();
        self.check_for_new_windows();
        self.update_layout_or_warn(false, false);
        self.maybe_send_menu_update();
    }

    fn force_refresh_all_windows(&mut self) {
        self.request_visible_windows_for_apps(true);
    }

    fn request_close_window(&mut self, wid: WindowId) {
        if let Some(app) = self.app_manager.apps.get(&wid.pid) {
            if let Err(err) = app.handle.send(Request::CloseWindow(wid)) {
                warn!(?wid, "Failed to send close window request: {}", err);
            }
        }
    }

    fn main_window(&self) -> Option<WindowId> {
        self.main_window_tracker.main_window()
    }

    fn main_window_space(&self) -> Option<SpaceId> {
        // TODO: Optimize this with a cache or something.
        let wid = self.main_window()?;
        self.best_space_for_window_id(wid)
    }

    fn workspace_command_space(&self) -> Option<SpaceId> {
        let candidate = self
            .space_for_cursor_screen()
            .or_else(|| self.main_window_space())
            .or_else(|| get_active_space_number())
            .or_else(|| self.space_manager.first_known_space());

        candidate.filter(|space| self.is_space_active(*space))
    }

    fn space_for_cursor_screen(&self) -> Option<SpaceId> {
        current_cursor_location().ok().and_then(|point| self.space_for_point(point))
    }

    fn space_for_point(&self, point: CGPoint) -> Option<SpaceId> {
        self.screen_for_point(point)
            .or_else(|| self.closest_screen_to_point(point))
            .and_then(|screen| screen.space)
    }

    fn screen_for_point(&self, point: CGPoint) -> Option<&ScreenInfo> {
        self.space_manager.screens.iter().find(|screen| screen.frame.contains(point))
    }

    fn closest_screen_to_point(&self, point: CGPoint) -> Option<&ScreenInfo> {
        self.space_manager.screens.iter().min_by(|a, b| {
            let da = Self::rectangle_distance_sq(a.frame, point);
            let db = Self::rectangle_distance_sq(b.frame, point);
            da.total_cmp(&db)
        })
    }

    fn rectangle_distance_sq(frame: CGRect, point: CGPoint) -> f64 {
        let min_x = frame.origin.x;
        let max_x = frame.origin.x + frame.size.width;
        let min_y = frame.origin.y;
        let max_y = frame.origin.y + frame.size.height;

        let dx = if point.x < min_x {
            min_x - point.x
        } else if point.x > max_x {
            point.x - max_x
        } else {
            0.0
        };

        let dy = if point.y < min_y {
            min_y - point.y
        } else if point.y > max_y {
            point.y - max_y
        } else {
            0.0
        };

        dx * dx + dy * dy
    }

    fn current_screen_center(&self) -> Option<CGPoint> {
        if let Ok(point) = current_cursor_location() {
            if let Some(screen) = self.screen_for_point(point) {
                return Some(screen.frame.mid());
            }
        }

        if let Some(space) = self.main_window_space() {
            if let Some(screen) = self.space_manager.screen_by_space(space) {
                return Some(screen.frame.mid());
            }
        }

        if let Some(space) = get_active_space_number() {
            if let Some(screen) = self.space_manager.screen_by_space(space) {
                return Some(screen.frame.mid());
            }
        }

        self.space_manager.screens.first().map(|screen| screen.frame.mid())
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

        for screen in &self.space_manager.screens {
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
                self.space_manager.screens.iter().find(|screen| screen.display_uuid == *uuid)
            }
        }
    }

    fn screens_in_physical_order(&self) -> Vec<&ScreenInfo> {
        let mut screens: Vec<&ScreenInfo> = self.space_manager.screens.iter().collect();
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
            .windows_in_active_workspace(space)
            .into_iter()
            .filter(|&wid| self.layout_manager.layout_engine.is_window_floating(wid))
            .filter_map(|wid| {
                self.window_manager
                    .windows
                    .get(&wid)
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
