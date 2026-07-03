//! Input processing via a CGEventTap on a dedicated thread.
//!
//! The `EventTap` (aka input processor) owns a `Default`-mode CGEventTap and
//! runs its own CFRunLoop on a dedicated thread (`input` thread). This isolates
//! keyboard/mouse input processing from main-thread stalls (layout computation,
//! animation, WindowServer IPC).
//!
//! Shared state between the input thread and the main thread uses lock-free
//! `Arc<ArcSwap<T>>` primitives:
//! - `SharedHotkeyTable`: hotkey bindings, written by the input thread on
//!   config/layout changes, read by the callback.
//! - `SharedHitRects`: stack-line indicator frames, written by the main-thread
//!   `StackLine` actor, read by the callback.
//!
//! Requests from the main thread arrive via the actor channel (`Receiver`).
//! The main thread's `GestureTap` is a separate `ListenOnly` tap for gestures.

use std::cell::{Cell, RefCell};
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use objc2_core_foundation::{CGPoint, CGRect};
use objc2_core_graphics::{
    CGEvent, CGEventField, CGEventFlags, CGEventMask, CGEventTapOptions as CGTapOpt,
    CGEventTapProxy, CGEventType,
};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, trace, warn};

use super::reactor::{self, Event};
use super::stack_line;
use crate::actor;
use crate::actor::wm_controller::{self, WmCommand, WmEvent};
use crate::common::collections::{HashMap, HashSet};
use crate::common::config::{Config, LayoutMode};
use crate::sys::event::{self, Hotkey, KeyCode, MouseState, set_mouse_state};
use crate::sys::hotkey::{
    Modifiers, is_modifier_key, key_code_from_event, modifier_flag_for_key,
    modifiers_from_flags_with_keys,
};
use crate::sys::power;
use crate::sys::screen::{CoordinateConverter, SpaceId};
use crate::sys::window_server::{self, WindowServerId};
use crate::ui::stack_line::point_hits_indicator_frame;

const MOUSE_MOVE_MIN_INTERVAL_NS_NORMAL: u64 = 8_000_000; // 8ms ~= 125 Hz
const MOUSE_MOVE_MIN_DISTANCE_PX_SQ_NORMAL: f64 = 4.0; // 2px^2
const MOUSE_MOVE_MIN_INTERVAL_NS_LOW_POWER: u64 = 16_000_000; // 16ms ~= 62 Hz
const MOUSE_MOVE_MIN_DISTANCE_PX_SQ_LOW_POWER: f64 = 9.0; // 3px^2

#[derive(Debug)]
pub enum Request {
    Warp(CGPoint),
    /// Move the cursor without a synthetic mouse-moved event, so focus-follows-mouse does not
    /// re-decide focus (used after move-window-to-display, where focus is already set).
    WarpSilent(CGPoint),
    EnforceHidden,
    ScreenParametersChanged(Vec<(CGRect, Option<SpaceId>)>, CoordinateConverter),
    SpaceChanged(Vec<Option<SpaceId>>),
    SetEventProcessing(bool),
    SetFocusFollowsMouseEnabled(bool),
    SetHotkeys(Vec<(String, WmCommand)>),
    KeyboardLayoutChanged,
    ConfigUpdated(Config),
    LayoutModesChanged(Vec<(SpaceId, crate::common::config::LayoutMode)>),
    SetLowPowerMode(bool),
}

/// What a focus-follows-mouse hover sample should do (see State::note_ffm_hover).
#[derive(Debug, PartialEq, Eq)]
enum FfmHover {
    RaiseNow(WindowServerId),
    Armed(u64),
    NoChange,
}

pub struct EventTap {
    events_tx: reactor::Sender,
    requests_rx: Option<Receiver>,
    state: RefCell<State>,
    event_mask: Cell<CGEventMask>,
    tap: RefCell<Option<crate::sys::event_tap::EventTap>>,
    disable_hotkey: RefCell<Option<Hotkey>>,
    hotkey_specs: RefCell<Vec<(String, WmCommand)>>,
    hotkeys: SharedHotkeyTable,
    wm_sender: wm_controller::Sender,
    stack_line_tx: stack_line::Sender,
    stack_line_hit_rects: stack_line::SharedHitRects,
    /// Re-arm handle for the ffm dwell timer owned by the run loop (set once in run()).
    ffm_dwell: RefCell<Option<crate::sys::timer::TimerHandle>>,
}

// SAFETY: EventTap is constructed on the input thread and all access occurs on
// that same thread (CFRunLoop callback + channel recv both run on the input
// thread's run loop). The Send impl is required only to move the struct across
// the thread::spawn boundary.
unsafe impl Send for EventTap {}

struct State {
    hidden: bool,
    above_window: Option<WindowServerId>,
    /// Window awaiting the ffm dwell confirmation (replaced on every hover change, taken on fire).
    pending_ffm_raise: Option<WindowServerId>,
    /// Dwell (ms) before an ffm raise; 0 = immediate (no debounce). Synced from config.
    ffm_dwell_ms: u64,
    mouse_hides_on_focus: bool,
    focus_follows_mouse_config_enabled: bool,
    default_layout_mode: LayoutMode,
    converter: CoordinateConverter,
    screens: Vec<CGRect>,
    event_processing_enabled: bool,
    focus_follows_mouse_enabled: bool,
    stack_line_enabled: bool,
    disable_hotkey_active: bool,
    low_power_mode: bool,
    pressed_keys: HashSet<KeyCode>,
    current_flags: CGEventFlags,
    screen_spaces: Vec<(CGRect, SpaceId)>,
    layout_mode_by_space: HashMap<SpaceId, crate::common::config::LayoutMode>,
    last_mouse_move_loc: Option<CGPoint>,
    last_mouse_move_timestamp: u64,
    /// Consecutive watchdog ticks observed with mouse-move handling wrongly
    /// suppressed (event processing off, or ffm config-on but runtime-disabled).
    /// Drives the watchdog self-heal that recovers a leaked menu/mission-control
    /// disable without a restart. Reset to 0 whenever handling is healthy.
    mouse_handling_stuck_ticks: u32,
}

impl Default for State {
    fn default() -> Self {
        Self {
            hidden: false,
            above_window: None,
            pending_ffm_raise: None,
            ffm_dwell_ms: 120,
            mouse_hides_on_focus: false,
            focus_follows_mouse_config_enabled: false,
            default_layout_mode: LayoutMode::Traditional,
            converter: CoordinateConverter::default(),
            screens: Vec::new(),
            event_processing_enabled: false,
            focus_follows_mouse_enabled: true,
            stack_line_enabled: false,
            disable_hotkey_active: false,
            low_power_mode: power::is_low_power_mode_enabled(),
            pressed_keys: HashSet::default(),
            current_flags: CGEventFlags::empty(),
            screen_spaces: Vec::new(),
            layout_mode_by_space: HashMap::default(),
            last_mouse_move_loc: None,
            last_mouse_move_timestamp: 0,
            mouse_handling_stuck_ticks: 0,
        }
    }
}

pub type Sender = actor::Sender<Request>;
pub type Receiver = actor::Receiver<Request>;

pub type SharedHotkeyTable = Arc<ArcSwap<HashMap<Hotkey, Vec<WmCommand>>>>;

struct CallbackCtx {
    this: Arc<EventTap>,
}

unsafe fn drop_mouse_ctx(ptr: *mut std::ffi::c_void) {
    unsafe { drop(Box::from_raw(ptr as *mut CallbackCtx)) };
}

impl EventTap {
    #[inline]
    fn stack_line_hover_enabled(&self, state: &State) -> bool {
        state.stack_line_enabled
    }

    #[inline]
    fn focus_follows_mouse_handler_enabled(state: &State) -> bool {
        state.focus_follows_mouse_config_enabled && state.focus_follows_mouse_enabled
    }

    fn keyboard_handlers_enabled(&self) -> bool {
        self.disable_hotkey.borrow().is_some() || !self.hotkeys.load().is_empty()
    }

    fn mouse_move_handlers_enabled(&self) -> bool {
        let state = self.state.borrow();
        state.event_processing_enabled
            && (self.stack_line_hover_enabled(&state)
                || Self::focus_follows_mouse_handler_enabled(&state))
    }

    fn desired_event_mask(&self) -> CGEventMask {
        build_event_mask(
            self.keyboard_handlers_enabled(),
            self.mouse_move_handlers_enabled(),
        )
    }

    fn create_tap_with_mask(
        self: &Arc<Self>,
        mask: CGEventMask,
    ) -> Option<crate::sys::event_tap::EventTap> {
        let ctx = Box::new(CallbackCtx { this: Arc::clone(self) });
        let ctx_ptr = Box::into_raw(ctx) as *mut std::ffi::c_void;

        let tap = unsafe {
            crate::sys::event_tap::EventTap::new_with_options(
                CGTapOpt::Default,
                mask,
                Some(mouse_callback),
                ctx_ptr,
                Some(drop_mouse_ctx),
            )
        };

        if tap.is_none() {
            unsafe { drop(Box::from_raw(ctx_ptr as *mut CallbackCtx)) };
        }

        tap
    }

    fn rebuild_event_tap_mask_if_needed(self: &Arc<Self>) {
        let next_mask = self.desired_event_mask();
        if next_mask == self.event_mask.get() {
            return;
        }

        let Some(new_tap) = self.create_tap_with_mask(next_mask) else {
            warn!("Failed to rebuild event tap with updated mask");
            return;
        };

        let old_tap = self.tap.borrow_mut().replace(new_tap);
        drop(old_tap);
        self.event_mask.set(next_mask);
    }

    pub fn new(
        config: Config,
        events_tx: reactor::Sender,
        requests_rx: Receiver,
        wm_sender: wm_controller::Sender,
        stack_line_tx: stack_line::Sender,
        stack_line_hit_rects: stack_line::SharedHitRects,
    ) -> Self {
        let disable_hotkey = config
            .settings
            .focus_follows_mouse_disable_hotkey
            .clone()
            .and_then(|spec| spec.to_hotkey());
        let mut state = State::default();
        state.mouse_hides_on_focus = config.settings.mouse_hides_on_focus;
        state.focus_follows_mouse_config_enabled = config.settings.focus_follows_mouse;
        state.ffm_dwell_ms = config.settings.focus_follows_mouse_dwell_ms;
        state.stack_line_enabled = config.settings.ui.stack_line.enabled;
        state.default_layout_mode = config.settings.layout.mode;
        state.disable_hotkey_active = disable_hotkey
            .as_ref()
            .map(|target| state.compute_disable_hotkey_active(target))
            .unwrap_or(false);
        let event_mask = build_event_mask(
            disable_hotkey.is_some(),
            state.event_processing_enabled
                && (state.stack_line_enabled || Self::focus_follows_mouse_handler_enabled(&state)),
        );
        EventTap {
            ffm_dwell: RefCell::new(None),
            events_tx,
            requests_rx: Some(requests_rx),
            state: RefCell::new(state),
            event_mask: Cell::new(event_mask),
            tap: RefCell::new(None),
            disable_hotkey: RefCell::new(disable_hotkey),
            hotkey_specs: RefCell::new(Vec::new()),
            hotkeys: Arc::new(ArcSwap::from_pointee(HashMap::default())),
            wm_sender,
            stack_line_tx,
            stack_line_hit_rects,
        }
    }

    pub async fn run(mut self) {
        use crate::sys::timer::Timer;
        use tracing::Span;

        enum Tick {
            Request(Request),
            Watchdog,
        }

        let requests_rx = self.requests_rx.take().unwrap();

        let this = Arc::new(self);

        let mask = this.event_mask.get();
        let tap = this.create_tap_with_mask(mask);

        if let Some(tap) = tap {
            *this.tap.borrow_mut() = Some(tap);
        } else {
            return;
        }

        if this.state.borrow().mouse_hides_on_focus {
            if let Err(e) = window_server::allow_hide_mouse() {
                error!(
                    "Could not enable mouse hiding: {e:?}. \
                    mouse_hides_on_focus will have no effect."
                );
            }
        }

        let watchdog = Timer::repeating(Duration::from_secs(5), Duration::from_secs(5));

        // ffm dwell confirmation: armed (re-armed) by the tap callback on every hover change; fires
        // only when the cursor has stayed on one window for the dwell — then the raise goes through.
        let mut ffm_dwell_timer = Timer::manual();
        *this.ffm_dwell.borrow_mut() = Some(ffm_dwell_timer.handle());

        let mut merged = StreamExt::merge(
            UnboundedReceiverStream::new(requests_rx).map(|(span, req)| (span, Tick::Request(req))),
            watchdog.map(|()| (Span::none(), Tick::Watchdog)),
        );

        loop {
            let (span, tick) = tokio::select! {
                item = merged.next() => match item {
                    Some(t) => t,
                    None => break,
                },
                _ = ffm_dwell_timer.next() => {
                    let pending = this.state.borrow_mut().pending_ffm_raise.take();
                    if let Some(wsid) = pending {
                        _ = this.events_tx.send(Event::MouseMovedOverWindow(wsid));
                    }
                    continue;
                }
            };
            let _guard = span.enter();
            match tick {
                Tick::Request(request) => this.on_request(request),
                Tick::Watchdog => {
                    let tap_enabled = this.tap.borrow().is_some();
                    if let Some(tap) = this.tap.borrow().as_ref() {
                        tap.set_enabled(true);
                    }
                    // Full modifier reconciliation: prune any pressed_keys not
                    // reflected in the last known flags.
                    let mut state = this.state.borrow_mut();
                    state.reconcile_modifier_keys();
                    trace!(
                        tap_enabled,
                        event_mask = this.event_mask.get(),
                        pressed_keys = state.pressed_keys.len(),
                        disable_hotkey_active = state.disable_hotkey_active,
                        event_processing = state.event_processing_enabled,
                        ffm_runtime_enabled = state.focus_follows_mouse_enabled,
                        "watchdog tick"
                    );

                    // Self-heal a leaked mouse-handling disable. Mouse-move handling is
                    // suppressed (mask rebuilt without MouseMoved) when event processing is
                    // off, or when ffm is config-on but runtime-disabled by a transient
                    // menu-open / mission-control state. If the reactor misses the matching
                    // "menu closed / MC exited" notification, that disable never clears and
                    // ffm stays dead until a manual restart (cursor still warps via the
                    // external focus daemon, but focus never follows). Both disables are only
                    // ever meant to be brief (~600 ms startup window; a menu/MC interaction),
                    // so if either is still asserted across several 5 s ticks, force handling
                    // back on. Harmless if the disable was legitimate and still active: the
                    // reactor's own menu-open / MC guards in should_raise_on_mouse_over still
                    // suppress raises, and the next genuine state change re-issues the request.
                    let ffm_stuck = state.focus_follows_mouse_config_enabled
                        && !state.focus_follows_mouse_enabled;
                    let processing_stuck = !state.event_processing_enabled;
                    if ffm_stuck || processing_stuck {
                        state.mouse_handling_stuck_ticks =
                            state.mouse_handling_stuck_ticks.saturating_add(1);
                    } else {
                        state.mouse_handling_stuck_ticks = 0;
                    }
                    // 3 ticks ≈ 15 s — long enough to never fight the 600 ms startup window
                    // or a normal menu/MC interaction, short enough to recover promptly.
                    if state.mouse_handling_stuck_ticks >= 3 {
                        warn!(
                            event_processing = state.event_processing_enabled,
                            ffm_runtime_enabled = state.focus_follows_mouse_enabled,
                            "mouse handling stuck disabled across watchdog ticks; self-healing"
                        );
                        state.event_processing_enabled = true;
                        state.focus_follows_mouse_enabled = true;
                        state.mouse_handling_stuck_ticks = 0;
                        state.reset(true);
                        drop(state);
                        this.rebuild_event_tap_mask_if_needed();
                    }
                }
            }
        }
    }

    fn on_request(self: &Arc<Self>, request: Request) {
        let mut should_rebuild_mask = false;
        let mut state = self.state.borrow_mut();
        match request {
            Request::Warp(point) => {
                if let Err(e) = event::warp_mouse(point) {
                    warn!("Failed to warp mouse: {e:?}");
                } else {
                    state.above_window = None;
                    state.pending_ffm_raise = None; // warp = programmatic move; don't raise the swept window
                }
                if state.mouse_hides_on_focus && !state.hidden {
                    debug!("Hiding mouse");
                    if let Err(e) = event::hide_mouse() {
                        warn!("Failed to hide mouse: {e:?}");
                    }
                    state.hidden = true;
                }
            }
            Request::WarpSilent(point) => {
                if let Err(e) = event::warp_mouse_silent(point) {
                    warn!("Failed to warp mouse: {e:?}");
                } else {
                    state.above_window = None;
                    state.pending_ffm_raise = None; // warp = programmatic move; don't raise the swept window
                }
                if state.mouse_hides_on_focus && !state.hidden {
                    debug!("Hiding mouse");
                    if let Err(e) = event::hide_mouse() {
                        warn!("Failed to hide mouse: {e:?}");
                    }
                    state.hidden = true;
                }
            }
            Request::EnforceHidden => {
                if state.mouse_hides_on_focus && state.hidden {
                    if let Err(e) = event::hide_mouse() {
                        warn!("Failed to hide mouse: {e:?}");
                    }
                }
            }
            Request::ScreenParametersChanged(screens_with_spaces, converter) => {
                state.screens = screens_with_spaces.iter().map(|(frame, _)| *frame).collect();
                state.screen_spaces = screens_with_spaces
                    .into_iter()
                    .filter_map(|(frame, maybe_space)| maybe_space.map(|space| (frame, space)))
                    .collect();
                state.converter = converter;
            }
            Request::SpaceChanged(spaces) => {
                state.screen_spaces = state
                    .screens
                    .iter()
                    .copied()
                    .zip(spaces.into_iter())
                    .filter_map(|(frame, maybe_space)| maybe_space.map(|space| (frame, space)))
                    .collect();
            }
            Request::SetEventProcessing(enabled) => {
                state.event_processing_enabled = enabled;
                state.reset(enabled);
                should_rebuild_mask = true;
            }
            Request::SetFocusFollowsMouseEnabled(enabled) => {
                // Redundant sets arrive on every app activation (menu-state
                // bookkeeping). Resetting hover state on a no-op toggle makes
                // the next mouse move count as a fresh hover and re-raise the
                // window under the cursor — which fights topmost escalations.
                if state.focus_follows_mouse_enabled != enabled {
                    debug!(
                        "focus_follows_mouse temporarily {}",
                        if enabled { "enabled" } else { "disabled" }
                    );
                    state.focus_follows_mouse_enabled = enabled;
                    state.reset(enabled);
                    should_rebuild_mask = true;
                }
            }
            Request::SetHotkeys(bindings) => {
                *self.hotkey_specs.borrow_mut() = bindings;
                self.rebuild_hotkeys_for_current_layout();
                should_rebuild_mask = true;
            }
            Request::KeyboardLayoutChanged => {
                self.rebuild_hotkeys_for_current_layout();
                should_rebuild_mask = true;
            }
            Request::ConfigUpdated(new_config) => {
                let mouse_hides_on_focus = new_config.settings.mouse_hides_on_focus;
                let focus_follows_mouse_config_enabled = new_config.settings.focus_follows_mouse;
                state.ffm_dwell_ms = new_config.settings.focus_follows_mouse_dwell_ms;
                let stack_line_enabled = new_config.settings.ui.stack_line.enabled;
                let default_layout_mode = new_config.settings.layout.mode;
                let disable_hotkey = new_config
                    .settings
                    .focus_follows_mouse_disable_hotkey
                    .clone()
                    .and_then(|spec| spec.to_hotkey());
                *self.disable_hotkey.borrow_mut() = disable_hotkey;
                {
                    let prev_mouse_hides_on_focus = state.mouse_hides_on_focus;
                    state.mouse_hides_on_focus = mouse_hides_on_focus;
                    state.focus_follows_mouse_config_enabled = focus_follows_mouse_config_enabled;
                    state.stack_line_enabled = stack_line_enabled;
                    state.default_layout_mode = default_layout_mode;
                    let prev_active = state.disable_hotkey_active;
                    state.disable_hotkey_active = self
                        .disable_hotkey
                        .borrow()
                        .as_ref()
                        .map(|target| state.compute_disable_hotkey_active(target))
                        .unwrap_or(false);
                    if prev_active && !state.disable_hotkey_active {
                        state.reset(true);
                    }
                    if prev_mouse_hides_on_focus && !state.mouse_hides_on_focus && state.hidden {
                        debug!("Showing mouse after disabling mouse_hides_on_focus");
                        if let Err(e) = event::show_mouse() {
                            warn!("Failed to show mouse: {e:?}");
                        }
                        state.hidden = false;
                    }
                }
                should_rebuild_mask = true;
            }
            Request::LayoutModesChanged(modes) => {
                state.layout_mode_by_space.clear();
                for (space, mode) in modes {
                    state.layout_mode_by_space.insert(space, mode);
                }
                debug!(
                    "Updated layout modes for {} spaces",
                    state.layout_mode_by_space.len()
                );
            }
            Request::SetLowPowerMode(enabled) => {
                if state.low_power_mode != enabled {
                    debug!("low_power_mode changed in event tap: {}", enabled);
                    state.low_power_mode = enabled;
                    state.last_mouse_move_loc = None;
                    state.last_mouse_move_timestamp = 0;
                }
            }
        }
        drop(state);

        if should_rebuild_mask {
            self.rebuild_event_tap_mask_if_needed();
        }
    }

    fn refresh_disable_hotkey_state(&self, state: &mut State) {
        let Some(target) = self.disable_hotkey.borrow().as_ref().cloned() else {
            return;
        };
        let prev_active = state.disable_hotkey_active;
        state.disable_hotkey_active = state.compute_disable_hotkey_active(&target);
        if state.disable_hotkey_active != prev_active {
            if state.disable_hotkey_active {
                debug!(?target, "focus_follows_mouse disabled while hotkey held");
            } else {
                debug!(?target, "focus_follows_mouse re-enabled after hotkey release");
                state.reset(true);
            }
        }
    }

    fn on_event(self: &Arc<Self>, event_type: CGEventType, event: &CGEvent) -> bool {
        // Check if the tap was re-enabled after being disabled by timeout or
        // user input. If so, clear pressed_keys to avoid phantom modifiers
        // from lost key-up events during the disabled period.
        if let Some(tap) = self.tap.borrow().as_ref() {
            if tap.take_reenabled_flag() {
                let mut state = self.state.borrow_mut();
                debug!(
                    "Event tap was re-enabled; clearing pressed_keys to prevent phantom modifiers"
                );
                state.pressed_keys.clear();
                state.current_flags = CGEvent::flags(Some(event));
                state.reconcile_modifier_keys();
                drop(state);
                self.refresh_disable_hotkey_state(&mut self.state.borrow_mut());
            }
        }

        let mut state = self.state.borrow_mut();

        if !matches!(
            event_type,
            CGEventType::KeyDown | CGEventType::KeyUp | CGEventType::FlagsChanged
        ) {
            // Keep modifier-only hotkey state in sync even when macOS drops a
            // key-up/flags-changed event (common after system UI interruptions).
            let flags = CGEvent::flags(Some(event));
            if flags != state.current_flags {
                state.current_flags = flags;
                state.reconcile_modifier_keys();
                self.refresh_disable_hotkey_state(&mut state);
            }
        }

        match event_type {
            CGEventType::LeftMouseDown | CGEventType::RightMouseDown => {
                set_mouse_state(MouseState::Down);

                let loc = CGEvent::location(Some(event));

                // The event tap is the single source of hit-testing for
                // stack-line indicators. Only forward the click and
                // suppress propagation when it lands on a visible,
                // non-occluded indicator.
                let hits_stack_line = self
                    .stack_line_hit_rects
                    .load()
                    .iter()
                    .copied()
                    .any(|frame| point_hits_indicator_frame(loc, frame));
                if hits_stack_line && !window_server::is_point_occluded_by_external_window(loc) {
                    let _ = self.stack_line_tx.try_send(stack_line::Event::MouseDown(loc));
                    return false;
                }
            }
            CGEventType::LeftMouseDragged | CGEventType::RightMouseDragged => {
                set_mouse_state(MouseState::Down);
            }
            CGEventType::LeftMouseUp | CGEventType::RightMouseUp => set_mouse_state(MouseState::Up),
            _ => {}
        }

        if matches!(
            event_type,
            CGEventType::KeyDown | CGEventType::KeyUp | CGEventType::FlagsChanged
        ) {
            return self.handle_keyboard_event(event_type, event, &mut state);
        }

        if !state.event_processing_enabled {
            trace!("Mouse event processing disabled, ignoring {:?}", event_type);
            return true;
        }

        if state.hidden {
            debug!("Showing mouse");
            if let Err(e) = event::show_mouse() {
                warn!("Failed to show mouse: {e:?}");
            }
            state.hidden = false;
        }
        match event_type {
            CGEventType::LeftMouseDown | CGEventType::RightMouseDown => {
                // Early topmost-reassert trigger: the system's click-raise
                // happens on mouse-down, before mouse-up arrives.
                _ = self.events_tx.send(Event::MouseDown);
            }
            CGEventType::RightMouseUp | CGEventType::LeftMouseUp => {
                _ = self.events_tx.send(Event::MouseUp);
            }
            CGEventType::MouseMoved => {
                let loc = CGEvent::location(Some(event));
                let ts = CGEvent::timestamp(Some(event));
                let sampling = mouse_move_sampling_profile(state.low_power_mode);
                if !state.should_sample_mouse_move(loc, ts, sampling) {
                    return true;
                }

                // stack line hover feedback
                if state.stack_line_enabled {
                    let hits = self
                        .stack_line_hit_rects
                        .load()
                        .iter()
                        .copied()
                        .any(|frame| point_hits_indicator_frame(loc, frame))
                        && !window_server::is_point_occluded_by_external_window(loc);
                    let _ = self.stack_line_tx.try_send(stack_line::Event::MouseMoved {
                        point: loc,
                        hits_indicator: hits,
                    });
                }

                _ = self.events_tx.send(Event::MouseMoved);

                // ffm — forward deduped window-under-cursor changes to the
                // reactor. The event's embedded window id can be stale after
                // cross-app focus changes, so prefer a WindowServer hit-test
                // at the event location and use the event field as fallback.
                if state.focus_follows_mouse_config_enabled
                    && state.focus_follows_mouse_enabled
                    && !state.disable_hotkey_active
                {
                    let wsid = window_from_mouse_event(event);
                    if let Some(wsid) = wsid {
                        match state.note_ffm_hover(wsid) {
                            FfmHover::RaiseNow(wsid) => {
                                _ = self.events_tx.send(Event::MouseMovedOverWindow(wsid));
                            }
                            FfmHover::Armed(dwell_ms) => {
                                if let Some(h) = self.ffm_dwell.borrow().as_ref() {
                                    h.set_next_fire(Duration::from_millis(dwell_ms));
                                }
                            }
                            FfmHover::NoChange => {}
                        }
                    }
                }
            }
            _ => (),
        }

        true
    }

    fn handle_keyboard_event(
        &self,
        event_type: CGEventType,
        event: &CGEvent,
        state: &mut State,
    ) -> bool {
        let key_code_opt = key_code_from_event(event);

        if let Some(key_code) = key_code_opt {
            match event_type {
                CGEventType::KeyDown => state.note_key_down(key_code),
                CGEventType::KeyUp => state.note_key_up(key_code),
                CGEventType::FlagsChanged => state.note_flags_changed(key_code),
                _ => {}
            }
        }

        let flags = CGEvent::flags(Some(event));
        state.current_flags = flags;
        self.refresh_disable_hotkey_state(state);

        if event_type == CGEventType::KeyDown {
            if let Some(key_code) = key_code_opt {
                let hotkey = Hotkey::new(
                    modifiers_from_flags_with_keys(state.current_flags, &state.pressed_keys),
                    key_code,
                );
                if native_mission_control_hotkey(&hotkey) {
                    _ = self.events_tx.send(Event::MissionControlNativeEntered);
                }
                let bindings = self.hotkeys.load();
                if let Some(commands) = bindings.get(&hotkey) {
                    for cmd in commands {
                        self.wm_sender.send(WmEvent::Command(cmd.clone()));
                    }
                    return false;
                }
            }
        }

        true
    }

    fn rebuild_hotkeys_for_current_layout(&self) {
        let specs = self.hotkey_specs.borrow();
        let mut map: HashMap<Hotkey, Vec<WmCommand>> = HashMap::default();

        for (spec, command) in specs.iter() {
            let Ok(hotkey) = Hotkey::from_str(spec) else {
                warn!(%spec, "Skipping hotkey that no longer resolves for current keyboard layout");
                continue;
            };

            if hotkey.modifiers.has_generic_modifiers() {
                for expanded_mods in hotkey.modifiers.expand_to_specific() {
                    let expanded_hotkey = Hotkey::new(expanded_mods, hotkey.key_code);
                    let entry = map.entry(expanded_hotkey).or_default();
                    if !entry.contains(command) {
                        entry.push(command.clone());
                    }
                }
            } else {
                let entry = map.entry(hotkey).or_default();
                if !entry.contains(command) {
                    entry.push(command.clone());
                }
            }
        }

        trace!(
            "Updated hotkey bindings for current keyboard layout: {}",
            map.len()
        );
        self.hotkeys.store(Arc::new(map));
    }
}

unsafe extern "C-unwind" fn mouse_callback(
    _proxy: CGEventTapProxy,
    event_type: CGEventType,
    event_ref: core::ptr::NonNull<CGEvent>,
    user_info: *mut std::ffi::c_void,
) -> *mut CGEvent {
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let ctx = unsafe { &*(user_info as *const CallbackCtx) };
        let event = unsafe { event_ref.as_ref() };
        ctx.this.on_event(event_type, event)
    }));

    match result {
        Ok(true) => event_ref.as_ptr(),
        Ok(false) => core::ptr::null_mut(),
        Err(_) => event_ref.as_ptr(),
    }
}

impl State {
    #[inline]
    fn should_sample_mouse_move(
        &mut self,
        loc: CGPoint,
        timestamp: u64,
        sampling: (u64, f64),
    ) -> bool {
        let Some(last_loc) = self.last_mouse_move_loc else {
            self.last_mouse_move_loc = Some(loc);
            self.last_mouse_move_timestamp = timestamp;
            return true;
        };

        let dx = loc.x - last_loc.x;
        let dy = loc.y - last_loc.y;
        let dist_sq = dx * dx + dy * dy;
        let elapsed = timestamp.saturating_sub(self.last_mouse_move_timestamp);

        // Sample only when the move BOTH covered enough distance AND enough time has passed since the
        // last sample — i.e. skip if it's too close OR too soon. The previous `&&` skipped a move only
        // when it was both too-close and too-soon, so any fast movement (always far enough) blew straight
        // past the time cap: every native HID event (up to ~1000 Hz) ran the full handler, including a
        // synchronous WindowServer hit-test (`window_from_mouse_event` → `get_window_at_point`) inside the
        // CGEvent tap callback. Because an event tap is in the synchronous HID delivery path, that stalled
        // cursor/event delivery and made the system throttle the tap — the "glitchy under fast movement"
        // jank. With `||`, the interval is a hard cap (~125 Hz) however fast the cursor travels.
        if dist_sq < sampling.1 || elapsed < sampling.0 {
            return false;
        }

        self.last_mouse_move_loc = Some(loc);
        self.last_mouse_move_timestamp = timestamp;
        true
    }

    #[cfg(test)]
    fn layout_mode_at_point(&self, loc: CGPoint) -> Option<crate::common::config::LayoutMode> {
        use crate::sys::geometry::CGRectExt;
        self.screen_spaces
            .iter()
            .find(|(frame, _)| frame.contains(loc))
            .and_then(|(_, space)| self.layout_mode_by_space.get(space).copied())
    }

    fn note_key_down(&mut self, key_code: KeyCode) {
        self.pressed_keys.insert(key_code);
    }

    fn note_key_up(&mut self, key_code: KeyCode) {
        self.pressed_keys.remove(&key_code);
    }

    fn note_flags_changed(&mut self, key_code: KeyCode) {
        if !is_modifier_key(key_code) {
            return;
        }
        // Determine whether this modifier is currently pressed by checking
        // the authoritative CGEventFlags, not our tracked set.
        if let Some(flag) = modifier_flag_for_key(key_code) {
            if self.current_flags.contains(flag) {
                self.pressed_keys.insert(key_code);
            } else {
                self.pressed_keys.remove(&key_code);
            }
        }
    }

    fn reconcile_modifier_keys(&mut self) {
        self.pressed_keys.retain(|key| {
            if let Some(flag) = modifier_flag_for_key(*key) {
                self.current_flags.contains(flag)
            } else {
                true // non-modifier keys are not reconciled here
            }
        });
    }

    fn compute_disable_hotkey_active(&self, target: &Hotkey) -> bool {
        let active_mods = modifiers_from_flags_with_keys(self.current_flags, &self.pressed_keys);

        let check_modifier = |left: Modifiers, right: Modifiers| -> bool {
            let target_has_left = target.modifiers.contains(left);
            let target_has_right = target.modifiers.contains(right);
            let active_has_left = active_mods.contains(left);
            let active_has_right = active_mods.contains(right);

            if target_has_left && target_has_right {
                active_has_left || active_has_right
            } else if target_has_left {
                active_has_left
            } else if target_has_right {
                active_has_right
            } else {
                true
            }
        };

        let shift_ok = check_modifier(Modifiers::SHIFT_LEFT, Modifiers::SHIFT_RIGHT);
        let ctrl_ok = check_modifier(Modifiers::CONTROL_LEFT, Modifiers::CONTROL_RIGHT);
        let alt_ok = check_modifier(Modifiers::ALT_LEFT, Modifiers::ALT_RIGHT);
        let meta_ok = check_modifier(Modifiers::META_LEFT, Modifiers::META_RIGHT);

        if !(shift_ok && ctrl_ok && alt_ok && meta_ok) {
            return false;
        }

        self.base_key_active(target.key_code)
    }

    fn base_key_active(&self, key_code: KeyCode) -> bool {
        if is_modifier_key(key_code) {
            modifier_flag_for_key(key_code)
                .map(|flag| self.current_flags.contains(flag))
                .unwrap_or(false)
        } else {
            self.pressed_keys.contains(&key_code)
        }
    }

    /// Returns true if the window under the cursor changed.
    fn above_window_changed(&mut self, wsid: WindowServerId) -> bool {
        if self.above_window == Some(wsid) {
            return false;
        }
        self.above_window = Some(wsid);
        true
    }

    /// ffm dwell decision for a hover sample. A fast sweep crosses many windows; raising each one
    /// is an app activation + menu bar switch + window reorder, and ~10/s of those saturate
    /// WindowServer (measured: border/overlay updates then composite 100-200 ms late — the
    /// "border trails the mouse" jank). So a hover only ARMS a raise; the dwell timer fires it iff
    /// the cursor is still on that window dwell_ms later. Each hover change replaces the pending
    /// window and pushes the timer forward, so sweeping raises nothing until the cursor settles.
    fn note_ffm_hover(&mut self, wsid: WindowServerId) -> FfmHover {
        if !self.above_window_changed(wsid) {
            return FfmHover::NoChange;
        }
        if self.ffm_dwell_ms == 0 {
            return FfmHover::RaiseNow(wsid); // dwell disabled: legacy immediate raise
        }
        self.pending_ffm_raise = Some(wsid);
        FfmHover::Armed(self.ffm_dwell_ms)
    }

    fn reset(&mut self, enabled: bool) {
        self.pending_ffm_raise = None; // toggling ffm must never fire a stale dwell raise
        if enabled {
            self.above_window = None;
            self.last_mouse_move_loc = None;
            self.last_mouse_move_timestamp = 0;
        }
    }
}

#[inline]
fn window_from_mouse_event(event: &CGEvent) -> Option<WindowServerId> {
    let loc = CGEvent::location(Some(event));
    if let Some(wsid) = window_server::get_window_at_point(loc) {
        return Some(wsid);
    }

    let field_value =
        CGEvent::integer_value_field(Some(event), CGEventField::MouseEventWindowUnderMousePointer);
    let id = u32::try_from(field_value).ok()?;
    (id != 0).then(|| WindowServerId::new(id))
}

#[inline]
fn native_mission_control_hotkey(hotkey: &Hotkey) -> bool {
    hotkey.key_code == KeyCode::F3
        || (hotkey.key_code == KeyCode::ArrowUp && hotkey.modifiers.intersects(Modifiers::CONTROL))
}

#[inline]
fn mouse_move_sampling_profile(low_power_mode: bool) -> (u64, f64) {
    if low_power_mode {
        (
            MOUSE_MOVE_MIN_INTERVAL_NS_LOW_POWER,
            MOUSE_MOVE_MIN_DISTANCE_PX_SQ_LOW_POWER,
        )
    } else {
        (
            MOUSE_MOVE_MIN_INTERVAL_NS_NORMAL,
            MOUSE_MOVE_MIN_DISTANCE_PX_SQ_NORMAL,
        )
    }
}

fn build_event_mask(keyboard_enabled: bool, mouse_move_enabled: bool) -> CGEventMask {
    let mut m: u64 = 0;
    let add = |m: &mut u64, ty: CGEventType| *m |= 1u64 << (ty.0 as u64);

    for ty in [
        CGEventType::LeftMouseDown,
        CGEventType::LeftMouseUp,
        CGEventType::RightMouseDown,
        CGEventType::RightMouseUp,
        CGEventType::LeftMouseDragged,
        CGEventType::RightMouseDragged,
    ] {
        add(&mut m, ty);
    }
    if mouse_move_enabled {
        add(&mut m, CGEventType::MouseMoved);
    }
    if keyboard_enabled {
        for ty in [
            CGEventType::KeyDown,
            CGEventType::KeyUp,
            CGEventType::FlagsChanged,
        ] {
            add(&mut m, ty);
        }
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_mode_at_point_uses_space_mapping() {
        let mut state = State::default();
        let left = CGRect::new(
            CGPoint::new(0.0, 0.0),
            objc2_core_foundation::CGSize::new(100.0, 100.0),
        );
        let right = CGRect::new(
            CGPoint::new(100.0, 0.0),
            objc2_core_foundation::CGSize::new(100.0, 100.0),
        );

        let left_space = SpaceId::new(1);
        let right_space = SpaceId::new(2);
        state.screen_spaces = vec![(left, left_space), (right, right_space)];
        state
            .layout_mode_by_space
            .insert(left_space, crate::common::config::LayoutMode::Traditional);
        state
            .layout_mode_by_space
            .insert(right_space, crate::common::config::LayoutMode::Scrolling);

        assert_eq!(
            state.layout_mode_at_point(CGPoint::new(50.0, 50.0)),
            Some(crate::common::config::LayoutMode::Traditional)
        );
        assert_eq!(
            state.layout_mode_at_point(CGPoint::new(150.0, 50.0)),
            Some(crate::common::config::LayoutMode::Scrolling)
        );
    }

    // Regression: fast mouse movement must stay time-capped. A torrent of large, sub-interval moves
    // (a mouse whipped across the screen) used to bypass the throttle entirely — the `&&` only skipped
    // moves that were both too-close AND too-soon, so every far-enough HID event sampled, running a
    // synchronous WindowServer hit-test in the event tap and stuttering the cursor. With the `||` cap,
    // only ~1 sample per interval passes however fast and far the cursor travels.
    #[test]
    fn fast_movement_is_time_capped() {
        let mut state = State::default();
        let sampling = (
            MOUSE_MOVE_MIN_INTERVAL_NS_NORMAL,
            MOUSE_MOVE_MIN_DISTANCE_PX_SQ_NORMAL,
        );
        // First move always samples (no prior reference) and arms the clock at t=0.
        assert!(state.should_sample_mouse_move(CGPoint::new(0.0, 0.0), 0, sampling));

        // 50 huge jumps (200px each, way over the distance floor) arriving every 1ms — a 50ms span. Every
        // one is "far enough", so under the old `&&` ~49 would sample (one synchronous hit-test each). With
        // the time cap they collapse to ~one per 8ms interval: at 125 Hz a 50ms span allows ≤7 samples.
        let mut sampled = 0;
        for i in 1..=50u64 {
            let x = (i as f64) * 200.0;
            if state.should_sample_mouse_move(CGPoint::new(x, 0.0), i * 1_000_000, sampling) {
                sampled += 1;
            }
        }
        assert!(
            (1..=7).contains(&sampled),
            "fast movement must be capped to ~125 Hz (≤7 over 50ms), got {sampled} (old &&: ~49)"
        );
    }

    // The ffm dwell decision: sweeping across windows must only ARM (replacing the pending window),
    // never raise directly; the same window twice is a no-op; dwell=0 keeps the legacy immediate raise.
    #[test]
    fn ffm_hover_arms_and_replaces_instead_of_raising() {
        let mut state = State::default();
        state.ffm_dwell_ms = 120;
        let a = WindowServerId::new(1);
        let b = WindowServerId::new(2);
        assert_eq!(state.note_ffm_hover(a), FfmHover::Armed(120));
        assert_eq!(state.pending_ffm_raise, Some(a));
        assert_eq!(state.note_ffm_hover(a), FfmHover::NoChange, "same window: no re-arm");
        assert_eq!(state.note_ffm_hover(b), FfmHover::Armed(120), "sweep: replace pending");
        assert_eq!(state.pending_ffm_raise, Some(b), "only the LAST hovered window can be raised");
        state.reset(true);
        assert_eq!(state.pending_ffm_raise, None, "ffm toggle clears any pending raise");
    }

    #[test]
    fn ffm_hover_zero_dwell_raises_immediately() {
        let mut state = State::default();
        state.ffm_dwell_ms = 0;
        let a = WindowServerId::new(7);
        assert_eq!(state.note_ffm_hover(a), FfmHover::RaiseNow(a));
        assert_eq!(state.pending_ffm_raise, None);
    }

    // A move past the interval but with negligible distance (resting-hand jitter) is still skipped, so
    // we don't resample the WindowServer on a stationary cursor.
    #[test]
    fn slow_jitter_below_distance_floor_is_skipped() {
        let mut state = State::default();
        let sampling = (
            MOUSE_MOVE_MIN_INTERVAL_NS_NORMAL,
            MOUSE_MOVE_MIN_DISTANCE_PX_SQ_NORMAL,
        );
        assert!(state.should_sample_mouse_move(CGPoint::new(100.0, 100.0), 0, sampling));
        // 20ms later (well past the cap) but only 1px away → below the 2px floor → skip.
        assert!(!state.should_sample_mouse_move(CGPoint::new(101.0, 100.0), 20_000_000, sampling));
        // A real 3px move after the interval samples.
        assert!(state.should_sample_mouse_move(CGPoint::new(104.0, 100.0), 40_000_000, sampling));
    }
}
