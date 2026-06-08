use std::convert::TryFrom;
use std::sync::atomic::{AtomicU8, Ordering};

use objc2_core_foundation::CGPoint;
use objc2_core_graphics::{
    CGDisplayHideCursor, CGDisplayShowCursor, CGError, CGEventSourceStateID, kCGNullDirectDisplay,
};
use serde::{Deserialize, Serialize};

pub use super::window_server::current_cursor_location;
use crate::sys::cg_ok;
pub use crate::sys::hotkey::{Hotkey, HotkeySpec, KeyCode, Modifiers};
use crate::sys::skylight::{
    CFRelease, CGAssociateMouseAndMouseCursorPosition, CGEventCreateMouseEvent, CGEventPost,
    CGEventSetIntegerValueField, CGEventSourceCreate,
    CGEventSourceSetLocalEventsSuppressionInterval, CGEventTapLocation, CGWarpMouseCursorPosition,
};

const K_CG_EVENT_MOUSE_MOVED: u32 = 5;
const K_CG_MOUSE_BUTTON_LEFT: u32 = 0;
const K_CG_MOUSE_EVENT_DELTA_X: u32 = 4;
const K_CG_MOUSE_EVENT_DELTA_Y: u32 = 5;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum MouseState {
    Up = 1,
    Down = 2,
}

const MOUSE_STATE_UNKNOWN: u8 = 0;

static MOUSE_STATE: AtomicU8 = AtomicU8::new(MOUSE_STATE_UNKNOWN);

impl From<MouseState> for u8 {
    fn from(state: MouseState) -> u8 {
        state as u8
    }
}

impl TryFrom<u8> for MouseState {
    type Error = ();

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            x if x == MouseState::Up as u8 => Ok(MouseState::Up),
            x if x == MouseState::Down as u8 => Ok(MouseState::Down),
            _ => Err(()),
        }
    }
}

pub fn set_mouse_state(state: MouseState) {
    MOUSE_STATE.store(state.into(), Ordering::Relaxed);
}

pub fn get_mouse_state() -> Option<MouseState> {
    match MouseState::try_from(MOUSE_STATE.load(Ordering::Relaxed)) {
        Ok(s) => Some(s),
        Err(_) => None,
    }
}

pub fn warp_mouse(point: CGPoint) -> Result<(), CGError> {
    let src = unsafe { CGEventSourceCreate(CGEventSourceStateID::CombinedSessionState) };
    unsafe { CGEventSourceSetLocalEventsSuppressionInterval(src, 0.0) };

    let res = cg_ok(unsafe { CGWarpMouseCursorPosition(point) });
    let _ = cg_ok(unsafe { CGAssociateMouseAndMouseCursorPosition(true) });
    let event = unsafe {
        CGEventCreateMouseEvent(src, K_CG_EVENT_MOUSE_MOVED, point, K_CG_MOUSE_BUTTON_LEFT)
    };
    if !event.is_null() {
        unsafe {
            CGEventSetIntegerValueField(event, K_CG_MOUSE_EVENT_DELTA_X, 4);
            CGEventSetIntegerValueField(event, K_CG_MOUSE_EVENT_DELTA_Y, 4);
            CGEventPost(CGEventTapLocation::HID, event);
            CFRelease(event);
        }
    }
    unsafe { CFRelease(src) };
    res
}

/// Move the cursor without synthesizing a mouse-moved event. Use this when focus has already been
/// decided (e.g. right after move-window-to-display) and a synthetic move would let focus-follows-
/// mouse re-pick a window — possibly one still overlapping mid-relayout — and steal focus back.
pub fn warp_mouse_silent(point: CGPoint) -> Result<(), CGError> {
    let res = cg_ok(unsafe { CGWarpMouseCursorPosition(point) });
    let _ = cg_ok(unsafe { CGAssociateMouseAndMouseCursorPosition(true) });
    res
}

pub fn hide_mouse() -> Result<(), CGError> {
    cg_ok(CGDisplayHideCursor(kCGNullDirectDisplay))
}

pub fn show_mouse() -> Result<(), CGError> {
    cg_ok(CGDisplayShowCursor(kCGNullDirectDisplay))
}
