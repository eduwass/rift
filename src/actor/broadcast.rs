use serde::{Deserialize, Serialize};

use crate::actor::app::WindowId;
use crate::layout_engine::{LayoutKind, VirtualWorkspaceId};
use crate::sys::screen::SpaceId;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct StackInfo {
    pub container_kind: LayoutKind,
    pub total_count: usize,
    pub selected_index: usize,
    pub windows: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastEvent {
    WorkspaceChanged {
        space_id: SpaceId,
        workspace_id: VirtualWorkspaceId,
        workspace_name: String,
        display_uuid: Option<String>,
    },
    WindowsChanged {
        workspace_id: VirtualWorkspaceId,
        workspace_name: String,
        windows: Vec<String>,
        space_id: SpaceId,
        display_uuid: Option<String>,
    },
    WindowTitleChanged {
        window_id: WindowId,
        workspace_id: VirtualWorkspaceId,
        workspace_index: Option<u64>,
        workspace_name: String,
        previous_title: String,
        new_title: String,
        space_id: SpaceId,
        display_uuid: Option<String>,
    },
    StacksChanged {
        workspace_id: VirtualWorkspaceId,
        workspace_index: Option<u64>,
        workspace_name: String,
        stacks: Vec<StackInfo>,
        active_workspace_has_fullscreen: bool,
        space_id: SpaceId,
        display_uuid: Option<String>,
    },
    // Fired whenever the focused window changes (keyboard focus, focus-follows-mouse, or a
    // programmatic raise), carrying the window's current frame so an event-driven overlay
    // renderer can place a border/halo without polling CGWindowList. Frame is sent as four plain
    // f64s (CG top-left coords) to keep the JSON dependency-free for the Swift subscriber.
    WindowFocused {
        window_id: WindowId,
        frame_x: f64,
        frame_y: f64,
        frame_width: f64,
        frame_height: f64,
        // Floating windows are excluded from tiling, so an overlay renderer can skip them (the old
        // borders blacklisted floating apps). Lets the renderer match that without a round-trip.
        is_floating: bool,
        space_id: SpaceId,
        display_uuid: Option<String>,
    },
}

pub type BroadcastSender = crate::actor::Sender<BroadcastEvent>;
pub type BroadcastReceiver = crate::actor::Receiver<BroadcastEvent>;
