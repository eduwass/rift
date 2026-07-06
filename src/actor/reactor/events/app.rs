<<<<<<< HEAD
use tracing::debug;
||||||| parent of 26e5e60 (fix: log app registration, thread termination, and watch failures)
use tracing::{debug, warn};
=======
use tracing::{debug, info, warn};
>>>>>>> 26e5e60 (fix: log app registration, thread termination, and watch failures)

use crate::actor::app::{AppInfo, AppThreadHandle, Quiet, WindowId};
use crate::actor::reactor::AppState;
use crate::actor::reactor::events::{EventOutcome, WindowDiscoveryRequest};
use crate::actor::reactor::managers::AppManager;
use crate::layout_engine::LayoutEvent;
use crate::sys::app::WindowInfo;
use crate::sys::window_server::WindowServerInfo;

#[derive(Debug)]
pub struct ApplicationLaunchedPayload {
    pub pid: i32,
    pub info: AppInfo,
    pub handle: AppThreadHandle,
    pub visible_windows: Vec<(WindowId, WindowInfo)>,
    pub window_server_info: Vec<WindowServerInfo>,
}

<<<<<<< HEAD
pub fn handle_application_launched(
    apps: &mut AppManager,
    payload: ApplicationLaunchedPayload,
) -> anyhow::Result<EventOutcome> {
    let ApplicationLaunchedPayload {
        pid,
        info,
        handle,
        visible_windows,
        window_server_info,
    } = payload;
    apps.apps.insert(pid, AppState { info: info.clone(), handle });
    Ok(EventOutcome::finalized_event(None, false, false, true)
        .with_window_server_updates(window_server_info)
        .with_discovery(WindowDiscoveryRequest {
            pid,
            new: visible_windows,
            known_visible: Vec::new(),
            app_info: Some(info),
        }))
}

pub fn handle_application_terminated(pid: i32) -> anyhow::Result<EventOutcome> {
    Ok(EventOutcome::finalized_event(None, false, false, true)
        .with_app_request(pid, crate::actor::app::Request::Terminate))
}

pub fn handle_application_thread_terminated(
    apps: &mut AppManager,
    pid: i32,
) -> anyhow::Result<EventOutcome> {
    apps.apps.remove(&pid);
    Ok(EventOutcome::finalized_event(None, false, false, true)
        .with_layout_event(LayoutEvent::AppClosed(pid)))
}

#[derive(Debug, Clone, Copy)]
pub struct ApplicationActivatedPayload {
    pub pid: i32,
    pub quiet: Quiet,
}

pub fn handle_application_activated(
    payload: ApplicationActivatedPayload,
) -> anyhow::Result<EventOutcome> {
    let ApplicationActivatedPayload { pid, quiet } = payload;
    if quiet == Quiet::Yes {
        debug!(
            pid,
            "Skipping auto workspace switch for quiet app activation (initiated by Rift)"
        );
        return Ok(EventOutcome::finalized_event(None, false, false, false));
||||||| parent of 26e5e60 (fix: log app registration, thread termination, and watch failures)
impl AppEventHandler {
    pub fn handle_application_launched(
        reactor: &mut Reactor,
        pid: i32,
        info: AppInfo,
        handle: AppThreadHandle,
        visible_windows: Vec<(WindowId, WindowInfo)>,
        window_server_info: Vec<WindowServerInfo>,
        _is_frontmost: bool,
        _main_window: Option<WindowId>,
    ) {
        reactor.app_manager.apps.insert(pid, AppState { info: info.clone(), handle });
        reactor.update_partial_window_server_info(window_server_info);
        reactor.on_windows_discovered_with_app_info(pid, visible_windows, vec![], Some(info));
=======
impl AppEventHandler {
    pub fn handle_application_launched(
        reactor: &mut Reactor,
        pid: i32,
        info: AppInfo,
        handle: AppThreadHandle,
        visible_windows: Vec<(WindowId, WindowInfo)>,
        window_server_info: Vec<WindowServerInfo>,
        _is_frontmost: bool,
        _main_window: Option<WindowId>,
    ) {
        info!(
            pid,
            bundle_id = ?info.bundle_id,
            windows = visible_windows.len(),
            "application registered"
        );
        reactor.app_manager.apps.insert(pid, AppState { info: info.clone(), handle });
        reactor.update_partial_window_server_info(window_server_info);
        reactor.on_windows_discovered_with_app_info(pid, visible_windows, vec![], Some(info));
>>>>>>> 26e5e60 (fix: log app registration, thread termination, and watch failures)
    }

    Ok(EventOutcome::finalized_event(None, false, false, false).with_application_activation(pid))
}

<<<<<<< HEAD
#[derive(Debug)]
pub struct WindowsDiscoveredPayload {
    pub pid: i32,
    pub new: Vec<(WindowId, WindowInfo)>,
    pub known_visible: Vec<WindowId>,
}
||||||| parent of 26e5e60 (fix: log app registration, thread termination, and watch failures)
    pub fn handle_application_thread_terminated(reactor: &mut Reactor, pid: i32) {
        reactor.app_manager.apps.remove(&pid);
        reactor.send_layout_event(LayoutEvent::AppClosed(pid));
    }
=======
    pub fn handle_application_thread_terminated(reactor: &mut Reactor, pid: i32) {
        warn!(pid, "app thread terminated; removing app and relayouting");
        reactor.app_manager.apps.remove(&pid);
        reactor.send_layout_event(LayoutEvent::AppClosed(pid));
    }
>>>>>>> 26e5e60 (fix: log app registration, thread termination, and watch failures)

pub fn handle_windows_discovered(
    payload: WindowsDiscoveredPayload,
) -> anyhow::Result<EventOutcome> {
    let WindowsDiscoveredPayload { pid, new, known_visible } = payload;
    Ok(
        EventOutcome::finalized_event(None, false, false, true).with_discovery(
            WindowDiscoveryRequest {
                pid,
                new,
                known_visible,
                app_info: None,
            },
        ),
    )
}
