# Lib-test triage (`test-triage`)

## Final counts

| Stage | Result |
| --- | --- |
| Baseline (`596894e`, compile-only repair) | **434 passed, 34 failed** (BRIEF said ~439/34; same 34 failures) |
| After harness + product + stale cleanup (`fc0df67`) | **462 passed, 8 failed** (470 tests; 3 stale tests deleted) |

Build/test command used throughout: `~/.cargo/bin/cargo test --lib`.

## Root causes

### 1. Harness (highest leverage) — fixed in `fe64b8f`

Synthetic windows were registered and “manageable” but never entered layout / never got resize requests.

1. **`refresh_visible_windows_snapshot()`** called live `get_visible_windows_with_layer`. In lib tests that returned **real desktop CG windows** and wiped the synthetic visible set just applied from `ApplicationLaunched`’s `window_server_info`.
2. **`ApplicationLaunched`** passed `known_visible: Vec::new()` while windows lived only in `new`. `emit_layout_events` tiles from visible WSIDs or `known_visible`, so nothing was emitted.

On a real machine the product still tiles because live CG refresh includes the app’s real windows. In tests, the snapshot must not fall through to CoreGraphics.

**Fix:** test-only visible-window override (default empty; never live CG); `Apps::make_app` registers synthetic WS infos; `ApplicationLaunched` passes launched window ids as `known_visible`.

Follow-up harness tweak in `fc0df67`: `make_app_with_opts` preserves an explicit `sys_id: None`; `animated_layout_handles_windows_without_server_ids` resets `frame_monotonic` after launch so animate still has a delta (launch now correctly tiles to fullscreen).

### 2. Product — restore adoption (`16a65a9`)

`LayoutEngine::rewrite_window_id` rewrote layout trees but did **not** call `WindowStore::transfer_persistent_window_metadata`. After id rewrite, workspace membership stayed on the old id → restore-time adoption left windows unmapped in `WindowStore`.

### 3. Product — prune zombies (`f9c37a3`)

`LayoutEngine::prune_window` removed layout leaves but did **not** call `virtual_workspace_manager.remove_window` / clear `WindowStore` membership → zombie assignments after prune. Also folded `has_window_in_layout` into `testing.rs` next to `has_windows_in_layout`, and made `it_ignores_windows_on_nonzero_layers` set `layer = 3` for real (it previously relied on the harness bug via `with_ws_info: false`).

### 4. Stale expectations — deleted in `fc0df67`

Fork-local `HiddenWindowPlacement` prefers edges / top corners (see module docs). Three tests asserted literal `BottomRight` corner coords and were deleted with comments pointing at the surviving placement tests.

## Genuine product bugs (fixed)

1. **`rewrite_window_id` did not transfer WindowStore workspace membership**  
   Scenario: restore/adoption rewrites saved `WindowId` → live id; trees update, but `WindowStore` still maps the old id, so workspace queries / placement look like the window was never adopted.

2. **`prune_window` did not clear WindowStore membership**  
   Scenario: a window is pruned from the layout engine; layout no longer contains it, but workspace assignment / floating metadata can remain → zombies after prune / rekey paths.

## Genuine product bugs / open failures (left failing)

Not fixed within the time box; categorized below. These still fail after the harness correctly tiles windows.

1. **Main-window / selection** (`it_tracks_frontmost_app_and_main_window_correctly`, `it_does_not_update_layout_for_quiet_raises`) — after activating app 2 and setting its main window, `selected_window(space)` stays on app 1 (or later quiet/non-quiet transitions disagree with the test). Looks like focus-selection policy drift vs the upstream test script.

2. **Display reconnect persistence** (`reconnecting_a_display_restores_its_saved_arrangement`) — asserts two-display layout state is retained while a display is unplugged; currently “two-display state is gone while the display is unplugged”.

3. **Mission Control space moves** (two tests) — a window moved to another native space during MC remains in (or is re-added to) the origin layout when refresh uses `known_visible` / exit refresh.

4. **Cross-display tiled move** (`moving_tiled_window_to_display_applies_destination_layout_after_transfer_frame`) — sees the transfer frame write but not the expected follow-up tiled layout writes.

5. **Passive command-space vs click focus** (`passive_command_space_change_does_not_override_clicked_window_focus`) — after passive display snapshot + AX focus, selection is `1,1` instead of clicked `1,2`.

6. **Orphan reap of minimized windows of “dead” apps** (`reconcile_reaps_windows_of_dead_apps_even_when_minimized`, user-authored) — uses pid 1 (launchd: alive as a process, not an NSRunningApplication). Minimized window state is not removed by `reconcile_orphan_windows`. Sibling `reconcile_reaps_layout_only_windows_of_dead_apps` passes.

## Commits on this branch

| Commit | What |
| --- | --- |
| `fe64b8f` | harness: visible-window override + `known_visible` on launch |
| `16a65a9` | product: `rewrite_window_id` transfers WindowStore metadata |
| `f9c37a3` | product: `prune_window` clears membership; fold helpers; nonzero-layer test |
| `fc0df67` | delete stale corner-hide tests; preserve `sys_id: None`; animate delta |

## Table of the original 34

| Test | Category | Action |
| --- | --- | --- |
| `actor::reactor::tests::it_manages_windows_on_enabled_spaces` | harness | fixed (`fe64b8f`) |
| `actor::reactor::tests::it_ignores_stale_resize_events` | harness | fixed |
| `actor::reactor::tests::it_keeps_discovered_windows_on_their_initial_screen` | harness | fixed |
| `actor::reactor::tests::it_preserves_layout_after_login_screen` | harness | fixed |
| `actor::reactor::tests::login_screen_refresh_preserves_manual_workspace_assignment` | harness | fixed |
| `actor::reactor::tests::discovery_minimize_transition_removes_window_from_layout` | harness | fixed |
| `actor::reactor::tests::discovery_manageability_loss_removes_window_from_layout` | harness | fixed |
| `actor::reactor::tests::discovery_does_not_replay_another_apps_global_main_window` | harness | fixed |
| `actor::reactor::tests::display_churn_end_refresh_is_idempotent_without_topology_change` | harness | fixed |
| `actor::reactor::tests::authoritative_active_window_snapshot_removes_missing_window_from_active_layout` | harness | fixed |
| `actor::reactor::tests::auto_workspace_switch_focuses_activated_window_not_stale_workspace_focus` | harness | fixed |
| `actor::reactor::tests::windows_discovered_does_not_reintroduce_inactive_workspace_window` | harness | fixed |
| `actor::reactor::tests::workspace_query_uses_authoritative_assignment_after_move` | harness | fixed |
| `actor::reactor::tests::workspace_switch_batches_all_windows_with_eui_enabled` | harness | fixed |
| `actor::reactor::tests::wsid_rekey_preserves_floating_membership_and_position` | harness (+ prune product) | fixed |
| `actor::reactor::persistence::tests::exact_match_beats_fuzzy_candidate` | harness | fixed |
| `actor::reactor::persistence::tests::fuzzy_adopts_matching_attributes_when_server_id_changed` | harness | fixed |
| `actor::reactor::persistence::tests::fuzzy_tiebreak_prefers_closest_frame` | harness | fixed |
| `actor::reactor::persistence::tests::try_adopt_window_rewrites_matched_ids_and_falls_through_otherwise` | harness + product (`rewrite_window_id`) | fixed |
| `actor::reactor::persistence::tests::prune_app_adoptions_evicts_windows_the_app_did_not_bring_back` | harness | fixed |
| `actor::reactor::persistence::tests::prune_app_adoptions_keys_on_bundle_after_reboot` | harness | fixed |
| `actor::reactor::persistence::tests::settle_timeout_still_evicts_windows_whose_app_never_returned` | harness | fixed |
| `layout_engine::engine::tests::rewrite_window_id_preserves_tiled_placement` | **product bug** | fixed (`16a65a9`) |
| `model::hidden_window_placement::tests::anchors_to_requested_corner` | stale expectation | deleted (`fc0df67`) |
| `model::virtual_workspace::tests::hidden_position_uses_corner_anchor_while_hiding_offscreen` | stale expectation | deleted (`fc0df67`) |
| `model::virtual_workspace::tests::hidden_position_flips_sides_to_avoid_neighboring_monitor_overlap` | stale expectation | deleted (`fc0df67`) |
| `actor::reactor::main_window::tests::it_tracks_frontmost_app_and_main_window_correctly` | product (focus/selection) | left failing |
| `actor::reactor::main_window::tests::it_does_not_update_layout_for_quiet_raises` | product (quiet raise / selection) | left failing |
| `actor::reactor::persistence::tests::reconnecting_a_display_restores_its_saved_arrangement` | product (offline display state) | left failing |
| `actor::reactor::tests::mission_control_exit_refresh_drops_windows_missing_from_origin_space_snapshot` | product (MC space move) | left failing |
| `actor::reactor::tests::mission_control_refresh_known_visible_fallback_does_not_restore_window_moved_to_other_space` | product (MC `known_visible`) | left failing |
| `actor::reactor::tests::moving_tiled_window_to_display_applies_destination_layout_after_transfer_frame` | product (cross-display tile) | left failing |
| `actor::reactor::tests::passive_command_space_change_does_not_override_clicked_window_focus` | product (focus vs passive snapshot) | left failing |
| `actor::reactor::tests::reconcile_reaps_windows_of_dead_apps_even_when_minimized` | product (orphan reap + minimized) | left failing |

## Deliberately left alone

- The **8 remaining failures** above: past the harness time box; each needs a focused product investigation, not another harness tweak. Assertions were not weakened.
- **`BRIEF.md`**: left untracked (task brief, not product code).
- No push, install, or codesign.

## Note on a near-regression

`animated_layout_handles_windows_without_server_ids` was **passing** on the broken harness (windows never tiled, so animate always had a delta) and **failed** once launch correctly tiled. Fixed by preserving `sys_id: None` and resetting the start frame — not by weakening the animate assertion.
