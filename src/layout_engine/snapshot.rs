//! On-disk snapshot schema for layout persistence.
//!
//! The snapshot pairs the already-serializable [`LayoutEngine`] with two sidecar
//! identity maps that translate rift's ephemeral runtime ids into durable
//! identities, keyed per display arrangement. Phase 0 only defines and hardens
//! this schema (roundtrip + stability + atomic write); the assembly of the
//! sidecar maps and the load/adopt pipeline land in later phases. The format is
//! fingerprint-keyed from day one so those phases don't need a migration.
//!
//! Loading never panics: a missing file, a parse error, or a version mismatch
//! all yield a fresh (empty) snapshot — worst case is today's behavior.

use std::io::Write;
use std::path::Path;

use objc2_core_foundation::CGRect;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tempfile::NamedTempFile;
use tracing::warn;

use crate::actor::app::WindowId;
use crate::common::collections::HashMap;
use crate::layout_engine::LayoutEngine;
use crate::sys::geometry::CGRectDef;
use crate::sys::screen::SpaceId;
use crate::sys::window_server::WindowServerId;

/// Current on-disk schema version. Bump only on a breaking change to the format;
/// [`Snapshot::load_or_default`] discards any file whose version differs.
pub const SNAPSHOT_VERSION: u32 = 1;

/// Durable identity for a window whose runtime [`WindowId`] is session-scoped.
///
/// `server_id` matches exactly while the window lives (crash/restart tier); the
/// `bundle_id`/`title`/`ax_role`/`frame` tuple is the fuzzy fallback used after a
/// reboot when server ids have changed. Assembly and matching are later phases;
/// Phase 0 only fixes the wire shape.
#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct WindowIdentity {
    pub server_id: WindowServerId,
    pub bundle_id: String,
    pub title: String,
    pub ax_role: String,
    #[serde_as(as = "CGRectDef")]
    pub frame: CGRect,
}

/// One saved arrangement: the engine plus the sidecars needed to re-adopt its
/// spaces and windows into a fresh session.
#[derive(Serialize, Deserialize)]
pub struct Arrangement {
    /// The engine's own serde output: trees, workspaces, names, floating,
    /// window->workspace mapping.
    pub engine: LayoutEngine,
    /// Durable space identity: live `SpaceId` -> `(display_uuid, ordinal)`.
    pub spaces: HashMap<SpaceId, (String, u32)>,
    /// Durable window identity, keyed by the (ephemeral) `WindowId` in the engine.
    pub windows: HashMap<WindowId, WindowIdentity>,
    /// Windows pinned above the focused window, to be re-applied on adoption.
    pub topmost: Vec<WindowId>,
}

/// The whole persisted file: a set of arrangements keyed by display fingerprint
/// (sorted display UUIDs joined). Phase 0/1 read and write only the current
/// fingerprint; the map exists now so later phases add keys without a migration.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub arrangements: HashMap<String, Arrangement>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Snapshot { version: SNAPSHOT_VERSION, arrangements: HashMap::default() }
    }
}

impl Snapshot {
    /// Read and parse a snapshot, falling back to a fresh empty one on any
    /// failure (missing file, parse error, or version mismatch). Never panics.
    pub fn load_or_default(path: &Path) -> Snapshot {
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("failed to read snapshot at {path:?}: {e}; starting fresh");
                }
                return Snapshot::default();
            }
        };
        match ron::from_str::<Snapshot>(&contents) {
            Ok(snapshot) if snapshot.version == SNAPSHOT_VERSION => snapshot,
            Ok(snapshot) => {
                warn!(
                    "snapshot at {path:?} has version {} (expected {SNAPSHOT_VERSION}); starting fresh",
                    snapshot.version
                );
                Snapshot::default()
            }
            Err(e) => {
                warn!("failed to parse snapshot at {path:?}: {e}; starting fresh");
                Snapshot::default()
            }
        }
    }

    /// Serialize and atomically write this snapshot to `path`.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let contents = ron::ser::to_string(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_atomic(path, &contents)
    }
}

/// Write `contents` to `path` atomically: write a sibling temp file, then rename
/// it over the target. A crash mid-write leaves the previous file intact, so a
/// partially written snapshot is impossible by construction. Shared by
/// [`Snapshot::save`] and [`LayoutEngine::save`](crate::layout_engine::LayoutEngine::save).
pub fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(dir) = dir {
        std::fs::create_dir_all(dir)?;
    }
    // Keep the temp file on the same filesystem as the target so persist() is a
    // pure rename (atomic), not a cross-device copy.
    let mut tmp = match dir {
        Some(dir) => NamedTempFile::new_in(dir)?,
        None => NamedTempFile::new()?,
    };
    tmp.write_all(contents.as_bytes())?;
    tmp.flush()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use objc2_core_foundation::{CGPoint, CGSize};

    use super::*;

    fn sample_identity() -> WindowIdentity {
        WindowIdentity {
            server_id: WindowServerId::new(42),
            bundle_id: "com.example.app".to_string(),
            title: "Untitled".to_string(),
            ax_role: "AXWindow".to_string(),
            frame: CGRect::new(CGPoint::new(1.0, 2.0), CGSize::new(300.0, 400.0)),
        }
    }

    #[test]
    fn write_atomic_roundtrips_through_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("layout.ron");
        write_atomic(&path, "hello world").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello world");

        // A second write replaces the file atomically.
        write_atomic(&path, "second").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second");
    }

    #[test]
    fn window_identity_roundtrips_via_ron() {
        let identity = sample_identity();
        let serialized = ron::ser::to_string(&identity).unwrap();
        let restored: WindowIdentity = ron::from_str(&serialized).unwrap();
        assert_eq!(identity, restored);
    }

    #[test]
    fn empty_snapshot_roundtrips_via_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        let snapshot = Snapshot::default();
        snapshot.save(&path).unwrap();

        let loaded = Snapshot::load_or_default(&path);
        assert_eq!(loaded.version, SNAPSHOT_VERSION);
        assert!(loaded.arrangements.is_empty());
    }

    #[test]
    fn missing_file_loads_fresh_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.ron");
        let loaded = Snapshot::load_or_default(&path);
        assert_eq!(loaded.version, SNAPSHOT_VERSION);
        assert!(loaded.arrangements.is_empty());
    }

    #[test]
    fn garbage_file_recovers_to_fresh_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        std::fs::write(&path, "this is not valid ron {{{").unwrap();
        let loaded = Snapshot::load_or_default(&path);
        assert_eq!(loaded.version, SNAPSHOT_VERSION);
        assert!(loaded.arrangements.is_empty());
    }

    #[test]
    fn version_mismatch_discards_and_loads_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.ron");
        // A syntactically valid snapshot from a hypothetical future version.
        std::fs::write(&path, "(version:999,arrangements:{})").unwrap();
        let loaded = Snapshot::load_or_default(&path);
        assert_eq!(loaded.version, SNAPSHOT_VERSION);
        assert!(loaded.arrangements.is_empty());
    }

    #[test]
    fn snapshot_schema_stability() {
        // Catches accidental serde renames on upstream rebases: the top-level
        // wire keys and the nested WindowIdentity keys must stay put.
        let serialized = ron::ser::to_string(&Snapshot::default()).unwrap();
        for key in ["version", "arrangements"] {
            assert!(serialized.contains(key), "snapshot missing wire key {key:?}: {serialized}");
        }
        let identity = ron::ser::to_string(&sample_identity()).unwrap();
        for key in ["server_id", "bundle_id", "title", "ax_role", "frame"] {
            assert!(
                identity.contains(key),
                "WindowIdentity missing wire key {key:?}: {identity}"
            );
        }
    }
}
