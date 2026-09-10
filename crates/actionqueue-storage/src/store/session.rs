//! Store sessions own the OS lock for the full writer or offline reader lifetime.
use super::{StoreError, StoreManifest};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Debug, Clone)]
pub enum OpenOptions {
    /// Initialize only absent/empty directories; otherwise open an existing store.
    Initialize {
        features: Vec<String>,
    },
    ReadWrite,
    ReadOnly,
}
#[derive(Debug)]
struct SessionInner {
    root: PathBuf,
    manifest: StoreManifest,
    _lock: File,
    writable: bool,
    writer_claimed: AtomicBool,
}
#[derive(Debug, Clone)]
pub struct StoreSession(Arc<SessionInner>);
impl StoreSession {
    pub fn root(&self) -> &Path {
        &self.0.root
    }
    pub fn manifest(&self) -> &StoreManifest {
        &self.0.manifest
    }
    pub fn wal_path(&self) -> PathBuf {
        self.root().join("wal/actionqueue.wal")
    }
    pub fn snapshot_path(&self) -> PathBuf {
        self.root().join("snapshots/snapshot.bin")
    }
    pub fn require_write(&self) -> Result<(), StoreError> {
        if self.0.writable {
            Ok(())
        } else {
            Err(StoreError::InvalidStore("read-only session".into()))
        }
    }
    pub(crate) fn claim_writer(&self) -> Result<(), StoreError> {
        self.require_write()?;
        self.0
            .writer_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StoreError::StoreInUse)?;
        Ok(())
    }
    pub(crate) fn release_writer(&self) {
        self.0.writer_claimed.store(false, Ordering::Release);
    }
    pub fn into_authority(
        self,
    ) -> Result<
        crate::mutation::authority::StorageMutationAuthority<
            crate::wal::fs_writer::WalFsWriter,
            crate::recovery::reducer::ReplayReducer,
        >,
        StoreError,
    > {
        let recovered = crate::recovery::bootstrap::recover_read_only(
            &self,
            crate::wal::repair::RepairPolicy::Strict,
        )?;
        let writer = crate::wal::fs_writer::WalFsWriter::new(self)
            .map_err(|e| StoreError::InvalidStore(e.to_string()))?;
        Ok(crate::mutation::authority::StorageMutationAuthority::new(writer, recovered.projection))
    }
}
/// Reject symlinks in all existing path components before opening storage files.
pub(crate) fn reject_symlinks(path: &Path) -> Result<(), StoreError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(StoreError::InvalidStore(format!("symlink: {}", current.display())))
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
pub(crate) fn sync_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut f = fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
pub(crate) fn is_empty_or_absent(root: &Path) -> Result<bool, StoreError> {
    reject_symlinks(root)?;
    match fs::read_dir(root) {
        Ok(mut entries) => Ok(entries.next().transpose()?.is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}
/// A sibling staging directory is never treated as evidence about a destination.
pub(crate) struct Staging(pub PathBuf);
impl Staging {
    pub fn new(dest: &Path) -> Result<Self, StoreError> {
        let parent = dest.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        reject_symlinks(parent)?;
        fs::create_dir_all(parent)?;
        let path = parent.join(format!(".aq-stage-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    pub fn publish(&self, dest: &Path) -> Result<(), StoreError> {
        if !is_empty_or_absent(dest)? {
            return Err(StoreError::InvalidStore("destination is populated".into()));
        }
        sync_dir(&self.0)?;
        fs::rename(&self.0, dest)?; // Directory rename cannot replace a populated winner.
        super::fault::checkpoint("publish_before_parent_sync")?;
        sync_dir(dest.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))
    }
}
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn initialize(root: &Path, features: Vec<String>) -> Result<(), StoreError> {
    let manifest = StoreManifest::new(features)?;
    let staging = Staging::new(root)?;
    fs::create_dir(staging.0.join("wal"))?;
    fs::create_dir(staging.0.join("snapshots"))?;
    write_synced(
        &staging.0.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest)
            .map_err(|e| StoreError::InvalidManifest(e.to_string()))?,
    )?;
    write_synced(&staging.0.join("store.lock"), &[])?;
    let event = crate::wal::event::WalEvent::new(
        1,
        crate::wal::event::WalEventType::StoreInitialized { manifest_digest: manifest.digest() },
    );
    let bytes = crate::wal::codec::encode_for_store(&event, manifest.store_id)
        .map_err(|e| StoreError::InvalidStore(e.to_string()))?;
    write_synced(&staging.0.join("wal/actionqueue.wal"), &bytes)?;
    sync_dir(&staging.0.join("wal"))?;
    sync_dir(&staging.0.join("snapshots"))?;
    super::fault::checkpoint("initialize_before_publish")?;
    match staging.publish(root) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Initialization losers validate the winner, never overwrite it. Our own
            // published identity means rename succeeded but parent sync failed: keep
            // that durability error visible instead of treating ourselves as a loser.
            if root.join("manifest.json").exists() {
                let winner = StoreManifest::read(root)?;
                if winner.store_id == manifest.store_id {
                    Err(e)
                } else {
                    Ok(())
                }
            } else {
                Err(e)
            }
        }
    }
}
pub fn open_store(root: &Path, options: OpenOptions) -> Result<StoreSession, StoreError> {
    reject_symlinks(root)?;
    if let OpenOptions::Initialize { features } = &options {
        if is_empty_or_absent(root)? {
            initialize(root, features.clone())?;
        }
    }
    // No lock file or writable descriptor is opened before compatibility validation.
    let manifest = StoreManifest::read(root)?;
    for relative in
        ["store.lock", "wal", "wal/actionqueue.wal", "snapshots", "snapshots/snapshot.bin"]
    {
        reject_symlinks(&root.join(relative))?;
    }
    for relative in ["store.lock", "wal/actionqueue.wal"] {
        if !fs::metadata(root.join(relative))?.is_file() {
            return Err(StoreError::InvalidStore(format!("missing regular {relative}")));
        }
    }
    let lock = File::open(root.join("store.lock"))?;
    let writable = !matches!(options, OpenOptions::ReadOnly);
    let result = if writable { lock.try_lock() } else { lock.try_lock_shared() };
    result.map_err(|e| match e {
        std::fs::TryLockError::WouldBlock => StoreError::StoreInUse,
        std::fs::TryLockError::Error(e) => e.into(),
    })?;
    if StoreManifest::read(root)? != manifest {
        return Err(StoreError::InvalidManifest("identity changed during opening".into()));
    }
    Ok(StoreSession(Arc::new(SessionInner {
        root: root.canonicalize()?,
        manifest,
        _lock: lock,
        writable,
        writer_claimed: AtomicBool::new(false),
    })))
}
