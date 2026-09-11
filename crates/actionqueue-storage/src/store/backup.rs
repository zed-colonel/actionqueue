//! Verified offline inspection, backup, and restore. Sources are never repaired.
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{open_store, session::*, OpenOptions, StoreError, StoreManifest, StoreSession};
use crate::{
    recovery::{
        bootstrap::{recover, recover_read_only},
        projection::ProjectionDigest,
    },
    wal::repair::RepairPolicy,
};
#[derive(Debug, Serialize)]
pub struct StoreInspection {
    pub manifest: StoreManifest,
    pub supported_features: Vec<String>,
    pub tail_health: String,
    pub sequence: u64,
    pub task_count: usize,
    pub run_count: usize,
    pub projection_digest: ProjectionDigest,
}
fn invalid(s: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(s.to_string())
}
fn inspect_session(
    session: &StoreSession,
    policy: RepairPolicy,
) -> Result<StoreInspection, StoreError> {
    let recovered = recover_read_only(session, policy)?;
    Ok(StoreInspection {
        manifest: session.manifest().clone(),
        supported_features: super::capabilities(),
        tail_health: if recovered.incomplete_tail.is_some() {
            "incomplete_final_frame"
        } else {
            "clean"
        }
        .into(),
        sequence: recovered.projection.latest_sequence(),
        task_count: recovered.projection.task_count(),
        run_count: recovered.projection.run_count(),
        projection_digest: recovered.projection.projection_digest()?,
    })
}
pub fn inspect_store(root: &Path) -> Result<StoreInspection, StoreError> {
    inspect_session(&open_store(root, OpenOptions::ReadOnly)?, RepairPolicy::TruncatePartial)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupFile {
    pub path: String,
    pub length: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupDescriptor {
    pub version: u32,
    pub store_id: uuid::Uuid,
    pub sequence: u64,
    pub projection_digest: ProjectionDigest,
    pub files: Vec<BackupFile>,
}
fn open_regular_file(path: &Path) -> Result<File, StoreError> {
    reject_symlinks(path)?;
    // Opening a FIFO for reading can block indefinitely. Check the path before
    // opening, and retain the descriptor check in case the entry was replaced.
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(invalid("backup input requires regular files"));
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("backup input requires regular files"));
    }
    Ok(file)
}
fn file_hash(path: &Path) -> Result<(u64, String), StoreError> {
    let mut file = open_regular_file(path)?;
    let mut sha = Sha256::new();
    let mut len = 0;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        sha.update(&buffer[..n]);
        len += n as u64;
    }
    Ok((len, format!("{:x}", sha.finalize())))
}
fn copy_file(source: &Path, dest: &Path) -> Result<(), StoreError> {
    let mut from = open_regular_file(source)?;
    let mut to = fs::OpenOptions::new().write(true).create_new(true).open(dest)?;
    std::io::copy(&mut from, &mut to)?;
    to.sync_all()?;
    Ok(())
}
fn normalized(path: &Path) -> Result<PathBuf, StoreError> {
    reject_symlinks(path)?;
    let abs = std::path::absolute(path)?;
    let mut normalized = PathBuf::new();
    for c in abs.components() {
        match c {
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::CurDir => {}
            _ => normalized.push(c),
        }
    }
    Ok(normalized)
}
fn check_destination(source: &Path, dest: &Path) -> Result<(), StoreError> {
    let source = normalized(source)?;
    let destination = normalized(dest)?;
    if source.starts_with(&destination) || destination.starts_with(&source) {
        return Err(invalid("source and destination overlap"));
    }
    if !is_empty_or_absent(dest)? {
        return Err(invalid("destination is populated"));
    }
    Ok(())
}
fn make_dirs(root: &Path) -> Result<(), StoreError> {
    fs::create_dir(root.join("wal"))?;
    fs::create_dir(root.join("snapshots"))?;
    Ok(())
}
fn sync_store(root: &Path) -> Result<(), StoreError> {
    sync_dir(&root.join("wal"))?;
    sync_dir(&root.join("snapshots"))?;
    sync_dir(root)
}
fn verify_projection(root: &Path, descriptor: &BackupDescriptor) -> Result<(), StoreError> {
    let session = open_store(root, OpenOptions::ReadOnly)?;
    if session.manifest().store_id != descriptor.store_id {
        return Err(invalid("backup identity mismatch"));
    }
    let recovered = recover_read_only(&session, RepairPolicy::Strict)?;
    // A staged snapshot must be valid, not merely eligible for WAL fallback.
    if root.join("snapshots/snapshot.bin").exists() && !recovered.snapshot_loaded {
        return Err(invalid("backup snapshot is damaged"));
    }
    let wal_only = recover(&session, RepairPolicy::Strict, false)?;
    let digest = recovered.projection.projection_digest()?;
    if recovered.projection.latest_sequence() != descriptor.sequence
        || digest != descriptor.projection_digest
        || wal_only.projection.projection_digest()? != digest
    {
        return Err(invalid("backup projection or sequence mismatch"));
    }
    Ok(())
}
pub fn backup_store(source: &Path, output: &Path) -> Result<BackupDescriptor, StoreError> {
    check_destination(source, output)?;
    let session = open_store(source, OpenOptions::ReadOnly)?;
    let recovered = recover_read_only(&session, RepairPolicy::Strict)?;
    let staging = Staging::new(output)?;
    make_dirs(&staging.0)?;
    let mut names = vec!["manifest.json", "store.lock", "wal/actionqueue.wal"];
    if recovered.snapshot_loaded {
        names.push("snapshots/snapshot.bin");
    }
    names.sort();
    let mut files = Vec::new();
    for name in names {
        copy_file(&source.join(name), &staging.0.join(name))?;
        let (length, sha256) = file_hash(&staging.0.join(name))?;
        files.push(BackupFile { path: name.into(), length, sha256 });
    }
    let descriptor = BackupDescriptor {
        version: 1,
        store_id: session.manifest().store_id,
        sequence: recovered.projection.latest_sequence(),
        projection_digest: recovered.projection.projection_digest()?,
        files,
    };
    verify_projection(&staging.0, &descriptor)?;
    write_synced(
        &staging.0.join("backup.json"),
        &serde_json::to_vec_pretty(&descriptor).map_err(invalid)?,
    )?;
    sync_store(&staging.0)?;
    staging.publish(output)?;
    Ok(descriptor)
}
fn inventory(root: &Path, relative: &Path, files: &mut BTreeSet<String>) -> Result<(), StoreError> {
    for entry in fs::read_dir(root.join(relative))? {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let name = path.to_str().ok_or_else(|| invalid("non-UTF8 inventory path"))?.to_owned();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            if name != "wal" && name != "snapshots" {
                return Err(invalid("unexpected backup directory"));
            }
            inventory(root, &path, files)?;
        } else if kind.is_file() {
            files.insert(name);
        } else {
            return Err(invalid("symlink or special backup entry"));
        }
    }
    Ok(())
}
fn read_descriptor(input: &Path) -> Result<BackupDescriptor, StoreError> {
    let f = open_regular_file(&input.join("backup.json"))?;
    if f.metadata()?.len() > 64 * 1024 {
        return Err(invalid("invalid backup descriptor size/type"));
    }
    let mut bytes = Vec::new();
    f.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err(invalid("oversized backup descriptor"));
    }
    let descriptor: BackupDescriptor = serde_json::from_slice(&bytes).map_err(invalid)?;
    if descriptor.version != 1 {
        return Err(StoreError::UnsupportedStoreFormat {
            component: "backup".into(),
            supported: 1,
            found: descriptor.version,
        });
    }
    let manifest = StoreManifest::read(input)?; // Before any WAL or snapshot decode.
    if manifest.store_id != descriptor.store_id {
        return Err(invalid("backup manifest identity mismatch"));
    }
    let mut expected = BTreeSet::from(["backup.json".to_owned()]);
    for file in &descriptor.files {
        if !["manifest.json", "store.lock", "wal/actionqueue.wal", "snapshots/snapshot.bin"]
            .contains(&file.path.as_str())
            || !expected.insert(file.path.clone())
        {
            return Err(invalid("invalid/duplicate inventory path"));
        }
        let (len, hash) = file_hash(&input.join(&file.path))?;
        if len != file.length || hash != file.sha256 {
            return Err(invalid(format!("backup checksum/length mismatch: {}", file.path)));
        }
    }
    for name in ["manifest.json", "store.lock", "wal/actionqueue.wal"] {
        if !expected.contains(name) {
            return Err(invalid("incomplete backup inventory"));
        }
    }
    let mut actual = BTreeSet::new();
    inventory(input, Path::new(""), &mut actual)?;
    if actual != expected {
        return Err(invalid("unexpected backup files"));
    }
    Ok(descriptor)
}
pub fn restore_store(input: &Path, dest: &Path) -> Result<BackupDescriptor, StoreError> {
    check_destination(input, dest)?;
    let descriptor = read_descriptor(input)?;
    let _source_session = open_store(input, OpenOptions::ReadOnly)?;
    verify_projection(input, &descriptor)?;
    let staging = Staging::new(dest)?;
    make_dirs(&staging.0)?;
    for file in &descriptor.files {
        copy_file(&input.join(&file.path), &staging.0.join(&file.path))?;
        let (len, hash) = file_hash(&staging.0.join(&file.path))?;
        if len != file.length || hash != file.sha256 {
            return Err(invalid("backup changed during restore"));
        }
    }
    verify_projection(&staging.0, &descriptor)?;
    sync_store(&staging.0)?;
    super::fault::checkpoint("restore_before_publish")?;
    staging.publish(dest)?;
    Ok(descriptor)
}
