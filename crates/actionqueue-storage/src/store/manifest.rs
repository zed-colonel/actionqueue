//! The immutable AQ-CONT-1 identity and compatibility boundary.
use std::{fs::File, io::Read, path::Path};

use serde::{Deserialize, Serialize};

use super::StoreError;

pub const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HashAlgorithms {
    pub projection: String,
    pub backup: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreManifest {
    pub manifest_schema: u32,
    pub contract: String,
    pub wal_format: u32,
    pub snapshot_schema: u32,
    pub projection_version: u32,
    pub store_id: uuid::Uuid,
    pub created_at: u64,
    pub created_by: String,
    pub features: Vec<String>,
    pub hash_algorithms: HashAlgorithms,
}
/// Capabilities of this binary, independent of a store's enabled profile.
pub fn capabilities() -> Vec<String> {
    let mut features = Vec::new();
    for (name, enabled) in [
        ("actor", cfg!(feature = "actor")),
        ("budget", cfg!(feature = "budget")),
        ("platform", cfg!(feature = "platform")),
        ("workflow", cfg!(feature = "workflow")),
    ] {
        if enabled {
            features.push(name.to_owned());
        }
    }
    features
}
impl StoreManifest {
    pub fn new(features: Vec<String>) -> Result<Self, StoreError> {
        let manifest = Self {
            manifest_schema: 1,
            contract: "AQ-CONT-1".into(),
            wal_format: 1,
            snapshot_schema: 7,
            projection_version: 7,
            store_id: uuid::Uuid::new_v4(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| StoreError::InvalidManifest(e.to_string()))?
                .as_secs(),
            created_by: concat!("actionqueue-storage/", env!("CARGO_PKG_VERSION")).into(),
            features,
            hash_algorithms: HashAlgorithms {
                projection: "sha256".into(),
                backup: "sha256".into(),
            },
        };
        manifest.validate()?;
        Ok(manifest)
    }
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.contract != "AQ-CONT-1"
            || self.store_id.is_nil()
            || self.created_by.is_empty()
            || self.created_by.len() > 256
        {
            return Err(StoreError::InvalidManifest(
                "invalid contract, identity, or creator".into(),
            ));
        }
        for (component, found, supported) in [
            ("manifest_schema", self.manifest_schema, 1),
            ("wal_format", self.wal_format, 1),
            ("snapshot_schema", self.snapshot_schema, 7),
            ("projection_version", self.projection_version, 7),
        ] {
            if found != supported {
                return Err(StoreError::UnsupportedStoreFormat {
                    component: component.into(),
                    supported,
                    found,
                });
            }
        }
        if self.hash_algorithms.projection != "sha256" || self.hash_algorithms.backup != "sha256" {
            return Err(StoreError::InvalidManifest("unsupported hash algorithm".into()));
        }
        if self.features.windows(2).any(|w| w[0] >= w[1]) {
            return Err(StoreError::InvalidManifest(
                "feature profile must be sorted and unique".into(),
            ));
        }
        let supported = capabilities();
        let unsupported: Vec<_> =
            self.features.iter().filter(|x| !supported.contains(x)).cloned().collect();
        if !unsupported.is_empty() {
            return Err(StoreError::UnsupportedFeatures(unsupported));
        }
        if self.features.iter().any(|x| x == "platform")
            && !self.features.iter().any(|x| x == "actor")
        {
            return Err(StoreError::InvalidManifest("platform requires actor".into()));
        }
        Ok(())
    }
    pub fn read(root: &Path) -> Result<Self, StoreError> {
        let path = root.join("manifest.json");
        let meta = std::fs::symlink_metadata(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StoreError::MissingTargetManifest
            } else {
                e.into()
            }
        })?;
        if !meta.is_file() || meta.len() > MAX_MANIFEST_BYTES {
            return Err(StoreError::InvalidManifest(
                "manifest must be a bounded regular file".into(),
            ));
        }
        let mut bytes = Vec::new();
        File::open(path)?.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(StoreError::InvalidManifest("manifest too large".into()));
        }
        let manifest: Self = serde_json::from_slice(&bytes)
            .map_err(|e| StoreError::InvalidManifest(e.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        // Fixed struct order, strict fields and sorted profile make this binding stable.
        Sha256::digest(serde_json::to_vec(self).expect("manifest serialization")).into()
    }
}
