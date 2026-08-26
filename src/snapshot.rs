use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub type Generation = u64;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Entry {
    File {
        digest: String,
        size: u64,
        modified_ns: i64,
        executable: bool,
    },
    Symlink {
        target: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub generation: Generation,
    #[serde(default)]
    pub state_id: String,
    pub entries: BTreeMap<PathBuf, Entry>,
}

impl Default for Snapshot {
    fn default() -> Self {
        let entries = BTreeMap::new();
        Self {
            generation: 0,
            state_id: state_id(&entries).expect("empty snapshot state is serializable"),
            entries,
        }
    }
}

impl Snapshot {
    pub fn normalize(mut self) -> Result<Self> {
        if self.state_id.is_empty() {
            self.state_id = state_id(&self.entries)?;
        }
        Ok(self)
    }
}

impl Entry {
    pub fn from_path(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("read metadata for {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            let target =
                fs::read_link(path).with_context(|| format!("read symlink {}", path.display()))?;
            let target = target
                .to_str()
                .context("symlink target is not valid UTF-8")?
                .to_owned();
            return Ok(Self::Symlink { target });
        }
        if !metadata.is_file() {
            bail!("unsupported file type: {}", path.display());
        }
        let mut file = fs::File::open(path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(Self::File {
            digest: hasher.finalize().to_hex().to_string(),
            size: metadata.size(),
            modified_ns: metadata
                .mtime()
                .saturating_mul(1_000_000_000)
                .saturating_add(metadata.mtime_nsec()),
            executable: metadata.permissions().mode() & 0o111 != 0,
        })
    }

    pub fn needs_payload(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    pub fn payload_matches(&self, actual: &Self) -> bool {
        matches!(
            (self, actual),
            (
                Self::File { digest, .. },
                Self::File {
                    digest: actual_digest,
                    ..
                }
            ) if digest == actual_digest
        )
    }

    pub fn content_matches(&self, actual: &Self) -> bool {
        match (self, actual) {
            (
                Self::File {
                    digest, executable, ..
                },
                Self::File {
                    digest: actual_digest,
                    executable: actual_executable,
                    ..
                },
            ) => digest == actual_digest && executable == actual_executable,
            (
                Self::Symlink { target },
                Self::Symlink {
                    target: actual_target,
                },
            ) => target == actual_target,
            _ => false,
        }
    }
}

pub fn state_id(entries: &BTreeMap<PathBuf, Entry>) -> Result<String> {
    validate_entries(entries)?;
    Ok(blake3::hash(&serde_json::to_vec(entries)?)
        .to_hex()
        .to_string())
}

pub fn delta_state_id(
    previous: &str,
    generation: Generation,
    changes: &BTreeMap<PathBuf, Option<Entry>>,
) -> Result<String> {
    validate_paths(changes.keys())?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"devsync-delta-state-v1\0");
    hasher.update(previous.as_bytes());
    hasher.update(&generation.to_be_bytes());
    hasher.update(&serde_json::to_vec(changes)?);
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.to_str().is_none()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().is_empty()
        })
    {
        bail!("unsafe relative path: {}", path.display());
    }
    Ok(())
}

pub fn validate_paths<'a>(paths: impl IntoIterator<Item = &'a PathBuf>) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for path in paths {
        validate_relative_path(path)?;
        let mut ancestor = path.parent();
        while let Some(parent) = ancestor {
            if seen.contains(parent) {
                bail!(
                    "managed paths collide: {} is an ancestor of {}",
                    parent.display(),
                    path.display()
                );
            }
            ancestor = parent.parent();
        }
        seen.insert(path.clone());
    }
    Ok(())
}

pub fn validate_entries(entries: &BTreeMap<PathBuf, Entry>) -> Result<()> {
    validate_paths(entries.keys())
}

pub fn scan_paths(
    root: &Path,
    paths: impl IntoIterator<Item = PathBuf>,
) -> Result<BTreeMap<PathBuf, Entry>> {
    let entries = paths
        .into_iter()
        .map(|path| {
            validate_relative_path(&path)?;
            let entry = Entry::from_path(&root.join(&path))?;
            Ok((path, entry))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    validate_entries(&entries)?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_paths() {
        for path in ["", "/tmp/x", "../x", "a/../x"] {
            assert!(validate_relative_path(Path::new(path)).is_err(), "{path}");
        }
        assert!(validate_relative_path(Path::new("a/b")).is_ok());
    }

    #[test]
    fn rejects_ancestor_descendant_entries() {
        let entries = BTreeMap::from([
            (
                PathBuf::from("a"),
                Entry::Symlink {
                    target: "target".into(),
                },
            ),
            (
                PathBuf::from("a/b"),
                Entry::Symlink {
                    target: "target".into(),
                },
            ),
        ]);
        assert!(validate_entries(&entries).is_err());
    }

    #[test]
    fn hashes_file_content_and_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"hello").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        let entry = Entry::from_path(&path).unwrap();
        assert_eq!(
            entry,
            Entry::File {
                digest: blake3::hash(b"hello").to_hex().to_string(),
                size: 5,
                modified_ns: fs::metadata(&path)
                    .unwrap()
                    .mtime()
                    .saturating_mul(1_000_000_000)
                    .saturating_add(fs::metadata(&path).unwrap().mtime_nsec()),
                executable: true,
            }
        );
    }
}
