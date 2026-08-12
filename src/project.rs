use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub remote: String,
    pub remote_path: String,
}

#[derive(Clone, Debug)]
pub struct Project {
    pub root: PathBuf,
    pub config_path: PathBuf,
    pub socket_path: PathBuf,
    pub log_path: PathBuf,
    pub snapshot_path: PathBuf,
}

impl Project {
    pub fn discover() -> Result<Self> {
        let output = Command::new("git")
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .context("run git; devsync requires Git")?;
        if !output.status.success() {
            bail!("current directory is not inside a Git worktree")
        }
        let root = PathBuf::from(String::from_utf8(output.stdout)?.trim()).canonicalize()?;
        Ok(Self::from_root(root))
    }

    pub fn from_root(root: PathBuf) -> Self {
        let id = stable_id(&root);
        let runtime = std::env::temp_dir().join("devsync");
        Self {
            config_path: root.join(".devsync.toml"),
            root,
            socket_path: runtime.join(format!("{id}.sock")),
            log_path: runtime.join(format!("{id}.log")),
            snapshot_path: runtime.join(format!("{id}.snapshot.json")),
        }
    }

    pub fn load_config(&self) -> Result<Config> {
        let raw = std::fs::read_to_string(&self.config_path).with_context(|| {
            format!(
                "read {}; copy .devsync.example.toml",
                self.config_path.display()
            )
        })?;
        let config: Config = toml::from_str(&raw)
            .with_context(|| format!("parse {}", self.config_path.display()))?;
        if config.remote.trim().is_empty() || config.remote.starts_with('-') {
            bail!("remote must be a non-empty SSH destination and must not start with '-'")
        }
        if !config.remote_path.starts_with('/') || config.remote_path == "/" {
            bail!("remote_path must be an absolute path other than /")
        }
        Ok(config)
    }
}

pub fn load_snapshot(path: &Path) -> Result<devsync::snapshot::Snapshot> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).context("parse local snapshot")?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(devsync::snapshot::Snapshot::default())
        }
        Err(error) => Err(error.into()),
    }
}

pub fn save_snapshot(path: &Path, snapshot: &devsync::snapshot::Snapshot) -> Result<()> {
    let parent = path.parent().context("snapshot path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.new",
        path.file_name()
            .context("snapshot path has no file name")?
            .to_string_lossy()
    ));
    let file = std::fs::File::create(&temporary)?;
    serde_json::to_writer(&file, snapshot)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn stable_id(path: &Path) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in path.as_os_str().as_encoded_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_id_is_stable_and_path_specific() {
        assert_eq!(stable_id(Path::new("/a")), stable_id(Path::new("/a")));
        assert_ne!(stable_id(Path::new("/a")), stable_id(Path::new("/b")));
    }
}
