use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use tar::{Builder, Header};

use crate::project::Config;

pub const RECONCILE_PATHS: [&str; 2] = [".dev-syncignore", ".git/info/exclude"];
const REMOTE_MANIFEST: &str = ".dev-sync-manifest";
const NEW_MANIFEST: &str = ".dev-sync-manifest.new";

pub fn manifest(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut command = Command::new("git");
    command.current_dir(root).args([
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    ]);
    if root.join(".dev-syncignore").is_file() {
        command.arg("--exclude-from=.dev-syncignore");
    }
    let output = command.output().context("build Git file manifest")?;
    if !output.status.success() {
        bail!(
            "git ls-files failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }
    let custom_ignored = custom_ignored_tracked(root)?;
    let mut paths = BTreeSet::new();
    for raw in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|raw| !raw.is_empty())
    {
        let path =
            PathBuf::from(std::str::from_utf8(raw).context("Git returned a non-UTF-8 path")?);
        validate_relative_path(&path)?;
        if root.join(&path).symlink_metadata().is_ok()
            && !custom_ignored.contains(&path)
            && path != Path::new(".dev-sync.toml")
            && path != Path::new(REMOTE_MANIFEST)
            && path != Path::new(NEW_MANIFEST)
        {
            paths.insert(path);
        }
    }
    Ok(paths)
}

fn custom_ignored_tracked(root: &Path) -> Result<BTreeSet<PathBuf>> {
    if !root.join(".dev-syncignore").is_file() {
        return Ok(BTreeSet::new());
    }
    let output = Command::new("git")
        .current_dir(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--ignored",
            "--exclude-from=.dev-syncignore",
        ])
        .output()
        .context("apply .dev-syncignore to tracked files")?;
    if !output.status.success() {
        bail!(
            "git custom-ignore query failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|raw| !raw.is_empty())
        .map(|raw| {
            Ok(PathBuf::from(
                std::str::from_utf8(raw).context("Git returned a non-UTF-8 path")?,
            ))
        })
        .collect()
}

pub fn reconcile(root: &Path, config: &Config, files: &BTreeSet<PathBuf>) -> Result<usize> {
    let remote = shell_quote(&config.remote_path);
    let script = format!(
        "set -eu; root={remote}; mkdir -p \"$root\"; tar -xf - -C \"$root\"; old=\"$root/{REMOTE_MANIFEST}\"; new=\"$root/{NEW_MANIFEST}\"; if [ -f \"$old\" ]; then comm -23 \"$old\" \"$new\" | while IFS= read -r path; do rm -rf \"$root/$path\"; done; fi; mv \"$new\" \"$old\""
    );
    transfer(root, config, files.iter(), files, &script)?;
    Ok(files.len())
}

pub fn apply_delta(
    root: &Path,
    config: &Config,
    changed: &BTreeSet<PathBuf>,
    previous: &BTreeSet<PathBuf>,
    current: &BTreeSet<PathBuf>,
) -> Result<usize> {
    let uploads: BTreeSet<_> = changed.intersection(current).cloned().collect();
    let deletions: Vec<_> = changed
        .intersection(previous)
        .filter(|path| !current.contains(*path))
        .collect();
    if uploads.is_empty() && deletions.is_empty() {
        return Ok(0);
    }

    let remote = shell_quote(&config.remote_path);
    let deletes = deletions
        .iter()
        .map(|path| format!("rm -rf {}/{};", remote, shell_quote_path(path)))
        .collect::<String>();
    let script = format!(
        "set -eu; root={remote}; mkdir -p \"$root\"; tar -xf - -C \"$root\"; {deletes} mv \"$root/{NEW_MANIFEST}\" \"$root/{REMOTE_MANIFEST}\""
    );
    transfer(root, config, uploads.iter(), current, &script)?;
    Ok(uploads.len() + deletions.len())
}

pub fn needs_reconcile(paths: &BTreeSet<PathBuf>) -> bool {
    paths.iter().any(|path| {
        path.file_name().is_some_and(|name| name == ".gitignore")
            || RECONCILE_PATHS
                .iter()
                .any(|required| path == Path::new(required))
    })
}

fn transfer<'a>(
    root: &Path,
    config: &Config,
    uploads: impl Iterator<Item = &'a PathBuf>,
    current: &BTreeSet<PathBuf>,
    script: &str,
) -> Result<()> {
    let mut child = Command::new("ssh")
        .args(["-o", "ControlMaster=auto", "-o", "ControlPersist=10m"])
        .arg(&config.remote)
        .arg("sh")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::piped())
        .spawn()
        .context("start ssh")?;
    let write_result = {
        let stdin = child.stdin.take().context("open ssh stdin")?;
        write_archive(root, uploads, current, stdin)
    };
    let status = child.wait().context("wait for ssh")?;
    write_result?;
    if !status.success() {
        bail!("remote apply failed with {status}")
    }
    Ok(())
}

fn write_archive<'a>(
    root: &Path,
    uploads: impl Iterator<Item = &'a PathBuf>,
    current: &BTreeSet<PathBuf>,
    writer: impl io::Write,
) -> Result<()> {
    let mut archive = Builder::new(writer);
    archive.follow_symlinks(false);
    for path in uploads {
        archive
            .append_path_with_name(root.join(path), path)
            .with_context(|| format!("archive {}", path.display()))?;
    }
    let manifest = current
        .iter()
        .map(|path| path.to_string_lossy())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut header = Header::new_gnu();
    header.set_size(manifest.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    archive.append_data(&mut header, NEW_MANIFEST, manifest.as_bytes())?;
    archive.finish()?;
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path.to_string_lossy().contains('\n')
        || path.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        })
    {
        bail!("unsupported manifest path: {}", path.display())
    }
    Ok(())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn shell_quote_path(path: &Path) -> String {
    shell_quote(&path.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_shell_values() {
        assert_eq!(shell_quote("/tmp/a b'c"), "'/tmp/a b'\\''c'");
    }

    #[test]
    fn detects_reconcile_inputs() {
        assert!(needs_reconcile(&BTreeSet::from([PathBuf::from(
            ".dev-syncignore"
        )])));
        assert!(!needs_reconcile(&BTreeSet::from([PathBuf::from(
            "src/main.rs"
        )])));
    }
}
