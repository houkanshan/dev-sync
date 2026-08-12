use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use tar::{Builder, Header};

use crate::project::Config;

pub const RECONCILE_PATHS: [&str; 2] = [".devsyncignore", ".git/info/exclude"];
const REMOTE_MANIFEST: &str = ".devsync-manifest";
const NEW_MANIFEST: &str = ".devsync-manifest.new";
const UPLOAD_MANIFEST: &str = ".devsync-uploads";

pub fn manifest(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut command = Command::new("git");
    command.current_dir(root).args([
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    ]);
    if root.join(".devsyncignore").is_file() {
        command.arg("--exclude-from=.devsyncignore");
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
            && path != Path::new(".devsync.toml")
            && !is_reserved(&path)
        {
            paths.insert(path);
        }
    }
    Ok(paths)
}

fn custom_ignored_tracked(root: &Path) -> Result<BTreeSet<PathBuf>> {
    if !root.join(".devsyncignore").is_file() {
        return Ok(BTreeSet::new());
    }
    let output = Command::new("git")
        .current_dir(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--ignored",
            "--exclude-from=.devsyncignore",
        ])
        .output()
        .context("apply .devsyncignore to tracked files")?;
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
    transfer(root, config, files.iter(), files)?;
    Ok(files.len())
}

pub fn delta_paths(
    changed: &BTreeSet<PathBuf>,
    previous: &BTreeSet<PathBuf>,
    current: &BTreeSet<PathBuf>,
) -> BTreeSet<PathBuf> {
    changed
        .iter()
        .filter(|path| current.contains(*path) || previous.contains(*path))
        .cloned()
        .collect()
}

pub fn apply_delta(
    root: &Path,
    config: &Config,
    changed: &BTreeSet<PathBuf>,
    previous: &BTreeSet<PathBuf>,
    current: &BTreeSet<PathBuf>,
) -> Result<usize> {
    let affected = delta_paths(changed, previous, current);
    let uploads: BTreeSet<_> = affected.intersection(current).cloned().collect();
    if affected.is_empty() {
        return Ok(0);
    }
    let count = affected.len();
    transfer(root, config, uploads.iter(), current)?;
    Ok(count)
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
) -> Result<()> {
    let uploads: Vec<_> = uploads.collect();
    let script = remote_apply_script(&config.remote_path);
    let remote_command = format!("sh -c {}", shell_quote(&script));
    let mut child = Command::new("ssh")
        .args(["-o", "ControlMaster=auto", "-o", "ControlPersist=10m"])
        .arg(&config.remote)
        .arg(remote_command)
        .stdin(Stdio::piped())
        .spawn()
        .context("start ssh")?;
    let write_result = {
        let stdin = child.stdin.take().context("open ssh stdin")?;
        write_archive(root, uploads.iter().copied(), current, stdin)
    };
    let status = child.wait().context("wait for ssh")?;
    write_result?;
    if !status.success() {
        bail!("remote apply failed with {status}")
    }
    Ok(())
}

fn remote_apply_script(remote_path: &str) -> String {
    let remote = shell_quote(remote_path);
    format!(
        "set -eu
root={remote}
parent=$(dirname \"$root\")
mkdir -p \"$root\" \"$parent\"
stage=$(mktemp -d \"$parent/.devsync.XXXXXX\")
trap 'rm -rf \"$stage\"' EXIT HUP INT TERM
tar -xf - -C \"$stage\"
safe_remove() {{
  path=$1
  [ -n \"$path\" ] || return 0
  case \"$path\" in /*|../*|*/../*|*/..) echo \"unsafe remote path: $path\" >&2; exit 1;; esac
  rm -rf \"$root/$path\"
}}
if [ -f \"$root/{REMOTE_MANIFEST}\" ]; then
  comm -23 \"$root/{REMOTE_MANIFEST}\" \"$stage/{NEW_MANIFEST}\" | while IFS= read -r path; do safe_remove \"$path\"; done
fi
while IFS= read -r path; do
  [ -n \"$path\" ] || continue
  safe_remove \"$path\"
done < \"$stage/{UPLOAD_MANIFEST}\"
tar -cf - -C \"$stage\" --exclude=\"{NEW_MANIFEST}\" --exclude=\"{UPLOAD_MANIFEST}\" . | tar -xf - -C \"$root\"
mv \"$stage/{NEW_MANIFEST}\" \"$root/{REMOTE_MANIFEST}\"
rm -rf \"$stage\"
trap - EXIT HUP INT TERM"
    )
}

fn write_archive<'a>(
    root: &Path,
    uploads: impl Iterator<Item = &'a PathBuf>,
    current: &BTreeSet<PathBuf>,
    writer: impl io::Write,
) -> Result<()> {
    let uploads: Vec<_> = uploads.collect();
    let mut archive = Builder::new(writer);
    archive.follow_symlinks(false);
    for path in &uploads {
        archive
            .append_path_with_name(root.join(path), path)
            .with_context(|| format!("archive {}", path.display()))?;
    }
    append_control_file(&mut archive, NEW_MANIFEST, &manifest_bytes(current))?;
    let upload_set = uploads.into_iter().cloned().collect();
    append_control_file(&mut archive, UPLOAD_MANIFEST, &manifest_bytes(&upload_set))?;
    archive.finish()?;
    Ok(())
}

fn append_control_file(
    archive: &mut Builder<impl io::Write>,
    name: &str,
    content: &[u8],
) -> Result<()> {
    let mut header = Header::new_gnu();
    header.set_size(content.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    archive.append_data(&mut header, name, content)?;
    Ok(())
}

fn manifest_bytes(paths: &BTreeSet<PathBuf>) -> Vec<u8> {
    if paths.is_empty() {
        return Vec::new();
    }
    (paths
        .iter()
        .map(|path| path.to_string_lossy())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n")
        .into_bytes()
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
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

fn is_reserved(path: &Path) -> bool {
    [REMOTE_MANIFEST, NEW_MANIFEST, UPLOAD_MANIFEST]
        .iter()
        .any(|reserved| path == Path::new(reserved))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_shell_values() {
        assert_eq!(shell_quote("/tmp/a b'c"), "'/tmp/a b'\\''c'");
    }

    #[test]
    fn empty_manifest_is_zero_bytes() {
        assert!(manifest_bytes(&BTreeSet::new()).is_empty());
    }

    #[test]
    fn non_empty_manifest_terminates_every_record() {
        let paths = BTreeSet::from([PathBuf::from("a"), PathBuf::from("b")]);
        assert_eq!(manifest_bytes(&paths), b"a\nb\n");
    }

    #[test]
    fn remote_command_is_one_shell_quoted_argument() {
        let script = remote_apply_script("/tmp/a b'c");
        let command = format!("sh -c {}", shell_quote(&script));
        assert!(command.starts_with("sh -c 'set -eu"));
        assert!(command.contains("root='\\''/tmp/a b'\\''\\'\\'''\\''c'\\''"));
    }

    #[test]
    fn delta_paths_exclude_ineligible_watchman_events() {
        let changed = BTreeSet::from([
            PathBuf::from(".git"),
            PathBuf::from(".git/index.lock"),
            PathBuf::from("src/main.rs"),
            PathBuf::from("deleted.rs"),
        ]);
        let previous = BTreeSet::from([PathBuf::from("src/main.rs"), PathBuf::from("deleted.rs")]);
        let current = BTreeSet::from([PathBuf::from("src/main.rs")]);
        assert_eq!(
            delta_paths(&changed, &previous, &current),
            BTreeSet::from([PathBuf::from("deleted.rs"), PathBuf::from("src/main.rs")])
        );
    }

    #[test]
    fn detects_reconcile_inputs() {
        assert!(needs_reconcile(&BTreeSet::from([PathBuf::from(
            "nested/.gitignore"
        )])));
        assert!(!needs_reconcile(&BTreeSet::from([PathBuf::from(
            "src/main.rs"
        )])));
    }
}
