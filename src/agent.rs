use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::protocol::{AgentMessage, ClientMessage, Plan, PlanKind, read_json, write_json};
use crate::snapshot::{Entry, Snapshot, validate_relative_path};

pub fn serve<R: Read, W: Write>(
    root: &Path,
    state_path: &Path,
    mut input: R,
    mut output: W,
) -> Result<()> {
    let result = serve_session(root, state_path, &mut input, &mut output);
    if let Err(error) = &result {
        let _ = write_json(
            &mut output,
            &AgentMessage::Error {
                message: format!("{error:#}"),
            },
        );
    }
    result
}

fn serve_session<R: Read, W: Write>(
    root: &Path,
    state_path: &Path,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    fs::create_dir_all(root).with_context(|| format!("create target root {}", root.display()))?;
    let snapshot = load_snapshot(state_path)?;
    let Some(ClientMessage::Plan(plan)) = read_json(input)? else {
        bail!("expected plan as first protocol message");
    };
    let desired = desired_snapshot(&snapshot, &plan)?;
    let needed = needed_payloads(root, &desired)?;
    write_json(
        &mut *output,
        &AgentMessage::NeedPayloads {
            paths: needed.iter().cloned().collect(),
        },
    )?;

    let stage = tempfile::Builder::new()
        .prefix("devsync-stage-")
        .tempdir_in(root.parent().context("target root has no parent")?)
        .context("create staging directory")?;
    for expected in &needed {
        let Some(ClientMessage::Payload { path, length }) = read_json(input)? else {
            bail!("expected payload for {}", expected.display());
        };
        if path != *expected {
            bail!(
                "expected payload for {}, received {}",
                expected.display(),
                path.display()
            );
        }
        receive_payload(
            &mut *input,
            stage.path(),
            &path,
            length,
            &desired.entries[&path],
        )?;
    }

    apply(
        root,
        stage.path(),
        &snapshot,
        &desired,
        matches!(plan.kind, PlanKind::Full { .. }),
    )?;
    save_snapshot(state_path, &desired)?;
    write_json(
        output,
        &AgentMessage::Ack {
            generation: desired.generation,
        },
    )?;
    Ok(())
}

fn desired_snapshot(previous: &Snapshot, plan: &Plan) -> Result<Snapshot> {
    if plan.expected_generation != previous.generation {
        bail!(
            "generation mismatch: expected {}, remote is {}",
            plan.expected_generation,
            previous.generation
        );
    }
    if plan.generation <= previous.generation {
        bail!("new generation must be greater than remote generation");
    }
    let mut entries = match &plan.kind {
        PlanKind::Full { entries } => entries.clone(),
        PlanKind::Delta { changes } => {
            let mut entries = previous.entries.clone();
            for (path, entry) in changes {
                match entry {
                    Some(entry) => {
                        entries.insert(path.clone(), entry.clone());
                    }
                    None => {
                        entries.remove(path);
                    }
                }
            }
            entries
        }
    };
    for path in entries.keys() {
        validate_relative_path(path)?;
    }
    if let PlanKind::Delta { changes } = &plan.kind {
        for path in changes.keys() {
            validate_relative_path(path)?;
        }
    }
    Ok(Snapshot {
        generation: plan.generation,
        entries: std::mem::take(&mut entries),
    })
}

fn needed_payloads(root: &Path, desired: &Snapshot) -> Result<BTreeSet<PathBuf>> {
    let mut needed = BTreeSet::new();
    for (path, entry) in &desired.entries {
        if !entry.needs_payload() {
            continue;
        }
        let actual = match Entry::from_path(&root.join(path)) {
            Ok(actual) => Some(actual),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                None
            }
            Err(_) => None,
        };
        if !actual.is_some_and(|actual| entry.content_matches(&actual)) {
            needed.insert(path.clone());
        }
    }
    Ok(needed)
}

fn receive_payload<R: Read>(
    reader: &mut R,
    stage: &Path,
    path: &Path,
    length: u64,
    expected: &Entry,
) -> Result<()> {
    let Entry::File { digest, size, .. } = expected else {
        bail!("received payload for non-file {}", path.display());
    };
    if length != *size {
        bail!("payload size mismatch for {}", path.display());
    }
    let destination = stage.join(path);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(&destination)?;
    let mut limited = reader.take(length);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    let mut received = 0;
    loop {
        let count = limited.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        file.write_all(&buffer[..count])?;
        hasher.update(&buffer[..count]);
        received += count as u64;
    }
    file.sync_all()?;
    if received != length {
        bail!("short payload for {}", path.display());
    }
    if hasher.finalize().to_hex().as_str() != digest {
        bail!("payload digest mismatch for {}", path.display());
    }
    Ok(())
}

fn apply(
    root: &Path,
    stage: &Path,
    previous: &Snapshot,
    desired: &Snapshot,
    full: bool,
) -> Result<()> {
    let mut removals: Vec<_> = if full {
        actual_paths(root)?
            .into_iter()
            .filter(|path| !desired.entries.contains_key(path))
            .collect()
    } else {
        previous
            .entries
            .keys()
            .filter(|path| !desired.entries.contains_key(*path))
            .cloned()
            .collect()
    };
    removals.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for path in removals {
        remove_any(&root.join(path))?;
    }

    for (path, entry) in &desired.entries {
        let destination = root.join(path);
        ensure_safe_parent(root, path)?;
        match entry {
            Entry::File { executable, .. } => {
                let staged = stage.join(path);
                if staged.exists() {
                    if fs::symlink_metadata(&destination).is_ok_and(|metadata| {
                        metadata.is_dir() && !metadata.file_type().is_symlink()
                    }) {
                        remove_any(&destination)?;
                    }
                    fs::rename(&staged, &destination)
                        .with_context(|| format!("install {}", path.display()))?;
                }
                let mode = if *executable { 0o755 } else { 0o644 };
                fs::set_permissions(&destination, fs::Permissions::from_mode(mode))?;
            }
            Entry::Symlink { target } => {
                let actual = Entry::from_path(&destination).ok();
                if !actual.is_some_and(|actual| entry.content_matches(&actual)) {
                    remove_any(&destination)?;
                    symlink(target, &destination)?;
                }
            }
        }
    }
    Ok(())
}

fn ensure_safe_parent(root: &Path, relative: &Path) -> Result<()> {
    let mut current = root.to_path_buf();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    remove_any(&current)?;
                    fs::create_dir(&current)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&current)?
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

fn remove_any(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path)?
        }
        Ok(_) => fs::remove_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn actual_paths(root: &Path) -> Result<BTreeSet<PathBuf>> {
    fn visit(root: &Path, relative: &Path, paths: &mut BTreeSet<PathBuf>) -> Result<()> {
        for child in fs::read_dir(root.join(relative))? {
            let child = child?;
            let path = relative.join(child.file_name());
            let metadata = fs::symlink_metadata(child.path())?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                visit(root, &path, paths)?;
            } else {
                validate_relative_path(&path)?;
                paths.insert(path);
            }
        }
        Ok(())
    }

    let mut paths = BTreeSet::new();
    visit(root, Path::new(""), &mut paths)?;
    Ok(paths)
}

fn load_snapshot(path: &Path) -> Result<Snapshot> {
    match fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).context("parse agent snapshot")?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Snapshot::default()),
        Err(error) => Err(error.into()),
    }
}

fn save_snapshot(path: &Path, snapshot: &Snapshot) -> Result<()> {
    let parent = path.parent().context("state path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.new",
        path.file_name()
            .context("state path has no file name")?
            .to_string_lossy()
    ));
    let file = fs::File::create(&temporary)?;
    serde_json::to_writer(&file, snapshot)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::protocol::{AgentMessage, ClientMessage, PlanKind};
    use std::collections::BTreeMap;

    fn entry(bytes: &[u8], executable: bool) -> Entry {
        Entry::File {
            digest: blake3::hash(bytes).to_hex().to_string(),
            size: bytes.len() as u64,
            executable,
            modified_ns: 0,
        }
    }

    #[test]
    fn full_then_delta_repairs_remote_drift_and_commits_acknowledged_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state/snapshot.json");
        fs::create_dir_all(state.parent().unwrap()).unwrap();

        let first = Plan {
            expected_generation: 0,
            generation: 1,
            kind: PlanKind::Full {
                entries: BTreeMap::from([
                    (PathBuf::from("bin/run"), entry(b"one", true)),
                    (
                        PathBuf::from("link"),
                        Entry::Symlink {
                            target: "bin/run".into(),
                        },
                    ),
                ]),
            },
        };
        transact(&root, &state, first, &[(PathBuf::from("bin/run"), b"one")]);
        assert_eq!(fs::read(root.join("bin/run")).unwrap(), b"one");
        assert_eq!(
            fs::read_link(root.join("link")).unwrap(),
            Path::new("bin/run")
        );
        assert_ne!(
            fs::metadata(root.join("bin/run"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );

        fs::write(root.join("bin/run"), b"remote drift").unwrap();
        let second = Plan {
            expected_generation: 1,
            generation: 2,
            kind: PlanKind::Delta {
                changes: BTreeMap::from([
                    (PathBuf::from("bin/run"), Some(entry(b"two", false))),
                    (PathBuf::from("link"), None),
                ]),
            },
        };
        transact(&root, &state, second, &[(PathBuf::from("bin/run"), b"two")]);
        assert_eq!(fs::read(root.join("bin/run")).unwrap(), b"two");
        assert!(!root.join("link").exists());
        assert_eq!(load_snapshot(&state).unwrap().generation, 2);
    }

    #[test]
    fn rejects_stale_generation_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state.json");
        save_snapshot(
            &state,
            &Snapshot {
                generation: 4,
                entries: BTreeMap::new(),
            },
        )
        .unwrap();
        let plan = ClientMessage::Plan(Plan {
            expected_generation: 3,
            generation: 5,
            kind: PlanKind::Full {
                entries: BTreeMap::new(),
            },
        });
        let mut input = Vec::new();
        write_json(&mut input, &plan).unwrap();
        assert!(serve(&root, &state, input.as_slice(), Vec::new()).is_err());
        assert_eq!(load_snapshot(&state).unwrap().generation, 4);
    }

    fn transact(root: &Path, state: &Path, plan: Plan, payloads: &[(PathBuf, &[u8])]) {
        let mut input = Vec::new();
        write_json(&mut input, &ClientMessage::Plan(plan)).unwrap();
        for (path, bytes) in payloads {
            write_json(
                &mut input,
                &ClientMessage::Payload {
                    path: path.clone(),
                    length: bytes.len() as u64,
                },
            )
            .unwrap();
            input.extend_from_slice(bytes);
        }
        let mut output = Vec::new();
        serve(root, state, Cursor::new(input), &mut output).unwrap();
        let mut output = output.as_slice();
        assert!(matches!(
            read_json::<_, AgentMessage>(&mut output).unwrap(),
            Some(AgentMessage::NeedPayloads { .. })
        ));
        assert!(matches!(
            read_json::<_, AgentMessage>(&mut output).unwrap(),
            Some(AgentMessage::Ack { .. })
        ));
    }
}
