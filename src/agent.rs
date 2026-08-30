use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::protocol::{
    AgentMessage, ClientMessage, PROTOCOL_VERSION, Plan, PlanKind, read_json, write_json,
};
use crate::snapshot::{
    Entry, Snapshot, delta_state_id, state_id, validate_entries, validate_paths,
};

#[derive(Deserialize, Serialize)]
struct JournalRecord {
    paths: Vec<PathBuf>,
}

struct StateLock(fs::File);

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

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
    let _state_lock = acquire_state_lock(state_path)?;
    let mut snapshot = load_snapshot(state_path)?;
    let mut recovery_paths = load_journal(state_path)?;
    let mut recovery_required = !recovery_paths.is_empty();
    let Some(ClientMessage::Hello { version }) = read_json(input)? else {
        bail!("expected hello as first protocol message");
    };
    if version != PROTOCOL_VERSION {
        bail!("unsupported protocol version {version}; expected {PROTOCOL_VERSION}");
    }
    write_json(
        &mut *output,
        &AgentMessage::Hello {
            version: PROTOCOL_VERSION,
            generation: snapshot.generation,
            state_id: snapshot.state_id.clone(),
            recovery_required,
        },
    )?;

    loop {
        match read_json(input)? {
            Some(ClientMessage::Plan(plan)) => {
                let reconciled = transact_plan(
                    root,
                    state_path,
                    input,
                    output,
                    &mut snapshot,
                    &mut recovery_paths,
                    recovery_required,
                    plan,
                )?;
                recovery_required &= !reconciled;
            }
            Some(ClientMessage::Complete) | None => {
                if !recovery_required {
                    checkpoint(state_path, &snapshot, &mut recovery_paths)?;
                }
                return Ok(());
            }
            Some(message) => bail!("expected plan or completion, received {message:?}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn transact_plan<R: Read, W: Write>(
    root: &Path,
    state_path: &Path,
    input: &mut R,
    output: &mut W,
    snapshot: &mut Snapshot,
    recovery_paths: &mut BTreeSet<PathBuf>,
    recovery_required: bool,
    plan: Plan,
) -> Result<bool> {
    validate_plan(snapshot, recovery_required, &plan)?;
    let (needed, mut changed, observed_unchanged) = match &plan.kind {
        PlanKind::Full { entries } => {
            let removals = full_removals(snapshot, recovery_paths, entries);
            let mut diff = full_diff(root, entries)?;
            for (path, entry) in entries {
                if overlaps_any(path, &removals) {
                    diff.changed.insert(path.clone());
                    if entry.needs_payload() {
                        diff.needed.insert(path.clone());
                    }
                }
            }
            write_json(
                &mut *output,
                &AgentMessage::NeedPayloads {
                    paths: diff.needed.iter().cloned().collect(),
                },
            )?;
            (diff.needed, diff.changed, diff.observed_unchanged)
        }
        PlanKind::Delta { changes } => (
            changes
                .iter()
                .filter(|(_, entry)| matches!(entry, Some(Entry::File { .. })))
                .map(|(path, _)| path.clone())
                .collect(),
            changes.keys().cloned().collect(),
            BTreeMap::new(),
        ),
    };

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
            plan.entry(&path)
                .context("requested payload is absent from plan")?,
        )?;
    }
    let Some(ClientMessage::Done) = read_json(input)? else {
        bail!("expected payload completion message");
    };

    if let PlanKind::Full { entries } = &plan.kind {
        let promoted = revalidate_skipped_full_paths(root, entries, &changed, &observed_unchanged)?;
        changed.extend(promoted);
    }
    let intent = intent_paths(snapshot, recovery_paths, &plan.kind, &changed);
    record_intent(state_path, &intent, recovery_paths)?;
    apply(
        root,
        stage.path(),
        snapshot,
        recovery_paths,
        &plan.kind,
        &intent,
    )?;
    sync_applied_paths(root, &intent)?;
    commit_snapshot(snapshot, &plan);
    let reconciled = matches!(plan.kind, PlanKind::Full { .. });
    if reconciled {
        checkpoint(state_path, snapshot, recovery_paths)?;
    }
    write_json(
        output,
        &AgentMessage::Ack {
            generation: snapshot.generation,
            state_id: snapshot.state_id.clone(),
        },
    )?;
    Ok(reconciled)
}

fn acquire_state_lock(state_path: &Path) -> Result<StateLock> {
    let parent = state_path.parent().context("state path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut lock_name = state_path.as_os_str().to_os_string();
    lock_name.push(".lock");
    let lock_path = PathBuf::from(lock_name);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open agent state lock {}", lock_path.display()))?;
    lock.try_lock().with_context(|| {
        format!(
            "lock agent state {}; another devsync session is active",
            lock_path.display()
        )
    })?;
    Ok(StateLock(lock))
}

fn intent_paths(
    snapshot: &Snapshot,
    recovery_paths: &BTreeSet<PathBuf>,
    kind: &PlanKind,
    changed: &BTreeSet<PathBuf>,
) -> BTreeSet<PathBuf> {
    match kind {
        PlanKind::Full { entries } => snapshot
            .entries
            .keys()
            .filter(|path| !entries.contains_key(*path))
            .chain(recovery_paths)
            .chain(changed)
            .cloned()
            .collect(),
        PlanKind::Delta { .. } => changed.clone(),
    }
}

fn validate_plan(snapshot: &Snapshot, recovery_required: bool, plan: &Plan) -> Result<()> {
    if plan.expected_generation != snapshot.generation
        || plan.expected_state_id != snapshot.state_id
    {
        bail!(
            "remote state mismatch: expected generation {} state {}, remote is generation {} state {}",
            plan.expected_generation,
            plan.expected_state_id,
            snapshot.generation,
            snapshot.state_id
        );
    }
    if plan.generation
        != snapshot
            .generation
            .checked_add(1)
            .context("generation overflow")?
    {
        bail!("new generation must immediately follow remote generation");
    }
    let computed = match &plan.kind {
        PlanKind::Full { entries } => {
            validate_entries(entries)?;
            state_id(entries)?
        }
        PlanKind::Delta { changes } => {
            if recovery_required {
                bail!("remote recovery requires a full plan");
            }
            validate_delta(&snapshot.entries, changes)?;
            delta_state_id(&snapshot.state_id, plan.generation, changes)?
        }
    };
    if computed != plan.state_id {
        bail!(
            "plan state identity mismatch: declared {}, computed {computed}",
            plan.state_id
        );
    }
    Ok(())
}

fn validate_delta(
    previous: &BTreeMap<PathBuf, Entry>,
    changes: &BTreeMap<PathBuf, Option<Entry>>,
) -> Result<()> {
    validate_paths(changes.keys())?;
    for (path, entry) in changes {
        if entry.is_none() {
            continue;
        }
        let mut ancestor = path.parent();
        while let Some(parent) = ancestor {
            let exists = changes
                .get(parent)
                .map_or_else(|| previous.contains_key(parent), Option::is_some);
            if exists {
                bail!(
                    "managed paths collide: {} is an ancestor of {}",
                    parent.display(),
                    path.display()
                );
            }
            ancestor = parent.parent();
        }
        for descendant in previous.range(path.clone()..) {
            let descendant = descendant.0;
            if descendant == path {
                continue;
            }
            if !descendant.starts_with(path) {
                break;
            }
            if !changes.get(descendant).is_some_and(Option::is_none) {
                bail!(
                    "managed paths collide: {} is an ancestor of {}",
                    path.display(),
                    descendant.display()
                );
            }
        }
    }
    Ok(())
}

fn commit_snapshot(snapshot: &mut Snapshot, plan: &Plan) {
    match &plan.kind {
        PlanKind::Full { entries } => snapshot.entries.clone_from(entries),
        PlanKind::Delta { changes } => {
            for (path, entry) in changes {
                match entry {
                    Some(entry) => {
                        snapshot.entries.insert(path.clone(), entry.clone());
                    }
                    None => {
                        snapshot.entries.remove(path);
                    }
                }
            }
        }
    }
    snapshot.generation = plan.generation;
    snapshot.state_id.clone_from(&plan.state_id);
}

struct FullDiff {
    needed: BTreeSet<PathBuf>,
    changed: BTreeSet<PathBuf>,
    observed_unchanged: BTreeMap<PathBuf, MetadataFingerprint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetadataFingerprint {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl MetadataFingerprint {
    fn from_path(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path)?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            size: metadata.size(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }
}

fn full_removals(
    snapshot: &Snapshot,
    recovery_paths: &BTreeSet<PathBuf>,
    entries: &std::collections::BTreeMap<PathBuf, Entry>,
) -> BTreeSet<PathBuf> {
    snapshot
        .entries
        .keys()
        .chain(recovery_paths)
        .filter(|path| !entries.contains_key(*path))
        .cloned()
        .collect()
}

fn overlaps_any(path: &Path, candidates: &BTreeSet<PathBuf>) -> bool {
    path.ancestors()
        .any(|ancestor| candidates.contains(ancestor))
        || candidates
            .range(path.to_path_buf()..)
            .next()
            .is_some_and(|candidate| candidate.starts_with(path))
}

fn full_diff(
    root: &Path,
    entries: &std::collections::BTreeMap<PathBuf, Entry>,
) -> Result<FullDiff> {
    let mut needed = BTreeSet::new();
    let mut changed = BTreeSet::new();
    let mut observed_unchanged = BTreeMap::new();
    for (path, entry) in entries {
        let absolute = root.join(path);
        let parents_are_safe = has_safe_real_parents(root, path)?;
        let before = parents_are_safe
            .then(|| MetadataFingerprint::from_path(&absolute).ok())
            .flatten();
        let actual = if parents_are_safe {
            Entry::from_path(&absolute).ok()
        } else {
            None
        };
        let after = parents_are_safe
            .then(|| MetadataFingerprint::from_path(&absolute).ok())
            .flatten();
        if entry.needs_payload()
            && !actual
                .as_ref()
                .is_some_and(|actual| entry.payload_matches(actual))
        {
            needed.insert(path.clone());
        }
        if actual.is_some_and(|actual| entry.content_matches(&actual)) {
            if before.is_some() && before == after {
                observed_unchanged.insert(path.clone(), after.expect("fingerprints match"));
            }
        } else {
            changed.insert(path.clone());
        }
    }
    Ok(FullDiff {
        needed,
        changed,
        observed_unchanged,
    })
}

fn revalidate_skipped_full_paths(
    root: &Path,
    entries: &std::collections::BTreeMap<PathBuf, Entry>,
    changed: &BTreeSet<PathBuf>,
    observed_unchanged: &BTreeMap<PathBuf, MetadataFingerprint>,
) -> Result<BTreeSet<PathBuf>> {
    let mut promoted = BTreeSet::new();
    for (path, entry) in entries {
        if changed.contains(path) {
            continue;
        }
        let absolute = root.join(path);
        if has_safe_real_parents(root, path)?
            && MetadataFingerprint::from_path(&absolute)
                .ok()
                .is_some_and(|current| observed_unchanged.get(path) == Some(&current))
        {
            continue;
        }
        let actual = if has_safe_real_parents(root, path)? {
            Entry::from_path(&absolute).ok()
        } else {
            None
        };
        if actual
            .as_ref()
            .is_some_and(|actual| entry.content_matches(actual))
        {
            continue;
        }
        if entry.needs_payload()
            && !actual
                .as_ref()
                .is_some_and(|actual| entry.payload_matches(actual))
        {
            bail!(
                "remote content changed during full validation: {}",
                path.display()
            );
        }
        promoted.insert(path.clone());
    }
    Ok(promoted)
}

fn has_safe_real_parents(root: &Path, relative: &Path) -> Result<bool> {
    let mut current = root.to_path_buf();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => return Ok(false),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(true)
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
    recovery_paths: &BTreeSet<PathBuf>,
    kind: &PlanKind,
    intent: &BTreeSet<PathBuf>,
) -> Result<()> {
    match kind {
        PlanKind::Full { entries } => {
            let removals = full_removals(previous, recovery_paths, entries);
            apply_removals(root, &removals)?;
            apply_entries(
                root,
                stage,
                entries.iter().filter(|(path, _)| intent.contains(*path)),
            )
        }
        PlanKind::Delta { changes } => {
            let removals = changes
                .iter()
                .filter_map(|(path, entry)| entry.is_none().then_some(path));
            apply_removals(root, removals)?;
            let entries = changes
                .iter()
                .filter_map(|(path, entry)| entry.as_ref().map(|entry| (path, entry)));
            apply_entries(root, stage, entries)
        }
    }
}

fn sync_applied_paths(root: &Path, paths: &BTreeSet<PathBuf>) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut directories = BTreeSet::from([root.to_path_buf()]);
    for path in paths {
        let destination = root.join(path);
        if fs::symlink_metadata(&destination)
            .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        {
            fs::File::open(&destination)?.sync_all()?;
        }
        let mut parent = path.parent();
        while let Some(relative) = parent {
            directories.insert(root.join(relative));
            parent = relative.parent();
        }
    }
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                fs::File::open(directory)?.sync_all()?;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn apply_removals<'a>(root: &Path, paths: impl IntoIterator<Item = &'a PathBuf>) -> Result<()> {
    let mut removals: Vec<_> = paths.into_iter().cloned().collect();
    removals.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for path in removals {
        ensure_safe_parent(root, &path)?;
        remove_any(&root.join(path))?;
    }
    Ok(())
}

fn apply_entries<'a>(
    root: &Path,
    stage: &Path,
    entries: impl IntoIterator<Item = (&'a PathBuf, &'a Entry)>,
) -> Result<()> {
    for (path, entry) in entries {
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

fn journal_path(state_path: &Path) -> PathBuf {
    let mut path = state_path.as_os_str().to_os_string();
    path.push(".journal");
    PathBuf::from(path)
}

fn load_journal(state_path: &Path) -> Result<BTreeSet<PathBuf>> {
    let path = journal_path(state_path);
    let existed = path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open agent journal {}", path.display()))?;
    if !existed {
        sync_parent(&path)?;
    }
    let mut paths = BTreeSet::new();
    loop {
        let record_start = file.stream_position()?;
        let record = match read_json::<_, JournalRecord>(&mut file) {
            Ok(Some(record)) => record,
            Ok(None) => {
                if file.metadata()?.len() > record_start {
                    truncate_journal_tail(&file, record_start)?;
                }
                break;
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::UnexpectedEof) =>
            {
                truncate_journal_tail(&file, record_start)?;
                break;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read agent journal {}", path.display()));
            }
        };
        for path in record.paths {
            crate::snapshot::validate_relative_path(&path)?;
            paths.insert(path);
        }
    }
    Ok(paths)
}

fn truncate_journal_tail(file: &fs::File, length: u64) -> Result<()> {
    file.set_len(length)?;
    file.sync_all()?;
    Ok(())
}

fn record_intent(
    state_path: &Path,
    intent: &BTreeSet<PathBuf>,
    recovery_paths: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let unrecorded = intent
        .difference(recovery_paths)
        .cloned()
        .collect::<BTreeSet<_>>();
    append_journal(state_path, &unrecorded)?;
    recovery_paths.extend(unrecorded);
    Ok(())
}

fn append_journal(state_path: &Path, paths: &BTreeSet<PathBuf>) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let path = journal_path(state_path);
    let existed = path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open agent journal {}", path.display()))?;
    write_json(
        &mut file,
        &JournalRecord {
            paths: paths.iter().cloned().collect(),
        },
    )?;
    file.sync_all()?;
    if !existed {
        sync_parent(&path)?;
    }
    Ok(())
}

fn clear_journal(state_path: &Path) -> Result<()> {
    let path = journal_path(state_path);
    let existed = path.exists();
    let file = fs::File::create(&path)
        .with_context(|| format!("truncate agent journal {}", path.display()))?;
    file.sync_all()?;
    if !existed {
        sync_parent(&path)?;
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("durable file has no parent")?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn checkpoint(
    state_path: &Path,
    snapshot: &Snapshot,
    recovery_paths: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    save_snapshot(state_path, snapshot)?;
    clear_journal(state_path)?;
    recovery_paths.clear();
    Ok(())
}

fn load_snapshot(path: &Path) -> Result<Snapshot> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice::<Snapshot>(&bytes)
            .context("parse agent snapshot")?
            .normalize(),
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
    let bytes = serde_json::to_vec(snapshot)?;
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_parent(path)?;
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
    fn state_lock_rejects_a_second_active_agent() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state/snapshot.json");
        let first = acquire_state_lock(&state).unwrap();
        assert!(acquire_state_lock(&state).is_err());
        drop(first);
        acquire_state_lock(&state).unwrap();
    }

    #[test]
    fn full_intent_includes_removed_paths() {
        let entries = BTreeMap::from([(PathBuf::from("removed"), entry(b"old", false))]);
        let snapshot = Snapshot {
            generation: 1,
            state_id: state_id(&entries).unwrap(),
            entries,
        };
        let kind = PlanKind::Full {
            entries: BTreeMap::new(),
        };

        assert_eq!(
            intent_paths(&snapshot, &BTreeSet::new(), &kind, &BTreeSet::new()),
            BTreeSet::from([PathBuf::from("removed")])
        );
    }

    #[test]
    fn full_then_delta_repairs_remote_drift_and_commits_acknowledged_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state/snapshot.json");
        fs::create_dir_all(state.parent().unwrap()).unwrap();

        let first_entries = BTreeMap::from([
            (PathBuf::from("bin/run"), entry(b"one", true)),
            (
                PathBuf::from("link"),
                Entry::Symlink {
                    target: "bin/run".into(),
                },
            ),
        ]);
        let first = Plan {
            expected_generation: 0,
            expected_state_id: state_id(&BTreeMap::new()).unwrap(),
            generation: 1,
            state_id: state_id(&first_entries).unwrap(),
            kind: PlanKind::Full {
                entries: first_entries,
            },
        };
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("unmanaged"), b"keep me").unwrap();
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
        let previous = load_snapshot(&state).unwrap();
        let changes = BTreeMap::from([
            (PathBuf::from("bin/run"), Some(entry(b"two", false))),
            (PathBuf::from("link"), None),
        ]);
        let second = Plan {
            expected_generation: 1,
            expected_state_id: previous.state_id.clone(),
            generation: 2,
            state_id: delta_state_id(&previous.state_id, 2, &changes).unwrap(),
            kind: PlanKind::Delta { changes },
        };
        transact(&root, &state, second, &[(PathBuf::from("bin/run"), b"two")]);
        assert_eq!(fs::read(root.join("bin/run")).unwrap(), b"two");
        assert!(!root.join("link").exists());
        assert_eq!(load_snapshot(&state).unwrap().generation, 2);
        assert_eq!(fs::read(root.join("unmanaged")).unwrap(), b"keep me");
    }

    #[test]
    fn full_replaces_stale_file_ancestor_with_desired_child() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state/snapshot.json");
        fs::create_dir_all(root.join("path")).unwrap();
        fs::write(root.join("path/child"), b"desired").unwrap();

        let previous_entries = BTreeMap::from([(PathBuf::from("path"), entry(b"previous", false))]);
        let previous = Snapshot {
            generation: 1,
            state_id: state_id(&previous_entries).unwrap(),
            entries: previous_entries,
        };
        save_snapshot(&state, &previous).unwrap();
        let desired = BTreeMap::from([(PathBuf::from("path/child"), entry(b"desired", false))]);
        let plan = Plan {
            expected_generation: previous.generation,
            expected_state_id: previous.state_id,
            generation: 2,
            state_id: state_id(&desired).unwrap(),
            kind: PlanKind::Full { entries: desired },
        };

        transact(
            &root,
            &state,
            plan,
            &[(PathBuf::from("path/child"), b"desired")],
        );

        assert_eq!(fs::read(root.join("path/child")).unwrap(), b"desired");
    }

    #[test]
    fn full_replaces_stale_child_with_desired_file_ancestor() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state/snapshot.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("path"), b"desired").unwrap();

        let previous_entries =
            BTreeMap::from([(PathBuf::from("path/child"), entry(b"previous", false))]);
        let previous = Snapshot {
            generation: 1,
            state_id: state_id(&previous_entries).unwrap(),
            entries: previous_entries,
        };
        save_snapshot(&state, &previous).unwrap();
        let desired = BTreeMap::from([(PathBuf::from("path"), entry(b"desired", false))]);
        let plan = Plan {
            expected_generation: previous.generation,
            expected_state_id: previous.state_id,
            generation: 2,
            state_id: state_id(&desired).unwrap(),
            kind: PlanKind::Full { entries: desired },
        };

        transact(&root, &state, plan, &[(PathBuf::from("path"), b"desired")]);

        assert_eq!(fs::read(root.join("path")).unwrap(), b"desired");
    }

    #[test]
    fn repeated_intent_is_journaled_once() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state/snapshot.json");
        save_snapshot(&state, &Snapshot::default()).unwrap();
        let intent = BTreeSet::from([PathBuf::from("same")]);
        let mut recovery_paths = BTreeSet::new();
        record_intent(&state, &intent, &mut recovery_paths).unwrap();
        let journal = journal_path(&state);
        let first_length = fs::metadata(&journal).unwrap().len();

        record_intent(&state, &intent, &mut recovery_paths).unwrap();

        assert_eq!(fs::metadata(journal).unwrap().len(), first_length);
        assert_eq!(recovery_paths, intent);
    }

    #[test]
    fn journal_ignores_and_truncates_a_torn_tail() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state/snapshot.json");
        save_snapshot(&state, &Snapshot::default()).unwrap();
        let paths = BTreeSet::from([PathBuf::from("kept")]);
        append_journal(&state, &paths).unwrap();
        let journal = journal_path(&state);
        let valid_length = fs::metadata(&journal).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&journal).unwrap();
        file.write_all(&100_u32.to_be_bytes()).unwrap();
        file.write_all(b"partial").unwrap();
        file.sync_all().unwrap();

        assert_eq!(load_journal(&state).unwrap(), paths);
        assert_eq!(fs::metadata(journal).unwrap().len(), valid_length);
    }

    #[test]
    fn full_recovery_removes_a_journaled_orphan() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state/snapshot.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("orphan"), b"partially applied").unwrap();
        let checkpoint = Snapshot::default();
        save_snapshot(&state, &checkpoint).unwrap();
        append_journal(&state, &BTreeSet::from([PathBuf::from("orphan")])).unwrap();

        let plan = Plan {
            expected_generation: checkpoint.generation,
            expected_state_id: checkpoint.state_id,
            generation: 1,
            state_id: state_id(&BTreeMap::new()).unwrap(),
            kind: PlanKind::Full {
                entries: BTreeMap::new(),
            },
        };
        let mut input = Vec::new();
        write_json(
            &mut input,
            &ClientMessage::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        write_json(&mut input, &ClientMessage::Plan(plan)).unwrap();
        write_json(&mut input, &ClientMessage::Done).unwrap();

        serve(&root, &state, input.as_slice(), Vec::new()).unwrap();

        assert!(!root.join("orphan").exists());
        assert!(load_journal(&state).unwrap().is_empty());
        assert_eq!(load_snapshot(&state).unwrap().generation, 1);
    }

    #[test]
    fn durability_sync_does_not_follow_symlink_ancestors() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        symlink("missing", root.join("link")).unwrap();

        sync_applied_paths(
            &root,
            &BTreeSet::from([PathBuf::from("link/previous-child")]),
        )
        .unwrap();
    }

    #[test]
    fn delta_apply_touches_only_changed_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let stage = temp.path().join("stage");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&stage).unwrap();
        fs::write(root.join("changed"), b"same").unwrap();
        symlink("missing-target", root.join("untouched")).unwrap();

        let unchanged_entry = entry(b"expected-but-drifted", false);
        let changed_entry = entry(b"same", true);
        let entries = BTreeMap::from([
            (PathBuf::from("changed"), entry(b"same", false)),
            (PathBuf::from("untouched"), unchanged_entry),
        ]);
        let previous = Snapshot {
            generation: 1,
            state_id: state_id(&entries).unwrap(),
            entries,
        };
        let kind = PlanKind::Delta {
            changes: BTreeMap::from([(PathBuf::from("changed"), Some(changed_entry))]),
        };

        apply(
            &root,
            &stage,
            &previous,
            &BTreeSet::new(),
            &kind,
            &BTreeSet::from([PathBuf::from("changed")]),
        )
        .unwrap();

        assert_ne!(
            fs::metadata(root.join("changed"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_eq!(
            fs::read_link(root.join("untouched")).unwrap(),
            Path::new("missing-target")
        );
        assert!(previous.entries.contains_key(Path::new("untouched")));
    }

    #[test]
    fn requests_payload_without_following_symlink_parent() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("file"), b"matching").unwrap();
        symlink(&outside, root.join("parent")).unwrap();
        let candidates =
            BTreeMap::from([(PathBuf::from("parent/file"), entry(b"matching", false))]);
        let diff = full_diff(&root, &candidates).unwrap();
        assert_eq!(diff.needed, BTreeSet::from([PathBuf::from("parent/file")]));
        assert_eq!(diff.changed, diff.needed);
    }

    #[test]
    fn full_with_matching_remote_content_needs_no_payload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file"), b"matching").unwrap();
        let candidates = BTreeMap::from([(PathBuf::from("file"), entry(b"matching", false))]);
        let diff = full_diff(&root, &candidates).unwrap();
        assert!(diff.needed.is_empty());
        assert!(diff.changed.is_empty());
    }

    #[test]
    fn full_mode_change_needs_no_payload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file"), b"matching").unwrap();
        let candidates = BTreeMap::from([(PathBuf::from("file"), entry(b"matching", true))]);
        let diff = full_diff(&root, &candidates).unwrap();
        assert!(diff.needed.is_empty());
        assert_eq!(diff.changed, BTreeSet::from([PathBuf::from("file")]));
    }

    #[test]
    fn full_revalidation_rejects_new_content_drift() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file"), b"matching").unwrap();
        let entries = BTreeMap::from([(PathBuf::from("file"), entry(b"matching", false))]);
        let diff = full_diff(&root, &entries).unwrap();
        assert!(diff.changed.is_empty());

        fs::write(root.join("file"), b"changed after negotiation").unwrap();

        assert!(
            revalidate_skipped_full_paths(
                &root,
                &entries,
                &diff.changed,
                &diff.observed_unchanged,
            )
            .is_err()
        );
    }

    #[test]
    fn full_revalidation_promotes_new_mode_drift() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file"), b"matching").unwrap();
        let entries = BTreeMap::from([(PathBuf::from("file"), entry(b"matching", false))]);
        let diff = full_diff(&root, &entries).unwrap();
        assert!(diff.changed.is_empty());
        fs::set_permissions(root.join("file"), fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(
            revalidate_skipped_full_paths(
                &root,
                &entries,
                &diff.changed,
                &diff.observed_unchanged,
            )
            .unwrap(),
            BTreeSet::from([PathBuf::from("file")])
        );
    }

    #[test]
    fn full_transaction_skips_unchanged_files_and_repairs_mode_drift() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state/snapshot.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("unchanged"), b"same").unwrap();
        fs::set_permissions(root.join("unchanged"), fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(root.join("mode-change"), b"same").unwrap();
        let entries = BTreeMap::from([
            (PathBuf::from("unchanged"), entry(b"same", false)),
            (PathBuf::from("mode-change"), entry(b"same", true)),
        ]);
        let plan = Plan {
            expected_generation: 0,
            expected_state_id: state_id(&BTreeMap::new()).unwrap(),
            generation: 1,
            state_id: state_id(&entries).unwrap(),
            kind: PlanKind::Full { entries },
        };

        transact(&root, &state, plan, &[]);

        assert_eq!(
            fs::metadata(root.join("unchanged"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_ne!(
            fs::metadata(root.join("mode-change"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }

    #[test]
    fn full_detects_remote_drift() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("file"), b"drift").unwrap();
        let candidates = BTreeMap::from([(PathBuf::from("file"), entry(b"local", false))]);
        let diff = full_diff(&root, &candidates).unwrap();
        assert_eq!(diff.needed, BTreeSet::from([PathBuf::from("file")]));
        assert_eq!(diff.changed, diff.needed);
    }

    #[test]
    fn rejects_colliding_desired_paths() {
        let entries = BTreeMap::from([
            (PathBuf::from("path"), entry(b"one", false)),
            (PathBuf::from("path/child"), entry(b"two", false)),
        ]);
        let plan = Plan {
            expected_generation: 0,
            expected_state_id: state_id(&BTreeMap::new()).unwrap(),
            generation: 1,
            state_id: "invalid".into(),
            kind: PlanKind::Full { entries },
        };
        let previous = Snapshot::default();
        assert!(validate_plan(&previous, false, &plan).is_err());
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
                state_id: state_id(&BTreeMap::new()).unwrap(),
                entries: BTreeMap::new(),
            },
        )
        .unwrap();
        let empty_state_id = state_id(&BTreeMap::new()).unwrap();
        let plan = ClientMessage::Plan(Plan {
            expected_generation: 3,
            expected_state_id: empty_state_id.clone(),
            generation: 5,
            state_id: empty_state_id,
            kind: PlanKind::Full {
                entries: BTreeMap::new(),
            },
        });
        let mut input = Vec::new();
        write_json(
            &mut input,
            &ClientMessage::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        write_json(&mut input, &plan).unwrap();
        assert!(serve(&root, &state, input.as_slice(), Vec::new()).is_err());
        assert_eq!(load_snapshot(&state).unwrap().generation, 4);
    }

    fn transact(root: &Path, state: &Path, plan: Plan, payloads: &[(PathBuf, &[u8])]) {
        let negotiates_payloads = matches!(&plan.kind, PlanKind::Full { .. });
        let mut input = Vec::new();
        write_json(
            &mut input,
            &ClientMessage::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
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
        write_json(&mut input, &ClientMessage::Done).unwrap();
        let mut output = Vec::new();
        serve(root, state, Cursor::new(input), &mut output).unwrap();
        let mut output = output.as_slice();
        assert!(matches!(
            read_json::<_, AgentMessage>(&mut output).unwrap(),
            Some(AgentMessage::Hello { .. })
        ));
        if negotiates_payloads {
            assert!(matches!(
                read_json::<_, AgentMessage>(&mut output).unwrap(),
                Some(AgentMessage::NeedPayloads { .. })
            ));
        }
        assert!(matches!(
            read_json::<_, AgentMessage>(&mut output).unwrap(),
            Some(AgentMessage::Ack { .. })
        ));
    }
}
