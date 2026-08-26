use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use devsync::protocol::{Plan, PlanKind};
use devsync::snapshot::{Entry, Snapshot, delta_state_id, state_id, validate_relative_path};
use devsync::transport::RemoteState;

pub const RECONCILE_PATHS: [&str; 3] = [".devsyncignore", ".git/info/exclude", ".git/index"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanMode {
    Full,
    Delta,
}

pub struct Planner<'a> {
    root: &'a Path,
    acknowledged: &'a Snapshot,
}

impl<'a> Planner<'a> {
    pub fn new(root: &'a Path, acknowledged: &'a Snapshot) -> Self {
        Self { root, acknowledged }
    }

    pub fn plan(
        &self,
        requested: PlanMode,
        changed: &BTreeSet<PathBuf>,
        remote: &RemoteState,
    ) -> Result<Plan> {
        let local_state_id = &self.acknowledged.state_id;
        let same_state = !remote.recovery_required
            && remote.generation == self.acknowledged.generation
            && remote.state_id == *local_state_id;
        let mode = if same_state
            && requested == PlanMode::Delta
            && !dirty_is_ambiguous(changed, &self.acknowledged.entries)
        {
            PlanMode::Delta
        } else {
            PlanMode::Full
        };
        let kind = match mode {
            PlanMode::Full => PlanKind::Full {
                entries: full_entries(self.root)?,
            },
            PlanMode::Delta => PlanKind::Delta {
                changes: dirty_entries(self.root, changed, &self.acknowledged.entries)?,
            },
        };
        if matches!(&kind, PlanKind::Delta { changes } if changes.is_empty()) {
            return Ok(Plan {
                expected_generation: remote.generation,
                expected_state_id: remote.state_id.clone(),
                generation: remote.generation,
                state_id: remote.state_id.clone(),
                kind,
            });
        }
        let generation = remote
            .generation
            .checked_add(1)
            .context("remote generation overflow")?;
        let desired_state_id = match &kind {
            PlanKind::Full { entries } => state_id(entries)?,
            PlanKind::Delta { changes } => delta_state_id(local_state_id, generation, changes)?,
        };
        if !remote.recovery_required && !same_state && desired_state_id == remote.state_id {
            // The remote already committed this exact tree. Adopt its generation
            // without sending another protocol transaction.
            return Ok(Plan {
                expected_generation: remote.generation,
                expected_state_id: remote.state_id.clone(),
                generation: remote.generation,
                state_id: remote.state_id.clone(),
                kind,
            });
        }
        Ok(Plan {
            expected_generation: remote.generation,
            expected_state_id: remote.state_id.clone(),
            generation,
            state_id: desired_state_id,
            kind,
        })
    }
}

pub fn commit_snapshot(snapshot: &mut Snapshot, plan: &Plan) {
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

pub fn full_entries(root: &Path) -> Result<BTreeMap<PathBuf, Entry>> {
    let paths = manifest(root)?;
    scan_entries(root, paths)
}

pub fn dirty_entries(
    root: &Path,
    changed: &BTreeSet<PathBuf>,
    previous: &BTreeMap<PathBuf, Entry>,
) -> Result<BTreeMap<PathBuf, Option<Entry>>> {
    let candidates = changed
        .iter()
        .filter(|path| is_literal_candidate(path))
        .cloned()
        .collect::<BTreeSet<_>>();
    let eligible = eligible_literals(root, &candidates)?;
    let mut changes = BTreeMap::new();
    for path in candidates {
        if eligible.contains(&path) {
            let entry = Entry::from_path(&root.join(&path))
                .with_context(|| format!("scan dirty path {}", path.display()))?;
            if !previous
                .get(&path)
                .is_some_and(|previous| previous.content_matches(&entry))
            {
                changes.insert(path, Some(entry));
            }
        } else if previous.contains_key(&path) {
            changes.insert(path, None);
        }
    }
    Ok(changes)
}

pub fn needs_reconcile(root: &Path, paths: &BTreeSet<PathBuf>) -> bool {
    paths.iter().any(|path| {
        path.file_name().is_some_and(|name| name == ".gitignore")
            || RECONCILE_PATHS
                .iter()
                .any(|required| path == Path::new(required))
            || path == Path::new(".git")
            || (root.join(path).is_dir() && !root.join(path).is_symlink())
    })
}

fn dirty_is_ambiguous(paths: &BTreeSet<PathBuf>, previous: &BTreeMap<PathBuf, Entry>) -> bool {
    paths.iter().any(|path| {
        if path.is_dir() && !path.is_symlink() {
            return true;
        }
        previous
            .range(path.clone()..)
            .map(|(managed, _)| managed)
            .find(|managed| *managed != path)
            .is_some_and(|managed| managed.starts_with(path))
    })
}

fn scan_entries(root: &Path, paths: BTreeSet<PathBuf>) -> Result<BTreeMap<PathBuf, Entry>> {
    paths
        .into_iter()
        .map(|path| {
            let entry = Entry::from_path(&root.join(&path))
                .with_context(|| format!("scan {}", path.display()))?;
            Ok((path, entry))
        })
        .collect()
}

fn manifest(root: &Path) -> Result<BTreeSet<PathBuf>> {
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
    let custom_ignored = custom_ignored_tracked(root, None)?;
    git_paths(root, output, &custom_ignored)
}

fn eligible_literals(root: &Path, candidates: &BTreeSet<PathBuf>) -> Result<BTreeSet<PathBuf>> {
    if candidates.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut command = Command::new("git");
    command.current_dir(root).args([
        "--literal-pathspecs",
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    ]);
    if root.join(".devsyncignore").is_file() {
        command.arg("--exclude-from=.devsyncignore");
    }
    command.arg("--").args(candidates);
    let output = command.output().context("query dirty Git paths")?;
    let custom_ignored = custom_ignored_tracked(root, Some(candidates))?;
    git_paths(root, output, &custom_ignored)
}

fn git_paths(
    root: &Path,
    output: std::process::Output,
    custom_ignored: &BTreeSet<PathBuf>,
) -> Result<BTreeSet<PathBuf>> {
    if !output.status.success() {
        bail!(
            "git ls-files failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }
    let mut paths = BTreeSet::new();
    for raw in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|raw| !raw.is_empty())
    {
        let path =
            PathBuf::from(std::str::from_utf8(raw).context("Git returned a non-UTF-8 path")?);
        validate_relative_path(&path)?;
        if !custom_ignored.contains(&path)
            && path != Path::new(".devsync.toml")
            && !path.starts_with(".git")
            && fs::symlink_metadata(root.join(&path)).is_ok()
        {
            paths.insert(path);
        }
    }
    Ok(paths)
}

fn custom_ignored_tracked(
    root: &Path,
    candidates: Option<&BTreeSet<PathBuf>>,
) -> Result<BTreeSet<PathBuf>> {
    if !root.join(".devsyncignore").is_file() {
        return Ok(BTreeSet::new());
    }
    let mut command = Command::new("git");
    command.current_dir(root).args([
        "--literal-pathspecs",
        "ls-files",
        "-z",
        "--cached",
        "--ignored",
        "--exclude-from=.devsyncignore",
    ]);
    if let Some(candidates) = candidates {
        command.arg("--").args(candidates);
    }
    let output = command
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

fn is_literal_candidate(path: &Path) -> bool {
    validate_relative_path(path).is_ok()
        && path != Path::new(".devsync.toml")
        && !path.starts_with(".git")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn repo() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        Command::new("git")
            .current_dir(temp.path())
            .args(["init", "-q"])
            .status()
            .unwrap();
        temp
    }

    #[test]
    fn dirty_plan_hashes_only_literal_changed_paths_and_deletes_managed_ineligible() {
        let temp = repo();
        fs::write(temp.path().join("changed"), b"new").unwrap();
        fs::write(temp.path().join("untouched"), b"old").unwrap();
        fs::write(temp.path().join("ignored"), b"old").unwrap();
        fs::write(temp.path().join(".devsyncignore"), b"ignored\n").unwrap();
        let previous = BTreeMap::from([
            (
                PathBuf::from("ignored"),
                Entry::from_path(&temp.path().join("ignored")).unwrap(),
            ),
            (
                PathBuf::from("untouched"),
                Entry::from_path(&temp.path().join("untouched")).unwrap(),
            ),
        ]);
        fs::remove_file(temp.path().join("untouched")).unwrap();
        let changed = BTreeSet::from([
            PathBuf::from("changed"),
            PathBuf::from("ignored"),
            PathBuf::from("untouched"),
        ]);
        let delta = dirty_entries(temp.path(), &changed, &previous).unwrap();
        assert!(delta[Path::new("changed")].is_some());
        assert_eq!(delta[Path::new("ignored")], None);
        assert_eq!(delta[Path::new("untouched")], None);
    }

    #[test]
    fn unchanged_dirty_path_is_a_noop() {
        let temp = repo();
        fs::write(temp.path().join("file"), b"same").unwrap();
        let previous = BTreeMap::from([(
            PathBuf::from("file"),
            Entry::from_path(&temp.path().join("file")).unwrap(),
        )]);
        let changed = BTreeSet::from([PathBuf::from("file")]);

        assert!(
            dirty_entries(temp.path(), &changed, &previous)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn remote_mismatch_forces_full_plan() {
        let temp = repo();
        fs::write(temp.path().join("file"), b"one").unwrap();
        let acknowledged = Snapshot::default();
        let plan = Planner::new(temp.path(), &acknowledged)
            .plan(
                PlanMode::Delta,
                &BTreeSet::from([PathBuf::from("file")]),
                &RemoteState {
                    generation: 7,
                    state_id: "other".into(),
                    recovery_required: false,
                },
            )
            .unwrap();
        assert!(matches!(plan.kind, PlanKind::Full { .. }));
        assert_eq!(plan.expected_generation, 7);
    }

    #[test]
    fn recovery_required_forces_a_full_transaction() {
        let temp = repo();
        let acknowledged = Snapshot::default();
        let plan = Planner::new(temp.path(), &acknowledged)
            .plan(
                PlanMode::Delta,
                &BTreeSet::new(),
                &RemoteState {
                    generation: acknowledged.generation,
                    state_id: acknowledged.state_id.clone(),
                    recovery_required: true,
                },
            )
            .unwrap();

        assert!(matches!(plan.kind, PlanKind::Full { .. }));
        assert_eq!(plan.generation, 1);
    }

    #[test]
    fn detects_ambiguous_and_eligibility_events() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("directory")).unwrap();
        assert!(needs_reconcile(
            temp.path(),
            &BTreeSet::from([PathBuf::from("directory")])
        ));
        assert!(needs_reconcile(
            temp.path(),
            &BTreeSet::from([PathBuf::from("nested/.gitignore")])
        ));
    }
}
