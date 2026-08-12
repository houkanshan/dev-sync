use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{Local, SecondsFormat};
use devsync::deploy;
use devsync::transport;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{Mutex, mpsc, oneshot};
use watchman_client::prelude::*;
use watchman_client::{SubscriptionData, fields::NameOnly};

use crate::project::{Config, Project, load_snapshot, save_snapshot};
use crate::sync::{self, PlanMode};

const MAX_LOGGED_PATHS: usize = 20;

fn log(message: impl std::fmt::Display) {
    eprintln!(
        "{} {message}",
        Local::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    );
}

fn format_changed_paths(paths: &BTreeSet<PathBuf>) -> String {
    if paths.is_empty() {
        return "none".into();
    }

    let mut displayed = paths
        .iter()
        .take(MAX_LOGGED_PATHS)
        .map(|path| format!("{:?}", path.to_string_lossy()))
        .collect::<Vec<_>>()
        .join(", ");
    let omitted = paths.len().saturating_sub(MAX_LOGGED_PATHS);
    if omitted > 0 {
        displayed.push_str(&format!(", ... (+{omitted} more)"));
    }
    displayed
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Request {
    Status,
    Flush,
    Stop,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "lowercase")]
pub enum Response {
    Ok { message: String },
    Error { message: String },
}

struct State {
    started: Instant,
    syncing: bool,
    pending: usize,
    last_synced: Option<Instant>,
    last_error: Option<String>,
}

enum Work {
    Changed {
        paths: BTreeSet<PathBuf>,
        reconcile: bool,
    },
    Flush(oneshot::Sender<Result<(), String>>),
    Stop(oneshot::Sender<Result<(), String>>),
}

pub async fn run(root: PathBuf, foreground: bool) -> Result<()> {
    let project = Project::from_root(root.canonicalize()?);
    std::fs::create_dir_all(project.socket_path.parent().expect("socket has parent"))?;
    if project.socket_path.exists() {
        let _ = std::fs::remove_file(&project.socket_path);
    }
    let config = project.load_config()?;
    let listener = UnixListener::bind(&project.socket_path)
        .with_context(|| format!("bind {}", project.socket_path.display()))?;
    let _socket_guard = SocketGuard(project.socket_path.clone());
    log(format!("daemon started for {}", project.root.display()));
    let state = Arc::new(Mutex::new(State {
        started: Instant::now(),
        syncing: false,
        pending: 0,
        last_synced: None,
        last_error: None,
    }));
    let (work_tx, work_rx) = mpsc::channel(128);
    let worker = tokio::task::spawn_blocking({
        let project = project.clone();
        let config = config.clone();
        let state = Arc::clone(&state);
        move || worker_loop(project, config, state, work_rx)
    });
    work_tx
        .send(Work::Changed {
            paths: BTreeSet::new(),
            reconcile: true,
        })
        .await?;

    let watcher = tokio::spawn(watch(project.root.clone(), work_tx.clone()));
    let mut interrupt = if foreground {
        Some(signal(SignalKind::interrupt()).context("listen for Ctrl-C")?)
    } else {
        None
    };
    let mut stopping = false;
    while !stopping {
        let connection = async {
            let (stream, _) = listener.accept().await?;
            handle_connection(stream, Arc::clone(&state), &work_tx).await
        };
        tokio::select! {
            result = connection => stopping = result?,
            _ = receive_interrupt(&mut interrupt), if foreground => {
                log("received Ctrl-C; stopping after current sync (press Ctrl-C again to force)");
                tokio::select! {
                    response = wait_for(&work_tx, true) => {
                        if let Response::Error { message } = response {
                            log(format!("stop failed: {message}"));
                        }
                    }
                    _ = receive_interrupt(&mut interrupt) => {
                        log("received second Ctrl-C; forcing exit");
                        std::process::exit(130);
                    }
                }
                stopping = true;
            }
        }
    }
    watcher.abort();
    drop(work_tx);
    worker.await??;
    log("daemon stopped");
    Ok(())
}

async fn receive_interrupt(interrupt: &mut Option<Signal>) {
    interrupt
        .as_mut()
        .expect("interrupt branch is enabled only in foreground mode")
        .recv()
        .await;
}

async fn watch(root: PathBuf, work_tx: mpsc::Sender<Work>) -> Result<()> {
    loop {
        match watch_once(&root, &work_tx).await {
            Ok(()) => {}
            Err(error) => {
                log(format!("watchman disconnected: {error:#}; reconnecting"));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn watch_once(root: &std::path::Path, work_tx: &mpsc::Sender<Work>) -> Result<()> {
    let client = Connector::new().connect().await?;
    let resolved = client
        .resolve_root(CanonicalPath::canonicalize(root)?)
        .await?;
    let (mut subscription, _) = client
        .subscribe::<NameOnly>(&resolved, SubscribeRequest::default())
        .await?;
    loop {
        match subscription.next().await? {
            SubscriptionData::FilesChanged(result) => {
                let reconcile = result.is_fresh_instance;
                let paths = result
                    .files
                    .unwrap_or_default()
                    .into_iter()
                    .map(|file| file.name.into_inner())
                    .collect();
                work_tx.send(Work::Changed { paths, reconcile }).await?;
            }
            SubscriptionData::Canceled => bail!("subscription canceled"),
            SubscriptionData::StateEnter { .. } | SubscriptionData::StateLeave { .. } => {}
        }
    }
}

fn worker_loop(
    project: Project,
    config: Config,
    state: Arc<Mutex<State>>,
    mut work_rx: mpsc::Receiver<Work>,
) -> Result<()> {
    let runtime = tokio::runtime::Handle::current();
    let mut acknowledged = load_snapshot(&project.snapshot_path)?;
    let mut force_full = true;
    while let Some(first) = work_rx.blocking_recv() {
        let mut changed = BTreeSet::new();
        let mut flushes = Vec::new();
        let mut stop = None;
        let mut reconcile = false;
        collect_work(first, &mut changed, &mut reconcile, &mut flushes, &mut stop);
        std::thread::sleep(Duration::from_millis(20));
        while let Ok(work) = work_rx.try_recv() {
            collect_work(work, &mut changed, &mut reconcile, &mut flushes, &mut stop);
        }
        if stop.is_some() && changed.is_empty() && !reconcile && flushes.is_empty() {
            if let Some(stop) = stop {
                let _ = stop.send(Ok(()));
            }
            break;
        }
        let mode = work_mode(
            force_full,
            reconcile,
            !flushes.is_empty(),
            &project.root,
            &changed,
        );
        runtime.block_on(async {
            let mut state = state.lock().await;
            state.syncing = true;
            state.pending = changed.len();
        });
        let action = if mode == PlanMode::Full {
            "validate"
        } else {
            "delta"
        };
        let sync_started = Instant::now();
        let result =
            sync_once(&project, &config, &acknowledged, mode, &changed).and_then(|outcome| {
                let next = sync::committed_snapshot(&acknowledged, &outcome.plan);
                save_snapshot(&project.snapshot_path, &next)?;
                Ok((outcome, next))
            });
        match &result {
            Ok((outcome, _)) => {
                let affected = affected_paths(&outcome.plan.kind);
                if affected.is_empty() {
                    log(format!(
                        "sync {action} completed in {}ms; no managed changes",
                        sync_started.elapsed().as_millis()
                    ));
                } else {
                    log(format!(
                        "sync {action} completed in {}ms; paths: {}",
                        sync_started.elapsed().as_millis(),
                        format_changed_paths(&affected)
                    ));
                }
                if !outcome.requested.is_empty() {
                    log(format!(
                        "uploaded paths: {}",
                        format_changed_paths(&outcome.requested)
                    ));
                }
            }
            Err(error) => log(format!(
                "sync {action} failed in {}ms: {error:#}",
                sync_started.elapsed().as_millis()
            )),
        }
        let reply = result
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("{error:#}"));
        match result {
            Ok((_, next)) => {
                acknowledged = next;
                force_full = false;
            }
            Err(_) => force_full = true,
        }
        runtime.block_on(async {
            let mut state = state.lock().await;
            state.syncing = false;
            state.pending = 0;
            if reply.is_ok() {
                state.last_synced = Some(Instant::now());
                state.last_error = None;
            } else {
                state.last_error = reply.clone().err();
            }
        });
        for flush in flushes {
            let _ = flush.send(reply.clone());
        }
        if let Some(stop) = stop {
            let _ = stop.send(reply);
            break;
        }
    }
    Ok(())
}

fn sync_once(
    project: &Project,
    config: &Config,
    acknowledged: &devsync::snapshot::Snapshot,
    mode: PlanMode,
    changed: &BTreeSet<PathBuf>,
) -> Result<transport::Transaction> {
    let mut agent = deploy::launch(&config.remote, &config.remote_path)?;
    let result = transport::transact(
        &mut agent.stdout,
        &mut agent.stdin,
        &project.root,
        |remote| sync::Planner::new(&project.root, acknowledged).plan(mode, changed, remote),
    );
    let wait = agent.wait();
    match (result, wait) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn affected_paths(kind: &devsync::protocol::PlanKind) -> BTreeSet<PathBuf> {
    match kind {
        devsync::protocol::PlanKind::Full { entries } => entries.keys().cloned().collect(),
        devsync::protocol::PlanKind::Delta { changes } => changes.keys().cloned().collect(),
    }
}

fn work_mode(
    force_full: bool,
    reconcile: bool,
    flush: bool,
    root: &std::path::Path,
    changed: &BTreeSet<PathBuf>,
) -> PlanMode {
    if force_full || reconcile || flush || sync::needs_reconcile(root, changed) {
        PlanMode::Full
    } else {
        PlanMode::Delta
    }
}

fn collect_work(
    work: Work,
    changed: &mut BTreeSet<PathBuf>,
    reconcile: &mut bool,
    flushes: &mut Vec<oneshot::Sender<Result<(), String>>>,
    stop: &mut Option<oneshot::Sender<Result<(), String>>>,
) {
    match work {
        Work::Changed {
            paths,
            reconcile: must_reconcile,
        } => {
            changed.extend(paths);
            *reconcile |= must_reconcile;
        }
        Work::Flush(reply) => flushes.push(reply),
        Work::Stop(reply) => *stop = Some(reply),
    }
}

async fn handle_connection(
    stream: UnixStream,
    state: Arc<Mutex<State>>,
    work_tx: &mpsc::Sender<Work>,
) -> Result<bool> {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    let request: Request = serde_json::from_str(&line)?;
    let (response, stopping) = match request {
        Request::Status => (status_response(&state).await, false),
        Request::Flush => (wait_for(work_tx, false).await, false),
        Request::Stop => (wait_for(work_tx, true).await, true),
    };
    writer
        .write_all(format!("{}\n", serde_json::to_string(&response)?).as_bytes())
        .await?;
    Ok(stopping)
}

async fn wait_for(work_tx: &mpsc::Sender<Work>, stop: bool) -> Response {
    let (reply_tx, reply_rx) = oneshot::channel();
    let work = if stop {
        Work::Stop(reply_tx)
    } else {
        Work::Flush(reply_tx)
    };
    if work_tx.send(work).await.is_err() {
        return Response::Error {
            message: "sync worker stopped".into(),
        };
    }
    match reply_rx.await {
        Ok(Ok(())) => Response::Ok {
            message: if stop { "stopped" } else { "flushed" }.into(),
        },
        Ok(Err(message)) => Response::Error { message },
        Err(_) => Response::Error {
            message: "sync worker stopped without replying".into(),
        },
    }
}

async fn status_response(state: &Mutex<State>) -> Response {
    let state = state.lock().await;
    let sync_status = if state.syncing { "syncing" } else { "idle" };
    let last_sync = state
        .last_synced
        .map(|time| format!("{}s ago", time.elapsed().as_secs()))
        .unwrap_or_else(|| "never".into());
    let error = state
        .last_error
        .as_ref()
        .map(|error| format!(", error: {error}"))
        .unwrap_or_default();
    Response::Ok {
        message: format!(
            "running ({sync_status}), pending: {}, last sync: {last_sync}, uptime: {}s{error}",
            state.pending,
            state.started.elapsed().as_secs()
        ),
    }
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_all_changed_paths_under_limit() {
        let paths = BTreeSet::from([PathBuf::from("a file.txt"), PathBuf::from("src/main.rs")]);
        assert_eq!(
            format_changed_paths(&paths),
            "\"a file.txt\", \"src/main.rs\""
        );
    }

    #[test]
    fn truncates_changed_paths_at_limit() {
        let paths = (0..MAX_LOGGED_PATHS + 2)
            .map(|index| PathBuf::from(format!("{index:02}.txt")))
            .collect();
        let formatted = format_changed_paths(&paths);
        assert!(formatted.contains("\"19.txt\""));
        assert!(!formatted.contains("\"20.txt\""));
        assert!(formatted.ends_with("... (+2 more)"));
    }

    #[test]
    fn startup_and_flush_validate_but_draining_changes_does_not() {
        let temp = tempfile::tempdir().unwrap();
        let changed = BTreeSet::from([PathBuf::from("file")]);
        assert_eq!(
            work_mode(true, false, false, temp.path(), &changed),
            PlanMode::Full
        );
        assert_eq!(
            work_mode(false, false, true, temp.path(), &changed),
            PlanMode::Full
        );
        assert_eq!(
            work_mode(false, false, false, temp.path(), &changed),
            PlanMode::Delta
        );
    }
}
