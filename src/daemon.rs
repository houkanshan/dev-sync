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

use crate::project::{Project, load_snapshot, save_snapshot};
use crate::sync::{self, PlanMode};

const MAX_LOGGED_PATHS: usize = 20;
const AGENT_SESSION_TIMEOUT: Duration = Duration::from_secs(60);
const AGENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const AGENT_FULL_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

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
    let deployment = deploy::Deployment::prepare(&config.remote, &config.remote_path)?;
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
        let state = Arc::clone(&state);
        move || worker_loop(project, deployment, state, work_rx)
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
        .subscribe::<NameOnly>(&resolved, watchman_subscribe_request())
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
    deployment: deploy::Deployment,
    state: Arc<Mutex<State>>,
    mut work_rx: mpsc::Receiver<Work>,
) -> Result<()> {
    let runtime = tokio::runtime::Handle::current();
    let mut acknowledged = load_snapshot(&project.snapshot_path)?;
    let mut session: Option<AgentSession> = None;
    let mut force_full = true;
    let mut heartbeat_at = tokio::time::Instant::now() + AGENT_HEARTBEAT_INTERVAL;
    loop {
        // Use an absolute deadline: ignored events and no-op plans must not
        // postpone heartbeats while the remote waits for its next frame.
        if tokio::time::Instant::now() >= heartbeat_at {
            if let Some(active) = session.as_mut()
                && let Err(error) = active.ping()
            {
                log(format!(
                    "agent heartbeat failed; reconnecting on next sync: {error:#}"
                ));
                session
                    .take()
                    .expect("failed heartbeat session exists")
                    .abort();
                force_full = true;
            }
            heartbeat_at = tokio::time::Instant::now() + AGENT_HEARTBEAT_INTERVAL;
        }
        let first =
            runtime.block_on(async { tokio::time::timeout_at(heartbeat_at, work_rx.recv()).await });
        let first = match first {
            Ok(Some(first)) => first,
            Ok(None) => break,
            Err(_) => continue,
        };
        let mut changed = BTreeSet::new();
        let mut flushes = Vec::new();
        let mut stop = None;
        let mut reconcile = false;
        collect_work(first, &mut changed, &mut reconcile, &mut flushes, &mut stop);
        std::thread::sleep(Duration::from_millis(20));
        while let Ok(work) = work_rx.try_recv() {
            collect_work(work, &mut changed, &mut reconcile, &mut flushes, &mut stop);
        }
        let mut recover_full = false;
        changed = match sync::retain_relevant_changes(&project.root, changed) {
            Ok(paths) => paths,
            Err(error) => {
                log(format!(
                    "filter watchman paths failed; validating fully: {error:#}"
                ));
                recover_full = true;
                BTreeSet::new()
            }
        };
        let index_eligibility = match sync::take_index_eligibility_change(
            &project.root,
            &acknowledged.entries,
            &mut changed,
        ) {
            Ok(changed_set) => changed_set,
            Err(error) => {
                log(format!(
                    "index eligibility check failed; validating fully: {error:#}"
                ));
                true
            }
        };
        let decision = classify_sync(
            force_full || recover_full,
            reconcile,
            !flushes.is_empty(),
            index_eligibility,
            &project.root,
            &changed,
        );
        if matches!(decision, SyncDecision::Skip) {
            if let Some(stop) = stop {
                let _ = stop.send(Ok(()));
                break;
            }
            continue;
        }
        let (mode, reason) = match decision {
            SyncDecision::Skip => unreachable!("skip handled above"),
            SyncDecision::Delta => (PlanMode::Delta, None),
            SyncDecision::Full(reason) => (PlanMode::Full, Some(reason)),
        };
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
        if let Some(reason) = reason {
            log(format!("sync validate started ({reason})"));
        }
        let sync_started = Instant::now();
        let result = match sync_once(
            &project,
            &deployment,
            &mut session,
            &acknowledged,
            mode,
            &changed,
        ) {
            Ok(outcome) => Ok(outcome),
            Err(first_error) => {
                log(format!(
                    "sync {action} attempt failed in {}ms; retrying with full validation: {first_error:#}",
                    sync_started.elapsed().as_millis()
                ));
                sync_once(
                    &project,
                    &deployment,
                    &mut session,
                    &acknowledged,
                    PlanMode::Full,
                    &changed,
                )
                .with_context(|| format!("full retry after {action} failure: {first_error:#}"))
            }
        }
        .and_then(|outcome| {
            sync::commit_snapshot(&mut acknowledged, &outcome.plan);
            if matches!(outcome.plan.kind, devsync::protocol::PlanKind::Full { .. }) {
                save_snapshot(&project.snapshot_path, &acknowledged)?;
            }
            Ok(outcome)
        });
        match &result {
            Ok(outcome) => {
                let elapsed = sync_started.elapsed().as_millis();
                match &outcome.plan.kind {
                    devsync::protocol::PlanKind::Full { entries } => log(format!(
                        "sync validate completed in {elapsed}ms; checked {} managed paths; uploaded {} files",
                        entries.len(),
                        outcome.requested.len()
                    )),
                    devsync::protocol::PlanKind::Delta { changes } if changes.is_empty() => log(
                        format!("sync delta completed in {elapsed}ms; no managed changes"),
                    ),
                    devsync::protocol::PlanKind::Delta { changes } => log(format!(
                        "sync delta completed in {elapsed}ms; changed paths: {}",
                        format_changed_paths(&changes.keys().cloned().collect())
                    )),
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
            Ok(_) => force_full = false,
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
    if let Some(session) = session
        && let Err(error) = session.close()
    {
        log(format!("close remote agent failed: {error:#}"));
    }
    save_snapshot(&project.snapshot_path, &acknowledged)?;
    Ok(())
}

struct AgentSession {
    agent: deploy::AgentChild,
    remote: transport::RemoteState,
}

impl AgentSession {
    fn connect(deployment: &deploy::Deployment) -> Result<Self> {
        let mut agent = deployment.launch()?;
        let timeout = agent.terminator().watchdog(AGENT_SESSION_TIMEOUT);
        let result = transport::connect(&mut agent.stdout, &mut agent.stdin);
        if timeout.finish() {
            let _ = agent.wait();
            bail!(
                "remote agent handshake timed out after {}s",
                AGENT_SESSION_TIMEOUT.as_secs()
            );
        }
        match result {
            Ok(remote) => Ok(Self { agent, remote }),
            Err(error) => {
                let _ = agent.terminator().terminate();
                let _ = agent.wait();
                Err(error)
            }
        }
    }

    fn transact(
        &mut self,
        project: &Project,
        plan: devsync::protocol::Plan,
    ) -> Result<transport::Transaction> {
        let transaction_timeout = if matches!(&plan.kind, devsync::protocol::PlanKind::Full { .. })
        {
            AGENT_FULL_TRANSACTION_TIMEOUT
        } else {
            AGENT_SESSION_TIMEOUT
        };
        let timeout = self.agent.terminator().watchdog(transaction_timeout);
        let result = transport::transact_plan_connected(
            &mut self.agent.stdout,
            &mut self.agent.stdin,
            &mut self.remote,
            &project.root,
            plan,
        );
        if timeout.finish() {
            bail!(
                "remote agent transaction timed out after {}s",
                transaction_timeout.as_secs()
            );
        }
        result
    }

    fn ping(&mut self) -> Result<()> {
        let timeout = self.agent.terminator().watchdog(AGENT_SESSION_TIMEOUT);
        let result = transport::ping(&mut self.agent.stdout, &mut self.agent.stdin);
        if timeout.finish() {
            bail!("remote agent heartbeat timed out");
        }
        result
    }

    fn close(mut self) -> Result<()> {
        let timeout = self.agent.terminator().watchdog(AGENT_SESSION_TIMEOUT);
        let close = transport::close(&mut self.agent.stdin);
        let wait = self.agent.wait();
        if timeout.finish() {
            bail!(
                "remote agent shutdown timed out after {}s",
                AGENT_SESSION_TIMEOUT.as_secs()
            );
        }
        match (close, wait) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        }
    }

    fn abort(self) {
        let _ = self.agent.terminator().terminate();
        let _ = self.agent.wait();
    }
}

fn sync_once(
    project: &Project,
    deployment: &deploy::Deployment,
    session: &mut Option<AgentSession>,
    acknowledged: &devsync::snapshot::Snapshot,
    mode: PlanMode,
    changed: &BTreeSet<PathBuf>,
) -> Result<transport::Transaction> {
    if session.is_none() {
        *session = Some(AgentSession::connect(deployment)?);
    }
    let plan = {
        let session = session.as_ref().expect("agent session was initialized");
        sync::Planner::new(&project.root, acknowledged).plan(mode, changed, &session.remote)?
    };
    let result = session
        .as_mut()
        .expect("agent session was initialized")
        .transact(project, plan);
    if result.is_err() {
        session.take().expect("failed agent session exists").abort();
    }
    result
}

fn watchman_subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        empty_on_fresh_instance: true,
        ..SubscribeRequest::default()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SyncDecision {
    Skip,
    Delta,
    Full(&'static str),
}

fn classify_sync(
    force_full: bool,
    reconcile: bool,
    flush: bool,
    index_eligibility: bool,
    root: &std::path::Path,
    changed: &BTreeSet<PathBuf>,
) -> SyncDecision {
    if !(force_full || reconcile || flush || index_eligibility || !changed.is_empty()) {
        return SyncDecision::Skip;
    }
    if force_full {
        SyncDecision::Full("forced")
    } else if reconcile {
        SyncDecision::Full("watchman recrawl")
    } else if flush {
        SyncDecision::Full("flush")
    } else if index_eligibility || sync::needs_reconcile(root, changed) {
        SyncDecision::Full("eligibility or directory change")
    } else {
        SyncDecision::Delta
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
            classify_sync(true, false, false, false, temp.path(), &changed),
            SyncDecision::Full("forced")
        );
        assert_eq!(
            classify_sync(false, false, true, false, temp.path(), &changed),
            SyncDecision::Full("flush")
        );
        assert_eq!(
            classify_sync(false, false, false, false, temp.path(), &changed),
            SyncDecision::Delta
        );
    }

    #[test]
    fn skips_empty_filtered_batches() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(
            classify_sync(false, false, false, false, temp.path(), &BTreeSet::new()),
            SyncDecision::Skip
        );
        assert_eq!(
            classify_sync(
                false,
                false,
                false,
                false,
                temp.path(),
                &BTreeSet::from([PathBuf::from("file")])
            ),
            SyncDecision::Delta
        );
        assert_eq!(
            classify_sync(false, true, false, false, temp.path(), &BTreeSet::new()),
            SyncDecision::Full("watchman recrawl")
        );
        assert_eq!(
            classify_sync(false, false, false, true, temp.path(), &BTreeSet::new()),
            SyncDecision::Full("eligibility or directory change")
        );
    }

    #[test]
    fn watchman_subscription_does_not_dump_fresh_trees() {
        assert!(watchman_subscribe_request().empty_on_fresh_instance);
    }
}
