use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, mpsc, oneshot};
use watchman_client::prelude::*;
use watchman_client::{SubscriptionData, fields::NameOnly};

use crate::project::{Config, Project};
use crate::sync;

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
    Changed(BTreeSet<PathBuf>),
    Flush(oneshot::Sender<Result<(), String>>),
    Stop(oneshot::Sender<Result<(), String>>),
}

pub async fn run(root: PathBuf) -> Result<()> {
    let project = Project::from_root(root.canonicalize()?);
    std::fs::create_dir_all(project.socket_path.parent().expect("socket has parent"))?;
    if project.socket_path.exists() {
        let _ = std::fs::remove_file(&project.socket_path);
    }
    let config = project.load_config()?;
    let listener = UnixListener::bind(&project.socket_path)
        .with_context(|| format!("bind {}", project.socket_path.display()))?;
    let _socket_guard = SocketGuard(project.socket_path.clone());
    let state = Arc::new(Mutex::new(State {
        started: Instant::now(),
        syncing: false,
        pending: 0,
        last_synced: None,
        last_error: None,
    }));
    let (work_tx, work_rx) = mpsc::channel(128);
    let worker = tokio::task::spawn_blocking({
        let root = project.root.clone();
        let config = config.clone();
        let state = Arc::clone(&state);
        move || worker_loop(root, config, state, work_rx)
    });
    work_tx.send(Work::Changed(BTreeSet::new())).await?;

    let watcher = tokio::spawn(watch(project.root.clone(), work_tx.clone()));
    let mut stopping = false;
    while !stopping {
        let (stream, _) = listener.accept().await?;
        stopping = handle_connection(stream, Arc::clone(&state), &work_tx).await?;
    }
    watcher.abort();
    drop(work_tx);
    worker.await??;
    Ok(())
}

async fn watch(root: PathBuf, work_tx: mpsc::Sender<Work>) -> Result<()> {
    loop {
        match watch_once(&root, &work_tx).await {
            Ok(()) => {}
            Err(error) => {
                eprintln!("watchman disconnected: {error:#}; reconnecting");
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
                let changed = result
                    .files
                    .unwrap_or_default()
                    .into_iter()
                    .map(|file| file.name.into_inner())
                    .collect();
                work_tx.send(Work::Changed(changed)).await?;
            }
            SubscriptionData::Canceled => bail!("subscription canceled"),
            SubscriptionData::StateEnter { .. } | SubscriptionData::StateLeave { .. } => {}
        }
    }
}

fn worker_loop(
    root: PathBuf,
    config: Config,
    state: Arc<Mutex<State>>,
    mut work_rx: mpsc::Receiver<Work>,
) -> Result<()> {
    let runtime = tokio::runtime::Handle::current();
    let mut known = BTreeSet::new();
    while let Some(first) = work_rx.blocking_recv() {
        let mut changed = BTreeSet::new();
        let mut flushes = Vec::new();
        let mut stop = None;
        collect_work(first, &mut changed, &mut flushes, &mut stop);
        std::thread::sleep(Duration::from_millis(20));
        while let Ok(work) = work_rx.try_recv() {
            collect_work(work, &mut changed, &mut flushes, &mut stop);
        }
        let barrier = !flushes.is_empty() || stop.is_some();
        runtime.block_on(async {
            let mut state = state.lock().await;
            state.syncing = true;
            state.pending = changed.len();
        });
        let result = (|| {
            let current = sync::manifest(&root)?;
            if known.is_empty() || barrier || sync::needs_reconcile(&changed) {
                sync::reconcile(&root, &config, &current)?;
            } else {
                sync::apply_delta(&root, &config, &changed, &known, &current)?;
            }
            known = current;
            Ok::<_, anyhow::Error>(())
        })();
        let reply = result
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("{error:#}"));
        runtime.block_on(async {
            let mut state = state.lock().await;
            state.syncing = false;
            state.pending = 0;
            if result.is_ok() {
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

fn collect_work(
    work: Work,
    changed: &mut BTreeSet<PathBuf>,
    flushes: &mut Vec<oneshot::Sender<Result<(), String>>>,
    stop: &mut Option<oneshot::Sender<Result<(), String>>>,
) {
    match work {
        Work::Changed(paths) => changed.extend(paths),
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
