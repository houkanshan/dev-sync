mod daemon;
mod project;
mod sync;

use std::io::ErrorKind;
use std::path::Path;
use std::process::{Command as ProcessCommand, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use daemon::{Request, Response};
use project::Project;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Parser)]
#[command(name = "devsync", version, about = "Fast Git-aware development sync")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Start {
        /// Run in the foreground and write logs to this terminal
        #[arg(long)]
        foreground: bool,
    },
    /// Show this worktree's daemon and sync status
    Status,
    /// Stop after the current sync finishes
    Stop,
    /// Stop this worktree's daemon, then start it (also starts when stopped)
    Restart,
    /// Show this worktree's background daemon log
    Tail {
        /// Number of recent lines to show
        #[arg(short = 'n', long, default_value_t = 20)]
        lines: usize,
        /// Keep streaming new log output until interrupted
        #[arg(short = 'f', long)]
        follow: bool,
    },
    /// Fully validate and sync eligible files now
    Flush,
    #[command(name = "__daemon", hide = true)]
    Daemon { root: String },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    if let Command::Daemon { root } = cli.command {
        return daemon::run(std::path::PathBuf::from(root), false).await;
    }

    let project = Project::discover()?;
    match cli.command {
        Command::Start { foreground } => start(&project, foreground).await,
        Command::Status => request(&project, Request::Status).await.map(print_response),
        Command::Flush => request(&project, Request::Flush).await.map(print_response),
        Command::Stop => request(&project, Request::Stop).await.map(print_response),
        Command::Restart => {
            stop_for_restart(&project).await?;
            start(&project, false).await
        }
        Command::Tail { lines, follow } => tail(&project, lines, follow),
        Command::Daemon { .. } => unreachable!(),
    }
}

async fn start(project: &Project, foreground: bool) -> Result<()> {
    if let Ok(response) = request(project, Request::Status).await {
        print_response(response);
        return Ok(());
    }

    project.load_config()?;
    if foreground {
        return daemon::run(project.root.clone(), true).await;
    }
    std::fs::create_dir_all(
        project
            .log_path
            .parent()
            .context("runtime path has no parent")?,
    )?;
    let log = std::fs::File::create(&project.log_path)
        .with_context(|| format!("create {}", project.log_path.display()))?;
    let stderr = log.try_clone()?;
    ProcessCommand::new(std::env::current_exe()?)
        .arg("__daemon")
        .arg(&project.root)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .context("launch daemon")?;

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if let Ok(response) = request(project, Request::Status).await {
            print_response(match response {
                Response::Ok { .. } => Response::Ok {
                    message: "started".into(),
                },
                error => error,
            });
            return Ok(());
        }
    }

    let detail = std::fs::read_to_string(&project.log_path).unwrap_or_default();
    bail!("daemon did not start; log: {}", detail.trim())
}

async fn stop_for_restart(project: &Project) -> Result<()> {
    let stream = match UnixStream::connect(&project.socket_path).await {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error).context("connect to daemon for restart"),
    };
    match request_on_stream(stream, Request::Stop).await? {
        Response::Ok { .. } => {}
        Response::Error { message } => bail!("daemon stop failed; not restarting: {message}"),
    }
    // Stop replies before remote shutdown and snapshot persistence finish.
    // SocketGuard removes the socket only after the worker has exited.
    wait_for_shutdown(&project.socket_path, Duration::from_secs(65)).await
}

async fn wait_for_shutdown(socket_path: &Path, timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async {
        while socket_path.try_exists().context("check daemon shutdown")? {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    })
    .await
    .context("daemon did not finish stopping; not restarting")?
}

fn tail(project: &Project, lines: usize, follow: bool) -> Result<()> {
    if !project.log_path.try_exists()? {
        bail!(
            "no background daemon log at {}; run `devsync start` first",
            project.log_path.display()
        );
    }
    let mut command = ProcessCommand::new("tail");
    command.arg("-n").arg(lines.to_string());
    if follow {
        command.arg("-f");
    }
    let status = command
        .arg(&project.log_path)
        .status()
        .context("run local tail")?;
    if !status.success() {
        bail!("tail exited with {status}");
    }
    Ok(())
}

async fn request(project: &Project, request: Request) -> Result<Response> {
    let stream = UnixStream::connect(&project.socket_path)
        .await
        .with_context(|| "devsync is not running; run `devsync start`")?;
    request_on_stream(stream, request).await
}

async fn request_on_stream(stream: UnixStream, request: Request) -> Result<Response> {
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(format!("{}\n", serde_json::to_string(&request)?).as_bytes())
        .await?;
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    if line.is_empty() {
        bail!("daemon closed the connection without a response")
    }
    Ok(serde_json::from_str(&line)?)
}

fn print_response(response: Response) {
    match response {
        Response::Ok { message } => println!("{message}"),
        Response::Error { message } => {
            eprintln!("error: {message}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_foreground_start() {
        let cli = Cli::try_parse_from(["devsync", "start", "--foreground"]).unwrap();
        assert!(matches!(cli.command, Command::Start { foreground: true }));
    }

    #[test]
    fn parses_restart_and_tail_options() {
        let cli = Cli::try_parse_from(["devsync", "restart"]).unwrap();
        assert!(matches!(cli.command, Command::Restart));
        let cli = Cli::try_parse_from(["devsync", "tail"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Tail {
                lines: 20,
                follow: false
            }
        ));
        for args in [
            ["devsync", "tail", "-n", "5", "-f"],
            ["devsync", "tail", "--lines", "5", "--follow"],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(
                cli.command,
                Command::Tail {
                    lines: 5,
                    follow: true
                }
            ));
        }
        assert!(Cli::try_parse_from(["devsync", "tail", "-n", "invalid"]).is_err());
    }

    fn temporary_project() -> (tempfile::TempDir, Project) {
        let temp = tempfile::tempdir().unwrap();
        let mut project = Project::from_root(temp.path().to_path_buf());
        project.socket_path = temp.path().join("daemon.sock");
        project.log_path = temp.path().join("daemon.log");
        (temp, project)
    }

    #[tokio::test]
    async fn restart_accepts_missing_and_stale_sockets() {
        let (_temp, project) = temporary_project();
        stop_for_restart(&project).await.unwrap();
        // Bind without listening: connections are refused even if a concurrent
        // subprocess temporarily inherits this descriptor before exec.
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;
        // SAFETY: AF_UNIX/SOCK_STREAM are valid socket parameters.
        let descriptor = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(descriptor >= 0);
        // SAFETY: socket returned a new descriptor owned by this test.
        let socket = unsafe { OwnedFd::from_raw_fd(descriptor) };
        // SAFETY: sockaddr_un contains only integer fields and a byte array.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let bytes = project.socket_path.as_os_str().as_bytes();
        assert!(bytes.len() < address.sun_path.len());
        for (target, byte) in address.sun_path.iter_mut().zip(bytes) {
            *target = *byte as libc::c_char;
        }
        let length = std::mem::size_of_val(&address) as libc::socklen_t;
        #[cfg(target_os = "macos")]
        {
            address.sun_len = length as u8;
        }
        // SAFETY: address is a NUL-terminated Unix address of the stated size.
        let result = unsafe {
            libc::bind(
                socket.as_raw_fd(),
                (&address as *const libc::sockaddr_un).cast(),
                length,
            )
        };
        assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
        drop(socket);
        assert!(project.socket_path.exists());
        stop_for_restart(&project).await.unwrap();
    }

    #[tokio::test]
    async fn restart_waits_for_cleanup_after_stop_acknowledgment() {
        let (_temp, project) = temporary_project();
        let listener = tokio::net::UnixListener::bind(&project.socket_path).unwrap();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let socket_path = project.socket_path.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut line = String::new();
            BufReader::new(reader).read_line(&mut line).await.unwrap();
            assert!(matches!(
                serde_json::from_str(&line).unwrap(),
                Request::Stop
            ));
            writer
                .write_all(b"{\"result\":\"ok\",\"message\":\"stopped\"}\n")
                .await
                .unwrap();
            ack_tx.send(()).unwrap();
            cleanup_rx.await.unwrap();
            drop(listener);
            std::fs::remove_file(socket_path).unwrap();
        });
        let restart = tokio::spawn(async move { stop_for_restart(&project).await });
        ack_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!restart.is_finished());
        cleanup_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), restart)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn restart_rejects_failed_or_missing_stop_response() {
        for reply in [
            "{\"result\":\"error\",\"message\":\"sync failed\"}\n",
            "",
            "not json\n",
        ] {
            let (_temp, project) = temporary_project();
            let listener = tokio::net::UnixListener::bind(&project.socket_path).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (reader, mut writer) = stream.into_split();
                let mut line = String::new();
                BufReader::new(reader).read_line(&mut line).await.unwrap();
                writer.write_all(reply.as_bytes()).await.unwrap();
            });
            assert!(stop_for_restart(&project).await.is_err());
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn cleanup_timeout_does_not_remove_the_socket() {
        let (_temp, project) = temporary_project();
        let _listener = tokio::net::UnixListener::bind(&project.socket_path).unwrap();
        let error = wait_for_shutdown(&project.socket_path, Duration::from_millis(30))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not restarting"));
        assert!(project.socket_path.exists());
    }

    #[tokio::test]
    async fn restart_rejects_non_socket_runtime_paths() {
        let (_temp, project) = temporary_project();
        std::fs::create_dir(&project.socket_path).unwrap();
        assert!(stop_for_restart(&project).await.is_err());
    }

    #[test]
    fn tail_reports_missing_log() {
        let (_temp, project) = temporary_project();
        let error = tail(&project, 20, false).unwrap_err();
        assert!(error.to_string().contains("no background daemon log"));
    }
}
