mod daemon;
mod project;
mod sync;

use std::process::{Command as ProcessCommand, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use daemon::{Request, Response};
use project::Project;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Parser)]
#[command(name = "dev-sync", version, about = "Fast Git-aware development sync")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Start,
    Status,
    Stop,
    Flush,
    #[command(name = "__daemon", hide = true)]
    Daemon {
        root: String,
    },
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
        return daemon::run(std::path::PathBuf::from(root)).await;
    }

    let project = Project::discover()?;
    match cli.command {
        Command::Start => start(&project).await,
        Command::Status => request(&project, Request::Status).await.map(print_response),
        Command::Flush => request(&project, Request::Flush).await.map(print_response),
        Command::Stop => request(&project, Request::Stop).await.map(print_response),
        Command::Daemon { .. } => unreachable!(),
    }
}

async fn start(project: &Project) -> Result<()> {
    if let Ok(response) = request(project, Request::Status).await {
        print_response(response);
        return Ok(());
    }

    project.load_config()?;
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
        if request(project, Request::Status).await.is_ok() {
            let response = request(project, Request::Flush).await?;
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

async fn request(project: &Project, request: Request) -> Result<Response> {
    let stream = UnixStream::connect(&project.socket_path)
        .await
        .with_context(|| "dev-sync is not running; run `dev-sync start`")?;
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
