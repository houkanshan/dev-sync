use std::fs;
use std::io::{self, Read};
use std::os::fd::{AsFd, AsRawFd};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Parser;
use devsync::agent;

#[derive(Parser)]
#[command(name = "devsync-agent", version)]
struct Cli {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    state: PathBuf,
    /// Maximum client silence, including local planning and payload preparation.
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..))]
    read_timeout_secs: u64,
}

fn main() {
    if let Err(error) = run() {
        // stdout is reserved exclusively for framed protocol messages.
        eprintln!("devsync-agent: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let root = absolute(&cli.root)?;
    let state = absolute(&cli.state)?;
    if state.starts_with(&root) {
        bail!("--state must be outside --root");
    }
    if let Some(parent) = state.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    // Do not use StdinLock: its buffering could hide bytes from poll.
    let stdin = fs::File::from(io::stdin().as_fd().try_clone_to_owned()?);
    let input = AgentInput {
        file: stdin,
        timeout: Duration::from_secs(cli.read_timeout_secs),
    };
    let stdout = io::stdout().lock();
    agent::serve(&root, &state, input, stdout)
}

/// A half-open SSH connection can keep stdin open after its client disappears.
/// Bound each read wait so that the session unwinds and releases its state lock.
/// Disk work between reads does not consume this budget.
struct AgentInput {
    file: fs::File,
    timeout: Duration,
}

impl Read for AgentInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let started = Instant::now();
        loop {
            let remaining = self.timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "client input timed out",
                ));
            }
            let mut fd = libc::pollfd {
                fd: self.file.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let milliseconds = remaining.as_millis().clamp(1, i32::MAX as u128) as i32;
            // SAFETY: fd points to one initialized pollfd and remains valid for this call.
            let ready = unsafe { libc::poll(&mut fd, 1, milliseconds) };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if ready == 0 {
                continue;
            }
            // Read on hangup too, consuming buffered pipe bytes before reporting EOF.
            return self.file.read(buffer);
        }
    }
}

fn absolute(path: &std::path::Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
