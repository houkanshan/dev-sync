use std::fs;
use std::io;
use std::path::PathBuf;

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
    let stdin = io::stdin().lock();
    let stdout = io::stdout().lock();
    agent::serve(&root, &state, stdin, stdout)
}

fn absolute(path: &std::path::Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
