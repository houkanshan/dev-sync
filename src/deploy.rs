use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use anyhow::{Context, Result, bail};

const SSH_OPTIONS: [&str; 4] = ["-o", "ControlMaster=auto", "-o", "ControlPersist=10m"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

impl Platform {
    pub fn tuple(&self) -> String {
        format!("{}-{}", self.os, self.arch)
    }
}

pub struct AgentChild {
    child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

impl AgentChild {
    pub fn wait(self) -> Result<()> {
        drop(self.stdin);
        drop(self.stdout);
        let output = self
            .child
            .wait_with_output()
            .context("wait for remote agent")?;
        if !output.status.success() {
            bail!(
                "remote agent failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }
}

pub fn launch(remote: &str, remote_path: &str) -> Result<AgentChild> {
    let platform = probe(remote)?;
    let artifact = select_artifact(&artifact_dir()?, &platform, &native_platform())?;
    let bytes = fs::read(&artifact)
        .with_context(|| format!("read agent artifact {}", artifact.display()))?;
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let install = install_command(&digest);
    let mut installer = ssh(remote)
        .arg(install)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .context("start remote agent installer")?;
    installer
        .stdin
        .take()
        .context("open installer stdin")?
        .write_all(&bytes)?;
    let status = installer
        .wait()
        .context("wait for remote agent installer")?;
    if !status.success() {
        bail!("remote agent installation failed with {status}");
    }

    let state_key = blake3::hash(remote_path.as_bytes()).to_hex().to_string();
    let command = launch_command(&digest, remote_path, &state_key);
    let mut child = ssh(remote)
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("launch remote devsync-agent")?;
    Ok(AgentChild {
        stdin: child.stdin.take().context("open remote agent stdin")?,
        stdout: child.stdout.take().context("open remote agent stdout")?,
        child,
    })
}

pub fn probe(remote: &str) -> Result<Platform> {
    let output = ssh(remote)
        .arg("uname -s; uname -m")
        .output()
        .context("probe remote platform")?;
    if !output.status.success() {
        bail!("remote platform probe failed with {}", output.status);
    }
    let text = String::from_utf8(output.stdout).context("remote uname was not UTF-8")?;
    let mut lines = text.lines();
    let os = lines.next().context("remote uname omitted OS")?;
    let arch = lines.next().context("remote uname omitted architecture")?;
    normalize_platform(os, arch)
}

pub fn normalize_platform(os: &str, arch: &str) -> Result<Platform> {
    let os = match os.trim().to_ascii_lowercase().as_str() {
        "darwin" => "darwin",
        "linux" => "linux",
        other => bail!("unsupported remote OS {other}"),
    };
    let arch = match arch.trim().to_ascii_lowercase().as_str() {
        "x86_64" | "amd64" => "x86_64",
        "arm64" | "aarch64" => "aarch64",
        other => bail!("unsupported remote architecture {other}"),
    };
    Ok(Platform {
        os: os.into(),
        arch: arch.into(),
    })
}

pub fn select_artifact(dir: &Path, remote: &Platform, native: &Platform) -> Result<PathBuf> {
    let named = dir.join(format!("devsync-agent-{}", remote.tuple()));
    if named.is_file() {
        return Ok(named);
    }
    let fallback = dir.join("devsync-agent");
    if remote == native && fallback.is_file() {
        return Ok(fallback);
    }
    bail!(
        "no devsync-agent artifact for {}; expected {}",
        remote.tuple(),
        named.display()
    )
}

fn native_platform() -> Platform {
    Platform {
        os: std::env::consts::OS.into(),
        arch: match std::env::consts::ARCH {
            "arm64" => "aarch64".into(),
            arch => arch.into(),
        },
    }
}

fn artifact_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("DEVSYNC_AGENT_ARTIFACT_DIR") {
        return Ok(path.into());
    }
    Ok(std::env::current_exe()?
        .parent()
        .context("current executable has no parent")?
        .to_path_buf())
}

fn ssh(remote: &str) -> Command {
    let mut command = Command::new("ssh");
    command.args(SSH_OPTIONS).arg(remote);
    command
}

fn install_command(digest: &str) -> String {
    let name = format!("devsync-agent-{digest}");
    let name = shell_quote(&name);
    format!(
        "set -eu; cache=${{XDG_CACHE_HOME:-\"$HOME/.cache\"}}/devsync; mkdir -p \"$cache\"; dst=\"$cache\"/{name}; if [ ! -x \"$dst\" ]; then tmp=\"$dst.tmp.$$\"; trap 'rm -f \"$tmp\"' EXIT HUP INT TERM; cat >\"$tmp\"; chmod 700 \"$tmp\"; mv \"$tmp\" \"$dst\"; trap - EXIT HUP INT TERM; else cat >/dev/null; fi"
    )
}

fn launch_command(digest: &str, root: &str, state_key: &str) -> String {
    let name = shell_quote(&format!("devsync-agent-{digest}"));
    let root = shell_quote(root);
    let state = shell_quote(&format!("{state_key}.json"));
    format!(
        "set -eu; cache=${{XDG_CACHE_HOME:-\"$HOME/.cache\"}}/devsync; exec \"$cache\"/{name} --root {root} --state \"$cache/state\"/{state}"
    )
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_common_uname_values() {
        assert_eq!(
            normalize_platform("Linux", "arm64").unwrap().tuple(),
            "linux-aarch64"
        );
        assert_eq!(
            normalize_platform("Darwin", "x86_64").unwrap().tuple(),
            "darwin-x86_64"
        );
    }

    #[test]
    fn artifact_fallback_is_native_only() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("devsync-agent"), b"native").unwrap();
        let native = Platform {
            os: "darwin".into(),
            arch: "aarch64".into(),
        };
        let linux = Platform {
            os: "linux".into(),
            arch: "aarch64".into(),
        };
        assert!(select_artifact(temp.path(), &linux, &native).is_err());
        assert_eq!(
            select_artifact(temp.path(), &native, &native).unwrap(),
            temp.path().join("devsync-agent")
        );
        fs::write(temp.path().join("devsync-agent-linux-aarch64"), b"linux").unwrap();
        assert_eq!(
            select_artifact(temp.path(), &linux, &native).unwrap(),
            temp.path().join("devsync-agent-linux-aarch64")
        );
    }

    #[test]
    fn commands_quote_remote_paths_and_install_by_content() {
        let install = install_command("abc");
        assert!(install.contains("devsync-agent-abc"));
        assert!(install.contains("mv \"$tmp\" \"$dst\""));
        let launch = launch_command("abc", "/tmp/a b'c", "state");
        assert!(launch.contains("--root '/tmp/a b'\\''c'"));
        assert!(launch.contains("--state \"$cache/state\"/'state.json'"));
    }
}
