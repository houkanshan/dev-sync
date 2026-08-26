use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result, anyhow, bail};

const SSH_OPTIONS: [&str; 14] = [
    "-o",
    "ControlMaster=auto",
    "-o",
    "ControlPersist=10m",
    "-o",
    "ControlPath=~/.ssh/devsync-%C",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=10",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=2",
];

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

pub struct Deployment {
    ssh_program: PathBuf,
    remote: String,
    command: String,
}

impl Deployment {
    pub fn prepare(remote: &str, remote_path: &str) -> Result<Self> {
        Self::prepare_with(
            Path::new("ssh"),
            remote,
            remote_path,
            &artifact_dir()?,
            &native_platform(),
        )
    }

    fn prepare_with(
        ssh_program: &Path,
        remote: &str,
        remote_path: &str,
        artifact_dir: &Path,
        native: &Platform,
    ) -> Result<Self> {
        let platform = probe_with(ssh_program, remote)?;
        let artifact = select_artifact(artifact_dir, &platform, native)?;
        let bytes = fs::read(&artifact)
            .with_context(|| format!("read agent artifact {}", artifact.display()))?;
        let digest = blake3::hash(&bytes).to_hex().to_string();
        ensure_installed(ssh_program, remote, &digest, &bytes)?;

        let state_key = blake3::hash(remote_path.as_bytes()).to_hex().to_string();
        Ok(Self {
            ssh_program: ssh_program.to_path_buf(),
            remote: remote.into(),
            command: launch_command(&digest, remote_path, &state_key),
        })
    }

    pub fn launch(&self) -> Result<AgentChild> {
        let child = remote_sh(&self.ssh_program, &self.remote, &self.command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("launch remote devsync-agent")?;
        AgentChild::from_child(child)
    }
}

pub struct AgentChild {
    child: Arc<Mutex<Child>>,
    stderr: JoinHandle<Vec<u8>>,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

#[derive(Clone)]
pub struct AgentTerminator {
    child: Arc<Mutex<Child>>,
}

impl AgentTerminator {
    pub fn terminate(&self) -> Result<()> {
        let mut child = self
            .child
            .lock()
            .map_err(|_| anyhow!("remote agent process lock was poisoned"))?;
        if child.try_wait()?.is_none() {
            child.kill().context("terminate remote agent")?;
        }
        Ok(())
    }
}

impl AgentChild {
    fn from_child(mut child: Child) -> Result<Self> {
        let stdin = child.stdin.take().context("open remote agent stdin")?;
        let stdout = child.stdout.take().context("open remote agent stdout")?;
        let stderr = child.stderr.take().context("open remote agent stderr")?;
        Ok(Self {
            child: Arc::new(Mutex::new(child)),
            stderr: thread::spawn(move || drain_stderr(stderr)),
            stdin,
            stdout,
        })
    }

    pub fn terminator(&self) -> AgentTerminator {
        AgentTerminator {
            child: Arc::clone(&self.child),
        }
    }

    pub fn wait(self) -> Result<()> {
        drop(self.stdin);
        drop(self.stdout);
        let status = loop {
            let status = self
                .child
                .lock()
                .map_err(|_| anyhow!("remote agent process lock was poisoned"))?
                .try_wait()
                .context("wait for remote agent")?;
            if let Some(status) = status {
                break status;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        };
        let stderr = self
            .stderr
            .join()
            .map_err(|_| anyhow!("remote agent stderr reader panicked"))?;
        if !status.success() {
            bail!(
                "remote agent failed with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        Ok(())
    }
}

fn drain_stderr(mut stderr: ChildStderr) -> Vec<u8> {
    const MAX_STDERR: usize = 64 * 1024;
    let mut tail = Vec::new();
    let mut buffer = [0_u8; 4096];
    while let Ok(count) = stderr.read(&mut buffer) {
        if count == 0 {
            break;
        }
        tail.extend_from_slice(&buffer[..count]);
        if tail.len() > MAX_STDERR {
            tail.drain(..tail.len() - MAX_STDERR);
        }
    }
    tail
}

fn ensure_installed(ssh_program: &Path, remote: &str, digest: &str, bytes: &[u8]) -> Result<()> {
    let output = remote_sh(ssh_program, remote, &installed_command(digest))
        .stdin(Stdio::null())
        .output()
        .context("check remote agent installation")?;
    if output.status.success() {
        return Ok(());
    }
    if output.status.code() != Some(1) {
        bail!(
            "remote agent installation check failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let mut installer = remote_sh(ssh_program, remote, &install_command(digest))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("start remote agent installer")?;
    let mut input = installer.stdin.take().context("open installer stdin")?;
    let upload = input.write_all(bytes).context("upload remote agent");
    drop(input);
    let output = installer
        .wait_with_output()
        .context("wait for remote agent installer")?;
    if !output.status.success() {
        bail!(
            "remote agent installation failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    upload?;
    Ok(())
}

pub fn probe(remote: &str) -> Result<Platform> {
    probe_with(Path::new("ssh"), remote)
}

fn probe_with(ssh_program: &Path, remote: &str) -> Result<Platform> {
    let output = remote_sh(ssh_program, remote, "uname -s; uname -m")
        .stdin(Stdio::null())
        .output()
        .context("probe remote platform")?;
    if !output.status.success() {
        bail!(
            "remote platform probe failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
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

fn remote_sh(program: &Path, remote: &str, script: &str) -> Command {
    let mut command = Command::new(program);
    command
        .args(SSH_OPTIONS)
        .arg(remote)
        .arg(posix_command(script));
    command
}

fn installed_command(digest: &str) -> String {
    let name = shell_quote(&format!("devsync-agent-{digest}"));
    format!(
        "set -eu; cache=${{XDG_CACHE_HOME:-\"$HOME/.cache\"}}/devsync; dst=\"$cache\"/{name}; test -f \"$dst\" && test -x \"$dst\""
    )
}

fn install_command(digest: &str) -> String {
    let name = shell_quote(&format!("devsync-agent-{digest}"));
    format!(
        "set -eu; cache=${{XDG_CACHE_HOME:-\"$HOME/.cache\"}}/devsync; mkdir -p \"$cache\"; dst=\"$cache\"/{name}; tmp=\"$dst.tmp.$$\"; trap 'rm -f \"$tmp\"' 0 HUP INT TERM; cat >\"$tmp\"; chmod 700 \"$tmp\"; mv \"$tmp\" \"$dst\"; trap - 0 HUP INT TERM"
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

fn posix_command(script: &str) -> String {
    format!("sh -c {}", shell_quote(script))
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
    fn terminator_stops_a_blocked_agent_child() {
        let child = Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let agent = AgentChild::from_child(child).unwrap();
        let started = std::time::Instant::now();

        agent.terminator().terminate().unwrap();
        assert!(agent.wait().is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }

    #[test]
    fn commands_quote_remote_paths_and_install_by_content() {
        let check = installed_command("abc");
        assert!(check.contains("test -f \"$dst\""));
        assert!(check.contains("test -x \"$dst\""));
        let install = install_command("abc");
        assert!(install.contains("devsync-agent-abc"));
        assert!(install.contains("mv \"$tmp\" \"$dst\""));
        assert!(!install.contains("cat >/dev/null"));
        let launch = launch_command("abc", "/tmp/a b'c", "state");
        assert!(launch.contains("--root '/tmp/a b'\\''c'"));
        assert!(launch.contains("--state \"$cache/state\"/'state.json'"));
    }

    #[test]
    fn commands_run_through_posix_shell() {
        let command = posix_command("value='a b'; test -n \"$value\"");
        assert!(command.starts_with("sh -c '"));
        assert!(command.contains("value='\\''a b'\\''"));
    }

    #[cfg(unix)]
    mod fake_ssh {
        use std::os::unix::fs::PermissionsExt;

        use super::*;

        struct Fixture {
            temp: tempfile::TempDir,
            ssh: PathBuf,
            marker: PathBuf,
            log: PathBuf,
            upload: PathBuf,
            artifact: Vec<u8>,
            platform: Platform,
        }

        impl Fixture {
            fn new() -> Self {
                let temp = tempfile::tempdir().unwrap();
                let ssh = temp.path().join("fake-ssh");
                let marker = temp.path().join("installed");
                let log = temp.path().join("calls");
                let upload = temp.path().join("upload-bytes");
                let artifact = b"agent artifact bytes".to_vec();
                let platform = Platform {
                    os: "linux".into(),
                    arch: "x86_64".into(),
                };
                fs::write(temp.path().join("devsync-agent-linux-x86_64"), &artifact).unwrap();
                let script = format!(
                    "#!/bin/sh\nset -eu\nfor command; do :; done\nprintf '%s\n' \"$*\" >> {}\ncase \"$command\" in\n  *'uname -s; uname -m'*) printf 'Linux\\nx86_64\\n' ;;\n  *'test -f'*) [ -f {} ] ;;\n  *'cat >\"$tmp\"'*) wc -c | tr -d ' ' > {}; touch {} ;;\n  *'exec \"$cache\"'*) cat >/dev/null ;;\n  *) exit 9 ;;\nesac\n",
                    shell_quote(log.to_str().unwrap()),
                    shell_quote(marker.to_str().unwrap()),
                    shell_quote(upload.to_str().unwrap()),
                    shell_quote(marker.to_str().unwrap()),
                );
                fs::write(&ssh, script).unwrap();
                let mut permissions = fs::metadata(&ssh).unwrap().permissions();
                permissions.set_mode(0o700);
                fs::set_permissions(&ssh, permissions).unwrap();
                Self {
                    temp,
                    ssh,
                    marker,
                    log,
                    upload,
                    artifact,
                    platform,
                }
            }

            fn prepare(&self) -> Deployment {
                Deployment::prepare_with(
                    &self.ssh,
                    "example",
                    "/remote root",
                    self.temp.path(),
                    &self.platform,
                )
                .unwrap()
            }

            fn calls(&self) -> String {
                fs::read_to_string(&self.log).unwrap()
            }

            fn assert_all_calls_use_required_ssh_options_and_posix_shell(&self) {
                let calls = self.calls();
                assert!(!calls.is_empty());
                let prefix = "-o ControlMaster=auto -o ControlPersist=10m -o ControlPath=~/.ssh/devsync-%C -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=2 example sh -c '";
                assert!(
                    calls.lines().all(|call| call.starts_with(prefix)),
                    "SSH invocation missing required options or POSIX shell in:\n{calls}"
                );
            }
        }

        #[test]
        fn existing_agent_skips_artifact_upload_and_prepares_only_once() {
            let fixture = Fixture::new();
            fs::write(&fixture.marker, b"existing").unwrap();

            let deployment = fixture.prepare();
            deployment.launch().unwrap().wait().unwrap();
            deployment.launch().unwrap().wait().unwrap();

            assert!(!fixture.upload.exists());
            let calls = fixture.calls();
            assert_eq!(calls.matches("uname -s").count(), 1);
            assert_eq!(calls.matches("test -f").count(), 1);
            assert_eq!(calls.matches("exec \"$cache\"").count(), 2);
            assert!(!calls.contains("cat >\"$tmp\""));
            fixture.assert_all_calls_use_required_ssh_options_and_posix_shell();
        }

        #[test]
        fn missing_agent_uploads_artifact_exactly_once() {
            let fixture = Fixture::new();

            let deployment = fixture.prepare();
            deployment.launch().unwrap().wait().unwrap();
            deployment.launch().unwrap().wait().unwrap();

            let uploaded: usize = fs::read_to_string(&fixture.upload)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            assert_eq!(uploaded, fixture.artifact.len());
            let calls = fixture.calls();
            assert_eq!(calls.matches("uname -s").count(), 1);
            assert_eq!(calls.matches("test -f").count(), 1);
            assert_eq!(calls.matches("cat >\"$tmp\"").count(), 1);
            assert_eq!(calls.matches("exec \"$cache\"").count(), 2);
            fixture.assert_all_calls_use_required_ssh_options_and_posix_shell();
        }
    }
}
