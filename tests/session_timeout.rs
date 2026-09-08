use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use devsync::transport::{close, connect, ping};

fn agent(root: &Path, state: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_devsync-agent"))
        .arg("--root")
        .arg(root)
        .arg("--state")
        .arg(state)
        .args(["--read-timeout-secs", "1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

#[test]
fn silent_open_pipe_and_partial_frames_release_the_session_lock() {
    // Keep stdin open, as a remote sshd does when its client disappears without EOF.
    for partial in [&b""[..], &b"\0\0"[..], &b"\0\0\0\x10{"[..]] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state.json");
        let mut first = agent(&root, &state);
        let mut input = first.stdin.take().unwrap();
        let mut output = first.stdout.take().unwrap();
        connect(&mut output, &mut input).unwrap();
        input.write_all(partial).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = first.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            if Instant::now() >= deadline {
                first.kill().unwrap();
                first.wait().unwrap();
                panic!("silent agent did not exit");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // A new agent must acquire the same lock while the old client's pipe
        // handle is still alive; deleting the lock file is never necessary.
        let mut second = agent(&root, &state);
        let mut second_input = second.stdin.take().unwrap();
        let mut second_output = second.stdout.take().unwrap();
        connect(&mut second_output, &mut second_input).unwrap();
        close(&mut second_input).unwrap();
        assert!(second.wait().unwrap().success());
        drop(input);
    }
}

#[test]
fn heartbeats_keep_the_session_and_lock_alive_without_syncing() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let state = temp.path().join("state.json");
    let mut first = agent(&root, &state);
    let mut input = first.stdin.take().unwrap();
    let mut output = first.stdout.take().unwrap();
    let initial = connect(&mut output, &mut input).unwrap();
    for _ in 0..8 {
        std::thread::sleep(Duration::from_millis(200));
        ping(&mut output, &mut input).unwrap();
    }
    let mut competing = agent(&root, &state);
    let error = connect(
        &mut competing.stdout.take().unwrap(),
        &mut competing.stdin.take().unwrap(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("another devsync session is active")
    );
    assert!(!competing.wait().unwrap().success());
    ping(&mut output, &mut input).unwrap();
    close(&mut input).unwrap();
    assert!(first.wait().unwrap().success());

    let mut next = agent(&root, &state);
    let mut input = next.stdin.take().unwrap();
    let mut output = next.stdout.take().unwrap();
    assert_eq!(connect(&mut output, &mut input).unwrap(), initial);
    close(&mut input).unwrap();
    assert!(next.wait().unwrap().success());
}
