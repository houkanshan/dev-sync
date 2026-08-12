use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use devsync::protocol::{Plan, PlanKind};
use devsync::snapshot::{Entry, state_id};
use devsync::transport::transact;

#[test]
fn local_agent_subprocess_requests_and_applies_whole_file() {
    let temp = tempfile::tempdir().unwrap();
    let local = temp.path().join("local");
    let remote = temp.path().join("remote");
    let state = temp.path().join("state/snapshot.json");
    fs::create_dir_all(&local).unwrap();
    fs::write(local.join("run"), b"hello from client").unwrap();
    fs::set_permissions(local.join("run"), fs::Permissions::from_mode(0o755)).unwrap();

    let bytes = fs::read(local.join("run")).unwrap();
    let entries = BTreeMap::from([(
        PathBuf::from("run"),
        Entry::File {
            digest: blake3::hash(&bytes).to_hex().to_string(),
            size: bytes.len() as u64,
            modified_ns: 0,
            executable: true,
        },
    )]);
    let plan = Plan {
        expected_generation: 0,
        expected_state_id: state_id(&BTreeMap::new()).unwrap(),
        generation: 1,
        state_id: state_id(&entries).unwrap(),
        kind: PlanKind::Full { entries },
    };

    let mut child = Command::new(env!("CARGO_BIN_EXE_devsync-agent"))
        .arg("--root")
        .arg(&remote)
        .arg("--state")
        .arg(&state)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    assert_eq!(transact(&mut stdout, &mut stdin, &local, plan).unwrap(), 1);
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(remote.join("run")).unwrap(), bytes);
    assert_ne!(
        fs::metadata(remote.join("run"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
}
