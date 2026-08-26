use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::protocol::{
    AgentMessage, ClientMessage, PROTOCOL_VERSION, Plan, PlanKind, read_json, write_json,
};
use crate::snapshot::{Entry, Generation, validate_relative_path};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteState {
    pub generation: Generation,
    pub state_id: String,
    pub recovery_required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transaction {
    pub plan: Plan,
    pub requested: BTreeSet<std::path::PathBuf>,
}
pub fn connect<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<RemoteState> {
    write_json(
        writer,
        &ClientMessage::Hello {
            version: PROTOCOL_VERSION,
        },
    )?;
    match read_json(reader)? {
        Some(AgentMessage::Hello {
            version,
            generation,
            state_id,
            recovery_required,
        }) if version == PROTOCOL_VERSION => Ok(RemoteState {
            generation,
            state_id,
            recovery_required,
        }),
        Some(AgentMessage::Hello { version, .. }) => {
            bail!("agent protocol version {version} does not match {PROTOCOL_VERSION}")
        }
        Some(AgentMessage::Error { message }) => bail!("agent handshake failed: {message}"),
        Some(message) => bail!("unexpected agent handshake response: {message:?}"),
        None => bail!("agent closed during handshake"),
    }
}

pub fn close<W: Write>(writer: &mut W) -> Result<()> {
    write_json(writer, &ClientMessage::Complete)
}

/// Runs one plan over an already-handshaken agent session and advances its
/// observed remote state after an acknowledgement.
pub fn transact_connected<R, W, F>(
    reader: &mut R,
    writer: &mut W,
    remote: &mut RemoteState,
    local_root: &Path,
    build_plan: F,
) -> Result<Transaction>
where
    R: Read,
    W: Write,
    F: FnOnce(&RemoteState) -> Result<Plan>,
{
    let plan = build_plan(remote)?;
    if !remote.recovery_required
        && remote.generation == plan.generation
        && remote.state_id == plan.state_id
    {
        return Ok(Transaction {
            plan,
            requested: BTreeSet::new(),
        });
    }
    if remote.generation != plan.expected_generation || remote.state_id != plan.expected_state_id {
        bail!(
            "plan expected generation {} state {}, session has generation {} state {}",
            plan.expected_generation,
            plan.expected_state_id,
            remote.generation,
            remote.state_id
        );
    }

    write_json(writer, &ClientMessage::Plan(plan.clone()))?;
    let requested = match &plan.kind {
        PlanKind::Full { .. } => match read_json(reader)? {
            Some(AgentMessage::NeedPayloads { paths }) => paths,
            Some(AgentMessage::Error { message }) => bail!("agent rejected plan: {message}"),
            Some(message) => bail!("unexpected agent response: {message:?}"),
            None => bail!("agent closed before requesting payloads"),
        },
        PlanKind::Delta { changes } => changes
            .iter()
            .filter(|(_, entry)| matches!(entry, Some(Entry::File { .. })))
            .map(|(path, _)| path.clone())
            .collect(),
    };
    let mut unique = BTreeSet::new();
    for path in requested {
        validate_relative_path(&path)?;
        if !unique.insert(path.clone()) {
            bail!("agent requested duplicate payload: {}", path.display());
        }
        let Some(expected @ Entry::File { digest, size, .. }) = plan.entry(&path) else {
            bail!(
                "agent requested payload not present as a file in plan: {}",
                path.display()
            );
        };
        let source = local_root.join(&path);
        let metadata = fs::symlink_metadata(&source)
            .with_context(|| format!("read payload metadata for {}", path.display()))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("agent requested a non-file payload: {}", path.display());
        }
        if metadata.len() != *size {
            bail!("source changed since plan was created: {}", path.display());
        }
        write_json(
            writer,
            &ClientMessage::Payload {
                path: path.clone(),
                length: *size,
            },
        )?;
        stream_verified(&source, writer, *size, digest, &path)?;
        // Re-read mode and content metadata so a concurrent replacement is detected.
        let after = Entry::from_path(&source)?;
        if !expected.content_matches(&after)
            || !matches!(after, Entry::File { size: after_size, .. } if after_size == *size)
        {
            bail!("source changed while sending: {}", path.display());
        }
        writer.flush()?;
    }
    write_json(writer, &ClientMessage::Done)?;
    match read_json(reader)? {
        Some(AgentMessage::Ack {
            generation,
            state_id,
        }) if generation == plan.generation && state_id == plan.state_id => {
            *remote = RemoteState {
                generation,
                state_id,
                recovery_required: false,
            };
            Ok(Transaction {
                plan,
                requested: unique,
            })
        }
        Some(AgentMessage::Ack {
            generation,
            state_id,
        }) => bail!(
            "agent acknowledged generation {generation} state {state_id}, expected generation {} state {}",
            plan.generation,
            plan.state_id
        ),
        Some(AgentMessage::Error { message }) => bail!("agent failed to apply plan: {message}"),
        Some(message) => bail!("unexpected agent response: {message:?}"),
        None => bail!("agent closed before acknowledging plan"),
    }
}

/// Runs one transaction and closes the agent session. This remains useful for
/// subprocess clients; the daemon uses `connect` and `transact_connected`.
pub fn transact<R, W, F>(
    reader: &mut R,
    writer: &mut W,
    local_root: &Path,
    build_plan: F,
) -> Result<Transaction>
where
    R: Read,
    W: Write,
    F: FnOnce(&RemoteState) -> Result<Plan>,
{
    let mut remote = connect(reader, writer)?;
    let transaction = transact_connected(reader, writer, &mut remote, local_root, build_plan)?;
    close(writer)?;
    Ok(transaction)
}

fn stream_verified<W: Write>(
    source: &Path,
    writer: &mut W,
    expected_size: u64,
    expected_digest: &str,
    relative: &Path,
) -> Result<()> {
    let mut file = fs::File::open(source)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    let mut sent = 0_u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        sent += count as u64;
        if sent > expected_size {
            bail!("source grew while sending: {}", relative.display());
        }
        hasher.update(&buffer[..count]);
        writer.write_all(&buffer[..count])?;
    }
    if sent != expected_size || hasher.finalize().to_hex().as_str() != expected_digest {
        bail!("source changed while sending: {}", relative.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;

    use super::*;
    use crate::protocol::{PlanKind, write_json};

    fn plan(path: &str, bytes: &[u8]) -> Plan {
        let entries = BTreeMap::from([(
            PathBuf::from(path),
            Entry::File {
                digest: blake3::hash(bytes).to_hex().to_string(),
                size: bytes.len() as u64,
                modified_ns: 0,
                executable: false,
            },
        )]);
        Plan {
            expected_generation: 0,
            expected_state_id: crate::snapshot::state_id(&BTreeMap::new()).unwrap(),
            generation: 1,
            state_id: crate::snapshot::state_id(&entries).unwrap(),
            kind: PlanKind::Full { entries },
        }
    }

    fn responses(plan: &Plan, requested: Vec<PathBuf>, ack_generation: u64) -> Cursor<Vec<u8>> {
        let mut bytes = Vec::new();
        write_json(
            &mut bytes,
            &AgentMessage::Hello {
                version: PROTOCOL_VERSION,
                generation: plan.expected_generation,
                state_id: plan.expected_state_id.clone(),
                recovery_required: false,
            },
        )
        .unwrap();
        write_json(&mut bytes, &AgentMessage::NeedPayloads { paths: requested }).unwrap();
        write_json(
            &mut bytes,
            &AgentMessage::Ack {
                generation: ack_generation,
                state_id: plan.state_id.clone(),
            },
        )
        .unwrap();
        Cursor::new(bytes)
    }

    #[test]
    fn rejects_unplanned_payload_request() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("other"), b"data").unwrap();
        let plan = plan("file", b"data");
        let mut reader = responses(&plan, vec![PathBuf::from("other")], 1);
        assert!(transact(&mut reader, &mut Vec::new(), temp.path(), |_| Ok(plan)).is_err());
    }

    #[test]
    fn rejects_changed_source_and_wrong_ack_generation() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("file"), b"changed").unwrap();
        let original_plan = plan("file", b"original");
        let mut reader = responses(&original_plan, vec![PathBuf::from("file")], 1);
        assert!(
            transact(&mut reader, &mut Vec::new(), temp.path(), |_| {
                Ok(original_plan)
            })
            .is_err()
        );

        let plan = plan("file", b"changed");
        let mut reader = responses(&plan, vec![], 2);
        assert!(transact(&mut reader, &mut Vec::new(), temp.path(), |_| Ok(plan)).is_err());
    }

    #[test]
    fn builds_plan_after_observing_remote_state() {
        let temp = tempfile::tempdir().unwrap();
        let plan = plan("file", b"data");
        let mut reader = responses(&plan, vec![], 1);
        let result = transact(&mut reader, &mut Vec::new(), temp.path(), |remote| {
            assert_eq!(remote.generation, 0);
            assert_eq!(remote.state_id, plan.expected_state_id);
            Ok(plan.clone())
        })
        .unwrap();
        assert_eq!(result.plan, plan);
    }

    #[test]
    fn treats_matching_handshake_state_as_lost_ack_replay() {
        let plan = plan("file", b"data");
        let mut bytes = Vec::new();
        write_json(
            &mut bytes,
            &AgentMessage::Hello {
                version: PROTOCOL_VERSION,
                generation: plan.generation,
                state_id: plan.state_id.clone(),
                recovery_required: false,
            },
        )
        .unwrap();
        assert_eq!(
            transact(
                &mut Cursor::new(bytes),
                &mut Vec::new(),
                Path::new("missing"),
                |_| Ok(plan.clone())
            )
            .unwrap()
            .plan,
            plan
        );
    }
}
